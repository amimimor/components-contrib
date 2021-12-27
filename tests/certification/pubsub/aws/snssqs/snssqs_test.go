// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package snssqs_test

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/simulate"
	kit_retry "github.com/dapr/kit/retry"

	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	// Pub/Sub.

	"github.com/dapr/components-contrib/pubsub"
	pubsub_snssqs "github.com/dapr/components-contrib/pubsub/aws/snssqs"
	pubsub_loader "github.com/dapr/dapr/pkg/components/pubsub"

	// Dapr runtime and Go-SDK
	"github.com/dapr/dapr/pkg/runtime"
	dapr "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	"github.com/dapr/kit/logger"
	// Certification testing runnables
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/components-contrib/tests/certification/flow/watcher"

	// AWS libraries
	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	//"github.com/aws/aws-sdk-go/service/sns"
	//"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"
)

const (
	sidecarName1      = "dapr-1"
	sidecarName2      = "dapr-2"
	sidecarName3      = "dapr-3"
	appID1            = "app-1"
	appID2            = "app-2"
	appID3            = "app-3"
	dockerComposeYAML = "docker-compose.yml"
	numMessages       = 10
	appPort           = 8000
	portOffset        = 2
	messageKey        = "partitionKey"
	clusterName       = "snssqs"
	pubsubName        = "snssqs"
	topicName         = "topicName"
)

func newLocalstackSession() *session.Session {
	sessionCfg := aws.NewConfig()
	sessionCfg.Endpoint = aws.String("http://localhost:4566")
	sessionCfg.Region = aws.String("us-east-1")
	sessionCfg.Credentials = credentials.NewStaticCredentials("my-accesskey", "my-secretkey", "")
	sessionCfg.DisableSSL = aws.Bool(true)
	opts := session.Options{Profile: "default", Config: *sessionCfg}
	mySession := session.Must(session.NewSessionWithOptions(opts))
	return mySession
}

func TestAWSSnsSqs(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("aws.snssqs", func() pubsub.PubSub {
		return pubsub_snssqs.NewSnsSqs(log)
	})

	// For snssqs without FIFO, we don't have ordering guarantee
	unorderedConsumerWatcher := watcher.NewUnordered()

	// Set the partition key on all messages so they
	// are written to the same partition.
	// This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: "test",
	}

	sendRecvTest := func(metadata map[string]string, messages ...*watcher.Watcher) flow.Runnable {
		_, hasKey := metadata[messageKey]
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)

			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
			}
			for _, m := range messages {
				m.ExpectStrings(msgs...)
			}
			// If no key it provided, create a random one.
			// the topic's partitions.
			if !hasKey {
				metadata[messageKey] = uuid.NewString()
			}

			// Send events that the application above will observe.
			ctx.Log("Sending messages!")
			for _, msg := range msgs {
				ctx.Logf("Sending: %q", msg)
				err := client.PublishEvent(
					ctx, pubsubName, topicName, msg,
					dapr.PublishEventWithMetadata(metadata))
				require.NoError(ctx, err, "error publishing message")
			}

			// Do the messages we observed match what we expect?
			for _, m := range messages {
				m.Assert(ctx, time.Minute)
			}

			return nil
		}
	}

	// sendMessagesInBackground and assertMessages are
	// Runnables for testing publishing and consuming
	// messages reliably when infrastructure and network
	// interruptions occur.
	var task flow.AsyncTask
	sendMessagesInBackground := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)
			for _, m := range messages {
				m.Reset()
			}

			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()

			counter := 1
			for {
				select {
				case <-task.Done():
					return nil
				case <-t.C:
					msg := fmt.Sprintf("Background message - %03d", counter)
					for _, m := range messages {
						m.Prepare(msg) // Track for observation
					}

					// Publish with retries.
					bo := backoff.WithContext(backoff.NewConstantBackOff(time.Second), task)
					if err := kit_retry.NotifyRecover(func() error {
						return client.PublishEvent(
							// Using ctx instead of task here is deliberate.
							// We don't want cancelation to prevent adding
							// the message, only to interrupt between tries.
							ctx, pubsubName, topicName, msg,
							dapr.PublishEventWithMetadata(metadata))
					}, bo, func(err error, t time.Duration) {
						ctx.Logf("Error publishing message, retrying in %s", t)
					}, func() {}); err == nil {
						for _, m := range messages {
							m.Add(msg) // Success
						}
						counter++
					}
				}
			}
		}
	}
	assertMessages := func(messages ...*watcher.Watcher) flow.Runnable {
		return func(ctx flow.Context) error {
			// Signal sendMessagesInBackground to stop and wait for it to complete.
			task.CancelAndWait()
			for _, m := range messages {
				m.Assert(ctx, 5*time.Minute)
			}

			return nil
		}
	}
	// Application logic that tracks messages from a topic.
	application := func(messages *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {
			// Simulate periodic errors.
			sim := simulate.PeriodicError(ctx, 100)

			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: "snssqs",
					Topic:      "topicName",
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					if err := sim(); err != nil {
						return true, err
					}
					// Track/Observe the data of the event.
					messages.Observe(e.Data)
					return false, nil
				}),
			)
		}
	}

	flow.New(t, "aws pubsub snssqs certification (unordered)").
		// Run Kafka using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for localstack (AWS) readiness", retry.Do(5*time.Second, 30, waitForSTS)).
		//
		// Run the application logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			application(unorderedConsumerWatcher))).
		//
		// Run the Dapr sidecar with the pubsub aws snssqs component.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/vanilla"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(component))).
		//
		// Run the second application.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			application(unorderedConsumerWatcher))).
		//
		// Run the Dapr sidecar with the Kafka component.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/vanilla"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithPubSubs(component))).
		//
		// Send messages using the same metadata/message keys
		Step("send and wait", sendRecvTest(metadata, unorderedConsumerWatcher)).
		//
		// Run the third application.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+portOffset*2),
			application(unorderedConsumerWatcher))).
		//
		// Run the Dapr sidecar with the snssqs component.
		Step(sidecar.Run(sidecarName3,
			embedded.WithComponentsPath("./components/vanilla"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*2),
			runtime.WithPubSubs(component))).
		Step("reset", flow.Reset(unorderedConsumerWatcher)).
		//
		// Send messages with random keys to test message consumption
		// across more than one consumer group and consumers per group.
		Step("send and wait", sendRecvTest(map[string]string{}, unorderedConsumerWatcher)).
		//
		// This tests the components' ability to handle reconnections
		// when sns and sqs are shutdown cleanly.
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(unorderedConsumerWatcher)).
		Step("stop sns sqs cluster", dockercompose.Stop(clusterName, dockerComposeYAML, "snssqs")).
		Step("wait", flow.Sleep(5*time.Second)).
		Step("restart sns sqs cluster", dockercompose.Start(clusterName, dockerComposeYAML, "snssqs")).
		Step("wait for localstack (AWS) readiness", retry.Do(5*time.Second, 30, waitForSTS)).
		Step("assert messages", assertMessages(unorderedConsumerWatcher)).
		Step("reset", flow.Reset(unorderedConsumerWatcher)).

		//
		// Simulate a network interruption.
		// This tests the components' ability to handle reconnections
		// when Dapr is disconnected abnormally.
		StepAsync("steady flow of messages to publish", &task,
			sendMessagesInBackground(unorderedConsumerWatcher)).
		Step("wait", flow.Sleep(5*time.Second)).

		// Errors will occur here.
		Step("interrupt network",
			network.InterruptNetwork(1*time.Second, nil, nil, "4566")).
		//
		// Component should recover at this point.
		Step("wait", flow.Sleep(5*time.Second)).
		Step("assert messages", assertMessages(unorderedConsumerWatcher)).

		//
		// Shutdown
		Step("reset", flow.Reset(unorderedConsumerWatcher)).
		Step("wait", flow.Sleep(15*time.Second)).
		Step("stop sidecar 1", sidecar.Stop(sidecarName1)).
		Step("wait", flow.Sleep(3*time.Second)).
		Step("stop app 1", app.Stop(appID1)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("stop sidecar 2", sidecar.Stop(sidecarName2)).
		Step("wait", flow.Sleep(3*time.Second)).
		Step("stop app 2", app.Stop(appID2)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("stop sidecar 3", sidecar.Stop(sidecarName3)).
		Step("wait", flow.Sleep(3*time.Second)).
		Step("stop app 3", app.Stop(appID3)).
		Step("wait", flow.Sleep(10*time.Second)).
		Step("stop sns sqs cluster", dockercompose.Stop(clusterName, dockerComposeYAML, "snssqs")).
		Step("wait", flow.Sleep(5*time.Second)).
		Run()
}

func TestAWSSnsSqsDeadletters(t *testing.T) {
	log := logger.NewLogger("dapr.components")
	component := pubsub_loader.New("aws.snssqs", func() pubsub.PubSub {
		return pubsub_snssqs.NewSnsSqs(log)
	})

	// For snssqs without FIFO, we don't have ordering guarantee
	unorderedConsumerWatcher := watcher.NewUnordered()
	dlqUnorderedConsumerWatcher := watcher.NewUnordered()

	// Set the partition key on all messages so they
	// are written to the same partition.
	// This allows for checking of ordered messages.
	metadata := map[string]string{
		messageKey: "test",
	}

	sendRecvTest := func(metadata map[string]string, messages ...*watcher.Watcher) flow.Runnable {
		_, hasKey := metadata[messageKey]
		return func(ctx flow.Context) error {
			client := sidecar.GetClient(ctx, sidecarName1)

			// Declare what is expected BEFORE performing any steps
			// that will satisfy the test.
			// Here we expect nothing to be present in the ack'ed messages as they would fail
			msgs := make([]string, numMessages)
			for i := range msgs {
				msgs[i] = fmt.Sprintf("Hello, Messages %03d", i)
			}

			for _, m := range messages {
				m.ExpectStrings(msgs...)
			}
			// If no key it provided, create a random one.
			// For Kafka, this will spread messages across
			// the topic's partitions.
			if !hasKey {
				metadata[messageKey] = uuid.NewString()
			}

			// Send events that the receiveFailingApplication above will observe.
			ctx.Log("Sending messages!")
			for _, msg := range msgs {
				ctx.Logf("Sending: %q", msg)
				err := client.PublishEvent(
					ctx, pubsubName, topicName, msg,
					dapr.PublishEventWithMetadata(metadata))
				require.NoError(ctx, err, "error publishing message")
			}

			// Do the messages we observed match what we expect?
			for _, m := range messages {
				m.Assert(t, time.Minute)
			}

			return nil
		}
	}

	// Application logic that fails messages from a topic.
	receiveFailingApplication := func(messages *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {

			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: "snssqs",
					Topic:      "topicName",
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {

					// always fail to process message
					messages.Observe(e.Data)
					return false, fmt.Errorf("simulating consumer error")
				}),
			)
		}
	}

	// Application logic that tracks DLQ messages from a topic.
	application := func(messages *watcher.Watcher) app.SetupFn {
		return func(ctx flow.Context, s common.Service) error {

			// Setup the /orders event handler.
			return multierr.Combine(
				s.AddTopicEventHandler(&common.Subscription{
					PubsubName: "snssqs",
					Topic:      "topicName",
					Route:      "/orders",
				}, func(_ context.Context, e *common.TopicEvent) (retry bool, err error) {
					// always fail to process message
					messages.Observe(e.Data)
					fmt.Printf("\napplication handled data. source: %s, data: %s, id: %s\n", e.Source, e.Data, e.ID)
					return false, nil
				}),
			)
		}
	}

	flow.New(t, "aws pubsub snssqs certification (unordered)").
		// Run Kafka using Docker Compose.
		Step(dockercompose.Run(clusterName, dockerComposeYAML)).
		Step("wait for localstack (AWS) readiness", retry.Do(time.Second, 30, waitForSTS)).
		//
		// Run the receiveFailingApplication logic above.
		Step(app.Run(appID1, fmt.Sprintf(":%d", appPort),
			receiveFailingApplication(unorderedConsumerWatcher))).
		//
		// Run the Dapr sidecar with the pubsub aws snssqs component.
		Step(sidecar.Run(sidecarName1,
			embedded.WithComponentsPath("./components/deadletters/alpha"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithPubSubs(component))).
		//
		// Run the second receiveFailingApplication.
		Step(app.Run(appID2, fmt.Sprintf(":%d", appPort+portOffset),
			receiveFailingApplication(unorderedConsumerWatcher))).
		//
		// Run the Dapr sidecar with the component.
		Step(sidecar.Run(sidecarName2,
			embedded.WithComponentsPath("./components/deadletters/alpha"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset),
			runtime.WithPubSubs(component))).

		// Run the third application.
		Step(app.Run(appID3, fmt.Sprintf(":%d", appPort+portOffset*2),
			application(dlqUnorderedConsumerWatcher))).
		//
		// Run the Dapr sidecar with the component.
		Step(sidecar.Run(sidecarName3,
			// using a component that would consume messages sent to the deadletters queue
			embedded.WithComponentsPath("./components/deadletters/beta"),
			embedded.WithAppProtocol(runtime.HTTPProtocol, appPort+portOffset*2),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort+portOffset*2),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort+portOffset*2),
			embedded.WithProfilePort(runtime.DefaultProfilePort+portOffset*2),
			runtime.WithPubSubs(component))).
		// Send messages using the same metadata/message keys
		Step("send and wait", sendRecvTest(metadata, unorderedConsumerWatcher, dlqUnorderedConsumerWatcher)).
		//Step("wait", flow.Sleep(30*time.Second)).
		//Step("stop app1", sidecar.Stop(appID1)).
		//Step("stop app2", sidecar.Stop(appID2)).
		//Step("stop app3", sidecar.Stop(appID3)).
		Run()
}

func waitForSTS(ctx flow.Context) error {
	sess := newLocalstackSession()
	svc := sts.New(sess)
	input := &sts.GetCallerIdentityInput{}

	callerIdentityOutput, err := svc.GetCallerIdentity(input)
	if err != nil {
		return err
	}

	if *callerIdentityOutput.Account != "000000000000" {
		return fmt.Errorf("account is not as expected: %s", *callerIdentityOutput.Account)
	}

	return nil
}
