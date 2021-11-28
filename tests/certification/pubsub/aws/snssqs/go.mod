module github.com/dapr/dapr-components-contrib-forked/tests/certification/pubsub/aws/snssqs

go 1.17

require (
	github.com/aws/aws-sdk-go v1.42.13
	github.com/dapr/components-contrib v1.5.0
	github.com/dapr/kit v0.0.2-0.20210614175626-b9074b64d233
	github.com/stretchr/testify v1.7.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	golang.org/x/sys v0.0.0-20211007075335-d3039528d8ac // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/dapr/components-contrib/tests/certification => ../../../

replace github.com/dapr/components-contrib => ../../../../../

// Uncomment for local development for testing with changes
// in the Dapr runtime. Don't commit with this uncommented!
//
// replace github.com/dapr/dapr => ../../../../../dapr
