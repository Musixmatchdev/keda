package scalers

import (
	"context"
	"fmt"
	"testing"
)

type parseActiveMQStatisticsMetadataTestData struct {
	name       string
	metadata   map[string]string
	authParams map[string]string
	isError    bool
}

type activeMQStatisticsMetricIdentifier struct {
	metadataTestData *parseActiveMQStatisticsMetadataTestData
	scalerIndex      int
	name             string
}

// Setting metric identifier mock name
var activeMQStatisticsMetricIdentifiers = []activeMQStatisticsMetricIdentifier{
	{&testActiveMQStatisticsMetadata[1], 0, "s0-activemq-statistics-testQueue"},
}

var testActiveMQStatisticsMetadata = []parseActiveMQStatisticsMetadataTestData{
	{
		name:       "nothing passed",
		metadata:   map[string]string{},
		authParams: map[string]string{},
		isError:    true,
	},
	{
		name: "properly formed metadata",
		metadata: map[string]string{
			"stompSSL":         "false",
			"endpoint":         "localhost:8161",
			"failoverEndpoint": "localhost:8162",
			"destinationName":  "testQueue",
			"targetQueueSize":  "10",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: false,
	},
	{
		name: "no failoverEndpoint and stompSSL passed, is ignored",
		metadata: map[string]string{
			"endpoint":        "localhost:8161",
			"destinationName": "testQueue",
			"targetQueueSize": "10",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: false,
	},
	{
		name: "no metricName passed, metricName is generated from destinationName",
		metadata: map[string]string{
			"stompSSL":         "true",
			"endpoint":         "localhost:8161",
			"failoverEndpoint": "localhost:8162",
			"destinationName":  "testQueue",
			"targetQueueSize":  "10",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: false,
	},
	{
		name: "Invalid targetQueueSize using a string",
		metadata: map[string]string{
			"endpoint":         "localhost:8161",
			"failoverEndpoint": "localhost:8162",
			"destinationName":  "testQueue",
			"targetQueueSize":  "AA",
			"metricName":       "testMetricName",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: true,
	},
	{
		name: "Invalid stompSSL",
		metadata: map[string]string{
			"stompSSL":         "1",
			"endpoint":         "localhost:8161",
			"failoverEndpoint": "localhost:8162",
			"destinationName":  "testQueue",
			"targetQueueSize":  "AA",
			"metricName":       "testMetricName",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: true,
	},
	{
		name: "missing endpoint should fail",
		metadata: map[string]string{
			"failoverEndpoint": "localhost:8162",
			"destinationName":  "testQueue",
			"targetQueueSize":  "10",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: true,
	},
	{
		name: "missing destination name, should fail",
		metadata: map[string]string{
			"endpoint":        "localhost:8161",
			"targetQueueSize": "10",
			"metricName":      "testMetricName",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: true,
	},
	{
		name: "missing username, should fail",
		metadata: map[string]string{
			"endpoint":        "localhost:8161",
			"destinationName": "testQueue",
			"targetQueueSize": "10",
			"metricName":      "testMetricName",
		},
		authParams: map[string]string{
			"password": "pass123",
		},
		isError: true,
	},
	{
		name: "missing password, should fail",
		metadata: map[string]string{
			"endpoint":        "localhost:8161",
			"destinationName": "testQueue",
			"targetQueueSize": "10",
			"metricName":      "testMetricName",
		},
		authParams: map[string]string{
			"username": "test",
		},
		isError: true,
	},
}

func TestParseActiveMQStatisticsMetadata(t *testing.T) {
	for _, testData := range testActiveMQStatisticsMetadata {
		t.Run(testData.name, func(t *testing.T) {
			metadata, err := amqsParseMetadata(&ScalerConfig{TriggerMetadata: testData.metadata, AuthParams: testData.authParams})
			if err != nil && !testData.isError {
				t.Error("Expected success but got error", err)
			}
			if testData.isError && err == nil {
				t.Error("Expected error but got success")
			}
			if metadata != nil && metadata.password != "" && metadata.password != testData.authParams["password"] {
				t.Error("Expected password from configuration but found something else: ", metadata.password)
				fmt.Println(testData)
			}
		})
	}
}

var testAmqsDefaultTargetQueueSize = []parseActiveMQStatisticsMetadataTestData{
	{
		name: "properly formed metadata",
		metadata: map[string]string{
			"endpoint":        "localhost:8161",
			"destinationName": "testQueue",
		},
		authParams: map[string]string{
			"username": "test",
			"password": "pass123",
		},
		isError: false,
	},
}

func TestAmqsParseDefaultTargetQueueSize(t *testing.T) {
	for _, testData := range testAmqsDefaultTargetQueueSize {
		t.Run(testData.name, func(t *testing.T) {
			metadata, err := amqsParseMetadata(&ScalerConfig{TriggerMetadata: testData.metadata, AuthParams: testData.authParams})
			switch {
			case err != nil && !testData.isError:
				t.Error("Expected success but got error", err)
			case testData.isError && err == nil:
				t.Error("Expected error but got success")
			case metadata.targetQueueSize != defaultTargetQueueSize:
				t.Error("Expected default targetQueueSize =", defaultTargetQueueSize, "but got", metadata.targetQueueSize)
			}
		})
	}
}

func TestActiveMQStatisticsGetMetricSpecForScaling(t *testing.T) {
	for _, testData := range activeMQStatisticsMetricIdentifiers {
		ctx := context.Background()
		metadata, err := amqsParseMetadata(&ScalerConfig{TriggerMetadata: testData.metadataTestData.metadata, AuthParams: testData.metadataTestData.authParams, ScalerIndex: testData.scalerIndex})
		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}
		mockActiveMQStatisticsScaler := activeMQStatisticsScaler{
			metadata:   metadata,
			connection: &activeMQStatisticsConnection{},
		}

		metricSpec := mockActiveMQStatisticsScaler.GetMetricSpecForScaling(ctx)
		metricName := metricSpec[0].External.Metric.Name
		if metricName != testData.name {
			t.Errorf("Wrong External metric source name: %s, expected: %s", metricName, testData.name)
		}
	}
}
