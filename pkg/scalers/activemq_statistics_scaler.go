package scalers

import (
	"context"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	v2 "k8s.io/api/autoscaling/v2"

	// "k8s.io/apimachinery/pkg/api/resource"
	// "k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"

	stomp "github.com/go-stomp/stomp/v3"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type activeMQStatisticsMessageMap struct {
	Entry []struct {
		String []string `xml:"string"`
		Long   string   `xml:"long"`
		Double string   `xml:"double"`
		Int    string   `xml:"int"`
	} `xml:"entry"`
}

type activeMQStatisticsMetrics struct {
	DequeueCount       int64
	AverageMessageSize int64
	BrokerID           string
	MemoryUsage        int64
	ConsumerCount      int64
	MinEnqueueTime     float64
	MemoryPercentUsage int
	AverageEnqueueTime float64
	MessagesCached     int64
	ExpiredCount       int64
	InflightCount      int64
	MaxEnqueueTime     float64
	DispatchCount      int64
	Size               int64
	DestinationName    string
	ProducerCount      int64
	MemoryLimit        int64
	BrokerName         string
	EnqueueCount       int64
}

type activeMQStatisticsScalerMetadata struct {
	stompSSL        bool
	endpoints       []string
	username        string
	password        string
	destinationName string
	targetQueueSize int64
	activationTargetQueueSize int64
	metricName      string
	scalerIndex     int
}

type activeMQStatisticsConnection struct {
	tls    *tls.Conn
	broker *stomp.Conn
}

type activeMQStatisticsScaler struct {
	metricType v2.MetricTargetType
	metadata   *activeMQStatisticsScalerMetadata
	connection *activeMQStatisticsConnection
}

const (
	amqsTargetQueueSizeDefault = 10
	amqsActivationTargetQueueSizeDefault = 0
	amqsQueuePrefix            = "/queue/ActiveMQ.Statistics.Destination."
)

var amqsLog = logf.Log.WithName("activemq_statistics_scaler")

func NewActiveMQStatisticsMetrics() (activeMQStatisticsMetrics) {
	metrics := activeMQStatisticsMetrics{}
	metrics.DequeueCount = 0
	metrics.AverageMessageSize = 0
	metrics.BrokerID = ""
	metrics.MemoryUsage = 0
	metrics.ConsumerCount = 0
	metrics.MinEnqueueTime = 0.0
	metrics.MemoryPercentUsage = 0
	metrics.AverageEnqueueTime = 0.0
	metrics.MessagesCached = 0
	metrics.ExpiredCount = 0
	metrics.InflightCount = 0
	metrics.MaxEnqueueTime = 0.0
	metrics.DispatchCount = 0
	metrics.Size = 0
	metrics.DestinationName = ""
	metrics.ProducerCount = 0
	metrics.MemoryLimit = 0
	metrics.BrokerName = ""
	metrics.EnqueueCount = 0
	return metrics
}

/* ActiveMQ Statistics utils */
func amqsParseMetadata(config *ScalerConfig) (*activeMQStatisticsScalerMetadata, error) {
	meta := activeMQStatisticsScalerMetadata{}

	if val, ok := config.TriggerMetadata["endpoint"]; ok && len(val) > 0 {
		meta.endpoints = append(meta.endpoints, val)
	} else {
		return nil, fmt.Errorf("no endpoints given in metadata")
	}
	if val, ok := config.TriggerMetadata["failoverEndpoint"]; ok && len(val) > 0 {
		meta.endpoints = append(meta.endpoints, val)
	}

	if val, ok := config.TriggerMetadata["stompSSL"]; ok && len(val) > 0 {
		enable, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("invalid stompSSL metadata: must be an string true/false")
		}
		meta.stompSSL = enable
	} else {
		meta.stompSSL = false
	}

	if config.TriggerMetadata["destinationName"] == "" {
		return nil, errors.New("no destinationName given")
	}
	meta.destinationName = config.TriggerMetadata["destinationName"]

	if val, ok := config.TriggerMetadata["targetQueueSize"]; ok {
		queueSize, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid targetQueueSize - must be an integer")
		}

		meta.targetQueueSize = queueSize
	} else {
		meta.targetQueueSize = amqsTargetQueueSizeDefault
	}

	if val, ok := config.TriggerMetadata["activationTargetQueueSize"]; ok {
		activationTargetQueueSize, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid activationTargetQueueSize - must be an integer")
		}
		meta.activationTargetQueueSize = activationTargetQueueSize
	} else {
		meta.activationTargetQueueSize = amqsActivationTargetQueueSizeDefault
	}

	if val, ok := config.AuthParams["username"]; ok && val != "" {
		meta.username = val
	} else if val, ok := config.TriggerMetadata["username"]; ok && val != "" {
		username := val

		if val, ok := config.ResolvedEnv[username]; ok && val != "" {
			meta.username = val
		} else {
			meta.username = username
		}
	}

	if meta.username == "" {
		return nil, fmt.Errorf("username cannot be empty")
	}

	if val, ok := config.AuthParams["password"]; ok && val != "" {
		meta.password = val
	} else if val, ok := config.TriggerMetadata["password"]; ok && val != "" {
		password := val

		if val, ok := config.ResolvedEnv[password]; ok && val != "" {
			meta.password = val
		} else {
			meta.password = password
		}
	}

	if meta.password == "" {
		return nil, fmt.Errorf("password cannot be empty")
	}

	meta.metricName = GenerateMetricNameWithIndex(config.ScalerIndex, kedautil.NormalizeString(fmt.Sprintf("activemq-statistics-%s", meta.destinationName)))
	meta.scalerIndex = config.ScalerIndex

	return &meta, nil
}

func amqsMessageMapParser(msg []byte) (*activeMQStatisticsMetrics, error) {
	var messageMap activeMQStatisticsMessageMap
	var metrics = NewActiveMQStatisticsMetrics()
	if len(msg) == 0 {
		return &metrics, nil
	}

	if err := xml.Unmarshal(msg, &messageMap); err != nil {
		return nil, err
	}
	for _, entry := range messageMap.Entry {
		field := reflect.ValueOf(&metrics).Elem().FieldByName(strings.Title(entry.String[0]))
		if !field.CanSet() {
			continue
		}
		if len(entry.String) > 1 {
			field.SetString(entry.String[1])
		} else if val, err := strconv.ParseInt(entry.Long, 10, 64); err == nil {
			field.SetInt(val)
		} else if val, err := strconv.ParseFloat(entry.Double, 64); err == nil {
			field.SetFloat(val)
		} else if val, err := strconv.ParseInt(entry.Int, 10, 64); err == nil {
			field.SetInt(val)
		} else {
			fmt.Printf("Unknown type of key: %s, entry: %#v\n", entry.String[0], entry)
		}
	}
	return &metrics, nil
}

/* ActiveMQ Statistics Connection */
func (c *activeMQStatisticsConnection) Connect(meta *activeMQStatisticsScalerMetadata) error {
	var err error
	c.tls = nil
	for _, edp := range meta.endpoints {
		if meta.stompSSL {
			if c.tls, err = tls.Dial("tcp", edp, &tls.Config{}); err != nil {
				continue
			}
			if c.broker, err = stomp.Connect(c.tls, stomp.ConnOpt.Login(meta.username, meta.password)); err != nil {
				c.tls.Close()
				continue
			}
		} else {
			if c.broker, err = stomp.Dial("tcp", edp, stomp.ConnOpt.Login(meta.username, meta.password)); err != nil {
				continue
			}
		}
		return nil
	}

	return err
}

func (c *activeMQStatisticsConnection) Disconnect(meta *activeMQStatisticsScalerMetadata) {
	c.broker.Disconnect()
	if meta.stompSSL {
		c.tls.Close()
	}
}

func (c *activeMQStatisticsConnection) RequireStatistics(meta *activeMQStatisticsScalerMetadata) error {
	return c.broker.Send(
		amqsQueuePrefix+meta.destinationName, "", []byte(""),
		stomp.SendOpt.Header("reply-to", amqsQueuePrefix+meta.destinationName),
	)
}

func (c *activeMQStatisticsConnection) GetStatistics(meta *activeMQStatisticsScalerMetadata) (*activeMQStatisticsMetrics, error) {
	sub, err := c.broker.Subscribe(
		amqsQueuePrefix+meta.destinationName,
		stomp.AckAuto, stomp.SubscribeOpt.Id(meta.metricName),
	)
	if err != nil {
		return nil, fmt.Errorf("broker subscribe error: %s", err)
	}
	defer sub.Unsubscribe()

	select {
	case msg := <-sub.C:
		return amqsMessageMapParser(msg.Body)
	case <-time.After(time.Second * 5):
		amqsLog.Info(meta.metricName, "Warning", "GetStatistics: MessageMap timeout (5s)")
		return amqsMessageMapParser([]byte(""))
	}
}

/* ActiveMQ Statistics Scaler */

// NewActiveMQStatisticsScaler creates a new HTTP scaler
func NewActiveMQStatisticsScaler(config *ScalerConfig) (Scaler, error) {
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %s", err)
	}

	meta, err := amqsParseMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing metric API metadata: %s", err)
	}

	return &activeMQStatisticsScaler{
		metricType: metricType,
		metadata:   meta,
		connection: &activeMQStatisticsConnection{},
	}, nil
}

func (s *activeMQStatisticsScaler) GetStatisticsQueueSize(ctx context.Context) (int64, error) {
	if err := s.connection.Connect(s.metadata); err != nil {
		return -1, fmt.Errorf("connection error: %s", err)
	}
	defer s.connection.Disconnect(s.metadata)

	if err := s.connection.RequireStatistics(s.metadata); err != nil {
		return -1, fmt.Errorf("RequireStatistics error: %s", err)
	}

	stats, err := s.connection.GetStatistics(s.metadata)
	if err != nil {
		return -1, fmt.Errorf("GetStatistics error: %s", err)
	}

	return stats.Size, nil
}

// Scaler Interface
//
// GetMetricsAndActivity
// This is the key function of a scaler; it returns:
//
// A value that represents a current state of an external metric (e.g. length of a queue). The return type is an ExternalMetricValue struct which has the following fields:
//
// MetricName: this is the name of the metric that we are returning. The name should be unique, to allow setting multiple (even the same type) Triggers in one ScaledObject, but each function call should return the same name.
// Timestamp: indicates the time at which the metrics were produced.
// WindowSeconds: //TODO
// Value: A numerical value that represents the state of the metric. It could be the length of a queue, or it can be the amount of lag in a stream, but it can also be a simple representation of the state.
// A value that represents an activity of the scaler. The return type is bool.
//
// KEDA polls ScaledObject object according to the pollingInterval configured in the ScaledObject; it checks the last time it was polled, it checks if the number of replicas is greater than 0, and if the scaler itself is active. So if the scaler returns false for IsActive, and if current number of replicas is greater than 0, and there is no configured minimum pods, then KEDA scales down to 0.
//
// Kubernetes HPA (Horizontal Pod Autoscaler) will poll GetMetricsAndActivity regularly through KEDA's metric server (as long as there is at least one pod), and compare the returned value to a configured value in the ScaledObject configuration. Kubernetes will use the following formula to decide whether to scale the pods up and down:
//
// desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )].
func (s *activeMQStatisticsScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) { 
	queueSize, err := s.GetStatisticsQueueSize(ctx)
	
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error inspecting ActiveMQ Statistics queue size: %s", err)
	}
	
	metric := GenerateMetricInMili(metricName, float64(queueSize))
	
	return []external_metrics.ExternalMetricValue{metric}, queueSize > s.metadata.activationTargetQueueSize, nil
}

// GetMetricSpecForScaling returns the MetricSpec for the Horizontal Pod Autoscaler
// Returns the metrics based on which this scaler determines that the ScaleTarget scales. This is used to construct the HPA spec that is created for
// this scaled object. The labels used should match the selectors used in GetMetrics
func (s *activeMQStatisticsScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: s.metadata.metricName,
		},
		Target: GetMetricTarget(s.metricType, s.metadata.targetQueueSize),
	}
	metricSpec := v2.MetricSpec{
		External: externalMetric, Type: externalMetricType,
	}
	return []v2.MetricSpec{metricSpec}
}

// IsActive returns true if there are pending messages to be processed
func (s *activeMQStatisticsScaler) IsActive(ctx context.Context) (bool, error) {
	queueSize, err := s.GetStatisticsQueueSize(ctx)
	if err != nil {
		amqsLog.Error(err, "Error_inspecting_ActiveMQ_statistics_queue_size", queueSize)
		return false, err
	}

	return queueSize > 0, nil
}

// Close does nothing in case of activeMQStatisticsScaler
func (s *activeMQStatisticsScaler) Close(context.Context) error {
	return nil
}
