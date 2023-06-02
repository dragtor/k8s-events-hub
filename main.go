package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/dragtor/k8s-events-hub/utils"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const (
	CONFIGMAP_K8S_EVENT_TRACKER string = "configmap-k8s-event-tracker"
)

type CustomKafkaEvent struct {
	EventType       string `json:"event_type"`
	EventReason     string `json:"event_reason"`
	Timestamp       string `json:"timestamp"`
	Object          string `json:"object"`
	Message         string `json:"message"`
	ResourceVersion string `json:"resourceVersion"`
}

type KafkaClient struct {
	Broker string
}

type EventSubscriber struct {
	Broker     string
	KakfaTopic string
	Config     *utils.Config
}

func EventSubscriberFactory(config *utils.Config) *EventSubscriber {
	return &EventSubscriber{
		Broker:     config.KafkaBrokerURL,
		KakfaTopic: config.FanOutTopicName,
		Config:     config,
	}
}

func (c *KafkaClient) Producer(servers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(servers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireOne,
	}
}

func ConfigMap(namespace, configMapName string, data map[string]string) *corev1.ConfigMap {
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: namespace,
		},
		Data: data,
	}
	return configMap
}

func createConfigMap(clientset *kubernetes.Clientset, namespace, configMapName string, data map[string]string) error {
	configMap := ConfigMap(namespace, configMapName, data)
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	createdConfigMap, err := clientset.CoreV1().ConfigMaps(namespace).Create(context.Background(), configMap, metav1.CreateOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create ConfigMap")
		return err
	}
	logger.Info().Msgf("ConfigMap %s created", createdConfigMap.Name)
	return nil
}

var (
	NOT_FOUND_CONFIGMAP = errors.New("Not Found")
)

func getConfigMap(clientset *kubernetes.Clientset, namespace, configMapName string) (*corev1.ConfigMap, error) {
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	configMap, err := clientset.CoreV1().ConfigMaps(namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to get ConfigMap %s", configMapName)
		return nil, NOT_FOUND_CONFIGMAP
	}
	logger.Info().Msgf("Fetched ConfigMap %s", configMap.Name)
	return configMap, nil
}

func updateConfigMap(clientset *kubernetes.Clientset, namespace, configMapName string, data map[string]string) error {
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	configMap, err := getConfigMap(clientset, namespace, configMapName)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to create ConfigMap %s", configMapName)
		return err
	}
	configMap.Data = data
	updatedConfigMap, err := clientset.CoreV1().ConfigMaps(namespace).Update(context.Background(), configMap, metav1.UpdateOptions{})
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to update ConfigMap %s", configMapName)
		return err
	}
	logger.Info().Msgf("Updated ConfigMap %s", updatedConfigMap.Name)
	return nil
}

func (es *EventSubscriber) pushEventToKafka(eventObject CustomKafkaEvent) {
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	topic := es.Config.FanOutTopicName
	kafkaBroker := es.Config.KafkaBrokerURL
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", kafkaBroker, topic, partition)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to dial leader")
		return
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	jsonMarshlEvent, err := json.Marshal(eventObject)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to marshal messages")
		return
	}
	logger.Info().Msg(string(jsonMarshlEvent))
	_, err = conn.WriteMessages(kafka.Message{Value: jsonMarshlEvent})

	if err != nil {
		logger.Error().Err(err).Msg("Failed to write messages to kafka")
		return
	}

	if err := conn.Close(); err != nil {
		logger.Error().Err(err).Msg("Failed to close writer")
		return
	}
}

func updateLastProcessedResourceVersion(clientset *kubernetes.Clientset, namespace, resourceVersion string) error {
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	logger.Info().Msgf("Processing resource version  %s", resourceVersion)
	_, err := getConfigMap(clientset, namespace, CONFIGMAP_K8S_EVENT_TRACKER)
	if err == NOT_FOUND_CONFIGMAP {
		// create configmap
		data := map[string]string{
			"resourceVersion": "0",
		}
		err = createConfigMap(clientset, namespace, CONFIGMAP_K8S_EVENT_TRACKER, data)
		if err != nil {
			return err
		}
		return nil
	}
	// if configmap is already present then update value for resourceVersion
	data := map[string]string{
		"resourceVersion": resourceVersion,
	}
	return updateConfigMap(clientset, namespace, CONFIGMAP_K8S_EVENT_TRACKER, data)
}

func checkIsEventProcessed(clientset *kubernetes.Clientset, namespace, resourceVersion string) bool {
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	logger.Info().Msgf("Processing resource version  %s", resourceVersion)
	configMap, err := getConfigMap(clientset, namespace, CONFIGMAP_K8S_EVENT_TRACKER)
	if err == NOT_FOUND_CONFIGMAP {
		return false
	}
	prevResourceVersion := configMap.Data["resourceVersion"]
	prevVersionIntValue, _ := strconv.Atoi(prevResourceVersion)
	newerVersionIntValue, _ := strconv.Atoi(resourceVersion)
	return prevVersionIntValue >= newerVersionIntValue
}

func NewEventWatcher(clientset *kubernetes.Clientset, namespace string) (watch.Interface, error) {
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	eventWatcher, err := clientset.CoreV1().Events(namespace).Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to watch k8s object type")
		return nil, err
	}
	return eventWatcher, nil
}

func (eventSubscriber *EventSubscriber) processK8sEvents(done chan bool, clientset *kubernetes.Clientset, namespace string) {
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	logger.Info().Msgf("Processing k8s events ")
	defer func() {
		logger.Info().Msgf("Stopping watch on events")
	}()
	eventWatcher, err := NewEventWatcher(clientset, namespace)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to watch k8s object type ")
		return
	}
	defer eventWatcher.Stop()
	for {
		resultChan := eventWatcher.ResultChan()
		select {
		case event, ok := <-resultChan:
			if !ok {
				logger.Info().Msgf("Channel closed. Recreating channel...")
				eventWatcher, _ = NewEventWatcher(clientset, namespace)
				continue
			}
			k8sEvent, ok := event.Object.(*corev1.Event)
			if !ok {
				logger.Error().Msg("Failed to parse event object ")
			}
			eventObject := CustomKafkaEvent{
				EventType:       k8sEvent.Type,
				EventReason:     k8sEvent.Reason,
				Timestamp:       k8sEvent.FirstTimestamp.String(),
				Object:          k8sEvent.InvolvedObject.Name,
				Message:         k8sEvent.Message,
				ResourceVersion: k8sEvent.ResourceVersion,
			}
			resourceVersion := k8sEvent.ResourceVersion
			eventProcessed := checkIsEventProcessed(clientset, namespace, resourceVersion)
			if eventProcessed {
				continue
			}
			eventSubscriber.pushEventToKafka(eventObject)
			err := updateLastProcessedResourceVersion(clientset, namespace, resourceVersion)
			if err != nil {
				logger.Error().Err(err).Msgf("Failed to Update resource version %s for eventReason: %s , object: %s generated on time %s", resourceVersion, k8sEvent.Reason, k8sEvent.InvolvedObject.Name, k8sEvent.FirstTimestamp.String())
			}
		case <-done:
			logger.Info().Msgf("Terminating goroutine")
			return
		}
	}

}

func main() {
	var err error
	logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
	logger.Info().Msg("Starting Producer")

	// Read Environment configuration
	envconfig, err := utils.LoadConfig()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to load config")
		return
	}
	var config *rest.Config
	eventSubscriber := EventSubscriberFactory(envconfig)

	// Set kube config context
	if eventSubscriber.Config.InClusterConfig == "false" {
		logger.Info().Msg("Loading out of cluster configuration for kube api server")
		var kubeconfig *string
		if home := homedir.HomeDir(); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
		flag.Parse()
		// use the current context in kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to load config")
			return
		}
	} else {
		// Uses In cluster config for connecting to kube api server
		logger.Info().Msg("Loading kubernetes InClusterConfig")
		config, err = rest.InClusterConfig()
		if err != nil {
			logger.Error().Err(err).Msg("Failed to configure inclusterconfig")
			return
		}
	}
	// Create kubernetes clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create kubernetes go client")
		return
	}
	namespace := "default"
	doneChan := make(chan bool, 1)
	go eventSubscriber.processK8sEvents(doneChan, clientset, namespace)

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	<-stopCh
	doneChan <- true
}
