package kafka_store

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/upekZ/matching-engine/internal/models"
	"sync"
)

type KafkaClient struct {
	producer sarama.SyncProducer
	topic    string
	mu       sync.Mutex
}

func NewKafkaClient(brokers []string, topic string) (*KafkaClient, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Transaction.ID = "matching-engine-tx"

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaClient{
		producer: producer,
		topic:    topic,
	}, nil
}

func (kc *KafkaClient) SaveExecutions(exec *models.Execution) error {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	data, err := json.Marshal(exec)
	if err != nil {
		return err
	}

	_, _, err = kc.producer.SendMessage(&sarama.ProducerMessage{
		Topic: kc.topic,
		Value: sarama.ByteEncoder(data),
	})
	return err
}

func (kc *KafkaClient) Close() error {
	return kc.producer.Close()
}
