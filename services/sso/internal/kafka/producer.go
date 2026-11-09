package kafka

import (
	"context"
	"fmt"
	"messenger/services/sso/internal/domain"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const deliveryTimeout = 10 * time.Second

type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Publish(ctx context.Context, message []domain.OutboxMessage) error {
	records := make([]*kgo.Record, 0, len(message))
	for _, msg := range message {
		records = append(records, &kgo.Record{
			Topic: msg.Topic,
			Key:   []byte(msg.Key),
			Value: msg.Payload,
		})
	}
	return p.client.ProduceSync(ctx, records...).FirstErr()
}

func (p *Producer) Close() {
	p.client.Close()
}
