package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	eventsv1 "messenger/gen/events/v1"
	"messenger/services/notifier/internal/domain"
)

const (
	TopicUserRegistered = "users.registered"
	commitTimeout       = 5 * time.Second
)

type Handler interface {
	UserRegistered(ctx context.Context, event domain.UserRegistered) error
}

type Consumer struct {
	client     *kgo.Client
	handler    Handler
	retryDelay time.Duration
}

func New(brokers []string, group string, handler Handler, retryDelay time.Duration) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(TopicUserRegistered),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Consumer{client: client, handler: handler, retryDelay: retryDelay}, nil
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.WarnContext(ctx, "fetch failed", "topic", topic, "partition", partition, "error", err)
		})

		var processed []*kgo.Record
		for iter := fetches.RecordIter(); !iter.Done(); {
			record := iter.Next()
			if err := c.process(ctx, record); err != nil {
				break
			}
			processed = append(processed, record)
		}

		c.commit(ctx, processed)
		if ctx.Err() != nil {
			return
		}
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}

func (c *Consumer) process(ctx context.Context, record *kgo.Record) error {
	var event eventsv1.UserRegistered
	if err := proto.Unmarshal(record.Value, &event); err != nil {
		slog.ErrorContext(ctx, "skip malformed event", "partition", record.Partition, "offset", record.Offset, "error", err)
		return nil
	}

	for {
		err := c.handler.UserRegistered(ctx, domain.UserRegistered{
			EventID: event.GetEventId(),
			UserID:  event.GetUserId(),
			Email:   event.GetEmail(),
		})
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.WarnContext(ctx, "handle event failed, retrying", "event_id", event.GetEventId(), "error", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.retryDelay):
		}
	}
}

func (c *Consumer) commit(ctx context.Context, records []*kgo.Record) {
	if len(records) == 0 {
		return
	}
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	if err := c.client.CommitRecords(commitCtx, records...); err != nil {
		slog.WarnContext(ctx, "commit offsets failed", "error", err)
	}
}
