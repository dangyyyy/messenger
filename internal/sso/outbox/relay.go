package outbox

import (
	"context"
	"log/slog"
	"time"

	"messenger/internal/sso/domain"
)

type Store interface {
	ProcessOutbox(ctx context.Context, limit int, publish func(context.Context, []domain.OutboxMessage) error) (int, error)
}

type Publisher interface {
	Publish(ctx context.Context, messages []domain.OutboxMessage) error
}

type Relay struct {
	store     Store
	publisher Publisher
	interval  time.Duration
	batchSize int
}

func NewRelay(store Store, publisher Publisher, interval time.Duration, batchSize int) *Relay {
	return &Relay{store: store, publisher: publisher, interval: interval, batchSize: batchSize}
}

func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		r.flush(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) flush(ctx context.Context) {
	for {
		sent, err := r.store.ProcessOutbox(ctx, r.batchSize, r.publisher.Publish)
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "outbox relay failed", "error", err)
			}
			return
		}
		if sent < r.batchSize {
			return
		}
	}
}
