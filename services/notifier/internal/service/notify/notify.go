package notify

import (
	"context"
	"fmt"
	"log/slog"

	"messenger/services/notifier/internal/domain"
)

type Store interface {
	SaveNotification(ctx context.Context, n domain.Notification) (bool, error)
}

type Service struct {
	store Store
}

func New(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) UserRegistered(ctx context.Context, event domain.UserRegistered) error {
	created, err := s.store.SaveNotification(ctx, domain.Notification{
		EventID: event.EventID,
		UserID:  event.UserID,
		Email:   event.Email,
		Kind:    domain.KindWelcome,
	})
	if err != nil {
		return fmt.Errorf("save welcome notification: %w", err)
	}
	if !created {
		slog.InfoContext(ctx, "duplicate event skipped", "event_id", event.EventID)
		return nil
	}
	slog.InfoContext(ctx, "welcome email queued", "user_id", event.UserID, "email", event.Email)
	return nil
}
