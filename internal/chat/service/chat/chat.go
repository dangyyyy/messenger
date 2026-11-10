package chat

import (
	"context"
	"fmt"
	"log/slog"

	"messenger/internal/chat/domain"
)

type Storage interface {
	SaveUser(ctx context.Context, user domain.User) error
	UserByEmail(ctx context.Context, email string) (domain.User, error)
	DirectChat(ctx context.Context, userA, userB int64) (int64, error)
	Chats(ctx context.Context, userID int64) ([]domain.Chat, error)
	Chat(ctx context.Context, chatID, userID int64) (domain.Chat, error)
	Members(ctx context.Context, chatID int64) ([]int64, error)
	CreateMessage(ctx context.Context, chatID, senderID int64, text string) (domain.Message, error)
	Messages(ctx context.Context, chatID, beforeID int64, limit int) ([]domain.Message, error)
}

// Publisher доставляет новое сообщение участникам в реальном времени
type Publisher interface {
	Publish(ctx context.Context, userIDs []int64, msg domain.Message) error
}

type Service struct {
	storage   Storage
	publisher Publisher
}

func New(storage Storage, publisher Publisher) *Service {
	return &Service{storage: storage, publisher: publisher}
}

func (s *Service) CreateChat(ctx context.Context, me domain.User, peerEmail string) (domain.Chat, error) {
	peer, err := s.storage.UserByEmail(ctx, peerEmail)
	if err != nil {
		return domain.Chat{}, err
	}
	if peer.ID == me.ID {
		return domain.Chat{}, domain.ErrSelfChat
	}
	// Себя сохраняем из токена: событие из Kafka могло еще не дойти
	if err := s.storage.SaveUser(ctx, me); err != nil {
		return domain.Chat{}, err
	}
	chatID, err := s.storage.DirectChat(ctx, me.ID, peer.ID)
	if err != nil {
		return domain.Chat{}, err
	}
	return s.storage.Chat(ctx, chatID, me.ID)
}

func (s *Service) ListChats(ctx context.Context, me domain.User) ([]domain.Chat, error) {
	return s.storage.Chats(ctx, me.ID)
}

func (s *Service) SendMessage(ctx context.Context, me domain.User, chatID int64, text string) (domain.Message, error) {
	members, err := s.members(ctx, me, chatID)
	if err != nil {
		return domain.Message{}, err
	}
	msg, err := s.storage.CreateMessage(ctx, chatID, me.ID, text)
	if err != nil {
		return domain.Message{}, err
	}
	// Сообщение уже сохранено: если realtime не сработал, получатель увидит его при загрузке истории
	if err := s.publisher.Publish(ctx, members, msg); err != nil {
		slog.WarnContext(ctx, "publish message failed", "chat_id", chatID, "error", err)
	}
	return msg, nil
}

func (s *Service) ListMessages(ctx context.Context, me domain.User, chatID, beforeID int64, limit int) ([]domain.Message, error) {
	if _, err := s.members(ctx, me, chatID); err != nil {
		return nil, err
	}
	return s.storage.Messages(ctx, chatID, beforeID, limit)
}

// members проверяет, что пользователь состоит в чате. Чужой чат для него "не существует"
func (s *Service) members(ctx context.Context, me domain.User, chatID int64) ([]int64, error) {
	members, err := s.storage.Members(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("load members: %w", err)
	}
	for _, id := range members {
		if id == me.ID {
			return members, nil
		}
	}
	return nil, domain.ErrChatNotFound
}

// UserRegistered — обработчик события из Kafka
func (s *Service) UserRegistered(ctx context.Context, user domain.User) error {
	return s.storage.SaveUser(ctx, user)
}
