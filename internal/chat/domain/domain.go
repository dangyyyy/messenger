package domain

import (
	"errors"
	"time"
)

var (
	ErrUserNotFound = errors.New("user not found")
	ErrChatNotFound = errors.New("chat not found")
	ErrSelfChat     = errors.New("cannot create chat with yourself")
)

// User — копия пользователя из sso: только то, что нужно чату
type User struct {
	ID    int64
	Email string
}

type Message struct {
	ID        int64
	ChatID    int64
	SenderID  int64
	Text      string
	CreatedAt time.Time
}

type Chat struct {
	ID          int64
	Peer        User
	LastMessage *Message
}
