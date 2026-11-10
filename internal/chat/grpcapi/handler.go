package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"net/mail"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"messenger/internal/chat/auth"
	"messenger/internal/chat/domain"
	"messenger/internal/chat/protoconv"
	chatv1 "messenger/pkg/api/chat/v1"
)

const (
	maxTextLength   = 4000
	defaultPageSize = 50
	maxPageSize     = 100
)

type Chats interface {
	CreateChat(ctx context.Context, me domain.User, peerEmail string) (domain.Chat, error)
	ListChats(ctx context.Context, me domain.User) ([]domain.Chat, error)
	SendMessage(ctx context.Context, me domain.User, chatID int64, text string) (domain.Message, error)
	ListMessages(ctx context.Context, me domain.User, chatID, beforeID int64, limit int) ([]domain.Message, error)
}

type handler struct {
	chatv1.UnimplementedChatServiceServer
	chats Chats
}

func (h *handler) CreateChat(ctx context.Context, req *chatv1.CreateChatRequest) (*chatv1.CreateChatResponse, error) {
	me, err := currentUser(ctx)
	if err != nil {
		return nil, err
	}
	email := strings.ToLower(strings.TrimSpace(req.GetPeerEmail()))
	if address, err := mail.ParseAddress(email); err != nil || address.Address != email {
		return nil, status.Error(codes.InvalidArgument, "invalid peer email")
	}
	chat, err := h.chats.CreateChat(ctx, me, email)
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	return &chatv1.CreateChatResponse{Chat: protoconv.Chat(chat)}, nil
}

func (h *handler) ListChats(ctx context.Context, _ *chatv1.ListChatsRequest) (*chatv1.ListChatsResponse, error) {
	me, err := currentUser(ctx)
	if err != nil {
		return nil, err
	}
	chats, err := h.chats.ListChats(ctx, me)
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	resp := &chatv1.ListChatsResponse{Chats: make([]*chatv1.Chat, 0, len(chats))}
	for _, chat := range chats {
		resp.Chats = append(resp.Chats, protoconv.Chat(chat))
	}
	return resp, nil
}

func (h *handler) SendMessage(ctx context.Context, req *chatv1.SendMessageRequest) (*chatv1.SendMessageResponse, error) {
	me, err := currentUser(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetChatId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "chat_id must be positive")
	}
	text := strings.TrimSpace(req.GetText())
	if text == "" || utf8.RuneCountInString(text) > maxTextLength {
		return nil, status.Error(codes.InvalidArgument, "text must be 1 to 4000 characters")
	}
	msg, err := h.chats.SendMessage(ctx, me, req.GetChatId(), text)
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	return &chatv1.SendMessageResponse{Message: protoconv.Message(msg)}, nil
}

func (h *handler) ListMessages(ctx context.Context, req *chatv1.ListMessagesRequest) (*chatv1.ListMessagesResponse, error) {
	me, err := currentUser(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetChatId() <= 0 || req.GetBeforeId() < 0 {
		return nil, status.Error(codes.InvalidArgument, "chat_id must be positive, before_id must not be negative")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = defaultPageSize
	}
	limit = min(limit, maxPageSize)
	messages, err := h.chats.ListMessages(ctx, me, req.GetChatId(), req.GetBeforeId(), limit)
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	resp := &chatv1.ListMessagesResponse{Messages: make([]*chatv1.Message, 0, len(messages))}
	for _, msg := range messages {
		resp.Messages = append(resp.Messages, protoconv.Message(msg))
	}
	return resp, nil
}

func currentUser(ctx context.Context) (domain.User, error) {
	user, ok := auth.UserFromContext(ctx)
	if !ok {
		return domain.User{}, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	return user, nil
}

func toStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, domain.ErrUserNotFound):
		return status.Error(codes.NotFound, "user not found")
	case errors.Is(err, domain.ErrChatNotFound):
		return status.Error(codes.NotFound, "chat not found")
	case errors.Is(err, domain.ErrSelfChat):
		return status.Error(codes.InvalidArgument, "cannot create chat with yourself")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request timed out")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	default:
		slog.ErrorContext(ctx, "request failed", "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}
