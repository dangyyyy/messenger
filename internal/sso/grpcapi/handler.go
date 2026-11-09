package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	ssov1 "messenger/pkg/api/sso/v1"
	"messenger/internal/sso/domain"
	"net/mail"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	minPasswordBytes = 8
	maxPasswordBytes = 72
)

type Auth interface {
	Register(ctx context.Context, email, password string) (int64, error)
	Login(ctx context.Context, email, password string, appID int32) (string, error)
	IsAdmin(ctx context.Context, userID int64) (bool, error)
}

type handler struct {
	ssov1.UnimplementedAuthServiceServer
	auth Auth
}

func (h *handler) Register(ctx context.Context, req *ssov1.RegisterRequest) (*ssov1.RegisterResponse, error) {
	email, err := normalizeEmail(req.GetEmail())
	if err != nil {
		return nil, err
	}
	password := req.GetPassword()
	if len(password) < minPasswordBytes || len(password) > maxPasswordBytes {
		return nil, status.Error(codes.InvalidArgument, "password must be 8 to 72 bytes long")
	}
	id, err := h.auth.Register(ctx, email, password)
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	return &ssov1.RegisterResponse{UserId: id}, nil
}
func (h *handler) Login(ctx context.Context, req *ssov1.LoginRequest) (*ssov1.LoginResponse, error) {
	email, err := normalizeEmail(req.GetEmail())
	if err != nil {
		return nil, err
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password cannot be empty")
	}
	if req.GetAppId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "app_id must be greater than zero")
	}
	token, err := h.auth.Login(ctx, email, req.GetPassword(), req.GetAppId())
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	return &ssov1.LoginResponse{Token: token}, nil
}
func (h *handler) IsAdmin(ctx context.Context, req *ssov1.IsAdminRequest) (*ssov1.IsAdminResponse, error) {
	if req.GetUserId() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id must be positive")
	}

	isAdmin, err := h.auth.IsAdmin(ctx, req.GetUserId())
	if err != nil {
		return nil, toStatus(ctx, err)
	}
	return &ssov1.IsAdminResponse{IsAdmin: isAdmin}, nil
}

func normalizeEmail(raw string) (string, error) {
	email := strings.ToLower(strings.TrimSpace(raw))
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email {
		return "", status.Error(codes.InvalidArgument, "invalid email address")
	}
	return email, nil
}

func toStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, domain.ErrUserExists):
		return status.Error(codes.AlreadyExists, "user already exists")
	case errors.Is(err, domain.ErrInvalidCredentials):
		return status.Error(codes.Unauthenticated, "invalid email or password")
	case errors.Is(err, domain.ErrTooManyAttempts):
		return status.Error(codes.ResourceExhausted, "too many login attempts, try again later")
	case errors.Is(err, domain.ErrAppNotFound):
		return status.Error(codes.InvalidArgument, "unknown app")
	case errors.Is(err, domain.ErrUserNotFound):
		return status.Error(codes.NotFound, "user not found")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request timed out")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	default:
		slog.ErrorContext(ctx, "request failed", "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}
