package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"messenger/services/sso/internal/domain"
	"messenger/services/sso/internal/jwt"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type UserStore interface {
	CreateUser(ctx context.Context, email string, passHash []byte) (int64, error)
	UserByEmail(ctx context.Context, email string) (domain.User, error)
	IsAdmin(ctx context.Context, userID int64) (bool, error)
}

type AppStore interface {
	AppByID(ctx context.Context, id int32) (domain.App, error)
}

type LoginLimiter interface {
	Blocked(ctx context.Context, email string) (bool, error)
	Fail(ctx context.Context, email string) error
	Reset(ctx context.Context, email string) error
}

type Service struct {
	users     UserStore
	apps      AppStore
	limiter   LoginLimiter
	tokenTTL  time.Duration
	dummyHash []byte
}

func New(users UserStore, apps AppStore, limiter LoginLimiter, tokenTTL time.Duration) (*Service, error) {
	dummyHash, err := bcrypt.GenerateFromPassword([]byte("dummy-password"), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("generate dummy hash: %w", err)
	}
	return &Service{users: users, apps: apps, limiter: limiter, tokenTTL: tokenTTL, dummyHash: dummyHash}, nil
}

func (s *Service) Register(ctx context.Context, email, password string) (int64, error) {
	passHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return 0, fmt.Errorf("generate password hash: %w", err)
	}
	id, err := s.users.CreateUser(ctx, email, passHash)
	if err != nil {
		return 0, fmt.Errorf("create user: %w", err)
	}
	return id, nil
}

func (s *Service) Login(ctx context.Context, email, password string, appID int32) (string, error) {
	blocked, err := s.limiter.Blocked(ctx, email)
	if err != nil {
		slog.WarnContext(ctx, "login limiter unavailable", "error", err)
	}
	if blocked {
		return "", domain.ErrTooManyAttempts
	}

	user, err := s.users.UserByEmail(ctx, email)
	if errors.Is(err, domain.ErrUserNotFound) {
		_ = bcrypt.CompareHashAndPassword(s.dummyHash, []byte(password))
		s.registerFailure(ctx, email)
		return "", domain.ErrInvalidCredentials
	}
	if err != nil {
		return "", fmt.Errorf("get user: %w", err)
	}
	if err := bcrypt.CompareHashAndPassword(user.PassHash, []byte(password)); err != nil {
		s.registerFailure(ctx, email)
		return "", domain.ErrInvalidCredentials
	}

	app, err := s.apps.AppByID(ctx, appID)
	if err != nil {
		return "", fmt.Errorf("get app: %w", err)
	}
	token, err := jwt.NewToken(user, app, s.tokenTTL)
	if err != nil {
		return "", fmt.Errorf("create token: %w", err)
	}

	if err := s.limiter.Reset(ctx, email); err != nil {
		slog.WarnContext(ctx, "reset login failures", "error", err)
	}
	return token, nil
}

func (s *Service) IsAdmin(ctx context.Context, userID int64) (bool, error) {
	isAdmin, err := s.users.IsAdmin(ctx, userID)
	if err != nil {
		return false, fmt.Errorf("check user admin: %w", err)
	}
	return isAdmin, nil
}

func (s *Service) registerFailure(ctx context.Context, email string) {
	if err := s.limiter.Fail(ctx, email); err != nil {
		slog.WarnContext(ctx, "register login failure", "error", err)
	}
}
