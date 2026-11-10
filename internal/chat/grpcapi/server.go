package grpcapi

import (
	"context"
	"log/slog"
	"runtime/debug"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"messenger/internal/chat/auth"
	chatv1 "messenger/pkg/api/chat/v1"
)

func NewServer(chats Chats, verifier *auth.Verifier, enableReflection bool) (*grpc.Server, *health.Server) {
	logger := logging.LoggerFunc(func(ctx context.Context, level logging.Level, msg string, fields ...any) {
		slog.Log(ctx, slog.Level(level), msg, fields...)
	})
	onPanic := func(ctx context.Context, p any) error {
		slog.ErrorContext(ctx, "panic recovered", "panic", p, "stack", string(debug.Stack()))
		return status.Error(codes.Internal, "internal error")
	}

	// Порядок важен: логируем все запросы, ловим панику, затем проверяем токен
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		logging.UnaryServerInterceptor(logger, logging.WithLogOnEvents(logging.FinishCall)),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandlerContext(onPanic)),
		auth.UnaryInterceptor(verifier),
	))

	chatv1.RegisterChatServiceServer(server, &handler{chats: chats})

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)

	if enableReflection {
		reflection.Register(server)
	}
	return server, healthServer
}
