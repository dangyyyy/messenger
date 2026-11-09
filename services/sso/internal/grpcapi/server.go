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

	ssov1 "messenger/gen/sso/v1"
)

func NewServer(auth Auth, enableReflection bool) (*grpc.Server, *health.Server) {
	logger := logging.LoggerFunc(func(ctx context.Context, level logging.Level, msg string, fields ...any) {
		slog.Log(ctx, slog.Level(level), msg, fields...)
	})

	onPanic := func(ctx context.Context, p any) error {
		slog.ErrorContext(ctx, "panic recovered", "panic", p, "stack", string(debug.Stack()))
		return status.Error(codes.Internal, "internal error")
	}

	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		logging.UnaryServerInterceptor(logger, logging.WithLogOnEvents(logging.FinishCall)),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandlerContext(onPanic)),
	))

	ssov1.RegisterAuthServiceServer(server, &handler{auth: auth})

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)

	if enableReflection {
		reflection.Register(server)
	}

	return server, healthServer
}
