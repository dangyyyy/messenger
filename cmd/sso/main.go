package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"messenger/internal/sso/cache"
	"messenger/internal/sso/config"
	"messenger/internal/sso/domain"
	"messenger/internal/sso/grpcapi"
	"messenger/internal/sso/kafka"
	"messenger/internal/sso/outbox"
	"messenger/internal/sso/service/auth"
	"messenger/internal/sso/storage/postgres"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

const (
	startupTimeout  = 30 * time.Second
	outboxBatchSize = 100
)

func main() {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Error("failed to load .env file", "error", err)
		os.Exit(1)
	}
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		slog.Error("invalid LOG_LEVEL", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))
	if err := run(&cfg); err != nil {
		slog.Error("application stopped", "error", err)
		os.Exit(1)
	}
}
func run(cfg *config.Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startupCtx, cancelStartup := context.WithTimeout(ctx, startupTimeout)
	defer cancelStartup()
	if err := postgres.Migrate(startupCtx, cfg.DatabaseURL); err != nil {
		return err
	}
	storage, err := postgres.New(startupCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer storage.Close()

	// Приложение (и его секрет для подписи JWT) регистрируем из конфига,
	// чтобы chat мог проверять токены тем же секретом
	if cfg.AppSecret != "" {
		app := domain.App{ID: cfg.AppID, Name: cfg.AppName, Secret: cfg.AppSecret}
		if err := storage.SaveApp(startupCtx, app); err != nil {
			return err
		}
	}

	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(redisOptions)
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(startupCtx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}

	apps := cache.NewApps(storage, rdb, cfg.AppCacheTTL)
	limiter := cache.NewLoginLimiter(rdb, cfg.LoginMaxAttempts, cfg.LoginWindow)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	relay := outbox.NewRelay(storage, producer, cfg.OutboxInterval, outboxBatchSize)
	relayCtx, cancelRelay := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancelRelay()
	wg.Go(func() { relay.Run(relayCtx) })

	authService, err := auth.New(storage, apps, limiter, cfg.TokenTTL)
	if err != nil {
		return err
	}
	server, healthServer := grpcapi.NewServer(authService, cfg.Reflection)
	listener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("grpc server starting", "port", cfg.GRPCPort)
		errCh <- server.Serve(listener)
	}()
	select {
	case err := <-errCh:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}
	healthServer.Shutdown()

	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		slog.Info("server stopped gracefully")
	case <-time.After(cfg.ShutdownTimeout):
		slog.Warn("shutdown timed out, closing connections")
		server.Stop()
	}
	return nil
}
