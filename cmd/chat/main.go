package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"messenger/internal/chat/auth"
	"messenger/internal/chat/config"
	"messenger/internal/chat/consumer"
	"messenger/internal/chat/grpcapi"
	"messenger/internal/chat/realtime"
	"messenger/internal/chat/service/chat"
	"messenger/internal/chat/storage/postgres"
)

const startupTimeout = 30 * time.Second

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

	redisOptions, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(redisOptions)
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(startupCtx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}

	verifier := auth.NewVerifier(cfg.AppSecret, cfg.AppID)
	service := chat.New(storage, realtime.NewPublisher(rdb))

	// Фоновые задачи (консьюмер, WebSocket) живут, пока не отменим bgCtx
	bgCtx, cancelBg := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancelBg()

	kafkaConsumer, err := consumer.New(cfg.KafkaBrokers, cfg.ConsumerGroup, service, cfg.RetryDelay)
	if err != nil {
		return err
	}
	defer kafkaConsumer.Close()
	wg.Go(func() { kafkaConsumer.Run(bgCtx) })

	grpcServer, healthServer := grpcapi.NewServer(service, verifier, cfg.Reflection)
	grpcListener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /ws", realtime.NewHandler(verifier, rdb))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	httpServer := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// WebSocket-соединения не закрываются через Shutdown, поэтому даем им контекст,
		// который отменим сами при остановке
		BaseContext: func(net.Listener) context.Context { return bgCtx },
	}

	errCh := make(chan error, 2)
	go func() {
		slog.Info("grpc server starting", "port", cfg.GRPCPort)
		errCh <- grpcServer.Serve(grpcListener)
	}()
	go func() {
		slog.Info("http server starting", "port", cfg.HTTPPort)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}
	healthServer.Shutdown()
	cancelBg()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Warn("http shutdown", "error", err)
	}

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		slog.Info("server stopped gracefully")
	case <-shutdownCtx.Done():
		slog.Warn("shutdown timed out, closing connections")
		grpcServer.Stop()
	}
	return nil
}
