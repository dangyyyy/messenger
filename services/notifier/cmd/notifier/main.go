package main

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"messenger/services/notifier/internal/config"
	"messenger/services/notifier/internal/consumer"
	"messenger/services/notifier/internal/service/notify"
	"messenger/services/notifier/internal/storage/postgres"
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

	service := notify.New(storage)
	kafkaConsumer, err := consumer.New(cfg.KafkaBrokers, cfg.ConsumerGroup, service, cfg.RetryDelay)
	if err != nil {
		return err
	}
	defer kafkaConsumer.Close()

	slog.Info("notifier started", "group", cfg.ConsumerGroup, "topic", consumer.TopicUserRegistered)
	kafkaConsumer.Run(ctx)
	slog.Info("notifier stopped")
	return nil
}
