package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCPort        string
	HTTPPort        string
	DatabaseURL     string
	RedisURL        string
	KafkaBrokers    []string
	ConsumerGroup   string
	AppID           int32
	AppSecret       string
	LogLevel        string
	Reflection      bool
	ShutdownTimeout time.Duration
	RetryDelay      time.Duration
}

func Load() (Config, error) {
	var errs []error
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		errs = append(errs, errors.New("missing DATABASE_URL environment variable"))
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		errs = append(errs, errors.New("missing REDIS_URL environment variable"))
	}
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		errs = append(errs, errors.New("missing KAFKA_BROKERS environment variable"))
	}
	appSecret := os.Getenv("APP_SECRET")
	if appSecret == "" {
		errs = append(errs, errors.New("missing APP_SECRET environment variable"))
	}
	appID, err := strconv.ParseInt(getEnv("APP_ID", "1"), 10, 32)
	if err != nil || appID <= 0 {
		errs = append(errs, errors.New("APP_ID must be a positive integer"))
	}
	reflection, err := strconv.ParseBool(getEnv("GRPC_REFLECTION", "false"))
	if err != nil {
		errs = append(errs, fmt.Errorf("GRPC_REFLECTION: %w", err))
	}
	shutdownTimeout, err := time.ParseDuration(getEnv("SHUTDOWN_TIMEOUT", "10s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err))
	}
	retryDelay, err := time.ParseDuration(getEnv("RETRY_DELAY", "1s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("RETRY_DELAY: %w", err))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return Config{
		GRPCPort:        getEnv("GRPC_PORT", "50052"),
		HTTPPort:        getEnv("HTTP_PORT", "8081"),
		DatabaseURL:     databaseURL,
		RedisURL:        redisURL,
		KafkaBrokers:    strings.Split(kafkaBrokers, ","),
		ConsumerGroup:   getEnv("KAFKA_CONSUMER_GROUP", "chat"),
		AppID:           int32(appID),
		AppSecret:       appSecret,
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		Reflection:      reflection,
		ShutdownTimeout: shutdownTimeout,
		RetryDelay:      retryDelay,
	}, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
