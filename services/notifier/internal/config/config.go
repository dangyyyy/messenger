package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL   string
	KafkaBrokers  []string
	ConsumerGroup string
	LogLevel      string
	RetryDelay    time.Duration
}

func Load() (Config, error) {
	var errs []error
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		errs = append(errs, errors.New("missing DATABASE_URL environment variable"))
	}
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		errs = append(errs, errors.New("missing KAFKA_BROKERS environment variable"))
	}
	retryDelay, err := time.ParseDuration(getEnv("RETRY_DELAY", "1s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("RETRY_DELAY: %w", err))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return Config{
		DatabaseURL:   databaseURL,
		KafkaBrokers:  strings.Split(kafkaBrokers, ","),
		ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "notifier"),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
		RetryDelay:    retryDelay,
	}, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
