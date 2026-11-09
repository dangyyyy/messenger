package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCPort         string
	DatabaseURL      string
	RedisURL         string
	LogLevel         string
	TokenTTL         time.Duration
	ShutdownTimeout  time.Duration
	Reflection       bool
	AppCacheTTL      time.Duration
	LoginMaxAttempts int64
	LoginWindow      time.Duration
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
	tokenTTL, err := time.ParseDuration(getEnv("TOKEN_TTL", "1h"))
	if err != nil {
		errs = append(errs, fmt.Errorf("TOKEN_TTL: %w", err))
	}
	shutdownTimeout, err := time.ParseDuration(getEnv("SHUTDOWN_TIMEOUT", "10s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err))
	}
	reflection, err := strconv.ParseBool(getEnv("GRPC_REFLECTION", "false"))
	if err != nil {
		errs = append(errs, fmt.Errorf("GRPC_REFLECTION: %w", err))
	}
	appCacheTTL, err := time.ParseDuration(getEnv("APP_CACHE_TTL", "10m"))
	if err != nil {
		errs = append(errs, fmt.Errorf("APP_CACHE_TTL: %w", err))
	}
	loginMaxAttempts, err := strconv.ParseInt(getEnv("LOGIN_MAX_ATTEMPTS", "5"), 10, 64)
	if err != nil || loginMaxAttempts <= 0 {
		errs = append(errs, errors.New("LOGIN_MAX_ATTEMPTS must be a positive integer"))
	}
	loginWindow, err := time.ParseDuration(getEnv("LOGIN_WINDOW", "15m"))
	if err != nil {
		errs = append(errs, fmt.Errorf("LOGIN_WINDOW: %w", err))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return Config{
		GRPCPort:         getEnv("GRPC_PORT", "50051"),
		DatabaseURL:      databaseURL,
		RedisURL:         redisURL,
		LogLevel:         getEnv("LOG_LEVEL", "info"),
		TokenTTL:         tokenTTL,
		ShutdownTimeout:  shutdownTimeout,
		Reflection:       reflection,
		AppCacheTTL:      appCacheTTL,
		LoginMaxAttempts: loginMaxAttempts,
		LoginWindow:      loginWindow,
	}, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
