package config

import (
	"errors"
	"fmt"
	"os"
	"time"
)

type Config struct {
	HTTPPort        string
	SSOAddr         string
	ChatAddr        string
	LogLevel        string
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	var errs []error
	ssoAddr := os.Getenv("SSO_ADDR")
	if ssoAddr == "" {
		errs = append(errs, errors.New("missing SSO_ADDR environment variable"))
	}
	chatAddr := os.Getenv("CHAT_ADDR")
	if chatAddr == "" {
		errs = append(errs, errors.New("missing CHAT_ADDR environment variable"))
	}
	requestTimeout, err := time.ParseDuration(getEnv("REQUEST_TIMEOUT", "5s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("REQUEST_TIMEOUT: %w", err))
	}
	shutdownTimeout, err := time.ParseDuration(getEnv("SHUTDOWN_TIMEOUT", "10s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return Config{
		HTTPPort:        getEnv("HTTP_PORT", "8080"),
		SSOAddr:         ssoAddr,
		ChatAddr:        chatAddr,
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		RequestTimeout:  requestTimeout,
		ShutdownTimeout: shutdownTimeout,
	}, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
