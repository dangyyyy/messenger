package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	"messenger/internal/gateway/config"
	chatv1 "messenger/pkg/api/chat/v1"
	ssov1 "messenger/pkg/api/sso/v1"
)

// round_robin + dns:/// = клиентская балансировка: gRPC-клиент сам резолвит все IP
// сервиса и раскидывает запросы по ним, а не держит одно соединение с одной репликой
const serviceConfig = `{"loadBalancingConfig": [{"round_robin": {}}]}`

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

	ssoConn, err := dial(cfg.SSOAddr)
	if err != nil {
		return err
	}
	defer func() { _ = ssoConn.Close() }()
	chatConn, err := dial(cfg.ChatAddr)
	if err != nil {
		return err
	}
	defer func() { _ = chatConn.Close() }()

	// EmitUnpopulated: пустые поля тоже попадают в JSON, фронту не нужно гадать про undefined
	gwMux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions:   protojson.MarshalOptions{EmitUnpopulated: true},
		UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
	}))
	if err := ssov1.RegisterAuthServiceHandler(ctx, gwMux, ssoConn); err != nil {
		return fmt.Errorf("register sso handler: %w", err)
	}
	if err := chatv1.RegisterChatServiceHandler(ctx, gwMux, chatConn); err != nil {
		return fmt.Errorf("register chat handler: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/", withTimeout(gwMux, cfg.RequestTimeout))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	server := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           logRequests(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("gateway starting", "port", cfg.HTTPPort, "sso", cfg.SSOAddr, "chat", cfg.ChatAddr)
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	select {
	case err := <-errCh:
		return fmt.Errorf("server: %w", err)
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	slog.Info("gateway stopped gracefully")
	return nil
}

func dial(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient("dns:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return conn, nil
}

func withTimeout(next http.Handler, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/healthz" {
			return
		}
		slog.Info("http request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration", time.Since(start).String())
	})
}
