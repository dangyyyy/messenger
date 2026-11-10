package realtime

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	"messenger/internal/chat/auth"
	"messenger/internal/chat/domain"
	"messenger/internal/chat/protoconv"
)

const (
	pingInterval = 30 * time.Second
	writeTimeout = 5 * time.Second
)

// Channel — Redis-канал пользователя: каждая реплика chat слушает каналы своих подключений
func Channel(userID int64) string {
	return "user:" + strconv.FormatInt(userID, 10)
}

type Publisher struct {
	rdb *redis.Client
}

func NewPublisher(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb}
}

// Publish отправляет сообщение в каналы всех участников чата.
// JSON тот же, что отдает REST API, поэтому фронт разбирает его одинаково
func (p *Publisher) Publish(ctx context.Context, userIDs []int64, msg domain.Message) error {
	payload, err := protojson.Marshal(protoconv.Message(msg))
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	pipe := p.rdb.Pipeline()
	for _, id := range userIDs {
		pipe.Publish(ctx, Channel(id), payload)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

// Handler — WebSocket /ws?token=<jwt>: держит соединение и пересылает в него сообщения пользователя
type Handler struct {
	verifier *auth.Verifier
	rdb      *redis.Client
}

func NewHandler(verifier *auth.Verifier, rdb *redis.Client) *Handler {
	return &Handler{verifier: verifier, rdb: rdb}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Браузер не умеет ставить заголовки на WebSocket, поэтому токен приходит в query
	user, err := h.verifier.Verify(r.URL.Query().Get("token"))
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		slog.WarnContext(r.Context(), "websocket accept failed", "error", err)
		return
	}
	defer func() { _ = conn.CloseNow() }()

	// Клиент нам ничего не шлет. CloseRead читает служебные фреймы и отменяет ctx, когда клиент ушел
	ctx := conn.CloseRead(r.Context())

	sub := h.rdb.Subscribe(ctx, Channel(user.ID))
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(ctx); err != nil {
		slog.WarnContext(ctx, "redis subscribe failed", "error", err)
		_ = conn.Close(websocket.StatusInternalError, "subscribe failed")
		return
	}
	slog.InfoContext(ctx, "websocket connected", "user_id", user.ID)

	messages := sub.Channel()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "websocket disconnected", "user_id", user.ID)
			_ = conn.Close(websocket.StatusGoingAway, "bye")
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			if err := write(ctx, conn, []byte(msg.Payload)); err != nil {
				return
			}
		case <-ticker.C:
			// Пинг не дает nginx и балансировщикам закрыть "молчащее" соединение
			pingCtx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func write(ctx context.Context, conn *websocket.Conn, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, payload)
}
