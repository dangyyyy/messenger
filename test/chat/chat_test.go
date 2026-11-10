package tests

import (
	"context"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"messenger/internal/chat/auth"
	"messenger/internal/chat/domain"
	"messenger/internal/chat/grpcapi"
	"messenger/internal/chat/realtime"
	"messenger/internal/chat/service/chat"
	pgstorage "messenger/internal/chat/storage/postgres"
	chatv1 "messenger/pkg/api/chat/v1"
)

const (
	appID     = 1
	appSecret = "test-secret"
)

var (
	client  chatv1.ChatServiceClient
	service *chat.Service
	rdb     *redis.Client
	nextID  atomic.Int64
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	pg, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("chat"),
		postgres.WithUsername("chat"),
		postgres.WithPassword("chat"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres: %v", err)
		return 1
	}
	defer func() { _ = pg.Terminate(ctx) }()
	databaseURL, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}
	if err := pgstorage.Migrate(ctx, databaseURL); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}
	storage, err := pgstorage.New(ctx, databaseURL)
	if err != nil {
		log.Printf("create storage: %v", err)
		return 1
	}
	defer storage.Close()

	redisContainer, err := tcredis.Run(ctx, "redis:8-alpine")
	if err != nil {
		log.Printf("start redis: %v", err)
		return 1
	}
	defer func() { _ = redisContainer.Terminate(ctx) }()
	redisURL, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		log.Printf("redis connection string: %v", err)
		return 1
	}
	redisOptions, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("parse redis url: %v", err)
		return 1
	}
	rdb = redis.NewClient(redisOptions)
	defer func() { _ = rdb.Close() }()

	service = chat.New(storage, realtime.NewPublisher(rdb))
	server, _ := grpcapi.NewServer(service, auth.NewVerifier(appSecret, appID), false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Printf("listen: %v", err)
		return 1
	}
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("create client: %v", err)
		return 1
	}
	defer func() { _ = conn.Close() }()
	client = chatv1.NewChatServiceClient(conn)

	nextID.Store(1000)
	return m.Run()
}

// newUser имитирует событие users.registered из sso
func newUser(t *testing.T) domain.User {
	t.Helper()
	id := nextID.Add(1)
	user := domain.User{ID: id, Email: "user" + strconv.FormatInt(id, 10) + "@test.ru"}
	if err := service.UserRegistered(context.Background(), user); err != nil {
		t.Fatalf("register user: %v", err)
	}
	return user
}

func token(t *testing.T, user domain.User, secret string) string {
	t.Helper()
	claims := gojwt.MapClaims{
		"sub":    strconv.FormatInt(user.ID, 10),
		"email":  user.Email,
		"app_id": appID,
		"exp":    time.Now().Add(time.Hour).Unix(),
	}
	signed, err := gojwt.NewWithClaims(gojwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

func as(t *testing.T, user domain.User) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token(t, user, appSecret))
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("code = %v, want %v (err: %v)", got, want, err)
	}
}

func TestAuth(t *testing.T) {
	user := newUser(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.ListChats(ctx, &chatv1.ListChatsRequest{})
	requireCode(t, err, codes.Unauthenticated)

	badCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token(t, user, "wrong-secret"))
	_, err = client.ListChats(badCtx, &chatv1.ListChatsRequest{})
	requireCode(t, err, codes.Unauthenticated)
}

func TestChatFlow(t *testing.T) {
	alice, bob := newUser(t), newUser(t)

	created, err := client.CreateChat(as(t, alice), &chatv1.CreateChatRequest{PeerEmail: bob.Email})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	chatID := created.GetChat().GetId()
	if created.GetChat().GetPeerEmail() != bob.Email {
		t.Fatalf("peer = %q, want %q", created.GetChat().GetPeerEmail(), bob.Email)
	}

	// Повторное создание с любой стороны возвращает тот же личный чат
	again, err := client.CreateChat(as(t, bob), &chatv1.CreateChatRequest{PeerEmail: alice.Email})
	if err != nil {
		t.Fatalf("create chat again: %v", err)
	}
	if again.GetChat().GetId() != chatID {
		t.Fatalf("chat id = %d, want %d", again.GetChat().GetId(), chatID)
	}

	// Боб "подключен": слушаем его Redis-канал, как это делает WebSocket-обработчик
	sub := rdb.Subscribe(context.Background(), realtime.Channel(bob.ID))
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(context.Background()); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	sent, err := client.SendMessage(as(t, alice), &chatv1.SendMessageRequest{ChatId: chatID, Text: "  привет  "})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}
	if sent.GetMessage().GetText() != "привет" {
		t.Fatalf("text = %q, want trimmed", sent.GetMessage().GetText())
	}

	select {
	case msg := <-sub.Channel():
		var got chatv1.Message
		if err := protojson.Unmarshal([]byte(msg.Payload), &got); err != nil {
			t.Fatalf("decode realtime message: %v", err)
		}
		if got.GetId() != sent.GetMessage().GetId() || got.GetSenderId() != alice.ID {
			t.Fatalf("realtime message = %v, want id %d from %d", &got, sent.GetMessage().GetId(), alice.ID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("realtime message not delivered")
	}

	history, err := client.ListMessages(as(t, bob), &chatv1.ListMessagesRequest{ChatId: chatID})
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(history.GetMessages()) != 1 {
		t.Fatalf("messages = %d, want 1", len(history.GetMessages()))
	}

	chats, err := client.ListChats(as(t, bob), &chatv1.ListChatsRequest{})
	if err != nil {
		t.Fatalf("list chats: %v", err)
	}
	if len(chats.GetChats()) != 1 || chats.GetChats()[0].GetLastMessage().GetText() != "привет" {
		t.Fatalf("chats = %v, want one chat with last message", chats.GetChats())
	}
}

func TestPagination(t *testing.T) {
	alice, bob := newUser(t), newUser(t)
	created, err := client.CreateChat(as(t, alice), &chatv1.CreateChatRequest{PeerEmail: bob.Email})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	chatID := created.GetChat().GetId()
	var ids []int64
	for i := range 5 {
		sent, err := client.SendMessage(as(t, alice), &chatv1.SendMessageRequest{ChatId: chatID, Text: "m" + strconv.Itoa(i)})
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		ids = append(ids, sent.GetMessage().GetId())
	}

	page, err := client.ListMessages(as(t, bob), &chatv1.ListMessagesRequest{ChatId: chatID, Limit: 2})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if got := page.GetMessages(); len(got) != 2 || got[0].GetId() != ids[3] || got[1].GetId() != ids[4] {
		t.Fatalf("first page = %v, want ids %v", got, ids[3:])
	}

	older, err := client.ListMessages(as(t, bob), &chatv1.ListMessagesRequest{ChatId: chatID, Limit: 2, BeforeId: ids[3]})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if got := older.GetMessages(); len(got) != 2 || got[0].GetId() != ids[1] || got[1].GetId() != ids[2] {
		t.Fatalf("second page = %v, want ids %v", got, ids[1:3])
	}
}

func TestErrors(t *testing.T) {
	alice, bob, carol := newUser(t), newUser(t), newUser(t)
	created, err := client.CreateChat(as(t, alice), &chatv1.CreateChatRequest{PeerEmail: bob.Email})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	chatID := created.GetChat().GetId()

	tests := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"unknown peer", func() error {
			_, err := client.CreateChat(as(t, alice), &chatv1.CreateChatRequest{PeerEmail: "nobody@test.ru"})
			return err
		}, codes.NotFound},
		{"chat with yourself", func() error {
			_, err := client.CreateChat(as(t, alice), &chatv1.CreateChatRequest{PeerEmail: alice.Email})
			return err
		}, codes.InvalidArgument},
		{"empty text", func() error {
			_, err := client.SendMessage(as(t, alice), &chatv1.SendMessageRequest{ChatId: chatID, Text: "   "})
			return err
		}, codes.InvalidArgument},
		{"outsider sends", func() error {
			_, err := client.SendMessage(as(t, carol), &chatv1.SendMessageRequest{ChatId: chatID, Text: "hi"})
			return err
		}, codes.NotFound},
		{"outsider reads", func() error {
			_, err := client.ListMessages(as(t, carol), &chatv1.ListMessagesRequest{ChatId: chatID})
			return err
		}, codes.NotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireCode(t, tt.call(), tt.want)
		})
	}
}
