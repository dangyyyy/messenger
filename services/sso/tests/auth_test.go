package tests

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ssov1 "messenger/gen/sso/v1"
	"messenger/services/sso/internal/cache"
	"messenger/services/sso/internal/grpcapi"
	"messenger/services/sso/internal/jwt"
	"messenger/services/sso/internal/service/auth"
	pgstorage "messenger/services/sso/internal/storage/postgres"
)

const (
	appID     = 1
	appSecret = "test-secret"
	tokenTTL  = time.Hour
	password  = "correct-horse-battery"

	loginMaxAttempts = 5
	loginWindow      = time.Minute
	appCacheTTL      = time.Minute
)

var (
	client ssov1.AuthServiceClient
	rdb    *redis.Client
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("sso"),
		postgres.WithUsername("sso"),
		postgres.WithPassword("sso"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres: %v", err)
		return 1
	}
	defer func() { _ = container.Terminate(ctx) }()

	databaseURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}

	if err := pgstorage.Migrate(ctx, databaseURL); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}

	if err := seedApp(ctx, databaseURL); err != nil {
		log.Printf("seed app: %v", err)
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

	apps := cache.NewApps(storage, rdb, appCacheTTL)
	limiter := cache.NewLoginLimiter(rdb, loginMaxAttempts, loginWindow)

	service, err := auth.New(storage, apps, limiter, tokenTTL)
	if err != nil {
		log.Printf("create service: %v", err)
		return 1
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Printf("listen: %v", err)
		return 1
	}

	server, _ := grpcapi.NewServer(service, false)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("create client: %v", err)
		return 1
	}
	defer func() { _ = conn.Close() }()

	client = ssov1.NewAuthServiceClient(conn)
	return m.Run()
}

func seedApp(ctx context.Context, databaseURL string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, `INSERT INTO apps (id, name, secret) VALUES ($1, $2, $3)`, appID, "test-app", appSecret)
	return err
}

func uniqueEmail() string {
	return fmt.Sprintf("%s@example.com", strings.ToLower(rand.Text()))
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestRegisterLogin(t *testing.T) {
	ctx := testContext(t)
	email := uniqueEmail()

	registered, err := client.Register(ctx, &ssov1.RegisterRequest{Email: email, Password: password})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if registered.GetUserId() <= 0 {
		t.Fatalf("user id = %d, want positive", registered.GetUserId())
	}

	login, err := client.Login(ctx, &ssov1.LoginRequest{Email: email, Password: password, AppId: appID})
	if err != nil {
		t.Fatalf("login: %v", err)
	}

	claims := &jwt.Claims{}
	_, err = gojwt.ParseWithClaims(login.GetToken(), claims, func(*gojwt.Token) (any, error) {
		return []byte(appSecret), nil
	}, gojwt.WithValidMethods([]string{gojwt.SigningMethodHS256.Alg()}))
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}

	if claims.Subject != strconv.FormatInt(registered.GetUserId(), 10) {
		t.Errorf("subject = %q, want user id %d", claims.Subject, registered.GetUserId())
	}
	if claims.Email != email {
		t.Errorf("email = %q, want %q", claims.Email, email)
	}
	if claims.AppID != appID {
		t.Errorf("app id = %d, want %d", claims.AppID, appID)
	}
	if remaining := time.Until(claims.ExpiresAt.Time); remaining <= 0 || remaining > tokenTTL {
		t.Errorf("token expires in %s, want within %s", remaining, tokenTTL)
	}
}

func TestRegister_Duplicate(t *testing.T) {
	ctx := testContext(t)
	email := uniqueEmail()

	if _, err := client.Register(ctx, &ssov1.RegisterRequest{Email: email, Password: password}); err != nil {
		t.Fatalf("first register: %v", err)
	}

	_, err := client.Register(ctx, &ssov1.RegisterRequest{Email: email, Password: password})
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("code = %s, want %s", got, codes.AlreadyExists)
	}
}

func TestRegister_Validation(t *testing.T) {
	tests := []struct {
		name     string
		email    string
		password string
	}{
		{name: "empty email", email: "", password: password},
		{name: "invalid email", email: "not-an-email", password: password},
		{name: "short password", email: uniqueEmail(), password: "short"},
		{name: "password over 72 bytes", email: uniqueEmail(), password: strings.Repeat("a", 73)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.Register(testContext(t), &ssov1.RegisterRequest{Email: tt.email, Password: tt.password})
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("code = %s, want %s", got, codes.InvalidArgument)
			}
		})
	}
}

func TestLogin_Failures(t *testing.T) {
	ctx := testContext(t)
	email := uniqueEmail()
	if _, err := client.Register(ctx, &ssov1.RegisterRequest{Email: email, Password: password}); err != nil {
		t.Fatalf("register: %v", err)
	}

	tests := []struct {
		name     string
		email    string
		password string
		appID    int32
		want     codes.Code
	}{
		{name: "wrong password", email: email, password: "wrong-password", appID: appID, want: codes.Unauthenticated},
		{name: "unknown email", email: uniqueEmail(), password: password, appID: appID, want: codes.Unauthenticated},
		{name: "unknown app", email: email, password: password, appID: 999, want: codes.InvalidArgument},
		{name: "zero app", email: email, password: password, appID: 0, want: codes.InvalidArgument},
		{name: "empty password", email: email, password: "", appID: appID, want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.Login(testContext(t), &ssov1.LoginRequest{Email: tt.email, Password: tt.password, AppId: tt.appID})
			if got := status.Code(err); got != tt.want {
				t.Fatalf("code = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestIsAdmin(t *testing.T) {
	ctx := testContext(t)

	registered, err := client.Register(ctx, &ssov1.RegisterRequest{Email: uniqueEmail(), Password: password})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	resp, err := client.IsAdmin(ctx, &ssov1.IsAdminRequest{UserId: registered.GetUserId()})
	if err != nil {
		t.Fatalf("is admin: %v", err)
	}
	if resp.GetIsAdmin() {
		t.Error("new user must not be admin")
	}

	_, err = client.IsAdmin(ctx, &ssov1.IsAdminRequest{UserId: 1 << 40})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %s, want %s", got, codes.NotFound)
	}
}

func TestLogin_RateLimit(t *testing.T) {
	ctx := testContext(t)
	email := uniqueEmail()
	if _, err := client.Register(ctx, &ssov1.RegisterRequest{Email: email, Password: password}); err != nil {
		t.Fatalf("register: %v", err)
	}

	for i := range loginMaxAttempts {
		_, err := client.Login(ctx, &ssov1.LoginRequest{Email: email, Password: "wrong-password", AppId: appID})
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Fatalf("attempt %d: code = %s, want %s", i+1, got, codes.Unauthenticated)
		}
	}

	_, err := client.Login(ctx, &ssov1.LoginRequest{Email: email, Password: password, AppId: appID})
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code = %s, want %s", got, codes.ResourceExhausted)
	}
}

func TestLogin_SuccessResetsFailures(t *testing.T) {
	ctx := testContext(t)
	email := uniqueEmail()
	if _, err := client.Register(ctx, &ssov1.RegisterRequest{Email: email, Password: password}); err != nil {
		t.Fatalf("register: %v", err)
	}

	wrongLogin := func() {
		t.Helper()
		_, err := client.Login(ctx, &ssov1.LoginRequest{Email: email, Password: "wrong-password", AppId: appID})
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Fatalf("code = %s, want %s", got, codes.Unauthenticated)
		}
	}

	for range loginMaxAttempts - 1 {
		wrongLogin()
	}
	if _, err := client.Login(ctx, &ssov1.LoginRequest{Email: email, Password: password, AppId: appID}); err != nil {
		t.Fatalf("login: %v", err)
	}
	for range loginMaxAttempts - 1 {
		wrongLogin()
	}
}

func TestLogin_CachesApp(t *testing.T) {
	ctx := testContext(t)
	email := uniqueEmail()
	if _, err := client.Register(ctx, &ssov1.RegisterRequest{Email: email, Password: password}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := client.Login(ctx, &ssov1.LoginRequest{Email: email, Password: password, AppId: appID}); err != nil {
		t.Fatalf("login: %v", err)
	}

	ttl, err := rdb.TTL(ctx, "app:"+strconv.Itoa(appID)).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= 0 || ttl > appCacheTTL {
		t.Fatalf("cache ttl = %s, want within %s", ttl, appCacheTTL)
	}
}
