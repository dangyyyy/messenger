package tests

import (
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventsv1 "messenger/gen/events/v1"
	"messenger/services/notifier/internal/consumer"
	"messenger/services/notifier/internal/service/notify"
	pgstorage "messenger/services/notifier/internal/storage/postgres"
)

var (
	producer *kgo.Client
	db       *pgxpool.Pool
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("notifier"),
		postgres.WithUsername("notifier"),
		postgres.WithPassword("notifier"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres: %v", err)
		return 1
	}
	defer func() { _ = pgContainer.Terminate(ctx) }()

	databaseURL, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
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

	db, err = pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Printf("create pool: %v", err)
		return 1
	}
	defer db.Close()

	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:8.1.0")
	if err != nil {
		log.Printf("start kafka: %v", err)
		return 1
	}
	defer func() { _ = kafkaContainer.Terminate(ctx) }()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		log.Printf("kafka brokers: %v", err)
		return 1
	}

	producer, err = kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Printf("create producer: %v", err)
		return 1
	}
	defer producer.Close()

	resp, err := kadm.NewClient(producer).CreateTopic(ctx, 3, 1, nil, consumer.TopicUserRegistered)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		log.Printf("create topic: %v", err)
		return 1
	}

	kafkaConsumer, err := consumer.New(brokers, "notifier-test", notify.New(storage), 100*time.Millisecond)
	if err != nil {
		log.Printf("create consumer: %v", err)
		return 1
	}
	defer kafkaConsumer.Close()

	consumerCtx, cancelConsumer := context.WithCancel(ctx)
	defer cancelConsumer()
	go kafkaConsumer.Run(consumerCtx)

	return m.Run()
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func publish(t *testing.T, ctx context.Context, event *eventsv1.UserRegistered) {
	t.Helper()
	payload, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	record := &kgo.Record{
		Topic: consumer.TopicUserRegistered,
		Key:   []byte(strconv.FormatInt(event.GetUserId(), 10)),
		Value: payload,
	}
	if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func newEvent(userID int64) *eventsv1.UserRegistered {
	return &eventsv1.UserRegistered{
		EventId:      uuid.NewString(),
		UserId:       userID,
		Email:        "user" + strconv.FormatInt(userID, 10) + "@example.com",
		RegisteredAt: timestamppb.Now(),
	}
}

func countNotifications(t *testing.T, ctx context.Context, email string) int {
	t.Helper()
	var count int
	err := db.QueryRow(ctx, `SELECT COUNT(*) FROM notifications WHERE email = $1`, email).Scan(&count)
	if err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	return count
}

func waitForNotification(t *testing.T, ctx context.Context, email string) {
	t.Helper()
	for countNotifications(t, ctx, email) == 0 {
		select {
		case <-ctx.Done():
			t.Fatalf("notification for %s not created", email)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestUserRegistered_CreatesWelcomeNotification(t *testing.T) {
	ctx := testContext(t)
	event := newEvent(time.Now().UnixNano())

	publish(t, ctx, event)
	waitForNotification(t, ctx, event.GetEmail())

	var kind string
	var userID int64
	err := db.QueryRow(ctx, `SELECT kind, user_id FROM notifications WHERE email = $1`, event.GetEmail()).Scan(&kind, &userID)
	if err != nil {
		t.Fatalf("select notification: %v", err)
	}
	if kind != "welcome" {
		t.Errorf("kind = %q, want welcome", kind)
	}
	if userID != event.GetUserId() {
		t.Errorf("user id = %d, want %d", userID, event.GetUserId())
	}
}

func TestUserRegistered_DuplicateIsIgnored(t *testing.T) {
	ctx := testContext(t)
	userID := time.Now().UnixNano()
	event := newEvent(userID)

	publish(t, ctx, event)
	publish(t, ctx, event)

	marker := newEvent(userID)
	marker.Email = "marker-" + marker.GetEmail()
	publish(t, ctx, marker)
	waitForNotification(t, ctx, marker.GetEmail())

	if got := countNotifications(t, ctx, event.GetEmail()); got != 1 {
		t.Fatalf("notifications = %d, want 1", got)
	}
}

func TestMalformedEvent_IsSkipped(t *testing.T) {
	ctx := testContext(t)
	userID := time.Now().UnixNano()

	record := &kgo.Record{
		Topic: consumer.TopicUserRegistered,
		Key:   []byte(strconv.FormatInt(userID, 10)),
		Value: []byte("not a protobuf message"),
	}
	if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	event := newEvent(userID)
	publish(t, ctx, event)
	waitForNotification(t, ctx, event.GetEmail())
}
