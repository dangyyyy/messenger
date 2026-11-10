package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"messenger/internal/chat/domain"
	"messenger/internal/chat/migrations"
)

type Storage struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Storage, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pool: %w", err)
	}
	return &Storage{pool: pool}, nil
}

func (s *Storage) Close() {
	s.pool.Close()
}

func Migrate(ctx context.Context, databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() { _ = db.Close() }()

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create migration locker: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("create migration provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// SaveUser — идемпотентная запись: повторное событие или повторный вызов ничего не ломают
func (s *Storage) SaveUser(ctx context.Context, user domain.User) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO users (id, email) VALUES ($1, $2)
		 ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email`,
		user.ID, user.Email,
	)
	if err != nil {
		return fmt.Errorf("save user: %w", err)
	}
	return nil
}

func (s *Storage) UserByEmail(ctx context.Context, email string) (domain.User, error) {
	var user domain.User
	err := s.pool.QueryRow(ctx,
		`SELECT id, email FROM users WHERE email = $1`, email,
	).Scan(&user.ID, &user.Email)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.User{}, domain.ErrUserNotFound
	}
	if err != nil {
		return domain.User{}, fmt.Errorf("select user: %w", err)
	}
	return user, nil
}

// DirectChat находит или создает личный чат двух пользователей
func (s *Storage) DirectChat(ctx context.Context, userA, userB int64) (int64, error) {
	key := fmt.Sprintf("%d:%d", min(userA, userB), max(userA, userB))

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// DO UPDATE вместо DO NOTHING, чтобы RETURNING вернул id и для уже существующего чата
	var chatID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO chats (direct_key) VALUES ($1)
		 ON CONFLICT (direct_key) DO UPDATE SET direct_key = EXCLUDED.direct_key
		 RETURNING id`, key,
	).Scan(&chatID)
	if err != nil {
		return 0, fmt.Errorf("upsert chat: %w", err)
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO chat_members (chat_id, user_id) VALUES ($1, $2), ($1, $3)
		 ON CONFLICT DO NOTHING`, chatID, userA, userB,
	)
	if err != nil {
		return 0, fmt.Errorf("insert members: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}
	return chatID, nil
}

// Chats — чаты пользователя с собеседником и последним сообщением, свежие сверху
func (s *Storage) Chats(ctx context.Context, userID int64) ([]domain.Chat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id, p.id, p.email, m.id, m.sender_id, m.text, m.created_at
		FROM chat_members me
		JOIN chats c         ON c.id = me.chat_id
		JOIN chat_members pm ON pm.chat_id = c.id AND pm.user_id <> me.user_id
		JOIN users p         ON p.id = pm.user_id
		LEFT JOIN LATERAL (
			SELECT id, sender_id, text, created_at
			FROM messages
			WHERE chat_id = c.id
			ORDER BY id DESC
			LIMIT 1
		) m ON true
		WHERE me.user_id = $1
		ORDER BY COALESCE(m.id, 0) DESC, c.id DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("select chats: %w", err)
	}
	defer rows.Close()

	var chats []domain.Chat
	for rows.Next() {
		var (
			chat     domain.Chat
			msgID    *int64
			senderID *int64
			text     *string
			created  sql.NullTime
		)
		if err := rows.Scan(&chat.ID, &chat.Peer.ID, &chat.Peer.Email, &msgID, &senderID, &text, &created); err != nil {
			return nil, fmt.Errorf("scan chat: %w", err)
		}
		if msgID != nil {
			chat.LastMessage = &domain.Message{
				ID: *msgID, ChatID: chat.ID, SenderID: *senderID, Text: *text, CreatedAt: created.Time,
			}
		}
		chats = append(chats, chat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate chats: %w", err)
	}
	return chats, nil
}

func (s *Storage) Chat(ctx context.Context, chatID, userID int64) (domain.Chat, error) {
	chats, err := s.Chats(ctx, userID)
	if err != nil {
		return domain.Chat{}, err
	}
	for _, chat := range chats {
		if chat.ID == chatID {
			return chat, nil
		}
	}
	return domain.Chat{}, domain.ErrChatNotFound
}

// Members возвращает участников чата; пустой список — чата нет
func (s *Storage) Members(ctx context.Context, chatID int64) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT user_id FROM chat_members WHERE chat_id = $1`, chatID)
	if err != nil {
		return nil, fmt.Errorf("select members: %w", err)
	}
	members, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("collect members: %w", err)
	}
	return members, nil
}

func (s *Storage) CreateMessage(ctx context.Context, chatID, senderID int64, text string) (domain.Message, error) {
	msg := domain.Message{ChatID: chatID, SenderID: senderID, Text: text}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO messages (chat_id, sender_id, text) VALUES ($1, $2, $3)
		 RETURNING id, created_at`, chatID, senderID, text,
	).Scan(&msg.ID, &msg.CreatedAt)
	if err != nil {
		return domain.Message{}, fmt.Errorf("insert message: %w", err)
	}
	return msg, nil
}

// Messages — страница истории: limit сообщений старее beforeID (0 — самые свежие),
// отсортированы от старых к новым
func (s *Storage) Messages(ctx context.Context, chatID, beforeID int64, limit int) ([]domain.Message, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, chat_id, sender_id, text, created_at
		FROM messages
		WHERE chat_id = $1 AND ($2 = 0 OR id < $2)
		ORDER BY id DESC
		LIMIT $3`, chatID, beforeID, limit)
	if err != nil {
		return nil, fmt.Errorf("select messages: %w", err)
	}
	messages, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Message, error) {
		var m domain.Message
		err := row.Scan(&m.ID, &m.ChatID, &m.SenderID, &m.Text, &m.CreatedAt)
		return m, err
	})
	if err != nil {
		return nil, fmt.Errorf("collect messages: %w", err)
	}
	slices.Reverse(messages)
	return messages, nil
}
