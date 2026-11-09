package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"messenger/services/sso/internal/domain"
	"messenger/services/sso/migrations"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

const uniqueViolation = "23505"

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
func (s *Storage) CreateUser(ctx context.Context, email string, passHash []byte) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO users (email, pass_hash) VALUES ($1, $2) RETURNING id`,
		email, string(passHash),
	).Scan(&id)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return 0, domain.ErrUserExists
		}
		return 0, fmt.Errorf("create user: %w", err)
	}
	return id, nil
}
func (s *Storage) UserByEmail(ctx context.Context, email string) (domain.User, error) {
	var user domain.User
	var passHash string
	err := s.pool.QueryRow(ctx,
		`SELECT id, email, pass_hash, is_admin FROM users WHERE email = $1`,
		email,
	).Scan(&user.ID, &user.Email, &passHash, &user.IsAdmin)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.User{}, domain.ErrUserNotFound
	}
	if err != nil {
		return domain.User{}, fmt.Errorf("get user by email: %w", err)
	}
	user.PassHash = []byte(passHash)
	return user, nil
}

func (s *Storage) IsAdmin(ctx context.Context, id int64) (bool, error) {
	var isAdmin bool
	err := s.pool.QueryRow(ctx, `SELECT is_admin FROM users WHERE id = $1`, id).Scan(&isAdmin)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, domain.ErrUserNotFound
	}
	if err != nil {
		return false, fmt.Errorf("select is_admin: %w", err)
	}
	return isAdmin, nil
}

func (s *Storage) AppByID(ctx context.Context, id int32) (domain.App, error) {
	var app domain.App
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, secret FROM apps WHERE id = $1`,
		id,
	).Scan(&app.ID, &app.Name, &app.Secret)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.App{}, domain.ErrAppNotFound
	}
	if err != nil {
		return domain.App{}, fmt.Errorf("select app: %w", err)
	}
	return app, nil
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
