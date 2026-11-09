package cache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"messenger/internal/sso/domain"
)

type AppStore interface {
	AppByID(ctx context.Context, id int32) (domain.App, error)
}

type Apps struct {
	next AppStore
	rdb  *redis.Client
	ttl  time.Duration
}

func NewApps(next AppStore, rdb *redis.Client, ttl time.Duration) *Apps {
	return &Apps{next: next, rdb: rdb, ttl: ttl}
}

func (a *Apps) AppByID(ctx context.Context, id int32) (domain.App, error) {
	key := "app:" + strconv.Itoa(int(id))

	data, err := a.rdb.Get(ctx, key).Bytes()
	switch {
	case err == nil:
		var app domain.App
		if err := json.Unmarshal(data, &app); err == nil {
			return app, nil
		}
	case !errors.Is(err, redis.Nil):
		slog.WarnContext(ctx, "app cache read failed", "error", err)
	}

	app, err := a.next.AppByID(ctx, id)
	if err != nil {
		return domain.App{}, err
	}

	data, err = json.Marshal(app)
	if err != nil {
		return app, nil
	}
	if err := a.rdb.Set(ctx, key, data, a.ttl).Err(); err != nil {
		slog.WarnContext(ctx, "app cache write failed", "error", err)
	}
	return app, nil
}
