package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type LoginLimiter struct {
	rdb         *redis.Client
	maxAttempts int64
	window      time.Duration
}

func NewLoginLimiter(rdb *redis.Client, maxAttempts int64, window time.Duration) *LoginLimiter {
	return &LoginLimiter{rdb: rdb, maxAttempts: maxAttempts, window: window}
}

func (l *LoginLimiter) Blocked(ctx context.Context, email string) (bool, error) {
	failures, err := l.rdb.Get(ctx, failuresKey(email)).Int64()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return failures >= l.maxAttempts, nil
}

func (l *LoginLimiter) Fail(ctx context.Context, email string) error {
	key := failuresKey(email)
	pipe := l.rdb.TxPipeline()
	pipe.Incr(ctx, key)
	pipe.ExpireNX(ctx, key, l.window)
	_, err := pipe.Exec(ctx)
	return err
}

func (l *LoginLimiter) Reset(ctx context.Context, email string) error {
	return l.rdb.Del(ctx, failuresKey(email)).Err()
}

func failuresKey(email string) string {
	return "login_failures:" + email
}
