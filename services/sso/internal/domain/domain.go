package domain

import "errors"

var (
	ErrUserExists         = errors.New("user already exists")
	ErrUserNotFound       = errors.New("user not found")
	ErrAppNotFound        = errors.New("app not found")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrTooManyAttempts    = errors.New("too many login attempts")
)

type User struct {
	ID       int64
	Email    string
	PassHash []byte
	IsAdmin  bool
}

type App struct {
	ID     int32
	Name   string
	Secret string
}
