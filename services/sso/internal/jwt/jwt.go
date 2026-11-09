package jwt

import (
	"fmt"
	"messenger/services/sso/internal/domain"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	Email string `json:"email"`
	AppID int32  `json:"app_id"`
	jwt.RegisteredClaims
}

func NewToken(user domain.User, app domain.App, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		Email: user.Email,
		AppID: app.ID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   strconv.FormatInt(user.ID, 10),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(app.Secret))
	if err != nil {
		return "", fmt.Errorf("sign token: %w", err)
	}
	return token, nil
}
