package auth

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"messenger/internal/chat/domain"
)

var ErrInvalidToken = errors.New("invalid token")

// claims повторяют то, что кладет в токен sso
type claims struct {
	Email string `json:"email"`
	AppID int32  `json:"app_id"`
	jwt.RegisteredClaims
}

// Verifier проверяет JWT, выпущенный sso, тем же секретом приложения
type Verifier struct {
	secret []byte
	appID  int32
}

func NewVerifier(secret string, appID int32) *Verifier {
	return &Verifier{secret: []byte(secret), appID: appID}
}

func (v *Verifier) Verify(raw string) (domain.User, error) {
	var c claims
	_, err := jwt.ParseWithClaims(raw, &c, func(*jwt.Token) (any, error) {
		return v.secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	if err != nil {
		return domain.User{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if c.AppID != v.appID {
		return domain.User{}, fmt.Errorf("%w: wrong app", ErrInvalidToken)
	}
	id, err := strconv.ParseInt(c.Subject, 10, 64)
	if err != nil || id <= 0 {
		return domain.User{}, fmt.Errorf("%w: bad subject", ErrInvalidToken)
	}
	return domain.User{ID: id, Email: c.Email}, nil
}

type userKey struct{}

func WithUser(ctx context.Context, user domain.User) context.Context {
	return context.WithValue(ctx, userKey{}, user)
}

func UserFromContext(ctx context.Context) (domain.User, bool) {
	user, ok := ctx.Value(userKey{}).(domain.User)
	return user, ok
}

// UnaryInterceptor пускает в ChatService только запросы с валидным "authorization: Bearer <jwt>"
func UnaryInterceptor(v *Verifier) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// health и reflection открыты, иначе пробы и grpcurl не смогут работать
		if strings.HasPrefix(info.FullMethod, "/grpc.") {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		}
		token, ok := strings.CutPrefix(values[0], "Bearer ")
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "authorization must be a Bearer token")
		}
		user, err := v.Verify(token)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return handler(WithUser(ctx, user), req)
	}
}
