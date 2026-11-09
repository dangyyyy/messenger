package events

import (
	eventsv1 "messenger/gen/events/v1"
	"messenger/services/sso/internal/domain"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const TopicUserRegistered = "users.registered"

func UserRegistered(userID int64, email string, registeredAt time.Time) (domain.OutboxMessage, error) {
	payload, err := proto.Marshal(&eventsv1.UserRegistered{
		EventId:      uuid.NewString(),
		UserId:       userID,
		Email:        email,
		RegisteredAt: timestamppb.New(registeredAt),
	})
	if err != nil {
		return domain.OutboxMessage{}, err
	}
	return domain.OutboxMessage{
		Topic:   TopicUserRegistered,
		Key:     strconv.FormatInt(userID, 10),
		Payload: payload,
	}, nil
}
