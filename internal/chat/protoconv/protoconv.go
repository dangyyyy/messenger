package protoconv

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	"messenger/internal/chat/domain"
	chatv1 "messenger/pkg/api/chat/v1"
)

func Message(m domain.Message) *chatv1.Message {
	return &chatv1.Message{
		Id:        m.ID,
		ChatId:    m.ChatID,
		SenderId:  m.SenderID,
		Text:      m.Text,
		CreatedAt: timestamppb.New(m.CreatedAt),
	}
}

func Chat(c domain.Chat) *chatv1.Chat {
	chat := &chatv1.Chat{Id: c.ID, PeerId: c.Peer.ID, PeerEmail: c.Peer.Email}
	if c.LastMessage != nil {
		chat.LastMessage = Message(*c.LastMessage)
	}
	return chat
}
