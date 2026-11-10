-- +goose Up
-- Пользователи приходят из sso через Kafka (событие users.registered)
CREATE TABLE users (
    id    BIGINT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE
);

CREATE TABLE chats (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- "меньший_id:больший_id", чтобы между двумя людьми был ровно один личный чат
    direct_key TEXT        NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE chat_members (
    chat_id BIGINT NOT NULL REFERENCES chats (id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users (id),
    PRIMARY KEY (chat_id, user_id)
);

CREATE INDEX chat_members_user_idx ON chat_members (user_id);

CREATE TABLE messages (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chat_id    BIGINT      NOT NULL REFERENCES chats (id) ON DELETE CASCADE,
    sender_id  BIGINT      NOT NULL REFERENCES users (id),
    text       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX messages_chat_idx ON messages (chat_id, id DESC);

-- +goose Down
DROP TABLE messages;
DROP TABLE chat_members;
DROP TABLE chats;
DROP TABLE users;
