-- +goose Up
CREATE TABLE processed_events (
                                  event_id     TEXT PRIMARY KEY,
                                  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE notifications (
                               id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                               user_id    BIGINT      NOT NULL,
                               email      TEXT        NOT NULL,
                               kind       TEXT        NOT NULL,
                               created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE notifications;
DROP TABLE processed_events;