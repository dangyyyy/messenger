-- +goose Up
CREATE TABLE outbox (
                        id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        topic      TEXT        NOT NULL,
                        key        TEXT        NOT NULL,
                        payload    BYTEA       NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        sent_at    TIMESTAMPTZ
);

CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;

-- +goose Down
DROP TABLE outbox;