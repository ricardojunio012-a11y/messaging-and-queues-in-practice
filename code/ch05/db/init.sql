-- Chapter 5: idempotency and deduplication

-- Orders. The idempotency key sent by the client is unique:
-- the same key never creates two orders.
CREATE TABLE orders (
    id               uuid PRIMARY KEY,
    idempotency_key  text UNIQUE,
    customer         text NOT NULL,
    email            text NOT NULL,
    total            numeric(12, 2) NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- Inbox: the messages the consumer has already processed
CREATE TABLE processed_messages (
    message_id    uuid PRIMARY KEY,
    processed_at  timestamptz NOT NULL DEFAULT now()
);

-- Effect in the consumer's own database: the notification history
CREATE TABLE notification_history (
    id           bigserial PRIMARY KEY,
    order_id     uuid NOT NULL,
    message_id   uuid NOT NULL,
    recorded_at  timestamptz NOT NULL DEFAULT now()
);

-- Simulates the external email provider. Like many real providers, it accepts
-- an optional idempotency key: the same key does not cause a second send.
CREATE TABLE provider_emails (
    id               bigserial PRIMARY KEY,
    order_id         uuid NOT NULL,
    idempotency_key  text UNIQUE,
    sent_at          timestamptz NOT NULL DEFAULT now()
);
