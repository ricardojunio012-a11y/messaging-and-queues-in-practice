-- Chapter 12: Transactional Outbox (and Inbox)

CREATE TABLE orders (
    id          uuid PRIMARY KEY,
    customer    text NOT NULL,
    email       text NOT NULL,
    total       numeric(12, 2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Outbox: the events to publish, written in the same transaction as the order.
-- The relay publishes the rows without published_at, in id order.
CREATE TABLE outbox (
    id            bigserial PRIMARY KEY,
    event_id      uuid NOT NULL UNIQUE,
    type          text NOT NULL,
    payload       jsonb NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz
);
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;

-- On the consumer side: each delivery received (with repeats), the inbox
-- (chapter 5) and the effect, one email per order
CREATE TABLE deliveries (
    id           bigserial PRIMARY KEY,
    message_id   uuid NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE processed_messages (
    message_id    uuid PRIMARY KEY,
    processed_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE sent_emails (
    id           bigserial PRIMARY KEY,
    order_id     uuid NOT NULL,
    message_id   uuid NOT NULL
);
