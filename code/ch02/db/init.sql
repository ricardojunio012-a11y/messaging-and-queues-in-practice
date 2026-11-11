-- Chapter 2: a durable queue inside PostgreSQL

CREATE TABLE orders (
    id          uuid PRIMARY KEY,
    customer    text NOT NULL,
    email       text NOT NULL,
    total       numeric(12, 2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Each row is a message. The columns at the top form the envelope;
-- the ones at the bottom control delivery.
CREATE TABLE messages (
    id          uuid PRIMARY KEY,
    queue       text NOT NULL,
    type        text NOT NULL,
    version     int NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL DEFAULT now(),
    payload     jsonb NOT NULL,

    status      text NOT NULL DEFAULT 'pending',    -- used by the "status" mode
    visible_at  timestamptz NOT NULL DEFAULT now(), -- used by the "lease" mode
    attempts    int NOT NULL DEFAULT 0
);

CREATE INDEX messages_next ON messages (queue, visible_at);

-- Record of what the consumer did, to count losses and duplicates
CREATE TABLE sent_emails (
    id          bigserial PRIMARY KEY,
    order_id    uuid NOT NULL,
    message_id  uuid NOT NULL,
    consumer    text NOT NULL,
    sent_at     timestamptz NOT NULL DEFAULT now()
);
