-- Chapter 13: Change Data Capture

CREATE EXTENSION pg_stat_statements;

CREATE TABLE orders (
    id          uuid PRIMARY KEY,
    customer    text NOT NULL,
    email       text NOT NULL,
    total       numeric(12, 2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Outbox in the format Debezium's Outbox Event Router expects: id, aggregatetype,
-- aggregateid, type and payload. published_at is only used by the polling relay.
CREATE TABLE outbox (
    id             uuid PRIMARY KEY,
    aggregatetype  text NOT NULL,
    aggregateid    text NOT NULL,
    type           text NOT NULL,
    payload        jsonb NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at   timestamptz
);
CREATE INDEX outbox_pending ON outbox (created_at) WHERE published_at IS NULL;

-- On the consumer side: each delivery received, the inbox and the effect
CREATE TABLE deliveries (
    id           bigserial PRIMARY KEY,
    message_id   uuid NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE processed_messages (
    message_id    uuid PRIMARY KEY,
    processed_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE sent_emails (
    id           bigserial PRIMARY KEY,
    order_id     uuid NOT NULL,
    message_id   uuid NOT NULL
);

-- Debezium user: read only, plus the replication permission to read the WAL
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT SELECT ON orders, outbox TO debezium;

-- Publications: which tables each connector receives from the WAL
CREATE PUBLICATION pub_outbox FOR TABLE outbox;
CREATE PUBLICATION pub_orders FOR TABLE orders;

-- User of the polling relay (step 5), separate so that its queries can be counted
CREATE ROLE relay WITH LOGIN PASSWORD 'relay';
GRANT SELECT, UPDATE ON outbox TO relay;
