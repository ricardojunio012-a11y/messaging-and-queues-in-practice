-- Chapter 18: what the broker confirmed and what reached the consumer

CREATE TABLE confirmed (
    order_id      text PRIMARY KEY,
    confirmed_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE received (
    order_id     text PRIMARY KEY,
    deliveries   integer NOT NULL DEFAULT 1,  -- more than 1: delivered again
    received_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);
