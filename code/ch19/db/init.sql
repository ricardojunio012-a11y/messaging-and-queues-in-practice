-- Chapter 19: every receive of a message, per queue (no unique key: the repeats
-- show up)
CREATE TABLE receives (
    id           bigserial PRIMARY KEY,
    queue        text NOT NULL,
    order_id     text NOT NULL,
    attempt      integer NOT NULL,  -- SQS ApproximateReceiveCount
    received_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);
