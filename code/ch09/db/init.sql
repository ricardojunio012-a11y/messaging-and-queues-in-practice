-- Chapter 9: scale, partitions and backpressure

-- One row per processed order, with the consumer instance that processed it
CREATE TABLE reservations (
    id           bigserial PRIMARY KEY,
    order_id     text NOT NULL,
    instance     text NOT NULL,
    -- reserved | expired (discarded because it went past the deadline, MAX_TIME_S)
    state        text NOT NULL DEFAULT 'reserved',
    reserved_at  timestamptz NOT NULL DEFAULT now()
);
