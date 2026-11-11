-- Chapter 6: message ordering

-- Projection: the current status of each order, built from the events.
-- The version column keeps the version of the last event applied.
CREATE TABLE order_status (
    order_id    uuid PRIMARY KEY,
    status      text NOT NULL,
    version     int NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Events discarded because they are older than the current state
CREATE TABLE stale_events (
    id        bigserial PRIMARY KEY,
    order_id  uuid NOT NULL,
    type      text NOT NULL,
    version   int NOT NULL
);
