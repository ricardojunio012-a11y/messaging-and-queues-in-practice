-- Chapter 8: retention, replay and reprocessing

-- The points of each order. One row per order: reprocessing the same event
-- replaces the row (upsert), and that is why the replay fixes the points without
-- duplicating them.
CREATE TABLE points_per_order (
    order_id     text PRIMARY KEY,
    customer_id  text NOT NULL,
    total        numeric(10, 2) NOT NULL,
    points       int NOT NULL,
    rule         int NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
