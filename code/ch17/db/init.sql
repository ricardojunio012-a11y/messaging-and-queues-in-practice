-- Chapter 17: PostgreSQL and Redis as a queue

-- The queue in PostgreSQL: a table. The worker locks the rows with SKIP LOCKED and
-- deletes them in the same transaction in which it records the result.
CREATE TABLE job_queue (
    id              bigserial PRIMARY KEY,
    job_id          uuid NOT NULL,
    payload         jsonb NOT NULL,
    enqueued_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- The result of each delivery, for all three backends
-- (no unique key: duplicates show up)
CREATE TABLE deliveries (
    id              bigserial PRIMARY KEY,
    job_id          uuid NOT NULL,
    backend         text NOT NULL,
    worker          text NOT NULL,
    enqueued_at     timestamptz NOT NULL,
    processed_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
