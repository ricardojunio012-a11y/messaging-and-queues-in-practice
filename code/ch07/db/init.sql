-- Chapter 7: ack, retry, backoff and DLQ

-- Issued invoices: one per order (issuing is idempotent)
CREATE TABLE issued_invoices (
    order_id   text PRIMARY KEY,
    issued_at  timestamptz NOT NULL DEFAULT now()
);

-- Each processing attempt, with the result:
--   issued | transient_failure | invalid
CREATE TABLE attempts (
    id        bigserial PRIMARY KEY,
    order_id  text,
    attempt   int NOT NULL,
    result    text NOT NULL,
    at        timestamptz NOT NULL DEFAULT now()
);
