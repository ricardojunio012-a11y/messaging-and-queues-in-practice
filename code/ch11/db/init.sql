-- Chapter 11: contracts and schema evolution

-- Emails "sent" by the consumer
CREATE TABLE sent_emails (
    order_id    text PRIMARY KEY,
    email       text NOT NULL,
    version     text,
    sent_at     timestamptz NOT NULL DEFAULT now()
);

-- Events the consumer could not understand (the role of a DLQ)
CREATE TABLE rejected_events (
    id          bigserial PRIMARY KEY,
    order_id    text,
    reason      text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now()
);
