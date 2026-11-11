-- Chapter 15: Event Sourcing and CQRS

-- Write side: the event store. Each customer is a stream of events, with versions
-- 1, 2, 3... The primary key prevents two equal versions in the same stream: it is
-- what provides optimistic concurrency.
CREATE TABLE events (
    position     bigserial UNIQUE,
    customer_id  text NOT NULL,
    version      integer NOT NULL,
    type         text NOT NULL,
    data         jsonb NOT NULL,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, version)
);

-- Snapshots: a customer's state at a version, so as not to reread every event
CREATE TABLE snapshots (
    customer_id  text PRIMARY KEY,
    version      integer NOT NULL,
    state        jsonb NOT NULL
);

-- Read side: projections, built from the events and disposable
CREATE TABLE balances (
    customer_id  text PRIMARY KEY,
    balance      integer NOT NULL,
    version      integer NOT NULL,  -- the customer's last version already applied
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE statement (
    customer_id  text NOT NULL,
    version      integer NOT NULL,
    description  text NOT NULL,
    points       integer NOT NULL,
    recorded_at  timestamptz NOT NULL,
    PRIMARY KEY (customer_id, version)
);
