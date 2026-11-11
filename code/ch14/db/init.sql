-- Chapter 14: Sagas. A single database, with one table per service, so that the lab
-- tool can see everything; in production, each service would have its own database.

-- Orders service
CREATE TABLE orders (
    id          uuid PRIMARY KEY,
    sku         text NOT NULL,
    amount      numeric(12, 2) NOT NULL,
    status      text NOT NULL DEFAULT 'pending',  -- pending | confirmed | cancelled
    reason      text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Saga state, kept by the orchestrator (only in orchestration)
CREATE TABLE sagas (
    order_id    uuid PRIMARY KEY,
    step        text NOT NULL,  -- payment | inventory | shipping | compensating
    -- in_progress | completed | cancelled
    status      text NOT NULL DEFAULT 'in_progress',
    reason      text,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Payments service
CREATE TABLE payments (
    order_id  uuid PRIMARY KEY,
    amount    numeric(12, 2) NOT NULL,
    status    text NOT NULL  -- authorized | refunded
);

-- Inventory service
CREATE TABLE inventory (
    sku       text PRIMARY KEY,
    quantity  integer NOT NULL
);
INSERT INTO inventory VALUES ('tshirt', 100), ('sold-out', 0);
CREATE TABLE reservations (
    order_id  uuid PRIMARY KEY,
    sku       text NOT NULL
);

-- Shipping service
CREATE TABLE shipments (
    order_id      uuid PRIMARY KEY,
    scheduled_at  timestamptz NOT NULL DEFAULT now()
);

-- Every message published, by any service: the trail of each saga
CREATE TABLE history (
    id        bigserial PRIMARY KEY,
    order_id  uuid NOT NULL,
    service   text NOT NULL,
    message   text NOT NULL,
    at        timestamptz NOT NULL DEFAULT clock_timestamp()
);
