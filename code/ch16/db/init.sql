-- Chapter 16: Claim Check and delayed messages

-- Invoices received by the sender service, with the result of the hash check
CREATE TABLE sent_invoices (
    order_id      text PRIMARY KEY,
    size          integer NOT NULL,
    hash_matches  boolean NOT NULL,
    received_at   timestamptz NOT NULL DEFAULT now()
);

-- Carts that became a purchase (the tool records them)
CREATE TABLE purchases (
    cart_id       text PRIMARY KEY,
    purchased_at  timestamptz NOT NULL DEFAULT now()
);

-- What the reminders service did with each abandoned cart
CREATE TABLE reminders (
    cart_id        text PRIMARY KEY,
    abandoned_at   timestamptz NOT NULL,
    processed_at   timestamptz NOT NULL DEFAULT now(),
    result         text NOT NULL  -- sent | discarded (already bought)
);
