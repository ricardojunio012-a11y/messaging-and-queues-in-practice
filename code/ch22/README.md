# Chapter 22: OrderCreated on NATS JetStream

- `nats`: NATS 2.15.1 server with JetStream enabled (data in the `data` volume), domain
  `cloud` and port 7422 for leaf nodes (`nats/cloud.conf`)
- `kiosk`: the kiosk's NATS, with its own JetStream (domain `kiosk`), connected to the
  cloud as a leaf node (`nats/kiosk.conf`); it is on the `default` (the internet) and
  `store` networks
- `notifications`: durable consumer `notifications` (explicit ack, `ack_wait` of 5 s,
  `max_deliver` 3); rejects orders with a negative total with `nak`
- `dlq`: listens to the `max_deliver` advisory and copies the abandoned message to
  `ORDERS_DLQ`
- `producer`: publishes `N` orders (`REPEAT`, `DEDUP` and `POISON` in `.env`)
  (`docker compose run --rm producer`)
- `tools`: `status` (streams and consumer), `replay` (rereads the stream from the start),
  `kiosk [N]` (publishes to the kiosk) and `sync` (counts the orders on the kiosk and in
  the cloud)

## Names used in this chapter

- Streams: `ORDERS` (subjects `orders.>`, events on `orders.created`), `ORDERS_DLQ`
  (subjects `dlq.>`), `ORDERS_STORE` (on the kiosk, subject `store.orders`) and
  `ORDERS_STORES` (in the cloud, sourced from `ORDERS_STORE`)
- Durable consumer: `notifications`
- Order JSON: `{"id", "customer", "total"}`
- JetStream domains: `cloud` and `kiosk`; networks `default` and `store`

## How to run

1. Start: `docker compose up -d --build --wait` and `docker compose --profile tools build`
2. Publish: `docker compose run --rm producer`
3. Check: `docker compose run --rm tools status`
4. Shut down: `docker compose --profile tools down -v`

## Expected results

| Situation | Result |
|---|---|
| `REPEAT=1` with `Nats-Msg-Id` | 100 stored, 100 duplicates dropped |
| `REPEAT=1 DEDUP=0` | 200 stored |
| Consumer killed with unacknowledged messages | redelivered after 5 s |
| `POISON=1` | abandoned after 3 deliveries and copied to `ORDERS_DLQ` |
| `replay` | every message in the stream reread |
| Kiosk disconnected from the `default` network, 50 orders, network reconnected | the 50 reach the cloud within a minute, with no duplicates |

The full walkthrough is in section 22.10 of the book.
