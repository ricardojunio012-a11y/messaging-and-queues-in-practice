# Chapter 12: Transactional Outbox (and Inbox)

- `orders-api`: receives `POST /orders` and writes the order with a dual write (`WRITE=dual`:
  writes and then publishes) or with an outbox (`WRITE=outbox`: order and event in the same
  transaction)
- `relay`: reads the outbox every 500 ms with `FOR UPDATE SKIP LOCKED`, publishes with
  confirmation and marks the events; with `RELAY_FAIL_AFTER_PUBLISH=yes`, it goes down once
  after publishing and before marking
- `notifications`: consumer with an inbox (chapter 5), which drops repeated messages
- `postgres`: orders, outbox, deliveries, inbox and emails sent
- `load`: creates orders (`N`) and shows the state of the database (`status`); `clear`
  deletes the data

The implementation is in TypeScript, in `node/`.

## How to run

1. Start everything: `docker compose up -d --build`
2. Create orders: `docker compose run --rm load`
3. See the result: `docker compose run --rm load status`
4. Stop and start the broker with `docker compose stop rabbitmq` and
   `docker compose start rabbitmq`
5. When you change `WRITE` or `RELAY_FAIL_AFTER_PUBLISH` in `.env`, recreate with
   `docker compose up -d`
6. Shut down: `docker compose down -v`

## Expected results

| Step | Write | Broker | Relay | Orders | Emails | Lost | Duplicates dropped |
|---|---|---|---|---|---|---|---|
| 1 | dual | up | not used | 10 | 10 | 0 | 0 |
| 2 | dual | down | not used | 20 | 10 | 10 | 0 |
| 3 | outbox | down, then up | normal | 10 | 10 | 0 | 0 |
| 4 | outbox | up | goes down once | 10 | 10 | 0 | 10 |

## Names used in this chapter

- Route `POST /orders`, response `{"id": ..., "status": "accepted"}`; queue `notifications`
- Event `OrderCreated` with `orderId`, `email`, `total`
- Tables: `orders` (`id`, `customer`, `email`, `total`, `created_at`), `outbox` (`id`,
  `event_id`, `type`, `payload`, `created_at`, `published_at`, index `outbox_pending`),
  `deliveries` (`message_id`, `received_at`), `processed_messages` (`message_id`,
  `processed_at`), `sent_emails` (`order_id`, `message_id`)
- `status` columns: `orders`, `outbox_pending`, `deliveries`, `emails`, `duplicates`
- `node/src/common.ts`: `QUEUE`, `log`, `sleep`, `Event`, `publish`

The full walkthrough is in section 12.10 of the book.
