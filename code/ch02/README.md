# Chapter 2: a durable queue in PostgreSQL

- `postgres`: stores the orders and the queue (table `messages`); schema in `db/init.sql`
- `orders-api` (producer): `POST /orders` writes the order and the message in the same transaction
- `notifications` (consumer): takes messages from the `emails` queue with `FOR UPDATE SKIP LOCKED`

The implementation is in TypeScript, in `node/`.

## How to run

1. Choose the consume mode in the `.env` file.
2. Start the services: `docker compose up -d --build`
3. Generate load: `docker compose run --rm load`
4. See the queue and the emails: `docker compose run --rm load status`
5. Poison message (the email always fails): `docker compose run --rm load poison`
6. SQL terminal: `docker compose exec postgres psql -U pedidocerto`
7. Shut down and delete the database: `docker compose down -v`

No port is published on your machine; the tools run inside the containers' network.

## Consume modes (`CONSUME_MODE`)

| Mode | How it takes a message | If the consumer dies |
|---|---|---|
| `status` | marks the row as `processing` | the message is stuck |
| `transaction` | locks the row in an open transaction | it comes back at once, but the transaction stays open during processing |
| `lease` | makes the message invisible for `LEASE_S` seconds | it comes back when the lease expires |

In `lease` mode, each reservation counts as one attempt. After `MAX_ATTEMPTS` (default 5) attempts,
the message stays parked in the table (column `exhausted` of the status) and is no longer taken.

## Names used in this chapter

- Tables: `orders` (`id`, `customer`, `email`, `total`, `created_at`), `messages` (`id`, `queue`,
  `type`, `version`, `created_at`, `payload`, `status`, `visible_at`, `attempts`) and
  `sent_emails` (`order_id`, `message_id`, `consumer`, `sent_at`).
- Message statuses: `pending`, `processing`. Queue: `emails`. Message type: `SendConfirmationEmail`,
  payload `{ orderId, to, subject }`.
- Routes: `POST /orders`, `GET /health`. Lab tool commands: `load`, `load status`, `load poison`.

The lab walkthrough is in section 2.9 of the book.
