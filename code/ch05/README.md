# Chapter 5: idempotency and deduplication

- `orders-api`: writes the order (honoring the `Idempotency-Key` header) and publishes
  `SendConfirmationEmail`; with `DUPLICATE_PUBLISH=yes`, it publishes each message twice
- `notifications`: records the history in the database and "sends" the email through a simulated
  provider; `IDEMPOTENCY` chooses the protection (`none`, `inbox` or `key`)
- `postgres`: orders, inbox (`processed_messages`), history and the simulated provider

The implementation is in TypeScript, in `node/`.

## How to run

1. Choose the scenario in `.env`; after changing it, run `docker compose up -d`.
2. Start: `docker compose up -d --build`
3. Reset the data: `docker compose run --rm load reset`
4. Send orders: `docker compose run --rm load` (or `repeat`, `with-key` or `concurrent`,
   which sends both requests with the same key at the same time)
5. See the result: `docker compose run --rm load status`
6. Shut down: `docker compose down -v`

## Expected results (20 orders)

| Scenario | Emails |
|---|---|
| `DUPLICATE_PUBLISH=yes`, no protection | 40 |
| `DUPLICATE_PUBLISH=yes`, `IDEMPOTENCY=inbox` | 20 |
| `FAIL_ON_MESSAGE=5`, `IDEMPOTENCY=inbox` | 21 (the email escapes the inbox) |
| `FAIL_ON_MESSAGE=5`, `IDEMPOTENCY=key` | 20 |
| `load with-key` or `load concurrent` (40 requests) | 20 orders and 20 emails |

## Names used in this chapter

- Tables: `orders` (`id`, `idempotency_key`, `customer`, `email`, `total`, `created_at`),
  `processed_messages` (`message_id`, `processed_at`), `notification_history` (`order_id`,
  `message_id`, `recorded_at`) and `provider_emails` (`order_id`, `idempotency_key`, `sent_at`).
- Variables `DUPLICATE_PUBLISH` (`yes`/`no`), `IDEMPOTENCY` (`none`, `inbox`, `key`),
  `FAIL_ON_MESSAGE`. Header `Idempotency-Key` on `POST /orders`.
- Lab tool commands: `load`, `load repeat`, `load with-key`, `load concurrent`, `load status`,
  `load reset`.

The full walkthrough is in section 5.7 of the book.
