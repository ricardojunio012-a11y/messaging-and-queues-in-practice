# Chapter 6: message ordering

- `orders-api`: for each `POST /orders`, publishes `OrderCreated` (version 1),
  `OrderPaid` (version 2) and `OrderShipped` (version 3) in sequence to the `order-events`
  topic; `KEY` chooses the Kafka key (`random` or `order`)
- `tracking`: three instances in the same consumer group, keeping the current status of
  each order; `APP` chooses the rule (`naive` or `versioned`)
- `kafka`: `order-events` topic with 3 partitions
- `postgres`: the `order_status` projection and the discarded `stale_events`

The implementation is in TypeScript, in `node/`.

## How to run

1. Choose the scenario in the `.env` file; after changing it, run `docker compose up -d`.
2. Start: `docker compose up -d --build`
3. Reset the data: `docker compose run --rm load clean`
4. Send orders: `docker compose run --rm load`
5. Wait a few seconds and see the result: `docker compose run --rm load status`
6. See the group's partitions: `docker compose run --rm groups`
7. Shut down: `docker compose down -v`

## Expected results (20 orders)

| `KEY` | `APP` | Orders in `shipped` | Stale |
|---|---|---|---|
| `random` | `naive` | about half (varies) | 0 |
| `order` | `naive` | 20 | 0 |
| `random` | `versioned` | 20 | about 10 (varies) |
| `order` | `versioned` | 20 | 0 |

## Names used in this chapter

- Topic `order-events`; consumer group `tracking`.
- Event fields: `orderId`, `type`, `status`, `version`.
- Statuses: `created`, `paid`, `shipped`.
- Tables: `order_status` (`order_id`, `status`, `version`, `updated_at`) and
  `stale_events` (`id`, `order_id`, `type`, `version`).

The full walkthrough is in section 6.8 of the book.
