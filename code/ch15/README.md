# Chapter 15: Event Sourcing and CQRS

- `loyalty-api`: the commands `POST /customers/{id}/earn` and `POST /customers/{id}/redeem`
  (body `{"points": N}`, with an optional `expectedVersion`) write events to the `events`
  table and publish them to the `loyalty-events` topic; the query `GET /customers/{id}`
  reads the projection
- `projections`: consumes the topic and keeps the `balances` and `statement` tables; with
  `REBUILD=1`, it rebuilds the projection by reading the event store (the `events` table, in
  global position order) and exits
- `tools`: sends commands, compares the event store with the projection, rebuilds the
  projection, creates a long history and measures command times

`SNAPSHOT_EVERY` in `.env` turns snapshots on (0 turns them off). The implementation is in
TypeScript, in `node/`.

## How to run

1. Start everything: `docker compose up -d --build --wait`
2. Send commands: `docker compose run --rm tools load 200`
3. Compare the event store with the projection: `docker compose run --rm tools status`
4. Simulate a publish that failed: `tools lose customer-7 50`
5. Rebuild the projection from the event store: `docker compose stop projections`,
   `tools rebuild`, `docker compose run --rm -e REBUILD=1 projections`,
   `docker compose start projections` (with `rebuild topic`, the projection is rebuilt by
   rereading the topic, which does not have the lost event)
6. Shut down: `docker compose down -v`

Run `docker compose run --rm tools` with no argument to see every command.

## Expected results

| Situation | What happens |
|---|---|
| Valid and invalid commands | only the valid ones become events; the projection matches the event store |
| Two simultaneous redemptions | one written, the other refused with 409 |
| Publish that failed | the customer diverges; rereading the topic, it still diverges |
| Projection deleted | rebuilt and correct by rereading the event store |
| Stream of 20,000 events | with snapshots, commands read few events and get faster |

## Names used in this chapter

- Events `PointsEarned` and `PointsRedeemed`; topic `loyalty-events` (key: the customer id);
  consumer group `projections`
- Message JSON: `customerId`, `version`, `type`, `data` (`points`), `recordedAt`
- Command response: `balance`, `version`, `eventsRead`; errors `version changed` (409) and
  `insufficient balance` (422)
- Tables: `events` (`position`, `customer_id`, `version`, `type`, `data`, `recorded_at`),
  `snapshots` (`customer_id`, `version`, `state`), `balances` (`customer_id`, `balance`,
  `version`, `updated_at`), `statement` (`customer_id`, `version`, `description`
  `earning` | `redemption`, `points`, `recorded_at`)
- Customers `customer-1` to `customer-20` and `old-customer`

The full walkthrough is in section 15.11 of the book.
