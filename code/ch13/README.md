# Chapter 13: Change Data Capture (CDC)

- `postgres`: PostgreSQL 17 with `wal_level=logical`, the publications `pub_outbox` and
  `pub_orders`, and the `debezium` user (see `db/init.sql`)
- `connect`: Kafka Connect with Debezium 3.6; the connectors are in `tools/connectors/` and
  are created by the tool
- `orders-api`: writes the order and the event to the outbox, in the same transaction
- `notifications`: reads the `outbox.event.order` topic and uses the `id` header in the inbox
- `relay`: the polling relay of chapter 12, publishing to Kafka; it only starts with
  `docker compose --profile polling up -d relay`
- `tools`: creates orders and connectors, shows latency, slots and queries to the database

The implementation is in TypeScript, in `node/`.

## How to run

1. Start everything: `docker compose up -d --build --wait`
2. Create the outbox connector: `docker compose run --rm tools connector outbox`
3. Create orders and see the result: `tools orders` and `tools status`
4. See the replication slots: `tools slots`
5. Shut down: `docker compose --profile polling down -v`

Run `docker compose run --rm tools` with no argument to see every command.

## Expected results

| Situation | What happens |
|---|---|
| Outbox read by Debezium | 100 of 100 events delivered |
| Connect stopped | the API accepts orders; the slot retains the WAL; nothing is lost |
| `raw` connector | 200 `r` events from the snapshot, then `c`; `total` in base64 |
| Connector removed | the slot stays, inactive, retaining WAL until it is deleted |
| Polling versus CDC | relay with lower latency by default, but querying all the time |

## Names used in this chapter

- Topics `outbox.event.order` (Outbox Event Router, `aggregatetype` `order`) and
  `cdc.public.orders` (`raw` connector); headers `id` and `type`
- Connectors and slots `outbox` and `raw`; publications `pub_outbox` and `pub_orders`
- Tables: `orders`, `outbox` (`id`, `aggregatetype`, `aggregateid`, `type`, `payload`,
  `created_at`, `published_at`), `deliveries`, `processed_messages`, `sent_emails`
- Tool commands: `orders`, `connector`, `remove`, `connectors`, `status`, `slots`, `drop`,
  `raw`, `queries`, `clear`

The full walkthrough is in section 13.9 of the book.
