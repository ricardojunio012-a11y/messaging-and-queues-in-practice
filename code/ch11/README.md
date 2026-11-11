# Chapter 11: contracts and schema evolution

- `notifications`: reads the `orders` topic and sends the email for each `OrderCreated`; it is
  a tolerant reader (it ignores unknown fields and rejects, with the reason, events without
  `email`). With `CONSUMER_SCHEMA` in `.env` (`v1` or `v2-compatible`), it starts refusing
  events outside that schema, like a consumer that validates
- `tests`: automated tests of the consumer in TypeScript (`node/test/`), against the lab's
  Kafka and database
- `schema-registry`: Confluent Schema Registry, in the default compatibility mode (backward)
- `postgres`: emails sent and events rejected
- `tools`: publishes events in three formats (`v1`, `v2-breaking`, `v2-compatible`), with the
  CloudEvents attributes in the headers, and registers and checks the schemas in
  `tools/schemas/`

The implementation is in TypeScript, in `node/`.

## How to run

1. Start everything: `docker compose up -d --build`
2. Publish: `docker compose run --rm tools publish v1 5` (or `v2-breaking`, `v2-compatible`)
3. See the result: `docker compose run --rm tools status`
4. Register and check schemas: `register v1`, `check v2-breaking`, `register v2-breaking`
   (`check` exits with code 1 when the version is incompatible, like a CI step)
5. Deploy order: register `v2-compatible`, set `CONSUMER_SCHEMA=v1`, run
   `docker compose up -d` and publish `v2-compatible` (refused); then
   `CONSUMER_SCHEMA=v2-compatible`, `docker compose up -d` and publish again (accepted)
6. Tests: `docker compose run --rm tests` (with the consumer on `v1`, the third one fails)
7. Shut down: `docker compose down -v` and set `CONSUMER_SCHEMA=` (empty) back in `.env`

## Expected results

| Change | Did the broker accept it? | Did the registry accept it? | The old consumer |
|---|---|---|---|
| `v1` | yes | yes | processed |
| `v2-breaking` | yes | no (409) | rejected the events |
| `v2-compatible`, consumer validating with `v1` | yes | yes | rejected the events |
| `v2-compatible`, consumer updated first | yes | yes | processed |

## Names used in this chapter

- Topic `orders`, registry subject `orders-value`, consumer group `notifications`
- CloudEvents: `ce_type` `br.com.pedidocerto.order.created`, `ce_source`
  `/pedido-certo/orders-api`
- `OrderCreated` fields: `orderId`, `customer`, `email`, `customerEmail`, `channel`
  (`site`, `app`, `store`), `total`
- Tables: `sent_emails` (`order_id`, `email`, `version`, `sent_at`) and `rejected_events`
  (`id`, `order_id`, `reason`, `rejected_at`)

The full walkthrough is in section 11.10 of the book.
