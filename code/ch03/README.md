# Chapter 3: queue, pub/sub, log and request/reply

The `OrderCreated` event is published to RabbitMQ (a `fanout` exchange called `orders`) and to
Kafka (topic `orders`, 3 partitions), to compare the models.

- `orders-api`: `POST /orders` publishes the event; `GET /shipping?zipCode=...` does request/reply
- `shipping`: answers the quotes of the `shipping.quotes` queue
- `consumer`: subscribes to the event; `BROKER` (`rabbitmq` or `kafka`) and `NAME` choose the broker
  and the subscriber
- `analytics-*`: subscribers that arrive later (profile `late`)

The implementation is in TypeScript, in `node/`.

## How to run

1. Adjust the shipping settings in `.env` if you want.
2. Start: `docker compose up -d --build`
3. Publish orders: `docker compose run --rm load`
4. See what each subscriber received: `docker compose run --rm load status`
5. Shipping quotes: `docker compose run --rm load shipping`
6. Late subscribers: `docker compose --profile late up -d analytics-rabbitmq analytics-kafka`
7. Shut down: `docker compose --profile late down -v`

To inspect the brokers:

```bash
docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
docker compose run --rm groups
docker compose run --rm groups --group notifications --members
docker compose run --rm topics
```

The `topics` service shows the configuration of the `orders` topic and, with `--alter --add-config`,
changes it (step 7 shortens retention to 1 minute). The broker checks retention every 10 s
(`KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS`), instead of the default 5 min.

## Names used in this chapter

- RabbitMQ: `fanout` exchange `orders`; subscriber queues `notifications.orders`, `loyalty.orders`,
  `analytics.orders`; request queue `shipping.quotes` (reply queue: exclusive, named by the broker).
- Kafka: topic `orders` (3 partitions, key = `orderId`); consumer groups `notifications`, `loyalty`,
  `analytics`.
- Event `OrderCreated` with `{ orderId, customer, email, total }`; RabbitMQ header `version`,
  Kafka headers `id`, `type`, `version`.
- Shipping quote: `GET /shipping?zipCode=...` returns `{ zipCode, price, deliveryDays }`.
- Lab tool commands: `load`, `load status`, `load shipping`, `load reset`.

The full walkthrough is in section 3.9 of the book.
