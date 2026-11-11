# Chapter 10: observability in asynchronous systems

- `orders-api`: receives `POST /orders` and publishes `OrderCreated` to the `orders` fanout
  exchange, with the trace context (`traceparent`), the correlation id and the message's
  `timestamp`
- `notifications` (50 ms per message) and `loyalty` (1.5 s per message): consumers, each
  with its own queue, that extract the context and continue the trace
- `jaeger`: receives the spans over OTLP; user interface at http://localhost:16686
- `prometheus`: collects RabbitMQ's per-queue metrics and evaluates the rules in
  `prometheus/alerts.yml`; user interface at http://localhost:9090
- `tools`: creates orders and shows traces, queues and alerts in the terminal

The implementation is in TypeScript, in `node/`, with the OpenTelemetry SDK for Node.js.

## How to run

1. Start: `docker compose up -d --build`
2. One order and its trace: `docker compose run --rm tools order`
3. The logs with the same trace_id: `docker compose logs --no-log-prefix loyalty`
4. Load and alert: `docker compose run --rm tools load 60`, then `queues` and `alerts`.
   Stopped consumer: `docker compose stop loyalty`, `load 5` and, about 40 s later,
   `alerts` shows `QueueWithoutConsumer` firing
5. Fix it: `docker compose up -d --scale loyalty=4 loyalty`
6. Shut down: `docker compose down -v`

If ports 16686 or 9090 are busy, change `JAEGER_PORT` and `PROMETHEUS_PORT` in `.env`.

## Names used in this chapter

- Exchange `orders` (fanout); queues `notifications` and `loyalty`; message type
  `OrderCreated`.
- Spans: `POST /orders`, `publish orders` (PRODUCER), `process orders` (CONSUMER),
  `send email` and `compute points`.
- Log fields: `service`, `message`, `trace_id`, `order_id`, `correlation_id`.
- Alerts: `OldMessageInQueue` and `QueueWithoutConsumer` (label `severity: page`,
  annotation `summary`).

The full walkthrough is in section 10.7 of the book.
