# Chapter 7: ack, retry, backoff and DLQ

- `invoice`: consumes the `invoices` queue, calls the simulated SEFAZ (the tax authority
  service that authorizes each electronic invoice, which fails with probability
  `FAILURE_RATE`) and records the invoice in PostgreSQL. `RETRY_MODE` chooses the policy:
  `immediate` (nack with requeue) or `backoff` (retry queues with a delay, and a DLQ)
- `rabbitmq`: `invoices` exchange, main queue, queues `invoices.retry.1` to `.retry.3`
  (no consumer, with dead lettering back to the exchange) and `invoices.dlq`
- `postgres`: issued invoices and the record of each attempt
- `load`: publishes orders (and the poison message) through the RabbitMQ HTTP API, shows
  the state, inspects the DLQ and runs the redrive. The redrive (`load/redrive.py`, over
  AMQP) reads the message without removing it, publishes the copy with a broker
  confirmation and only then removes it from the DLQ, at `REDRIVE_PER_SECOND` messages
  per second (default 5)

The implementation of the consumer is in TypeScript, in `node/`.

## How to run

1. Choose the scenario in the `.env` file; after changing it, run `docker compose up -d`.
2. Start: `docker compose up -d --build`
3. Reset the data: `docker compose run --rm load clean`
4. Publish: `docker compose run --rm load` (and `load poison` for the invalid message)
5. See the result: `docker compose run --rm load status`
6. Inspect and reprocess the DLQ: `load dlq` and `load redrive`. To see the tool crash in
   the middle of the redrive: `load redrive --crash-after 3` (the third message stays in
   the DLQ and in the main queue; nothing is lost)
7. Shut down: `docker compose down -v`

## Expected results (20 orders and 1 poison message)

| Scenario | Invoices issued | In the DLQ |
|---|---|---|
| `immediate`, without the poison message | 20 | 0 |
| `immediate`, with the poison message | 0 (the queue stalls) | 0 |
| `backoff`, `FAILURE_RATE=0.3` | 20 | 1 |
| `backoff`, `FAILURE_RATE=0.7` | between 12 and 17 | between 4 and 9 |
| fix (`ACCEPT_TOTAL_AS_TEXT=yes`), `load redrive --crash-after 3` | (in progress) | 2 fewer than before |
| then, `load redrive` | 21 | 0 |
| `backoff`, `FAILURE_RATE=1.0`, `N=100` (SEFAZ down) | 0 | 100, in about 30 s |

## Names used in this chapter

- Exchange `invoices` (direct), routing key `issue`; queues `invoices`,
  `invoices.retry.1` to `invoices.retry.3` and `invoices.dlq`.
- Headers: `x-attempt` (number of the next attempt) and `x-error` (reason of the failure).
- Message fields: `orderId`, `customer`, `total`.
- Tables: `issued_invoices` (`order_id`, `issued_at`) and `attempts` (`id`, `order_id`,
  `attempt`, `result`, `at`); results `issued`, `transient_failure` and `invalid`.

The full walkthrough is in section 7.9 of the book.
