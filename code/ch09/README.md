# Chapter 9: scale, partitions and backpressure

- `inventory`: reads the `orders-received` topic in the `inventory` group and reserves the
  stock of each order, taking `PROCESSING_MS` per order (100 ms, about 9 orders per second
  per instance). Scale with `docker compose up -d --scale inventory=N`. With `MAX_TIME_S`
  greater than zero, an order that waited longer than that deadline is recorded as
  `expired`, without processing
- `kafka`: `orders-received` topic, starting with 3 partitions
- `postgres`: one row per reserved (or expired) order, with the instance that processed it
- `tools`: Kafka's load generator (`kafka-producer-perf-test`), `psql` and
  `kafka-consumer-groups`

The implementation is in TypeScript, in `node/`.

## How to run

1. Start: `docker compose up -d --build`
2. Publish: `docker compose run --rm tools load 50 40` (50 per second, 40 s)
3. See the throughput and the lag: `docker compose run --rm tools status`
4. See each instance's partitions: `docker compose run --rm tools members`
5. Scale: `docker compose up -d --scale inventory=3`
6. Add partitions: `docker compose run --rm tools partitions 6`
7. Deadline-based dropping: `MAX_TIME_S=60` in `.env`, one instance and `load 50 120`;
   `status` starts showing the expired orders
8. Shut down: `docker compose down -v`

## Expected results

| Situation | Instances working | Approximate throughput |
|---|---|---|
| 1 instance, 3 partitions | 1 | 9 per second |
| 3 instances, 3 partitions | 3 | 27 per second |
| 6 instances, 3 partitions | 3 (3 idle) | 27 per second |
| 6 instances, 6 partitions | 6 | more than 40 per second |
| 1 instance, `MAX_TIME_S=60`, `load 50 120` | 1 | 750 to 1,500 reservations, the rest expired; zero lag about 1 minute after the load |

Throughputs depend on the machine.

## Names used in this chapter

- Topic `orders-received`; consumer group `inventory`.
- Message fields: `orderId`, `sku`, `quantity`.
- Table `reservations` (`id`, `order_id`, `instance`, `state`, `reserved_at`); states
  `reserved` and `expired`.

The full walkthrough is in section 9.10 of the book.
