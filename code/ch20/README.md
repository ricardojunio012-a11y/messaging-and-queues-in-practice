# Chapter 20: Apache Kafka in depth

- `controller`: the KRaft controller (cluster metadata)
- `broker1`, `broker2` and `broker3`: the Kafka 4.3 brokers
- `producer`: publishes `N` payments to `TOPIC` with `ACKS` and counts confirmations and
  failures (`docker compose run --rm producer`)
- `consumer`: reconciles the payments of `CONSUME_TOPIC` (default `payments-safe`) in the
  `reconciliation` group, `PROCESSING_MS` per payment (`docker compose up -d consumer`, or
  `--scale consumer=2`)
- `tools`: `topics` (creates the two topics), `describe` (leader and ISR), `count` and
  `group` (lag and members of the `reconciliation` group)

The topics: `payments-risky` (`min.insync.replicas=1`, election outside the ISR allowed) and
`payments-safe` (`min.insync.replicas=2`, election outside the ISR forbidden). The
implementation is in TypeScript, in `node/`.

## How to run

1. Start the cluster: `docker compose up -d --build --wait`
2. Create the topics: `docker compose run --rm tools topics`
3. Publish: `docker compose run --rm -e TOPIC=payments-risky -e ACKS=1 producer`
4. Stop and start brokers with `docker compose kill brokerN` and `docker compose start brokerN`
5. Slow consumer: with the three brokers up, `docker compose up -d consumer` and
   `docker compose run --rm tools group`; then `--scale consumer=2` (the second one gets no
   partition, because the topic has only one)
6. Shut down: `docker compose --profile tools --profile consume down -v`

## Names used in this chapter

- Topics `payments-risky` and `payments-safe`; consumer group `reconciliation`
- Message value: `{"payment": <n>}`, key `p<n>`
- Environment variables: `TOPIC`, `ACKS`, `N`, `CONSUME_TOPIC`, `PROCESSING_MS`
- Services: `controller`, `broker1` to `broker3`, `producer`, `consumer`, `tools`

## Expected results

| Situation | Confirmed | In the topic |
|---|---|---|
| Risky, `acks=1`, replicas down, leader fails | 1,500 | 500 |
| Safe, `acks=all`, one broker down, leader fails | 1,500 | 1,500 |
| Safe, ISR with one replica | 0 (refused) | unchanged |

With the consumer, the group's lag drops about 5 payments per second, with one or two
consumers: the second member gets 0 partitions.

The full walkthrough is in section 20.12 of the book.
