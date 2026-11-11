# Chapter 18: RabbitMQ in depth

- `rabbit1`, `rabbit2` and `rabbit3`: a RabbitMQ 4.3 cluster (configuration in
  `rabbitmq/rabbitmq.conf`); queues with no declared type become quorum queues
- `postgres`: the tables `confirmed` (by the broker) and `received` (by the consumer)
- `publisher`: publishes `order.created` every 20 ms, with publisher confirms
- `consumer`: consumes the quorum queue `inventory.reserve`, with manual ack
- `tools`: imports the topology (`tools/topology.json`), publishes test messages and
  shows queues, nodes, leaders and the count

Both clients know the three nodes and reconnect to the next one when the connection
drops. The implementation is in TypeScript, in `node/`.

## How to run

1. Start the cluster and the database: `docker compose up -d --build --wait`
2. Import the topology: `docker compose run --rm tools topology`
3. Start the load: `docker compose --profile load up -d`
4. See the leaders and the count: `tools leaders` and `tools count`
5. Kill the leader: `docker compose kill rabbit1` (the node `leaders` shows)
6. Shut down: `docker compose --profile load down -v`

## Names used in this chapter

- Exchanges: `orders` (topic), `notices` (fanout), `reports` (headers, matched on
  the `format` header), `orders.dlx` (fanout)
- Queues: `inventory.reserve`, `notifications.email`, `loyalty.points`, `audit`
  (stream), `dashboard.alerts`, `reports.pdf`, `orders.dead`
- Routing keys: `order.created`, `order.paid`; message field `orderId`
- Tables: `confirmed (order_id, confirmed_at)` and
  `received (order_id, deliveries, received_at)`
- Services: `publisher`, `consumer`, `tools`; profile `load`; shared module
  `common.ts`; variables `RABBITMQ_NODES`, `DATABASE_URL`, `SERVICE`
- Tool commands: `topology`, `queues`, `publish`, `cluster`, `leaders`, `count`, `clear`

## Expected results

| Situation | What happens |
|---|---|
| Leader goes down | new leader in seconds; no confirmed order lost |
| Two nodes go down | the queue stops (no majority) and resumes when the nodes return |

The full walkthrough is in section 18.10 of the book.
