# Chapter 19: Amazon SQS and SNS (with LocalStack)

- `localstack`: local SQS and SNS (version 4.14.0, no token); `localstack/create-resources.sh`
  creates the `orders` topic, the `notifications` queue (with a DLQ after 3 receives) and the
  `loyalty` queue, and the subscriptions, the loyalty one with the filter `total >= 500`
- `publisher`: publishes 20 orders to the topic (`docker compose run --rm publisher`)
- `notifications` and `loyalty`: consumers with long polling; `PROCESSING_MS` and
  `EXTEND_VISIBILITY` in `.env`
- `postgres`: every receive, with the attempt
- `tools`: the AWS CLI pointed at LocalStack (`queues`, `result`, `visibility`, `poison`,
  `dlq`, `redrive`, `clear`)

The implementation is in TypeScript, in `node/`.

## Names used in this chapter

- SNS topic `orders`; SQS queues `notifications`, `notifications-dlq` and `loyalty`
- message body `{"orderId": "o1", "total": 50}`; message attribute `total` (Number)
- table `receives` (`queue`, `order_id`, `attempt`, `received_at`)
- variables `PROCESSING_MS`, `EXTEND_VISIBILITY` (`yes` or `no`), `QUEUE`, `TOPIC_ARN`

## How to run

1. Start: `docker compose up -d --build --wait`
2. Publish and look: `docker compose run --rm publisher` and `docker compose run --rm tools result`
3. For the duplicates: `PROCESSING_MS=3000`, `tools visibility 10` and
   `docker compose up -d --scale notifications=3`
4. Shut down: `docker compose --profile tools down -v`

## Expected results

| Situation | What happens |
|---|---|
| Publishing to the topic | 20 orders in notifications, 11 in loyalty |
| Slow batch, 3 consumers | repeats (13 in 20 in the tests) |
| `EXTEND_VISIBILITY=yes` | no repeats |
| Poison order | goes to the DLQ after 3 receives |

The full walkthrough is in section 19.10 of the book.
