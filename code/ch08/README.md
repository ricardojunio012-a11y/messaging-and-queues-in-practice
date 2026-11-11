# Chapter 8: retention, replay and reprocessing

- `loyalty`: reads the `paid-orders` topic in the `loyalty` group, computes the points of
  each order with the rule chosen in `POINTS_RULE` (1, with the bug; 2, fixed), writes them
  to `points_per_order` with an upsert and publishes the customer's balance to the
  compacted `customer-balance` topic
- `kafka`: `paid-orders` (3 partitions, 30-day retention) and `customer-balance` (compacted)
- `postgres`: the points of each order
- `tools`: the Kafka command-line tools and `psql`

The implementation is in TypeScript, in `node/`.

## How to run

1. Choose the rule in the `.env` file; after changing it, run `docker compose up -d`.
2. Start: `docker compose up -d --build`
3. Publish a week: `docker compose run --rm tools week` (or `day 8`)
4. See the result: `docker compose run --rm tools status`
5. Replay: `docker compose stop loyalty`, `docker compose run --rm tools replay` (shows the
   plan with `--dry-run` and then runs it) and `docker compose up -d loyalty`. In a second
   terminal, before starting the consumer, `docker compose run --rm tools follow customer-1`
   shows the intermediate balances published during the replay (until it reaches 5244; the
   values in between vary)
6. Compacted topic: `docker compose run --rm tools balances`
7. Shut down: `docker compose down -v`

## Expected results

| Moment | Correct orders | Total balance |
|---|---|---|
| Week with `POINTS_RULE=1` | 0 of 210 | 4,661 |
| `POINTS_RULE=2` and `day 8` | 30 of 240 | 11,186 |
| After the replay | 240 of 240 | 54,080 |

## Names used in this chapter

- Topics `paid-orders` and `customer-balance` (compacted, key = customer); group `loyalty`.
- Event fields: `orderId` (`d1-p1`: day 1, order 1), `customerId` (`customer-1` to
  `customer-10`), `total`, `day`.
- Table `points_per_order` (`order_id`, `customer_id`, `total`, `points`, `rule`,
  `updated_at`).

The full walkthrough is in section 8.9 of the book.
