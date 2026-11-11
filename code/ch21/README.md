# Chapter 21: Stream processing

Orders per minute and per region, counted in three ways from the `orders` topic:

- `windows`: a fixed 1-minute window built by hand, in TypeScript (`node/src/windows.ts`),
  writes to `windows-manual`
- `kafka-streams`: the same count with Kafka Streams (Java), writes to
  `windows-kafka-streams`
- `flink-jobmanager` and `flink-taskmanager`: a Flink 2.2.1 cluster; the SQL job
  (`flink/windows.sql`) is submitted with `docker compose run --rm flink-sql` and writes
  to `windows-flink`
- `tools`: `generate [burst] [no-end]`, `end` and `result`

The tolerance for late events is in `TOLERANCE_MS`, in `.env` (hand-made window and Kafka
Streams); in Flink, it is in the watermark of the SQL.

## Names used in this chapter

- Topics: `orders`, `windows-manual`, `windows-kafka-streams`, `windows-flink` (and
  `late-orders` in exercise 8)
- Order JSON: `{"region", "total", "ts"}`; regions `southeast`, `south`, `northeast`
- Window JSON: `{"window", "region", "orders"}`

## How to run

1. Start: `docker compose up -d --build` and `docker compose --profile tools build`
2. Submit the job to Flink: `docker compose run --rm flink-sql`
3. Publish: `docker compose run --rm tools generate`
4. Compare: `docker compose run --rm tools result`
5. To start from scratch: `docker compose --profile tools down -v` and repeat from step 1

## Expected results

| Situation | Southeast 12:00 (manual, k.streams, flink) | South 12:00 |
|---|---|---|
| `generate`, tolerance 30 s | 60, 60, 60 | 30, 30, 30 |
| `generate no-end` | no 12:03 window until you run `end` | |
| `generate burst` | 60, 60, 60 | 30, 30, 45 |
| `generate` with `TOLERANCE_MS=0` | 50, 50, 60 | 30, 30, 30 |

The full walkthrough is in section 21.9 of the book.
