# Chapter 17: PostgreSQL and Redis as a queue

- `producer`: enqueues `N` jobs, in batches of 100, in the queue chosen in `BACKEND` and measures
  the time (`docker compose run --rm producer`)
- `worker`: consumes from the chosen queue and records each delivery in the `deliveries` table;
  run two instances with `--scale worker=2`
- `postgres`: the `job_queue` queue (with `SKIP LOCKED`) and the result of the jobs
- `redis`: Redis 8, with the `jobs` stream and the `workers` group; `REDIS_AOF` turns on AOF
- `rabbitmq`: the `jobs` queue, durable, with persistent messages
- `tools`: `pending` (in Redis, also the deliveries without `XACK`), `result`, `clear`, `pubsub`
  and `persistence`

The implementation is in TypeScript, in `node/`.

## Names used in this chapter

- SQL tables: `job_queue` (`id`, `job_id`, `payload`, `enqueued_at`) and `deliveries`
  (`id`, `job_id`, `backend`, `worker`, `enqueued_at`, `processed_at`)
- Job JSON: `id`, `orderId`, `enqueuedAt`
- Redis: stream `jobs`, consumer group `workers`, Pub/Sub channel `notices`
- RabbitMQ: queue `jobs`
- Environment variables: `BACKEND`, `REDIS_AOF`, `N`, `DATABASE_URL`, `REDIS_URL`,
  `RABBITMQ_URL`, `SERVICE`

## How to run

1. Choose the queue and the persistence in `.env` and start:
   `docker compose up -d --build --wait --scale worker=2`
2. Enqueue: `docker compose run --rm producer`
3. See the queues and the result: `docker compose run --rm tools pending` and
   `docker compose run --rm tools result`
4. To simulate a crash, stop the workers, enqueue and kill the queue:
   `docker compose kill redis` (or `postgres`, `rabbitmq`) and `docker compose start redis`
5. To kill a worker with `BACKEND=redis`: enqueue and run
   `docker kill messaging-ch17-worker-1`; `tools pending` shows the entries without
   `XACK`, which the other worker takes over with `XAUTOCLAIM` after 30 s
6. Shut down: `docker compose down -v`

## Expected results

| Situation | PostgreSQL | Redis without AOF | Redis with AOF | RabbitMQ |
|---|---|---|---|---|
| Process 20,000 jobs | seconds | seconds | seconds | seconds |
| Crash with 10,000 in the queue | nothing lost | unpredictable: from nothing to everything | nothing lost | nothing lost |
| A worker crashes (Redis) | | pending entries taken over, nothing lost | pending entries taken over, nothing lost | |

The full walkthrough is in section 17.7 of the book.
