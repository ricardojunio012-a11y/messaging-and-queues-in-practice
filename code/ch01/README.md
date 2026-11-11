# Chapter 1: when the direct call breaks

Two services of the Pedido Certo store, with no broker:

- `orders-api` (port 8080): `POST /orders` and `GET /health`
- `notifications` (port 8081): `POST /emails` and `GET /emails` (count of emails sent and duplicates)

The implementation is in TypeScript, in `node/`.

## How to run

1. Choose the scenario in the `.env` file.
2. Start the services: `docker compose up -d --build`
3. Generate load: `docker compose run --rm load`
4. See the state: `docker compose run --rm load status`
5. Shut down: `docker compose down`

## Lab scenarios

| Step | `.env` | Extra command | What to observe |
|---|---|---|---|
| 1 | `MODE=direct` | | 20 `201` responses, 20 emails |
| 2 | `MODE=direct` | `docker compose stop notifications` | 20 `503` responses |
| 3 | `MODE=direct`, `LATENCY_MS=3000` | | each order takes about 3 s |
| 4 | `MODE=timeout`, `LATENCY_MS=1500` | | `503` and 60 emails for 20 orders |
| 5 | `MODE=memory-queue` | `stop notifications`, then `kill orders-api` | fast `202` and no email delivered |

The full walkthrough, with an explanation of each result, is in section 1.10 of the book.
