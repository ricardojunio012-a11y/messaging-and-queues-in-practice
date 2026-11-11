# Chapter 4: delivery guarantees with RabbitMQ

- `orders-api` (producer): publishes `SendConfirmationEmail` to the `emails` queue, with or without
  publisher confirms (`CONFIRMATION`) and as a persistent or transient message (`PERSISTENT`)
- `notifications` (consumer): acknowledges `after`, `before` or in `auto` mode (`ACK_MODE`)
  and can die once while processing the Nth message (`FAIL_ON_MESSAGE`)

The implementation is in TypeScript, in `node/`.

## How to run

1. Choose the scenario in `.env`; after changing it, run `docker compose up -d`.
2. Start: `docker compose up -d --build`
3. Reset the counts: `docker compose run --rm load reset`
4. Send orders: `docker compose run --rm load`
5. See the result: `docker compose run --rm load status`
6. Limit the queue to 10 messages: `docker compose run --rm load limit` (and `unlimit` to remove it)
7. Shut down: `docker compose down -v`

## Expected results (20 orders, failure on the 5th message)

| `ACK_MODE` | Emails | What happened |
|---|---|---|
| `after` | 21 | one duplicate (at least once) |
| `before` | 19 | one loss (at most once) |
| `auto` | 4 | sixteen losses |

## Names used in this chapter

- Queue `emails`; message type `SendConfirmationEmail` with `{ orderId, to }`.
- Variables `CONFIRMATION` and `PERSISTENT` (`yes`/`no`), `ACK_MODE` (`after`, `before`, `auto`),
  `FAIL_ON_MESSAGE`.
- RabbitMQ policy `emails-limit`; lab tool commands `load`, `load status`, `load reset`,
  `load limit`, `load unlimit`.

The full walkthrough is in section 4.7 of the book.
