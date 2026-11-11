# Chapter 16: Claim Check and delayed messages

- `invoice-issuer`: `POST /invoices/{orderId}` generates a 5 MB PDF (simulated) and
  publishes the event to the `invoices` queue; with `CLAIM_CHECK=yes`, the PDF goes to S3
  and the message carries only the reference and the hash
- `invoice-sender`: consumes the queue, fetches the PDF (from the message or from S3) and
  checks the hash. If it fails (S3 or the database down), it publishes a copy to the
  waiting queue `invoices.retry` (TTL of 10 s, with dead-letter back to `invoices`) and
  only then acks the original; after 5 attempts, the invoice goes to `invoices.dlq`
- `reminders`: declares the waiting queue `carts-waiting` (TTL of `REMINDER_S` seconds,
  with dead-letter to the `reminders` queue) and, on delivery, checks whether the cart
  became a purchase
- `rabbitmq`: with `max_message_size` of 1 MiB (`rabbitmq/limit.conf`)
- `localstack`: local S3, version 4.14.0 (versions from 2026.03 on require a token)
- `load`: issues invoices, abandons and buys carts, and shows the results

The implementation is in TypeScript, in `node/`.

## Names used in this chapter

- Bucket `invoices`, object key `invoices/{orderId}.pdf`
- Queues `invoices`, `invoices.retry`, `invoices.dlq`, `carts-waiting`, `reminders`
- Event fields: `orderId`, `sha256`, `size`, `invoice` (`bucket`, `key`) or `pdf`
- Cart message: `cartId`, `abandonedAt`; retry headers `x-attempt` and `x-error`
- HTTP response: `orderId`, `bytesInMessage`, `error`
- Tables `sent_invoices` (`order_id`, `size`, `hash_matches`, `received_at`),
  `purchases` (`cart_id`, `purchased_at`) and `reminders` (`cart_id`, `abandoned_at`,
  `processed_at`, `result`: `sent` or `discarded (already bought)`)
- Variables `CLAIM_CHECK` (`no` or `yes`), `REMINDER_S`, `S3_ENDPOINT`, `DATABASE_URL`

## How to run

1. Start: `docker compose up -d --build --wait`
2. Issue an invoice: `docker compose run --rm load invoice P1`
3. Switch to `CLAIM_CHECK=yes`, run `docker compose up -d` and issue again
4. Abandon and buy carts: `load cart C1`, `load buy C1`, `load reminders`
5. Transient S3 failure: stop `invoice-sender`, issue an invoice, run
   `docker network disconnect messaging-ch16_default messaging-ch16-localstack-1`, start
   `invoice-sender`, wait about 20 s and reconnect with
   `docker network connect --alias localstack messaging-ch16_default messaging-ch16-localstack-1`
6. Replaced object: with `invoice-sender` stopped, issue invoice P5, overwrite the object
   with `docker compose exec localstack awslocal s3 cp /etc/hostname s3://invoices/invoices/P5.pdf`
   and start the service
7. Shut down: `docker compose down -v`

## Expected results

| Situation | What happens |
|---|---|
| 5 MB PDF in the message | rejected by the broker (1 MiB limit) |
| Claim Check | message under 200 bytes; PDF delivered whole, hash checked |
| Abandoned cart | reminder delivered after `REMINDER_S` seconds |
| Cart bought in the meantime | reminder discarded |
| S3 down during consumption | the invoice comes back through the waiting queue and is delivered when S3 returns |
| Object overwritten in S3 | `hash_matches = f` |

The full walkthrough is in section 16.7 of the book.
