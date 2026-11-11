# Chapter 14: Sagas

- `orders-api`: records the order as `pending` and publishes `order.created`
- `payments`, `inventory` and `shipping`: the saga participants; the same steps in both
  styles, bound to different messages
- `orders`: in choreography, follows the outcome; in orchestration, it is the orchestrator,
  with the `sagas` table and a timeout for the payment (`SAGA_TIMEOUT_S`)
- `load`: creates orders and shows the result, the flow of the last order and the state of
  the sagas

The style is chosen by `SAGA` in `.env` (`choreography` or `orchestration`). The `tshirt`
SKU has 100 units; the `sold-out` SKU has none.

The implementation is in TypeScript, in `node/`.

## How to run

1. Choose the style in `.env` and start: `docker compose up -d --build --wait`
2. Create orders: `docker compose run --rm load orders` or `load orders 5 sold-out`
3. See the result: `load status`, `load flow` and `load sagas`
4. When you change the style in `.env`, recreate with `docker compose up -d`
5. Slow orchestrator (step 6): `SAGA=orchestration`, `SAGA_TIMEOUT_S=2` and
   `ORCHESTRATOR_PAUSE_MS=4000` in `.env`, then `docker compose up -d --scale orders=2`
   and `load orders 1`
6. Shut down: `docker compose down -v` (and set `.env` back to the default values)

## Expected results

| Scenario | Choreography | Orchestration |
|---|---|---|
| Everything fine | 5 confirmed | 5 confirmed |
| Product sold out | payment refunded, order cancelled | the same, commanded by the orchestrator |
| Payments down | orders pending until the service comes back | cancelled after 15 s; late payments refunded |
| Slow orchestrator | does not apply | 1 cancelled and 1 refunded: the conditional advance loses to the timeout |

The orchestrator only advances a saga that is in progress and at the expected step
(`UPDATE ... WHERE step = ... AND status = 'in_progress'`, checking the rows affected).
Without this condition, the advance would overwrite the timeout's cancellation.

## Names used in this chapter

- Topic exchange `saga`; queues `<service>-<style>` (for example `payments-orchestration`)
- Events: `order.created`, `payment.authorized`, `payment.refunded`, `inventory.reserved`,
  `inventory.unavailable`, `shipping.scheduled`
- Commands: `command.payment.authorize`, `command.payment.refund`,
  `command.inventory.reserve`, `command.shipping.schedule`
- Context of every message: `orderId`, `sku`, `amount`
- Tables: `orders` (`status` `pending` | `confirmed` | `cancelled`, `reason`), `sagas`
  (`order_id`, `step` `payment` | `inventory` | `shipping` | `compensating`, `status`
  `in_progress` | `completed` | `cancelled`, `reason`, `updated_at`), `payments`
  (`authorized` | `refunded`), `inventory` (`sku`, `quantity`), `reservations`, `shipments`,
  `history` (`order_id`, `service`, `message`, `at`)
- Reasons: `out of stock`, `payment did not respond`

The full walkthrough is in section 14.10 of the book.
