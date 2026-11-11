import { db, consume, log as logFor, SAGA, type Context } from "./common.ts";

const TIMEOUT_S = Number(process.env.SAGA_TIMEOUT_S ?? 15);
const log = (message: string) => logFor("orders", message);

async function finishOrder(ctx: Context, status: string, reason: string | null) {
  await db.query("UPDATE orders SET status = $2, reason = $3 WHERE id = $1",
                 [ctx.orderId, status, reason]);
}

// Choreography: the orders service only learns the outcome
const choreography = {
  "shipping.scheduled": async (ctx: Context) => {
    await finishOrder(ctx, "confirmed", null);
    return null;
  },
  "payment.refunded": async (ctx: Context) => {
    await finishOrder(ctx, "cancelled", "out of stock");
    return null;
  },
};

// Advances the saga only if it is still in progress and at the expected step. It is
// the "reread the value" countermeasure (section 14.7) done by the UPDATE itself: if
// the timeout cancelled the saga after the message arrived, no row changes, and the
// caller finds out from the number of rows affected.
async function advance(ctx: Context, from: string, to: string,
                       status = "in_progress", reason: string | null = null) {
  const { rowCount } = await db.query(
    `UPDATE sagas SET step = $3, status = $4, reason = $5, updated_at = now()
     WHERE order_id = $1 AND step = $2 AND status = 'in_progress'`,
    [ctx.orderId, from, to, status, reason],
  );
  return rowCount === 1;
}

async function sagaStatus(ctx: Context) {
  const { rows } = await db.query("SELECT status FROM sagas WHERE order_id = $1",
                                  [ctx.orderId]);
  return rows[0]?.status as string | undefined;
}

// Step 6 of the lab: simulates a slow orchestrator (GC pause, contended CPU)
const PAUSE_MS = Number(process.env.ORCHESTRATOR_PAUSE_MS ?? 0);
const pause = () => new Promise((done) => setTimeout(done, PAUSE_MS));

// Orchestration: the orchestrator keeps the saga state and decides the next step
async function decide(ctx: Context, event: string): Promise<string | null> {
  switch (event) {
    case "order.created":
      await db.query("INSERT INTO sagas (order_id, step) VALUES ($1, 'payment')",
                     [ctx.orderId]);
      return "command.payment.authorize";
    case "payment.authorized":
      if (PAUSE_MS > 0) await pause();
      if (await advance(ctx, "payment", "inventory")) {
        return "command.inventory.reserve";
      }
      // The saga was no longer waiting for the payment. If the timeout cancelled it,
      // the reply arrived late and the payment must be refunded; if not, it is a
      // duplicate.
      const status = await sagaStatus(ctx);
      return status === "cancelled" ? "command.payment.refund" : null;
    case "inventory.reserved":
      return (await advance(ctx, "inventory", "shipping"))
        ? "command.shipping.schedule" : null;
    case "inventory.unavailable":
      return (await advance(ctx, "inventory", "compensating"))
        ? "command.payment.refund" : null;
    case "payment.refunded":
      // In a saga cancelled by the timeout, the order has already been finished
      if (await advance(ctx, "compensating", "compensating", "cancelled",
                        "out of stock")) {
        await finishOrder(ctx, "cancelled", "out of stock");
      }
      return null;
    case "shipping.scheduled":
      if (await advance(ctx, "shipping", "shipping", "completed")) {
        await finishOrder(ctx, "confirmed", null);
      }
      return null;
  }
  return null;
}

// Timeout: if the payment does not respond, the orchestrator gives up on the saga.
// There has been no pivot transaction yet, so cancelling is enough; if the payment
// happens later, the late reply is refunded (see "payment.authorized" above).
async function cancelExpiredSagas() {
  const { rows } = await db.query(
    `WITH expired AS (
       UPDATE sagas SET status = 'cancelled', reason = 'payment did not respond',
                        updated_at = now()
       WHERE status = 'in_progress' AND step = 'payment'
         AND updated_at < now() - make_interval(secs => $1)
       RETURNING order_id)
     UPDATE orders SET status = 'cancelled', reason = 'payment did not respond'
     WHERE id IN (SELECT order_id FROM expired) RETURNING id`,
    [TIMEOUT_S],
  );
  for (const { id } of rows) {
    log(`order ${id.slice(0, 8)} cancelled: payment did not respond`);
  }
}

if (SAGA === "choreography") {
  await consume("orders", choreography);
} else {
  const events = ["order.created", "payment.authorized", "payment.refunded",
                  "inventory.reserved", "inventory.unavailable", "shipping.scheduled"];
  await consume("orders", Object.fromEntries(events.map((e) => [e, decide])));
  setInterval(() => cancelExpiredSagas().catch((e) => log(e.message)), 1000);
}
