import { db, consume, SAGA, type Context } from "./common.ts";

// Compensable transaction: authorizes the payment, and the refund undoes it
async function authorize(ctx: Context): Promise<string> {
  await db.query(
    `INSERT INTO payments (order_id, amount, status) VALUES ($1, $2, 'authorized')
     ON CONFLICT DO NOTHING`,
    [ctx.orderId, ctx.amount],
  );
  return "payment.authorized";
}

// Compensation: in the real world, the refund at the payment gateway
async function refund(ctx: Context): Promise<string> {
  await db.query("UPDATE payments SET status = 'refunded' WHERE order_id = $1",
                 [ctx.orderId]);
  return "payment.refunded";
}

// The same steps in both saga styles; only the message that triggers each one changes
const STEPS = {
  choreography: { "order.created": authorize, "inventory.unavailable": refund },
  orchestration: { "command.payment.authorize": authorize,
                   "command.payment.refund": refund },
};
await consume("payments", STEPS[SAGA]);
