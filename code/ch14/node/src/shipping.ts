import { db, consume, SAGA, type Context } from "./common.ts";

// Retriable transaction: after the pivot, this step must succeed, even if it takes
// retrying; that is why it is idempotent
async function schedule(ctx: Context): Promise<string> {
  await db.query("INSERT INTO shipments (order_id) VALUES ($1) ON CONFLICT DO NOTHING",
                 [ctx.orderId]);
  return "shipping.scheduled";
}

const STEPS = {
  choreography: { "inventory.reserved": schedule },
  orchestration: { "command.shipping.schedule": schedule },
};
await consume("shipping", STEPS[SAGA]);
