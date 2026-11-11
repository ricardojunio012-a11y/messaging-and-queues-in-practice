import { db, consume, SAGA, type Context } from "./common.ts";

// Pivot transaction: if the reservation fails, the saga rolls back;
// if it succeeds, the saga only moves forward
async function reserve(ctx: Context): Promise<string> {
  const { rowCount } = await db.query(
    "UPDATE inventory SET quantity = quantity - 1 WHERE sku = $1 AND quantity > 0",
    [ctx.sku],
  );
  if (rowCount === 0) return "inventory.unavailable";
  await db.query("INSERT INTO reservations (order_id, sku) VALUES ($1, $2)",
                 [ctx.orderId, ctx.sku]);
  return "inventory.reserved";
}

const STEPS = {
  choreography: { "payment.authorized": reserve },
  orchestration: { "command.inventory.reserve": reserve },
};
await consume("inventory", STEPS[SAGA]);
