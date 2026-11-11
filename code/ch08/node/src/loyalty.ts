import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

const POINTS_RULE = Number(process.env.POINTS_RULE ?? 1); // 1 (buggy) | 2 (fixed)

type PaidOrder = { orderId: string; customerId: string; total: number; day: number };

const log = (message: string) => console.log(`[loyalty] ${message}`);
const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
await db.connect();

// The points rule. Version 1 has a bug: it divides the total by 10.
function computePoints(total: number): number {
  return POINTS_RULE === 1 ? Math.floor(total / 10) : Math.floor(total);
}

// Writes the order's points. The upsert makes processing idempotent:
// reprocessing the same event replaces the row instead of adding again.
async function savePoints(order: PaidOrder, points: number): Promise<number> {
  await db.query(
    `INSERT INTO points_per_order (order_id, customer_id, total, points, rule)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (order_id) DO UPDATE
       SET points = EXCLUDED.points, rule = EXCLUDED.rule, updated_at = now()`,
    [order.orderId, order.customerId, order.total, points, POINTS_RULE],
  );
  const { rows } = await db.query(
    "SELECT sum(points) AS balance FROM points_per_order WHERE customer_id = $1",
    [order.customerId],
  );
  return Number(rows[0].balance);
}

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const producer = kafka.producer();
await producer.connect();
const consumer = kafka.consumer({
  kafkaJS: { groupId: "loyalty", fromBeginning: true },
});
await consumer.connect();
await consumer.subscribe({ topic: "paid-orders" });

// On stop (docker compose stop sends SIGTERM), leaves the group in an orderly way.
// Without this, the group only becomes empty when the session deadline expires, and
// the replay's offset reset is refused until then.
process.on("SIGTERM", async () => {
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
});
log(`reading "paid-orders" in group "loyalty" (points rule: ${POINTS_RULE})`);

await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const order: PaidOrder = JSON.parse(message.value!.toString());
    const balance = await savePoints(order, computePoints(order.total));
    // The customer's current state goes to a compacted topic, keyed by customer
    await producer.send({
      topic: "customer-balance",
      messages: [{ key: order.customerId, value: String(balance) }],
    });
    log(`${order.orderId} (partition ${partition}, offset ${message.offset}): ` +
        `${order.customerId} has ${balance} points`);
  },
});
