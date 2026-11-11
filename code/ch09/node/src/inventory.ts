import { hostname } from "node:os";
import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

const PROCESSING_MS = Number(process.env.PROCESSING_MS ?? 100);
// The order's deadline (s): older than this, it is discarded without processing.
// 0 = no deadline
const MAX_TIME_S = Number(process.env.MAX_TIME_S ?? 0);
const INSTANCE = hostname();

type Order = { orderId: string; sku: string; quantity: number };

const log = (message: string) => console.log(`[inventory ${INSTANCE}] ${message}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
await db.connect();

// Reserves the order's stock. The processing time is fixed: each instance handles
// at most 1000 / PROCESSING_MS orders per second. An order that waited longer than
// the deadline has lost its value: it is recorded as expired, without spending the
// processing.
async function reserve(order: Order, publishedAt: number): Promise<void> {
  if (MAX_TIME_S > 0 && Date.now() - publishedAt > MAX_TIME_S * 1000) {
    await db.query(
      "INSERT INTO reservations (order_id, instance, state) " +
        "VALUES ($1, $2, 'expired')",
      [order.orderId, INSTANCE]);
    return;
  }
  await sleep(PROCESSING_MS);
  await db.query("INSERT INTO reservations (order_id, instance) VALUES ($1, $2)",
                 [order.orderId, INSTANCE]);
}

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: "inventory", fromBeginning: true },
  // Looks for new partitions in the topic every 10 s (the default is 5 minutes), so
  // the lab can show the partition increase without a long wait
  "topic.metadata.refresh.interval.ms": 10000,
});
await consumer.connect();
await consumer.subscribe({ topic: "orders-received" });

// On stop, leaves the group in an orderly way: this instance's partitions are
// redistributed right away, without waiting for the session deadline (chapter 8).
process.on("SIGTERM", async () => {
  await consumer.disconnect();
  process.exit(0);
});
log(`reading "orders-received" in group "inventory" (${PROCESSING_MS} ms per order)`);

// Within an instance, messages are processed one at a time:
// parallelism comes from having more instances, and each partition has a single owner.
await consumer.run({
  eachMessage: async ({ message }) => {
    await reserve(JSON.parse(message.value!.toString()), Number(message.timestamp));
  },
});
