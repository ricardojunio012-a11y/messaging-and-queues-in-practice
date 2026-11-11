import { hostname } from "node:os";
import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

const APP = process.env.APP ?? "naive"; // naive | versioned
const LATENCY_MAX_MS = Number(process.env.LATENCY_MAX_MS ?? 300);
const INSTANCE = hostname();

type OrderEvent = { orderId: string; type: string; status: string; version: number };

const log = (message: string) => console.log(`[tracking ${INSTANCE}] ${message}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
await db.connect();

// Naive: the last event to arrive sets the status, whatever it is
async function applyNaive(event: OrderEvent): Promise<void> {
  await db.query(
    `INSERT INTO order_status (order_id, status, version) VALUES ($1, $2, $3)
     ON CONFLICT (order_id) DO UPDATE
       SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = now()`,
    [event.orderId, event.status, event.version],
  );
}

// Versioned: only applies the event if it is newer than the stored state
async function applyVersioned(event: OrderEvent): Promise<void> {
  const result = await db.query(
    `INSERT INTO order_status (order_id, status, version) VALUES ($1, $2, $3)
     ON CONFLICT (order_id) DO UPDATE
       SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = now()
       WHERE order_status.version < EXCLUDED.version`,
    [event.orderId, event.status, event.version],
  );
  if (result.rowCount === 0) {
    await db.query(
      "INSERT INTO stale_events (order_id, type, version) VALUES ($1, $2, $3)",
      [event.orderId, event.type, event.version],
    );
    const id = event.orderId.slice(0, 8);
    log(`${event.type} v${event.version} of ${id} is stale`);
  }
}

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: "tracking", fromBeginning: true },
});
await consumer.connect();
await consumer.subscribe({ topic: "order-events" });
log(`reading "order-events" in group "tracking" (app: ${APP})`);

// Within a partition, messages arrive one at a time and in order
await consumer.run({
  eachMessage: async ({ partition, message }) => {
    const event: OrderEvent = JSON.parse(message.value!.toString());
    await sleep(Math.random() * LATENCY_MAX_MS); // variable processing time
    if (APP === "versioned") await applyVersioned(event);
    else await applyNaive(event);
    log(`${event.type} of ${event.orderId.slice(0, 8)} (partition ${partition})`);
  },
});
