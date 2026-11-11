import kafkaJavascript from "@confluentinc/kafka-javascript";
import { readFileSync } from "node:fs";
import pg from "pg";

const EXPECTED_TYPE = "br.com.pedidocerto.order.created";

const log = (message: string) => console.log(`[notifications] ${message}`);
const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
await db.connect();

// Step 4: with CONSUMER_SCHEMA, the consumer only accepts events that follow the
// schema it knows (declared and required properties), as a reader that validates
// does, or one that deserializes into a class that refuses extra fields.
// Without the variable, it is the tolerant reader.
const SCHEMA = process.env.CONSUMER_SCHEMA || null;
const schema = SCHEMA
  ? JSON.parse(readFileSync(`/schemas/${SCHEMA}.json`, "utf8")) : null;

function outsideSchema(event: any): string | null {
  if (!schema) return null;
  const extra = Object.keys(event).find((field) => !(field in schema.properties));
  if (extra) return `outside schema ${SCHEMA}: unknown field "${extra}"`;
  const missing = schema.required.find((field: string) => !(field in event));
  return missing ? `outside schema ${SCHEMA}: missing field "${missing}"` : null;
}

async function reject(event: any, reason: string): Promise<void> {
  await db.query(
    "INSERT INTO rejected_events (order_id, reason) VALUES ($1, $2)",
    [event.orderId ?? null, reason],
  );
  log(`${event.orderId}: rejected, ${reason}`);
}

// Tolerant reader: uses only the fields it needs and ignores the ones it does not
// know. A new field in the event does not break this consumer; a missing field it
// uses does, and in that case the event is rejected with the reason.
async function handleOrderCreated(event: any, version: string): Promise<void> {
  const reason = outsideSchema(event);
  if (reason) return reject(event, reason);
  if (typeof event.email !== "string") {
    return reject(event, 'required field "email" missing');
  }
  await db.query(
    `INSERT INTO sent_emails (order_id, email, version) VALUES ($1, $2, $3)
     ON CONFLICT DO NOTHING`,
    [event.orderId, event.email, version],
  );
  log(`${event.orderId}: email sent to ${event.email}`);
}

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: "notifications", fromBeginning: true },
});
await consumer.connect();
await consumer.subscribe({ topic: "orders" });
process.on("SIGTERM", async () => {
  await consumer.disconnect();
  process.exit(0);
});
log('reading the "orders" topic');

await consumer.run({
  eachMessage: async ({ message }) => {
    // The CloudEvents attributes arrive in the headers (binary mode)
    const header = (name: string) => message.headers?.[name]?.toString();
    if (header("ce_type") !== EXPECTED_TYPE) return; // not an event we handle
    await handleOrderCreated(JSON.parse(message.value!.toString()),
                             header("ce_dataschema") ?? "?");
  },
});
