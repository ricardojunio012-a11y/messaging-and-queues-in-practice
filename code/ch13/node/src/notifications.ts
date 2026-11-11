import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

const TOPIC = "outbox.event.order";
const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
await db.connect();
const log = (message: string) => console.log(`[notifications] ${message}`);

// Inbox (chapter 5). The event id arrives in the "id" header: it is the id of the
// outbox row, the same on any resend, whether from Debezium or the polling relay.
async function processMessage(messageId: string, orderId: string): Promise<void> {
  await db.query("INSERT INTO deliveries (message_id) VALUES ($1)", [messageId]);
  await db.query("BEGIN");
  const inbox = await db.query(
    "INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING",
    [messageId],
  );
  if (inbox.rowCount === 0) {
    await db.query("ROLLBACK");
    log(`message ${messageId.slice(0, 8)} repeated: dropped`);
    return;
  }
  await db.query("INSERT INTO sent_emails (order_id, message_id) VALUES ($1, $2)",
                 [orderId, messageId]);
  await db.query("COMMIT");
}

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: "notifications", fromBeginning: true },
});
await consumer.connect();
await consumer.subscribe({ topic: TOPIC });
process.on("SIGTERM", async () => {
  await consumer.disconnect();
  process.exit(0);
});
log(`reading the "${TOPIC}" topic`);

await consumer.run({
  eachMessage: async ({ message }) => {
    const messageId = message.headers?.id?.toString();
    const event = JSON.parse(message.value!.toString());
    if (messageId) await processMessage(messageId, event.orderId);
  },
});
