import { existsSync, writeFileSync } from "node:fs";
import { hostname } from "node:os";
import amqp, { type ConsumeMessage } from "amqplib";
import pg from "pg";

const IDEMPOTENCY = process.env.IDEMPOTENCY ?? "none"; // none | inbox | key
const FAIL_ON_MESSAGE = Number(process.env.FAIL_ON_MESSAGE ?? 0);
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 100);
const INSTANCE = hostname();
const FAILURE_MARKER = "/counter/failure.marker";

// Two connections: the consumer's database and the "email provider", which is
// an external system and does not take part in the consumer's transactions.
const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
const provider = new pg.Client({ connectionString: process.env.DATABASE_URL });

const log = (text: string) => console.log(`[notifications ${INSTANCE}] ${text}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let processed = 0;

// Simulates the process dying (like a kill -9) only once, on the chosen message
function maybeFail(moment: string): void {
  if (processed !== FAIL_ON_MESSAGE || existsSync(FAILURE_MARKER)) return;
  writeFileSync(FAILURE_MARKER, moment);
  log(`SIMULATED FAILURE: the process dies ${moment}`);
  process.exit(1);
}

// Call to the email provider. With a key, the provider ignores the repeated request.
async function sendEmail(orderId: string, key: string | null): Promise<void> {
  await sleep(LATENCY_MS);
  await provider.query(
    `INSERT INTO provider_emails (order_id, idempotency_key) VALUES ($1, $2)
     ON CONFLICT (idempotency_key) DO NOTHING`,
    [orderId, key],
  );
}

async function recordHistory(orderId: string, messageId: string): Promise<void> {
  await db.query(
    "INSERT INTO notification_history (order_id, message_id) VALUES ($1, $2)",
    [orderId, messageId],
  );
}

async function processMessage(message: ConsumeMessage, channel: amqp.Channel) {
  processed++;
  const messageId: string = message.properties.messageId;
  const { orderId } = JSON.parse(message.content.toString());

  if (IDEMPOTENCY === "none") {
    await recordHistory(orderId, messageId);
    await sendEmail(orderId, null);
    maybeFail("after sending the email and before acknowledging");
    channel.ack(message);
    return;
  }

  // Inbox: record the message and the effect in the database in the same transaction
  await db.query("BEGIN");
  // ① Records the id in the inbox; if it was already there, the message is a duplicate
  const inbox = await db.query(
    `INSERT INTO processed_messages (message_id) VALUES ($1)
     ON CONFLICT DO NOTHING`,
    [messageId],
  );
  if (inbox.rowCount === 0) {
    await db.query("ROLLBACK");
    log(`message ${messageId.slice(0, 8)} already processed: duplicate ignored`);
    channel.ack(message);
    return;
  }
  // ② Inserted: records the effect in the database, in the same transaction
  await recordHistory(orderId, messageId);
  // The email is an external effect: ROLLBACK does not undo it. With IDEMPOTENCY=key,
  // the message id goes to the provider as the idempotency key.
  await sendEmail(orderId, IDEMPOTENCY === "key" ? messageId : null);
  maybeFail("after sending the email and before the COMMIT");
  // ③ COMMIT: the inbox and the effect together, or neither
  await db.query("COMMIT");
  // ④ The ack only after the COMMIT
  channel.ack(message);
}

await db.connect();
await provider.connect();
const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createChannel();
await channel.assertQueue("emails", { durable: true });
await channel.prefetch(1);
await channel.consume("emails", (message) => {
  if (message) processMessage(message, channel);
});
log(`consuming the "emails" queue (idempotency: ${IDEMPOTENCY})`);
