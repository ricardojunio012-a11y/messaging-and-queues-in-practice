import amqp, { type ConfirmChannel, type ConsumeMessage } from "amqplib";
import pg from "pg";

const RETRY_MODE = process.env.RETRY_MODE ?? "immediate"; // immediate | backoff
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS ?? 3);
const FAILURE_RATE = Number(process.env.FAILURE_RATE ?? 0.3);
const ACCEPT_TOTAL_AS_TEXT = process.env.ACCEPT_TOTAL_AS_TEXT === "yes";
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 50);
const BASE_DELAY_MS = 1000;

const QUEUE = "invoices";
const DLQ = "invoices.dlq";

// Errors that are not solved by trying again (invalid message, business rule)
class PermanentError extends Error {}

type Order = { orderId: string; total: number };

const log = (message: string) => console.log(`[invoice] ${message}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });

function readOrder(body: string): Order {
  let data;
  try {
    data = JSON.parse(body);
  } catch {
    throw new PermanentError("invalid JSON");
  }
  let total = data.total;
  // The bug fix: accept the total as text ("150.00", or "150,00" with a decimal comma)
  if (ACCEPT_TOTAL_AS_TEXT && typeof total === "string") {
    total = Number(total.replace(",", "."));
  }
  if (typeof total !== "number" || !Number.isFinite(total)) {
    throw new PermanentError(`invalid total: ${JSON.stringify(data.total)}`);
  }
  return { orderId: data.orderId, total };
}

// The simulated SEFAZ: slow and unstable. A failure here is transient.
async function callSefaz(): Promise<void> {
  await sleep(LATENCY_MS);
  if (Math.random() < FAILURE_RATE) throw new Error("SEFAZ unavailable (HTTP 503)");
}

async function issueInvoice(order: Order): Promise<void> {
  await callSefaz();
  await db.query(
    "INSERT INTO issued_invoices (order_id) VALUES ($1) ON CONFLICT DO NOTHING",
    [order.orderId],
  );
}

async function record(orderId: string, attempt: number, result: string) {
  await db.query(
    "INSERT INTO attempts (order_id, attempt, result) VALUES ($1, $2, $3)",
    [orderId, attempt, result],
  );
}

// Exponential backoff (1 s, 3 s, 9 s...) with jitter: half fixed, half random
function delayWithJitter(attempt: number): number {
  const delay = BASE_DELAY_MS * 3 ** (attempt - 1);
  return Math.round(delay / 2 + Math.random() * (delay / 2));
}

// Permanent error or attempts exhausted: DLQ. Transient error: the retry queue for
// the attempt's level, where the message waits and then goes back to the main queue.
async function route(channel: ConfirmChannel, message: ConsumeMessage,
                     attempt: number, error: Error): Promise<void> {
  const toDlq = error instanceof PermanentError || attempt > MAX_ATTEMPTS;
  const target = toDlq ? DLQ : `${QUEUE}.retry.${attempt}`;
  channel.sendToQueue(target, message.content, {
    messageId: message.properties.messageId,
    contentType: "application/json",
    persistent: true,
    headers: { "x-attempt": attempt + 1, "x-error": error.message },
    expiration: toDlq ? undefined : String(delayWithJitter(attempt)),
  });
  await channel.waitForConfirms(); // only acks the original once the copy is safe
  channel.ack(message);
  log(`${message.properties.messageId.slice(0, 8)} -> ${target} (${error.message})`);
}

async function processMessage(channel: ConfirmChannel, message: ConsumeMessage) {
  const id: string = message.properties.messageId;
  const attempt = Number(message.properties.headers?.["x-attempt"] ?? 1);
  try {
    await issueInvoice(readOrder(message.content.toString()));
    await record(id, attempt, "issued");
    channel.ack(message);
  } catch (error) {
    const permanent = error instanceof PermanentError;
    await record(id, attempt, permanent ? "invalid" : "transient_failure");
    if (RETRY_MODE === "immediate") {
      channel.nack(message, false, true); // back to the queue, redelivered right away
      return;
    }
    await route(channel, message, attempt, error as Error);
  }
}

// Topology: the "invoices" exchange, the main queue, the retry queues and the DLQ.
// The retry queues have no consumer: when the message's deadline (expiration) passes,
// RabbitMQ sends it to the dead letter exchange, which returns it to the main queue.
async function declareTopology(channel: ConfirmChannel): Promise<void> {
  await channel.assertExchange("invoices", "direct", { durable: true });
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, "invoices", "issue");
  for (let level = 1; level <= MAX_ATTEMPTS; level++) {
    await channel.assertQueue(`${QUEUE}.retry.${level}`, {
      durable: true,
      deadLetterExchange: "invoices",
      deadLetterRoutingKey: "issue",
    });
  }
  await channel.assertQueue(DLQ, { durable: true });
}

const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createConfirmChannel();
await declareTopology(channel);
await channel.prefetch(1);
await channel.consume(QUEUE, (message) => {
  if (message) processMessage(channel, message);
});
log(`consuming "${QUEUE}" (retry: ${RETRY_MODE}, failure rate: ${FAILURE_RATE})`);
