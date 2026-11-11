import { createHash } from "node:crypto";
import amqp, { type ConfirmChannel, type ConsumeMessage } from "amqplib";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import pg from "pg";

const QUEUE = "invoices";
const RETRY = "invoices.retry"; // waits 10 s and returns to the main queue
const DLQ = "invoices.dlq";
const MAX_ATTEMPTS = 5;
const s3 = new S3Client({ endpoint: process.env.S3_ENDPOINT, forcePathStyle: true });
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (text: string) => console.log(`[invoice-sender] ${text}`);

// Receives the invoice event. With the Claim Check, fetches the PDF from S3 by the
// reference and checks the hash: the file must be exactly what the issuer wrote.
async function receive(message: ConsumeMessage): Promise<void> {
  const event = JSON.parse(message.content.toString()); // ③ consumes the event
  let pdf: Buffer;
  if (event.invoice) {
    // ④ fetches the PDF from S3 by the reference
    const object = await s3.send(new GetObjectCommand({ Bucket: event.invoice.bucket,
                                                        Key: event.invoice.key }));
    pdf = Buffer.from(await object.Body!.transformToByteArray());
  } else {
    pdf = Buffer.from(event.pdf, "base64");
  }
  const matches = createHash("sha256").update(pdf).digest("hex") === event.sha256;
  await db.query(
    `INSERT INTO sent_invoices (order_id, size, hash_matches) VALUES ($1, $2, $3)
     ON CONFLICT DO NOTHING`,
    [event.orderId, pdf.length, matches],
  );
  log(`invoice for order ${event.orderId}: ${pdf.length} bytes, `
      + `hash matches: ${matches}`);
}

// It failed (S3 or the database down): do not lose the invoice. Up to MAX_ATTEMPTS,
// publish a copy to the waiting queue, which returns it to the main queue after
// 10 s; after that, to the DLQ. Ack the original only once the copy is safe
// (chapter 7).
async function handleFailure(channel: ConfirmChannel, message: ConsumeMessage,
                             error: Error): Promise<void> {
  const attempt = Number(message.properties.headers?.["x-attempt"] ?? 1);
  const target = attempt < MAX_ATTEMPTS ? RETRY : DLQ;
  channel.sendToQueue(target, message.content, {
    persistent: true,
    headers: { "x-attempt": attempt + 1, "x-error": error.message },
  });
  await channel.waitForConfirms();
  channel.ack(message);
  const order = message.content.toString().match(/"orderId":"([^"]*)"/)?.[1];
  log(`invoice for order ${order}: attempt ${attempt} failed (${error.message}); `
      + `sent to ${target}`);
}

const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createConfirmChannel();
await channel.assertQueue(QUEUE, { durable: true });
// The waiting queue has no consumer: the TTL expires and the dead-letter returns
// the message
await channel.assertQueue(RETRY, {
  durable: true,
  messageTtl: 10_000,
  deadLetterExchange: "",
  deadLetterRoutingKey: QUEUE,
});
await channel.assertQueue(DLQ, { durable: true });
await channel.prefetch(1);
await channel.consume(QUEUE, async (message) => {
  if (!message) return;
  try {
    await receive(message);
    channel.ack(message);
  } catch (error) {
    await handleFailure(channel, message, error as Error);
  }
});
log(`consuming the "${QUEUE}" queue`);
