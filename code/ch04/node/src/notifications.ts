import { appendFileSync, existsSync, writeFileSync } from "node:fs";
import { hostname } from "node:os";
import amqp, { type ConsumeMessage } from "amqplib";

const ACK_MODE = process.env.ACK_MODE ?? "after"; // after | before | auto
const FAIL_ON_MESSAGE = Number(process.env.FAIL_ON_MESSAGE ?? 0);
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 200);
const INSTANCE = hostname();
const FAILURE_MARKER = "/counter/failure.marker";

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

// The email "sending": records the order in a file for the lab to count
function sendEmail(orderId: string, redelivery: boolean): void {
  appendFileSync("/counter/emails.txt",
    `${orderId} ${INSTANCE} ${redelivery ? "yes" : "no"}\n`);
}

const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createChannel();
await channel.assertQueue("emails", { durable: true });

async function processMessage(message: ConsumeMessage): Promise<void> {
  processed++;
  const { orderId } = JSON.parse(message.content.toString());
  const redelivery = message.fields.redelivered; // did the broker deliver it before?
  const note = redelivery ? " (redelivery)" : "";
  log(`message ${processed}: order ${orderId.slice(0, 8)}${note}`);

  if (ACK_MODE === "before") channel.ack(message); // acks before doing the work

  await sleep(LATENCY_MS / 2);
  if (ACK_MODE !== "after") {
    maybeFail("in the middle of sending, with the message already acknowledged");
  }
  await sleep(LATENCY_MS / 2);
  sendEmail(orderId, redelivery);

  if (ACK_MODE === "after") {
    maybeFail("after sending the email and before acknowledging");
    channel.ack(message); // acks only after the work is done
  }
}

// With manual acknowledgement, the broker delivers one message at a time (prefetch 1).
// With automatic acknowledgement, it considers the message delivered once it is sent.
const automatic = ACK_MODE === "auto";
if (!automatic) await channel.prefetch(1);
let previous = Promise.resolve();
await channel.consume("emails", (message) => {
  if (message) previous = previous.then(() => processMessage(message)); // one at a time
}, { noAck: automatic });
log(`consuming the "emails" queue (acknowledgement: ${ACK_MODE})`);
