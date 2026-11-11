// Tests of the notifications consumer, against the lab's own Kafka and
// PostgreSQL: `docker compose run --rm tests`. They work for a consumer written in
// any language, because they only publish events and look at the database.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!],
             logLevel: kafkaJavascript.KafkaJS.logLevel.NOTHING },
});
const producer = kafka.producer();

before(async () => {
  await db.connect();
  await producer.connect();
});
after(async () => {
  await producer.disconnect();
  await db.end();
});

// Publishes an OrderCreated like the real producer: CloudEvents in binary mode
async function publish(event: object, version: string, id = randomUUID()) {
  await producer.send({
    topic: "orders",
    messages: [{
      key: "test",
      value: JSON.stringify(event),
      headers: {
        ce_specversion: "1.0", ce_id: id, ce_source: "/tests",
        ce_type: "br.com.pedidocerto.order.created", ce_dataschema: version,
      },
    }],
  });
}

// Waits until the query returns something, instead of sleeping a fixed time: the
// test ends as soon as the condition holds and only fails after the limit
async function waitUntil<T>(query: () => Promise<T | undefined>,
                            limitMs = 15_000): Promise<T> {
  const end = Date.now() + limitMs;
  while (Date.now() < end) {
    const value = await query();
    if (value !== undefined) return value;
    await new Promise((ready) => setTimeout(ready, 100));
  }
  throw new Error(`the condition was not met within ${limitMs} ms`);
}

const emailsFor = async (orderId: string) =>
  (await db.query("SELECT email FROM sent_emails WHERE order_id = $1",
                  [orderId])).rows;
const sent = (orderId: string) => async () => {
  const rows = await emailsFor(orderId);
  return rows.length > 0 ? rows : undefined;
};

test("a v1 OrderCreated produces an email", async () => {
  const orderId = `test-${randomUUID()}`;
  await publish({ orderId, email: "ana@example.com", total: 10 }, "v1");
  const [row] = await waitUntil(sent(orderId));
  assert.equal(row.email, "ana@example.com");
});

test("redelivering the same event does not send the email twice", async () => {
  const orderId = `test-${randomUUID()}`;
  const event = { orderId, email: "ana@example.com", total: 10 };
  const id = randomUUID();
  await publish(event, "v1", id);
  await publish(event, "v1", id); // the same message, delivered again
  // To prove that something did NOT happen, wait for a sentinel published after it:
  // once it is processed, the copy (which came before, in the same partition) was too
  const sentinel = `test-${randomUUID()}`;
  await publish({ orderId: sentinel, email: "s@example.com", total: 1 }, "v1");
  await waitUntil(sent(sentinel));
  assert.equal((await emailsFor(orderId)).length, 1);
});

test("the new fields of v2-compatible do not break the consumer", async () => {
  const orderId = `test-${randomUUID()}`;
  await publish({ orderId, email: "ana@example.com",
                  customerEmail: "ana@example.com", channel: "app", total: 10 },
                "v2-compatible");
  const rejected = async () => {
    const { rows } = await db.query(
      "SELECT reason FROM rejected_events WHERE order_id = $1", [orderId]);
    return rows[0]?.reason as string | undefined;
  };
  // Waits for the first outcome: email sent or event rejected
  const outcome = await waitUntil(async () =>
    (await sent(orderId)()) ? "sent" : await rejected());
  assert.equal(outcome, "sent");
});
