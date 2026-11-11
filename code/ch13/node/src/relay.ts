import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

// The same topic, the same key and the same "id" header that Debezium uses, so that
// the consumer does not notice when one relay is replaced by the other
const TOPIC = "outbox.event.order";
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (message: string) => console.log(`[relay] ${message}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const producer = kafka.producer();
await producer.connect();

// The polling relay of chapter 12: locks the pending events, publishes and marks them
async function publishPending(): Promise<number> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `SELECT id, aggregateid, payload FROM outbox WHERE published_at IS NULL
       ORDER BY created_at LIMIT 50 FOR UPDATE SKIP LOCKED`,
    );
    if (rows.length > 0) {
      await producer.send({
        topic: TOPIC,
        messages: rows.map((r) => ({
          key: r.aggregateid,
          value: JSON.stringify(r.payload),
          headers: { id: r.id },
        })),
      });
      await client.query("UPDATE outbox SET published_at = now() WHERE id = ANY($1)",
                         [rows.map((r) => r.id)]);
    }
    await client.query("COMMIT");
    return rows.length;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

log("reading the outbox every 500 ms");
while (true) {
  try {
    const published = await publishPending();
    if (published > 0) log(`${published} event(s) published`);
  } catch (error) {
    log(`could not publish, trying again: ${(error as Error).message}`);
  }
  await sleep(500);
}
