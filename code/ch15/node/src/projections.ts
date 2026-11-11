import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

const TOPIC = "loyalty-events";
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (message: string) => console.log(`[projections] ${message}`);

type Event = {
  customerId: string; version: number; type: string;
  data: { points: number }; recordedAt: string;
};

// Idempotent projection: the balance keeps the last version applied, and the UPDATE
// only happens for higher versions. Rereading the whole topic does not count any
// event twice.
async function project(event: Event): Promise<void> {
  const redemption = event.type === "PointsRedeemed";
  const points = redemption ? -event.data.points : event.data.points;
  await db.query(
    `INSERT INTO balances (customer_id, balance, version) VALUES ($1, $2, $3)
     ON CONFLICT (customer_id) DO UPDATE
       SET balance = balances.balance + EXCLUDED.balance,
           version = EXCLUDED.version, updated_at = now()
       WHERE balances.version < EXCLUDED.version`,
    [event.customerId, points, event.version],
  );
  await db.query(
    `INSERT INTO statement (customer_id, version, description, points, recorded_at)
     VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
    [event.customerId, event.version, redemption ? "redemption" : "earning", points,
     event.recordedAt],
  );
}

// Rebuild: rereads the source of truth, the event store, in global order (position),
// and applies each event with the same projection function. The topic may have lost
// events (the API's dual write); the event store has not. Each row becomes the same
// JSON the API publishes to the topic.
const EVENTS_IN_ORDER = `
  SELECT json_build_object('customerId', customer_id, 'version', version, 'type', type,
                           'data', data, 'recordedAt', recorded_at)::text AS event
  FROM events ORDER BY position`;

if (process.env.REBUILD === "1") {
  const { rows } = await db.query(EVENTS_IN_ORDER);
  for (const { event } of rows) await project(JSON.parse(event));
  log(`projection rebuilt from ${rows.length} events in the event store`);
  await db.end();
  process.exit(0);
}

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: "projections", fromBeginning: true },
});
await consumer.connect();
await consumer.subscribe({ topic: TOPIC });
process.on("SIGTERM", async () => {
  await consumer.disconnect();
  process.exit(0);
});
log(`reading the "${TOPIC}" topic`);

await consumer.run({
  eachMessage: async ({ message }) => project(JSON.parse(message.value!.toString())),
});
