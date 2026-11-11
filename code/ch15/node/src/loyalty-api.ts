import { createServer, type ServerResponse } from "node:http";
import kafkaJavascript from "@confluentinc/kafka-javascript";
import pg from "pg";

const PORT = Number(process.env.PORT ?? 8080);
const SNAPSHOT_EVERY = Number(process.env.SNAPSHOT_EVERY ?? 0);
const TOPIC = "loyalty-events";
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (message: string) => console.log(`[loyalty-api] ${message}`);

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const producer = kafka.producer();
await producer.connect();

type State = { balance: number; version: number };
type Event = { version: number; type: string; data: { points: number } };
class Conflict extends Error {}
class Refusal extends Error {}

// The state is not stored anywhere: it is the result of applying the events,
// in order
function apply(state: State, event: Event): State {
  const points = event.data.points;
  const balance = event.type === "PointsRedeemed" ? state.balance - points
                                                  : state.balance + points;
  return { balance, version: event.version };
}

// Loads the state from the most recent snapshot (if any) and the events
// after it
async function load(customerId: string) {
  let state: State = { balance: 0, version: 0 };
  if (SNAPSHOT_EVERY > 0) {
    const { rows } = await db.query(
      "SELECT state FROM snapshots WHERE customer_id = $1", [customerId]);
    if (rows.length > 0) state = rows[0].state;
  }
  const { rows: events } = await db.query(
    `SELECT version, type, data FROM events
     WHERE customer_id = $1 AND version > $2 ORDER BY version`,
    [customerId, state.version],
  );
  return { state: events.reduce(apply, state), read: events.length };
}

// Appends the event at the version after the expected one. If another command wrote
// that version first, the primary key (customer_id, version) refuses it: that is
// optimistic concurrency.
async function append(customerId: string, expectedVersion: number, type: string,
                      data: { points: number }): Promise<Event> {
  const event = { version: expectedVersion + 1, type, data };
  try {
    const { rows } = await db.query(
      `INSERT INTO events (customer_id, version, type, data) VALUES ($1, $2, $3, $4)
       RETURNING recorded_at`,
      [customerId, event.version, type, data],
    );
    // Simplification: in production, the event would go to Kafka through an outbox
    // or CDC (chapters 12 and 13)
    const message = { customerId, ...event, recordedAt: rows[0].recorded_at };
    await producer.send({
      topic: TOPIC,
      messages: [{ key: customerId, value: JSON.stringify(message) }],
    });
  } catch (error) {
    if ((error as { code?: string }).code === "23505") throw new Conflict();
    throw error;
  }
  return event;
}

// A command: loads the state, validates the business rule and appends the event
async function execute(customerId: string, action: string, points: number,
                       expectedVersion?: number) {
  const { state, read } = await load(customerId);
  if (action === "redeem" && points > state.balance) {
    throw new Refusal("insufficient balance");
  }
  const type = action === "redeem" ? "PointsRedeemed" : "PointsEarned";
  const expected = expectedVersion ?? state.version;
  const event = await append(customerId, expected, type, { points });
  const updated = apply(state, event);
  if (SNAPSHOT_EVERY > 0 && read >= SNAPSHOT_EVERY) {
    await db.query(
      `INSERT INTO snapshots (customer_id, version, state) VALUES ($1, $2, $3)
       ON CONFLICT (customer_id) DO UPDATE SET version = $2, state = $3`,
      [customerId, updated.version, updated],
    );
  }
  return { ...updated, eventsRead: read };
}

function respond(res: ServerResponse, status: number, data: object): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

const server = createServer(async (req, res) => {
  const [, resource, customerId, action] = (req.url ?? "").split("/");
  if (resource !== "customers" || !customerId) {
    return respond(res, 404, { error: "Route not found" });
  }
  // Query: reads the projection (read side), which may be slightly behind
  if (req.method === "GET") {
    const { rows } = await db.query(
      "SELECT balance, version FROM balances WHERE customer_id = $1", [customerId]);
    return respond(res, 200, rows[0] ?? { balance: 0, version: 0 });
  }
  let body = "";
  for await (const chunk of req) body += chunk;
  const { points, expectedVersion } = JSON.parse(body);
  try {
    respond(res, 201, await execute(customerId, action, points, expectedVersion));
  } catch (error) {
    if (error instanceof Conflict) {
      return respond(res, 409, { error: "version changed" });
    }
    if (error instanceof Refusal) return respond(res, 422, { error: error.message });
    log(`error: ${(error as Error).message}`);
    respond(res, 500, { error: "Failed to execute the command." });
  }
});

server.listen(PORT, () =>
  log(`listening on port ${PORT} (snapshot every ${SNAPSHOT_EVERY})`));
