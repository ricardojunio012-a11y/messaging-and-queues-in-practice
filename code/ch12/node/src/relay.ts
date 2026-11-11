import { existsSync, writeFileSync } from "node:fs";
import pg from "pg";
import { sleep, log as logFor, publish } from "./common.ts";

const FAIL_AFTER_PUBLISH = process.env.RELAY_FAIL_AFTER_PUBLISH === "yes";
const FAILURE_MARKER = "/counter/relay.marker";
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (message: string) => logFor("relay", message);

// Simulates the relay going down (like a kill -9) only once
function maybeFail(): void {
  if (!FAIL_AFTER_PUBLISH || existsSync(FAILURE_MARKER)) return;
  writeFileSync(FAILURE_MARKER, "x");
  log("SIMULATED FAILURE: relay goes down after publishing, before marking");
  process.exit(1);
}

// One relay round: locks up to 50 pending events, in order, publishes and marks them.
// SKIP LOCKED lets several relay instances work without taking the same rows.
// If publishing fails, the ROLLBACK returns the rows, and the next round tries
// again.
async function publishPending(): Promise<number> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query(
      `SELECT id, event_id, type, payload FROM outbox
       WHERE published_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED`,
    );
    if (rows.length > 0) {
      await publish(rows.map((r) => ({ id: r.event_id, type: r.type,
                                       data: r.payload })));
      maybeFail();
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
