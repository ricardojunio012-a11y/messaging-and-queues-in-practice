import { hostname } from "node:os";
import amqp from "amqplib";
import pg from "pg";
import { createClient } from "redis";

const BACKEND = process.env.BACKEND ?? "postgres";
const NAME = hostname();
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (text: string) => console.log(`[worker] ${text}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type Job = { id: string; enqueuedAt: string };
type Executor = { query: pg.Pool["query"] };

// The "work" of each job: record the delivery
async function processJob(executor: Executor, job: Job): Promise<void> {
  await executor.query(
    `INSERT INTO deliveries (job_id, backend, worker, enqueued_at)
     VALUES ($1, $2, $3, $4)`,
    [job.id, BACKEND, NAME, job.enqueuedAt],
  );
}

// PostgreSQL: locks up to 10 jobs with SKIP LOCKED, processes and deletes them, in the
// same transaction. If the worker dies in the middle, the ROLLBACK returns the jobs.
async function consumePostgres(): Promise<void> {
  while (true) {
    const client = await db.connect();
    let processed = 0;
    try {
      await client.query("BEGIN");
      const { rows } = await client.query(
        `SELECT id, payload FROM job_queue ORDER BY id LIMIT 10
         FOR UPDATE SKIP LOCKED`);
      for (const row of rows) await processJob(client, row.payload);
      await client.query("DELETE FROM job_queue WHERE id = ANY($1)",
                         [rows.map((row) => row.id)]);
      await client.query("COMMIT");
      processed = rows.length;
    } catch (error) {
      await client.query("ROLLBACK");
      log(`error: ${(error as Error).message}`);
    } finally {
      client.release();
    }
    // empty queue: ask again in a moment
    if (processed === 0) await sleep(200);
  }
}

// Redis Streams: reads up to 10 new jobs for this consumer of the group; after
// processing, XACK acknowledges and XDEL deletes the entry (XACK alone deletes
// nothing). With no new jobs, XAUTOCLAIM takes over entries pending for more than
// 30 s, from consumers that died.
async function consumeRedis(): Promise<void> {
  const redis = createClient({ url: process.env.REDIS_URL });
  await redis.connect();
  await redis.xGroupCreate("jobs", "workers", "0", { MKSTREAM: true })
    .catch(() => {}); // BUSYGROUP: the group already exists
  while (true) {
    const response = await redis.xReadGroup("workers", NAME,
      { key: "jobs", id: ">" }, { COUNT: 10, BLOCK: 1000 });
    let entries = response?.[0]?.messages ?? [];
    if (entries.length === 0) {
      const claimed = await redis.xAutoClaim("jobs", "workers", NAME, 30_000,
                                             "0-0", { COUNT: 10 });
      entries = claimed.messages.filter((entry) => entry !== null);
    }
    if (entries.length === 0) continue;
    // the whole batch in one transaction, a single COMMIT
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      for (const { message } of entries) {
        await processJob(client, JSON.parse(message.job));
      }
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    const ids = entries.map((entry) => entry.id);
    await redis.xAck("jobs", "workers", ids);
    await redis.xDel("jobs", ids);
  }
}

// RabbitMQ: the broker delivers up to 10 unacknowledged messages at a time (prefetch)
async function consumeRabbitmq(): Promise<void> {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  const channel = await connection.createChannel();
  await channel.assertQueue("jobs", { durable: true });
  await channel.prefetch(10);
  await channel.consume("jobs", async (message) => {
    if (!message) return;
    await processJob(db, JSON.parse(message.content.toString()));
    channel.ack(message);
  });
}

log(`${NAME} consuming from ${BACKEND}`);
if (BACKEND === "redis") await consumeRedis();
else if (BACKEND === "rabbitmq") await consumeRabbitmq();
else await consumePostgres();
