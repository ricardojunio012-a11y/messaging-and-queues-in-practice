import { randomUUID } from "node:crypto";
import amqp from "amqplib";
import pg from "pg";
import { createClient } from "redis";

const BACKEND = process.env.BACKEND ?? "postgres";
const N = Number(process.env.N ?? 20000);
const BATCH = 100;

type Job = { id: string; orderId: number; enqueuedAt: string };

// PostgreSQL: the queue is a table; each batch is one INSERT of 100 rows
async function enqueuePostgres(db: pg.Client, jobs: Job[]) {
  await db.query(
    `INSERT INTO job_queue (job_id, payload)
     SELECT (j->>'id')::uuid, j FROM jsonb_array_elements($1::jsonb) AS j`,
    [JSON.stringify(jobs)],
  );
}

// Redis Streams: one XADD per job, sent together in a pipeline
async function enqueueRedis(redis: ReturnType<typeof createClient>, jobs: Job[]) {
  const pipeline = redis.multi();
  for (const job of jobs) pipeline.xAdd("jobs", "*", { job: JSON.stringify(job) });
  await pipeline.execAsPipeline();
}

// RabbitMQ: publishes the batch as persistent messages and waits for the confirms
async function enqueueRabbitmq(channel: amqp.ConfirmChannel, jobs: Job[]) {
  for (const job of jobs) {
    channel.sendToQueue("jobs", Buffer.from(JSON.stringify(job)), { persistent: true });
  }
  await channel.waitForConfirms();
}

let enqueue: (jobs: Job[]) => Promise<void>;
let close: () => Promise<unknown>;
if (BACKEND === "redis") {
  const redis = createClient({ url: process.env.REDIS_URL });
  await redis.connect();
  enqueue = (jobs) => enqueueRedis(redis, jobs);
  close = () => redis.quit();
} else if (BACKEND === "rabbitmq") {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  const channel = await connection.createConfirmChannel();
  await channel.assertQueue("jobs", { durable: true });
  enqueue = (jobs) => enqueueRabbitmq(channel, jobs);
  close = () => connection.close();
} else {
  const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
  await db.connect();
  enqueue = (jobs) => enqueuePostgres(db, jobs);
  close = () => db.end();
}

const start = Date.now();
for (let i = 0; i < N; i += BATCH) {
  const jobs = Array.from({ length: Math.min(BATCH, N - i) }, (_, k) => ({
    id: randomUUID(), orderId: i + k + 1, enqueuedAt: new Date().toISOString(),
  }));
  await enqueue(jobs);
}
const ms = Date.now() - start;
console.log(`${N} jobs enqueued in ${BACKEND} in ${ms} ms ` +
            `(${Math.round(N / (ms / 1000))} jobs/s)`);
await close();
