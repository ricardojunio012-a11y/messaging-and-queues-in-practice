import { hostname } from "node:os";
import pg from "pg";

// Consume mode: status | transaction | lease
const CONSUME_MODE = process.env.CONSUME_MODE ?? "status";
const LEASE_S = Number(process.env.LEASE_S ?? 15);
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 200);
// Attempt limit of the lease mode: after it, the message stays parked
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS ?? 5);
const CONSUMER = hostname();
const QUEUE = "emails";

type Message = {
  id: string;
  attempts: number;
  payload: { orderId: string; to: string; subject: string };
};

// Two connections: the queue's and the "email provider's". Sending the email
// happens outside the queue transaction, as it would with a real provider.
const queue = new pg.Client({ connectionString: process.env.DATABASE_URL });
const provider = new pg.Client({ connectionString: process.env.DATABASE_URL });

const log = (text: string) => console.log(`[notifications ${CONSUMER}] ${text}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Taking with a status: marks the row and commits. If the consumer dies
// after that, the message stays "processing" forever.
const SQL_STATUS = `
  UPDATE messages SET status = 'processing', attempts = attempts + 1
   WHERE id = (SELECT id FROM messages
                WHERE queue = $1 AND status = 'pending'
                ORDER BY created_at
                LIMIT 1 FOR UPDATE SKIP LOCKED)
  RETURNING id, attempts, payload`;

// Taking with a lease: the message becomes invisible for a few seconds.
// If it is not deleted by then, it becomes available again, up to MAX_ATTEMPTS times.
const SQL_LEASE = `
  UPDATE messages SET visible_at = now() + $2::int * interval '1 second',
                      attempts = attempts + 1
   WHERE id = (SELECT id FROM messages
                WHERE queue = $1 AND visible_at <= now() AND attempts < $3
                ORDER BY visible_at
                LIMIT 1 FOR UPDATE SKIP LOCKED)
  RETURNING id, attempts, payload`;

// Taking with a transaction: the row stays locked until the COMMIT.
const SQL_TRANSACTION = `
  SELECT id, attempts, payload FROM messages
   WHERE queue = $1 AND status = 'pending'
   ORDER BY created_at
   LIMIT 1 FOR UPDATE SKIP LOCKED`;

async function sendEmail(message: Message): Promise<void> {
  const { orderId, to } = message.payload;
  const { id, attempts } = message;
  log(`message ${id} (attempt ${attempts}): sending to ${to}`);
  await sleep(LATENCY_MS);
  // Simulated poison message: the provider always refuses this address
  if (to.startsWith("poison")) throw new Error("address refused by the provider");
  await provider.query(
    `INSERT INTO sent_emails (order_id, message_id, consumer)
     VALUES ($1, $2, $3)`,
    [orderId, message.id, CONSUMER],
  );
  log(`message ${message.id}: email sent`);
}

// Tries to process one message. Returns false if the queue was empty.
async function consumeOne(): Promise<boolean> {
  if (CONSUME_MODE === "transaction") {
    await queue.query("BEGIN");
    const { rows } = await queue.query<Message>(SQL_TRANSACTION, [QUEUE]);
    if (rows.length === 0) {
      await queue.query("COMMIT");
      return false;
    }
    await sendEmail(rows[0]);
    await queue.query("DELETE FROM messages WHERE id = $1", [rows[0].id]);
    await queue.query("COMMIT");
    return true;
  }

  const { rows } = CONSUME_MODE === "lease"
    ? await queue.query<Message>(SQL_LEASE, [QUEUE, LEASE_S, MAX_ATTEMPTS])
    : await queue.query<Message>(SQL_STATUS, [QUEUE]);
  if (rows.length === 0) return false;
  try {
    await sendEmail(rows[0]);
  } catch (error) {
    // No DELETE: in lease mode, the message comes back when the lease expires
    log(`message ${rows[0].id}: failed (${(error as Error).message})`);
    return true;
  }
  // The acknowledgement: only now does the message leave the queue
  await queue.query("DELETE FROM messages WHERE id = $1", [rows[0].id]);
  return true;
}

await queue.connect();
await provider.connect();
log(`consuming the "${QUEUE}" queue in ${CONSUME_MODE} mode`);
while (true) {
  const consumed = await consumeOne();
  if (!consumed) await sleep(500); // empty queue: wait before asking again
}
