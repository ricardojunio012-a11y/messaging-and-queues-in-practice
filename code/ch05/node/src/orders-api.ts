import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import amqp from "amqplib";
import pg from "pg";

const PORT = Number(process.env.PORT ?? 8080);
const DUPLICATE_PUBLISH = process.env.DUPLICATE_PUBLISH === "yes";
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });

const log = (text: string) => console.log(`[orders-api] ${text}`);

// Publishes with broker confirmation. With DUPLICATE_PUBLISH, publishes twice
// with the same id, like a producer that repeats because it got no confirmation.
async function publish(messageId: string, orderId: string, email: string) {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  try {
    const channel = await connection.createConfirmChannel();
    await channel.assertQueue("emails", { durable: true });
    const body = Buffer.from(JSON.stringify({ orderId, to: email }));
    const times = DUPLICATE_PUBLISH ? 2 : 1;
    for (let i = 0; i < times; i++) {
      channel.sendToQueue("emails", body, {
        messageId, // the same id on every copy
        type: "SendConfirmationEmail",
        persistent: true,
      });
    }
    await channel.waitForConfirms();
    await channel.close();
  } finally {
    await connection.close();
  }
}

// With an Idempotency-Key, the same key returns the order already created
// instead of creating another one
async function createOrder(
  data: any,
  key: string | null,
): Promise<[number, object]> {
  const { rows } = await db.query(
    `INSERT INTO orders (id, idempotency_key, customer, email, total)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (idempotency_key) DO NOTHING
     RETURNING id`,
    [randomUUID(), key, data.customer, data.email, data.total ?? 0],
  );
  if (rows.length === 0) {
    const existing = await db.query(
      "SELECT id FROM orders WHERE idempotency_key = $1", [key]);
    const orderId = existing.rows[0].id;
    log(`key ${key} repeated: returning order ${orderId}`);
    return [202, { id: orderId, status: "accepted" }];
  }
  const orderId = rows[0].id;
  await publish(randomUUID(), orderId, data.email);
  return [202, { id: orderId, status: "accepted" }];
}

async function readBody(req: IncomingMessage): Promise<string> {
  let body = "";
  for await (const chunk of req) body += chunk;
  return body;
}

function respond(res: ServerResponse, status: number, data: object): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

const server = createServer(async (req, res) => {
  if (req.method !== "POST" || req.url !== "/orders") {
    return respond(res, 404, { error: "Route not found" });
  }
  const key = (req.headers["idempotency-key"] as string | undefined) ?? null;
  try {
    const [status, data] = await createOrder(JSON.parse(await readBody(req)), key);
    respond(res, status, data);
  } catch (error) {
    log(`error: ${(error as Error).message}`);
    respond(res, 503, { error: "Could not record the order." });
  }
});

server.listen(PORT, () => log(`listening on port ${PORT}`));
