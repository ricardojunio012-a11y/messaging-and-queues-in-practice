import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import pg from "pg";

const PORT = Number(process.env.PORT ?? 8080);
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });

const log = (message: string) => console.log(`[orders-api] ${message}`);

type NewOrder = { customer?: string; email?: string; total?: number };

// Producer: the order and the message go into the same transaction.
// Either both are saved, or neither is.
async function createOrder(data: NewOrder): Promise<[number, object]> {
  if (typeof data.email !== "string" || typeof data.customer !== "string") {
    return [400, { error: "Provide customer and email." }];
  }
  const orderId = randomUUID();
  const message = {
    id: randomUUID(),
    type: "SendConfirmationEmail",
    payload: { orderId, to: data.email, subject: "We received your order" },
  };

  const connection = await db.connect();
  try {
    await connection.query("BEGIN");
    await connection.query(
      "INSERT INTO orders (id, customer, email, total) VALUES ($1, $2, $3, $4)",
      [orderId, data.customer, data.email, data.total ?? 0],
    );
    await connection.query(
      "INSERT INTO messages (id, queue, type, payload) VALUES ($1, 'emails', $2, $3)",
      [message.id, message.type, JSON.stringify(message.payload)],
    );
    await connection.query("COMMIT");
  } catch (error) {
    await connection.query("ROLLBACK").catch(() => {});
    log(`order rejected: ${(error as Error).message}`);
    return [503, { error: "Database unavailable. Try again." }];
  } finally {
    connection.release();
  }
  log(`order ${orderId} accepted; message ${message.id} in the "emails" queue`);
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
  if (req.method === "POST" && req.url === "/orders") {
    const [status, data] = await createOrder(JSON.parse(await readBody(req)));
    return respond(res, status, data);
  }
  if (req.method === "GET" && req.url === "/health") {
    return respond(res, 200, { service: "orders-api" });
  }
  respond(res, 404, { error: "Route not found" });
});

server.listen(PORT, () => log(`listening on port ${PORT}`));
