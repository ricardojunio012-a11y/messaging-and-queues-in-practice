import { createServer, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import pg from "pg";

const PORT = Number(process.env.PORT ?? 8080);
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (message: string) => console.log(`[orders-api] ${message}`);

// The order and the event in the same transaction, as in chapter 12. The outbox
// columns follow the format of Debezium's Outbox Event Router: aggregatetype chooses
// the topic, aggregateid becomes the message key, and the id goes in the "id" header.
async function createOrder(orderId: string, data: any): Promise<void> {
  const event = { orderId, email: data.email, total: data.total };
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      "INSERT INTO orders (id, customer, email, total) VALUES ($1, $2, $3, $4)",
      [orderId, data.customer, data.email, data.total],
    );
    await client.query(
      `INSERT INTO outbox (id, aggregatetype, aggregateid, type, payload)
       VALUES ($1, 'order', $2, 'OrderCreated', $3)`,
      [randomUUID(), orderId, event],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

function respond(res: ServerResponse, status: number, data: object): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

const server = createServer(async (req, res) => {
  if (req.method !== "POST" || req.url !== "/orders") {
    return respond(res, 404, { error: "Route not found" });
  }
  let body = "";
  for await (const chunk of req) body += chunk;
  const orderId = randomUUID();
  try {
    await createOrder(orderId, JSON.parse(body));
    respond(res, 202, { id: orderId, status: "accepted" });
  } catch (error) {
    log(`order ${orderId.slice(0, 8)}: error: ${(error as Error).message}`);
    respond(res, 500, { error: "Failed to record the order." });
  }
});

server.listen(PORT, () => log(`listening on port ${PORT}`));
