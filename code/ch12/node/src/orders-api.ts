import { createServer, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { log as logFor, publish } from "./common.ts";

const PORT = Number(process.env.PORT ?? 8080);
const WRITE = process.env.WRITE ?? "dual"; // dual | outbox
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (message: string) => logFor("orders-api", message);

// Dual write: writes the order and, after the COMMIT, publishes the event. These are
// two writes to two systems: if the second fails, the first has already happened.
async function createWithDualWrite(orderId: string, data: any, event: object) {
  await db.query(
    "INSERT INTO orders (id, customer, email, total) VALUES ($1, $2, $3, $4)",
    [orderId, data.customer, data.email, data.total],
  );
  await publish([{ id: randomUUID(), type: "OrderCreated", data: event }]);
}

// Outbox: the order and the event are written in the same transaction, in the same
// database. Either both exist, or neither does. The relay publishes later.
async function createWithOutbox(orderId: string, data: any, event: object) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      "INSERT INTO orders (id, customer, email, total) VALUES ($1, $2, $3, $4)",
      [orderId, data.customer, data.email, data.total],
    );
    await client.query(
      "INSERT INTO outbox (event_id, type, payload) VALUES ($1, $2, $3)",
      [randomUUID(), "OrderCreated", event],
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
  const data = JSON.parse(body);
  const orderId = randomUUID();
  const event = { orderId, email: data.email, total: data.total };
  try {
    if (WRITE === "outbox") await createWithOutbox(orderId, data, event);
    else await createWithDualWrite(orderId, data, event);
    respond(res, 202, { id: orderId, status: "accepted" });
  } catch (error) {
    log(`order ${orderId.slice(0, 8)}: error: ${(error as Error).message}`);
    respond(res, 500, { error: "Failed to record the order." });
  }
});

server.listen(PORT, () => log(`listening on port ${PORT} (write: ${WRITE})`));
