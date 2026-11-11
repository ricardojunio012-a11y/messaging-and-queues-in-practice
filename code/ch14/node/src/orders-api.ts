import { createServer, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { db, connect, log as logFor, publish } from "./common.ts";

const PORT = Number(process.env.PORT ?? 8080);
const log = (message: string) => logFor("orders-api", message);

function respond(res: ServerResponse, status: number, data: object): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

await connect();
const server = createServer(async (req, res) => {
  if (req.method !== "POST" || req.url !== "/orders") {
    return respond(res, 404, { error: "Route not found" });
  }
  let body = "";
  for await (const chunk of req) body += chunk;
  const { sku, amount } = JSON.parse(body);
  const ctx = { orderId: randomUUID(), sku, amount };
  try {
    // Simplification: in production, the order and the event would go through an
    // outbox (ch. 12)
    await db.query("INSERT INTO orders (id, sku, amount) VALUES ($1, $2, $3)",
                   [ctx.orderId, sku, amount]);
    await publish("orders-api", "order.created", ctx);
    respond(res, 202, { id: ctx.orderId, status: "pending" });
  } catch (error) {
    log(`error: ${(error as Error).message}`);
    respond(res, 500, { error: "Failed to record the order." });
  }
});

server.listen(PORT, () => log(`listening on port ${PORT}`));
