import { createServer, type IncomingMessage, type ServerResponse } from "node:http";

const PORT = Number(process.env.PORT ?? 8081);
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 0);

// How many emails each order received. Makes losses and duplicates visible.
const sentPerOrder = new Map<string, number>();
let totalSent = 0;

const log = (message: string) => console.log(`[notifications] ${message}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
  if (req.method === "POST" && req.url === "/emails") {
    const email = JSON.parse(await readBody(req)) as { orderId: string; to: string };
    // Simulates a slow email provider. The email is sent even if
    // the caller has given up waiting for the response.
    await sleep(LATENCY_MS);
    totalSent++;
    const previous = sentPerOrder.get(email.orderId) ?? 0;
    sentPerOrder.set(email.orderId, previous + 1);
    log(`email sent to ${email.to} (order ${email.orderId})`);
    return respond(res, 200, { status: "sent" });
  }
  if (req.method === "GET" && req.url === "/emails") {
    return respond(res, 200, {
      total: totalSent,
      uniqueOrders: sentPerOrder.size,
      duplicates: totalSent - sentPerOrder.size,
    });
  }
  respond(res, 404, { error: "Route not found" });
});

server.listen(PORT, () => {
  log(`listening on port ${PORT} (latency ${LATENCY_MS} ms)`);
});
