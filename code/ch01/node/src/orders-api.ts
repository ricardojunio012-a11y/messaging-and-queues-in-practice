import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";

type Order = { id: string; customer: string; email: string; total: number };

const PORT = Number(process.env.PORT ?? 8080);
const NOTIFICATIONS_URL = process.env.NOTIFICATIONS_URL ?? "http://localhost:8081";
const MODE = process.env.MODE ?? "direct"; // direct | timeout | memory-queue
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS ?? 1000);
const ATTEMPTS = Number(process.env.ATTEMPTS ?? 3);

const orders = new Map<string, Order>();
const inMemoryQueue: Order[] = [];

const log = (message: string) => console.log(`[orders-api] ${message}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Calls the notifications service. Without timeoutMs, it waits as long as it takes.
async function sendEmail(order: Order, timeoutMs?: number): Promise<void> {
  const response = await fetch(`${NOTIFICATIONS_URL}/emails`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      orderId: order.id,
      to: order.email,
      subject: "We received your order",
    }),
    signal: timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined,
  });
  if (!response.ok) throw new Error(`notifications responded HTTP ${response.status}`);
}

function describeError(error: unknown): string {
  const e = error as Error & { cause?: { code?: string } };
  if (e.name === "TimeoutError") return "timeout";
  return e.cause?.code ?? e.message;
}

async function sendWithRetries(order: Order): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await sendEmail(order, TIMEOUT_MS);
      return;
    } catch (error) {
      const reason = describeError(error);
      log(`order ${order.id}: attempt ${attempt} failed (${reason})`);
      if (attempt >= ATTEMPTS) throw error;
      await sleep(100);
    }
  }
}

// Consumer of the in-memory queue: runs inside the same process as the API.
async function processQueue(): Promise<void> {
  while (true) {
    const order = inMemoryQueue.shift();
    if (!order) {
      await sleep(200);
      continue;
    }
    try {
      await sendEmail(order, TIMEOUT_MS);
      log(`email for order ${order.id} sent (pending: ${inMemoryQueue.length})`);
    } catch (error) {
      const reason = describeError(error);
      log(`email for order ${order.id} failed (${reason}); back to the queue`);
      inMemoryQueue.push(order);
      await sleep(1000);
    }
  }
}

async function createOrder(data: Partial<Order>): Promise<[number, object]> {
  if (typeof data.email !== "string" || typeof data.customer !== "string") {
    return [400, { error: "Provide customer and email." }];
  }
  const order: Order = {
    id: randomUUID(),
    customer: data.customer,
    email: data.email,
    total: Number(data.total ?? 0),
  };
  orders.set(order.id, order);

  if (MODE === "memory-queue") {
    inMemoryQueue.push(order);
    return [202, { id: order.id, status: "accepted" }];
  }

  try {
    if (MODE === "timeout") await sendWithRetries(order);
    else await sendEmail(order);
    log(`order ${order.id} confirmed`);
    return [201, { id: order.id, status: "confirmed" }];
  } catch (error) {
    orders.delete(order.id);
    log(`order ${order.id} rejected (${describeError(error)})`);
    return [503, { error: "Could not send the confirmation. Order not completed." }];
  }
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
    return respond(res, 200, {
      service: "orders-api",
      mode: MODE,
      pendingQueue: inMemoryQueue.length,
    });
  }
  respond(res, 404, { error: "Route not found" });
});

server.listen(PORT, () => log(`listening on port ${PORT} (mode ${MODE})`));
if (MODE === "memory-queue") processQueue();
