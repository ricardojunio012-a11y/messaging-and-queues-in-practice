import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import amqp from "amqplib";
import kafkaJavascript from "@confluentinc/kafka-javascript";

const PORT = Number(process.env.PORT ?? 8080);
const SHIPPING_TIMEOUT_MS = Number(process.env.SHIPPING_TIMEOUT_MS ?? 2000);

const log = (text: string) => console.log(`[orders-api] ${text}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// RabbitMQ: one connection and one channel for the whole process
const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createChannel();
await channel.assertExchange("orders", "fanout", { durable: true });
await channel.assertQueue("shipping.quotes", { durable: true });

// Kafka: one producer for the whole process
const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const producer = kafka.producer();
await producer.connect();

type OrderCreated = { orderId: string; customer: string; email: string; total: number };

// Pub/sub and log: the same event goes to the RabbitMQ fanout exchange
// and to the Kafka topic, so we can compare the two models.
async function publishOrderCreated(event: OrderCreated): Promise<void> {
  const id = randomUUID();
  const body = Buffer.from(JSON.stringify(event));

  channel.publish("orders", "", body, {
    messageId: id,
    type: "OrderCreated",
    contentType: "application/json",
    persistent: true,
    headers: { version: 1 },
  });

  await producer.send({
    topic: "orders",
    messages: [{
      key: event.orderId, // orders with the same key go to the same partition
      value: body,
      headers: { id, type: "OrderCreated", version: "1" },
    }],
  });
}

// Request/reply: a reply queue exclusive to this process. The replies
// arrive through it and are matched to the requests by the correlation id.
const { queue: replyQueue } = await channel.assertQueue("", { exclusive: true });
const waiting = new Map<string, (reply: object) => void>();

await channel.consume(replyQueue, (message) => {
  if (!message) return;
  const { correlationId } = message.properties;
  const deliver = waiting.get(correlationId);
  if (deliver) deliver(JSON.parse(message.content.toString()));
  else log(`reply ${correlationId} arrived too late and was discarded`);
}, { noAck: true });

async function quoteShipping(zipCode: string): Promise<object | null> {
  const correlationId = randomUUID();
  const reply = new Promise<object>((deliver) => {
    waiting.set(correlationId, deliver);
  });
  channel.sendToQueue("shipping.quotes", Buffer.from(JSON.stringify({ zipCode })), {
    replyTo: replyQueue,
    correlationId,
    // the request expires if nobody picks it up in time
    expiration: String(SHIPPING_TIMEOUT_MS),
  });
  const timedOut = sleep(SHIPPING_TIMEOUT_MS).then(() => null);
  const result = await Promise.race([reply, timedOut]);
  waiting.delete(correlationId);
  return result;
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
  const url = new URL(req.url ?? "/", "http://localhost");
  if (req.method === "POST" && url.pathname === "/orders") {
    const data = JSON.parse(await readBody(req));
    const event = { orderId: randomUUID(), customer: data.customer, email: data.email,
      total: Number(data.total ?? 0) };
    await publishOrderCreated(event);
    log(`OrderCreated ${event.orderId} published`);
    return respond(res, 202, { id: event.orderId, status: "accepted" });
  }
  if (req.method === "GET" && url.pathname === "/shipping") {
    const quote = await quoteShipping(url.searchParams.get("zipCode") ?? "");
    if (quote) return respond(res, 200, quote);
    return respond(res, 504, { error: "Shipping service did not respond in time." });
  }
  respond(res, 404, { error: "Route not found" });
});

server.listen(PORT, () => log(`listening on port ${PORT}`));
