import { createServer, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import kafkaJavascript from "@confluentinc/kafka-javascript";

const PORT = Number(process.env.PORT ?? 8080);
const KEY = process.env.KEY ?? "random"; // random | order

const log = (message: string) => console.log(`[orders-api] ${message}`);

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const producer = kafka.producer();
await producer.connect();

// The life cycle of an order: each event carries the order's version
const LIFE_CYCLE = [
  { type: "OrderCreated", status: "created" },
  { type: "OrderPaid", status: "paid" },
  { type: "OrderShipped", status: "shipped" },
];

async function publishOrderLifeCycle(orderId: string): Promise<void> {
  for (const [index, step] of LIFE_CYCLE.entries()) {
    const event = { orderId, ...step, version: index + 1 };
    // With the order key, the three events land in the same partition, in order.
    // With a random key, each event can go to a different partition.
    const key = KEY === "order" ? orderId : randomUUID();
    await producer.send({
      topic: "order-events",
      messages: [{ key, value: JSON.stringify(event) }],
    });
  }
}

function respond(res: ServerResponse, status: number, data: object): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

const server = createServer(async (req, res) => {
  if (req.method === "POST" && req.url === "/orders") {
    for await (const _ of req); // discards the body: only the order id matters here
    const orderId = randomUUID();
    await publishOrderLifeCycle(orderId);
    log(`order ${orderId.slice(0, 8)}: 3 events published (key ${KEY})`);
    return respond(res, 202, { id: orderId, status: "accepted" });
  }
  respond(res, 404, { error: "Route not found" });
});

server.listen(PORT, () => log(`listening on port ${PORT}`));
