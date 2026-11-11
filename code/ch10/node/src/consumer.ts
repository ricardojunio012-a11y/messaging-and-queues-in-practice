import { context, propagation, ROOT_CONTEXT, SpanKind } from "@opentelemetry/api";
import amqp, { type ConsumeMessage } from "amqplib";
import { shutdownTelemetry, log, tracer } from "./telemetry.ts";

const NAME = process.env.NAME ?? "notifications"; // notifications | loyalty
const LATENCY_MS = Number(process.env.LATENCY_MS ?? 50);
const TASK = NAME === "notifications" ? "send email" : "compute points";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Extracts the context the producer wrote into the headers and opens the CONSUMER
// span as its child: processing shows up in the same trace as the HTTP request.
async function processMessage(channel: amqp.Channel,
                              message: ConsumeMessage): Promise<void> {
  // ④ Extracts from the headers the context the producer wrote
  const producerContext = propagation.extract(ROOT_CONTEXT,
                                              message.properties.headers ?? {});
  // ⑤ Opens the CONSUMER span as a child of the producer's context
  await tracer.startActiveSpan("process orders", {
    kind: SpanKind.CONSUMER,
    attributes: {
      "messaging.system": "rabbitmq",
      "messaging.operation.type": "process",
      "messaging.destination.name": "orders",
      "messaging.message.id": message.properties.messageId,
    },
  }, producerContext, async (span) => {
    const orderId: string = message.properties.correlationId;
    // The work itself, in a child span: this is what shows who is slow
    await tracer.startActiveSpan(TASK, async (task) => {
      await sleep(LATENCY_MS);
      task.end();
    });
    log(`${TASK}: done`, { order_id: orderId, correlation_id: orderId });
    channel.ack(message);
    span.end();
  });
}

const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createChannel();
await channel.assertExchange("orders", "fanout", { durable: true });
await channel.assertQueue(NAME, { durable: true });
await channel.bindQueue(NAME, "orders", "");
await channel.prefetch(1);
await channel.consume(NAME, (message) => {
  if (message) context.with(ROOT_CONTEXT, () => processMessage(channel, message));
});

process.on("SIGTERM", async () => {
  await shutdownTelemetry();
  process.exit(0);
});
log(`consuming the "${NAME}" queue (${LATENCY_MS} ms per message)`);
