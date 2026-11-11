import { createServer, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { context, propagation, SpanKind } from "@opentelemetry/api";
import amqp from "amqplib";
import { shutdownTelemetry, log, tracer } from "./telemetry.ts";

const PORT = Number(process.env.PORT ?? 8080);

const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createConfirmChannel();
await channel.assertExchange("orders", "fanout", { durable: true });

// Publishes the event inside a PRODUCER span and injects the trace context into the
// message headers: that is how the consumer continues the same trace.
async function publishOrderCreated(order: object, orderId: string): Promise<void> {
  // ① The PRODUCER span wraps the publish
  await tracer.startActiveSpan("publish orders", {
    kind: SpanKind.PRODUCER,
    attributes: {
      "messaging.system": "rabbitmq",
      "messaging.operation.type": "send",
      "messaging.destination.name": "orders",
    },
  }, async (span) => {
    const headers: Record<string, string> = {};
    propagation.inject(context.active(), headers); // ② writes "traceparent"
    // ③ The message carries the context in the headers, and the correlation id
    channel.publish("orders", "", Buffer.from(JSON.stringify(order)), {
      messageId: randomUUID(),
      correlationId: orderId, // ties together every message of this order
      type: "OrderCreated",
      timestamp: Math.floor(Date.now() / 1000), // used by the age alert
      persistent: true,
      headers,
    });
    await channel.waitForConfirms();
    log("OrderCreated published", { order_id: orderId, correlation_id: orderId });
    span.end();
  });
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
  // The span of the HTTP request is the root of the trace
  await tracer.startActiveSpan("POST /orders", { kind: SpanKind.SERVER }, async (span) => {
    const orderId = randomUUID();
    await publishOrderCreated({ orderId, ...JSON.parse(body) }, orderId);
    respond(res, 202, { id: orderId, traceId: span.spanContext().traceId });
    span.end();
  });
});

process.on("SIGTERM", async () => {
  await shutdownTelemetry();
  process.exit(0);
});
server.listen(PORT, () => log(`listening on port ${PORT}`));
