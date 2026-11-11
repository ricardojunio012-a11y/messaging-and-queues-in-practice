import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import amqp, { type ConfirmChannel } from "amqplib";

const PORT = Number(process.env.PORT ?? 8080);
const CONFIRMATION = (process.env.CONFIRMATION ?? "yes") === "yes";
const PERSISTENT = (process.env.PERSISTENT ?? "yes") === "yes";

const log = (text: string) => console.log(`[orders-api] ${text}`);

// To keep the example simple, each publish opens and closes its own connection.
// In production, reuse the connection and handle reconnection.
async function publish(orderId: string, email: string): Promise<void> {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  try {
    const channel = CONFIRMATION
      ? await connection.createConfirmChannel()
      : await connection.createChannel();
    await channel.assertQueue("emails", { durable: true });
    const body = Buffer.from(JSON.stringify({ orderId, to: email }));
    channel.sendToQueue("emails", body, {
      messageId: randomUUID(),
      type: "SendConfirmationEmail",
      persistent: PERSISTENT, // written to disk by the broker
    });
    // With publisher confirms, waits for the broker to say whether it accepted (ack)
    // or refused (nack). Without confirmation, the function ends without knowing
    // what happened to the message.
    if (CONFIRMATION) await (channel as ConfirmChannel).waitForConfirms();
    // closes the channel before the connection, so nothing that was sent is lost
    await channel.close();
  } finally {
    await connection.close();
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
  if (req.method !== "POST" || req.url !== "/orders") {
    return respond(res, 404, { error: "Route not found" });
  }
  const data = JSON.parse(await readBody(req));
  const orderId = randomUUID();
  try {
    await publish(orderId, data.email);
    return respond(res, 202, { id: orderId, status: "accepted" });
  } catch (error) {
    log(`order ${orderId} rejected: ${(error as Error).message}`);
    return respond(res, 503, { error: "The broker did not accept the message." });
  }
});

server.listen(PORT, () => {
  log(`listening on port ${PORT}`);
  log(`confirmation: ${CONFIRMATION}, persistent: ${PERSISTENT}`);
});
