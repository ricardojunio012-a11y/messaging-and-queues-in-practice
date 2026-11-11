import amqp, { type ConsumeMessage } from "amqplib";
import pg from "pg";
import { sleep, QUEUE, log as logFor } from "./common.ts";

const db = new pg.Client({ connectionString: process.env.DATABASE_URL });
const log = (message: string) => logFor("notifications", message);

// Inbox (chapter 5): the message and the effect in the same transaction. A second
// delivery of the same message, from the relay or from the broker, is dropped.
async function processMessage(channel: amqp.Channel, message: ConsumeMessage) {
  const messageId: string = message.properties.messageId;
  const { orderId } = JSON.parse(message.content.toString());
  await db.query("INSERT INTO deliveries (message_id) VALUES ($1)", [messageId]);
  await db.query("BEGIN");
  const inbox = await db.query(
    "INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING",
    [messageId],
  );
  if (inbox.rowCount === 0) {
    await db.query("ROLLBACK");
    log(`message ${messageId.slice(0, 8)} repeated: dropped`);
  } else {
    await db.query("INSERT INTO sent_emails (order_id, message_id) VALUES ($1, $2)",
                   [orderId, messageId]);
    await db.query("COMMIT");
  }
  channel.ack(message);
}

await db.connect();
// The consumer tries to reconnect if the broker goes down (the lab stops RabbitMQ)
while (true) {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL!);
    connection.on("error", () => {}); // the drop is handled by the "close" event
    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    await channel.prefetch(1);
    await channel.consume(QUEUE, (message) => {
      if (message) processMessage(channel, message);
    });
    log(`consuming the "${QUEUE}" queue`);
    await new Promise((closed) => connection.on("close", closed));
    log("connection to the broker lost");
  } catch (error) {
    log(`broker unavailable: ${(error as Error).message}`);
  }
  await sleep(2000);
}
