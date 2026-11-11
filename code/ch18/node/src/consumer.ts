import { connect, db, log as write, sleep } from "./common.ts";

const log = (text: string) => write("consumer", text);

// Consumes the quorum queue with manual ack. If the connection's node goes down, the
// unacked messages go back to the queue and are delivered again, to the same or to
// another consumer: the "deliveries" column counts how many times each order arrived.
async function consume(): Promise<void> {
  const connection = await connect("consumer");
  const channel = await connection.createChannel();
  channel.on("error", () => {});
  await channel.prefetch(20);
  await channel.consume("inventory.reserve", async (message) => {
    if (!message) return;
    const { orderId } = JSON.parse(message.content.toString());
    await db.query(
      `INSERT INTO received (order_id) VALUES ($1)
       ON CONFLICT (order_id) DO UPDATE SET deliveries = received.deliveries + 1`,
      [orderId],
    );
    channel.ack(message);
  });
  await new Promise((dropped) => connection.on("close", dropped));
}

while (true) {
  try {
    await consume();
    log("connection lost; reconnecting");
  } catch (error) {
    log(`error: ${(error as Error).message}; reconnecting`);
  }
  await sleep(1000);
}
