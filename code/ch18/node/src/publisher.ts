import { connect, db, log as write, sleep } from "./common.ts";

const log = (text: string) => write("publisher", text);
let number = 0;

// Publishes one order at a time and counts it as confirmed only after the broker's ack.
// In a quorum queue, the ack means that a majority of the nodes stored the message.
async function publishForever(): Promise<void> {
  const connection = await connect("publisher");
  const channel = await connection.createConfirmChannel();
  channel.on("error", () => {});
  while (true) {
    const orderId = `o-${process.pid}-${++number}`;
    channel.publish("orders", "order.created", Buffer.from(JSON.stringify({ orderId })),
                   { persistent: true, messageId: orderId });
    await channel.waitForConfirms();
    await db.query("INSERT INTO confirmed (order_id) VALUES ($1)", [orderId]);
    await sleep(20);
  }
}

while (true) {
  try {
    await publishForever();
  } catch (error) {
    log(`connection lost (${(error as Error).message}); reconnecting`);
    await sleep(1000);
  }
}
