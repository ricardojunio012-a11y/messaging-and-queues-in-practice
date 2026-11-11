import { jetstream } from "@nats-io/jetstream";
import { CONSUMER, STREAM, connectNats, sleep, setUpJetStream } from "./common.ts";
import type { Order } from "./common.ts";

const PROCESSING_MS = Number(process.env.PROCESSING_MS ?? 0);
const log = (text: string) => console.log(`[notifications] ${text}`);

const nc = await connectNats("notifications");
await setUpJetStream(nc);
const js = jetstream(nc);
const consumer = await js.consumers.get(STREAM, CONSUMER);
let sent = 0;
log("consuming orders.created");

// Each message is acknowledged (ack) only after the email goes out. An invalid order
// gets a nak: it comes back in 1 second, up to the consumer's limit of 3 deliveries.
for await (const message of await consumer.consume({ max_messages: 20 })) {
  const order = message.json<Order>();
  if (order.total < 0) {
    log(`order ${order.id} invalid, delivery ${message.info.deliveryCount}: nak`);
    message.nak(1000);
    continue;
  }
  await sleep(PROCESSING_MS);
  sent++;
  const repeated = message.info.deliveryCount > 1 ? " (redelivery)" : "";
  if (sent % 50 === 0 || repeated) {
    log(`email for order ${order.id}${repeated}; total ${sent}`);
  }
  message.ack();
}
