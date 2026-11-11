import { jetstream } from "@nats-io/jetstream";
import { connectNats, setUpJetStream } from "./common.ts";
import type { Order } from "./common.ts";

const N = Number(process.env.N ?? 100);
const REPEAT = process.env.REPEAT === "1"; // publishes each order twice
const DEDUP = process.env.DEDUP !== "0"; // sends the Nats-Msg-Id
const POISON = process.env.POISON === "1"; // includes an order the consumer rejects

const nc = await connectNats("producer");
await setUpJetStream(nc);
const js = jetstream(nc);

// Publishes an OrderCreated. The order id goes as Nats-Msg-Id: if the same id arrives
// again within the deduplication window, the server confirms without storing a copy.
async function publish(order: Order): Promise<boolean> {
  const ack = await js.publish("orders.created", JSON.stringify(order),
    DEDUP ? { msgID: order.id } : {});
  return ack.duplicate;
}

const batch = Date.now().toString(36);
let stored = 0;
let duplicates = 0;
for (let i = 1; i <= N; i++) {
  const order = { id: `${batch}-${i}`, customer: `customer-${i % 7}`,
                  total: POISON && i === 1 ? -1 : 100 + i };
  for (let round = 0; round < (REPEAT ? 2 : 1); round++) {
    if (await publish(order)) duplicates++;
    else stored++;
  }
}
console.log(`[producer] ${stored} stored, ${duplicates} duplicates dropped`);
await nc.drain();
