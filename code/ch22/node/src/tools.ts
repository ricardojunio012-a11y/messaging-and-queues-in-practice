// Lab tool for chapter 22.
//   status    -> messages in the stream and in the DLQ, and the state of the
//                notifications consumer
//   replay    -> rereads the ORDERS stream from the start with a new consumer, and
//                counts
//   kiosk [N] -> publishes N orders to the kiosk's NATS (default 50)
//   sync      -> counts the orders on the kiosk and the ones that reached the cloud
import { connect } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { CONSUMER, DLQ, STREAM, connectNats, setUpJetStream } from "./common.ts";

const nc = await connectNats("tools");
const jsm = await setUpJetStream(nc);

async function status(): Promise<void> {
  for (const name of [STREAM, DLQ]) {
    const { state } = await jsm.streams.info(name);
    console.log(`  stream ${name}: ${state.messages} messages`);
  }
  const c = await jsm.consumers.info(STREAM, CONSUMER);
  console.log(`  consumer ${CONSUMER}: delivered ${c.delivered.stream_seq}, ` +
    `acked up to ${c.ack_floor.stream_seq}, pending ${c.num_pending}, ` +
    `awaiting ack ${c.num_ack_pending}, redelivered ${c.num_redelivered}`);
}

async function replay(): Promise<void> {
  // Without a name, the client creates an ordered, ephemeral consumer that reads
  // from the start
  const consumer = await jetstream(nc).consumers.get(STREAM);
  const { state } = await jsm.streams.info(STREAM);
  let read = 0;
  const customers = new Set<string>();
  for await (const message of await consumer.consume()) {
    customers.add(message.json<{ customer: string }>().customer);
    if (++read >= state.messages) break;
  }
  console.log(`  replay: ${read} messages reread from the start, ` +
    `from ${customers.size} customers`);
}

// The kiosk keeps the orders in its own JetStream ("kiosk" domain), and the cloud
// copies them with a stream whose source is the kiosk's: the copy only moves while
// the leaf node is connected, and resumes where it stopped when the network returns
const STORE = "ORDERS_STORE";
const CLOUD = "ORDERS_STORES";

async function setUpKiosk() {
  const kiosk = await connect({ servers: process.env.KIOSK_URL, name: "tools" });
  const jsmStore = await jetstreamManager(kiosk);
  await jsmStore.streams.add({ name: STORE, subjects: ["store.orders"] })
    .catch(() => {});
  await jsm.streams.add({ name: CLOUD, sources: [{ name: STORE, domain: "kiosk" }] })
    .catch(() => {});
  return { kiosk, jsmStore };
}

async function publishToKiosk(): Promise<void> {
  const { kiosk } = await setUpKiosk();
  const js = jetstream(kiosk);
  const n = Number(process.argv[3] ?? 50);
  const batch = Date.now().toString(36);
  for (let i = 1; i <= n; i++) {
    const order = { id: `${batch}-${i}`, total: 100 + i };
    await js.publish("store.orders", JSON.stringify(order), { msgID: order.id });
  }
  console.log(`[kiosk] ${n} orders stored on the kiosk`);
  await kiosk.drain();
}

async function sync(): Promise<void> {
  const { kiosk, jsmStore } = await setUpKiosk();
  const store = (await jsmStore.streams.info(STORE)).state.messages;
  const cloud = (await jsm.streams.info(CLOUD)).state.messages;
  console.log(`  kiosk ${STORE}: ${store} orders; cloud ${CLOUD}: ${cloud} orders`);
  await kiosk.drain();
}

const commands: Record<string, () => Promise<void>> = {
  status, replay, kiosk: publishToKiosk, sync };
const command = commands[process.argv[2] ?? ""];
if (command) await command();
else console.log("use: status | replay | kiosk [N] | sync");
await nc.drain();
