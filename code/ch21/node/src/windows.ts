import kafkaJavascript from "@confluentinc/kafka-javascript";

const MINUTE = 60_000;
const TOLERANCE = Number(process.env.TOLERANCE_MS ?? 30_000);
const log = (text: string) => console.log(`[windows] ${text}`);

// ① State: count per window (start, in ms) and per region
const counts = new Map<number, Map<string, number>>();
let streamTime = 0; // ② the latest event time seen so far
let dropped = 0;

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
});
const producer = kafka.producer();
await producer.connect();

const hour = (ms: number) => new Date(ms).toISOString().slice(11, 16);

// ③ A window closes when the stream time passes its end plus the tolerance.
// Once closed, it is published once and forgotten.
async function closeWindows(): Promise<void> {
  for (const [start, byRegion] of [...counts].sort(([a], [b]) => a - b)) {
    if (start + MINUTE + TOLERANCE > streamTime) break;
    for (const [region, orders] of byRegion) {
      await producer.send({ topic: "windows-manual", messages: [{ key: region,
        value: JSON.stringify({ window: hour(start), region, orders }) }] });
    }
    log(`window ${hour(start)} closed (dropped so far: ${dropped})`);
    counts.delete(start);
  }
}

// ④ Each order falls into the 1-minute window of its event time, not of the time
// it arrived. If that window has already closed, the order is too late and is dropped.
async function count(order: { region: string; ts: number }): Promise<void> {
  const start = order.ts - (order.ts % MINUTE);
  if (start + MINUTE + TOLERANCE <= streamTime) {
    dropped++;
    return;
  }
  const byRegion = counts.get(start) ?? new Map<string, number>();
  byRegion.set(order.region, (byRegion.get(order.region) ?? 0) + 1);
  counts.set(start, byRegion);
  streamTime = Math.max(streamTime, order.ts);
  await closeWindows();
}

const consumer = kafka.consumer({ kafkaJS: { groupId: "windows-manual",
                                             fromBeginning: true } });
await consumer.connect();
await consumer.subscribe({ topic: "orders" });
log("counting orders per minute and region");
await consumer.run({
  eachMessage: async ({ message }) => count(JSON.parse(message.value!.toString())),
});
