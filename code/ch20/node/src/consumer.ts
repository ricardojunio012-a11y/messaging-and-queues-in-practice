import { hostname } from "node:os";
import kafkaJavascript from "@confluentinc/kafka-javascript";

const TOPIC = process.env.TOPIC!;
const PROCESSING_MS = Number(process.env.PROCESSING_MS ?? 200);
const NAME = hostname();
const log = (text: string) => console.log(`[consumer ${NAME}] ${text}`);
const wait = (ms: number) => new Promise((ready) => setTimeout(ready, ms));

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: { brokers: process.env.KAFKA_BROKERS!.split(",") },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: "reconciliation", fromBeginning: true, autoCommitInterval: 1000 },
});
await consumer.connect();
await consumer.subscribe({ topic: TOPIC });

// On stop, leave the group in an orderly way (chapter 8)
process.on("SIGTERM", async () => {
  await consumer.disconnect();
  process.exit(0);
});
log(`reading "${TOPIC}" in group "reconciliation", ${PROCESSING_MS} ms per payment`);

// Reconciles one payment at a time. The offset is committed automatically, every
// second, after processing: what is left up to the end of the partition is the lag.
let processed = 0;
await consumer.run({
  eachMessage: async ({ partition, message }) => {
    await wait(PROCESSING_MS); // the work of reconciling one payment
    if (++processed % 250 === 0) {
      log(`${processed} reconciled (partition ${partition}, ` +
          `offset ${message.offset})`);
    }
  },
});
