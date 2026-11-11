import kafkaJavascript from "@confluentinc/kafka-javascript";

const ACKS = process.env.ACKS ?? "1";
const TOPIC = process.env.TOPIC!;
const N = Number(process.env.N ?? 1000);

// The idempotent producer requires acks=all; with acks=1, it stays off. The delivery
// deadline is short (10 s) so that, without enough replicas, the failure shows up fast.
const producer = new kafkaJavascript.Producer({
  "bootstrap.servers": process.env.KAFKA_BROKERS!,
  "acks": ACKS === "all" ? -1 : Number(ACKS),
  "enable.idempotence": ACKS === "all",
  "message.timeout.ms": 10000,
  "dr_cb": true,
});

let confirmed = 0;
let failed = 0;
let lastError = "";
// The delivery report says, for each message, whether the broker confirmed it
producer.on("delivery-report", (error) => {
  if (error) { failed++; lastError = error.message; } else confirmed++;
});

await new Promise((ready) => { producer.on("ready", ready); producer.connect(); });
producer.setPollInterval(50);
for (let i = 1; i <= N; i++) {
  const value = Buffer.from(JSON.stringify({ payment: i }));
  producer.produce(TOPIC, null, value, `p${i}`);
}
while (confirmed + failed < N) await new Promise((r) => setTimeout(r, 100));
console.log(`acks=${ACKS} on ${TOPIC}: ${confirmed} confirmed, ${failed} failed` +
            (failed ? ` (${lastError})` : ""));
producer.disconnect();
