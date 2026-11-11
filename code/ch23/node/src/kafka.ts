// Kafka client for the chapter 23 lab. Each command authenticates with SCRAM-SHA-512
// using the given user.
//   publish <user> <topic> [n] [bytes]   publishes n messages and measures throughput
//   read <user> <topic>                  reads from the start, with the user's group
import kafkaJavascript from "@confluentinc/kafka-javascript";

const [command, user = "", topic = "", n = "100", bytes = "1024"] =
  process.argv.slice(2);
const password = process.env[`PASSWORD_${user.toUpperCase()}`] ?? "wrong-password";

// The errors the library only writes to the log (and does not throw) are kept here
const errors: string[] = [];
const logger = {
  info() {}, warn() {}, debug() {}, setLogLevel() {}, namespace: () => logger,
  error(message: string) { errors.push(message); },
};

const kafka = new kafkaJavascript.KafkaJS.Kafka({
  kafkaJS: {
    brokers: ["kafka:9092"],
    sasl: { mechanism: "scram-sha-512", username: user, password },
    connectionTimeout: 5000,
    logger,
  },
});

async function publish(): Promise<void> {
  const producer = kafka.producer({ kafkaJS: { acks: -1 } });
  await producer.connect();
  const body = "x".repeat(Number(bytes));
  const start = Date.now();
  for (let i = 0; i < Number(n); i += 10) {
    await producer.send({ topic, messages: Array.from({ length: 10 }, () =>
      ({ value: body })) });
  }
  const seconds = (Date.now() - start) / 1000;
  const kb = (Number(n) * Number(bytes)) / 1024;
  console.log(`  ${user}: ${n} messages (${kb} KB) to ${topic}, ` +
    `${seconds.toFixed(1)} s, ${(kb / seconds).toFixed(0)} KB/s`);
  await producer.disconnect();
}

async function read(): Promise<void> {
  const consumer = kafka.consumer({
    kafkaJS: { groupId: user, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topic });
  let count = 0;
  let failure: Error | undefined;
  const deadline = new Promise((ok) => setTimeout(ok, 8000));
  consumer.run({ eachMessage: async () => { count++; } })
    .catch((e) => { failure = e; });
  await deadline;
  if (failure) throw failure;
  console.log(`  ${user}: ${count} messages read from ${topic}`);
  // The consumer without permission does not crash: it retries forever and only
  // logs the error
  const denied = errors.find((e) => e.includes("authorization failed"));
  if (denied) console.log(`  ${user}: in the log: ${denied.split(" : ")[0]}`);
  await consumer.disconnect();
}

try {
  await ({ publish, read } as Record<string, () => Promise<void>>)[command]();
} catch (error) {
  console.log(`  ${user}: refused: ${(error as Error).message.split("\n")[0]}`);
  const authentication = errors.find((e) => e.includes("SASL authentication error"))
    ?.replace(/.*(SASL authentication error)/, "$1");
  if (authentication) {
    console.log(`  ${user}: in the log: ${authentication.split(" (after")[0]}`);
  }
}
process.exit(0);
