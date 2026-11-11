import { appendFileSync } from "node:fs";
import { hostname } from "node:os";
import amqp from "amqplib";
import kafkaJavascript from "@confluentinc/kafka-javascript";

const BROKER = process.env.BROKER ?? "rabbitmq"; // rabbitmq | kafka
// notifications | loyalty | analytics
const NAME = process.env.NAME ?? "notifications";
const INSTANCE = hostname();

type OrderCreated = { orderId: string };

const prefix = `[${NAME} via ${BROKER} ${INSTANCE}]`;
const log = (text: string) => console.log(`${prefix} ${text}`);

// Records the received event in a shared file, so the lab can count
function record(event: OrderCreated, origin: string): void {
  const file = `/counter/${NAME}-${BROKER}.txt`;
  appendFileSync(file, `${event.orderId} ${INSTANCE}\n`);
  log(`OrderCreated ${event.orderId.slice(0, 8)} received (${origin})`);
}

// Pub/sub on RabbitMQ: each subscriber has its own queue bound to the exchange.
// Instances of the same subscriber share that queue (competing consumers).
async function consumeFromRabbitMQ(): Promise<void> {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  const channel = await connection.createChannel();
  const queue = `${NAME}.orders`;
  await channel.assertExchange("orders", "fanout", { durable: true });
  await channel.assertQueue(queue, { durable: true });
  await channel.bindQueue(queue, "orders", "");
  await channel.prefetch(10); // at most 10 unacknowledged messages per consumer
  await channel.consume(queue, (message) => {
    if (!message) return;
    record(JSON.parse(message.content.toString()), `queue ${queue}`);
    channel.ack(message);
  });
  log(`subscribed to the "orders" exchange through the "${queue}" queue`);
}

// Log on Kafka: each subscriber is a consumer group. A new group starts
// at the beginning of the topic; instances of the same group share the partitions.
async function consumeFromKafka(): Promise<void> {
  const kafka = new kafkaJavascript.KafkaJS.Kafka({
    kafkaJS: { brokers: [process.env.KAFKA_BROKERS!] },
  });
  const consumer = kafka.consumer({
    kafkaJS: { groupId: NAME, fromBeginning: true },
  });
  await consumer.connect();
  await consumer.subscribe({ topic: "orders" });
  log(`reading the "orders" topic in the "${NAME}" group`);
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const event = JSON.parse(message.value!.toString());
      record(event, `partition ${partition}, offset ${message.offset}`);
    },
  });
}

if (BROKER === "kafka") await consumeFromKafka();
else await consumeFromRabbitMQ();
