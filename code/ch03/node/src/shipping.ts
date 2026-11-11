import amqp from "amqplib";

const LATENCY_MS = Number(process.env.LATENCY_MS ?? 50);

const log = (text: string) => console.log(`[shipping] ${text}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Example rule: the price and the delivery time depend on the sum of the zip digits
function calculateShipping(zipCode: string) {
  const sum = [...zipCode].reduce((total, digit) => total + (Number(digit) || 0), 0);
  return { zipCode, price: 15 + (sum % 10) * 3.5, deliveryDays: 2 + (sum % 5) };
}

const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createChannel();
await channel.assertQueue("shipping.quotes", { durable: true });
await channel.prefetch(1);

// Answers each request on the queue given in reply-to, repeating the correlation id
await channel.consume("shipping.quotes", async (message) => {
  if (!message) return;
  const { replyTo, correlationId } = message.properties;
  const { zipCode } = JSON.parse(message.content.toString());
  await sleep(LATENCY_MS);
  const quote = calculateShipping(zipCode);
  channel.sendToQueue(replyTo, Buffer.from(JSON.stringify(quote)), { correlationId });
  channel.ack(message);
  log(`quote ${correlationId.slice(0, 8)} for zip code ${zipCode}: $${quote.price}`);
});
log(`answering quotes from the "shipping.quotes" queue (latency ${LATENCY_MS} ms)`);
