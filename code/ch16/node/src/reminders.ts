import amqp from "amqplib";
import pg from "pg";

const WAIT_S = Number(process.env.REMINDER_S ?? 30);
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (text: string) => console.log(`[reminders] ${text}`);

const connection = await amqp.connect(process.env.RABBITMQ_URL!);
const channel = await connection.createChannel();

// Delayed message with TTL and dead-letter: the abandoned cart goes into the waiting
// queue, which nobody consumes. When the TTL expires, RabbitMQ moves the message to
// the "reminders" queue. The TTL belongs to the queue, not to the message: they all
// wait the same time, and the oldest always expires first.
await channel.assertQueue("reminders", { durable: true });
await channel.assertQueue("carts-waiting", {
  durable: true,
  arguments: {
    "x-message-ttl": WAIT_S * 1000,
    "x-dead-letter-exchange": "",
    "x-dead-letter-routing-key": "reminders",
  },
});

// At the time to act, check again: the customer may have bought while the message
// waited. The delayed message says "check in a while", not "send".
async function remind(cartId: string, abandonedAt: string): Promise<void> {
  const { rowCount } = await db.query("SELECT 1 FROM purchases WHERE cart_id = $1",
                                      [cartId]);
  const result = rowCount ? "discarded (already bought)" : "sent";
  await db.query(
    `INSERT INTO reminders (cart_id, abandoned_at, result) VALUES ($1, $2, $3)
     ON CONFLICT DO NOTHING`,
    [cartId, abandonedAt, result],
  );
  log(`cart ${cartId}: reminder ${result}`);
}

await channel.prefetch(1);
await channel.consume("reminders", async (message) => {
  if (!message) return;
  const { cartId, abandonedAt } = JSON.parse(message.content.toString());
  await remind(cartId, abandonedAt);
  channel.ack(message);
});
log(`abandoned carts wait ${WAIT_S} s in the "carts-waiting" queue`);
