import amqp from "amqplib";
import pg from "pg";

export const SAGA =
  process.env.SAGA === "orchestration" ? "orchestration" : "choreography";
const EXCHANGE = "saga"; // topic exchange: events and commands go through it

export const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
export const log = (service: string, message: string) =>
  console.log(`[${service}] ${message}`);

// Every saga message carries the same context: what each step needs to know
export type Context = { orderId: string; sku: string; amount: number };
// A step receives the context and returns the key of the message to publish (or null)
export type Step = (ctx: Context, key: string) => Promise<string | null>;

let publishChannel: amqp.ConfirmChannel;

export async function connect(): Promise<amqp.ChannelModel> {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  publishChannel = await connection.createConfirmChannel();
  await publishChannel.assertExchange(EXCHANGE, "topic", { durable: true });
  return connection;
}

// Publishes an event or command, with confirmation, and records it in the order history
export async function publish(service: string, key: string, ctx: Context) {
  publishChannel.publish(EXCHANGE, key, Buffer.from(JSON.stringify(ctx)),
                         { persistent: true });
  await publishChannel.waitForConfirms();
  await db.query(
    "INSERT INTO history (order_id, service, message) VALUES ($1, $2, $3)",
    [ctx.orderId, service, key],
  );
}

// Binds the service's queue to the keys of each step and consumes. Whatever the step
// returns is published before the ack. The queue is named after the saga style, so
// that switching styles does not mix the bindings; unused queues are deleted after
// 10 minutes.
export async function consume(service: string, steps: Record<string, Step>) {
  const connection = await connect();
  const channel = await connection.createChannel();
  const queue = `${service}-${SAGA}`;
  await channel.assertQueue(queue,
                            { durable: true, arguments: { "x-expires": 600_000 } });
  for (const key of Object.keys(steps)) await channel.bindQueue(queue, EXCHANGE, key);
  await channel.prefetch(1);
  await channel.consume(queue, async (message) => {
    if (!message) return;
    const key = message.fields.routingKey;
    try {
      const ctx: Context = JSON.parse(message.content.toString());
      const reply = await steps[key](ctx, key);
      if (reply) await publish(service, reply, ctx);
    } catch (error) {
      log(service, `error handling ${key}: ${(error as Error).message}`);
    }
    channel.ack(message);
  });
  log(service, `queue "${queue}" bound to: ${Object.keys(steps).join(", ")}`);
}
