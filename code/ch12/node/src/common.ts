import amqp from "amqplib";

export const QUEUE = "notifications";

export const log = (service: string, message: string) =>
  console.log(`[${service}] ${message}`);
export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export type Event = { id: string; type: string; data: object };

// Publishes the events with broker confirmation (chapter 4). Opens one connection per
// call: if the broker is down, publishing fails right away, with an error.
export async function publish(events: Event[]): Promise<void> {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  try {
    const channel = await connection.createConfirmChannel();
    await channel.assertQueue(QUEUE, { durable: true });
    for (const event of events) {
      channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(event.data)), {
        messageId: event.id, // the same id on any new attempt
        type: event.type,
        persistent: true,
      });
    }
    await channel.waitForConfirms();
    await channel.close();
  } finally {
    await connection.close();
  }
}
