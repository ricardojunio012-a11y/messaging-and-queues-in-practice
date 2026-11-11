import { connect, nanos } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/transport-node";
import { AckPolicy, jetstreamManager } from "@nats-io/jetstream";
import type { JetStreamManager } from "@nats-io/jetstream";

export const STREAM = "ORDERS";
export const DLQ = "ORDERS_DLQ";
export const CONSUMER = "notifications";

export type Order = { id: string; customer: string; total: number };

export const sleep = (ms: number) => new Promise((ok) => setTimeout(ok, ms));

export async function connectNats(name: string): Promise<NatsConnection> {
  for (;;) {
    try {
      return await connect({ servers: process.env.NATS_URL, name });
    } catch {
      await sleep(1000);
    }
  }
}

// Creates (or updates) the streams and the durable consumer. Running it again
// changes nothing.
export async function setUpJetStream(nc: NatsConnection): Promise<JetStreamManager> {
  const jsm = await jetstreamManager(nc);
  const streams = [
    // The stream keeps everything published to orders.*; a Nats-Msg-Id repeated
    // within 2 minutes is dropped as a duplicate
    { name: STREAM, subjects: ["orders.>"], duplicate_window: nanos(120_000) },
    { name: DLQ, subjects: ["dlq.>"] },
  ];
  for (const config of streams) {
    try {
      await jsm.streams.add(config);
    } catch {
      await jsm.streams.update(config.name, config);
    }
  }
  const consumer = {
    durable_name: CONSUMER,
    filter_subject: "orders.created",
    ack_policy: AckPolicy.Explicit,
    // Without an ack in 5 s, the message comes back; after 3 deliveries, NATS gives up
    ack_wait: nanos(5_000),
    max_deliver: 3,
  };
  try {
    await jsm.consumers.add(STREAM, consumer);
  } catch {
    await jsm.consumers.update(STREAM, CONSUMER, consumer);
  }
  return jsm;
}
