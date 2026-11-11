import { AdvisoryKind, jetstream } from "@nats-io/jetstream";
import { STREAM, connectNats, setUpJetStream } from "./common.ts";

// JetStream has no ready-made DLQ: when a consumer gives up on a message (it reached
// max_deliver), the server publishes an advisory. This service listens to the
// advisory, fetches the original message by its sequence number and copies it to
// ORDERS_DLQ.
const nc = await connectNats("dlq");
const jsm = await setUpJetStream(nc);
const js = jetstream(nc);
console.log("[dlq] waiting for max_deliver advisories");

// ① Listens to the JetStream advisories and keeps only the max_deliver ones
for await (const advisory of jsm.advisories()) {
  if (advisory.kind !== AdvisoryKind.MaxDeliver) continue;
  const { stream, consumer, stream_seq, deliveries } = advisory.data as {
    stream: string; consumer: string; stream_seq: number; deliveries: number;
  };
  if (stream !== STREAM) continue;
  // ② The advisory carries only the sequence number: fetch the original message
  const original = await jsm.streams.getMessage(STREAM, { seq: stream_seq });
  if (!original) continue;
  // ③ Copies it to the DLQ; the msgID avoids two copies if the advisory comes twice
  await js.publish(`dlq.${consumer}`, original.data,
                   { msgID: `${STREAM}-${stream_seq}` });
  console.log(`[dlq] message ${stream_seq} given up by ${consumer} after ` +
    `${deliveries} deliveries: copied to the DLQ`);
}
