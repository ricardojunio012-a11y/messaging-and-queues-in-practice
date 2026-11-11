import {
  ChangeMessageVisibilityBatchCommand, DeleteMessageCommand, GetQueueUrlCommand,
  ReceiveMessageCommand, SQSClient, type Message,
} from "@aws-sdk/client-sqs";
import pg from "pg";

const QUEUE = process.env.QUEUE!;
const PROCESSING_MS = Number(process.env.PROCESSING_MS ?? 200);
const EXTEND = process.env.EXTEND_VISIBILITY === "yes";
const sqs = new SQSClient({ endpoint: process.env.AWS_ENDPOINT });
const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const log = (text: string) => console.log(`[${QUEUE}] ${text}`);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const { QueueUrl: url } = await sqs.send(new GetQueueUrlCommand({ QueueName: QUEUE }));

// Extends the visibility timeout of the whole batch by the time it will take, with
// some slack. Without this, the last messages of a slow batch become visible again
// and are delivered again, to this or another consumer, while they still wait here.
async function extendVisibility(messages: Message[]): Promise<void> {
  const seconds = Math.ceil((messages.length * PROCESSING_MS) / 1000) + 5;
  await sqs.send(new ChangeMessageVisibilityBatchCommand({
    QueueUrl: url,
    Entries: messages.map((m, i) => ({ Id: String(i), ReceiptHandle: m.ReceiptHandle!,
                                       VisibilityTimeout: seconds })),
  }));
}

// Processes a message and only then deletes it. If processing fails, the message is not
// deleted: it becomes visible again when the visibility timeout expires and, after
// 3 receives, SQS moves it to the dead-letter queue.
async function processMessage(message: Message): Promise<void> {
  const { orderId } = JSON.parse(message.Body!);
  const attempt = Number(message.Attributes?.ApproximateReceiveCount ?? 1);
  await db.query(
    "INSERT INTO receives (queue, order_id, attempt) VALUES ($1, $2, $3)",
    [QUEUE, orderId, attempt]);
  if (orderId === "poison") {
    log(`order ${orderId}: failed (attempt ${attempt})`);
    return;
  }
  await sleep(PROCESSING_MS);
  await sqs.send(new DeleteMessageCommand({ QueueUrl: url,
                                            ReceiptHandle: message.ReceiptHandle! }));
}

log(`long polling (processing ${PROCESSING_MS} ms, extend: ${EXTEND})`);
while (true) {
  // Long polling: the call waits up to 20 s for messages instead of returning empty
  const { Messages: messages = [] } = await sqs.send(new ReceiveMessageCommand({
    QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 20,
    MessageSystemAttributeNames: ["ApproximateReceiveCount"],
  }));
  if (messages.length > 0 && EXTEND) await extendVisibility(messages);
  for (const message of messages) await processMessage(message);
}
