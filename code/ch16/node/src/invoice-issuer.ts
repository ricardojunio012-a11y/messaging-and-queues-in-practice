import { createServer, type ServerResponse } from "node:http";
import { createHash, randomBytes } from "node:crypto";
import amqp from "amqplib";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

const PORT = Number(process.env.PORT ?? 8080);
const CLAIM_CHECK = process.env.CLAIM_CHECK === "yes";
const BUCKET = "invoices";
const QUEUE = "invoices";
const s3 = new S3Client({ endpoint: process.env.S3_ENDPOINT, forcePathStyle: true });
const log = (text: string) => console.log(`[invoice-issuer] ${text}`);

// Publishes with confirmation. A message above the broker's limit closes the channel
// with an error, and the confirmation never arrives: the publish fails.
async function publish(message: Buffer): Promise<void> {
  const connection = await amqp.connect(process.env.RABBITMQ_URL!);
  connection.on("error", () => {});
  let reason = "";
  try {
    const channel = await connection.createConfirmChannel();
    channel.on("error", (error: Error) => { reason = error.message; }); // the broker's
    await channel.assertQueue(QUEUE, { durable: true });
    channel.sendToQueue(QUEUE, message, { persistent: true });
    await channel.waitForConfirms();
  } catch (error) {
    throw new Error(reason || (error as Error).message);
  } finally {
    await connection.close().catch(() => {});
  }
}

// Issues the invoice (a 5 MB PDF, simulated) and publishes the event for sending.
// With the Claim Check, the PDF goes to S3, and the message carries only the
// reference and the hash.
async function issue(orderId: string): Promise<number> {
  const pdf = randomBytes(5 * 1024 * 1024);
  const sha256 = createHash("sha256").update(pdf).digest("hex");
  let event: object;
  if (CLAIM_CHECK) {
    const key = `invoices/${orderId}.pdf`;
    // ① writes the PDF to S3 before publishing
    await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: pdf,
                                         ContentType: "application/pdf" }));
    event = { orderId, sha256, size: pdf.length, invoice: { bucket: BUCKET, key } };
  } else {
    event = { orderId, sha256, size: pdf.length, pdf: pdf.toString("base64") };
  }
  const message = Buffer.from(JSON.stringify(event));
  await publish(message); // ② publishes the event (with the Claim Check, the ticket)
  return message.length;
}

function respond(res: ServerResponse, status: number, data: object): void {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(data));
}

const server = createServer(async (req, res) => {
  const [, resource, orderId] = (req.url ?? "").split("/");
  if (req.method !== "POST" || resource !== "invoices" || !orderId) {
    return respond(res, 404, { error: "Route not found" });
  }
  try {
    const bytes = await issue(orderId);
    log(`invoice for order ${orderId} published: message of ${bytes} bytes`);
    respond(res, 202, { orderId, bytesInMessage: bytes });
  } catch (error) {
    log(`invoice for order ${orderId}: error: ${(error as Error).message}`);
    respond(res, 500, { error: (error as Error).message });
  }
});

server.listen(PORT, () => {
  log(`listening on port ${PORT} (claim check: ${CLAIM_CHECK})`);
});
