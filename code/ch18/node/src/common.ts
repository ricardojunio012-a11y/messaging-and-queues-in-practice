import amqp from "amqplib";
import pg from "pg";

const NODES = process.env.RABBITMQ_NODES!.split(",");
export const db = new pg.Pool({ connectionString: process.env.DATABASE_URL });
export const log = (service: string, text: string) =>
  console.log(`[${service}] ${text}`);
export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let next = Math.floor(Math.random() * NODES.length);

// Connects to the next node on the list that answers. In a cluster, any node serves
// any queue: if the node goes down, the client simply tries the next one.
export async function connect(service: string): Promise<amqp.ChannelModel> {
  while (true) {
    const node = NODES[next++ % NODES.length];
    try {
      const url = `amqp://guest:guest@${node}:5672?heartbeat=5`;
      const connection = await amqp.connect(url);
      connection.on("error", () => {}); // whoever uses the connection handles the drop
      log(service, `connected to ${node}`);
      return connection;
    } catch {
      log(service, `${node} did not answer; trying the next node`);
      await sleep(1000);
    }
  }
}
