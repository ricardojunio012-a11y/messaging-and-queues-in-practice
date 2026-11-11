// RabbitMQ client for the chapter 23 lab. Each command connects with the given user,
// always over TLS (amqps, port 5671).
//   publish <user>              publishes 5 OrderCreated to the orders exchange
//   read <user> <queue>         reads whatever is in the queue
//   no-tls <user>               tries port 5672, without encryption
//   other-ca <user>             trusts a CA that did not sign the server
import fs from "node:fs";
import amqp from "amqplib";

const [command, user = "", queue = ""] = process.argv.slice(2);
const password = process.env[`PASSWORD_${user.toUpperCase().replace("-", "_")}`] ?? "";
let reason: Error | undefined; // why the server closed the channel or the connection

// The client trusts only the lab CA and checks that the certificate is for "rabbitmq"
async function connect(ca = "/certs/ca.crt", url = "amqps://rabbitmq:5671/store") {
  const target = new URL(url);
  target.username = user;
  target.password = password;
  const connection = await amqp.connect(target.toString(),
    { ca: [fs.readFileSync(ca)], servername: "rabbitmq" });
  // A denied permission closes the channel: the server sends the reason in an error
  // event
  connection.on("error", (error: Error) => { reason = error; });
  return connection;
}

async function publish(): Promise<void> {
  const connection = await connect();
  const channel = await connection.createConfirmChannel();
  channel.on("error", (error: Error) => { reason = error; });
  for (let i = 1; i <= 5; i++) {
    channel.publish("orders", "order.created", Buffer.from(JSON.stringify({ id: i })));
  }
  await channel.waitForConfirms();
  console.log(`  ${user}: 5 orders published`);
  await connection.close();
}

async function read(): Promise<void> {
  const connection = await connect();
  const channel = await connection.createChannel();
  channel.on("error", (error: Error) => { reason = error; });
  let count = 0;
  for (let m = await channel.get(queue); m; m = await channel.get(queue)) {
    channel.ack(m);
    count++;
  }
  console.log(`  ${user}: ${count} messages read from ${queue}`);
  await connection.close();
}

const commands: Record<string, () => Promise<unknown>> = {
  publish, read,
  "no-tls": () => connect(undefined, "amqp://rabbitmq:5672/store"),
  "other-ca": () => connect("/certs/other-ca.crt"),
};
try {
  await commands[command]();
  if (command === "no-tls" || command === "other-ca") {
    console.log("  connected (it should not have)");
  }
} catch (error) {
  const message = (reason ?? (error as Error)).message ?? String(error);
  console.log(`  ${user}: refused: ${message.split("\n")[0]}`);
}
process.exit(0);
