import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";

const N = Number(process.env.N ?? 20);
const sns = new SNSClient({ endpoint: process.env.AWS_ENDPOINT });

// Publishes to the topic, not to a queue: SNS delivers a copy to each subscribed queue.
// The total also goes as a message attribute, which the subscription filter reads.
for (let i = 1; i <= N; i++) {
  const order = { orderId: `o${i}`, total: 50 * i };
  await sns.send(new PublishCommand({
    TopicArn: process.env.TOPIC_ARN,
    Message: JSON.stringify(order),
    MessageAttributes: {
      total: { DataType: "Number", StringValue: String(order.total) },
    },
  }));
}
console.log(`${N} orders published to the orders topic`);
