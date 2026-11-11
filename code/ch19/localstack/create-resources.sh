#!/bin/bash
# Run by LocalStack when it is ready: creates the topic, the queues and the
# subscriptions
set -e
ACCOUNT=000000000000
TOPIC=$(awslocal sns create-topic --name orders --query TopicArn --output text)

# Dead-letter queue and notifications queue: after 3 receives without DeleteMessage,
# the message goes to notifications-dlq
awslocal sqs create-queue --queue-name notifications-dlq > /dev/null
awslocal sqs create-queue --queue-name notifications --attributes '{
  "VisibilityTimeout": "30",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:'$ACCOUNT':notifications-dlq\",\"maxReceiveCount\":\"3\"}"
}' > /dev/null
awslocal sqs create-queue --queue-name loyalty > /dev/null

# Subscriptions with raw delivery (the message body arrives as it was published).
# Loyalty only receives orders with a total of 500 or more.
for queue in notifications loyalty; do
  SUBSCRIPTION=$(awslocal sns subscribe --topic-arn "$TOPIC" --protocol sqs \
    --notification-endpoint "arn:aws:sqs:us-east-1:$ACCOUNT:$queue" \
    --attributes RawMessageDelivery=true --query SubscriptionArn --output text)
  if [ "$queue" = loyalty ]; then
    awslocal sns set-subscription-attributes --subscription-arn "$SUBSCRIPTION" \
      --attribute-name FilterPolicy --attribute-value '{"total":[{"numeric":[">=",500]}]}'
  fi
done
echo "Topic, queues and subscriptions created."
