#!/bin/sh
# Lab tool for chapter 19 (SQS and SNS on LocalStack).
#   queues              -> visible and in-flight messages in each queue
#   result              -> receives per queue: distinct orders and repeats
#   visibility SEC      -> changes the visibility timeout of the notifications queue
#   poison              -> publishes an order that the consumer always rejects
#   dlq                 -> messages in the dead-letter queue, with the receive count
#   redrive             -> moves the messages in the DLQ back to the source queue
#   clear               -> purges the queues and deletes the receives

aws() { command aws --endpoint-url "$AWS_ENDPOINT" "$@"; }
url() { aws sqs get-queue-url --queue-name "$1" --query QueueUrl --output text; }

case "$1" in
  queues)
    for queue in notifications loyalty notifications-dlq; do
      aws sqs get-queue-attributes --queue-url "$(url $queue)" --attribute-names \
        ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible |
        jq -r --arg q "$queue" '.Attributes |
          "  \($q): \(.ApproximateNumberOfMessages) visible, \(.ApproximateNumberOfMessagesNotVisible) in flight"'
    done
    ;;
  result)
    psql -q -P footer=off -c "
      SELECT queue, count(DISTINCT order_id) AS orders, count(*) AS receives,
             count(*) - count(DISTINCT order_id) AS repeats,
             max(attempt) AS max_attempt
      FROM receives GROUP BY queue ORDER BY queue;"
    ;;
  visibility)
    aws sqs set-queue-attributes --queue-url "$(url notifications)" \
      --attributes VisibilityTimeout="$2"
    echo "Visibility timeout of the notifications queue: $2 s."
    ;;
  poison)
    aws sns publish --topic-arn "$TOPIC_ARN" \
      --message '{"orderId":"poison","total":100}' \
      --message-attributes '{"total":{"DataType":"Number","StringValue":"100"}}' > /dev/null
    echo "Poison order published."
    ;;
  dlq)
    aws sqs receive-message --queue-url "$(url notifications-dlq)" \
      --attribute-names ApproximateReceiveCount --max-number-of-messages 10 \
      --visibility-timeout 0 |
      jq -r '.Messages // [] | .[] | "  \(.Body)  receives in the dead-letter queue: \(.Attributes.ApproximateReceiveCount)"'
    ;;
  redrive)
    aws sqs start-message-move-task \
      --source-arn arn:aws:sqs:us-east-1:000000000000:notifications-dlq > /dev/null
    echo "Messages in the DLQ moved back to the source queue."
    ;;
  clear)
    for queue in notifications loyalty notifications-dlq; do
      aws sqs purge-queue --queue-url "$(url $queue)"
    done
    psql -q -c "TRUNCATE receives;"
    echo "Queues and receives deleted."
    ;;
  *)
    sed -n '2,9p' "$0"
    ;;
esac
