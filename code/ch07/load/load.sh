#!/bin/sh
# Lab tool for chapter 7. Publishes straight to RabbitMQ through the HTTP management
# API, so the chapter can focus on the consumer.
#   docker compose run --rm load           -> publishes N valid orders
#   docker compose run --rm load poison    -> publishes an order with the total as text
#   docker compose run --rm load status    -> invoices, attempts and messages per queue
#   docker compose run --rm load dlq       -> shows the DLQ, without taking anything out
#   docker compose run --rm load redrive   -> returns the DLQ to the main queue, slowly
#   docker compose run --rm load clean     -> empties the queues and the tables

N="${N:-20}"
API=http://rabbitmq:15672/api
QUEUES="invoices invoices.retry.1 invoices.retry.2 invoices.retry.3 invoices.dlq"

http() { curl -s -u guest:guest -H 'Content-Type: application/json' "$@"; }

# Publishes a JSON body to the "invoices" exchange with the routing key "issue"
publish() {
  jq -n --arg body "$1" --arg id "$2" '{
    routing_key: "issue", payload: $body, payload_encoding: "string",
    properties: { delivery_mode: 2, message_id: $id, content_type: "application/json" }
  }' | http -X POST "$API/exchanges/%2F/invoices/publish" -d @- > /dev/null
}

uuid() { cat /proc/sys/kernel/random/uuid; }

show_state() {
  psql -q -At -F ' ' -c "
    SELECT 'Invoices issued: ' || (SELECT count(*) FROM issued_invoices)
      || ' | calls to SEFAZ: ' || count(*) FILTER (WHERE result <> 'invalid')
      || ' (failures: ' || count(*) FILTER (WHERE result = 'transient_failure') || ')'
      || ' | invalid messages: '
      || count(*) FILTER (WHERE result = 'invalid')
    FROM attempts;"
  echo "Messages ready in the queues:"
  python3 /count-queues.py $QUEUES
}

case "$1" in
  poison)
    id=$(uuid)
    body="{\"orderId\":\"$id\",\"customer\":\"Partner X\",\"total\":\"150.00\"}"
    publish "$body" "$id"
    echo "Published order $id with the total \"150.00\" (text)."
    ;;
  status)
    show_state
    ;;
  dlq)
    # ack_requeue_true: reads and puts back, the message stays in the DLQ
    http -X POST "$API/queues/%2F/invoices.dlq/get" \
      -d '{"count":50,"ackmode":"ack_requeue_true","encoding":"auto"}' \
      | jq -r '.[] | "\(.properties.headers["x-error"] // "?") | \(.payload)"'
    ;;
  redrive)
    # Reads without removing, publishes with confirmation and only then removes from
    # the DLQ, at a limited speed (redrive.py). "load redrive --crash-after 3"
    # simulates the crash.
    shift
    python3 /redrive.py "$@"
    ;;
  clean)
    for queue in $QUEUES; do
      http -X DELETE "$API/queues/%2F/$queue/contents" > /dev/null
    done
    psql -q -c "TRUNCATE issued_invoices, attempts;"
    echo "Queues emptied and tables cleared."
    ;;
  *)
    echo "Publishing $N orders..."
    for i in $(seq 1 "$N"); do
      id=$(uuid)
      publish "{\"orderId\":\"$id\",\"customer\":\"Customer $i\",\"total\":150.0}" "$id"
    done
    echo "Published: $N"
    ;;
esac
