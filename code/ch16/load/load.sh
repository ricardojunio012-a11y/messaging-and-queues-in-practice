#!/bin/sh
# Lab tool for chapter 16.
#   docker compose run --rm load invoice ORDER   -> issues the invoice for ORDER
#   docker compose run --rm load invoices        -> invoices received by the sender
#   docker compose run --rm load cart ID         -> the customer abandons cart ID
#   docker compose run --rm load buy ID          -> the customer buys cart ID
#   docker compose run --rm load reminders       -> what the reminders service did
#   docker compose run --rm load queues          -> messages in each queue

ISSUER="http://invoice-issuer:8080"
RABBIT="http://guest:guest@rabbitmq:15672/api"

case "$1" in
  invoice)
    curl -s -m 60 -w '\nHTTP %{http_code}\n' -X POST "$ISSUER/invoices/$2"
    ;;
  invoices)
    psql -q -P footer=off -c "
      SELECT order_id, size, hash_matches, to_char(received_at, 'HH24:MI:SS') AS time
      FROM sent_invoices ORDER BY received_at;"
    ;;
  cart)
    # Publishes straight to the waiting queue through the RabbitMQ management API
    now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    body="{\"cartId\":\"$2\",\"abandonedAt\":\"$now\"}"
    payload=$(printf '%s' "$body" | sed 's/"/\\"/g')
    curl -s -o /dev/null -X POST "$RABBIT/exchanges/%2F/amq.default/publish" \
      -H 'Content-Type: application/json' \
      -d "{\"properties\":{\"delivery_mode\":2},\"routing_key\":\"carts-waiting\",\"payload\":\"$payload\",\"payload_encoding\":\"string\"}"
    echo "$(date +%H:%M:%S) cart $2 abandoned"
    ;;
  buy)
    psql -q -c "INSERT INTO purchases (cart_id) VALUES ('$2') ON CONFLICT DO NOTHING;"
    echo "$(date +%H:%M:%S) cart $2 bought"
    ;;
  reminders)
    psql -q -P footer=off -c "
      SELECT cart_id, to_char(abandoned_at, 'HH24:MI:SS') AS abandoned,
             to_char(processed_at, 'HH24:MI:SS') AS processed, result
      FROM reminders ORDER BY processed_at;"
    ;;
  queues)
    curl -s "$RABBIT/queues/%2F" | jq -r '.[] | "  \(.name): \(.messages) message(s)"'
    ;;
  *)
    sed -n '2,8p' "$0"
    ;;
esac
