#!/bin/sh
# Lab tool for chapter 12.
#   docker compose run --rm load          -> creates N orders and shows the responses
#   docker compose run --rm load status   -> orders, outbox, deliveries and emails
#   docker compose run --rm load clear    -> deletes the data and rearms the relay failure

API="${API_URL:-http://orders-api:8080}"
N="${N:-10}"

case "$1" in
  status)
    psql -q -P footer=off -c "
      SELECT (SELECT count(*) FROM orders)                                 AS orders,
             (SELECT count(*) FROM outbox WHERE published_at IS NULL)      AS outbox_pending,
             (SELECT count(*) FROM deliveries)                             AS deliveries,
             (SELECT count(DISTINCT order_id) FROM sent_emails)            AS emails,
             (SELECT count(*) FROM deliveries)
               - (SELECT count(*) FROM processed_messages)                 AS duplicates;"
    ;;
  clear)
    psql -q -c "TRUNCATE orders, outbox, deliveries, processed_messages, sent_emails;"
    rm -f /counter/*
    echo "Data deleted and relay failure rearmed."
    ;;
  *)
    echo "Creating $N orders..."
    for i in $(seq 1 "$N"); do
      curl -s -o /dev/null -m 30 -w '%{http_code}\n' -X POST "$API/orders" \
        -H 'Content-Type: application/json' \
        -d "{\"customer\":\"Customer $i\",\"email\":\"customer$i@example.com\",\"total\":150.0}"
    done | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
    ;;
esac
