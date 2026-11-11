#!/bin/sh
# Lab tool for chapter 5.
#   docker compose run --rm load             -> sends N orders
#   docker compose run --rm load repeat      -> sends each order twice, without a key
#   docker compose run --rm load with-key    -> sends each order twice, with the same
#                                               Idempotency-Key on both
#   docker compose run --rm load concurrent  -> sends each order twice AT THE SAME TIME,
#                                               with the same Idempotency-Key
#   docker compose run --rm load status      -> orders, processed messages and emails
#   docker compose run --rm load reset       -> deletes the data, re-arms the failure

API="${API_URL:-http://orders-api:8080}"
N="${N:-20}"
C="${C:-5}"

show_state() {
  psql -q -P footer=off -c "
    SELECT (SELECT count(*) FROM orders)                          AS orders,
           (SELECT count(*) FROM processed_messages)              AS inbox,
           (SELECT count(*) FROM notification_history)            AS history,
           (SELECT count(*) FROM provider_emails)                 AS emails,
           (SELECT count(*) - count(DISTINCT order_id)
              FROM provider_emails)                               AS duplicate_emails;"
}

# Sends one order; $1 = order number, $2 = idempotency key (optional)
send() {
  header=""
  [ -n "$2" ] && header="Idempotency-Key: $2"
  curl -s -o /dev/null -m 30 -w '%{http_code}\n' -X POST "$API/orders" \
    -H 'Content-Type: application/json' ${header:+-H "$header"} \
    -d "{\"customer\":\"Customer $1\",\"email\":\"customer$1@example.com\",\"total\":150.0}"
}

summarize() {
  sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
}

case "$1" in
  status)
    show_state
    ;;
  reset)
    psql -q -c "TRUNCATE orders, processed_messages, notification_history,
                         provider_emails;"
    rm -f /counter/*
    echo "Data deleted and simulated failure re-armed."
    ;;
  repeat)
    echo "Sending $N orders, each one twice, without an idempotency key..."
    for i in $(seq 1 "$N"); do send "$i"; send "$i"; done | summarize
    ;;
  with-key)
    echo "Sending $N orders, each one twice, with the same Idempotency-Key..."
    batch=$(date +%s)
    for i in $(seq 1 "$N"); do send "$i" "order-$batch-$i"; send "$i" "order-$batch-$i"; done \
      | summarize
    ;;
  concurrent)
    echo "Sending $N orders, each one twice at the same time, with the same key..."
    batch=$(date +%s)
    for i in $(seq 1 "$N"); do
      send "$i" "concurrent-$batch-$i" &
      send "$i" "concurrent-$batch-$i" &
      wait
    done | summarize
    ;;
  *)
    echo "Sending $N orders ($C in parallel)..."
    export API
    seq 1 "$N" | xargs -P "$C" -I {} curl -s -o /dev/null -m 30 -w '%{http_code}\n' \
      -X POST "$API/orders" -H 'Content-Type: application/json' \
      -d '{"customer":"Customer {}","email":"customer{}@example.com","total":150.0}' \
      | summarize
    ;;
esac
