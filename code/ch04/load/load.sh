#!/bin/sh
# Lab tool for chapter 4.
#   docker compose run --rm load            -> sends N orders and shows the responses
#   docker compose run --rm load status     -> emails sent, duplicates and redeliveries
#   docker compose run --rm load reset      -> resets the counts, re-arms the failure
#   docker compose run --rm load limit      -> limits the "emails" queue to 10 messages
#   docker compose run --rm load unlimit    -> removes the limit

API="${API_URL:-http://orders-api:8080}"
N="${N:-20}"
C="${C:-5}"
FILE=/counter/emails.txt
POLICY=http://rabbitmq:15672/api/policies/%2F/emails-limit

show_state() {
  if [ ! -s "$FILE" ]; then
    echo "Emails sent: 0"
    return
  fi
  total=$(wc -l < "$FILE")
  orders=$(cut -d' ' -f1 "$FILE" | sort -u | wc -l)
  redelivered=$(grep -c ' yes$' "$FILE")
  echo "Emails sent: $total | distinct orders: $orders" \
       "| duplicates: $((total - orders)) | redeliveries: $redelivered"
}

case "$1" in
  status)
    show_state
    ;;
  limit)
    # RabbitMQ policy: at most 10 messages; when full, the queue refuses new ones
    curl -s -u guest:guest -X PUT -H 'Content-Type: application/json' "$POLICY" \
      -d '{"pattern":"^emails$","apply-to":"queues",
           "definition":{"max-length":10,"overflow":"reject-publish"}}' \
      && echo "Queue \"emails\" limited to 10 messages."
    ;;
  unlimit)
    curl -s -u guest:guest -X DELETE "$POLICY" && echo "Limit removed."
    ;;
  reset)
    rm -f /counter/*
    echo "Counts reset and simulated failure re-armed."
    ;;
  *)
    echo "Sending $N orders ($C in parallel)..."
    seq 1 "$N" | xargs -P "$C" -I {} curl -s -o /dev/null -m 30 -w '%{http_code}\n' \
      -X POST "$API/orders" -H 'Content-Type: application/json' \
      -d '{"customer":"Customer {}","email":"customer{}@example.com","total":150.0}' \
      | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
    ;;
esac
