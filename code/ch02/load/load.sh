#!/bin/sh
# Lab tool for chapter 2.
#   docker compose run --rm load          -> sends N orders, C at a time
#   docker compose run --rm load status   -> shows the queue and the emails sent
#   docker compose run --rm load poison   -> sends an order whose email always fails

API="${API_URL:-http://orders-api:8080}"
N="${N:-20}"
C="${C:-5}"
MAX="${MAX_ATTEMPTS:-5}"

show_state() {
  echo "Queue \"emails\":"
  psql -q -P footer=off -c "
    SELECT count(*) FILTER (WHERE status = 'pending' AND visible_at <= now()
                                  AND attempts < $MAX)              AS available,
           count(*) FILTER (WHERE status = 'processing')             AS processing,
           count(*) FILTER (WHERE visible_at > now())                AS leased,
           count(*) FILTER (WHERE status = 'pending' AND visible_at <= now()
                                  AND attempts >= $MAX)             AS exhausted,
           coalesce(max(attempts), 0)                                AS max_attempts
      FROM messages WHERE queue = 'emails';"
  echo "Emails sent:"
  psql -q -P footer=off -c "
    SELECT count(*)                            AS total,
           count(DISTINCT order_id)            AS orders,
           count(*) - count(DISTINCT order_id) AS duplicates,
           count(DISTINCT consumer)            AS consumers
      FROM sent_emails;"
  echo "Transactions open for more than 1 second:"
  psql -q -P footer=off -c "
    SELECT pid, state, round(extract(epoch FROM now() - xact_start)) AS seconds
      FROM pg_stat_activity
     WHERE datname = current_database()
       AND xact_start < now() - interval '1 second'
       AND pid <> pg_backend_pid();"
}

if [ "$1" = "status" ]; then
  show_state
  exit 0
fi

if [ "$1" = "poison" ]; then
  echo "Sending an order whose email the provider always refuses..."
  curl -s -o /dev/null -m 30 -w '  HTTP %{http_code}\n' -X POST "$API/orders" \
    -H 'Content-Type: application/json' \
    -d '{"customer":"Poison customer","email":"poison@example.com","total":150.0}'
  exit 0
fi

echo "Sending $N orders ($C in parallel) to $API ..."
seq 1 "$N" | xargs -P "$C" -I {} curl -s -o /dev/null -m 30 \
  -w '%{http_code}\n' \
  -X POST "$API/orders" \
  -H 'Content-Type: application/json' \
  -d '{"customer":"Customer {}","email":"customer{}@example.com","total":150.0}' \
  | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'

echo
show_state
