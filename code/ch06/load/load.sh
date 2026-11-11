#!/bin/sh
# Lab tool for chapter 6.
#   docker compose run --rm load          -> creates N orders (3 events each)
#   docker compose run --rm load status   -> final status of the orders and stale events
#   docker compose run --rm load clean    -> deletes the projection

API="${API_URL:-http://orders-api:8080}"
N="${N:-20}"
C="${C:-5}"

show_state() {
  echo "Final status of the orders (the correct result is all in \"shipped\"):"
  psql -q -P footer=off -c "
    SELECT status, count(*) AS orders FROM order_status
    GROUP BY status ORDER BY status;"
  echo "Stale events discarded by the consumer:"
  psql -q -At -c "SELECT count(*) FROM stale_events;"
}

case "$1" in
  status)
    show_state
    ;;
  clean)
    psql -q -c "TRUNCATE order_status, stale_events;"
    echo "Projection deleted."
    ;;
  *)
    echo "Creating $N orders ($C in parallel); each one generates 3 events..."
    seq 1 "$N" | xargs -P "$C" -I {} curl -s -o /dev/null -m 30 -w '%{http_code}\n' \
      -X POST "$API/orders" -H 'Content-Type: application/json' \
      -d '{"customer":"Customer {}","email":"customer{}@example.com","total":150.0}' \
      | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
    ;;
esac
