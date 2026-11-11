#!/bin/sh
# Load generator for chapter 1.
#   docker compose run --rm load          -> sends N orders, C at a time
#   docker compose run --rm load status   -> only shows the state of the services

API="${API_URL:-http://orders-api:8080}"
NOTIFICATIONS="${NOTIFICATIONS_URL:-http://notifications:8081}"
N="${N:-20}"
C="${C:-5}"
RESULTS=/tmp/results.txt

show_state() {
  echo "orders-api:    $(curl -s -m 3 "$API/health" || echo 'down')"
  echo "notifications: $(curl -s -m 3 "$NOTIFICATIONS/emails" || echo 'down')"
}

if [ "$1" = "status" ]; then
  show_state
  exit 0
fi

echo "Sending $N orders ($C in parallel) to $API ..."
start=$(cut -d' ' -f1 /proc/uptime)

seq 1 "$N" | xargs -P "$C" -I {} curl -s -o /dev/null -m 30 \
  -w '%{http_code} %{time_total}\n' \
  -X POST "$API/orders" \
  -H 'Content-Type: application/json' \
  -d '{"customer":"Customer {}","email":"customer{}@example.com","total":150.0}' \
  > "$RESULTS"

end=$(cut -d' ' -f1 /proc/uptime)

awk -v start="$start" -v end="$end" '
  { n++; codes[$1]++; sum += $2; if ($2 > max) max = $2 }
  END {
    print ""
    print "Responses by HTTP status:"
    for (c in codes) {
      label = (c == "000") ? "000 (no response)" : c
      printf "  %s: %d\n", label, codes[c]
    }
    printf "Average latency: %.0f ms | max: %.0f ms\n", sum / n * 1000, max * 1000
    printf "Total time: %.1f s\n", end - start
  }' "$RESULTS"

echo
show_state
