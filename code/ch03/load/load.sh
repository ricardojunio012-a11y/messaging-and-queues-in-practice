#!/bin/sh
# Lab tool for chapter 3.
#   docker compose run --rm load            -> publishes N orders (OrderCreated)
#   docker compose run --rm load status     -> what each consumer received
#   docker compose run --rm load shipping   -> makes N shipping quotes (request/reply)
#   docker compose run --rm load reset      -> resets the counts

API="${API_URL:-http://orders-api:8080}"
N="${N:-10}"
C="${C:-5}"

show_state() {
  printf "%-26s %8s %8s %10s\n" "consumer" "events" "orders" "instances"
  for file in /counter/*.txt; do
    [ -e "$file" ] || { echo "(no events received yet)"; return; }
    name=$(basename "$file" .txt)
    events=$(wc -l < "$file")
    orders=$(cut -d' ' -f1 "$file" | sort -u | wc -l)
    instances=$(cut -d' ' -f2 "$file" | sort -u | wc -l)
    printf "%-26s %8s %8s %10s\n" "$name" "$events" "$orders" "$instances"
  done
}

case "$1" in
  status)
    show_state
    ;;
  reset)
    rm -f /counter/*.txt
    echo "Counts reset."
    ;;
  shipping)
    echo "Requesting $N shipping quotes ($C in parallel)..."
    seq 1 "$N" | xargs -P "$C" -I {} curl -s -o /dev/null -m 30 \
      -w '%{http_code} %{time_total}\n' "$API/shipping?zipCode=3014{}000" \
      | awk '{ n++; c[$1]++; s += $2 }
             END { for (k in c) printf "  HTTP %s: %d\n", k, c[k];
                   printf "  average time: %.0f ms\n", s / n * 1000 }'
    ;;
  *)
    echo "Publishing $N orders ($C in parallel) to $API ..."
    seq 1 "$N" | xargs -P "$C" -I {} curl -s -o /dev/null -m 30 \
      -w '%{http_code}\n' \
      -X POST "$API/orders" \
      -H 'Content-Type: application/json' \
      -d '{"customer":"Customer {}","email":"customer{}@example.com","total":150.0}' \
      | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
    sleep 2
    echo
    show_state
    ;;
esac
