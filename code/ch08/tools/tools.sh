#!/bin/bash
# Lab tool for chapter 8 (Kafka and PostgreSQL).
#   week     -> publishes a week of payments (7 days, 30 orders per day)
#   day N    -> publishes the 30 payments of day N
#   status   -> right and wrong points, computed and expected balance, group lag
#   replay   -> moves the "loyalty" group back to the start (stop the consumer first)
#   balances -> reads the compacted customer-balance topic from the start
#   follow CUSTOMER -> shows the customer's balances published from now on
#                      (stops after 30 s with no new balance)
#   clean    -> clears the points table
set -e
BIN=/opt/kafka/bin
SERVER=kafka:9092

# Generates the events of one day, in the "key|value" format the console producer
# understands. The values are fixed (computed from the day and the order number), so
# that the expected balance is always the same.
day_events() {
  for i in $(seq 1 30); do
    customer="customer-$(( i % 10 + 1 ))"
    dollars=$(( (i * 37 + $1 * 13) % 400 + 20 ))
    printf '%s|{"orderId":"d%s-p%s","customerId":"%s","total":%s.90,"day":%s}\n' \
      "d$1-p$i" "$1" "$i" "$customer" "$dollars" "$1"
  done
}

publish() {
  "$BIN/kafka-console-producer.sh" --bootstrap-server "$SERVER" --topic paid-orders \
    --reader-property parse.key=true --reader-property key.separator='|' > /dev/null
}

case "$1" in
  week)
    for day in 1 2 3 4 5 6 7; do day_events "$day"; done | publish
    echo "Published 210 payments (days 1 to 7)."
    ;;
  day)
    day_events "$2" | publish
    echo "Published 30 payments (day $2)."
    ;;
  status)
    psql -q -At -F ' ' -c "
      SELECT 'Orders: ' || count(*)
        || ' | correct: ' || count(*) FILTER (WHERE points = floor(total))
        || ' | wrong: ' || count(*) FILTER (WHERE points <> floor(total))
        || ' | total balance: ' || coalesce(sum(points), 0)
        || ' (expected: ' || coalesce(sum(floor(total)), 0) || ')'
      FROM points_per_order;"
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$SERVER" --describe \
      --group loyalty 2>/dev/null \
      | awk '$2 == "paid-orders" { read += $4; end += $5 }
             END { printf "Group loyalty: offsets read %d of %d (lag %d)\n",
                   read, end, end - read }'
    ;;
  replay)
    # The reset only works with an empty group: stop the consumer first.
    # Waits up to 60 s for the group to become empty (a consumer that crashed without
    # notice only leaves the group when the session deadline expires).
    for i in $(seq 1 30); do
      state=$("$BIN/kafka-consumer-groups.sh" --bootstrap-server "$SERVER" \
        --describe --group loyalty --state 2>/dev/null \
        | awk '$1 == "loyalty" { print $(NF-1) }')
      [ "$state" = "Empty" ] && break
      echo "Group loyalty: $state; waiting for it to become empty..."
      sleep 2
    done
    # First the plan (--dry-run: nothing changes), then the execution
    echo "Plan:"
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$SERVER" --group loyalty \
      --topic paid-orders --reset-offsets --to-earliest --dry-run
    echo "Executing:"
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$SERVER" --group loyalty \
      --topic paid-orders --reset-offsets --to-earliest --execute
    ;;
  follow)
    # Without --from-beginning: only what is published after the command starts
    echo "Balances of $2 published from now on:"
    "$BIN/kafka-console-consumer.sh" --bootstrap-server "$SERVER" \
      --topic customer-balance \
      --formatter-property print.key=true --formatter-property key.separator=' ' \
      --timeout-ms 30000 2>/dev/null \
      | awk -v c="$2" '$1 == c { print "  " $2; fflush() }' || true
    ;;
  balances)
    "$BIN/kafka-console-consumer.sh" --bootstrap-server "$SERVER" \
      --topic customer-balance \
      --from-beginning --formatter-property print.key=true \
      --formatter-property key.separator=' ' \
      --timeout-ms 5000 2>/dev/null > /tmp/balances.txt || true
    echo "Records in the topic: $(wc -l < /tmp/balances.txt)" \
         "| distinct customers: $(cut -d' ' -f1 /tmp/balances.txt | sort -u | wc -l)"
    echo "Latest balance of each customer:"
    # The last record of each key is the current state
    awk '{ last[$1] = $2 } END { for (c in last) print "  " c " " last[c] }' \
      /tmp/balances.txt | sort -V
    ;;
  clean)
    psql -q -c "TRUNCATE points_per_order;"
    echo "Points table cleared."
    ;;
  *)
    echo "Commands: week | day N | status | replay | balances | follow CUSTOMER | clean"
    ;;
esac
