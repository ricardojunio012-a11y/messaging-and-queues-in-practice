#!/bin/bash
# Lab tool for chapter 9 (Kafka and PostgreSQL).
#   load RATE SECONDS -> publishes orders at RATE per second, for SECONDS
#   status            -> processed, throughput of the last 30 s, lag, active instances
#                        (and the expired ones, when the consumer has a deadline)
#   members           -> the group's consumers and the partitions of each one
#   partitions N      -> increases the topic to N partitions
#   clean             -> clears the reservations table
set -e
BIN=/opt/kafka/bin
SERVER=kafka:9092
TOPIC=orders-received

case "$1" in
  load)
    rate=${2:-15}
    seconds=${3:-20}
    # 500 different orders; the generator picks one of them for each message
    for i in $(seq 1 500); do
      printf '{"orderId":"p%s","sku":"SKU-%s","quantity":%s}\n' "$i" $(( i % 40 )) \
        $(( i % 3 + 1 ))
    done > /tmp/orders.txt
    echo "Publishing $(( rate * seconds )) orders at $rate per second..."
    "$BIN/kafka-producer-perf-test.sh" --bootstrap-server "$SERVER" --topic "$TOPIC" \
      --num-records $(( rate * seconds )) --throughput "$rate" \
      --payload-file /tmp/orders.txt --payload-delimiter $'\n' | tail -1
    ;;
  status)
    psql -q -At -F ' ' -c "
      SELECT 'Processed: ' || count(*)
        || ' | average over the last 30 s: '
        || round(count(*) FILTER (WHERE reserved_at > now() - interval '30 s')
                 / 30.0, 1)
        || ' per second'
        || ' | active instances: '
        || count(DISTINCT instance) FILTER (WHERE reserved_at > now() - interval '30 s')
      FROM reservations WHERE state = 'reserved';"
    # Only shows up when there is a deadline (MAX_TIME_S) and some order went past it
    psql -q -At -c "
      SELECT 'Expired (discarded without processing): ' || count(*)
      FROM reservations WHERE state = 'expired' HAVING count(*) > 0;"
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$SERVER" --describe \
      --group inventory 2>/dev/null \
      | awk -v t="$TOPIC" '$2 == t { lag += $6; n++ }
             END { printf "Group inventory: lag %d across %d partitions\n", lag, n }'
    ;;
  members)
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$SERVER" --describe \
      --group inventory --members 2>/dev/null \
      | awk '$1 == "inventory" {
               printf "  consumer at %-14s %s partition(s)\n", $3, $5 }' \
      | sort
    ;;
  partitions)
    "$BIN/kafka-topics.sh" --bootstrap-server "$SERVER" --alter --topic "$TOPIC" \
      --partitions "$2"
    echo "Topic $TOPIC now has $2 partitions."
    ;;
  clean)
    psql -q -c "TRUNCATE reservations;"
    echo "Reservations table cleared."
    ;;
  *)
    echo "Commands: load RATE SECONDS | status | members | partitions N | clean"
    ;;
esac
