#!/bin/bash
# Lab tool for chapter 15 (Event Sourcing and CQRS).
#   load [N]                -> N random commands for customers 1 to 20
#   earn CUSTOMER POINTS    -> one earn command
#   redeem CUSTOMER POINTS  -> one redeem command
#   balance CUSTOMER        -> queries the balance (read side)
#   events CUSTOMER         -> the customer's events in the event store
#   conflict CUSTOMER       -> two redemptions at once, with the same expected version
#   status                  -> compares the event store with the projection
#   lose CUSTOMER POINTS    -> writes an earning to the event store without publishing it
#                              to the topic (the API's dual write that failed)
#   rebuild                 -> deletes the projection (stop projections first); then
#                              `docker compose run --rm -e REBUILD=1 projections`
#                              rebuilds it by reading the event store
#   rebuild topic           -> deletes the projection and moves the consumer back to the
#                              start of the topic
#   history CUSTOMER N      -> creates N old events for the CUSTOMER (years of use)
#   time CUSTOMER [N]       -> average time of N earn commands for the CUSTOMER
set -e
BIN=/opt/kafka/bin
KAFKA=kafka:9092
API=http://loyalty-api:8080
TOPIC=loyalty-events

send_command() {  # $1 customer, $2 earn|redeem, $3 body; prints the HTTP code
  curl -s -o /tmp/response -m 30 -w '%{http_code}' -X POST "$API/customers/$1/$2" \
    -H 'Content-Type: application/json' -d "$3"
}

case "$1" in
  load)
    total=${2:-200}
    echo "Sending $total commands..."
    for i in $(seq 1 "$total"); do
      customer="customer-$(( RANDOM % 20 + 1 ))"
      if [ $(( RANDOM % 4 )) -eq 0 ]; then
        send_command "$customer" redeem '{"points":100}'; echo
      else
        send_command "$customer" earn "{\"points\":$(( RANDOM % 191 + 10 ))}"; echo
      fi
    done | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
    ;;
  earn|redeem)
    code=$(send_command "$2" "$1" "{\"points\":$3}")
    echo "HTTP $code: $(cat /tmp/response)"
    ;;
  balance)
    curl -s "$API/customers/$2"; echo
    ;;
  events)
    psql -q -P footer=off -c "
      SELECT version, type, data, to_char(recorded_at, 'HH24:MI:SS') AS time
      FROM events WHERE customer_id = '$2' ORDER BY version;"
    ;;
  conflict)
    version=$(psql -At -c "SELECT coalesce(max(version), 0) FROM events
                           WHERE customer_id = '$2'")
    echo "Two redemptions of 10 points, both expecting version $version:"
    body="{\"points\":10,\"expectedVersion\":$version}"
    curl -s -o /dev/null -w '  HTTP %{http_code}\n' -X POST "$API/customers/$2/redeem" \
      -H 'Content-Type: application/json' -d "$body" &
    curl -s -o /dev/null -w '  HTTP %{http_code}\n' -X POST "$API/customers/$2/redeem" \
      -H 'Content-Type: application/json' -d "$body" &
    wait
    ;;
  status)
    # The truth is the event store; the projection must reach the same balance
    psql -q -P footer=off -c "
      WITH truth AS (
        SELECT customer_id,
               sum(CASE type WHEN 'PointsRedeemed' THEN -(data->>'points')::int
                             ELSE (data->>'points')::int END) AS balance
        FROM events GROUP BY customer_id)
      SELECT (SELECT count(*) FROM events)                          AS events,
             count(*)                                               AS customers,
             count(b.customer_id)                                   AS in_projection,
             count(*) FILTER (WHERE b.balance IS DISTINCT FROM t.balance) AS divergent,
             (SELECT count(*) FROM statement)                       AS statement_rows
      FROM truth t LEFT JOIN balances b USING (customer_id);"
    ;;
  lose)
    version=$(psql -At -c "
      INSERT INTO events (customer_id, version, type, data)
      SELECT '$2', coalesce(max(version), 0) + 1, 'PointsEarned',
             jsonb_build_object('points', $3)
      FROM events WHERE customer_id = '$2' RETURNING version;" | head -1)
    echo "Event written at version $version of the event store and not published to the topic."
    ;;
  rebuild)
    # Deleting the projection and changing the offsets only works with the group
    # empty: the projections service stopped
    for _ in $(seq 1 30); do
      "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA" \
        --describe --group projections --state 2>/dev/null | grep -qw Empty && break
      echo "The projections group still has consumers; stop the projections service..."
      sleep 2
    done
    psql -q -c "TRUNCATE balances, statement;"
    if [ "$2" = topic ]; then
      "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA" --group projections \
        --topic "$TOPIC" --reset-offsets --to-earliest --execute > /dev/null
      echo "Projection deleted and the projections group back at the start of the topic."
    else
      echo "Projection deleted. Rebuild with:" \
        "docker compose run --rm -e REBUILD=1 projections"
    fi
    ;;
  history)
    customer=$2; total=${3:-20000}
    # Writes the events straight to the event store and publishes the same events to
    # Kafka, as if the customer had years of use
    psql -q -c "
      INSERT INTO events (customer_id, version, type, data, recorded_at)
      SELECT '$customer', v, 'PointsEarned', jsonb_build_object('points', 10),
             now() - make_interval(hours => $total - v)
      FROM generate_series(1, $total) AS v;"
    psql -At -c "
      SELECT customer_id || '|' || json_build_object('customerId', customer_id,
               'version', version, 'type', type, 'data', data,
               'recordedAt', recorded_at)
      FROM events WHERE customer_id = '$customer' ORDER BY version;" |
      "$BIN/kafka-console-producer.sh" --bootstrap-server "$KAFKA" --topic "$TOPIC" \
        --reader-property parse.key=true --reader-property key.separator='|' 2>/dev/null
    echo "$total events created for $customer."
    ;;
  time)
    customer=$2; total=${3:-20}
    for _ in $(seq 1 "$total"); do
      curl -s -o /tmp/response -w '%{time_total} ' -X POST "$API/customers/$customer/earn" \
        -H 'Content-Type: application/json' -d '{"points":10}'
      jq -r '.eventsRead' /tmp/response
    done | awk -v n="$total" '{ ms = $1 * 1000; sum += ms; nread += $2
                                if (NR == 1) { first = ms; nread1 = $2 } }
      END { printf "%d commands. The first read %d events and took %.0f ms.\n",
                   n, nread1, first
            printf "On average: %.0f events read and %.0f ms per command.\n",
                   nread / n, sum / n }'
    ;;
  *)
    sed -n '2,18p' "$0"
    ;;
esac
