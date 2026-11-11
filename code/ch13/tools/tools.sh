#!/bin/bash
# Lab tool for chapter 13 (PostgreSQL, Debezium and Kafka).
#   orders [N]      -> creates N orders through the API (default: N from .env)
#   connector NAME  -> creates the NAME connector (outbox or raw) in Kafka Connect
#   remove NAME     -> removes the NAME connector (the replication slot stays in the DB)
#   connectors      -> existing connectors and the state of each one
#   status          -> orders, emails, duplicates and event latency
#   slots           -> replication slots and how much WAL each has not confirmed yet
#   drop SLOT       -> deletes the replication slot SLOT
#   raw             -> events of the orders table topic (CDC without an outbox)
#   queries SEC     -> queries made to the database by relay and Debezium in SEC seconds
#   clear           -> deletes the data in the tables
set -e
BIN=/opt/kafka/bin
CONNECT=http://connect:8083
API=http://orders-api:8080

state() {  # $1 connector
  curl -s "$CONNECT/connectors/$1/status" | jq -r '.connector.state // "missing"'
}

case "$1" in
  orders)
    total=${2:-$N}
    echo "Creating $total orders..."
    for i in $(seq 1 "$total"); do
      curl -s -o /dev/null -m 30 -w '%{http_code}\n' -X POST "$API/orders" \
        -H 'Content-Type: application/json' \
        -d "{\"customer\":\"Customer $i\",\"email\":\"customer$i@example.com\",\"total\":150.0}"
    done | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
    ;;
  connector)
    curl -s -o /dev/null -X PUT "$CONNECT/connectors/$2/config" \
      -H 'Content-Type: application/json' -d @"/connectors/$2.json"
    for _ in $(seq 1 30); do
      [ "$(state "$2")" = RUNNING ] && break
      sleep 1
    done
    echo "Connector $2: $(state "$2")"
    ;;
  remove)
    curl -s -o /dev/null -X DELETE "$CONNECT/connectors/$2"
    echo "Connector $2 removed."
    ;;
  connectors)
    for name in $(curl -s "$CONNECT/connectors" | jq -r '.[]'); do
      echo "$name: $(state "$name")"
    done
    ;;
  status)
    # Latency: from the INSERT into the outbox until the consumer's inbox records the event
    psql -q -P footer=off -c "
      SELECT (SELECT count(*) FROM orders)                             AS orders,
             (SELECT count(DISTINCT order_id) FROM sent_emails)        AS emails,
             (SELECT count(*) FROM deliveries)
               - (SELECT count(*) FROM processed_messages)             AS duplicates,
             round(avg(ms))                                            AS avg_latency_ms,
             round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms))   AS p95_latency_ms
      FROM (SELECT extract(epoch FROM p.processed_at - o.created_at) * 1000 AS ms
            FROM processed_messages p JOIN outbox o ON o.id = p.message_id) t;"
    ;;
  slots)
    psql -q -P footer=off -c "
      SELECT slot_name AS slot, active,
             pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
               AS pending_wal
      FROM pg_replication_slots ORDER BY slot_name;"
    ;;
  drop)
    psql -q -c "SELECT pg_drop_replication_slot('$2');" > /dev/null
    echo "Slot $2 deleted."
    ;;
  raw)
    # Reads everything in the topic and shows how many events there are of each operation
    # (r = read in the snapshot, c = insert, u = update, d = delete) and the last one
    "$BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 \
      --topic cdc.public.orders --from-beginning --timeout-ms 10000 \
      2>/dev/null > /tmp/raw.json || true
    echo "Events by operation:"
    jq -r '.op' /tmp/raw.json | sort | uniq -c | awk '{ printf "  %s: %d\n", $2, $1 }'
    echo "Last event:"
    tail -n 1 /tmp/raw.json | jq '{op, before, after,
      source: {connector: .source.connector, table: .source.table, lsn: .source.lsn,
               snapshot: .source.snapshot}}'
    ;;
  queries)
    psql -q -c "SELECT pg_stat_statements_reset();" > /dev/null
    echo "Counting the queries for ${2:-30} seconds..."
    sleep "${2:-30}"
    psql -q -P footer=off -c "
      SELECT r.rolname AS username, coalesce(sum(s.calls), 0) AS queries
      FROM pg_roles r LEFT JOIN pg_stat_statements s ON s.userid = r.oid
      WHERE r.rolname IN ('relay', 'debezium')
      GROUP BY r.rolname ORDER BY r.rolname;"
    ;;
  clear)
    psql -q -c "TRUNCATE orders, outbox, deliveries, processed_messages,
                         sent_emails;"
    echo "Data deleted."
    ;;
  *)
    sed -n '2,13p' "$0"
    ;;
esac
