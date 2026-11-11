#!/bin/sh
# Lab tool for chapter 18.
#   topology               -> imports tools/topology.json into the cluster (definitions)
#   queues                 -> queues, type, messages and consumers
#   publish EXCHANGE KEY [HEADER=VALUE] -> publishes a test message
#   cluster                -> cluster nodes and whether they are running
#   leaders                -> leader and members of each quorum queue
#   count                  -> confirmed by the broker vs. received by the consumer
#   clear                  -> deletes the counts and empties the queues

# Uses the first node that answers
API=""
for node in rabbit1 rabbit2 rabbit3; do
  if curl -s -o /dev/null -m 3 "http://guest:guest@$node:15672/api/overview"; then
    API="http://guest:guest@$node:15672/api"; break
  fi
done
[ -z "$API" ] && { echo "No RabbitMQ node answered."; exit 1; }

case "$1" in
  topology)
    curl -s -o /dev/null -w "Topology imported (HTTP %{http_code}).\n" -X POST \
      "$API/definitions" -H 'Content-Type: application/json' -d @/topology.json
    ;;
  queues)
    curl -s "$API/queues/%2F" | jq -r '.[] |
      "  \(.name) [\(.type)]: \(.messages // 0) message(s), \(.consumers // 0) consumer(s)"'
    ;;
  publish)
    headers="{}"
    [ -n "$4" ] && headers="{\"${4%%=*}\":\"${4#*=}\"}"
    curl -s -X POST "$API/exchanges/%2F/$2/publish" -H 'Content-Type: application/json' \
      -d "{\"properties\":{\"delivery_mode\":2,\"headers\":$headers},
           \"routing_key\":\"$3\",\"payload\":\"test\",\"payload_encoding\":\"string\"}" |
      jq -r '"Published to \"'"$2"'\" with key \"'"$3"'\": routed = \(.routed)"'
    ;;
  cluster)
    curl -s "$API/nodes" |
      jq -r '.[] | "  \(.name): \(if .running then "running" else "down" end)"'
    ;;
  leaders)
    curl -s "$API/queues/%2F" | jq -r '.[] | select(.type == "quorum") |
      "  \(.name): leader \(.leader // "none"), members \(.members | length),"
      + " online \(.online | length)"'
    ;;
  count)
    psql -q -P footer=off -c "
      SELECT (SELECT count(*) FROM confirmed)                            AS confirmed,
             (SELECT count(*) FROM received)                             AS received,
             (SELECT count(*) FROM confirmed c
              WHERE NOT EXISTS (SELECT 1 FROM received r
                                WHERE r.order_id = c.order_id))          AS not_received,
             (SELECT coalesce(sum(deliveries - 1), 0) FROM received)     AS redeliveries;"
    ;;
  clear)
    psql -q -c "TRUNCATE confirmed, received;"
    for queue in inventory.reserve notifications.email loyalty.points dashboard.alerts \
                 reports.pdf orders.dead; do
      curl -s -o /dev/null -X DELETE "$API/queues/%2F/$queue/contents"
    done
    echo "Counts deleted and queues emptied."
    ;;
  *)
    sed -n '2,10p' "$0"
    ;;
esac
