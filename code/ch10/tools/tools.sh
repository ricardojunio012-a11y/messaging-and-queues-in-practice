#!/bin/sh
# Lab tool for chapter 10.
#   order         -> creates an order and shows its trace, as in Jaeger
#   trace ID      -> shows the spans of a trace, as a tree, with the duration of each one
#   load N        -> creates N orders at once
#   queues        -> ready messages and the age of the oldest one, according to Prometheus
#   alerts        -> Prometheus alerts (pending and firing)

API=http://orders-api:8080
JAEGER=http://jaeger:16686
PROMETHEUS=http://prometheus:9090

# Fetches the trace from Jaeger and prints the spans as a tree: each span followed by
# its children, in start order
show_trace() {
  curl -s "$JAEGER/api/traces/$1" | jq -r '
    .data[0] as $t
    | ($t.spans | map(.startTime) | min) as $start
    | ($t.spans | map(.spanID)) as $ids
    | def parent: [.references[]? | select(.refType == "CHILD_OF") | .spanID][0];
      def line($level): ("  " * $level) + $t.processes[.processID].serviceName + ": "
        + .operationName + "  (start +" + (((.startTime - $start) / 1000) | floor
        | tostring) + " ms, duration " + ((.duration / 1000) | floor | tostring) + " ms)";
      def tree($level): . as $s | ($s | line($level)),
        ([$t.spans[] | select(parent == $s.spanID)] | sort_by(.startTime)[]
         | tree($level + 1));
      [$t.spans[] | select(parent == null or (parent as $p | $ids | index($p) | not))]
      | sort_by(.startTime)[] | tree(0)'
}

case "$1" in
  order)
    response=$(curl -s -X POST "$API/orders" -H 'Content-Type: application/json' \
      -d '{"customer":"Ana","email":"ana@example.com","total":150.0}')
    echo "Response: $response"
    trace=$(echo "$response" | jq -r .traceId)
    # Waits for the consumers to finish and the spans to reach Jaeger (the SDKs
    # send the spans in batches, every 5 s)
    sleep 10
    echo "Trace $trace:"
    show_trace "$trace"
    ;;
  trace)
    show_trace "$2"
    ;;
  load)
    n=${2:-60}
    for i in $(seq 1 "$n"); do
      curl -s -o /dev/null -X POST "$API/orders" -H 'Content-Type: application/json' \
        -d "{\"customer\":\"Customer $i\",\"email\":\"c$i@example.com\",\"total\":99.9}"
    done
    echo "Created $n orders."
    ;;
  queues)
    echo "Ready messages per queue:"
    curl -s "$PROMETHEUS/api/v1/query" --data-urlencode \
      'query=rabbitmq_detailed_queue_messages_ready' \
      | jq -r '.data.result[] | "  \(.metric.queue): \(.value[1])"'
    echo "Age of the oldest message (s):"
    curl -s "$PROMETHEUS/api/v1/query" --data-urlencode \
      'query=(time() - rabbitmq_detailed_queue_head_message_timestamp)
             and rabbitmq_detailed_queue_messages_ready > 0' \
      | jq -r '.data.result[] | "  \(.metric.queue): \(.value[1] | tonumber | floor)"'
    ;;
  alerts)
    curl -s "$PROMETHEUS/api/v1/alerts" | jq -r '
      .data.alerts[] | "  \(.state): \(.labels.alertname) | \(.annotations.summary)"'
    ;;
  *)
    echo "Commands: order | trace ID | load N | queues | alerts"
    ;;
esac
