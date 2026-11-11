#!/bin/bash
# Lab tool for chapter 11 (Kafka, Schema Registry and PostgreSQL).
#   publish VERSION N  -> publishes N OrderCreated events in the VERSION format
#                         (v1, v2-breaking or v2-compatible), as CloudEvents
#   check VERSION      -> asks the registry whether VERSION is compatible with the latest
#                         (exits with code 1 if it is not, like a CI step)
#   register VERSION   -> registers VERSION as a new version of the contract
#   versions           -> registered versions and the compatibility mode
#   status             -> emails sent and events rejected by the consumer
#   clear              -> empties the tables
set -e
BIN=/opt/kafka/bin
REGISTRY=http://schema-registry:8081
SUBJECT=orders-value  # the contract for the value of the messages in the "orders" topic

# One event in the format of each version of the contract
event() {  # $1 version, $2 number
  local id="o$(date +%s)-$2"
  case "$1" in
    v1)
      printf '{"orderId":"%s","customer":"Ana","email":"ana@example.com","total":150.0}' \
        "$id" ;;
    v2-breaking)
      printf '{"orderId":"%s","customer":"Ana","customerEmail":"ana@example.com",' "$id"
      printf '"total":150.0}' ;;
    v2-compatible)
      printf '{"orderId":"%s","customer":"Ana","email":"ana@example.com",' "$id"
      printf '"customerEmail":"ana@example.com","channel":"app","total":150.0}' ;;
  esac
}

# Body of the registration request: the JSON schema goes as text inside another JSON
schema_body() {
  jq -n --rawfile schema "/schemas/$1.json" '{schemaType: "JSON", schema: $schema}'
}

case "$1" in
  publish)
    version=$2
    for i in $(seq 1 "${3:-5}"); do
      # CloudEvents headers in binary mode (ce_ prefix), then key and value
      headers="ce_specversion:1.0,ce_id:$(cat /proc/sys/kernel/random/uuid)"
      headers="$headers,ce_source:/pedido-certo/orders-api"
      headers="$headers,ce_type:br.com.pedidocerto.order.created"
      headers="$headers,ce_dataschema:$version,content-type:application/json"
      printf '%s\t%s|%s\n' "$headers" "order-$i" "$(event "$version" "$i")"
    done | "$BIN/kafka-console-producer.sh" --bootstrap-server kafka:9092 --topic orders \
      --reader-property parse.headers=true --reader-property parse.key=true \
      --reader-property key.separator='|' > /dev/null
    echo "Published ${3:-5} events in the $version format."
    ;;
  check)
    response=$(curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      "$REGISTRY/compatibility/subjects/$SUBJECT/versions/latest?verbose=true" \
      -d "$(schema_body "$2")")
    echo "$response" | jq -r '
        "Compatible with the latest version: \(if .is_compatible then "yes" else "no" end)",
        (.messages[] | select(startswith("{errorType")) | capture("errorType:\"(?<t>[A-Z_]+)\"")
         | "  reason: \(.t)")'
    # Exits with code 1 if the version is incompatible, so the CI build fails
    echo "$response" | jq -e .is_compatible > /dev/null
    ;;
  register)
    curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      "$REGISTRY/subjects/$SUBJECT/versions" -d "$(schema_body "$2")" | jq -r '
        if .version then "Registered: version \(.version) of the contract (id \(.id))"
        else "Rejected (\(.error_code)): schema incompatible with an earlier version" end'
    ;;
  versions)
    echo "Compatibility mode: $(curl -s "$REGISTRY/config" | jq -r .compatibilityLevel)"
    echo "Versions of $SUBJECT: $(curl -s "$REGISTRY/subjects/$SUBJECT/versions" | jq -c .)"
    ;;
  status)
    psql -q -At -F ' ' -c "
      SELECT 'Emails sent: ' || (SELECT count(*) FROM sent_emails)
        || ' | events rejected: ' || (SELECT count(*) FROM rejected_events);"
    psql -q -At -F ' | ' -c "
      SELECT 'sent ' || version, count(*)::text FROM sent_emails GROUP BY version
      UNION ALL
      SELECT 'rejected: ' || reason, count(*)::text FROM rejected_events GROUP BY reason
      ORDER BY 1;"
    ;;
  clear)
    psql -q -c "TRUNCATE sent_emails, rejected_events;"
    echo "Tables cleared."
    ;;
  *)
    echo "Commands: publish VERSION N | check VERSION | register VERSION | versions"
    echo "          status | clear"
    ;;
esac
