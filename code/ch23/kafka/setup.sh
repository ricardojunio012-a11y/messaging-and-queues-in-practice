#!/bin/bash
# Sets up the lab's Kafka through the internal listener (127.0.0.1:9094), the only one
# without authentication, which exists only inside the broker's container:
#   SCRAM users, topics, ACLs and the quota of the reports user
set -e
K=/opt/kafka/bin
B="--bootstrap-server 127.0.0.1:9094"
for pair in "payments:$PASSWORD_PAYMENTS" "finance:$PASSWORD_FINANCE" \
            "marketing:$PASSWORD_MARKETING" "reports:$PASSWORD_REPORTS"; do
  $K/kafka-configs.sh $B --alter --entity-type users --entity-name "${pair%%:*}" \
    --add-config "SCRAM-SHA-512=[iterations=8192,password=${pair#*:}]" >/dev/null
done
for topic in payments reports; do
  $K/kafka-topics.sh $B --create --if-not-exists --topic $topic --partitions 1 \
    --replication-factor 1 >/dev/null
done
acl() { $K/kafka-acls.sh $B --add "$@" >/dev/null; }
# payments writes to the payments topic; finance reads from it, with the finance group
acl --allow-principal User:payments --operation Write --operation Describe \
  --topic payments
acl --allow-principal User:finance --operation Read --operation Describe \
  --topic payments
acl --allow-principal User:finance --operation Read --group finance
# marketing only has its own group: it reads no topic
acl --allow-principal User:marketing --operation Read --group marketing
# reports writes to the reports topic, with a throughput limit
acl --allow-principal User:reports --operation Write --operation Describe \
  --topic reports
$K/kafka-configs.sh $B --alter --entity-type users --entity-name reports \
  --add-config "producer_byte_rate=$REPORTS_QUOTA" >/dev/null
echo "users, topics, ACLs and quota created"
$K/kafka-acls.sh $B --list | grep -E "^Current|principal" | sed 's/^\s*/  /'
