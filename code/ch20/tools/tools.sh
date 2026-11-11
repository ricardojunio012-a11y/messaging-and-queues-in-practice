#!/bin/bash
# Lab tool for chapter 20.
#   topics      -> creates the two topics, with one partition and replicas on brokers 1, 2, 3
#   describe    -> leader, replicas and ISR of each topic
#   count       -> how many messages each topic has (the end offset)
#   group       -> lag and members of the "reconciliation" consumer group
BIN=/opt/kafka/bin
# Uses the first broker that answers
for b in broker1 broker2 broker3; do
  if (echo > "/dev/tcp/$b/9092") 2> /dev/null; then
    KAFKA="$b:9092"; break
  fi
done
[ -z "$KAFKA" ] && { echo "No broker answered."; exit 1; }

case "$1" in
  topics)
    # The replica order (1, 2, 3) makes broker 1 the preferred leader
    "$BIN/kafka-topics.sh" --bootstrap-server "$KAFKA" --create --topic payments-risky \
      --replica-assignment 1:2:3 --config min.insync.replicas=1 \
      --config unclean.leader.election.enable=true
    "$BIN/kafka-topics.sh" --bootstrap-server "$KAFKA" --create --topic payments-safe \
      --replica-assignment 1:2:3 --config min.insync.replicas=2 \
      --config unclean.leader.election.enable=false
    ;;
  describe)
    "$BIN/kafka-topics.sh" --bootstrap-server "$KAFKA" --describe \
      --topic 'payments-.*' 2>/dev/null | grep "Partition:" |
      awk '{ for (i = 1; i < NF; i++) {
               if ($i == "Topic:") t = $(i + 1)
               if ($i == "Leader:") l = $(i + 1)
               if ($i == "Replicas:") r = $(i + 1)
               if ($i == "Isr:") s = ($(i + 1) ~ /^[0-9]/) ? $(i + 1) : "(empty)" }
             printf "  %s: leader %s, replicas %s, ISR %s\n", t, l, r, s }'
    ;;
  count)
    "$BIN/kafka-get-offsets.sh" --bootstrap-server "$KAFKA" --topic 'payments-.*' 2>/dev/null |
      awk -F: '{ printf "  %s: %s messages\n", $1, $3 }'
    ;;
  group)
    # Per partition: how far the group has committed, the log end and the gap (the lag)
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA" --describe \
      --group reconciliation 2>/dev/null | awk '$1 == "reconciliation" {
        printf "  %s, partition %s: committed %s, end %s, lag %s\n", $2, $3, $4, $5, $6 }'
    # The group members and how many partitions each one got
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA" --describe \
      --group reconciliation --members 2>/dev/null | awk '$1 == "reconciliation" {
        printf "  member %s: %s partition(s)\n", $3, $5 }'
    ;;
  *)
    sed -n '2,6p' "$0"
    ;;
esac
