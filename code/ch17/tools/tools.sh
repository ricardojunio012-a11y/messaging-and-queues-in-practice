#!/bin/sh
# Lab tool for chapter 17.
#   pending     -> jobs in each queue and, in Redis, the delivered ones without XACK
#   result      -> jobs processed per queue: total, duplicates and time
#   clear       -> empties the three queues and deletes the results
#   pubsub      -> publishes a message to a Redis Pub/Sub channel with no one listening
#   persistence -> how Redis is storing the data (RDB and AOF)

RABBIT="http://guest:guest@rabbitmq:15672/api"
REDIS="redis-cli -h redis"

case "$1" in
  pending)
    pg=$(psql -At -c "SELECT count(*) FROM job_queue")
    rd=$($REDIS XLEN jobs)
    rb=$(curl -s "$RABBIT/queues/%2F/jobs" | jq -r '.messages // 0')
    printf "  postgres: %s\n  redis: %s\n  rabbitmq: %s\n" "$pg" "$rd" "$rb"
    # In Redis, the jobs delivered to a worker and not yet XACKed (the pending list)
    $REDIS XPENDING jobs workers 2> /dev/null | awk '
      NR == 1 { printf "  redis, delivered without XACK: %s\n", $0 }
      NR > 3 && NR % 2 == 0 { w = $0 }
      NR > 3 && NR % 2 == 1 { printf "    %s: %s\n", w, $0 }'
    ;;
  result)
    # Time: from the first job enqueued to the last one processed
    psql -q -P footer=off -c "
      SELECT backend, count(*) AS deliveries, count(DISTINCT job_id) AS jobs,
             count(*) - count(DISTINCT job_id) AS duplicates,
             count(DISTINCT worker) AS workers,
             round(extract(epoch FROM max(processed_at) - min(enqueued_at))
                   ::numeric, 1)
               AS seconds
      FROM deliveries GROUP BY backend ORDER BY backend;"
    ;;
  clear)
    psql -q -c "TRUNCATE job_queue, deliveries;"
    $REDIS XTRIM jobs MAXLEN 0 > /dev/null
    curl -s -o /dev/null -X DELETE "$RABBIT/queues/%2F/jobs/contents"
    echo "Queues and results deleted."
    ;;
  pubsub)
    listeners=$($REDIS PUBLISH notices "order created")
    echo "Message published to the notices channel;" \
         "received by $listeners subscriber(s)."
    ;;
  persistence)
    $REDIS CONFIG GET appendonly | paste - -
    $REDIS CONFIG GET save | paste - -
    $REDIS INFO persistence | grep -E "rdb_last_save_time|aof_enabled" | tr -d '\r'
    ;;
  *)
    sed -n '2,7p' "$0"
    ;;
esac
