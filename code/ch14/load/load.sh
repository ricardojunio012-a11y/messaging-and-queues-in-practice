#!/bin/sh
# Lab tool for chapter 14.
#   docker compose run --rm load orders [N] [SKU] -> creates N orders of the SKU
#                                (default: 5 tshirts; the "sold-out" SKU has no stock)
#   docker compose run --rm load status  -> orders, payments, shipments and stock
#   docker compose run --rm load flow    -> the saga messages of the last order
#   docker compose run --rm load sagas   -> the state of the orchestrator's sagas
#   docker compose run --rm load clear   -> deletes the data and restocks

API="${API_URL:-http://orders-api:8080}"

case "$1" in
  orders)
    n="${2:-5}"; sku="${3:-tshirt}"
    echo "Creating $n order(s) of $sku..."
    for i in $(seq 1 "$n"); do
      curl -s -o /dev/null -m 30 -w '%{http_code}\n' -X POST "$API/orders" \
        -H 'Content-Type: application/json' -d "{\"sku\":\"$sku\",\"amount\":89.9}"
    done | sort | uniq -c | awk '{ printf "  HTTP %s: %d\n", $2, $1 }'
    ;;
  status)
    psql -q -P footer=off -c "
      SELECT count(*) FILTER (WHERE status = 'confirmed')                  AS confirmed,
             count(*) FILTER (WHERE status = 'cancelled')                  AS cancelled,
             count(*) FILTER (WHERE status = 'pending')                    AS pending,
             (SELECT count(*) FROM payments WHERE status = 'authorized')   AS paid,
             (SELECT count(*) FROM payments WHERE status = 'refunded')     AS refunded,
             (SELECT count(*) FROM shipments)                              AS shipments,
             (SELECT quantity FROM inventory WHERE sku = 'tshirt')         AS stock
      FROM orders;"
    ;;
  flow)
    psql -q -P footer=off -c "
      SELECT to_char(h.at, 'HH24:MI:SS.MS') AS time, h.service, h.message
      FROM history h
      WHERE h.order_id = (SELECT id FROM orders ORDER BY created_at DESC LIMIT 1)
      ORDER BY h.at;"
    psql -q -t -c "SELECT 'Order: ' || status || coalesce(' (' || reason || ')', '')
                   FROM orders ORDER BY created_at DESC LIMIT 1;"
    ;;
  sagas)
    psql -q -P footer=off -c "
      SELECT status, step, coalesce(reason, '') AS reason, count(*) AS sagas
      FROM sagas GROUP BY 1, 2, 3 ORDER BY 1, 2;"
    ;;
  clear)
    psql -q -c "TRUNCATE orders, sagas, payments, reservations, shipments, history;
                UPDATE inventory SET quantity = 100 WHERE sku = 'tshirt';"
    echo "Data deleted and stock restored."
    ;;
  *)
    sed -n '2,9p' "$0"
    ;;
esac
