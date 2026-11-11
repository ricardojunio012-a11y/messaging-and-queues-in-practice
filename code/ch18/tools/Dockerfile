# curl and jq (RabbitMQ management API) and the PostgreSQL client
FROM alpine:3.20
RUN apk add --no-cache curl jq postgresql16-client
COPY tools.sh /tools.sh
COPY topology.json /topology.json
ENTRYPOINT ["sh", "/tools.sh"]
