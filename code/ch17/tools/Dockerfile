# curl and jq (RabbitMQ API), redis-cli and the PostgreSQL client
FROM alpine:3.20
RUN apk add --no-cache curl jq redis postgresql16-client
COPY tools.sh /tools.sh
ENTRYPOINT ["sh", "/tools.sh"]
