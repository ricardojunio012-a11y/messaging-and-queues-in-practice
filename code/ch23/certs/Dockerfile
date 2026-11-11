# Generates a lab certificate authority (CA) and the RabbitMQ certificate
FROM alpine:3.22
RUN apk add --no-cache openssl
COPY generate.sh /generate.sh
ENTRYPOINT ["sh", "/generate.sh"]
