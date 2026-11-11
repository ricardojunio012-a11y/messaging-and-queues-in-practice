#!/bin/sh
# Generates the lab certificates once, in the /certs volume:
#   ca.crt         the lab CA, which the clients trust
#   rabbitmq.crt   the server certificate, signed by the CA, valid for "rabbitmq"
#   other-ca.crt   some other CA, which signed nothing (for the TLS error step)
set -e
cd /certs
if [ -f rabbitmq.crt ]; then echo "certificates already exist"; exit 0; fi
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=Lab CA" \
  -keyout ca.key -out ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=rabbitmq" \
  -keyout rabbitmq.key -out rabbitmq.csr 2>/dev/null
printf 'subjectAltName=DNS:rabbitmq\n' > san.ext
openssl x509 -req -in rabbitmq.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 \
  -extfile san.ext -out rabbitmq.crt 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=Other CA" \
  -keyout other-ca.key -out other-ca.crt 2>/dev/null
rm -f rabbitmq.csr san.ext ca.srl
# RabbitMQ runs as another user and needs to read the key
chmod 644 *.crt rabbitmq.key
echo "certificates generated"
