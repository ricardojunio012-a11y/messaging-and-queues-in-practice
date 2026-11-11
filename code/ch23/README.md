# Chapter 23: security

- `certs`: generates the lab CA and the RabbitMQ certificate in the `certs` volume
- `rabbitmq`: RabbitMQ 4.3 with TLS only (port 5671); vhost `store`, users and permissions
  in `rabbitmq/definitions.json`
- `kafka`: Kafka 4.3 with SASL/SCRAM-SHA-512 on port 9092, ACLs (`StandardAuthorizer`) and
  an internal listener, only on 127.0.0.1, for the setup
- `kafka-init`: creates the SCRAM users, topics, ACLs and the quota of `reports`
  (`kafka/setup.sh`)
- `rabbit` and `kafka-client`: TypeScript clients that connect with the given user

The lab passwords are in `.env` (the RabbitMQ ones also in `definitions.json`, as
hashes). They are only for this local environment.

## Names used in this chapter

- RabbitMQ: vhost `store`, exchange `orders` (topic), routing key `order.created`,
  quorum queues `notifications` and `marketing` bound with `order.#`; users `admin`,
  `orders-api`, `notifications` and `marketing`
- Kafka: users `payments`, `finance`, `marketing` and `reports`; topics `payments` and
  `reports`; listeners `CLIENTS` (SASL/SCRAM) and `INTERNAL` (127.0.0.1 only)
- Variables: `PASSWORD_<USER>` (for example `PASSWORD_ORDERS_API`) and `REPORTS_QUOTA`
- Certificates: `ca.crt`, `rabbitmq.crt` and `other-ca.crt`

## How to run

1. Start: `docker compose up -d --build --wait` and `docker compose --profile tools build`
2. RabbitMQ: `docker compose run --rm rabbit publish orders-api`,
   `docker compose run --rm rabbit read marketing notifications`
3. Kafka: `docker compose run --rm kafka-client publish payments payments 100`,
   `docker compose run --rm kafka-client read marketing payments`
4. Password change without stopping: with `publish reports reports 5000` running, change
   the password of `reports` with `kafka-configs.sh --alter` through the internal
   listener (section 23.9, step 6) and try a new connection with the old password
5. Shut down: `docker compose --profile tools down -v`

## Expected results

| Situation | Result |
|---|---|
| Port 5672, without TLS | connection refused |
| TLS trusting another CA | `self-signed certificate in certificate chain` |
| `marketing` reads the `notifications` queue | `ACCESS_REFUSED` (403) |
| Wrong password in Kafka | SASL authentication failure |
| `marketing` reads the `payments` topic | 0 messages; `Topic authorization failed` in the log |
| `reports` publishes 1 MB with a 50 KB/s quota | about 13 s, against 1 s without a quota |
| `reports` password changed during the send | the open send finishes; a new connection with the old password is refused |

The full walkthrough is in section 23.9 of the book.
