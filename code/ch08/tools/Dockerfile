# The Kafka command-line tools, plus the PostgreSQL client
FROM apache/kafka:4.3.2
USER root
RUN apk add --no-cache postgresql-client
COPY tools.sh /tools.sh
USER appuser
ENTRYPOINT ["bash", "/tools.sh"]
