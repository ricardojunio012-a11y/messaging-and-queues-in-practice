# The Kafka command-line tools, plus curl, jq and the PostgreSQL client
FROM apache/kafka:4.3.2
USER root
RUN apk add --no-cache curl jq postgresql-client
COPY tools.sh /tools.sh
COPY schemas /schemas
USER appuser
ENTRYPOINT ["bash", "/tools.sh"]
