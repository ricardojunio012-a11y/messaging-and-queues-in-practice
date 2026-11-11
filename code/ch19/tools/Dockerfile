# AWS CLI, jq and the PostgreSQL client
FROM alpine:3.20
RUN apk add --no-cache aws-cli jq postgresql16-client
COPY tools.sh /tools.sh
ENTRYPOINT ["sh", "/tools.sh"]
