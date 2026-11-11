FROM alpine:3.20
RUN apk add --no-cache curl jq postgresql16-client
COPY load.sh /load.sh
ENTRYPOINT ["sh", "/load.sh"]
