FROM alpine:3.20
RUN apk add --no-cache curl jq
COPY tools.sh /tools.sh
ENTRYPOINT ["sh", "/tools.sh"]
