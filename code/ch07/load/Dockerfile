FROM alpine:3.20
RUN apk add --no-cache curl jq postgresql16-client python3 py3-pip \
    && pip install --no-cache-dir --break-system-packages pika==1.3.2
COPY load.sh count-queues.py redrive.py /
ENTRYPOINT ["sh", "/load.sh"]
