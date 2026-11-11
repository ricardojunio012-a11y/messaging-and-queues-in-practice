# DLQ redrive over AMQP, in the safe order of section 7.4: reads the message without
# removing it, publishes the copy to the source exchange with a broker confirmation and
# only then removes the original from the DLQ. If the tool crashes in the middle, the
# worst case is a duplicate (the original stays in the DLQ), never a lost message.
#   python3 redrive.py                 -> returns the whole DLQ (REDRIVE_PER_SECOND)
#   python3 redrive.py --crash-after 3 -> simulates the tool crashing right after it
#                                         publishes the 3rd copy, before removing it
import os
import sys
import time

import pika

DLQ = "invoices.dlq"
PER_SECOND = float(os.environ.get("REDRIVE_PER_SECOND", "5"))
# Retry control headers, which must not go back to the main queue
CONTROL = {"x-attempt", "x-error", "x-death"}

crash_after = 0
if len(sys.argv) > 2 and sys.argv[1] == "--crash-after":
    crash_after = int(sys.argv[2])

parameters = pika.URLParameters("amqp://guest:guest@rabbitmq:5672")
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.confirm_delivery()  # basic_publish only returns after the broker's confirmation

# Moves only what was already in the DLQ: what goes back there during the redrive waits
total = channel.queue_declare(DLQ, passive=True).method.message_count
moved = 0
while moved < total:
    method, properties, body = channel.basic_get(DLQ, auto_ack=False)
    if method is None:
        break
    headers = {k: v for k, v in (properties.headers or {}).items() if k not in CONTROL}
    # 1. Publishes the copy to the source exchange, with the attempt counter reset
    channel.basic_publish("invoices", "issue", body, pika.BasicProperties(
        message_id=properties.message_id, content_type=properties.content_type,
        delivery_mode=2, headers=headers or None))
    moved += 1
    if moved == crash_after:
        print(f"Redrive: {moved} copies published; the tool crashed before "
              f"removing the last one from the DLQ.", flush=True)
        os._exit(1)  # exits without closing the connection, like a killed process
    # 2. Only now removes the original from the DLQ
    channel.basic_ack(method.delivery_tag)
    # 3. Limits the speed: the redrive is new load on the dependency
    time.sleep(1 / PER_SECOND)

connection.close()
print(f"Redrive: {moved} messages returned to the main queue "
      f"({PER_SECOND:g} per second).")
