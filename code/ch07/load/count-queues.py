# Counts the ready messages in each queue with a passive declaration (AMQP).
# The passive declaration queries the queue right now; the statistics of the HTTP
# management API are sampled and can be stale for idle queues.
import sys

import pika

parameters = pika.URLParameters("amqp://guest:guest@rabbitmq:5672")
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
for queue in sys.argv[1:]:
    total = channel.queue_declare(queue, passive=True).method.message_count
    print(f"  {queue}: {total}")
connection.close()
