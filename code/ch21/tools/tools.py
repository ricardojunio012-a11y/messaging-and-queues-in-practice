"""Lab tool for chapter 21.
  generate [burst] [no-end] -> publishes 4 minutes of orders, with late ones (see
                    the book): one every 20 ms or, with "burst", all at once; with
                    "no-end", without the 12:05 order that closes the last windows
  end        -> publishes only the 12:05 order
  result     -> the windows computed by the three implementations, side by side
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

KAFKA = os.environ["KAFKA_BROKERS"]
START = int(datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc).timestamp() * 1000)
PER_MINUTE = {"southeast": 50, "south": 30, "northeast": 20}


def order(region: str, ms: int) -> dict:
    return {"region": region, "total": 100.0, "ts": START + ms}


def generate(burst: bool = False, with_end: bool = True) -> None:
    # Orders for 4 minutes (12:00 to 12:03), in time order
    events = []
    for minute in range(4):
        for region, quantity in PER_MINUTE.items():
            for i in range(quantity):
                events.append(order(region, minute * 60_000 + i * 60_000 // quantity))
    events.sort(key=lambda e: e["ts"])
    # 10 southeast orders from 12:00:50 to 12:00:59, which arrive when the time is
    # already 12:01:10: late, but within the 30 s tolerance
    on_time = [order("southeast", 50_000 + i * 1000) for i in range(10)]
    # 15 south orders from 12:00:10 to 12:00:24, which arrive at 12:02:10: too late
    too_late = [order("south", 10_000 + i * 1000) for i in range(15)]
    position = next(i for i, e in enumerate(events) if e["ts"] >= START + 70_000)
    events[position:position] = on_time
    position = next(i for i, e in enumerate(events) if e["ts"] >= START + 130_000)
    events[position:position] = too_late
    # An order at 12:05 moves time forward and closes the windows up to 12:03
    if with_end:
        events.append(order("south", 5 * 60_000))
    publish(events, burst)
    print(f"{len(events)} orders published: {len(on_time)} late within the "
          f"tolerance and {len(too_late)} too late")


def publish(events: list, burst: bool) -> None:
    producer = Producer({"bootstrap.servers": KAFKA})
    for event in events:
        # The message timestamp is the event time (Kafka Streams uses this field)
        producer.produce("orders", key=event["region"], value=json.dumps(event),
                         timestamp=event["ts"])
        if not burst:
            producer.flush(5)
            time.sleep(0.02)
    producer.flush(30)


def read(topic: str) -> dict:
    consumer = Consumer({"bootstrap.servers": KAFKA,
                         "group.id": f"reader-{os.getpid()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    windows, empty = {}, 0
    while empty < 5:
        message = consumer.poll(1.0)
        if message is None or message.error():
            empty += 1
            continue
        data = json.loads(message.value())
        windows[(data["window"], data["region"])] = data["orders"]
    consumer.close()
    return windows


def result() -> None:
    topics = {"manual": "windows-manual", "kafka-streams": "windows-kafka-streams",
              "flink": "windows-flink"}
    read_all = {name: read(topic) for name, topic in topics.items()}
    keys = sorted(set().union(*read_all.values()))
    print(f"  {'window':<7} {'region':<9} {'manual':>7} {'k.streams':>10} {'flink':>6}")
    for window, region in keys:
        values = [str(read_all[n].get((window, region), "-")) for n in topics]
        print(f"  {window:<7} {region:<9} "
              f"{values[0]:>7} {values[1]:>10} {values[2]:>6}")


if __name__ == "__main__":
    options = sys.argv[2:]
    commands = {"generate": lambda: generate("burst" in options,
                                             "no-end" not in options),
                "end": lambda: publish([order("south", 5 * 60_000)], False),
                "result": result}
    commands.get(sys.argv[1] if len(sys.argv) > 1 else "", lambda: print(__doc__))()
