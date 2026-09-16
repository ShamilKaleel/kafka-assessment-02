#!/usr/bin/env python3
"""Prints everything currently in the Dead Letter Queue, with the reason each
message ended up there. Always reads the whole topic from the beginning."""

import io
import os

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from fastavro import schemaless_reader
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
DLQ_TOPIC = "orders-dlq"
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schemas", "order.avsc")


def print_message(msg, schema):
    headers = {k: v.decode() for k, v in (msg.headers() or [])}
    try:
        body = schemaless_reader(io.BytesIO(msg.value()), schema)
    except Exception:
        body = f"undecodable bytes: {msg.value()!r}"
    key = msg.key().decode() if msg.key() else "<none>"

    print(f"DLQ offset {msg.offset()}  key={key}", flush=True)
    print(f"  reason : {headers.get('error', '?')}", flush=True)
    print(
        f"  from   : {headers.get('original-topic', '?')} "
        f"[partition {headers.get('original-partition', '?')}, "
        f"offset {headers.get('original-offset', '?')}]",
        flush=True,
    )
    print(f"  order  : {body}", flush=True)


def main():
    schema = load_schema(SCHEMA_PATH)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "dlq-reader",
        "enable.auto.commit": False,
    })

    try:
        topic = consumer.list_topics(DLQ_TOPIC, timeout=10).topics.get(DLQ_TOPIC)
        if topic is None or topic.error is not None or not topic.partitions:
            print(f"0 message(s) in {DLQ_TOPIC} (topic does not exist yet)", flush=True)
            return

        # Assign directly (no consumer-group join) so reading starts instantly
        # from the beginning; stop once each partition's end offset is reached.
        partitions = [TopicPartition(DLQ_TOPIC, p, OFFSET_BEGINNING) for p in topic.partitions]
        consumer.assign(partitions)
        end_offsets = {}
        for tp in partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            if high > low:
                end_offsets[tp.partition] = high

        total = 0
        empty_polls = 0
        while end_offsets and empty_polls < 30:
            msg = consumer.poll(1.0)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}", flush=True)
                continue
            empty_polls = 0
            total += 1
            print_message(msg, schema)
            if msg.offset() + 1 >= end_offsets.get(msg.partition(), 0):
                end_offsets.pop(msg.partition(), None)

        print(f"\n{total} message(s) in {DLQ_TOPIC}", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
