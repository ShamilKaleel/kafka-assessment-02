#!/usr/bin/env python3
"""Producer: generates random orders, Avro-encodes them, sends to Kafka."""

import argparse
import io
import os
import random
import time

from confluent_kafka import Producer
from fastavro import schemaless_writer
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "order.avsc")
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def random_order(order_id):
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def encode(schema, order):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, order)
    return buf.getvalue()


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", flush=True)
    else:
        print(
            f"sent orderId={msg.key().decode()} -> "
            f"{msg.topic()} [partition {msg.partition()}, offset {msg.offset()}]",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description="Random order producer")
    parser.add_argument("--count", type=int, default=None, help="number of orders to send (default: run forever)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between sends (default: 1.0)")
    args = parser.parse_args()

    schema = load_schema(SCHEMA_PATH)
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    order_id = 1001
    sent = 0
    try:
        while args.count is None or sent < args.count:
            order = random_order(order_id)
            producer.produce(
                TOPIC,
                key=order["orderId"].encode(),
                value=encode(schema, order),
                callback=delivery_report,
            )
            producer.poll(0)
            order_id += 1
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nstopping...", flush=True)
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
