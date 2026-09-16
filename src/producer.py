#!/usr/bin/env python3
"""Producer: generates random orders, Avro-encodes them, sends to Kafka."""

import argparse
import random
import time

from confluent_kafka import Producer

from src import avro_codec, config

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def random_order(order_id: int) -> dict:
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}", flush=True)
    else:
        print(
            f"sent orderId={msg.key().decode()} -> "
            f"{msg.topic()} [partition {msg.partition()}, offset {msg.offset()}]",
            flush=True,
        )


def send_orders(count=None, interval=1.0, topic=config.ORDERS_TOPIC,
                bootstrap_servers=config.BOOTSTRAP_SERVERS, start_id=1001):
    """Send `count` random orders (None = forever), one every `interval` seconds."""
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    sent = 0
    try:
        while count is None or sent < count:
            order = random_order(start_id + sent)
            producer.produce(topic, key=order["orderId"].encode(),
                             value=avro_codec.encode(order), callback=delivery_report)
            producer.poll(0)
            sent += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nstopping...", flush=True)
    finally:
        producer.flush()
    return sent


def main():
    parser = argparse.ArgumentParser(description="Random order producer")
    parser.add_argument("--count", type=int, default=None, help="number of orders to send (default: run forever)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between sends (default: 1.0)")
    args = parser.parse_args()
    send_orders(count=args.count, interval=args.interval)


if __name__ == "__main__":
    main()
