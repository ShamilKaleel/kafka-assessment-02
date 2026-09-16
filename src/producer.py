#!/usr/bin/env python3
"""Producer: generates random orders, Avro-encodes them, sends to Kafka.

Each order can be tagged with a "simulate-failure" header so the consumer's
retry and Dead Letter Queue paths can be demonstrated on demand:
  --scenario normal     no failures (default)
  --scenario transient  every order fails once, then succeeds when retried
  --scenario permanent  every order fails on all attempts -> DLQ
  --scenario mixed      a repeating mix of the three
  --poison              send one message that is not valid Avro at all
"""

import argparse
import random
import time

from confluent_kafka import Producer

from src import avro_codec, config, console
from src.config import SIMULATE_HEADER
from src.processing import SCENARIOS, scenario_for

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def random_order(order_id: int) -> dict:
    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(order, tag, err, msg):
    if err is not None:
        console.error(f"delivery failed for #{order['orderId']}: {err}")
    else:
        console.sent(order, msg.topic(), msg.partition(), msg.offset(), tag or "")


def send_orders(count=None, interval=1.0, scenario="normal", topic=config.ORDERS_TOPIC,
                bootstrap_servers=config.BOOTSTRAP_SERVERS, start_id=1001):
    """Send `count` random orders (None = forever), one every `interval` seconds."""
    console.banner("Order producer", topic=topic, scenario=scenario,
                   count=count or "until Ctrl+C", interval=f"{interval}s")
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    sent = 0
    try:
        while count is None or sent < count:
            order = random_order(start_id + sent)
            tag = scenario_for(sent, scenario)
            producer.produce(
                topic,
                key=order["orderId"].encode(),
                value=avro_codec.encode(order),
                headers={SIMULATE_HEADER: tag.encode()} if tag else None,
                callback=lambda err, msg, order=order, tag=tag: delivery_report(order, tag, err, msg),
            )
            producer.poll(0)
            sent += 1
            time.sleep(interval)
    except KeyboardInterrupt:
        console.info("\nstopping...")
    finally:
        producer.flush()
    return sent


def send_poison(topic=config.ORDERS_TOPIC, bootstrap_servers=config.BOOTSTRAP_SERVERS):
    """Send one message whose value is not Avro at all (an undecodable 'poison pill')."""
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    producer.produce(topic, key=b"poison", value=b"this is not an avro order")
    producer.flush()
    console.error(f"SENT      #poison  (not valid Avro)  -> {topic}")


def main():
    parser = argparse.ArgumentParser(description="Random order producer")
    parser.add_argument("--count", type=int, default=None, help="number of orders to send (default: run forever)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between sends (default: 1.0)")
    parser.add_argument("--scenario", choices=SCENARIOS, default="normal", help="which failures to simulate (default: normal)")
    parser.add_argument("--poison", action="store_true", help="send one undecodable message and exit")
    args = parser.parse_args()

    if args.poison:
        send_poison()
    else:
        send_orders(count=args.count, interval=args.interval, scenario=args.scenario)


if __name__ == "__main__":
    main()
