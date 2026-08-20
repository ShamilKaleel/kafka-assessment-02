#!/usr/bin/env python3
"""Consumer: reads orders from Kafka and Avro-decodes them."""

import argparse
import io
import random
import time

from confluent_kafka import Consumer
from fastavro import schemaless_reader
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SCHEMA_PATH = "order.avsc"
GROUP_ID = "order-consumer-group"


class ProcessingError(Exception):
    """Raised by the simulated failure injector below."""


def process_with_retry(order, fail_rate, max_retries, retry_delay):
    """Simulate processing that may temporarily fail, retrying up to max_retries times.

    fail_rate is a demo knob (default 0.0, i.e. off) since nothing in this
    toy pipeline can genuinely fail on its own. Raises ProcessingError if
    still failing after the last attempt.
    """
    for attempt in range(1, max_retries + 1):
        try:
            if fail_rate and random.random() < fail_rate:
                raise ProcessingError(f"simulated transient failure (attempt {attempt}/{max_retries})")
            return
        except ProcessingError as e:
            print(f"processing failed: {e}", flush=True)
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise


def main():
    parser = argparse.ArgumentParser(description="Order consumer with retry logic")
    parser.add_argument("--fail-rate", type=float, default=0.0, help="probability [0-1] a processing attempt simulates a failure (default: 0.0, off)")
    parser.add_argument("--max-retries", type=int, default=3, help="max processing attempts before treating a message as permanently failed (default: 3)")
    parser.add_argument("--retry-delay", type=float, default=1.0, help="seconds to wait between retries (default: 1.0)")
    args = parser.parse_args()

    schema = load_schema(SCHEMA_PATH)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    total_price = 0.0
    count = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            order = schemaless_reader(io.BytesIO(msg.value()), schema)

            try:
                process_with_retry(order, args.fail_rate, args.max_retries, args.retry_delay)
            except ProcessingError:
                print(
                    f"orderId={order['orderId']} permanently failed after {args.max_retries} attempts",
                    flush=True,
                )
                continue

            count += 1
            total_price += order["price"]
            running_avg = total_price / count
            print(
                f"received orderId={order['orderId']} product={order['product']} "
                f"price={order['price']:.2f} | running avg={running_avg:.2f} (n={count}) "
                f"[partition {msg.partition()}, offset {msg.offset()}]",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
