#!/usr/bin/env python3
"""Consumer: reads Avro-encoded orders from Kafka, keeps a running average
of prices, retries temporary failures, and routes permanent failures to a
Dead Letter Queue."""

import argparse
import io
import os
import random
import time

from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "order.avsc")
GROUP_ID = "order-consumer-group"


class ProcessingError(Exception):
    pass


def process_order(order, fail_rate):
    """The unit of work for one order.

    Nothing in this pipeline can fail on its own, so fail_rate (default 0.0)
    injects a transient failure with that probability to exercise the retry
    and DLQ paths.
    """
    if fail_rate and random.random() < fail_rate:
        raise ProcessingError(f"simulated transient failure for orderId={order['orderId']}")


def process_with_retry(order, fail_rate, max_attempts, retry_delay):
    for attempt in range(1, max_attempts + 1):
        try:
            process_order(order, fail_rate)
            return
        except ProcessingError as e:
            print(f"processing failed (attempt {attempt}/{max_attempts}): {e}", flush=True)
            if attempt < max_attempts:
                time.sleep(retry_delay)
            else:
                raise ProcessingError(f"failed after {max_attempts} attempts: {e}") from None


def dlq_delivery_report(err, msg):
    key = msg.key().decode() if msg.key() else "<none>"
    if err is not None:
        print(f"DLQ delivery failed for key={key}: {err}", flush=True)
    else:
        print(
            f"routed to DLQ key={key} -> "
            f"{msg.topic()} [partition {msg.partition()}, offset {msg.offset()}]",
            flush=True,
        )


def send_to_dlq(dlq_producer, msg, reason):
    dlq_producer.produce(
        DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers={
            "error": reason.encode(),
            "original-topic": msg.topic().encode(),
            "original-partition": str(msg.partition()).encode(),
            "original-offset": str(msg.offset()).encode(),
        },
        callback=dlq_delivery_report,
    )


def main():
    parser = argparse.ArgumentParser(description="Order consumer: running average, retry logic, DLQ")
    parser.add_argument("--fail-rate", type=float, default=0.0, help="probability [0-1] a processing attempt simulates a failure (default: 0.0, off)")
    parser.add_argument("--max-attempts", type=int, default=3, help="processing attempts before a message is treated as permanently failed (default: 3)")
    parser.add_argument("--retry-delay", type=float, default=1.0, help="seconds to wait between attempts (default: 1.0)")
    args = parser.parse_args()

    schema = load_schema(SCHEMA_PATH)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    total_price = 0.0
    count = 0

    try:
        while True:
            dlq_producer.poll(0)
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}", flush=True)
                continue

            try:
                order = schemaless_reader(io.BytesIO(msg.value()), schema)
            except Exception as e:
                # Undecodable bytes are permanently failed: no retry can fix them.
                send_to_dlq(dlq_producer, msg, f"decode error: {e}")
                continue

            try:
                process_with_retry(order, args.fail_rate, args.max_attempts, args.retry_delay)
            except ProcessingError as e:
                send_to_dlq(dlq_producer, msg, str(e))
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
        print("\nstopping...", flush=True)
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
