#!/usr/bin/env python3
"""Consumer: reads orders from Kafka and Avro-decodes them."""

import io

from confluent_kafka import Consumer
from fastavro import schemaless_reader
from fastavro.schema import load_schema

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SCHEMA_PATH = "order.avsc"
GROUP_ID = "order-consumer-group"


def main():
    schema = load_schema(SCHEMA_PATH)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            order = schemaless_reader(io.BytesIO(msg.value()), schema)
            print(
                f"received orderId={order['orderId']} product={order['product']} "
                f"price={order['price']:.2f} "
                f"[partition {msg.partition()}, offset {msg.offset()}]",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
