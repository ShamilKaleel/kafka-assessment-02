"""Shared settings for the producer, consumer and DLQ tools."""

from pathlib import Path

BOOTSTRAP_SERVERS = "localhost:9092"

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
CONSUMER_GROUP = "order-consumer-group"

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 1.0

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"

# Message header the producer uses to request a simulated failure
# ("transient" or "permanent") so the retry and DLQ paths can be demonstrated.
SIMULATE_HEADER = "simulate-failure"
