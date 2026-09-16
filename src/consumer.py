#!/usr/bin/env python3
"""Consumer: reads Avro-encoded orders from Kafka, keeps a running average
of prices, retries temporary failures, and routes permanent failures to the
Dead Letter Queue."""

import argparse
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, Producer

from src import avro_codec, config, console
from src.config import SIMULATE_HEADER
from src.dlq import send_to_dlq
from src.processing import ProcessingError, RunningAverage, process_with_retry


@dataclass
class Stats:
    processed: int = 0   # orders that succeeded (first try or after retries)
    recovered: int = 0   # ...of which needed at least one retry
    dlq: int = 0         # messages routed to the DLQ
    average: float = 0.0


class OrderConsumer:
    def __init__(self, topic=config.ORDERS_TOPIC, dlq_topic=config.DLQ_TOPIC,
                 group_id=config.CONSUMER_GROUP, bootstrap_servers=config.BOOTSTRAP_SERVERS,
                 max_attempts=config.MAX_ATTEMPTS, retry_delay=config.RETRY_DELAY_SECONDS):
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.group_id = group_id
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "log_level": 3,  # errors only; hides librdkafka notices during shutdown
        })
        self.dlq_producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.running_average = RunningAverage()
        self.stats = Stats()

    def run(self, max_messages=None, timeout=None) -> Stats:
        """Consume until Ctrl+C, or until max_messages reached a final state
        (processed or DLQ'd), or timeout seconds have passed."""
        console.banner("Order consumer", topic=self.topic, dlq=self.dlq_topic,
                       group=self.group_id, attempts=self.max_attempts, delay=f"{self.retry_delay}s")
        self.consumer.subscribe([self.topic])
        deadline = time.monotonic() + timeout if timeout else None
        finished = 0
        try:
            while True:
                if max_messages is not None and finished >= max_messages:
                    break
                if deadline and time.monotonic() > deadline:
                    break
                self.dlq_producer.poll(0)
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    console.error(f"consumer error: {msg.error()}")
                    continue
                self.handle(msg)
                finished += 1
        except KeyboardInterrupt:
            console.info("\nstopping...")
        finally:
            self.consumer.close()
            self.dlq_producer.flush()
            console.summary(self.stats.processed, self.stats.recovered, self.stats.dlq, self.stats.average)
        return self.stats

    def handle(self, msg):
        try:
            order = avro_codec.decode(msg.value())
        except Exception as e:
            # Undecodable bytes are permanently failed: no retry can fix them.
            self._to_dlq(msg, f"decode error: {e}")
            return

        headers = {k: v.decode() for k, v in (msg.headers() or [])}
        simulate = headers.get(SIMULATE_HEADER)
        try:
            attempt = process_with_retry(
                order, simulate, self.max_attempts, self.retry_delay,
                on_failure=lambda attempt, error, will_retry: self._on_failure(order, attempt, error, will_retry),
            )
        except ProcessingError as e:
            self._to_dlq(msg, str(e))
            return

        avg = self.running_average.add(order["price"])
        self.stats.processed += 1
        self.stats.average = avg
        if attempt > 1:
            self.stats.recovered += 1
            console.recovered(order["orderId"], attempt)
        console.received(order, avg, self.running_average.count, msg.partition(), msg.offset())

    def _on_failure(self, order, attempt, error, will_retry):
        if will_retry:
            console.retry(order["orderId"], attempt, self.max_attempts, str(error), self.retry_delay)
        else:
            console.gave_up(order["orderId"], self.max_attempts, str(error))

    def _to_dlq(self, msg, reason):
        send_to_dlq(self.dlq_producer, msg, reason, self.dlq_topic, on_delivery=self._dlq_delivery_report)
        self.stats.dlq += 1

    @staticmethod
    def _dlq_delivery_report(err, msg):
        key = msg.key().decode() if msg.key() else "<none>"
        if err is not None:
            console.error(f"DLQ delivery failed for key={key}: {err}")
        else:
            console.dlq(key, msg.topic(), msg.partition(), msg.offset())


def main():
    parser = argparse.ArgumentParser(description="Order consumer: running average, retry logic, DLQ")
    parser.add_argument("--max-attempts", type=int, default=config.MAX_ATTEMPTS, help=f"processing attempts before a message is treated as permanently failed (default: {config.MAX_ATTEMPTS})")
    parser.add_argument("--retry-delay", type=float, default=config.RETRY_DELAY_SECONDS, help=f"seconds to wait between attempts (default: {config.RETRY_DELAY_SECONDS})")
    args = parser.parse_args()

    OrderConsumer(max_attempts=args.max_attempts, retry_delay=args.retry_delay).run()


if __name__ == "__main__":
    main()
