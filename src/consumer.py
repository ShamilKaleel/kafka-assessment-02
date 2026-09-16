#!/usr/bin/env python3
"""Consumer: reads Avro-encoded orders from Kafka, keeps a running average
of prices, retries temporary failures, and routes permanent failures to the
Dead Letter Queue."""

import argparse
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, Producer

from src import avro_codec, config
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
                 max_attempts=config.MAX_ATTEMPTS, retry_delay=config.RETRY_DELAY_SECONDS,
                 fail_rate=0.0):
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.fail_rate = fail_rate
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        })
        self.dlq_producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.running_average = RunningAverage()
        self.stats = Stats()

    def run(self, max_messages=None, timeout=None) -> Stats:
        """Consume until Ctrl+C, or until max_messages reached a final state
        (processed or DLQ'd), or timeout seconds have passed."""
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
                    print(f"consumer error: {msg.error()}", flush=True)
                    continue
                self.handle(msg)
                finished += 1
        except KeyboardInterrupt:
            print("\nstopping...", flush=True)
        finally:
            self.consumer.close()
            self.dlq_producer.flush()
        return self.stats

    def handle(self, msg):
        try:
            order = avro_codec.decode(msg.value())
        except Exception as e:
            # Undecodable bytes are permanently failed: no retry can fix them.
            self._to_dlq(msg, f"decode error: {e}")
            return

        try:
            attempt = process_with_retry(order, self.fail_rate, self.max_attempts,
                                         self.retry_delay, on_failure=self._on_failure)
        except ProcessingError as e:
            self._to_dlq(msg, str(e))
            return

        avg = self.running_average.add(order["price"])
        self.stats.processed += 1
        self.stats.average = avg
        if attempt > 1:
            self.stats.recovered += 1
        print(
            f"received orderId={order['orderId']} product={order['product']} "
            f"price={order['price']:.2f} | running avg={avg:.2f} (n={self.running_average.count}) "
            f"[partition {msg.partition()}, offset {msg.offset()}]",
            flush=True,
        )

    def _on_failure(self, attempt, error, will_retry):
        outcome = f"retrying in {self.retry_delay}s" if will_retry else "giving up"
        print(f"processing failed (attempt {attempt}/{self.max_attempts}): {error} - {outcome}", flush=True)

    def _to_dlq(self, msg, reason):
        send_to_dlq(self.dlq_producer, msg, reason, self.dlq_topic, on_delivery=self._dlq_delivery_report)
        self.stats.dlq += 1

    @staticmethod
    def _dlq_delivery_report(err, msg):
        key = msg.key().decode() if msg.key() else "<none>"
        if err is not None:
            print(f"DLQ delivery failed for key={key}: {err}", flush=True)
        else:
            print(
                f"routed to DLQ key={key} -> {msg.topic()} "
                f"[partition {msg.partition()}, offset {msg.offset()}]",
                flush=True,
            )


def main():
    parser = argparse.ArgumentParser(description="Order consumer: running average, retry logic, DLQ")
    parser.add_argument("--fail-rate", type=float, default=0.0, help="probability [0-1] a processing attempt simulates a failure (default: 0.0, off)")
    parser.add_argument("--max-attempts", type=int, default=config.MAX_ATTEMPTS, help=f"processing attempts before a message is treated as permanently failed (default: {config.MAX_ATTEMPTS})")
    parser.add_argument("--retry-delay", type=float, default=config.RETRY_DELAY_SECONDS, help=f"seconds to wait between attempts (default: {config.RETRY_DELAY_SECONDS})")
    args = parser.parse_args()

    OrderConsumer(max_attempts=args.max_attempts, retry_delay=args.retry_delay,
                  fail_rate=args.fail_rate).run()


if __name__ == "__main__":
    main()
