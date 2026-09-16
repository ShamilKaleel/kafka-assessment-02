"""Dead Letter Queue: sending failed messages to it, and reading it back."""

from dataclasses import dataclass, field

from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

from src import avro_codec
from src.config import BOOTSTRAP_SERVERS, DLQ_TOPIC


def send_to_dlq(producer: Producer, msg, reason: str, dlq_topic: str = DLQ_TOPIC, on_delivery=None):
    """Forward the original message bytes and key to the DLQ, adding headers
    that say why it failed and where it came from."""
    headers = list(msg.headers() or []) + [
        ("error", reason.encode()),
        ("original-topic", msg.topic().encode()),
        ("original-partition", str(msg.partition()).encode()),
        ("original-offset", str(msg.offset()).encode()),
    ]
    producer.produce(dlq_topic, key=msg.key(), value=msg.value(), headers=headers, callback=on_delivery)


@dataclass
class DlqMessage:
    offset: int
    key: str
    headers: dict = field(default_factory=dict)
    order: dict | None = None
    raw: bytes = b""

    @property
    def reason(self) -> str:
        return self.headers.get("error", "?")

    @property
    def origin(self) -> str:
        return (f"{self.headers.get('original-topic', '?')} "
                f"[partition {self.headers.get('original-partition', '?')}, "
                f"offset {self.headers.get('original-offset', '?')}]")


def read_dlq(dlq_topic: str = DLQ_TOPIC, bootstrap_servers: str = BOOTSTRAP_SERVERS) -> list[DlqMessage]:
    """Read every message currently in the DLQ, from the beginning.

    Assigns partitions directly (no consumer-group join, so it starts
    instantly) and stops as soon as each partition's end offset is reached.
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "dlq-reader",
        "enable.auto.commit": False,
    })
    messages = []
    try:
        topic = consumer.list_topics(dlq_topic, timeout=10).topics.get(dlq_topic)
        if topic is None or topic.error is not None or not topic.partitions:
            return messages

        partitions = [TopicPartition(dlq_topic, p, OFFSET_BEGINNING) for p in topic.partitions]
        consumer.assign(partitions)
        end_offsets = {}
        for tp in partitions:
            low, high = consumer.get_watermark_offsets(tp, timeout=10)
            if high > low:
                end_offsets[tp.partition] = high

        empty_polls = 0
        while end_offsets and empty_polls < 30:
            msg = consumer.poll(1.0)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                continue
            empty_polls = 0
            messages.append(_to_dlq_message(msg))
            if msg.offset() + 1 >= end_offsets.get(msg.partition(), 0):
                end_offsets.pop(msg.partition(), None)
    finally:
        consumer.close()
    return messages


def _to_dlq_message(msg) -> DlqMessage:
    headers = {k: v.decode() for k, v in (msg.headers() or [])}
    try:
        order = avro_codec.decode(msg.value())
    except Exception:
        order = None
    return DlqMessage(
        offset=msg.offset(),
        key=msg.key().decode() if msg.key() else "<none>",
        headers=headers,
        order=order,
        raw=msg.value(),
    )
