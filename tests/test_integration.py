"""End-to-end check against the real Kafka broker (start it with `make up`).

Skipped automatically when the broker isn't reachable. Uses its own uniquely
named topics and consumer group, and deletes the topics afterwards, so it
never touches the demo's `orders` / `orders-dlq` data.
"""

import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from src.config import BOOTSTRAP_SERVERS, SIMULATE_HEADER
from src.consumer import OrderConsumer
from src.dlq import read_dlq
from src.processing import PERMANENT, TRANSIENT
from src.producer import send_orders, send_poison

pytestmark = pytest.mark.integration


def broker_reachable() -> bool:
    try:
        AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=3)
        return True
    except Exception:
        return False


@pytest.fixture
def topics():
    """Create a fresh orders + DLQ topic pair for this test run, delete them afterwards."""
    if not broker_reachable():
        pytest.skip(f"Kafka broker not reachable at {BOOTSTRAP_SERVERS} (run `make up`)")
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    run_id = uuid.uuid4().hex[:8]
    names = (f"test-orders-{run_id}", f"test-orders-dlq-{run_id}")
    for future in admin.create_topics([NewTopic(n, num_partitions=1, replication_factor=1) for n in names]).values():
        future.result()
    yield names
    for future in admin.delete_topics(list(names), operation_timeout=15).values():
        future.result()


def test_end_to_end_normal_transient_permanent_and_poison(topics):
    orders_topic, dlq_topic = topics

    # normal, transient, normal, permanent  (see MIXED_CYCLE order: n, n, t, n, p)
    send_orders(count=5, interval=0.05, scenario="mixed", topic=orders_topic)
    send_poison(topic=orders_topic)

    consumer = OrderConsumer(topic=orders_topic, dlq_topic=dlq_topic,
                             group_id=f"test-group-{uuid.uuid4().hex[:8]}", retry_delay=0)
    stats = consumer.run(max_messages=6, timeout=60)

    # 3 normal + 1 recovered transient succeed; 1 permanent + 1 poison go to the DLQ
    assert stats.processed == 4
    assert stats.recovered == 1
    assert stats.dlq == 2
    assert stats.average == pytest.approx(consumer.running_average.average)
    assert consumer.running_average.count == 4

    dlq = {m.key: m for m in read_dlq(dlq_topic)}
    assert set(dlq) == {"1005", "poison"}

    permanent = dlq["1005"]
    assert permanent.headers[SIMULATE_HEADER] == PERMANENT
    assert permanent.reason.startswith("failed after 3 attempts")
    assert permanent.headers["original-topic"] == orders_topic
    assert permanent.order["orderId"] == "1005"        # original Avro bytes preserved

    poison = dlq["poison"]
    assert poison.reason.startswith("decode error")
    assert poison.order is None
    assert TRANSIENT not in {m.headers.get(SIMULATE_HEADER) for m in dlq.values()}  # recovered ones never reach the DLQ
