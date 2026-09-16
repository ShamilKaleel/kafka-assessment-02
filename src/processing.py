"""Order processing logic: running average and retry handling.

No Kafka in here — everything is plain Python so it can be unit-tested.
"""

import random
import time


class ProcessingError(Exception):
    pass


class RunningAverage:
    """Real-time aggregation: the average price of all orders processed so far."""

    def __init__(self):
        self.count = 0
        self.total = 0.0

    def add(self, price: float) -> float:
        self.count += 1
        self.total += price
        return self.average

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0


def process_order(order: dict, fail_rate: float = 0.0) -> None:
    """The unit of work for one order.

    Nothing in this pipeline can fail on its own, so fail_rate (default 0.0)
    injects a transient failure with that probability to exercise the retry
    and DLQ paths.
    """
    if fail_rate and random.random() < fail_rate:
        raise ProcessingError(f"simulated transient failure for orderId={order['orderId']}")


def process_with_retry(order, fail_rate, max_attempts, retry_delay, on_failure=None) -> int:
    """Try process_order up to max_attempts times.

    Returns the attempt number that succeeded (1 = first try). Calls
    on_failure(attempt, error, will_retry) after every failed attempt.
    Raises ProcessingError once every attempt has failed.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            process_order(order, fail_rate)
            return attempt
        except ProcessingError as e:
            will_retry = attempt < max_attempts
            if on_failure:
                on_failure(attempt, e, will_retry)
            if will_retry:
                time.sleep(retry_delay)
            else:
                raise ProcessingError(f"failed after {max_attempts} attempts: {e}") from None
