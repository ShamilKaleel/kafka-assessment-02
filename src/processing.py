"""Order processing logic: running average and retry handling.

No Kafka in here — everything is plain Python so it can be unit-tested.
"""

import time

# Values of the "simulate-failure" message header. Nothing in this pipeline
# can fail on its own, so the producer tags orders with one of these to
# exercise the assignment's two failure types deterministically.
TRANSIENT = "transient"    # fails on the first attempt, succeeds when retried
PERMANENT = "permanent"    # fails on every attempt -> Dead Letter Queue
SCENARIOS = ("normal", TRANSIENT, PERMANENT, "mixed")
MIXED_CYCLE = ("normal", "normal", TRANSIENT, "normal", PERMANENT)


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


def scenario_for(index: int, scenario: str) -> str | None:
    """Which failure (if any) the producer should tag the index-th order with."""
    if scenario == "mixed":
        scenario = MIXED_CYCLE[index % len(MIXED_CYCLE)]
    return scenario if scenario in (TRANSIENT, PERMANENT) else None


def process_order(order: dict, simulate: str | None, attempt: int) -> None:
    """The unit of work for one order (stands in for e.g. saving it to a
    database or calling a payment service).

    `simulate` is the message's "simulate-failure" header, if any:
      transient -> raises on attempt 1 only, like a timeout that clears up
      permanent -> raises on every attempt, like invalid data
    """
    if order["price"] <= 0:
        raise ProcessingError(f"invalid price {order['price']}")
    if simulate == TRANSIENT and attempt == 1:
        raise ProcessingError("simulated downstream timeout (transient)")
    if simulate == PERMANENT:
        raise ProcessingError("simulated validation error (permanent)")


def process_with_retry(order, simulate, max_attempts, retry_delay, on_failure=None) -> int:
    """Try process_order up to max_attempts times.

    Returns the attempt number that succeeded (1 = first try). Calls
    on_failure(attempt, error, will_retry) after every failed attempt.
    Raises ProcessingError once every attempt has failed.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            process_order(order, simulate, attempt)
            return attempt
        except ProcessingError as e:
            will_retry = attempt < max_attempts
            if on_failure:
                on_failure(attempt, e, will_retry)
            if will_retry:
                time.sleep(retry_delay)
            else:
                raise ProcessingError(f"failed after {max_attempts} attempts: {e}") from None
