"""Running-average and retry logic checks (pure Python, no Kafka needed)."""

import pytest

from src.processing import (
    MIXED_CYCLE, PERMANENT, TRANSIENT, ProcessingError, RunningAverage,
    process_order, process_with_retry, scenario_for,
)

ORDER = {"orderId": "1001", "product": "Item1", "price": 50.0}


# --- real-time aggregation -------------------------------------------------

def test_running_average_updates_after_every_order():
    avg = RunningAverage()
    # the example from the assignment explanation: 10, 20, 30 -> 10, 15, 20
    assert avg.add(10) == 10
    assert avg.add(20) == 15
    assert avg.add(30) == 20
    assert avg.count == 3


def test_running_average_is_zero_before_any_order():
    assert RunningAverage().average == 0.0


# --- processing a single order --------------------------------------------

def test_normal_order_processes_without_error():
    process_order(ORDER, simulate=None, attempt=1)


def test_order_with_invalid_price_is_rejected():
    with pytest.raises(ProcessingError, match="invalid price"):
        process_order({**ORDER, "price": 0.0}, simulate=None, attempt=1)


# --- retry logic for temporary failures -----------------------------------

def test_normal_order_succeeds_on_first_attempt():
    assert process_with_retry(ORDER, None, max_attempts=3, retry_delay=0) == 1


def test_transient_failure_is_retried_and_recovers():
    failures = []
    attempt = process_with_retry(
        ORDER, TRANSIENT, max_attempts=3, retry_delay=0,
        on_failure=lambda attempt, _error, will_retry: failures.append((attempt, will_retry)),
    )
    assert attempt == 2                       # failed once, succeeded when retried
    assert failures == [(1, True)]


# --- permanent failures go to the DLQ after the last attempt --------------

def test_permanent_failure_gives_up_after_max_attempts():
    failures = []
    with pytest.raises(ProcessingError, match="failed after 3 attempts"):
        process_with_retry(
            ORDER, PERMANENT, max_attempts=3, retry_delay=0,
            on_failure=lambda attempt, _error, will_retry: failures.append((attempt, will_retry)),
        )
    assert failures == [(1, True), (2, True), (3, False)]


# --- producer-side scenario tagging ---------------------------------------

def test_scenario_tags():
    assert scenario_for(0, "normal") is None
    assert scenario_for(0, TRANSIENT) == TRANSIENT
    assert scenario_for(0, PERMANENT) == PERMANENT


def test_mixed_scenario_cycles_through_all_types():
    tags = [scenario_for(i, "mixed") for i in range(10)]
    assert tags.count(TRANSIENT) == 2
    assert tags.count(PERMANENT) == 2
    assert tags.count(None) == 6
    assert len(MIXED_CYCLE) == 5
