from datetime import UTC, datetime

import pytest

from dunning.policy import RetryPolicy

FAILED_AT = datetime(2026, 10, 1, 9, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("failed_attempts", "expected_day"),
    [(1, 2), (2, 4), (3, 6), (4, 8)],
)
def test_retries_follow_the_schedule(failed_attempts: int, expected_day: int) -> None:
    policy = RetryPolicy((1, 3, 5, 7))

    next_attempt = policy.next_attempt_at(failed_attempts, FAILED_AT)

    assert next_attempt == datetime(2026, 10, expected_day, 9, 30, tzinfo=UTC)


def test_gives_up_after_the_last_scheduled_retry() -> None:
    policy = RetryPolicy((1, 3, 5, 7))

    assert policy.max_attempts == 5
    assert policy.next_attempt_at(5, FAILED_AT) is None


def test_invalid_input_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        RetryPolicy(())
    with pytest.raises(ValueError, match="positive"):
        RetryPolicy((1, 0))
    with pytest.raises(ValueError, match="starts at 1"):
        RetryPolicy().next_attempt_at(0, FAILED_AT)
    with pytest.raises(ValueError, match="timezone-aware"):
        RetryPolicy().next_attempt_at(1, datetime(2026, 1, 1))  # noqa: DTZ001
