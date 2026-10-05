from datetime import UTC, datetime, timedelta

import pytest

from dunning.config import Settings
from dunning.policy import RetryPolicy

FAILED_AT = datetime(2026, 10, 1, 9, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    ("failed_attempts", "expected_day"),
    [(1, 2), (2, 4), (3, 6), (4, 8)],
)
def test_retries_follow_the_schedule(failed_attempts: int, expected_day: int) -> None:
    policy = RetryPolicy()

    next_attempt = policy.next_attempt_at(failed_attempts, FAILED_AT)

    assert next_attempt == datetime(2026, 10, expected_day, 9, 30, tzinfo=UTC)


def test_gives_up_after_the_last_scheduled_retry() -> None:
    policy = RetryPolicy()

    assert policy.max_attempts == 5
    assert policy.next_attempt_at(5, FAILED_AT) is None


def test_invalid_input_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        RetryPolicy(())
    with pytest.raises(ValueError, match="positive"):
        RetryPolicy.days(1, 0)
    with pytest.raises(ValueError, match="starts at 1"):
        RetryPolicy().next_attempt_at(0, FAILED_AT)
    with pytest.raises(ValueError, match="timezone-aware"):
        RetryPolicy().next_attempt_at(1, datetime(2026, 1, 1))  # noqa: DTZ001


def test_short_schedules_make_live_demos_possible() -> None:
    policy = RetryPolicy((timedelta(minutes=1), timedelta(minutes=2)))

    assert policy.next_attempt_at(2, FAILED_AT) == FAILED_AT + timedelta(minutes=2)


def test_the_schedule_is_configurable_with_iso_durations(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUNNING_RETRY_SCHEDULE", '["PT1M", "PT2M", "P1D"]')

    settings = Settings()

    assert settings.retry_schedule == [
        timedelta(minutes=1),
        timedelta(minutes=2),
        timedelta(days=1),
    ]


def test_non_positive_durations_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DUNNING_RETRY_SCHEDULE", '["PT0S"]')

    with pytest.raises(ValueError, match="positive durations"):
        Settings()
