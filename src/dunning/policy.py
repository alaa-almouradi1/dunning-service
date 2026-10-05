from dataclasses import dataclass
from datetime import datetime, timedelta

DEFAULT_SCHEDULE = tuple(timedelta(days=d) for d in (1, 3, 5, 7))


@dataclass(frozen=True)
class RetryPolicy:
    """When to retry a failed invoice payment, and when to give up.

    ``schedule[k]`` is the wait after the (k+1)-th failure. With the default
    of 1, 3, 5 and 7 days: fail on day 0, retry on day 1, 4, 9 and 16; if the
    fifth attempt fails too, the invoice is written off.

    Spacing retries out (instead of retrying every hour) gives customers time
    to top up or replace a card, and avoids hammering the card network, which
    can get a merchant flagged.
    """

    schedule: tuple[timedelta, ...] = DEFAULT_SCHEDULE

    def __post_init__(self) -> None:
        if not self.schedule or any(wait <= timedelta(0) for wait in self.schedule):
            raise ValueError("schedule must be a non-empty tuple of positive durations")

    @classmethod
    def days(cls, *days: float) -> "RetryPolicy":
        return cls(tuple(timedelta(days=d) for d in days))

    @property
    def max_attempts(self) -> int:
        """The original charge plus one retry per schedule entry."""
        return len(self.schedule) + 1

    def next_attempt_at(self, failed_attempts: int, failed_at: datetime) -> datetime | None:
        """When to retry after ``failed_attempts`` failures, or None when exhausted."""
        if failed_attempts < 1:
            raise ValueError("failed_attempts starts at 1")
        if failed_at.tzinfo is None:
            raise ValueError("failed_at must be timezone-aware")

        if failed_attempts > len(self.schedule):
            return None

        return failed_at + self.schedule[failed_attempts - 1]
