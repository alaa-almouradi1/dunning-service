"""State transitions of a dunning case.

Both the event consumer and the retry scheduler learn about payment
outcomes (from Kafka and from the billing API response respectively), in
either order and possibly twice. These functions are therefore written so
that applying the same fact again is harmless.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from dunning.db import CaseStatus, DunningCase
from dunning.policy import RetryPolicy


class NotificationKind(StrEnum):
    PAYMENT_FAILED = "payment_failed"
    RECOVERED = "recovered"
    EXHAUSTED = "exhausted"


@dataclass(frozen=True)
class Notification:
    """Something the customer should hear about. Sent after commit."""

    kind: NotificationKind
    invoice_id: str
    customer_id: str
    next_attempt_at: datetime | None = None


def apply_failure(
    case: DunningCase,
    policy: RetryPolicy,
    failed_attempts: int,
    failure_code: str | None,
    failed_at: datetime,
) -> Notification | None:
    """Record that the invoice has now failed ``failed_attempts`` times.

    Counts only move forward: an older or repeated report is ignored.
    """
    if not case.status.is_active or failed_attempts <= case.failed_attempts:
        return None

    case.failed_attempts = failed_attempts
    case.last_failure_code = failure_code
    next_attempt = policy.next_attempt_at(failed_attempts, failed_at)

    if next_attempt is None:
        # Due immediately: the scheduler writes off the invoice and cancels.
        case.status = CaseStatus.EXHAUSTING
        case.next_attempt_at = failed_at
        return None

    case.status = CaseStatus.RETRYING
    case.next_attempt_at = next_attempt
    return Notification(
        NotificationKind.PAYMENT_FAILED, case.invoice_id, case.customer_id, next_attempt
    )


def apply_recovery(case: DunningCase) -> Notification | None:
    if case.status in (CaseStatus.RECOVERED, CaseStatus.CLOSED):
        return None

    # Even an exhausted case recovers: a written-off invoice can still be paid.
    case.status = CaseStatus.RECOVERED
    case.next_attempt_at = None
    case.closed_reason = "paid"
    return Notification(NotificationKind.RECOVERED, case.invoice_id, case.customer_id)


def apply_closure(case: DunningCase, reason: str) -> None:
    """Stop dunning because the invoice is no longer collectible."""
    if case.status in (CaseStatus.RETRYING, CaseStatus.RETRY_IN_FLIGHT):
        case.status = CaseStatus.CLOSED
        case.next_attempt_at = None
        case.closed_reason = reason


def apply_exhausted(case: DunningCase) -> Notification:
    case.status = CaseStatus.EXHAUSTED
    case.next_attempt_at = None
    case.closed_reason = "retries_exhausted"
    return Notification(NotificationKind.EXHAUSTED, case.invoice_id, case.customer_id)
