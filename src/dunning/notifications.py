from typing import Protocol

import structlog

from dunning.cases import Notification

log = structlog.get_logger(__name__)


class Notifier(Protocol):
    async def send(self, notification: Notification) -> None: ...


class LogNotifier:
    """Stand-in for an email/SMS integration: writes the notification to the log."""

    async def send(self, notification: Notification) -> None:
        log.info(
            "customer_notification",
            kind=notification.kind.value,
            invoice_id=notification.invoice_id,
            customer_id=notification.customer_id,
            next_attempt_at=notification.next_attempt_at.isoformat()
            if notification.next_attempt_at
            else None,
        )


class RecordingNotifier:
    """Test double."""

    def __init__(self) -> None:
        self.sent: list[Notification] = []

    async def send(self, notification: Notification) -> None:
        self.sent.append(notification)
