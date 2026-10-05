"""Applies billing events to dunning cases, exactly once per event."""

from dataclasses import dataclass, field
from enum import StrEnum
from uuid import UUID

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dunning.cases import Notification, apply_closure, apply_failure, apply_recovery
from dunning.db import CaseStatus, DunningCase, ProcessedEvent
from dunning.events import (
    Envelope,
    InvoiceChanged,
    PaymentFailed,
    PaymentSucceeded,
    SubscriptionChanged,
    parse_data,
)
from dunning.policy import RetryPolicy

log = structlog.get_logger(__name__)


class HandleResult(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    IGNORED = "ignored"


@dataclass
class HandleOutcome:
    result: HandleResult
    notifications: list[Notification] = field(default_factory=list)


class EventHandler:
    def __init__(self, policy: RetryPolicy) -> None:
        self._policy = policy

    async def handle(self, session: AsyncSession, envelope: Envelope) -> HandleOutcome:
        """Apply one event inside the caller's transaction.

        The event ID is stored in the same transaction as its effects, so a
        redelivered event (Kafka is at-least-once) is recognised and skipped.
        """
        event_id = str(envelope.id)

        if await session.get(ProcessedEvent, event_id) is not None:
            return HandleOutcome(HandleResult.DUPLICATE)

        session.add(ProcessedEvent(event_id=event_id, event_type=envelope.type))

        match envelope.type:
            case "payment.failed":
                return await self._payment_failed(session, envelope)
            case "payment.succeeded":
                data = parse_data(envelope, PaymentSucceeded)
                return await self._recovered(session, data.invoice_id)
            case "invoice.paid":
                invoice = parse_data(envelope, InvoiceChanged)
                return await self._recovered(session, invoice.invoice_id)
            case "invoice.voided":
                invoice = parse_data(envelope, InvoiceChanged)
                return await self._closed(session, invoice.invoice_id, "invoice_voided")
            case "invoice.marked_uncollectible":
                invoice = parse_data(envelope, InvoiceChanged)
                return await self._closed(session, invoice.invoice_id, "written_off")
            case "subscription.canceled":
                subscription = parse_data(envelope, SubscriptionChanged)
                return await self._subscription_canceled(session, subscription.subscription_id)
            case _:
                # The contract allows new event types; skipping them is correct.
                return HandleOutcome(HandleResult.IGNORED)

    async def _payment_failed(self, session: AsyncSession, envelope: Envelope) -> HandleOutcome:
        data = parse_data(envelope, PaymentFailed)
        case = await self._case_for_update(session, data.invoice_id)

        if case is None:
            case = DunningCase(
                invoice_id=str(data.invoice_id),
                subscription_id=str(data.subscription_id) if data.subscription_id else None,
                customer_id=str(data.customer_id),
                status=CaseStatus.RETRYING,
                failed_attempts=0,
                amount=data.amount.amount,
                currency=data.amount.currency,
            )
            session.add(case)

        notification = apply_failure(
            case,
            self._policy,
            failed_attempts=data.failed_attempts,
            failure_code=data.failure_code,
            failed_at=envelope.occurred_at,
        )

        log.info(
            "payment_failure_recorded",
            invoice_id=case.invoice_id,
            failed_attempts=case.failed_attempts,
            status=case.status.value,
            next_attempt_at=case.next_attempt_at.isoformat() if case.next_attempt_at else None,
        )

        return HandleOutcome(HandleResult.APPLIED, [notification] if notification else [])

    async def _recovered(self, session: AsyncSession, invoice_id: UUID) -> HandleOutcome:
        case = await self._case_for_update(session, invoice_id)
        if case is None:
            # Most payments succeed first time: no case, nothing to do.
            return HandleOutcome(HandleResult.IGNORED)

        notification = apply_recovery(case)
        return HandleOutcome(HandleResult.APPLIED, [notification] if notification else [])

    async def _closed(self, session: AsyncSession, invoice_id: UUID, reason: str) -> HandleOutcome:
        case = await self._case_for_update(session, invoice_id)
        if case is None:
            return HandleOutcome(HandleResult.IGNORED)

        apply_closure(case, reason)
        return HandleOutcome(HandleResult.APPLIED)

    async def _subscription_canceled(
        self, session: AsyncSession, subscription_id: UUID
    ) -> HandleOutcome:
        cases = (
            await session.scalars(
                select(DunningCase)
                .where(DunningCase.subscription_id == str(subscription_id))
                .with_for_update()
            )
        ).all()

        for case in cases:
            apply_closure(case, "subscription_canceled")

        return HandleOutcome(HandleResult.APPLIED if cases else HandleResult.IGNORED)

    @staticmethod
    async def _case_for_update(session: AsyncSession, invoice_id: UUID) -> DunningCase | None:
        return await session.scalar(
            select(DunningCase).where(DunningCase.invoice_id == str(invoice_id)).with_for_update()
        )
