from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.cases import NotificationKind
from dunning.db import CaseStatus, DunningCase, ProcessedEvent
from dunning.events import InvalidEventError, parse_envelope
from dunning.handlers import EventHandler, HandleOutcome, HandleResult
from dunning.policy import RetryPolicy
from tests.factories import (
    invoice_event,
    payment_failed,
    payment_succeeded,
    subscription_canceled,
    to_bytes,
)

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def handler() -> EventHandler:
    return EventHandler(RetryPolicy((1, 3)))


async def deliver(
    sessions: async_sessionmaker[AsyncSession], handler: EventHandler, event: dict[str, Any]
) -> HandleOutcome:
    async with sessions.begin() as session:
        return await handler.handle(session, parse_envelope(to_bytes(event)))


async def case_for(sessions: async_sessionmaker[AsyncSession], invoice_id: UUID) -> DunningCase:
    async with sessions() as session:
        case = await session.scalar(
            select(DunningCase).where(DunningCase.invoice_id == str(invoice_id))
        )
    assert case is not None
    return case


async def test_a_first_failure_opens_a_case_and_schedules_a_retry(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    invoice_id = uuid4()

    outcome = await deliver(
        session_factory, handler, payment_failed(invoice_id, occurred_at=T0.isoformat())
    )

    case = await case_for(session_factory, invoice_id)
    assert outcome.result is HandleResult.APPLIED
    assert case.status is CaseStatus.RETRYING
    assert case.failed_attempts == 1
    assert case.next_attempt_at == T0 + timedelta(days=1)
    assert case.last_failure_code == "insufficient_funds"
    assert [n.kind for n in outcome.notifications] == [NotificationKind.PAYMENT_FAILED]


async def test_redelivered_events_are_applied_once(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    event = payment_failed(uuid4())

    first = await deliver(session_factory, handler, event)
    second = await deliver(session_factory, handler, event)

    assert first.result is HandleResult.APPLIED
    assert second.result is HandleResult.DUPLICATE
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1


async def test_failure_counts_only_move_forward(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    invoice_id = uuid4()
    await deliver(session_factory, handler, payment_failed(invoice_id, failed_attempts=2))

    # An older failure arriving late (different event ID) must not rewind the case.
    stale = await deliver(session_factory, handler, payment_failed(invoice_id, failed_attempts=1))

    assert (await case_for(session_factory, invoice_id)).failed_attempts == 2
    assert stale.notifications == []


async def test_the_last_failure_marks_the_case_for_exhaustion(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    invoice_id = uuid4()

    await deliver(
        session_factory,
        handler,
        payment_failed(invoice_id, failed_attempts=3, occurred_at=T0.isoformat()),
    )

    case = await case_for(session_factory, invoice_id)
    assert case.status is CaseStatus.EXHAUSTING
    assert case.next_attempt_at == T0


@pytest.mark.parametrize(
    "recovery_event",
    [
        lambda invoice_id: payment_succeeded(invoice_id),
        lambda invoice_id: invoice_event("invoice.paid", invoice_id, "paid"),
    ],
)
async def test_a_payment_recovers_the_case(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler, recovery_event: Any
) -> None:
    invoice_id = uuid4()
    await deliver(session_factory, handler, payment_failed(invoice_id))

    outcome = await deliver(session_factory, handler, recovery_event(invoice_id))

    case = await case_for(session_factory, invoice_id)
    assert case.status is CaseStatus.RECOVERED
    assert case.next_attempt_at is None
    assert [n.kind for n in outcome.notifications] == [NotificationKind.RECOVERED]


async def test_successful_payments_without_a_case_are_ignored(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    outcome = await deliver(session_factory, handler, payment_succeeded(uuid4()))

    assert outcome.result is HandleResult.IGNORED


async def test_a_voided_invoice_closes_the_case(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    invoice_id = uuid4()
    await deliver(session_factory, handler, payment_failed(invoice_id))

    await deliver(session_factory, handler, invoice_event("invoice.voided", invoice_id, "void"))

    case = await case_for(session_factory, invoice_id)
    assert case.status is CaseStatus.CLOSED
    assert case.closed_reason == "invoice_voided"


async def test_canceling_the_subscription_stops_its_retries(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    invoice_id, subscription_id = uuid4(), uuid4()
    await deliver(session_factory, handler, payment_failed(invoice_id, subscription_id))

    await deliver(session_factory, handler, subscription_canceled(subscription_id))

    assert (await case_for(session_factory, invoice_id)).status is CaseStatus.CLOSED


async def test_unknown_event_types_are_skipped(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    outcome = await deliver(
        session_factory, handler, payment_failed(uuid4(), type="loyalty.points_awarded")
    )

    assert outcome.result is HandleResult.IGNORED


async def test_malformed_payloads_roll_back_and_raise(
    session_factory: async_sessionmaker[AsyncSession], handler: EventHandler
) -> None:
    event = payment_failed(uuid4())
    event["data"]["failed_attempts"] = 0

    with pytest.raises(InvalidEventError):
        await deliver(session_factory, handler, event)

    async with session_factory() as session:
        assert await session.get(ProcessedEvent, event["id"]) is None
