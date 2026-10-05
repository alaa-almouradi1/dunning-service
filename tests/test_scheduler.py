from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.cases import NotificationKind, apply_failure
from dunning.db import CaseStatus, DunningCase, RetryAttempt
from dunning.notifications import RecordingNotifier
from dunning.policy import RetryPolicy
from dunning.scheduler import IN_FLIGHT_LEASE, RETRY_WHEN_UNAVAILABLE, RetryScheduler
from tests.fake_billing import FakeBillingApi

NOW = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
INVOICE = "0192a1b2-0000-7000-8000-00000000000a"
SUBSCRIPTION = "0192a1b2-0000-7000-8000-00000000000b"


@pytest.fixture
def api() -> FakeBillingApi:
    return FakeBillingApi()


@pytest.fixture
def notifier() -> RecordingNotifier:
    return RecordingNotifier()


@pytest.fixture
def scheduler(
    session_factory: async_sessionmaker[AsyncSession],
    api: FakeBillingApi,
    notifier: RecordingNotifier,
) -> RetryScheduler:
    return RetryScheduler(session_factory, api.client(), RetryPolicy.days(1, 3), notifier)


async def open_case(
    sessions: async_sessionmaker[AsyncSession],
    *,
    status: CaseStatus = CaseStatus.RETRYING,
    failed_attempts: int = 1,
    due: datetime = NOW,
) -> None:
    async with sessions.begin() as session:
        session.add(
            DunningCase(
                invoice_id=INVOICE,
                subscription_id=SUBSCRIPTION,
                customer_id="cus-1",
                status=status,
                failed_attempts=failed_attempts,
                next_attempt_at=due,
                amount=4900,
                currency="EUR",
            )
        )


async def load(sessions: async_sessionmaker[AsyncSession]) -> DunningCase:
    async with sessions() as session:
        case = await session.scalar(select(DunningCase).where(DunningCase.invoice_id == INVOICE))
    assert case is not None
    return case


async def attempts(sessions: async_sessionmaker[AsyncSession]) -> list[RetryAttempt]:
    async with sessions() as session:
        return list((await session.scalars(select(RetryAttempt))).all())


async def test_cases_that_are_not_due_are_left_alone(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
) -> None:
    await open_case(session_factory, due=NOW + timedelta(seconds=1))

    assert await scheduler.run_once(NOW) == 0
    assert api.requests == []


async def test_a_successful_retry_recovers_the_case(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
    notifier: RecordingNotifier,
) -> None:
    await open_case(session_factory)
    api.queue_payment(INVOICE, "succeeded")

    await scheduler.run_once(NOW)

    case = await load(session_factory)
    assert case.status is CaseStatus.RECOVERED
    assert api.requests[0].headers["Idempotency-Key"] == f"dunning:{INVOICE}:retry:1"
    assert [n.kind for n in notifier.sent] == [NotificationKind.RECOVERED]
    [attempt] = await attempts(session_factory)
    assert (attempt.outcome, attempt.executions) == ("succeeded", 1)


async def test_a_failed_retry_schedules_the_next_one(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
) -> None:
    await open_case(session_factory)
    api.queue_payment(INVOICE, "failed", "card_declined")

    await scheduler.run_once(NOW)

    case = await load(session_factory)
    assert case.status is CaseStatus.RETRYING
    assert case.failed_attempts == 2
    assert case.last_failure_code == "card_declined"
    assert case.next_attempt_at is not None
    assert case.next_attempt_at - NOW >= timedelta(days=3)


async def test_the_payment_failed_event_for_our_own_retry_is_not_double_counted(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
) -> None:
    await open_case(session_factory)
    api.queue_payment(INVOICE, "failed")
    await scheduler.run_once(NOW)

    # The event consumer later applies payment.failed with failed_attempts=2.
    async with session_factory.begin() as session:
        case = await session.scalar(select(DunningCase))
        assert case is not None
        apply_failure(case, RetryPolicy.days(1, 3), 2, "card_declined", NOW)

    assert (await load(session_factory)).failed_attempts == 2


async def test_exhaustion_writes_off_the_invoice_and_cancels_the_subscription(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
    notifier: RecordingNotifier,
) -> None:
    await open_case(session_factory, failed_attempts=2)
    api.queue_payment(INVOICE, "failed")

    await scheduler.run_once(NOW)  # last retry fails -> exhausting, due now
    assert (await load(session_factory)).status is CaseStatus.EXHAUSTING

    await scheduler.run_once(NOW + timedelta(seconds=1))

    case = await load(session_factory)
    assert case.status is CaseStatus.EXHAUSTED
    assert len(api.calls("/mark-uncollectible")) == 1
    assert api.calls("/cancel")[0].url.path.endswith(f"/subscriptions/{SUBSCRIPTION}/cancel")
    assert notifier.sent[-1].kind is NotificationKind.EXHAUSTED


async def test_an_invoice_paid_before_write_off_is_closed_not_canceled(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
) -> None:
    await open_case(session_factory, status=CaseStatus.EXHAUSTING, failed_attempts=3)
    api.uncollectible_status = 409

    await scheduler.run_once(NOW)

    assert (await load(session_factory)).status is CaseStatus.CLOSED
    assert api.calls("/cancel") == []


async def test_an_unreachable_api_reschedules_the_same_attempt(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
) -> None:
    await open_case(session_factory)
    api.queue_error(INVOICE, 503, "gateway_unavailable")
    api.queue_payment(INVOICE, "succeeded")

    await scheduler.run_once(NOW)
    case = await load(session_factory)
    assert case.status is CaseStatus.RETRYING
    assert case.next_attempt_at == NOW + RETRY_WHEN_UNAVAILABLE

    await scheduler.run_once(NOW + RETRY_WHEN_UNAVAILABLE)

    keys = [r.headers["Idempotency-Key"] for r in api.calls("/pay")]
    assert keys == [f"dunning:{INVOICE}:retry:1"] * 2, "same logical attempt, same key"
    [attempt] = await attempts(session_factory)
    assert attempt.executions == 2
    assert (await load(session_factory)).status is CaseStatus.RECOVERED


async def test_a_retry_lost_in_a_crash_is_replayed_safely_after_the_lease(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
) -> None:
    # A previous worker claimed the case, the API charged the card, then the worker died.
    await open_case(session_factory)
    api.queue_payment(INVOICE, "succeeded")
    await api.client().pay_invoice(INVOICE, f"dunning:{INVOICE}:retry:1")
    async with session_factory.begin() as session:
        case = await session.scalar(select(DunningCase))
        assert case is not None
        case.status = CaseStatus.RETRY_IN_FLIGHT
        case.next_attempt_at = NOW + IN_FLIGHT_LEASE

    assert await scheduler.run_once(NOW) == 0, "lease still valid"
    await scheduler.run_once(NOW + IN_FLIGHT_LEASE)

    assert (await load(session_factory)).status is CaseStatus.RECOVERED
    # The queued answer was used once; the second call was a replay, not a new charge.
    assert api.pay_responses[INVOICE] == []


async def test_an_invoice_that_was_paid_meanwhile_closes_the_case(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: RetryScheduler,
    api: FakeBillingApi,
) -> None:
    await open_case(session_factory)
    api.queue_error(INVOICE, 409, "invoice_not_payable")

    await scheduler.run_once(NOW)

    case = await load(session_factory)
    assert case.status is CaseStatus.CLOSED
    assert case.closed_reason == "invoice_not_payable"
