"""Executes due retries and write-offs.

Work is claimed in a short transaction (``FOR UPDATE SKIP LOCKED``, so
several replicas never pick the same case), the billing API is called with
no transaction open, and the result is recorded in a second transaction.

A claim is a lease: the case's ``next_attempt_at`` moves forward by
``IN_FLIGHT_LEASE``. If the process dies mid-call, the case becomes due
again after the lease and is retried with the same idempotency key, so the
billing API replays the original answer instead of charging twice.
"""

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime, timedelta

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning import metrics
from dunning.billing_client import BillingClient, PayOutcome, PayResult
from dunning.cases import (
    Notification,
    apply_closure,
    apply_exhausted,
    apply_failure,
    apply_recovery,
)
from dunning.db import ACTIVE_STATUSES, CaseStatus, DunningCase, RetryAttempt, utcnow
from dunning.notifications import Notifier
from dunning.policy import RetryPolicy

log = structlog.get_logger(__name__)

IN_FLIGHT_LEASE = timedelta(minutes=10)
WAIT_FOR_PROVIDER = timedelta(hours=1)
RETRY_WHEN_UNAVAILABLE = timedelta(minutes=15)


@dataclass(frozen=True)
class Work:
    case_id: int
    invoice_id: str
    subscription_id: str | None
    exhaust: bool
    attempt_number: int
    idempotency_key: str
    failed_attempts_at_claim: int


class RetryScheduler:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        billing: BillingClient,
        policy: RetryPolicy,
        notifier: Notifier,
        batch_size: int = 50,
        concurrency: int = 10,
    ) -> None:
        self._sessions = sessions
        self._billing = billing
        self._policy = policy
        self._notifier = notifier
        self._batch_size = batch_size
        self._concurrency = concurrency
        self.running = False

    async def run(self, stop: asyncio.Event, interval_seconds: float) -> None:
        self.running = True
        log.info("scheduler_started", interval_seconds=interval_seconds)
        try:
            while not stop.is_set():
                try:
                    await self.run_once()
                    await self._update_gauges()
                except Exception:
                    log.exception("scheduler_iteration_failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        finally:
            self.running = False
            log.info("scheduler_stopped")

    async def run_once(self, now: datetime | None = None) -> int:
        now = now or utcnow()
        claimed = await self._claim(now)

        # Each item is an independent API call plus its own transaction, so
        # they run concurrently; the semaphore bounds the load we put on the
        # billing API and on the connection pool.
        limit = asyncio.Semaphore(self._concurrency)

        async def execute(work: Work) -> None:
            async with limit:
                # Each task has its own copy of the logging context.
                structlog.contextvars.bind_contextvars(invoice_id=work.invoice_id)
                try:
                    if work.exhaust:
                        await self._exhaust(work)
                    else:
                        await self._retry(work, now)
                except Exception:
                    # The lease expires and the case is picked up again later.
                    log.exception("dunning_work_failed", exhaust=work.exhaust)

        await asyncio.gather(*(execute(work) for work in claimed))
        return len(claimed)

    async def _claim(self, now: datetime) -> list[Work]:
        claimed: list[Work] = []

        async with self._sessions.begin() as session:
            cases = (
                await session.scalars(
                    select(DunningCase)
                    .where(
                        DunningCase.status.in_(ACTIVE_STATUSES),
                        DunningCase.next_attempt_at <= now,
                    )
                    .order_by(DunningCase.next_attempt_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            ).all()

            for case in cases:
                exhaust = case.status is CaseStatus.EXHAUSTING
                attempt_number = case.failed_attempts
                key = (
                    f"dunning:{case.invoice_id}:write-off"
                    if exhaust
                    else f"dunning:{case.invoice_id}:retry:{attempt_number}"
                )

                if not exhaust:
                    await self._record_attempt_start(session, case, attempt_number, key, now)
                    case.status = CaseStatus.RETRY_IN_FLIGHT

                case.next_attempt_at = now + IN_FLIGHT_LEASE
                claimed.append(
                    Work(
                        case_id=case.id,
                        invoice_id=case.invoice_id,
                        subscription_id=case.subscription_id,
                        exhaust=exhaust,
                        attempt_number=attempt_number,
                        idempotency_key=key,
                        failed_attempts_at_claim=case.failed_attempts,
                    )
                )

        return claimed

    @staticmethod
    async def _record_attempt_start(
        session: AsyncSession, case: DunningCase, attempt_number: int, key: str, now: datetime
    ) -> None:
        attempt = await session.scalar(
            select(RetryAttempt).where(
                RetryAttempt.case_id == case.id, RetryAttempt.attempt_number == attempt_number
            )
        )
        if attempt is None:
            attempt = RetryAttempt(
                case_id=case.id, attempt_number=attempt_number, idempotency_key=key, executions=0
            )
            session.add(attempt)
        attempt.executions += 1
        attempt.started_at = now

    async def _retry(self, work: Work, now: datetime) -> None:
        result = await self._billing.pay_invoice(work.invoice_id, work.idempotency_key)
        metrics.RETRY_ATTEMPTS.labels(result.outcome.value).inc()
        log.info("retry_executed", outcome=result.outcome.value, attempt=work.attempt_number)

        notification: Notification | None = None
        async with self._sessions.begin() as session:
            case = await self._lock_case(session, work.case_id)
            await self._record_attempt_result(session, work, result, now)

            match result.outcome:
                case PayOutcome.SUCCEEDED:
                    notification = apply_recovery(case)
                case PayOutcome.FAILED:
                    # The payment.failed event reports the same count; whichever
                    # of the two arrives second is a no-op.
                    notification = apply_failure(
                        case,
                        self._policy,
                        work.failed_attempts_at_claim + 1,
                        result.failure_code,
                        now,
                    )
                case PayOutcome.NOT_PAYABLE:
                    apply_closure(case, "invoice_not_payable")
                case PayOutcome.PROCESSING | PayOutcome.IN_PROGRESS:
                    self._release(case, now + WAIT_FOR_PROVIDER)
                case PayOutcome.UNAVAILABLE:
                    self._release(case, now + RETRY_WHEN_UNAVAILABLE)

        if notification:
            await self._notifier.send(notification)

    async def _exhaust(self, work: Work) -> None:
        """Out of retries: write the invoice off and cancel the subscription."""
        written_off = await self._billing.mark_uncollectible(
            work.invoice_id, f"dunning:{work.invoice_id}:write-off"
        )
        if written_off and work.subscription_id:
            await self._billing.cancel_subscription(
                work.subscription_id, f"dunning:{work.invoice_id}:cancel"
            )

        notification = None
        async with self._sessions.begin() as session:
            case = await self._lock_case(session, work.case_id)
            if case.status is CaseStatus.EXHAUSTING:
                if written_off:
                    notification = apply_exhausted(case)
                    metrics.CASES_EXHAUSTED.inc()
                else:
                    # Paid or voided in the meantime; nothing left to collect.
                    case.status = CaseStatus.CLOSED
                    case.next_attempt_at = None
                    case.closed_reason = "invoice_not_collectible"

        log.info("case_exhausted", written_off=written_off)
        if notification:
            await self._notifier.send(notification)

    @staticmethod
    def _release(case: DunningCase, retry_at: datetime) -> None:
        """Hand an in-flight case back for a later retry of the same attempt."""
        if case.status is CaseStatus.RETRY_IN_FLIGHT:
            case.status = CaseStatus.RETRYING
            case.next_attempt_at = retry_at

    @staticmethod
    async def _lock_case(session: AsyncSession, case_id: int) -> DunningCase:
        case = await session.scalar(
            select(DunningCase).where(DunningCase.id == case_id).with_for_update()
        )
        if case is None:
            raise LookupError(f"Dunning case {case_id} disappeared")
        return case

    @staticmethod
    async def _record_attempt_result(
        session: AsyncSession, work: Work, result: PayResult, now: datetime
    ) -> None:
        attempt = await session.scalar(
            select(RetryAttempt).where(
                RetryAttempt.case_id == work.case_id,
                RetryAttempt.attempt_number == work.attempt_number,
            )
        )
        if attempt is not None:
            attempt.outcome = result.outcome.value
            attempt.payment_id = result.payment_id
            attempt.detail = result.failure_code or result.detail
            attempt.finished_at = now

    async def _update_gauges(self) -> None:
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(DunningCase.status, func.count())
                    .where(DunningCase.status.in_(ACTIVE_STATUSES))
                    .group_by(DunningCase.status)
                )
            ).all()
        counts = {status: count for status, count in rows}
        for status in ACTIVE_STATUSES:
            metrics.ACTIVE_CASES.labels(status.value).set(counts.get(status, 0))
