"""Process wiring: HTTP API, event consumer and retry scheduler in one service.

    uvicorn dunning.app:app        (or: python -m dunning)

The consumer and scheduler run as background tasks and can be switched off
per deployment (DUNNING_RUN_CONSUMER / DUNNING_RUN_SCHEDULER), e.g. to scale
the stateless scheduler separately from the partition-bound consumer.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Any

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from dunning import __version__
from dunning.billing_client import BillingClient
from dunning.config import Settings, get_settings
from dunning.consumer import EventConsumer
from dunning.db import CaseStatus, DunningCase, create_engine, create_session_factory
from dunning.handlers import EventHandler
from dunning.kafka import KafkaDeadLetters, KafkaSource, build_consumer, build_producer
from dunning.logging_setup import configure_logging
from dunning.notifications import LogNotifier
from dunning.policy import RetryPolicy
from dunning.scheduler import RetryScheduler
from dunning.security import require_token

log = structlog.get_logger(__name__)


@dataclass
class Runtime:
    sessions: async_sessionmaker[AsyncSession]
    tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    configure_logging(settings.log_level, settings.log_json)

    engine: AsyncEngine = create_engine(
        settings.database_url.get_secret_value(),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )
    sessions = create_session_factory(engine)
    billing = BillingClient.from_settings(settings)
    policy = RetryPolicy(tuple(settings.retry_schedule))
    notifier = LogNotifier()
    stop = asyncio.Event()
    cleanup: list[Callable[[], Awaitable[Any]]] = [billing.aclose, engine.dispose]

    runtime = Runtime(sessions=sessions)
    app.state.runtime = runtime

    if settings.run_consumer:
        kafka_consumer, producer = build_consumer(settings), build_producer(settings)
        await _start_with_retry("kafka producer", producer.start)
        await _start_with_retry("kafka consumer", kafka_consumer.start)
        cleanup[:0] = [kafka_consumer.stop, producer.stop]

        consumer = EventConsumer(
            KafkaSource(kafka_consumer),
            KafkaDeadLetters(producer, settings.kafka_dlq_topic),
            EventHandler(policy),
            sessions,
            notifier,
        )
        runtime.tasks["consumer"] = asyncio.create_task(consumer.run(stop), name="consumer")

    if settings.run_scheduler:
        scheduler = RetryScheduler(
            sessions,
            billing,
            policy,
            notifier,
            batch_size=settings.scheduler_batch_size,
            concurrency=settings.scheduler_concurrency,
            processed_event_retention=settings.processed_event_retention,
        )
        runtime.tasks["scheduler"] = asyncio.create_task(
            scheduler.run(stop, settings.scheduler_interval_seconds), name="scheduler"
        )

    log.info("service_started", version=__version__, components=sorted(runtime.tasks))
    try:
        yield
    finally:
        # SIGTERM -> uvicorn ends the lifespan -> finish the current batch, then close.
        stop.set()
        await asyncio.gather(*runtime.tasks.values(), return_exceptions=True)
        for close in cleanup:
            await close()
        log.info("service_stopped")


async def _start_with_retry(
    name: str, start: Callable[[], Awaitable[Any]], attempts: int = 30, delay: float = 2.0
) -> None:
    """Brokers often start after their clients in docker compose or Kubernetes."""
    for attempt in range(1, attempts + 1):
        try:
            await start()
            return
        except Exception as error:
            if attempt == attempts:
                raise
            log.warning("startup_retry", component=name, attempt=attempt, error=str(error))
            await asyncio.sleep(delay)


class AttemptOut(BaseModel):
    attempt_number: int
    outcome: str | None
    executions: int
    payment_id: str | None
    started_at: datetime
    finished_at: datetime | None


class CaseOut(BaseModel):
    invoice_id: str
    subscription_id: str | None
    customer_id: str
    status: CaseStatus
    failed_attempts: int
    next_attempt_at: datetime | None
    last_failure_code: str | None
    amount: int
    currency: str
    closed_reason: str | None
    created_at: datetime
    updated_at: datetime
    attempts: list[AttemptOut] = []

    @classmethod
    def of(cls, case: DunningCase, with_attempts: bool = False) -> "CaseOut":
        return cls(
            invoice_id=case.invoice_id,
            subscription_id=case.subscription_id,
            customer_id=case.customer_id,
            status=case.status,
            failed_attempts=case.failed_attempts,
            next_attempt_at=case.next_attempt_at,
            last_failure_code=case.last_failure_code,
            amount=case.amount,
            currency=case.currency,
            closed_reason=case.closed_reason,
            created_at=case.created_at,
            updated_at=case.updated_at,
            attempts=[
                AttemptOut(
                    attempt_number=a.attempt_number,
                    outcome=a.outcome,
                    executions=a.executions,
                    payment_id=a.payment_id,
                    started_at=a.started_at,
                    finished_at=a.finished_at,
                )
                for a in case.attempts
            ]
            if with_attempts
            else [],
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(title="dunning-service", version=__version__, lifespan=_lifespan)
    app.state.settings = settings or get_settings()

    def runtime(request: Request) -> Runtime:
        current: Runtime = request.app.state.runtime
        return current

    @app.get("/health/live", tags=["operations"])
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", tags=["operations"])
    async def ready(request: Request, response: Response) -> dict[str, Any]:
        rt = runtime(request)
        checks: dict[str, str] = {}

        try:
            async with rt.sessions() as session:
                await session.execute(text("SELECT 1"))
            checks["database"] = "ok"
        except Exception:
            checks["database"] = "unreachable"

        for name, task in rt.tasks.items():
            checks[name] = "stopped" if task.done() else "ok"

        healthy = all(value == "ok" for value in checks.values())
        response.status_code = 200 if healthy else 503
        return {"status": "ok" if healthy else "degraded", "checks": checks}

    metrics_auth = [Depends(require_token("metrics_token"))]
    admin_auth = [Depends(require_token("admin_token"))]

    @app.get("/metrics", tags=["operations"], dependencies=metrics_auth)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/cases", tags=["cases"], dependencies=admin_auth)
    async def list_cases(
        request: Request,
        status: Annotated[CaseStatus | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[CaseOut]:
        query = select(DunningCase).order_by(DunningCase.updated_at.desc()).limit(limit)
        if status is not None:
            query = query.where(DunningCase.status == status)
        async with runtime(request).sessions() as session:
            return [CaseOut.of(case) for case in (await session.scalars(query)).all()]

    @app.get("/cases/{invoice_id}", tags=["cases"], dependencies=admin_auth)
    async def get_case(request: Request, invoice_id: str) -> CaseOut:
        async with runtime(request).sessions() as session:
            case = await session.scalar(
                select(DunningCase)
                .where(DunningCase.invoice_id == invoice_id)
                .options(selectinload(DunningCase.attempts))
            )
        if case is None:
            raise HTTPException(status_code=404, detail="No dunning case for this invoice")
        return CaseOut.of(case, with_attempts=True)

    return app


app = create_app()
