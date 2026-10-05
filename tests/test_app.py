import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.app import Runtime, create_app
from dunning.config import Settings
from dunning.db import CaseStatus, DunningCase, RetryAttempt


@pytest.fixture
async def runtime(session_factory: async_sessionmaker[AsyncSession]) -> Runtime:
    return Runtime(sessions=session_factory)


@pytest.fixture
async def client(runtime: Runtime) -> AsyncIterator[httpx.AsyncClient]:
    # The lifespan (Kafka, scheduler) is not started: routes get a test runtime.
    app = create_app(Settings(run_consumer=False, run_scheduler=False))
    app.state.runtime = runtime
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


async def seed(sessions: async_sessionmaker[AsyncSession]) -> None:
    async with sessions.begin() as session:
        case = DunningCase(
            invoice_id="inv-1",
            subscription_id="sub-1",
            customer_id="cus-1",
            status=CaseStatus.RETRYING,
            failed_attempts=2,
            next_attempt_at=datetime(2026, 10, 9, tzinfo=UTC),
            last_failure_code="insufficient_funds",
            amount=4900,
            currency="EUR",
        )
        session.add(case)
        await session.flush()
        session.add(
            RetryAttempt(
                case_id=case.id,
                attempt_number=1,
                idempotency_key="dunning:inv-1:retry:1",
                executions=1,
                outcome="failed",
            )
        )


async def test_liveness(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/live")

    assert response.status_code == 200


async def test_readiness_reports_each_dependency(
    client: httpx.AsyncClient, runtime: Runtime
) -> None:
    async def forever() -> None:
        await asyncio.Event().wait()

    runtime.tasks["consumer"] = asyncio.create_task(forever())
    try:
        response = await client.get("/health/ready")
    finally:
        runtime.tasks["consumer"].cancel()

    assert response.status_code == 200
    assert response.json()["checks"] == {"database": "ok", "consumer": "ok"}


async def test_readiness_fails_when_a_background_task_died(
    client: httpx.AsyncClient, runtime: Runtime
) -> None:
    async def crash() -> None:
        raise RuntimeError("boom")

    task = asyncio.create_task(crash())
    await asyncio.gather(task, return_exceptions=True)
    runtime.tasks["consumer"] = task

    response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["consumer"] == "stopped"


async def test_metrics_are_exposed(client: httpx.AsyncClient) -> None:
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert "dunning_events_processed_total" in response.text


async def test_a_case_can_be_inspected_with_its_attempts(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await seed(session_factory)

    response = await client.get("/cases/inv-1")

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "retrying"
    assert body["failed_attempts"] == 2
    assert body["next_attempt_at"].startswith("2026-10-09T00:00:00")
    assert [a["outcome"] for a in body["attempts"]] == ["failed"]


async def test_cases_can_be_filtered_by_status(
    client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    await seed(session_factory)

    assert len((await client.get("/cases", params={"status": "retrying"})).json()) == 1
    assert (await client.get("/cases", params={"status": "recovered"})).json() == []


async def test_unknown_cases_are_404(client: httpx.AsyncClient) -> None:
    assert (await client.get("/cases/nope")).status_code == 404
