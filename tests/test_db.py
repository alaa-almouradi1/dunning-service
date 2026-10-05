from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.db import Base, CaseStatus, DunningCase

ROOT = Path(__file__).resolve().parent.parent


def test_migrations_produce_exactly_the_models_schema(tmp_path: Path) -> None:
    db = tmp_path / "migrated.db"
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{db}")

    command.upgrade(config, "head")

    with create_sync_engine(f"sqlite:///{db}").connect() as connection:
        diff = compare_metadata(MigrationContext.configure(connection), Base.metadata)

    assert diff == [], "Models changed without a migration (run alembic revision --autogenerate)"


async def test_datetimes_round_trip_as_aware_utc(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    istanbul = timezone(timedelta(hours=3))
    due = datetime(2026, 10, 3, 18, 0, tzinfo=istanbul)

    async with session_factory.begin() as session:
        session.add(
            DunningCase(
                invoice_id="inv-1",
                customer_id="cus-1",
                status=CaseStatus.RETRYING,
                next_attempt_at=due,
                amount=100,
                currency="EUR",
            )
        )

    async with session_factory() as session:
        case = await session.get(DunningCase, 1)

    assert case is not None
    assert case.next_attempt_at == datetime(2026, 10, 3, 15, 0, tzinfo=UTC)
    assert case.next_attempt_at.tzinfo is UTC
    assert case.status is CaseStatus.RETRYING


async def test_naive_datetimes_are_refused(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    with pytest.raises(Exception, match="Naive datetimes"):
        async with session_factory.begin() as session:
            session.add(
                DunningCase(
                    invoice_id="inv-2",
                    customer_id="cus-1",
                    status=CaseStatus.RETRYING,
                    next_attempt_at=datetime(2026, 1, 1),  # noqa: DTZ001
                    amount=100,
                    currency="EUR",
                )
            )
