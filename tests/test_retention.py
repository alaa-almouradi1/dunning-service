from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.db import ProcessedEvent
from dunning.retention import prune_processed_events

NOW = datetime(2026, 10, 4, 12, 0, tzinfo=UTC)


def event(event_id: str, age_days: int) -> ProcessedEvent:
    return ProcessedEvent(
        event_id=event_id, event_type="x", processed_at=NOW - timedelta(days=age_days)
    )


async def test_old_processed_events_are_pruned_in_batches(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory.begin() as session:
        for n in range(5):
            session.add(event(f"old-{n}", 31))
        session.add(event("recent", 1))

    deleted = await prune_processed_events(
        session_factory, NOW, timedelta(days=30), batch_size=2, max_batches=10
    )

    async with session_factory() as session:
        remaining = (await session.scalars(select(ProcessedEvent.event_id))).all()
    assert deleted == 5
    assert remaining == ["recent"]


async def test_pruning_is_bounded_per_run(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory.begin() as session:
        for n in range(5):
            session.add(event(f"old-{n}", 60))

    deleted = await prune_processed_events(
        session_factory, NOW, timedelta(days=30), batch_size=2, max_batches=1
    )

    assert deleted == 2
