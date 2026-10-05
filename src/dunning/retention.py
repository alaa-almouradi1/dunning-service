"""Keeps the processed_events table bounded."""

from datetime import datetime, timedelta

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.db import ProcessedEvent

log = structlog.get_logger(__name__)


async def prune_processed_events(
    sessions: async_sessionmaker[AsyncSession],
    now: datetime,
    retention: timedelta,
    batch_size: int = 5000,
    max_batches: int = 20,
) -> int:
    """Delete processed-event IDs older than the retention window.

    The window only has to exceed how long Kafka could redeliver an event
    (topic retention plus consumer downtime). Deleting in small batches keeps
    each transaction short, so the consumer is never blocked for long.
    """
    cutoff = now - retention
    deleted = 0

    for _ in range(max_batches):
        async with sessions.begin() as session:
            # Select first, then delete by key: MariaDB does not support
            # LIMIT inside an IN (...) subquery.
            ids = (
                await session.scalars(
                    select(ProcessedEvent.event_id)
                    .where(ProcessedEvent.processed_at < cutoff)
                    .limit(batch_size)
                )
            ).all()
            if not ids:
                break
            await session.execute(delete(ProcessedEvent).where(ProcessedEvent.event_id.in_(ids)))
            deleted += len(ids)

    if deleted:
        log.info("processed_events_pruned", deleted=deleted, cutoff=cutoff.isoformat())
    return deleted
