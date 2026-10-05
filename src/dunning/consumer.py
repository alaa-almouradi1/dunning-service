"""Consumes billing events with at-least-once semantics.

Offsets are committed only after an event's effects are committed to the
database, one record at a time. A crash in between means the event is
delivered again, and the handler recognises it by its ID.

Two kinds of failure are treated differently:

* Poison messages (malformed, unsupported version) can never succeed.
  They go to the dead-letter topic and the partition moves on.
* Transient failures (database down) will succeed later. The partition is
  rewound to the failed record and retried with exponential backoff, so
  events are never skipped or reordered.
"""

import asyncio
import contextlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning import metrics
from dunning.events import InvalidEventError, parse_envelope
from dunning.handlers import EventHandler, HandleOutcome
from dunning.notifications import Notifier

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Record:
    """A consumed message, independent of the Kafka client library."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: tuple[tuple[str, bytes], ...] = ()


class MessageSource(Protocol):
    async def fetch(self, timeout_ms: int, max_records: int) -> Sequence[Record]: ...

    async def commit(self, record: Record) -> None:
        """Mark everything up to and including ``record`` as processed."""

    async def seek(self, record: Record) -> None:
        """Make ``record`` the next message fetched from its partition."""


class DeadLetterSink(Protocol):
    async def send(self, record: Record, error: str) -> None: ...


@dataclass(frozen=True)
class PollResult:
    processed: int
    failed: bool


class EventConsumer:
    def __init__(
        self,
        source: MessageSource,
        dead_letters: DeadLetterSink,
        handler: EventHandler,
        sessions: async_sessionmaker[AsyncSession],
        notifier: Notifier,
        *,
        max_backoff_seconds: float = 30.0,
    ) -> None:
        self._source = source
        self._dead_letters = dead_letters
        self._handler = handler
        self._sessions = sessions
        self._notifier = notifier
        self._max_backoff = max_backoff_seconds
        self._consecutive_failures = 0
        self.running = False

    async def run(self, stop: asyncio.Event) -> None:
        self.running = True
        log.info("consumer_started")
        try:
            while not stop.is_set():
                result = await self.poll_once()
                if result.failed:
                    await self._backoff(stop)
                else:
                    # Always yield to the event loop, even if a source returns
                    # immediately, so the API and scheduler tasks keep running.
                    await asyncio.sleep(0)
        finally:
            self.running = False
            log.info("consumer_stopped")

    async def poll_once(self, timeout_ms: int = 1000, max_records: int = 100) -> PollResult:
        records = await self._source.fetch(timeout_ms, max_records)
        blocked: set[tuple[str, int]] = set()
        processed = 0

        for record in records:
            partition = (record.topic, record.partition)
            if partition in blocked:
                continue  # an earlier record of this partition failed; keep order

            if await self._process(record):
                await self._source.commit(record)
                processed += 1
            else:
                await self._source.seek(record)
                blocked.add(partition)

        if blocked:
            self._consecutive_failures += 1
        elif processed:
            self._consecutive_failures = 0

        return PollResult(processed=processed, failed=bool(blocked))

    async def _process(self, record: Record) -> bool:
        """Returns True when the record is done with (applied or dead-lettered)."""
        structlog.contextvars.bind_contextvars(
            kafka_partition=record.partition, kafka_offset=record.offset
        )
        try:
            envelope = parse_envelope(record.value or b"")
            structlog.contextvars.bind_contextvars(
                event_id=str(envelope.id), event_type=envelope.type
            )

            async with self._sessions.begin() as session:
                outcome = await self._handler.handle(session, envelope)

            metrics.EVENTS_PROCESSED.labels(envelope.type, outcome.result.value).inc()
            log.debug("event_processed", result=outcome.result.value)
            await self._notify(outcome)
            return True
        except InvalidEventError as error:
            return await self._dead_letter(record, str(error))
        except Exception:
            metrics.CONSUMER_TRANSIENT_ERRORS.inc()
            log.exception("event_processing_failed_will_retry")
            return False
        finally:
            structlog.contextvars.unbind_contextvars(
                "kafka_partition", "kafka_offset", "event_id", "event_type"
            )

    async def _dead_letter(self, record: Record, error: str) -> bool:
        try:
            await self._dead_letters.send(record, error)
        except Exception:
            log.exception("dead_letter_publish_failed_will_retry")
            return False

        metrics.EVENTS_DEAD_LETTERED.inc()
        log.warning("event_dead_lettered", error=error)
        return True

    async def _notify(self, outcome: HandleOutcome) -> None:
        # After commit, best effort: a failed email must not re-run billing logic.
        for notification in outcome.notifications:
            try:
                await self._notifier.send(notification)
            except Exception:
                log.exception("notification_failed", kind=notification.kind.value)

    async def _backoff(self, stop: asyncio.Event) -> None:
        delay = min(self._max_backoff, 0.5 * 2 ** (self._consecutive_failures - 1))
        log.info("consumer_backing_off", seconds=delay)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)
