import asyncio
from collections import defaultdict
from collections.abc import Sequence
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.cases import Notification
from dunning.consumer import EventConsumer, Record
from dunning.db import DunningCase, ProcessedEvent
from dunning.events import Envelope
from dunning.handlers import EventHandler, HandleOutcome
from dunning.notifications import RecordingNotifier
from dunning.policy import RetryPolicy
from tests.factories import payment_failed, to_bytes


class FakeSource:
    """Mimics Kafka: per-partition logs, fetch positions and committed offsets."""

    def __init__(self) -> None:
        self.logs: dict[int, list[Record]] = defaultdict(list)
        self.position: dict[int, int] = defaultdict(int)
        self.committed: dict[int, int] = {}
        self.commit_calls = 0

    def append(self, value: bytes, partition: int = 0) -> Record:
        record = Record("billing.events", partition, len(self.logs[partition]), b"key", value)
        self.logs[partition].append(record)
        return record

    async def fetch(self, timeout_ms: int, max_records: int) -> Sequence[Record]:
        batch: list[Record] = []
        for partition, log in self.logs.items():
            pending = log[self.position[partition] :]
            batch.extend(pending)
            self.position[partition] = len(log)
        return batch[:max_records]

    async def commit(self, record: Record) -> None:
        self.commit_calls += 1
        self.committed[record.partition] = record.offset + 1

    async def seek(self, record: Record) -> None:
        self.position[record.partition] = record.offset


class FakeDeadLetters:
    def __init__(self) -> None:
        self.sent: list[tuple[Record, str]] = []
        self.fail = False

    async def send(self, record: Record, error: str) -> None:
        if self.fail:
            raise ConnectionError("DLQ unavailable")
        self.sent.append((record, error))


class FlakyHandler(EventHandler):
    """Fails the first ``failures`` calls, as if the database were down."""

    def __init__(self, failures: int) -> None:
        super().__init__(RetryPolicy())
        self.failures = failures

    async def handle(self, session: AsyncSession, envelope: Envelope) -> HandleOutcome:
        if self.failures > 0:
            self.failures -= 1
            raise ConnectionError("database unavailable")
        return await super().handle(session, envelope)


def make_consumer(
    sessions: async_sessionmaker[AsyncSession],
    source: FakeSource,
    *,
    handler: EventHandler | None = None,
    dead_letters: FakeDeadLetters | None = None,
    notifier: Any = None,
) -> EventConsumer:
    return EventConsumer(
        source,
        dead_letters or FakeDeadLetters(),
        handler or EventHandler(RetryPolicy()),
        sessions,
        notifier or RecordingNotifier(),
    )


async def count(sessions: async_sessionmaker[AsyncSession], model: Any) -> int:
    async with sessions() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_events_are_applied_and_committed_in_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    source = FakeSource()
    source.append(to_bytes(payment_failed(uuid4())))
    source.append(to_bytes(payment_failed(uuid4())))
    consumer = make_consumer(session_factory, source)

    result = await consumer.poll_once()

    assert result.processed == 2
    assert source.committed == {0: 2}
    assert source.commit_calls == 1, "one commit per partition per poll"
    assert await count(session_factory, DunningCase) == 2


async def test_poison_messages_go_to_the_dead_letter_topic_and_do_not_block(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    source = FakeSource()
    poison = source.append(b"{not json")
    source.append(to_bytes(payment_failed(uuid4())))
    dead_letters = FakeDeadLetters()
    consumer = make_consumer(session_factory, source, dead_letters=dead_letters)

    await consumer.poll_once()

    assert [record for record, _ in dead_letters.sent] == [poison]
    assert "Malformed" in dead_letters.sent[0][1]
    assert source.committed == {0: 2}
    assert await count(session_factory, DunningCase) == 1


async def test_transient_failures_rewind_and_retry_without_losing_events(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    source = FakeSource()
    source.append(to_bytes(payment_failed(uuid4())))
    source.append(to_bytes(payment_failed(uuid4())))
    consumer = make_consumer(session_factory, source, handler=FlakyHandler(failures=1))

    first = await consumer.poll_once()
    assert first.failed
    assert source.committed == {}
    assert await count(session_factory, ProcessedEvent) == 0

    second = await consumer.poll_once()
    assert second.processed == 2
    assert source.committed == {0: 2}


async def test_a_failing_partition_does_not_hold_up_the_others(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    source = FakeSource()
    source.append(to_bytes(payment_failed(uuid4())), partition=0)
    source.append(to_bytes(payment_failed(uuid4())), partition=1)
    consumer = make_consumer(session_factory, source, handler=FlakyHandler(failures=1))

    await consumer.poll_once()

    assert source.committed == {1: 1}
    assert source.position[0] == 0, "partition 0 is rewound to the failed record"


async def test_an_unavailable_dead_letter_topic_is_a_transient_failure(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    source = FakeSource()
    source.append(b"garbage")
    dead_letters = FakeDeadLetters()
    dead_letters.fail = True
    consumer = make_consumer(session_factory, source, dead_letters=dead_letters)

    result = await consumer.poll_once()

    assert result.failed
    assert source.committed == {}


async def test_notifications_are_sent_after_commit_and_failures_are_contained(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class BrokenNotifier:
        async def send(self, notification: Notification) -> None:
            raise RuntimeError("SMTP down")

    source = FakeSource()
    source.append(to_bytes(payment_failed(uuid4())))
    consumer = make_consumer(session_factory, source, notifier=BrokenNotifier())

    result = await consumer.poll_once()

    assert result.processed == 1
    assert source.committed == {0: 1}


async def test_run_stops_when_asked(session_factory: async_sessionmaker[AsyncSession]) -> None:
    consumer = make_consumer(session_factory, FakeSource())
    stop = asyncio.Event()

    task = asyncio.create_task(consumer.run(stop))
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(task, timeout=2)

    assert not consumer.running


@pytest.mark.parametrize("failures", [1, 3])
async def test_backoff_grows_and_resets(
    session_factory: async_sessionmaker[AsyncSession], failures: int
) -> None:
    source = FakeSource()
    source.append(to_bytes(payment_failed(uuid4())))
    consumer = make_consumer(session_factory, source, handler=FlakyHandler(failures=failures))

    for _ in range(failures):
        assert (await consumer.poll_once()).failed
    assert consumer._consecutive_failures == failures

    assert (await consumer.poll_once()).processed == 1
    assert consumer._consecutive_failures == 0


async def test_records_before_a_failure_are_still_committed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class FailSecond(EventHandler):
        calls = 0

        async def handle(self, session: AsyncSession, envelope: Envelope) -> HandleOutcome:
            FailSecond.calls += 1
            if FailSecond.calls == 2:
                raise ConnectionError("database blip")
            return await super().handle(session, envelope)

    source = FakeSource()
    for _ in range(3):
        source.append(to_bytes(payment_failed(uuid4())))
    consumer = make_consumer(session_factory, source, handler=FailSecond(RetryPolicy()))

    await consumer.poll_once()

    assert source.committed == {0: 1}, "the first record is done, the second is retried"
    assert source.position[0] == 1
