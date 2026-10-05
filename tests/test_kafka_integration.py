"""Runs against a real broker: pytest -m kafka (CI starts one, see ci.yml).

Locally: start the billing-api compose stack, then
    KAFKA_BROKERS=localhost:29092 pytest -m kafka -o addopts=""
"""

import asyncio
import os
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dunning.config import Settings
from dunning.consumer import EventConsumer
from dunning.db import CaseStatus, DunningCase
from dunning.handlers import EventHandler
from dunning.kafka import KafkaDeadLetters, KafkaSource, build_consumer, build_producer
from dunning.notifications import RecordingNotifier
from dunning.policy import RetryPolicy
from tests.factories import payment_failed, to_bytes

pytestmark = pytest.mark.kafka

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")


async def test_events_flow_from_kafka_into_cases_and_poison_reaches_the_dlq(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run = uuid4().hex[:8]
    settings = Settings(
        kafka_brokers=BROKERS,
        kafka_topic=f"it.billing.events.{run}",
        kafka_dlq_topic=f"it.billing.events.{run}.dlq",
        kafka_group_id=f"it-dunning-{run}",
    )
    invoice_id = uuid4()

    producer = build_producer(settings)
    await producer.start()
    consumer = build_consumer(settings)
    dlq_reader = AIOKafkaConsumer(
        settings.kafka_dlq_topic, bootstrap_servers=BROKERS, auto_offset_reset="earliest"
    )

    try:
        await producer.send_and_wait(
            settings.kafka_topic, to_bytes(payment_failed(invoice_id)), key=b"sub-1"
        )
        await producer.send_and_wait(settings.kafka_topic, b"definitely not json", key=b"sub-1")

        await consumer.start()
        event_consumer = EventConsumer(
            KafkaSource(consumer),
            KafkaDeadLetters(producer, settings.kafka_dlq_topic),
            EventHandler(RetryPolicy()),
            session_factory,
            RecordingNotifier(),
        )

        processed = 0
        for _ in range(30):  # the first polls only join the consumer group
            processed += (await event_consumer.poll_once(timeout_ms=1000)).processed
            if processed >= 2:
                break
        assert processed == 2

        async with session_factory() as session:
            case = await session.scalar(
                select(DunningCase).where(DunningCase.invoice_id == str(invoice_id))
            )
        assert case is not None
        assert case.status is CaseStatus.RETRYING

        assert await consumer.committed(TopicPartition(settings.kafka_topic, 0)) == 2

        await dlq_reader.start()
        dead = await asyncio.wait_for(dlq_reader.getone(), timeout=30)
        assert dead.value == b"definitely not json"
        assert "dlq-error" in dict(dead.headers)
    finally:
        await dlq_reader.stop()
        await consumer.stop()
        await producer.stop()
