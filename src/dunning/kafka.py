"""aiokafka adapters for the transport-agnostic consumer."""

from collections.abc import Sequence
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.helpers import create_ssl_context

from dunning.config import Settings
from dunning.consumer import Record


class KafkaSource:
    def __init__(self, consumer: AIOKafkaConsumer) -> None:
        self._consumer = consumer

    async def fetch(self, timeout_ms: int, max_records: int) -> Sequence[Record]:
        batches = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        return [
            Record(
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                key=message.key,
                value=message.value,
                headers=tuple(message.headers or ()),
            )
            for messages in batches.values()
            for message in messages
        ]

    async def commit(self, record: Record) -> None:
        partition = TopicPartition(record.topic, record.partition)
        await self._consumer.commit({partition: record.offset + 1})

    async def seek(self, record: Record) -> None:
        self._consumer.seek(TopicPartition(record.topic, record.partition), record.offset)


class KafkaDeadLetters:
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def send(self, record: Record, error: str) -> None:
        await self._producer.send_and_wait(
            self._topic,
            value=record.value,
            key=record.key,
            headers=[
                *record.headers,
                ("dlq-error", error.encode()),
                ("dlq-origin", f"{record.topic}/{record.partition}/{record.offset}".encode()),
            ],
        )


def connection_options(settings: Settings) -> dict[str, Any]:
    """Security options shared by the consumer and the producer."""
    options: dict[str, Any] = {"security_protocol": settings.kafka_security_protocol}

    if settings.kafka_security_protocol in ("SSL", "SASL_SSL"):
        # Verifies the broker certificate and hostname.
        options["ssl_context"] = create_ssl_context(cafile=settings.kafka_ssl_cafile)

    if settings.kafka_security_protocol.startswith("SASL"):
        assert settings.kafka_sasl_password is not None  # enforced by Settings
        options["sasl_mechanism"] = settings.kafka_sasl_mechanism
        options["sasl_plain_username"] = settings.kafka_sasl_username
        options["sasl_plain_password"] = settings.kafka_sasl_password.get_secret_value()

    return options


def build_consumer(settings: Settings) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_brokers,
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        **connection_options(settings),
    )


def build_producer(settings: Settings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.kafka_brokers,
        acks="all",
        enable_idempotence=True,
        **connection_options(settings),
    )
