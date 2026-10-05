"""Persistence: dunning cases, retry attempts and processed events."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    DateTime,
    Dialect,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(UTC)


class UTCDateTime(TypeDecorator[datetime]):
    """Stores naive UTC, always returns timezone-aware UTC.

    MariaDB DATETIME and SQLite have no time zone, so values are normalised
    on the way in and made aware again on the way out. Comparing aware and
    naive datetimes is a classic source of scheduling bugs.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("Naive datetimes are not allowed; use timezone-aware UTC")
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        return None if value is None else value.replace(tzinfo=UTC)


class CaseStatus(StrEnum):
    RETRYING = "retrying"  # waiting for next_attempt_at
    RETRY_IN_FLIGHT = "retry_in_flight"  # claimed by the scheduler, calling the billing API
    EXHAUSTING = "exhausting"  # out of retries; write-off and cancellation pending
    RECOVERED = "recovered"  # the invoice got paid
    EXHAUSTED = "exhausted"  # written off and subscription canceled
    CLOSED = "closed"  # no longer collectible for other reasons (voided, canceled)

    @property
    def is_active(self) -> bool:
        return self in ACTIVE_STATUSES


ACTIVE_STATUSES = frozenset(
    {CaseStatus.RETRYING, CaseStatus.RETRY_IN_FLIGHT, CaseStatus.EXHAUSTING}
)


class Base(DeclarativeBase):
    pass


class DunningCase(Base):
    """One unpaid invoice that we are trying to recover."""

    __tablename__ = "dunning_cases"
    __table_args__ = (Index("ix_dunning_cases_due", "status", "next_attempt_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    invoice_id: Mapped[str] = mapped_column(String(36), unique=True)
    subscription_id: Mapped[str | None] = mapped_column(String(36))
    customer_id: Mapped[str] = mapped_column(String(36))
    status: Mapped[CaseStatus] = mapped_column(
        Enum(
            CaseStatus,
            native_enum=False,
            length=32,
            values_callable=lambda statuses: [s.value for s in statuses],
        )
    )
    failed_attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    last_failure_code: Mapped[str | None] = mapped_column(String(64))
    amount: Mapped[int] = mapped_column(Integer)
    currency: Mapped[str] = mapped_column(String(3))
    closed_reason: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow, onupdate=utcnow)

    attempts: Mapped[list["RetryAttempt"]] = relationship(
        back_populates="case", order_by="RetryAttempt.attempt_number"
    )


class RetryAttempt(Base):
    """One retry we made (or are making) through the billing API."""

    __tablename__ = "retry_attempts"
    __table_args__ = (UniqueConstraint("case_id", "attempt_number"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[int] = mapped_column(ForeignKey("dunning_cases.id", ondelete="CASCADE"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    # Sent as the billing API's Idempotency-Key: re-sending the same logical
    # attempt (after a crash or timeout) can never charge twice.
    idempotency_key: Mapped[str] = mapped_column(String(128), unique=True)
    executions: Mapped[int] = mapped_column(Integer, default=0)
    outcome: Mapped[str | None] = mapped_column(String(32))
    payment_id: Mapped[str | None] = mapped_column(String(36))
    detail: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    case: Mapped[DunningCase] = relationship(back_populates="attempts")


class ProcessedEvent(Base):
    """Event IDs already applied: makes the at-least-once consumer idempotent."""

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64))
    processed_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow, index=True)


def create_engine(
    url: str, *, pool_size: int = 5, max_overflow: int = 5, **kwargs: Any
) -> AsyncEngine:
    if not url.startswith("sqlite"):
        kwargs |= {
            "pool_size": pool_size,
            "max_overflow": max_overflow,
            # Recycle before MariaDB's wait_timeout closes idle connections.
            "pool_recycle": 1800,
        }
    return create_async_engine(url, pool_pre_ping=True, **kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
