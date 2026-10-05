"""The billing event contract, as consumed by this service.

Mirrors docs/events.md in subscription-billing-api. Parsing is deliberately
lenient where the contract allows evolution (unknown fields are ignored) and
strict where it does not (a wrong version or a missing field is an error).
"""

from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

SUPPORTED_VERSION = 1


class InvalidEventError(Exception):
    """A message that can never be processed, however often it is retried.

    The consumer sends these to the dead-letter topic instead of blocking
    the partition.
    """


class _Model(BaseModel):
    # Producers may add fields at any time; consumers must ignore them.
    model_config = ConfigDict(frozen=True, extra="ignore")


class Money(_Model):
    amount: int
    currency: str = Field(min_length=3, max_length=3)


class Envelope(_Model):
    id: UUID
    type: str
    version: int
    source: str
    occurred_at: AwareDatetime
    data: dict[str, Any]


class PaymentFailed(_Model):
    payment_id: UUID
    invoice_id: UUID
    subscription_id: UUID | None
    customer_id: UUID
    amount: Money
    failure_code: str | None = None
    failure_message: str | None = None
    failed_attempts: int = Field(ge=1)


class PaymentSucceeded(_Model):
    payment_id: UUID
    invoice_id: UUID
    subscription_id: UUID | None
    customer_id: UUID
    amount: Money


class InvoiceChanged(_Model):
    """invoice.paid, invoice.voided, invoice.marked_uncollectible"""

    invoice_id: UUID
    invoice_number: str
    subscription_id: UUID | None
    customer_id: UUID
    status: str
    amount_due: Money


class SubscriptionChanged(_Model):
    """subscription.* events"""

    subscription_id: UUID
    customer_id: UUID
    plan_id: UUID
    status: str
    reason: str | None = None


def parse_envelope(raw: bytes) -> Envelope:
    try:
        envelope = Envelope.model_validate_json(raw)
    except ValidationError as e:
        raise InvalidEventError(f"Malformed event envelope ({e.error_count()} error(s))") from e

    if envelope.version != SUPPORTED_VERSION:
        raise InvalidEventError(
            f"Unsupported version {envelope.version} of {envelope.type} "
            f"(this consumer understands version {SUPPORTED_VERSION})"
        )

    return envelope


def parse_data[T: BaseModel](envelope: Envelope, model: type[T]) -> T:
    try:
        return model.model_validate(envelope.data)
    except ValidationError as e:
        raise InvalidEventError(
            f"Malformed data for {envelope.type} ({e.error_count()} error(s))"
        ) from e
