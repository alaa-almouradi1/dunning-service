from uuid import UUID, uuid4

import pytest

from dunning.events import (
    InvalidEventError,
    PaymentFailed,
    parse_data,
    parse_envelope,
)
from tests.factories import payment_failed, to_bytes


def test_parses_a_payment_failed_event() -> None:
    invoice_id = uuid4()
    raw = to_bytes(payment_failed(invoice_id, failed_attempts=2))

    envelope = parse_envelope(raw)
    data = parse_data(envelope, PaymentFailed)

    assert envelope.type == "payment.failed"
    assert data.invoice_id == invoice_id
    assert data.failed_attempts == 2
    assert data.amount.amount == 4900


def test_unknown_fields_are_ignored_for_forward_compatibility() -> None:
    event = payment_failed(uuid4())
    event["new_envelope_field"] = "added later"
    event["data"]["new_data_field"] = {"nested": True}

    data = parse_data(parse_envelope(to_bytes(event)), PaymentFailed)

    assert isinstance(data.payment_id, UUID)


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"{}",
        b'{"id": "not-a-uuid", "type": "x", "version": 1, "source": "s",'
        b' "occurred_at": "2026-01-01T00:00:00Z", "data": {}}',
    ],
)
def test_malformed_envelopes_are_invalid(raw: bytes) -> None:
    with pytest.raises(InvalidEventError):
        parse_envelope(raw)


def test_timestamps_without_a_timezone_are_rejected() -> None:
    event = payment_failed(uuid4(), occurred_at="2026-01-01T00:00:00")

    with pytest.raises(InvalidEventError):
        parse_envelope(to_bytes(event))


def test_unsupported_versions_are_invalid_rather_than_misread() -> None:
    event = payment_failed(uuid4(), version=2)

    with pytest.raises(InvalidEventError, match="Unsupported version 2"):
        parse_envelope(to_bytes(event))


def test_missing_payload_fields_are_invalid() -> None:
    event = payment_failed(uuid4())
    del event["data"]["failed_attempts"]

    with pytest.raises(InvalidEventError, match=r"payment\.failed"):
        parse_data(parse_envelope(to_bytes(event)), PaymentFailed)
