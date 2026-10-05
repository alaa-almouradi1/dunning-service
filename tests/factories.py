"""Builders for billing events as they appear on the wire."""

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4


def envelope(event_type: str, data: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    return {
        "id": str(uuid4()),
        "type": event_type,
        "version": 1,
        "source": "subscription-billing-api",
        "occurred_at": datetime.now(UTC).isoformat(),
        "data": data,
    } | overrides


def payment_failed(
    invoice_id: UUID,
    subscription_id: UUID | None = None,
    *,
    failed_attempts: int = 1,
    failure_code: str = "insufficient_funds",
    **overrides: Any,
) -> dict[str, Any]:
    return envelope(
        "payment.failed",
        {
            "payment_id": str(uuid4()),
            "invoice_id": str(invoice_id),
            "subscription_id": str(subscription_id or uuid4()),
            "customer_id": str(uuid4()),
            "amount": {"amount": 4900, "currency": "EUR"},
            "failure_code": failure_code,
            "failure_message": "The card has insufficient funds.",
            "failed_attempts": failed_attempts,
        },
        **overrides,
    )


def payment_succeeded(invoice_id: UUID, subscription_id: UUID | None = None) -> dict[str, Any]:
    return envelope(
        "payment.succeeded",
        {
            "payment_id": str(uuid4()),
            "invoice_id": str(invoice_id),
            "subscription_id": str(subscription_id or uuid4()),
            "customer_id": str(uuid4()),
            "amount": {"amount": 4900, "currency": "EUR"},
        },
    )


def invoice_event(event_type: str, invoice_id: UUID, status: str) -> dict[str, Any]:
    return envelope(
        event_type,
        {
            "invoice_id": str(invoice_id),
            "invoice_number": "INV-2026-000001",
            "subscription_id": str(uuid4()),
            "customer_id": str(uuid4()),
            "status": status,
            "amount_due": {"amount": 4900, "currency": "EUR"},
        },
    )


def subscription_canceled(subscription_id: UUID, reason: str = "requested") -> dict[str, Any]:
    return envelope(
        "subscription.canceled",
        {
            "subscription_id": str(subscription_id),
            "customer_id": str(uuid4()),
            "plan_id": str(uuid4()),
            "status": "canceled",
            "current_period_end": datetime.now(UTC).isoformat(),
            "reason": reason,
        },
    )


def to_bytes(event: dict[str, Any]) -> bytes:
    return json.dumps(event).encode()
