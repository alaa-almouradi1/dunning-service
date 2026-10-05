"""Client for the subscription billing API."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Self

import httpx

from dunning.config import Settings


class BillingApiError(Exception):
    """An answer we cannot act on (bad credentials, validation error, ...)."""


class BillingUnavailableError(Exception):
    """The API could not be reached or failed with a 5xx; try again later."""


class PayOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PROCESSING = "processing"  # provider has not decided yet; a webhook will
    NOT_PAYABLE = "not_payable"  # already paid or voided
    IN_PROGRESS = "in_progress"  # another attempt is still running
    UNAVAILABLE = "unavailable"  # network error or 5xx: outcome unknown


@dataclass(frozen=True)
class PayResult:
    outcome: PayOutcome
    payment_id: str | None = None
    failure_code: str | None = None
    detail: str | None = None


class BillingClient:
    def __init__(self, http: httpx.AsyncClient) -> None:
        self._http = http

    @classmethod
    def from_settings(cls, settings: Settings) -> Self:
        return cls(
            httpx.AsyncClient(
                base_url=settings.billing_api_url.rstrip("/") + "/",
                headers={
                    "X-Api-Key": settings.billing_api_key.get_secret_value(),
                    "Accept": "application/json",
                },
                timeout=settings.billing_api_timeout_seconds,
            )
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def pay_invoice(self, invoice_id: str, idempotency_key: str) -> PayResult:
        """Retry the invoice with the customer's current payment method.

        The idempotency key makes it safe to call again for the same logical
        attempt: the API replays the first answer instead of charging twice.
        """
        try:
            response = await self._post(f"invoices/{invoice_id}/pay", idempotency_key)
        except BillingUnavailableError as e:
            return PayResult(PayOutcome.UNAVAILABLE, detail=str(e))

        body = _json(response)

        if response.status_code == 201:
            payment = body.get("data", {})
            status = payment.get("status")
            outcome = {
                "succeeded": PayOutcome.SUCCEEDED,
                "failed": PayOutcome.FAILED,
                "processing": PayOutcome.PROCESSING,
            }.get(status)
            if outcome is None:
                raise BillingApiError(f"Unknown payment status {status!r}")
            return PayResult(outcome, payment.get("id"), payment.get("failure_code"))

        code = _error_code(body)
        if response.status_code == 409 and code == "invoice_not_payable":
            return PayResult(PayOutcome.NOT_PAYABLE, detail=code)
        if response.status_code == 409 and code in {
            "payment_in_progress",
            "idempotency_request_in_progress",
        }:
            return PayResult(PayOutcome.IN_PROGRESS, detail=code)

        raise BillingApiError(f"POST pay returned {response.status_code}: {code or body}")

    async def mark_uncollectible(self, invoice_id: str, idempotency_key: str) -> bool:
        """Write the invoice off. False if it can no longer be (paid or voided)."""
        response = await self._post(f"invoices/{invoice_id}/mark-uncollectible", idempotency_key)
        if response.status_code == 200:
            return True
        if response.status_code == 409:
            return False
        raise BillingApiError(f"mark-uncollectible returned {response.status_code}")

    async def cancel_subscription(self, subscription_id: str, idempotency_key: str) -> None:
        response = await self._post(
            f"subscriptions/{subscription_id}/cancel", idempotency_key, {"at_period_end": False}
        )
        already_canceled = (
            response.status_code == 409 and _error_code(_json(response)) == "subscription_canceled"
        )
        if response.status_code != 200 and not already_canceled:
            raise BillingApiError(f"cancel returned {response.status_code}")

    async def _post(
        self, path: str, idempotency_key: str, payload: dict[str, Any] | None = None
    ) -> httpx.Response:
        try:
            response = await self._http.post(
                path, json=payload or {}, headers={"Idempotency-Key": idempotency_key}
            )
        except httpx.TransportError as e:
            raise BillingUnavailableError(f"{type(e).__name__}: {e}") from e

        if response.status_code >= 500:
            raise BillingUnavailableError(
                f"{response.status_code}: {_error_code(_json(response)) or 'server error'}"
            )
        return response


def _json(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _error_code(body: dict[str, Any]) -> str | None:
    error = body.get("error")
    return error.get("code") if isinstance(error, dict) else None
