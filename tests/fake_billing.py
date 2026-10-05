"""An in-process fake of the billing API, plugged into httpx via MockTransport."""

import json
from collections.abc import Callable
from typing import Any

import httpx

from dunning.billing_client import BillingClient

Responder = Callable[[httpx.Request], httpx.Response]


class FakeBillingApi:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        # invoice_id -> queue of responses for POST /invoices/{id}/pay
        self.pay_responses: dict[str, list[httpx.Response]] = {}
        self.uncollectible_status = 200
        self.cancel_status = 200
        self.replayed: dict[str, httpx.Response] = {}

    def client(self) -> BillingClient:
        transport = httpx.MockTransport(self._handle)
        return BillingClient(
            httpx.AsyncClient(transport=transport, base_url="http://billing/api/v1/")
        )

    def queue_payment(self, invoice_id: str, status: str, failure_code: str | None = None) -> None:
        self.pay_responses.setdefault(invoice_id, []).append(
            httpx.Response(
                201,
                json={
                    "data": {
                        "id": f"pay-{len(self.requests)}-{status}",
                        "status": status,
                        "failure_code": failure_code,
                    }
                },
            )
        )

    def queue_error(self, invoice_id: str, status_code: int, code: str) -> None:
        self.pay_responses.setdefault(invoice_id, []).append(
            httpx.Response(status_code, json={"error": {"code": code, "message": code}})
        )

    def calls(self, suffix: str) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path.endswith(suffix)]

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        key = request.headers.get("Idempotency-Key", "")

        # Behave like the real API: same key + same request -> same answer.
        if key in self.replayed:
            return self.replayed[key]

        if path.endswith("/pay"):
            invoice_id = path.split("/")[-2]
            queue = self.pay_responses.get(invoice_id) or [
                httpx.Response(500, json={"error": {"code": "no_fake_response"}})
            ]
            response = queue.pop(0)
        elif path.endswith("/mark-uncollectible"):
            status = self.uncollectible_status
            response = httpx.Response(status, json=_body(status))
        elif path.endswith("/cancel"):
            response = httpx.Response(self.cancel_status, json=_body(self.cancel_status))
        else:
            response = httpx.Response(404)

        if response.status_code < 500:
            self.replayed[key] = response
        return response

    @staticmethod
    def payload(request: httpx.Request) -> Any:
        return json.loads(request.content or b"{}")


def _body(status: int) -> dict[str, Any]:
    if status == 200:
        return {"data": {}}
    return {"error": {"code": "invalid_state_transition"}}
