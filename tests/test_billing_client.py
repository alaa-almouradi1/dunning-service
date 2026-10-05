import httpx
import pytest

from dunning.billing_client import (
    BillingApiError,
    BillingClient,
    BillingUnavailableError,
    PayOutcome,
)
from tests.fake_billing import FakeBillingApi


def client_returning(response: httpx.Response | Exception) -> BillingClient:
    def handle(request: httpx.Request) -> httpx.Response:
        if isinstance(response, Exception):
            raise response
        return response

    return BillingClient(
        httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://billing/")
    )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("succeeded", PayOutcome.SUCCEEDED),
        ("failed", PayOutcome.FAILED),
        ("processing", PayOutcome.PROCESSING),
    ],
)
async def test_payment_attempts_map_to_outcomes(status: str, expected: PayOutcome) -> None:
    client = client_returning(
        httpx.Response(201, json={"data": {"id": "pay-1", "status": status, "failure_code": None}})
    )

    result = await client.pay_invoice("inv-1", "key-1")

    assert result.outcome is expected
    assert result.payment_id == "pay-1"


@pytest.mark.parametrize(
    ("status_code", "code", "expected"),
    [
        (409, "invoice_not_payable", PayOutcome.NOT_PAYABLE),
        (409, "payment_in_progress", PayOutcome.IN_PROGRESS),
        (409, "idempotency_request_in_progress", PayOutcome.IN_PROGRESS),
        (503, "gateway_unavailable", PayOutcome.UNAVAILABLE),
    ],
)
async def test_conflicts_and_outages_map_to_outcomes(
    status_code: int, code: str, expected: PayOutcome
) -> None:
    client = client_returning(httpx.Response(status_code, json={"error": {"code": code}}))

    assert (await client.pay_invoice("inv-1", "key-1")).outcome is expected


async def test_network_errors_mean_the_outcome_is_unknown() -> None:
    client = client_returning(httpx.ConnectTimeout("timed out"))

    result = await client.pay_invoice("inv-1", "key-1")

    assert result.outcome is PayOutcome.UNAVAILABLE
    assert "ConnectTimeout" in (result.detail or "")


async def test_unexpected_answers_fail_loudly() -> None:
    client = client_returning(httpx.Response(401, json={"error": {"code": "unauthenticated"}}))

    with pytest.raises(BillingApiError, match="401"):
        await client.pay_invoice("inv-1", "key-1")


async def test_requests_carry_the_idempotency_key() -> None:
    api = FakeBillingApi()
    api.queue_payment("inv-1", "succeeded")

    await api.client().pay_invoice("inv-1", "dunning:inv-1:retry:1")

    assert api.requests[0].headers["Idempotency-Key"] == "dunning:inv-1:retry:1"
    assert api.requests[0].url.path == "/api/v1/invoices/inv-1/pay"


async def test_write_off_and_cancel() -> None:
    api = FakeBillingApi()
    client = api.client()

    assert await client.mark_uncollectible("inv-1", "k1") is True
    await client.cancel_subscription("sub-1", "k2")

    assert api.payload(api.calls("/cancel")[0]) == {"at_period_end": False}


async def test_write_off_of_a_paid_invoice_reports_false() -> None:
    api = FakeBillingApi()
    api.uncollectible_status = 409

    assert await api.client().mark_uncollectible("inv-1", "k1") is False


async def test_server_errors_on_write_off_are_retryable() -> None:
    client = client_returning(httpx.Response(502))

    with pytest.raises(BillingUnavailableError):
        await client.mark_uncollectible("inv-1", "k1")


async def test_a_customer_without_a_card_counts_as_a_failed_attempt() -> None:
    client = client_returning(
        httpx.Response(422, json={"error": {"code": "payment_method_required"}})
    )

    result = await client.pay_invoice("inv-1", "key-1")

    assert result.outcome is PayOutcome.FAILED
    assert result.failure_code == "payment_method_required"
