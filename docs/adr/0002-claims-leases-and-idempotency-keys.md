# 2. Scheduling retries with claims, leases and idempotency keys

Status: accepted

## Context

Retrying a payment calls the billing API, which charges a card. The
scheduler may run on several replicas, may crash between the call and
recording its result, and the API may time out with an unknown outcome.
None of these may charge a customer twice.

## Decision

1. **Claim**: in a short transaction, select due cases with
   `FOR UPDATE SKIP LOCKED` (replicas skip each other's rows), mark them
   `retry_in_flight` and push `next_attempt_at` forward by a 10-minute
   **lease**.
2. **Call** the API with no transaction open, using a deterministic
   `Idempotency-Key: dunning:{invoice}:retry:{n}`.
3. **Record** the outcome in a second transaction.

If the worker dies after step 2, the lease expires and another worker
retries the *same logical attempt* with the *same key*. The billing API
replays its stored answer instead of charging again.

Network errors and 5xx are "unknown": the attempt is rescheduled 15
minutes later under the same key. Only an explicit `failed` result counts
as a failure.

## Consequences

- At most one charge per logical attempt, across crashes and replicas.
- This relies on the billing API keeping idempotency keys longer than the
  longest retry delay plus lease (24 h vs. 15 min), a cross-service contract
  documented in both repositories.
