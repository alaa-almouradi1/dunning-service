# 3. Reconciling the API response with the event stream

Status: accepted

## Context

After a retry, the scheduler learns the outcome twice: from the HTTP
response, and later from the `payment.failed` / `payment.succeeded` event
the billing API publishes. Either can arrive first, and events can be
delayed (e.g. while the billing outbox relay is down).

Relying only on events would stall dunning whenever Kafka lags. Relying
only on responses would miss payments settled later by webhook, and
payments the customer makes on their own.

## Decision

Apply both, through the same idempotent transitions in `cases.py`:

- `apply_failure(case, n)`: the response handler uses
  `n = failures at claim + 1`, and the event carries the same `n`. Whichever
  comes second is a no-op.
- `apply_recovery(case)`: a no-op once recovered.
- The scheduler only moves a case out of `retry_in_flight` for
  "processing" or "unavailable" outcomes if no event has changed it
  meanwhile.

## Consequences

- Dunning keeps working when the event stream lags, and converges to the
  same state when it catches up.
- The billing API owns the failure count (`failed_attempts` in the event).
  This service never invents its own numbering, which would drift.
