# 1. At-least-once consumption with idempotent handlers

Status: accepted

## Context

Kafka offers at-most-once (commit before processing: a crash loses events)
or at-least-once (commit after processing: a crash replays events)
delivery. The billing API's outbox is at-least-once too. Losing a
`payment.failed` event means a customer is never retried; processing one
twice must not schedule two retries.

## Decision

- Auto-commit is off. For each record, the handler runs in one database
  transaction that **also stores the event ID** (`processed_events`). The
  offset is committed only after that transaction commits.
- A redelivered event finds its ID and is skipped.
- Failure counts only move forward (`apply_failure` ignores counts at or
  below the current one), so even a re-published event with a new ID, or
  out-of-order delivery, cannot rewind a case.
- Poison messages (malformed, unknown version) go to `billing.events.dlq`
  with the error in a header, and the partition moves on. Transient errors
  rewind **only the affected partition** to the failed record and back off
  exponentially (capped at 30 s).

## Consequences

- No exactly-once infrastructure (Kafka transactions) is needed; the
  database is the deduplication point.
- Committing per record is chatty. At dunning volumes (a fraction of all
  payments) that is irrelevant; batching commits per poll would be the
  first optimisation.
- `processed_events` grows forever; a retention job (e.g. 30 days, well
  beyond Kafka's retention) is a known to-do.
