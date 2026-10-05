# 4. Run the consumer and the scheduler as separate deployments

Status: accepted

## Context

Both background tasks live in one codebase and one image, but they scale
along different axes:

- **Consumer**: Kafka assigns each partition to at most one consumer in a
  group, so more replicas than partitions only gives standbys.
- **Scheduler**: claims due work with `FOR UPDATE SKIP LOCKED` and leases, so
  any number of replicas can share it, and its load grows with the number of
  failed payments, not with event volume.

Running both in every pod forces a single replica count onto two different
needs, and a slow billing API (scheduler work) would compete with event
consumption for the same pool of resources.

## Decision

The Helm chart renders two Deployments from the same image, with
`DUNNING_RUN_CONSUMER` / `DUNNING_RUN_SCHEDULER` switched on for one and off
for the other, each with its own replica count and disruption budget. For
local development a single process can still run both.

## Consequences

- Consumers can be sized to the partition count and schedulers to the retry
  backlog, independently.
- Each pod's connection pool is sized for one job, not two.
- One more Deployment to operate, with identical probes and configuration
  from shared templates.
