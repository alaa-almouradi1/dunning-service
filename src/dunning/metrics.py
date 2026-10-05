from prometheus_client import Counter, Gauge

EVENTS_PROCESSED = Counter(
    "dunning_events_processed_total",
    "Billing events consumed, by type and result (applied, duplicate, ignored).",
    ["type", "result"],
)
EVENTS_DEAD_LETTERED = Counter(
    "dunning_events_dead_lettered_total",
    "Messages that could never be processed and were sent to the dead-letter topic.",
)
CONSUMER_TRANSIENT_ERRORS = Counter(
    "dunning_consumer_transient_errors_total",
    "Processing failures that will be retried (e.g. database unavailable).",
)
RETRY_ATTEMPTS = Counter(
    "dunning_retry_attempts_total",
    "Payment retries made through the billing API, by outcome.",
    ["outcome"],
)
CASES_EXHAUSTED = Counter(
    "dunning_cases_exhausted_total",
    "Invoices written off after all retries failed.",
)
ACTIVE_CASES = Gauge(
    "dunning_active_cases",
    "Dunning cases still being worked on, by status.",
    ["status"],
)
