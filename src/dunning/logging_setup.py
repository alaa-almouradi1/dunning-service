import logging
import sys

import structlog


def configure_logging(level: str = "INFO", json: bool = True) -> None:
    """Structured logs: one JSON object per line in production, readable in dev.

    Context bound with structlog.contextvars (event ID, invoice ID, Kafka
    offset) is attached to every line logged while handling that work.
    """
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Third-party libraries (uvicorn, aiokafka) use the standard library.
    logging.basicConfig(level=logging.WARNING, stream=sys.stdout, format="%(name)s %(message)s")
