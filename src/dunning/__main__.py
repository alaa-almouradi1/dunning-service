import os

import uvicorn


def main() -> None:
    uvicorn.run(
        "dunning.app:app",
        host=os.environ.get("DUNNING_HOST", "0.0.0.0"),  # noqa: S104 - runs in a container
        port=int(os.environ.get("DUNNING_PORT", "8000")),
        log_config=None,  # structlog takes over (see dunning.logging_setup)
        timeout_graceful_shutdown=30,
    )


if __name__ == "__main__":
    main()
