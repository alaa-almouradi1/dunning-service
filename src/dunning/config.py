from datetime import timedelta
from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from dunning.policy import DEFAULT_SCHEDULE


class Settings(BaseSettings):
    """Configuration from environment variables prefixed with DUNNING_."""

    model_config = SettingsConfigDict(env_prefix="DUNNING_", env_file=".env", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./dunning.db"

    kafka_brokers: str = "localhost:29092"
    kafka_topic: str = "billing.events"
    kafka_dlq_topic: str = "billing.events.dlq"
    kafka_group_id: str = "dunning-service"

    billing_api_url: str = "http://localhost:8080/api/v1"
    billing_api_key: SecretStr = SecretStr("local-dev-key")
    billing_api_timeout_seconds: float = Field(default=10.0, gt=0)

    # How long to wait after the 1st, 2nd, ... failure before retrying. After
    # the last entry the invoice is written off and the subscription canceled.
    # JSON list of ISO 8601 durations (or seconds), e.g.
    #   DUNNING_RETRY_SCHEDULE='["P1D","P3D","P5D","P7D"]'   (production default)
    #   DUNNING_RETRY_SCHEDULE='["PT1M","PT2M"]'             (live demo)
    retry_schedule: list[timedelta] = Field(default_factory=lambda: list(DEFAULT_SCHEDULE))

    scheduler_interval_seconds: float = Field(default=30.0, gt=0)
    scheduler_batch_size: int = Field(default=50, gt=0)

    run_consumer: bool = True
    run_scheduler: bool = True

    log_level: str = "INFO"
    log_json: bool = True

    @field_validator("retry_schedule")
    @classmethod
    def _schedule_is_positive(cls, value: list[timedelta]) -> list[timedelta]:
        if not value or any(wait <= timedelta(0) for wait in value):
            raise ValueError("retry_schedule must be a non-empty list of positive durations")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
