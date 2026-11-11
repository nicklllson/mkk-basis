from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "secret-api-key"

    # Retry / DLQ
    max_attempts: int = 3
    retry_base_delay_seconds: int = 5

    # Outbox relay
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 100
    outbox_retention_hours: int = 24
    outbox_cleanup_interval_seconds: float = 3600.0

    # Consumer
    consumer_concurrency: int = 10
    webhook_timeout_seconds: float = 10.0

    # Эмуляция платёжного шлюза
    processing_min_seconds: float = 2.0
    processing_max_seconds: float = 5.0
    processing_success_rate: float = 0.9


settings = Settings()
