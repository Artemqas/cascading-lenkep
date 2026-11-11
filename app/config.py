from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://cascading:cascading@localhost:5432/cascading"
    max_cascade_attempts: int = 10
    reconciliation_deadline_seconds: float = 10.0
    merchant_webhook_secret: str = "dev-secret"


@lru_cache
def get_settings() -> Settings:
    return Settings()
