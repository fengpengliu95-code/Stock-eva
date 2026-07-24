from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="STOCK_EVA_",
        extra="ignore",
    )

    app_name: str = "Stock EVA API"
    service_name: str = "stock-eva-api"
    version: str = "0.1.0"
    environment: str = "development"
    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://127.0.0.1:8080",
    ]
    market_data_dir: Path = Path("var/market")
    market_database_name: str = "stock_eva.duckdb"
    user_data_dir: Path = Path("var/user")
    user_database_name: str = "stock_eva_user.sqlite3"
    auto_refresh_enabled: bool = False
    auto_refresh_min_request_interval_seconds: float = Field(
        default=0.5,
        ge=0.2,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
