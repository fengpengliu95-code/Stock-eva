from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator, model_validator
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
    classification_database_name: str = "classification.duckdb"
    user_data_dir: Path = Path("var/user")
    user_database_name: str = "stock_eva_user.sqlite3"
    portfolio_database_name: str = "stock_eva_portfolio.sqlite3"
    local_control_dir: Path = Path("var/control")
    factor_cache_database_name: str = "baostock_back_factor_cache.sqlite3"
    calendar_sync_database_name: str = "calendar_sync.sqlite3"
    regime_snapshot_database_name: str = "market_regime_snapshots.sqlite3"
    local_staging_dir: Path = Path("var/staging")
    local_lock_dir: Path = Path("var/locks")
    local_temp_dir: Path = Path("var/tmp")
    nas_market_dataset_root: Path | None = None
    local_market_dataset_root: Path | None = None
    supplemental_data_dir: Path | None = None
    supplemental_audit_database_name: str = "supplemental_ingestion.sqlite3"
    akshare_supplemental_enabled: bool = False
    auto_refresh_enabled: bool = False
    scheduled_refresh_enabled: bool = False
    auto_refresh_min_request_interval_seconds: float = Field(
        default=0.5,
        ge=0.2,
    )
    baostock_socket_timeout_seconds: float = Field(
        default=30.0,
        ge=1.0,
        le=120.0,
    )

    @field_validator("user_database_name", "portfolio_database_name")
    @classmethod
    def validate_private_database_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or value in {".", ".."}
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("private database name must be a local .sqlite3 basename")
        return value

    @field_validator("regime_snapshot_database_name")
    @classmethod
    def validate_regime_snapshot_database_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or value in {".", ".."}
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("regime snapshot database name must be a local .sqlite3 basename")
        return value

    @model_validator(mode="after")
    def private_database_names_are_distinct(self) -> "Settings":
        if self.user_database_name.casefold() == self.portfolio_database_name.casefold():
            raise ValueError("user and portfolio database names must be distinct")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
