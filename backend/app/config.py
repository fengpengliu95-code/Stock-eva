from datetime import date
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
    provider_health_database_name: str = "provider_health.sqlite3"
    provider_registry_database_name: str = "provider_registry.sqlite3"
    local_staging_dir: Path = Path("var/staging")
    local_lock_dir: Path = Path("var/locks")
    local_temp_dir: Path = Path("var/tmp")
    provider_evidence_root: Path = Path("var/evidence")
    provider_evidence_enabled: bool = False
    provider_evidence_max_object_bytes: int = Field(default=64 * 1024 * 1024, ge=1)
    provider_evidence_max_manifest_bytes: int = Field(default=1 * 1024 * 1024, ge=1)
    provider_evidence_max_rows: int = Field(default=10_000_000, ge=1)
    provider_evidence_contract_version: str = "r2f2-evidence-v1"
    provider_shadow_root: Path = Path("var/provider-shadow")
    provider_shadow_enabled: bool = False
    provider_shadow_max_object_bytes: int = Field(default=64 * 1024 * 1024, ge=1)
    provider_shadow_max_manifest_bytes: int = Field(default=1 * 1024 * 1024, ge=1)
    provider_shadow_max_rows: int = Field(default=10_000_000, ge=1)
    provider_shadow_max_requests: int = Field(default=256, ge=1, le=10_000)
    provider_shadow_request_timeout_seconds: float = Field(default=30.0, ge=1.0, le=120.0)
    provider_shadow_execute_enabled: bool = False
    provider_tickflow_token_env_name: str = "STOCK_EVA_TICKFLOW_TOKEN"
    provider_tushare_token_env_name: str = "STOCK_EVA_TUSHARE_TOKEN"
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
    provider_circuit_failure_threshold: int = Field(default=3, gt=0)
    provider_circuit_cooldown_seconds: float = Field(default=900.0, gt=0)
    provider_circuit_probe_lease_seconds: float = Field(default=120.0, gt=0)
    market_continuity_start_date: date | None = None
    market_repair_enabled: bool = False
    market_repair_max_attempts: int = Field(default=4, ge=1, le=20)
    market_repair_lease_seconds: int = Field(default=1800, ge=60, le=86400)
    market_repair_retry_base_seconds: int = Field(default=3600, ge=900, le=86400)

    @field_validator("provider_evidence_max_object_bytes")
    @classmethod
    def validate_evidence_object_bound(cls, value: int) -> int:
        if value != 64 * 1024 * 1024:
            raise ValueError("provider evidence object bound is fixed")
        return value

    @field_validator("provider_evidence_max_manifest_bytes")
    @classmethod
    def validate_evidence_manifest_bound(cls, value: int) -> int:
        if value != 1 * 1024 * 1024:
            raise ValueError("provider evidence manifest bound is fixed")
        return value

    @field_validator("provider_evidence_max_rows")
    @classmethod
    def validate_evidence_row_bound(cls, value: int) -> int:
        if value != 10_000_000:
            raise ValueError("provider evidence row bound is fixed")
        return value

    @field_validator("provider_evidence_contract_version")
    @classmethod
    def validate_evidence_contract_version(cls, value: str) -> str:
        if value != "r2f2-evidence-v1":
            raise ValueError("provider evidence contract version is unsupported")
        return value

    @field_validator("provider_registry_database_name")
    @classmethod
    def validate_provider_registry_database_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("provider registry database name must be a local .sqlite3 basename")
        return value

    @field_validator("provider_shadow_max_object_bytes")
    @classmethod
    def validate_shadow_object_bound(cls, value: int) -> int:
        if value != 64 * 1024 * 1024:
            raise ValueError("provider shadow object bound is fixed")
        return value

    @field_validator("provider_shadow_max_manifest_bytes")
    @classmethod
    def validate_shadow_manifest_bound(cls, value: int) -> int:
        if value != 1 * 1024 * 1024:
            raise ValueError("provider shadow manifest bound is fixed")
        return value

    @field_validator("provider_shadow_max_rows")
    @classmethod
    def validate_shadow_row_bound(cls, value: int) -> int:
        if value != 10_000_000:
            raise ValueError("provider shadow row bound is fixed")
        return value

    @field_validator("provider_tickflow_token_env_name")
    @classmethod
    def validate_tickflow_token_env_name(cls, value: str) -> str:
        if value != "STOCK_EVA_TICKFLOW_TOKEN":
            raise ValueError("tickflow credential environment mapping is fixed")
        return value

    @field_validator("provider_tushare_token_env_name")
    @classmethod
    def validate_tushare_token_env_name(cls, value: str) -> str:
        if value != "STOCK_EVA_TUSHARE_TOKEN":
            raise ValueError("tushare credential environment mapping is fixed")
        return value

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

    @field_validator("provider_health_database_name")
    @classmethod
    def validate_provider_health_database_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or value in {".", ".."}
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("provider health database name must be a local .sqlite3 basename")
        return value

    @model_validator(mode="after")
    def private_database_names_are_distinct(self) -> "Settings":
        if self.user_database_name.casefold() == self.portfolio_database_name.casefold():
            raise ValueError("user and portfolio database names must be distinct")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
