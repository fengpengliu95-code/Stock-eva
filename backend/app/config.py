import json
import os
from collections.abc import Mapping
from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _validate_provider_priority(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = tuple(value.split(","))
    if not isinstance(value, (tuple, list)):
        raise ValueError("invalid market provider priority")
    priority = tuple(value)
    known = {"baostock", "tickflow", "tushare"}
    if any(
        not isinstance(item, str) or not item or item.strip() != item or item not in known
        for item in priority
    ):
        raise ValueError("invalid market provider priority")
    if not priority or priority[0] != "baostock" or len(set(priority)) != len(priority):
        raise ValueError("invalid market provider priority")
    return priority


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
    calendar_runtime_enabled: bool = False
    calendar_generation_database_name: str = "calendar_generations.sqlite3"
    regime_snapshot_database_name: str = "market_regime_snapshots.sqlite3"
    provider_health_database_name: str = "provider_health.sqlite3"
    provider_registry_database_name: str = "provider_registry.sqlite3"
    universe_contract_database_name: str = "market_universe.sqlite3"
    market_universe_mode: str = "off"
    market_universe_maintenance_enabled: bool = False
    market_universe_maintenance_interval_seconds: int = Field(default=86400, ge=60, le=604800)
    daily_bar_shadow_database_name: str = "daily_bar_shadow.sqlite3"
    daily_bar_shadow_calendar_root: Path | None = None
    local_staging_dir: Path = Path("var/staging")
    local_lock_dir: Path = Path("var/locks")
    local_temp_dir: Path = Path("var/tmp")
    provider_evidence_root: Path = Path("var/evidence")
    provider_evidence_enabled: bool = False
    provider_evidence_max_object_bytes: int = Field(default=64 * 1024 * 1024, ge=1)
    provider_evidence_max_manifest_bytes: int = Field(default=1 * 1024 * 1024, ge=1)
    provider_evidence_max_rows: int = Field(default=10_000_000, ge=1)
    provider_evidence_contract_version: str = "r2f2-evidence-v1"
    provider_shadow_root: Path = Path.cwd() / "var/provider-shadow"
    provider_shadow_enabled: bool = False
    provider_shadow_max_object_bytes: int = Field(default=64 * 1024 * 1024, ge=1)
    provider_shadow_max_manifest_bytes: int = Field(default=1 * 1024 * 1024, ge=1)
    provider_shadow_max_rows: int = Field(default=10_000_000, ge=1)
    provider_shadow_max_requests: int = Field(default=256, ge=1, le=10_000)
    provider_shadow_max_attempts: int = Field(default=3, ge=1, le=5)
    provider_shadow_max_response_bytes: int = Field(default=8 * 1024 * 1024, ge=1)
    provider_shadow_max_retry_after_seconds: float = Field(default=10.0, ge=0, le=60)
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
    market_auto_failover_enabled: bool = False
    market_provider_priority: tuple[str, ...] = ("baostock",)
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

    @field_validator("market_provider_priority", mode="before")
    @classmethod
    def validate_market_provider_priority(cls, value: object) -> tuple[str, ...]:
        return _validate_provider_priority(value)

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

    @field_validator(
        "provider_registry_database_name",
        "daily_bar_shadow_database_name",
        "calendar_generation_database_name",
        "universe_contract_database_name",
    )
    @classmethod
    def validate_provider_registry_database_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("provider sidecar database name must be a local .sqlite3 basename")
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

    @field_validator("provider_shadow_root")
    @classmethod
    def validate_provider_shadow_root_absolute(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("provider shadow root must be absolute")
        return value

    @field_validator("daily_bar_shadow_calendar_root")
    @classmethod
    def validate_daily_bar_shadow_calendar_root(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("Daily Bar shadow calendar root must be absolute")
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
        if self.market_universe_mode not in {"off", "shadow", "enforce"}:
            raise ValueError("market universe mode is unsupported")
        control_databases = (
            self.factor_cache_database_name,
            self.calendar_sync_database_name,
            self.calendar_generation_database_name,
            self.regime_snapshot_database_name,
            self.provider_health_database_name,
            self.provider_registry_database_name,
            self.daily_bar_shadow_database_name,
            self.universe_contract_database_name,
        )
        if len({name.casefold() for name in control_databases}) != len(control_databases):
            raise ValueError("local control database names must be distinct")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()


class CalendarRuntimeSettings(BaseModel):
    """The closed, credential-free settings surface for runtime calendars."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True, allow_inf_nan=False)

    calendar_runtime_enabled: bool = False
    calendar_generation_database_name: str = "calendar_generations.sqlite3"
    calendar_sync_database_name: str = "calendar_sync.sqlite3"
    local_control_dir: Path = Path.cwd() / "var/control"
    local_lock_dir: Path = Path.cwd() / "var/locks"
    provider_health_database_name: str = "provider_health.sqlite3"
    baostock_socket_timeout_seconds: float = Field(default=30.0, ge=1.0, le=120.0)
    auto_refresh_min_request_interval_seconds: float = Field(default=0.5, ge=0.2)
    provider_circuit_failure_threshold: int = Field(default=3, gt=0)
    provider_circuit_cooldown_seconds: float = Field(default=900.0, gt=0)
    provider_circuit_probe_lease_seconds: float = Field(default=120.0, gt=0)

    @field_validator(
        "calendar_generation_database_name",
        "calendar_sync_database_name",
        "provider_health_database_name",
    )
    @classmethod
    def validate_calendar_database_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or value in {".", ".."}
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("calendar runtime database name must be a local .sqlite3 basename")
        return value

    @field_validator("local_control_dir", "local_lock_dir")
    @classmethod
    def validate_calendar_runtime_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("calendar runtime path must be absolute")
        return value

    @model_validator(mode="after")
    def calendar_database_names_are_distinct(self) -> "CalendarRuntimeSettings":
        names = (
            self.calendar_generation_database_name,
            self.calendar_sync_database_name,
            self.provider_health_database_name,
        )
        if len({name.casefold() for name in names}) != len(names):
            raise ValueError("calendar runtime database names must be distinct")
        return self

    @classmethod
    def from_settings(cls, settings: Settings) -> "CalendarRuntimeSettings":
        if not isinstance(settings, Settings):
            raise TypeError("settings must be Settings")
        return cls(
            calendar_runtime_enabled=settings.calendar_runtime_enabled,
            calendar_generation_database_name=settings.calendar_generation_database_name,
            calendar_sync_database_name=settings.calendar_sync_database_name,
            local_control_dir=_anchor_runtime_path(settings.local_control_dir),
            local_lock_dir=_anchor_runtime_path(settings.local_lock_dir),
            provider_health_database_name=settings.provider_health_database_name,
            baostock_socket_timeout_seconds=settings.baostock_socket_timeout_seconds,
            auto_refresh_min_request_interval_seconds=settings.auto_refresh_min_request_interval_seconds,
            provider_circuit_failure_threshold=settings.provider_circuit_failure_threshold,
            provider_circuit_cooldown_seconds=settings.provider_circuit_cooldown_seconds,
            provider_circuit_probe_lease_seconds=settings.provider_circuit_probe_lease_seconds,
        )


def _anchor_runtime_path(value: Path | str) -> Path:
    """Anchor once without resolving symlinks or touching the filesystem."""
    return Path(os.path.abspath(os.fspath(value)))


def _runtime_bool(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("calendar runtime boolean setting is invalid")


def _runtime_float(value: str | None, *, default: float) -> float:
    if value is None or not value.strip():
        if value is None:
            return default
        raise ValueError("calendar runtime numeric setting is invalid")
    return float(value)


def _runtime_int(value: str | None, *, default: int) -> int:
    if value is None or not value.strip():
        if value is None:
            return default
        raise ValueError("calendar runtime numeric setting is invalid")
    return int(value)


def _runtime_path(value: str | None, *, default: Path) -> Path:
    if value is None:
        return default
    if not value.strip():
        raise ValueError("calendar runtime path setting is invalid")
    return _anchor_runtime_path(value)


def get_calendar_runtime_settings(
    settings: Settings | Mapping[str, str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> CalendarRuntimeSettings:
    """Load exactly the calendar runtime allowlist, or project resolved Settings.

    This intentionally does not use ``Settings()``: that would load ``.env`` and
    inspect unrelated credentials.  The eleven ``get`` calls below are the full
    process-environment boundary for the runtime calendar lane.
    """
    if isinstance(settings, Mapping):
        if environ is not None:
            raise ValueError("settings and environ are mutually exclusive")
        environ = settings
        settings = None
    if settings is not None:
        if environ is not None:
            raise ValueError("settings and environ are mutually exclusive")
        return CalendarRuntimeSettings.from_settings(settings)
    source = os.environ if environ is None else environ
    enabled = source.get("STOCK_EVA_CALENDAR_RUNTIME_ENABLED")
    generation_name = source.get("STOCK_EVA_CALENDAR_GENERATION_DATABASE_NAME")
    sync_name = source.get("STOCK_EVA_CALENDAR_SYNC_DATABASE_NAME")
    control_dir = source.get("STOCK_EVA_LOCAL_CONTROL_DIR")
    lock_dir = source.get("STOCK_EVA_LOCAL_LOCK_DIR")
    health_name = source.get("STOCK_EVA_PROVIDER_HEALTH_DATABASE_NAME")
    timeout = source.get("STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS")
    interval = source.get("STOCK_EVA_AUTO_REFRESH_MIN_REQUEST_INTERVAL_SECONDS")
    threshold = source.get("STOCK_EVA_PROVIDER_CIRCUIT_FAILURE_THRESHOLD")
    cooldown = source.get("STOCK_EVA_PROVIDER_CIRCUIT_COOLDOWN_SECONDS")
    lease = source.get("STOCK_EVA_PROVIDER_CIRCUIT_PROBE_LEASE_SECONDS")
    return CalendarRuntimeSettings(
        calendar_runtime_enabled=_runtime_bool(enabled, default=False),
        calendar_generation_database_name=(
            generation_name if generation_name is not None else "calendar_generations.sqlite3"
        ),
        calendar_sync_database_name=sync_name if sync_name is not None else "calendar_sync.sqlite3",
        local_control_dir=_runtime_path(control_dir, default=Path.cwd() / "var/control"),
        local_lock_dir=_runtime_path(lock_dir, default=Path.cwd() / "var/locks"),
        provider_health_database_name=(
            health_name if health_name is not None else "provider_health.sqlite3"
        ),
        baostock_socket_timeout_seconds=_runtime_float(timeout, default=30.0),
        auto_refresh_min_request_interval_seconds=_runtime_float(interval, default=0.5),
        provider_circuit_failure_threshold=_runtime_int(threshold, default=3),
        provider_circuit_cooldown_seconds=_runtime_float(cooldown, default=900.0),
        provider_circuit_probe_lease_seconds=_runtime_float(lease, default=120.0),
    )


class MarketFailoverRuntimeSettings(BaseModel):
    """Credential-free, allowlisted settings used by the readiness CLI."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    market_auto_failover_enabled: bool = False
    market_provider_priority: tuple[str, ...] = ("baostock",)
    provider_shadow_root: Path = Path.cwd() / "var/provider-shadow"
    provider_evidence_root: Path = Path.cwd() / "var/evidence"
    local_control_dir: Path = Path.cwd() / "var/control"
    provider_registry_database_name: str = "provider_registry.sqlite3"
    daily_bar_shadow_database_name: str = "daily_bar_shadow.sqlite3"
    provider_health_database_name: str = "provider_health.sqlite3"
    local_market_dataset_root: Path | None = None
    nas_market_dataset_root: Path | None = None
    daily_bar_shadow_calendar_root: Path | None = None

    @field_validator("market_provider_priority", mode="before")
    @classmethod
    def validate_priority(cls, value: object) -> tuple[str, ...]:
        return _validate_provider_priority(value)

    @field_validator(
        "provider_shadow_root",
        "provider_evidence_root",
        "local_control_dir",
        "local_market_dataset_root",
        "nas_market_dataset_root",
        "daily_bar_shadow_calendar_root",
    )
    @classmethod
    def validate_runtime_paths(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("market failover runtime path must be absolute")
        return value

    @field_validator(
        "provider_registry_database_name",
        "daily_bar_shadow_database_name",
        "provider_health_database_name",
    )
    @classmethod
    def validate_sidecar_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or value in {".", ".."}
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("market failover sidecar name is invalid")
        return value

    @model_validator(mode="after")
    def sidecar_names_are_distinct(self) -> "MarketFailoverRuntimeSettings":
        names = (
            self.provider_registry_database_name,
            self.daily_bar_shadow_database_name,
            self.provider_health_database_name,
        )
        if len({name.casefold() for name in names}) != len(names):
            raise ValueError("market failover sidecar names must be distinct")
        return self


def get_market_failover_runtime_settings(
    environ: Mapping[str, str] | None = None,
) -> MarketFailoverRuntimeSettings:
    """Read only the failover CLI's explicit non-credential allowlist.

    No BaseSettings construction occurs here: only the named Mapping ``get`` calls below are
    allowed, and no token or provider credential can influence the projection.
    """

    source = os.environ if environ is None else environ
    return MarketFailoverRuntimeSettings(
        market_auto_failover_enabled=_closed_bool(
            source, "STOCK_EVA_MARKET_AUTO_FAILOVER_ENABLED", default=False
        ),
        market_provider_priority=_validate_provider_priority(
            source.get("STOCK_EVA_MARKET_PROVIDER_PRIORITY", ("baostock",))
        ),
        provider_shadow_root=_closed_path(
            source,
            "STOCK_EVA_PROVIDER_SHADOW_ROOT",
            default=Path.cwd() / "var/provider-shadow",
        ),
        provider_evidence_root=_closed_path(
            source,
            "STOCK_EVA_PROVIDER_EVIDENCE_ROOT",
            default=Path.cwd() / "var/evidence",
        ),
        local_control_dir=_closed_path(
            source,
            "STOCK_EVA_LOCAL_CONTROL_DIR",
            default=Path.cwd() / "var/control",
        ),
        provider_registry_database_name=source.get(
            "STOCK_EVA_PROVIDER_REGISTRY_DATABASE_NAME", "provider_registry.sqlite3"
        ),
        daily_bar_shadow_database_name=source.get(
            "STOCK_EVA_DAILY_BAR_SHADOW_DATABASE_NAME", "daily_bar_shadow.sqlite3"
        ),
        provider_health_database_name=source.get(
            "STOCK_EVA_PROVIDER_HEALTH_DATABASE_NAME", "provider_health.sqlite3"
        ),
        local_market_dataset_root=_closed_path(
            source, "STOCK_EVA_LOCAL_MARKET_DATASET_ROOT", default=None
        ),
        nas_market_dataset_root=_closed_path(
            source, "STOCK_EVA_NAS_MARKET_DATASET_ROOT", default=None
        ),
        daily_bar_shadow_calendar_root=_closed_path(
            source, "STOCK_EVA_DAILY_BAR_SHADOW_CALENDAR_ROOT", default=None
        ),
    )


class TickFlowFreeRuntimeSettings(BaseModel):
    """Allowlisted settings projection that never enumerates the process environment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_shadow_enabled: bool = False
    provider_shadow_execute_enabled: bool = False
    provider_shadow_root: Path = Path.cwd() / "var/provider-shadow"
    provider_evidence_root: Path = Path("var/evidence")
    local_control_dir: Path = Path("var/control")
    provider_registry_database_name: str = "provider_registry.sqlite3"
    daily_bar_shadow_database_name: str = "daily_bar_shadow.sqlite3"
    daily_bar_shadow_calendar_root: Path | None = None
    local_market_dataset_root: Path | None = None
    nas_market_dataset_root: Path | None = None

    @field_validator("provider_shadow_root")
    @classmethod
    def validate_shadow_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("provider shadow root must be absolute")
        return value

    @field_validator("daily_bar_shadow_calendar_root")
    @classmethod
    def validate_daily_calendar_root(cls, value: Path | None) -> Path | None:
        if value is not None and not value.is_absolute():
            raise ValueError("Free Daily calendar root must be absolute")
        return value

    @field_validator("provider_registry_database_name", "daily_bar_shadow_database_name")
    @classmethod
    def validate_registry_name(cls, value: str) -> str:
        candidate = Path(value)
        if (
            not value
            or candidate.is_absolute()
            or candidate.name != value
            or value in {".", ".."}
            or not value.endswith(".sqlite3")
        ):
            raise ValueError("provider registry database name is invalid")
        return value

    @model_validator(mode="after")
    def daily_database_is_isolated(self) -> "TickFlowFreeRuntimeSettings":
        if (
            self.provider_registry_database_name.casefold()
            == self.daily_bar_shadow_database_name.casefold()
        ):
            raise ValueError("Free provider sidecar database names must be distinct")
        return self


def _closed_bool(environ: Mapping[str, str], name: str, *, default: bool) -> bool:
    value = environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("Free canary boolean setting is invalid")


def _closed_path(environ: Mapping[str, str], name: str, *, default: Path | None) -> Path | None:
    value = environ.get(name)
    if value is None:
        return default
    if not value.strip():
        raise ValueError("Free canary path setting is invalid")
    return Path(value)


def get_tickflow_free_runtime_settings(
    *, environ: Mapping[str, str] | None = None
) -> TickFlowFreeRuntimeSettings:
    """Read only the allowlisted non-credential names required by Free discovery.

    Do not replace this projection with ``Settings()``: BaseSettings enumerates the complete
    environment, which would cross the Free-mode credential-read boundary.
    """

    source = os.environ if environ is None else environ
    return TickFlowFreeRuntimeSettings(
        provider_shadow_enabled=_closed_bool(
            source, "STOCK_EVA_PROVIDER_SHADOW_ENABLED", default=False
        ),
        provider_shadow_execute_enabled=_closed_bool(
            source, "STOCK_EVA_PROVIDER_SHADOW_EXECUTE_ENABLED", default=False
        ),
        provider_shadow_root=_closed_path(
            source,
            "STOCK_EVA_PROVIDER_SHADOW_ROOT",
            default=Path.cwd() / "var/provider-shadow",
        ),
        provider_evidence_root=_closed_path(
            source, "STOCK_EVA_PROVIDER_EVIDENCE_ROOT", default=Path("var/evidence")
        ),
        local_control_dir=_closed_path(
            source, "STOCK_EVA_LOCAL_CONTROL_DIR", default=Path("var/control")
        ),
        provider_registry_database_name=source.get(
            "STOCK_EVA_PROVIDER_REGISTRY_DATABASE_NAME", "provider_registry.sqlite3"
        ),
        daily_bar_shadow_database_name=source.get(
            "STOCK_EVA_DAILY_BAR_SHADOW_DATABASE_NAME", "daily_bar_shadow.sqlite3"
        ),
        daily_bar_shadow_calendar_root=_closed_path(
            source, "STOCK_EVA_DAILY_BAR_SHADOW_CALENDAR_ROOT", default=None
        ),
        local_market_dataset_root=_closed_path(
            source, "STOCK_EVA_LOCAL_MARKET_DATASET_ROOT", default=None
        ),
        nas_market_dataset_root=_closed_path(
            source, "STOCK_EVA_NAS_MARKET_DATASET_ROOT", default=None
        ),
    )
