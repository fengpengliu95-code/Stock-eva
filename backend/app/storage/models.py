from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class VerifiedReadySessionInventory(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["ready"] = "ready"
    source: Literal["baostock"] = "baostock"
    manifest_generation: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    manifest_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    sessions: tuple[date, ...]
    verified_at: datetime

    @field_validator("sessions")
    @classmethod
    def require_sorted_unique_sessions(cls, value: tuple[date, ...]) -> tuple[date, ...]:
        if tuple(sorted(set(value))) != value:
            raise ValueError("sessions must be sorted and unique")
        return value

    @field_validator("verified_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("verified_at must be timezone-aware")
        return value.astimezone(UTC)


class StorageReadiness(BaseModel):
    mode: Literal["local", "local_dataset", "nas"]
    status: Literal["ready", "unavailable", "misconfigured"]
    market_data_available: bool
    serving_source: Literal["local", "nas", "none"]
    reason_code: str | None = None
    mount_type: str | None = None
    sentinel_status: Literal["not_required", "ready", "missing", "invalid", "unreadable"]
    manifest_status: Literal["not_required", "ready", "missing", "invalid", "unreadable"]
    dataset_generation: str | None = None
    local_mutable_storage: bool = True
    smb3_expected: bool = True


class DatasetManifest(BaseModel):
    dataset: Literal["stock-eva-market"]
    schema_version: Literal[2]
    generation: str = Field(min_length=1, max_length=128)
    files: list[dict[str, object]]


class DatasetSentinel(BaseModel):
    dataset: Literal["stock-eva-market"]
    schema_version: Literal[2]


class LocalStoragePaths(BaseModel):
    control: Path
    staging: Path
    locks: Path
    temporary: Path
    market_database: Path
    user_database: Path
    portfolio_database: Path
    factor_cache_database: Path
    provider_health_database: Path
