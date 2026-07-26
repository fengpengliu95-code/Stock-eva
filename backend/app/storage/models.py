from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class StorageReadiness(BaseModel):
    mode: Literal["local", "nas"]
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
    factor_cache_database: Path
