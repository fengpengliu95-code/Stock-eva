"""Frozen models for capability-scoped Daily Bar shadow qualification."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

DAILY_SHADOW_PROFILE = "TICKFLOW_FREE_DAILY_BAR_OHLC_V1"
_CANONICAL_SYMBOL = re.compile(r"^(?:sh|sz)\.\d{6}$")


def canonical_json_bytes(value: Any) -> bytes:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return (unicodedata.normalize("NFC", encoded) + "\n").encode("utf-8")


def domain_sha256(domain: str, value: Any) -> str:
    return hashlib.sha256(domain.encode("ascii") + b"\n" + canonical_json_bytes(value)).hexdigest()


def canonical_to_tickflow_daily_symbol(value: str) -> str:
    if not isinstance(value, str) or _CANONICAL_SYMBOL.fullmatch(value) is None:
        raise ValueError("daily shadow symbol mapping unavailable")
    return f"{value[3:]}.{value[:2].upper()}"


class DailyCanonicalLineageState(StrEnum):
    LEGACY_UNAVAILABLE = "LEGACY_UNAVAILABLE"
    R2F2_VERIFIED = "R2F2_VERIFIED"


class DailyCanonicalUnavailableReason(StrEnum):
    ROOT_UNAVAILABLE = "ROOT_UNAVAILABLE"
    CANONICAL_DESCRIPTOR_INVALID = "CANONICAL_DESCRIPTOR_INVALID"
    PARTITION_UNAVAILABLE = "PARTITION_UNAVAILABLE"
    PARTITION_SCHEMA_INVALID = "PARTITION_SCHEMA_INVALID"
    PARTITION_SEMANTICS_INVALID = "PARTITION_SEMANTICS_INVALID"
    CANONICAL_CHANGED = "CANONICAL_CHANGED"


class CanonicalDailyOhlcRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    trade_date: date
    symbol: str = Field(pattern=r"^(?:sh|sz)\.\d{6}$")
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    @model_validator(mode="after")
    def validate_prices(self) -> CanonicalDailyOhlcRow:
        values = (self.open, self.high, self.low, self.close)
        if any(not value.is_finite() or value <= 0 for value in values):
            raise ValueError("canonical Daily OHLC is invalid")
        if self.high < max(values) or self.low > min(values):
            raise ValueError("canonical Daily OHLC ordering is invalid")
        return self


class DailyCanonicalSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    profile: Literal["TICKFLOW_FREE_DAILY_BAR_OHLC_V1"] = DAILY_SHADOW_PROFILE
    trade_date: date
    lineage_state: DailyCanonicalLineageState
    r2f2_lineage: dict[str, str] | None = None
    manifest_generation: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    partition_relative_path: str
    partition_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    partition_row_count: int = Field(ge=1, le=4000)
    eligible_symbol_count: int = Field(ge=1, le=4000)
    excluded_symbol_count: int = Field(ge=0, le=4000)
    canonical_universe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_exclusion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    symbol_mapping_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    ohlc_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    rows: tuple[CanonicalDailyOhlcRow, ...] = Field(min_length=1, max_length=4000)
    snapshot_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_snapshot(self) -> DailyCanonicalSnapshot:
        symbols = tuple(row.symbol for row in self.rows)
        if symbols != tuple(sorted(symbols)) or len(symbols) != len(set(symbols)):
            raise ValueError("canonical Daily rows are not an exact sorted set")
        if len(self.rows) != self.eligible_symbol_count:
            raise ValueError("canonical Daily eligible count mismatch")
        if self.lineage_state == DailyCanonicalLineageState.LEGACY_UNAVAILABLE:
            if self.r2f2_lineage is not None:
                raise ValueError("legacy canonical descriptor cannot claim R2-F2 lineage")
        elif not self.r2f2_lineage:
            raise ValueError("R2-F2 canonical descriptor requires lineage")
        values = self.model_dump(mode="json")
        values.pop("snapshot_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/daily-canonical-snapshot/v1", values)
        if self.snapshot_sha256 == "0" * 64:
            object.__setattr__(self, "snapshot_sha256", expected)
        elif self.snapshot_sha256 != expected:
            raise ValueError("canonical Daily snapshot hash mismatch")
        return self


class DailyCanonicalReadResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["ready", "unavailable"]
    snapshot: DailyCanonicalSnapshot | None = None
    unavailable_reason: DailyCanonicalUnavailableReason | None = None

    @model_validator(mode="after")
    def validate_result(self) -> DailyCanonicalReadResult:
        if (self.status == "ready") != (self.snapshot is not None):
            raise ValueError("canonical Daily read result is inconsistent")
        if (self.status == "unavailable") != (self.unavailable_reason is not None):
            raise ValueError("canonical Daily unavailable reason is inconsistent")
        return self
