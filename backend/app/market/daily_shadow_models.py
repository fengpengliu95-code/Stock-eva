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


class DailyShadowShard(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ordinal: int = Field(ge=0, lt=40)
    request_id: str = Field(pattern=r"^[A-Za-z0-9._-]{1,128}$")
    canonical_symbols: tuple[str, ...] = Field(min_length=1, max_length=100)
    provider_symbols: tuple[str, ...] = Field(min_length=1, max_length=100)
    request_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_shard(self) -> DailyShadowShard:
        if (
            self.canonical_symbols != tuple(sorted(self.canonical_symbols))
            or len(set(self.canonical_symbols)) != len(self.canonical_symbols)
            or len(self.provider_symbols) != len(self.canonical_symbols)
            or self.provider_symbols
            != tuple(canonical_to_tickflow_daily_symbol(item) for item in self.canonical_symbols)
        ):
            raise ValueError("Daily shadow shard symbol set is invalid")
        values = self.model_dump(mode="json")
        values.pop("request_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-request/v1", values)
        if self.request_sha256 == "0" * 64:
            object.__setattr__(self, "request_sha256", expected)
        elif self.request_sha256 != expected:
            raise ValueError("Daily shadow request hash mismatch")
        return self


class DailyShadowPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: Literal["tickflow"] = "tickflow"
    profile: Literal["TICKFLOW_FREE_DAILY_BAR_OHLC_V1"] = DAILY_SHADOW_PROFILE
    trade_date: date
    canonical_snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_universe_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_exclusion_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    symbol_mapping_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    period: Literal["1d"] = "1d"
    adjustment_mode: Literal["none"] = "none"
    shards: tuple[DailyShadowShard, ...] = Field(min_length=1, max_length=40)
    request_plan_sha256: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_plan(self) -> DailyShadowPlan:
        if tuple(item.ordinal for item in self.shards) != tuple(range(len(self.shards))):
            raise ValueError("Daily shadow ordinals are not contiguous")
        canonical = tuple(symbol for shard in self.shards for symbol in shard.canonical_symbols)
        provider = tuple(symbol for shard in self.shards for symbol in shard.provider_symbols)
        if canonical != tuple(sorted(canonical)) or len(set(canonical)) != len(canonical):
            raise ValueError("Daily shadow plan symbol set is invalid")
        if self.canonical_universe_sha256 != domain_sha256(
            "stock-eva/r2f3/daily-canonical-universe/v1", canonical
        ):
            raise ValueError("Daily shadow universe hash mismatch")
        if self.symbol_mapping_sha256 != domain_sha256(
            "stock-eva/r2f3/daily-symbol-mapping/v1", tuple(zip(canonical, provider, strict=True))
        ):
            raise ValueError("Daily shadow mapping hash mismatch")
        values = self.model_dump(mode="json")
        values.pop("request_plan_sha256", None)
        expected = domain_sha256("stock-eva/r2f3/free-daily-plan/v1", values)
        if self.request_plan_sha256 == "0" * 64:
            object.__setattr__(self, "request_plan_sha256", expected)
        elif self.request_plan_sha256 != expected:
            raise ValueError("Daily shadow plan hash mismatch")
        return self

    @property
    def request_count(self) -> int:
        return len(self.shards)

    @property
    def symbol_count(self) -> int:
        return sum(len(item.canonical_symbols) for item in self.shards)


class DailyShadowSourceRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    trade_date: date
    timestamp: int = Field(ge=0)
    provider_symbol: str = Field(pattern=r"^\d{6}\.(?:SH|SZ)$")
    symbol: str = Field(pattern=r"^(?:sh|sz)\.\d{6}$")
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int = Field(ge=0)
    amount: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_source_row(self) -> DailyShadowSourceRow:
        if canonical_to_tickflow_daily_symbol(self.symbol) != self.provider_symbol:
            raise ValueError("Daily shadow source symbol mapping mismatch")
        prices = (self.open, self.high, self.low, self.close)
        if any(not value.is_finite() or value <= 0 for value in prices):
            raise ValueError("Daily shadow source price is invalid")
        if self.high < max(prices) or self.low > min(prices) or not self.amount.is_finite():
            raise ValueError("Daily shadow source row is invalid")
        return self


class DailyShadowFetchObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    endpoint: Literal["historical_daily_1d"] = "historical_daily_1d"
    ordinal: int = Field(ge=0, lt=40)
    attempt: Literal[1] = 1
    outcome: Literal["SUCCESS", "FAILURE"]
    elapsed_ms: int = Field(ge=0)
    response_bytes: int = Field(ge=0, le=8 * 1024 * 1024)
    expected_rows: int = Field(ge=1, le=100)
    observed_rows: int = Field(ge=0, le=100)
    failure_class: str | None = Field(default=None, pattern=r"^[a-z0-9_]{1,64}$")

    @model_validator(mode="after")
    def validate_observation(self) -> DailyShadowFetchObservation:
        if self.outcome == "SUCCESS" and (
            self.failure_class is not None or self.observed_rows != self.expected_rows
        ):
            raise ValueError("Daily shadow success observation is invalid")
        if self.outcome == "FAILURE" and self.failure_class is None:
            raise ValueError("Daily shadow failure observation is invalid")
        return self


class DailyShadowFetchResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["ready", "unavailable"]
    provider: Literal["tickflow"] = "tickflow"
    profile: Literal["TICKFLOW_FREE_DAILY_BAR_OHLC_V1"] = DAILY_SHADOW_PROFILE
    trade_date: date
    request_plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_count: int = Field(ge=0, le=40)
    rows: tuple[DailyShadowSourceRow, ...] = Field(default=(), max_length=4000)
    observations: tuple[DailyShadowFetchObservation, ...] = Field(default=(), max_length=40)
    failure_class: str | None = Field(default=None, pattern=r"^[a-z0-9_]{1,64}$")
    failed_ordinal: int | None = Field(default=None, ge=0, lt=40)
    units_state: Literal["UNKNOWN"] = "UNKNOWN"
    suspension_semantics_state: Literal["UNKNOWN"] = "UNKNOWN"
    factor_evidence_state: Literal["UNQUALIFIED"] = "UNQUALIFIED"
    adjustment_request_state: Literal["NONE_REQUESTED"] = "NONE_REQUESTED"

    @model_validator(mode="after")
    def validate_fetch_result(self) -> DailyShadowFetchResult:
        if self.status == "ready":
            if self.failure_class is not None or self.failed_ordinal is not None or not self.rows:
                raise ValueError("Daily shadow ready result is invalid")
            if any(item.outcome != "SUCCESS" for item in self.observations):
                raise ValueError("Daily shadow ready observations are invalid")
        elif self.rows or self.failure_class is None:
            raise ValueError("Daily shadow unavailable result must discard rows")
        return self
