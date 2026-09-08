from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from backend.app.market.failures import MarketFailureClass, MarketFailureStage

DataStatus = Literal["empty", "ready", "partial", "stale", "error"]


class DailyBar(BaseModel):
    trade_date: date
    symbol: str
    security_type: Literal["stock", "index"]
    exchange: Literal["sh", "sz"]
    board: Literal["main", "chinext", "star", "index"]
    open: float
    high: float
    low: float
    close: float
    preclose: float
    volume: float
    amount: float
    turnover_rate: float | None
    pct_change: float | None
    adjust_factor: float | None
    price_adjustment: Literal["none"] = "none"
    is_trading: bool
    is_suspended: bool
    is_st: bool
    source: Literal["baostock"] = "baostock"
    source_record_id: str
    ingested_at: datetime
    quality_status: Literal["ready", "partial", "error"]
    quality_issues: list[str] = Field(default_factory=list)


class RefreshResult(BaseModel):
    run_id: str
    request_key: str | None = None
    run_kind: Literal["daily", "backfill", "repair"] = "daily"
    requested_date: date
    source: Literal["baostock"]
    status: Literal["ready", "partial", "error"]
    requested_count: int = Field(ge=0)
    succeeded_count: int = Field(ge=0)
    coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    failed_symbols: list[str] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)
    error_message: str | None = None
    failure_stage: MarketFailureStage | None = None
    failure_class: MarketFailureClass | None = None
    retryable: bool | None = None
    started_at: datetime
    completed_at: datetime

    @model_validator(mode="after")
    def validate_ready_is_complete(self) -> "RefreshResult":
        if self.status != "ready":
            return self
        if (
            self.requested_count <= 0
            or self.succeeded_count != self.requested_count
            or self.coverage_ratio != 1
            or self.failed_symbols
            or self.quality_issues
            or self.error_message is not None
            or self.failure_stage is not None
            or self.failure_class is not None
            or self.retryable is not None
        ):
            raise ValueError("ready refresh must be complete")
        return self


class Completeness(BaseModel):
    requested: int = 0
    loaded: int = 0
    failed: int = 0
    failed_symbols: list[str] = Field(default_factory=list)
    coverage_ratio: float | None = None
    required_ratio: float = 1.0
    is_complete: bool = False


class Freshness(BaseModel):
    evaluated: bool = False
    expected_as_of: date | None = None
    is_fresh: bool | None = None
    lag_calendar_days: int | None = None


class Breadth(BaseModel):
    advancing: int = 0
    declining: int = 0
    unchanged: int = 0
    suspended: int = 0
    eligible: int = 0


class TurnoverOverview(BaseModel):
    amount: float | None = None
    unit: Literal["CNY"] = "CNY"
    coverage: int = 0


class IndexSnapshot(BaseModel):
    symbol: str
    close: float
    pct_change: float | None
    is_suspended: bool


class MarketSummary(BaseModel):
    status: DataStatus
    as_of: date | None
    source: Literal["baostock"] | None
    completeness: Completeness
    freshness: Freshness
    breadth: Breadth
    turnover: TurnoverOverview
    indexes: list[IndexSnapshot]
    quality_issues: list[str] = Field(default_factory=list)


class MarketCapability(BaseModel):
    mode: Literal["end_of_day"] = "end_of_day"
    realtime: Literal[False] = False
    automatic_when_running: Literal[True] = True
    sleep_catch_up: Literal[True] = True


class CalendarSourceMetadata(BaseModel):
    exchange: Literal["SSE", "SZSE"]
    title: str
    url: str
    notice_no: str | None = None


class MarketDataStatus(BaseModel):
    market_phase: Literal[
        "pre_market",
        "market_open",
        "after_close_waiting",
        "refreshing",
        "complete",
        "delayed",
        "closed",
        "calendar_unavailable",
    ]
    calendar_status: Literal["confirmed", "conflict", "unavailable"]
    latest_expected_session: date | None
    published_as_of: date | None
    refresh_state: Literal[
        "disabled",
        "idle",
        "scheduled",
        "running",
        "retry_wait",
        "success",
        "delayed",
        "error",
    ]
    last_success_at: datetime | None
    next_retry_at: datetime | None
    calendar_last_sync_at: datetime | None = None
    calendar_last_success_at: datetime | None = None
    calendar_conflict_detected: bool = False
    calendar_conflict_at: datetime | None = None
    calendar_next_sync_at: datetime | None = None
    capability: MarketCapability = Field(default_factory=MarketCapability)
    calendar_sources: list[CalendarSourceMetadata] = Field(default_factory=list)
    publication_gates: list[str] = Field(
        default_factory=lambda: [
            "full_expected_universe",
            "required_indexes",
            "user_symbols_present",
            "valid_canonical_quality",
            "adjust_factors_for_eligible_stocks",
        ]
    )
    # R2-F1 continuity is deliberately additive.  These fields are defaulted so older
    # consumers and fixtures can continue constructing the legacy status payload.
    continuity_status: Literal["current", "gaps", "blocked", "unavailable"] = "unavailable"
    continuity_start_date: date | None = None
    missing_session_count: int = Field(default=0, ge=0)
    oldest_missing_session: date | None = None
    repair_execution_enabled: bool = False
    repair_pending_count: int = Field(default=0, ge=0)
    repair_retry_wait_count: int = Field(default=0, ge=0)
    repair_active_count: int = Field(default=0, ge=0)
    repair_dead_letter_count: int = Field(default=0, ge=0)
    active_lane: Literal["freshness", "repair"] | None = None
    continuity_reason_code: (
        Literal[
            "CONTINUITY_START_UNCONFIGURED",
            "CONTINUITY_RANGE_INVALID",
            "CALENDAR_UNAVAILABLE",
            "CALENDAR_CONFLICT",
            "MANIFEST_INVENTORY_UNAVAILABLE",
            "IMMUTABLE_OBJECT_INVALID",
            "CONTROL_STATE_UNAVAILABLE",
            "REPAIR_EXECUTION_DISABLED",
            "PROVIDER_CIRCUIT_OPEN",
            "PROVIDER_CIRCUIT_HALF_OPEN",
            "PROVIDER_HEALTH_UNAVAILABLE",
            "REPAIR_QUEUE_NOT_ENQUEUED",
            "REPAIR_DEAD_LETTER_ONLY",
        ]
        | None
    ) = None


class UniverseStatusCounts(BaseModel):
    total: int = Field(ge=0)
    trading: int = Field(ge=0)
    suspended: int = Field(ge=0)
    not_yet_listed: int = Field(ge=0)
    delisted: int = Field(ge=0)
    unknown: int = Field(ge=0)
    session_expected: int = Field(ge=0)
    loaded: int | None = Field(default=None, ge=0)
    critical_attribute_unknown_count: int = Field(ge=0)


class UniverseStatusLayers(BaseModel):
    classification_evidence_count: int = Field(ge=0)
    classification_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    effective_main_board_count: int = Field(ge=0)
    effective_main_board_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    required_additions_count: int = Field(ge=0)
    required_additions_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class UniverseStatusSnapshotV1(BaseModel):
    """Sanitized public projection of one verified universe sidecar snapshot."""

    trade_date: date
    universe_id: Literal["all-main-board-plus-required-symbols"]
    schema_version: Literal[1] = 1
    scope: Literal["all-main-board-plus-required-symbols"]
    provider_requests: Literal[0] = 0
    writes: Literal[False] = False
    status: Literal["ready", "stale", "blocked", "unavailable"]
    verified_head_trade_date: date | None
    contract_id: str | None
    contract_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    source_state_id: str | None
    source_state_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    source_version_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    classification_generation_id: str | None
    calendar_generation_id: str | None
    calendar_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    counts: UniverseStatusCounts | None
    layers: UniverseStatusLayers | None
    required_indexes: tuple[Literal["sh.000001", "sz.399001"], ...] | None
    required_user_symbol_count: int | None = Field(default=None, ge=0)
    publication_eligible: bool
    reason_code: Literal[
        "CONTROL_STATE_UNAVAILABLE",
        "PIT_VISIBILITY_INVALID",
        "CALENDAR_UNAVAILABLE",
        "CALENDAR_CONFLICT",
        "CLASSIFICATION_UNAVAILABLE",
        "USER_STORE_UNAVAILABLE",
        "BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED",
        "PIT_CUTOFF_VIOLATION",
        "USER_SNAPSHOT_CHANGED",
        "REQUIRED_INDEX_NOT_TRADING",
        "REQUIRED_SYMBOL_INVALID",
        "UNIVERSE_UNKNOWN_NONZERO",
        "UNIVERSE_COUNT_MISMATCH",
        "UNIVERSE_MISSING_SYMBOL",
        "UNIVERSE_EXTRA_SYMBOL",
        "UNIVERSE_DUPLICATE_SYMBOL",
        "UNIVERSE_SESSION_DRIFT",
        "UNIVERSE_STATE_MISMATCH",
        "UNIVERSE_SOURCE_VERSION_CHANGED",
        "DATE_MISMATCH",
        "UNIVERSE_STORAGE_UNAVAILABLE",
        "UNIVERSE_SCHEMA_MISMATCH",
        "UNIVERSE_HEAD_CAS_CONFLICT",
        "BLOCKED_ENFORCE_NOT_ENABLED",
        "BLOCKED_PRODUCTION_MODE_OFF",
        "LEGACY_SHADOW_DRIFT",
        "ATTEMPT_INDETERMINATE",
        "UNIVERSE_IDENTITY_CONFLICT",
        "NONE",
    ]

    @model_validator(mode="after")
    def validate_projection_shape(self) -> "UniverseStatusSnapshotV1":
        nullable = (
            self.verified_head_trade_date,
            self.contract_id,
            self.contract_sha256,
            self.source_state_id,
            self.source_state_sha256,
            self.source_version_digest,
            self.classification_generation_id,
            self.calendar_generation_id,
            self.calendar_sha256,
            self.counts,
            self.layers,
            self.required_indexes,
            self.required_user_symbol_count,
        )
        if self.status in {"blocked", "unavailable"}:
            if any(value is not None for value in nullable) or self.publication_eligible:
                raise ValueError("blocked/unavailable Universe projection must be empty")
        elif any(value is None for value in nullable):
            raise ValueError("ready/stale Universe projection is incomplete")
        if self.status == "ready" and not self.publication_eligible:
            raise ValueError("ready Universe projection must be publication eligible")
        if self.status == "ready" and self.reason_code != "NONE":
            raise ValueError("ready Universe projection reason must be NONE")
        if self.status == "stale" and self.publication_eligible:
            raise ValueError("stale Universe projection cannot be publication eligible")
        if self.required_indexes is not None and self.required_indexes != (
            "sh.000001",
            "sz.399001",
        ):
            raise ValueError("required Universe index set is incomplete")
        return self


class PriceSeriesPoint(BaseModel):
    trade_date: date
    symbol: str
    open: float
    high: float
    low: float
    close: float
    preclose: float
    volume: float
    amount: float
    adjust_factor: float | None
    price_adjustment: Literal["none", "qfq"]
    source: Literal["baostock"]
