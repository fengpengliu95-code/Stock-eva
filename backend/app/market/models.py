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
