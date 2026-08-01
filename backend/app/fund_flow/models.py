from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.market.supplemental import ReportedFundFlowPoint

AvailabilityStatus = Literal[
    "not_configured",
    "empty",
    "insufficient_history",
    "ready",
    "stale",
    "error",
]
EvidenceScope = Literal["market", "sector"]


class FundFlowModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class FundFlowEvidenceSnapshot(BaseModel):
    """Internal immutable input read from the supplemental publication pointer."""

    model_config = ConfigDict(extra="forbid")

    source_status: Literal["not_configured", "empty", "ready", "partial", "error"]
    as_of: date
    scope: EvidenceScope
    scope_id: str
    publication_as_of: date | None = None
    published_at: datetime | None = None
    provider_contract: str | None = None
    object_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    points: list[ReportedFundFlowPoint] = Field(default_factory=list)
    future_point_count: int = Field(default=0, ge=0)
    quality_issues: list[str] = Field(default_factory=list)


class FundFlowSource(FundFlowModel):
    source: str
    upstream: str
    endpoint: str


class FundFlowUnits(FundFlowModel):
    reported_main_net_inflow: Literal["CNY"] = "CNY"
    reported_main_net_inflow_ratio: Literal["percent"] = "percent"
    reported_super_large_net_inflow: Literal["CNY"] = "CNY"
    reported_large_net_inflow: Literal["CNY"] = "CNY"
    reported_medium_net_inflow: Literal["CNY"] = "CNY"
    reported_small_net_inflow: Literal["CNY"] = "CNY"


class FundFlowObservedPoint(FundFlowModel):
    trade_date: date
    reported_main_net_inflow: float
    reported_main_net_inflow_ratio: float | None = None
    reported_super_large_net_inflow: float | None = None
    reported_large_net_inflow: float | None = None
    reported_medium_net_inflow: float | None = None
    reported_small_net_inflow: float | None = None
    source: str
    upstream: str
    endpoint: str
    scope: EvidenceScope
    scope_id: str
    date_semantics: Literal["explicit_trade_date"] = "explicit_trade_date"


class FundFlowMetrics(FundFlowModel):
    net_inflow_1d: float | None
    net_inflow_5d: float | None
    net_inflow_20d: float | None
    positive_ratio_20d: float | None = Field(default=None, ge=0, le=1)
    continuity_ratio_20d: float = Field(ge=0, le=1)
    z_score_20d: float | None
    concentration_1d: float | None = Field(default=None, ge=0, le=1)
    price_divergence_20d: float | None = None
    sector_diffusion_20d: float | None = Field(default=None, ge=0, le=1)


class FundFlowConfidence(FundFlowModel):
    value: float = Field(ge=0, le=1)
    level: Literal["low", "medium", "high"]
    reasons: list[str] = Field(default_factory=list)


class FundFlowConclusion(FundFlowModel):
    direction: Literal[
        "persistent_reported_inflow",
        "persistent_reported_outflow",
        "mixed_reported_flow",
    ]
    statement: str


class FundFlowSemanticLineage(FundFlowModel):
    lineage_id: str = Field(pattern=r"^fund-lineage-[0-9a-f]{24}$")
    source_version: str
    object_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source: FundFlowSource
    scope: EvidenceScope
    scope_id: str
    metric: Literal["reported_main_net_inflow"] = "reported_main_net_inflow"
    date_semantics: Literal["explicit_trade_date"] = "explicit_trade_date"
    earliest_input_date: date
    latest_input_date: date
    record_count: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_dates(self) -> "FundFlowSemanticLineage":
        if self.earliest_input_date > self.latest_input_date:
            raise ValueError("earliest_input_date must not exceed latest_input_date")
        return self


class UnavailableEvidenceLevel(FundFlowModel):
    evidence_level: Literal["L2", "L3"]
    status: Literal["unavailable"] = "unavailable"
    reason: str


class FundFlowEvidenceResult(FundFlowModel):
    result_id: str = Field(pattern=r"^fund-flow-[0-9a-f]{24}$")
    evidence_level: Literal["L1"] = "L1"
    evidence_basis: Literal["upstream_reported_main_net_inflow_proxy"] = (
        "upstream_reported_main_net_inflow_proxy"
    )
    interpretation: Literal["upstream_reported_not_ohlcv_inferred"] = (
        "upstream_reported_not_ohlcv_inferred"
    )
    availability: AvailabilityStatus
    scope: EvidenceScope
    scope_id: str
    as_of: date
    data_as_of: date | None
    last_trusted_date: date | None
    staleness_sessions: int | None = Field(default=None, ge=0)
    publication_knowledge: Literal["verified", "unverifiable", "not_applicable"]
    computed_at: datetime | None
    source: FundFlowSource | None
    source_version: str
    formula_version: Literal["fund-flow-evidence-l1-v1"] = "fund-flow-evidence-l1-v1"
    date_semantics: Literal["explicit_trade_date"] = "explicit_trade_date"
    units: FundFlowUnits = Field(default_factory=FundFlowUnits)
    raw_observed_points: list[FundFlowObservedPoint] = Field(default_factory=list)
    metrics: FundFlowMetrics | None
    missing_sessions: list[date] = Field(default_factory=list)
    missing_inputs: list[str] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)
    confidence: FundFlowConfidence
    can_publish_trend: bool
    conclusion: FundFlowConclusion | None
    semantic_lineage: list[FundFlowSemanticLineage] = Field(default_factory=list)
    unavailable_levels: list[UnavailableEvidenceLevel] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_publication_gate(self) -> "FundFlowEvidenceResult":
        if self.can_publish_trend and (self.availability != "ready" or self.conclusion is None):
            raise ValueError("published trend requires ready availability and conclusion")
        if self.can_publish_trend and not self.semantic_lineage:
            raise ValueError("published trend requires semantic lineage")
        if not self.can_publish_trend and self.conclusion is not None:
            raise ValueError("unpublished trend must not contain a conclusion")
        if self.data_as_of is not None and self.data_as_of > self.as_of:
            raise ValueError("data_as_of must not exceed as_of")
        if any(point.trade_date > self.as_of for point in self.raw_observed_points):
            raise ValueError("observed points must not exceed as_of")
        return self
