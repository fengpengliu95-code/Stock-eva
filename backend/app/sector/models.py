from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

AnalysisStatus = Literal["empty", "ready", "degraded"]
MetricQuality = Literal["ready", "degraded", "missing"]


class SectorModel(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)


class ClassificationLineage(SectorModel):
    generation_id: str
    schema_version: str
    source: str
    source_version: str
    source_snapshot_date: date
    taxonomy_id: str
    coverage_ratio: float = Field(ge=0, le=1)


class MarketLineage(SectorModel):
    source: str
    source_version: str
    earliest_input_date: date
    latest_input_date: date
    record_count: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_date_order(self) -> "MarketLineage":
        if self.earliest_input_date > self.latest_input_date:
            raise ValueError("market lineage start must not exceed end")
        return self


class ActualSectorScope(SectorModel):
    model_config = ConfigDict(allow_inf_nan=False, extra="forbid")

    scope_status: Literal["narrow_main_board"] = "narrow_main_board"
    included_markets: list[str]
    included_boards: list[Literal["main"]] = Field(default_factory=lambda: ["main"])
    excluded_classification_boards: list[str]
    classification_eligible_symbols: int = Field(ge=0)
    observed_market_symbols: int = Field(ge=0)
    priced_classified_symbols: int = Field(ge=0)
    coverage_basis: Literal["promoted_classification_and_observed_canonical_prices"] = (
        "promoted_classification_and_observed_canonical_prices"
    )
    can_support_full_a_share_conclusion: Literal[False] = False
    can_support_all_industry_conclusion: Literal[False] = False
    conclusion_disclaimer: str


class Confidence(SectorModel):
    value: float = Field(ge=0, le=1)
    level: Literal["low", "medium", "high"]
    reasons: list[str] = Field(default_factory=list)


class EvidenceReason(SectorModel):
    code: str
    detail: str
    metric: str
    raw_value: float | None
    score: float | None


class MetricScore(SectorModel):
    metric: str
    raw_value: float | None
    unit: str
    score: float | None = Field(default=None, ge=-100, le=100)
    weight: float = Field(ge=0, le=1)
    weighted_score: float | None
    formula_version: str
    quality_status: MetricQuality
    missing_inputs: list[str] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)


class MissingFundFlowEvidence(SectorModel):
    status: Literal["missing"] = "missing"
    evidence_tier: None = None
    reason: Literal[
        "R2 fund-flow evidence is not integrated; turnover is price-volume evidence only"
    ] = "R2 fund-flow evidence is not integrated; turnover is price-volume evidence only"


class SectorRanking(SectorModel):
    rank: int = Field(ge=1)
    ranking_id: str
    sector_id: str
    sector_name: str
    member_count: int = Field(ge=0)
    priced_member_count: int = Field(ge=0)
    total_score: float | None = Field(default=None, ge=-100, le=100)
    confidence: Confidence
    metric_scores: list[MetricScore]
    supporting_evidence: list[EvidenceReason]
    contrary_evidence: list[EvidenceReason]
    quality_status: MetricQuality
    missing_inputs: list[str]
    quality_issues: list[str]


class LeaderExclusion(SectorModel):
    symbol: str
    reasons: list[str]


class LeaderCandidate(SectorModel):
    rank: int = Field(ge=1)
    candidate_id: str
    symbol: str
    name: str
    total_score: float | None = Field(default=None, ge=-100, le=100)
    confidence: Confidence
    actionable_primary: Literal[False] = False
    actionability_status: Literal["risk_inputs_unavailable"] = "risk_inputs_unavailable"
    limit_lock_status: Literal["unavailable"] = "unavailable"
    metric_scores: list[MetricScore]
    supporting_evidence: list[EvidenceReason]
    contrary_evidence: list[EvidenceReason]
    quality_status: MetricQuality
    missing_inputs: list[str]
    quality_issues: list[str]


class SectorRotationResponse(SectorModel):
    result_id: str
    formula_version: Literal["sector-rotation-v1"] = "sector-rotation-v1"
    status: AnalysisStatus
    quality_status: AnalysisStatus
    as_of: date
    data_as_of: date | None
    taxonomy_id: str
    classification_lineage: ClassificationLineage | None
    market_lineage: MarketLineage | None
    actual_scope: ActualSectorScope
    rankings: list[SectorRanking]
    fund_flow_evidence: MissingFundFlowEvidence
    missing_inputs: list[str]
    quality_issues: list[str]

    @model_validator(mode="after")
    def validate_replay_boundary(self) -> "SectorRotationResponse":
        if self.data_as_of is not None and self.data_as_of > self.as_of:
            raise ValueError("data_as_of must not exceed as_of")
        if (
            self.classification_lineage is not None
            and self.classification_lineage.source_snapshot_date > self.as_of
        ):
            raise ValueError("classification lineage must not exceed as_of")
        if self.market_lineage is not None and self.market_lineage.latest_input_date > self.as_of:
            raise ValueError("market lineage must not exceed as_of")
        return self


class LeaderRankingResponse(SectorModel):
    result_id: str
    formula_version: Literal["leader-ranking-v1"] = "leader-ranking-v1"
    status: AnalysisStatus
    quality_status: AnalysisStatus
    as_of: date
    data_as_of: date | None
    taxonomy_id: str
    sector_id: str
    sector_name: str | None
    classification_lineage: ClassificationLineage | None
    market_lineage: MarketLineage | None
    actual_scope: ActualSectorScope
    candidates: list[LeaderCandidate]
    exclusions: list[LeaderExclusion]
    fund_flow_evidence: MissingFundFlowEvidence
    missing_inputs: list[str]
    quality_issues: list[str]

    @model_validator(mode="after")
    def validate_replay_boundary(self) -> "LeaderRankingResponse":
        if self.data_as_of is not None and self.data_as_of > self.as_of:
            raise ValueError("data_as_of must not exceed as_of")
        if (
            self.classification_lineage is not None
            and self.classification_lineage.source_snapshot_date > self.as_of
        ):
            raise ValueError("classification lineage must not exceed as_of")
        if self.market_lineage is not None and self.market_lineage.latest_input_date > self.as_of:
            raise ValueError("market lineage must not exceed as_of")
        return self
