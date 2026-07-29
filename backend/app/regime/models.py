from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

ComponentName = Literal["trend", "breadth", "liquidity", "risk", "leadership"]
ComponentQuality = Literal["ready", "degraded", "missing"]

EXPECTED_BOARDS = ["sse_main", "szse_main", "chinext", "star"]
EXPECTED_INDEX_SERIES = [
    "sh.000001",
    "sz.399001",
    "sh.000300",
    "sh.000905",
    "sh.000852",
    "sz.399006",
]


class RegimeModel(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)


class SourceLineage(RegimeModel):
    source: str
    source_version: str
    earliest_input_date: date
    latest_input_date: date
    record_count: int = Field(ge=0)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_date_order(self) -> "SourceLineage":
        if self.earliest_input_date > self.latest_input_date:
            raise ValueError("lineage earliest_input_date must not exceed latest_input_date")
        return self


class EvidenceItem(RegimeModel):
    component: ComponentName
    code: str
    detail: str
    value: float | None
    as_of: date
    source: str


class RegimeComponentInput(RegimeModel):
    name: ComponentName
    score: float | None = Field(default=None, ge=-100, le=100)
    formula_version: str
    quality_status: ComponentQuality
    missing_inputs: list[str] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)
    supporting_evidence: list[EvidenceItem] = Field(default_factory=list)
    contrary_evidence: list[EvidenceItem] = Field(default_factory=list)
    source_lineage: list[SourceLineage] = Field(default_factory=list)


class ActualMarketScope(RegimeModel):
    model_config = ConfigDict(allow_inf_nan=False, extra="forbid")

    expected_boards: list[str]
    observed_boards: list[str]
    missing_boards: list[str]
    expected_index_series: list[str]
    observed_index_series: list[str]
    missing_index_series: list[str]
    coverage_basis: Literal["symbol_presence_only"] = "symbol_presence_only"
    coverage_evidence_status: Literal["unavailable"] = "unavailable"
    observed_universe_count: int = Field(ge=0)
    index_coverage_ratio: float = Field(ge=0, le=1)
    scope_status: Literal["narrow_provisional"] = "narrow_provisional"
    can_support_full_a_share_conclusion: Literal[False] = False
    conclusion_disclaimer: str


class MarketRegimeInput(RegimeModel):
    as_of: date
    data_as_of: date | None
    trend: RegimeComponentInput
    breadth: RegimeComponentInput
    liquidity: RegimeComponentInput
    risk: RegimeComponentInput
    leadership: RegimeComponentInput
    actual_market_scope: ActualMarketScope
    source_lineage: list[SourceLineage] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_as_of_boundaries(self) -> "MarketRegimeInput":
        if self.data_as_of is not None and self.data_as_of > self.as_of:
            raise ValueError("data_as_of must not exceed as_of")
        for expected_name in (
            "trend",
            "breadth",
            "liquidity",
            "risk",
            "leadership",
        ):
            component = getattr(self, expected_name)
            if component.name != expected_name:
                raise ValueError(
                    f"{expected_name} field must contain the {expected_name} component"
                )
            for lineage in component.source_lineage:
                if lineage.latest_input_date > self.as_of:
                    raise ValueError("component lineage must not exceed as_of")
            for evidence in (
                *component.supporting_evidence,
                *component.contrary_evidence,
            ):
                if evidence.as_of > self.as_of:
                    raise ValueError("component evidence must not exceed as_of")
        if any(item.latest_input_date > self.as_of for item in self.source_lineage):
            raise ValueError("source lineage must not exceed as_of")
        return self


class ComponentScore(RegimeModel):
    name: ComponentName
    score: float | None
    weight: float
    weighted_score: float | None
    formula_version: str
    quality_status: ComponentQuality
    missing_inputs: list[str]
    quality_issues: list[str]
    supporting_evidence: list[EvidenceItem]
    contrary_evidence: list[EvidenceItem]
    source_lineage: list[SourceLineage]


class RegimeConfidence(RegimeModel):
    value: float = Field(ge=0, le=1)
    level: Literal["low", "medium", "high"]
    reasons: list[str] = Field(default_factory=list)


class MarketRegimeResult(RegimeModel):
    result_id: str
    formula_version: str
    as_of: date
    data_as_of: date | None
    status: Literal["empty", "ready", "degraded"]
    quality_status: Literal["empty", "ready", "degraded"]
    strategic_state: Literal["bull", "range", "bear"]
    tactical_state: Literal["risk_on", "neutral", "risk_off"]
    total_score: float | None
    component_scores: list[ComponentScore]
    weights: dict[str, float]
    thresholds: dict[str, float]
    confidence: RegimeConfidence
    missing_inputs: list[str]
    supporting_evidence: list[EvidenceItem]
    contrary_evidence: list[EvidenceItem]
    quality_issues: list[str]
    actual_market_scope: ActualMarketScope
    source_lineage: list[SourceLineage]
