from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

TAXONOMY_BAOSTOCK_INDUSTRY = "baostock.industry_classification"
CLASSIFICATION_SCHEMA_VERSION = "classification-v3"
BOARD_DERIVATION_VERSION = "cn-symbol-prefix-v1"

ClassificationStatus = Literal["empty", "ready", "degraded", "not_available"]
SourceDateSemantics = Literal["source_observed", "requested_unverified"]
ComponentHistoryCapability = Literal["verified", "unverified", "not_supplied"]


class IndexMetadata(BaseModel):
    index_id: str
    symbol: str
    name: str
    component_source: str | None
    component_history_capability: ComponentHistoryCapability


INDEX_CATALOG = {
    "sse_composite": IndexMetadata(
        index_id="sse_composite",
        symbol="sh.000001",
        name="上证综指",
        component_source=None,
        component_history_capability="not_supplied",
    ),
    "szse_component": IndexMetadata(
        index_id="szse_component",
        symbol="sz.399001",
        name="深证成指",
        component_source=None,
        component_history_capability="not_supplied",
    ),
    "hs300": IndexMetadata(
        index_id="hs300",
        symbol="sh.000300",
        name="沪深300",
        component_source="baostock.query_hs300_stocks",
        component_history_capability="unverified",
    ),
    "sz50": IndexMetadata(
        index_id="sz50",
        symbol="sh.000016",
        name="上证50",
        component_source="baostock.query_sz50_stocks",
        component_history_capability="unverified",
    ),
    "csi500": IndexMetadata(
        index_id="csi500",
        symbol="sh.000905",
        name="中证500",
        component_source="baostock.query_zz500_stocks",
        component_history_capability="unverified",
    ),
    "csi1000": IndexMetadata(
        index_id="csi1000",
        symbol="sh.000852",
        name="中证1000",
        component_source=None,
        component_history_capability="not_supplied",
    ),
    "chinext_index": IndexMetadata(
        index_id="chinext_index",
        symbol="sz.399006",
        name="创业板指",
        component_source=None,
        component_history_capability="not_supplied",
    ),
}


class SecurityMasterRecord(BaseModel):
    record_id: str
    security_id: str
    symbol: str
    name: str
    exchange: str
    board: str
    security_type: str
    list_date: date | None
    delist_date: date | None
    is_tradable: bool
    listing_status: str
    daily_trade_status: str
    source: str
    source_version: str
    source_snapshot_date: date
    source_date_semantics: SourceDateSemantics = "source_observed"
    effective_from: date | None
    effective_to: date | None
    observed_at: datetime
    source_record_id: str
    lineage_hash: str
    price_available: bool | None = None


class IndexComponentRecord(BaseModel):
    record_id: str
    index_id: str
    index_name: str
    security_id: str
    symbol: str
    source: str
    source_version: str
    source_snapshot_date: date
    source_date_semantics: SourceDateSemantics = "source_observed"
    effective_from: date | None
    effective_to: date | None
    observed_at: datetime
    source_record_id: str
    lineage_hash: str


class SectorMembershipRecord(BaseModel):
    record_id: str
    taxonomy_id: str
    taxonomy_name: str
    sector_id: str
    sector_name: str
    security_id: str
    symbol: str
    raw_industry: str
    raw_classification: str
    source: str
    source_version: str
    source_snapshot_date: date
    source_date_semantics: SourceDateSemantics = "source_observed"
    effective_from: date | None
    effective_to: date | None
    observed_at: datetime
    source_record_id: str
    lineage_hash: str


class ClassificationSnapshot(BaseModel):
    source: str
    source_version: str
    source_snapshot_date: date
    source_date_semantics: SourceDateSemantics = "source_observed"
    observed_at: datetime
    securities: list[SecurityMasterRecord] = Field(default_factory=list)
    index_components: list[IndexComponentRecord] = Field(default_factory=list)
    sector_memberships: list[SectorMembershipRecord] = Field(default_factory=list)
    declared_taxonomies: list[str] = Field(default_factory=lambda: [TAXONOMY_BAOSTOCK_INDUSTRY])


class Eligibility(BaseModel):
    eligible: bool
    exclusion_reason: str | None = None


class Actionability(BaseModel):
    actionable: bool
    reasons: list[str] = Field(default_factory=list)


class SecurityAtAsOf(SecurityMasterRecord):
    eligibility: Eligibility
    actionability: Actionability


class MarketScope(BaseModel):
    covered_markets: list[str]
    covered_boards: list[str]
    eligible_markets: list[str]
    eligible_boards: list[str]
    excluded_markets: list[str]
    excluded_security_types: list[str]
    price_coverage_status: Literal["verified", "partial", "not_audited"]
    derivation_version: str = BOARD_DERIVATION_VERSION
    derivation_source: str = (
        "BaoStock exchange-qualified symbol and versioned SSE/SZSE code-prefix rules"
    )


class GenerationSummary(BaseModel):
    generation_id: str
    sequence: int
    schema_version: str = CLASSIFICATION_SCHEMA_VERSION
    source: str
    source_version: str
    source_snapshot_date: date
    source_date_semantics: SourceDateSemantics = "source_observed"
    observed_at: datetime
    row_counts: dict[str, int]
    coverage_audits: list["CoverageAudit"]
    market_scope: MarketScope


class ClassificationPublishOutcome(BaseModel):
    generation: GenerationSummary
    inserted: bool
    promoted: bool


class SecurityResponse(BaseModel):
    status: ClassificationStatus
    as_of: date
    generation_id: str | None
    source_snapshot_date: date | None
    securities: list[SecurityAtAsOf]
    market_scope: MarketScope
    quality_issues: list[str] = Field(default_factory=list)


class IndexComponentsResponse(BaseModel):
    status: ClassificationStatus
    as_of: date
    generation_id: str | None
    index: IndexMetadata
    source_snapshot_date: date | None
    components: list[IndexComponentRecord]
    quality_issues: list[str] = Field(default_factory=list)


class SectorSummary(BaseModel):
    sector_id: str
    sector_name: str
    member_count: int


class SectorResponse(BaseModel):
    status: ClassificationStatus
    as_of: date
    generation_id: str | None
    taxonomy_id: str
    source_snapshot_date: date | None
    sectors: list[SectorSummary]
    quality_issues: list[str] = Field(default_factory=list)


class SectorMembersResponse(BaseModel):
    status: ClassificationStatus
    as_of: date
    generation_id: str | None
    taxonomy_id: str
    sector_id: str
    source_snapshot_date: date | None
    members: list[SectorMembershipRecord]
    database_query_count: int
    quality_issues: list[str] = Field(default_factory=list)


class CoverageAudit(BaseModel):
    status: ClassificationStatus
    as_of: date
    generation_id: str | None
    taxonomy_id: str
    eligible_count: int
    mapped_count: int
    coverage_ratio: float | None
    target_ratio: float = 0.95
    unmapped_symbols: list[str]
    exclusion_reasons: dict[str, int]
    actionability_reasons: dict[str, int] = Field(default_factory=dict)
    source_lineage: list[str]
    quality_issues: list[str] = Field(default_factory=list)


class ClassificationSyncResult(BaseModel):
    status: Literal["dry-run", "ready", "degraded"]
    as_of: date
    network_requests: int
    writes_classification_data: bool
    new_generation: bool
    execute_requires: str | None
    generation: GenerationSummary | None
    quality_issues: list[str] = Field(default_factory=list)
