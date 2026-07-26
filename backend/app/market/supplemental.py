from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class IndustryClassificationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_symbol: str
    industry_code: str
    industry_name: str | None = None
    effective_from: date
    source_updated_on: date
    source: Literal["akshare"] = "akshare"
    upstream: Literal["swsresearch"] = "swsresearch"
    source_endpoint: str
    observed_at: datetime
    quality_status: Literal["ready"] = "ready"


class ReportedFundFlowPoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade_date: date
    scope: Literal["market", "industry"]
    scope_name: str
    reported_main_net_inflow: float
    reported_main_net_inflow_ratio: float | None = None
    reported_super_large_net_inflow: float | None = None
    reported_super_large_net_inflow_ratio: float | None = None
    reported_large_net_inflow: float | None = None
    reported_large_net_inflow_ratio: float | None = None
    reported_medium_net_inflow: float | None = None
    reported_medium_net_inflow_ratio: float | None = None
    reported_small_net_inflow: float | None = None
    reported_small_net_inflow_ratio: float | None = None
    source: Literal["akshare"] = "akshare"
    upstream: Literal["eastmoney"] = "eastmoney"
    source_endpoint: str
    observed_at: datetime
    quality_status: Literal["ready"] = "ready"


class SupplementalCapability(BaseModel):
    name: str
    status: Literal["contract_ready", "rejected"]
    upstream: str
    date_semantics: str
    reason: str


class SupplementalMarketResponse(BaseModel):
    status: Literal["not_configured", "empty", "ready", "partial", "error"]
    provider: Literal["akshare"]
    as_of: date | None
    market_flow: ReportedFundFlowPoint | None
    sector_flows: list[ReportedFundFlowPoint]
    industry_classifications: list[IndustryClassificationRecord] = Field(default_factory=list)
    capabilities: list[SupplementalCapability]
    quality_issues: list[str]
    interpretation: Literal["upstream_reported_not_ohlcv_inferred"] = (
        "upstream_reported_not_ohlcv_inferred"
    )


def supplemental_capabilities() -> list[SupplementalCapability]:
    return [
        SupplementalCapability(
            name="industry_classification_history",
            status="contract_ready",
            upstream="swsresearch",
            date_semantics="explicit_effective_from_and_source_update_date",
            reason="raw history only; effective end dates are not inferred",
        ),
        SupplementalCapability(
            name="market_fund_flow_history",
            status="contract_ready",
            upstream="eastmoney",
            date_semantics="explicit_trade_date",
            reason="upstream-reported order-size labels; index prices are discarded",
        ),
        SupplementalCapability(
            name="sector_fund_flow_history",
            status="contract_ready",
            upstream="eastmoney",
            date_semantics="explicit_trade_date",
            reason="per-industry history with explicit dates",
        ),
        SupplementalCapability(
            name="sector_fund_flow_rank",
            status="rejected",
            upstream="eastmoney",
            date_semantics="no_reliable_date_in_akshare_output",
            reason="today/5-day/10-day rankings cannot be historical facts",
        ),
        SupplementalCapability(
            name="eastmoney_current_sector_membership",
            status="rejected",
            upstream="eastmoney",
            date_semantics="observation_only",
            reason="current board and constituent outputs have no effective date",
        ),
    ]


def empty_supplemental_response(*, enabled: bool) -> SupplementalMarketResponse:
    return SupplementalMarketResponse(
        status="empty" if enabled else "not_configured",
        provider="akshare",
        as_of=None,
        market_flow=None,
        sector_flows=[],
        industry_classifications=[],
        capabilities=supplemental_capabilities(),
        quality_issues=[
            (
                "supplemental_ingestion_not_implemented"
                if enabled
                else "akshare_supplemental_disabled"
            )
        ],
    )
