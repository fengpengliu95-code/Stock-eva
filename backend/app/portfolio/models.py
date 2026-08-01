from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.app.user.models import _normalize_symbol


def _finite_decimal(value: Decimal | None, *, field: str) -> Decimal | None:
    if value is not None and not value.is_finite():
        raise ValueError(f"{field} must be finite")
    return value


class PortfolioModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class PortfolioPositionInput(PortfolioModel):
    symbol: str
    quantity: Decimal = Field(gt=0)
    avg_cost: Decimal | None = Field(default=None, ge=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("quantity")
    @classmethod
    def finite_quantity(cls, value: Decimal) -> Decimal:
        return _finite_decimal(value, field="quantity")

    @field_validator("avg_cost")
    @classmethod
    def finite_avg_cost(cls, value: Decimal | None) -> Decimal | None:
        return _finite_decimal(value, field="avg_cost")


class PortfolioSnapshotContent(PortfolioModel):
    as_of: date
    cash: Decimal | None = Field(default=None, ge=0)
    positions: list[PortfolioPositionInput] = Field(default_factory=list)
    manual_nav: Decimal | None = Field(default=None, ge=0)

    @field_validator("cash")
    @classmethod
    def finite_cash(cls, value: Decimal | None) -> Decimal | None:
        return _finite_decimal(value, field="cash")

    @field_validator("manual_nav")
    @classmethod
    def finite_manual_nav(cls, value: Decimal | None) -> Decimal | None:
        return _finite_decimal(value, field="manual_nav")

    @model_validator(mode="after")
    def positions_are_unique(self) -> "PortfolioSnapshotContent":
        symbols = [position.symbol for position in self.positions]
        if len(symbols) != len(set(symbols)):
            raise ValueError("duplicate position symbol")
        self.positions.sort(key=lambda item: item.symbol)
        return self


class PortfolioDailySnapshotCreate(PortfolioSnapshotContent):
    pass


class PortfolioDailySnapshotUpdate(PortfolioSnapshotContent):
    expected_revision: int = Field(ge=1)


class PortfolioDailySnapshot(PortfolioSnapshotContent):
    snapshot_id: str = Field(pattern=r"^portfolio-snapshot-[0-9a-f]{24}$")
    revision: int = Field(ge=1)
    previous_revision: int | None = Field(default=None, ge=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    recorded_at: datetime
    source: Literal["manual"] = "manual"


class PortfolioSnapshotWriteResult(PortfolioModel):
    write_status: Literal["created", "revised", "idempotent"]
    snapshot: PortfolioDailySnapshot


class SnapshotLineage(PortfolioModel):
    snapshot_id: str
    as_of: date
    revision: int
    content_hash: str
    recorded_at: datetime
    source: Literal["manual"]


class RiskPolicyMetadata(PortfolioModel):
    formula_version: str
    guardrail_version: str
    thresholds: dict[str, Decimal]
    target_band_version: str
    target_bands: dict[str, tuple[Decimal, Decimal]]


class PortfolioNav(PortfolioModel):
    cash: Decimal | None
    manual_nav: Decimal | None
    derived_nav: Decimal | None
    selected_nav: Decimal | None
    valuation_basis: Literal["manual_nav", "derived_nav", "unavailable"]


class PortfolioExposure(PortfolioModel):
    gross_market_value: Decimal | None
    net_market_value: Decimal | None
    gross_ratio: Decimal | None
    net_ratio: Decimal | None
    max_single_weight: Decimal | None


class PositionRisk(PortfolioModel):
    symbol: str
    quantity: Decimal
    avg_cost: Decimal | None
    price_as_of: date | None
    close: Decimal | None
    market_value: Decimal | None
    weight: Decimal | None
    sector_id: str
    sector_name: str
    atr14: Decimal | None
    atr_risk_amount: Decimal | None
    atr_risk_ratio: Decimal | None
    quality_status: Literal["ready", "degraded", "missing"]
    missing_inputs: list[str] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)


class SectorExposure(PortfolioModel):
    sector_id: str
    sector_name: str
    symbol_count: int = Field(ge=0)
    market_value: Decimal
    weight: Decimal | None
    is_unknown: bool


class SectorConcentration(PortfolioModel):
    status: Literal["ready", "degraded", "missing"]
    mapping_ratio: Decimal | None
    can_publish_complete: bool
    max_sector_weight: Decimal | None
    sectors: list[SectorExposure]


class AtrBudget(PortfolioModel):
    status: Literal["ready", "degraded", "missing"]
    total_atr_risk: Decimal | None
    total_atr_risk_ratio: Decimal | None


class DrawdownMetrics(PortfolioModel):
    status: Literal["ready", "degraded", "missing"]
    observation_count: int = Field(ge=0)
    current_drawdown: Decimal | None
    max_drawdown: Decimal | None


class RiskGuardrail(PortfolioModel):
    code: Literal[
        "max_single_weight",
        "max_sector_weight",
        "max_total_atr_risk_ratio",
        "max_drawdown",
    ]
    status: Literal["pass", "breach", "unavailable"]
    current_value: Decimal | None
    limit_value: Decimal
    unit: Literal["ratio"] = "ratio"
    reason: str


class TargetExposureBand(PortfolioModel):
    status: Literal["ready", "degraded", "unavailable"]
    base_lower: Decimal | None
    base_upper: Decimal | None
    lower: Decimal | None
    upper: Decimal | None
    current: Decimal | None
    within_band: bool | None
    advisory_only: Literal[True] = True
    personalization_allowed: Literal[False] = False
    reasons: list[str] = Field(default_factory=list)
    research_note: Literal[
        "Research-only exposure range; not an order or personalized investment advice."
    ] = "Research-only exposure range; not an order or personalized investment advice."


class MarketRegimeContext(PortfolioModel):
    result_id: str
    formula_version: str
    as_of: date
    data_as_of: date | None
    status: Literal["empty", "ready", "degraded"]
    strategic_state: Literal["bull", "range", "bear"]
    tactical_state: Literal["risk_on", "neutral", "risk_off"]
    confidence_value: Decimal
    confidence_level: Literal["low", "medium", "high"]
    scope_status: str
    can_support_full_a_share_conclusion: bool


class PortfolioRiskResult(PortfolioModel):
    result_id: str = Field(pattern=r"^portfolio-risk-[0-9a-f]{24}$")
    formula_version: str
    status: Literal["empty", "ready", "degraded"]
    quality_status: Literal["empty", "ready", "degraded"]
    as_of: date
    data_as_of: date | None
    snapshot: SnapshotLineage | None
    policy: RiskPolicyMetadata
    nav: PortfolioNav
    exposure: PortfolioExposure
    positions: list[PositionRisk]
    sector_concentration: SectorConcentration
    atr_budget: AtrBudget
    drawdown: DrawdownMetrics
    guardrails: list[RiskGuardrail]
    target_exposure: TargetExposureBand
    market_regime: MarketRegimeContext | None
    missing_inputs: list[str] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)
    decision_boundary: Literal["local_after_close_research_only_no_broker_no_orders"] = (
        "local_after_close_research_only_no_broker_no_orders"
    )

    @model_validator(mode="after")
    def validate_no_future_lineage(self) -> "PortfolioRiskResult":
        if self.data_as_of is not None and self.data_as_of > self.as_of:
            raise ValueError("data_as_of must not exceed as_of")
        if self.snapshot is not None and self.snapshot.as_of > self.as_of:
            raise ValueError("snapshot as_of must not exceed risk as_of")
        if self.market_regime is not None:
            if self.market_regime.as_of > self.as_of:
                raise ValueError("market regime as_of must not exceed risk as_of")
            if (
                self.market_regime.data_as_of is not None
                and self.market_regime.data_as_of > self.as_of
            ):
                raise ValueError("market regime data_as_of must not exceed risk as_of")
        return self
