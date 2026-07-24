from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator


def _normalize_symbol(value: str) -> str:
    normalized = value.strip().lower()
    parts = normalized.split(".")
    if len(parts) != 2 or parts[0] not in {"sh", "sz"} or not (
        len(parts[1]) == 6 and parts[1].isdigit()
    ):
        raise ValueError("symbol must look like sh.600000 or sz.000001")
    return normalized


class PositionCreate(BaseModel):
    symbol: str
    quantity: Decimal = Field(gt=0)
    avg_cost: Decimal = Field(ge=0)
    as_of_date: date
    today_buy_qty: Decimal = Field(default=Decimal("0"), ge=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return _normalize_symbol(value)

    @field_validator("today_buy_qty")
    @classmethod
    def today_buy_not_above_quantity(
        cls,
        value: Decimal,
        info,
    ) -> Decimal:
        quantity = info.data.get("quantity")
        if quantity is not None and value > quantity:
            raise ValueError("today_buy_qty cannot exceed quantity")
        return value


class PositionUpdate(BaseModel):
    quantity: Decimal = Field(gt=0)
    avg_cost: Decimal = Field(ge=0)
    as_of_date: date
    today_buy_qty: Decimal = Field(default=Decimal("0"), ge=0)
    expected_version: int = Field(ge=1)

    @field_validator("today_buy_qty")
    @classmethod
    def today_buy_not_above_quantity(
        cls,
        value: Decimal,
        info,
    ) -> Decimal:
        quantity = info.data.get("quantity")
        if quantity is not None and value > quantity:
            raise ValueError("today_buy_qty cannot exceed quantity")
        return value


class Position(BaseModel):
    id: str
    symbol: str
    quantity: Decimal
    avg_cost: Decimal
    as_of_date: date
    today_buy_qty: Decimal
    version: int
    created_at: datetime
    updated_at: datetime


class WatchlistCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)

    @field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        return value.strip()


class Watchlist(BaseModel):
    id: str
    name: str
    version: int
    created_at: datetime
    updated_at: datetime


class WatchlistItemCreate(BaseModel):
    symbol: str

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return _normalize_symbol(value)


class WatchlistItem(BaseModel):
    id: str
    watchlist_id: str
    symbol: str
    created_at: datetime


class ValuationCoverage(BaseModel):
    covered: int
    total: int
    missing_symbols: list[str]
    suspended_symbols: list[str]


class PositionValuation(BaseModel):
    position_id: str
    symbol: str
    quantity: Decimal
    avg_cost: Decimal
    today_buy_qty: Decimal
    status: Literal["valued", "missing", "suspended"]
    close: Decimal | None = None
    market_value: Decimal | None = None
    cost_basis: Decimal | None = None
    unrealized_pnl: Decimal | None = None


class PortfolioValuation(BaseModel):
    status: Literal["empty", "ready", "partial", "stale", "error"]
    data_date: date | None
    source: Literal["baostock"] | None
    market_status: str | None
    coverage: ValuationCoverage
    items: list[PositionValuation]
    covered_market_value: Decimal | None
    covered_cost_basis: Decimal | None
    covered_unrealized_pnl: Decimal | None
    valuation_basis: str
    quality_issues: list[str]
