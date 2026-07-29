from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class TechnicalAnalysisPoint(BaseModel):
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    ma5: float | None
    ma10: float | None
    ma20: float | None
    ma60: float | None
    ma120: float | None
    ma250: float | None
    macd: float | None
    macd_signal: float | None
    macd_hist: float | None
    rsi14: float | None


class SecurityAnalysisResponse(BaseModel):
    symbol: str
    status: Literal["empty", "ready"]
    as_of: date | None
    source: Literal["baostock"] = "baostock"
    price_adjustment: Literal["qfq"] = "qfq"
    formula_version: str
    quality_issues: list[str] = Field(default_factory=list)
    series: list[TechnicalAnalysisPoint] = Field(default_factory=list)
