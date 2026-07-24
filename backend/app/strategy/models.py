from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class SignalResult(BaseModel):
    symbol: str
    status: Literal["matched", "not_matched", "excluded", "insufficient_data"]
    matched: bool | None
    signal_date: date
    quality_status: Literal["ready", "excluded", "insufficient"]
    quality_issues: list[str] = Field(default_factory=list)
    explanation: dict[str, object]


class StrategyEvaluation(BaseModel):
    rule_hash: str
    as_of_date: date
    data_fingerprint: str
    results: list[SignalResult]


class StrategyRecord(BaseModel):
    id: str
    name: str
    current_version: int
    created_at: datetime
    updated_at: datetime


class StrategyVersionRecord(BaseModel):
    id: str
    strategy_id: str
    version: int
    rule_ast: dict[str, object]
    rule_hash: str
    created_at: datetime


class StrategyRunRecord(BaseModel):
    id: str
    strategy_id: str
    strategy_version: int
    as_of_date: date
    status: Literal["completed", "partial", "error"]
    rule_hash: str
    data_fingerprint: str
    symbols: list[str]
    results: list[SignalResult]
    created_at: datetime
