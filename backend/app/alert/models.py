from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field

AlertState = Literal[
    "pending",
    "eligible",
    "triggered",
    "acknowledged",
    "resolved",
    "suppressed",
    "error",
]


class AlertRuleRecord(BaseModel):
    id: str
    name: str
    watchlist_id: str
    strategy_id: str
    strategy_version: int
    strategy_version_id: str
    created_at: datetime


class AlertTransitionRecord(BaseModel):
    id: int
    alert_event_id: str
    from_state: AlertState | None
    to_state: AlertState
    actor: Literal["system", "user"]
    reason: str
    created_at: datetime


class AlertEventRecord(BaseModel):
    id: str
    alert_rule_id: str
    strategy_id: str
    strategy_version: int
    strategy_version_id: str
    symbol: str
    signal_date: date
    idempotency_key: str
    state: AlertState
    source: Literal["baostock"] | None
    quality_status: Literal["ready", "excluded", "insufficient", "error"]
    quality_issues: list[str] = Field(default_factory=list)
    explanation: dict[str, object] = Field(default_factory=dict)
    data_fingerprint: str | None
    created_at: datetime
    updated_at: datetime
    transitions: list[AlertTransitionRecord] = Field(default_factory=list)


class AlertEvaluationResponse(BaseModel):
    status: Literal["empty", "ready", "partial", "error"]
    signal_date: date
    events: list[AlertEventRecord]
    quality_issues: list[str] = Field(default_factory=list)


class AlertEventListResponse(BaseModel):
    status: Literal["empty", "ready", "partial", "error"]
    unacknowledged: int
    items: list[AlertEventRecord]
    quality_issues: list[str] = Field(default_factory=list)
