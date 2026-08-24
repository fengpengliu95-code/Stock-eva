"""Source-shaped TickFlow shadow canary adapter.

This module intentionally has no dependency on canonical evidence, candidates or dataset
writers.  It records what the source returned and leaves units/normalization to Task 12.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from ..shadow_evidence import ShadowLogicalRequest
from .http import CanaryExecutor, _AuthorizedCanarySession

ADAPTER_HASH = "1" * 64
ENDPOINT_CONTRACT_HASH = "2" * 64
SOURCE_SCHEMA_HASH = "3" * 64


class TickFlowPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    symbols: tuple[str, ...]
    requests: tuple[ShadowLogicalRequest, ...]
    request_count: int = Field(ge=0)
    requires_factor_evidence: bool = True
    factor_status: str = "unavailable"


class TickFlowSchemaError(ValueError):
    """Sanitized source schema/identity failure."""


@dataclass(frozen=True)
class TickFlowSourceBatch:
    trade_date: date
    daily: tuple[dict[str, Any], ...]
    universe: tuple[dict[str, Any], ...]
    indexes: tuple[dict[str, Any], ...]
    adjusted: bool = False
    factor_status: str = "unavailable"
    request_count: int = 0
    units: dict[str, str] | None = None


def _rows(payload: Any) -> tuple[dict[str, Any], ...]:
    if isinstance(payload, dict):
        payload = payload.get("data", payload.get("items", ()))
        if isinstance(payload, dict):
            fields = payload.get("fields")
            payload = payload.get("items", payload.get("data", ()))
            if isinstance(fields, list) and isinstance(payload, list):
                payload = [
                    dict(zip(fields, row, strict=False)) if isinstance(row, list) else row
                    for row in payload
                ]
    if not isinstance(payload, list):
        raise TickFlowSchemaError("tickflow response schema unavailable")
    rows = []
    for row in payload:
        if not isinstance(row, dict):
            raise TickFlowSchemaError("tickflow response schema unavailable")
        rows.append(dict(row))
    return tuple(rows)


def _validate_daily(rows: tuple[dict[str, Any], ...], trade_date: date) -> None:
    seen: set[tuple[Any, Any]] = set()
    for row in rows:
        row_date = row.get("date") or row.get("trade_date")
        if row_date is not None and str(row_date) != trade_date.isoformat():
            raise TickFlowSchemaError("tickflow date binding unavailable")
        key = (row.get("symbol") or row.get("code"), row_date)
        if key in seen:
            raise TickFlowSchemaError("tickflow duplicate row unavailable")
        seen.add(key)
        for field in ("open", "high", "low", "close"):
            if field in row:
                try:
                    if not isfinite(float(row[field])):
                        raise TickFlowSchemaError("tickflow non-finite value unavailable")
                except (TypeError, ValueError):
                    raise TickFlowSchemaError("tickflow numeric value unavailable") from None


class TickFlowAdapter:
    PROVIDER_ID = "tickflow"
    ADAPTER_HASH = ADAPTER_HASH
    ENDPOINT_CONTRACT_HASH = ENDPOINT_CONTRACT_HASH
    SOURCE_SCHEMA_HASH = SOURCE_SCHEMA_HASH
    ENDPOINTS = ("daily", "universe", "indexes")

    def __init__(self, *, plan_only: bool = True):
        self.plan_only = plan_only

    def plan(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TickFlowPlan:
        requests = tuple(
            ShadowLogicalRequest(
                provider_id="tickflow",
                ordinal=ordinal,
                request_id=f"tickflow-{endpoint}-{trade_date.isoformat()}-{ordinal}",
                endpoint=endpoint,
                endpoint_class=endpoint,
                role="daily" if endpoint == "daily" else endpoint,
                trade_date=trade_date,
                symbol_or_index_shard="symbols" if symbols else "universe",
                schema_contract_hash="1" * 64,
                unit_contract_hash="2" * 64,
            )
            for ordinal, endpoint in enumerate(self.ENDPOINTS)
        )
        return TickFlowPlan(
            trade_date=trade_date,
            symbols=tuple(symbols),
            requests=requests,
            request_count=len(requests),
        )

    def canary_plan(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TickFlowPlan:
        return self.plan(trade_date, symbols=symbols)

    def request_params(
        self, endpoint: str, trade_date: date, symbols: tuple[str, ...]
    ) -> dict[str, str]:
        return {"trade_date": trade_date.isoformat(), "symbols": ",".join(symbols)}

    def parse(
        self, trade_date: date, payloads: dict[str, Any], *, request_count: int = 0
    ) -> TickFlowSourceBatch:
        daily = _rows(payloads["daily"])
        _validate_daily(daily, trade_date)
        return TickFlowSourceBatch(
            trade_date=trade_date,
            daily=daily,
            universe=_rows(payloads["universe"]),
            indexes=_rows(payloads["indexes"]),
            request_count=request_count,
            units=(
                dict(payloads.get("units", {}))
                if isinstance(payloads.get("units", {}), dict)
                else None
            ),
        )

    def execute(
        self,
        trade_date: date,
        *,
        symbols: tuple[str, ...] = (),
        session: CanaryExecutor,
    ) -> TickFlowSourceBatch:
        if type(session) is not _AuthorizedCanarySession:
            raise PermissionError("authorized canary session required")
        return session.execute(self, trade_date, symbols=symbols)

    def fetch(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TickFlowPlan:
        return self.plan(trade_date, symbols=symbols)

    def run(
        self, trade_date: date, *, symbols: tuple[str, ...] = ()
    ) -> TickFlowSourceBatch | TickFlowPlan:
        return self.plan(trade_date, symbols=symbols)


# Short aliases are useful to callers that name the source rather than the adapter role.
TickFlowProvider = TickFlowAdapter
TickflowAdapter = TickFlowAdapter
