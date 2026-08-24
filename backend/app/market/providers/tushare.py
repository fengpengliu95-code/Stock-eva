"""Source-shaped Tushare shadow canary adapter.

Tushare's documented daily units remain raw (lots and thousand CNY).  This adapter never
converts them and refuses explicit execution until reviewed HTTPS/terms evidence exists.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import Any

from pydantic import BaseModel, ConfigDict

from ..shadow_evidence import ShadowLogicalRequest
from .http import BoundedHttpClient


class TushareExecutionBlocked(RuntimeError):
    def __init__(self, reason: str = "tushare execution is not authorized") -> None:
        self.reason = reason
        super().__init__(reason)


class TushareSchemaError(ValueError):
    """Sanitized source schema/identity failure."""


class TushareSourceContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    endpoint: str
    units: dict[str, str]
    normalization_required: bool = True
    suspension_semantics: str = "no-row-is-not-suspension-proof"


class TusharePlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    symbols: tuple[str, ...]
    requests: tuple[ShadowLogicalRequest, ...]
    request_count: int


@dataclass(frozen=True)
class TushareSourceBatch:
    trade_date: date
    daily: tuple[dict[str, Any], ...]
    adj_factor: tuple[dict[str, Any], ...]
    suspend_d: tuple[dict[str, Any], ...]
    trade_cal: tuple[dict[str, Any], ...]
    index_daily: tuple[dict[str, Any], ...]
    request_count: int


def _rows(payload: Any) -> tuple[dict[str, Any], ...]:
    if isinstance(payload, dict):
        data = payload.get("data", payload)
        if isinstance(data, dict):
            fields = data.get("fields")
            data = data.get("items", ())
            if isinstance(fields, list) and isinstance(data, list):
                data = [
                    dict(zip(fields, row, strict=False)) if isinstance(row, list) else row
                    for row in data
                ]
    else:
        data = ()
    if not isinstance(data, list):
        raise TushareSchemaError("tushare response schema unavailable")
    if any(not isinstance(row, dict) for row in data):
        raise TushareSchemaError("tushare response schema unavailable")
    return tuple(dict(row) for row in data)


def _validate_daily(rows: tuple[dict[str, Any], ...], trade_date: date) -> None:
    expected = trade_date.strftime("%Y%m%d")
    seen: set[tuple[Any, Any]] = set()
    for row in rows:
        row_date = row.get("trade_date")
        if row_date is not None and str(row_date) != expected:
            raise TushareSchemaError("tushare date binding unavailable")
        key = (row.get("ts_code"), row_date)
        if key in seen:
            raise TushareSchemaError("tushare duplicate row unavailable")
        seen.add(key)
        for field in ("open", "high", "low", "close", "vol", "amount"):
            if field in row and row[field] is not None:
                try:
                    if not isfinite(float(row[field])):
                        raise TushareSchemaError("tushare non-finite value unavailable")
                except (TypeError, ValueError):
                    raise TushareSchemaError("tushare numeric value unavailable") from None


class TushareAdapter:
    ENDPOINTS = ("daily", "adj_factor", "suspend_d", "trade_cal", "index_daily")
    _CONTRACTS = {
        "daily": TushareSourceContract(
            endpoint="daily",
            units={"vol": "lots", "amount": "thousand_cny"},
        ),
        "adj_factor": TushareSourceContract(endpoint="adj_factor", units={"adj_factor": "source"}),
        "suspend_d": TushareSourceContract(endpoint="suspend_d", units={"suspend_type": "source"}),
        "trade_cal": TushareSourceContract(endpoint="trade_cal", units={"is_open": "source"}),
        "index_daily": TushareSourceContract(
            endpoint="index_daily", units={"vol": "lots", "amount": "thousand_cny"}
        ),
    }

    def __init__(
        self,
        http_client: BoundedHttpClient | None = None,
        *,
        plan_only: bool = False,
        terms_approved: bool = False,
        official_https_proof: bool = False,
    ) -> None:
        self.http = http_client
        self.plan_only = plan_only
        self.terms_approved = terms_approved
        self.official_https_proof = official_https_proof

    def source_contract(self, endpoint: str) -> TushareSourceContract:
        try:
            return self._CONTRACTS[endpoint]
        except KeyError:
            raise ValueError("unknown tushare endpoint") from None

    def plan(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TusharePlan:
        requests = tuple(
            ShadowLogicalRequest(
                ordinal=ordinal,
                request_id=f"tushare-{endpoint}-{trade_date.isoformat()}-{ordinal}",
                endpoint=endpoint,
                endpoint_class=endpoint,
                role=endpoint,
                trade_date=trade_date,
                symbol_or_index_shard="symbols" if symbols else "universe",
                schema_contract_hash="3" * 64,
                unit_contract_hash="4" * 64,
            )
            for ordinal, endpoint in enumerate(self.ENDPOINTS)
        )
        return TusharePlan(
            trade_date=trade_date,
            symbols=tuple(symbols),
            requests=requests,
            request_count=len(requests),
        )

    def fetch(
        self, trade_date: date, *, symbols: tuple[str, ...] = ()
    ) -> TushareSourceBatch | TusharePlan:
        plan = self.plan(trade_date, symbols=symbols)
        if self.plan_only:
            return plan
        if self.http is None:
            raise ValueError("shadow HTTP client is required")
        params = {"trade_date": trade_date.strftime("%Y%m%d"), "ts_code": ",".join(symbols)}
        payloads = {
            endpoint: self.http.request_json(
                endpoint, method="POST", json_body={"api_name": endpoint, **params}
            )
            for endpoint in self.ENDPOINTS
        }
        daily = _rows(payloads["daily"])
        _validate_daily(daily, trade_date)
        return TushareSourceBatch(
            trade_date=trade_date,
            daily=daily,
            adj_factor=_rows(payloads["adj_factor"]),
            suspend_d=_rows(payloads["suspend_d"]),
            trade_cal=_rows(payloads["trade_cal"]),
            index_daily=_rows(payloads["index_daily"]),
            request_count=self.http.request_count,
        )

    def execute(
        self,
        trade_date: date,
        *,
        symbols: tuple[str, ...] = (),
        external_authorization_id: str | None = None,
    ) -> TushareSourceBatch:
        # This gate is deliberately before any token/env lookup or HTTP invocation.
        if not external_authorization_id:
            raise TushareExecutionBlocked("external authorization is required")
        if not self.terms_approved:
            raise TushareExecutionBlocked("terms evidence unavailable")
        if not self.official_https_proof:
            raise TushareExecutionBlocked("official HTTPS transport proof unavailable")
        if self.http is None:
            raise TushareExecutionBlocked("shadow HTTP client unavailable")
        return self.fetch(trade_date, symbols=symbols)  # type: ignore[return-value]

    run = fetch
    TushareProvider = None


TushareProvider = TushareAdapter
