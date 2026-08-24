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
from .http import BoundedHttpClient, CanaryPermissionError, _CanaryPermit, _check_canary_permit

ADAPTER_HASH = "4" * 64
ENDPOINT_CONTRACT_HASH = "5" * 64
SOURCE_SCHEMA_HASH = "6" * 64
OFFICIAL_HTTPS_PROVEN = False


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
    stock_basic: tuple[dict[str, Any], ...]
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
    ENDPOINTS = (
        "daily",
        "adj_factor",
        "suspend_d",
        "trade_cal",
        "index_daily",
        "stock_basic",
    )
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
        "stock_basic": TushareSourceContract(
            endpoint="stock_basic", units={"list_status": "source"}
        ),
    }

    def __init__(
        self,
        http_client: BoundedHttpClient | None = None,
        *,
        plan_only: bool = False,
    ) -> None:
        self.http = http_client
        self.plan_only = plan_only

    def source_contract(self, endpoint: str) -> TushareSourceContract:
        try:
            return self._CONTRACTS[endpoint]
        except KeyError:
            raise ValueError("unknown tushare endpoint") from None

    def plan(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TusharePlan:
        requests = tuple(
            ShadowLogicalRequest(
                provider_id="tushare",
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
        self,
        trade_date: date,
        *,
        symbols: tuple[str, ...] = (),
        permit: _CanaryPermit | None = None,
    ) -> TushareSourceBatch | TusharePlan:
        if not OFFICIAL_HTTPS_PROVEN:
            raise CanaryPermissionError("tushare official HTTPS is unproven")
        checked = _check_canary_permit(
            permit,
            provider_id="tushare",
            adapter_hash=ADAPTER_HASH,
            endpoint_contract_hash=ENDPOINT_CONTRACT_HASH,
            source_schema_hash=SOURCE_SCHEMA_HASH,
        )
        return self._fetch_with_permit(trade_date, symbols=symbols, permit=checked)

    def _fetch_with_permit(
        self,
        trade_date: date,
        *,
        symbols: tuple[str, ...] = (),
        permit: _CanaryPermit,
    ) -> TushareSourceBatch | TusharePlan:
        if not OFFICIAL_HTTPS_PROVEN:
            raise CanaryPermissionError("tushare official HTTPS is unproven")
        plan = self.plan(trade_date, symbols=symbols)
        if self.plan_only:
            return plan
        if self.http is None:
            raise ValueError("shadow HTTP client is required")
        headers = permit.headers()
        params = {"trade_date": trade_date.strftime("%Y%m%d"), "ts_code": ",".join(symbols)}
        payloads = {
            endpoint: self.http.request_json(
                endpoint,
                method="POST",
                json_body={"api_name": endpoint, **params},
                headers=headers,
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
            stock_basic=_rows(payloads["stock_basic"]),
            request_count=self.http.request_count,
        )

    def execute(
        self,
        trade_date: date,
        *,
        symbols: tuple[str, ...] = (),
        permit: _CanaryPermit | None = None,
    ) -> TushareSourceBatch:
        # The permit factory has already checked terms, static HTTPS and the closed env map.
        if not OFFICIAL_HTTPS_PROVEN:
            raise TushareExecutionBlocked("tushare official HTTPS is unproven")
        try:
            checked = _check_canary_permit(
                permit,
                provider_id="tushare",
                adapter_hash=ADAPTER_HASH,
                endpoint_contract_hash=ENDPOINT_CONTRACT_HASH,
                source_schema_hash=SOURCE_SCHEMA_HASH,
            )
        except CanaryPermissionError as exc:
            raise TushareExecutionBlocked("private canary permit required") from exc
        result = self._fetch_with_permit(trade_date, symbols=symbols, permit=checked)
        if isinstance(result, TusharePlan):
            raise TushareExecutionBlocked("shadow HTTP client unavailable")
        return result

    run = fetch
    TushareProvider = None


TushareProvider = TushareAdapter
