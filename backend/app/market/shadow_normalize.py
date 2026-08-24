"""Provider-shaped to stable comparison units for the R2-F3 shadow lane.

Nothing in this module is used by the canonical publication path.  In particular, source
rows are converted only after the provider contract has declared their units; unknown units
fail closed instead of being guessed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ShadowNormalizationError(ValueError):
    """A bounded, non-secret normalization or self-quality failure."""


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        + "\n"
    ).encode()


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _finite(value: Any, *, positive: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ShadowNormalizationError("shadow numeric value unavailable") from None
    if not math.isfinite(result) or (positive and result <= 0):
        raise ShadowNormalizationError("shadow numeric value unavailable")
    return result


def _rows(source: Any, key: str) -> tuple[dict[str, Any], ...]:
    if isinstance(source, Mapping):
        value = source.get(key, ())
    else:
        value = getattr(source, key, ())
    if value is None:
        return ()
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ShadowNormalizationError("shadow source rows unavailable")
    if any(not isinstance(item, Mapping) for item in value):
        raise ShadowNormalizationError("shadow source rows unavailable")
    return tuple(dict(item) for item in value)


def _symbol(value: Any) -> str:
    raw = str(value or "").strip()
    if "." in raw:
        code, exchange = raw.rsplit(".", 1)
        exchange = exchange.lower()
        if exchange in {"sz", "sh"}:
            return f"{exchange}.{code.zfill(6)}"
    if raw.lower().startswith(("sh.", "sz.")):
        exchange, code = raw.lower().split(".", 1)
        return f"{exchange}.{code.zfill(6)}"
    raise ShadowNormalizationError("shadow symbol unavailable")


def _row_symbol(row: Mapping[str, Any]) -> str:
    return _symbol(row.get("symbol") or row.get("code") or row.get("ts_code"))


def _date(value: Any, expected: date) -> date:
    raw = str(value or "")
    parsed = (
        date.fromisoformat(raw[:10])
        if "-" in raw
        else date.fromisoformat(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
    )
    if parsed != expected:
        raise ShadowNormalizationError("shadow date binding unavailable")
    return parsed


class ShadowNormalizedRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    symbol: str
    provider_id: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    preclose: float
    volume: float = Field(ge=0)
    amount: float = Field(ge=0)
    suspension: bool = False
    listed: bool = True
    factor: float | None = None
    factor_semantics: str = "provider_raw"
    price_unit: str = "CNY"
    volume_unit: str = "shares"
    amount_unit: str = "CNY"

    @model_validator(mode="after")
    def valid_prices(self) -> ShadowNormalizedRow:
        values = (self.open, self.high, self.low, self.close, self.preclose)
        if any(not math.isfinite(item) for item in values) or any(item <= 0 for item in values):
            raise ValueError("shadow OHLC is non-finite or impossible")
        if self.high < max(self.open, self.close, self.low) or self.low > min(
            self.open, self.close, self.high
        ):
            raise ValueError("shadow OHLC is impossible")
        if self.factor is not None and (not math.isfinite(self.factor) or self.factor <= 0):
            raise ValueError("shadow factor is invalid")
        return self


class ShadowNormalizedCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_id: str
    trade_date: date
    universe_id: str
    rows: tuple[ShadowNormalizedRow, ...]
    expected_symbols: tuple[str, ...]
    index_symbols: tuple[str, ...] = ()
    complete: bool = True
    quality_status: str = "ready"
    normalized_sha256: str = "0" * 64

    @model_validator(mode="after")
    def validate_candidate(self) -> ShadowNormalizedCandidate:
        symbols = tuple(row.symbol for row in self.rows)
        if symbols != tuple(sorted(set(symbols))):
            raise ValueError("shadow rows must be sorted and unique")
        if tuple(sorted(set(self.expected_symbols))) != self.expected_symbols:
            raise ValueError("shadow universe must be sorted and unique")
        if set(symbols) - set(self.expected_symbols):
            raise ValueError("shadow row is outside declared universe")
        expected = _sha(
            {
                "provider_id": self.provider_id,
                "trade_date": self.trade_date.isoformat(),
                "universe_id": self.universe_id,
                "rows": [row.model_dump(mode="json") for row in self.rows],
            }
        )
        if self.normalized_sha256 == "0" * 64:
            object.__setattr__(self, "normalized_sha256", expected)
        elif self.normalized_sha256 != expected:
            raise ValueError("shadow normalized hash mismatch")
        return self


def _ensure_complete(
    rows: Sequence[ShadowNormalizedRow],
    expected: tuple[str, ...],
    suspended: set[str],
    listed: set[str],
) -> None:
    observed = {row.symbol for row in rows}
    missing = set(expected) - observed
    illegal = missing - suspended - (set(expected) - listed)
    if illegal:
        raise ShadowNormalizationError("shadow candidate coverage unavailable")


def _source_contract(payload: Any, endpoint: str) -> Mapping[str, Any] | None:
    if isinstance(payload, Mapping):
        contracts = payload.get("contracts") or payload.get("units")
        if isinstance(contracts, Mapping):
            value = contracts.get(endpoint, contracts if endpoint == "daily" else None)
            if isinstance(value, Mapping):
                return value
    else:
        value = getattr(payload, "units", None) if endpoint == "daily" else None
        if isinstance(value, Mapping):
            return value
    return None


def normalize_tushare(
    payload: Any,
    *,
    trade_date: date,
    universe_id: str,
    provider_id: str = "tushare",
    required_indexes: tuple[str, ...] = (),
) -> ShadowNormalizedCandidate:
    daily = _rows(payload, "daily")
    universe = _rows(payload, "universe") or _rows(payload, "stock_basic")
    suspend = _rows(payload, "suspend_d")
    factors = _rows(payload, "adj_factor")
    indexes = _rows(payload, "index_daily") or _rows(payload, "indexes")
    contract = _source_contract(payload, "daily")
    if contract is not None and (
        contract.get("vol") not in {None, "lots"}
        or contract.get("amount") not in {None, "thousand_cny"}
    ):
        raise ShadowNormalizationError("tushare unit contract unavailable")
    expected = tuple(sorted({_row_symbol(item) for item in universe}))
    if not expected:
        raise ShadowNormalizationError("shadow universe unavailable")
    listed = {
        _row_symbol(item)
        for item in universe
        if str(item.get("list_status", "L")).upper() in {"L", "P", "G"}
    }
    suspended = {
        _row_symbol(item)
        for item in suspend
        if str(item.get("trade_date", item.get("suspend_date", ""))).replace("-", "")
        == trade_date.strftime("%Y%m%d")
    }
    factor_by_symbol = {
        _row_symbol(item): _finite(item.get("adj_factor"), positive=True)
        for item in factors
        if item.get("adj_factor") is not None
    }
    if required_indexes and not set(required_indexes).issubset(
        {_row_symbol(item) for item in indexes}
    ):
        raise ShadowNormalizationError("shadow required index coverage unavailable")
    rows: list[ShadowNormalizedRow] = []
    seen: set[str] = set()
    for source in daily:
        symbol = _row_symbol(source)
        if symbol in seen or symbol not in expected:
            raise ShadowNormalizationError("shadow symbol coverage unavailable")
        seen.add(symbol)
        _date(source.get("trade_date") or source.get("date"), trade_date)
        try:
            rows.append(
                ShadowNormalizedRow(
                    symbol=symbol,
                    provider_id=provider_id,
                    trade_date=trade_date,
                    open=_finite(source.get("open"), positive=True),
                    high=_finite(source.get("high"), positive=True),
                    low=_finite(source.get("low"), positive=True),
                    close=_finite(source.get("close"), positive=True),
                    preclose=_finite(
                        source.get("pre_close", source.get("preclose")), positive=True
                    ),
                    volume=_finite(source.get("vol"), positive=False) * 100,
                    amount=_finite(source.get("amount"), positive=False) * 1000,
                    suspension=False,
                    listed=symbol in listed,
                    factor=factor_by_symbol.get(symbol),
                )
            )
        except ValueError as exc:
            raise ShadowNormalizationError("shadow OHLC self-quality gate failed") from exc
    _ensure_complete(rows, expected, suspended, listed)
    if any(row.symbol in listed and not row.suspension and row.factor is None for row in rows):
        raise ShadowNormalizationError("shadow factor evidence unavailable")
    return ShadowNormalizedCandidate(
        provider_id=provider_id,
        trade_date=trade_date,
        universe_id=universe_id,
        rows=tuple(sorted(rows, key=lambda row: row.symbol)),
        expected_symbols=expected,
        complete=True,
    )


def normalize_tickflow(
    payload: Any,
    *,
    trade_date: date,
    universe_id: str,
    provider_id: str = "tickflow",
    required_indexes: tuple[str, ...] = (),
) -> ShadowNormalizedCandidate:
    daily = _rows(payload, "daily")
    universe = _rows(payload, "universe")
    indexes = _rows(payload, "indexes")
    units = _source_contract(payload, "daily") or (
        payload.get("unit_contract") if isinstance(payload, Mapping) else None
    )
    if (
        not isinstance(units, Mapping)
        or units.get("volume") not in {"shares", "share"}
        or units.get("amount") not in {"CNY", "cny"}
    ):
        raise ShadowNormalizationError("tickflow units are unproven")
    expected = tuple(sorted({_row_symbol(item) for item in universe}))
    if not expected:
        raise ShadowNormalizationError("shadow universe unavailable")
    if required_indexes and not set(required_indexes).issubset(
        {_row_symbol(item) for item in indexes}
    ):
        raise ShadowNormalizationError("shadow required index coverage unavailable")
    rows: list[ShadowNormalizedRow] = []
    for source in daily:
        symbol = _row_symbol(source)
        _date(source.get("date") or source.get("trade_date"), trade_date)
        try:
            rows.append(
                ShadowNormalizedRow(
                    symbol=symbol,
                    provider_id=provider_id,
                    trade_date=trade_date,
                    open=_finite(source.get("open"), positive=True),
                    high=_finite(source.get("high"), positive=True),
                    low=_finite(source.get("low"), positive=True),
                    close=_finite(source.get("close"), positive=True),
                    preclose=_finite(
                        source.get("preclose", source.get("pre_close")), positive=True
                    ),
                    volume=_finite(source.get("volume"), positive=False),
                    amount=_finite(source.get("amount"), positive=False),
                    factor=None,
                )
            )
        except ValueError as exc:
            raise ShadowNormalizationError("shadow OHLC self-quality gate failed") from exc
    _ensure_complete(rows, expected, set(), set(expected))
    return ShadowNormalizedCandidate(
        provider_id=provider_id,
        trade_date=trade_date,
        universe_id=universe_id,
        rows=tuple(sorted(rows, key=lambda row: row.symbol)),
        expected_symbols=expected,
        index_symbols=tuple(sorted({_row_symbol(item) for item in indexes})),
    )


normalize_tushare_batch = normalize_tushare
normalize_tickflow_batch = normalize_tickflow
