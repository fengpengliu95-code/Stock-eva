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
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ShadowNormalizationError(ValueError):
    """A bounded, non-secret normalization or self-quality failure."""


SHADOW_PROVIDERS = frozenset({"tickflow", "tushare"})


class TushareUnitContract(BaseModel):
    """Reviewed source contract; payload fields are never trusted to define it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    volume_unit: Literal["lots"]
    amount_unit: Literal["thousand_cny"]
    factor_semantics: Literal["multiplicative_back_adjust"]
    factor_anchor: Literal["trade_date"]
    factor_direction: Literal["back_adjust"]

    @model_validator(mode="after")
    def reviewed_values_only(self) -> TushareUnitContract:
        if self.model_dump() != {
            "volume_unit": "lots",
            "amount_unit": "thousand_cny",
            "factor_semantics": "multiplicative_back_adjust",
            "factor_anchor": "trade_date",
            "factor_direction": "back_adjust",
        }:
            raise ValueError("tushare source contract is not reviewed")
        return self


REVIEWED_TUSHARE_UNIT_CONTRACT = TushareUnitContract(
    volume_unit="lots",
    amount_unit="thousand_cny",
    factor_semantics="multiplicative_back_adjust",
    factor_anchor="trade_date",
    factor_direction="back_adjust",
)


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
    try:
        parsed = (
            date.fromisoformat(raw[:10])
            if "-" in raw
            else date.fromisoformat(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
        )
    except ValueError:
        raise ShadowNormalizationError("shadow date binding unavailable") from None
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
    factor_previous: float | None = None
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
        if self.factor_previous is not None and (
            not math.isfinite(self.factor_previous) or self.factor_previous <= 0
        ):
            raise ValueError("shadow previous factor is invalid")
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
    factor_semantics: str = ""
    factor_anchor: str = ""
    factor_direction: str = ""
    required_index_symbols: tuple[str, ...] = ()
    suspended_symbols: tuple[str, ...] = ()
    not_listed_symbols: tuple[str, ...] = ()
    job_id: str = ""
    window_id: str = ""
    session_id: str = ""
    version_vector_sha256: str = ""
    unavailable_reason: str | None = None
    normalized_sha256: str = "0" * 64

    @model_validator(mode="after")
    def validate_candidate(self) -> ShadowNormalizedCandidate:
        if self.provider_id not in SHADOW_PROVIDERS:
            raise ValueError("shadow provider identity is not allowlisted")
        if self.complete and self.unavailable_reason is not None:
            raise ValueError("complete shadow candidate has unavailable reason")
        if not self.complete and not self.unavailable_reason:
            raise ValueError("incomplete shadow candidate lacks typed unavailable reason")
        if any(row.provider_id != self.provider_id for row in self.rows):
            raise ValueError("shadow candidate contains mixed providers")
        symbols = tuple(row.symbol for row in self.rows)
        if symbols != tuple(sorted(set(symbols))):
            raise ValueError("shadow rows must be sorted and unique")
        if tuple(sorted(set(self.expected_symbols))) != self.expected_symbols:
            raise ValueError("shadow universe must be sorted and unique")
        if set(symbols) - set(self.expected_symbols):
            raise ValueError("shadow row is outside declared universe")
        legal_missing = set(self.suspended_symbols) | set(self.not_listed_symbols)
        suspended = set(self.suspended_symbols)
        not_listed = set(self.not_listed_symbols)
        if suspended & not_listed or suspended & set(symbols) or not_listed & set(symbols):
            raise ValueError("shadow status evidence contradicts daily rows")
        if any(row.suspension and row.symbol not in suspended for row in self.rows):
            raise ValueError("shadow row suspension lacks explicit suspension evidence")
        if not (set(self.expected_symbols) - set(symbols)).issubset(legal_missing):
            raise ValueError("shadow candidate coverage is incomplete")
        if legal_missing != set(self.expected_symbols) - set(symbols):
            raise ValueError("shadow status evidence must exactly explain missing symbols")
        if tuple(sorted(set(self.required_index_symbols))) != self.required_index_symbols:
            raise ValueError("shadow required indexes are not ordered")
        if self.required_index_symbols and set(self.required_index_symbols) != set(
            self.index_symbols
        ):
            raise ValueError("shadow required indexes are incomplete")
        expected = _sha(
            {
                "provider_id": self.provider_id,
                "trade_date": self.trade_date.isoformat(),
                "universe_id": self.universe_id,
                "job_id": self.job_id,
                "window_id": self.window_id,
                "session_id": self.session_id,
                "version_vector_sha256": self.version_vector_sha256,
                "unavailable_reason": self.unavailable_reason,
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
    contract: TushareUnitContract | None = None,
    required_indexes: tuple[str, ...] = (),
) -> ShadowNormalizedCandidate:
    daily = _rows(payload, "daily")
    universe = _rows(payload, "universe") or _rows(payload, "stock_basic")
    suspend = _rows(payload, "suspend_d")
    factors = _rows(payload, "adj_factor")
    indexes = _rows(payload, "index_daily") or _rows(payload, "indexes")
    if type(contract) is not TushareUnitContract or contract != REVIEWED_TUSHARE_UNIT_CONTRACT:
        raise ShadowNormalizationError("tushare unit contract unavailable")
    factor_semantics = contract.factor_semantics
    factor_anchor = contract.factor_anchor
    factor_direction = contract.factor_direction
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
    listed_missing = set(expected) - listed
    daily_symbols = {_row_symbol(item) for item in daily}
    if daily_symbols & (suspended | listed_missing):
        raise ShadowNormalizationError("shadow status evidence contradicts daily rows")
    factor_by_symbol: dict[str, tuple[float, float | None]] = {}
    for item in factors:
        factor_date = item.get("trade_date") or item.get("date")
        _date(factor_date, trade_date)
        if str(item.get("factor_semantics", factor_semantics)) != factor_semantics:
            raise ShadowNormalizationError("shadow factor semantics unavailable")
        if str(item.get("anchor_date", trade_date.isoformat()))[:10] != trade_date.isoformat():
            raise ShadowNormalizationError("shadow factor anchor date unavailable")
        symbol = _row_symbol(item)
        if symbol in factor_by_symbol:
            raise ShadowNormalizationError("shadow factor row duplicated")
        previous = item.get("prev_adj_factor", item.get("factor_previous"))
        factor_by_symbol[symbol] = (
            _finite(item.get("adj_factor"), positive=True),
            None if previous is None else _finite(previous, positive=True),
        )
    index_symbols = {_row_symbol(item) for item in indexes}
    if required_indexes and set(required_indexes) != index_symbols:
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
                    provider_id="tushare",
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
                    factor=(factor_by_symbol.get(symbol) or (None, None))[0],
                    factor_previous=(factor_by_symbol.get(symbol) or (None, None))[1],
                )
            )
        except ValueError as exc:
            raise ShadowNormalizationError("shadow OHLC self-quality gate failed") from exc
    _ensure_complete(rows, expected, suspended, listed)
    if any(
        row.symbol in listed
        and not row.suspension
        and (row.factor is None or row.factor_previous is None)
        for row in rows
    ):
        raise ShadowNormalizationError("shadow factor evidence unavailable")
    return ShadowNormalizedCandidate(
        provider_id="tushare",
        trade_date=trade_date,
        universe_id=universe_id,
        rows=tuple(sorted(rows, key=lambda row: row.symbol)),
        expected_symbols=expected,
        index_symbols=tuple(sorted(index_symbols)),
        required_index_symbols=tuple(sorted(required_indexes)),
        suspended_symbols=tuple(sorted(suspended - {row.symbol for row in rows})),
        not_listed_symbols=tuple(sorted(set(expected) - listed - {row.symbol for row in rows})),
        factor_semantics=factor_semantics,
        factor_anchor=factor_anchor,
        factor_direction=factor_direction,
        job_id=str(payload.get("job_id", "")) if isinstance(payload, Mapping) else "",
        window_id=str(payload.get("window_id", "")) if isinstance(payload, Mapping) else "",
        session_id=str(payload.get("session_id", "")) if isinstance(payload, Mapping) else "",
        version_vector_sha256=(
            str(payload.get("version_vector_sha256", "")) if isinstance(payload, Mapping) else ""
        ),
        complete=True,
    )


def normalize_tickflow(
    payload: Any,
    *,
    trade_date: date,
    universe_id: str,
    required_indexes: tuple[str, ...] = (),
) -> ShadowNormalizedCandidate:
    daily = _rows(payload, "daily")
    universe = _rows(payload, "universe")
    indexes = _rows(payload, "indexes")
    factors = _rows(payload, "factors") or _rows(payload, "adj_factor")
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
    index_symbols = {_row_symbol(item) for item in indexes}
    if required_indexes and set(required_indexes) != index_symbols:
        raise ShadowNormalizationError("shadow required index coverage unavailable")
    suspension_rows = _rows(payload, "suspensions") or _rows(payload, "suspend_d")
    suspended = {
        _row_symbol(item)
        for item in suspension_rows
        if str(item.get("trade_date", item.get("suspend_date", ""))).replace("-", "")
        == trade_date.strftime("%Y%m%d")
    }
    listed = {
        _row_symbol(item)
        for item in universe
        if str(item.get("list_status", "L")).upper() in {"L", "P", "G"}
    }
    daily_symbols = {_row_symbol(item) for item in daily}
    if daily_symbols & (suspended | (set(expected) - listed)):
        raise ShadowNormalizationError("shadow status evidence contradicts daily rows")
    factor_values: dict[str, tuple[float, float | None]] = {}
    if factors:
        seen_factors: set[str] = set()
        for item in factors:
            factor_symbol = _row_symbol(item)
            if factor_symbol in seen_factors:
                raise ShadowNormalizationError("tickflow factor row duplicated")
            seen_factors.add(factor_symbol)
            _date(item.get("trade_date") or item.get("date"), trade_date)
            if item.get("anchor_date") is not None:
                _date(item["anchor_date"], trade_date)
        raise ShadowNormalizationError("tickflow factor contract unproven")
    rows: list[ShadowNormalizedRow] = []
    for source in daily:
        symbol = _row_symbol(source)
        _date(source.get("date") or source.get("trade_date"), trade_date)
        try:
            rows.append(
                ShadowNormalizedRow(
                    symbol=symbol,
                    provider_id="tickflow",
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
                    factor=(factor_values.get(symbol) or (None, None))[0],
                    factor_previous=(factor_values.get(symbol) or (None, None))[1],
                )
            )
        except ValueError as exc:
            raise ShadowNormalizationError("shadow OHLC self-quality gate failed") from exc
    _ensure_complete(rows, expected, suspended, listed)
    factor_semantics = ""
    factor_anchor = ""
    factor_direction = ""
    complete = False
    return ShadowNormalizedCandidate(
        provider_id="tickflow",
        trade_date=trade_date,
        universe_id=universe_id,
        rows=tuple(sorted(rows, key=lambda row: row.symbol)),
        expected_symbols=expected,
        index_symbols=tuple(sorted(index_symbols)),
        required_index_symbols=tuple(sorted(required_indexes)),
        suspended_symbols=tuple(sorted(suspended - {row.symbol for row in rows})),
        not_listed_symbols=tuple(sorted(set(expected) - listed - {row.symbol for row in rows})),
        factor_semantics=factor_semantics,
        factor_anchor=factor_anchor,
        factor_direction=factor_direction,
        job_id=str(payload.get("job_id", "")) if isinstance(payload, Mapping) else "",
        window_id=str(payload.get("window_id", "")) if isinstance(payload, Mapping) else "",
        session_id=str(payload.get("session_id", "")) if isinstance(payload, Mapping) else "",
        version_vector_sha256=(
            str(payload.get("version_vector_sha256", "")) if isinstance(payload, Mapping) else ""
        ),
        complete=complete,
        quality_status="ready" if complete else "unavailable",
        unavailable_reason="FACTOR_CONTRACT_UNPROVEN",
    )


normalize_tushare_batch = normalize_tushare
normalize_tickflow_batch = normalize_tickflow
