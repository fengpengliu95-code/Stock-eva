"""Pinned, source-shaped TickFlow discovery canary.

The adapter is deliberately not a market-data provider: it never normalizes, publishes or
qualifies data. Network execution is possible only through the separately gated private
canary session in :mod:`http`; tests use the private
:meth:`TickFlowCanaryRunner._offline_for_tests` helper with a fake
transport.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from io import StringIO
from math import isfinite
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..shadow_evidence import ShadowLogicalRequest, ShadowLogicalRequestPlan
from .http import CanaryExecutor, HttpPolicy, _AuthorizedCanarySession

TICKFLOW_SERVER = "https://api.tickflow.org"
TICKFLOW_FREE_SERVER = "https://free-api.tickflow.org"
TICKFLOW_OPENAPI_URL = "https://docs.tickflow.org/zh-Hans/api-reference/openapi.json"
TICKFLOW_TERMS_URL = "https://tickflow.org/legal/terms-of-service.md"
TICKFLOW_FREE_SDK_VERSION = "0.1.24"
TICKFLOW_FREE_PROVIDER_SYMBOLS = (
    "600000.SH",
    "600519.SH",
    "601318.SH",
    "000001.SZ",
    "000002.SZ",
)
TickFlowFreeProviderSymbols = tuple[
    Literal["600000.SH"],
    Literal["600519.SH"],
    Literal["601318.SH"],
    Literal["000001.SZ"],
    Literal["000002.SZ"],
]
TickFlowFreeEndpoints = tuple[
    Literal["connectivity"],
    Literal["instrument_metadata"],
    Literal["universe_metadata"],
    Literal["historical_daily_1d"],
]
TickFlowFreeMethods = tuple[Literal["GET"], Literal["POST"], Literal["GET"], Literal["GET"]]
TickFlowFreePaths = tuple[
    Literal["/v1/exchanges"],
    Literal["/v1/instruments"],
    Literal["/v1/universes/CN_Equity_A"],
    Literal["/v1/klines/batch"],
]
TICKFLOW_FREE_SAFE_FAILURE_CLASSES = frozenset(
    {
        "discovery_unavailable",
        "request_budget_exhausted",
        "transport_error",
        "timeout",
        "malformed_response",
        "endpoint_url_unavailable",
        "redirect_or_endpoint_mismatch",
        "response_oversize",
        "rate_limited",
        "server_error",
        "unauthorized",
        "http_error",
        "malformed_json",
        "row_budget_exhausted",
        "retry_budget_exhausted",
        "symbol_schema",
        "schema_drift",
        "missing_endpoint",
        "empty_endpoint",
        "wrong_date",
        "duplicate_row",
        "column_length_mismatch",
        "numeric_value_invalid",
        "symbol_set_invalid",
        "endpoint_unavailable",
        "fixed_sample_required",
        "sdk_version_mismatch",
        "sdk_contract_invalid",
        "sdk_initialization_failed",
    }
)
_SAFE_SYMBOL = re.compile(r"^(?:sh|sz)\.\d{6}$")
_MAIN_BOARD = ("sh.600", "sh.601", "sh.603", "sh.605", "sz.000", "sz.001", "sz.002", "sz.003")
_PROVIDER_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ)$")
_INT64_MAX = 2**63 - 1


def tickflow_to_canonical_symbol(value: str) -> str:
    if not isinstance(value, str) or _PROVIDER_SYMBOL.fullmatch(value) is None:
        raise TickFlowDiscoveryError("symbol_schema")
    return f"{value[-2:].lower()}.{value[:6]}"


def canonical_to_tickflow_symbol(value: str) -> str:
    if not isinstance(value, str) or _SAFE_SYMBOL.fullmatch(value) is None:
        raise TickFlowDiscoveryError("symbol_schema")
    return f"{value[3:]}.{value[:2].upper()}"


def _hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(b"stock-eva/r2f3/task14/tickflow/v1\n" + encoded).hexdigest()


class TickFlowContract(BaseModel):
    """The reviewed static contract; arbitrary URLs and endpoint names are impossible."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    server: Literal["https://api.tickflow.org"] = TICKFLOW_SERVER
    openapi_url: Literal["https://docs.tickflow.org/zh-Hans/api-reference/openapi.json"] = (
        TICKFLOW_OPENAPI_URL
    )
    api_key_header: Literal["x-api-key"] = "x-api-key"
    endpoints: tuple[str, ...] = ("daily_batch", "ex_factors", "universe", "indexes")
    endpoint_paths: tuple[str, ...] = (
        "/v1/klines/batch",
        "/v1/klines/ex-factors",
        "/v1/universes/CN_Equity_A",
        "/v1/universes/CN_Index",
    )

    @property
    def request_hash(self) -> str:
        return _hash({"endpoints": self.endpoints, "paths": self.endpoint_paths})


ADAPTER_HASH = _hash("tickflow-adapter-task14")
ENDPOINT_CONTRACT_HASH = TickFlowContract().request_hash
SOURCE_SCHEMA_HASH = _hash("tickflow-openapi-source-shape-2026-08-26")
UNIT_CONTRACT_HASH = _hash("tickflow-units-unknown-discovery")


class TickFlowDiscoveryError(ValueError):
    """Sanitized source schema/date/unit failure; never includes provider text."""

    def __init__(self, failure_class: str = "discovery_unavailable") -> None:
        self.failure_class = failure_class
        super().__init__("tickflow discovery contract unavailable")


TickFlowSchemaError = TickFlowDiscoveryError


class TickFlowFreeDiscoveryError(TickFlowDiscoveryError):
    """Typed Free contract failure with only a fixed, non-provider message."""

    _MESSAGES = {
        "fixed_sample_required": "fixed Free sample is required",
        "sdk_version_mismatch": "pinned SDK version is unavailable",
        "sdk_contract_invalid": "pinned SDK Free contract is unavailable",
        "sdk_initialization_failed": "pinned SDK Free initialization is unavailable",
    }

    def __init__(self, failure_class: str = "discovery_unavailable") -> None:
        self.failure_class = failure_class
        ValueError.__init__(self, self._MESSAGES.get(failure_class, "Free discovery unavailable"))


class TickFlowCapabilityState(StrEnum):
    DISCOVERED = "DISCOVERED"
    UNQUALIFIED = "UNQUALIFIED"
    UNKNOWN = "UNKNOWN"
    FORBIDDEN = "FORBIDDEN"


class _FreeImmutable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)


class TickFlowFreeContract(_FreeImmutable):
    """Closed Free surface; it cannot represent authenticated/full-service endpoints."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    server: Literal["https://free-api.tickflow.org"] = TICKFLOW_FREE_SERVER
    endpoints: TickFlowFreeEndpoints = (
        "connectivity",
        "instrument_metadata",
        "universe_metadata",
        "historical_daily_1d",
    )
    endpoint_methods: TickFlowFreeMethods = ("GET", "POST", "GET", "GET")
    endpoint_paths: TickFlowFreePaths = (
        "/v1/exchanges",
        "/v1/instruments",
        "/v1/universes/CN_Equity_A",
        "/v1/klines/batch",
    )

    @property
    def request_hash(self) -> str:
        return _hash(
            {
                "server": self.server,
                "endpoints": self.endpoints,
                "methods": self.endpoint_methods,
                "paths": self.endpoint_paths,
                "symbols": TICKFLOW_FREE_PROVIDER_SYMBOLS,
            }
        )


FREE_ADAPTER_HASH = _hash("tickflow-free-adapter-task14-v1")
FREE_ENDPOINT_CONTRACT_HASH = TickFlowFreeContract().request_hash
FREE_SOURCE_SCHEMA_HASH = _hash("tickflow-free-source-shape-0.1.24-2026-08-27")
FREE_UNIT_CONTRACT_HASH = _hash("tickflow-free-units-unknown-unqualified")


class TickFlowFreePlan(_FreeImmutable):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: Literal["tickflow"] = "tickflow"
    provider_mode: Literal["FREE_DAILY_DISCOVERY"] = "FREE_DAILY_DISCOVERY"
    trade_date: date
    fixed_symbols: TickFlowFreeProviderSymbols = TICKFLOW_FREE_PROVIDER_SYMBOLS
    requests: tuple[ShadowLogicalRequest, ...]
    request_count: Literal[4] = 4


class TickFlowFreeSourceBatch(_FreeImmutable):
    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    exchanges: tuple[dict[str, Any], ...]
    instruments: tuple[dict[str, Any], ...]
    universe: tuple[dict[str, Any], ...]
    daily: tuple[dict[str, Any], ...]
    complete_candidate: Literal[False] = False


class TickFlowFreeCapabilities(_FreeImmutable):
    model_config = ConfigDict(extra="forbid", frozen=True)

    connectivity: Literal[TickFlowCapabilityState.UNKNOWN, TickFlowCapabilityState.DISCOVERED] = (
        TickFlowCapabilityState.UNKNOWN
    )
    instrument_metadata: Literal[
        TickFlowCapabilityState.UNKNOWN, TickFlowCapabilityState.DISCOVERED
    ] = TickFlowCapabilityState.UNKNOWN
    universe_metadata: Literal[
        TickFlowCapabilityState.UNKNOWN, TickFlowCapabilityState.DISCOVERED
    ] = TickFlowCapabilityState.UNKNOWN
    historical_daily_1d: Literal[
        TickFlowCapabilityState.UNKNOWN, TickFlowCapabilityState.DISCOVERED
    ] = TickFlowCapabilityState.UNKNOWN
    realtime_quote: Literal[TickFlowCapabilityState.FORBIDDEN] = TickFlowCapabilityState.FORBIDDEN
    minute_kline: Literal[TickFlowCapabilityState.FORBIDDEN] = TickFlowCapabilityState.FORBIDDEN
    adjustment_factor: Literal[TickFlowCapabilityState.UNQUALIFIED] = (
        TickFlowCapabilityState.UNQUALIFIED
    )
    suspension_semantics: Literal[TickFlowCapabilityState.UNKNOWN] = TickFlowCapabilityState.UNKNOWN
    units: Literal[TickFlowCapabilityState.UNKNOWN] = TickFlowCapabilityState.UNKNOWN
    rate_limit_quota: Literal[TickFlowCapabilityState.UNKNOWN] = TickFlowCapabilityState.UNKNOWN
    raw_retention_contract: Literal[TickFlowCapabilityState.UNKNOWN] = (
        TickFlowCapabilityState.UNKNOWN
    )

    @classmethod
    def discovered(cls) -> TickFlowFreeCapabilities:
        return cls(
            connectivity=TickFlowCapabilityState.DISCOVERED,
            instrument_metadata=TickFlowCapabilityState.DISCOVERED,
            universe_metadata=TickFlowCapabilityState.DISCOVERED,
            historical_daily_1d=TickFlowCapabilityState.DISCOVERED,
        )


class TickFlowFreeSdkProbe(_FreeImmutable):
    model_config = ConfigDict(extra="forbid", frozen=True)

    sdk_version: Literal["0.1.24"] = TICKFLOW_FREE_SDK_VERSION
    base_url: Literal["https://free-api.tickflow.org"] = TICKFLOW_FREE_SERVER
    max_retries: Literal[0] = 0
    provider_requests: Literal[0] = 0


class TickFlowFreeCanaryReport(_FreeImmutable):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["discovered", "unavailable"]
    outcome: Literal["discovery", "unavailable"]
    provider: Literal["tickflow"] = "tickflow"
    provider_mode: Literal["FREE_DAILY_DISCOVERY"] = "FREE_DAILY_DISCOVERY"
    trade_date: date
    fixed_symbols: TickFlowFreeProviderSymbols = TICKFLOW_FREE_PROVIDER_SYMBOLS
    request_count: int = Field(default=0, ge=0, le=4)
    endpoints: TickFlowFreeEndpoints = TickFlowFreeContract().endpoints
    capabilities: TickFlowFreeCapabilities = Field(default_factory=TickFlowFreeCapabilities)
    failure_class: str | None = None
    endpoint: str | None = None
    complete_candidate: Literal[False] = False
    daily_bar_qualified: Literal[False] = False
    adjustment_factor_qualified: Literal[False] = False
    writes: Literal[False] = False
    writes_evidence: Literal[False] = False
    writes_canonical: Literal[False] = False
    starts_shadow: Literal[False] = False

    @field_validator("failure_class")
    @classmethod
    def validate_failure_class(cls, value: str | None) -> str | None:
        if value is not None and value not in TICKFLOW_FREE_SAFE_FAILURE_CLASSES:
            raise ValueError("Free failure class is not allowlisted")
        return value

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, value: str | None) -> str | None:
        if value is not None and value not in TickFlowFreeContract().endpoint_paths:
            raise ValueError("Free endpoint is not allowlisted")
        return value

    @model_validator(mode="after")
    def validate_outcome(self) -> TickFlowFreeCanaryReport:
        discovered = self.status == "discovered"
        if (
            discovered != (self.outcome == "discovery")
            or (discovered and self.request_count != 4)
            or (discovered and self.capabilities != TickFlowFreeCapabilities.discovered())
            or (discovered and (self.failure_class is not None or self.endpoint is not None))
            or (
                not discovered
                and (self.capabilities != TickFlowFreeCapabilities() or self.failure_class is None)
            )
        ):
            raise ValueError("Free discovery report state is inconsistent")
        return self


def initialize_tickflow_free_sdk(
    *,
    sdk_class: type[Any] | None = None,
    sdk_version: str | None = None,
    cache_dir: Path = Path("/dev/null/stock-eva-tickflow-free"),
) -> TickFlowFreeSdkProbe:
    """Validate official ``TickFlow.free()`` without using its network resources.

    The pinned SDK's base constructor otherwise consults ``TICKFLOW_API_KEY`` when given
    ``None``.  This local subclass preserves the official factory call while forcing the
    credential value to the empty string, so Free mode never reads or forwards a credential.
    """

    if sdk_version is None:
        from importlib.metadata import version

        try:
            sdk_version = version("tickflow")
        except Exception:
            raise TickFlowFreeDiscoveryError("sdk_version_mismatch") from None
    if sdk_version != TICKFLOW_FREE_SDK_VERSION:
        raise TickFlowFreeDiscoveryError("sdk_version_mismatch")
    if sdk_class is None:
        try:
            from tickflow import TickFlow as sdk_class
        except Exception:
            raise TickFlowFreeDiscoveryError("sdk_initialization_failed") from None

    class _CredentiallessTickFlow(sdk_class):
        def __init__(self, api_key: str | None = None, **kwargs: Any) -> None:
            del api_key
            super().__init__(api_key="", **kwargs)

    client: Any | None = None
    try:
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
            client = _CredentiallessTickFlow.free(
                base_url=TICKFLOW_FREE_SERVER,
                timeout=30.0,
                max_retries=0,
                cache_dir=str(cache_dir),
            )
        if getattr(client, "api_key", None) != "" or getattr(client, "base_url", None) != (
            TICKFLOW_FREE_SERVER
        ):
            raise TickFlowFreeDiscoveryError("sdk_contract_invalid")
        return TickFlowFreeSdkProbe()
    except TickFlowFreeDiscoveryError:
        raise
    except Exception:
        raise TickFlowFreeDiscoveryError("sdk_initialization_failed") from None
    finally:
        if client is not None:
            close = getattr(client, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    raise TickFlowFreeDiscoveryError("sdk_contract_invalid") from None


class TickFlowPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    symbols: tuple[str, ...]
    requests: tuple[ShadowLogicalRequest, ...]
    request_count: int = Field(ge=0)
    requires_factor_evidence: bool = True
    factor_status: str = "unavailable"


class TickFlowSourceBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    trade_date: date
    daily: tuple[dict[str, Any], ...]
    factors: tuple[dict[str, Any], ...] = ()
    universe: tuple[dict[str, Any], ...]
    indexes: tuple[dict[str, Any], ...]
    adjusted: bool = False
    factor_status: str = "unavailable"
    request_count: int = Field(default=0, ge=0)
    units_status: Literal["unknown", "declared"] = "unknown"
    units: dict[str, str] | None = None
    complete_candidate: bool = False


class TickFlowCanaryReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["discovered", "blocked", "error"]
    outcome: Literal["discovery", "unavailable", "failure"]
    provider: Literal["tickflow"] = "tickflow"
    trade_date: date
    request_count: int = Field(default=0, ge=0)
    endpoints: tuple[str, ...] = ()
    units_status: Literal["unknown", "declared"] = "unknown"
    failure_class: str | None = None
    writes_evidence: bool = False
    writes_canonical: bool = False
    starts_shadow: bool = False
    evidence_id: str | None = None


def _is_main_board(symbol: str) -> bool:
    return symbol.startswith(_MAIN_BOARD)


def _validate_symbols(symbols: tuple[str, ...], *, for_execute: bool) -> None:
    if not for_execute:
        return
    if not 5 <= len(symbols) <= 10 or len(set(symbols)) != len(symbols):
        raise TickFlowDiscoveryError("symbol_count_invalid")
    if any(_SAFE_SYMBOL.fullmatch(item) is None or not _is_main_board(item) for item in symbols):
        raise TickFlowDiscoveryError("symbol_not_main_board")


def _trade_bounds(trade_date: date) -> tuple[int, int]:
    start = datetime.combine(trade_date, time.min, tzinfo=UTC)
    end = start + timedelta(days=1) - timedelta(milliseconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _rows(payload: Any, *, allow_empty: bool = False) -> tuple[dict[str, Any], ...]:
    if not isinstance(payload, dict) or set(payload) - {"data", "units"} or "data" not in payload:
        raise TickFlowDiscoveryError("schema_drift")
    value = payload["data"]
    if not isinstance(value, list):
        raise TickFlowDiscoveryError("schema_drift")
    rows: list[dict[str, Any]] = []
    for row in value:
        if not isinstance(row, dict):
            raise TickFlowDiscoveryError("schema_drift")
        rows.append(dict(row))
    if not rows and not allow_empty:
        raise TickFlowDiscoveryError("missing_endpoint")
    return tuple(rows)


def _validate_rows(
    rows: tuple[dict[str, Any], ...], trade_date: date, *, date_required: bool
) -> None:
    seen: set[tuple[Any, Any]] = set()
    for row in rows:
        row_date = row.get("date") or row.get("trade_date")
        if date_required and str(row_date) != trade_date.isoformat():
            raise TickFlowDiscoveryError("wrong_date")
        key = (row.get("symbol") or row.get("code"), row_date)
        if key in seen:
            raise TickFlowDiscoveryError("duplicate_row")
        seen.add(key)
        for field in ("open", "high", "low", "close"):
            if field in row:
                try:
                    if not isfinite(float(row[field])):
                        raise TickFlowDiscoveryError("non_finite_value")
                except (TypeError, ValueError):
                    raise TickFlowDiscoveryError("numeric_value_invalid") from None


_KLINE_CORE_COLUMNS = frozenset({"timestamp", "open", "high", "low", "close", "volume", "amount"})
_KLINE_OPTIONAL_COLUMNS = frozenset({"open_interest", "prev_close", "settlement_price"})
_KLINE_COLUMNS = _KLINE_CORE_COLUMNS | _KLINE_OPTIONAL_COLUMNS


def _official_data(payload: Any) -> Any:
    if not isinstance(payload, dict) or set(payload) != {"data"}:
        raise TickFlowDiscoveryError("schema_drift")
    return payload["data"]


def _official_kline_rows(data: Any, trade_date: date) -> tuple[dict[str, Any], ...]:
    if not isinstance(data, dict) or not data:
        raise TickFlowDiscoveryError("empty_endpoint")
    start_ms, end_ms = _trade_bounds(trade_date)
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for provider_symbol, compact in data.items():
        canonical = tickflow_to_canonical_symbol(provider_symbol)
        if (
            not isinstance(compact, dict)
            or not _KLINE_CORE_COLUMNS <= set(compact)
            or set(compact) - _KLINE_COLUMNS
        ):
            raise TickFlowDiscoveryError("schema_drift")
        lengths = {len(value) for value in compact.values() if isinstance(value, list)}
        if len(lengths) != 1 or any(not isinstance(value, list) for value in compact.values()):
            raise TickFlowDiscoveryError("column_length_mismatch")
        for index in range(next(iter(lengths))):
            timestamp = compact["timestamp"][index]
            if type(timestamp) is not int or not start_ms <= timestamp <= end_ms:
                raise TickFlowDiscoveryError("wrong_date")
            identity = (provider_symbol, timestamp)
            if identity in seen:
                raise TickFlowDiscoveryError("duplicate_row")
            seen.add(identity)
            row_fields = _KLINE_CORE_COLUMNS | (_KLINE_OPTIONAL_COLUMNS & set(compact))
            row = {field: compact[field][index] for field in row_fields}
            for field in ("open", "high", "low", "close"):
                if (
                    type(row[field]) not in (int, float)
                    or not isfinite(float(row[field]))
                    or row[field] <= 0
                ):
                    raise TickFlowDiscoveryError("numeric_value_invalid")
            volume = row["volume"]
            if type(volume) is not int or not 0 <= volume <= _INT64_MAX:
                raise TickFlowDiscoveryError("numeric_value_invalid")
            amount = row["amount"]
            if type(amount) not in (int, float) or not isfinite(float(amount)) or amount < 0:
                raise TickFlowDiscoveryError("numeric_value_invalid")
            for field in _KLINE_OPTIONAL_COLUMNS & set(compact):
                optional = compact[field][index]
                if (
                    type(optional) not in (int, float)
                    or not isfinite(float(optional))
                    or optional < 0
                ):
                    raise TickFlowDiscoveryError("numeric_value_invalid")
            rows.append({"provider_symbol": provider_symbol, "symbol": canonical, **row})
    if not rows:
        raise TickFlowDiscoveryError("empty_endpoint")
    return tuple(rows)


def _official_factor_rows(data: Any, trade_date: date) -> tuple[dict[str, Any], ...]:
    if not isinstance(data, dict):
        raise TickFlowDiscoveryError("empty_endpoint")
    start_ms, end_ms = _trade_bounds(trade_date)
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for provider_symbol, entries in data.items():
        canonical = tickflow_to_canonical_symbol(provider_symbol)
        if not isinstance(entries, list):
            raise TickFlowDiscoveryError("empty_endpoint")
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != {"timestamp", "ex_factor"}:
                raise TickFlowDiscoveryError("schema_drift")
            timestamp = entry["timestamp"]
            factor = entry["ex_factor"]
            if type(timestamp) is not int or not start_ms <= timestamp <= end_ms:
                raise TickFlowDiscoveryError("wrong_date")
            identity = (provider_symbol, timestamp)
            if identity in seen:
                raise TickFlowDiscoveryError("duplicate_row")
            seen.add(identity)
            if type(factor) not in (int, float) or not isfinite(float(factor)) or factor <= 0:
                raise TickFlowDiscoveryError("numeric_value_invalid")
            rows.append({"provider_symbol": provider_symbol, "symbol": canonical, **entry})
    return tuple(rows)


def _official_universe(data: Any, *, expected_id: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(data, dict):
        raise TickFlowDiscoveryError("schema_drift")
    required = {"id", "name", "region", "category", "symbol_count", "symbols"}
    allowed = required | {"description"}
    if set(data) - allowed or not required <= set(data):
        raise TickFlowDiscoveryError("schema_drift")
    if (
        data["id"] != expected_id
        or any(
            type(data[field]) is not str or not data[field]
            for field in ("id", "name", "region", "category")
        )
        or type(data["symbol_count"]) is not int
        or data["symbol_count"] < 0
        or not isinstance(data["symbols"], list)
        or (
            "description" in data
            and data["description"] is not None
            and type(data["description"]) is not str
        )
    ):
        raise TickFlowDiscoveryError("schema_drift")
    symbols = tuple(data["symbols"])
    if expected_id == "CN_Index" and not symbols:
        raise TickFlowDiscoveryError("empty_endpoint")
    if any(type(symbol) is not str for symbol in symbols):
        raise TickFlowDiscoveryError("symbol_schema")
    if data["symbol_count"] != len(symbols) or len(set(symbols)) != len(symbols):
        raise TickFlowDiscoveryError("symbol_set_invalid")
    if any(_PROVIDER_SYMBOL.fullmatch(symbol) is None for symbol in symbols):
        raise TickFlowDiscoveryError("symbol_schema")
    return tuple(
        {"provider_symbol": symbol, "symbol": tickflow_to_canonical_symbol(symbol)}
        for symbol in symbols
    )


def _official_free_exchanges(data: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(data, list) or not data:
        raise TickFlowFreeDiscoveryError("empty_endpoint")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in data:
        if not isinstance(row, dict) or set(row) != {"exchange", "region", "count"}:
            raise TickFlowFreeDiscoveryError("schema_drift")
        exchange = row["exchange"]
        region = row["region"]
        count = row["count"]
        if (
            type(exchange) is not str
            or not exchange
            or type(region) is not str
            or not region
            or type(count) is not int
            or count < 0
            or exchange in seen
        ):
            raise TickFlowFreeDiscoveryError("schema_drift")
        seen.add(exchange)
        rows.append(dict(row))
    if not {"SH", "SZ"} <= seen or any(
        row["region"] != "CN" for row in rows if row["exchange"] in {"SH", "SZ"}
    ):
        raise TickFlowFreeDiscoveryError("symbol_set_invalid")
    return tuple(rows)


def _official_free_instruments(data: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(data, list) or not data:
        raise TickFlowFreeDiscoveryError("empty_endpoint")
    required = {"symbol", "code", "exchange", "region"}
    allowed = required | {"name", "type", "ext"}
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in data:
        if not isinstance(row, dict) or set(row) - allowed or not required <= set(row):
            raise TickFlowFreeDiscoveryError("schema_drift")
        symbol = row["symbol"]
        if (
            type(symbol) is not str
            or _PROVIDER_SYMBOL.fullmatch(symbol) is None
            or row["code"] != symbol[:6]
            or row["exchange"] != symbol[-2:]
            or row["region"] != "CN"
            or symbol in seen
            or (
                "name" in row
                and row["name"] is not None
                and (type(row["name"]) is not str or not row["name"])
            )
            or (
                "type" in row
                and row["type"] is not None
                and (type(row["type"]) is not str or not row["type"])
            )
            or ("ext" in row and row["ext"] is not None and not isinstance(row["ext"], dict))
        ):
            raise TickFlowFreeDiscoveryError("schema_drift")
        seen.add(symbol)
        rows.append(dict(row))
    if seen != set(TICKFLOW_FREE_PROVIDER_SYMBOLS):
        raise TickFlowFreeDiscoveryError("symbol_set_invalid")
    return tuple(rows)


def _validate_free_daily_rows(rows: tuple[dict[str, Any], ...]) -> None:
    if len(rows) != len(TICKFLOW_FREE_PROVIDER_SYMBOLS):
        raise TickFlowFreeDiscoveryError("symbol_set_invalid")
    seen = {row["provider_symbol"] for row in rows}
    if seen != set(TICKFLOW_FREE_PROVIDER_SYMBOLS):
        raise TickFlowFreeDiscoveryError("symbol_set_invalid")
    for row in rows:
        open_value = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        if high < max(open_value, low, close) or low > min(open_value, high, close):
            raise TickFlowFreeDiscoveryError("numeric_value_invalid")


class TickFlowFreeAdapter:
    """Credentialless, Daily-Bar-only Free discovery adapter."""

    PROVIDER_ID = "tickflow"
    PROVIDER_MODE = "FREE_DAILY_DISCOVERY"
    ADAPTER_HASH = FREE_ADAPTER_HASH
    ENDPOINT_CONTRACT_HASH = FREE_ENDPOINT_CONTRACT_HASH
    SOURCE_SCHEMA_HASH = FREE_SOURCE_SCHEMA_HASH
    CONTRACT = TickFlowFreeContract()
    ENDPOINTS = CONTRACT.endpoints

    def plan(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TickFlowFreePlan:
        if symbols:
            raise TickFlowFreeDiscoveryError("fixed_sample_required")
        requests = tuple(
            ShadowLogicalRequest(
                provider_id=self.PROVIDER_ID,
                ordinal=ordinal,
                request_id=f"tickflow-free-{endpoint}-{trade_date.isoformat()}-{ordinal}",
                endpoint=endpoint,
                endpoint_class=endpoint,
                role="daily" if endpoint == "historical_daily_1d" else endpoint,
                trade_date=trade_date,
                symbol_or_index_shard="fixed-five-a-share-sample",
                schema_contract_hash=self.SOURCE_SCHEMA_HASH,
                unit_contract_hash=FREE_UNIT_CONTRACT_HASH,
            )
            for ordinal, endpoint in enumerate(self.ENDPOINTS)
        )
        return TickFlowFreePlan(trade_date=trade_date, requests=requests)

    def request_method(self, endpoint: str) -> str:
        try:
            return self.CONTRACT.endpoint_methods[self.ENDPOINTS.index(endpoint)]
        except (ValueError, IndexError):
            raise TickFlowFreeDiscoveryError("endpoint_unavailable") from None

    def request_endpoint(self, endpoint: str) -> str:
        try:
            return self.CONTRACT.endpoint_paths[self.ENDPOINTS.index(endpoint)]
        except (ValueError, IndexError):
            raise TickFlowFreeDiscoveryError("endpoint_unavailable") from None

    def request_params(self, endpoint: str, trade_date: date) -> dict[str, Any]:
        if endpoint in {"connectivity", "instrument_metadata", "universe_metadata"}:
            return {}
        if endpoint == "historical_daily_1d":
            start_ms, end_ms = _trade_bounds(trade_date)
            return {
                "symbols": ",".join(TICKFLOW_FREE_PROVIDER_SYMBOLS),
                "period": "1d",
                "start_time": start_ms,
                "end_time": end_ms,
                "adjust": "none",
            }
        raise TickFlowFreeDiscoveryError("endpoint_unavailable")

    def request_json_body(self, endpoint: str) -> dict[str, Any] | None:
        if endpoint == "instrument_metadata":
            return {"symbols": list(TICKFLOW_FREE_PROVIDER_SYMBOLS)}
        if endpoint in {"connectivity", "universe_metadata", "historical_daily_1d"}:
            return None
        raise TickFlowFreeDiscoveryError("endpoint_unavailable")

    def parse(self, trade_date: date, payloads: dict[str, Any]) -> TickFlowFreeSourceBatch:
        if set(payloads) != set(self.ENDPOINTS):
            raise TickFlowFreeDiscoveryError("missing_endpoint")
        exchanges = _official_free_exchanges(_official_data(payloads["connectivity"]))
        instruments = _official_free_instruments(_official_data(payloads["instrument_metadata"]))
        universe = _official_universe(
            _official_data(payloads["universe_metadata"]), expected_id="CN_Equity_A"
        )
        universe_symbols = {row["provider_symbol"] for row in universe}
        if not set(TICKFLOW_FREE_PROVIDER_SYMBOLS) <= universe_symbols:
            raise TickFlowFreeDiscoveryError("symbol_set_invalid")
        daily = _official_kline_rows(_official_data(payloads["historical_daily_1d"]), trade_date)
        _validate_free_daily_rows(daily)
        return TickFlowFreeSourceBatch(
            trade_date=trade_date,
            exchanges=exchanges,
            instruments=instruments,
            universe=universe,
            daily=daily,
        )


class TickFlowFreeCanaryRunner:
    """Read-only Free discovery runner; it has no persistence dependency."""

    def __init__(
        self,
        *,
        settings: Any,
        layout: Any,
        registry: Any,
        client_factory: Any,
    ) -> None:
        self.settings = settings
        self.layout = layout
        self.registry = registry
        self.client_factory = client_factory

    @staticmethod
    def _failure_report(
        trade_date: date,
        *,
        request_count: int,
        failure_class: str,
        endpoint: str | None = None,
    ) -> TickFlowFreeCanaryReport:
        return TickFlowFreeCanaryReport(
            status="unavailable",
            outcome="unavailable",
            trade_date=trade_date,
            request_count=request_count,
            failure_class=failure_class,
            endpoint=endpoint,
        )

    @classmethod
    def _run_free(
        cls,
        *,
        transport_factory: Any,
        trade_date: date,
        sdk_class: type[Any] | None = None,
        sdk_version: str | None = None,
        cache_dir: Path = Path("/dev/null/stock-eva-tickflow-free"),
    ) -> TickFlowFreeCanaryReport:
        from .http import (
            BoundedHttpClient,
            HttpPolicy,
            ProviderHttpError,
            safe_public_endpoint,
            safe_public_failure_class,
        )

        client: BoundedHttpClient | None = None
        try:
            initialize_tickflow_free_sdk(
                sdk_class=sdk_class,
                sdk_version=sdk_version,
                cache_dir=cache_dir,
            )
            client = BoundedHttpClient(
                transport_factory(),
                policy=HttpPolicy(
                    connect_timeout_seconds=5,
                    read_timeout_seconds=30,
                    write_timeout_seconds=5,
                    pool_timeout_seconds=5,
                    max_attempts=1,
                    max_requests=4,
                    max_response_bytes=8 * 1024 * 1024,
                ),
                allowed_server=TICKFLOW_FREE_SERVER,
                owns_client=True,
            )
            adapter = TickFlowFreeAdapter()
            payloads: dict[str, Any] = {}
            for endpoint in adapter.ENDPOINTS:
                payloads[endpoint] = client.request_json(
                    adapter.request_endpoint(endpoint),
                    method=adapter.request_method(endpoint),
                    params=adapter.request_params(endpoint, trade_date),
                    json_body=adapter.request_json_body(endpoint),
                    headers={},
                )
            adapter.parse(trade_date, payloads)
            return TickFlowFreeCanaryReport(
                status="discovered",
                outcome="discovery",
                trade_date=trade_date,
                request_count=client.request_count,
                capabilities=TickFlowFreeCapabilities.discovered(),
            )
        except (ProviderHttpError, TickFlowDiscoveryError) as exc:
            failure_class = safe_public_failure_class(getattr(exc, "failure_class", None))
            endpoint = safe_public_endpoint(getattr(exc, "endpoint", None))
            return cls._failure_report(
                trade_date,
                request_count=client.request_count if client is not None else 0,
                failure_class=failure_class or "discovery_unavailable",
                endpoint=endpoint,
            )
        except Exception:
            return cls._failure_report(
                trade_date,
                request_count=client.request_count if client is not None else 0,
                failure_class="discovery_unavailable",
            )
        finally:
            if client is not None:
                client.close()

    @classmethod
    def _offline_for_tests(
        cls,
        *,
        transport: Any,
        trade_date: date,
        sdk_class: type[Any] | None = None,
        sdk_version: str | None = None,
        cache_dir: Path = Path("/dev/null/stock-eva-tickflow-free"),
    ) -> TickFlowFreeCanaryReport:
        return cls._run_free(
            transport_factory=lambda: transport,
            trade_date=trade_date,
            sdk_class=sdk_class,
            sdk_version=sdk_version,
            cache_dir=cache_dir,
        )

    def execute(
        self,
        trade_date: date,
        *,
        external_authorization_id: str,
        acknowledge_provider_requests: bool,
    ) -> TickFlowFreeCanaryReport:
        from .http import _read_canary_descriptor, _valid_authorization_id

        if not (
            type(getattr(self.settings, "provider_shadow_enabled", False)) is bool
            and self.settings.provider_shadow_enabled is True
            and type(getattr(self.settings, "provider_shadow_execute_enabled", False)) is bool
            and self.settings.provider_shadow_execute_enabled is True
        ):
            raise PermissionError("provider shadow execution is disabled")
        if (
            type(acknowledge_provider_requests) is not bool
            or not acknowledge_provider_requests
            or not _valid_authorization_id(external_authorization_id)
        ):
            raise PermissionError("provider request acknowledgement is required")
        registry_path = getattr(self.registry, "path", None)
        canonical = tuple(
            root
            for root in (
                getattr(self.settings, "local_market_dataset_root", None),
                getattr(self.settings, "nas_market_dataset_root", None),
                getattr(self.settings, "local_control_dir", None),
                Path(registry_path).parent if registry_path is not None else None,
            )
            if root is not None
        )
        self.layout.validate_provider_shadow_root(canonical_roots=canonical)
        record = self.registry.read_status("tickflow")
        if str(record.admission_state.value).lower() != "canary":
            raise PermissionError("provider is not admitted for canary")
        # Terms/admission is read-only.  The existing authenticated descriptor remains the
        # provider-level reviewed record; the Free request graph is separately frozen above.
        _read_canary_descriptor(
            "tickflow",
            self.registry,
            adapter_hash=TickFlowAdapter.ADAPTER_HASH,
            endpoint_contract_hash=TickFlowAdapter.ENDPOINT_CONTRACT_HASH,
            source_schema_hash=TickFlowAdapter.SOURCE_SCHEMA_HASH,
        )
        return self._run_free(
            transport_factory=self.client_factory,
            trade_date=trade_date,
        )


class TickFlowAdapter:
    PROVIDER_ID = "tickflow"
    ADAPTER_HASH = ADAPTER_HASH
    ENDPOINT_CONTRACT_HASH = ENDPOINT_CONTRACT_HASH
    SOURCE_SCHEMA_HASH = SOURCE_SCHEMA_HASH
    ENDPOINTS = TickFlowContract().endpoints
    CONTRACT = TickFlowContract()

    def __init__(self, *, plan_only: bool = True):
        self.plan_only = plan_only

    def plan(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TickFlowPlan:
        requests = tuple(
            ShadowLogicalRequest(
                provider_id=self.PROVIDER_ID,
                ordinal=ordinal,
                request_id=f"tickflow-{endpoint}-{trade_date.isoformat()}-{ordinal}",
                endpoint=endpoint,
                endpoint_class=endpoint,
                role="daily" if endpoint == "daily_batch" else endpoint,
                trade_date=trade_date,
                symbol_or_index_shard=("symbols" if symbols else "universe"),
                schema_contract_hash=self.SOURCE_SCHEMA_HASH,
                unit_contract_hash=UNIT_CONTRACT_HASH,
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

    def request_endpoint(self, endpoint: str) -> str:
        try:
            return self.CONTRACT.endpoint_paths[self.ENDPOINTS.index(endpoint)]
        except (ValueError, IndexError):
            raise TickFlowDiscoveryError("endpoint_unavailable") from None

    def request_params(
        self, endpoint: str, trade_date: date, symbols: tuple[str, ...]
    ) -> dict[str, Any]:
        start_ms, end_ms = _trade_bounds(trade_date)
        try:
            joined = ",".join(canonical_to_tickflow_symbol(symbol) for symbol in symbols)
        except TickFlowDiscoveryError:
            raise TickFlowDiscoveryError("symbol_schema") from None
        if endpoint == "daily_batch":
            return {
                "symbols": joined,
                "period": "1d",
                "start_time": start_ms,
                "end_time": end_ms,
                "adjust": "none",
            }
        if endpoint == "ex_factors":
            return {"symbols": joined, "start_time": start_ms, "end_time": end_ms}
        if endpoint in {"universe", "indexes"}:
            return {}
        raise TickFlowDiscoveryError("endpoint_unavailable")

    def parse(
        self,
        trade_date: date,
        payloads: dict[str, Any],
        *,
        request_count: int = 0,
        symbols: tuple[str, ...] = (),
    ) -> TickFlowSourceBatch:
        if set(self.ENDPOINTS).issubset(payloads) and all(
            isinstance(payloads[item], dict) and isinstance(payloads[item].get("data"), dict)
            for item in self.ENDPOINTS
        ):
            official_daily = _official_kline_rows(
                _official_data(payloads["daily_batch"]), trade_date
            )
            official_factors = _official_factor_rows(
                _official_data(payloads["ex_factors"]), trade_date
            )
            official_universe = _official_universe(
                _official_data(payloads["universe"]), expected_id="CN_Equity_A"
            )
            official_indexes = _official_universe(
                _official_data(payloads["indexes"]), expected_id="CN_Index"
            )
            if symbols:
                expected = {canonical_to_tickflow_symbol(symbol) for symbol in symbols}
                daily_symbols = {row["provider_symbol"] for row in official_daily}
                # Coverage is defined by the provider map keys, not by emitted rows:
                # an empty list is a valid no-event result for that symbol.
                factor_data = _official_data(payloads["ex_factors"])
                factor_symbols = set(factor_data)
                equity_symbols = {row["provider_symbol"] for row in official_universe}
                if (
                    daily_symbols != expected
                    or factor_symbols != expected
                    or not expected <= equity_symbols
                ):
                    raise TickFlowDiscoveryError("symbol_set_invalid")
            return TickFlowSourceBatch(
                trade_date=trade_date,
                daily=official_daily,
                factors=official_factors,
                universe=official_universe,
                indexes=official_indexes,
                request_count=request_count,
                units_status="unknown",
                factor_status=(
                    "no_event"
                    if symbols
                    and not official_factors
                    and set(_official_data(payloads["ex_factors"]))
                    == {canonical_to_tickflow_symbol(symbol) for symbol in symbols}
                    else "unavailable"
                ),
            )
        # Preserve the Task11 read-only parser shape for callers that only replayed the
        # historical three-endpoint fixture. The execute contract below is always four calls.
        if set(payloads) >= {"daily", "universe", "indexes"} and "daily_batch" not in payloads:
            daily = _rows(payloads["daily"], allow_empty=True)
            universe = _rows(payloads["universe"], allow_empty=True)
            indexes = _rows(payloads["indexes"], allow_empty=True)
            _validate_rows(daily, trade_date, date_required=True)
            _validate_rows(universe, trade_date, date_required=False)
            _validate_rows(indexes, trade_date, date_required=False)
            return TickFlowSourceBatch(
                trade_date=trade_date,
                daily=daily,
                universe=universe,
                indexes=indexes,
                request_count=request_count,
            )
        if set(payloads) == {"daily_batch", "ex_factors", "universe"}:
            # Compatibility-only replay of the Task11 three-call fixture.
            daily = _rows(payloads["daily_batch"], allow_empty=True)
            factors = _rows(payloads["ex_factors"], allow_empty=True)
            universe = _rows(payloads["universe"], allow_empty=True)
            return TickFlowSourceBatch(
                trade_date=trade_date,
                daily=daily,
                factors=factors,
                universe=universe,
                indexes=factors,
                request_count=request_count,
            )
        aliases = {"daily": "daily_batch", "adjusted": "ex_factors"}
        normalized = {aliases.get(key, key): value for key, value in payloads.items()}
        if set(self.ENDPOINTS) - set(normalized):
            raise TickFlowDiscoveryError("missing_endpoint")
        parsed = {endpoint: _rows(normalized[endpoint]) for endpoint in self.ENDPOINTS}
        _validate_rows(parsed["daily_batch"], trade_date, date_required=True)
        _validate_rows(parsed["ex_factors"], trade_date, date_required=True)
        _validate_rows(parsed["universe"], trade_date, date_required=False)
        _validate_rows(parsed["indexes"], trade_date, date_required=False)
        unit_values = [
            normalized[item].get("units")
            for item in self.ENDPOINTS
            if isinstance(normalized[item], dict)
        ]
        declared = [value for value in unit_values if isinstance(value, dict) and value]
        units = (
            dict(declared[0])
            if declared and all(value == declared[0] for value in declared)
            else None
        )
        return TickFlowSourceBatch(
            trade_date=trade_date,
            daily=parsed["daily_batch"],
            factors=parsed["ex_factors"],
            universe=parsed["universe"],
            indexes=parsed["indexes"],
            request_count=request_count,
            units_status="declared" if units is not None else "unknown",
            units=units,
        )

    def execute(
        self, trade_date: date, *, symbols: tuple[str, ...] = (), session: CanaryExecutor
    ) -> TickFlowSourceBatch:
        if type(session) is not _AuthorizedCanarySession:
            raise PermissionError("authorized canary session required")
        if not symbols:
            raise PermissionError("canary symbols are required")
        _validate_symbols(tuple(symbols), for_execute=True)
        return session.execute(self, trade_date, symbols=symbols)

    def fetch(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TickFlowPlan:
        plan = self.plan(trade_date, symbols=symbols)
        return plan.model_copy(update={"requests": plan.requests[:3], "request_count": 3})

    def run(
        self, trade_date: date, *, symbols: tuple[str, ...] = ()
    ) -> TickFlowSourceBatch | TickFlowPlan:
        return self.plan(trade_date, symbols=symbols)


class TickFlowCanaryRunner:
    """Canary runner; production execution is separately gated and fixed-host only."""

    @classmethod
    def _offline_for_tests(
        cls,
        *,
        transport: Any,
        trade_date: date,
        symbols: tuple[str, ...],
        environ: Mapping[str, str],
        evidence_store: Any | None = None,
        evidence_id: str = "task14-tickflow-canary",
    ) -> TickFlowCanaryReport:
        _validate_symbols(tuple(symbols), for_execute=True)
        token = environ.get("STOCK_EVA_TICKFLOW_TOKEN", "")
        if not token:
            raise PermissionError("provider credential unavailable")
        adapter = TickFlowAdapter(plan_only=False)
        from .http import BoundedHttpClient

        client = BoundedHttpClient(
            transport,
            policy=HttpPolicy(
                connect_timeout_seconds=5,
                read_timeout_seconds=30,
                write_timeout_seconds=5,
                pool_timeout_seconds=5,
                max_attempts=1,
                max_requests=4,
                max_response_bytes=8 * 1024 * 1024,
            ),
            allowed_server=TICKFLOW_SERVER,
        )
        payloads: dict[str, Any] = {}
        for endpoint in adapter.ENDPOINTS:
            payloads[endpoint] = client.request_json(
                adapter.request_endpoint(endpoint),
                params=adapter.request_params(endpoint, trade_date, tuple(symbols)),
                headers={"x-api-key": token},
            )
        # Parse only after all four complete responses. Partial responses can never be
        # published, while call order and the four-request bound stay deterministic.
        parsed = adapter.parse(
            trade_date,
            payloads,
            request_count=client.request_count,
            symbols=tuple(symbols),
        )
        published_id = None
        if evidence_store is not None:
            raw_by_ordinal = {
                ordinal: client.raw_content_by_endpoint[adapter.request_endpoint(endpoint)]
                for ordinal, endpoint in enumerate(adapter.ENDPOINTS)
            }
            rows_by_ordinal = {
                0: parsed.daily,
                1: parsed.factors,
                2: parsed.universe,
                3: parsed.indexes,
            }
            evidence_store.publish_raw_responses(
                plan=ShadowLogicalRequestPlan(
                    provider_id="tickflow",
                    window_id="task14-canary-window",
                    requests=adapter.plan(trade_date, symbols=tuple(symbols)).requests,
                ),
                response_bytes_by_ordinal=raw_by_ordinal,
                rows_by_ordinal=rows_by_ordinal,
                evidence_id=evidence_id,
            )
            published_id = evidence_id
        return TickFlowCanaryReport(
            status="discovered",
            outcome="discovery",
            trade_date=trade_date,
            request_count=client.request_count,
            endpoints=adapter.ENDPOINTS,
            units_status=parsed.units_status,
            writes_evidence=published_id is not None,
            evidence_id=published_id,
        )

    def __init__(self, *, settings: Any, layout: Any, registry: Any, client_factory: Any):
        self.settings = settings
        self.layout = layout
        self.registry = registry
        self.client_factory = client_factory

    def execute(
        self,
        trade_date: date,
        *,
        symbols: tuple[str, ...],
        external_authorization_id: str,
        acknowledge_provider_requests: bool,
    ) -> TickFlowCanaryReport:
        """Execute only after every pre-client gate has passed.

        The factory is deliberately mandatory and injected. Production code therefore cannot
        accidentally discover a URL or create a default network client in this runner.
        """
        from .http import (
            _read_canary_descriptor,
            _valid_authorization_id,
            build_authorized_canary_session,
        )

        if not (
            type(getattr(self.settings, "provider_shadow_enabled", False)) is bool
            and self.settings.provider_shadow_enabled is True
            and type(getattr(self.settings, "provider_shadow_execute_enabled", False)) is bool
            and self.settings.provider_shadow_execute_enabled is True
        ):
            raise PermissionError("provider shadow execution is disabled")
        if (
            type(acknowledge_provider_requests) is not bool
            or not acknowledge_provider_requests
            or not _valid_authorization_id(external_authorization_id)
        ):
            raise PermissionError("provider request acknowledgement is required")
        registry_path = getattr(self.registry, "path", None)
        canonical = tuple(
            root
            for root in (
                getattr(self.settings, "local_market_dataset_root", None),
                getattr(self.settings, "nas_market_dataset_root", None),
                getattr(self.settings, "local_control_dir", None),
                Path(registry_path).parent if registry_path is not None else None,
            )
            if root is not None
        )
        self.layout.validate_provider_shadow_root(canonical_roots=canonical)
        record = self.registry.read_status("tickflow")
        if str(record.admission_state.value).lower() != "canary":
            raise PermissionError("provider is not admitted for canary")
        _validate_symbols(tuple(symbols), for_execute=True)
        # Re-open and validate the immutable descriptor and reviewed TermsEvidence before the
        # closed credential lookup. This ordering is intentional and tested with a getenv spy.
        _read_canary_descriptor(
            "tickflow",
            self.registry,
            adapter_hash=TickFlowAdapter.ADAPTER_HASH,
            endpoint_contract_hash=TickFlowAdapter.ENDPOINT_CONTRACT_HASH,
            source_schema_hash=TickFlowAdapter.SOURCE_SCHEMA_HASH,
        )
        # The closed environment is intentionally read only after all filesystem, registry and
        # request gates. No alternate env name or token argument is accepted here.
        import os

        token = os.environ.get("STOCK_EVA_TICKFLOW_TOKEN", "")
        if not token:
            raise PermissionError("provider credential unavailable")
        session = build_authorized_canary_session(
            "tickflow",
            self.registry,
            external_authorization_id=external_authorization_id,
            environ={"STOCK_EVA_TICKFLOW_TOKEN": token},
            client_factory=self.client_factory,
        )
        result = TickFlowAdapter().execute(trade_date, symbols=tuple(symbols), session=session)
        from ..shadow_evidence import ShadowEvidenceStore

        evidence_id = f"task14-tickflow-{trade_date.isoformat()}"
        raw_by_ordinal = {
            ordinal: session.last_raw_content_by_endpoint[
                TickFlowAdapter().request_endpoint(endpoint)
            ]
            for ordinal, endpoint in enumerate(TickFlowAdapter.ENDPOINTS)
        }
        rows_by_ordinal = {
            0: result.daily,
            1: result.factors,
            2: result.universe,
            3: result.indexes,
        }
        evidence_plan = ShadowLogicalRequestPlan(
            provider_id="tickflow",
            window_id=f"task14-{trade_date.isoformat()}",
            requests=TickFlowAdapter().plan(trade_date, symbols=tuple(symbols)).requests,
        )
        ShadowEvidenceStore(self.layout.provider_shadow_root).publish_raw_responses(
            plan=evidence_plan,
            response_bytes_by_ordinal=raw_by_ordinal,
            rows_by_ordinal=rows_by_ordinal,
            evidence_id=evidence_id,
        )
        return TickFlowCanaryReport(
            status="discovered",
            outcome="discovery",
            trade_date=trade_date,
            request_count=result.request_count,
            endpoints=TickFlowAdapter.ENDPOINTS,
            units_status=result.units_status,
            writes_evidence=True,
            evidence_id=evidence_id,
        )


TickFlowProvider = TickFlowAdapter
TickflowAdapter = TickFlowAdapter
