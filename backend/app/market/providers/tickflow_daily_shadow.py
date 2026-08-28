"""Credentialless, one-attempt, whole-session TickFlow Free Daily fetch contract."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from math import isfinite
from time import monotonic as _monotonic
from typing import Any

from backend.app.market.daily_shadow_models import (
    DailyCanonicalSnapshot,
    DailyShadowFetchObservation,
    DailyShadowFetchResult,
    DailyShadowPlan,
    DailyShadowShard,
    DailyShadowSourceRow,
    canonical_to_tickflow_daily_symbol,
    domain_sha256,
)

from .http import BoundedHttpClient, HttpPolicy, ProviderHttpError
from .tickflow import TICKFLOW_FREE_SERVER, initialize_tickflow_free_sdk

TICKFLOW_FREE_DAILY_ENDPOINT = "/v1/klines/batch"
_MAX_SHARD_SYMBOLS = 100
_MAX_REQUESTS = 40
_CORE_COLUMNS = frozenset({"timestamp", "open", "high", "low", "close", "volume", "amount"})
_OPTIONAL_COLUMNS = frozenset({"open_interest", "prev_close", "settlement_price"})
_ALLOWED_FAILURES = frozenset(
    {
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
        "symbol_set_invalid",
        "schema_drift",
        "wrong_date",
        "column_length_mismatch",
        "numeric_value_invalid",
        "sdk_initialization_failed",
    }
)


def _contract_hash(label: str) -> str:
    return hashlib.sha256(f"stock-eva/r2f3/free-daily-shadow/v1\n{label}".encode()).hexdigest()


DAILY_SHADOW_ADAPTER_HASH = _contract_hash("adapter-sequential-one-attempt")
DAILY_SHADOW_ENDPOINT_CONTRACT_HASH = _contract_hash(
    "GET:/v1/klines/batch:period=1d:adjust=none:max-shard=100"
)
DAILY_SHADOW_SOURCE_SCHEMA_HASH = _contract_hash(
    "compact-kline:timestamp,open,high,low,close,volume,amount:optional-reviewed"
)
DAILY_SHADOW_UNIT_STATE_HASH = _contract_hash("volume=UNKNOWN:amount=UNKNOWN")
DAILY_SHADOW_MAPPING_HASH = _contract_hash("sh.sz-six-digit-reversible-v1")


class TickFlowFreeDailyShadowError(ValueError):
    """Sanitized parser failure; source values never cross this boundary."""

    def __init__(self, failure_class: str):
        self.failure_class = failure_class if failure_class in _ALLOWED_FAILURES else "schema_drift"
        super().__init__("TickFlow Free Daily shadow response unavailable")


def _trade_bounds(trade_date: date) -> tuple[int, int]:
    start = datetime.combine(trade_date, time.min, tzinfo=UTC)
    end = start + timedelta(days=1) - timedelta(milliseconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def _decimal(value: Any, *, positive: bool) -> Decimal:
    if isinstance(value, bool) or type(value) not in {int, float, Decimal}:
        raise TickFlowFreeDailyShadowError("numeric_value_invalid")
    if isinstance(value, float) and not isfinite(value):
        raise TickFlowFreeDailyShadowError("numeric_value_invalid")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise TickFlowFreeDailyShadowError("numeric_value_invalid") from None
    if not parsed.is_finite() or (positive and parsed <= 0) or (not positive and parsed < 0):
        raise TickFlowFreeDailyShadowError("numeric_value_invalid")
    return parsed


def _parse_shard(
    payload: Any, *, shard: DailyShadowShard, trade_date: date
) -> tuple[DailyShadowSourceRow, ...]:
    if not isinstance(payload, dict) or set(payload) != {"data"}:
        raise TickFlowFreeDailyShadowError("schema_drift")
    data = payload["data"]
    if not isinstance(data, dict) or set(data) != set(shard.provider_symbols):
        raise TickFlowFreeDailyShadowError("symbol_set_invalid")
    start_ms, end_ms = _trade_bounds(trade_date)
    rows: list[DailyShadowSourceRow] = []
    for canonical, provider in zip(shard.canonical_symbols, shard.provider_symbols, strict=True):
        compact = data[provider]
        if (
            not isinstance(compact, dict)
            or not _CORE_COLUMNS <= set(compact)
            or set(compact) - (_CORE_COLUMNS | _OPTIONAL_COLUMNS)
            or any(not isinstance(value, list) for value in compact.values())
        ):
            raise TickFlowFreeDailyShadowError("schema_drift")
        if {len(value) for value in compact.values()} != {1}:
            raise TickFlowFreeDailyShadowError("column_length_mismatch")
        timestamp = compact["timestamp"][0]
        if type(timestamp) is not int or not start_ms <= timestamp <= end_ms:
            raise TickFlowFreeDailyShadowError("wrong_date")
        open_value = _decimal(compact["open"][0], positive=True)
        high = _decimal(compact["high"][0], positive=True)
        low = _decimal(compact["low"][0], positive=True)
        close = _decimal(compact["close"][0], positive=True)
        if high < max(open_value, high, low, close) or low > min(open_value, high, low, close):
            raise TickFlowFreeDailyShadowError("numeric_value_invalid")
        volume = compact["volume"][0]
        if type(volume) is not int or not 0 <= volume <= 2**63 - 1:
            raise TickFlowFreeDailyShadowError("numeric_value_invalid")
        _decimal(compact["amount"][0], positive=False)
        for field in _OPTIONAL_COLUMNS & set(compact):
            _decimal(compact[field][0], positive=False)
        try:
            row = DailyShadowSourceRow(
                trade_date=trade_date,
                timestamp=timestamp,
                provider_symbol=provider,
                symbol=canonical,
                open=compact["open"][0],
                high=compact["high"][0],
                low=compact["low"][0],
                close=compact["close"][0],
                volume=volume,
                amount=compact["amount"][0],
            )
        except Exception as exc:
            raise TickFlowFreeDailyShadowError("numeric_value_invalid") from exc
        rows.append(row)
    return tuple(rows)


class TickFlowFreeDailyShadowAdapter:
    PROVIDER = "tickflow"
    PROFILE = "TICKFLOW_FREE_DAILY_BAR_OHLC_V1"
    ADAPTER_HASH = DAILY_SHADOW_ADAPTER_HASH
    ENDPOINT_CONTRACT_HASH = DAILY_SHADOW_ENDPOINT_CONTRACT_HASH
    SOURCE_SCHEMA_HASH = DAILY_SHADOW_SOURCE_SCHEMA_HASH
    UNIT_STATE_HASH = DAILY_SHADOW_UNIT_STATE_HASH
    MAPPING_CONTRACT_HASH = DAILY_SHADOW_MAPPING_HASH

    def plan(self, snapshot: DailyCanonicalSnapshot) -> DailyShadowPlan:
        if type(snapshot) is not DailyCanonicalSnapshot:
            raise TypeError("Daily shadow plan requires a canonical snapshot")
        symbols = tuple(row.symbol for row in snapshot.rows)
        provider = tuple(canonical_to_tickflow_daily_symbol(symbol) for symbol in symbols)
        mapping_hash = domain_sha256(
            "stock-eva/r2f3/daily-symbol-mapping/v1", tuple(zip(symbols, provider, strict=True))
        )
        if (
            snapshot.eligible_symbol_count != len(symbols)
            or snapshot.canonical_universe_sha256
            != domain_sha256("stock-eva/r2f3/daily-canonical-universe/v1", symbols)
            or snapshot.symbol_mapping_sha256 != mapping_hash
        ):
            raise ValueError("Daily shadow canonical snapshot binding is invalid")
        shards: list[DailyShadowShard] = []
        for ordinal, start in enumerate(range(0, len(symbols), _MAX_SHARD_SYMBOLS)):
            canonical_shard = symbols[start : start + _MAX_SHARD_SYMBOLS]
            provider_shard = provider[start : start + _MAX_SHARD_SYMBOLS]
            shard_digest = domain_sha256("stock-eva/r2f3/daily-shard-id/v1", provider_shard)[:16]
            request_id = (
                f"tickflow-free-daily-{snapshot.trade_date.isoformat()}-"
                f"{ordinal:02d}-{shard_digest}"
            )
            shards.append(
                DailyShadowShard(
                    ordinal=ordinal,
                    request_id=request_id,
                    canonical_symbols=canonical_shard,
                    provider_symbols=provider_shard,
                )
            )
        if not shards or len(shards) > _MAX_REQUESTS:
            raise ValueError("Daily shadow request budget unavailable")
        return DailyShadowPlan(
            trade_date=snapshot.trade_date,
            canonical_snapshot_sha256=snapshot.snapshot_sha256,
            canonical_universe_sha256=snapshot.canonical_universe_sha256,
            canonical_exclusion_sha256=snapshot.canonical_exclusion_sha256,
            symbol_mapping_sha256=mapping_hash,
            shards=tuple(shards),
        )

    @staticmethod
    def request_params(plan: DailyShadowPlan, shard: DailyShadowShard) -> dict[str, Any]:
        start_ms, end_ms = _trade_bounds(plan.trade_date)
        return {
            "symbols": ",".join(shard.provider_symbols),
            "period": "1d",
            "start_time": start_ms,
            "end_time": end_ms,
            "adjust": "none",
        }

    @staticmethod
    def parse(
        payload: Any, *, plan: DailyShadowPlan, shard: DailyShadowShard
    ) -> tuple[DailyShadowSourceRow, ...]:
        return _parse_shard(payload, shard=shard, trade_date=plan.trade_date)


class TickFlowFreeDailyShadowFetcher:
    """Fetch one exact plan in memory; persistence belongs to the later worker."""

    @classmethod
    def _run(
        cls,
        *,
        transport_factory: Callable[[], Any],
        plan: DailyShadowPlan,
        sdk_initializer: Callable[[], Any],
        monotonic: Callable[[], float] = _monotonic,
    ) -> DailyShadowFetchResult:
        if type(plan) is not DailyShadowPlan:
            raise TypeError("Daily shadow plan is required")
        client: BoundedHttpClient | None = None
        observations: list[DailyShadowFetchObservation] = []
        rows: list[DailyShadowSourceRow] = []
        active_ordinal: int | None = None
        sdk_initialized = False
        try:
            sdk_initializer()
            sdk_initialized = True
            client = BoundedHttpClient(
                transport_factory(),
                policy=HttpPolicy(
                    connect_timeout_seconds=5,
                    read_timeout_seconds=30,
                    write_timeout_seconds=5,
                    pool_timeout_seconds=5,
                    max_attempts=1,
                    max_requests=plan.request_count,
                    max_response_bytes=8 * 1024 * 1024,
                    max_rows=100_000,
                    max_retry_after_seconds=0,
                ),
                allowed_server=TICKFLOW_FREE_SERVER,
                owns_client=True,
            )
            adapter = TickFlowFreeDailyShadowAdapter()
            for shard in plan.shards:
                active_ordinal = shard.ordinal
                started = monotonic()
                result = client.request(
                    TICKFLOW_FREE_DAILY_ENDPOINT,
                    method="GET",
                    params=adapter.request_params(plan, shard),
                    headers={},
                )
                response_bytes = len(client.raw_content_by_endpoint[TICKFLOW_FREE_DAILY_ENDPOINT])
                try:
                    parsed = adapter.parse(result.payload, plan=plan, shard=shard)
                except TickFlowFreeDailyShadowError as exc:
                    elapsed = max(0, int((monotonic() - started) * 1000))
                    observations.append(
                        DailyShadowFetchObservation(
                            ordinal=shard.ordinal,
                            outcome="FAILURE",
                            elapsed_ms=elapsed,
                            response_bytes=response_bytes,
                            expected_rows=len(shard.provider_symbols),
                            observed_rows=0,
                            failure_class=exc.failure_class,
                        )
                    )
                    return DailyShadowFetchResult(
                        status="unavailable",
                        trade_date=plan.trade_date,
                        request_plan_sha256=plan.request_plan_sha256,
                        request_count=client.request_count,
                        observations=tuple(observations),
                        failure_class=exc.failure_class,
                        failed_ordinal=shard.ordinal,
                    )
                elapsed = max(0, int((monotonic() - started) * 1000))
                rows.extend(parsed)
                observations.append(
                    DailyShadowFetchObservation(
                        ordinal=shard.ordinal,
                        outcome="SUCCESS",
                        elapsed_ms=elapsed,
                        response_bytes=response_bytes,
                        expected_rows=len(shard.provider_symbols),
                        observed_rows=len(parsed),
                    )
                )
            return DailyShadowFetchResult(
                status="ready",
                trade_date=plan.trade_date,
                request_plan_sha256=plan.request_plan_sha256,
                request_count=client.request_count,
                rows=tuple(rows),
                observations=tuple(observations),
            )
        except ProviderHttpError as exc:
            failure = (
                exc.failure_class if exc.failure_class in _ALLOWED_FAILURES else "transport_error"
            )
            if active_ordinal is not None:
                shard = plan.shards[active_ordinal]
                observations.append(
                    DailyShadowFetchObservation(
                        ordinal=active_ordinal,
                        outcome="FAILURE",
                        elapsed_ms=0,
                        response_bytes=client.last_response_bytes if client is not None else 0,
                        expected_rows=len(shard.provider_symbols),
                        observed_rows=0,
                        failure_class=failure,
                    )
                )
            return DailyShadowFetchResult(
                status="unavailable",
                trade_date=plan.trade_date,
                request_plan_sha256=plan.request_plan_sha256,
                request_count=client.request_count if client is not None else 0,
                observations=tuple(observations),
                failure_class=failure,
                failed_ordinal=active_ordinal,
            )
        except Exception:
            failure = "sdk_initialization_failed" if not sdk_initialized else "transport_error"
            if active_ordinal is not None:
                shard = plan.shards[active_ordinal]
                observations.append(
                    DailyShadowFetchObservation(
                        ordinal=active_ordinal,
                        outcome="FAILURE",
                        elapsed_ms=0,
                        response_bytes=client.last_response_bytes if client is not None else 0,
                        expected_rows=len(shard.provider_symbols),
                        observed_rows=0,
                        failure_class=failure,
                    )
                )
            return DailyShadowFetchResult(
                status="unavailable",
                trade_date=plan.trade_date,
                request_plan_sha256=plan.request_plan_sha256,
                request_count=client.request_count if client is not None else 0,
                observations=tuple(observations),
                failure_class=failure,
                failed_ordinal=active_ordinal,
            )
        finally:
            if client is not None:
                client.close()

    @classmethod
    def _offline_for_tests(
        cls,
        *,
        transport: Any,
        plan: DailyShadowPlan,
        sdk_initializer: Callable[[], Any],
        monotonic: Callable[[], float] = _monotonic,
    ) -> DailyShadowFetchResult:
        return cls._run(
            transport_factory=lambda: transport,
            plan=plan,
            sdk_initializer=sdk_initializer,
            monotonic=monotonic,
        )

    @classmethod
    def execute(
        cls,
        *,
        transport_factory: Callable[[], Any],
        plan: DailyShadowPlan,
    ) -> DailyShadowFetchResult:
        return cls._run(
            transport_factory=transport_factory,
            plan=plan,
            sdk_initializer=initialize_tickflow_free_sdk,
        )
