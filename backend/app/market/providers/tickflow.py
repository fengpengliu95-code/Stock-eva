"""Pinned, source-shaped TickFlow discovery canary.

The adapter is deliberately not a market-data provider: it never normalizes, publishes or
qualifies data. Network execution is possible only through the separately gated private
canary session in :mod:`http`; tests use :meth:`TickFlowCanaryRunner.offline` with a fake
transport.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, time, timedelta
from math import isfinite
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from ..shadow_evidence import ShadowLogicalRequest, ShadowLogicalRequestPlan
from .http import CanaryExecutor, HttpPolicy, _AuthorizedCanarySession

TICKFLOW_SERVER = "https://api.tickflow.org"
TICKFLOW_OPENAPI_URL = "https://docs.tickflow.org/zh-Hans/api-reference/openapi.json"
TICKFLOW_TERMS_URL = "https://tickflow.org/legal/terms-of-service.md"
_SAFE_SYMBOL = re.compile(r"^(?:sh|sz)\.\d{6}$")
_MAIN_BOARD = ("sh.600", "sh.601", "sh.603", "sh.605", "sz.000", "sz.001", "sz.002", "sz.003")


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
        joined = ",".join(symbols)
        if endpoint == "daily_batch":
            return {
                "symbols": joined,
                "period": "1d",
                "start_time": start_ms,
                "end_time": end_ms,
                "adjust": "none",
            }
        if endpoint == "ex_factors":
            return {"symbols": joined, "start": start_ms, "end": end_ms}
        if endpoint in {"universe", "indexes"}:
            return {"start_time": start_ms, "end_time": end_ms}
        raise TickFlowDiscoveryError("endpoint_unavailable")

    def parse(
        self, trade_date: date, payloads: dict[str, Any], *, request_count: int = 0
    ) -> TickFlowSourceBatch:
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
        _validate_symbols(tuple(symbols), for_execute=bool(symbols))
        return session.execute(self, trade_date, symbols=symbols)

    def fetch(self, trade_date: date, *, symbols: tuple[str, ...] = ()) -> TickFlowPlan:
        plan = self.plan(trade_date, symbols=symbols)
        return plan.model_copy(update={"requests": plan.requests[:3], "request_count": 3})

    def run(
        self, trade_date: date, *, symbols: tuple[str, ...] = ()
    ) -> TickFlowSourceBatch | TickFlowPlan:
        return self.plan(trade_date, symbols=symbols)


class TickFlowCanaryRunner:
    """Offline runner facade; no real client factory is accepted here."""

    @classmethod
    def offline(
        cls,
        *,
        transport: Any,
        trade_date: date,
        symbols: tuple[str, ...],
        token: str,
        evidence_store: Any | None = None,
        evidence_id: str = "task14-tickflow-canary",
    ) -> TickFlowCanaryReport:
        _validate_symbols(tuple(symbols), for_execute=True)
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
        )
        payloads: dict[str, Any] = {}
        for endpoint in adapter.ENDPOINTS:
            payloads[endpoint] = client.request_json(
                adapter.request_endpoint(endpoint),
                params=adapter.request_params(endpoint, trade_date, tuple(symbols)),
                headers={"x-api-key": token},
            )
            # Reject shape drift at the first response; do not spend the remaining bounded
            # request budget or retain a partial evidence set.
            _rows(payloads[endpoint])
        parsed = adapter.parse(trade_date, payloads, request_count=client.request_count)
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
        from .http import build_authorized_canary_session

        if not (
            type(getattr(self.settings, "provider_shadow_enabled", False)) is bool
            and self.settings.provider_shadow_enabled is True
            and type(getattr(self.settings, "provider_shadow_execute_enabled", False)) is bool
            and self.settings.provider_shadow_execute_enabled is True
        ):
            raise PermissionError("provider shadow execution is disabled")
        if not acknowledge_provider_requests or not external_authorization_id:
            raise PermissionError("provider request acknowledgement is required")
        canonical = tuple(
            root
            for root in (
                getattr(self.settings, "local_market_dataset_root", None),
                getattr(self.settings, "nas_market_dataset_root", None),
            )
            if root is not None
        )
        self.layout.validate_provider_shadow_root(canonical_roots=canonical)
        record = self.registry.read_status("tickflow")
        if str(record.admission_state.value).lower() != "canary":
            raise PermissionError("provider is not admitted for canary")
        _validate_symbols(tuple(symbols), for_execute=True)
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
