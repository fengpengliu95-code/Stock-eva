"""Deterministic after-close publication and scheduling policy."""

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import threading
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from copy import copy
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.blocking import run_blocking_drained
from backend.app.market.baostock import INDEX_SYMBOLS, ProviderBatch
from backend.app.market.baostock_vendor import transport_observation_sink
from backend.app.market.calendar import SHANGHAI, TradingCalendar
from backend.app.market.candidates import (
    CandidateStore,
    PublishedSelection,
    build_candidate_manifest,
    evaluate_candidate_gates,
    select_primary_candidate,
)
from backend.app.market.continuity import ContinuityDecision, RepairExecutionResult
from backend.app.market.evidence import (
    CacheFactorResolution,
    EvidenceManifest,
    EvidenceStore,
    FactorResolutionBinding,
    PublishedEvidence,
    _digest,
    _factor_value_semantic_hash,
    factor_snapshot_records_from_cache,
)
from backend.app.market.failures import (
    MarketFailure,
    legacy_failure_quality_issues,
    market_failure_from_exception,
    public_failure_message,
)
from backend.app.market.models import RefreshResult
from backend.app.market.provider_health import (
    CircuitState,
    InMemoryProviderHealthStore,
    ProviderHealthError,
)
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
    TransportOutcome,
)
from backend.app.market.providers.base import (
    ExpectedLogicalRequest,
    ExpectedLogicalRequestPlan,
    InstrumentRole,
    PaginationPolicy,
    ProviderId,
    ProviderRequest,
    RequestRole,
)
from backend.app.market.providers.base import (
    ProviderEndpoint as ContractProviderEndpoint,
)
from backend.app.market.store import MarketStore
from backend.app.market.universe import UniversePublicationContextV1

logger = logging.getLogger("stock_eva.market.automation")


def _safe_outcome_id(outcome: "AutomationOutcome") -> str:
    """Return only a bounded public identifier for handoff diagnostics."""
    result = getattr(outcome, "result", None)
    value = getattr(result, "run_id", None) or getattr(outcome.decision, "target_session", None)
    text = str(value or "unknown")
    return text[:96] if all(char.isalnum() or char in "-_.:" for char in text) else "unknown"


AUTOMATION_PROBE_STOCK_SYMBOL = "sh.600000"
AUTOMATION_PROBE_INDEX_SYMBOL = "sh.000001"
_TRANSPORT_ERROR_PRIORITY = {
    error: position
    for position, error in enumerate(
        (
            NormalizedTransportError.RATE_LIMIT,
            NormalizedTransportError.CONNECT_ERROR,
            NormalizedTransportError.SEND_ERROR,
            NormalizedTransportError.RECV_TIMEOUT,
            NormalizedTransportError.EOF,
            NormalizedTransportError.SHORT_HEADER,
            NormalizedTransportError.BAD_COMPRESSION,
            NormalizedTransportError.PAGINATION_STALLED,
            NormalizedTransportError.UNKNOWN_PROVIDER_PROTOCOL_ERROR,
            NormalizedTransportError.PROTOCOL_ERROR,
        )
    )
}


def _calendar_snapshot(calendar: TradingCalendar) -> TradingCalendar:
    snapshot = getattr(calendar, "snapshot", None)
    return snapshot() if callable(snapshot) else calendar


class UniversePostSuccessHook(Protocol):
    def offer(self, trade_date: str, now: str, context: object | None) -> "UniverseHookResult": ...


class _UniverseDecisionModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    action: Literal["run", "defer", "none"]
    reason_code: str | None
    provider_requests: int = Field(ge=0)
    writes_canonical: Literal[False] = False
    priority_order: int | None = None


UniverseMaintenanceDecision = _UniverseDecisionModel


class UniverseHookResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: Literal["PROMOTED", "BLOCKED", "DEFER"]
    reason_code: str | None
    provider_requests: int = Field(ge=0)
    writes_canonical: Literal[False] = False


@dataclass(frozen=True)
class MainBoardInspection:
    trade_date: date
    main_board_count: int
    shanghai_count: int
    shenzhen_count: int
    total_expected_count: int
    metadata_provider_requests: int
    main_board_symbols: tuple[str, ...]

    def as_preflight_snapshot(
        self, observed_symbols: tuple[str, ...] | list[str]
    ) -> "LegacyPreflightSnapshot":
        symbols = tuple(sorted(set(observed_symbols)))
        return LegacyPreflightSnapshot(
            inspection=self,
            observed_symbols=symbols,
            observed_symbol_count=len(symbols),
            observed_symbols_sha256=_symbol_set_sha256(symbols),
        )


@dataclass(frozen=True)
class LegacyPreflightSnapshot:
    inspection: MainBoardInspection
    observed_symbols: tuple[str, ...]
    observed_symbol_count: int
    observed_symbols_sha256: str


@dataclass(frozen=True)
class LegacyBuilderDiagnostic:
    outcome: Literal["accepted", "rejected"]
    observed_symbol_count: int
    observed_symbols_sha256: str
    failure_class: Literal["NONE", "INPUT_REJECTED", "BUILDER_ERROR"]


@dataclass(frozen=True)
class CanonicalRefreshExecution:
    """Private additive handoff; legacy callers still see RefreshResult attributes."""

    result: RefreshResult
    legacy_shadow_input: LegacyPreflightSnapshot | None = None
    builder_outcome: Literal["accepted", "rejected"] = "rejected"
    builder_diagnostic: LegacyBuilderDiagnostic | None = None

    def __getattr__(self, name: str):
        return getattr(self.result, name)


class LegacyShadowObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    provider_id: Literal["baostock"] = "baostock"
    request_trade_date: date
    contract_sha256: str | None
    expected_symbol_count: int | None
    observed_symbol_count: int
    missing_required_symbol_count: int | None
    extra_symbol_count: int | None
    drift_sha256: str | None
    reason_code: Literal["LEGACY_SHADOW_DRIFT", "NONE", "NO_COMPARISON"]
    control_reason: Literal["CONTROL_STATE_UNAVAILABLE"] | None


@dataclass(frozen=True)
class LegacyShadowHandoff:
    trade_date: date
    builder_outcome: Literal["accepted", "rejected"]
    diagnostic: LegacyShadowObservation


UniversePublicationContext = UniversePublicationContextV1


def _symbol_set_sha256(symbols: tuple[str, ...]) -> str:
    from backend.app.market.universe import domain_sha256

    return domain_sha256("stock-eva/r2f4.2/legacy-shadow-symbol-set/v1", list(symbols))


def inspect_legacy_main_board_input(adapter: object, trade_date: date) -> MainBoardInspection:
    inspection = adapter.inspect_main_board(trade_date)
    symbols = tuple(sorted(set(inspection.main_board_symbols)))
    if not symbols:
        raise ProviderHealthError("canonical universe inspection is empty")
    shanghai = sum(symbol.startswith("sh.") for symbol in symbols)
    return MainBoardInspection(
        trade_date=trade_date,
        main_board_count=getattr(inspection, "main_board_count", len(symbols)),
        shanghai_count=getattr(inspection, "shanghai_count", shanghai),
        shenzhen_count=getattr(inspection, "shenzhen_count", len(symbols) - shanghai),
        total_expected_count=getattr(inspection, "total_expected_count", len(symbols) + 2),
        metadata_provider_requests=getattr(inspection, "metadata_provider_requests", 2),
        main_board_symbols=symbols,
    )


def classify_universe_mode(environment: object, mode: object) -> UniverseMaintenanceDecision:
    if not isinstance(environment, str) or not isinstance(mode, str):
        return UniverseMaintenanceDecision(
            action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
        )
    profile = environment.strip().casefold()
    normalized_mode = mode
    if profile not in {"production", "development", "test", "staging"}:
        return UniverseMaintenanceDecision(
            action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
        )
    if normalized_mode not in {"off", "shadow", "enforce"}:
        return UniverseMaintenanceDecision(
            action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
        )
    if normalized_mode == "off":
        return UniverseMaintenanceDecision(action="none", reason_code="NONE", provider_requests=0)
    if profile == "production":
        return UniverseMaintenanceDecision(
            action="defer", reason_code="BLOCKED_PRODUCTION_MODE_OFF", provider_requests=0
        )
    if normalized_mode == "enforce":
        return UniverseMaintenanceDecision(
            action="defer", reason_code="BLOCKED_ENFORCE_NOT_ENABLED", provider_requests=0
        )
    return UniverseMaintenanceDecision(action="run", reason_code=None, provider_requests=0)


class UniverseMaintenanceRunner:
    """Dependency-injected writer pipeline; no dependency is invoked at construction."""

    def __init__(
        self,
        *,
        calendar_authority_reader: Callable[..., object],
        required_snapshot_capture: Callable[..., object],
        classification_fetcher: Callable[..., object],
        contract_builder: Callable[..., object],
        sidecar_promoter: Callable[..., object],
    ) -> None:
        self.calendar_authority_reader = calendar_authority_reader
        self.required_snapshot_capture = required_snapshot_capture
        self.classification_fetcher = classification_fetcher
        self.contract_builder = contract_builder
        self.sidecar_promoter = sidecar_promoter
        self._claimed_operation_days: set[tuple[str, str]] = set()
        self._claim_lock = threading.Lock()

    def __call__(self, *, trade_date: str, now: str, context: object, sidecar: object):
        try:
            operation_day = (
                datetime.fromisoformat(now.replace("Z", "+00:00"))
                .astimezone(SHANGHAI)
                .date()
                .isoformat()
            )
            key = (trade_date, operation_day)
            with self._claim_lock:
                if key in self._claimed_operation_days:
                    return UniverseHookResult(
                        status="DEFER", reason_code="NONE", provider_requests=0
                    )
                self._claimed_operation_days.add(key)
            calendar = self.calendar_authority_reader(trade_date=trade_date, now=now)
            snapshot = self.required_snapshot_capture()
            evidence = self.classification_fetcher(trade_date=trade_date, calendar=calendar)
            contract = self.contract_builder(
                trade_date=trade_date,
                calendar=calendar,
                required_snapshot=snapshot,
                evidence=evidence,
                context=context,
            )
            promoted = self.sidecar_promoter(sidecar=sidecar, contract=contract)
            if isinstance(promoted, UniverseHookResult):
                return promoted
            return UniverseHookResult(status="PROMOTED", reason_code=None, provider_requests=1)
        except Exception:
            return UniverseHookResult(
                status="DEFER", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
            )


class ConcreteUniversePostSuccessHook:
    """Safe, default-closed maintenance adapter for the Universe sidecar.

    The actual writer is injected as a bounded callable.  This keeps the legacy
    refresh path independent while making the enabled shadow lane concrete and
    testable without constructing a provider in this module.
    """

    def __init__(
        self,
        *,
        environment: str,
        mode: str,
        enabled: bool,
        interval_seconds: int = 86400,
        sidecar: object | None = None,
        maintenance_runner: UniverseMaintenanceRunner
        | Callable[..., UniverseHookResult]
        | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.environment = environment
        self.mode = mode
        self.enabled = enabled
        self.interval_seconds = interval_seconds
        self.sidecar = sidecar
        self.maintenance_runner = maintenance_runner
        self.clock = clock
        self.last_offer: tuple[str, str] | None = None

    def _terminal_finished_at(self, trade_date: str) -> datetime | None:
        reader = getattr(self.sidecar, "read_latest_terminal_finished_at", None)
        if not callable(reader):
            return None
        value = reader(date.fromisoformat(trade_date))
        if value is None:
            return None
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError("terminal timestamp is not trusted")
        return value.astimezone(UTC)

    def offer(
        self,
        trade_date: str,
        now: str,
        context: UniversePublicationContextV1 | None,
    ) -> UniverseHookResult:
        self.last_offer = (trade_date, now)
        if not self.enabled:
            return UniverseHookResult(status="DEFER", reason_code="NONE", provider_requests=0)
        decision = classify_universe_mode(self.environment, self.mode)
        if decision.reason_code is not None:
            status = "DEFER" if decision.action == "defer" else "BLOCKED"
            return UniverseHookResult(
                status=status,
                reason_code=decision.reason_code,
                provider_requests=0,
            )
        try:
            current = (
                self.clock()
                if self.clock is not None
                else datetime.fromisoformat(now.replace("Z", "+00:00"))
            )
            if current.tzinfo is None:
                raise ValueError("maintenance timestamp is not timezone aware")
            if context is not None and not isinstance(context, UniversePublicationContextV1):
                raise TypeError("publication context is not sealed")
            if context is not None and context.trade_date.isoformat() != trade_date:
                raise ValueError("publication context date mismatch")
            terminal = self._terminal_finished_at(trade_date)
            if terminal is None:
                return UniverseHookResult(
                    status="DEFER",
                    reason_code="CONTROL_STATE_UNAVAILABLE",
                    provider_requests=0,
                )
            if current.astimezone(UTC) < terminal:
                return UniverseHookResult(
                    status="DEFER",
                    reason_code="CONTROL_STATE_UNAVAILABLE",
                    provider_requests=0,
                )
            if (current.astimezone(UTC) - terminal).total_seconds() < self.interval_seconds:
                return UniverseHookResult(status="DEFER", reason_code="NONE", provider_requests=0)
            if self.sidecar is None or self.maintenance_runner is None:
                return UniverseHookResult(
                    status="DEFER",
                    reason_code="CONTROL_STATE_UNAVAILABLE",
                    provider_requests=0,
                )
            result = self.maintenance_runner(
                trade_date=trade_date,
                now=now,
                context=context,
                sidecar=self.sidecar,
            )
            if not isinstance(result, UniverseHookResult):
                raise TypeError("maintenance result is not typed")
            return result
        except Exception:
            return UniverseHookResult(
                status="DEFER",
                reason_code="CONTROL_STATE_UNAVAILABLE",
                provider_requests=0,
            )


def make_universe_post_success_hook(
    *,
    environment: str,
    mode: str,
    enabled: bool,
    interval_seconds: int = 86400,
    sidecar: object | None = None,
    maintenance_runner: Callable[..., UniverseHookResult] | None = None,
    calendar_authority_reader: Callable[..., object] | None = None,
    required_snapshot_capture: Callable[..., object] | None = None,
    classification_fetcher: Callable[..., object] | None = None,
    contract_builder: Callable[..., object] | None = None,
    sidecar_promoter: Callable[..., object] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> ConcreteUniversePostSuccessHook:
    if maintenance_runner is None and all(
        item is not None
        for item in (
            calendar_authority_reader,
            required_snapshot_capture,
            classification_fetcher,
            contract_builder,
            sidecar_promoter,
        )
    ):
        maintenance_runner = UniverseMaintenanceRunner(
            calendar_authority_reader=calendar_authority_reader,
            required_snapshot_capture=required_snapshot_capture,
            classification_fetcher=classification_fetcher,
            contract_builder=contract_builder,
            sidecar_promoter=sidecar_promoter,
        )
    return ConcreteUniversePostSuccessHook(
        environment=environment,
        mode=mode,
        enabled=enabled,
        interval_seconds=interval_seconds,
        sidecar=sidecar,
        maintenance_runner=maintenance_runner,
        clock=clock,
    )


def observe_legacy_request_universe(
    snapshot: LegacyPreflightSnapshot,
    builder_outcome: Literal["accepted", "rejected"],
    builder_diagnostic: LegacyBuilderDiagnostic | None,
    contract: object | None,
) -> LegacyShadowObservation:
    if contract is None:
        return LegacyShadowObservation(
            request_trade_date=snapshot.inspection.trade_date,
            contract_sha256=None,
            expected_symbol_count=None,
            observed_symbol_count=snapshot.observed_symbol_count,
            missing_required_symbol_count=None,
            extra_symbol_count=None,
            drift_sha256=None,
            reason_code="NO_COMPARISON",
            control_reason="CONTROL_STATE_UNAVAILABLE",
        )
    expected = tuple(
        sorted(
            item.symbol
            for item in getattr(contract, "members", ())
            if getattr(item, "expected_trading_state", None) in {"trading", "suspended"}
        )
    )
    observed = snapshot.observed_symbols
    missing = tuple(item for item in expected if item not in observed)
    extra = tuple(item for item in observed if item not in expected)
    drift = None
    if missing or extra:
        from backend.app.market.universe import domain_sha256

        drift = domain_sha256(
            "stock-eva/r2f4.2/legacy-shadow-drift/v1",
            {
                "provider_id": "baostock",
                "request_trade_date": snapshot.inspection.trade_date.isoformat(),
                "contract_sha256": getattr(contract, "contract_sha256", None),
                "expected_symbols": expected,
                "observed_symbols": observed,
                "missing_symbols": missing,
                "extra_symbols": extra,
            },
        )
    return LegacyShadowObservation(
        request_trade_date=snapshot.inspection.trade_date,
        contract_sha256=getattr(contract, "contract_sha256", None),
        expected_symbol_count=len(expected),
        observed_symbol_count=len(observed),
        missing_required_symbol_count=len(missing),
        extra_symbol_count=len(extra),
        drift_sha256=drift,
        reason_code="LEGACY_SHADOW_DRIFT" if missing or extra else "NONE",
        control_reason=None,
    )


def publish_provider_evidence(
    raw_batch,
    *,
    evidence_root: Path,
) -> tuple[EvidenceManifest, PublishedEvidence]:
    """Publish and immediately reopen one validated provider batch.

    The helper is intentionally narrow: it does not fetch, retry, normalize,
    select or move a canonical pointer.  Callers must already hold the
    existing ``RefreshRunLock`` and supply only the final-success batch.
    """
    store = EvidenceStore(evidence_root)
    manifest = store.publish(raw_batch)
    evidence = store.read(manifest.evidence_id)
    return manifest, evidence


def publish_provider_evidence_with_factor_cache(
    raw_batch,
    *,
    evidence_root: Path,
    factor_cache,
    factor_symbols: tuple[str, ...],
    trade_date: date,
    lock_path: Path,
    factor_resolution=(),
    factor_resolution_factory: Callable[[object, object], tuple] | None = None,
    _lock_held: bool = False,
) -> tuple[EvidenceManifest, PublishedEvidence]:
    """Capture factors and publish evidence under the existing refresh lock only."""
    lock_context = nullcontext() if _lock_held else RefreshRunLock(lock_path)
    with lock_context:
        capture_id = uuid.uuid4().hex
        before_rows = factor_cache.exact_snapshot_records(factor_symbols, trade_date)
        before_fingerprint = hashlib.sha256(
            json.dumps(before_rows, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        after_rows = factor_cache.exact_snapshot_records(factor_symbols, trade_date)
        after_fingerprint = hashlib.sha256(
            json.dumps(after_rows, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        if before_fingerprint != after_fingerprint or before_rows != after_rows:
            raise ProviderHealthError("factor snapshot changed during evidence capture")
        requested_keys = tuple(f"{symbol}.{trade_date.isoformat()}" for symbol in factor_symbols)
        actual_keys = tuple(f"{row['symbol']}.{row['trade_date']}" for row in after_rows)
        if (
            tuple(sorted(set(factor_symbols))) != tuple(factor_symbols)
            or len(actual_keys) != len(set(actual_keys))
            or tuple(sorted(actual_keys)) != tuple(sorted(requested_keys))
        ):
            raise ProviderHealthError("factor snapshot does not exactly cover requested symbols")
        records = factor_snapshot_records_from_cache(
            list(after_rows),
            before_fingerprint=before_fingerprint,
            after_fingerprint=after_fingerprint,
        )
        factor_store = EvidenceStore(evidence_root)
        factor_manifest, _factor_descriptor, _factor_payload = factor_store._build_factor_snapshot(
            records, capture_id=capture_id
        )
        binding = factor_resolution or (
            factor_resolution_factory(records, factor_manifest)
            if factor_resolution_factory is not None
            else getattr(raw_batch, "factor_resolution", ())
        )
        if not binding:
            raise ProviderHealthError("factor resolution binding is required")
        manifest = factor_store.publish(
            raw_batch,
            factor_records=records,
            capture_id=capture_id,
            factor_resolution=tuple(binding),
        )
        evidence = factor_store.read(manifest.evidence_id)
        return manifest, evidence


def run_canonical_raw_refresh(
    adapter: object,
    request: object,
    *,
    evidence_root: Path,
    lock_path: Path,
    factor_cache: object | None = None,
    factor_symbols: tuple[str, ...] = (),
    trade_date: date | None = None,
    factor_resolution=(),
    factor_resolution_factory: Callable[[object, object], tuple] | None = None,
    _lock_held: bool = False,
    validate_batch: Callable[[object], None] | None = None,
) -> tuple[EvidenceManifest, PublishedEvidence, tuple[object, ...]]:
    """Run the explicit evidence-first compatibility seam for a raw refresh.

    The legacy ``run_publication_refresh`` remains frozen for Task 7 callers.  New
    evidence work must enter here: fetch a typed raw batch, publish/read it, then
    normalize only through the bound evidence reader and its frozen clock.
    """
    from backend.app.market.providers.baostock import BaoStockProviderAdapter

    if not isinstance(adapter, BaoStockProviderAdapter):
        raise ProviderHealthError("canonical raw refresh requires BaoStock adapter")
    fetch_raw = getattr(adapter, "fetch_raw", None)
    if not callable(fetch_raw):
        raise ProviderHealthError("canonical raw refresh requires raw provider contract")
    lock_context = nullcontext() if _lock_held else RefreshRunLock(lock_path)
    with lock_context:
        batch = fetch_raw(request)
        if validate_batch is not None:
            validate_batch(batch)
        if factor_cache is not None:
            if not factor_symbols or trade_date is None:
                raise ProviderHealthError("canonical factor capture requires symbols and date")
            manifest, evidence = publish_provider_evidence_with_factor_cache(
                batch,
                evidence_root=evidence_root,
                factor_cache=factor_cache,
                factor_symbols=factor_symbols,
                trade_date=trade_date,
                lock_path=lock_path,
                factor_resolution=factor_resolution,
                factor_resolution_factory=factor_resolution_factory,
                _lock_held=True,
            )
        else:
            manifest, evidence = publish_provider_evidence(
                batch,
                evidence_root=evidence_root,
            )
        normalized = adapter.normalize(
            evidence,
            normalization_clock_utc=manifest.normalization_clock_utc,
        )
    return manifest, evidence, normalized


def build_canonical_raw_request(
    adapter: object,
    *,
    trade_date: date,
    refresh_id: str,
    required_symbols: set[str],
    inspection: MainBoardInspection | None = None,
) -> ProviderRequest:
    """Build the complete, typed daily raw plan used by automatic refresh."""
    if inspection is None:
        inspection = inspect_legacy_main_board_input(adapter, trade_date)
    elif inspection.trade_date != trade_date:
        raise ProviderHealthError("canonical universe inspection date mismatch")
    main_symbols = tuple(sorted(set(inspection.main_board_symbols)))
    if not main_symbols:
        raise ProviderHealthError("canonical universe inspection is empty")
    if not set(required_symbols).issubset(main_symbols):
        raise ProviderHealthError("required symbols are outside canonical universe")
    session_symbols = tuple(sorted(set(main_symbols) | set(INDEX_SYMBOLS)))
    requests: list[ExpectedLogicalRequest] = []

    def add(
        endpoint: ContractProviderEndpoint,
        role: RequestRole,
        instrument: InstrumentRole | None,
        variant: str,
        symbols: tuple[str, ...],
    ) -> None:
        ordinal = len(requests)
        requests.append(
            ExpectedLogicalRequest(
                plan_ordinal=ordinal,
                endpoint=endpoint,
                request_role=role,
                instrument_role=instrument,
                schema_variant=variant,
                shard_id=f"daily-{ordinal}",
                symbols=symbols,
                start_date=trade_date,
                end_date=trade_date,
                pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
            )
        )

    add(
        ContractProviderEndpoint.ALL_STOCK,
        RequestRole.UNIVERSE,
        InstrumentRole.STOCK,
        "all_stock.market.v1",
        (),
    )
    add(
        ContractProviderEndpoint.DAILY_ASTOCK,
        RequestRole.DAILY_STOCK,
        InstrumentRole.STOCK,
        "daily_astock.v1",
        main_symbols,
    )
    add(
        ContractProviderEndpoint.DAILY_FACTOR,
        RequestRole.DAILY_FACTOR,
        InstrumentRole.STOCK,
        "daily_factor.v1",
        main_symbols,
    )
    for symbol in INDEX_SYMBOLS:
        add(
            ContractProviderEndpoint.INDEX_HISTORY,
            RequestRole.INDEX_HISTORY,
            InstrumentRole.INDEX,
            "index_history.session.v1",
            (symbol,),
        )
    plan = ExpectedLogicalRequestPlan(
        requests=tuple(requests),
        request_count=len(requests),
        request_plan_hash=_digest([item.model_dump(mode="json") for item in requests]),
    )
    return ProviderRequest(
        provider_id=ProviderId.BAOSTOCK,
        refresh_id=refresh_id,
        trade_date=trade_date,
        universe_id="all-main-board",
        session_symbols=session_symbols,
        logical_request_plan=plan,
    )


def canonical_refresh_callback(
    store: MarketStore,
    adapter: object,
    *,
    evidence_root: Path,
    lock_path: Path,
    factor_cache: object,
) -> Callable[..., CanonicalRefreshExecution]:
    """Return the automatic evidence-first refresh operation.

    The callback is invoked while ``MarketAutomationService`` owns the refresh
    lock.  It deliberately never calls the legacy provider ``fetch`` method.
    """

    context_holder: dict[tuple[str, date], object] = {}
    context_holder_lock = threading.Lock()

    def execute(
        *,
        trade_date: date,
        required_symbols: set[str],
        request_key: str,
        run_id: str,
        before_store: Callable[[], None] | None = None,
    ) -> CanonicalRefreshExecution:
        adapter_scope_factory = getattr(adapter, "refresh_operation", None)

        def adapter_scope():
            return (
                adapter_scope_factory(run_id) if callable(adapter_scope_factory) else nullcontext()
            )

        def failure_result(error: Exception) -> RefreshResult:
            failure = market_failure_from_exception(error, stage="fetch")
            return RefreshResult(
                run_id=run_id,
                request_key=request_key,
                run_kind="daily",
                requested_date=trade_date,
                source="baostock",
                status="error",
                requested_count=0,
                succeeded_count=0,
                coverage_ratio=0,
                failed_symbols=[],
                quality_issues=legacy_failure_quality_issues(failure),
                error_message=public_failure_message(failure),
                failure_stage=failure.failure_stage,
                failure_class=failure.failure_class,
                retryable=failure.retryable,
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC),
            )

        inspection: MainBoardInspection | None = None
        preflight: LegacyPreflightSnapshot | None = None
        builder_outcome: Literal["accepted", "rejected"] = "rejected"
        builder_diagnostic: LegacyBuilderDiagnostic | None = None
        try:
            with adapter_scope():
                inspection = inspect_legacy_main_board_input(adapter, trade_date)
                request = build_canonical_raw_request(
                    adapter,
                    trade_date=trade_date,
                    refresh_id=run_id,
                    required_symbols=required_symbols,
                    inspection=inspection,
                )
            builder_outcome = "accepted"
            stock_request = next(
                item
                for item in request.logical_request_plan.requests
                if item.endpoint is ContractProviderEndpoint.DAILY_ASTOCK
            )
            main_symbols = stock_request.symbols
            preflight = inspection.as_preflight_snapshot(request.session_symbols)
            builder_diagnostic = LegacyBuilderDiagnostic(
                outcome="accepted",
                observed_symbol_count=preflight.observed_symbol_count,
                observed_symbols_sha256=preflight.observed_symbols_sha256,
                failure_class="NONE",
            )
        except Exception as error:
            if inspection is not None:
                preflight = inspection.as_preflight_snapshot(
                    inspection.main_board_symbols + tuple(INDEX_SYMBOLS)
                )
            builder_diagnostic = LegacyBuilderDiagnostic(
                outcome="rejected",
                observed_symbol_count=(preflight.observed_symbol_count if preflight else 0),
                observed_symbols_sha256=(
                    preflight.observed_symbols_sha256 if preflight else "0" * 64
                ),
                failure_class=(
                    "INPUT_REJECTED" if isinstance(error, ProviderHealthError) else "BUILDER_ERROR"
                ),
            )
            return CanonicalRefreshExecution(
                result=failure_result(error),
                legacy_shadow_input=preflight,
                builder_outcome="rejected",
                builder_diagnostic=builder_diagnostic,
            )

        def validate_universe(batch: object) -> None:
            universe = tuple(
                item
                for item in batch.endpoint_batches
                if item.endpoint is ContractProviderEndpoint.ALL_STOCK
            )
            if len(universe) != 1:
                raise ProviderHealthError("canonical universe page is missing")
            actual = tuple(row.code for row in universe[0].rows)
            if actual != main_symbols:
                raise ProviderHealthError("canonical universe changed during raw refresh")

        def factor_bindings(
            records: object,
            _factor_manifest: EvidenceManifest,
        ) -> tuple[FactorResolutionBinding, ...]:
            rows = records.rows
            bindings = []
            factor_plan_ordinal = next(
                item.plan_ordinal
                for item in request.logical_request_plan.requests
                if item.endpoint is ContractProviderEndpoint.DAILY_FACTOR
            )
            for _ordinal, row in enumerate(rows):
                row_data = row.model_dump(mode="python")
                cache = CacheFactorResolution(
                    cache_object_id=_factor_manifest.object_id,
                    cache_object_sha256=_factor_manifest.object_sha256,
                    record_key=f"{row_data['symbol']}.{row_data['trade_date']}",
                )
                values = {
                    "plan_ordinal": factor_plan_ordinal,
                    "symbol": row_data["symbol"],
                    "trade_date": date.fromisoformat(str(row_data["trade_date"])),
                    "selected_kind": "factor_cache_snapshot",
                    "selected_value_semantic_hash": _factor_value_semantic_hash(row_data),
                    "live": None,
                    "cache": cache,
                }
                candidate = FactorResolutionBinding.model_construct(
                    **values,
                    resolution_sha256="0" * 64,
                )
                serialized = candidate.model_dump(mode="json")
                serialized.pop("resolution_sha256", None)
                bindings.append(
                    FactorResolutionBinding(
                        **values,
                        resolution_sha256=_digest(serialized),
                    )
                )
            if not bindings:
                raise ProviderHealthError("canonical factor snapshot is empty")
            return tuple(bindings)

        with adapter_scope():
            try:
                manifest, evidence, bars = run_canonical_raw_refresh(
                    adapter,
                    request,
                    evidence_root=evidence_root,
                    lock_path=lock_path,
                    factor_cache=factor_cache,
                    factor_symbols=main_symbols,
                    trade_date=trade_date,
                    factor_resolution_factory=factor_bindings,
                    validate_batch=validate_universe,
                    _lock_held=True,
                )
                published_selection = None
                try:
                    normalized = ProviderBatch(
                        bars=list(bars),
                        expected_symbols=list(request.session_symbols),
                        failed_symbols=[],
                    )
                    if before_store is not None:
                        before_store()
                    gate_report = evaluate_candidate_gates(
                        candidate_id=f"candidate-{manifest.evidence_id[3:]}",
                        trade_date=trade_date,
                        required_symbols=tuple(request.session_symbols),
                        rows=tuple(normalized.bars),
                        evidence=evidence,
                        factor_resolution=manifest.factor_resolution,
                        created_at=datetime.now(UTC),
                    )
                    normalized_payload = json.dumps(
                        [item.model_dump(mode="json") for item in normalized.bars],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    normalized_hash = hashlib.sha256(normalized_payload).hexdigest()
                    candidate = build_candidate_manifest(
                        candidate_id=f"candidate-{manifest.evidence_id[3:]}",
                        trade_date=trade_date,
                        universe_id=manifest.universe_id,
                        provider_id=manifest.provider_id,
                        evidence_id=manifest.evidence_id,
                        evidence_sha256=manifest.manifest_sha256,
                        normalized_object_relative_path=(
                            f"bundles/candidate-{manifest.evidence_id[3:]}/normalized.json"
                        ),
                        normalized_object_sha256=normalized_hash,
                        gate_report_relative_path=(
                            f"bundles/candidate-{manifest.evidence_id[3:]}/gate.json"
                        ),
                        gate_report_sha256=gate_report.aggregate_sha256,
                        factor_resolution_sha256=manifest.factor_resolution_sha256,
                        adapter_version=manifest.adapter_version,
                        source_schema_version="daily_astock.v1",
                        row_count=len(normalized.bars),
                        required_symbol_count=len(request.session_symbols),
                        status="accepted" if gate_report.verdict == "pass" else "rejected",
                    )
                    selection = None
                    if candidate.status == "accepted":
                        selection = select_primary_candidate(
                            trade_date=trade_date,
                            universe_id=manifest.universe_id,
                            candidates=(candidate,),
                            selected_at=datetime.now(UTC),
                            gate_report=gate_report,
                            evidence=evidence,
                        )
                    published_selection = CandidateStore(evidence_root).publish_chain(
                        report=gate_report,
                        candidate=candidate,
                        selection=selection,
                        evidence=evidence,
                        normalized_payload=normalized_payload,
                    )
                    if gate_report.verdict != "pass":
                        raise ProviderHealthError("canonical candidate gates did not pass")
                    if not isinstance(published_selection, PublishedSelection):
                        raise ProviderHealthError("accepted candidate bundle was not published")
                    result = RefreshResult(
                        run_id=run_id,
                        request_key=request_key,
                        run_kind="daily",
                        requested_date=trade_date,
                        source="baostock",
                        status="ready",
                        requested_count=len(request.session_symbols),
                        succeeded_count=len(normalized.bars),
                        coverage_ratio=1.0,
                        failed_symbols=[],
                        quality_issues=[],
                        started_at=datetime.now(UTC),
                        completed_at=datetime.now(UTC),
                    )
                    publication_lineage = {
                        "provider_id": candidate.provider_id.value,
                        "universe_id": candidate.universe_id,
                        "evidence_id": candidate.evidence_id,
                        "evidence_sha256": candidate.evidence_sha256,
                        "candidate_id": candidate.candidate_id,
                        "candidate_manifest_sha256": candidate.manifest_sha256,
                        "gate_report_sha256": candidate.gate_report_sha256,
                        "adapter_version": candidate.adapter_version,
                        "source_schema_version": candidate.source_schema_version,
                    }
                    store.save_refresh(
                        normalized.bars,
                        result,
                        publish=True,
                        publication_lineage=publication_lineage,
                        selection=published_selection,
                    )
                    try:
                        from backend.app.market.universe import canonical_json_bytes, domain_sha256

                        lineage_json = canonical_json_bytes(publication_lineage).decode()
                        lineage_sha = domain_sha256(
                            "stock-eva/r2f4.2/publication-lineage/v2", publication_lineage
                        )
                        evidence_refs = tuple(
                            sorted(
                                {
                                    candidate.evidence_sha256,
                                    candidate.manifest_sha256,
                                    candidate.gate_report_sha256,
                                }
                            )
                        )
                        context_preimage = {
                            "run_id": run_id,
                            "trade_date": trade_date.isoformat(),
                            "manifest_ref": candidate.manifest_sha256,
                            "evidence_refs": list(evidence_refs),
                            "publication_lineage_json": lineage_json,
                            "publication_lineage_sha256": lineage_sha,
                            "status": "ready",
                            "created_at": result.completed_at.astimezone(UTC).isoformat(),
                        }
                        context_sha = domain_sha256(
                            "stock-eva/r2f4.2/universe-publication-context/v1",
                            context_preimage,
                        )
                        context = UniversePublicationContextV1(
                            context_id=context_sha[:32],
                            run_id=run_id,
                            trade_date=trade_date,
                            manifest_ref=candidate.manifest_sha256,
                            evidence_refs=evidence_refs,
                            publication_lineage_json=lineage_json,
                            publication_lineage_sha256=lineage_sha,
                            status="ready",
                            created_at=result.completed_at,
                            context_sha256=context_sha,
                        )
                        with context_holder_lock:
                            context_holder[(run_id, trade_date)] = context
                    except Exception:
                        _log_event(logging.WARNING, "universe_context_unavailable", outcome="ready")
                    return CanonicalRefreshExecution(
                        result=result,
                        legacy_shadow_input=preflight,
                        builder_outcome=builder_outcome,
                        builder_diagnostic=builder_diagnostic,
                    )
                finally:
                    if published_selection is not None:
                        published_selection.close()
                    evidence.close()
            except Exception as error:
                return CanonicalRefreshExecution(
                    result=failure_result(error),
                    legacy_shadow_input=preflight,
                    builder_outcome=builder_outcome,
                    builder_diagnostic=builder_diagnostic,
                )

    locked_execute = execute

    def execute_with_lock(**kwargs):
        lock_held = bool(kwargs.pop("_lock_held", False))
        lease = nullcontext() if lock_held else RefreshRunLock(lock_path)
        with lease:
            return locked_execute(**kwargs)

    def consume_universe_publication_context(run_id: str, trade_date: str):
        try:
            key = (run_id, date.fromisoformat(trade_date))
        except (TypeError, ValueError):
            return None
        with context_holder_lock:
            return context_holder.pop(key, None)

    execute_with_lock.consume_universe_publication_context = consume_universe_publication_context
    execute_with_lock._refresh_lock_held = True
    return execute_with_lock


def _log_event(level: int, event: str, **fields) -> None:
    logger.log(
        level,
        "%s %s",
        event,
        json.dumps(fields, default=str, sort_keys=True),
    )


RefreshState = Literal[
    "disabled",
    "idle",
    "scheduled",
    "running",
    "retry_wait",
    "success",
    "delayed",
    "error",
]


class RefreshAlreadyRunning(RuntimeError):
    pass


class RefreshRunLock:
    """Non-blocking cross-process lease for the single daily refresh."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._descriptor: int | None = None

    def __enter__(self) -> "RefreshRunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise RefreshAlreadyRunning("market refresh already running") from exc
        self._descriptor = descriptor
        return self

    def __exit__(self, *_args) -> None:
        if self._descriptor is None:
            return
        fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        os.close(self._descriptor)
        self._descriptor = None


class SchedulerState(BaseModel):
    target_session: date | None = None
    refresh_state: RefreshState = "idle"
    attempt_count: int = 0
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    next_retry_at: datetime | None = None
    calendar_status: Literal["confirmed", "conflict", "unavailable"] = "confirmed"
    error_code: str | None = None


class ScheduleDecision(BaseModel):
    action: Literal["run", "wait", "none"]
    target_session: date | None
    refresh_state: RefreshState
    next_run_at: datetime | None = None


class AutomationOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: ScheduleDecision
    state: SchedulerState
    result: RefreshResult | None = None
    # Repair execution is continuity evidence, not a freshness scheduler result.  The field is
    # omitted when absent so the pre-R2-F1 JSON contract remains byte-for-byte compatible.
    continuity_result: RepairExecutionResult | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @model_validator(mode="after")
    def valid_lane_shape(self) -> "AutomationOutcome":
        if self.continuity_result is not None:
            if self.result is not None:
                raise ValueError("repair evidence cannot populate freshness result")
            if self.continuity_result.decision.lane != "repair":
                raise ValueError("continuity result must use repair decision lane")
            if self.continuity_result.status == "skipped":
                raise ValueError("skipped continuity evidence is not a public repair result")
        if self.result is not None and self.result.run_kind == "repair":
            raise ValueError("repair refresh result must use continuity_result")
        return self


class ProviderProbeRunner(Protocol):
    def run(self, endpoint: ProviderEndpoint, *, trade_date: date, refresh_id: str) -> None: ...


class ContinuityCoordinator(Protocol):
    def claim_ready_once(
        self,
        *,
        freshness: ScheduleDecision,
        latest_expected_session: date,
        repair_enabled: bool,
        now: datetime,
        revalidator: Callable[[], ScheduleDecision],
        lease_consumer: None,
    ) -> ContinuityDecision: ...


class BaoStockProbeRunner:
    """Construct one write-free, one-attempt provider for a leased endpoint probe."""

    def __init__(
        self,
        provider_factory,
        *,
        stock_symbol: str = AUTOMATION_PROBE_STOCK_SYMBOL,
        index_symbol: str = AUTOMATION_PROBE_INDEX_SYMBOL,
    ) -> None:
        self.provider_factory = provider_factory
        self.stock_symbol = stock_symbol
        self.index_symbol = index_symbol

    def run(self, endpoint: ProviderEndpoint, *, trade_date: date, refresh_id: str) -> None:
        provider = self.provider_factory(max_attempts=1)
        if getattr(provider, "max_attempts", None) != 1:
            raise ProviderHealthError("provider probe retry policy is invalid")
        with provider.refresh_operation(refresh_id):
            provider.probe_endpoint(
                endpoint,
                trade_date=trade_date,
                stock_symbol=self.stock_symbol,
                index_symbol=self.index_symbol,
            )


class _RefreshObservationCollector:
    def __init__(self, health_store, refresh_id: str) -> None:
        self.health_store = health_store
        self.refresh_id = refresh_id
        self.observations: list[TransportObservation] = []
        self._resolved = False

    def record(self, observation: TransportObservation) -> None:
        if observation.refresh_id != self.refresh_id:
            raise ProviderHealthError("provider observation refresh scope does not match")
        self.health_store.record_observation(observation)
        self.observations.append(observation)

    def terminal_error(
        self,
        endpoint: ProviderEndpoint,
    ) -> NormalizedTransportError | None:
        endpoint_observations = [
            item
            for item in self.observations
            if item.endpoint == endpoint and item.protocol_stage == ProtocolStage.OPERATION
        ]
        if not endpoint_observations:
            return NormalizedTransportError.PROTOCOL_ERROR
        errors = [
            item.normalized_error
            for item in endpoint_observations
            if item.normalized_error is not None
        ]
        if not errors:
            return None
        return max(errors, key=_TRANSPORT_ERROR_PRIORITY.__getitem__)

    def probe_error(self, endpoint: ProviderEndpoint) -> NormalizedTransportError | None:
        endpoint_observations = [item for item in self.observations if item.endpoint == endpoint]
        operations = [
            item for item in endpoint_observations if item.protocol_stage == ProtocolStage.OPERATION
        ]
        if not operations:
            return NormalizedTransportError.PROTOCOL_ERROR
        errors = [
            item.normalized_error
            for item in endpoint_observations
            if item.normalized_error is not None
        ]
        if errors:
            return max(errors, key=_TRANSPORT_ERROR_PRIORITY.__getitem__)
        if (
            len({item.provider_session_id for item in endpoint_observations}) != 1
            or any(item.attempt != 1 for item in endpoint_observations)
            or any(item.outcome == TransportOutcome.ERROR for item in endpoint_observations)
            or not any(item.outcome == TransportOutcome.SUCCESS for item in operations)
        ):
            return NormalizedTransportError.PROTOCOL_ERROR
        return None

    def resolve_touched_endpoints(self) -> None:
        if self._resolved:
            return
        self._resolved = True
        touched = {
            item.endpoint
            for item in self.observations
            if isinstance(item.endpoint, ProviderEndpoint)
        }
        for endpoint in ProviderEndpoint:
            if endpoint not in touched:
                continue
            error = self.terminal_error(endpoint)
            if error is None:
                self.health_store.record_terminal_success(self.refresh_id, endpoint)
            else:
                self.health_store.record_terminal_failure(self.refresh_id, endpoint, error)


class SchedulePolicy:
    RETRY_TIMES = (time(18, 40), time(19, 20), time(20, 10), time(21, 0))
    CORRECTION_TIME = time(7, 15)

    def __init__(self, calendar: TradingCalendar, *, _pinned: bool = False) -> None:
        self.calendar = calendar
        self._pinned = _pinned

    def for_snapshot(self, calendar: TradingCalendar) -> "SchedulePolicy":
        operation = copy(self)
        operation.calendar = calendar
        operation._pinned = True
        return operation

    @staticmethod
    def availability_for(session: date) -> datetime:
        return datetime.combine(session, time(18, 10), tzinfo=SHANGHAI)

    def next_retry_after(self, session: date, after: datetime) -> datetime | None:
        local = after.astimezone(SHANGHAI)
        slots = [datetime.combine(session, item, tzinfo=SHANGHAI) for item in self.RETRY_TIMES]
        slots.append(
            datetime.combine(
                session + timedelta(days=1),
                self.CORRECTION_TIME,
                tzinfo=SHANGHAI,
            )
        )
        return next((slot for slot in slots if slot > local), None)

    def decide(
        self,
        now: datetime,
        *,
        published_as_of: date | None,
        state: SchedulerState | None,
    ) -> ScheduleDecision:
        local = now.astimezone(SHANGHAI)
        calendar = self.calendar if self._pinned else _calendar_snapshot(self.calendar)
        target = calendar.latest_expected_session(local)
        if target is None:
            return ScheduleDecision(
                action="none",
                target_session=None,
                refresh_state="error",
            )
        if published_as_of is not None and published_as_of >= target:
            return ScheduleDecision(
                action="none",
                target_session=target,
                refresh_state="success",
            )
        available_at = self.availability_for(target)
        if local < available_at:
            return ScheduleDecision(
                action="wait",
                target_session=target,
                refresh_state="scheduled",
                next_run_at=available_at,
            )
        if state is None or state.target_session != target:
            return ScheduleDecision(
                action="run",
                target_session=target,
                refresh_state="running",
            )
        if state.next_retry_at is not None:
            if local >= state.next_retry_at.astimezone(SHANGHAI):
                return ScheduleDecision(
                    action="run",
                    target_session=target,
                    refresh_state="running",
                )
            return ScheduleDecision(
                action="wait",
                target_session=target,
                refresh_state="retry_wait",
                next_run_at=state.next_retry_at,
            )
        if state.attempt_count == 0 and state.refresh_state != "delayed":
            return ScheduleDecision(
                action="run",
                target_session=target,
                refresh_state="running",
            )
        return ScheduleDecision(
            action="none",
            target_session=target,
            refresh_state="delayed",
        )


def _publication_issues(batch, required_symbols: set[str]) -> list[str]:
    loaded = {bar.symbol for bar in batch.bars}
    expected = set(batch.expected_symbols)
    issues = [
        f"missing_symbol:{symbol}"
        for symbol in sorted((expected - loaded) | set(batch.failed_symbols))
    ]
    issues.extend(
        f"required_index_missing:{symbol}" for symbol in INDEX_SYMBOLS if symbol not in loaded
    )
    issues.extend(
        f"required_symbol_missing:{symbol}" for symbol in sorted(required_symbols - loaded)
    )
    issues.extend(
        f"missing_adjust_factor:{bar.symbol}"
        for bar in batch.bars
        if bar.security_type == "stock" and not bar.is_suspended and bar.adjust_factor is None
    )
    issues.extend(
        f"invalid_publication_bar_quality:{bar.symbol}:{issue}"
        for bar in batch.bars
        if (issue := MarketStore.publication_bar_quality_issue(bar)) is not None
    )
    return list(dict.fromkeys(issues))


def run_publication_refresh(
    store: MarketStore,
    provider,
    *,
    trade_date: date,
    required_symbols: set[str],
    request_key: str | None = None,
    run_id: str | None = None,
    run_kind: Literal["daily", "backfill", "repair"] = "daily",
    before_store: Callable[[], None] | None = None,
    normalized_batch=None,
) -> RefreshResult:
    """Fetch to canonical staging, validate all gates, then move the pointer."""
    started_at = datetime.now(UTC)
    request_key = request_key or (f"daily:baostock:{trade_date.isoformat()}:all-main-board")
    request_prefix = hashlib.sha256(request_key.encode()).hexdigest()[:12]
    run_id = run_id or f"{request_prefix}-{uuid.uuid4().hex[:12]}"
    _log_event(
        logging.INFO,
        "market_publication_started",
        run_id=run_id,
        trade_date=trade_date,
    )
    failure_stage: Literal["fetch", "normalize", "validate", "publish"] = "fetch"
    requested_count = 0
    succeeded_count = 0
    failures: list[str] = []
    before_store_called = False

    def finalize_provider_audit() -> None:
        nonlocal before_store_called
        if before_store_called or before_store is None:
            return
        before_store_called = True
        before_store()

    def failed_result(failure: MarketFailure) -> RefreshResult:
        result = RefreshResult(
            run_id=run_id,
            request_key=request_key,
            run_kind=run_kind,
            requested_date=trade_date,
            source="baostock",
            status="error",
            requested_count=requested_count,
            succeeded_count=succeeded_count,
            coverage_ratio=0,
            failed_symbols=failures,
            quality_issues=legacy_failure_quality_issues(failure),
            error_message=public_failure_message(failure),
            failure_stage=failure.failure_stage,
            failure_class=failure.failure_class,
            retryable=failure.retryable,
            started_at=started_at,
            completed_at=datetime.now(UTC),
        )
        if failure.failure_stage != "publish":
            try:
                finalize_provider_audit()
                store.save_refresh([], result, publish=False)
            except Exception as error:
                storage_failure = market_failure_from_exception(
                    error,
                    stage="publish",
                    default_class="storage",
                    default_retryable=False,
                )
                return failed_result(storage_failure)
        _log_event(
            logging.ERROR,
            "market_publication_failed",
            run_id=run_id,
            failure_stage=failure.failure_stage,
            failure_class=failure.failure_class,
            retryable=failure.retryable,
        )
        return result

    try:
        batch = (
            normalized_batch
            if normalized_batch is not None
            else provider.fetch(trade_date, symbols=None)
        )
        failure_stage = "validate"
        loaded = {bar.symbol for bar in batch.bars}
        expected = set(batch.expected_symbols)
        issues = _publication_issues(batch, required_symbols)
        batch_failure = getattr(batch, "failure", None)
        if batch_failure is not None:
            issues.extend(legacy_failure_quality_issues(batch_failure))
        failures = sorted((expected - loaded) | set(batch.failed_symbols))
        requested_count = len(expected)
        succeeded_count = len(expected & loaded)
        coverage = succeeded_count / requested_count if requested_count else 0.0
        if coverage < 1:
            issues.append("incomplete_symbol_coverage")
        status = "ready" if requested_count and not issues else "partial"
        publication_failure = None
        if status == "partial":
            if batch_failure is not None:
                publication_failure = batch_failure
            elif requested_count == 0:
                publication_failure = MarketFailure(
                    failure_stage="validate",
                    failure_class="universe",
                    retryable=True,
                )
            elif failures or any(
                issue.startswith(
                    (
                        "missing_symbol:",
                        "required_index_missing:",
                        "required_symbol_missing:",
                        "incomplete_symbol_coverage",
                    )
                )
                for issue in issues
            ):
                publication_failure = MarketFailure(
                    failure_stage="validate",
                    failure_class="coverage",
                    retryable=True,
                )
            else:
                publication_failure = MarketFailure(
                    failure_stage="validate",
                    failure_class="semantic",
                    retryable=False,
                )
        result = RefreshResult(
            run_id=run_id,
            request_key=request_key,
            run_kind=run_kind,
            requested_date=trade_date,
            source="baostock",
            status=status,
            requested_count=requested_count,
            succeeded_count=succeeded_count,
            coverage_ratio=coverage,
            failed_symbols=failures,
            quality_issues=list(dict.fromkeys(issues)),
            failure_stage=(
                publication_failure.failure_stage if publication_failure is not None else None
            ),
            failure_class=(
                publication_failure.failure_class if publication_failure is not None else None
            ),
            retryable=(publication_failure.retryable if publication_failure is not None else None),
            started_at=started_at,
            completed_at=datetime.now(UTC),
        )
        # A failed validation remains auditable, but cannot mutate the canonical
        # rows behind an existing published pointer for the same session.
        failure_stage = "publish"
        finalize_provider_audit()
        store.save_refresh(
            batch.bars if status == "ready" else [],
            result,
            publish=status == "ready",
        )
        _log_event(
            logging.INFO if status == "ready" else logging.WARNING,
            "market_publication_finished",
            run_id=run_id,
            status=status,
            requested_count=requested_count,
            succeeded_count=succeeded_count,
            coverage_ratio=coverage,
        )
        return result
    except Exception as error:
        failure = market_failure_from_exception(
            error,
            stage=failure_stage,
            default_class="storage" if failure_stage == "publish" else "internal",
            default_retryable=False,
        )
        return failed_result(failure)


class MarketAutomationService:
    def __init__(
        self,
        store: MarketStore,
        provider,
        calendar: TradingCalendar,
        *,
        required_symbols: Callable[[], set[str]],
        lock_path: Path | None = None,
        post_publish=None,
        shadow_handoff=None,
        health_store=None,
        probe_runner: ProviderProbeRunner | None = None,
        continuity: ContinuityCoordinator | None = None,
        repair_enabled: bool = False,
        repair_executor=None,
        canonical_refresh: Callable[..., RefreshResult] | None = None,
        universe_post_success_hook: UniversePostSuccessHook | None = None,
        market_universe_maintenance_enabled: bool = False,
        market_universe_mode: str = "off",
        environment: str = "development",
    ) -> None:
        self.store = store
        self.provider = provider
        self.calendar = calendar
        self.required_symbols = required_symbols
        self.lock_path = lock_path
        self.post_publish = post_publish
        self.shadow_handoff = shadow_handoff
        self.health_store = health_store or InMemoryProviderHealthStore()
        self.probe_runner = probe_runner
        self.continuity = continuity
        self.repair_enabled = repair_enabled
        self.repair_executor = repair_executor
        self.canonical_refresh = canonical_refresh
        self.universe_post_success_hook = universe_post_success_hook
        self.market_universe_maintenance_enabled = market_universe_maintenance_enabled
        self.market_universe_mode = market_universe_mode
        self.environment = environment
        self.last_universe_decision: UniverseMaintenanceDecision | None = None
        self.last_universe_result: UniverseHookResult | None = None
        self.last_legacy_shadow_observation: LegacyShadowObservation | None = None
        self._last_canonical_execution: CanonicalRefreshExecution | None = None
        self.last_continuity_decision: ContinuityDecision | None = None
        self.last_repair_result: RepairExecutionResult | None = None
        self.policy = SchedulePolicy(calendar)

    def run_due_once(self, now: datetime) -> AutomationOutcome:
        self._last_canonical_execution = None
        local = now.astimezone(SHANGHAI)
        operation_policy = self.policy.for_snapshot(_calendar_snapshot(self.calendar))
        published = self.store.published_refresh()
        current = self.store.scheduler_state()
        decision = operation_policy.decide(
            local,
            published_as_of=published.requested_date if published else None,
            state=current,
        )
        _log_event(
            logging.INFO,
            "market_refresh_decision",
            action=decision.action,
            refresh_state=decision.refresh_state,
            target_session=decision.target_session,
            next_run_at=decision.next_run_at,
        )
        repair_result = self._plan_continuity(decision, local, operation_policy)
        if repair_result is not None and repair_result.status != "skipped":
            # A repair must never rewrite the daily freshness state.  Return the prior
            # state (or an unsaved projection when no state exists) and expose repair evidence
            # only on its dedicated continuity field.  ``result`` remains exclusively owned by
            # the freshness lane, preserving the pre-R2-F1 consumer contract.
            state = current or SchedulerState(
                target_session=decision.target_session,
                refresh_state=decision.refresh_state,
                next_retry_at=decision.next_run_at,
            )
            outcome = AutomationOutcome(
                decision=decision,
                state=state,
                continuity_result=repair_result,
            )
            self._offer_shadow(outcome)
            self._last_canonical_execution = None
            return outcome
        if decision.action != "run":
            state = current or SchedulerState(
                target_session=decision.target_session,
                refresh_state=decision.refresh_state,
                next_retry_at=decision.next_run_at,
            )
            if decision.action == "wait" and current is None:
                self.store.save_scheduler_state(state)
            outcome = AutomationOutcome(decision=decision, state=state)
            if (
                decision.refresh_state == "success"
                and published is not None
                and published.status == "ready"
            ):
                self._run_post_publish(published)
            self._offer_shadow(outcome)
            self._offer_universe_maintenance(outcome, local)
            self._last_canonical_execution = None
            return outcome

        lease = RefreshRunLock(self.lock_path) if self.lock_path is not None else nullcontext()
        try:
            with lease:
                outcome = self._execute_due(decision, current, local, operation_policy)
        except RefreshAlreadyRunning:
            target = decision.target_session
            next_retry = (
                operation_policy.next_retry_after(target, local) if target is not None else None
            )
            state = SchedulerState(
                target_session=target,
                refresh_state="retry_wait" if next_retry else "delayed",
                attempt_count=current.attempt_count if current else 0,
                last_attempt_at=current.last_attempt_at if current else None,
                last_success_at=current.last_success_at if current else None,
                next_retry_at=next_retry,
                calendar_status="confirmed",
                error_code="refresh_already_running",
            )
            self.store.save_scheduler_state(state)
            _log_event(
                logging.WARNING,
                "market_refresh_lock_busy",
                target_session=target,
                next_retry_at=next_retry,
            )
            return AutomationOutcome(decision=decision, state=state)
        # The canonical publication transaction and RefreshRunLock are both complete before
        # after-close or shadow work is offered.  Neither callback can delay this result.
        try:
            if outcome.result is not None and outcome.result.status == "ready":
                self._run_post_publish(outcome.result)
            self._offer_shadow(outcome, self._last_canonical_execution)
            self._offer_universe_maintenance(outcome, local)
        finally:
            self._last_canonical_execution = None
        return outcome

    def _plan_continuity(
        self,
        decision: ScheduleDecision,
        local: datetime,
        policy: SchedulePolicy,
    ):
        if not self.repair_enabled or self.continuity is None:
            return None
        target = decision.target_session
        if target is None:
            return None

        def revalidate() -> ScheduleDecision:
            published = self.store.published_refresh()
            current = self.store.scheduler_state()
            return policy.decide(
                local,
                published_as_of=published.requested_date if published else None,
                state=current,
            )

        if self.repair_executor is not None:
            operation_scanner = self.continuity
            for_snapshot = getattr(operation_scanner, "for_snapshot", None)
            if callable(for_snapshot):
                operation_scanner = for_snapshot(policy.calendar)
            result = self.repair_executor.execute_once(
                freshness=decision,
                latest_expected_session=target,
                repair_enabled=True,
                revalidator=revalidate,
                scanner=operation_scanner,
            )
            self.last_repair_result = result
            self.last_continuity_decision = result.decision
            return result
        self.last_continuity_decision = self.continuity.claim_ready_once(
            freshness=decision,
            latest_expected_session=target,
            repair_enabled=True,
            now=local.astimezone(UTC),
            revalidator=revalidate,
            lease_consumer=None,
        )
        return None

    def _execute_due(
        self,
        decision: ScheduleDecision,
        current: SchedulerState | None,
        local: datetime,
        policy: SchedulePolicy,
    ) -> AutomationOutcome:
        target = decision.target_session
        if target is None:
            state = SchedulerState(
                refresh_state="error",
                calendar_status="unavailable",
                error_code="calendar_unavailable",
            )
            self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)

        try:
            provider_health = self.health_store.provider_health()
        except ProviderHealthError:
            state = self._circuit_wait_state(
                decision,
                current,
                local,
                error_code="PROVIDER_HEALTH_UNAVAILABLE",
                policy=policy,
            )
            self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)
        if provider_health.state != CircuitState.CLOSED:
            return self._handle_open_circuit(
                decision,
                current,
                local,
                provider_health,
                policy=policy,
            )

        attempt_count = (
            current.attempt_count + 1
            if current is not None and current.target_session == target
            else 1
        )
        running = SchedulerState(
            target_session=target,
            refresh_state="running",
            attempt_count=attempt_count,
            last_attempt_at=local,
            last_success_at=current.last_success_at if current else None,
        )
        self.store.save_scheduler_state(running)

        request_key = f"daily:baostock:{target.isoformat()}:all-main-board"
        request_prefix = hashlib.sha256(request_key.encode()).hexdigest()[:12]
        refresh_id = f"{request_prefix}-{uuid.uuid4().hex[:12]}"
        collector = _RefreshObservationCollector(self.health_store, refresh_id)
        refresh_operation = getattr(self.provider, "refresh_operation", None)

        def provider_scope():
            return refresh_operation(refresh_id) if callable(refresh_operation) else nullcontext()

        with transport_observation_sink(collector.record):
            try:
                with provider_scope():
                    provider_sessions = self.provider.trading_dates(target, target)
            except Exception as error:
                collector.resolve_touched_endpoints()
                failure = market_failure_from_exception(error, stage="validate")
                state = self._failed_state(
                    running,
                    local,
                    error_code=failure.failure_class,
                    calendar_status="unavailable",
                    retryable=failure.retryable,
                    policy=policy,
                )
                self.store.save_scheduler_state(state)
                _log_event(
                    logging.ERROR,
                    "market_calendar_validation_failed",
                    failure_stage=failure.failure_stage,
                    failure_class=failure.failure_class,
                    retryable=failure.retryable,
                )
                return AutomationOutcome(decision=decision, state=state)
            if provider_sessions != [target]:
                collector.resolve_touched_endpoints()
                state = running.model_copy(
                    update={
                        "refresh_state": "error",
                        "calendar_status": "conflict",
                        "error_code": "calendar_provider_conflict",
                    }
                )
                self.store.save_scheduler_state(state)
                return AutomationOutcome(decision=decision, state=state)

            if self.canonical_refresh is not None:
                canonical_kwargs = {
                    "trade_date": target,
                    "required_symbols": self.required_symbols(),
                    "request_key": request_key,
                    "run_id": refresh_id,
                    "before_store": collector.resolve_touched_endpoints,
                }
                if getattr(self.canonical_refresh, "_refresh_lock_held", False):
                    canonical_kwargs["_lock_held"] = True
                execution = self.canonical_refresh(
                    **canonical_kwargs,
                )
                self._last_canonical_execution = (
                    execution if isinstance(execution, CanonicalRefreshExecution) else None
                )
                result = (
                    execution.result
                    if isinstance(execution, CanonicalRefreshExecution)
                    else execution
                )
            else:
                with provider_scope():
                    result = run_publication_refresh(
                        self.store,
                        self.provider,
                        trade_date=target,
                        required_symbols=self.required_symbols(),
                        request_key=request_key,
                        run_id=refresh_id,
                        before_store=collector.resolve_touched_endpoints,
                    )
            collector.resolve_touched_endpoints()
        if result.status == "ready":
            state = running.model_copy(
                update={
                    "refresh_state": "success",
                    "last_success_at": result.completed_at,
                    "calendar_status": "confirmed",
                    "next_retry_at": None,
                    "error_code": None,
                }
            )
        else:
            state = self._failed_state(
                running,
                local,
                error_code=result.failure_class
                or ("publication_incomplete" if result.status == "partial" else "internal"),
                calendar_status="confirmed",
                retryable=result.retryable is True,
                policy=policy,
            )
        self.store.save_scheduler_state(state)
        return AutomationOutcome(decision=decision, state=state, result=result)

    def _offer_shadow(
        self,
        outcome: AutomationOutcome,
        execution: CanonicalRefreshExecution | None = None,
    ) -> None:
        """Offer a durable shadow handoff after the canonical lock, with zero wait."""
        try:
            if (
                execution is not None
                and execution.legacy_shadow_input is not None
                and isinstance(self.market_universe_mode, str)
                and self.market_universe_mode.strip().casefold() == "shadow"
            ):
                snapshot = execution.legacy_shadow_input
                self.last_legacy_shadow_observation = observe_legacy_request_universe(
                    snapshot, execution.builder_outcome, execution.builder_diagnostic, None
                )
                observer = getattr(self.shadow_handoff, "offer_legacy", None)
                if self.shadow_handoff is not None and callable(observer):
                    observer(
                        LegacyShadowHandoff(
                            trade_date=snapshot.inspection.trade_date,
                            builder_outcome=execution.builder_outcome,
                            diagnostic=self.last_legacy_shadow_observation,
                        )
                    )
            if self.shadow_handoff is None:
                return
            self.shadow_handoff.offer(outcome, wait_budget=0, nonblocking=True)
        except RefreshAlreadyRunning:
            # A contended canonical run is never retried or handed to the shadow lane.
            return
        except Exception:
            _log_event(
                logging.WARNING, "shadow_handoff_dropped", outcome_id=_safe_outcome_id(outcome)
            )

    def _offer_universe_maintenance(self, outcome: AutomationOutcome, now: datetime) -> None:
        """Offer exactly one lowest-priority maintenance hook for an eligible tick."""
        if not self.market_universe_maintenance_enabled:
            self.last_universe_result = None
            self.last_universe_decision = UniverseMaintenanceDecision(
                action="none", reason_code="NONE", provider_requests=0
            )
            return
        if outcome.continuity_result is not None or outcome.state.refresh_state in {
            "error",
            "retry_wait",
            "running",
        }:
            self.last_universe_decision = UniverseMaintenanceDecision(
                action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
            )
            return
        decision = classify_universe_mode(self.environment, self.market_universe_mode)
        self.last_universe_decision = decision
        self.last_universe_result = None
        if decision.action != "run" or self.universe_post_success_hook is None:
            return
        context = None
        execution = self._last_canonical_execution
        if (
            outcome.result is not None
            and outcome.result.status == "ready"
            and execution is not None
        ):
            consumer = getattr(self.canonical_refresh, "consume_universe_publication_context", None)
            if not callable(consumer):
                self.last_universe_decision = UniverseMaintenanceDecision(
                    action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
                )
                return
            candidate_context = consumer(
                execution.result.run_id,
                execution.result.requested_date.isoformat(),
            )
            if isinstance(candidate_context, UniversePublicationContextV1):
                try:
                    validated_context = UniversePublicationContextV1.model_validate(
                        candidate_context.model_dump(mode="python"), strict=False
                    )
                    if (
                        validated_context.run_id == execution.result.run_id
                        and validated_context.trade_date == execution.result.requested_date
                    ):
                        context = validated_context
                except (TypeError, ValueError):
                    context = None
            if context is None:
                self.last_universe_decision = UniverseMaintenanceDecision(
                    action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
                )
                return
        try:
            target = outcome.decision.target_session or now.date()
            self.last_universe_result = self.universe_post_success_hook.offer(
                target.isoformat(), now.astimezone(UTC).isoformat(), context
            )
        except Exception:
            _log_event(logging.WARNING, "universe_maintenance_dropped", outcome="deferred")

    def _handle_open_circuit(
        self,
        decision: ScheduleDecision,
        current: SchedulerState | None,
        local: datetime,
        provider_health,
        *,
        policy: SchedulePolicy,
    ) -> AutomationOutcome:
        target = decision.target_session
        assert target is not None
        lease = None
        if provider_health.state == CircuitState.OPEN:
            for endpoint in provider_health.blocking_endpoints:
                lease = self.health_store.acquire_probe(
                    endpoint,
                    owner=f"scheduler-{uuid.uuid4().hex}",
                )
                if lease is not None:
                    break
        if lease is None:
            state = self._circuit_wait_state(
                decision,
                current,
                local,
                error_code="SKIPPED_CIRCUIT_OPEN",
                policy=policy,
            )
            self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)

        refresh_id = f"probe-{uuid.uuid4().hex}"
        collector = _RefreshObservationCollector(self.health_store, refresh_id)
        normalized_error = None
        try:
            if self.probe_runner is None:
                raise ProviderHealthError("provider probe runner is unavailable")
            with transport_observation_sink(collector.record):
                self.probe_runner.run(
                    lease.endpoint,
                    trade_date=target,
                    refresh_id=refresh_id,
                )
            normalized_error = collector.probe_error(lease.endpoint)
        except Exception as error:
            candidate = getattr(error, "normalized_error", None)
            normalized_error = (
                candidate
                if isinstance(candidate, NormalizedTransportError)
                else collector.probe_error(lease.endpoint)
                or NormalizedTransportError.PROTOCOL_ERROR
            )

        if normalized_error is None:
            self.health_store.resolve_probe(
                lease.lease_id,
                owner=lease.owner,
                success=True,
            )
            error_code = "PROVIDER_PROBE_SUCCEEDED"
        else:
            self.health_store.resolve_probe(
                lease.lease_id,
                owner=lease.owner,
                success=False,
                normalized_error=normalized_error,
            )
            error_code = "PROVIDER_PROBE_FAILED"
        state = self._circuit_wait_state(
            decision,
            current,
            local,
            error_code=error_code,
            policy=policy,
        )
        self.store.save_scheduler_state(state)
        _log_event(
            logging.INFO if normalized_error is None else logging.WARNING,
            "market_provider_probe_finished",
            endpoint=lease.endpoint,
            outcome="success" if normalized_error is None else "error",
            normalized_error=normalized_error,
        )
        return AutomationOutcome(decision=decision, state=state)

    def _circuit_wait_state(
        self,
        decision: ScheduleDecision,
        current: SchedulerState | None,
        now: datetime,
        *,
        error_code: str,
        policy: SchedulePolicy,
    ) -> SchedulerState:
        target = decision.target_session
        next_retry = policy.next_retry_after(target, now) if target is not None else None
        return SchedulerState(
            target_session=target,
            refresh_state="retry_wait" if next_retry is not None else "delayed",
            attempt_count=(
                current.attempt_count
                if current is not None and current.target_session == target
                else 0
            ),
            last_attempt_at=now,
            last_success_at=current.last_success_at if current is not None else None,
            next_retry_at=next_retry,
            calendar_status="confirmed",
            error_code=error_code,
        )

    def _run_post_publish(self, result: RefreshResult) -> None:
        if self.post_publish is None:
            return
        try:
            self.post_publish.run_after_publication(result)
        except Exception:
            _log_event(
                logging.ERROR,
                "after_close_pipeline_failed",
                publication_run_id=result.run_id,
                trade_date=result.requested_date,
                error_code="after_close_pipeline_failed",
            )

    def _failed_state(
        self,
        running: SchedulerState,
        now: datetime,
        *,
        error_code: str,
        calendar_status: Literal["confirmed", "conflict", "unavailable"],
        retryable: bool,
        policy: SchedulePolicy,
    ) -> SchedulerState:
        next_retry = policy.next_retry_after(running.target_session, now) if retryable else None
        return running.model_copy(
            update={
                "refresh_state": (
                    "retry_wait" if next_retry else ("delayed" if retryable else "error")
                ),
                "next_retry_at": next_retry,
                "calendar_status": calendar_status,
                "error_code": error_code,
            }
        )


def collect_required_symbols(user_store) -> set[str]:
    symbols = {item.symbol for item in user_store.list_positions()}
    for watchlist in user_store.list_watchlists():
        symbols.update(item.symbol for item in user_store.list_watchlist_items(watchlist.id))
    return symbols


async def run_automation_loop(
    service: MarketAutomationService,
    stop: asyncio.Event,
    *,
    clock,
    poll_seconds: float = 60,
) -> None:
    """Re-check regularly so process start and wake both perform catch-up."""
    while not stop.is_set():
        try:
            await run_blocking_drained(service.run_due_once, clock())
        except Exception:
            _log_event(
                logging.ERROR,
                "market_refresh_iteration_failed",
                error_code="unexpected_iteration_failure",
            )
        if stop.is_set():
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
        except TimeoutError:
            continue


def get_market_clock():
    return lambda: datetime.now(SHANGHAI)
