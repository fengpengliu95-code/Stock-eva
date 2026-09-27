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
from backend.app.classification.models import ClassificationSnapshot
from backend.app.classification.store import ClassificationReadSnapshot, ClassificationStore
from backend.app.market.baostock import INDEX_SYMBOLS, ProviderBatch
from backend.app.market.baostock_vendor import transport_observation_sink
from backend.app.market.calendar import SHANGHAI, TradingCalendar
from backend.app.market.calendar_generation import (
    CalendarGenerationError,
    CalendarGenerationStore,
    CalendarStoreUnavailable,
)
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
    MarketFailureError,
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
from backend.app.market.universe import (
    RequiredSymbolSnapshotV1,
    SourceRefsV1,
    UniverseAttemptPlanV1,
    UniverseAttemptResultV1,
    UniverseAuthorityBundleV1,
    UniverseHeadV1,
    UniverseInstrumentEvidenceV1,
    UniverseMemberV1,
    UniversePublicationContextV1,
    UniverseSidecarStore,
    UniverseSourceStateV1,
    UniverseStoreUnavailable,
    _source_state_identity,
    _utc_z,
    build_universe_contract,
    canonical_json_bytes,
    classification_snapshot_sha256,
    domain_sha256,
    source_version_digest,
    validate_calendar_authority,
)
from backend.app.storage.dataset import DatasetError, LineageInput, NasMarketStore
from backend.app.user.store import UserStore

logger = logging.getLogger("stock_eva.market.automation")


class _UniverseProviderAcquisitionFailure(ProviderHealthError):
    """Sanitized acquisition failure carrying the logical request count."""

    def __init__(self, message: str, *, request_count: int) -> None:
        super().__init__(message)
        self.request_count = request_count


def _calendar_failure_reason(
    error: BaseException | None = None,
    authority: object | None = None,
) -> str:
    """Map calendar control failures without exposing source exception text."""
    if isinstance(error, CalendarStoreUnavailable):
        return "CONTROL_STATE_UNAVAILABLE"
    if isinstance(error, CalendarGenerationError):
        return (
            "CALENDAR_CONFLICT" if "conflict" in str(error).casefold() else "CALENDAR_UNAVAILABLE"
        )
    reason = getattr(getattr(authority, "read_result", None), "reason", None)
    if isinstance(reason, str) and "conflict" in reason.casefold():
        return "CALENDAR_CONFLICT"
    return "CALENDAR_UNAVAILABLE"


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
            trade_date=self.trade_date,
            main_board_count=self.main_board_count,
            shanghai_count=self.shanghai_count,
            shenzhen_count=self.shenzhen_count,
            total_expected_count=self.total_expected_count,
            metadata_provider_requests=self.metadata_provider_requests,
            observed_symbol_count=len(symbols),
            observed_symbols_sha256=_symbol_set_sha256(symbols),
        )


@dataclass(frozen=True)
class LegacyPreflightSnapshot:
    """Bounded builder diagnostic; raw symbols never cross the callback boundary."""

    trade_date: date
    main_board_count: int
    shanghai_count: int
    shenzhen_count: int
    total_expected_count: int
    metadata_provider_requests: int
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
    """Bounded additive handoff; raw provider symbols stay inside the callback."""

    result: RefreshResult
    builder_outcome: Literal["accepted", "rejected"] = "rejected"
    builder_diagnostic: LegacyBuilderDiagnostic | None = None
    legacy_shadow_observation: "LegacyShadowObservation | None" = None

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
        raise MarketFailureError(
            MarketFailure(
                failure_stage="validate",
                failure_class="universe",
                retryable=True,
            )
        )
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


class UniverseMaintenanceService:
    """Concrete, bounded writer for the post-success Universe maintenance lane.

    All authority inputs are supplied by strict readers.  The service owns the
    sidecar claim/result/promotion lifecycle; it intentionally has no generic
    promoter callback, which prevents a truthy or partially-built object from
    becoming Universe authority.
    """

    def __init__(
        self,
        *,
        sidecar: UniverseSidecarStore,
        calendar_store: CalendarGenerationStore,
        classification_store: ClassificationStore,
        user_store: UserStore,
        lock_path: Path | None,
        provider_factory: Callable[..., object] | None,
        evidence_root: Path | None = None,
        context_validator: Callable[..., object] | None = None,
        reviewed_authority: object | None = None,
        interval_seconds: int = 86400,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.sidecar = sidecar
        self.calendar_store = calendar_store
        self.classification_store = classification_store
        self.user_store = user_store
        self.lock_path = lock_path
        self.provider_factory = provider_factory
        self.evidence_root = Path(evidence_root) if evidence_root is not None else None
        self.context_validator = context_validator
        self.reviewed_authority = reviewed_authority
        self.interval_seconds = interval_seconds
        self.clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def _result(
        *,
        attempt_id: str,
        status: Literal["blocked", "deferred", "failed", "succeeded", "ATTEMPT_INDETERMINATE"],
        reason: str,
        request_count: int,
        source_state_id: str | None,
        finished_at: datetime,
    ) -> UniverseAttemptResultV1:
        finished_at = finished_at.astimezone(UTC)
        preimage = {
            "attempt_id": attempt_id,
            "terminal_status": status,
            "classification_request_count": request_count,
            "reason_code": reason,
            "source_state_id": source_state_id,
            "finished_at": _utc_z(finished_at),
        }
        return UniverseAttemptResultV1(
            attempt_id=attempt_id,
            terminal_status=status,
            classification_request_count=request_count,
            reason_code=reason,
            source_state_id=source_state_id,
            finished_at=finished_at,
            result_sha256=domain_sha256("stock-eva/r2f4.2/universe-attempt-result/v1", preimage),
        )

    @staticmethod
    def _preflight_digest(snapshot: ClassificationReadSnapshot) -> str:
        generation = snapshot.generation
        if generation is None:
            projection = {
                "provider_id": "baostock",
                "adapter_version": "r2f4.2-universe.v1",
                "endpoint_contract_version": "r2f4-endpoints.v1",
                "classification": None,
                "classification_snapshot_sha256": None,
                "semantic_mapping_sha256": None,
                "instrument_evidence": [],
            }
        else:
            observed_at = getattr(generation, "observed_at", None)
            if isinstance(observed_at, datetime):
                try:
                    observed_text = _utc_z(observed_at)
                except ValueError:
                    observed_text = None
            else:
                # Invalid or absent timestamps are represented as an explicit null in
                # the blocked-attempt identity; no non-canonical time enters a hash.
                observed_text = None
            projection = {
                "provider_id": "baostock",
                "adapter_version": "r2f4.2-universe.v1",
                "endpoint_contract_version": "r2f4-endpoints.v1",
                "classification": {
                    "source": getattr(generation, "source", None),
                    "source_version": getattr(generation, "source_version", None),
                    "sequence": getattr(generation, "sequence", None),
                    "observed_at": observed_text,
                    "source_date_semantics": getattr(generation, "source_date_semantics", None),
                },
                "classification_snapshot_sha256": None,
                "semantic_mapping_sha256": None,
                "instrument_evidence": [],
            }
        return domain_sha256("stock-eva/r2f4.2/universe-source-version/v1", projection)

    @staticmethod
    def _reviewed_preflight_digest(
        snapshot: ClassificationReadSnapshot,
        authority: UniverseAuthorityBundleV1,
    ) -> str:
        generation = snapshot.generation
        if generation is None:
            return UniverseMaintenanceService._preflight_digest(snapshot)
        evidence = authority.evidence
        refs = SourceRefsV1(
            calendar_generation_id="0" * 32,
            calendar_sha256="0" * 64,
            classification_generation_id=authority.classification_generation_id,
            classification_generation_sequence=authority.classification_generation_sequence,
            classification_source=authority.classification_source,
            classification_source_version=authority.classification_source_version,
            classification_source_snapshot_date=authority.classification_source_snapshot_date,
            classification_observed_at=authority.classification_observed_at,
            classification_snapshot_sha256=authority.classification_snapshot_sha256,
            required_symbol_snapshot_id="0" * 32,
            required_symbol_snapshot_sha256="0" * 64,
            instrument_evidence_ids=tuple(sorted(item.evidence_id for item in evidence)),
            semantic_mapping_sha256=authority.mapping.mapping_sha256,
            source_version_digest="0" * 64,
            provider_id="baostock",
            trade_date=generation.source_snapshot_date,
            exact_pit_cutoff=generation.observed_at,
            source_date_semantics="source_observed",
        )
        return source_version_digest(refs, evidence)

    @staticmethod
    def _authority_binds_generation(
        authority: UniverseAuthorityBundleV1,
        generation: object,
    ) -> bool:
        return (
            authority.classification_generation_id == getattr(generation, "generation_id", None)
            and authority.classification_generation_sequence
            == getattr(generation, "sequence", None)
            and authority.classification_source == getattr(generation, "source", None)
            and authority.classification_source_version
            == getattr(generation, "source_version", None)
            and authority.classification_source_snapshot_date
            == getattr(generation, "source_snapshot_date", None)
            and authority.classification_observed_at == getattr(generation, "observed_at", None)
            and authority.source_date_semantics
            == getattr(generation, "source_date_semantics", None)
            and authority.classification_snapshot_sha256
            == classification_snapshot_sha256(authority.evidence)
        )

    def _current_head_contract(self) -> tuple[UniverseHeadV1 | None, object | None]:
        """Read one strict current head; permit only a proven empty sidecar."""
        try:
            return self.sidecar.read_verified_snapshot()
        except UniverseStoreUnavailable:
            path = getattr(self.sidecar, "path", None)
            if isinstance(path, Path) and not path.exists():
                return None, None
            try:
                if self.sidecar.read_head() is None:
                    return None, None
            except UniverseStoreUnavailable:
                raise
            raise

    def _is_due(
        self,
        *,
        trade_date: date,
        now: datetime,
        source_digest: str,
        required_snapshot: RequiredSymbolSnapshotV1,
    ) -> bool:
        """Apply source/user/cadence preflight before claiming or acquiring."""
        _head, contract = self._current_head_contract()
        if contract is None or contract.trade_date != trade_date:
            return True
        latest_source = self.sidecar.read_latest_source_state(trade_date)
        if latest_source is None:
            raise UniverseStoreUnavailable("universe source state is missing")
        if (
            latest_source.source_version_digest != source_digest
            or contract.source_refs.source_version_digest != source_digest
            or contract.source_refs.required_symbol_snapshot_sha256
            != required_snapshot.snapshot_sha256
        ):
            return True
        terminal = self.sidecar.read_latest_terminal_finished_at(trade_date)
        if terminal is None:
            return True
        terminal = terminal.astimezone(UTC)
        current = now.astimezone(UTC)
        if current < terminal:
            raise UniverseStoreUnavailable("universe terminal time is in the future")
        return (current - terminal).total_seconds() >= self.interval_seconds

    def _source_digest_for_plan(self, snapshot: ClassificationReadSnapshot) -> str:
        """Return the source identity that can be proven before acquisition.

        A dynamic reviewed authority must publish the same digest from its
        immutable mapping/evidence registry before it is allowed to acquire a
        provider snapshot.  Falling back to a made-up digest would let a
        request run and only fail after the network call, defeating the
        durable preflight contract.
        """
        authority = self.reviewed_authority
        if isinstance(authority, UniverseAuthorityBundleV1):
            return self._reviewed_preflight_digest(snapshot, authority)
        if authority is None:
            return self._preflight_digest(snapshot)
        value = getattr(authority, "preflight_source_version_digest", None)
        if callable(value):
            value = value(snapshot=snapshot)
        if value is None:
            value = getattr(authority, "source_version_digest", None)
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ProviderHealthError("reviewed source preflight is unavailable")
        return value

    @staticmethod
    def _operation_day(now: datetime) -> date:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("maintenance timestamp is not timezone aware")
        return now.astimezone(SHANGHAI).date()

    def _validated_context(
        self,
        trade_date: date,
        context: UniversePublicationContextV1 | None,
    ) -> UniversePublicationContextV1 | None:
        if context is None:
            context = self.sidecar.read_latest_publication_context(trade_date)
            if context is None:
                return None
        if not isinstance(context, UniversePublicationContextV1):
            raise UniverseStoreUnavailable("publication context is not sealed")
        if context.trade_date != trade_date:
            raise UniverseStoreUnavailable("publication context date mismatch")
        if not callable(self.context_validator):
            raise UniverseStoreUnavailable("publication context validator is unavailable")
        checked = self.context_validator(
            context,
            expected_run_id=context.run_id,
            expected_trade_date=trade_date,
        )
        if checked is not context and not isinstance(checked, UniversePublicationContextV1):
            raise UniverseStoreUnavailable("publication context validation failed")
        return context

    def _authority_from_provider(
        self,
        *,
        trade_date: date,
        refresh_id: str,
    ) -> tuple[UniverseAuthorityBundleV1 | None, int]:
        """Acquire one reviewed bundle, returning the exact logical request count."""
        if isinstance(self.reviewed_authority, UniverseAuthorityBundleV1):
            return self.reviewed_authority, 0
        if self.provider_factory is None or self.reviewed_authority is None:
            return None, 0
        request_count = 0
        try:
            provider = self.provider_factory(max_attempts=1)
            provider_attempts = getattr(provider, "max_attempts", None)
            if provider_attempts is None:
                provider_attempts = getattr(
                    getattr(provider, "session", None), "max_attempts", None
                )
            if provider_attempts != 1:
                raise ProviderHealthError("universe classification retry policy is invalid")
            fetch = getattr(provider, "fetch", None)
            if not callable(fetch):
                raise ProviderHealthError("universe classification provider is unavailable")
            request_count = 1
            snapshot = fetch(trade_date)
            if not isinstance(snapshot, ClassificationSnapshot):
                raise ProviderHealthError("universe classification snapshot is invalid")
            admit = getattr(self.reviewed_authority, "admit", None)
            if not callable(admit):
                raise ProviderHealthError("reviewed classification authority is unavailable")
            authority = admit(snapshot, trade_date=trade_date, refresh_id=refresh_id)
            if not isinstance(authority, UniverseAuthorityBundleV1):
                raise ProviderHealthError("reviewed classification authority is invalid")
            return authority, request_count
        except Exception as error:
            if isinstance(error, _UniverseProviderAcquisitionFailure):
                raise
            raise _UniverseProviderAcquisitionFailure(
                "universe classification acquisition failed", request_count=request_count
            ) from error

    @staticmethod
    def _members(
        evidence: tuple[UniverseInstrumentEvidenceV1, ...],
        required: RequiredSymbolSnapshotV1,
        trade_date: date,
    ) -> tuple[UniverseMemberV1, ...]:
        required_roles = dict(required.symbols)
        members: list[UniverseMemberV1] = []
        seen_symbols: set[str] = set()
        seen_security_ids: set[str] = set()
        for item in evidence:
            if item.symbol in seen_symbols or item.security_id in seen_security_ids:
                raise ValueError("UNIVERSE_IDENTITY_CONFLICT")
            seen_symbols.add(item.symbol)
            seen_security_ids.add(item.security_id)
            roles: set[str] = set()
            if (
                item.security_type == "stock"
                and item.board == "main"
                and item.list_date is not None
                and item.list_date <= trade_date
                and (item.delist_date is None or trade_date < item.delist_date)
            ):
                roles.add("effective_main_board")
            roles.update(required_roles.get(item.symbol, ()))
            if item.index_role == "required_index":
                roles.add("required_index")
            if not roles:
                if item.exclusion_reason:
                    continue
                raise ValueError("UNIVERSE_MISSING_SYMBOL")
            if item.index_role == "required_index" and item.expected_trading_state != "trading":
                raise ValueError("REQUIRED_INDEX_NOT_TRADING")
            members.append(
                UniverseMemberV1(
                    symbol=item.symbol,
                    security_id=item.security_id,
                    member_kind=item.member_kind_for_evidence(),
                    scope_roles=tuple(sorted(roles)),
                    exchange=item.exchange,
                    board=item.board,
                    list_date=item.list_date,
                    delist_date=item.delist_date,
                    expected_trading_state=item.expected_trading_state or "unknown",
                    st_state=item.st_state,
                    state_source=item.source_schema,
                    effective_from=item.list_date,
                    effective_to=item.delist_date,
                    instrument_evidence_id=item.evidence_id,
                    exclusion_reason=item.exclusion_reason,
                )
            )
        required_symbols = set(required_roles)
        if not required_symbols.issubset(seen_symbols):
            raise ValueError("REQUIRED_SYMBOL_INVALID")
        if set(INDEX_SYMBOLS) - seen_symbols:
            raise ValueError("REQUIRED_INDEX_NOT_TRADING")
        return tuple(sorted(members, key=lambda value: value.symbol))

    def __call__(
        self,
        *,
        trade_date: str,
        now: str,
        context: UniversePublicationContextV1 | None,
        sidecar: object,
    ) -> UniverseHookResult:
        request_count = 0
        try:
            target = date.fromisoformat(trade_date)
            current = datetime.fromisoformat(now.replace("Z", "+00:00"))
            if current.tzinfo is None or current.utcoffset() is None:
                raise ValueError("maintenance timestamp is not timezone aware")
            if sidecar is not self.sidecar:
                raise UniverseStoreUnavailable("universe sidecar binding mismatch")
            if self.lock_path is None or not isinstance(self.lock_path, Path):
                return UniverseHookResult(
                    status="DEFER", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
                )
            with RefreshRunLock(self.lock_path):
                sealed = self._validated_context(target, context)
                if sealed is None:
                    return UniverseHookResult(
                        status="DEFER", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
                    )
                cutoff = current.astimezone(UTC)
                classification = self.classification_store.read_snapshot(
                    target, known_at=cutoff, include_securities=True
                )
                if not isinstance(classification, ClassificationReadSnapshot):
                    raise UniverseStoreUnavailable("classification snapshot is unavailable")
                generation = classification.generation
                observed_at = getattr(generation, "observed_at", None)
                generation_after_cutoff = bool(
                    generation is not None
                    and (
                        not isinstance(observed_at, datetime)
                        or observed_at.tzinfo is None
                        or observed_at.utcoffset() is None
                        or observed_at.astimezone(UTC) > cutoff
                        or not isinstance(getattr(generation, "source_snapshot_date", None), date)
                        or getattr(generation, "source_snapshot_date", target) > target
                    )
                )
                source_preflight_valid = True
                authority_preflight_valid = True
                if (
                    isinstance(self.reviewed_authority, UniverseAuthorityBundleV1)
                    and generation is not None
                    and not generation_after_cutoff
                ):
                    authority_preflight_valid = self._authority_binds_generation(
                        self.reviewed_authority, generation
                    )
                if (
                    generation is None
                    or generation.source_date_semantics != "source_observed"
                    or generation_after_cutoff
                ):
                    source_digest = self._preflight_digest(classification)
                else:
                    try:
                        source_digest = self._source_digest_for_plan(classification)
                    except Exception:
                        # Reserve and close the operation-day slot even when the
                        # reviewed mapping/source projection cannot be proven.  The
                        # fallback identity is never used for acquisition or publish.
                        source_digest = self._preflight_digest(classification)
                        source_preflight_valid = False
                required = None
                if (
                    generation is not None
                    and generation.source_date_semantics == "source_observed"
                    and not generation_after_cutoff
                ):
                    try:
                        captured = self.user_store.capture_required_symbol_snapshot_existing()
                        required = RequiredSymbolSnapshotV1.from_user_capture(captured)
                    except Exception:
                        return UniverseHookResult(
                            status="DEFER",
                            reason_code="CONTROL_STATE_UNAVAILABLE",
                            provider_requests=0,
                        )
                    if (
                        source_preflight_valid
                        and authority_preflight_valid
                        and not self._is_due(
                            trade_date=target,
                            now=current,
                            source_digest=source_digest,
                            required_snapshot=required,
                        )
                    ):
                        return UniverseHookResult(
                            status="DEFER", reason_code="NONE", provider_requests=0
                        )
                operation_day = self._operation_day(current)
                plan_values = {
                    "attempt_id": domain_sha256(
                        "stock-eva/r2f4.2/universe-attempt-id/v1",
                        {
                            "trade_date": target.isoformat(),
                            "operation_day": operation_day.isoformat(),
                            "refresh_id": sealed.run_id,
                            "source_version_digest": source_digest,
                        },
                    )[:32],
                    "hook_kind": "universe_post_success",
                    "refresh_id": sealed.run_id,
                    "canonical_run_id": sealed.run_id,
                    "source_version_digest": source_digest,
                    "trade_date": target,
                    "operation_day": operation_day,
                    "attempt_status": "RUNNING",
                    "request_budget": 1,
                    "classification_max_attempts": 1,
                    "created_at": current.astimezone(UTC),
                }
                dedup_preimage = {
                    "trade_date": target.isoformat(),
                    "operation_day": operation_day.isoformat(),
                    "canonical_run_id": sealed.run_id,
                    "source_version_digest": source_digest,
                }
                plan_preimage = {
                    **dedup_preimage,
                    "hook_kind": "universe_post_success",
                    "refresh_id": sealed.run_id,
                    "request_budget": 1,
                    "classification_max_attempts": 1,
                    "created_at": _utc_z(current),
                    "attempt_status": "RUNNING",
                }
                plan_values["dedup_key"] = domain_sha256(
                    "stock-eva/r2f4.2/universe-attempt-dedup/v1", dedup_preimage
                )
                plan_values["planned_sha256"] = domain_sha256(
                    "stock-eva/r2f4.2/universe-attempt-plan/v1", plan_preimage
                )
                plan = UniverseAttemptPlanV1(**plan_values)
                claim = self.sidecar.record_publication_context_and_claim(sealed, plan)
                if not claim.claimed:
                    return UniverseHookResult(
                        status="DEFER", reason_code="NONE", provider_requests=0
                    )
                if generation_after_cutoff:
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="blocked",
                        reason="PIT_CUTOFF_VIOLATION",
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="BLOCKED", reason_code=result.reason_code, provider_requests=0
                    )
                if not source_preflight_valid:
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="blocked",
                        reason="BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED",
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="BLOCKED", reason_code=result.reason_code, provider_requests=0
                    )
                if not authority_preflight_valid:
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason="UNIVERSE_SOURCE_VERSION_CHANGED",
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="BLOCKED", reason_code=result.reason_code, provider_requests=0
                    )
                try:
                    calendar_authority = self.calendar_store.read_authority()
                except Exception as error:
                    reason = _calendar_failure_reason(error)
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed" if reason == "CONTROL_STATE_UNAVAILABLE" else "blocked",
                        reason=reason,
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="DEFER" if reason == "CONTROL_STATE_UNAVAILABLE" else "BLOCKED",
                        reason_code=result.reason_code,
                        provider_requests=0,
                    )
                try:
                    calendar_generation = calendar_authority.read_result.generation
                    calendar_hash = (
                        calendar_generation.generation_sha256
                        if calendar_generation is not None
                        else ""
                    )
                    validate_calendar_authority(
                        calendar_authority,
                        trade_date=target,
                        calendar_generation_id=calendar_hash[:32],
                        calendar_sha256=calendar_hash,
                        exact_pit_cutoff=current,
                    )
                except Exception as error:
                    reason = _calendar_failure_reason(error, calendar_authority)
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed" if reason == "CONTROL_STATE_UNAVAILABLE" else "blocked",
                        reason=reason,
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="DEFER" if reason == "CONTROL_STATE_UNAVAILABLE" else "BLOCKED",
                        reason_code=result.reason_code,
                        provider_requests=0,
                    )
                if generation is None:
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="blocked",
                        reason="CLASSIFICATION_UNAVAILABLE",
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="BLOCKED", reason_code=result.reason_code, provider_requests=0
                    )
                if generation.source_date_semantics != "source_observed":
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="blocked",
                        reason="BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED",
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="BLOCKED", reason_code=result.reason_code, provider_requests=0
                    )
                if self.reviewed_authority is None:
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="blocked",
                        reason="BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED",
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(result)
                    return UniverseHookResult(
                        status="BLOCKED", reason_code=result.reason_code, provider_requests=0
                    )
                if isinstance(
                    self.reviewed_authority, UniverseAuthorityBundleV1
                ) and not self._authority_binds_generation(self.reviewed_authority, generation):
                    failed = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason="UNIVERSE_SOURCE_VERSION_CHANGED",
                        request_count=0,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(failed)
                    return UniverseHookResult(
                        status="BLOCKED",
                        reason_code=failed.reason_code,
                        provider_requests=0,
                    )
                try:
                    authority, request_count = self._authority_from_provider(
                        trade_date=target, refresh_id=sealed.run_id
                    )
                except _UniverseProviderAcquisitionFailure as error:
                    request_count = error.request_count
                    failed = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason="CLASSIFICATION_UNAVAILABLE",
                        request_count=request_count,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(failed)
                    return UniverseHookResult(
                        status="BLOCKED",
                        reason_code=failed.reason_code,
                        provider_requests=request_count,
                    )
                except Exception:
                    failed = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason="CLASSIFICATION_UNAVAILABLE",
                        request_count=request_count,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(failed)
                    return UniverseHookResult(
                        status="BLOCKED",
                        reason_code=failed.reason_code,
                        provider_requests=request_count,
                    )
                if authority is None:
                    raise ProviderHealthError("reviewed authority is unavailable")
                if not self._authority_binds_generation(authority, generation):
                    failed = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason="UNIVERSE_SOURCE_VERSION_CHANGED",
                        request_count=request_count,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(failed)
                    return UniverseHookResult(
                        status="BLOCKED",
                        reason_code=failed.reason_code,
                        provider_requests=request_count,
                    )
                if required is None:
                    failed = self._result(
                        attempt_id=plan.attempt_id,
                        status="blocked",
                        reason="USER_STORE_UNAVAILABLE",
                        request_count=request_count,
                        source_state_id=None,
                        finished_at=current,
                    )
                    self.sidecar.record_attempt_result(failed)
                    return UniverseHookResult(
                        status="BLOCKED",
                        reason_code=failed.reason_code,
                        provider_requests=request_count,
                    )
                members = self._members(authority.evidence, required, target)
                generation = classification.generation
                cutoff = current.astimezone(UTC)
                refs = SourceRefsV1(
                    calendar_generation_id=calendar_authority.head_generation_sha256[:32]
                    if calendar_authority.head_generation_sha256
                    else "",
                    calendar_sha256=calendar_authority.head_generation_sha256 or "0" * 64,
                    classification_generation_id=generation.generation_id,
                    classification_generation_sequence=generation.sequence,
                    classification_source=generation.source,
                    classification_source_version=generation.source_version,
                    classification_source_snapshot_date=generation.source_snapshot_date,
                    classification_observed_at=generation.observed_at,
                    classification_snapshot_sha256=classification_snapshot_sha256(
                        authority.evidence
                    ),
                    required_symbol_snapshot_id=required.snapshot_id,
                    required_symbol_snapshot_sha256=required.snapshot_sha256,
                    instrument_evidence_ids=tuple(
                        sorted(item.evidence_id for item in authority.evidence)
                    ),
                    semantic_mapping_sha256=authority.mapping.mapping_sha256,
                    source_version_digest="0" * 64,
                    provider_id="baostock",
                    trade_date=target,
                    exact_pit_cutoff=cutoff,
                    source_date_semantics=authority.source_date_semantics,
                )
                refs = refs.model_copy(
                    update={
                        "source_version_digest": source_version_digest(refs, authority.evidence)
                    }
                )
                source_state_id, source_state_sha = _source_state_identity(refs)
                source_state = UniverseSourceStateV1(
                    source_state_id=source_state_id,
                    source_state_sha256=source_state_sha,
                    trade_date=target,
                    provider_id="baostock",
                    source_version_digest=refs.source_version_digest,
                    source_refs_json=canonical_json_bytes(
                        refs.model_dump(mode="json", warnings="error")
                    ).decode(),
                    verified_at=cutoff,
                )
                head = self.sidecar.read_head()
                next_sequence = head.sequence + 1 if head is not None else 1
                parent_contract_id = head.contract_id if head is not None else None
                contract = build_universe_contract(
                    trade_date=target,
                    calendar_generation_id=refs.calendar_generation_id,
                    calendar_sha256=refs.calendar_sha256,
                    classification_generation_id=generation.generation_id,
                    classification_generation_sequence=generation.sequence,
                    classification_source=generation.source,
                    classification_source_version=generation.source_version,
                    classification_source_snapshot_date=generation.source_snapshot_date,
                    classification_observed_at=generation.observed_at,
                    exact_pit_cutoff=cutoff,
                    source_refs=refs,
                    members=members,
                    authority_bundle=authority,
                    calendar_authority=calendar_authority,
                    required_user_snapshot=captured,
                    sequence=next_sequence,
                    parent_contract_id=parent_contract_id,
                    created_at=cutoff,
                )
                if (
                    contract.source_state_id != source_state_id
                    or contract.source_state_sha256 != source_state_sha
                ):
                    raise UniverseStoreUnavailable("universe source state identity mismatch")
                result = self._result(
                    attempt_id=plan.attempt_id,
                    status="succeeded",
                    reason="NONE",
                    request_count=request_count,
                    source_state_id=source_state_id,
                    finished_at=cutoff,
                )
                expected_sequence = head.sequence if head is not None else 0
                expected_hash = head.head_sha256 if head is not None else None
                try:
                    promoted = self.sidecar.promote(
                        contract,
                        expected_sequence=expected_sequence,
                        expected_head_sha256=expected_hash,
                        mapping=authority.mapping,
                        evidence=authority.evidence,
                        required_snapshot=captured,
                        authority_bundle=authority,
                        calendar_authority=calendar_authority,
                        attempt_result=result,
                    )
                except UniverseStoreUnavailable as error:
                    reason = (
                        "UNIVERSE_HEAD_CAS_CONFLICT"
                        if "head CAS conflict" in str(error) or "sequence invalid" in str(error)
                        else "UNIVERSE_SOURCE_VERSION_CHANGED"
                        if "attempt lineage mismatch" in str(error)
                        else "CONTROL_STATE_UNAVAILABLE"
                    )
                    failed = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason=reason,
                        request_count=request_count,
                        source_state_id=None,
                        finished_at=cutoff,
                    )
                    self.sidecar.record_attempt_result(failed)
                    return UniverseHookResult(
                        status="BLOCKED" if reason != "CONTROL_STATE_UNAVAILABLE" else "DEFER",
                        reason_code=reason,
                        provider_requests=request_count,
                    )
                if (
                    not isinstance(promoted, UniverseHeadV1)
                    or promoted.sequence != contract.sequence
                ):
                    raise UniverseStoreUnavailable("universe promotion result is invalid")
                return UniverseHookResult(
                    status="PROMOTED", reason_code=None, provider_requests=request_count
                )
        except UniverseStoreUnavailable:
            try:
                if "plan" in locals():
                    failed = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason="CONTROL_STATE_UNAVAILABLE",
                        request_count=request_count,
                        source_state_id=(
                            source_state.source_state_id if "source_state" in locals() else None
                        ),
                        finished_at=current,
                    )
                    if "source_state" in locals():
                        self.sidecar.record_source_state_and_result(source_state, failed)
                    else:
                        self.sidecar.record_attempt_result(failed)
            except Exception:
                pass
            return UniverseHookResult(
                status="DEFER",
                reason_code="CONTROL_STATE_UNAVAILABLE",
                provider_requests=request_count,
            )
        except ValueError as error:
            reason = str(error)
            if reason not in {
                "REQUIRED_INDEX_NOT_TRADING",
                "REQUIRED_SYMBOL_INVALID",
                "UNIVERSE_IDENTITY_CONFLICT",
                "UNIVERSE_MISSING_SYMBOL",
            }:
                reason = "CONTROL_STATE_UNAVAILABLE"
            result = None
            try:
                # A malformed plan or sidecar is intentionally not repaired here;
                # only an already durable claim may receive a terminal result.
                if "plan" in locals():
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason=reason,
                        request_count=request_count,
                        source_state_id=(
                            source_state.source_state_id if "source_state" in locals() else None
                        ),
                        finished_at=current,
                    )
                    if "source_state" in locals():
                        self.sidecar.record_source_state_and_result(source_state, result)
                    else:
                        self.sidecar.record_attempt_result(result)
            except Exception:
                pass
            return UniverseHookResult(
                status="BLOCKED" if reason != "CONTROL_STATE_UNAVAILABLE" else "DEFER",
                reason_code=reason,
                provider_requests=request_count,
            )
        except Exception:
            try:
                if "plan" in locals():
                    result = self._result(
                        attempt_id=plan.attempt_id,
                        status="failed",
                        reason="CONTROL_STATE_UNAVAILABLE",
                        request_count=request_count,
                        source_state_id=(
                            source_state.source_state_id if "source_state" in locals() else None
                        ),
                        finished_at=current,
                    )
                    if "source_state" in locals():
                        self.sidecar.record_source_state_and_result(source_state, result)
                    else:
                        self.sidecar.record_attempt_result(result)
            except Exception:
                pass
            return UniverseHookResult(
                status="DEFER",
                reason_code="CONTROL_STATE_UNAVAILABLE",
                provider_requests=request_count,
            )


class ConcreteUniversePostSuccessHook:
    """Safe, default-closed maintenance adapter for the Universe sidecar.

    The enabled shadow lane is backed exclusively by ``UniverseMaintenanceService``.
    There is no callable runner escape hatch: a misconfigured enabled lane reports
    an explicit control failure and cannot perform provider work.
    """

    def __init__(
        self,
        *,
        environment: str,
        mode: str,
        enabled: bool,
        interval_seconds: int = 86400,
        maintenance_service: UniverseMaintenanceService | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if maintenance_service is not None and not isinstance(
            maintenance_service, UniverseMaintenanceService
        ):
            raise TypeError("maintenance_service must be UniverseMaintenanceService")
        self.environment = environment
        self.mode = mode
        self.enabled = enabled
        self.interval_seconds = interval_seconds
        self.maintenance_service = maintenance_service
        self.clock = clock
        self.last_offer: tuple[str, str] | None = None

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
        if self.maintenance_service is None:
            return UniverseHookResult(
                status="BLOCKED",
                reason_code="CONTROL_STATE_UNAVAILABLE",
                provider_requests=0,
            )
        try:
            current = (
                self.clock()
                if self.clock is not None
                else datetime.fromisoformat(now.replace("Z", "+00:00"))
            )
            if current.tzinfo is None or current.utcoffset() is None:
                raise ValueError("maintenance timestamp is not timezone aware")
            if context is not None and not isinstance(context, UniversePublicationContextV1):
                raise TypeError("publication context is not sealed")
            if context is not None and context.trade_date.isoformat() != trade_date:
                raise ValueError("publication context date mismatch")
            result = self.maintenance_service(
                trade_date=trade_date,
                now=now,
                context=context,
                sidecar=self.maintenance_service.sidecar,
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
    maintenance_service: UniverseMaintenanceService | None = None,
    calendar_store: CalendarGenerationStore | None = None,
    classification_store: ClassificationStore | None = None,
    user_store: UserStore | None = None,
    lock_path: Path | None = None,
    provider_factory: Callable[..., object] | None = None,
    evidence_root: Path | None = None,
    context_validator: Callable[..., object] | None = None,
    reviewed_authority: object | None = None,
    clock: Callable[[], datetime] | None = None,
) -> ConcreteUniversePostSuccessHook:
    strict_service: UniverseMaintenanceService | None = maintenance_service
    provider_is_bound = provider_factory is not None or isinstance(
        reviewed_authority, UniverseAuthorityBundleV1
    )
    if (
        strict_service is None
        and provider_is_bound
        and all(
            item is not None
            for item in (
                sidecar,
                calendar_store,
                classification_store,
                user_store,
                lock_path,
                evidence_root,
                context_validator,
            )
        )
    ):
        strict_service = UniverseMaintenanceService(
            sidecar=sidecar,
            calendar_store=calendar_store,
            classification_store=classification_store,
            user_store=user_store,
            lock_path=lock_path,
            provider_factory=provider_factory,
            evidence_root=evidence_root,
            context_validator=context_validator,
            reviewed_authority=reviewed_authority,
            interval_seconds=interval_seconds,
            clock=clock,
        )
    return ConcreteUniversePostSuccessHook(
        environment=environment,
        mode=mode,
        enabled=enabled,
        interval_seconds=interval_seconds,
        maintenance_service=strict_service,
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
            request_trade_date=snapshot.trade_date,
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
    expected_hash = _symbol_set_sha256(expected)
    # The observer receives only the digest/count projection.  Reconstructing
    # or returning provider symbols here would make the callback's public
    # execution object a second raw-universe channel.
    drifted = (
        snapshot.observed_symbol_count != len(expected)
        or snapshot.observed_symbols_sha256 != expected_hash
    )
    drift = None
    if drifted:
        from backend.app.market.universe import domain_sha256

        drift = domain_sha256(
            "stock-eva/r2f4.2/legacy-shadow-drift/v1",
            {
                "provider_id": "baostock",
                "request_trade_date": snapshot.trade_date.isoformat(),
                "contract_sha256": getattr(contract, "contract_sha256", None),
                "expected_symbol_count": len(expected),
                "expected_symbols_sha256": expected_hash,
                "observed_symbol_count": snapshot.observed_symbol_count,
                "observed_symbols_sha256": snapshot.observed_symbols_sha256,
            },
        )
    return LegacyShadowObservation(
        request_trade_date=snapshot.trade_date,
        contract_sha256=getattr(contract, "contract_sha256", None),
        expected_symbol_count=len(expected),
        observed_symbol_count=snapshot.observed_symbol_count,
        missing_required_symbol_count=None if drifted else 0,
        extra_symbol_count=None if drifted else 0,
        drift_sha256=drift,
        reason_code="LEGACY_SHADOW_DRIFT" if drifted else "NONE",
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
        raise MarketFailureError(
            MarketFailure(
                failure_stage="validate",
                failure_class="universe",
                retryable=True,
            )
        )
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
        request_plan_hash=_digest(
            [item.model_dump(mode="json", warnings="error") for item in requests]
        ),
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
        request: ProviderRequest | None = None
        main_symbols: tuple[str, ...] = ()
        builder_outcome: Literal["accepted", "rejected"] = "rejected"
        builder_diagnostic: LegacyBuilderDiagnostic | None = None
        legacy_shadow_observation: LegacyShadowObservation | None = None
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
            assert request is not None
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
            legacy_shadow_observation = observe_legacy_request_universe(
                preflight, builder_outcome, builder_diagnostic, None
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
            if preflight is not None:
                legacy_shadow_observation = observe_legacy_request_universe(
                    preflight, builder_outcome, builder_diagnostic, None
                )
            execution = CanonicalRefreshExecution(
                result=failure_result(error),
                builder_outcome="rejected",
                builder_diagnostic=builder_diagnostic,
                legacy_shadow_observation=legacy_shadow_observation,
            )
            inspection = None
            preflight = None
            return execution

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
                row_data = row.model_dump(mode="python", warnings="error")
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
                serialized = candidate.model_dump(mode="json", warnings="error")
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
                if adapter.factor_cache is factor_cache:
                    adapter.ensure_factor_snapshot(main_symbols, trade_date)
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
                        [
                            item.model_dump(mode="json", warnings="error")
                            for item in normalized.bars
                        ],
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
                            "created_at": _utc_z(result.completed_at),
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
                        builder_outcome=builder_outcome,
                        builder_diagnostic=builder_diagnostic,
                        legacy_shadow_observation=legacy_shadow_observation,
                    )
                finally:
                    if published_selection is not None:
                        published_selection.close()
                    evidence.close()
            except Exception as error:
                return CanonicalRefreshExecution(
                    result=failure_result(error),
                    builder_outcome=builder_outcome,
                    builder_diagnostic=builder_diagnostic,
                    legacy_shadow_observation=legacy_shadow_observation,
                )
            finally:
                # Raw inspection/request symbols are scoped to the callback only.  The
                # returned execution carries counts and hashes, never this material.
                inspection = None
                preflight = None
                request = None
                main_symbols = ()

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

    def dominant_observed_error(self) -> NormalizedTransportError | None:
        errors = [
            item.normalized_error
            for item in self.observations
            if item.protocol_stage == ProtocolStage.OPERATION and item.normalized_error is not None
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
        return datetime.combine(session, time(16, 0), tzinfo=SHANGHAI)

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
    lineage_input: LineageInput | dict[str, object] | None = None,
) -> RefreshResult:
    """Fetch to canonical staging, validate all gates, then move the pointer."""
    if isinstance(store, NasMarketStore) and lineage_input is None:
        raise DatasetError("source lineage input is required for NAS publication")
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
                if isinstance(store, NasMarketStore):
                    store.save_refresh(
                        [],
                        result,
                        publish=False,
                        lineage_input=lineage_input,
                    )
                else:
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
        if isinstance(store, NasMarketStore):
            if status == "ready" and lineage_input is None:
                raise DatasetError("source lineage input is required for NAS publication")
            store.save_refresh(
                batch.bars if status == "ready" else [],
                result,
                publish=status == "ready",
                lineage_input=lineage_input,
            )
        else:
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
        universe_context_validator: Callable[..., object] | None = None,
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
        self.universe_context_validator = universe_context_validator
        self.last_universe_decision: UniverseMaintenanceDecision | None = None
        self.last_universe_result: UniverseHookResult | None = None
        self.last_legacy_shadow_observation: LegacyShadowObservation | None = None
        self._last_canonical_execution: CanonicalRefreshExecution | None = None
        self.last_continuity_decision: ContinuityDecision | None = None
        self.last_repair_result: RepairExecutionResult | None = None
        self.policy = SchedulePolicy(calendar)

    def run_due_once(self, now: datetime) -> AutomationOutcome:
        self._last_canonical_execution = None
        if not self.market_universe_maintenance_enabled:
            self.last_universe_result = None
            self.last_universe_decision = UniverseMaintenanceDecision(
                action="none", reason_code="NONE", provider_requests=0
            )
        local = now.astimezone(SHANGHAI)
        operation_policy = self.policy.for_snapshot(_calendar_snapshot(self.calendar))
        published = self.store.published_refresh()
        current = self.store.scheduler_state()
        decision = operation_policy.decide(
            local,
            published_as_of=published.requested_date if published else None,
            state=current,
        )
        current = self._reconcile_current_publication(decision, current, published)
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
            if self.market_universe_maintenance_enabled:
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
            if self.market_universe_maintenance_enabled:
                self._offer_universe_maintenance(outcome, local)
        finally:
            self._last_canonical_execution = None
        return outcome

    def _reconcile_current_publication(
        self,
        decision: ScheduleDecision,
        current: SchedulerState | None,
        published: RefreshResult | None,
    ) -> SchedulerState | None:
        if (
            decision.action != "none"
            or decision.refresh_state != "success"
            or decision.target_session is None
            or published is None
            or published.status != "ready"
            or published.requested_date is None
            or published.requested_date < decision.target_session
        ):
            return current
        same_target = current is not None and current.target_session == decision.target_session
        state = SchedulerState(
            target_session=decision.target_session,
            refresh_state="success",
            attempt_count=current.attempt_count if same_target else 0,
            last_attempt_at=current.last_attempt_at if same_target else None,
            last_success_at=published.completed_at,
            next_retry_at=None,
            calendar_status="confirmed",
            error_code=None,
        )
        if state != current:
            self.store.save_scheduler_state(state)
        return state

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
                        lineage_input={"mode": "legacy"},
                    )
            collector.resolve_touched_endpoints()
        if (
            result.status == "error"
            and result.failure_class == "internal"
            and (observed_error := collector.dominant_observed_error()) is not None
        ):
            failure = MarketFailure(
                failure_stage="fetch",
                failure_class=(
                    "transport_timeout"
                    if observed_error == NormalizedTransportError.RECV_TIMEOUT
                    else "rate_limit"
                    if observed_error == NormalizedTransportError.RATE_LIMIT
                    else "transport_connect"
                ),
                retryable=True,
            )
            result = result.model_copy(
                update={
                    "failure_stage": failure.failure_stage,
                    "failure_class": failure.failure_class,
                    "retryable": failure.retryable,
                    "quality_issues": legacy_failure_quality_issues(failure),
                    "error_message": public_failure_message(failure),
                }
            )
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
                and execution.legacy_shadow_observation is not None
                and self.market_universe_maintenance_enabled
                and isinstance(self.environment, str)
                and self.environment.strip().casefold() != "production"
                and isinstance(self.market_universe_mode, str)
                and self.market_universe_mode.strip().casefold() == "shadow"
            ):
                # The canonical callback has already reduced the raw request
                # universe to this count/hash diagnostic while its provider
                # objects were still scoped.  Never retain or reconstruct the
                # symbol list in the automation outcome.
                self.last_legacy_shadow_observation = execution.legacy_shadow_observation
                observer = getattr(self.shadow_handoff, "offer_legacy", None)
                if self.shadow_handoff is not None and callable(observer):
                    observer(
                        LegacyShadowHandoff(
                            trade_date=execution.legacy_shadow_observation.request_trade_date,
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
        # The concrete Universe lane is downstream of the additive canonical
        # context handoff.  A caller that did not provide that callable cannot
        # safely offer maintenance, even on a non-run tick with an old result.
        if self.canonical_refresh is None:
            self.last_universe_decision = UniverseMaintenanceDecision(
                action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
            )
            self.last_universe_result = None
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
        if outcome.result is not None and outcome.result.status == "ready":
            # A plain legacy RefreshResult has no sealed publication context.  It
            # must never be treated as equivalent to the additive execution
            # wrapper: doing so would allow Universe work after a caller bypassed
            # the immutable CandidateStore/evidence handoff.
            if execution is None:
                self.last_universe_decision = UniverseMaintenanceDecision(
                    action="defer", reason_code="CONTROL_STATE_UNAVAILABLE", provider_requests=0
                )
                return
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
                        candidate_context.model_dump(mode="python", warnings="error"), strict=False
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
            validator = self.universe_context_validator
            if callable(validator):
                try:
                    checked = validator(
                        context,
                        expected_run_id=execution.result.run_id,
                        expected_trade_date=execution.result.requested_date,
                    )
                    if checked is not context and not isinstance(
                        checked, UniversePublicationContextV1
                    ):
                        raise TypeError("publication context immutable readback failed")
                except Exception:
                    self.last_universe_decision = UniverseMaintenanceDecision(
                        action="defer",
                        reason_code="CONTROL_STATE_UNAVAILABLE",
                        provider_requests=0,
                    )
                    return
        try:
            target = outcome.decision.target_session or now.date()
            self.last_universe_result = self.universe_post_success_hook.offer(
                target.isoformat(), _utc_z(now), context
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
    replication_drain=None,
) -> None:
    """Re-check regularly so process start and wake both perform catch-up."""
    while not stop.is_set():
        try:
            await run_blocking_drained(service.run_due_once, clock())
            if replication_drain is not None:
                # One bounded claim at most, after the refresh slot. Drain
                # failures are observational and cannot alter canonical
                # refresh success.
                await run_blocking_drained(replication_drain.run_once)
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
