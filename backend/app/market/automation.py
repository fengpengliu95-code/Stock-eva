"""Deterministic after-close publication and scheduling policy."""

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel

from backend.app.market.baostock import INDEX_SYMBOLS
from backend.app.market.baostock_vendor import transport_observation_sink
from backend.app.market.calendar import SHANGHAI, TradingCalendar
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
from backend.app.market.store import MarketStore

logger = logging.getLogger("stock_eva.market.automation")

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
    decision: ScheduleDecision
    state: SchedulerState
    result: RefreshResult | None = None


class ProviderProbeRunner(Protocol):
    def run(self, endpoint: ProviderEndpoint, *, trade_date: date, refresh_id: str) -> None: ...


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

    def __init__(self, calendar: TradingCalendar) -> None:
        self.calendar = calendar

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
        target = self.calendar.latest_expected_session(local)
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
    run_kind: Literal["daily", "backfill"] = "daily",
    before_store: Callable[[], None] | None = None,
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
        batch = provider.fetch(trade_date, symbols=None)
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
        health_store=None,
        probe_runner: ProviderProbeRunner | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self.calendar = calendar
        self.required_symbols = required_symbols
        self.lock_path = lock_path
        self.post_publish = post_publish
        self.health_store = health_store or InMemoryProviderHealthStore()
        self.probe_runner = probe_runner
        self.policy = SchedulePolicy(calendar)

    def run_due_once(self, now: datetime) -> AutomationOutcome:
        local = now.astimezone(SHANGHAI)
        published = self.store.published_refresh()
        current = self.store.scheduler_state()
        decision = self.policy.decide(
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
        if decision.action != "run":
            state = current or SchedulerState(
                target_session=decision.target_session,
                refresh_state=decision.refresh_state,
                next_retry_at=decision.next_run_at,
            )
            if decision.action == "wait" and current is None:
                self.store.save_scheduler_state(state)
            if (
                decision.refresh_state == "success"
                and published is not None
                and published.status == "ready"
            ):
                self._run_post_publish(published)
            return AutomationOutcome(decision=decision, state=state)

        lease = RefreshRunLock(self.lock_path) if self.lock_path is not None else nullcontext()
        try:
            with lease:
                return self._execute_due(decision, current, local)
        except RefreshAlreadyRunning:
            target = decision.target_session
            next_retry = self.policy.next_retry_after(target, local) if target is not None else None
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

    def _execute_due(
        self,
        decision: ScheduleDecision,
        current: SchedulerState | None,
        local: datetime,
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
            )
            self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)
        if provider_health.state != CircuitState.CLOSED:
            return self._handle_open_circuit(
                decision,
                current,
                local,
                provider_health,
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
        provider_scope = (
            refresh_operation(refresh_id) if callable(refresh_operation) else nullcontext()
        )

        with transport_observation_sink(collector.record), provider_scope:
            try:
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

            result = run_publication_refresh(
                self.store,
                self.provider,
                trade_date=target,
                required_symbols=self.required_symbols(),
                request_key=request_key,
                run_id=refresh_id,
                before_store=collector.resolve_touched_endpoints,
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
            self._run_post_publish(result)
        else:
            state = self._failed_state(
                running,
                local,
                error_code=result.failure_class
                or ("publication_incomplete" if result.status == "partial" else "internal"),
                calendar_status="confirmed",
                retryable=result.retryable is True,
            )
        self.store.save_scheduler_state(state)
        return AutomationOutcome(decision=decision, state=state, result=result)

    def _handle_open_circuit(
        self,
        decision: ScheduleDecision,
        current: SchedulerState | None,
        local: datetime,
        provider_health,
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
    ) -> SchedulerState:
        target = decision.target_session
        next_retry = self.policy.next_retry_after(target, now) if target is not None else None
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
    ) -> SchedulerState:
        next_retry = (
            self.policy.next_retry_after(running.target_session, now) if retryable else None
        )
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
            await asyncio.to_thread(service.run_due_once, clock())
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
