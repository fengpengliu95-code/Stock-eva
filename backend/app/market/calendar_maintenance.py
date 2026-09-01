"""Bounded official acquisition and durable promoted-calendar maintenance.

Source admission remains owned by the calendar-generation store.  This module adds only the
policy and one-slot worker that consume that verified authority; it never writes market canonical
data.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from math import isfinite
from typing import Literal
from uuid import uuid4

import httpx

from .baostock import BaoStockError, BaoStockProvider
from .baostock_vendor import transport_observation_sink
from .calendar import SHANGHAI
from .calendar_generation import (
    MAX_BODY_BYTES,
    MAX_SOURCE_BYTES,
    CalendarAttemptAlreadySpent,
    CalendarGenerationError,
    CalendarGenerationStore,
    CalendarMachineDayV1,
    CalendarMachineObservationV1,
    CalendarMaintenanceAttempt,
    CalendarSourceBundleV1,
    CalendarStoreUnavailable,
    body_sha256,
    build_observation,
    build_source_sha256,
    canonical_json_bytes,
    validate_source_bundle,
)
from .calendar_maintenance_health import CalendarMaintenanceHealthStore
from .provider_health import CircuitState, ProviderHealthError
from .provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
    TransportOutcome,
)

Failure = Literal[
    "SOURCE_INVALID",
    "SOURCE_CONFLICT",
    "OFFICIAL_UNAVAILABLE",
    "OFFICIAL_HASH_MISMATCH",
]
ClientFactory = Callable[..., object]
_EXPECTED_TRANSPORT_ERRORS = (httpx.HTTPError, OSError, TimeoutError)
_SUPPORTED_CONTENT_ENCODINGS = frozenset({"identity", "gzip", "deflate"})


@dataclass(frozen=True, slots=True)
class _OfficialFetchResult:
    """Private fetch result; verified bodies are intentionally absent from repr output."""

    failure: Failure | None
    official_requests: int
    _bodies: tuple[bytes, bytes] | None = field(default=None, repr=False)

    @property
    def bodies(self) -> tuple[bytes, bytes] | None:
        return self._bodies


def _unavailable(requests: int) -> _OfficialFetchResult:
    return _OfficialFetchResult("OFFICIAL_UNAVAILABLE", requests)


def _close_client(client: object) -> bool:
    close = client.close
    try:
        close()
    except _EXPECTED_TRANSPORT_ERRORS:
        return False
    return True


def _fetch_body(client: object, url: str) -> tuple[bytes, bool]:
    """Return one bounded decoded body and whether the HTTP response was usable."""
    with client.stream("GET", url) as response:
        if response.status_code != 200:
            return b"", False
        content_encoding = response.headers.get("Content-Encoding")
        if content_encoding is not None:
            encodings = tuple(item.strip().lower() for item in content_encoding.split(","))
            if not encodings or any(item not in _SUPPORTED_CONTENT_ENCODINGS for item in encodings):
                return b"", False
        body = bytearray()
        for chunk in response.iter_bytes():
            if not isinstance(chunk, bytes):
                return b"", False
            if len(body) + len(chunk) > MAX_BODY_BYTES:
                return b"", False
            body.extend(chunk)
        if not body:
            return b"", False
        return bytes(body), True


def _pin_source(
    source: CalendarSourceBundleV1, now: datetime
) -> tuple[CalendarSourceBundleV1, str]:
    """Bound, parse, and admit a source snapshot before any client access."""
    if not isinstance(source, CalendarSourceBundleV1):
        raise CalendarGenerationError("source must be CalendarSourceBundleV1")
    try:
        serialized = canonical_json_bytes(source.model_dump(mode="json", warnings="error"))
    except (TypeError, ValueError) as exc:
        raise CalendarGenerationError("source serialization invalid") from exc
    if len(serialized) > MAX_SOURCE_BYTES:
        raise CalendarGenerationError("source exceeds size limit")
    try:
        pinned = CalendarSourceBundleV1.model_validate_json(serialized)
    except (TypeError, ValueError) as exc:
        raise CalendarGenerationError("source validation invalid") from exc
    admission = validate_source_bundle(pinned, now)
    return pinned, admission.outcome


def fetch_official_calendars(
    source: CalendarSourceBundleV1,
    *,
    now: datetime,
    client_factory: ClientFactory | None = None,
) -> _OfficialFetchResult:
    """Fetch exactly the two already-admitted official calendar URLs.

    The function is synchronous and intentionally returns only a private in-memory result.
    ``client_factory`` is injectable for offline tests; the default creates a fresh, fixed
    policy HTTPX client for each request.
    """
    try:
        pinned_source, admission = _pin_source(source, now)
    except CalendarGenerationError:
        return _OfficialFetchResult("SOURCE_INVALID", 0)

    if admission == "SOURCE_CONFLICT":
        return _OfficialFetchResult("SOURCE_CONFLICT", 0)

    def make_client(**kwargs: object) -> object:
        return httpx.Client(**kwargs)

    factory = make_client if client_factory is None else client_factory
    requests = 0
    bodies: list[bytes] = []
    client_options = {
        "trust_env": False,
        "verify": True,
        "follow_redirects": False,
        "auth": None,
        "headers": {"Accept-Encoding": "gzip, deflate"},
    }

    for schedule in pinned_source.schedules:
        client: object | None = None
        try:
            client = factory(
                timeout=httpx.Timeout(connect=5, read=30, write=5, pool=5), **client_options
            )
        except _EXPECTED_TRANSPORT_ERRORS:
            return _unavailable(requests)

        request_result: _OfficialFetchResult | None = None
        try:
            requests += 1
            try:
                body, usable = _fetch_body(client, schedule.official_url)
            except _EXPECTED_TRANSPORT_ERRORS:
                request_result = _unavailable(requests)
            else:
                if not usable:
                    request_result = _unavailable(requests)
                elif body_sha256(body) != schedule.body_sha256:
                    request_result = _OfficialFetchResult("OFFICIAL_HASH_MISMATCH", requests)
                else:
                    bodies.append(body)
        finally:
            close_ok = _close_client(client)
        if not close_ok:
            return _unavailable(requests)
        if request_result is not None:
            return request_result

    return _OfficialFetchResult(None, requests, (bodies[0], bodies[1]))


MaintenanceOutcome = Literal[
    "STAGED",
    "SOURCE_PENDING",
    "SOURCE_INVALID",
    "SOURCE_CONFLICT",
    "OFFICIAL_UNAVAILABLE",
    "OFFICIAL_HASH_MISMATCH",
    "MACHINE_UNAVAILABLE",
    "MACHINE_CONFLICT",
    "SKIPPED_CIRCUIT_OPEN",
    "ALREADY_ATTEMPTED",
    "PARENT_CHANGED",
    "HISTORY_CHANGE",
    "CALENDAR_AUTHORITY_CHANGED",
    "CONTROL_STATE_UNAVAILABLE",
    "NOT_DUE",
    "PROMOTED",
]


@dataclass(frozen=True, slots=True)
class CalendarMaintenancePlan:
    """Safe, immutable policy output.

    Source payloads and control paths intentionally never cross this public
    boundary.  ``execute`` recomputes its own private decision instead of
    accepting a plan as authority.
    """

    due: bool
    outcome: MaintenanceOutcome
    target_year: int | None = None
    source_sha256: str | None = None
    current_year_ready: bool = False
    next_year_status: Literal["not_due", "pending", "action_required", "confirmed"] = "not_due"
    network_requests: int = 0
    official_requests: int = 0
    machine_requests: int = 0
    writes_calendar_state: bool = False
    canonical_writes: bool = False


@dataclass(frozen=True, slots=True)
class CalendarMaintenanceResult:
    """Sanitized outcome of one bounded maintenance attempt."""

    outcome: MaintenanceOutcome
    due: bool = False
    target_year: int | None = None
    source_sha256: str | None = None
    generation_sha256: str | None = None
    current_year_ready: bool = False
    next_year_status: Literal["not_due", "pending", "action_required", "confirmed"] = "not_due"
    network_requests: int = 0
    official_requests: int = 0
    machine_requests: int = 0
    writes_calendar_state: bool = False
    canonical_writes: bool = False


@dataclass(frozen=True, slots=True)
class _MaintenanceDecision:
    now: datetime
    target_year: int | None
    source: CalendarSourceBundleV1 | None
    source_sha256: str | None
    expected_parent_sha256: str | None
    outcome: MaintenanceOutcome
    due: bool
    current_year_ready: bool
    next_year_status: Literal["not_due", "pending", "action_required", "confirmed"]


_VALID_YEARS = range(1900, 9999)


def _safe_plan(decision: _MaintenanceDecision) -> CalendarMaintenancePlan:
    return CalendarMaintenancePlan(
        due=decision.due,
        outcome=decision.outcome,
        target_year=decision.target_year,
        source_sha256=decision.source_sha256,
        current_year_ready=decision.current_year_ready,
        next_year_status=decision.next_year_status,
    )


def _safe_result(
    decision: _MaintenanceDecision,
    *,
    outcome: MaintenanceOutcome | None = None,
    generation_sha256: str | None = None,
    official_requests: int = 0,
    machine_requests: int = 0,
    writes_calendar_state: bool = False,
) -> CalendarMaintenanceResult:
    return CalendarMaintenanceResult(
        outcome=decision.outcome if outcome is None else outcome,
        due=decision.due,
        target_year=decision.target_year,
        source_sha256=decision.source_sha256,
        generation_sha256=generation_sha256,
        current_year_ready=decision.current_year_ready,
        next_year_status=decision.next_year_status,
        network_requests=official_requests + machine_requests,
        official_requests=official_requests,
        machine_requests=machine_requests,
        writes_calendar_state=writes_calendar_state,
    )


def _aware_now(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CalendarGenerationError("maintenance clock must be timezone-aware")
    return value.astimezone(UTC)


def _validate_target_year(target_year: int | None, current_year: int) -> int | None:
    if target_year is None:
        return None
    if type(target_year) is not int or target_year not in _VALID_YEARS:
        raise CalendarGenerationError("target year is invalid")
    if target_year not in (current_year, current_year + 1):
        raise CalendarGenerationError("target year is outside the maintenance horizon")
    return target_year


def _candidate_source(candidate: object) -> CalendarSourceBundleV1:
    """Revalidate a candidate before using any candidate field as authority."""
    try:
        payload = candidate.canonical_source_bytes
        source = CalendarSourceBundleV1.model_validate_json(payload)
        candidate_hash = candidate.source_sha256
    except (AttributeError, TypeError, ValueError) as exc:
        raise CalendarStoreUnavailable("calendar candidate unavailable") from exc
    if source.source_sha256 != candidate_hash or build_source_sha256(source) != candidate_hash:
        raise CalendarStoreUnavailable("calendar candidate unavailable")
    return source


class CalendarMaintenanceService:
    """Durable, one-slot-per-Shanghai-day calendar maintenance worker."""

    def __init__(
        self,
        store: CalendarGenerationStore,
        health_store: CalendarMaintenanceHealthStore,
        *,
        provider_factory: Callable[..., object] | None = None,
        http_client_factory: ClientFactory | None = None,
        clock: Callable[[], datetime] | None = None,
        socket_timeout_seconds: float = 30.0,
        min_request_interval_seconds: float = 0.5,
    ) -> None:
        if (
            type(socket_timeout_seconds) not in (int, float)
            or not isfinite(float(socket_timeout_seconds))
            or socket_timeout_seconds <= 0
        ):
            raise ValueError("socket timeout must be positive")
        if (
            type(min_request_interval_seconds) not in (int, float)
            or not isfinite(float(min_request_interval_seconds))
            or min_request_interval_seconds < 0
        ):
            raise ValueError("request interval must not be negative")
        self._store = store
        self._health_store = health_store
        self._provider_factory = provider_factory or (lambda **kwargs: BaoStockProvider(**kwargs))
        self._http_client_factory = http_client_factory
        self._clock = clock or (lambda: datetime.now(UTC))
        self._socket_timeout_seconds = float(socket_timeout_seconds)
        self._min_request_interval_seconds = float(min_request_interval_seconds)

    def _read_decision(self, now: datetime, target_year: int | None) -> _MaintenanceDecision:
        local = now.astimezone(SHANGHAI)
        current_year = local.year
        target_year = _validate_target_year(target_year, current_year)
        try:
            control = self._store.read_control()
        except (CalendarStoreUnavailable, OSError):
            return _MaintenanceDecision(
                now,
                target_year,
                None,
                None,
                None,
                "CONTROL_STATE_UNAVAILABLE",
                False,
                False,
                "not_due",
            )
        read_result = getattr(control, "read_result", None)
        calendar = getattr(read_result, "calendar", None)
        if (
            read_result is None
            or getattr(read_result, "status", None) != "ready"
            or calendar is None
        ):
            return _MaintenanceDecision(
                now,
                target_year,
                None,
                None,
                None,
                "CONTROL_STATE_UNAVAILABLE",
                False,
                False,
                "not_due",
            )
        try:
            configs = calendar.configs
            covered_years = {int(year) for year in configs}
            promoted = tuple(control.promoted_sources)
            active: dict[int, str] = {source.year: source.source_sha256 for source in promoted}
            candidates = tuple(control.candidates)
            latest: dict[int, object] = {}
            for candidate in candidates:
                source = _candidate_source(candidate)
                latest[source.year] = candidate
            current_ready = current_year in covered_years

            def inspect(
                year: int,
            ) -> tuple[object | None, CalendarSourceBundleV1 | None, str | None]:
                candidate = latest.get(year)
                if candidate is None:
                    return None, None, None
                source = _candidate_source(candidate)
                return candidate, source, source.source_sha256

            _, current_source, current_hash = inspect(current_year)
            current_candidate = latest.get(current_year)
            if current_candidate is not None and current_source is not None:
                if current_candidate.admission == "quarantined":
                    return _MaintenanceDecision(
                        now,
                        current_year,
                        current_source,
                        current_hash,
                        _expected_parent(control),
                        "SOURCE_CONFLICT",
                        False,
                        current_ready,
                        "not_due",
                    )
                elif active.get(current_year) != current_hash:
                    if target_year in (None, current_year):
                        return self._eligible(
                            now,
                            current_year,
                            current_source,
                            current_hash,
                            control,
                            current_ready,
                            "not_due",
                        )
            if target_year == current_year:
                if current_source is None and not current_ready:
                    return _MaintenanceDecision(
                        now,
                        current_year,
                        None,
                        None,
                        _expected_parent(control),
                        "SOURCE_PENDING",
                        False,
                        False,
                        "not_due",
                    )
                return _MaintenanceDecision(
                    now,
                    current_year,
                    None,
                    None,
                    _expected_parent(control),
                    "NOT_DUE",
                    False,
                    True,
                    "not_due",
                )
            if not current_ready:
                return _MaintenanceDecision(
                    now,
                    current_year,
                    None,
                    None,
                    _expected_parent(control),
                    "SOURCE_PENDING",
                    False,
                    False,
                    "not_due",
                )

            next_year = current_year + 1
            _, next_source, next_hash = inspect(next_year)
            next_candidate = latest.get(next_year)
            next_ready = next_year in covered_years
            next_status: Literal["not_due", "pending", "action_required", "confirmed"]
            if next_candidate is not None and next_candidate.admission == "quarantined":
                next_status = (
                    "not_due"
                    if local.date() < date(current_year, 10, 1) and target_year != next_year
                    else (
                        "pending"
                        if local.date() < date(current_year, 12, 15)
                        else "action_required"
                    )
                )
            elif next_ready and active.get(next_year) == (
                next_hash if next_hash else active.get(next_year)
            ):
                next_status = "confirmed"
            elif local.date() < date(current_year, 10, 1) and target_year != next_year:
                next_status = "not_due"
            elif local.date() < date(current_year, 12, 15):
                next_status = "pending"
            else:
                next_status = "action_required"
            if next_candidate is not None and next_candidate.admission == "quarantined":
                return _MaintenanceDecision(
                    now,
                    next_year,
                    next_source,
                    next_hash,
                    _expected_parent(control),
                    "SOURCE_CONFLICT",
                    False,
                    current_ready,
                    next_status,
                )
            if target_year == next_year or (
                target_year is None and local.date() >= date(current_year, 10, 1)
            ):
                if next_candidate is not None and next_source is not None:
                    if not next_ready or active.get(next_year) != next_hash:
                        return self._eligible(
                            now,
                            next_year,
                            next_source,
                            next_hash,
                            control,
                            current_ready,
                            next_status,
                        )
                if not next_ready:
                    return _MaintenanceDecision(
                        now,
                        next_year,
                        None,
                        None,
                        _expected_parent(control),
                        "SOURCE_PENDING",
                        False,
                        current_ready,
                        next_status,
                    )
            return _MaintenanceDecision(
                now,
                target_year,
                None,
                None,
                _expected_parent(control),
                "NOT_DUE",
                False,
                current_ready,
                next_status,
            )
        except (CalendarStoreUnavailable, CalendarGenerationError):
            return _MaintenanceDecision(
                now,
                target_year,
                None,
                None,
                None,
                "CONTROL_STATE_UNAVAILABLE",
                False,
                False,
                "not_due",
            )

    @staticmethod
    def _eligible(now, target_year, source, source_hash, control, current_ready, next_status):
        slot_date = now.astimezone(SHANGHAI).date()
        if any(
            attempt.target_year == target_year and attempt.slot_date == slot_date
            for attempt in control.attempts
        ):
            return _MaintenanceDecision(
                now,
                target_year,
                source,
                source_hash,
                _expected_parent(control),
                "ALREADY_ATTEMPTED",
                False,
                current_ready,
                next_status,
            )
        return _MaintenanceDecision(
            now,
            target_year,
            source,
            source_hash,
            _expected_parent(control),
            "STAGED",
            True,
            current_ready,
            next_status,
        )

    def plan(
        self, *, now: datetime | None = None, target_year: int | None = None
    ) -> CalendarMaintenancePlan:
        effective = _aware_now(self._clock() if now is None else now)
        decision = self._read_decision(effective, target_year)
        if decision.due:
            try:
                health = self._health_store.provider_health_snapshot()
            except (ProviderHealthError, OSError):
                decision = _MaintenanceDecision(
                    effective,
                    decision.target_year,
                    decision.source,
                    decision.source_sha256,
                    decision.expected_parent_sha256,
                    "CONTROL_STATE_UNAVAILABLE",
                    False,
                    decision.current_year_ready,
                    decision.next_year_status,
                )
                return _safe_plan(decision)
            if health.state != CircuitState.CLOSED:
                decision = _MaintenanceDecision(
                    effective,
                    decision.target_year,
                    decision.source,
                    decision.source_sha256,
                    decision.expected_parent_sha256,
                    "SKIPPED_CIRCUIT_OPEN",
                    False,
                    decision.current_year_ready,
                    decision.next_year_status,
                )
        return _safe_plan(decision)

    def execute(self, *, target_year: int | None = None) -> CalendarMaintenanceResult:
        started_at = _aware_now(self._clock())
        decision = self._read_decision(started_at, target_year)
        if not decision.due:
            if decision.outcome == "STAGED":
                decision = _MaintenanceDecision(
                    started_at,
                    decision.target_year,
                    decision.source,
                    decision.source_sha256,
                    decision.expected_parent_sha256,
                    "CONTROL_STATE_UNAVAILABLE",
                    False,
                    decision.current_year_ready,
                    decision.next_year_status,
                )
            return _safe_result(decision)
        try:
            health = self._health_store.provider_health_snapshot()
        except (ProviderHealthError, OSError):
            return _safe_result(_with_outcome(decision, "CONTROL_STATE_UNAVAILABLE"))
        if health.state != CircuitState.CLOSED:
            return _safe_result(_with_outcome(decision, "SKIPPED_CIRCUIT_OPEN"))
        assert decision.source is not None
        try:
            attempt = self._store.reserve_attempt(
                decision.source, started_at, target_year=decision.target_year
            )
        except CalendarAttemptAlreadySpent:
            return _safe_result(_with_outcome(decision, "ALREADY_ATTEMPTED"))
        except (CalendarStoreUnavailable, CalendarGenerationError, OSError, sqlite3.Error):
            return _safe_result(_with_outcome(decision, "CONTROL_STATE_UNAVAILABLE"))
        if attempt.expected_parent_sha256 != decision.expected_parent_sha256:
            return self._finish_result(
                decision, attempt, "PARENT_CHANGED", 0, 0, self._completion_time()
            )

        official = fetch_official_calendars(
            decision.source, now=started_at, client_factory=self._http_client_factory
        )
        if official.failure is not None:
            failure = (
                official.failure
                if official.failure in {"OFFICIAL_UNAVAILABLE", "OFFICIAL_HASH_MISMATCH"}
                else "CONTROL_STATE_UNAVAILABLE"
            )
            return self._finish_result(
                decision,
                attempt,
                failure,
                official.official_requests,
                0,
                self._completion_time(),
            )
        assert official.bodies is not None
        try:
            retained = self._store.retain_official_evidence(
                attempt, decision.source, official.bodies
            )
        except (CalendarStoreUnavailable, OSError, sqlite3.Error):
            retained = False
        if not retained:
            return self._finish_result(
                decision,
                attempt,
                "CONTROL_STATE_UNAVAILABLE",
                2,
                0,
                self._completion_time(),
            )
        try:
            health = self._health_store.provider_health_snapshot()
        except (ProviderHealthError, OSError):
            return self._finish_result(
                decision,
                attempt,
                "CONTROL_STATE_UNAVAILABLE",
                2,
                0,
                self._completion_time(),
            )
        if health.state != CircuitState.CLOSED:
            return self._finish_result(
                decision,
                attempt,
                "SKIPPED_CIRCUIT_OPEN",
                2,
                0,
                self._completion_time(),
            )
        try:
            provider = self._provider_factory(
                max_attempts=1,
                socket_timeout_seconds=self._socket_timeout_seconds,
                min_request_interval_seconds=self._min_request_interval_seconds,
            )
        except (BaoStockError, OSError, TimeoutError):
            return self._finish_result(
                decision,
                attempt,
                "MACHINE_UNAVAILABLE",
                2,
                0,
                self._completion_time(),
            )

        run_id = uuid4().hex
        try:

            def sink(observation: TransportObservation) -> None:
                if (
                    not isinstance(observation, TransportObservation)
                    or observation.refresh_id != run_id
                ):
                    raise ProviderHealthError("provider observation refresh identity is invalid")
                try:
                    self._health_store.record_observation(observation)
                except OSError as exc:
                    raise ProviderHealthError("provider observation could not be recorded") from exc

            refresh = getattr(provider, "refresh_operation", None)
            refresh_context = refresh(run_id) if callable(refresh) else nullcontext()
            with transport_observation_sink(sink), refresh_context:
                rows = provider.calendar_days(
                    date(attempt.target_year, 1, 1), date(attempt.target_year, 12, 31)
                )
        except ProviderHealthError:
            return self._finish_result(
                decision,
                attempt,
                "CONTROL_STATE_UNAVAILABLE",
                2,
                1,
                self._completion_time(),
            )
        except (BaoStockError, OSError, TimeoutError):
            completed_at = self._completion_time()
            if completed_at < attempt.started_at:
                return _safe_result(
                    _with_outcome(decision, "CONTROL_STATE_UNAVAILABLE"),
                    official_requests=2,
                    machine_requests=1,
                    writes_calendar_state=True,
                )
            if not self._record_machine_failure(run_id, completed_at):
                return self._finish_result(
                    decision, attempt, "CONTROL_STATE_UNAVAILABLE", 2, 1, completed_at
                )
            return self._finish_result(decision, attempt, "MACHINE_UNAVAILABLE", 2, 1, completed_at)

        completed_at = self._completion_time()
        if completed_at < attempt.started_at:
            return self._finish_result(
                decision, attempt, "CONTROL_STATE_UNAVAILABLE", 2, 1, completed_at
            )
        machine = self._build_machine(rows, attempt.target_year, completed_at)
        if machine is None or not self._audit_machine(run_id):
            if not self._record_machine_failure(run_id, completed_at):
                return self._finish_result(
                    decision, attempt, "CONTROL_STATE_UNAVAILABLE", 2, 1, completed_at
                )
            return self._finish_result(decision, attempt, "MACHINE_UNAVAILABLE", 2, 1, completed_at)
        try:
            self._health_store.record_terminal_success(
                run_id, ProviderEndpoint.TRADE_DATES, observed_at=completed_at
            )
        except (ProviderHealthError, OSError):
            return self._finish_result(
                decision, attempt, "CONTROL_STATE_UNAVAILABLE", 2, 1, completed_at
            )
        promoted_at = self._completion_time()
        if promoted_at < completed_at:
            return _safe_result(
                _with_outcome(decision, "CONTROL_STATE_UNAVAILABLE"),
                official_requests=2,
                machine_requests=1,
                writes_calendar_state=True,
            )
        try:
            promoted = self._store.promote(
                attempt, decision.source, official.bodies, machine, promoted_at
            )
        except (CalendarStoreUnavailable, OSError, sqlite3.Error):
            return self._finish_result(
                decision,
                attempt,
                "CONTROL_STATE_UNAVAILABLE",
                2,
                1,
                promoted_at,
            )
        if promoted.outcome == "PROMOTED":
            return _safe_result(
                _with_outcome(decision, "PROMOTED"),
                generation_sha256=promoted.generation_sha256,
                official_requests=2,
                machine_requests=1,
                writes_calendar_state=True,
            )
        known_promotion_failures = {
            "OFFICIAL_UNAVAILABLE",
            "OFFICIAL_HASH_MISMATCH",
            "MACHINE_UNAVAILABLE",
            "MACHINE_CONFLICT",
            "ALREADY_ATTEMPTED",
            "PARENT_CHANGED",
            "HISTORY_CHANGE",
            "CONTROL_STATE_UNAVAILABLE",
        }
        outcome = (
            promoted.outcome if promoted.outcome in known_promotion_failures else "PARENT_CHANGED"
        )
        if outcome in {"PARENT_CHANGED", "HISTORY_CHANGE", "CONTROL_STATE_UNAVAILABLE"}:
            return self._finish_result(decision, attempt, outcome, 2, 1, promoted_at)
        return _safe_result(
            _with_outcome(decision, outcome),
            official_requests=2,
            machine_requests=1,
            writes_calendar_state=True,
        )

    def _completion_time(self) -> datetime:
        return _aware_now(self._clock())

    def _finish_result(
        self,
        decision: _MaintenanceDecision,
        attempt: CalendarMaintenanceAttempt,
        outcome: str,
        official: int,
        machine: int,
        now: datetime,
    ) -> CalendarMaintenanceResult:
        recorded = self._finish(attempt, outcome, official, machine, now)
        safe_outcome = outcome if recorded else "CONTROL_STATE_UNAVAILABLE"
        return _safe_result(
            _with_outcome(decision, safe_outcome),
            official_requests=official,
            machine_requests=machine,
            writes_calendar_state=True,
        )

    def _finish(
        self,
        attempt: CalendarMaintenanceAttempt,
        outcome: str,
        official: int,
        machine: int,
        now: datetime,
    ) -> bool:
        try:
            return self._store.finish_attempt(attempt, outcome, official, machine, finished_at=now)
        except (CalendarStoreUnavailable, CalendarGenerationError, OSError, sqlite3.Error):
            return False

    @staticmethod
    def _build_machine(
        rows: object, year: int, observed_at: datetime
    ) -> CalendarMachineObservationV1 | None:
        if not isinstance(rows, (list, tuple)):
            return None
        start, end = date(year, 1, 1), date(year, 12, 31)
        expected = tuple(start + timedelta(days=i) for i in range((end - start).days + 1))
        days: list[CalendarMachineDayV1] = []
        if len(rows) != len(expected):
            return None
        for item, expected_date in zip(rows, expected, strict=True):
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                return None
            day, is_open = item
            if type(day) is not date or day != expected_date or type(is_open) is not bool:
                return None
            days.append(CalendarMachineDayV1(date=day, is_open=is_open))
        try:
            return build_observation(
                provider="baostock",
                contract_version="r2f4.1-baostock-calendar-days-v1",
                range_start=start,
                range_end=end,
                observed_at=observed_at,
                days=tuple(days),
            )
        except (CalendarGenerationError, TypeError, ValueError):
            return None

    def _audit_machine(self, run_id: str) -> bool:
        try:
            observations = [
                item for item in self._health_store.list_observations() if item.refresh_id == run_id
            ]
        except (ProviderHealthError, OSError):
            return False
        operations = [
            item for item in observations if item.protocol_stage == ProtocolStage.OPERATION
        ]
        sessions = {item.provider_session_id for item in observations}
        return (
            bool(observations)
            and len(operations) == 1
            and len(sessions) == 1
            and all(
                item.endpoint == ProviderEndpoint.TRADE_DATES
                and item.attempt == 1
                and item.outcome == TransportOutcome.SUCCESS
                and item.normalized_error is None
                for item in observations
            )
        )

    def _record_machine_failure(self, run_id: str, observed_at: datetime) -> bool:
        normalized_error = self._machine_failure_error(run_id)
        if normalized_error is None:
            return False
        try:
            self._health_store.record_terminal_failure(
                run_id,
                ProviderEndpoint.TRADE_DATES,
                normalized_error,
                observed_at=observed_at,
            )
            return True
        except (ProviderHealthError, OSError):
            return False

    def _machine_failure_error(self, run_id: str) -> NormalizedTransportError | None:
        try:
            observations = [
                item for item in self._health_store.list_observations() if item.refresh_id == run_id
            ]
        except (ProviderHealthError, OSError):
            return None
        operations = [
            item for item in observations if item.protocol_stage == ProtocolStage.OPERATION
        ]
        if (
            not observations
            or len(operations) != 1
            or len({item.provider_session_id for item in observations}) != 1
            or any(
                item.endpoint != ProviderEndpoint.TRADE_DATES or item.attempt != 1
                for item in observations
            )
        ):
            return None
        operation = operations[0]
        if (
            operation.outcome == TransportOutcome.SUCCESS
            and operation.normalized_error is None
            and all(
                item.outcome == TransportOutcome.SUCCESS and item.normalized_error is None
                for item in observations
            )
        ):
            return NormalizedTransportError.PROTOCOL_ERROR
        if (
            operation.outcome == TransportOutcome.ERROR
            and operation.normalized_error is not None
            and all(
                item.normalized_error in (None, operation.normalized_error) for item in observations
            )
        ):
            return operation.normalized_error
        return None


def _expected_parent(control: object) -> str | None:
    try:
        generation = control.read_result.generation
        return generation.generation_sha256 if generation is not None else None
    except (AttributeError, TypeError):
        return None


def _with_outcome(
    decision: _MaintenanceDecision, outcome: MaintenanceOutcome
) -> _MaintenanceDecision:
    return _MaintenanceDecision(
        decision.now,
        decision.target_year,
        decision.source,
        decision.source_sha256,
        decision.expected_parent_sha256,
        outcome,
        decision.due,
        decision.current_year_ready,
        decision.next_year_status,
    )


__all__ = [
    "CalendarMaintenancePlan",
    "CalendarMaintenanceResult",
    "CalendarMaintenanceService",
    "fetch_official_calendars",
]
