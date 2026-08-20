import contextlib
import hashlib
import json
import re
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal, Protocol

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from backend.app.market.failures import MarketFailureClass, MarketFailureStage
from backend.app.market.models import RefreshResult
from backend.app.market.provider_health import ProviderHealth
from backend.app.storage.models import VerifiedReadySessionInventory

RepairJobState = Literal["pending", "leased", "retry_wait", "published", "dead_letter"]
RepairAttemptOutcome = Literal["running", "succeeded", "failed", "abandoned"]
RepairFinalizationOutcome = Literal["succeeded", "failed"]
ContinuityControlReason = Literal["CONTROL_STATE_UNAVAILABLE"]
ContinuityUnavailableReason = Literal[
    "CONTINUITY_START_UNCONFIGURED",
    "CONTINUITY_RANGE_INVALID",
    "CALENDAR_UNAVAILABLE",
    "CALENDAR_CONFLICT",
    "MANIFEST_INVENTORY_UNAVAILABLE",
    "IMMUTABLE_OBJECT_INVALID",
]
ContinuityStatusReason = Literal[
    "CONTINUITY_START_UNCONFIGURED",
    "CONTINUITY_RANGE_INVALID",
    "CALENDAR_UNAVAILABLE",
    "CALENDAR_CONFLICT",
    "MANIFEST_INVENTORY_UNAVAILABLE",
    "IMMUTABLE_OBJECT_INVALID",
    "CONTROL_STATE_UNAVAILABLE",
    "REPAIR_EXECUTION_DISABLED",
    "PROVIDER_CIRCUIT_OPEN",
    "PROVIDER_CIRCUIT_HALF_OPEN",
    "PROVIDER_HEALTH_UNAVAILABLE",
    "REPAIR_QUEUE_NOT_ENQUEUED",
    "REPAIR_DEAD_LETTER_ONLY",
]
ContinuityInventoryMode = Literal["immutable_dataset", "local_mutable"]
ContinuityLane = Literal["freshness", "repair"]
ContinuityAction = Literal["run", "wait", "none"]
ContinuityDecisionReason = Literal[
    "FRESHNESS_DUE",
    "FRESHNESS_WAIT",
    "REPAIR_READY",
    "REPAIR_DISABLED",
    "CONTROL_STATE_UNAVAILABLE",
    "NO_ELIGIBLE_REPAIR",
    "CALENDAR_UNAVAILABLE",
    "PROVIDER_HEALTH_UNAVAILABLE",
    "PROVIDER_NOT_CLOSED",
    "REPAIR_CLAIMED",
    "ALREADY_RUNNING",
    "REPAIR_CONSUMER_FAILED",
    "POST_PUBLISH_INVENTORY_MISSING",
    "REPAIR_PUBLISHED",
]
_FRESHNESS_WAIT_REASONS = frozenset(
    {
        "FRESHNESS_WAIT",
        "REPAIR_DISABLED",
        "CONTROL_STATE_UNAVAILABLE",
        "NO_ELIGIBLE_REPAIR",
    }
)
_REPAIR_WAIT_REASONS = frozenset(
    {
        "PROVIDER_HEALTH_UNAVAILABLE",
        "PROVIDER_NOT_CLOSED",
        "CONTROL_STATE_UNAVAILABLE",
        "NO_ELIGIBLE_REPAIR",
        "ALREADY_RUNNING",
        "REPAIR_CONSUMER_FAILED",
    }
)
_FRESHNESS_NONE_REASONS = _FRESHNESS_WAIT_REASONS

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SAFE_GENERATION = re.compile(r"^generation-[A-Za-z0-9][A-Za-z0-9._-]{0,120}$")
_R2F1_UNIVERSE_ID = "all-main-board"


def repair_request_key(trade_date: date) -> str:
    """Return the stable provider-scoped request identity for one full session."""
    if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
        raise RepairQueueError("repair queue trade date is invalid")
    return f"repair:baostock:{trade_date.isoformat()}:{_R2F1_UNIVERSE_ID}"


class RepairQueueError(RuntimeError):
    """A sanitized continuity queue contract or storage failure."""


class RepairQueueConflictError(RepairQueueError):
    """A stale lease or state version failed its compare-and-swap."""


class ContinuityScanResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["current", "gaps"]
    effective_start: date
    effective_end: date
    manifest_generation: str = Field(
        min_length=1,
        max_length=128,
        pattern=_SAFE_GENERATION.pattern,
    )
    confirmed_open_sessions: tuple[date, ...]
    published_ready_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    writes_control_state: Literal[False] = False
    provider_requests: Literal[0] = 0

    @field_validator(
        "confirmed_open_sessions",
        "published_ready_sessions",
        "missing_sessions",
    )
    @classmethod
    def require_sorted_unique_dates(cls, value: tuple[date, ...]) -> tuple[date, ...]:
        if tuple(sorted(set(value))) != value:
            raise ValueError("continuity sessions must be sorted and unique")
        return value

    @model_validator(mode="after")
    def valid_scan_shape(self) -> "ContinuityScanResult":
        if self.effective_start > self.effective_end:
            raise ValueError("continuity range is invalid")
        for sessions in (
            self.confirmed_open_sessions,
            self.published_ready_sessions,
            self.missing_sessions,
        ):
            if any(
                session < self.effective_start or session > self.effective_end
                for session in sessions
            ):
                raise ValueError("continuity session falls outside the effective range")
        published = set(self.published_ready_sessions)
        expected_missing = tuple(
            session for session in self.confirmed_open_sessions if session not in published
        )
        if self.missing_sessions != expected_missing:
            raise ValueError("continuity missing sessions do not match verified evidence")
        if (self.status == "gaps") != bool(self.missing_sessions):
            raise ValueError("continuity status does not match the gap set")
        return self


class ContinuityUnavailable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["unavailable"] = "unavailable"
    reason_code: ContinuityUnavailableReason
    writes_control_state: Literal[False] = False
    provider_requests: Literal[0] = 0


class ContinuityDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: ContinuityAction
    lane: ContinuityLane
    target_session: date | None
    repair_job_id: str | None = None
    next_run_at: datetime | None = None
    reason_code: ContinuityDecisionReason
    provider_requests: Literal[0] = 0

    @field_validator("repair_job_id")
    @classmethod
    def safe_repair_job_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return require_safe_identifier(value)
        except RepairQueueError as exc:
            raise ValueError("repair decision identifier is invalid") from exc

    @field_validator("next_run_at")
    @classmethod
    def aware_next_run_at(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.utcoffset() is None:
            raise ValueError("continuity decision timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def valid_decision_shape(self) -> "ContinuityDecision":
        if self.lane == "repair":
            if self.target_session is None or self.repair_job_id is None:
                raise ValueError("repair decision requires a target job")
            if self.repair_job_id != repair_job_id(self.target_session, _R2F1_UNIVERSE_ID):
                raise ValueError("repair decision target does not match job identity")
        elif self.repair_job_id is not None:
            raise ValueError("only a repair decision may carry a repair job")

        if self.action == "run":
            if self.target_session is None:
                raise ValueError("run decision requires a target")
            if self.lane == "freshness":
                if self.reason_code != "FRESHNESS_DUE" or self.next_run_at is not None:
                    raise ValueError("freshness run decision is inconsistent")
            elif self.reason_code not in {"REPAIR_READY", "REPAIR_CLAIMED"}:
                raise ValueError("repair run decision is inconsistent")
        elif self.action == "wait":
            if self.target_session is None:
                raise ValueError("wait decision requires a target")
            if self.lane == "freshness":
                if self.reason_code not in _FRESHNESS_WAIT_REASONS or self.next_run_at is None:
                    raise ValueError("freshness wait decision is inconsistent")
            elif self.reason_code not in _REPAIR_WAIT_REASONS:
                raise ValueError("repair wait decision is inconsistent")
        else:
            if self.lane != "freshness" or self.next_run_at is not None:
                raise ValueError("none decision is inconsistent")
            if self.reason_code == "CALENDAR_UNAVAILABLE":
                if self.target_session is not None:
                    raise ValueError("calendar-unavailable decision cannot carry a target")
            elif self.reason_code not in _FRESHNESS_NONE_REASONS or self.target_session is None:
                raise ValueError("freshness none decision is inconsistent")
        return self


ContinuityEnqueueStatus = Literal[
    "current",
    "enqueued",
    "unavailable",
    "already_running",
]
ContinuityEnqueueReason = Literal[
    "CONTINUITY_START_UNCONFIGURED",
    "CONTINUITY_RANGE_INVALID",
    "CALENDAR_UNAVAILABLE",
    "CALENDAR_CONFLICT",
    "CALENDAR_CONFLICT_SOURCE_UNAVAILABLE",
    "MANIFEST_INVENTORY_UNAVAILABLE",
    "IMMUTABLE_OBJECT_INVALID",
    "REFRESH_ALREADY_RUNNING",
    "CONTROL_STATE_UNAVAILABLE",
]


class ContinuityEnqueueResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: ContinuityEnqueueStatus
    reason_code: ContinuityEnqueueReason | None = None
    fresh_scan: ContinuityScanResult | None = None
    manifest_generation: str | None = None
    scan_identity: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    created_jobs: tuple["RepairJob", ...] = ()
    created_count: int = Field(default=0, ge=0)
    existing_count: int = Field(default=0, ge=0)
    writes_control_state: bool = False
    provider_requests: Literal[0] = 0

    @model_validator(mode="after")
    def valid_enqueue_shape(self) -> "ContinuityEnqueueResult":
        available = self.status in {"current", "enqueued"}
        if available != (self.fresh_scan is not None):
            raise ValueError("continuity enqueue scan evidence is incomplete")
        if available != (self.reason_code is None):
            raise ValueError("continuity enqueue reason does not match status")
        if available != (self.manifest_generation is not None):
            raise ValueError("continuity enqueue generation does not match status")
        if available != (self.scan_identity is not None):
            raise ValueError("continuity enqueue identity does not match status")
        if self.created_count != len(self.created_jobs):
            raise ValueError("continuity enqueue created count does not match jobs")
        if self.status != "enqueued" and (self.created_jobs or self.existing_count):
            raise ValueError("non-enqueue result cannot carry queue counts")
        if self.status == "current" and self.fresh_scan.status != "current":
            raise ValueError("current enqueue result requires current scan evidence")
        if self.status == "enqueued" and self.fresh_scan.status != "gaps":
            raise ValueError("enqueue result requires gap scan evidence")
        if self.status == "already_running" and self.reason_code != "REFRESH_ALREADY_RUNNING":
            raise ValueError("busy enqueue result requires the busy reason")
        if self.status == "unavailable" and self.reason_code in {
            None,
            "REFRESH_ALREADY_RUNNING",
        }:
            raise ValueError("unavailable enqueue result requires an unavailable reason")
        if self.writes_control_state is not available:
            raise ValueError(
                "continuity enqueue write flag does not match successful orchestration"
            )
        return self


class ConfirmedCalendarReader(Protocol):
    @property
    def status(self) -> Literal["confirmed", "unavailable"]: ...

    def confirmed_open_sessions(self, start: date, end: date) -> tuple[date, ...] | None: ...


class VerifiedInventoryReader(Protocol):
    def verified_ready_session_inventory(
        self,
        source: Literal["baostock"] = "baostock",
    ) -> VerifiedReadySessionInventory: ...


class ContinuityQueueWriter(Protocol):
    def initialize_continuity_schema(self) -> None: ...

    def enqueue_repair_jobs(
        self,
        missing_dates: tuple[date, ...],
        *,
        universe_id: str,
        now: datetime,
    ) -> list["RepairJob"]: ...


class RefreshLock(Protocol):
    def __enter__(self) -> "RefreshLock": ...

    def __exit__(self, *args: object) -> None: ...


class ContinuityInventory:
    """Pure confirmed-calendar minus verified immutable-manifest scanner."""

    def __init__(
        self,
        *,
        calendar: ConfirmedCalendarReader,
        inventory_reader: VerifiedInventoryReader,
        inventory_mode: ContinuityInventoryMode,
        calendar_conflict: bool | Callable[[], bool] = False,
    ) -> None:
        self._calendar = calendar
        self._inventory_reader = inventory_reader
        self._inventory_mode = inventory_mode
        self._calendar_conflict = calendar_conflict

    @property
    def calendar_conflict_is_dynamic(self) -> bool:
        return callable(self._calendar_conflict)

    def scan(
        self,
        *,
        configured_start: date | None,
        latest_completed_session: date | None,
        requested_start: date | None = None,
        requested_end: date | None = None,
    ) -> ContinuityScanResult | ContinuityUnavailable:
        if configured_start is None:
            return ContinuityUnavailable(reason_code="CONTINUITY_START_UNCONFIGURED")
        if not self._valid_date(configured_start) or not self._valid_date(latest_completed_session):
            return ContinuityUnavailable(reason_code="CONTINUITY_RANGE_INVALID")
        if requested_start is not None and not self._valid_date(requested_start):
            return ContinuityUnavailable(reason_code="CONTINUITY_RANGE_INVALID")
        if requested_end is not None and not self._valid_date(requested_end):
            return ContinuityUnavailable(reason_code="CONTINUITY_RANGE_INVALID")

        effective_start = requested_start or configured_start
        effective_end = requested_end or latest_completed_session
        if (
            configured_start > latest_completed_session
            or effective_start < configured_start
            or effective_end > latest_completed_session
            or effective_start > effective_end
        ):
            return ContinuityUnavailable(reason_code="CONTINUITY_RANGE_INVALID")
        if self._inventory_mode != "immutable_dataset":
            return ContinuityUnavailable(reason_code="MANIFEST_INVENTORY_UNAVAILABLE")
        try:
            conflict = (
                self._calendar_conflict()
                if callable(self._calendar_conflict)
                else self._calendar_conflict
            )
            if type(conflict) is not bool:
                return ContinuityUnavailable(reason_code="CALENDAR_UNAVAILABLE")
            if conflict:
                return ContinuityUnavailable(reason_code="CALENDAR_CONFLICT")
            if self._calendar.status != "confirmed":
                return ContinuityUnavailable(reason_code="CALENDAR_UNAVAILABLE")
            confirmed_open = self._calendar.confirmed_open_sessions(
                effective_start,
                effective_end,
            )
        except Exception:
            return ContinuityUnavailable(reason_code="CALENDAR_UNAVAILABLE")
        day_count = (effective_end - effective_start).days + 1
        if (
            confirmed_open is None
            or not isinstance(confirmed_open, tuple)
            or len(confirmed_open) > day_count
            or any(not self._valid_date(session) for session in confirmed_open)
            or tuple(sorted(set(confirmed_open))) != confirmed_open
            or any(
                session < effective_start or session > effective_end for session in confirmed_open
            )
        ):
            return ContinuityUnavailable(reason_code="CALENDAR_UNAVAILABLE")
        try:
            inventory = self._inventory_reader.verified_ready_session_inventory("baostock")
            inventory = VerifiedReadySessionInventory.model_validate(
                inventory.model_dump(mode="python")
            )
        except Exception:
            return ContinuityUnavailable(reason_code="MANIFEST_INVENTORY_UNAVAILABLE")

        ready_in_range = tuple(
            session for session in inventory.sessions if effective_start <= session <= effective_end
        )
        ready_set = set(ready_in_range)
        missing = tuple(session for session in confirmed_open if session not in ready_set)
        return ContinuityScanResult(
            status="gaps" if missing else "current",
            effective_start=effective_start,
            effective_end=effective_end,
            manifest_generation=inventory.manifest_generation,
            confirmed_open_sessions=confirmed_open,
            published_ready_sessions=ready_in_range,
            missing_sessions=missing,
        )

    @staticmethod
    def _valid_date(value: object) -> bool:
        return isinstance(value, date) and not isinstance(value, datetime)


class ContinuityEnqueueService:
    """Writer-only lock/revalidate/enqueue orchestration for continuity gaps."""

    def __init__(
        self,
        *,
        scanner: ContinuityInventory,
        store: ContinuityQueueWriter | None = None,
        store_factory: Callable[[], ContinuityQueueWriter] | None = None,
        lock_path: Path,
        universe_id: str,
        clock: Callable[[], datetime],
        lock_factory: Callable[[Path], RefreshLock] | None = None,
        schema_initializer: Callable[[], None] | None = None,
    ) -> None:
        self._scanner = scanner
        self._store = store
        if store is None and store_factory is None:
            raise ValueError("continuity enqueue requires a queue store")
        if store is not None and store_factory is not None:
            raise ValueError("continuity enqueue accepts one queue store source")
        self._store_factory = store_factory
        self._lock_path = lock_path
        self._universe_id = require_universe_id(universe_id)
        self._clock = clock
        self._lock_factory = lock_factory
        self._schema_initializer = schema_initializer

    def execute(
        self,
        *,
        configured_start: date | None,
        latest_completed_session: date | None,
        requested_start: date | None = None,
        requested_end: date | None = None,
        completed_scan: ContinuityScanResult | None = None,
    ) -> ContinuityEnqueueResult:
        if completed_scan is not None:
            validated_completed = self._revalidate_scan_evidence(completed_scan)
            if not isinstance(validated_completed, ContinuityScanResult):
                return ContinuityEnqueueResult(
                    status="unavailable",
                    reason_code="CONTINUITY_RANGE_INVALID",
                )
        try:
            timestamp = require_utc(self._clock())
        except Exception:
            return ContinuityEnqueueResult(
                status="unavailable",
                reason_code="CONTROL_STATE_UNAVAILABLE",
            )
        try:
            if not self._scanner.calendar_conflict_is_dynamic:
                return ContinuityEnqueueResult(
                    status="unavailable",
                    reason_code="CALENDAR_CONFLICT_SOURCE_UNAVAILABLE",
                )
            raw_preflight = self._scanner.scan(
                configured_start=configured_start,
                latest_completed_session=latest_completed_session,
                requested_start=requested_start,
                requested_end=requested_end,
            )
        except Exception:
            return ContinuityEnqueueResult(
                status="unavailable",
                reason_code="CONTROL_STATE_UNAVAILABLE",
            )
        preflight = self._revalidate_scan_evidence(raw_preflight)
        if preflight is None:
            return ContinuityEnqueueResult(
                status="unavailable",
                reason_code="CONTROL_STATE_UNAVAILABLE",
            )
        if isinstance(preflight, ContinuityUnavailable):
            return ContinuityEnqueueResult(
                status="unavailable",
                reason_code=preflight.reason_code,
            )
        try:
            default_lock, already_running = self._resolve_default_lock()
            lock_factory = self._lock_factory or default_lock
            try:
                lock = lock_factory(self._lock_path)
                with lock:
                    raw_fresh = self._scanner.scan(
                        configured_start=configured_start,
                        latest_completed_session=latest_completed_session,
                        requested_start=requested_start,
                        requested_end=requested_end,
                    )
                    fresh = self._revalidate_scan_evidence(raw_fresh)
                    if fresh is None:
                        return ContinuityEnqueueResult(
                            status="unavailable",
                            reason_code="CONTROL_STATE_UNAVAILABLE",
                        )
                    if isinstance(fresh, ContinuityUnavailable):
                        return ContinuityEnqueueResult(
                            status="unavailable",
                            reason_code=fresh.reason_code,
                        )
                    scan_identity = self._scan_identity(fresh)
                    store = self._store
                    if store is None:
                        assert self._store_factory is not None
                        store = self._store_factory()
                    if self._schema_initializer is not None:
                        self._schema_initializer()
                    else:
                        store.initialize_continuity_schema()
                    if fresh.status == "current":
                        return ContinuityEnqueueResult(
                            status="current",
                            fresh_scan=fresh,
                            manifest_generation=fresh.manifest_generation,
                            scan_identity=scan_identity,
                            writes_control_state=True,
                        )
                    created = tuple(
                        store.enqueue_repair_jobs(
                            fresh.missing_sessions,
                            universe_id=self._universe_id,
                            now=timestamp,
                        )
                    )
                    return ContinuityEnqueueResult(
                        status="enqueued",
                        fresh_scan=fresh,
                        manifest_generation=fresh.manifest_generation,
                        scan_identity=scan_identity,
                        created_jobs=created,
                        created_count=len(created),
                        existing_count=len(fresh.missing_sessions) - len(created),
                        writes_control_state=True,
                    )
            except already_running:
                return ContinuityEnqueueResult(
                    status="already_running",
                    reason_code="REFRESH_ALREADY_RUNNING",
                )
        except RepairQueueError:
            return ContinuityEnqueueResult(
                status="unavailable",
                reason_code="CONTROL_STATE_UNAVAILABLE",
            )
        except Exception:
            return ContinuityEnqueueResult(
                status="unavailable",
                reason_code="CONTROL_STATE_UNAVAILABLE",
            )

    @staticmethod
    def _resolve_default_lock() -> tuple[
        Callable[[Path], RefreshLock],
        type[Exception],
    ]:
        from backend.app.market.automation import RefreshAlreadyRunning, RefreshRunLock

        return RefreshRunLock, RefreshAlreadyRunning

    @staticmethod
    def _scan_identity(scan: ContinuityScanResult) -> str:
        content = json.dumps(scan.model_dump(mode="json"), separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()

    @staticmethod
    def _revalidate_scan_evidence(
        value: object,
    ) -> ContinuityScanResult | ContinuityUnavailable | None:
        """Round-trip scanner output through the public models before dereference.

        Scanner output is a control-plane boundary: tests, adapters, and legacy callers can
        construct Pydantic models with ``model_construct`` and bypass their validators.  Do
        not inspect any fields until a complete strict round-trip succeeds.  ``Exception`` is
        intentionally the boundary here; process-level ``BaseException`` signals must remain
        visible to the caller.
        """
        if not isinstance(value, (ContinuityScanResult, ContinuityUnavailable)):
            return None
        try:
            payload = value.model_dump(mode="python", warnings=False)
            if isinstance(value, ContinuityScanResult):
                return ContinuityScanResult.model_validate(payload)
            return ContinuityUnavailable.model_validate(payload)
        except Exception:
            return None


def require_utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise RepairQueueError("repair queue timestamp is invalid")
    return value.astimezone(UTC)


def require_safe_identifier(value: str) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise RepairQueueError("repair queue identifier is invalid")
    return value


def require_universe_id(value: str) -> str:
    if value != _R2F1_UNIVERSE_ID:
        raise RepairQueueError("repair queue universe is invalid")
    return value


def repair_job_id(trade_date: date, universe_id: str) -> str:
    if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
        raise RepairQueueError("repair queue trade date is invalid")
    universe_id = require_universe_id(universe_id)
    return require_safe_identifier(f"repair:{trade_date.isoformat()}:{universe_id}")


def _aware_utc(value: datetime) -> datetime:
    try:
        return require_utc(value)
    except RepairQueueError as exc:
        raise ValueError("timestamp must be timezone-aware") from exc


UtcDateTime = Annotated[datetime, AfterValidator(_aware_utc)]


class RepairRetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_attempts: int = Field(ge=1, le=20)
    base_seconds: int = Field(ge=900, le=86400)

    def delay_after(self, attempt_number: int) -> timedelta:
        if (
            not isinstance(attempt_number, int)
            or isinstance(attempt_number, bool)
            or attempt_number < 1
        ):
            raise RepairQueueError("repair attempt number is invalid")
        seconds = min(86400, self.base_seconds * 2 ** (attempt_number - 1))
        return timedelta(seconds=seconds)


class RepairJob(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str
    trade_date: date
    universe_id: str
    state: RepairJobState
    state_version: int = Field(ge=1)
    attempt_count: int = Field(ge=0)
    abandoned_attempt_count: int = Field(ge=0)
    next_attempt_at: UtcDateTime | None = None
    lease_id: str | None = None
    lease_owner: str | None = None
    lease_expires_at: UtcDateTime | None = None
    last_attempt_id: str | None = None
    last_failure_stage: MarketFailureStage | None = None
    last_failure_class: MarketFailureClass | None = None
    created_at: UtcDateTime
    updated_at: UtcDateTime
    published_at: UtcDateTime | None = None

    @field_validator("job_id", "lease_id", "lease_owner", "last_attempt_id")
    @classmethod
    def safe_identifiers(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return require_safe_identifier(value)
        except RepairQueueError as exc:
            raise ValueError("identifier is invalid") from exc

    @field_validator("universe_id")
    @classmethod
    def safe_universe(cls, value: str) -> str:
        try:
            return require_universe_id(value)
        except RepairQueueError as exc:
            raise ValueError("universe is invalid") from exc

    @model_validator(mode="after")
    def valid_state_shape(self) -> "RepairJob":
        try:
            expected_job_id = repair_job_id(self.trade_date, self.universe_id)
        except RepairQueueError as exc:
            raise ValueError("repair job identity is invalid") from exc
        if self.job_id != expected_job_id:
            raise ValueError("repair job identity does not match its scope")
        lease_fields = (self.lease_id, self.lease_owner, self.lease_expires_at)
        if self.state == "leased":
            if any(value is None for value in lease_fields):
                raise ValueError("leased repair job requires a complete lease")
            if self.last_attempt_id is None or self.attempt_count < 1:
                raise ValueError("leased repair job requires a claimed attempt")
            if self.lease_expires_at <= self.updated_at:
                raise ValueError("repair lease must expire after the job update")
        elif any(value is not None for value in lease_fields):
            raise ValueError("only a leased repair job may carry a lease")
        if (self.attempt_count == 0) != (self.last_attempt_id is None):
            raise ValueError("repair job attempt count and reference do not match")
        if self.state == "pending" and (
            self.attempt_count != 0
            or self.abandoned_attempt_count != 0
            or self.last_failure_stage is not None
            or self.last_failure_class is not None
        ):
            raise ValueError("pending repair job must be unattempted")
        if self.state in {"retry_wait", "dead_letter"} and self.attempt_count < 1:
            raise ValueError("failed repair job requires attempt evidence")
        if self.state == "retry_wait":
            if self.next_attempt_at is None:
                raise ValueError("retry-wait repair job requires a due time")
        elif self.next_attempt_at is not None:
            raise ValueError("only a retry-wait repair job may carry a due time")
        if self.state == "published":
            if self.published_at is None:
                raise ValueError("published repair job requires publication time")
            if self.published_at != self.updated_at:
                raise ValueError("repair publication time must match its transition")
        elif self.published_at is not None:
            raise ValueError("only a published repair job may carry publication time")
        if self.abandoned_attempt_count > self.attempt_count:
            raise ValueError("abandoned attempts cannot exceed claimed attempts")
        if (self.last_failure_stage is None) != (self.last_failure_class is None):
            raise ValueError("repair job failure evidence is incomplete")
        if self.updated_at < self.created_at:
            raise ValueError("repair job update precedes creation")
        if self.next_attempt_at is not None and self.next_attempt_at <= self.updated_at:
            raise ValueError("repair retry due time must be in the future")
        if self.published_at is not None and self.published_at < self.created_at:
            raise ValueError("repair publication precedes creation")
        return self


class RepairAttempt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    attempt_id: str
    job_id: str
    attempt_number: int = Field(gt=0)
    lease_id: str
    lease_owner: str
    outcome: RepairAttemptOutcome
    refresh_run_id: str | None = None
    failure_stage: MarketFailureStage | None = None
    failure_class: MarketFailureClass | None = None
    retryable: bool | None = None
    started_at: UtcDateTime
    completed_at: UtcDateTime | None = None

    @field_validator("attempt_id", "job_id", "lease_id", "lease_owner", "refresh_run_id")
    @classmethod
    def safe_identifiers(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            return require_safe_identifier(value)
        except RepairQueueError as exc:
            raise ValueError("identifier is invalid") from exc

    @model_validator(mode="after")
    def valid_outcome_shape(self) -> "RepairAttempt":
        if self.outcome == "running":
            if (
                self.completed_at is not None
                or self.retryable is not None
                or self.refresh_run_id is not None
                or self.failure_stage is not None
                or self.failure_class is not None
            ):
                raise ValueError("running repair attempt cannot be terminal")
        elif self.completed_at is None or self.retryable is None:
            raise ValueError("terminal repair attempt requires completion evidence")
        if self.outcome == "succeeded" and (
            self.refresh_run_id is None
            or self.failure_stage is not None
            or self.failure_class is not None
            or self.retryable is not False
        ):
            raise ValueError("successful repair attempt requires internal refresh evidence")
        if self.outcome == "failed" and (self.failure_stage is None or self.failure_class is None):
            raise ValueError("failed repair attempt requires sanitized failure evidence")
        if self.outcome == "failed" and self.refresh_run_id is None:
            raise ValueError("failed repair attempt requires a refresh audit reference")
        if self.outcome == "abandoned" and (
            self.refresh_run_id is not None
            or self.failure_stage is not None
            or self.failure_class is not None
            or self.retryable is not True
        ):
            raise ValueError("abandoned repair attempt carries invalid failure evidence")
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("repair attempt completion precedes start")
        return self


class RepairLease(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str
    attempt_id: str
    lease_id: str
    owner: str
    target_session: date
    state_version: int = Field(ge=2)
    expires_at: UtcDateTime

    @field_validator("job_id", "attempt_id", "lease_id", "owner")
    @classmethod
    def safe_identifiers(cls, value: str) -> str:
        try:
            return require_safe_identifier(value)
        except RepairQueueError as exc:
            raise ValueError("identifier is invalid") from exc

    @model_validator(mode="after")
    def valid_job_scope(self) -> "RepairLease":
        if self.job_id != repair_job_id(self.target_session, _R2F1_UNIVERSE_ID):
            raise ValueError("repair lease target does not match job identity")
        return self


class RepairQueueSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["ready", "unavailable"]
    reason_code: ContinuityControlReason | None = None
    jobs: tuple[RepairJob, ...] = ()
    attempts: tuple[RepairAttempt, ...] = ()

    @model_validator(mode="after")
    def valid_availability_shape(self) -> "RepairQueueSnapshot":
        if self.status == "ready" and self.reason_code is not None:
            raise ValueError("ready repair queue cannot carry an unavailable reason")
        if self.status == "unavailable" and (
            self.reason_code != "CONTROL_STATE_UNAVAILABLE" or self.jobs or self.attempts
        ):
            raise ValueError("unavailable repair queue cannot expose partial state")
        if self.status == "ready":
            attempts = {item.attempt_id: item for item in self.attempts}
            jobs = {item.job_id: item for item in self.jobs}
            attempts_by_job: dict[str, list[RepairAttempt]] = {job_id: [] for job_id in jobs}
            for attempt in self.attempts:
                if attempt.job_id not in attempts_by_job:
                    raise ValueError("repair attempt references an unknown job")
                attempts_by_job[attempt.job_id].append(attempt)
            for job in self.jobs:
                job_attempts = sorted(
                    attempts_by_job[job.job_id], key=lambda item: item.attempt_number
                )
                attempt_numbers = sorted(item.attempt_number for item in job_attempts)
                if len(attempt_numbers) != job.attempt_count or any(
                    number != expected for expected, number in enumerate(attempt_numbers, start=1)
                ):
                    raise ValueError("repair job attempt sequence is incomplete")
                abandoned = sum(item.outcome == "abandoned" for item in job_attempts)
                if abandoned != job.abandoned_attempt_count:
                    raise ValueError("repair job abandoned count does not match attempts")
                if any(
                    item.started_at < job.created_at or item.started_at > job.updated_at
                    for item in job_attempts
                ):
                    raise ValueError("repair attempt time falls outside its job lineage")
                if any(
                    item.completed_at is not None and item.completed_at > job.updated_at
                    for item in job_attempts
                ):
                    raise ValueError("repair attempt completion exceeds its job transition")
                for previous, current in zip(job_attempts, job_attempts[1:], strict=False):
                    if previous.completed_at is None or previous.completed_at > current.started_at:
                        raise ValueError("repair attempt time lineage overlaps")
                    if previous.outcome == "succeeded" or (
                        previous.outcome == "failed" and previous.retryable is False
                    ):
                        raise ValueError("terminal repair attempt cannot have a successor")
                if job.last_attempt_id is None:
                    continue
                attempt = attempts.get(job.last_attempt_id)
                if (
                    attempt is None
                    or attempt.job_id != job.job_id
                    or attempt.attempt_number != job.attempt_count
                ):
                    raise ValueError("repair job attempt reference is invalid")
                if job.state == "leased" and (
                    attempt.outcome != "running"
                    or attempt.lease_id != job.lease_id
                    or attempt.lease_owner != job.lease_owner
                    or job.lease_expires_at is None
                    or job.lease_expires_at <= attempt.started_at
                    or attempt.started_at != job.updated_at
                ):
                    raise ValueError("repair lease attempt reference is invalid")
                if job.state in {"retry_wait", "dead_letter"} and attempt.outcome not in {
                    "failed",
                    "abandoned",
                }:
                    raise ValueError("failed repair job lacks a terminal failure attempt")
                if (
                    job.state in {"retry_wait", "dead_letter"}
                    and attempt.completed_at != job.updated_at
                ):
                    raise ValueError("failed repair transition time is inconsistent")
                if job.state == "published" and attempt.outcome == "running":
                    raise ValueError("published repair job cannot have a running attempt")
                succeeded = [item for item in job_attempts if item.outcome == "succeeded"]
                if succeeded and (
                    len(succeeded) != 1
                    or succeeded[0].attempt_id != job.last_attempt_id
                    or job.state != "published"
                ):
                    raise ValueError("successful repair attempt lineage is invalid")
                if succeeded and succeeded[0].completed_at != job.updated_at:
                    raise ValueError("successful repair transition time is inconsistent")
                nonretryable_failures = [
                    item
                    for item in job_attempts
                    if item.outcome == "failed" and item.retryable is False
                ]
                if nonretryable_failures and (
                    len(nonretryable_failures) != 1
                    or nonretryable_failures[0].attempt_id != job.last_attempt_id
                    or job.state not in {"dead_letter", "published"}
                ):
                    raise ValueError("non-retryable repair attempt lineage is invalid")
            for attempt in self.attempts:
                if attempt.outcome == "running":
                    job = jobs.get(attempt.job_id)
                    if (
                        job is None
                        or job.state != "leased"
                        or job.last_attempt_id != attempt.attempt_id
                    ):
                        raise ValueError("running repair attempt is orphaned")
        return self


class ContinuityStatusSummary(BaseModel):
    """Sanitized, read-only continuity evidence for API and operator consumers."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    continuity_status: Literal["current", "gaps", "blocked", "unavailable"] = "unavailable"
    continuity_start_date: date | None = None
    missing_session_count: int = Field(default=0, ge=0)
    oldest_missing_session: date | None = None
    repair_execution_enabled: bool = False
    repair_pending_count: int = Field(default=0, ge=0)
    repair_retry_wait_count: int = Field(default=0, ge=0)
    repair_active_count: int = Field(default=0, ge=0)
    repair_dead_letter_count: int = Field(default=0, ge=0)
    active_lane: ContinuityLane | None = None
    continuity_reason_code: ContinuityStatusReason | None = None


def build_continuity_status_summary(
    *,
    scanner: "ContinuityInventory | None",
    configured_start: date | None,
    latest_completed_session: date | None,
    queue_snapshot: RepairQueueSnapshot | None,
    repair_enabled: bool,
    provider_health: object | None,
    freshness_state: object | None,
    now: datetime | None = None,
) -> ContinuityStatusSummary:
    """Build continuity status using only strict evidence and SELECT-only snapshots.

    This function has no writer, lock, provider, or circuit-reclamation dependency.  Invalid
    internal objects intentionally degrade to an allowlisted unavailable result instead of
    serializing arbitrary implementation details.
    """

    base = dict(
        continuity_start_date=(
            configured_start
            if isinstance(configured_start, date) and not isinstance(configured_start, datetime)
            else None
        ),
        repair_execution_enabled=bool(repair_enabled),
    )

    if configured_start is None:
        return ContinuityStatusSummary(
            **base,
            continuity_reason_code="CONTINUITY_START_UNCONFIGURED",
        )
    if scanner is None:
        return ContinuityStatusSummary(
            **base,
            continuity_reason_code="MANIFEST_INVENTORY_UNAVAILABLE",
        )
    try:
        raw_scan = scanner.scan(
            configured_start=configured_start,
            latest_completed_session=latest_completed_session,
        )
        if isinstance(raw_scan, ContinuityUnavailable):
            scan: ContinuityScanResult | ContinuityUnavailable = (
                ContinuityUnavailable.model_validate(
                    raw_scan.model_dump(mode="python", round_trip=True)
                )
            )
        else:
            scan = ContinuityScanResult.model_validate(
                raw_scan.model_dump(mode="python", round_trip=True)
            )
    except Exception:
        return ContinuityStatusSummary(
            **base,
            continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
        )
    if isinstance(scan, ContinuityUnavailable):
        return ContinuityStatusSummary(
            **base,
            continuity_reason_code=scan.reason_code,
        )

    queue: RepairQueueSnapshot | None = None
    if queue_snapshot is not None:
        try:
            queue = RepairQueueSnapshot.model_validate(
                queue_snapshot.model_dump(mode="python", round_trip=True)
            )
        except Exception:
            return ContinuityStatusSummary(
                **base,
                continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
            )

    missing = scan.missing_sessions
    if not missing:
        return ContinuityStatusSummary(
            **base,
            continuity_status="current",
        )

    # A queue snapshot is optional only for a current scan.  For gaps, its absence or
    # unavailable status means the controller cannot prove control state and must fail closed.
    if queue is None:
        return ContinuityStatusSummary(
            **base,
            missing_session_count=len(missing),
            oldest_missing_session=missing[0],
            continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
        )
    if queue.status != "ready":
        return ContinuityStatusSummary(
            **base,
            missing_session_count=len(missing),
            oldest_missing_session=missing[0],
            continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
        )

    missing_set = set(missing)
    jobs = tuple(
        job
        for job in queue.jobs
        if job.universe_id == _R2F1_UNIVERSE_ID and job.trade_date in missing_set
    )
    pending = sum(job.state == "pending" for job in jobs)
    retry_wait = sum(job.state == "retry_wait" for job in jobs)
    active = sum(job.state == "leased" for job in jobs)
    dead_letter = sum(job.state == "dead_letter" for job in jobs)

    active_lane: ContinuityLane | None = None
    timestamp = now or datetime.now(UTC)
    try:
        timestamp = require_utc(timestamp)
    except RepairQueueError:
        return ContinuityStatusSummary(
            **base,
            missing_session_count=len(missing),
            oldest_missing_session=missing[0],
            repair_pending_count=pending,
            repair_retry_wait_count=retry_wait,
            repair_active_count=active,
            repair_dead_letter_count=dead_letter,
            continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
        )
    for job in jobs:
        if (
            job.state == "leased"
            and job.lease_expires_at is not None
            and job.lease_expires_at > timestamp
        ):
            active_lane = "repair"
            break
    if active_lane is None and freshness_state is not None:
        refresh_state = getattr(freshness_state, "refresh_state", None)
        if refresh_state not in {
            None,
            "disabled",
            "idle",
            "scheduled",
            "retry_wait",
            "success",
            "delayed",
            "error",
            "running",
        }:
            return ContinuityStatusSummary(
                **base,
                missing_session_count=len(missing),
                oldest_missing_session=missing[0],
                repair_pending_count=pending,
                repair_retry_wait_count=retry_wait,
                repair_active_count=active,
                repair_dead_letter_count=dead_letter,
                continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
            )
        if refresh_state == "running":
            active_lane = "freshness"

    counts = dict(
        missing_session_count=len(missing),
        oldest_missing_session=missing[0],
        repair_pending_count=pending,
        repair_retry_wait_count=retry_wait,
        repair_active_count=active,
        repair_dead_letter_count=dead_letter,
        active_lane=active_lane,
    )
    if not repair_enabled:
        return ContinuityStatusSummary(
            **base,
            continuity_status="blocked",
            **counts,
            continuity_reason_code="REPAIR_EXECUTION_DISABLED",
        )

    if pending == 0 and retry_wait == 0 and active == 0:
        return ContinuityStatusSummary(
            **base,
            continuity_status="blocked",
            **counts,
            continuity_reason_code=(
                "REPAIR_DEAD_LETTER_ONLY" if dead_letter else "REPAIR_QUEUE_NOT_ENQUEUED"
            ),
        )

    if provider_health is not None:
        try:
            if isinstance(provider_health, ProviderHealth):
                provider_health = ProviderHealth.model_validate(
                    provider_health.model_dump(mode="python", round_trip=True)
                )
            else:
                # Keep the pure summary helper usable with lightweight test doubles, while
                # still constraining non-model snapshots to the public circuit-state enum.
                provider_state = getattr(provider_health, "state", None)
                provider_state = getattr(provider_state, "value", provider_state)
                if provider_state not in {"CLOSED", "OPEN", "HALF_OPEN"}:
                    raise ValueError("provider health state is invalid")
        except Exception:
            return ContinuityStatusSummary(
                **base,
                **counts,
                continuity_reason_code="CONTROL_STATE_UNAVAILABLE",
            )

    provider_state = getattr(provider_health, "state", None)
    provider_state = getattr(provider_state, "value", provider_state)
    if provider_state == "OPEN":
        reason: ContinuityStatusReason = "PROVIDER_CIRCUIT_OPEN"
    elif provider_state == "HALF_OPEN":
        reason = "PROVIDER_CIRCUIT_HALF_OPEN"
    elif provider_state != "CLOSED":
        reason = "PROVIDER_HEALTH_UNAVAILABLE"
    else:
        reason = None
    if reason is not None:
        return ContinuityStatusSummary(
            **base,
            continuity_status="blocked",
            **counts,
            continuity_reason_code=reason,
        )
    return ContinuityStatusSummary(
        **base,
        continuity_status="gaps",
        **counts,
    )


class FreshnessDecision(Protocol):
    action: str
    target_session: date | None
    refresh_state: str
    next_run_at: datetime | None


class RepairClaimStore(Protocol):
    def repair_queue_snapshot(self) -> RepairQueueSnapshot: ...

    def claim_repair_job(
        self,
        job_id: str,
        *,
        owner: str,
        expected_version: int,
        now: datetime,
        lease_seconds: int,
    ) -> RepairLease | None: ...


class ProviderHealthReader(Protocol):
    def provider_health(self) -> object: ...


class ContinuityPolicy:
    """Wrap the existing freshness decision without reproducing its slot policy."""

    def decide(
        self,
        *,
        freshness: FreshnessDecision,
        jobs: tuple[RepairJob, ...] | None,
        repair_enabled: bool,
        now: datetime,
    ) -> ContinuityDecision:
        timestamp = require_utc(now)
        target = freshness.target_session
        if target is None:
            return ContinuityDecision(
                action="none",
                lane="freshness",
                target_session=None,
                reason_code="CALENDAR_UNAVAILABLE",
            )
        if freshness.action == "run":
            return ContinuityDecision(
                action="run",
                lane="freshness",
                target_session=target,
                next_run_at=freshness.next_run_at,
                reason_code="FRESHNESS_DUE",
            )
        if not repair_enabled:
            return ContinuityDecision(
                action=freshness.action,
                lane="freshness",
                target_session=target,
                next_run_at=freshness.next_run_at,
                reason_code="REPAIR_DISABLED",
            )
        can_repair = freshness.action == "none" and freshness.refresh_state == "success"
        can_repair = can_repair or (
            freshness.action == "wait"
            and freshness.refresh_state == "retry_wait"
            and freshness.next_run_at is not None
            and require_utc(freshness.next_run_at) > timestamp
        )
        if not can_repair:
            return ContinuityDecision(
                action=freshness.action,
                lane="freshness",
                target_session=target,
                next_run_at=freshness.next_run_at,
                reason_code="FRESHNESS_WAIT",
            )
        if jobs is None:
            return ContinuityDecision(
                action=freshness.action,
                lane="freshness",
                target_session=target,
                next_run_at=freshness.next_run_at,
                reason_code="CONTROL_STATE_UNAVAILABLE",
            )
        validated = tuple(RepairJob.model_validate(item.model_dump(mode="python")) for item in jobs)
        eligible = tuple(
            sorted(
                (
                    job
                    for job in validated
                    if job.trade_date < target
                    and (
                        job.state == "pending"
                        or (
                            job.state == "retry_wait"
                            and job.next_attempt_at is not None
                            and job.next_attempt_at <= timestamp
                        )
                    )
                ),
                key=lambda job: (job.trade_date, job.job_id),
            )
        )
        if not eligible:
            return ContinuityDecision(
                action=freshness.action,
                lane="freshness",
                target_session=target,
                next_run_at=freshness.next_run_at,
                reason_code="NO_ELIGIBLE_REPAIR",
            )
        selected = eligible[0]
        return ContinuityDecision(
            action="run",
            lane="repair",
            target_session=selected.trade_date,
            repair_job_id=selected.job_id,
            next_run_at=freshness.next_run_at,
            reason_code="REPAIR_READY",
        )


class RepairClaimCoordinator:
    """Revalidate and synchronously hand one claimed repair lease to its owner."""

    def __init__(
        self,
        *,
        store: RepairClaimStore,
        health_store: ProviderHealthReader,
        lock_path: Path,
        owner: str,
        lease_seconds: int,
        policy: ContinuityPolicy | None = None,
    ) -> None:
        self._store = store
        self._health_store = health_store
        self._lock_path = lock_path
        self._owner = require_safe_identifier(owner)
        if (
            not isinstance(lease_seconds, int)
            or isinstance(lease_seconds, bool)
            or not 60 <= lease_seconds <= 86400
        ):
            raise RepairQueueError("repair claim lease policy is invalid")
        self._lease_seconds = lease_seconds
        self._policy = policy or ContinuityPolicy()

    def claim_ready_once(
        self,
        *,
        freshness: FreshnessDecision,
        latest_expected_session: date,
        repair_enabled: bool,
        now: datetime,
        revalidator: Callable[[], FreshnessDecision],
        lease_consumer: Callable[[RepairLease], object] | None,
        pre_claim: Callable[[], object] | None = None,
    ) -> ContinuityDecision:
        timestamp = require_utc(now)
        initial = self._decision(freshness, repair_enabled, timestamp)
        needs_locked_preflight = (
            pre_claim is not None and repair_enabled and lease_consumer is not None
        )
        if (initial.action != "run" or initial.lane != "repair") and not needs_locked_preflight:
            return initial
        if (
            freshness.target_session != latest_expected_session
            or initial.target_session is None
            or initial.target_session >= latest_expected_session
        ) and not needs_locked_preflight:
            return self._freshness_wait(freshness, "NO_ELIGIBLE_REPAIR")
        if not needs_locked_preflight:
            health_closed, _health_unavailable = self._health_status()
            if not health_closed:
                # Before entering the shared lock, every non-CLOSED state is an unavailable
                # claim gate.  The more specific PROVIDER_NOT_CLOSED reason is reserved for a
                # state that changed while the lock was held.
                return self._repair_wait(initial, "PROVIDER_HEALTH_UNAVAILABLE")
        if lease_consumer is None:
            return initial

        try:
            from backend.app.market.automation import RefreshAlreadyRunning, RefreshRunLock
        except Exception:
            return self._repair_wait(initial, "CONTROL_STATE_UNAVAILABLE")

        try:
            with RefreshRunLock(self._lock_path):
                if pre_claim is not None:
                    pre_claim()
                refreshed_freshness = revalidator()
                refreshed = self._decision(refreshed_freshness, repair_enabled, timestamp)
                if refreshed.action != "run" or refreshed.lane != "repair":
                    return refreshed
                if (
                    refreshed.target_session is None
                    or refreshed.target_session >= refreshed_freshness.target_session
                ):
                    return self._freshness_wait(
                        refreshed_freshness,
                        "NO_ELIGIBLE_REPAIR",
                    )
                health_closed, health_unavailable = self._health_status()
                if not health_closed:
                    return self._repair_wait(
                        refreshed,
                        "PROVIDER_HEALTH_UNAVAILABLE"
                        if health_unavailable
                        else "PROVIDER_NOT_CLOSED",
                    )
                snapshot = self._store.repair_queue_snapshot()
                refreshed = self._policy.decide(
                    freshness=refreshed_freshness,
                    jobs=snapshot.jobs if snapshot.status == "ready" else None,
                    repair_enabled=repair_enabled,
                    now=timestamp,
                )
                if refreshed.action != "run" or refreshed.lane != "repair":
                    return refreshed
                job = next(
                    (item for item in snapshot.jobs if item.job_id == refreshed.repair_job_id),
                    None,
                )
                if snapshot.status != "ready" or job is None:
                    return self._repair_wait(refreshed, "CONTROL_STATE_UNAVAILABLE")
                lease = self._store.claim_repair_job(
                    job.job_id,
                    owner=self._owner,
                    expected_version=job.state_version,
                    now=timestamp,
                    lease_seconds=self._lease_seconds,
                )
                if lease is None:
                    return self._repair_wait(refreshed, "NO_ELIGIBLE_REPAIR")
                try:
                    lease_consumer(lease)
                except Exception:
                    return self._repair_wait(refreshed, "REPAIR_CONSUMER_FAILED")
                return self._replace_decision(refreshed, reason_code="REPAIR_CLAIMED")
        except RefreshAlreadyRunning:
            return self._repair_wait(initial, "ALREADY_RUNNING")
        except Exception:
            return self._repair_wait(initial, "CONTROL_STATE_UNAVAILABLE")

    def _decision(
        self,
        freshness: FreshnessDecision,
        repair_enabled: bool,
        now: datetime,
    ) -> ContinuityDecision:
        snapshot = self._store.repair_queue_snapshot()
        return self._policy.decide(
            freshness=freshness,
            jobs=snapshot.jobs if snapshot.status == "ready" else None,
            repair_enabled=repair_enabled,
            now=now,
        )

    def _health_status(self) -> tuple[bool, bool]:
        try:
            health = self._health_store.provider_health()
        except Exception:
            return False, True
        return getattr(health, "state", None) == "CLOSED", False

    @staticmethod
    def _repair_wait(
        decision: ContinuityDecision,
        reason: ContinuityDecisionReason,
    ) -> ContinuityDecision:
        return RepairClaimCoordinator._replace_decision(
            decision,
            action="wait",
            reason_code=reason,
        )

    @staticmethod
    def _replace_decision(
        decision: ContinuityDecision,
        **updates: object,
    ) -> ContinuityDecision:
        payload = decision.model_dump(mode="python")
        payload.update(updates)
        return ContinuityDecision.model_validate(payload)

    @staticmethod
    def _freshness_wait(
        freshness: FreshnessDecision,
        reason: ContinuityDecisionReason,
    ) -> ContinuityDecision:
        return ContinuityDecision(
            action=freshness.action,
            lane="freshness",
            target_session=freshness.target_session,
            next_run_at=freshness.next_run_at,
            reason_code=reason,
        )


RepairExecutionStatus = Literal["published", "failed", "skipped"]
RepairExecutionReason = Literal[
    "REPAIR_PUBLISHED",
    "POST_PUBLISH_INVENTORY_MISSING",
    "REPAIR_RESULT_FAILED",
    "REPAIR_DISABLED",
    "FRESHNESS_DUE",
    "FRESHNESS_WAIT",
    "CONTROL_STATE_UNAVAILABLE",
    "PROVIDER_HEALTH_UNAVAILABLE",
    "PROVIDER_NOT_CLOSED",
    "NO_ELIGIBLE_REPAIR",
    "ALREADY_RUNNING",
    "REPAIR_CONSUMER_FAILED",
]


class RepairExecutionResult(BaseModel):
    """Sanitized result of one bounded continuity repair invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: RepairExecutionStatus
    decision: ContinuityDecision
    refresh_result: RefreshResult | None = None
    reason_code: RepairExecutionReason
    provider_requests: Literal[0, 1] = 0

    @field_validator("provider_requests", mode="before")
    @classmethod
    def strict_provider_request_count(cls, value: object) -> int:
        if type(value) is not int or value not in (0, 1):
            raise ValueError("provider request count must be 0 or 1")
        return value

    @model_validator(mode="after")
    def valid_execution_matrix(self) -> "RepairExecutionResult":
        decision = self.decision
        refresh = self.refresh_result
        target = decision.target_session

        if self.status == "published":
            if (
                decision.action != "run"
                or decision.lane != "repair"
                or decision.reason_code != "REPAIR_CLAIMED"
                or target is None
                or self.reason_code != "REPAIR_PUBLISHED"
                or self.provider_requests != 1
                or refresh is None
            ):
                raise ValueError("published repair execution evidence is inconsistent")
            self._validate_refresh_evidence(refresh, target, require_ready=True)
            return self

        if self.status == "failed":
            if decision.lane != "repair" or target is None:
                raise ValueError("failed repair execution requires a repair decision")
            if self.reason_code in {"REPAIR_RESULT_FAILED", "POST_PUBLISH_INVENTORY_MISSING"}:
                if (
                    decision.action != "run"
                    or decision.reason_code != "REPAIR_CLAIMED"
                    or self.provider_requests != 1
                    or refresh is None
                ):
                    raise ValueError("failed repair publication evidence is inconsistent")
                self._validate_refresh_evidence(refresh, target, require_ready=False)
                if refresh.status not in {"partial", "error"}:
                    raise ValueError("failed repair publication requires partial or error result")
                return self
            if self.reason_code == "REPAIR_CONSUMER_FAILED":
                if decision.lane != "repair" or (
                    (decision.action, decision.reason_code)
                    not in {
                        ("wait", "REPAIR_CONSUMER_FAILED"),
                        # A consumer may fail before the coordinator rewrites the claimed
                        # decision.  This is an explicit, sanitized no-refresh-result contract;
                        # it is never accepted for any other failure reason.
                        ("run", "REPAIR_CLAIMED"),
                    }
                ):
                    raise ValueError("consumer failure requires a repair wait decision")
                if refresh is not None:
                    if self.provider_requests != 1:
                        raise ValueError(
                            "consumer failure with refresh evidence requires one provider request"
                        )
                    self._validate_refresh_evidence(refresh, target, require_ready=False)
                    if refresh.status not in {"partial", "error"}:
                        raise ValueError("consumer failure result must be partial or error")
                return self
            raise ValueError("failed repair execution reason is not allowlisted")

        # A skipped invocation has no provider or publication evidence.  A freshness run is
        # allowed to be represented as skipped when the repair lane yielded to it; a claimed
        # repair run is never allowed to masquerade as skipped.
        if refresh is not None or self.provider_requests != 0:
            raise ValueError("skipped repair execution cannot carry provider evidence")
        if (
            decision.action == "run"
            and decision.lane == "repair"
            and decision.reason_code == "REPAIR_CLAIMED"
        ):
            raise ValueError("claimed repair execution cannot be skipped")
        allowed_skip_reasons = {
            "REPAIR_DISABLED",
            "FRESHNESS_DUE",
            "FRESHNESS_WAIT",
            "CONTROL_STATE_UNAVAILABLE",
            "PROVIDER_HEALTH_UNAVAILABLE",
            "PROVIDER_NOT_CLOSED",
            "NO_ELIGIBLE_REPAIR",
            "ALREADY_RUNNING",
            "REPAIR_CONSUMER_FAILED",
        }
        if self.reason_code not in allowed_skip_reasons:
            raise ValueError("skipped repair execution reason is not allowlisted")
        if decision.lane == "repair" and decision.action == "wait":
            if self.reason_code != decision.reason_code:
                raise ValueError("skipped repair reason does not match decision")
        elif decision.lane == "freshness":
            if self.reason_code != decision.reason_code:
                raise ValueError("skipped freshness reason does not match decision")
        return self

    def public_payload(self) -> dict[str, object]:
        """Return the bounded repair evidence contract for public consumers.

        ``RefreshResult`` remains the internal persistence/finalization model and its
        ``model_dump`` intentionally retains the sanitized diagnostic message needed by
        storage.  Public repair consumers must use this explicit projection instead; no
        arbitrary string fields (message, run id, request key, symbols, or paths) cross the
        boundary.
        """
        # ``model_copy``/``model_construct`` can bypass Pydantic validators.  Revalidate the
        # complete internal object before projecting any field across this boundary.
        validated = type(self).model_validate(self.model_dump(mode="python"))
        refresh_payload: dict[str, object] | None = None
        if validated.refresh_result is not None:
            refresh = validated.refresh_result
            refresh_payload = {
                "status": refresh.status,
                "requested_date": refresh.requested_date.isoformat(),
                "run_kind": refresh.run_kind,
                "requested_count": refresh.requested_count,
                "succeeded_count": refresh.succeeded_count,
                "coverage_ratio": refresh.coverage_ratio,
                "failure_stage": refresh.failure_stage,
                "failure_class": refresh.failure_class,
                "retryable": refresh.retryable,
            }
        decision = validated.decision
        decision_payload = {
            "action": decision.action,
            "lane": decision.lane,
            "target_session": (
                decision.target_session.isoformat() if decision.target_session is not None else None
            ),
            "next_run_at": (
                decision.next_run_at.isoformat() if decision.next_run_at is not None else None
            ),
            "reason_code": decision.reason_code,
        }
        return {
            "status": validated.status,
            "decision": decision_payload,
            "refresh_result": refresh_payload,
            "reason_code": validated.reason_code,
        }

    @staticmethod
    def _validate_refresh_evidence(
        refresh: RefreshResult,
        target: date,
        *,
        require_ready: bool,
    ) -> None:
        if refresh.run_kind != "repair":
            raise ValueError("repair evidence must use repair run kind")
        if refresh.requested_date != target:
            raise ValueError("repair evidence date does not match decision target")
        if refresh.request_key != repair_request_key(target):
            raise ValueError("repair evidence request key does not match repair job")
        try:
            started_at = require_utc(refresh.started_at)
            completed_at = require_utc(refresh.completed_at)
        except RepairQueueError as exc:
            raise ValueError("repair evidence timestamps must be UTC-aware") from exc
        if completed_at < started_at:
            raise ValueError("repair evidence completed time precedes start")
        if require_ready and refresh.status != "ready":
            raise ValueError("published repair requires ready refresh evidence")


class ContinuityRepairExecutor:
    """Execute at most one leased full-session repair under the coordinator lock.

    ``RepairClaimCoordinator`` owns the cross-process lock for the complete callback lifetime.
    The callback therefore must not acquire ``RefreshRunLock`` again; it only performs canonical
    publication, strict post-publication evidence and the queue CAS finalization while that same
    lock remains held.
    """

    def __init__(
        self,
        *,
        coordinator: RepairClaimCoordinator,
        queue_store: RepairClaimStore,
        canonical_store,
        provider,
        inventory_reader: VerifiedInventoryReader,
        required_symbols: Callable[[], set[str]],
        clock: Callable[[], datetime],
        retry_policy: RepairRetryPolicy,
        before_store: Callable[[], None] | None = None,
        post_publish: Callable[[RefreshResult], None] | None = None,
        pre_claim: Callable[[], object] | None = None,
        scanner: ContinuityInventory | None = None,
        configured_start: date | None = None,
        queue_schema_initializer: Callable[[], None] | None = None,
        health_store=None,
    ) -> None:
        self.coordinator = coordinator
        self.queue_store = queue_store
        self.canonical_store = canonical_store
        self.provider = provider
        self.inventory_reader = inventory_reader
        self.required_symbols = required_symbols
        self.clock = clock
        self.retry_policy = retry_policy
        self.before_store = before_store
        self.post_publish = post_publish
        self.pre_claim = pre_claim
        self.scanner = scanner
        self.configured_start = configured_start
        self.queue_schema_initializer = queue_schema_initializer
        self.health_store = health_store

    def execute_once(
        self,
        *,
        freshness: FreshnessDecision,
        latest_expected_session: date,
        repair_enabled: bool,
        revalidator: Callable[[], FreshnessDecision],
    ) -> RepairExecutionResult:
        timestamp = require_utc(self.clock())
        holder: dict[str, RefreshResult] = {}

        def prepare() -> None:
            # A strict inventory read is the only authority for crash reconciliation.  When a
            # scanner is configured it also proves calendar coverage before any queue mutation.
            scan: ContinuityScanResult | ContinuityUnavailable | None = None
            inventory: VerifiedReadySessionInventory | None = None
            if self.scanner is not None:
                if self.configured_start is None:
                    raise RepairQueueError("continuity start is unconfigured")
                scan = self.scanner.scan(
                    configured_start=self.configured_start,
                    latest_completed_session=latest_expected_session,
                )
                if isinstance(scan, ContinuityUnavailable):
                    raise RepairQueueError("continuity evidence is unavailable")
            else:
                inventory = self.inventory_reader.verified_ready_session_inventory("baostock")
            if self.queue_schema_initializer is not None:
                self.queue_schema_initializer()
            if scan is not None:
                self.queue_store.reconcile_published_repair_jobs(
                    scan.published_ready_sessions,
                    now=timestamp,
                )
                self.queue_store.enqueue_repair_jobs(
                    scan.missing_sessions,
                    universe_id=_R2F1_UNIVERSE_ID,
                    now=timestamp,
                )
            else:
                assert inventory is not None
                self.queue_store.reconcile_published_repair_jobs(
                    inventory.sessions,
                    now=timestamp,
                )
            reap_expired = getattr(self.queue_store, "reap_expired_repair_leases", None)
            if callable(reap_expired):
                reap_expired(now=timestamp, retry_policy=self.retry_policy)
            if self.pre_claim is not None:
                self.pre_claim()

        def consume(lease: RepairLease) -> None:
            request_key = repair_request_key(lease.target_session)
            run_id = require_safe_identifier(
                f"repair-{lease.target_session.isoformat()}-{lease.attempt_id}"
            )
            from backend.app.market.automation import run_publication_refresh

            required_symbols_error = False
            provider_called = False
            try:
                required_symbols = self.required_symbols()
            except Exception:
                required_symbols = set()
                required_symbols_error = True
            before_store = self.before_store
            refresh_operation = getattr(self.provider, "refresh_operation", None)
            if self.health_store is not None:
                from backend.app.market.automation import (
                    _RefreshObservationCollector,
                    transport_observation_sink,
                )

                collector = _RefreshObservationCollector(self.health_store, run_id)

                def before_store_with_audit() -> None:
                    collector.resolve_touched_endpoints()
                    if before_store is not None:
                        before_store()

                before_store = before_store_with_audit
                provider_scope_factory = (
                    (lambda: refresh_operation(run_id))
                    if callable(refresh_operation)
                    else contextlib.nullcontext
                )
                observation_scope = transport_observation_sink(collector.record)
            else:
                provider_scope_factory = (
                    (lambda: refresh_operation(run_id))
                    if callable(refresh_operation)
                    else contextlib.nullcontext
                )
                observation_scope = contextlib.nullcontext()
            if required_symbols_error:
                result = self._unexpected_failure(
                    lease.target_session,
                    request_key=request_key,
                    run_id=run_id,
                    started_at=timestamp,
                )
            else:
                # Record the provider boundary before entering it.  A later queue/control
                # failure must not erase the fact that this repair attempted one provider
                # refresh.  BaseException is intentionally not caught below so process-kill
                # remains recoverable through manifest reconciliation.
                provider_called = True
                holder["provider_requests"] = 1
                try:
                    with observation_scope, provider_scope_factory():
                        result = run_publication_refresh(
                            self.canonical_store,
                            self.provider,
                            trade_date=lease.target_session,
                            required_symbols=required_symbols,
                            request_key=request_key,
                            run_id=run_id,
                            run_kind="repair",
                            before_store=before_store,
                        )
                except Exception:
                    result = self._unexpected_failure(
                        lease.target_session,
                        request_key=request_key,
                        run_id=run_id,
                        started_at=timestamp,
                    )
            if not isinstance(result, RefreshResult):
                raise RepairQueueError("repair publication result is invalid")
            if result.status == "ready":
                try:
                    inventory = self.inventory_reader.verified_ready_session_inventory("baostock")
                except Exception as error:
                    # A ready result is not a complete repair until the strict post-publish
                    # inventory can be read back.  Convert storage/control failures to the
                    # sanitized publication failure and finalize the lease exactly once.
                    result = self._storage_failure(result, error=error)
                else:
                    if lease.target_session not in inventory.sessions:
                        result = self._storage_failure(result)
                    elif self.post_publish is not None:
                        try:
                            self.post_publish(result)
                        except Exception:
                            result = self._storage_failure(result)
            # A pre-provider failure still needs the internal RefreshResult for queue
            # finalization, but it is not public provider evidence.  Expose a refresh result
            # only after the provider boundary was entered so a zero-count result can never
            # be returned to RepairExecutionResult consumers.
            if provider_called:
                holder["result"] = result
            holder["provider_requests"] = 1 if provider_called else 0
            finalization_time = max(timestamp, require_utc(result.completed_at))
            outcome: RepairFinalizationOutcome = (
                "succeeded" if result.status == "ready" else "failed"
            )
            self.queue_store.finalize_repair_attempt(
                lease,
                outcome=outcome,
                refresh_result=result,
                now=finalization_time,
                retry_policy=self.retry_policy,
            )

        decision = self.coordinator.claim_ready_once(
            freshness=freshness,
            latest_expected_session=latest_expected_session,
            repair_enabled=repair_enabled,
            now=timestamp,
            revalidator=revalidator,
            lease_consumer=consume,
            pre_claim=prepare,
        )
        result = holder.get("result")
        provider_requests = holder.get("provider_requests", 0)
        if decision.reason_code == "REPAIR_CLAIMED":
            if result is not None:
                if result.status == "ready":
                    return RepairExecutionResult(
                        status="published",
                        decision=decision,
                        refresh_result=result,
                        reason_code="REPAIR_PUBLISHED",
                        provider_requests=provider_requests,
                    )
                return RepairExecutionResult(
                    status="failed",
                    decision=decision,
                    refresh_result=result,
                    reason_code=(
                        "POST_PUBLISH_INVENTORY_MISSING"
                        if result.failure_class == "storage" and result.failure_stage == "publish"
                        else "REPAIR_RESULT_FAILED"
                    ),
                    provider_requests=provider_requests,
                )
            # The lease was claimed, but the consumer failed before entering the provider
            # boundary.  There is deliberately no refresh evidence and therefore no provider
            # request count to report.
            return RepairExecutionResult(
                status="failed",
                decision=decision,
                reason_code="REPAIR_CONSUMER_FAILED",
                provider_requests=provider_requests,
            )
        # Preserve a sanitized result if finalization/control handling itself failed after
        # the provider callback.  The coordinator's generic wait reason must not report zero
        # provider requests for a callback that already ran.
        if decision.reason_code == "REPAIR_CONSUMER_FAILED":
            return RepairExecutionResult(
                status="failed",
                decision=decision,
                refresh_result=result,
                reason_code="REPAIR_CONSUMER_FAILED",
                provider_requests=provider_requests,
            )
        return RepairExecutionResult(
            status="skipped",
            decision=decision,
            reason_code=self._skip_reason(decision.reason_code),
            provider_requests=provider_requests,
        )

    @staticmethod
    def _storage_failure(
        result: RefreshResult,
        *,
        error: Exception | None = None,
    ) -> RefreshResult:
        retryable = False
        if error is not None:
            # Dataset integrity failures are terminal for this candidate.  A local control
            # database read outage is a transient storage failure and follows the existing
            # retry policy.  Arbitrary exceptions remain non-retryable and sanitized.
            from backend.app.market.store import MarketStoreReadError

            retryable = isinstance(error, MarketStoreReadError)
        return result.model_copy(
            update={
                "status": "error",
                "error_message": "market publication failed",
                "quality_issues": ["market_refresh_failed"],
                "failure_stage": "publish",
                "failure_class": "storage",
                "retryable": retryable,
            }
        )

    @staticmethod
    def _unexpected_failure(
        trade_date: date,
        *,
        request_key: str,
        run_id: str,
        started_at: datetime,
    ) -> RefreshResult:
        return RefreshResult(
            run_id=run_id,
            request_key=request_key,
            run_kind="repair",
            requested_date=trade_date,
            source="baostock",
            status="error",
            requested_count=0,
            succeeded_count=0,
            coverage_ratio=0,
            quality_issues=["market_refresh_failed"],
            error_message="market refresh failed",
            failure_stage="fetch",
            failure_class="internal",
            retryable=False,
            started_at=started_at,
            completed_at=started_at,
        )

    @staticmethod
    def _skip_reason(reason: ContinuityDecisionReason) -> RepairExecutionReason:
        if reason in {
            "REPAIR_DISABLED",
            "FRESHNESS_DUE",
            "FRESHNESS_WAIT",
            "CONTROL_STATE_UNAVAILABLE",
            "PROVIDER_HEALTH_UNAVAILABLE",
            "PROVIDER_NOT_CLOSED",
            "NO_ELIGIBLE_REPAIR",
            "ALREADY_RUNNING",
            "REPAIR_CONSUMER_FAILED",
        }:
            return reason
        return "CONTROL_STATE_UNAVAILABLE"
