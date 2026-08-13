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
ContinuityInventoryMode = Literal["immutable_dataset", "local_mutable"]

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_R2F1_UNIVERSE_ID = "all-main-board"


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
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
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
        store: ContinuityQueueWriter,
        lock_path: Path,
        universe_id: str,
        clock: Callable[[], datetime],
        lock_factory: Callable[[Path], RefreshLock] | None = None,
    ) -> None:
        self._scanner = scanner
        self._store = store
        self._lock_path = lock_path
        self._universe_id = require_universe_id(universe_id)
        self._clock = clock
        self._lock_factory = lock_factory

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
            try:
                ContinuityScanResult.model_validate(completed_scan.model_dump(mode="python"))
            except Exception:
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
            preflight = self._scanner.scan(
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
                    fresh = self._scanner.scan(
                        configured_start=configured_start,
                        latest_completed_session=latest_completed_session,
                        requested_start=requested_start,
                        requested_end=requested_end,
                    )
                    if isinstance(fresh, ContinuityUnavailable):
                        return ContinuityEnqueueResult(
                            status="unavailable",
                            reason_code=fresh.reason_code,
                        )
                    scan_identity = self._scan_identity(fresh)
                    self._store.initialize_continuity_schema()
                    if fresh.status == "current":
                        return ContinuityEnqueueResult(
                            status="current",
                            fresh_scan=fresh,
                            manifest_generation=fresh.manifest_generation,
                            scan_identity=scan_identity,
                            writes_control_state=True,
                        )
                    created = tuple(
                        self._store.enqueue_repair_jobs(
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
