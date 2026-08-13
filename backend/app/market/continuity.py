import re
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from backend.app.market.failures import MarketFailureClass, MarketFailureStage

RepairJobState = Literal["pending", "leased", "retry_wait", "published", "dead_letter"]
RepairAttemptOutcome = Literal["running", "succeeded", "failed", "abandoned"]
RepairFinalizationOutcome = Literal["succeeded", "failed"]
ContinuityControlReason = Literal["CONTROL_STATE_UNAVAILABLE"]

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_R2F1_UNIVERSE_ID = "all-main-board"


class RepairQueueError(RuntimeError):
    """A sanitized continuity queue contract or storage failure."""


class RepairQueueConflictError(RepairQueueError):
    """A stale lease or state version failed its compare-and-swap."""


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
        elif any(value is not None for value in lease_fields):
            raise ValueError("only a leased repair job may carry a lease")
        if (self.attempt_count == 0) != (self.last_attempt_id is None):
            raise ValueError("repair job attempt count and reference do not match")
        if self.state == "retry_wait":
            if self.next_attempt_at is None:
                raise ValueError("retry-wait repair job requires a due time")
        elif self.next_attempt_at is not None:
            raise ValueError("only a retry-wait repair job may carry a due time")
        if self.state == "published":
            if self.published_at is None:
                raise ValueError("published repair job requires publication time")
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
            self.failure_stage is not None
            or self.failure_class is not None
            or self.retryable is not False
        ):
            raise ValueError("successful repair attempt cannot carry a failure")
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
                job_attempts = attempts_by_job[job.job_id]
                if sorted(item.attempt_number for item in job_attempts) != list(
                    range(1, job.attempt_count + 1)
                ):
                    raise ValueError("repair job attempt sequence is incomplete")
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
                ):
                    raise ValueError("repair lease attempt reference is invalid")
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
