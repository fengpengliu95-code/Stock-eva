"""Immutable, reviewed calendar generations for R2-F4.1.

This module is deliberately self contained.  It is a control-plane store: it never
changes the bundled calendar and it does not construct a market-data provider.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .calendar import TradingCalendar

SHANGHAI = ZoneInfo("Asia/Shanghai")
DATA_AVAILABLE_AT = (18, 10)
MAX_SOURCE_BYTES = 256 * 1024
MAX_BODY_BYTES = 1024 * 1024
MAX_MACHINE_DAYS = 366
MAX_GENERATIONS = 256
MAX_CANDIDATES = 1024
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
OFFICIAL_ORIGINS = frozenset({"www.sse.com.cn", "www.szse.cn", "investor.szse.cn"})
OUTCOMES = (
    "STAGED",
    "ALREADY_STAGED",
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
)
Outcome = Literal[
    "STAGED",
    "ALREADY_STAGED",
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


class CalendarGenerationError(ValueError):
    """Expected, sanitized calendar-control validation failure."""


class CalendarStoreUnavailable(RuntimeError):
    """The runtime store cannot safely be used."""


class CalendarAttemptAlreadySpent(CalendarStoreUnavailable):
    """The requested Shanghai maintenance slot already has an audit row."""


class _Frozen(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, allow_inf_nan=False, arbitrary_types_allowed=True
    )

    # Pydantic's escape hatches are public APIs.  Revalidate their result before
    # anything in this module treats an object as authority.
    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)

    @classmethod
    def model_construct(cls, _fields_set: set[str] | None = None, **values: Any):
        return cls.model_validate(values)


def canonical_json_bytes(value: Any) -> bytes:
    """Return the one canonical JSON representation used by authority hashes."""
    try:
        text = json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
    except (TypeError, ValueError) as exc:
        raise CalendarGenerationError("non-canonical JSON value") from exc
    return (text + "\n").encode("utf-8")


def _model_json_bytes(model: BaseModel) -> bytes:
    try:
        return canonical_json_bytes(model.model_dump(mode="json", warnings="error"))
    except (TypeError, ValueError) as exc:
        raise CalendarGenerationError("authority serialization invalid") from exc


def _revalidate_attempt(value: Any) -> CalendarMaintenanceAttempt:
    """Re-parse public attempt objects before any authority field is accessed."""
    if not isinstance(value, CalendarMaintenanceAttempt):
        raise CalendarGenerationError("attempt authority invalid")
    try:
        return CalendarMaintenanceAttempt.model_validate_json(_model_json_bytes(value))
    except (AttributeError, TypeError, ValueError) as exc:
        raise CalendarGenerationError("attempt authority invalid") from exc


def domain_sha256(domain: str, value: Any) -> str:
    if not domain.isascii():
        raise CalendarGenerationError("hash domain must be ASCII")
    return hashlib.sha256(domain.encode("ascii") + b"\n" + canonical_json_bytes(value)).hexdigest()


def body_sha256(body: bytes) -> str:
    if not isinstance(body, bytes):
        raise TypeError("body must be bytes")
    return hashlib.sha256(body).hexdigest()


def _check_digest(value: str, name: str = "digest") -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise CalendarGenerationError(f"invalid {name}")
    return value


class OfficialCalendarScheduleV1(_Frozen):
    exchange: Literal["SSE", "SZSE"]
    year: int = Field(ge=1900, le=9998)
    coverage_start: date
    coverage_end: date
    title: str = Field(min_length=1)
    notice_no: str = Field(min_length=1)
    official_url: str = Field(min_length=1)
    published_on: date
    body_sha256: str = Field(pattern=SHA256_RE.pattern)
    closed_dates: tuple[date, ...]
    review_id: str = Field(min_length=1)
    reviewed_on: datetime
    schedule_sha256: str = Field(pattern=SHA256_RE.pattern)

    @field_validator("review_id")
    @classmethod
    def _review_identity(cls, value: str) -> str:
        if not value.strip():
            raise CalendarGenerationError("review_id must not be blank")
        return value

    @model_validator(mode="after")
    def _shape(self) -> OfficialCalendarScheduleV1:
        if self.reviewed_on.tzinfo is None or self.reviewed_on.utcoffset() is None:
            raise CalendarGenerationError("reviewed_on must be timezone aware")
        object.__setattr__(self, "reviewed_on", self.reviewed_on.astimezone(UTC))
        if self.coverage_start != date(self.year, 1, 1) or self.coverage_end != date(
            self.year, 12, 31
        ):
            raise CalendarGenerationError("schedule must cover one complete year")
        if tuple(sorted(set(self.closed_dates))) != self.closed_dates:
            raise CalendarGenerationError("closed dates must be sorted and unique")
        if any(item.year != self.year or item.weekday() >= 5 for item in self.closed_dates):
            raise CalendarGenerationError("invalid closure date")
        parts = urlsplit(self.official_url)
        if (
            parts.scheme != "https"
            or parts.hostname not in OFFICIAL_ORIGINS
            or parts.username is not None
            or parts.password is not None
            or parts.query
            or parts.fragment
            or parts.port not in (None, 443)
        ):
            raise CalendarGenerationError("official URL is not allowlisted")
        return self


class CalendarSourceBundleV1(_Frozen):
    schema_version: Literal[1] = 1
    rule_version: Literal["cn-a-share-weekends-closed-v1"] = "cn-a-share-weekends-closed-v1"
    year: int = Field(ge=1900, le=9998)
    schedules: tuple[OfficialCalendarScheduleV1, OfficialCalendarScheduleV1]
    source_sha256: str = Field(pattern=SHA256_RE.pattern)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_schema_version(cls, value: Any) -> int:
        if type(value) is not int or value != 1:
            raise CalendarGenerationError("schema_version must be integer one")
        return value

    @model_validator(mode="after")
    def _shape(self) -> CalendarSourceBundleV1:
        if tuple(item.exchange for item in self.schedules) != ("SSE", "SZSE"):
            raise CalendarGenerationError("source schedules must be SSE then SZSE")
        if any(item.year != self.year for item in self.schedules):
            raise CalendarGenerationError("source schedule year mismatch")
        return self


class CalendarMachineDayV1(_Frozen):
    date: date
    is_open: bool


class CalendarMachineObservationV1(_Frozen):
    provider: Literal["baostock"]
    contract_version: Literal["r2f4.1-baostock-calendar-days-v1"]
    range_start: date
    range_end: date
    observed_at: datetime
    days: tuple[CalendarMachineDayV1, ...]
    observation_sha256: str = Field(pattern=SHA256_RE.pattern)

    @model_validator(mode="after")
    def _shape(self) -> CalendarMachineObservationV1:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise CalendarGenerationError("observed_at must be timezone aware")
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))
        if self.range_end != date(self.range_start.year, 12, 31) or self.range_start != date(
            self.range_start.year, 1, 1
        ):
            raise CalendarGenerationError("machine range must be a complete year")
        expected = tuple(
            self.range_start + timedelta(days=i)
            for i in range((self.range_end - self.range_start).days + 1)
        )
        if tuple(item.date for item in self.days) != expected or len(self.days) > MAX_MACHINE_DAYS:
            raise CalendarGenerationError("machine days must be complete and ordered")
        return self


class CalendarGenerationV1(_Frozen):
    sequence: int = Field(gt=0)
    parent_sha256: str | None = Field(default=None, pattern=SHA256_RE.pattern)
    bundled_sha256: str = Field(pattern=SHA256_RE.pattern)
    source_sha256: str = Field(pattern=SHA256_RE.pattern)
    attempt_target_year: int = Field(ge=1900, le=9998)
    attempt_slot_date: date
    official_body_hashes: tuple[str, str]
    machine: CalendarMachineObservationV1
    promoted_at: datetime
    generation_sha256: str = Field(pattern=SHA256_RE.pattern)

    @field_validator("official_body_hashes", mode="before")
    @classmethod
    def _body_hashes(cls, value: Any) -> tuple[str, str]:
        if not isinstance(value, (tuple, list)) or len(value) != 2:
            raise CalendarGenerationError("generation requires two body hashes")
        return tuple(_check_digest(item, "official body digest") for item in value)

    @model_validator(mode="after")
    def _timestamps(self) -> CalendarGenerationV1:
        if self.promoted_at.tzinfo is None or self.promoted_at.utcoffset() is None:
            raise CalendarGenerationError("promoted_at must be timezone aware")
        object.__setattr__(self, "promoted_at", self.promoted_at.astimezone(UTC))
        if self.machine.range_start.year != self.attempt_target_year:
            raise CalendarGenerationError("generation year mismatch")
        if self.machine.observed_at > self.promoted_at:
            raise CalendarGenerationError("machine observation is after promotion")
        return self


class CalendarMaintenanceAttempt(_Frozen):
    target_year: int = Field(ge=1900, le=9998)
    slot_date: date
    source_sha256: str = Field(pattern=SHA256_RE.pattern)
    expected_parent_sha256: str | None = Field(default=None, pattern=SHA256_RE.pattern)
    started_at: datetime
    finished_at: datetime | None = None
    outcome: Literal[
        "RUNNING",
        "OFFICIAL_UNAVAILABLE",
        "OFFICIAL_HASH_MISMATCH",
        "MACHINE_UNAVAILABLE",
        "MACHINE_CONFLICT",
        "SKIPPED_CIRCUIT_OPEN",
        "PARENT_CHANGED",
        "HISTORY_CHANGE",
        "CONTROL_STATE_UNAVAILABLE",
        "PROMOTED",
    ]
    official_requests: int = Field(default=0, ge=0, le=2)
    machine_requests: int = Field(default=0, ge=0, le=1)

    @model_validator(mode="after")
    def _timestamps(self) -> CalendarMaintenanceAttempt:
        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise CalendarGenerationError("started_at must be timezone aware")
        if self.finished_at is not None and (
            self.finished_at.tzinfo is None or self.finished_at.utcoffset() is None
        ):
            raise CalendarGenerationError("finished_at must be timezone aware")
        object.__setattr__(self, "started_at", self.started_at.astimezone(UTC))
        if self.finished_at is not None:
            object.__setattr__(self, "finished_at", self.finished_at.astimezone(UTC))
        if self.slot_date != self.started_at.astimezone(SHANGHAI).date():
            raise CalendarGenerationError("attempt slot does not match Shanghai date")
        if self.finished_at is not None and self.finished_at < self.started_at:
            raise CalendarGenerationError("attempt finished before it started")
        if (self.outcome == "RUNNING") != (self.finished_at is None):
            raise CalendarGenerationError("invalid attempt transition")
        if self.outcome == "RUNNING" and (self.official_requests, self.machine_requests) != (
            0,
            0,
        ):
            raise CalendarGenerationError("running attempt has requests")
        if self.outcome == "PROMOTED" and (self.official_requests, self.machine_requests) != (
            2,
            1,
        ):
            raise CalendarGenerationError("promoted attempt has invalid requests")
        return self


class CalendarStageResult(_Frozen):
    outcome: Outcome
    source_sha256: str | None = None
    admission: Literal["awaiting_machine", "quarantined"] | None = None


class CalendarPromotionResult(_Frozen):
    outcome: Outcome
    generation_sha256: str | None = None
    source_sha256: str | None = None
    official_requests: int = 0
    machine_requests: int = 0


class CalendarReadResult(_Frozen):
    status: Literal["ready", "unavailable"]
    generation: CalendarGenerationV1 | None = None
    bundled: tuple[dict[str, Any], ...] | None = None
    source: CalendarSourceBundleV1 | None = None
    calendar: ImmutableCalendarSnapshot | None = None


class CalendarSourceMetadata(_Frozen):
    exchange: str
    title: str
    url: str
    notice_no: str | None = None


class CalendarConfigSnapshot(_Frozen):
    year: int
    status: Literal["confirmed"]
    published_on: date
    sources: tuple[CalendarSourceMetadata, ...]
    closed_dates: tuple[date, ...]


class ImmutableCalendarSnapshot(TradingCalendar):
    """Concrete immutable calendar composed from bundled and promoted years."""

    def __init__(
        self,
        configs: tuple[CalendarConfigSnapshot, ...] = (),
        *,
        bundled_sha256: str | None = None,
        generation_sha256: str | None = None,
    ) -> None:
        object.__setattr__(
            self, "configs", MappingProxyType({config.year: config for config in configs})
        )
        object.__setattr__(self, "bundled_sha256", bundled_sha256)
        object.__setattr__(self, "generation_sha256", generation_sha256)

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(self, name):
            raise AttributeError("calendar snapshot is immutable")
        object.__setattr__(self, name, value)

    def snapshot(self) -> ImmutableCalendarSnapshot:
        return self


EMPTY_CALENDAR = ImmutableCalendarSnapshot()


class CalendarCandidate(_Frozen):
    source_sha256: str = Field(pattern=SHA256_RE.pattern)
    canonical_source_bytes: bytes
    staged_at: datetime
    staging_sequence: int = Field(gt=0)
    admission: Literal["awaiting_machine", "quarantined"]
    reason: Literal["STAGED", "SOURCE_CONFLICT"]


class CalendarOfficialObject(_Frozen):
    body_sha256: str = Field(pattern=SHA256_RE.pattern)
    body_bytes: bytes = Field(min_length=1, max_length=MAX_BODY_BYTES)

    @model_validator(mode="after")
    def _hash(self) -> CalendarOfficialObject:
        if body_sha256(self.body_bytes) != self.body_sha256:
            raise CalendarGenerationError("official body digest mismatch")
        return self


class CalendarHead(_Frozen):
    singleton: Literal[1] = 1
    sequence: int = Field(ge=0)
    generation_sha256: str | None = Field(default=None, pattern=SHA256_RE.pattern)

    @model_validator(mode="after")
    def _shape(self) -> CalendarHead:
        if (self.sequence == 0) != (self.generation_sha256 is None):
            raise CalendarGenerationError("invalid calendar head")
        return self


def _projection(model: BaseModel, excluded: str) -> dict[str, Any]:
    values = model.model_dump(mode="json", warnings="error")
    values.pop(excluded, None)
    return values


def build_schedule_sha256(schedule: OfficialCalendarScheduleV1) -> str:
    return domain_sha256(
        "stock-eva/r2f4.1/calendar-schedule/v1", _projection(schedule, "schedule_sha256")
    )


def build_source_sha256(source: CalendarSourceBundleV1) -> str:
    return domain_sha256(
        "stock-eva/r2f4.1/calendar-source/v1", _projection(source, "source_sha256")
    )


def build_observation_sha256(observation: CalendarMachineObservationV1) -> str:
    return domain_sha256(
        "stock-eva/r2f4.1/calendar-machine/v1", _projection(observation, "observation_sha256")
    )


def build_generation_sha256(generation: CalendarGenerationV1) -> str:
    return domain_sha256(
        "stock-eva/r2f4.1/calendar-generation/v1", _projection(generation, "generation_sha256")
    )


def _unchecked(cls: type[BaseModel], values: dict[str, Any]) -> BaseModel:
    return BaseModel.model_construct.__func__(cls, **values)


def build_schedule(**values: Any) -> OfficialCalendarScheduleV1:
    values = dict(values)
    values["schedule_sha256"] = "0" * 64
    draft = OfficialCalendarScheduleV1.model_validate(values)
    values["schedule_sha256"] = build_schedule_sha256(draft)
    return OfficialCalendarScheduleV1.model_validate(values)


def build_source(**values: Any) -> CalendarSourceBundleV1:
    values = dict(values)
    schedules = tuple(values["schedules"])
    values["schedules"] = tuple(
        schedule.model_copy(update={"schedule_sha256": build_schedule_sha256(schedule)})
        for schedule in schedules
    )
    values["source_sha256"] = "0" * 64
    draft = CalendarSourceBundleV1.model_validate(values)
    values["source_sha256"] = build_source_sha256(draft)
    return CalendarSourceBundleV1.model_validate(values)


def build_observation(**values: Any) -> CalendarMachineObservationV1:
    values = dict(values)
    values["observation_sha256"] = "0" * 64
    draft = CalendarMachineObservationV1.model_validate(values)
    values["observation_sha256"] = build_observation_sha256(draft)
    return CalendarMachineObservationV1.model_validate(values)


def _calendar_map(schedule: OfficialCalendarScheduleV1) -> dict[date, bool]:
    closed = set(schedule.closed_dates)
    start, end = schedule.coverage_start, schedule.coverage_end
    return {
        start + timedelta(days=i): (start + timedelta(days=i)).weekday() < 5
        and (start + timedelta(days=i)) not in closed
        for i in range((end - start).days + 1)
    }


def _config_map(config: dict[str, Any]) -> dict[date, bool]:
    year = int(config["year"])
    closed = {date.fromisoformat(item) for item in config["closed_dates"]}
    start = date(year, 1, 1)
    return {
        start + timedelta(days=i): (start + timedelta(days=i)).weekday() < 5
        and start + timedelta(days=i) not in closed
        for i in range((date(year, 12, 31) - start).days + 1)
    }


def _compose_snapshot(
    bundled: tuple[dict[str, Any], ...],
    promoted: tuple[CalendarSourceBundleV1, ...] = (),
    *,
    bundled_sha256: str | None = None,
    generation_sha256: str | None = None,
) -> ImmutableCalendarSnapshot:
    configs = {
        int(config["year"]): CalendarConfigSnapshot(
            year=int(config["year"]),
            status=config["status"],
            published_on=date.fromisoformat(config["published_on"]),
            sources=tuple(
                CalendarSourceMetadata.model_validate(item) for item in config["sources"]
            ),
            closed_dates=tuple(date.fromisoformat(item) for item in config["closed_dates"]),
        )
        for config in bundled
    }
    for source in promoted:
        configs[source.year] = CalendarConfigSnapshot(
            year=source.year,
            status="confirmed",
            published_on=max(item.published_on for item in source.schedules),
            sources=tuple(
                CalendarSourceMetadata(
                    exchange=item.exchange,
                    title=item.title,
                    url=item.official_url,
                    notice_no=item.notice_no,
                )
                for item in source.schedules
            ),
            closed_dates=source.schedules[0].closed_dates,
        )
    return ImmutableCalendarSnapshot(
        tuple(configs[key] for key in sorted(configs)),
        bundled_sha256=bundled_sha256,
        generation_sha256=generation_sha256,
    )


def _bundled_payload() -> tuple[dict[str, Any], ...]:
    directory = Path(__file__).with_name("calendars")
    payloads: list[dict[str, Any]] = []
    for path in sorted(directory.glob("cn_a_share_*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        payloads.extend(raw if isinstance(raw, list) else [raw])
    return tuple(sorted(payloads, key=lambda value: value["year"]))


def _bundled_source_map(year: int) -> dict[date, bool] | None:
    for config in _bundled_payload():
        if int(config["year"]) == year:
            return _config_map(config)
    return None


def bundled_sha256(configs: tuple[dict[str, Any], ...] | None = None) -> str:
    return domain_sha256(
        "stock-eva/r2f4.1/calendar-bundled/v1", {"configs": list(configs or _bundled_payload())}
    )


def load_source(path: Path | str) -> CalendarSourceBundleV1:
    """Read one bounded source package with duplicate-key rejection."""
    source_path = Path(path)

    def reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise CalendarGenerationError("duplicate source key")
            result[key] = value
        return result

    try:
        fd = os.open(source_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                raise CalendarGenerationError("source package is not a regular file")
            if info.st_size > MAX_SOURCE_BYTES:
                raise CalendarGenerationError("source package exceeds size limit")
            chunks: list[bytes] = []
            remaining = MAX_SOURCE_BYTES + 1
            while remaining:
                chunk = os.read(fd, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            after = os.fstat(fd)
            current = source_path.lstat()
            if (
                len(raw) > MAX_SOURCE_BYTES
                or (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino)
                or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                != (info.st_dev, info.st_ino, len(raw), info.st_mtime_ns)
            ):
                raise CalendarGenerationError("source package changed")
        finally:
            os.close(fd)
        if len(raw) > MAX_SOURCE_BYTES:
            raise CalendarGenerationError("source package exceeds size limit")
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicate_pairs)
        # JSON arrays are intentionally converted by the JSON validator to the
        # frozen tuple representation used by the authority models.
        return CalendarSourceBundleV1.model_validate_json(json.dumps(payload, ensure_ascii=False))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CalendarGenerationError("invalid source package") from exc


# Normative DDL.  Do not derive this from sqlite_master; schema identity is the source bytes.
CALENDAR_GENERATION_DDL = """CREATE TABLE calendar_generation_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    schema_sha256 TEXT NOT NULL CHECK(length(schema_sha256) = 64),
    bundled_sha256 TEXT NOT NULL CHECK(length(bundled_sha256) = 64)
);
CREATE TABLE calendar_generation_candidate (
    staging_sequence INTEGER PRIMARY KEY CHECK(staging_sequence > 0),
    source_sha256 TEXT NOT NULL UNIQUE CHECK(length(source_sha256) = 64),
    payload_json TEXT NOT NULL CHECK(length(payload_json) <= 262144),
    staged_at TEXT NOT NULL,
    admission TEXT NOT NULL CHECK(admission IN ('awaiting_machine', 'quarantined')),
    reason TEXT NOT NULL CHECK(
        (admission = 'awaiting_machine' AND reason = 'STAGED') OR
        (admission = 'quarantined' AND reason = 'SOURCE_CONFLICT'))
);
CREATE TABLE calendar_official_object (
    body_sha256 TEXT PRIMARY KEY CHECK(length(body_sha256) = 64),
    body_bytes BLOB NOT NULL CHECK(length(body_bytes) BETWEEN 1 AND 1048576)
);
CREATE TABLE calendar_maintenance_attempt (
    target_year INTEGER NOT NULL CHECK(target_year BETWEEN 1900 AND 9998),
    slot_date TEXT NOT NULL,
    source_sha256 TEXT NOT NULL REFERENCES calendar_generation_candidate(source_sha256),
    expected_parent_sha256 TEXT REFERENCES calendar_generation_promotion(generation_sha256),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    outcome TEXT NOT NULL CHECK(outcome IN (
        'RUNNING', 'OFFICIAL_UNAVAILABLE', 'OFFICIAL_HASH_MISMATCH',
        'MACHINE_UNAVAILABLE', 'MACHINE_CONFLICT', 'SKIPPED_CIRCUIT_OPEN',
        'PARENT_CHANGED', 'HISTORY_CHANGE', 'CONTROL_STATE_UNAVAILABLE', 'PROMOTED')),
    official_requests INTEGER NOT NULL DEFAULT 0 CHECK(official_requests BETWEEN 0 AND 2),
    machine_requests INTEGER NOT NULL DEFAULT 0 CHECK(machine_requests BETWEEN 0 AND 1),
    PRIMARY KEY(target_year, slot_date),
    CHECK((outcome = 'RUNNING' AND finished_at IS NULL AND
           official_requests = 0 AND machine_requests = 0) OR
          (outcome != 'RUNNING' AND finished_at IS NOT NULL)),
    CHECK(outcome != 'PROMOTED' OR (official_requests = 2 AND machine_requests = 1))
);
CREATE TABLE calendar_generation_promotion (
    sequence INTEGER PRIMARY KEY CHECK(sequence > 0),
    generation_sha256 TEXT NOT NULL UNIQUE CHECK(length(generation_sha256) = 64),
    parent_sha256 TEXT REFERENCES calendar_generation_promotion(generation_sha256),
    source_sha256 TEXT NOT NULL REFERENCES calendar_generation_candidate(source_sha256),
    attempt_target_year INTEGER NOT NULL,
    attempt_slot_date TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK(length(payload_json) <= 262144),
    promoted_at TEXT NOT NULL,
    UNIQUE(sequence, generation_sha256),
    UNIQUE(attempt_target_year, attempt_slot_date),
    FOREIGN KEY(attempt_target_year, attempt_slot_date)
        REFERENCES calendar_maintenance_attempt(target_year, slot_date)
);
CREATE TABLE calendar_generation_head (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    generation_sha256 TEXT,
    CHECK((sequence = 0 AND generation_sha256 IS NULL) OR
          (sequence > 0 AND generation_sha256 IS NOT NULL)),
    FOREIGN KEY(sequence, generation_sha256)
        REFERENCES calendar_generation_promotion(sequence, generation_sha256)
);
CREATE TRIGGER calendar_meta_no_update BEFORE UPDATE ON calendar_generation_meta
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_meta_no_delete BEFORE DELETE ON calendar_generation_meta
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_candidate_no_update BEFORE UPDATE ON calendar_generation_candidate
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_candidate_no_delete BEFORE DELETE ON calendar_generation_candidate
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_object_no_update BEFORE UPDATE ON calendar_official_object
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_object_no_delete BEFORE DELETE ON calendar_official_object
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_promotion_no_update BEFORE UPDATE ON calendar_generation_promotion
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_promotion_no_delete BEFORE DELETE ON calendar_generation_promotion
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_head_no_delete BEFORE DELETE ON calendar_generation_head
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_attempt_no_delete BEFORE DELETE ON calendar_maintenance_attempt
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_attempt_terminal_only BEFORE UPDATE ON calendar_maintenance_attempt
WHEN OLD.outcome != 'RUNNING' OR NEW.outcome = 'RUNNING'
  OR NEW.target_year != OLD.target_year OR NEW.slot_date != OLD.slot_date
  OR NEW.source_sha256 != OLD.source_sha256
  OR NEW.expected_parent_sha256 IS NOT OLD.expected_parent_sha256
  OR NEW.started_at != OLD.started_at
BEGIN SELECT RAISE(ABORT, 'calendar_attempt_immutable'); END;
"""
SCHEMA_SHA256 = hashlib.sha256(CALENDAR_GENERATION_DDL.encode("utf-8")).hexdigest()
_schema_probe = sqlite3.connect(":memory:")
_schema_probe.executescript(CALENDAR_GENERATION_DDL)
_EXPECTED_SCHEMA = tuple(
    (row[0], row[1], row[2])
    for row in _schema_probe.execute(
        "SELECT type,name,sql FROM sqlite_master "
        "WHERE type IN ('table','index','trigger','view') "
        "ORDER BY type,name"
    )
)
_schema_probe.close()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CalendarGenerationError("timestamp must be timezone aware")
    return value.astimezone(UTC)


def _stamp(value: datetime) -> str:
    value = _utc(value)
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_stamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@contextmanager
def _lock(path: Path, exclusive: bool, *, create: bool = True):
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_NOFOLLOW
    if create:
        flags |= os.O_CREAT
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ELOOP, errno.EACCES):
            raise CalendarStoreUnavailable("calendar store lock unavailable") from exc
        raise
    try:
        lock_info = os.fstat(fd)
        if lock_info.st_nlink != 1 or lock_info.st_uid != os.getuid() or lock_info.st_mode & 0o077:
            raise CalendarStoreUnavailable("unsafe calendar store lock")
        try:
            fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CalendarStoreUnavailable("calendar store locked") from exc
        yield fd
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


class CalendarGenerationStore:
    """Lazy, append-only generation store.

    Constructor performs no filesystem access.  ``plan_stage`` is pure; only
    ``stage_execute`` may create the private control path.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._reader_fds: dict[int, int] = {}
        self._writer_fds: dict[int, tuple[int, Path | None]] = {}

    def _validate_source(
        self, source: CalendarSourceBundleV1, now: datetime
    ) -> tuple[str, str, bool]:
        if not isinstance(source, CalendarSourceBundleV1):
            raise CalendarGenerationError("source must be CalendarSourceBundleV1")
        now = _utc(now)
        try:
            serialized = canonical_json_bytes(source.model_dump(mode="json", warnings="error"))
        except (TypeError, ValueError) as exc:
            raise CalendarGenerationError("source serialization invalid") from exc
        if len(serialized) > MAX_SOURCE_BYTES:
            raise CalendarGenerationError("source exceeds size limit")
        try:
            source = CalendarSourceBundleV1.model_validate_json(serialized)
        except (TypeError, ValueError) as exc:
            raise CalendarGenerationError("source failed serialized validation") from exc
        for schedule in source.schedules:
            if (
                schedule.published_on > now.astimezone(SHANGHAI).date()
                or schedule.reviewed_on > now
            ):
                raise CalendarGenerationError("source is not yet reviewed")
            if build_schedule_sha256(schedule) != schedule.schedule_sha256:
                raise CalendarGenerationError("schedule digest mismatch")
        source_hash = build_source_sha256(source)
        if source_hash != source.source_sha256:
            raise CalendarGenerationError("source digest mismatch")
        conflict = _calendar_map(source.schedules[0]) != _calendar_map(source.schedules[1])
        return (
            source_hash,
            serialized.decode("utf-8"),
            conflict,
        )

    def plan_stage(self, source: CalendarSourceBundleV1, now: datetime) -> CalendarStageResult:
        source_hash, _payload, conflict = self._validate_source(source, now)
        return CalendarStageResult(
            outcome="SOURCE_CONFLICT" if conflict else "STAGED",
            source_sha256=source_hash,
            admission="quarantined" if conflict else "awaiting_machine",
        )

    def _check_path(self, *, allow_missing: bool) -> None:
        path = self.path
        self._check_ancestors()
        try:
            path_info = path.lstat()
        except FileNotFoundError:
            path_info = None
        if path_info is not None and (
            stat.S_ISLNK(path_info.st_mode) or not stat.S_ISREG(path_info.st_mode)
        ):
            raise CalendarStoreUnavailable("unsafe calendar store path")
        if path_info is not None:
            info = path_info
            if info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise CalendarStoreUnavailable("unsafe calendar store path")
            if info.st_size == 0:
                raise CalendarStoreUnavailable("empty existing calendar store")
        elif not allow_missing:
            raise CalendarStoreUnavailable("calendar store unavailable")
        for sidecar in (
            Path(str(path) + "-journal"),
            Path(str(path) + "-wal"),
            Path(str(path) + "-shm"),
        ):
            try:
                sidecar.lstat()
                sidecar_exists = True
            except FileNotFoundError:
                sidecar_exists = False
            if sidecar_exists:
                raise CalendarStoreUnavailable("calendar store has active sidecar")

    def _check_ancestors(self) -> None:
        private_parent = self.path.parent
        try:
            info = private_parent.lstat()
        except FileNotFoundError:
            info = None
        if info is not None and (
            stat.S_ISLNK(info.st_mode)
            or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
        ):
            raise CalendarStoreUnavailable("unsafe calendar store directory")
        current = private_parent
        while True:
            try:
                info = current.lstat()
            except FileNotFoundError:
                parent = current.parent
                if parent == current:
                    return
                current = parent
                continue
            if stat.S_ISLNK(info.st_mode):
                raise CalendarStoreUnavailable("unsafe calendar store directory")
            parent = current.parent
            if parent == current:
                return
            current = parent

    def _connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        self._check_path(allow_missing=not readonly)
        if readonly:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                before = os.fstat(fd)
                connection = sqlite3.connect(f"file:/dev/fd/{fd}?mode=ro", uri=True, timeout=0)
                after = os.fstat(fd)
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                    after.st_dev,
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                ):
                    connection.close()
                    raise CalendarStoreUnavailable("calendar store changed")
                self._reader_fds[id(connection)] = fd
            except BaseException:
                os.close(fd)
                raise
        else:
            # Bind the SQLite handle to an already-validated inode.  A
            # pathname check immediately followed by sqlite3.connect(path)
            # still permits a swap between those operations.  Keeping the
            # descriptor alive for the connection also lets the commit-time
            # identity proof detect that the public path was replaced.
            alias_path: Path | None = None
            try:
                expected = self.path.lstat()
            except FileNotFoundError:
                expected = None
            if expected is not None:
                alias_path = self.path.with_name(f".{self.path.name}.bound-{secrets.token_hex(12)}")
                os.link(self.path, alias_path, follow_symlinks=False)
            flags = os.O_RDWR | os.O_NOFOLLOW
            if expected is None:
                flags |= os.O_CREAT | os.O_EXCL
            fd: int | None = None
            try:
                fd = os.open(self.path, flags, 0o600)
                opened = os.fstat(fd)
                if expected is not None and (
                    (opened.st_dev, opened.st_ino, opened.st_uid)
                    != (expected.st_dev, expected.st_ino, expected.st_uid)
                ):
                    raise CalendarStoreUnavailable("calendar store changed")
                if (
                    opened.st_nlink != (2 if expected is not None else 1)
                    or opened.st_uid != os.getuid()
                    or opened.st_mode & 0o077
                ):
                    raise CalendarStoreUnavailable("unsafe calendar store path")
                os.fchmod(fd, 0o600)
                if alias_path is None:
                    alias_path = self.path.with_name(
                        f".{self.path.name}.bound-{secrets.token_hex(12)}"
                    )
                    os.link(self.path, alias_path, follow_symlinks=False)
                    alias_info = os.stat(alias_path, follow_symlinks=False)
                    if (alias_info.st_dev, alias_info.st_ino) != (
                        opened.st_dev,
                        opened.st_ino,
                    ):
                        raise CalendarStoreUnavailable("calendar store changed")
                connection = sqlite3.connect(alias_path, timeout=0, isolation_level=None)
                self._writer_fds[id(connection)] = (fd, alias_path)
            except BaseException:
                if fd is not None:
                    os.close(fd)
                if alias_path is not None:
                    try:
                        alias_path.unlink()
                    except FileNotFoundError:
                        pass
                raise
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=0")
            return connection
        except BaseException:
            self._close_connection(connection)
            raise

    def _assert_reader_identity(self, connection: sqlite3.Connection) -> None:
        underlying = getattr(connection, "_connection", connection)
        fd = self._reader_fds.get(id(underlying))
        if fd is None:
            raise CalendarStoreUnavailable("calendar reader is not descriptor-bound")
        opened = os.fstat(fd)
        try:
            current = self.path.lstat()
        except FileNotFoundError as exc:
            raise CalendarStoreUnavailable("calendar store path changed") from exc
        if (
            stat.S_ISLNK(current.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 1
            or current.st_uid != os.getuid()
            or current.st_mode & 0o077
            or (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns)
            != (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        ):
            raise CalendarStoreUnavailable("calendar store path changed")

    def _close_connection(self, connection: sqlite3.Connection) -> None:
        fd = self._reader_fds.pop(id(connection), None)
        if fd is None:
            bound = self._writer_fds.pop(id(connection), None)
            # Test and integration wrappers may proxy the real connection;
            # the descriptor is registered against the underlying handle.
            if bound is None:
                underlying = getattr(connection, "_connection", None)
                if underlying is not None:
                    bound = self._writer_fds.pop(id(underlying), None)
            fd = bound[0] if bound is not None else None
            alias_path = bound[1] if bound is not None else None
        else:
            alias_path = None
        connection.close()
        if fd is not None:
            os.close(fd)
        if alias_path is not None:
            try:
                alias_path.unlink()
            except FileNotFoundError:
                pass

    def _assert_writer_identity(self, connection: sqlite3.Connection) -> None:
        """Reject a path swap before a writer can make its transaction durable."""
        bound = self._writer_fds.get(id(connection))
        if bound is None:
            underlying = getattr(connection, "_connection", None)
            if underlying is not None:
                bound = self._writer_fds.get(id(underlying))
        if bound is None:
            raise CalendarStoreUnavailable("calendar writer is not descriptor-bound")
        fd, alias_path = bound
        opened = os.fstat(fd)
        try:
            current = self.path.lstat()
        except FileNotFoundError as exc:
            raise CalendarStoreUnavailable("calendar store path changed") from exc
        if (
            stat.S_ISLNK(current.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or current.st_nlink not in (1, 2)
            or current.st_uid != os.getuid()
            or current.st_mode & 0o077
            or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise CalendarStoreUnavailable("calendar store path changed")
        if alias_path is None:
            raise CalendarStoreUnavailable("calendar writer alias unavailable")
        try:
            alias_info = os.stat(alias_path, follow_symlinks=False)
        except FileNotFoundError as exc:
            raise CalendarStoreUnavailable("calendar writer alias changed") from exc
        if (alias_info.st_dev, alias_info.st_ino) != (
            opened.st_dev,
            opened.st_ino,
        ) or alias_info.st_nlink != 2:
            raise CalendarStoreUnavailable("calendar writer alias changed")

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        expected_tables = {
            "calendar_generation_meta",
            "calendar_generation_candidate",
            "calendar_official_object",
            "calendar_maintenance_attempt",
            "calendar_generation_promotion",
            "calendar_generation_head",
        }
        expected_triggers = {
            "calendar_meta_no_update",
            "calendar_meta_no_delete",
            "calendar_candidate_no_update",
            "calendar_candidate_no_delete",
            "calendar_object_no_update",
            "calendar_object_no_delete",
            "calendar_promotion_no_update",
            "calendar_promotion_no_delete",
            "calendar_head_no_delete",
            "calendar_attempt_no_delete",
            "calendar_attempt_terminal_only",
        }
        rows = connection.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE type IN ('table','index','trigger','view') "
            "ORDER BY type,name"
        ).fetchall()
        if tuple((row["type"], row["name"], row["sql"]) for row in rows) != _EXPECTED_SCHEMA:
            raise CalendarStoreUnavailable("calendar schema unavailable")
        rows = connection.execute(
            "SELECT type,name FROM sqlite_master WHERE type IN ('table','index','trigger','view')"
        ).fetchall()
        tables = {row["name"] for row in rows if row["type"] == "table"}
        triggers = {row["name"] for row in rows if row["type"] == "trigger"}
        indexes = {row["name"] for row in rows if row["type"] == "index"}
        views = {row["name"] for row in rows if row["type"] == "view"}
        expected_indexes = {row[1] for row in _EXPECTED_SCHEMA if row[0] == "index"}
        if (
            tables != expected_tables
            or triggers != expected_triggers
            or indexes != expected_indexes
            or views
        ):
            raise CalendarStoreUnavailable("calendar schema unavailable")

    @staticmethod
    def _validate_control_meta(
        connection: sqlite3.Connection, bundled: tuple[dict[str, Any], ...] | None = None
    ) -> sqlite3.Row:
        rows = connection.execute("SELECT * FROM calendar_generation_meta").fetchall()
        if len(rows) != 1 or type(rows[0]["singleton"]) is not int or rows[0]["singleton"] != 1:
            raise CalendarStoreUnavailable("calendar metadata unavailable")
        meta = rows[0]
        if (
            type(meta["schema_version"]) is not int
            or meta["schema_version"] != 1
            or meta["schema_sha256"] != SCHEMA_SHA256
            or meta["bundled_sha256"] != bundled_sha256(bundled)
        ):
            raise CalendarStoreUnavailable("calendar metadata unavailable")
        return meta

    def _initialize(self, connection: sqlite3.Connection) -> None:
        statement = ""
        for line in CALENDAR_GENERATION_DDL.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                connection.execute(statement)
                statement = ""
        if statement.strip():
            raise CalendarStoreUnavailable("calendar schema unavailable")
        connection.execute(
            "INSERT INTO calendar_generation_meta VALUES (1, 1, ?, ?)",
            (SCHEMA_SHA256, bundled_sha256()),
        )
        connection.execute("INSERT INTO calendar_generation_head VALUES (1, 0, NULL)")

    def stage_execute(self, source: CalendarSourceBundleV1, now: datetime) -> CalendarStageResult:
        planned = self.plan_stage(source, now)
        source = CalendarSourceBundleV1.model_validate_json(_model_json_bytes(source))
        path = self.path
        self._check_ancestors()
        parent_missing = not path.parent.exists()
        path.parent.mkdir(parents=True, exist_ok=True)
        if parent_missing:
            os.chmod(path.parent, 0o700)
        with _lock(path.with_name(path.name + ".lock"), True):
            self._check_path(allow_missing=True)
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode=DELETE")
                connection.execute("PRAGMA synchronous=FULL")
                schema_meta = connection.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name='calendar_generation_meta'"
                ).fetchone()
                if not schema_meta and path.stat().st_size:
                    raise CalendarStoreUnavailable("calendar store unavailable")
                connection.execute("BEGIN IMMEDIATE")
                if not schema_meta:
                    self._initialize(connection)
                else:
                    self._validate_schema(connection)
                    self._validate_control_meta(connection)
                self._load_verified_state(connection)
                row = connection.execute(
                    "SELECT admission FROM calendar_generation_candidate WHERE source_sha256 = ?",
                    (planned.source_sha256,),
                ).fetchone()
                if row:
                    return planned.model_copy(update={"outcome": "ALREADY_STAGED"})
                count = connection.execute(
                    "SELECT COUNT(*) FROM calendar_generation_candidate"
                ).fetchone()[0]
                if count >= MAX_CANDIDATES:
                    raise CalendarStoreUnavailable("calendar candidate limit")
                payload = _model_json_bytes(source).decode("utf-8")
                sequence = connection.execute(
                    "SELECT COALESCE(MAX(staging_sequence), 0) + 1 "
                    "FROM calendar_generation_candidate"
                ).fetchone()[0]
                connection.execute(
                    "INSERT INTO calendar_generation_candidate VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        sequence,
                        planned.source_sha256,
                        payload,
                        _stamp(now),
                        planned.admission,
                        planned.outcome,
                    ),
                )
                self._assert_writer_identity(connection)
                connection.commit()
                return planned
            except sqlite3.Error as exc:
                connection.rollback()
                raise CalendarStoreUnavailable("calendar store unavailable") from exc
            finally:
                self._close_connection(connection)

    def _load_verified_state(self, connection: sqlite3.Connection) -> dict[str, Any]:
        """Validate the complete immutable control graph on one connection."""
        self._validate_schema(connection)
        bundled = _bundled_payload()
        meta = self._validate_control_meta(connection, bundled)
        head_rows = connection.execute("SELECT * FROM calendar_generation_head").fetchall()
        if len(head_rows) != 1:
            raise CalendarStoreUnavailable("calendar schema unavailable")
        head = head_rows[0]
        if (
            type(head["singleton"]) is not int
            or head["singleton"] != 1
            or type(head["sequence"]) is not int
            or head["sequence"] < 0
            or (head["sequence"] == 0) != (head["generation_sha256"] is None)
            or (
                head["generation_sha256"] is not None
                and SHA256_RE.fullmatch(head["generation_sha256"]) is None
            )
        ):
            raise CalendarStoreUnavailable("calendar head unavailable")
        if connection.execute("PRAGMA foreign_key_check").fetchone():
            raise CalendarStoreUnavailable("calendar graph unavailable")
        candidate_count = connection.execute(
            "SELECT COUNT(*) FROM calendar_generation_candidate"
        ).fetchone()[0]
        if candidate_count > MAX_CANDIDATES:
            raise CalendarStoreUnavailable("calendar candidate limit")
        candidates = connection.execute(
            "SELECT * FROM calendar_generation_candidate ORDER BY staging_sequence LIMIT ?",
            (MAX_CANDIDATES + 1,),
        ).fetchall()
        if len(candidates) != candidate_count or any(
            row["staging_sequence"] != index for index, row in enumerate(candidates, 1)
        ):
            raise CalendarStoreUnavailable("calendar graph unavailable")
        candidate_sources: dict[str, CalendarSourceBundleV1] = {}
        candidate_staged_at: dict[str, str] = {}
        for candidate in candidates:
            try:
                payload = candidate["payload_json"]
                source = CalendarSourceBundleV1.model_validate_json(payload)
                staged_at = _parse_stamp(candidate["staged_at"])
                if _stamp(staged_at) != candidate["staged_at"]:
                    raise CalendarGenerationError("candidate timestamp is not canonical")
                source_hash, canonical_source, conflict = self._validate_source(source, staged_at)
            except (TypeError, ValueError, KeyError) as exc:
                raise CalendarStoreUnavailable("calendar candidate unavailable") from exc
            if (
                canonical_source != payload
                or source.source_sha256 != candidate["source_sha256"]
                or source_hash != source.source_sha256
                or any(
                    build_schedule_sha256(schedule) != schedule.schedule_sha256
                    for schedule in source.schedules
                )
                or candidate["admission"] not in {"awaiting_machine", "quarantined"}
                or candidate["reason"] != ("SOURCE_CONFLICT" if conflict else "STAGED")
                or candidate["admission"] != ("quarantined" if conflict else "awaiting_machine")
            ):
                raise CalendarStoreUnavailable("calendar graph unavailable")
            candidate_sources[source.source_sha256] = source
            candidate_staged_at[source.source_sha256] = candidate["staged_at"]

        objects: set[str] = set()
        object_cursor = connection.execute(
            "SELECT body_sha256,body_bytes FROM calendar_official_object"
        )
        for obj in object_cursor:
            raw = bytes(obj["body_bytes"])
            if not raw or len(raw) > MAX_BODY_BYTES or body_sha256(raw) != obj["body_sha256"]:
                raise CalendarStoreUnavailable("calendar graph unavailable")
            objects.add(obj["body_sha256"])

        attempt_rows: dict[tuple[int, str], dict[str, Any]] = {}
        attempt_cursor = connection.execute("SELECT * FROM calendar_maintenance_attempt")
        for attempt_row in attempt_cursor:
            try:
                attempt = CalendarMaintenanceAttempt.model_validate_json(
                    json.dumps(dict(attempt_row))
                )
                if _stamp(attempt.started_at) != attempt_row["started_at"]:
                    raise CalendarGenerationError("attempt timestamp is not canonical")
                if (
                    attempt.finished_at is not None
                    and _stamp(attempt.finished_at) != attempt_row["finished_at"]
                ):
                    raise CalendarGenerationError("attempt timestamp is not canonical")
                candidate_source = candidate_sources.get(attempt.source_sha256)
                if (
                    candidate_source is None
                    or candidate_source.year != attempt.target_year
                    or attempt.slot_date != attempt.started_at.astimezone(SHANGHAI).date()
                    or candidate_staged_at.get(attempt.source_sha256, "")
                    > attempt_row["started_at"]
                ):
                    raise CalendarGenerationError("attempt binding unavailable")
            except (TypeError, ValueError, KeyError) as exc:
                raise CalendarStoreUnavailable("calendar attempt unavailable") from exc
            attempt_rows[(attempt_row["target_year"], attempt_row["slot_date"])] = dict(attempt_row)

        promotion_count = connection.execute(
            "SELECT COUNT(*) FROM calendar_generation_promotion"
        ).fetchone()[0]
        if head["sequence"] == 0 and promotion_count:
            raise CalendarStoreUnavailable("calendar head unavailable")
        generations: list[CalendarGenerationV1] = []
        promoted_sources: list[CalendarSourceBundleV1] = []
        prior_maps = {int(config["year"]): _config_map(config) for config in bundled}
        if head["sequence"]:
            rows = connection.execute(
                "SELECT * FROM calendar_generation_promotion ORDER BY sequence LIMIT ?",
                (MAX_GENERATIONS + 1,),
            ).fetchall()
            if len(rows) != head["sequence"] or len(rows) > MAX_GENERATIONS:
                raise CalendarStoreUnavailable("calendar chain unavailable")
            previous = None
            previous_promoted_at: datetime | None = None
            promoted_attempt_keys: set[tuple[int, str]] = set()
            object_hashes = objects
            for row in rows:
                try:
                    generation = CalendarGenerationV1.model_validate_json(row["payload_json"])
                    canonical_generation = _model_json_bytes(generation).decode("utf-8")
                except (TypeError, ValueError, KeyError) as exc:
                    raise CalendarStoreUnavailable("calendar chain unavailable") from exc
                if (
                    canonical_generation != row["payload_json"]
                    or row["promoted_at"] != _stamp(generation.promoted_at)
                    or row["attempt_target_year"] != generation.attempt_target_year
                    or row["attempt_slot_date"] != generation.attempt_slot_date.isoformat()
                    or generation.sequence != row["sequence"]
                    or generation.generation_sha256 != row["generation_sha256"]
                    or generation.parent_sha256 != row["parent_sha256"]
                    or generation.source_sha256 != row["source_sha256"]
                    or generation.parent_sha256 != previous
                    or generation.bundled_sha256 != meta["bundled_sha256"]
                    or build_generation_sha256(generation) != generation.generation_sha256
                    or build_observation_sha256(generation.machine)
                    != generation.machine.observation_sha256
                    or (
                        previous_promoted_at is not None
                        and generation.promoted_at < previous_promoted_at
                    )
                ):
                    raise CalendarStoreUnavailable("calendar chain unavailable")
                candidate_source = candidate_sources.get(generation.source_sha256)
                if candidate_source is None or tuple(generation.official_body_hashes) != tuple(
                    item.body_sha256 for item in candidate_source.schedules
                ):
                    raise CalendarStoreUnavailable("calendar chain unavailable")
                attempt_key = (
                    generation.attempt_target_year,
                    generation.attempt_slot_date.isoformat(),
                )
                attempt_row = attempt_rows.get(attempt_key)
                if (
                    attempt_row is None
                    or attempt_row["outcome"] != "PROMOTED"
                    or attempt_row["source_sha256"] != generation.source_sha256
                    or attempt_row["expected_parent_sha256"] != generation.parent_sha256
                    or attempt_row["official_requests"] != 2
                    or attempt_row["machine_requests"] != 1
                    or attempt_row["finished_at"] != _stamp(generation.promoted_at)
                    or attempt_row["started_at"] > _stamp(generation.machine.observed_at)
                    or generation.machine.observed_at > generation.promoted_at
                ):
                    raise CalendarStoreUnavailable("calendar chain unavailable")
                candidate_row = next(
                    candidate
                    for candidate in candidates
                    if candidate["source_sha256"] == generation.source_sha256
                )
                covered_years = set(prior_maps)
                promoted_year = generation.promoted_at.astimezone(SHANGHAI).year
                if (
                    candidate_row["staged_at"] > attempt_row["started_at"]
                    or candidate_row["admission"] != "awaiting_machine"
                    or generation.attempt_target_year not in (promoted_year, promoted_year + 1)
                    or generation.attempt_target_year > max(covered_years) + 1
                    or (
                        promoted_year not in covered_years
                        and generation.attempt_target_year == promoted_year + 1
                    )
                    or any(
                        body_hash not in object_hashes
                        for body_hash in generation.official_body_hashes
                    )
                ):
                    raise CalendarStoreUnavailable("calendar chain unavailable")
                source_map = _calendar_map(candidate_source.schedules[0])
                if {item.date: item.is_open for item in generation.machine.days} != source_map:
                    raise CalendarStoreUnavailable("calendar chain unavailable")
                local_promoted = generation.promoted_at.astimezone(SHANGHAI)
                horizon = (
                    local_promoted.date()
                    if (local_promoted.hour, local_promoted.minute) >= DATA_AVAILABLE_AT
                    else local_promoted.date() - timedelta(days=1)
                )
                prior_map = prior_maps.get(candidate_source.year)
                if prior_map is not None and any(
                    day <= horizon and prior_map.get(day) != source_map.get(day)
                    for day in prior_map
                ):
                    raise CalendarStoreUnavailable("calendar history transition")
                prior_maps[candidate_source.year] = source_map
                generations.append(generation)
                promoted_sources.append(candidate_source)
                promoted_attempt_keys.add(attempt_key)
                previous_promoted_at = generation.promoted_at
                previous = generation.generation_sha256
            if head["generation_sha256"] != previous:
                raise CalendarStoreUnavailable("calendar head unavailable")
            if {
                key
                for key, attempt_row in attempt_rows.items()
                if attempt_row["outcome"] == "PROMOTED"
            } != promoted_attempt_keys:
                raise CalendarStoreUnavailable("calendar chain unavailable")
        elif any(row["outcome"] == "PROMOTED" for row in attempt_rows.values()):
            raise CalendarStoreUnavailable("calendar chain unavailable")
        return {
            "meta": meta,
            "head": head,
            "candidates": candidates,
            "candidate_sources": candidate_sources,
            "objects": objects,
            "attempt_rows": attempt_rows,
            "generations": generations,
            "promoted_sources": promoted_sources,
            "year_maps": prior_maps,
            "bundled": bundled,
        }

    def read(self) -> CalendarReadResult:
        if not self.path.exists():
            return CalendarReadResult(status="unavailable", calendar=EMPTY_CALENDAR)
        try:
            self._check_path(allow_missing=False)
            with _lock(self.path.with_name(self.path.name + ".lock"), False, create=False):
                connection = self._connect(readonly=True)
                try:
                    self._assert_reader_identity(connection)
                    verified = self._load_verified_state(connection)
                    self._assert_reader_identity(connection)
                    generations = verified["generations"]
                    generation = generations[-1] if generations else None
                    source = (
                        verified["candidate_sources"][generation.source_sha256]
                        if generation is not None
                        else None
                    )
                    return CalendarReadResult(
                        status="ready",
                        generation=generation,
                        bundled=verified["bundled"],
                        source=source,
                        calendar=_compose_snapshot(
                            verified["bundled"],
                            tuple(verified["promoted_sources"]),
                            bundled_sha256=verified["meta"]["bundled_sha256"],
                            generation_sha256=(
                                generation.generation_sha256 if generation else None
                            ),
                        ),
                    )
                finally:
                    fd = self._reader_fds.pop(id(connection), None)
                    connection.close()
                    if fd is not None:
                        os.close(fd)
        except (OSError, sqlite3.Error, CalendarGenerationError, CalendarStoreUnavailable):
            return CalendarReadResult(status="unavailable", calendar=EMPTY_CALENDAR)

    def reserve_attempt(
        self,
        source: CalendarSourceBundleV1,
        now: datetime,
        target_year: int | None = None,
    ) -> CalendarMaintenanceAttempt:
        planned = self.plan_stage(source, now)
        if planned.outcome == "SOURCE_CONFLICT":
            raise CalendarGenerationError("source conflict cannot be reserved")
        local = _utc(now).astimezone(SHANGHAI)
        target_year = source.year if target_year is None else target_year
        if target_year not in (local.year, local.year + 1) or source.year != target_year:
            raise CalendarGenerationError("target year is not admissible")
        if not self.path.exists():
            raise CalendarStoreUnavailable("calendar store unavailable")
        self._check_path(allow_missing=False)
        with _lock(self.path.with_name(self.path.name + ".lock"), True, create=False):
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                verified = self._load_verified_state(connection)
                latest = None
                for candidate in reversed(verified["candidates"]):
                    candidate_source = CalendarSourceBundleV1.model_validate_json(
                        candidate["payload_json"]
                    )
                    if candidate_source.year == target_year:
                        latest = candidate
                        break
                if (
                    latest is None
                    or latest["admission"] != "awaiting_machine"
                    or latest["source_sha256"] != planned.source_sha256
                ):
                    raise CalendarStoreUnavailable("calendar candidate unavailable")
                row = verified["head"]
                parent = row["generation_sha256"] if row and row["sequence"] else None
                slot = local.date().isoformat()
                existing = connection.execute(
                    "SELECT * FROM calendar_maintenance_attempt "
                    "WHERE target_year=? AND slot_date=?",
                    (target_year, slot),
                ).fetchone()
                if existing:
                    raise CalendarAttemptAlreadySpent("calendar slot already spent")
                started = _stamp(now)
                connection.execute(
                    "INSERT INTO calendar_maintenance_attempt "
                    "(target_year,slot_date,source_sha256,expected_parent_sha256,started_at,"
                    "outcome) "
                    "VALUES (?,?,?,?,?,'RUNNING')",
                    (target_year, slot, planned.source_sha256, parent, started),
                )
                self._assert_writer_identity(connection)
                connection.commit()
                return CalendarMaintenanceAttempt(
                    target_year=target_year,
                    slot_date=local.date(),
                    source_sha256=planned.source_sha256,
                    expected_parent_sha256=parent,
                    started_at=_utc(now),
                    outcome="RUNNING",
                )
            finally:
                self._close_connection(connection)

    def _spend_attempt(
        self,
        attempt: CalendarMaintenanceAttempt,
        outcome: str,
        official: int,
        machine: int,
        *,
        finished_at: datetime,
    ) -> None:
        """Best-effort terminal audit update; never grants authority."""
        if outcome not in {
            "OFFICIAL_UNAVAILABLE",
            "OFFICIAL_HASH_MISMATCH",
            "MACHINE_UNAVAILABLE",
            "MACHINE_CONFLICT",
            "SKIPPED_CIRCUIT_OPEN",
            "PARENT_CHANGED",
            "HISTORY_CHANGE",
            "CONTROL_STATE_UNAVAILABLE",
        }:
            return
        try:
            attempt = _revalidate_attempt(attempt)
            finished_at = _utc(finished_at)
        except CalendarGenerationError:
            return
        if finished_at < attempt.started_at or (official, machine) not in {
            (0, 0),
            (2, 0),
            (2, 1),
        }:
            return
        if not self.path.exists():
            return
        try:
            with _lock(self.path.with_name(self.path.name + ".lock"), True, create=False):
                connection = self._connect()
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    verified = self._load_verified_state(connection)
                    row = verified["attempt_rows"].get(
                        (attempt.target_year, attempt.slot_date.isoformat())
                    )
                    if (
                        row is None
                        or row["outcome"] != "RUNNING"
                        or row["source_sha256"] != attempt.source_sha256
                        or row["expected_parent_sha256"] != attempt.expected_parent_sha256
                        or row["started_at"] != _stamp(attempt.started_at)
                    ):
                        connection.rollback()
                        return
                    connection.execute(
                        "UPDATE calendar_maintenance_attempt SET finished_at=?, outcome=?, "
                        "official_requests=?, machine_requests=? "
                        "WHERE target_year=? AND slot_date=? AND outcome='RUNNING'",
                        (
                            _stamp(finished_at),
                            outcome,
                            official,
                            machine,
                            attempt.target_year,
                            attempt.slot_date.isoformat(),
                        ),
                    )
                    self._assert_writer_identity(connection)
                    connection.commit()
                finally:
                    self._close_connection(connection)
        except (OSError, sqlite3.Error, CalendarStoreUnavailable):
            return

    def promote(
        self,
        attempt: CalendarMaintenanceAttempt,
        source: CalendarSourceBundleV1,
        official_bodies: tuple[bytes, bytes],
        machine: CalendarMachineObservationV1,
        promoted_at: datetime,
    ) -> CalendarPromotionResult:
        attempt = _revalidate_attempt(attempt)
        promoted_at = _utc(promoted_at)
        if attempt.outcome != "RUNNING":
            return CalendarPromotionResult(
                outcome="ALREADY_ATTEMPTED", source_sha256=attempt.source_sha256
            )
        planned = self.plan_stage(source, attempt.started_at)
        source = CalendarSourceBundleV1.model_validate_json(_model_json_bytes(source))
        try:
            machine = CalendarMachineObservationV1.model_validate_json(_model_json_bytes(machine))
        except (TypeError, ValueError):
            self._spend_attempt(attempt, "MACHINE_CONFLICT", 2, 1, finished_at=promoted_at)
            return CalendarPromotionResult(
                outcome="MACHINE_CONFLICT",
                source_sha256=attempt.source_sha256,
                official_requests=2,
                machine_requests=1,
            )
        if planned.source_sha256 != attempt.source_sha256 or planned.outcome == "SOURCE_CONFLICT":
            self._spend_attempt(attempt, "MACHINE_CONFLICT", 0, 0, finished_at=promoted_at)
            return CalendarPromotionResult(
                outcome="MACHINE_CONFLICT", source_sha256=attempt.source_sha256
            )
        if len(official_bodies) != 2 or any(
            not isinstance(body, bytes) or not body or len(body) > MAX_BODY_BYTES
            for body in official_bodies
        ):
            self._spend_attempt(attempt, "OFFICIAL_UNAVAILABLE", 2, 0, finished_at=promoted_at)
            return CalendarPromotionResult(
                outcome="OFFICIAL_UNAVAILABLE", source_sha256=attempt.source_sha256
            )
        body_hashes = tuple(body_sha256(body) for body in official_bodies)
        if body_hashes != tuple(schedule.body_sha256 for schedule in source.schedules):
            self._spend_attempt(attempt, "OFFICIAL_HASH_MISMATCH", 2, 0, finished_at=promoted_at)
            return CalendarPromotionResult(
                outcome="OFFICIAL_HASH_MISMATCH",
                source_sha256=attempt.source_sha256,
                official_requests=2,
            )
        if machine.observed_at < attempt.started_at or machine.observed_at > promoted_at:
            self._spend_attempt(attempt, "MACHINE_UNAVAILABLE", 2, 1, finished_at=promoted_at)
            return CalendarPromotionResult(
                outcome="MACHINE_UNAVAILABLE",
                source_sha256=attempt.source_sha256,
                official_requests=2,
                machine_requests=1,
            )
        source_map = _calendar_map(source.schedules[0])
        machine_map = {item.date: item.is_open for item in machine.days}
        if machine_map != source_map:
            self._spend_attempt(attempt, "MACHINE_CONFLICT", 2, 1, finished_at=promoted_at)
            return CalendarPromotionResult(
                outcome="MACHINE_CONFLICT",
                source_sha256=attempt.source_sha256,
                official_requests=2,
                machine_requests=1,
            )
        if build_observation_sha256(machine) != machine.observation_sha256:
            self._spend_attempt(attempt, "MACHINE_CONFLICT", 2, 1, finished_at=promoted_at)
            return CalendarPromotionResult(
                outcome="MACHINE_CONFLICT",
                source_sha256=attempt.source_sha256,
                official_requests=2,
                machine_requests=1,
            )
        if not self.path.exists():
            return CalendarPromotionResult(
                outcome="CONTROL_STATE_UNAVAILABLE", source_sha256=attempt.source_sha256
            )
        self._check_path(allow_missing=False)
        with _lock(self.path.with_name(self.path.name + ".lock"), True, create=False):
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._validate_schema(connection)
                    verified = self._load_verified_state(connection)
                except CalendarStoreUnavailable:
                    connection.rollback()
                    return CalendarPromotionResult(
                        outcome="CONTROL_STATE_UNAVAILABLE",
                        source_sha256=attempt.source_sha256,
                    )
                row = verified["attempt_rows"].get(
                    (attempt.target_year, attempt.slot_date.isoformat())
                )
                head = verified["head"]
                if (
                    not row
                    or row["outcome"] != "RUNNING"
                    or row["source_sha256"] != attempt.source_sha256
                    or row["expected_parent_sha256"] != attempt.expected_parent_sha256
                    or row["started_at"] != _stamp(attempt.started_at)
                    or (head["generation_sha256"] if head["sequence"] else None)
                    != row["expected_parent_sha256"]
                ):
                    connection.rollback()
                    return CalendarPromotionResult(
                        outcome="PARENT_CHANGED",
                        source_sha256=attempt.source_sha256,
                        official_requests=2,
                        machine_requests=1,
                    )
                verified_years = set(verified["year_maps"])
                if (
                    attempt.target_year
                    not in (
                        promoted_at.astimezone(SHANGHAI).year,
                        promoted_at.astimezone(SHANGHAI).year + 1,
                    )
                    or attempt.target_year > max(verified_years, default=attempt.target_year) + 1
                    or (
                        promoted_at.astimezone(SHANGHAI).year not in verified_years
                        and attempt.target_year == promoted_at.astimezone(SHANGHAI).year + 1
                    )
                ):
                    connection.rollback()
                    return CalendarPromotionResult(
                        outcome="PARENT_CHANGED",
                        source_sha256=attempt.source_sha256,
                        official_requests=2,
                        machine_requests=1,
                    )
                prior_map = verified["year_maps"].get(source.year)
                local_promoted = promoted_at.astimezone(SHANGHAI)
                horizon = (
                    local_promoted.date()
                    if (local_promoted.hour, local_promoted.minute) >= DATA_AVAILABLE_AT
                    else local_promoted.date() - timedelta(days=1)
                )
                proposed_map = _calendar_map(source.schedules[0])
                if prior_map is not None and any(
                    day <= horizon and prior_map.get(day) != proposed_map.get(day)
                    for day in prior_map
                ):
                    connection.rollback()
                    return CalendarPromotionResult(
                        outcome="HISTORY_CHANGE",
                        source_sha256=attempt.source_sha256,
                        official_requests=2,
                        machine_requests=1,
                    )
                seq = (head["sequence"] if head else 0) + 1
                generation0 = CalendarGenerationV1(
                    sequence=seq,
                    parent_sha256=row["expected_parent_sha256"],
                    bundled_sha256=verified["meta"]["bundled_sha256"],
                    source_sha256=attempt.source_sha256,
                    attempt_target_year=attempt.target_year,
                    attempt_slot_date=attempt.slot_date,
                    official_body_hashes=body_hashes,
                    machine=machine,
                    promoted_at=promoted_at,
                    generation_sha256="0" * 64,
                )
                generation = generation0.model_copy(
                    update={"generation_sha256": build_generation_sha256(generation0)}
                )
                payload = _model_json_bytes(generation).decode("utf-8")
                for digest, body in zip(body_hashes, official_bodies, strict=True):
                    connection.execute(
                        "INSERT OR IGNORE INTO calendar_official_object VALUES (?,?)",
                        (digest, body),
                    )
                connection.execute(
                    "INSERT INTO calendar_generation_promotion VALUES (?,?,?,?,?,?,?,?)",
                    (
                        seq,
                        generation.generation_sha256,
                        generation.parent_sha256,
                        generation.source_sha256,
                        generation.attempt_target_year,
                        generation.attempt_slot_date.isoformat(),
                        payload,
                        _stamp(promoted_at),
                    ),
                )
                readback = connection.execute(
                    "SELECT * FROM calendar_generation_promotion WHERE sequence=?",
                    (seq,),
                ).fetchone()
                if readback is None:
                    raise CalendarStoreUnavailable("calendar generation readback missing")
                readback_payload = readback["payload_json"]
                try:
                    readback_generation = CalendarGenerationV1.model_validate_json(readback_payload)
                    readback_canonical = _model_json_bytes(readback_generation).decode("utf-8")
                except (TypeError, ValueError) as exc:
                    raise CalendarStoreUnavailable("calendar generation readback invalid") from exc
                if (
                    readback_canonical != readback_payload
                    or readback_generation.generation_sha256 != generation.generation_sha256
                    or build_generation_sha256(readback_generation)
                    != readback_generation.generation_sha256
                    or readback["generation_sha256"] != generation.generation_sha256
                    or readback["parent_sha256"] != generation.parent_sha256
                    or readback["source_sha256"] != generation.source_sha256
                    or readback["attempt_target_year"] != generation.attempt_target_year
                    or readback["attempt_slot_date"] != generation.attempt_slot_date.isoformat()
                    or readback["promoted_at"] != _stamp(generation.promoted_at)
                ):
                    raise CalendarStoreUnavailable("calendar generation readback mismatch")
                head_update = connection.execute(
                    "UPDATE calendar_generation_head SET sequence=?, generation_sha256=? "
                    "WHERE singleton=1 AND sequence=? AND generation_sha256 IS ?",
                    (seq, generation.generation_sha256, seq - 1, row["expected_parent_sha256"]),
                )
                if head_update.rowcount != 1:
                    raise CalendarStoreUnavailable("calendar head changed")
                connection.execute(
                    "UPDATE calendar_maintenance_attempt SET finished_at=?, "
                    "outcome='PROMOTED', official_requests=2, machine_requests=1 "
                    "WHERE target_year=? AND slot_date=?",
                    (_stamp(promoted_at), attempt.target_year, attempt.slot_date.isoformat()),
                )
                self._assert_writer_identity(connection)
                connection.commit()
                return CalendarPromotionResult(
                    outcome="PROMOTED",
                    generation_sha256=generation.generation_sha256,
                    source_sha256=attempt.source_sha256,
                    official_requests=2,
                    machine_requests=1,
                )
            except (sqlite3.Error, CalendarGenerationError, CalendarStoreUnavailable):
                connection.rollback()
                return CalendarPromotionResult(
                    outcome="PARENT_CHANGED",
                    source_sha256=attempt.source_sha256,
                    official_requests=2,
                    machine_requests=1,
                )
            finally:
                self._close_connection(connection)


def validate_source_bundle(source: CalendarSourceBundleV1, now: datetime) -> CalendarStageResult:
    """Pure public source validation convenience wrapper."""
    return CalendarGenerationStore("").plan_stage(source, now)


__all__ = [
    "CALENDAR_GENERATION_DDL",
    "SCHEMA_SHA256",
    "CalendarGenerationError",
    "CalendarAttemptAlreadySpent",
    "CalendarStoreUnavailable",
    "CalendarGenerationStore",
    "CalendarCandidate",
    "CalendarOfficialObject",
    "CalendarHead",
    "CalendarGenerationV1",
    "CalendarMachineDayV1",
    "CalendarMachineObservationV1",
    "CalendarMaintenanceAttempt",
    "CalendarPromotionResult",
    "CalendarReadResult",
    "CalendarSourceBundleV1",
    "CalendarStageResult",
    "OfficialCalendarScheduleV1",
    "body_sha256",
    "build_generation_sha256",
    "build_observation_sha256",
    "build_schedule_sha256",
    "build_source_sha256",
    "build_schedule",
    "build_source",
    "build_observation",
    "bundled_sha256",
    "canonical_json_bytes",
    "domain_sha256",
    "load_source",
    "validate_source_bundle",
]
