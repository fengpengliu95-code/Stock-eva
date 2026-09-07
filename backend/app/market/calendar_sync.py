"""Versioned machine observations for the authoritative A-share calendar.

Bundled exchange notices remain authoritative. BaoStock observations can confirm
or quarantine a range, but can never turn an unknown workday into an open
session.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import stat
import threading
import uuid
from collections.abc import Callable
from contextlib import closing, nullcontext
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, Field, model_validator

from backend.app.blocking import run_blocking_drained
from backend.app.market.baostock import BaoStockError
from backend.app.market.baostock_vendor import transport_observation_sink
from backend.app.market.calendar import SHANGHAI, TradingCalendar
from backend.app.market.provider_health import CircuitState, ProviderHealthError
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
)

SyncMode = Literal["full", "light"]
SyncAction = Literal["full", "light", "none"]
SyncStatus = Literal[
    "ready",
    "observed_only",
    "quarantined",
    "error",
    "skipped_circuit_open",
]
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
_RUNTIME_MAINTENANCE_SUCCESS = {
    "STAGED",
    "SOURCE_PENDING",
    "ALREADY_ATTEMPTED",
    "NOT_DUE",
    "PROMOTED",
}
logger = logging.getLogger(__name__)


class TradingDateProvider(Protocol):
    def trading_dates(self, start_date: date, end_date: date) -> list[date]: ...


class CalendarSyncState(BaseModel):
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_full_sync_at: datetime | None = None
    last_light_sync_at: datetime | None = None
    last_full_slot_date: date | None = None
    last_light_slot_date: date | None = None
    last_startup_slot_date: date | None = None
    last_good_run_id: str | None = None
    conflict_detected: bool = False
    conflict_run_id: str | None = None
    conflict_at: datetime | None = None
    next_sync_at: datetime | None = None


class CalendarSyncDecision(BaseModel):
    action: SyncAction
    reason: Literal[
        "monthly_reconciliation",
        "startup_check",
        "daily_1630_check",
        "not_due",
    ]
    next_sync_at: datetime | None = None


class CalendarSyncPlan(BaseModel):
    mode: SyncMode
    reason: str
    requested_at: datetime
    range_start: date
    range_end: date
    authority_years: list[int] = Field(default_factory=list)
    authority_checksum: str
    sources: list[dict[str, str | None]] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_semantics(self) -> CalendarSyncPlan:
        if self.requested_at.tzinfo is None or self.requested_at.utcoffset() is None:
            raise ValueError("requested_at must be timezone-aware")
        if self.range_start > self.range_end:
            raise ValueError("range_start must not exceed range_end")
        if self.authority_years != sorted(set(self.authority_years)) or any(
            year < 1990 or year > 9999 for year in self.authority_years
        ):
            raise ValueError("authority_years must be sorted, unique, and valid")
        if re.fullmatch(r"[0-9a-f]{64}", self.authority_checksum) is None:
            raise ValueError("authority_checksum must be lowercase sha256")
        return self


class CalendarObservation(BaseModel):
    session_date: date
    provider_is_open: bool
    official_status: Literal["open", "closed", "unknown"]


class CalendarSyncResult(BaseModel):
    run_id: str
    mode: SyncMode
    status: SyncStatus
    range_start: date
    range_end: date
    fetched_at: datetime
    completed_at: datetime
    observed_open_count: int
    conflicts: list[dict[str, str]] = Field(default_factory=list)
    authority_checksum: str
    failure_code: Literal["CALENDAR_AUTHORITY_CHANGED"] | None = None

    @model_validator(mode="after")
    def _validate_failure_code(self) -> CalendarSyncResult:
        if self.failure_code is not None and self.status != "error":
            raise ValueError("failure_code requires an error result")
        if self.range_start > self.range_end:
            raise ValueError("range_start must not exceed range_end")
        if (
            self.fetched_at.tzinfo is None
            or self.fetched_at.utcoffset() is None
            or self.completed_at.tzinfo is None
            or self.completed_at.utcoffset() is None
        ):
            raise ValueError("result timestamps must be timezone-aware")
        if self.completed_at < self.fetched_at:
            raise ValueError("completed_at must not precede fetched_at")
        if self.observed_open_count < 0:
            raise ValueError("observed_open_count must be non-negative")
        return self


class CalendarSyncRun(BaseModel):
    run_id: str
    mode: SyncMode
    reason: str
    status: SyncStatus
    requested_at: datetime
    fetched_at: datetime
    completed_at: datetime
    range_start: date
    range_end: date
    authority_checksum: str
    authority_years: list[int]
    sources: list[dict[str, str | None]]
    observed_open_count: int
    conflicts: list[dict[str, str]]


class CalendarSyncPolicy:
    """Deterministic monthly and daily maintenance decisions."""

    MONTHLY_AT = time(4, 5)
    DAILY_AT = time(16, 30)

    def decide(
        self,
        now: datetime,
        *,
        state: CalendarSyncState | None,
        startup: bool = False,
    ) -> CalendarSyncDecision:
        local = now.astimezone(SHANGHAI)
        current = local.date()
        state = state or CalendarSyncState()
        if (
            current.day == 1
            and local.time() >= self.MONTHLY_AT
            and state.last_full_slot_date != current
        ):
            return CalendarSyncDecision(
                action="full",
                reason="monthly_reconciliation",
                next_sync_at=local,
            )
        if local.time() >= self.DAILY_AT and state.last_light_slot_date != current:
            return CalendarSyncDecision(
                action="light",
                reason="daily_1630_check",
                next_sync_at=local,
            )
        if startup and state.last_startup_slot_date != current:
            return CalendarSyncDecision(
                action="light",
                reason="startup_check",
                next_sync_at=local,
            )
        return CalendarSyncDecision(
            action="none",
            reason="not_due",
            next_sync_at=self.next_scheduled_after(local),
        )

    def next_scheduled_after(self, now: datetime) -> datetime:
        local = now.astimezone(SHANGHAI)
        current = local.date()
        if current.day == 1 and local.time() < self.MONTHLY_AT:
            return datetime.combine(current, self.MONTHLY_AT, tzinfo=SHANGHAI)
        if local.time() < self.DAILY_AT:
            return datetime.combine(current, self.DAILY_AT, tzinfo=SHANGHAI)
        following = current + timedelta(days=1)
        when = self.MONTHLY_AT if following.day == 1 else self.DAILY_AT
        return datetime.combine(following, when, tzinfo=SHANGHAI)


class CalendarSyncStoreReadError(RuntimeError):
    """A SELECT-only calendar control database cannot be read safely."""


class CalendarSyncPlanError(ValueError):
    """A caller supplied a calendar sync plan that fails the public contract."""


_CALENDAR_SYNC_CONNECTION_LOCK = threading.RLock()
_IDENTITY_TRIGGERS = {
    "calendar_store_identity_no_update": """
        CREATE TRIGGER calendar_store_identity_no_update
        BEFORE UPDATE ON calendar_store_identity
        BEGIN SELECT RAISE(ABORT, 'calendar store identity is immutable'); END
    """,
    "calendar_store_identity_no_delete": """
        CREATE TRIGGER calendar_store_identity_no_delete
        BEFORE DELETE ON calendar_store_identity
        BEGIN SELECT RAISE(ABORT, 'calendar store identity is immutable'); END
    """,
    "calendar_store_identity_single_insert": """
        CREATE TRIGGER calendar_store_identity_single_insert
        BEFORE INSERT ON calendar_store_identity
        WHEN EXISTS (SELECT 1 FROM calendar_store_identity)
        BEGIN SELECT RAISE(ABORT, 'calendar store identity already exists'); END
    """,
}


class _CalendarSyncConnection(sqlite3.Connection):
    """A connection that owns the process-local pathname verification lock."""

    _calendar_sync_lock_held = False
    _calendar_sync_lock_owner: int | None = None

    def bind_calendar_sync_lock(self) -> None:
        self._calendar_sync_lock_held = True
        self._calendar_sync_lock_owner = threading.get_ident()

    def close(self) -> None:
        super().close()
        if self._calendar_sync_lock_held:
            if self._calendar_sync_lock_owner != threading.get_ident():
                raise RuntimeError("calendar connection closed by non-owner")
            self._calendar_sync_lock_held = False
            self._calendar_sync_lock_owner = None
            _CALENDAR_SYNC_CONNECTION_LOCK.release()


class CalendarSyncStore:
    """Local SQLite control state; no market or user data lives here."""

    def __init__(
        self,
        path: Path,
        *,
        initialize: bool = True,
        missing_parent_is_empty: bool = False,
    ) -> None:
        self.path = path
        self._missing_parent_is_empty = missing_parent_is_empty
        if initialize:
            self._validate_ancestors()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def _validate_ancestors(self) -> None:
        try:
            for ancestor in self.path.parents:
                try:
                    info = os.lstat(ancestor)
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(info.st_mode):
                    raise CalendarSyncStoreReadError("calendar control database is unavailable")
        except CalendarSyncStoreReadError:
            raise
        except (OSError, TypeError, ValueError):
            raise CalendarSyncStoreReadError("calendar control database is unavailable") from None

    def _validate_file(self, *, allow_transaction_sidecars: bool = False) -> os.stat_result:
        self._validate_ancestors()
        try:
            info = os.lstat(self.path)
            parent = os.lstat(self.path.parent)
        except (OSError, TypeError, ValueError):
            raise CalendarSyncStoreReadError("calendar control database is unavailable") from None
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or parent.st_mode & 0o022
            or stat.S_ISLNK(info.st_mode)
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or info.st_nlink != 1
            or info.st_mode & 0o022
        ):
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        if not allow_transaction_sidecars:
            self._validate_no_sidecars()
        return info

    def _validate_no_sidecars(self) -> None:
        try:
            for suffix in ("-journal", "-wal", "-shm"):
                try:
                    os.lstat(self.path.with_name(self.path.name + suffix))
                except FileNotFoundError:
                    continue
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
        except CalendarSyncStoreReadError:
            raise
        except (OSError, TypeError, ValueError):
            raise CalendarSyncStoreReadError("calendar control database is unavailable") from None

    def _validate_missing_parent(self) -> None:
        self._validate_ancestors()
        try:
            parent = os.lstat(self.path.parent)
        except (OSError, TypeError, ValueError):
            raise CalendarSyncStoreReadError("calendar control database is unavailable") from None
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.geteuid()
            or parent.st_mode & 0o022
        ):
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        self._validate_no_sidecars()

    def _ensure_file_for_write(self) -> os.stat_result:
        if self.path.exists() or self.path.is_symlink():
            return self._validate_file()
        try:
            parent = os.lstat(self.path.parent)
            if (
                not stat.S_ISDIR(parent.st_mode)
                or parent.st_uid != os.geteuid()
                or parent.st_mode & 0o022
            ):
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            self._validate_no_sidecars()
            self._publish_identity_database()
            return self._validate_file()
        except CalendarSyncStoreReadError:
            raise
        except (OSError, TypeError, ValueError):
            raise CalendarSyncStoreReadError("calendar control database is unavailable") from None

    def _publish_identity_database(self) -> None:
        temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.bootstrap")
        descriptor: int | None = None
        identity_stat: os.stat_result | None = None
        try:
            descriptor = os.open(
                temporary,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
            )
            identity_stat = os.fstat(descriptor)
            os.close(descriptor)
            descriptor = None
            with closing(sqlite3.connect(temporary)) as connection, connection:
                store_id = uuid.uuid4().hex
                identity_checksum = self._identity_checksum(
                    store_id,
                    identity_stat.st_dev,
                    identity_stat.st_ino,
                )
                connection.execute(
                    "CREATE TABLE calendar_store_identity "
                    "(singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
                    "store_id TEXT NOT NULL UNIQUE, device INTEGER NOT NULL, "
                    "inode INTEGER NOT NULL, identity_checksum TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO calendar_store_identity "
                    "(singleton, store_id, device, inode, identity_checksum) "
                    "VALUES (1, ?, ?, ?, ?)",
                    (
                        store_id,
                        identity_stat.st_dev,
                        identity_stat.st_ino,
                        identity_checksum,
                    ),
                )
                for trigger_sql in _IDENTITY_TRIGGERS.values():
                    connection.execute(trigger_sql)
            os.link(temporary, self.path, follow_symlinks=False)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    @staticmethod
    def _identity_checksum(store_id: str, device: int, inode: int) -> str:
        return hashlib.sha256(
            f"stock-eva-calendar-store-v1\0{store_id}\0{device}\0{inode}".encode()
        ).hexdigest()

    @classmethod
    def _read_store_identity(cls, connection: sqlite3.Connection) -> tuple[str, int, int]:
        trigger_rows = connection.execute(
            "SELECT name, sql FROM sqlite_master "
            "WHERE type = 'trigger' AND tbl_name = 'calendar_store_identity'"
        ).fetchall()
        triggers = {str(row[0]): " ".join(str(row[1]).split()) for row in trigger_rows}
        expected_triggers = {
            name: " ".join(sql.split()) for name, sql in _IDENTITY_TRIGGERS.items()
        }
        if triggers != expected_triggers:
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        identity_rows = connection.execute(
            "SELECT singleton, store_id, device, inode, identity_checksum "
            "FROM calendar_store_identity ORDER BY singleton"
        ).fetchall()
        if (
            len(identity_rows) != 1
            or type(identity_rows[0][0]) is not int
            or identity_rows[0][0] != 1
        ):
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        row = identity_rows[0]
        store_id = str(row[1])
        if re.fullmatch(r"[0-9a-f]{32}", store_id) is None:
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        if type(row[2]) is not int or type(row[3]) is not int:
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        if row[4] != cls._identity_checksum(store_id, row[2], row[3]):
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        return store_id, row[2], row[3]

    def _read_bound_store_identity(self, descriptor: int) -> tuple[str, int, int]:
        with closing(
            sqlite3.connect(f"file:///dev/fd/{descriptor}?mode=ro", uri=True, timeout=0)
        ) as connection:
            identity = self._read_store_identity(connection)
        bound = os.fstat(descriptor)
        if identity[1:] != (bound.st_dev, bound.st_ino):
            raise CalendarSyncStoreReadError("calendar control database is unavailable")
        return identity

    def _connect(self) -> sqlite3.Connection:
        _CALENDAR_SYNC_CONNECTION_LOCK.acquire()
        lock_owned = True
        connection: sqlite3.Connection | None = None
        descriptor: int | None = None
        try:
            expected = self._ensure_file_for_write()
            descriptor = os.open(self.path, os.O_RDWR | os.O_NOFOLLOW)
            bound = os.fstat(descriptor)
            if (bound.st_dev, bound.st_ino) != (expected.st_dev, expected.st_ino):
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            expected_identity = self._read_bound_store_identity(descriptor)
            connection = sqlite3.connect(self.path, factory=_CalendarSyncConnection)
            connection.bind_calendar_sync_lock()
            lock_owned = False
            connection.row_factory = sqlite3.Row
            current = self._validate_file()
            if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            if self._read_store_identity(connection) != expected_identity:
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            current = self._validate_file()
            if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            os.close(descriptor)
            descriptor = None
            return connection
        except BaseException as error:
            if connection is not None:
                connection.close()
            if lock_owned:
                _CALENDAR_SYNC_CONNECTION_LOCK.release()
            if isinstance(error, CalendarSyncStoreReadError):
                raise
            if isinstance(error, (OSError, sqlite3.Error, TypeError, ValueError)):
                raise CalendarSyncStoreReadError(
                    "calendar control database is unavailable"
                ) from None
            raise
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except BaseException:
                    pass

    def _connect_reader(self) -> sqlite3.Connection:
        _CALENDAR_SYNC_CONNECTION_LOCK.acquire()
        lock_owned = True
        descriptor: int | None = None
        connection: sqlite3.Connection | None = None
        try:
            info = self._validate_file()
            descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
            bound = os.fstat(descriptor)
            if (bound.st_dev, bound.st_ino) != (info.st_dev, info.st_ino):
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            connection = sqlite3.connect(
                f"file:///dev/fd/{descriptor}?mode=ro",
                uri=True,
                timeout=0,
                factory=_CalendarSyncConnection,
            )
            connection.bind_calendar_sync_lock()
            lock_owned = False
            connection.row_factory = sqlite3.Row
            identity = self._read_store_identity(connection)
            if identity[1:] != (info.st_dev, info.st_ino):
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            os.close(descriptor)
            descriptor = None
            return connection
        except BaseException as error:
            if connection is not None:
                connection.close()
            if lock_owned:
                _CALENDAR_SYNC_CONNECTION_LOCK.release()
            if isinstance(error, CalendarSyncStoreReadError):
                raise
            if isinstance(error, (OSError, sqlite3.Error, TypeError, ValueError)):
                raise CalendarSyncStoreReadError(
                    "calendar control database is unavailable"
                ) from None
            raise
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except BaseException:
                    pass

    def _initialize(self) -> None:
        with closing(self._connect()) as connection, connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS calendar_authority_versions (
                    checksum TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS calendar_sync_runs (
                    run_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    range_start TEXT NOT NULL,
                    range_end TEXT NOT NULL,
                    authority_checksum TEXT NOT NULL,
                    authority_years_json TEXT NOT NULL,
                    sources_json TEXT NOT NULL,
                    observed_open_count INTEGER NOT NULL,
                    conflicts_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS calendar_provider_observations (
                    run_id TEXT NOT NULL,
                    session_date TEXT NOT NULL,
                    provider_is_open INTEGER NOT NULL,
                    official_status TEXT NOT NULL,
                    PRIMARY KEY (run_id, session_date),
                    FOREIGN KEY (run_id) REFERENCES calendar_sync_runs(run_id)
                );
                CREATE TABLE IF NOT EXISTS calendar_sync_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    payload_json TEXT NOT NULL
                );
                """
            )

    def initialize_for_write(self) -> None:
        """Initialize writer state only after an operation has verified its authority."""
        self._validate_ancestors()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def state(self) -> CalendarSyncState:
        if not self.path.exists():
            if self.path.is_symlink():
                self._validate_file()
            self._validate_ancestors()
            if not os.path.lexists(self.path.parent):
                if self._missing_parent_is_empty:
                    return CalendarSyncState()
                raise CalendarSyncStoreReadError("calendar control database is unavailable")
            self._validate_missing_parent()
            return CalendarSyncState()
        try:
            with closing(self._connect_reader()) as connection:
                row = connection.execute(
                    "SELECT payload_json FROM calendar_sync_state WHERE singleton = 1"
                ).fetchone()
            return (
                CalendarSyncState()
                if row is None
                else CalendarSyncState.model_validate_json(row["payload_json"])
            )
        except (sqlite3.Error, OSError, UnicodeError, TypeError, ValueError) as exc:
            raise CalendarSyncStoreReadError(
                "calendar sync control database cannot be read"
            ) from exc

    def save(
        self,
        *,
        plan: CalendarSyncPlan,
        result: CalendarSyncResult,
        observations: list[CalendarObservation],
        authority_payload: dict[str, object],
        next_sync_at: datetime,
    ) -> None:
        current = self.state()
        next_state = current.model_copy(
            update={
                "last_attempt_at": result.completed_at,
                "last_full_sync_at": (
                    result.completed_at
                    if plan.mode == "full" and result.status == "ready"
                    else current.last_full_sync_at
                ),
                "last_light_sync_at": (
                    result.completed_at
                    if plan.mode == "light" and result.status in {"ready", "observed_only"}
                    else current.last_light_sync_at
                ),
                "last_full_slot_date": (
                    plan.requested_at.astimezone(SHANGHAI).date()
                    if plan.mode == "full"
                    else current.last_full_slot_date
                ),
                "last_light_slot_date": (
                    plan.requested_at.astimezone(SHANGHAI).date()
                    if plan.mode == "light" and plan.reason != "startup_check"
                    else current.last_light_slot_date
                ),
                "last_startup_slot_date": (
                    plan.requested_at.astimezone(SHANGHAI).date()
                    if plan.reason == "startup_check"
                    else current.last_startup_slot_date
                ),
                "last_success_at": (
                    result.completed_at
                    if result.status in {"ready", "observed_only"}
                    else current.last_success_at
                ),
                "last_good_run_id": (
                    result.run_id if result.status == "ready" else current.last_good_run_id
                ),
                "conflict_detected": (
                    True
                    if result.status == "quarantined"
                    else False
                    if result.status == "ready"
                    else current.conflict_detected
                ),
                "conflict_run_id": (
                    result.run_id
                    if result.status == "quarantined"
                    else None
                    if result.status == "ready"
                    else current.conflict_run_id
                ),
                "conflict_at": (
                    result.completed_at
                    if result.status == "quarantined"
                    else None
                    if result.status == "ready"
                    else current.conflict_at
                ),
                "next_sync_at": next_sync_at,
            }
        )
        with closing(self._connect()) as connection, connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO calendar_authority_versions
                    (checksum, payload_json, first_seen_at)
                VALUES (?, ?, ?)
                """,
                (
                    plan.authority_checksum,
                    json.dumps(authority_payload, ensure_ascii=False, sort_keys=True),
                    result.fetched_at.isoformat(),
                ),
            )
            connection.execute(
                """
                INSERT INTO calendar_sync_runs (
                    run_id, mode, reason, status, requested_at, fetched_at,
                    completed_at, range_start, range_end, authority_checksum,
                    authority_years_json, sources_json, observed_open_count,
                    conflicts_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.run_id,
                    plan.mode,
                    plan.reason,
                    result.status,
                    plan.requested_at.isoformat(),
                    result.fetched_at.isoformat(),
                    result.completed_at.isoformat(),
                    plan.range_start.isoformat(),
                    plan.range_end.isoformat(),
                    plan.authority_checksum,
                    json.dumps(plan.authority_years),
                    json.dumps(plan.sources, ensure_ascii=False),
                    result.observed_open_count,
                    json.dumps(result.conflicts, ensure_ascii=False),
                ),
            )
            connection.executemany(
                """
                INSERT INTO calendar_provider_observations (
                    run_id, session_date, provider_is_open, official_status
                ) VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        result.run_id,
                        item.session_date.isoformat(),
                        int(item.provider_is_open),
                        item.official_status,
                    )
                    for item in observations
                ],
            )
            connection.execute(
                """
                INSERT INTO calendar_sync_state (singleton, payload_json)
                VALUES (1, ?)
                ON CONFLICT(singleton) DO UPDATE SET payload_json = excluded.payload_json
                """,
                (next_state.model_dump_json(),),
            )

    def run(self, run_id: str) -> CalendarSyncRun:
        with closing(self._connect_reader()) as connection:
            row = connection.execute(
                "SELECT * FROM calendar_sync_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return CalendarSyncRun(
            run_id=row["run_id"],
            mode=row["mode"],
            reason=row["reason"],
            status=row["status"],
            requested_at=datetime.fromisoformat(row["requested_at"]),
            fetched_at=datetime.fromisoformat(row["fetched_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"]),
            range_start=date.fromisoformat(row["range_start"]),
            range_end=date.fromisoformat(row["range_end"]),
            authority_checksum=row["authority_checksum"],
            authority_years=json.loads(row["authority_years_json"]),
            sources=json.loads(row["sources_json"]),
            observed_open_count=row["observed_open_count"],
            conflicts=json.loads(row["conflicts_json"]),
        )

    def observations(self, run_id: str) -> list[CalendarObservation]:
        with closing(self._connect_reader()) as connection:
            rows = connection.execute(
                """
                SELECT session_date, provider_is_open, official_status
                FROM calendar_provider_observations
                WHERE run_id = ?
                ORDER BY session_date
                """,
                (run_id,),
            ).fetchall()
        return [
            CalendarObservation(
                session_date=date.fromisoformat(row["session_date"]),
                provider_is_open=bool(row["provider_is_open"]),
                official_status=row["official_status"],
            )
            for row in rows
        ]


def read_calendar_conflict(path: Path) -> bool:
    """Read the current calendar conflict flag without initializing control state.

    Repair gates must distinguish a missing control database from a valid state with no
    conflict. The caller receives only the typed boolean or sanitized read-boundary exception.
    """
    if not path.is_file():
        raise CalendarSyncStoreReadError("calendar sync control database cannot be read")
    store = CalendarSyncStore(path, initialize=False)
    try:
        with closing(store._connect_reader()) as connection:
            row = connection.execute(
                "SELECT payload_json FROM calendar_sync_state WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise ValueError("calendar sync state is unavailable")
        payload = json.loads(row["payload_json"])
        if not isinstance(payload, dict) or type(payload.get("conflict_detected")) is not bool:
            raise ValueError("calendar sync state is invalid")
        return CalendarSyncState.model_validate(payload).conflict_detected
    except (sqlite3.Error, OSError, UnicodeError, TypeError, ValueError) as exc:
        raise CalendarSyncStoreReadError("calendar sync control database cannot be read") from exc


class CalendarSyncService:
    def __init__(
        self,
        store: CalendarSyncStore,
        calendar: TradingCalendar,
        provider: TradingDateProvider | None,
        *,
        clock: Callable[[], datetime] | None = None,
        health_store=None,
    ) -> None:
        self.store = store
        self.calendar = calendar
        self.provider = provider
        self.clock = clock or (lambda: datetime.now(UTC))
        self.health_store = health_store
        self.policy = CalendarSyncPolicy()

    def plan(
        self,
        *,
        now: datetime,
        mode: Literal["auto", "full", "light"] = "auto",
        startup: bool = False,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> CalendarSyncPlan | None:
        # Capture authority before consulting legacy sync control.  An unavailable live
        # generation must not even open that reader, and all subsequent range/source work in
        # this operation is bound to the same concrete object.
        calendar_snapshot = self._calendar_snapshot()
        if getattr(calendar_snapshot, "status", "confirmed") != "confirmed":
            return None
        local = now.astimezone(SHANGHAI)
        if mode == "auto":
            decision = self.policy.decide(
                local,
                state=self.store.state(),
                startup=startup,
            )
            if decision.action == "none":
                return None
            selected_mode: SyncMode = decision.action
            reason = decision.reason
        else:
            selected_mode = mode
            reason = "explicit_request"
        if start_date is None:
            start_date = (
                _month_start_24_months_before(local.date())
                if selected_mode == "full"
                else local.date()
            )
        if end_date is None:
            end_date = local.date()
        if start_date > end_date:
            raise ValueError("calendar sync start date must not exceed end date")
        authority_payload = self._authority_payload(calendar_snapshot)
        encoded = json.dumps(
            authority_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        sources = [
            {
                "year": str(config.year),
                "published_on": config.published_on.isoformat(),
                **source.model_dump(mode="json"),
            }
            for config in sorted(
                calendar_snapshot.configs.values(),
                key=lambda item: item.year,
            )
            for source in config.sources
        ]
        return CalendarSyncPlan(
            mode=selected_mode,
            reason=reason,
            requested_at=local,
            range_start=start_date,
            range_end=end_date,
            authority_years=sorted(calendar_snapshot.configs),
            authority_checksum=hashlib.sha256(encoded).hexdigest(),
            sources=sources,
        )

    def authority_payload(self) -> dict[str, object]:
        return self._authority_payload(self._calendar_snapshot())

    def _calendar_snapshot(self):
        snapshot = getattr(self.calendar, "snapshot", None)
        return snapshot() if callable(snapshot) else self.calendar

    @staticmethod
    def _authority_payload(calendar_snapshot) -> dict[str, object]:
        payload: dict[str, object] = {
            "configs": [
                config.model_dump(mode="json")
                for config in sorted(
                    calendar_snapshot.configs.values(),
                    key=lambda item: item.year,
                )
            ]
        }
        for identity in ("bundled_sha256", "generation_sha256"):
            value = getattr(calendar_snapshot, identity, None)
            if value is not None:
                payload[identity] = value
        return payload

    def _authority_checksum(self, calendar_snapshot) -> str:
        encoded = json.dumps(
            self._authority_payload(calendar_snapshot),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    def _authority_changed_result(self, plan: CalendarSyncPlan) -> CalendarSyncResult:
        completed_at = self.clock().astimezone(UTC)
        return CalendarSyncResult(
            run_id=uuid.uuid4().hex,
            mode=plan.mode,
            status="error",
            range_start=plan.range_start,
            range_end=plan.range_end,
            fetched_at=completed_at,
            completed_at=completed_at,
            observed_open_count=0,
            authority_checksum=plan.authority_checksum,
            failure_code="CALENDAR_AUTHORITY_CHANGED",
        )

    def execute(self, plan: CalendarSyncPlan) -> CalendarSyncResult:
        try:
            plan = CalendarSyncPlan.model_validate(plan.model_dump(mode="python"))
        except (AttributeError, TypeError, ValueError):
            raise CalendarSyncPlanError("calendar sync plan is invalid") from None
        calendar_snapshot = self._calendar_snapshot()
        if self._authority_checksum(calendar_snapshot) != plan.authority_checksum:
            return self._authority_changed_result(plan)
        if self.provider is None:
            raise RuntimeError("calendar sync execution requires a provider")
        self.store.initialize_for_write()
        fetched_at = self.clock().astimezone(UTC)
        run_id = uuid.uuid4().hex
        if (
            self.health_store is not None
            and self.health_store.provider_health().state != CircuitState.CLOSED
        ):
            return CalendarSyncResult(
                run_id=run_id,
                mode=plan.mode,
                status="skipped_circuit_open",
                range_start=plan.range_start,
                range_end=plan.range_end,
                fetched_at=fetched_at,
                completed_at=self.clock().astimezone(UTC),
                observed_open_count=0,
                authority_checksum=plan.authority_checksum,
            )
        observations: list[TransportObservation] = []

        def record_observation(observation: TransportObservation) -> None:
            if observation.refresh_id != run_id:
                raise ProviderHealthError("calendar provider observation scope does not match")
            assert self.health_store is not None
            self.health_store.record_observation(observation)
            observations.append(observation)

        refresh_operation = getattr(self.provider, "refresh_operation", None)
        provider_scope = refresh_operation(run_id) if callable(refresh_operation) else nullcontext()

        def failed_result() -> CalendarSyncResult:
            return CalendarSyncResult(
                run_id=run_id,
                mode=plan.mode,
                status="error",
                range_start=plan.range_start,
                range_end=plan.range_end,
                fetched_at=fetched_at,
                completed_at=self.clock().astimezone(UTC),
                observed_open_count=0,
                authority_checksum=plan.authority_checksum,
            )

        try:
            if self.health_store is None:
                provider_open = set(self.provider.trading_dates(plan.range_start, plan.range_end))
            else:
                with transport_observation_sink(record_observation), provider_scope:
                    provider_open = set(
                        self.provider.trading_dates(plan.range_start, plan.range_end)
                    )
        except ProviderHealthError:
            raise
        except (BaoStockError, TimeoutError, OSError):
            if self.health_store is not None:
                self._resolve_calendar_transport(
                    run_id,
                    observations,
                    provider_succeeded=False,
                )
            result = failed_result()
            successful_operation = any(
                item.endpoint == ProviderEndpoint.TRADE_DATES
                and item.protocol_stage == ProtocolStage.OPERATION
                and item.normalized_error is None
                for item in observations
            )
            if not successful_operation:
                self.store.save(
                    plan=plan,
                    result=result,
                    observations=[],
                    authority_payload=self._authority_payload(calendar_snapshot),
                    next_sync_at=self.policy.next_scheduled_after(plan.requested_at),
                )
            return result
        except Exception:
            if self.health_store is not None:
                self._resolve_calendar_transport(
                    run_id,
                    observations,
                    provider_succeeded=False,
                )
            return failed_result()
        if self.health_store is not None:
            evidence_valid = self._resolve_calendar_transport(
                run_id,
                observations,
                provider_succeeded=True,
            )
            if not evidence_valid:
                return failed_result()
        invalid = [
            item for item in provider_open if item < plan.range_start or item > plan.range_end
        ]
        if invalid:
            raise ValueError("provider returned calendar dates outside requested range")
        observations: list[CalendarObservation] = []
        conflicts: list[dict[str, str]] = []
        current = plan.range_start
        unknown_count = 0
        while current <= plan.range_end:
            official = calendar_snapshot.session_status(current)
            provider_status = "open" if current in provider_open else "closed"
            observations.append(
                CalendarObservation(
                    session_date=current,
                    provider_is_open=current in provider_open,
                    official_status=official,
                )
            )
            if official != "unknown":
                if official != provider_status:
                    conflicts.append(
                        {
                            "date": current.isoformat(),
                            "official": official,
                            "provider": provider_status,
                        }
                    )
            else:
                unknown_count += 1
            current += timedelta(days=1)
        completed_at = self.clock().astimezone(UTC)
        status: SyncStatus = (
            "quarantined" if conflicts else "ready" if unknown_count == 0 else "observed_only"
        )
        result = CalendarSyncResult(
            run_id=run_id,
            mode=plan.mode,
            status=status,
            range_start=plan.range_start,
            range_end=plan.range_end,
            fetched_at=fetched_at,
            completed_at=completed_at,
            observed_open_count=len(provider_open),
            conflicts=conflicts,
            authority_checksum=plan.authority_checksum,
        )
        self.store.save(
            plan=plan,
            result=result,
            observations=observations,
            authority_payload=self._authority_payload(calendar_snapshot),
            next_sync_at=self.policy.next_scheduled_after(plan.requested_at),
        )
        return result

    def _resolve_calendar_transport(
        self,
        run_id: str,
        observations: list[TransportObservation],
        *,
        provider_succeeded: bool,
    ) -> bool:
        assert self.health_store is not None
        endpoint_observations = [
            item for item in observations if item.endpoint == ProviderEndpoint.TRADE_DATES
        ]
        operations = [
            item for item in endpoint_observations if item.protocol_stage == ProtocolStage.OPERATION
        ]
        errors = [
            item.normalized_error
            for item in endpoint_observations
            if item.normalized_error is not None
        ]
        if errors:
            self.health_store.record_terminal_failure(
                run_id,
                ProviderEndpoint.TRADE_DATES,
                max(errors, key=_TRANSPORT_ERROR_PRIORITY.__getitem__),
            )
            return False
        elif not provider_succeeded:
            self.health_store.record_terminal_failure(
                run_id,
                ProviderEndpoint.TRADE_DATES,
                NormalizedTransportError.PROTOCOL_ERROR,
            )
            return False
        evidence_valid = (
            len(operations) == 1
            and len({item.provider_session_id for item in endpoint_observations}) == 1
            and all(item.attempt == 1 for item in endpoint_observations)
        )
        if evidence_valid:
            self.health_store.record_terminal_success(run_id, ProviderEndpoint.TRADE_DATES)
            return True
        else:
            self.health_store.record_terminal_failure(
                run_id,
                ProviderEndpoint.TRADE_DATES,
                NormalizedTransportError.PROTOCOL_ERROR,
            )
            if not endpoint_observations:
                raise ProviderHealthError("calendar provider audit is incomplete")
            return False


async def run_calendar_sync_loop(
    service: CalendarSyncService,
    stop: asyncio.Event,
    *,
    clock: Callable[[], datetime],
    poll_seconds: float = 60,
    execute_plan: Callable[[CalendarSyncPlan], CalendarSyncResult | None] | None = None,
    runtime_maintenance: Callable[[], object] | None = None,
) -> None:
    """Run runtime maintenance plus legacy monthly/daily checks without blocking the API."""

    startup = True
    while not stop.is_set():
        runtime_blocked = False
        if runtime_maintenance is not None:
            try:
                runtime_result = await run_blocking_drained(runtime_maintenance)
            except Exception:
                # A runtime maintenance bug/control failure must not terminate the existing
                # scheduler or fall through to a legacy provider request in this iteration.
                logger.error(
                    "calendar runtime maintenance iteration failed",
                    extra={"error_code": "CALENDAR_RUNTIME_MAINTENANCE_FAILED"},
                )
                runtime_blocked = True
            else:
                runtime_blocked = (
                    runtime_result is None
                    or getattr(runtime_result, "outcome", None) not in _RUNTIME_MAINTENANCE_SUCCESS
                )
        plan = (
            None
            if runtime_blocked
            else service.plan(
                now=clock(),
                mode="auto",
                startup=startup,
            )
        )
        if plan is not None:
            result = await run_blocking_drained(
                execute_plan or service.execute,
                plan,
            )
            if result is not None:
                startup = False
        else:
            startup = False
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
        except TimeoutError:
            continue


def _month_start_24_months_before(value: date) -> date:
    absolute_month = value.year * 12 + value.month - 1 - 24
    return date(absolute_month // 12, absolute_month % 12 + 1, 1)
