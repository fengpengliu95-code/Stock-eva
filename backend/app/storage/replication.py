# ruff: noqa: E501

"""Local replication primitives.

This module contains the durable local evidence boundary for R2-F4.3 and the
offline, descriptor-bound destination archive primitives.  It never contacts a
provider or performs restore/API/CLI wiring.  Destination and sidecar writes
are explicit writer operations; status/readers remain read-only.
"""

from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
import sys
import uuid
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, Self, get_args

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator

from backend.app.config import Settings
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import MountInspector, SystemMountInspector

ZERO_SHA256 = "0" * 64
SIDECAR_SCHEMA_IDENTITY = "stock-eva/r2f4.3/replication-sidecar/v1"
SOURCE_INSTANCE_SCHEMA = "stock-eva/r2f4.3/source-instance/v2"
SOURCE_INSTANCE_DOMAIN: Literal["stock-eva/r2f4.3/source-instance/v3"] = (
    "stock-eva/r2f4.3/source-instance/v3"
)
SOURCE_DATASET_DOMAIN: Literal["stock-eva/r2f4.3/local-canonical"] = (
    "stock-eva/r2f4.3/local-canonical"
)
DATASET_IDENTITY_DOMAIN: Literal["stock-eva/r2f4.3/dataset-identity/v1"] = (
    "stock-eva/r2f4.3/dataset-identity/v1"
)
SOURCE_OBJECT_SET_DOMAIN: Literal["stock-eva/r2f4.3/object-set/v1"] = (
    "stock-eva/r2f4.3/object-set/v1"
)
GENERATION_PAYLOAD_DOMAIN: Literal["stock-eva/r2f4.3/replication-generation-payload/v1"] = (
    "stock-eva/r2f4.3/replication-generation-payload/v1"
)
GENERATION_TRUST_SCOPE: Literal["LOCAL_CHAIN_ONLY"] = "LOCAL_CHAIN_ONLY"
JOURNAL_SCHEMA_VERSION = 1

# The replication sidecar is an immutable generation log.  The former
# ``replication.sqlite3`` file is deliberately not part of this namespace.
SIDECAR_ROOT_NAME = "replication-sidecar"
SIDECAR_LOCK_NAME = ".writer.lock"
SIDECAR_GENESIS_NAME = "genesis.json"
SIDECAR_STAGING_NAME = ".staging"
SIDECAR_GENERATION_WIDTH = 20
SIDECAR_GENERATION_SUFFIX = ".db"

ReplicationState = Literal["disabled", "ready", "degraded", "unavailable"]
# These are the private, durable queue states.  They intentionally remain
# separate from the public status projection above.
OutboxState = Literal["pending", "copying", "verifying", "retry_wait", "replicated", "dead_letter"]
RETRY_DELAYS_SECONDS: tuple[int, ...] = (60, 300, 1800, 7200, 43200)
MAX_REPLICATION_ATTEMPTS = 6
REPLICATION_LEASE_SECONDS = 900
_OUTBOX_STATES = frozenset(
    {"pending", "copying", "verifying", "retry_wait", "replicated", "dead_letter"}
)
_RETRYABLE_REASONS = frozenset(
    {
        "DESTINATION_UNAVAILABLE",
        "DESTINATION_MOUNT_UNAVAILABLE",
        "COPY_FAILED",
        "VERIFY_FAILED",
        "RETRY_WAIT",
    }
)
ReplicationReason = Literal[
    "NONE",
    "DISABLED",
    "SOURCE_NOT_CONFIGURED",
    "SOURCE_UNAVAILABLE",
    "LOCAL_POINTER_MISMATCH",
    "REPLICATION_STATE_UNAVAILABLE",
    "OUTBOX_ENQUEUE_FAILED",
    "OUTBOX_JOURNALED",
    "OUTBOX_DURABILITY_UNAVAILABLE",
    "CONTROL_STATE_UNAVAILABLE",
    "DESTINATION_UNAVAILABLE",
    "DESTINATION_MOUNT_UNAVAILABLE",
    "DESTINATION_TRUST_FAILED",
    "DESTINATION_REBOUND",
    "DESTINATION_AHEAD",
    "DESTINATION_CONFLICT",
    "CAS_CONFLICT",
    "COPY_FAILED",
    "VERIFY_FAILED",
    "RETRY_WAIT",
    "DEAD_LETTER",
    "ALREADY_REPLICATED",
    "PATH_INVALID",
    "PATH_CHANGED",
    "SYMLINK_UNSAFE",
    "DIRECTION_NOT_ALLOWED",
    "MOUNT_UNSUPPORTED",
]


class ReplicationDurabilityError(RuntimeError):
    """A local immutable artifact could not be safely persisted or read."""


class DestinationHeadInstallError(ReplicationDurabilityError):
    """Head install failure carrying whether the atomic syscall linearized."""

    def __init__(
        self,
        message: str,
        *,
        linearized: bool,
        reason_code: ReplicationReason = "COPY_FAILED",
        cleanup_failed: bool = False,
    ) -> None:
        super().__init__(message)
        self.linearized = linearized
        self.reason_code = reason_code
        # This is intentionally a boolean only.  Cleanup diagnostics must not
        # expose the temporary name, path, or underlying OS exception.
        self.cleanup_failed = cleanup_failed


@dataclass(frozen=True)
class DestinationHeadInstallResult:
    """Successful head-install outcome at the atomic linearization point."""

    linearized: bool


class ReplicationStateUnavailable(ReplicationDurabilityError):
    """The local sidecar is missing, corrupt, or cannot be proven consistent."""


class ReplicationCASConflict(ReplicationDurabilityError):
    """A deterministic next generation already exists with no overwrite allowed."""


class ReplicationPostCommitConflict(ReplicationStateUnavailable):
    """A generation was linked, then its final namespace proof failed."""

    reason_code: Literal["CONTROL_STATE_UNAVAILABLE"] = "CONTROL_STATE_UNAVAILABLE"


class VerifiedDestinationCommitProof(BaseModel):
    """Closed, immutable proof projection supplied to the verifier.

    The model is intentionally constructible so callers cannot rely on
    secrecy of an in-memory token.  Completion still requires the configured
    descriptor-native verifier to reread the destination and compare every
    field before touching the local sidecar.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    intent_id: str | None = Field(
        default=None, min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    destination_id: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")
    source_sequence: int = Field(ge=1)
    destination_generation: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    destination_record_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    destination_head_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_id: str | None = Field(
        default=None, min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    source_instance_sha256: str | None = Field(
        default=None, min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    worker_id: str | None = Field(default=None, min_length=1, max_length=128)
    state_version: int | None = Field(default=None, ge=1)
    now: str

    _now_is_utc = field_validator("now")(lambda value: _validate_utc_timestamp(value, "now"))

    @model_validator(mode="before")
    @classmethod
    def exact_fields(cls, value: object) -> object:
        if isinstance(value, Mapping) and set(value) != set(cls.model_fields):
            raise ValueError("destination proof fields are not exact")
        return value


class ReplicationClaimContext(BaseModel):
    """Frozen sidecar claim identity required for writer-owned completion."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    intent_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    worker_id: str = Field(min_length=1, max_length=128)
    state_version: int = Field(ge=1)
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")


def canonical_json_bytes(value: object) -> bytes:
    """Return the one-newline canonical JSON representation used by R2-F4.3."""
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def domain_sha256(domain: str, value: object) -> str:
    if not domain or any(ord(char) > 127 for char in domain):
        raise ValueError("hash domain must be non-empty ASCII")
    return hashlib.sha256(domain.encode("ascii") + b"\n" + canonical_json_bytes(value)).hexdigest()


def normalized_ddl_bytes(ddl: str) -> bytes:
    normalized = ddl.replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(line.rstrip() for line in normalized.splitlines()).strip("\n")
    return (normalized + "\n").encode("utf-8")


def _expected_sqlite_objects() -> set[tuple[str, str, str | None, str | None]]:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(SIDECAR_DDL)
        return set(
            connection.execute(
                """SELECT type, name, tbl_name, sql
                     FROM sqlite_master
                    WHERE type IN ('table', 'index', 'trigger')"""
            ).fetchall()
        )
    finally:
        failures: list[str] = []
        _close_connection_best_effort(connection, failures)
        if failures:
            raise _cleanup_error(failures)


SIDECAR_DDL = """PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA user_version = 1;

CREATE TABLE replication_sidecar_meta (
    sidecar_id INTEGER PRIMARY KEY NOT NULL CHECK (sidecar_id = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    schema_identity TEXT NOT NULL CHECK (schema_identity = 'stock-eva/r2f4.3/replication-sidecar/v1'),
    ddl_sha256 TEXT NOT NULL CHECK (length(ddl_sha256) = 64),
    schema_digest TEXT NOT NULL CHECK (length(schema_digest) = 64),
    source_instance_id TEXT NOT NULL CHECK (length(source_instance_id) = 64),
    source_instance_sha256 TEXT NOT NULL CHECK (length(source_instance_sha256) = 64),
    created_at TEXT NOT NULL,
    generation_number INTEGER NOT NULL CHECK (generation_number >= 0),
    previous_generation_sha256 TEXT NOT NULL CHECK (length(previous_generation_sha256) = 64),
    generation_payload_sha256 TEXT NOT NULL CHECK (length(generation_payload_sha256) = 64)
) STRICT;

CREATE TABLE replication_intents (
    intent_id TEXT PRIMARY KEY NOT NULL CHECK (length(intent_id) = 64),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    operation_day TEXT NOT NULL CHECK (operation_day GLOB '????-??-??'),
    direction TEXT NOT NULL CHECK (direction = 'local_to_nas'),
    destination_id TEXT NOT NULL CHECK (length(destination_id) = 32),
    pointer_row_sha256 TEXT NOT NULL CHECK (length(pointer_row_sha256) = 64),
    pointer_generation TEXT NOT NULL CHECK (length(pointer_generation) BETWEEN 1 AND 128),
    source_run_id TEXT NOT NULL CHECK (length(source_run_id) BETWEEN 1 AND 128),
    source_trade_date TEXT NOT NULL CHECK (source_trade_date GLOB '????-??-??'),
    source_published_at TEXT NOT NULL,
    pointer_db_device INTEGER NOT NULL CHECK (pointer_db_device > 0),
    pointer_db_inode INTEGER NOT NULL CHECK (pointer_db_inode > 0),
    pointer_db_schema_digest TEXT NOT NULL CHECK (length(pointer_db_schema_digest) = 64),
    manifest_canonical_sha256 TEXT NOT NULL CHECK (length(manifest_canonical_sha256) = 64),
    source_object_set_sha256 TEXT NOT NULL CHECK (length(source_object_set_sha256) = 64),
    publication_binding_sha256 TEXT NOT NULL CHECK (length(publication_binding_sha256) = 64),
    source_instance_id TEXT NOT NULL CHECK (length(source_instance_id) = 64),
    source_instance_sha256 TEXT NOT NULL CHECK (length(source_instance_sha256) = 64),
    source_sequence INTEGER NOT NULL CHECK (source_sequence >= 1),
    checkpoint_id TEXT NOT NULL CHECK (length(checkpoint_id) = 64),
    source_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(source_manifest_bytes_sha256) = 64),
    plan_sha256 TEXT NOT NULL CHECK (length(plan_sha256) = 64),
    intent_sha256 TEXT NOT NULL CHECK (length(intent_sha256) = 64),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    created_at TEXT NOT NULL,
    UNIQUE (direction, destination_id, checkpoint_id),
    UNIQUE (direction, destination_id, source_instance_id, source_sequence),
    UNIQUE (source_instance_id, source_sequence)
) STRICT;

CREATE TABLE replication_destination_cache (
    destination_id TEXT PRIMARY KEY NOT NULL CHECK (length(destination_id) = 32),
    descriptor_sha256 TEXT NOT NULL CHECK (length(descriptor_sha256) = 64),
    head_sha256 TEXT CHECK (head_sha256 IS NULL OR length(head_sha256) = 64),
    replication_generation TEXT CHECK (replication_generation IS NULL OR length(replication_generation) = 64),
    record_sha256 TEXT CHECK (record_sha256 IS NULL OR length(record_sha256) = 64),
    source_instance_id TEXT CHECK (source_instance_id IS NULL OR length(source_instance_id) = 64),
    source_sequence INTEGER CHECK (source_sequence IS NULL OR source_sequence >= 1),
    health_state TEXT NOT NULL CHECK (health_state IN ('unknown','healthy','unavailable','unsupported')),
    health_observed_at TEXT,
    cache_version INTEGER NOT NULL CHECK (cache_version >= 0),
    updated_at TEXT NOT NULL,
    CHECK ((health_state = 'unknown' AND health_observed_at IS NULL)
        OR (health_state <> 'unknown' AND health_observed_at IS NOT NULL))
) STRICT;

CREATE TABLE replication_attempt_events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK (length(event_id) = 64),
    intent_id TEXT NOT NULL REFERENCES replication_intents(intent_id),
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
    prev_event_sha256 TEXT NOT NULL CHECK (length(prev_event_sha256) = 64),
    event_type TEXT NOT NULL CHECK (event_type IN ('intent_created','claim','transition','attempt','terminal')),
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (to_state IN ('pending','copying','verifying','retry_wait','replicated','dead_letter')),
    attempt INTEGER NOT NULL CHECK (attempt >= 0 AND attempt <= 6),
    reason_code TEXT NOT NULL,
    state_version INTEGER NOT NULL CHECK (state_version >= 0),
    occurred_at TEXT NOT NULL,
    destination_replication_generation TEXT CHECK (destination_replication_generation IS NULL OR length(destination_replication_generation) = 64),
    destination_record_sha256 TEXT CHECK (destination_record_sha256 IS NULL OR length(destination_record_sha256) = 64),
    destination_head_sha256 TEXT CHECK (destination_head_sha256 IS NULL OR length(destination_head_sha256) = 64),
    event_sha256 TEXT NOT NULL CHECK (length(event_sha256) = 64),
    UNIQUE (intent_id, event_sequence)
) STRICT;

CREATE TABLE replication_heads (
    intent_id TEXT PRIMARY KEY NOT NULL REFERENCES replication_intents(intent_id),
    current_state TEXT NOT NULL CHECK (current_state IN ('pending','copying','verifying','retry_wait','replicated','dead_letter')),
    state_version INTEGER NOT NULL CHECK (state_version >= 0),
    last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 0),
    lease_owner TEXT,
    lease_until TEXT,
    next_attempt_at TEXT NOT NULL,
    last_reason_code TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((current_state IN ('copying','verifying') AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR current_state NOT IN ('copying','verifying'))
) STRICT;

CREATE INDEX replication_heads_due_idx ON replication_heads (current_state, next_attempt_at);
CREATE INDEX replication_attempt_events_intent_idx
    ON replication_attempt_events (intent_id, event_sequence);
CREATE INDEX replication_intents_checkpoint_idx
    ON replication_intents (source_instance_id, checkpoint_id);
CREATE INDEX replication_destination_cache_health_idx
    ON replication_destination_cache (health_state, updated_at);

CREATE TRIGGER replication_sidecar_meta_no_update
BEFORE UPDATE ON replication_sidecar_meta
WHEN OLD.generation_payload_sha256 <> '0000000000000000000000000000000000000000000000000000000000000000'
BEGIN
    SELECT RAISE(ABORT, 'replication schema identity is immutable');
END;
CREATE TRIGGER replication_sidecar_meta_no_delete
BEFORE DELETE ON replication_sidecar_meta BEGIN
    SELECT RAISE(ABORT, 'replication schema identity is immutable');
END;
CREATE TRIGGER replication_intents_no_update
BEFORE UPDATE ON replication_intents BEGIN
    SELECT RAISE(ABORT, 'replication intent is immutable');
END;
CREATE TRIGGER replication_intents_no_delete
BEFORE DELETE ON replication_intents BEGIN
    SELECT RAISE(ABORT, 'replication intent is immutable');
END;
CREATE TRIGGER replication_attempt_events_no_update
BEFORE UPDATE ON replication_attempt_events BEGIN
    SELECT RAISE(ABORT, 'replication event is immutable');
END;
CREATE TRIGGER replication_attempt_events_no_delete
BEFORE DELETE ON replication_attempt_events BEGIN
    SELECT RAISE(ABORT, 'replication event is immutable');
END;
CREATE TRIGGER replication_heads_no_delete
BEFORE DELETE ON replication_heads BEGIN
    SELECT RAISE(ABORT, 'replication head is a derived audit projection');
END;
CREATE TRIGGER replication_heads_monotonic_cas
BEFORE UPDATE ON replication_heads
WHEN NEW.state_version <> OLD.state_version + 1
BEGIN
    SELECT RAISE(ABORT, 'replication head requires state-version CAS');
END;"""

SIDECAR_DDL_SHA256 = hashlib.sha256(normalized_ddl_bytes(SIDECAR_DDL)).hexdigest()
SIDECAR_SCHEMA_DIGEST = domain_sha256(
    "stock-eva/r2f4.3/replication-schema/v1",
    {
        "schema_identity": SIDECAR_SCHEMA_IDENTITY,
        "schema_version": 1,
        "ddl_sha256": SIDECAR_DDL_SHA256,
    },
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validate_sha(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(f"{field} must be a lower-case SHA-256 digest")


def _validate_utc_timestamp(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field} must use a UTC Z timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must use UTC")
    return value


def _validate_iso_date(value: str, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO date")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date") from exc
    return value


def _safe_parent(path: Path) -> None:
    path = _physical_path(path)
    if not path.is_absolute():
        raise ReplicationDurabilityError("replication path must be absolute")
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ReplicationDurabilityError("replication path ancestor is unsafe")


def _physical_path(path: Path) -> Path:
    """Bind macOS's system /var alias before descriptor operations."""
    if not path.is_absolute():
        return path
    absolute = Path(os.path.normpath(path))
    if absolute.parts[1:2] == ("var",) and Path("/var").is_symlink():
        return Path("/private/var", *absolute.parts[2:])
    return absolute


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW


def _open_directory_chain(path: Path, *, create: bool = False) -> tuple[int, list[int]]:
    """Open every ancestor with ``openat``-style descriptor traversal.

    The returned descriptors remain open until the caller closes the list.  No
    later operation in the critical section needs to reopen a checked path.
    """
    path = _physical_path(path)
    if not path.is_absolute() or path == Path("/") and not create:
        if not path.is_absolute():
            raise ReplicationDurabilityError("replication path must be absolute")
    descriptors: list[int] = []
    try:
        current = os.open(Path(path.anchor), _DIRECTORY_FLAGS)
        descriptors.append(current)
        for component in path.parts[1:]:
            created = False
            try:
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=current)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(component, mode=0o700, dir_fd=current)
                created = True
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=current)
            if created:
                # A newly-created directory is not durable until both the
                # child inode and its containing directory have been synced.
                _fsync_open_directory(child)
                _fsync_open_directory(current)
            descriptors.append(child)
            current = child
        return descriptors[-1], descriptors
    except (OSError, ReplicationDurabilityError) as exc:
        cleanup_failures: list[str] = []
        _close_descriptors(descriptors, cleanup_failures)
        if isinstance(exc, ReplicationDurabilityError):
            _finish_cleanup(exc, cleanup_failures)
            raise
        primary = ReplicationDurabilityError("replication path ancestor is unavailable")
        _finish_cleanup(primary, cleanup_failures)
        raise primary from exc


def _close_fd_best_effort(fd: int, label: str, failures: list[str]) -> None:
    try:
        os.close(fd)
    except OSError:
        failures.append(label)


def _cleanup_error(failures: list[str]) -> ReplicationDurabilityError:
    labels = ",".join(dict.fromkeys(failures))
    return ReplicationDurabilityError(f"replication cleanup failed: {labels}")


def _close_connection_best_effort(connection: sqlite3.Connection, failures: list[str]) -> None:
    try:
        connection.close()
    except BaseException:
        failures.append("memory_connection")


def _close_descriptors(descriptors: list[int], failures: list[str]) -> None:
    for descriptor in reversed(descriptors):
        _close_fd_best_effort(descriptor, "dir_fd", failures)


def _finish_cleanup(primary: BaseException | None, failures: list[str]) -> None:
    """Finish a cleanup scope without masking its primary failure."""
    if not failures:
        return
    if primary is not None:
        primary.add_note("replication_cleanup=failed")
        return
    raise _cleanup_error(failures)


def _read_descriptor(
    fd: int,
    *,
    require_private_mode: bool = True,
) -> tuple[bytes, os.stat_result]:
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode):
        raise ReplicationDurabilityError("replication artifact is not a regular file")
    if require_private_mode and stat.S_IMODE(before.st_mode) != 0o600:
        raise ReplicationDurabilityError("replication artifact permissions are unsafe")
    chunks: list[bytes] = []
    offset = 0
    while True:
        # ``pread`` makes every proof independent of the descriptor's current
        # offset.  A held writer descriptor is read more than once (baseline,
        # CAS and final proof), so a plain ``read`` would turn the second proof
        # into an empty read and could hide an ABA/replacement race.
        chunk = os.pread(fd, 1024 * 1024, offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    after = os.fstat(fd)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ReplicationDurabilityError("replication artifact changed during read")
    return b"".join(chunks), after


def _read_nofollow(
    path: Path,
    *,
    require_private_mode: bool = True,
) -> tuple[bytes, os.stat_result]:
    path = _physical_path(path)
    _, descriptors = _open_directory_chain(path.parent)
    parent_fd = descriptors[-1]
    fd: int | None = None
    primary_error: BaseException | None = None
    try:
        fd = os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=parent_fd,
        )
        return _read_descriptor(fd, require_private_mode=require_private_mode)
    except FileNotFoundError as exc:
        primary_error = ReplicationDurabilityError("replication artifact is unreadable")
        raise primary_error from exc
    except OSError as exc:
        primary_error = ReplicationDurabilityError("replication artifact read failed")
        raise primary_error from exc
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if fd is not None:
            cleanup_failures: list[str] = []
            _close_fd_best_effort(fd, "artifact_fd", cleanup_failures)
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)
        else:
            cleanup_failures = []
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)


def _read_optional_nofollow(
    path: Path,
    *,
    require_private_mode: bool = True,
) -> tuple[bytes, os.stat_result] | None:
    """Read an optional sidecar without treating a symlink as absence."""
    path = _physical_path(path)
    try:
        _, descriptors = _open_directory_chain(path.parent)
    except ReplicationDurabilityError:
        # A missing ancestor means this optional artifact is absent.  An
        # existing but unsafe ancestor must still fail closed.
        if not path.parent.exists():
            return None
        raise
    parent_fd = descriptors[-1]
    fd: int | None = None
    primary_error: BaseException | None = None
    try:
        try:
            fd = os.open(
                path.name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            return None
        return _read_descriptor(fd, require_private_mode=require_private_mode)
    except OSError as exc:
        primary_error = ReplicationDurabilityError("replication artifact read failed")
        raise primary_error from exc
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if fd is not None:
            cleanup_failures = []
            _close_fd_best_effort(fd, "artifact_fd", cleanup_failures)
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)
        else:
            cleanup_failures = []
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)


def _stat_nofollow(path: Path) -> os.stat_result:
    path = _physical_path(path)
    _, descriptors = _open_directory_chain(path.parent)
    fd: int | None = None
    primary_error: BaseException | None = None
    try:
        fd = os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=descriptors[-1],
        )
        return os.fstat(fd)
    except OSError as exc:
        primary_error = ReplicationDurabilityError("replication artifact is unreadable")
        raise primary_error from exc
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if fd is not None:
            cleanup_failures: list[str] = []
            _close_fd_best_effort(fd, "artifact_fd", cleanup_failures)
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)
        else:
            cleanup_failures = []
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)


def _fsync_directory(path: Path) -> None:
    path = _physical_path(path)
    _, descriptors = _open_directory_chain(path)
    fd = descriptors[-1]
    primary_error: BaseException | None = None
    try:
        _fsync_open_directory(fd)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_failures: list[str] = []
        _close_descriptors(descriptors, cleanup_failures)
        _finish_cleanup(primary_error, cleanup_failures)


def _fsync_open_directory(fd: int) -> None:
    try:
        os.fsync(fd)
    except OSError as exc:
        raise ReplicationDurabilityError("replication parent fsync failed") from exc


def _fsync_nofollow(path: Path, *, optional: bool = False) -> None:
    path = _physical_path(path)
    try:
        _, descriptors = _open_directory_chain(path.parent)
    except ReplicationDurabilityError:
        if optional and not path.parent.exists():
            return
        raise
    fd: int | None = None
    primary_error: BaseException | None = None
    try:
        try:
            fd = os.open(
                path.name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptors[-1],
            )
        except FileNotFoundError:
            if optional:
                return
            raise
        os.fsync(fd)
    except OSError as exc:
        primary_error = ReplicationDurabilityError("replication artifact fsync failed")
        raise primary_error from exc
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if fd is not None:
            cleanup_failures: list[str] = []
            _close_fd_best_effort(fd, "artifact_fd", cleanup_failures)
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)
        else:
            cleanup_failures = []
            _close_descriptors(descriptors, cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)


def _write_fully(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:
            raise ReplicationDurabilityError("replication artifact short write")
        offset += written


def _install_no_replace(path: Path, payload: bytes) -> Path:
    path = _physical_path(path)
    parent = path.parent
    parent_fd, descriptors = _open_directory_chain(parent, create=True)
    target_fd: int | None = None
    temporary_fd: int | None = None
    primary_error: BaseException | None = None
    cleanup_failures: list[str] = []
    temporary_name = f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        try:
            target_fd = os.open(
                path.name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            target_fd = None
        except OSError as exc:
            raise ReplicationDurabilityError("replication artifact target is unsafe") from exc
        if target_fd is not None:
            try:
                existing, _ = _read_descriptor(target_fd)
            finally:
                _close_fd_best_effort(target_fd, "target_fd", cleanup_failures)
                target_fd = None
            if existing == payload:
                return path
            raise ReplicationDurabilityError(
                "replication durability conflict: artifact already exists"
            )

        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        try:
            _write_fully(temporary_fd, payload)
            os.fsync(temporary_fd)
        finally:
            _close_fd_best_effort(temporary_fd, "temporary_fd", cleanup_failures)
            temporary_fd = None
        try:
            os.link(
                temporary_name,
                path.name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            target_fd = os.open(
                path.name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
            try:
                existing, _ = _read_descriptor(target_fd)
            finally:
                _close_fd_best_effort(target_fd, "target_fd", cleanup_failures)
                target_fd = None
            if existing != payload:
                raise ReplicationDurabilityError(
                    "replication durability conflict: artifact already exists"
                ) from exc
        _fsync_open_directory(parent_fd)
        return path
    except OSError as exc:
        primary_error = ReplicationDurabilityError("replication artifact install failed")
        raise primary_error from exc
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if temporary_fd is not None:
            _close_fd_best_effort(temporary_fd, "temporary_fd", cleanup_failures)
        try:
            os.unlink(temporary_name, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        except OSError:
            cleanup_failures.append("temporary_unlink")
        if target_fd is not None:
            _close_fd_best_effort(target_fd, "target_fd", cleanup_failures)
        _close_descriptors(descriptors, cleanup_failures)
        _finish_cleanup(primary_error, cleanup_failures)


class ReplicationEffects(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    writes: bool
    canonical_writes: bool
    destination_writes: bool
    outbox_writes: bool
    restore_writes: bool


class ReplicationStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: ReplicationState
    reason_code: ReplicationReason
    enabled: bool
    source_ready: bool
    destination_configured: bool
    outbox_schema_version: Literal[1] | None
    pending_count: int = Field(ge=0)
    copying_count: int = Field(ge=0)
    verifying_count: int = Field(ge=0)
    retry_wait_count: int = Field(ge=0)
    dead_letter_count: int = Field(ge=0)
    last_replicated_source_manifest_sha256: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    last_replicated_at: str | None
    local_ready: bool
    destination_health: Literal["unknown", "healthy", "unavailable", "unsupported"]
    destination_health_observed_at: str | None
    queue_lag_seconds: int | None = Field(default=None, ge=0)
    lag_seconds: int | None = Field(default=None, ge=0)
    provider_requests: Literal[0] = 0
    mode: Literal["status"] = "status"
    execution_allowed: Literal[False] = False
    effects: ReplicationEffects
    paths_exposed: Literal[False] = False
    trust_scope: Literal["LOCAL_CHAIN_ONLY"] = GENERATION_TRUST_SCOPE

    @model_validator(mode="after")
    def public_projection_is_sanitized(self) -> ReplicationStatusResponse:
        for value, field in (
            (self.last_replicated_source_manifest_sha256, "last_replicated_source_manifest_sha256"),
        ):
            if value is not None:
                _validate_sha(value, field)
        for value, field in (
            (self.last_replicated_at, "last_replicated_at"),
            (self.destination_health_observed_at, "destination_health_observed_at"),
        ):
            if value is not None:
                _validate_utc_timestamp(value, field)
        return self


class ReplicationIntent(BaseModel):
    """Closed read model for one immutable local outbox intent."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    intent_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    schema_version: Literal[1] = 1
    operation_day: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    direction: Literal["local_to_nas"] = "local_to_nas"
    destination_id: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")
    pointer_row_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    pointer_generation: str = Field(min_length=1, max_length=128)
    source_run_id: str = Field(min_length=1, max_length=128)
    source_trade_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    source_published_at: str = Field(min_length=20)
    pointer_db_device: int = Field(gt=0)
    pointer_db_inode: int = Field(gt=0)
    pointer_db_schema_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    manifest_canonical_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_object_set_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    publication_binding_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_sequence: int = Field(ge=1)
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_manifest_bytes_sha256: str = Field(
        min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    plan_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    intent_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    object_count: int = Field(ge=0)
    row_count: int = Field(ge=0)
    byte_count: int = Field(ge=0)
    created_at: str = Field(min_length=20)

    _operation_day_is_iso = field_validator("operation_day")(
        lambda value: _validate_iso_date(value, "operation_day")
    )
    _source_trade_date_is_iso = field_validator("source_trade_date")(
        lambda value: _validate_iso_date(value, "source_trade_date")
    )
    _timestamps_are_utc = field_validator("source_published_at", "created_at")(
        lambda value, info: _validate_utc_timestamp(value, info.field_name)
    )


class ReplicationClaim(BaseModel):
    """The lease proof returned by a successful state-version CAS claim."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    intent_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    worker_id: str = Field(min_length=1, max_length=128)
    current_state: Literal["copying"] = "copying"
    state_version: int = Field(ge=1)
    attempt: int = Field(ge=1, le=MAX_REPLICATION_ATTEMPTS)
    lease_until: str = Field(min_length=20)
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_sequence: int = Field(ge=1)
    provider_requests: Literal[0] = 0

    _lease_is_utc = field_validator("lease_until")(
        lambda value: _validate_utc_timestamp(value, "lease_until")
    )


class ReplicationHead(BaseModel):
    """Sanitized replay projection of one intent head."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    intent_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    current_state: OutboxState
    state_version: int = Field(ge=0)
    last_event_sequence: int = Field(ge=0)
    lease_owner: str | None = Field(default=None, max_length=128)
    lease_until: str | None = None
    next_attempt_at: str
    last_reason_code: ReplicationReason
    updated_at: str
    attempt: int = Field(ge=0, le=MAX_REPLICATION_ATTEMPTS)
    retry_delay_seconds: int = Field(ge=0)
    provider_requests: Literal[0] = 0

    @model_validator(mode="after")
    def validate_head_timestamps(self) -> ReplicationHead:
        _validate_utc_timestamp(self.next_attempt_at, "next_attempt_at")
        _validate_utc_timestamp(self.updated_at, "updated_at")
        if self.lease_until is not None:
            _validate_utc_timestamp(self.lease_until, "lease_until")
        if self.current_state in {"copying", "verifying"}:
            if not self.lease_owner or self.lease_until is None:
                raise ValueError("leased state requires lease owner and expiry")
        elif self.lease_owner is not None or self.lease_until is not None:
            raise ValueError("non-leased state cannot retain a lease")
        return self


class ReplicationImportResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    intent_ids: tuple[str, ...]
    source_sequences: tuple[int, ...]
    imported_count: int = Field(ge=0)
    existing_count: int = Field(ge=0)
    provider_requests: Literal[0] = 0

    @property
    def intent_id(self) -> str | None:
        """Convenience projection for the single-checkpoint enqueue case."""
        return self.intent_ids[0] if len(self.intent_ids) == 1 else None

    @property
    def source_sequence(self) -> int | None:
        return self.source_sequences[0] if len(self.source_sequences) == 1 else None


class ReplicationOutboxResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: Literal["disabled", "queued", "existing", "unavailable"]
    reason_code: ReplicationReason
    intent_ids: tuple[str, ...] = ()
    source_sequences: tuple[int, ...] = ()
    provider_requests: Literal[0] = 0
    outbox_writes: bool = False


def is_retryable_replication_reason(reason_code: str) -> bool:
    """Return the closed retry classification used by the local drain."""
    return reason_code in _RETRYABLE_REASONS


class SourceInstanceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_instance_schema: Literal[SOURCE_INSTANCE_SCHEMA] = SOURCE_INSTANCE_SCHEMA
    schema_version: Literal[2] = 2
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    canonical_root_path: str = Field(min_length=1)
    canonical_schema_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_domain: Literal[SOURCE_DATASET_DOMAIN] = SOURCE_DATASET_DOMAIN
    fsync_contract: Literal["file_and_parent_directory"] = "file_and_parent_directory"
    source_instance_nonce: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    dataset_identity: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    created_at: str = Field(min_length=20)
    source_instance_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    @field_validator("canonical_root_path")
    @classmethod
    def canonical_root_is_absolute(cls, value: str) -> str:
        root = Path(value)
        if not root.is_absolute() or root == Path("/"):
            raise ValueError("canonical_root_path must be an absolute local root")
        return value

    _created_at_is_utc = field_validator("created_at")(
        lambda value: _validate_utc_timestamp(value, "created_at")
    )

    def hash_preimage(self) -> dict[str, object]:
        value = self.model_dump(mode="json")
        value.pop("source_instance_sha256")
        return value

    def verify_hash(self) -> None:
        expected = domain_sha256("stock-eva/r2f4.3/source-instance-record/v2", self.hash_preimage())
        if expected != self.source_instance_sha256:
            raise ReplicationDurabilityError("source instance hash is invalid")

    def verify_identity(self) -> None:
        dataset_identity = domain_sha256(
            DATASET_IDENTITY_DOMAIN,
            {
                "canonical_root_path": self.canonical_root_path,
                "canonical_schema_digest": self.canonical_schema_digest,
                "source_instance_domain": self.source_instance_domain,
                "source_instance_nonce": self.source_instance_nonce,
            },
        )
        source_instance_id = domain_sha256(
            SOURCE_INSTANCE_DOMAIN,
            {"dataset_identity": dataset_identity},
        )
        if (
            self.dataset_identity != dataset_identity
            or self.source_instance_id != source_instance_id
        ):
            raise ReplicationDurabilityError("source instance identity is invalid")


class SourceInstanceStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def create(
        self,
        *,
        canonical_root_path: Path,
        canonical_schema_digest: str,
        source_instance_domain: str = SOURCE_DATASET_DOMAIN,
        source_instance_nonce: str | None = None,
        created_at: str | None = None,
    ) -> SourceInstanceRecord:
        if not canonical_root_path.is_absolute():
            raise ReplicationDurabilityError("canonical root must be an absolute local root")
        canonical_root = _physical_path(canonical_root_path)
        if canonical_root == Path("/"):
            raise ReplicationDurabilityError("canonical root is unsafe")
        expected_path = canonical_root / "_replication" / "source-instance.json"
        if _physical_path(self.path) != expected_path:
            raise ReplicationDurabilityError("source instance path is outside canonical layout")
        _validate_sha(canonical_schema_digest, "canonical_schema_digest")
        nonce = source_instance_nonce or secrets.token_hex(32)
        _validate_sha(nonce, "source_instance_nonce")
        if source_instance_domain != SOURCE_DATASET_DOMAIN:
            raise ReplicationDurabilityError("source instance domain is not approved")
        dataset_identity = domain_sha256(
            DATASET_IDENTITY_DOMAIN,
            {
                "canonical_root_path": str(canonical_root),
                "canonical_schema_digest": canonical_schema_digest,
                "source_instance_domain": source_instance_domain,
                "source_instance_nonce": nonce,
            },
        )
        source_instance_id = domain_sha256(
            SOURCE_INSTANCE_DOMAIN,
            {"dataset_identity": dataset_identity},
        )
        values: dict[str, object] = {
            "source_instance_schema": SOURCE_INSTANCE_SCHEMA,
            "schema_version": 2,
            "source_instance_id": source_instance_id,
            "canonical_root_path": str(canonical_root),
            "canonical_schema_digest": canonical_schema_digest,
            "source_instance_domain": source_instance_domain,
            "fsync_contract": "file_and_parent_directory",
            "source_instance_nonce": nonce,
            "dataset_identity": dataset_identity,
            "created_at": created_at or _utc_now(),
        }
        values["source_instance_sha256"] = domain_sha256(
            "stock-eva/r2f4.3/source-instance-record/v2", values
        )
        record = SourceInstanceRecord.model_validate(values)
        record.verify_hash()
        record.verify_identity()
        payload = canonical_json_bytes(record.model_dump(mode="json"))
        _install_no_replace(self.path, payload)
        return self.read(canonical_root_path=canonical_root)

    def read(
        self,
        *,
        canonical_root_path: Path | None = None,
        canonical_schema_digest: str | None = None,
    ) -> SourceInstanceRecord:
        _safe_parent(self.path.parent)
        payload, _ = _read_nofollow(self.path)
        try:
            record = SourceInstanceRecord.model_validate(json.loads(payload))
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ReplicationDurabilityError("source instance record is invalid") from exc
        if canonical_json_bytes(record.model_dump(mode="json")) != payload:
            raise ReplicationDurabilityError("source instance record is not canonical")
        record.verify_hash()
        record.verify_identity()
        expected_path = _physical_path(
            Path(record.canonical_root_path) / "_replication" / "source-instance.json"
        )
        if _physical_path(self.path) != expected_path:
            raise ReplicationDurabilityError("source instance path is outside canonical layout")
        if canonical_root_path is not None:
            if not canonical_root_path.is_absolute():
                raise ReplicationDurabilityError("canonical root must be an absolute local root")
            expected_root = str(_physical_path(canonical_root_path))
            if record.canonical_root_path != expected_root:
                raise ReplicationDurabilityError("source instance canonical root mismatch")
        if (
            canonical_schema_digest is not None
            and record.canonical_schema_digest != canonical_schema_digest
        ):
            raise ReplicationDurabilityError("source instance schema digest mismatch")
        return record


class ObjectInventoryEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    relative_path: str = Field(min_length=1)
    object_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)
    row_count: int = Field(ge=0)
    trade_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    source: str = Field(min_length=1, max_length=128)

    _trade_date_is_iso = field_validator("trade_date")(
        lambda value: _validate_iso_date(value, "trade_date")
    )

    @field_validator("relative_path")
    @classmethod
    def relative_path_is_safe(cls, value: str) -> str:
        path = Path(value)
        if (
            path.is_absolute()
            or "\\" in value
            or any(component in {"", ".", ".."} for component in value.split("/"))
            or any(component in _DESTINATION_CONTROL_NAMES for component in path.parts)
        ):
            raise ValueError("relative_path must be a safe relative path")
        return value


def _object_inventory_projection(
    inventory: Iterable[ObjectInventoryEntry | Mapping[str, object]],
) -> list[dict[str, object]]:
    try:
        entries = tuple(
            item
            if isinstance(item, ObjectInventoryEntry)
            else ObjectInventoryEntry.model_validate(item)
            for item in inventory
        )
    except (TypeError, ValueError) as exc:
        raise ReplicationDurabilityError("source checkpoint inventory is invalid") from exc
    keys = tuple((item.relative_path, item.object_sha256) for item in entries)
    if tuple(sorted(keys)) != keys or len({item.relative_path for item in entries}) != len(entries):
        raise ReplicationDurabilityError("source checkpoint inventory is not sorted and unique")
    return [item.model_dump(mode="json") for item in entries]


class SourceCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    checkpoint_schema: Literal["stock-eva/r2f4.3/source-checkpoint/v1"] = (
        "stock-eva/r2f4.3/source-checkpoint/v1"
    )
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    publication_binding_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    pointer_row_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    pointer_generation: str = Field(min_length=1, max_length=128)
    source_run_id: str = Field(min_length=1, max_length=128)
    source_trade_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    source_published_at: str = Field(min_length=20)
    pointer_db_device: int = Field(gt=0)
    pointer_db_inode: int = Field(gt=0)
    pointer_db_schema_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    manifest_canonical_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_manifest_bytes_sha256: str = Field(
        min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    source_object_set_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    object_inventory: tuple[ObjectInventoryEntry, ...]
    checkpoint_payload_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    _trade_date_is_iso = field_validator("source_trade_date")(
        lambda value: _validate_iso_date(value, "source_trade_date")
    )
    _published_at_is_utc = field_validator("source_published_at")(
        lambda value: _validate_utc_timestamp(value, "source_published_at")
    )

    @model_validator(mode="before")
    @classmethod
    def closed_projection(cls, value: object) -> object:
        if isinstance(value, Mapping) and set(value) != set(cls.model_fields):
            raise ValueError("source checkpoint projection fields are not exact")
        return value

    def hash_preimage(self) -> dict[str, object]:
        value = self.model_dump(mode="json")
        value.pop("checkpoint_schema")
        value.pop("checkpoint_id")
        value.pop("checkpoint_payload_sha256")
        return value

    def verify_hashes(self) -> None:
        if self.checkpoint_schema != "stock-eva/r2f4.3/source-checkpoint/v1":
            raise ReplicationDurabilityError("source checkpoint schema is invalid")
        inventory_projection = _object_inventory_projection(self.object_inventory)
        expected_object_set = domain_sha256(SOURCE_OBJECT_SET_DOMAIN, inventory_projection)
        if self.source_object_set_sha256 != expected_object_set:
            raise ReplicationDurabilityError("source object set digest does not match inventory")
        expected_id = domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1", self.hash_preimage())
        if expected_id != self.checkpoint_id:
            raise ReplicationDurabilityError("source checkpoint identity is invalid")
        payload = self.hash_preimage() | {"checkpoint_id": self.checkpoint_id}
        expected_payload = domain_sha256("stock-eva/r2f4.3/source-checkpoint-payload/v1", payload)
        if expected_payload != self.checkpoint_payload_sha256:
            raise ReplicationDurabilityError("source checkpoint payload hash is invalid")


def build_source_checkpoint(
    *,
    source_instance_id: str,
    source_instance_sha256: str,
    publication_binding_sha256: str,
    pointer_row_sha256: str,
    pointer_generation: str,
    source_run_id: str,
    source_trade_date: str,
    source_published_at: str,
    pointer_db_device: int,
    pointer_db_inode: int,
    pointer_db_schema_digest: str,
    manifest_canonical_sha256: str,
    source_manifest_bytes_sha256: str,
    source_object_set_sha256: str,
    object_inventory: Iterable[ObjectInventoryEntry | Mapping[str, object]],
) -> SourceCheckpoint:
    inventory_projection = _object_inventory_projection(object_inventory)
    expected_object_set = domain_sha256(SOURCE_OBJECT_SET_DOMAIN, inventory_projection)
    if source_object_set_sha256 != expected_object_set:
        raise ReplicationDurabilityError("source object set digest does not match inventory")
    values: dict[str, object] = {
        "checkpoint_schema": "stock-eva/r2f4.3/source-checkpoint/v1",
        "source_instance_id": source_instance_id,
        "source_instance_sha256": source_instance_sha256,
        "publication_binding_sha256": publication_binding_sha256,
        "pointer_row_sha256": pointer_row_sha256,
        "pointer_generation": pointer_generation,
        "source_run_id": source_run_id,
        "source_trade_date": source_trade_date,
        "source_published_at": source_published_at,
        "pointer_db_device": pointer_db_device,
        "pointer_db_inode": pointer_db_inode,
        "pointer_db_schema_digest": pointer_db_schema_digest,
        "manifest_canonical_sha256": manifest_canonical_sha256,
        "source_manifest_bytes_sha256": source_manifest_bytes_sha256,
        "source_object_set_sha256": source_object_set_sha256,
        "object_inventory": tuple(inventory_projection),
    }
    checkpoint_preimage = dict(values)
    checkpoint_preimage.pop("checkpoint_schema")
    checkpoint_id = domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1", checkpoint_preimage)
    values["checkpoint_id"] = checkpoint_id
    payload_preimage = dict(checkpoint_preimage)
    payload_preimage["checkpoint_id"] = checkpoint_id
    values["checkpoint_payload_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/source-checkpoint-payload/v1", payload_preimage
    )
    checkpoint = SourceCheckpoint.model_validate(values)
    checkpoint.verify_hashes()
    return checkpoint


class JournalRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    journal_schema_version: Literal[1] = 1
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_published_at: str = Field(min_length=20)
    checkpoint_projection: SourceCheckpoint
    publication_binding_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    checkpoint_payload_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    created_at: str = Field(min_length=20)
    journal_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    _source_published_at_is_utc = field_validator("source_published_at")(
        lambda value: _validate_utc_timestamp(value, "source_published_at")
    )
    _created_at_is_utc = field_validator("created_at")(
        lambda value: _validate_utc_timestamp(value, "created_at")
    )

    @model_validator(mode="before")
    @classmethod
    def closed_projection(cls, value: object) -> object:
        if isinstance(value, Mapping) and set(value) != set(cls.model_fields):
            raise ValueError("replication journal fields are not exact")
        return value

    @model_validator(mode="after")
    def checkpoint_projection_is_bound(self) -> JournalRecord:
        checkpoint = self.checkpoint_projection
        if (
            checkpoint.checkpoint_id != self.checkpoint_id
            or checkpoint.source_instance_id != self.source_instance_id
            or checkpoint.source_instance_sha256 != self.source_instance_sha256
            or checkpoint.publication_binding_sha256 != self.publication_binding_sha256
            or checkpoint.source_published_at != self.source_published_at
            or checkpoint.checkpoint_payload_sha256 != self.checkpoint_payload_sha256
        ):
            raise ValueError("journal checkpoint projection is not bound")
        return self

    def hash_preimage(self) -> dict[str, object]:
        return {
            "journal_schema_version": self.journal_schema_version,
            "checkpoint_id": self.checkpoint_id,
            "source_instance_id": self.source_instance_id,
            "source_instance_sha256": self.source_instance_sha256,
            "publication_binding_sha256": self.publication_binding_sha256,
            "checkpoint_payload_sha256": self.checkpoint_payload_sha256,
            "source_published_at": self.source_published_at,
            "created_at": self.created_at,
        }

    def verify_hash(self) -> None:
        expected = domain_sha256("stock-eva/r2f4.3/replication-journal/v1", self.hash_preimage())
        if expected != self.journal_sha256:
            raise ReplicationDurabilityError("replication journal hash is invalid")

    def install(self, root: Path) -> Path:
        # ``model_copy(update=...)`` intentionally bypasses Pydantic
        # validators.  Re-validate the closed nested projection at the write
        # boundary so an in-memory mutation cannot add a path, payload or
        # other journal-only field while retaining the outer journal hash.
        try:
            values = self.model_dump(mode="json", warnings="error")
        except Exception as exc:
            raise ReplicationDurabilityError("replication journal projection is invalid") from exc
        projection = dict(values["checkpoint_projection"])
        projection["object_inventory"] = tuple(projection.get("object_inventory", ()))
        values["checkpoint_projection"] = projection
        try:
            record = type(self).model_validate(values)
        except (TypeError, ValueError) as exc:
            raise ReplicationDurabilityError("replication journal projection is invalid") from exc
        if record != self:
            raise ReplicationDurabilityError("replication journal projection is not immutable")
        record.checkpoint_projection.verify_hashes()
        record.verify_hash()
        path = Path(root) / f"{self.checkpoint_id}.json"
        _install_no_replace(path, canonical_json_bytes(record.model_dump(mode="json")))
        return path

    @classmethod
    def read(cls, path: Path) -> Self:
        payload, _ = _read_nofollow(Path(path))
        return cls._from_payload(Path(path).name, payload)

    @classmethod
    def _from_payload(cls, filename: str, payload: bytes) -> Self:
        try:
            values = json.loads(payload)
            projection = dict(values["checkpoint_projection"])
            projection["object_inventory"] = tuple(projection.get("object_inventory", ()))
            values["checkpoint_projection"] = projection
            record = cls.model_validate(values)
        except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ReplicationDurabilityError("replication journal is invalid") from exc
        if filename != f"{record.checkpoint_id}.json":
            raise ReplicationDurabilityError("replication journal filename is invalid")
        if canonical_json_bytes(record.model_dump(mode="json")) != payload:
            raise ReplicationDurabilityError("replication journal is not canonical")
        record.checkpoint_projection.verify_hashes()
        record.verify_hash()
        return record


def build_journal_record(
    *,
    checkpoint_id: str,
    source_instance_id: str,
    source_instance_sha256: str,
    publication_binding_sha256: str,
    source_published_at: str,
    checkpoint_projection: SourceCheckpoint | Mapping[str, object],
    created_at: str | None = None,
) -> JournalRecord:
    try:
        checkpoint = (
            checkpoint_projection
            if isinstance(checkpoint_projection, SourceCheckpoint)
            else SourceCheckpoint.model_validate(
                {
                    **checkpoint_projection,
                    "object_inventory": tuple(checkpoint_projection.get("object_inventory", ())),
                }
            )
        )
    except (TypeError, ValueError) as exc:
        raise ReplicationDurabilityError("journal checkpoint projection is invalid") from exc
    checkpoint.verify_hashes()
    checkpoint_payload_sha256 = checkpoint.checkpoint_payload_sha256
    values: dict[str, object] = {
        "journal_schema_version": JOURNAL_SCHEMA_VERSION,
        "checkpoint_id": checkpoint_id,
        "source_instance_id": source_instance_id,
        "source_instance_sha256": source_instance_sha256,
        "source_published_at": source_published_at,
        "checkpoint_projection": checkpoint,
        "publication_binding_sha256": publication_binding_sha256,
        "checkpoint_payload_sha256": checkpoint_payload_sha256,
        "created_at": created_at or _utc_now(),
    }
    values["journal_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/replication-journal/v1",
        {
            "journal_schema_version": values["journal_schema_version"],
            "checkpoint_id": values["checkpoint_id"],
            "source_instance_id": values["source_instance_id"],
            "source_instance_sha256": values["source_instance_sha256"],
            "publication_binding_sha256": values["publication_binding_sha256"],
            "checkpoint_payload_sha256": values["checkpoint_payload_sha256"],
            "source_published_at": values["source_published_at"],
            "created_at": values["created_at"],
        },
    )
    record = JournalRecord.model_validate(values)
    record.verify_hash()
    return record


def _validate_sqlite_schema(connection: sqlite3.Connection) -> None:
    user_version = connection.execute("PRAGMA user_version").fetchone()
    if user_version != (1,):
        raise ReplicationStateUnavailable("replication sidecar user version is invalid")
    actual = set(
        connection.execute(
            """SELECT type, name, tbl_name, sql
                 FROM sqlite_master
                WHERE type IN ('table', 'index', 'trigger')"""
        ).fetchall()
    )
    if actual != _expected_sqlite_objects():
        raise ReplicationStateUnavailable("replication sidecar DDL is invalid")


def _event_hash_preimage(row: tuple[object, ...]) -> dict[str, object]:
    keys = (
        "event_id",
        "intent_id",
        "event_sequence",
        "prev_event_sha256",
        "event_type",
        "from_state",
        "to_state",
        "attempt",
        "reason_code",
        "state_version",
        "occurred_at",
        "destination_replication_generation",
        "destination_record_sha256",
        "destination_head_sha256",
    )
    return dict(zip(keys, row, strict=True))


def _validate_intent_rows(
    connection: sqlite3.Connection,
    *,
    expected_source_instance_id: str | None = None,
    expected_source_instance_sha256: str | None = None,
) -> set[str]:
    """Validate the immutable intent projection before replaying events."""
    columns = (
        "intent_id",
        "schema_version",
        "operation_day",
        "direction",
        "destination_id",
        "pointer_row_sha256",
        "pointer_generation",
        "source_run_id",
        "source_trade_date",
        "source_published_at",
        "pointer_db_device",
        "pointer_db_inode",
        "pointer_db_schema_digest",
        "manifest_canonical_sha256",
        "source_object_set_sha256",
        "publication_binding_sha256",
        "source_instance_id",
        "source_instance_sha256",
        "source_sequence",
        "checkpoint_id",
        "source_manifest_bytes_sha256",
        "plan_sha256",
        "intent_sha256",
        "object_count",
        "row_count",
        "byte_count",
        "created_at",
    )
    rows = connection.execute(
        "SELECT " + ", ".join(columns) + " FROM replication_intents ORDER BY intent_id"
    ).fetchall()
    intent_ids: set[str] = set()
    for row in rows:
        values = dict(zip(columns, row, strict=True))
        intent_id = values["intent_id"]
        if not isinstance(intent_id, str):
            raise ReplicationStateUnavailable("replication intent identity is invalid")
        _validate_sha(intent_id, "intent_id")
        if values["schema_version"] != 1 or values["direction"] != "local_to_nas":
            raise ReplicationStateUnavailable("replication intent schema is invalid")
        if expected_source_instance_id is not None and (
            values["source_instance_id"] != expected_source_instance_id
            or values["source_instance_sha256"] != expected_source_instance_sha256
        ):
            raise ReplicationStateUnavailable("replication intent source identity is invalid")
        if not isinstance(values["destination_id"], str) or len(values["destination_id"]) != 32:
            raise ReplicationStateUnavailable("replication intent destination is invalid")
        if not isinstance(values["pointer_generation"], str) or not (
            1 <= len(values["pointer_generation"]) <= 128
        ):
            raise ReplicationStateUnavailable("replication intent pointer generation is invalid")
        if not isinstance(values["source_run_id"], str) or not (
            1 <= len(values["source_run_id"]) <= 128
        ):
            raise ReplicationStateUnavailable("replication intent run id is invalid")
        for field in ("operation_day", "source_trade_date"):
            try:
                _validate_iso_date(values[field], field)
            except (TypeError, ValueError) as exc:
                raise ReplicationStateUnavailable("replication intent date is invalid") from exc
        for field in ("source_published_at", "created_at"):
            try:
                _validate_utc_timestamp(values[field], field)
            except (TypeError, ValueError) as exc:
                raise ReplicationStateUnavailable(
                    "replication intent timestamp is invalid"
                ) from exc
        for field in (
            "pointer_row_sha256",
            "pointer_db_schema_digest",
            "manifest_canonical_sha256",
            "source_object_set_sha256",
            "publication_binding_sha256",
            "source_instance_id",
            "source_instance_sha256",
            "checkpoint_id",
            "source_manifest_bytes_sha256",
            "plan_sha256",
            "intent_sha256",
        ):
            try:
                _validate_sha(values[field], field)
            except (TypeError, ValueError) as exc:
                raise ReplicationStateUnavailable("replication intent digest is invalid") from exc
        for field in (
            "pointer_db_device",
            "pointer_db_inode",
            "source_sequence",
            "object_count",
            "row_count",
            "byte_count",
        ):
            if not isinstance(values[field], int) or values[field] < (
                1 if field in {"pointer_db_device", "pointer_db_inode", "source_sequence"} else 0
            ):
                raise ReplicationStateUnavailable("replication intent numeric field is invalid")
        expected_id = domain_sha256(
            "stock-eva/r2f4.3/replication-intent/v1",
            {
                "direction": values["direction"],
                "destination_id": values["destination_id"],
                "source_instance_id": values["source_instance_id"],
                "source_sequence": values["source_sequence"],
                "checkpoint_id": values["checkpoint_id"],
                "plan_sha256": values["plan_sha256"],
            },
        )
        if intent_id != expected_id:
            raise ReplicationStateUnavailable("replication intent identity is invalid")
        # operation_day is scheduling metadata and is deliberately excluded
        # from the immutable row digest.  This is what permits a restart or a
        # later scheduler slot to observe the same intent without changing
        # its identity or audit chain.
        intent_preimage = {
            field: values[field]
            for field in (
                "intent_id",
                "schema_version",
                "direction",
                "destination_id",
                "checkpoint_id",
                "pointer_row_sha256",
                "pointer_generation",
                "source_run_id",
                "source_trade_date",
                "source_published_at",
                "pointer_db_inode",
                "pointer_db_device",
                "pointer_db_schema_digest",
                "manifest_canonical_sha256",
                "source_object_set_sha256",
                "source_instance_id",
                "source_instance_sha256",
                "source_sequence",
                "source_manifest_bytes_sha256",
                "publication_binding_sha256",
                "plan_sha256",
                "object_count",
                "row_count",
                "byte_count",
                "created_at",
            )
        }
        expected_hash = domain_sha256("stock-eva/r2f4.3/replication-intent-row/v1", intent_preimage)
        if values["intent_sha256"] != expected_hash:
            raise ReplicationStateUnavailable("replication intent hash is invalid")
        intent_ids.add(intent_id)
    return intent_ids


def _validate_event_reachability(
    connection: sqlite3.Connection,
    *,
    expected_source_instance_id: str | None = None,
    expected_source_instance_sha256: str | None = None,
) -> None:
    intent_ids = _validate_intent_rows(
        connection,
        expected_source_instance_id=expected_source_instance_id,
        expected_source_instance_sha256=expected_source_instance_sha256,
    )
    event_rows = connection.execute(
        """SELECT event_id, intent_id, event_sequence, prev_event_sha256, event_type,
                  from_state, to_state, attempt, reason_code, state_version, occurred_at,
                  destination_replication_generation, destination_record_sha256,
                  destination_head_sha256, event_sha256
             FROM replication_attempt_events
            ORDER BY intent_id, event_sequence"""
    ).fetchall()
    head_rows = {
        row[0]: row
        for row in connection.execute(
            """SELECT intent_id, current_state, state_version, last_event_sequence,
                      lease_owner, lease_until, next_attempt_at, last_reason_code,
                      updated_at
                 FROM replication_heads"""
        )
    }
    events_by_intent: dict[str, list[tuple[object, ...]]] = {}
    for row in event_rows:
        events_by_intent.setdefault(row[1], []).append(row)
    if set(events_by_intent) != intent_ids or set(head_rows) != intent_ids:
        raise ReplicationStateUnavailable("replication event reachability is incomplete")
    allowed = {
        ("pending", "copying"),
        ("copying", "verifying"),
        ("verifying", "replicated"),
        ("pending", "retry_wait"),
        ("copying", "retry_wait"),
        ("verifying", "retry_wait"),
        ("retry_wait", "pending"),
        ("pending", "dead_letter"),
        ("copying", "dead_letter"),
        ("verifying", "dead_letter"),
    }
    for intent_id, rows in events_by_intent.items():
        if not rows or [row[2] for row in rows] != list(range(len(rows))):
            raise ReplicationStateUnavailable("replication event sequence has a gap")
        prior_state: str | None = None
        prior_sha = ZERO_SHA256
        prior_version = -1
        for index, row in enumerate(rows):
            (
                event_id,
                _row_intent_id,
                event_sequence,
                prev_event_sha256,
                event_type,
                from_state,
                to_state,
                attempt,
                reason_code,
                state_version,
                occurred_at,
                destination_replication_generation,
                destination_record_sha256,
                destination_head_sha256,
                event_sha256,
            ) = row
            if event_sequence != index or prev_event_sha256 != prior_sha:
                raise ReplicationStateUnavailable("replication event hash chain is broken")
            if not isinstance(event_id, str) or not isinstance(intent_id, str):
                raise ReplicationStateUnavailable("replication event identity is invalid")
            for digest, name in (
                (event_id, "event_id"),
                (prev_event_sha256, "prev_event_sha256"),
                (event_sha256, "event_sha256"),
            ):
                try:
                    _validate_sha(digest, name)
                except (TypeError, ValueError) as exc:
                    raise ReplicationStateUnavailable(
                        "replication event digest is invalid"
                    ) from exc
            if reason_code not in set(get_args(ReplicationReason)):
                raise ReplicationStateUnavailable("replication event reason is invalid")
            try:
                _validate_utc_timestamp(occurred_at, "occurred_at")
            except (TypeError, ValueError) as exc:
                raise ReplicationStateUnavailable("replication event timestamp is invalid") from exc
            expected_event_id = domain_sha256(
                "stock-eva/r2f4.3/replication-event-id/v1",
                {
                    "intent_id": intent_id,
                    "event_sequence": event_sequence,
                    "attempt": attempt,
                    "state_version": state_version,
                    "occurred_at": occurred_at,
                },
            )
            if event_id != expected_event_id:
                raise ReplicationStateUnavailable("replication event id is invalid")
            expected_event_sha = domain_sha256(
                "stock-eva/r2f4.3/replication-event/v1",
                _event_hash_preimage(row[:-1]),
            )
            if event_sha256 != expected_event_sha:
                raise ReplicationStateUnavailable("replication event hash is invalid")
            if event_type not in {"intent_created", "claim", "transition", "attempt", "terminal"}:
                raise ReplicationStateUnavailable("replication event type is invalid")
            if not isinstance(to_state, str) or not isinstance(state_version, int):
                raise ReplicationStateUnavailable("replication event state is invalid")
            if state_version < 0 or not isinstance(attempt, int) or not 0 <= attempt <= 6:
                raise ReplicationStateUnavailable("replication event counters are invalid")
            for digest, name in (
                (destination_replication_generation, "destination_replication_generation"),
                (destination_record_sha256, "destination_record_sha256"),
                (destination_head_sha256, "destination_head_sha256"),
            ):
                if digest is not None:
                    try:
                        _validate_sha(digest, name)
                    except (TypeError, ValueError) as exc:
                        raise ReplicationStateUnavailable(
                            "replication event destination digest is invalid"
                        ) from exc
            if index == 0:
                if (
                    event_type != "intent_created"
                    or from_state is not None
                    or to_state != "pending"
                    or attempt != 0
                    or state_version != 0
                ):
                    raise ReplicationStateUnavailable("replication event genesis is invalid")
            elif (
                from_state != prior_state
                or (from_state, to_state) not in allowed
                or state_version != prior_version + 1
            ):
                raise ReplicationStateUnavailable("replication event transition is invalid")
            prior_state = to_state
            prior_version = state_version
            prior_sha = event_sha256
        head = head_rows[intent_id]
        if head[1] not in {
            "pending",
            "copying",
            "verifying",
            "retry_wait",
            "replicated",
            "dead_letter",
        }:
            raise ReplicationStateUnavailable("replication head state is invalid")
        if (
            not isinstance(head[2], int)
            or not isinstance(head[3], int)
            or head[2] < 0
            or head[3] < 0
        ):
            raise ReplicationStateUnavailable("replication head counters are invalid")
        lease_expected = head[1] in {"copying", "verifying"}
        if lease_expected != (head[4] is not None and head[5] is not None):
            raise ReplicationStateUnavailable("replication head lease is invalid")
        if not lease_expected and (head[4] is not None or head[5] is not None):
            raise ReplicationStateUnavailable("replication head lease is stale")
        if lease_expected and (not isinstance(head[4], str) or not head[4] or head[5] is None):
            raise ReplicationStateUnavailable("replication head lease is invalid")
        for timestamp, name in (
            (head[5], "lease_until"),
            (head[6], "next_attempt_at"),
            (head[8], "updated_at"),
        ):
            if timestamp is not None:
                try:
                    _validate_utc_timestamp(timestamp, name)
                except (TypeError, ValueError) as exc:
                    raise ReplicationStateUnavailable(
                        "replication head timestamp is invalid"
                    ) from exc
        if head[7] not in set(get_args(ReplicationReason)):
            raise ReplicationStateUnavailable("replication head reason is invalid")
        final_reason = rows[-1][8]
        # Event rows include state_version at index 9; occurred_at is index
        # 10.  Comparing the wrong slot would reject every valid head as a
        # stale replay projection as soon as an intent is imported.
        final_occurred_at = rows[-1][10]
        if head[7] != final_reason or head[8] != final_occurred_at:
            raise ReplicationStateUnavailable("replication head replay projection is stale")
        if head[1] != prior_state or head[2] != prior_version or head[3] != len(rows) - 1:
            raise ReplicationStateUnavailable("replication head does not replay events")


def _stat_fingerprint(
    value: os.stat_result | None,
    payload: bytes | None = None,
) -> tuple[int, int, int, int, str | None] | None:
    if value is None:
        return None
    digest = hashlib.sha256(payload).hexdigest() if payload is not None else None
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, digest)


_SqliteFingerprint = tuple[int, int, int, int, str | None]


def _deserialize_sqlite_bytes(payload: bytes) -> sqlite3.Connection:
    """Deserialize one private SQLite image and close it on every failure."""
    if len(payload) < 20 or payload[:16] != b"SQLite format 3\x00":
        raise ReplicationStateUnavailable("replication sidecar header is invalid")
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(":memory:")
        # A database checkpointed from WAL mode retains the WAL header bits.
        # Rebind those two bytes only in the private image; the source bytes
        # are never modified and no WAL/SHM is opened or created.
        private_payload = bytearray(payload)
        private_payload[18:20] = b"\x01\x01"
        connection.deserialize(bytes(private_payload), name="main")
        return connection
    except ReplicationStateUnavailable as exc:
        if connection is not None:
            cleanup_failures: list[str] = []
            _close_connection_best_effort(connection, cleanup_failures)
            if cleanup_failures:
                exc.add_note("replication_cleanup=failed")
        raise
    except BaseException as exc:
        if connection is not None:
            cleanup_failures = []
            _close_connection_best_effort(connection, cleanup_failures)
            if cleanup_failures:
                exc.add_note("replication_cleanup=failed")
        if isinstance(exc, ReplicationStateUnavailable):
            raise
        raise ReplicationStateUnavailable(
            "replication sidecar in-memory deserialize failed"
        ) from exc


class _WriterLockToken:
    """Descriptor-bound proof for the fixed immutable-sidecar lock inode."""

    __slots__ = (
        "lock_dev",
        "lock_ino",
        "lock_nlink",
        "lock_mode",
        "parent_dev",
        "parent_ino",
        "released",
    )

    def __init__(self, lock_info: os.stat_result, parent_info: os.stat_result) -> None:
        self.lock_dev = lock_info.st_dev
        self.lock_ino = lock_info.st_ino
        self.lock_nlink = lock_info.st_nlink
        self.lock_mode = stat.S_IMODE(lock_info.st_mode)
        self.parent_dev = parent_info.st_dev
        self.parent_ino = parent_info.st_ino
        self.released = False


def _acquire_writer_lock(parent_fd: int, lock_name: str) -> tuple[int, _WriterLockToken]:
    lock_fd: int | None = None
    try:
        lock_fd = os.open(
            lock_name,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        before = os.fstat(lock_fd)
        if not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o600:
            raise ReplicationDurabilityError("replication writer lock is unsafe")
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        after = os.fstat(lock_fd)
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise ReplicationDurabilityError("replication writer lock changed during acquire")
        token = _WriterLockToken(after, os.fstat(parent_fd))
        _verify_writer_lock_entry(parent_fd, lock_name, lock_fd, token)
        return lock_fd, token
    except ReplicationDurabilityError as exc:
        cleanup_failures: list[str] = []
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                cleanup_failures.append("lock_unlock")
            _close_fd_best_effort(lock_fd, "lock_fd", cleanup_failures)
        if cleanup_failures:
            exc.add_note("replication_cleanup=failed")
        raise
    except OSError as exc:
        cleanup_failures = []
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                cleanup_failures.append("lock_unlock")
            _close_fd_best_effort(lock_fd, "lock_fd", cleanup_failures)
        if cleanup_failures:
            exc.add_note("replication_cleanup=failed")
        raise ReplicationDurabilityError("replication writer lock is unavailable") from exc


def _release_writer_lock(
    lock_fd: int, token: _WriterLockToken, failures: list[str] | None = None
) -> list[str]:
    collected = failures if failures is not None else []
    if token.released:
        return collected
    token.released = True
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except OSError:
        collected.append("lock_unlock")
    _close_fd_best_effort(lock_fd, "lock_fd", collected)
    return collected


def _verify_writer_lock_entry(
    parent_fd: int, lock_name: str, lock_fd: int, token: _WriterLockToken
) -> None:
    entry_fd: int | None = None
    primary_error: BaseException | None = None
    try:
        parent_info = os.fstat(parent_fd)
        if (parent_info.st_dev, parent_info.st_ino) != (token.parent_dev, token.parent_ino):
            raise ReplicationDurabilityError("replication writer parent descriptor changed")
        held = os.fstat(lock_fd)
        held_identity = (held.st_dev, held.st_ino, held.st_nlink, stat.S_IMODE(held.st_mode))
        token_identity = (token.lock_dev, token.lock_ino, token.lock_nlink, token.lock_mode)
        if held_identity != token_identity:
            raise ReplicationDurabilityError("replication writer lock identity changed")
        entry_fd = os.open(lock_name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent_fd)
        entry = os.fstat(entry_fd)
        if (
            entry.st_dev,
            entry.st_ino,
            entry.st_nlink,
            stat.S_IMODE(entry.st_mode),
        ) != token_identity:
            raise ReplicationDurabilityError("replication writer lock entry changed")
    except ReplicationDurabilityError as exc:
        primary_error = exc
        raise
    except OSError as exc:
        primary_error = ReplicationDurabilityError("replication writer lock entry is unavailable")
        raise primary_error from exc
    finally:
        if entry_fd is not None:
            cleanup_failures: list[str] = []
            _close_fd_best_effort(entry_fd, "lock_entry_fd", cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)


# Batch1.6 replaces the mutable prototype above with an immutable generation
# namespace.  These helpers intentionally use only a held sidecar dirfd for
# authority; a generation is never opened for writing after installation.
_GENERATION_RE = re.compile(r"^[0-9]{20}\.db$")
_STAGING_TEMP_RE = re.compile(r"^\.([0-9]{20}\.db)\.tmp$")


def _sidecar_root(path: Path) -> Path:
    path = _physical_path(Path(path))
    if not path.is_absolute():
        raise ReplicationDurabilityError("replication sidecar root must be absolute")
    return path


def _generation_filename(sequence: int) -> str:
    if sequence < 0 or sequence >= 10**SIDECAR_GENERATION_WIDTH:
        raise ReplicationDurabilityError("replication generation sequence is invalid")
    return f"{sequence:0{SIDECAR_GENERATION_WIDTH}d}{SIDECAR_GENERATION_SUFFIX}"


def _generation_lock_identity(info: os.stat_result) -> dict[str, int]:
    return {
        "lock_dev": int(info.st_dev),
        "lock_ino": int(info.st_ino),
        "lock_nlink": int(info.st_nlink),
        "lock_mode": int(stat.S_IMODE(info.st_mode)),
    }


def _genesis_preimage(values: Mapping[str, object]) -> dict[str, object]:
    return {key: values[key] for key in values if key != "genesis_sha256"}


def _build_genesis(
    *,
    source_instance_id: str,
    source_instance_sha256: str,
    lock_info: os.stat_result,
    created_at: str,
) -> dict[str, object]:
    _validate_sha(source_instance_id, "source_instance_id")
    _validate_sha(source_instance_sha256, "source_instance_sha256")
    _validate_utc_timestamp(created_at, "created_at")
    values: dict[str, object] = {
        "genesis_schema": "stock-eva/r2f4.3/replication-sidecar-genesis/v1",
        "schema_version": 1,
        "sidecar_schema_identity": SIDECAR_SCHEMA_IDENTITY,
        "sidecar_schema_digest": SIDECAR_SCHEMA_DIGEST,
        "source_instance_id": source_instance_id,
        "source_instance_sha256": source_instance_sha256,
        "lock_dev": int(lock_info.st_dev),
        "lock_ino": int(lock_info.st_ino),
        "lock_nlink": int(lock_info.st_nlink),
        "lock_mode": int(stat.S_IMODE(lock_info.st_mode)),
        "created_at": created_at,
    }
    values["genesis_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/replication-sidecar-genesis/v1", _genesis_preimage(values)
    )
    return values


def _validate_genesis(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ReplicationStateUnavailable("replication sidecar genesis is invalid") from exc
    expected = {
        "genesis_schema",
        "schema_version",
        "sidecar_schema_identity",
        "sidecar_schema_digest",
        "source_instance_id",
        "source_instance_sha256",
        "lock_dev",
        "lock_ino",
        "lock_nlink",
        "lock_mode",
        "created_at",
        "genesis_sha256",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise ReplicationStateUnavailable("replication sidecar genesis fields are invalid")
    if (
        value["genesis_schema"] != "stock-eva/r2f4.3/replication-sidecar-genesis/v1"
        or value["schema_version"] != 1
        or value["sidecar_schema_identity"] != SIDECAR_SCHEMA_IDENTITY
        or value["sidecar_schema_digest"] != SIDECAR_SCHEMA_DIGEST
    ):
        raise ReplicationStateUnavailable("replication sidecar genesis identity is invalid")
    for field in ("source_instance_id", "source_instance_sha256", "sidecar_schema_digest"):
        try:
            _validate_sha(value[field], field)
        except (TypeError, ValueError) as exc:
            raise ReplicationStateUnavailable(
                "replication sidecar genesis digest is invalid"
            ) from exc
    for field in ("lock_dev", "lock_ino", "lock_nlink"):
        if not isinstance(value[field], int) or value[field] <= 0:
            raise ReplicationStateUnavailable(
                "replication sidecar genesis lock identity is invalid"
            )
    if value["lock_mode"] != 0o600:
        raise ReplicationStateUnavailable("replication sidecar genesis lock mode is invalid")
    try:
        _validate_utc_timestamp(value["created_at"], "created_at")
        _validate_sha(value["genesis_sha256"], "genesis_sha256")
    except (TypeError, ValueError) as exc:
        raise ReplicationStateUnavailable("replication sidecar genesis digest is invalid") from exc
    if (
        domain_sha256("stock-eva/r2f4.3/replication-sidecar-genesis/v1", _genesis_preimage(value))
        != value["genesis_sha256"]
    ):
        raise ReplicationStateUnavailable("replication sidecar genesis hash is invalid")
    if canonical_json_bytes(value) != payload:
        raise ReplicationStateUnavailable("replication sidecar genesis is not canonical")
    return value


def _read_at(parent_fd: int, name: str) -> tuple[bytes, os.stat_result]:
    fd: int | None = None
    primary: BaseException | None = None
    try:
        fd = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=parent_fd)
        return _read_descriptor(fd)
    except FileNotFoundError as exc:
        primary = ReplicationStateUnavailable("replication sidecar entry disappeared")
        raise primary from exc
    except OSError as exc:
        primary = ReplicationStateUnavailable("replication sidecar entry is unsafe")
        raise primary from exc
    except BaseException as exc:
        primary = exc
        raise
    finally:
        failures: list[str] = []
        if fd is not None:
            _close_fd_best_effort(fd, "sidecar_entry_fd", failures)
        _finish_cleanup(primary, failures)


@dataclass(frozen=True)
class _GenerationEntry:
    sequence: int
    name: str
    payload: bytes
    stat: os.stat_result
    fingerprint: _SqliteFingerprint


def _directory_fingerprint(info: os.stat_result) -> tuple[int, int, int, int]:
    if not stat.S_ISDIR(info.st_mode):
        raise ReplicationDurabilityError("replication directory is not a directory")
    return (
        int(info.st_dev),
        int(info.st_ino),
        int(stat.S_IMODE(info.st_mode)),
        int(info.st_nlink),
    )


def _assert_directory_fingerprint(fd: int, expected: tuple[int, int, int, int]) -> None:
    try:
        actual = _directory_fingerprint(os.fstat(fd))
    except OSError as exc:
        raise ReplicationStateUnavailable("replication directory changed during read") from exc
    if actual != expected:
        raise ReplicationStateUnavailable("replication directory changed during read")


def _ensure_staging_directory(root_fd: int) -> int:
    """Open the fixed staging namespace through the already-held root dirfd."""
    try:
        staging_fd = os.open(SIDECAR_STAGING_NAME, _DIRECTORY_FLAGS, dir_fd=root_fd)
    except FileNotFoundError:
        try:
            os.mkdir(SIDECAR_STAGING_NAME, mode=0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        try:
            staging_fd = os.open(SIDECAR_STAGING_NAME, _DIRECTORY_FLAGS, dir_fd=root_fd)
        except OSError as exc:
            raise ReplicationDurabilityError(
                "replication staging namespace is unavailable"
            ) from exc
    except OSError as exc:
        raise ReplicationDurabilityError("replication staging namespace is unavailable") from exc
    try:
        info = os.fstat(staging_fd)
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
            raise ReplicationDurabilityError("replication staging namespace is unsafe")
        return staging_fd
    except BaseException as exc:
        failures: list[str] = []
        _close_fd_best_effort(staging_fd, "staging_dir_fd", failures)
        _finish_cleanup(exc, failures)
        raise


def _has_writer_lock_entry(root_fd: int) -> bool:
    """Tell the low-level install helper whether it runs in a sidecar session."""
    fd: int | None = None
    try:
        fd = os.open(
            SIDECAR_LOCK_NAME,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=root_fd,
        )
        info = os.fstat(fd)
        return stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
    except OSError:
        return False
    finally:
        if fd is not None:
            failures: list[str] = []
            _close_fd_best_effort(fd, "lock_entry_probe_fd", failures)
            _finish_cleanup(None, failures)


def _scan_staging_namespace(
    root_fd: int,
    entries: tuple[_GenerationEntry, ...],
    *,
    ignore_name: str | None = None,
    repair_orphans: bool = False,
) -> set[int]:
    """Validate staging entries and return generations proven to have aliases.

    A staging entry is trusted only when it is the deterministic temporary name
    for an existing generation and is the exact same hardlink (bytes, device,
    inode, mode and link count).  A regular pre-link orphan may be removed only
    by a locked writer; status remains strictly read-only and reports it as
    unavailable.  All other names, links and types fail closed.
    """
    try:
        staging_fd = os.open(SIDECAR_STAGING_NAME, _DIRECTORY_FLAGS, dir_fd=root_fd)
    except FileNotFoundError:
        return set()
    except OSError as exc:
        raise ReplicationStateUnavailable("replication staging namespace is unavailable") from exc
    aliases: set[int] = set()
    orphan_names: list[str] = []
    entries_by_sequence = {entry.sequence: entry for entry in entries}
    primary: BaseException | None = None
    try:
        staging_info = os.fstat(staging_fd)
        if not stat.S_ISDIR(staging_info.st_mode) or stat.S_IMODE(staging_info.st_mode) != 0o700:
            raise ReplicationStateUnavailable("replication staging namespace is unsafe")
        for name in sorted(os.listdir(staging_fd)):
            if ignore_name is not None and name == ignore_name:
                continue
            match = _STAGING_TEMP_RE.fullmatch(name)
            if match is None:
                raise ReplicationStateUnavailable(
                    "replication staging namespace contains unknown entry"
                )
            payload, info = _read_at(staging_fd, name)
            if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
                raise ReplicationStateUnavailable("replication staging entry is unsafe")
            sequence = int(match.group(1)[:SIDECAR_GENERATION_WIDTH])
            generation = entries_by_sequence.get(sequence)
            if generation is None:
                if not repair_orphans:
                    raise ReplicationStateUnavailable("replication staging orphan is unavailable")
                orphan_names.append(name)
                continue
            if (
                payload != generation.payload
                or generation.stat.st_dev != info.st_dev
                or generation.stat.st_ino != info.st_ino
                or generation.stat.st_nlink != info.st_nlink
                or generation.stat.st_nlink != 2
                or stat.S_IMODE(generation.stat.st_mode) != stat.S_IMODE(info.st_mode)
            ):
                raise ReplicationStateUnavailable("replication staging alias is invalid")
            aliases.add(sequence)
        if orphan_names:
            for name in orphan_names:
                try:
                    os.unlink(name, dir_fd=staging_fd)
                except OSError as exc:
                    raise ReplicationDurabilityError(
                        "replication staging orphan cleanup failed"
                    ) from exc
            _fsync_open_directory(staging_fd)
            _fsync_open_directory(root_fd)
        return aliases
    except (ReplicationStateUnavailable, ReplicationDurabilityError) as exc:
        primary = exc
        raise
    except (OSError, TypeError, ValueError) as exc:
        primary = ReplicationStateUnavailable("replication staging namespace is unavailable")
        raise primary from exc
    finally:
        failures: list[str] = []
        _close_fd_best_effort(staging_fd, "staging_dir_fd", failures)
        _finish_cleanup(primary, failures)


def _scan_generation_namespace(
    root_fd: int,
    *,
    require_genesis: bool = True,
    ignore_name: str | None = None,
    repair_staging_orphans: bool = False,
) -> tuple[
    dict[str, object] | None, bytes | None, _GenerationEntry | None, tuple[_GenerationEntry, ...]
]:
    try:
        names = os.listdir(root_fd)
    except OSError as exc:
        raise ReplicationStateUnavailable("replication sidecar namespace is unavailable") from exc
    allowed = {SIDECAR_LOCK_NAME, SIDECAR_GENESIS_NAME, SIDECAR_STAGING_NAME}
    generations: list[tuple[int, str]] = []
    for name in names:
        if ignore_name is not None and name == ignore_name:
            continue
        if name in allowed:
            continue
        if _GENERATION_RE.fullmatch(name):
            generations.append((int(name[:SIDECAR_GENERATION_WIDTH]), name))
            continue
        raise ReplicationStateUnavailable("replication sidecar namespace contains unknown entry")
    generations.sort()
    if [sequence for sequence, _name in generations] != list(range(len(generations))):
        raise ReplicationStateUnavailable("replication sidecar generation chain has a gap")
    genesis: dict[str, object] | None = None
    genesis_payload: bytes | None = None
    try:
        genesis_payload, genesis_stat = _read_at(root_fd, SIDECAR_GENESIS_NAME)
    except ReplicationStateUnavailable:
        if require_genesis or generations:
            raise
    else:
        if stat.S_IMODE(genesis_stat.st_mode) != 0o600 or genesis_stat.st_nlink != 1:
            raise ReplicationStateUnavailable("replication sidecar genesis mode is unsafe")
        genesis = _validate_genesis(genesis_payload)
    entries: list[_GenerationEntry] = []
    for sequence, name in generations:
        payload, info = _read_at(root_fd, name)
        fingerprint = _stat_fingerprint(info, payload)
        if fingerprint is None or stat.S_IMODE(info.st_mode) != 0o600:
            raise ReplicationStateUnavailable("replication sidecar generation is unsafe")
        entries.append(_GenerationEntry(sequence, name, payload, info, fingerprint))
    aliases = _scan_staging_namespace(
        root_fd,
        tuple(entries),
        ignore_name=ignore_name,
        repair_orphans=repair_staging_orphans,
    )
    for entry in entries:
        if entry.stat.st_nlink not in ({1, 2} if entry.sequence in aliases else {1}):
            raise ReplicationStateUnavailable("replication sidecar generation link count is unsafe")
    return genesis, genesis_payload, entries[-1] if entries else None, tuple(entries)


_GENERATION_DATA_TABLES = (
    "replication_sidecar_meta",
    "replication_intents",
    "replication_destination_cache",
    "replication_attempt_events",
    "replication_heads",
)


def _canonical_sqlite_value(value: object) -> object:
    if isinstance(value, bytes):
        return {"__bytes__": value.hex()}
    if value is None or isinstance(value, (int, float, str, bool)):
        return value
    raise ReplicationStateUnavailable("replication generation contains unsupported value")


def _generation_payload_preimage(connection: sqlite3.Connection) -> dict[str, object]:
    """Build the closed logical snapshot used by generation content hashes.

    The digest field itself is excluded to avoid a circular preimage.  SQLite
    schema objects remain independently checked by ``_validate_sqlite_schema``.
    """
    tables: list[dict[str, object]] = []
    for table in _GENERATION_DATA_TABLES:
        columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')]
        if not columns:
            raise ReplicationStateUnavailable("replication generation table is missing")
        selected = [
            column
            for column in columns
            if not (table == "replication_sidecar_meta" and column == "generation_payload_sha256")
        ]
        quoted = ", ".join(f'"{column}"' for column in selected)
        rows = connection.execute(f'SELECT {quoted} FROM "{table}" ORDER BY rowid').fetchall()
        tables.append(
            {
                "table": table,
                "columns": selected,
                "rows": [[_canonical_sqlite_value(value) for value in row] for row in rows],
            }
        )
    meta = connection.execute(
        """SELECT generation_number, previous_generation_sha256
             FROM replication_sidecar_meta"""
    ).fetchall()
    if len(meta) != 1:
        raise ReplicationStateUnavailable("replication generation metadata is invalid")
    return {
        "generation_number": meta[0][0],
        "previous_generation_sha256": meta[0][1],
        "tables": tables,
    }


def _generation_payload_digest(connection: sqlite3.Connection) -> str:
    return domain_sha256(GENERATION_PAYLOAD_DOMAIN, _generation_payload_preimage(connection))


def _generation_meta(connection: sqlite3.Connection) -> tuple[int, str, str]:
    rows = connection.execute(
        """SELECT generation_number, previous_generation_sha256,
                          generation_payload_sha256
                     FROM replication_sidecar_meta"""
    ).fetchall()
    if len(rows) != 1:
        raise ReplicationStateUnavailable("replication generation metadata is invalid")
    generation_number, previous_hash, payload_hash = rows[0]
    if not isinstance(generation_number, int) or generation_number < 0:
        raise ReplicationStateUnavailable("replication generation number is invalid")
    try:
        _validate_sha(previous_hash, "previous_generation_sha256")
        _validate_sha(payload_hash, "generation_payload_sha256")
    except (TypeError, ValueError) as exc:
        raise ReplicationStateUnavailable("replication generation digest is invalid") from exc
    expected = _generation_payload_digest(connection)
    if payload_hash != expected:
        raise ReplicationStateUnavailable("replication generation payload hash is invalid")
    return generation_number, previous_hash, payload_hash


def _validate_generation_chain(
    entries: tuple[_GenerationEntry, ...], genesis: Mapping[str, object]
) -> None:
    """Validate every immutable image, not only the highest filename."""
    for index, entry in enumerate(entries):
        connection: sqlite3.Connection | None = None
        primary_error: BaseException | None = None
        try:
            connection = _deserialize_sqlite_bytes(entry.payload)
            _validate_sqlite_schema(connection)
            meta = connection.execute(
                """SELECT schema_version, schema_identity, ddl_sha256, schema_digest,
                          source_instance_id, source_instance_sha256, created_at
                     FROM replication_sidecar_meta"""
            ).fetchall()
            if (
                len(meta) != 1
                or meta[0][:4]
                != (1, SIDECAR_SCHEMA_IDENTITY, SIDECAR_DDL_SHA256, SIDECAR_SCHEMA_DIGEST)
                or meta[0][4] != genesis["source_instance_id"]
                or meta[0][5] != genesis["source_instance_sha256"]
            ):
                raise ReplicationStateUnavailable("replication generation identity is invalid")
            _validate_utc_timestamp(meta[0][6], "created_at")
            _validate_event_reachability(
                connection,
                expected_source_instance_id=genesis["source_instance_id"],
                expected_source_instance_sha256=genesis["source_instance_sha256"],
            )
            generation_number, previous_hash, _payload_hash = _generation_meta(connection)
            expected_previous = (
                ZERO_SHA256
                if index == 0
                else hashlib.sha256(entries[index - 1].payload).hexdigest()
            )
            if generation_number != entry.sequence or previous_hash != expected_previous:
                raise ReplicationStateUnavailable("replication generation parent chain is invalid")
        except ReplicationStateUnavailable as exc:
            primary_error = exc
            raise
        except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
            primary_error = exc
            raise ReplicationStateUnavailable("replication generation is invalid") from exc
        finally:
            if connection is not None:
                failures: list[str] = []
                _close_connection_best_effort(connection, failures)
                if failures:
                    if primary_error is not None:
                        primary_error.add_note("replication_cleanup=failed")
                    else:
                        raise _cleanup_error(failures)


def _validate_generation_payload(
    payload: bytes,
    genesis: Mapping[str, object],
    *,
    expected_sequence: int | None = None,
    expected_previous: str | None = None,
) -> None:
    """Validate a candidate image before it can enter the immutable chain."""
    connection: sqlite3.Connection | None = None
    primary_error: BaseException | None = None
    try:
        connection = _deserialize_sqlite_bytes(payload)
        _validate_sqlite_schema(connection)
        meta = connection.execute(
            """SELECT schema_version, schema_identity, ddl_sha256, schema_digest,
                      source_instance_id, source_instance_sha256, created_at
                 FROM replication_sidecar_meta"""
        ).fetchall()
        if (
            len(meta) != 1
            or meta[0][:4]
            != (
                1,
                SIDECAR_SCHEMA_IDENTITY,
                SIDECAR_DDL_SHA256,
                SIDECAR_SCHEMA_DIGEST,
            )
            or meta[0][4] != genesis["source_instance_id"]
            or meta[0][5] != genesis["source_instance_sha256"]
        ):
            raise ReplicationStateUnavailable("replication generation identity is invalid")
        _validate_utc_timestamp(meta[0][6], "created_at")
        generation_number, previous_hash, _payload_hash = _generation_meta(connection)
        if expected_sequence is not None and generation_number != expected_sequence:
            raise ReplicationStateUnavailable("replication generation number is invalid")
        if expected_previous is not None and previous_hash != expected_previous:
            raise ReplicationStateUnavailable("replication generation parent is invalid")
        _validate_event_reachability(
            connection,
            expected_source_instance_id=genesis["source_instance_id"],
            expected_source_instance_sha256=genesis["source_instance_sha256"],
        )
    except ReplicationStateUnavailable as exc:
        primary_error = exc
        raise
    except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
        primary_error = ReplicationStateUnavailable("replication generation is invalid")
        raise primary_error from exc
    finally:
        if connection is not None:
            failures: list[str] = []
            _close_connection_best_effort(connection, failures)
            _finish_cleanup(primary_error, failures)


def _compare_generation_snapshot(
    root_fd: int,
    *,
    genesis_payload: bytes,
    entries: tuple[_GenerationEntry, ...],
    ignore_name: str | None = None,
) -> None:
    current_genesis, current_genesis_payload, _latest, current_entries = _scan_generation_namespace(
        root_fd, ignore_name=ignore_name
    )
    if current_genesis_payload != genesis_payload or tuple(
        (entry.sequence, entry.name, entry.fingerprint) for entry in current_entries
    ) != tuple((entry.sequence, entry.name, entry.fingerprint) for entry in entries):
        raise ReplicationStateUnavailable("replication sidecar generation changed during read")
    if current_genesis is None:
        raise ReplicationStateUnavailable("replication sidecar genesis is unavailable")
    _validate_generation_chain(current_entries, current_genesis)
    _lock_payload, lock_info = _read_at(root_fd, SIDECAR_LOCK_NAME)
    if _generation_lock_identity(lock_info) != {
        key: current_genesis[key] for key in ("lock_dev", "lock_ino", "lock_nlink", "lock_mode")
    }:
        raise ReplicationStateUnavailable("replication sidecar lock authority changed")


def _assert_generation_lock(session: _GenerationWriterSession) -> None:
    if session.lock_fd is None or session.lock_token is None or session.lock_token.released:
        raise ReplicationDurabilityError("replication writer lock token is missing")
    _verify_writer_lock_entry(
        session.root_fd, SIDECAR_LOCK_NAME, session.lock_fd, session.lock_token
    )
    if session.genesis is not None:
        info = os.fstat(session.lock_fd)
        if _generation_lock_identity(info) != {
            key: session.genesis[key] for key in ("lock_dev", "lock_ino", "lock_nlink", "lock_mode")
        }:
            raise ReplicationDurabilityError("replication writer lock authority changed")


@dataclass
class _GenerationWriterSession:
    path: Path
    root_fd: int
    descriptors: list[int]
    lock_fd: int
    lock_token: _WriterLockToken
    genesis: dict[str, object] | None
    genesis_payload: bytes | None
    entries: tuple[_GenerationEntry, ...]

    @property
    def latest_sequence(self) -> int:
        return self.entries[-1].sequence if self.entries else -1


def _open_generation_writer_session(path: Path) -> _GenerationWriterSession:
    root = _sidecar_root(path)
    root_fd, descriptors = _open_directory_chain(root, create=True)
    lock_fd: int | None = None
    token: _WriterLockToken | None = None
    try:
        lock_fd, token = _acquire_writer_lock(root_fd, SIDECAR_LOCK_NAME)
        # Scan while the lock is held.  Unknown/WAL/SHM entries are never
        # ignored, including on an otherwise empty sidecar.  A validated
        # regular pre-link orphan is cleaned here, while status remains
        # strictly read-only and reports it as unavailable.
        genesis, genesis_payload, _latest, entries = _scan_generation_namespace(
            root_fd, require_genesis=False, repair_staging_orphans=True
        )
        session = _GenerationWriterSession(
            root, root_fd, descriptors, lock_fd, token, genesis, genesis_payload, entries
        )
        _assert_generation_lock(session)
        return session
    except BaseException as exc:
        failures: list[str] = []
        if lock_fd is not None and token is not None:
            _release_writer_lock(lock_fd, token, failures)
        _close_descriptors(descriptors, failures)
        _finish_cleanup(exc, failures)
        raise


def _close_generation_writer_session(session: _GenerationWriterSession) -> None:
    failures: list[str] = []
    if session.lock_fd is not None:
        _release_writer_lock(session.lock_fd, session.lock_token, failures)
    _close_descriptors(session.descriptors, failures)
    _finish_cleanup(None, failures)


def _install_fixed_at(
    root_fd: int,
    name: str,
    payload: bytes,
    *,
    before_install: object | None = None,
) -> None:
    """Install one immutable basename without overwrite."""
    temp = f".{name}.tmp"
    staging_fd: int | None = None
    temp_fd: int | None = None
    primary: BaseException | None = None
    failures: list[str] = []
    installed = False
    temp_owned = False
    try:
        staging_fd = _ensure_staging_directory(root_fd)
        temp_fd = os.open(
            temp,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=staging_fd,
        )
        temp_owned = True
        _write_fully(temp_fd, payload)
        os.fsync(temp_fd)
        close_failures: list[str] = []
        _close_fd_best_effort(temp_fd, "generation_temp_fd", close_failures)
        if close_failures:
            raise _cleanup_error(close_failures)
        temp_fd = None
        if before_install is not None:
            before_install(temp)
        try:
            os.link(
                temp,
                name,
                src_dir_fd=staging_fd,
                dst_dir_fd=root_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            raise ReplicationCASConflict("replication generation CAS conflict") from exc
        installed = True
        os.unlink(temp, dir_fd=staging_fd)
        temp = ""
        temp_owned = False
        _fsync_open_directory(staging_fd)
        # Keep the fixed namespace hidden when there is no residual staging
        # evidence.  A committed hardlink alias or an external entry remains
        # visible for the strict scanner to prove or reject.
        if _has_writer_lock_entry(root_fd) and not os.listdir(staging_fd):
            try:
                os.rmdir(SIDECAR_STAGING_NAME, dir_fd=root_fd)
            except FileNotFoundError:
                pass
            else:
                _fsync_open_directory(root_fd)
        _fsync_open_directory(root_fd)
    except ReplicationCASConflict as exc:
        primary = exc
        raise
    except ReplicationPostCommitConflict as exc:
        primary = exc
        raise
    except ReplicationDurabilityError as exc:
        if installed:
            primary = ReplicationPostCommitConflict(
                "replication generation committed; CONTROL_STATE_UNAVAILABLE "
                "post-install durability proof failed"
            )
            raise primary from exc
        primary = exc
        raise
    except OSError as exc:
        if not installed and isinstance(exc, FileExistsError):
            primary = ReplicationCASConflict("replication generation CAS conflict")
            raise primary from exc
        if installed:
            primary = ReplicationPostCommitConflict(
                "replication generation committed; CONTROL_STATE_UNAVAILABLE "
                "post-install durability proof failed"
            )
            raise primary from exc
        primary = ReplicationDurabilityError("replication generation install failed")
        raise primary from exc
    except BaseException as exc:
        primary = exc
        raise
    finally:
        if temp_fd is not None:
            _close_fd_best_effort(temp_fd, "generation_temp_fd", failures)
        if temp and temp_owned:
            try:
                if staging_fd is not None:
                    os.unlink(temp, dir_fd=staging_fd)
            except FileNotFoundError:
                pass
            except OSError:
                failures.append("generation_temp_unlink")
        if staging_fd is not None:
            _close_fd_best_effort(staging_fd, "staging_dir_fd", failures)
        if failures:
            error = _cleanup_error(failures)
            if primary is not None:
                primary.add_note("replication_cleanup=failed")
            else:
                raise error


def _generation_payload(
    source_instance_id: str,
    source_instance_sha256: str,
    created_at: str,
    *,
    generation_number: int = 0,
    previous_generation_sha256: str = ZERO_SHA256,
) -> bytes:
    if generation_number < 0:
        raise ValueError("generation_number must be non-negative")
    _validate_sha(previous_generation_sha256, "previous_generation_sha256")
    connection = sqlite3.connect(":memory:")
    primary_error: BaseException | None = None
    try:
        connection.executescript(SIDECAR_DDL)
        connection.execute(
            """INSERT INTO replication_sidecar_meta
               (sidecar_id, schema_version, schema_identity, ddl_sha256, schema_digest,
                source_instance_id, source_instance_sha256, created_at,
                generation_number, previous_generation_sha256, generation_payload_sha256)
               VALUES (1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                SIDECAR_SCHEMA_IDENTITY,
                SIDECAR_DDL_SHA256,
                SIDECAR_SCHEMA_DIGEST,
                source_instance_id,
                source_instance_sha256,
                created_at,
                generation_number,
                previous_generation_sha256,
                ZERO_SHA256,
            ),
        )
        connection.commit()
        digest = _generation_payload_digest(connection)
        connection.execute(
            """UPDATE replication_sidecar_meta
                  SET generation_payload_sha256 = ?
                WHERE sidecar_id = 1""",
            (digest,),
        )
        connection.commit()
        return connection.serialize(name="main")
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        failures: list[str] = []
        _close_connection_best_effort(connection, failures)
        if failures:
            if primary_error is not None:
                primary_error.add_note("replication_cleanup=failed")
            else:
                raise _cleanup_error(failures)


def _rebuild_generation_payload(
    source: sqlite3.Connection,
    *,
    generation_number: int,
    previous_generation_sha256: str,
) -> bytes:
    """Construct a sealed next image without mutating the sealed source image.

    ``replication_sidecar_meta`` is immutable once its payload hash is set.
    A writer therefore copies the already-mutated private image into a fresh
    in-memory database, assigns the next generation metadata while its new
    payload hash is still the construction zero, and seals that image.  The
    normative trigger remains installed throughout the candidate lifecycle.
    """
    _validate_sha(previous_generation_sha256, "previous_generation_sha256")
    candidate: sqlite3.Connection | None = None
    primary: BaseException | None = None
    try:
        candidate = sqlite3.connect(":memory:")
        candidate.executescript(SIDECAR_DDL)
        table_names = (
            "replication_sidecar_meta",
            "replication_intents",
            "replication_attempt_events",
            "replication_heads",
            "replication_destination_cache",
        )
        for table in table_names:
            columns = tuple(
                row[1] for row in source.execute(f'PRAGMA table_info("{table}")').fetchall()
            )
            if not columns:
                raise ReplicationStateUnavailable("replication sidecar table is missing")
            quoted = ",".join(f'"{column}"' for column in columns)
            rows = source.execute(f'SELECT {quoted} FROM "{table}" ORDER BY rowid').fetchall()
            if table == "replication_sidecar_meta":
                if len(rows) != 1:
                    raise ReplicationStateUnavailable("replication sidecar metadata is invalid")
                values = list(rows[0])
                values[8] = generation_number
                values[9] = previous_generation_sha256
                values[10] = ZERO_SHA256
                rows = [tuple(values)]
            if rows:
                placeholders = ",".join("?" * len(columns))
                candidate.executemany(
                    f'INSERT INTO "{table}" ({quoted}) VALUES ({placeholders})', rows
                )
        candidate.commit()
        payload_digest = _generation_payload_digest(candidate)
        candidate.execute(
            "UPDATE replication_sidecar_meta SET generation_payload_sha256=? WHERE sidecar_id=1",
            (payload_digest,),
        )
        candidate.commit()
        return candidate.serialize(name="main")
    except BaseException as exc:
        primary = exc
        raise
    finally:
        if candidate is not None:
            failures: list[str] = []
            _close_connection_best_effort(candidate, failures)
            _finish_cleanup(primary, failures)


def _install_generation(session: _GenerationWriterSession, payload: bytes) -> None:
    _assert_generation_lock(session)
    if session.genesis_payload is None or session.genesis is None:
        raise ReplicationDurabilityError("replication sidecar genesis is unavailable")
    sequence = session.latest_sequence + 1
    expected_previous = (
        ZERO_SHA256
        if not session.entries
        else hashlib.sha256(session.entries[-1].payload).hexdigest()
    )
    _validate_generation_payload(
        payload,
        session.genesis,
        expected_sequence=sequence,
        expected_previous=expected_previous,
    )
    _compare_generation_snapshot(
        session.root_fd, genesis_payload=session.genesis_payload, entries=session.entries
    )
    name = _generation_filename(sequence)
    _install_fixed_at(
        session.root_fd,
        name,
        payload,
        before_install=lambda temporary_name: (
            _assert_generation_lock(session),
            _compare_generation_snapshot(
                session.root_fd,
                genesis_payload=session.genesis_payload or b"",
                entries=session.entries,
                ignore_name=temporary_name,
            ),
        ),
    )
    # The installed generation is already immutable.  Re-read it and prove
    # the lock and complete namespace before reporting success.  Failure here
    # is post-linearization: retain the generation and make the sidecar
    # unavailable for subsequent readers.
    try:
        _assert_generation_lock(session)
        _genesis, genesis_payload, _latest, entries = _scan_generation_namespace(session.root_fd)
        if genesis_payload != session.genesis_payload or entries[-1].payload != payload:
            raise ReplicationStateUnavailable("replication generation install readback mismatch")
        session.entries = entries
    except (ReplicationDurabilityError, ReplicationStateUnavailable) as exc:
        raise ReplicationPostCommitConflict(
            "replication generation committed; CONTROL_STATE_UNAVAILABLE "
            "final lock/namespace proof failed"
        ) from exc


def _open_generation_readonly(
    path: Path,
) -> tuple[sqlite3.Connection, int, bytes, tuple[_GenerationEntry, ...]]:
    root = _sidecar_root(path)
    root_fd, descriptors = _open_directory_chain(root, create=False)
    connection: sqlite3.Connection | None = None
    try:
        genesis, genesis_payload, latest, entries = _scan_generation_namespace(root_fd)
        if genesis is None or genesis_payload is None or latest is None:
            raise ReplicationStateUnavailable("replication sidecar genesis is unavailable")
        lock_payload, lock_info = _read_at(root_fd, SIDECAR_LOCK_NAME)
        del lock_payload
        if _generation_lock_identity(lock_info) != {
            key: genesis[key] for key in ("lock_dev", "lock_ino", "lock_nlink", "lock_mode")
        }:
            raise ReplicationStateUnavailable("replication sidecar lock authority changed")
        connection = _deserialize_sqlite_bytes(latest.payload)
        _compare_generation_snapshot(root_fd, genesis_payload=genesis_payload, entries=entries)
        # The root descriptor is the authority for the remainder of this
        # read.  Ancestor descriptors have already been checked and can be
        # closed without reopening the path.
        ancestor_failures: list[str] = []
        _close_descriptors(descriptors[:-1], ancestor_failures)
        descriptors = [root_fd]
        if ancestor_failures:
            _close_connection_best_effort(connection, ancestor_failures)
            _close_fd_best_effort(root_fd, "sidecar_root_fd", ancestor_failures)
            _finish_cleanup(None, ancestor_failures)
        return connection, root_fd, genesis_payload, entries
    except BaseException as exc:
        failures: list[str] = []
        if connection is not None:
            _close_connection_best_effort(connection, failures)
        _close_descriptors(descriptors, failures)
        _finish_cleanup(exc, failures)
        raise


def _close_readonly_generation(root_fd: int, connection: sqlite3.Connection) -> None:
    failures: list[str] = []
    _close_connection_best_effort(connection, failures)
    _close_fd_best_effort(root_fd, "sidecar_root_fd", failures)
    _finish_cleanup(None, failures)


@dataclass(frozen=True)
class _MutationOutcome:
    value: object
    changed: bool


def _timestamp_value(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except (TypeError, ValueError) as exc:
        raise ReplicationDurabilityError("replication timestamp is invalid") from exc


def _add_seconds(timestamp: str, seconds: int) -> str:
    if not isinstance(seconds, int) or seconds < 0:
        raise ValueError("seconds must be non-negative")
    return (
        (_timestamp_value(timestamp).replace(tzinfo=UTC) + timedelta(seconds=seconds))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _intent_hash_preimage(values: Mapping[str, object]) -> dict[str, object]:
    return {
        field: values[field]
        for field in (
            "intent_id",
            "schema_version",
            "direction",
            "destination_id",
            "checkpoint_id",
            "pointer_row_sha256",
            "pointer_generation",
            "source_run_id",
            "source_trade_date",
            "source_published_at",
            "pointer_db_inode",
            "pointer_db_device",
            "pointer_db_schema_digest",
            "manifest_canonical_sha256",
            "source_object_set_sha256",
            "source_instance_id",
            "source_instance_sha256",
            "source_sequence",
            "source_manifest_bytes_sha256",
            "publication_binding_sha256",
            "plan_sha256",
            "object_count",
            "row_count",
            "byte_count",
            "created_at",
        )
    }


def _event_values(
    *,
    intent_id: str,
    event_sequence: int,
    prev_event_sha256: str,
    event_type: str,
    from_state: str | None,
    to_state: str,
    attempt: int,
    reason_code: str,
    state_version: int,
    occurred_at: str,
    destination_replication_generation: str | None = None,
    destination_record_sha256: str | None = None,
    destination_head_sha256: str | None = None,
) -> dict[str, object]:
    event_id = domain_sha256(
        "stock-eva/r2f4.3/replication-event-id/v1",
        {
            "intent_id": intent_id,
            "event_sequence": event_sequence,
            "attempt": attempt,
            "state_version": state_version,
            "occurred_at": occurred_at,
        },
    )
    values: dict[str, object] = {
        "event_id": event_id,
        "intent_id": intent_id,
        "event_sequence": event_sequence,
        "prev_event_sha256": prev_event_sha256,
        "event_type": event_type,
        "from_state": from_state,
        "to_state": to_state,
        "attempt": attempt,
        "reason_code": reason_code,
        "state_version": state_version,
        "occurred_at": occurred_at,
        "destination_replication_generation": destination_replication_generation,
        "destination_record_sha256": destination_record_sha256,
        "destination_head_sha256": destination_head_sha256,
    }
    values["event_sha256"] = domain_sha256("stock-eva/r2f4.3/replication-event/v1", values)
    return values


def _intent_row_from_checkpoint(
    checkpoint: SourceCheckpoint,
    *,
    operation_day: str,
    destination_id: str,
    source_sequence: int,
    created_at: str,
) -> dict[str, object]:
    inventory = _object_inventory_projection(checkpoint.object_inventory)
    plan_sha256 = domain_sha256(
        "stock-eva/r2f4.3/replication-plan/v1",
        {
            "direction": "local_to_nas",
            "destination_id": destination_id,
            "checkpoint_id": checkpoint.checkpoint_id,
            "object_inventory": inventory,
        },
    )
    intent_id = domain_sha256(
        "stock-eva/r2f4.3/replication-intent/v1",
        {
            "direction": "local_to_nas",
            "destination_id": destination_id,
            "source_instance_id": checkpoint.source_instance_id,
            "source_sequence": source_sequence,
            "checkpoint_id": checkpoint.checkpoint_id,
            "plan_sha256": plan_sha256,
        },
    )
    values: dict[str, object] = {
        "intent_id": intent_id,
        "schema_version": 1,
        "operation_day": operation_day,
        "direction": "local_to_nas",
        "destination_id": destination_id,
        "pointer_row_sha256": checkpoint.pointer_row_sha256,
        "pointer_generation": checkpoint.pointer_generation,
        "source_run_id": checkpoint.source_run_id,
        "source_trade_date": checkpoint.source_trade_date,
        "source_published_at": checkpoint.source_published_at,
        "pointer_db_device": checkpoint.pointer_db_device,
        "pointer_db_inode": checkpoint.pointer_db_inode,
        "pointer_db_schema_digest": checkpoint.pointer_db_schema_digest,
        "manifest_canonical_sha256": checkpoint.manifest_canonical_sha256,
        "source_object_set_sha256": checkpoint.source_object_set_sha256,
        "publication_binding_sha256": checkpoint.publication_binding_sha256,
        "source_instance_id": checkpoint.source_instance_id,
        "source_instance_sha256": checkpoint.source_instance_sha256,
        "source_sequence": source_sequence,
        "checkpoint_id": checkpoint.checkpoint_id,
        "source_manifest_bytes_sha256": checkpoint.source_manifest_bytes_sha256,
        "plan_sha256": plan_sha256,
        "object_count": len(inventory),
        "row_count": sum(int(item["row_count"]) for item in inventory),
        "byte_count": sum(int(item["size_bytes"]) for item in inventory),
        "created_at": created_at,
    }
    values["intent_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/replication-intent-row/v1", _intent_hash_preimage(values)
    )
    return values


class ImmutableReplicationSidecarStore:
    def __init__(self, path: Path, *, destination_verifier: object | None = None) -> None:
        self.path = _sidecar_root(path)
        # Completion is deliberately opt-in and requires a concrete
        # descriptor-native verifier.  A bare sidecar can still queue and
        # inspect local state, but it cannot promote an intent to replicated.
        self._destination_verifier = destination_verifier

    @staticmethod
    def _connect_writer(path: Path) -> sqlite3.Connection:
        connection, root_fd, _genesis, _entries = _open_generation_readonly(path)
        try:
            connection.execute("PRAGMA query_only = ON")
        except BaseException as exc:
            failures: list[str] = []
            _close_connection_best_effort(connection, failures)
            _close_fd_best_effort(root_fd, "sidecar_root_fd", failures)
            _finish_cleanup(exc, failures)
            raise
        failures = []
        _close_fd_best_effort(root_fd, "sidecar_root_fd", failures)
        if failures:
            _close_connection_best_effort(connection, failures)
            _finish_cleanup(None, failures)
        return connection

    def initialize(
        self,
        *,
        source_instance_id: str,
        source_instance_sha256: str,
        created_at: str | None = None,
    ) -> None:
        _validate_sha(source_instance_id, "source_instance_id")
        _validate_sha(source_instance_sha256, "source_instance_sha256")
        session = _open_generation_writer_session(self.path)
        connection: sqlite3.Connection | None = None
        primary: BaseException | None = None
        try:
            now = created_at or _utc_now()
            if session.genesis is None:
                if session.entries:
                    raise ReplicationStateUnavailable("replication sidecar genesis is missing")
                genesis = _build_genesis(
                    source_instance_id=source_instance_id,
                    source_instance_sha256=source_instance_sha256,
                    lock_info=os.fstat(session.lock_fd),
                    created_at=now,
                )
                _install_fixed_at(
                    session.root_fd, SIDECAR_GENESIS_NAME, canonical_json_bytes(genesis)
                )
                session.genesis = genesis
                session.genesis_payload = canonical_json_bytes(genesis)
            elif (
                session.genesis["source_instance_id"] != source_instance_id
                or session.genesis["source_instance_sha256"] != source_instance_sha256
            ):
                raise ReplicationDurabilityError("replication sidecar source identity conflicts")
            if session.entries:
                connection = _deserialize_sqlite_bytes(session.entries[-1].payload)
                connection.execute("PRAGMA query_only = ON")
                meta = connection.execute(
                    "SELECT source_instance_id, source_instance_sha256 FROM replication_sidecar_meta"
                ).fetchall()
                _validate_sqlite_schema(connection)
                if meta != [(source_instance_id, source_instance_sha256)]:
                    raise ReplicationDurabilityError(
                        "replication sidecar source identity conflicts"
                    )
                _compare_generation_snapshot(
                    session.root_fd,
                    genesis_payload=session.genesis_payload or b"",
                    entries=session.entries,
                )
                return
            payload = _generation_payload(
                source_instance_id,
                source_instance_sha256,
                now,
            )
            _install_generation(session, payload)
        except (sqlite3.Error, OSError) as exc:
            primary = ReplicationDurabilityError("replication sidecar initialization failed")
            raise primary from exc
        except BaseException as exc:
            primary = exc
            raise
        finally:
            failures: list[str] = []
            if connection is not None:
                _close_connection_best_effort(connection, failures)
            try:
                _close_generation_writer_session(session)
            except ReplicationDurabilityError:
                failures.append("writer_session")
            if failures:
                if primary is not None:
                    primary.add_note("replication_cleanup=failed")
                else:
                    raise _cleanup_error(failures)

    def _mutate_generation(self, mutator: object) -> object:
        """Run one sidecar mutation in a private SQLite image.

        The transaction is equivalent to ``BEGIN IMMEDIATE``: the anchored
        sidecar lock serializes writers, while the immutable generation link
        is the durable CAS.  A no-op never installs a new generation.
        """
        if not callable(mutator):
            raise TypeError("sidecar mutator must be callable")
        # A mutation never bootstraps an empty path.  Initialization is an
        # explicit writer operation; this guard also keeps an unavailable
        # sidecar from leaving behind a lock directory as a failure artifact.
        if not self.path.is_dir() or not (self.path / SIDECAR_GENESIS_NAME).is_file():
            raise ReplicationStateUnavailable("replication sidecar is uninitialized")
        session: _GenerationWriterSession | None = None
        connection: sqlite3.Connection | None = None
        primary_error: BaseException | None = None
        try:
            session = _open_generation_writer_session(self.path)
            if not session.entries or session.genesis is None:
                raise ReplicationStateUnavailable("replication sidecar is uninitialized")
            _assert_generation_lock(session)
            _compare_generation_snapshot(
                session.root_fd,
                genesis_payload=session.genesis_payload or b"",
                entries=session.entries,
            )
            connection = _deserialize_sqlite_bytes(session.entries[-1].payload)
            _validate_sqlite_schema(connection)
            meta = connection.execute(
                "SELECT source_instance_id, source_instance_sha256 FROM replication_sidecar_meta"
            ).fetchone()
            if meta != (
                session.genesis["source_instance_id"],
                session.genesis["source_instance_sha256"],
            ):
                raise ReplicationStateUnavailable("replication sidecar source identity is invalid")
            _validate_event_reachability(
                connection,
                expected_source_instance_id=session.genesis["source_instance_id"],
                expected_source_instance_sha256=session.genesis["source_instance_sha256"],
            )
            connection.execute("BEGIN IMMEDIATE")
            raw_result = mutator(connection)
            if not isinstance(raw_result, _MutationOutcome):
                raise ReplicationDurabilityError("replication mutation result is invalid")
            if not raw_result.changed:
                connection.rollback()
                return raw_result.value
            # Generation metadata is part of the immutable image, not a
            # post-install patch.  Build a fresh candidate image with the
            # deterministic next number/parent and seal its payload digest;
            # the previous sealed image remains untouched.
            next_generation = session.latest_sequence + 1
            previous_generation = hashlib.sha256(session.entries[-1].payload).hexdigest()
            payload = _rebuild_generation_payload(
                connection,
                generation_number=next_generation,
                previous_generation_sha256=previous_generation,
            )
            connection.rollback()
            _install_generation(session, payload)
            return raw_result.value
        except ReplicationCASConflict as exc:
            primary_error = exc
            raise
        except (ReplicationStateUnavailable, ReplicationDurabilityError) as exc:
            primary_error = exc
            if connection is not None:
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
            raise
        except sqlite3.IntegrityError as exc:
            primary_error = ReplicationCASConflict("replication outbox CAS conflict")
            if connection is not None:
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
            raise primary_error from exc
        except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
            primary_error = ReplicationDurabilityError("replication outbox mutation failed")
            if connection is not None:
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
            raise primary_error from exc
        except BaseException as exc:
            primary_error = exc
            if connection is not None:
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
            raise
        finally:
            failures: list[str] = []
            if connection is not None:
                _close_connection_best_effort(connection, failures)
            if session is not None:
                try:
                    _close_generation_writer_session(session)
                except ReplicationDurabilityError:
                    failures.append("writer_session")
            _finish_cleanup(primary_error, failures)

    @staticmethod
    def _validate_operation_inputs(operation_day: str, destination_id: str) -> None:
        try:
            _validate_iso_date(operation_day, "operation_day")
        except (TypeError, ValueError) as exc:
            raise ReplicationDurabilityError("replication operation day is invalid") from exc
        if (
            not isinstance(destination_id, str)
            or len(destination_id) != 32
            or any(char not in "0123456789abcdef" for char in destination_id)
        ):
            raise ReplicationDurabilityError("replication destination identity is invalid")

    @staticmethod
    def _validate_checkpoint(
        connection: sqlite3.Connection,
        checkpoint: SourceCheckpoint,
    ) -> None:
        try:
            checkpoint.verify_hashes()
        except (ReplicationDurabilityError, ValueError) as exc:
            raise ReplicationStateUnavailable("source checkpoint is unavailable") from exc
        meta = connection.execute(
            "SELECT source_instance_id, source_instance_sha256 FROM replication_sidecar_meta"
        ).fetchone()
        if meta != (checkpoint.source_instance_id, checkpoint.source_instance_sha256):
            raise ReplicationStateUnavailable("source checkpoint identity conflicts")

    @staticmethod
    def _insert_intent(
        connection: sqlite3.Connection,
        values: Mapping[str, object],
    ) -> None:
        columns = tuple(values)
        connection.execute(
            f"INSERT INTO replication_intents ({','.join(columns)}) VALUES ({','.join('?' * len(columns))})",
            tuple(values[column] for column in columns),
        )
        event = _event_values(
            intent_id=str(values["intent_id"]),
            event_sequence=0,
            prev_event_sha256=ZERO_SHA256,
            event_type="intent_created",
            from_state=None,
            to_state="pending",
            attempt=0,
            reason_code="NONE",
            state_version=0,
            occurred_at=str(values["created_at"]),
        )
        event_columns = tuple(event)
        connection.execute(
            f"INSERT INTO replication_attempt_events ({','.join(event_columns)}) VALUES ({','.join('?' * len(event_columns))})",
            tuple(event[column] for column in event_columns),
        )
        connection.execute(
            """INSERT INTO replication_heads
               (intent_id, current_state, state_version, last_event_sequence,
                lease_owner, lease_until, next_attempt_at, last_reason_code, updated_at)
               VALUES (?, 'pending', 0, 0, NULL, NULL, ?, 'NONE', ?)""",
            (values["intent_id"], values["created_at"], values["created_at"]),
        )

    @staticmethod
    def _existing_intent(
        connection: sqlite3.Connection,
        *,
        checkpoint_id: str,
        destination_id: str,
    ) -> tuple[object, ...] | None:
        return connection.execute(
            """SELECT intent_id, source_sequence, source_instance_id,
                      source_instance_sha256, checkpoint_id, plan_sha256
                 FROM replication_intents
                WHERE direction='local_to_nas' AND destination_id=? AND checkpoint_id=?""",
            (destination_id, checkpoint_id),
        ).fetchone()

    def _enqueue_checkpoints(
        self,
        connection: sqlite3.Connection,
        checkpoints: Iterable[SourceCheckpoint],
        *,
        operation_day: str,
        destination_id: str,
        created_at: str,
    ) -> _MutationOutcome:
        ordered = sorted(
            checkpoints, key=lambda item: (item.source_published_at, item.checkpoint_id)
        )
        unique: list[SourceCheckpoint] = []
        seen: set[str] = set()
        for checkpoint in ordered:
            if checkpoint.checkpoint_id in seen:
                continue
            seen.add(checkpoint.checkpoint_id)
            self._validate_checkpoint(connection, checkpoint)
            unique.append(checkpoint)
        max_sequence = connection.execute(
            "SELECT COALESCE(MAX(source_sequence), 0) FROM replication_intents WHERE source_instance_id=?",
            (unique[0].source_instance_id,) if unique else ("",),
        ).fetchone()[0]
        if not isinstance(max_sequence, int):
            raise ReplicationStateUnavailable("replication source sequence is invalid")
        next_sequence = max_sequence + 1
        intent_ids: list[str] = []
        source_sequences: list[int] = []
        imported = 0
        existing = 0
        for checkpoint in unique:
            found = self._existing_intent(
                connection,
                checkpoint_id=checkpoint.checkpoint_id,
                destination_id=destination_id,
            )
            if found is not None:
                (
                    found_id,
                    found_sequence,
                    found_source_id,
                    found_source_sha,
                    found_cp,
                    found_plan,
                ) = found
                if (
                    found_source_id != checkpoint.source_instance_id
                    or found_source_sha != checkpoint.source_instance_sha256
                    or found_cp != checkpoint.checkpoint_id
                ):
                    raise ReplicationStateUnavailable("replication intent identity conflicts")
                intent_values = _intent_row_from_checkpoint(
                    checkpoint,
                    operation_day=operation_day,
                    destination_id=destination_id,
                    source_sequence=int(found_sequence),
                    created_at=created_at,
                )
                if (
                    found_id != intent_values["intent_id"]
                    or found_plan != intent_values["plan_sha256"]
                ):
                    raise ReplicationStateUnavailable("replication intent identity conflicts")
                intent_ids.append(str(found_id))
                source_sequences.append(int(found_sequence))
                existing += 1
                continue
            # A checkpoint has one global source sequence in this sidecar.  A
            # previously imported destination must therefore be reused rather
            # than assigned a second sequence.
            prior = connection.execute(
                "SELECT intent_id, source_sequence FROM replication_intents WHERE source_instance_id=? AND checkpoint_id=?",
                (checkpoint.source_instance_id, checkpoint.checkpoint_id),
            ).fetchone()
            if prior is not None:
                raise ReplicationCASConflict(
                    "replication checkpoint has conflicting destination intent"
                )
            values = _intent_row_from_checkpoint(
                checkpoint,
                operation_day=operation_day,
                destination_id=destination_id,
                source_sequence=next_sequence,
                created_at=created_at,
            )
            self._insert_intent(connection, values)
            intent_ids.append(str(values["intent_id"]))
            source_sequences.append(next_sequence)
            imported += 1
            next_sequence += 1
        return _MutationOutcome(
            ReplicationImportResult(
                intent_ids=tuple(intent_ids),
                source_sequences=tuple(source_sequences),
                imported_count=imported,
                existing_count=existing,
            ),
            imported > 0,
        )

    def enqueue_checkpoint(
        self,
        checkpoint: SourceCheckpoint | Mapping[str, object],
        *,
        operation_day: str,
        destination_id: str,
        created_at: str | None = None,
        plan_sha256: str | None = None,
    ) -> ReplicationImportResult:
        """Persist one checkpoint as an immutable, idempotent intent."""
        self._validate_operation_inputs(operation_day, destination_id)
        try:
            normalized = (
                checkpoint
                if isinstance(checkpoint, SourceCheckpoint)
                else SourceCheckpoint.model_validate(
                    {
                        **checkpoint,
                        "object_inventory": tuple(checkpoint.get("object_inventory", ())),
                    }
                    if isinstance(checkpoint, Mapping)
                    else checkpoint
                )
            )
        except (TypeError, ValueError) as exc:
            raise ReplicationStateUnavailable("source checkpoint is unavailable") from exc
        normalized.verify_hashes()
        if plan_sha256 is not None:
            expected = _intent_row_from_checkpoint(
                normalized,
                operation_day=operation_day,
                destination_id=destination_id,
                source_sequence=1,
                created_at=created_at or _utc_now(),
            )["plan_sha256"]
            if plan_sha256 != expected:
                raise ReplicationDurabilityError("replication plan digest is invalid")
        result = self._mutate_generation(
            lambda connection: self._enqueue_checkpoints(
                connection,
                [normalized],
                operation_day=operation_day,
                destination_id=destination_id,
                created_at=created_at or _utc_now(),
            )
        )
        if not isinstance(result, ReplicationImportResult):
            raise ReplicationDurabilityError("replication enqueue result is invalid")
        return result

    def enqueue(
        self,
        checkpoint: SourceCheckpoint | Mapping[str, object],
        *,
        operation_day: str,
        destination_id: str,
        created_at: str | None = None,
    ) -> ReplicationImportResult:
        return self.enqueue_checkpoint(
            checkpoint,
            operation_day=operation_day,
            destination_id=destination_id,
            created_at=created_at,
        )

    def import_journals(
        self,
        records: Iterable[JournalRecord],
        *,
        operation_day: str,
        destination_id: str,
        created_at: str | None = None,
    ) -> ReplicationImportResult:
        """Import complete journals in deterministic order in one transaction."""
        self._validate_operation_inputs(operation_day, destination_id)
        normalized: list[SourceCheckpoint] = []
        for record in records:
            if not isinstance(record, JournalRecord):
                raise ReplicationStateUnavailable("replication journal is unavailable")
            record.verify_hash()
            record.checkpoint_projection.verify_hashes()
            normalized.append(record.checkpoint_projection)
        result = self._mutate_generation(
            lambda connection: self._enqueue_checkpoints(
                connection,
                normalized,
                operation_day=operation_day,
                destination_id=destination_id,
                created_at=created_at or _utc_now(),
            )
        )
        if not isinstance(result, ReplicationImportResult):
            raise ReplicationDurabilityError("replication journal import result is invalid")
        return result

    def reconcile(
        self,
        current: SourceCheckpoint | JournalRecord | None,
        *,
        operation_day: str,
        destination_id: str,
        journals: Iterable[JournalRecord] = (),
        now: str | None = None,
    ) -> ReplicationImportResult:
        """Reconcile strict local evidence without any provider request."""
        records = list(journals)
        if isinstance(current, JournalRecord):
            records.append(current)
        elif isinstance(current, SourceCheckpoint):
            current.verify_hashes()
            records.append(
                build_journal_record(
                    checkpoint_id=current.checkpoint_id,
                    source_instance_id=current.source_instance_id,
                    source_instance_sha256=current.source_instance_sha256,
                    publication_binding_sha256=current.publication_binding_sha256,
                    source_published_at=current.source_published_at,
                    checkpoint_projection=current,
                    created_at=now,
                )
            )
        return self.import_journals(
            records,
            operation_day=operation_day,
            destination_id=destination_id,
            created_at=now,
        )

    @staticmethod
    def _latest_event(
        connection: sqlite3.Connection,
        intent_id: str,
    ) -> tuple[object, ...]:
        event = connection.execute(
            """SELECT event_id, event_sequence, prev_event_sha256, event_type,
                      from_state, to_state, attempt, reason_code, state_version,
                      occurred_at, destination_replication_generation,
                      destination_record_sha256, destination_head_sha256, event_sha256
                 FROM replication_attempt_events
                WHERE intent_id=? ORDER BY event_sequence DESC LIMIT 1""",
            (intent_id,),
        ).fetchone()
        if event is None:
            raise ReplicationStateUnavailable("replication intent event history is missing")
        return event

    @staticmethod
    def _head_row(connection: sqlite3.Connection, intent_id: str) -> tuple[object, ...]:
        head = connection.execute(
            """SELECT intent_id, current_state, state_version, last_event_sequence,
                      lease_owner, lease_until, next_attempt_at, last_reason_code,
                      updated_at
                 FROM replication_heads WHERE intent_id=?""",
            (intent_id,),
        ).fetchone()
        if head is None:
            raise ReplicationStateUnavailable("replication intent head is missing")
        return head

    @classmethod
    def _head_view(cls, connection: sqlite3.Connection, intent_id: str) -> ReplicationHead:
        row = cls._head_row(connection, intent_id)
        event = cls._latest_event(connection, intent_id)
        attempt = int(event[6])
        retry_delay = (
            RETRY_DELAYS_SECONDS[min(attempt - 1, len(RETRY_DELAYS_SECONDS) - 1)]
            if row[1] == "retry_wait" and attempt > 0
            else 0
        )
        try:
            return ReplicationHead(
                intent_id=row[0],
                current_state=row[1],
                state_version=row[2],
                last_event_sequence=row[3],
                lease_owner=row[4],
                lease_until=row[5],
                next_attempt_at=row[6],
                last_reason_code=row[7],
                updated_at=row[8],
                attempt=attempt,
                retry_delay_seconds=retry_delay,
            )
        except (TypeError, ValueError) as exc:
            raise ReplicationStateUnavailable("replication intent head is invalid") from exc

    @staticmethod
    def _append_event_and_update_head(
        connection: sqlite3.Connection,
        *,
        intent_id: str,
        from_state: str,
        to_state: str,
        attempt: int,
        reason_code: str,
        occurred_at: str,
        lease_owner: str | None,
        lease_until: str | None,
        next_attempt_at: str,
        destination_replication_generation: str | None = None,
        destination_record_sha256: str | None = None,
        destination_head_sha256: str | None = None,
        event_type: str | None = None,
    ) -> None:
        head = ImmutableReplicationSidecarStore._head_row(connection, intent_id)
        if head[1] != from_state:
            raise ReplicationCASConflict("replication intent state changed")
        latest = ImmutableReplicationSidecarStore._latest_event(connection, intent_id)
        state_version = int(head[2]) + 1
        event_sequence = int(head[3]) + 1
        event = _event_values(
            intent_id=intent_id,
            event_sequence=event_sequence,
            prev_event_sha256=str(latest[13]),
            event_type=event_type or ("terminal" if to_state == "dead_letter" else "transition"),
            from_state=from_state,
            to_state=to_state,
            attempt=attempt,
            reason_code=reason_code,
            state_version=state_version,
            occurred_at=occurred_at,
            destination_replication_generation=destination_replication_generation,
            destination_record_sha256=destination_record_sha256,
            destination_head_sha256=destination_head_sha256,
        )
        columns = tuple(event)
        connection.execute(
            f"INSERT INTO replication_attempt_events ({','.join(columns)}) VALUES ({','.join('?' * len(columns))})",
            tuple(event[column] for column in columns),
        )
        connection.execute(
            """UPDATE replication_heads
                  SET current_state=?, state_version=?, last_event_sequence=?,
                      lease_owner=?, lease_until=?, next_attempt_at=?,
                      last_reason_code=?, updated_at=?
                WHERE intent_id=? AND state_version=?""",
            (
                to_state,
                state_version,
                event_sequence,
                lease_owner,
                lease_until,
                next_attempt_at,
                reason_code,
                occurred_at,
                intent_id,
                int(head[2]),
            ),
        )
        if connection.execute("SELECT changes()").fetchone() != (1,):
            raise ReplicationCASConflict("replication intent state-version CAS conflict")

    def claim_due(
        self,
        *,
        worker_id: str,
        now: str | None = None,
        lease_seconds: int = REPLICATION_LEASE_SECONDS,
    ) -> ReplicationClaim | None:
        """Claim at most one due intent with an immutable lease event."""
        if not isinstance(worker_id, str) or not (1 <= len(worker_id) <= 128):
            raise ReplicationDurabilityError("replication worker identity is invalid")
        if not isinstance(lease_seconds, int) or not 1 <= lease_seconds <= 3600:
            raise ReplicationDurabilityError("replication lease duration is invalid")
        occurred_at = now or _utc_now()
        _validate_utc_timestamp(occurred_at, "now")

        def mutate(connection: sqlite3.Connection) -> _MutationOutcome:
            row = connection.execute(
                """SELECT h.intent_id, h.current_state, h.state_version,
                          h.lease_owner, h.lease_until, h.next_attempt_at,
                          i.checkpoint_id, i.source_sequence
                     FROM replication_heads h JOIN replication_intents i ON i.intent_id=h.intent_id
                    WHERE (
                        (h.current_state IN ('pending','retry_wait') AND h.next_attempt_at <= ?)
                        OR (h.current_state IN ('copying','verifying') AND h.lease_until <= ?)
                    )
                    ORDER BY i.source_published_at, i.checkpoint_id
                    LIMIT 1""",
                (occurred_at, occurred_at),
            ).fetchone()
            if row is None:
                return _MutationOutcome(None, False)
            intent_id, state, _version, _owner, _until, _next, checkpoint_id, sequence = row
            if state in {"copying", "verifying"}:
                # A lease expiry is made replayable through the allow-listed
                # retry path.  The old worker cannot advance after this
                # state-version change, while the new claim remains bounded
                # and does not contact a destination or provider.
                latest = self._latest_event(connection, str(intent_id))
                self._append_event_and_update_head(
                    connection,
                    intent_id=str(intent_id),
                    from_state=str(state),
                    to_state="retry_wait",
                    attempt=int(latest[6]),
                    reason_code="RETRY_WAIT",
                    occurred_at=occurred_at,
                    lease_owner=None,
                    lease_until=None,
                    next_attempt_at=occurred_at,
                )
                state = "retry_wait"
            if state == "retry_wait":
                latest = self._latest_event(connection, str(intent_id))
                self._append_event_and_update_head(
                    connection,
                    intent_id=str(intent_id),
                    from_state="retry_wait",
                    to_state="pending",
                    attempt=int(latest[6]),
                    reason_code="RETRY_WAIT",
                    occurred_at=occurred_at,
                    lease_owner=None,
                    lease_until=None,
                    next_attempt_at=occurred_at,
                )
            head = self._head_row(connection, str(intent_id))
            latest = self._latest_event(connection, str(intent_id))
            attempt = int(latest[6]) + 1
            if attempt > MAX_REPLICATION_ATTEMPTS:
                self._append_event_and_update_head(
                    connection,
                    intent_id=str(intent_id),
                    from_state="pending",
                    to_state="dead_letter",
                    attempt=MAX_REPLICATION_ATTEMPTS,
                    reason_code="DEAD_LETTER",
                    occurred_at=occurred_at,
                    lease_owner=None,
                    lease_until=None,
                    next_attempt_at=occurred_at,
                )
                return _MutationOutcome(None, True)
            lease_until = _add_seconds(occurred_at, lease_seconds)
            self._append_event_and_update_head(
                connection,
                intent_id=str(intent_id),
                from_state="pending",
                to_state="copying",
                attempt=attempt,
                reason_code="NONE",
                occurred_at=occurred_at,
                lease_owner=worker_id,
                lease_until=lease_until,
                next_attempt_at=occurred_at,
                event_type="claim",
            )
            head = self._head_row(connection, str(intent_id))
            try:
                claim = ReplicationClaim(
                    intent_id=str(intent_id),
                    worker_id=worker_id,
                    current_state="copying",
                    state_version=head[2],
                    attempt=attempt,
                    lease_until=lease_until,
                    checkpoint_id=str(checkpoint_id),
                    source_sequence=int(sequence),
                )
            except (TypeError, ValueError) as exc:
                raise ReplicationStateUnavailable("replication claim is invalid") from exc
            return _MutationOutcome(claim, True)

        result = self._mutate_generation(mutate)
        if result is not None and not isinstance(result, ReplicationClaim):
            # A malformed pending row may have been terminalized above; a
            # caller receives no claim and the durable audit remains visible.
            return None
        return result

    def transition(
        self,
        *,
        intent_id: str,
        worker_id: str,
        expected_state_version: int,
        to_state: OutboxState,
        reason_code: ReplicationReason = "NONE",
        now: str | None = None,
        destination_replication_generation: str | None = None,
        destination_record_sha256: str | None = None,
        destination_head_sha256: str | None = None,
    ) -> ReplicationHead:
        """Append one allow-listed state transition under lease/CAS proof."""
        _validate_sha(intent_id, "intent_id")
        if not isinstance(worker_id, str) or not (1 <= len(worker_id) <= 128):
            raise ReplicationDurabilityError("replication worker identity is invalid")
        if not isinstance(expected_state_version, int) or expected_state_version < 1:
            raise ReplicationCASConflict("replication state-version expectation is invalid")
        if to_state not in _OUTBOX_STATES or to_state == "pending":
            raise ReplicationDurabilityError("replication transition is not allowed")
        if to_state == "replicated":
            # Completion is a destination-authority operation.  A generic
            # local transition must never be able to manufacture that state
            # from caller-supplied digests; the dedicated proof seam is not
            # implemented in this batch.
            raise ReplicationDurabilityError(
                "replication completion requires a dedicated verified destination proof"
            )
        if reason_code not in set(get_args(ReplicationReason)):
            raise ReplicationDurabilityError("replication reason is not allowed")
        occurred_at = now or _utc_now()
        _validate_utc_timestamp(occurred_at, "now")
        for value, field in (
            (destination_replication_generation, "destination_replication_generation"),
            (destination_record_sha256, "destination_record_sha256"),
            (destination_head_sha256, "destination_head_sha256"),
        ):
            if value is not None:
                _validate_sha(value, field)

        def mutate(connection: sqlite3.Connection) -> _MutationOutcome:
            head = self._head_row(connection, intent_id)
            if head[2] != expected_state_version:
                raise ReplicationCASConflict("replication state-version CAS conflict")
            current = str(head[1])
            if current not in {"copying", "verifying"}:
                raise ReplicationCASConflict("replication intent is not leased")
            if head[4] != worker_id or head[5] is None:
                raise ReplicationCASConflict("replication lease owner mismatch")
            if _timestamp_value(str(head[5])) <= _timestamp_value(occurred_at):
                raise ReplicationCASConflict("replication lease has expired")
            latest = self._latest_event(connection, intent_id)
            attempt = int(latest[6])
            next_state = to_state
            if next_state == "retry_wait":
                if not is_retryable_replication_reason(reason_code):
                    raise ReplicationDurabilityError("non-retryable reason cannot enter retry_wait")
                if attempt >= MAX_REPLICATION_ATTEMPTS:
                    next_state = "dead_letter"
                delay = RETRY_DELAYS_SECONDS[min(attempt - 1, len(RETRY_DELAYS_SECONDS) - 1)]
                next_at = _add_seconds(occurred_at, delay)
            else:
                next_at = occurred_at
            if next_state == "verifying" and current != "copying":
                raise ReplicationCASConflict("replication transition is not allowed")
            if next_state == "replicated" and current != "verifying":
                raise ReplicationCASConflict("replication transition is not allowed")
            if next_state == "dead_letter" and current not in {"copying", "verifying"}:
                raise ReplicationCASConflict("replication transition is not allowed")
            lease_owner = worker_id if next_state == "verifying" else None
            lease_until = str(head[5]) if next_state == "verifying" else None
            self._append_event_and_update_head(
                connection,
                intent_id=intent_id,
                from_state=current,
                to_state=next_state,
                attempt=attempt,
                reason_code=reason_code,
                occurred_at=occurred_at,
                lease_owner=lease_owner,
                lease_until=lease_until,
                next_attempt_at=next_at,
                destination_replication_generation=destination_replication_generation,
                destination_record_sha256=destination_record_sha256,
                destination_head_sha256=destination_head_sha256,
            )
            return _MutationOutcome(self._head_view(connection, intent_id), True)

        result = self._mutate_generation(mutate)
        if not isinstance(result, ReplicationHead):
            raise ReplicationDurabilityError("replication transition result is invalid")
        return result

    def complete_replication(self, proof: VerifiedDestinationCommitProof) -> ReplicationHead:
        """Advance a leased intent only after fresh destination readback."""
        verifier = self._destination_verifier
        if verifier is None or type(verifier) is not DestinationCommitVerifier:
            raise ReplicationDurabilityError(
                "replication completion requires a configured destination verifier"
            )
        verified = verifier.verify(proof)
        return _complete_replication_with_proof(self, verified)

    def get_intent(self, intent_id: str) -> ReplicationIntent:
        """Read one immutable intent from the strict sidecar without writes."""
        _validate_sha(intent_id, "intent_id")
        connection, root_fd, genesis_payload, entries = _open_generation_readonly(self.path)
        primary: BaseException | None = None
        try:
            _validate_sqlite_schema(connection)
            _validate_event_reachability(connection)
            columns = tuple(ReplicationIntent.model_fields)
            row = connection.execute(
                "SELECT " + ",".join(columns) + " FROM replication_intents WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            if row is None:
                raise ReplicationStateUnavailable("replication intent is unavailable")
            return ReplicationIntent.model_validate(dict(zip(columns, row, strict=True)))
        except (ReplicationStateUnavailable, ReplicationDurabilityError) as exc:
            primary = exc
            raise
        except (sqlite3.Error, TypeError, ValueError) as exc:
            primary = ReplicationStateUnavailable("replication intent is unavailable")
            raise primary from exc
        finally:
            failures: list[str] = []
            _close_connection_best_effort(connection, failures)
            _close_fd_best_effort(root_fd, "sidecar_root_fd", failures)
            _finish_cleanup(primary, failures)

    def import_journal_files(
        self,
        root: Path,
        *,
        operation_day: str,
        destination_id: str,
        created_at: str | None = None,
    ) -> ReplicationImportResult:
        """Read and import no-follow journal files, then best-effort unlink them.

        A failed unlink intentionally leaves the complete journal available for
        a later idempotent reconciliation; it can never remove a committed
        intent or rewrite a generation.
        """
        root = _physical_path(Path(root))
        if not root.is_absolute():
            raise ReplicationDurabilityError("replication journal root must be absolute")
        root_fd, descriptors = _open_directory_chain(root, create=False)
        records: list[tuple[JournalRecord, str]] = []
        primary: BaseException | None = None
        try:
            root_baseline = _directory_fingerprint(os.fstat(root_fd))
            names = sorted(os.listdir(root_fd))
            _assert_directory_fingerprint(root_fd, root_baseline)
            for name in names:
                if not isinstance(name, str) or not re.fullmatch(r"[0-9a-f]{64}\.json", name):
                    raise ReplicationStateUnavailable("replication journal namespace is invalid")
                payload, info = _read_at(root_fd, name)
                if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
                    raise ReplicationStateUnavailable("replication journal entry is unsafe")
                record = JournalRecord._from_payload(name, payload)
                records.append((record, name))
                _assert_directory_fingerprint(root_fd, root_baseline)
            _assert_directory_fingerprint(root_fd, root_baseline)
            result = self.import_journals(
                [record for record, _name in records],
                operation_day=operation_day,
                destination_id=destination_id,
                created_at=created_at,
            )
            _assert_directory_fingerprint(root_fd, root_baseline)
            # The generation is already durable.  Unlinking is an optimization
            # and is deliberately outside the transaction so a crash is harmless.
            expected_root = root_baseline
            for _record, name in records:
                _assert_directory_fingerprint(root_fd, expected_root)
                try:
                    os.unlink(name, dir_fd=root_fd)
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    raise ReplicationDurabilityError(
                        "replication journal cleanup is unavailable"
                    ) from exc
                else:
                    # Directory link-count behavior differs across filesystems:
                    # macOS decrements it for this unlink, while some POSIX
                    # filesystems leave it unchanged for regular children.
                    # Re-read the held fd and permit only those two outcomes;
                    # device/inode/mode must remain exact.
                    actual_root = _directory_fingerprint(os.fstat(root_fd))
                    if actual_root[:3] != expected_root[:3] or actual_root[3] not in {
                        expected_root[3],
                        expected_root[3] - 1,
                    }:
                        raise ReplicationStateUnavailable(
                            "replication directory changed during cleanup"
                        )
                    expected_root = actual_root
                _assert_directory_fingerprint(root_fd, expected_root)
            _fsync_open_directory(root_fd)
            _assert_directory_fingerprint(root_fd, expected_root)
            return result
        except (ReplicationStateUnavailable, ReplicationDurabilityError) as exc:
            primary = exc
            raise
        except (OSError, TypeError, ValueError) as exc:
            primary = ReplicationStateUnavailable("replication journal is unavailable")
            raise primary from exc
        finally:
            failures: list[str] = []
            _close_descriptors(descriptors, failures)
            _finish_cleanup(primary, failures)

    def _fsync_database(self) -> None:
        _fsync_directory(self.path)

    def read_status(
        self,
        *,
        local_ready: bool = False,
        source_instance: SourceInstanceRecord | None = None,
    ) -> ReplicationStatusResponse:
        connection: sqlite3.Connection | None = None
        root_fd: int | None = None
        counts: dict[str, int] = {}
        replicated: tuple[object, ...] | None = None
        row: tuple[object, ...] | None = None
        primary_error: BaseException | None = None
        try:
            connection, root_fd, genesis_payload, entries = _open_generation_readonly(self.path)
            connection.execute("PRAGMA query_only = ON")
            _validate_sqlite_schema(connection)
            meta = connection.execute(
                """SELECT schema_version, schema_identity, ddl_sha256, schema_digest,
                          source_instance_id, source_instance_sha256, created_at
                     FROM replication_sidecar_meta"""
            ).fetchall()
            if len(meta) != 1 or meta[0][:4] != (
                1,
                SIDECAR_SCHEMA_IDENTITY,
                SIDECAR_DDL_SHA256,
                SIDECAR_SCHEMA_DIGEST,
            ):
                raise ReplicationStateUnavailable("replication sidecar identity is invalid")
            _validate_sha(meta[0][4], "source_instance_id")
            _validate_sha(meta[0][5], "source_instance_sha256")
            _validate_utc_timestamp(meta[0][6], "created_at")
            if source_instance is not None and (
                meta[0][4] != source_instance.source_instance_id
                or meta[0][5] != source_instance.source_instance_sha256
            ):
                raise ReplicationDurabilityError("replication source identity is invalid")
            counts = dict(
                connection.execute(
                    "SELECT current_state, COUNT(*) FROM replication_heads GROUP BY current_state"
                ).fetchall()
            )
            if set(counts) - {
                "pending",
                "copying",
                "verifying",
                "retry_wait",
                "replicated",
                "dead_letter",
            }:
                raise ReplicationStateUnavailable("replication sidecar state is invalid")
            _validate_event_reachability(
                connection,
                expected_source_instance_id=meta[0][4],
                expected_source_instance_sha256=meta[0][5],
            )
            replicated = connection.execute(
                """SELECT i.manifest_canonical_sha256, e.occurred_at
                     FROM replication_heads h JOIN replication_intents i ON i.intent_id=h.intent_id
                     JOIN replication_attempt_events e ON e.intent_id=h.intent_id AND e.event_sequence=h.last_event_sequence
                    WHERE h.current_state='replicated' ORDER BY e.occurred_at DESC LIMIT 1"""
            ).fetchone()
            row = connection.execute(
                """SELECT destination_id, descriptor_sha256, head_sha256, replication_generation,
                          record_sha256, source_instance_id, source_sequence, health_state,
                          health_observed_at, cache_version, updated_at
                     FROM replication_destination_cache ORDER BY updated_at DESC LIMIT 1"""
            ).fetchone()
            _compare_generation_snapshot(root_fd, genesis_payload=genesis_payload, entries=entries)
        except (ReplicationStateUnavailable, ReplicationDurabilityError) as exc:
            primary_error = exc
            raise
        except (sqlite3.Error, OSError, ValueError, TypeError) as exc:
            primary_error = ReplicationStateUnavailable("replication sidecar status is unavailable")
            raise primary_error from exc
        finally:
            cleanup_failures: list[str] = []
            if connection is not None:
                _close_connection_best_effort(connection, cleanup_failures)
            if root_fd is not None:
                _close_fd_best_effort(root_fd, "sidecar_root_fd", cleanup_failures)
            _finish_cleanup(primary_error, cleanup_failures)
        if row is None:
            health, observed = "unknown", None
        else:
            try:
                (
                    destination_id,
                    descriptor_sha256,
                    head_sha256,
                    generation,
                    record_sha256,
                    cache_source,
                    sequence,
                    health,
                    observed,
                    cache_version,
                    updated_at,
                ) = row
                if not isinstance(destination_id, str) or len(destination_id) != 32:
                    raise ReplicationStateUnavailable("replication cache identity is invalid")
                for digest, name in (
                    (descriptor_sha256, "descriptor_sha256"),
                    (head_sha256, "head_sha256"),
                    (generation, "replication_generation"),
                    (record_sha256, "record_sha256"),
                    (cache_source, "source_instance_id"),
                ):
                    if digest is not None:
                        _validate_sha(digest, name)
                if sequence is not None and (not isinstance(sequence, int) or sequence < 1):
                    raise ReplicationStateUnavailable("replication cache sequence is invalid")
                if health not in {"unknown", "healthy", "unavailable", "unsupported"} or (
                    health == "unknown"
                ) != (observed is None):
                    raise ReplicationStateUnavailable("replication cache state is invalid")
                for timestamp, name in (
                    (observed, "health_observed_at"),
                    (updated_at, "updated_at"),
                ):
                    if timestamp is not None:
                        _validate_utc_timestamp(timestamp, name)
                if not isinstance(cache_version, int) or cache_version < 0:
                    raise ReplicationStateUnavailable("replication cache version is invalid")
            except (TypeError, ValueError) as exc:
                raise ReplicationStateUnavailable(
                    "replication cache projection is invalid"
                ) from exc
        return ReplicationStatusResponse(
            status="ready" if not counts else "degraded",
            reason_code="NONE",
            enabled=True,
            source_ready=True,
            destination_configured=False,
            outbox_schema_version=1,
            pending_count=int(counts.get("pending", 0)),
            copying_count=int(counts.get("copying", 0)),
            verifying_count=int(counts.get("verifying", 0)),
            retry_wait_count=int(counts.get("retry_wait", 0)),
            dead_letter_count=int(counts.get("dead_letter", 0)),
            last_replicated_source_manifest_sha256=replicated[0] if replicated else None,
            last_replicated_at=replicated[1] if replicated else None,
            local_ready=local_ready,
            destination_health=health,
            destination_health_observed_at=observed,
            queue_lag_seconds=None,
            lag_seconds=None,
            effects=ReplicationEffects(
                writes=False,
                canonical_writes=False,
                destination_writes=False,
                outbox_writes=False,
                restore_writes=False,
            ),
        )


# Keep the public symbol stable; the old mutable implementation above is not
# reachable by callers after this binding and its pathname is never opened.
ReplicationSidecarStore = ImmutableReplicationSidecarStore  # noqa: F811


class ReplicationOutboxService:
    """Small local-only facade used by the future publication observer.

    It is intentionally not wired to canonical publication, NAS or a
    provider in this batch.  The disabled branch returns before touching the
    sidecar path, which keeps status/automation zero-write by construction.
    """

    def __init__(
        self,
        path: Path,
        *,
        enabled: bool = True,
        source_instance_id: str | None = None,
        source_instance_sha256: str | None = None,
    ) -> None:
        self.path = _sidecar_root(Path(path))
        self.enabled = enabled
        self.source_instance_id = source_instance_id
        self.source_instance_sha256 = source_instance_sha256

    def enqueue(
        self,
        checkpoint: SourceCheckpoint | Mapping[str, object],
        *,
        operation_day: str,
        destination_id: str,
        created_at: str | None = None,
    ) -> ReplicationOutboxResult:
        if not self.enabled:
            return ReplicationOutboxResult(status="disabled", reason_code="DISABLED")
        if self.source_instance_id is None or self.source_instance_sha256 is None:
            return ReplicationOutboxResult(
                status="unavailable", reason_code="SOURCE_NOT_CONFIGURED"
            )
        store = ReplicationSidecarStore(self.path)
        try:
            if not (self.path / SIDECAR_GENESIS_NAME).is_file():
                store.initialize(
                    source_instance_id=self.source_instance_id,
                    source_instance_sha256=self.source_instance_sha256,
                    created_at=created_at,
                )
            result = store.enqueue_checkpoint(
                checkpoint,
                operation_day=operation_day,
                destination_id=destination_id,
                created_at=created_at,
            )
        except ReplicationDurabilityError:
            return ReplicationOutboxResult(
                status="unavailable", reason_code="OUTBOX_DURABILITY_UNAVAILABLE"
            )
        return ReplicationOutboxResult(
            status="queued" if result.imported_count else "existing",
            reason_code="NONE",
            intent_ids=result.intent_ids,
            source_sequences=result.source_sequences,
            outbox_writes=bool(result.imported_count),
        )

    def import_journals(
        self,
        records: Iterable[JournalRecord],
        *,
        operation_day: str,
        destination_id: str,
        created_at: str | None = None,
    ) -> ReplicationOutboxResult:
        if not self.enabled:
            return ReplicationOutboxResult(status="disabled", reason_code="DISABLED")
        try:
            if not (self.path / SIDECAR_GENESIS_NAME).is_file():
                if self.source_instance_id is None or self.source_instance_sha256 is None:
                    return ReplicationOutboxResult(
                        status="unavailable", reason_code="SOURCE_NOT_CONFIGURED"
                    )
                ReplicationSidecarStore(self.path).initialize(
                    source_instance_id=self.source_instance_id,
                    source_instance_sha256=self.source_instance_sha256,
                    created_at=created_at,
                )
            result = ReplicationSidecarStore(self.path).import_journals(
                records,
                operation_day=operation_day,
                destination_id=destination_id,
                created_at=created_at,
            )
        except ReplicationDurabilityError:
            return ReplicationOutboxResult(
                status="unavailable", reason_code="OUTBOX_DURABILITY_UNAVAILABLE"
            )
        return ReplicationOutboxResult(
            status="queued" if result.imported_count else "existing",
            reason_code="NONE",
            intent_ids=result.intent_ids,
            source_sequences=result.source_sequences,
            outbox_writes=bool(result.imported_count),
        )

    def claim_due(
        self,
        *,
        worker_id: str,
        now: str | None = None,
        lease_seconds: int = REPLICATION_LEASE_SECONDS,
    ) -> ReplicationClaim | None:
        if not self.enabled:
            return None
        return ReplicationSidecarStore(self.path).claim_due(
            worker_id=worker_id, now=now, lease_seconds=lease_seconds
        )


class ReplicationStatusService:
    def __init__(self, settings: Settings, *, local_ready: bool = False) -> None:
        self.settings = settings
        self.local_ready = local_ready

    def read(self) -> ReplicationStatusResponse:
        if not self.settings.replication_enabled:
            return _disabled_status(self.local_ready)
        if self.settings.local_market_dataset_root is None:
            return _unavailable_status("SOURCE_NOT_CONFIGURED", self.local_ready)
        layout = StorageLayout(self.settings)
        source_instance_path = layout.replication_source_instance
        if source_instance_path is None:
            return _unavailable_status("SOURCE_NOT_CONFIGURED", self.local_ready)
        sidecar = ReplicationSidecarStore(layout.replication_sidecar_root)
        try:
            # Establish sidecar availability first.  A missing/corrupt sidecar
            # is REPLICATION_STATE_UNAVAILABLE even when source evidence is
            # also absent; this is the status surface's fixed precedence.
            sidecar.read_status(local_ready=self.local_ready)
        except ReplicationStateUnavailable:
            return _unavailable_status("REPLICATION_STATE_UNAVAILABLE", self.local_ready)
        except ReplicationDurabilityError:
            return _unavailable_status("REPLICATION_STATE_UNAVAILABLE", self.local_ready)
        try:
            source_instance = SourceInstanceStore(source_instance_path).read(
                canonical_root_path=self.settings.local_market_dataset_root
            )
            status = sidecar.read_status(
                local_ready=self.local_ready,
                source_instance=source_instance,
            )
            values = status.model_dump()
            values["destination_configured"] = (
                self.settings.replication_destination_root is not None
            )
            return ReplicationStatusResponse.model_validate(values)
        except ReplicationStateUnavailable:
            return _unavailable_status("REPLICATION_STATE_UNAVAILABLE", self.local_ready)
        except ReplicationDurabilityError:
            return _unavailable_status("OUTBOX_DURABILITY_UNAVAILABLE", self.local_ready)


def _disabled_status(local_ready: bool) -> ReplicationStatusResponse:
    return ReplicationStatusResponse(
        status="disabled",
        reason_code="DISABLED",
        enabled=False,
        source_ready=False,
        destination_configured=False,
        outbox_schema_version=None,
        pending_count=0,
        copying_count=0,
        verifying_count=0,
        retry_wait_count=0,
        dead_letter_count=0,
        last_replicated_source_manifest_sha256=None,
        last_replicated_at=None,
        local_ready=local_ready,
        destination_health="unknown",
        destination_health_observed_at=None,
        queue_lag_seconds=None,
        lag_seconds=None,
        effects=ReplicationEffects(
            writes=False,
            canonical_writes=False,
            destination_writes=False,
            outbox_writes=False,
            restore_writes=False,
        ),
    )


def _unavailable_status(reason_code: str, local_ready: bool) -> ReplicationStatusResponse:
    result = _disabled_status(local_ready)
    values = result.model_dump()
    values.update(status="unavailable", reason_code=reason_code, enabled=True)
    return ReplicationStatusResponse.model_validate(values)


# ---------------------------------------------------------------------------
# Batch 3: offline destination archive primitives
# ---------------------------------------------------------------------------

# The descriptor's schema identity is distinct from its hash domain.  Keep
# this value aligned with the wire record described by the R2-F4.3 spec;
# ``destination-descriptor/v1`` remains the domain separator for its digest.
DESTINATION_DESCRIPTOR_SCHEMA = "stock-eva/r2f4.3/destination/v1"
DESTINATION_SENTINEL_SCHEMA = "stock-eva/r2f4.3/destination-sentinel/v1"
DESTINATION_RECORD_SCHEMA = "stock-eva/r2f4.3/replication-record/v1"
DESTINATION_HEAD_SCHEMA = "stock-eva/r2f4.3/replication-head/v1"
DESTINATION_DESCRIPTOR_NAME = ".stock-eva-replication-destination.json"
DESTINATION_SENTINEL_NAME = ".stock-eva-dataset.json"
DESTINATION_REPLICATION_DIR = "_replication"
DESTINATION_HISTORY_DIR = "history"
DESTINATION_HEAD_HISTORY_DIR = "head-history"
DESTINATION_STAGING_DIR = ".staging"
DESTINATION_LOCK_NAME = ".writer.lock"
DESTINATION_INIT_ACK = "CREATE_EMPTY_NAS_ARCHIVE_R2F4_3"
DESTINATION_HEAD_SLOT_PATTERN = r"slot-[0-9]{20}\.json"
_DESTINATION_CONTROL_NAMES = frozenset(
    {
        DESTINATION_DESCRIPTOR_NAME,
        DESTINATION_SENTINEL_NAME,
        "manifest.json",
        DESTINATION_REPLICATION_DIR,
    }
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _parse_destination_manifest(
    payload: bytes, *, expected_sha256: str | None = None
) -> Mapping[str, object]:
    """Parse the published manifest without changing canonical manifest bytes.

    The canonical dataset manifest model remains owned by ``storage.models``;
    this boundary only proves that the copied bytes are stable JSON and an
    object, while the checkpoint/object inventory supplies the immutable
    content contract. Existing canonical manifests use their established
    serializer, so replication must preserve their bytes rather than impose a
    second JSON encoding.
    """
    if expected_sha256 is not None and _sha256_bytes(payload) != expected_sha256:
        raise ReplicationStateUnavailable("destination manifest hash mismatch")
    try:
        values = json.loads(payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ReplicationStateUnavailable("destination manifest is invalid") from exc
    if not isinstance(values, Mapping):
        raise ReplicationStateUnavailable("destination manifest is invalid")
    return values


def _destination_root_error(path: Path) -> ReplicationDurabilityError:
    del path
    return ReplicationDurabilityError("destination trust validation failed")


def _validate_destination_root(path: Path, *, local_root: Path | None = None) -> Path:
    """Validate an explicit destination lexically and through no-follow lstat."""
    raw = str(path)
    if (
        not path.is_absolute()
        or any(token in raw for token in ("$", "~", "`"))
        or path == Path("/")
        or path == Path.home()
    ):
        raise _destination_root_error(path)
    normalized = _physical_path(Path(os.path.normpath(path)))
    if local_root is not None:
        local = _physical_path(Path(os.path.normpath(local_root)))
        if (
            not local.is_absolute()
            or normalized == local
            or normalized in local.parents
            or local in normalized.parents
        ):
            raise _destination_root_error(path)
    try:
        _safe_parent(normalized)
        info = os.lstat(normalized)
    except FileNotFoundError:
        # A separate init operation may create a new child, but it may never
        # infer/create a missing parent or turn a missing path into a source.
        return normalized
    except OSError as exc:
        raise _destination_root_error(path) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise _destination_root_error(path)
    return normalized


def _validate_source_root(path: Path) -> Path:
    """Validate an explicit local source without destination home policy."""
    raw = str(path)
    if (
        not path.is_absolute()
        or any(token in raw for token in ("$", "~", "`"))
        or path == Path("/")
    ):
        raise ReplicationDurabilityError("source root is unsafe")
    normalized = _physical_path(Path(os.path.normpath(path)))
    try:
        _safe_parent(normalized)
        info = os.lstat(normalized)
    except (FileNotFoundError, OSError) as exc:
        raise ReplicationDurabilityError("source root is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ReplicationDurabilityError("source root is unsafe")
    return normalized


def _destination_mount_fingerprint(
    *,
    mount_point: str,
    fs_type: str,
    normalized_options: str,
    st_dev: int,
    volume_id: str | None,
) -> str:
    return domain_sha256(
        "stock-eva/r2f4.3/mount-fingerprint/v1",
        {
            "mount_point": mount_point,
            "fs_type": fs_type,
            "normalized_options": normalized_options,
            "st_dev": st_dev,
            "volume_id": volume_id,
        },
    )


def _normalize_mount_options(options: Iterable[str]) -> str:
    values = sorted({str(option).strip() for option in options if str(option).strip()})
    if any("," in value or "\n" in value or "\r" in value for value in values):
        raise ReplicationDurabilityError("destination mount options are invalid")
    return ",".join(values)


def _destination_live_mount_identity(
    descriptor: DestinationDescriptor,
    root_fd: int,
    mount_inspector: MountInspector,
) -> None:
    """Compare persisted expectations with an independent live mount probe."""
    try:
        info = mount_inspector.find_mount(descriptor.root_path)
        root_info = os.fstat(root_fd)
    except (OSError, TypeError, ValueError) as exc:
        raise ReplicationStateUnavailable("destination mount identity is unavailable") from exc
    if info is None:
        raise ReplicationStateUnavailable("destination mount identity is unavailable")
    try:
        live_mount_point = str(_physical_path(Path(info.mount_point)))
        live_fs_type = str(info.filesystem_type).lower()
        live_options = _normalize_mount_options(info.options)
        live_volume_id = getattr(info, "volume_id", None)
    except (TypeError, ValueError) as exc:
        raise ReplicationStateUnavailable("destination mount identity is invalid") from exc
    if live_fs_type in {"smb", "smbfs", "cifs", "nfs", "sshfs"}:
        raise ReplicationDurabilityError("destination mount is unsupported")
    try:
        _physical_path(descriptor.root_path).relative_to(_physical_path(Path(live_mount_point)))
    except ValueError as exc:
        raise ReplicationStateUnavailable("destination root is outside live mount") from exc
    if (
        (int(root_info.st_dev) != descriptor.root_dev)
        or live_mount_point != descriptor.mount_point
        or live_fs_type != descriptor.fs_type.lower()
        or live_options != descriptor.normalized_options
        or live_volume_id != descriptor.volume_id
    ):
        raise ReplicationStateUnavailable("destination mount identity changed")


class DestinationDescriptor(BaseModel):
    """Closed private trust descriptor for one explicit archive root."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    descriptor_schema: Literal[DESTINATION_DESCRIPTOR_SCHEMA] = DESTINATION_DESCRIPTOR_SCHEMA
    schema_version: Literal[1] = 1
    dataset: Literal["stock-eva-market"] = "stock-eva-market"
    role: Literal["nas_archive"] = "nas_archive"
    direction: Literal["local_to_nas"] = "local_to_nas"
    root_dev: int = Field(gt=0)
    root_ino: int = Field(gt=0)
    parent_dev: int = Field(gt=0)
    parent_ino: int = Field(gt=0)
    mount_point: str = Field(min_length=1)
    fs_type: str = Field(min_length=1, max_length=64)
    normalized_options: str = Field(max_length=4096)
    volume_id: str | None = Field(default=None, max_length=256)
    mount_generation: str = Field(min_length=32, max_length=128, pattern=r"^[0-9a-f]+$")
    mount_fingerprint: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    sentinel_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    single_writer_host_id: str = Field(min_length=1, max_length=128)
    created_at: str = Field(min_length=20)
    descriptor_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    _root_path: Path | None = PrivateAttr(default=None)

    _created_at_is_utc = field_validator("created_at")(
        lambda value: _validate_utc_timestamp(value, "created_at")
    )

    @model_validator(mode="before")
    @classmethod
    def exact_fields(cls, value: object) -> object:
        if isinstance(value, Mapping) and set(value) != set(cls.model_fields):
            raise ValueError("destination descriptor fields are not exact")
        return value

    @classmethod
    def from_root(
        cls,
        root: Path,
        *,
        single_writer_host_id: str = "local-host",
        sentinel_sha256: str = ZERO_SHA256,
        mount_point: str | None = None,
        fs_type: str = "local",
        normalized_options: str = "",
        volume_id: str | None = None,
        mount_generation: str | None = None,
        created_at: str | None = None,
        local_root: Path | None = None,
        mount_inspector: MountInspector | None = None,
    ) -> DestinationDescriptor:
        bound = _validate_destination_root(Path(root), local_root=local_root)
        if not bound.is_dir():
            raise _destination_root_error(bound)
        if not single_writer_host_id or len(single_writer_host_id) > 128:
            raise _destination_root_error(bound)
        if fs_type.lower() in {"smb", "smbfs", "cifs", "nfs", "sshfs"}:
            raise ReplicationDurabilityError("destination mount is unsupported")
        try:
            root_info = os.stat(bound, follow_symlinks=False)
            parent_info = os.stat(bound.parent, follow_symlinks=False)
        except OSError as exc:
            raise _destination_root_error(bound) from exc
        inspector = mount_inspector or SystemMountInspector()
        mount_info = inspector.find_mount(bound)
        if mount_info is None:
            raise ReplicationDurabilityError("destination mount is unavailable")
        live_mount_point = str(_physical_path(Path(mount_info.mount_point)))
        live_fs_type = str(mount_info.filesystem_type).lower()
        if live_fs_type in {"smb", "smbfs", "cifs", "nfs", "sshfs"}:
            raise ReplicationDurabilityError("destination mount is unsupported")
        # ``local`` is an offline fixture placeholder; the persisted
        # descriptor records the inspector's actual filesystem identity.
        mount_name = (
            live_mount_point if mount_point is None or mount_point == str(bound) else mount_point
        )
        descriptor_fs_type = live_fs_type if fs_type == "local" else fs_type
        descriptor_options = _normalize_mount_options(mount_info.options)
        descriptor_volume_id = getattr(mount_info, "volume_id", None)
        generation = mount_generation or secrets.token_hex(32)
        fingerprint = _destination_mount_fingerprint(
            mount_point=mount_name,
            fs_type=descriptor_fs_type,
            normalized_options=descriptor_options,
            st_dev=int(root_info.st_dev),
            volume_id=descriptor_volume_id if volume_id is None else volume_id,
        )
        values: dict[str, object] = {
            "descriptor_schema": DESTINATION_DESCRIPTOR_SCHEMA,
            "schema_version": 1,
            "dataset": "stock-eva-market",
            "role": "nas_archive",
            "direction": "local_to_nas",
            "root_dev": int(root_info.st_dev),
            "root_ino": int(root_info.st_ino),
            "parent_dev": int(parent_info.st_dev),
            "parent_ino": int(parent_info.st_ino),
            "mount_point": mount_name,
            "fs_type": descriptor_fs_type,
            "normalized_options": descriptor_options,
            "volume_id": descriptor_volume_id if volume_id is None else volume_id,
            "mount_generation": generation,
            "mount_fingerprint": fingerprint,
            "sentinel_sha256": sentinel_sha256,
            "single_writer_host_id": single_writer_host_id,
            "created_at": created_at or _utc_now(),
        }
        values["descriptor_sha256"] = domain_sha256(
            "stock-eva/r2f4.3/destination-descriptor/v1", values
        )
        descriptor = cls.model_validate(values)
        descriptor._root_path = bound
        return descriptor

    @property
    def destination_id(self) -> str:
        return self.descriptor_sha256[:32]

    @property
    def root_path(self) -> Path:
        if self._root_path is None:
            raise ReplicationDurabilityError("destination root is not bound")
        return self._root_path

    def hash_preimage(self) -> dict[str, object]:
        values = self.model_dump(mode="json")
        values.pop("descriptor_sha256")
        return values

    def verify(self) -> None:
        expected = domain_sha256("stock-eva/r2f4.3/destination-descriptor/v1", self.hash_preimage())
        if expected != self.descriptor_sha256:
            raise ReplicationDurabilityError("destination descriptor hash is invalid")
        if self.direction != "local_to_nas" or self.role != "nas_archive":
            raise ReplicationDurabilityError("destination direction is not allowed")
        if self.fs_type.lower() in {"smb", "smbfs", "cifs", "nfs", "sshfs"}:
            raise ReplicationDurabilityError("destination mount is unsupported")
        _validate_sha(self.sentinel_sha256, "sentinel_sha256")
        _validate_sha(self.mount_fingerprint, "mount_fingerprint")
        _validate_utc_timestamp(self.created_at, "created_at")

    def verify_bound(self) -> None:
        self.verify()
        root = _physical_path(self.root_path)
        if _validate_destination_root(root) != root:
            raise ReplicationDurabilityError("destination root changed")
        try:
            root_info = os.stat(root, follow_symlinks=False)
            parent_info = os.stat(root.parent, follow_symlinks=False)
        except OSError as exc:
            raise ReplicationDurabilityError("destination mount is unavailable") from exc
        if (int(root_info.st_dev), int(root_info.st_ino)) != (self.root_dev, self.root_ino):
            raise ReplicationDurabilityError("destination root was rebound")
        if (int(parent_info.st_dev), int(parent_info.st_ino)) != (self.parent_dev, self.parent_ino):
            raise ReplicationDurabilityError("destination parent was rebound")
        current_fp = _destination_mount_fingerprint(
            mount_point=self.mount_point,
            fs_type=self.fs_type,
            normalized_options=self.normalized_options,
            st_dev=int(root_info.st_dev),
            volume_id=self.volume_id,
        )
        # A descriptor produced with an explicit synthetic mount fingerprint
        # remains valid only when it uses the same local probe inputs.  The
        # initializer always uses these values; an imported descriptor with a
        # different mount identity fails closed.
        if current_fp != self.mount_fingerprint:
            raise ReplicationDurabilityError("destination mount fingerprint changed")

    def verify_root_fd(self, root_fd: int) -> None:
        """Verify a caller-owned root descriptor without reopening its path."""
        self.verify()
        try:
            info = os.fstat(root_fd)
        except OSError as exc:
            raise ReplicationDurabilityError("destination root descriptor is unavailable") from exc
        if not stat.S_ISDIR(info.st_mode) or (int(info.st_dev), int(info.st_ino)) != (
            self.root_dev,
            self.root_ino,
        ):
            raise ReplicationDurabilityError("destination root was rebound")

    def persist(self) -> Path:
        self.verify_bound()
        path = self.root_path / DESTINATION_DESCRIPTOR_NAME
        _install_no_replace(path, canonical_json_bytes(self.model_dump(mode="json")))
        return path

    @classmethod
    def read(cls, path: Path) -> DestinationDescriptor:
        payload, _ = _read_nofollow(path)
        try:
            model = cls.model_validate(json.loads(payload))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ReplicationDurabilityError("destination descriptor is invalid") from exc
        if canonical_json_bytes(model.model_dump(mode="json")) != payload:
            raise ReplicationDurabilityError("destination descriptor is not canonical")
        model._root_path = _physical_path(Path(path).parent)
        model.verify_bound()
        return model

    load = read


def _destination_open_dir(parent_fd: int, name: str) -> int:
    try:
        fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise ReplicationDurabilityError("destination directory is unavailable") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode):
            raise ReplicationDurabilityError("destination component is not a directory")
        return fd
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def _destination_open_dirs(root_fd: int, components: tuple[str, ...]) -> tuple[int, list[int]]:
    current = root_fd
    opened: list[int] = []
    try:
        for name in components:
            if not name or name in {".", ".."} or "/" in name or "\\" in name:
                raise ReplicationDurabilityError("destination path component is unsafe")
            current = _destination_open_dir(current, name)
            opened.append(current)
        return current, opened
    except BaseException:
        failures: list[str] = []
        _close_descriptors(opened, failures)
        _finish_cleanup(None, failures)
        raise


def _destination_read_at(
    root_fd: int,
    components: tuple[str, ...],
    *,
    optional: bool = False,
    require_private_mode: bool = True,
) -> tuple[bytes, os.stat_result] | None:
    if not components:
        raise ReplicationDurabilityError("destination artifact name is empty")
    parent_fd = root_fd
    dirs: list[int] = []
    fd: int | None = None
    primary: BaseException | None = None
    try:
        if len(components) > 1:
            try:
                parent_fd, dirs = _destination_open_dirs(root_fd, components[:-1])
            except ReplicationDurabilityError:
                if optional:
                    try:
                        os.stat(components[0], dir_fd=root_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        return None
                raise
        name = components[-1]
        if not name or name in {".", ".."} or "/" in name or "\\" in name:
            raise ReplicationDurabilityError("destination artifact name is unsafe")
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            if optional:
                return None
            raise ReplicationDurabilityError("destination artifact is unavailable") from None
        return _read_descriptor(fd, require_private_mode=require_private_mode)
    except ReplicationDurabilityError as exc:
        primary = exc
        raise
    except OSError as exc:
        primary = ReplicationDurabilityError("destination artifact read failed")
        raise primary from exc
    finally:
        failures: list[str] = []
        if fd is not None:
            _close_fd_best_effort(fd, "destination_artifact_fd", failures)
        _close_descriptors(dirs, failures)
        _finish_cleanup(primary, failures)


def _destination_open_relative_file(root_fd: int, relative_path: str) -> tuple[int, list[int]]:
    try:
        candidate = Path(relative_path)
    except TypeError as exc:
        raise ReplicationDurabilityError("destination object path is unsafe") from exc
    if (
        candidate.is_absolute()
        or "\\" in relative_path
        or any(part in {"", ".", ".."} for part in relative_path.split("/"))
    ):
        raise ReplicationDurabilityError("destination object path is unsafe")
    parts = tuple(relative_path.split("/"))
    parent_fd = root_fd
    dirs: list[int] = []
    try:
        for name in parts[:-1]:
            parent_fd = _destination_open_dir(parent_fd, name)
            dirs.append(parent_fd)
        fd = os.open(
            parts[-1],
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=parent_fd,
        )
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            os.close(fd)
            raise ReplicationDurabilityError("destination object is not regular")
        return fd, dirs
    except OSError as exc:
        failures: list[str] = []
        _close_descriptors(dirs, failures)
        primary = ReplicationDurabilityError("destination object is unavailable")
        _finish_cleanup(primary, failures)
        raise primary from exc
    except BaseException:
        failures: list[str] = []
        _close_descriptors(dirs, failures)
        _finish_cleanup(None, failures)
        raise


def _destination_read_relative(root_fd: int, relative_path: str) -> bytes:
    fd, dirs = _destination_open_relative_file(root_fd, relative_path)
    primary: BaseException | None = None
    try:
        payload, _ = _read_descriptor(fd, require_private_mode=False)
        return payload
    except BaseException as exc:
        primary = exc
        raise
    finally:
        failures: list[str] = []
        _close_fd_best_effort(fd, "destination_source_fd", failures)
        _close_descriptors(dirs, failures)
        _finish_cleanup(primary, failures)


def _destination_list_files(dir_fd: int, prefix: str = "") -> set[str]:
    """Enumerate only regular, no-follow files below a bound generation fd."""
    found: set[str] = set()
    try:
        names = os.listdir(dir_fd)
    except OSError as exc:
        raise ReplicationStateUnavailable("destination generation listing is unavailable") from exc
    for name in names:
        if not name or name in {".", ".."} or "/" in name or "\\" in name:
            raise ReplicationStateUnavailable("destination generation entry is unsafe")
        relative = f"{prefix}/{name}" if prefix else name
        try:
            info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except OSError as exc:
            raise ReplicationStateUnavailable(
                "destination generation entry is unavailable"
            ) from exc
        if stat.S_ISLNK(info.st_mode):
            raise ReplicationStateUnavailable("destination generation entry is a symlink")
        if stat.S_ISREG(info.st_mode):
            found.add(relative)
            continue
        if not stat.S_ISDIR(info.st_mode):
            raise ReplicationStateUnavailable("destination generation entry type is invalid")
        child = _destination_open_dir(dir_fd, name)
        try:
            found.update(_destination_list_files(child, relative))
        finally:
            failures: list[str] = []
            _close_fd_best_effort(child, "destination_generation_child_fd", failures)
            _finish_cleanup(None, failures)
    return found


def _destination_mkdir_at(parent_fd: int, name: str) -> int:
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    except OSError as exc:
        raise ReplicationDurabilityError("destination staging is unavailable") from exc
    return _destination_open_dir(parent_fd, name)


def _destination_write_at(parent_fd: int, name: str, payload: bytes) -> None:
    """Install one private file without replacement through a held dirfd."""
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise ReplicationDurabilityError("destination file name is unsafe")
    existing_fd: int | None = None
    temporary_fd: int | None = None
    temporary_name = f".{name}.{secrets.token_hex(12)}.tmp"
    try:
        try:
            existing_fd = os.open(
                name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            pass
        if existing_fd is not None:
            existing, _ = _read_descriptor(existing_fd)
            if existing != payload:
                raise ReplicationDurabilityError("destination artifact conflict")
            return
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        _write_fully(temporary_fd, payload)
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = None
        try:
            os.link(
                temporary_name,
                name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            try:
                existing_fd = os.open(
                    name,
                    os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                    dir_fd=parent_fd,
                )
                existing, _ = _read_descriptor(existing_fd)
            except OSError as read_exc:
                raise ReplicationCASConflict("destination file CAS conflict") from read_exc
            if existing != payload:
                raise ReplicationCASConflict("destination file CAS conflict") from exc
        _fsync_open_directory(parent_fd)
    except ReplicationDurabilityError:
        raise
    except OSError as exc:
        raise ReplicationDurabilityError("destination artifact install failed") from exc
    finally:
        failures: list[str] = []
        if existing_fd is not None:
            _close_fd_best_effort(existing_fd, "destination_existing_fd", failures)
        if temporary_fd is not None:
            _close_fd_best_effort(temporary_fd, "destination_temporary_fd", failures)
        try:
            os.unlink(temporary_name, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        except OSError:
            failures.append("destination_temporary_unlink")
        _finish_cleanup(None, failures)


def _destination_remove_tree_at(parent_fd: int, name: str) -> None:
    """Remove only a private staging directory owned by this writer."""
    try:
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ReplicationDurabilityError("destination staging cleanup failed") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ReplicationDurabilityError("destination staging entry is unsafe")
    child = _destination_open_dir(parent_fd, name)
    primary: BaseException | None = None
    try:
        for entry in os.listdir(child):
            if entry in {".", ".."}:
                continue
            try:
                info = os.stat(entry, dir_fd=child, follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    _destination_remove_tree_at(child, entry)
                elif stat.S_ISREG(info.st_mode):
                    os.unlink(entry, dir_fd=child)
                else:
                    raise ReplicationDurabilityError("destination staging entry is unsafe")
            except FileNotFoundError:
                continue
        os.rmdir(name, dir_fd=parent_fd)
    except ReplicationDurabilityError as exc:
        primary = exc
        raise
    except OSError as exc:
        primary = ReplicationDurabilityError("destination staging cleanup failed")
        raise primary from exc
    finally:
        failures: list[str] = []
        _close_fd_best_effort(child, "destination_staging_fd", failures)
        _finish_cleanup(primary, failures)


def _destination_rename_noreplace(parent_fd: int, source: str, target: str) -> None:
    """Install a directory with same-parent no-replace semantics.

    macOS exposes the required primitive as ``renameatx_np`` while Linux
    exposes it as ``renameat2``.  The regular-file fallback is an exclusive
    hard-link followed by removal of the temporary name; it is intentionally
    not used for directories because hard-linking directories is unsupported
    by the filesystem.  There is no stat-then-rename fallback.
    """
    if not source or not target or "/" in source or "/" in target:
        raise ReplicationDurabilityError("destination generation name is unsafe")
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        renameatx_np = getattr(libc, "renameatx_np", None)
    except OSError:
        renameat2 = None
        renameatx_np = None
    if renameat2 is not None:
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(
            parent_fd,
            source.encode("utf-8"),
            parent_fd,
            target.encode("utf-8"),
            1,
        )
        if result == 0:
            return
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            raise ReplicationCASConflict("destination generation already exists")
        if error not in {errno.ENOSYS, errno.EINVAL}:
            raise ReplicationDurabilityError("destination generation install failed")
    if renameatx_np is not None:
        renameatx_np.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameatx_np.restype = ctypes.c_int
        result = renameatx_np(
            parent_fd,
            source.encode("utf-8"),
            parent_fd,
            target.encode("utf-8"),
            0x00000004,  # RENAME_EXCL
        )
        if result == 0:
            return
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            raise ReplicationCASConflict("destination generation already exists")
        if error not in {errno.ENOSYS, errno.EINVAL}:
            raise ReplicationDurabilityError("destination generation install failed")
    # Portable fallback uses the filesystem's atomic link-create rule.  A
    # prior stat followed by rename is not a no-replace operation: another
    # writer can create the target between those calls and the rename would
    # silently overwrite it.  If hard links are unavailable this destination
    # cannot provide the required publication contract.
    try:
        os.link(
            source,
            target,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
            follow_symlinks=False,
        )
    except FileExistsError as exc:
        raise ReplicationCASConflict("destination generation already exists") from exc
    except OSError as exc:
        if exc.errno in {errno.EOPNOTSUPP, errno.ENOTSUP, errno.EXDEV, errno.EPERM}:
            raise ReplicationDurabilityError("destination generation install unsupported") from exc
        raise ReplicationDurabilityError("destination generation install failed") from exc
    try:
        os.unlink(source, dir_fd=parent_fd)
    except FileExistsError as exc:
        raise ReplicationDurabilityError("destination generation cleanup failed") from exc
    except OSError as exc:
        raise ReplicationDurabilityError("destination generation cleanup failed") from exc


def _destination_exchange_at(source_fd: int, source: str, target_fd: int, target: str) -> bool:
    """Atomically exchange two regular files through their held dirfds."""
    if not source or not target or "/" in source or "/" in target:
        raise ReplicationDurabilityError("destination head name is unsafe")
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = getattr(libc, "renameat2", None)
        renameatx_np = getattr(libc, "renameatx_np", None)
    except OSError:
        renameat2 = None
        renameatx_np = None
    if renameat2 is not None:
        renameat2.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameat2.restype = ctypes.c_int
        result = renameat2(
            source_fd,
            source.encode("utf-8"),
            target_fd,
            target.encode("utf-8"),
            0x00000002,  # RENAME_EXCHANGE
        )
        if result == 0:
            return True
        error = ctypes.get_errno()
        if error not in {errno.ENOSYS, errno.EINVAL, errno.ENOENT}:
            raise ReplicationDurabilityError("destination head exchange failed")
    if renameatx_np is not None:
        renameatx_np.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        renameatx_np.restype = ctypes.c_int
        result = renameatx_np(
            source_fd,
            source.encode("utf-8"),
            target_fd,
            target.encode("utf-8"),
            0x00000002,  # RENAME_SWAP
        )
        if result == 0:
            return True
        error = ctypes.get_errno()
        if error not in {errno.ENOSYS, errno.EINVAL, errno.ENOENT}:
            raise ReplicationDurabilityError("destination head exchange failed")
    raise ReplicationDurabilityError("destination head exchange unsupported")


@contextmanager
def _destination_root_session(
    descriptor: DestinationDescriptor,
    *,
    mount_inspector: MountInspector | None = None,
):
    descriptor.verify_bound()
    root_fd, descriptors = _open_directory_chain(descriptor.root_path, create=False)
    primary: BaseException | None = None
    try:
        descriptor.verify_root_fd(root_fd)
        persisted = _destination_verify_persisted_descriptor(descriptor, root_fd)
        _destination_live_mount_identity(
            persisted, root_fd, mount_inspector or SystemMountInspector()
        )
        yield root_fd
    except BaseException as exc:
        primary = exc
        raise
    finally:
        failures: list[str] = []
        _close_descriptors(descriptors, failures)
        _finish_cleanup(primary, failures)


def _destination_verify_persisted_descriptor(
    expected: DestinationDescriptor, root_fd: int
) -> DestinationDescriptor:
    """Bind a session to the exact descriptor bytes stored at the root."""
    expected.verify()
    result = _destination_read_at(root_fd, (DESTINATION_DESCRIPTOR_NAME,))
    if result is None:
        raise ReplicationStateUnavailable("destination descriptor is unavailable")
    payload, _ = result
    try:
        values = json.loads(payload)
        persisted = DestinationDescriptor.model_validate(values)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ReplicationStateUnavailable("destination descriptor is invalid") from exc
    if canonical_json_bytes(persisted.model_dump(mode="json")) != payload:
        raise ReplicationStateUnavailable("destination descriptor is not canonical")
    if persisted.model_dump(mode="json") != expected.model_dump(mode="json"):
        raise ReplicationStateUnavailable("destination descriptor does not match expected")
    persisted._root_path = expected.root_path
    persisted.verify()
    persisted.verify_root_fd(root_fd)
    return persisted


def _destination_lock(root_fd: int) -> tuple[int, int, list[int]]:
    replication_fd = _destination_open_dir(root_fd, DESTINATION_REPLICATION_DIR)
    lock_fd: int | None = None
    try:
        lock_fd = os.open(
            DESTINATION_LOCK_NAME,
            os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=replication_fd,
        )
        info = os.fstat(lock_fd)
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise ReplicationDurabilityError("destination writer lock is unsafe")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ReplicationDurabilityError("destination writer lock is unavailable") from exc
        return replication_fd, lock_fd, [int(info.st_dev), int(info.st_ino), int(info.st_nlink)]
    except OSError as exc:
        failures: list[str] = []
        if lock_fd is not None:
            _close_fd_best_effort(lock_fd, "destination_lock_fd", failures)
        _close_fd_best_effort(replication_fd, "destination_replication_fd", failures)
        primary = ReplicationDurabilityError("destination writer lock is unavailable")
        _finish_cleanup(primary, failures)
        raise primary from exc
    except BaseException:
        failures: list[str] = []
        if lock_fd is not None:
            _close_fd_best_effort(lock_fd, "destination_lock_fd", failures)
        _close_fd_best_effort(replication_fd, "destination_replication_fd", failures)
        _finish_cleanup(None, failures)
        raise


def _destination_unlock(replication_fd: int, lock_fd: int) -> None:
    failures: list[str] = []
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except OSError:
        failures.append("destination_lock_unlock")
    _close_fd_best_effort(lock_fd, "destination_lock_fd", failures)
    _close_fd_best_effort(replication_fd, "destination_replication_fd", failures)
    _finish_cleanup(None, failures)


def _destination_assert_lock(replication_fd: int, lock_fd: int, identity: list[int]) -> None:
    try:
        held = os.fstat(lock_fd)
        current = os.stat(
            DESTINATION_LOCK_NAME,
            dir_fd=replication_fd,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise ReplicationDurabilityError("destination writer lock changed") from exc
    if (
        not stat.S_ISREG(held.st_mode)
        or (int(held.st_dev), int(held.st_ino), int(held.st_nlink)) != tuple(identity)
        or (int(current.st_dev), int(current.st_ino), int(current.st_nlink)) != tuple(identity)
        or stat.S_IMODE(current.st_mode) != 0o600
    ):
        raise ReplicationDurabilityError("destination writer lock changed")


def initialize_destination(
    root: Path,
    *,
    acknowledgement: str,
    single_writer_host_id: str = "local-host",
    normalized_options: str = "",
    volume_id: str | None = None,
    sentinel_bytes: bytes | None = None,
    local_root: Path | None = None,
    created_at: str | None = None,
    mount_inspector: MountInspector | None = None,
) -> DestinationDescriptor:
    """Create a new empty offline archive root under an exact typed ack.

    This is deliberately separate from replication execution.  It accepts
    only a new child and never adopts or overwrites an existing archive.
    """
    if acknowledgement != DESTINATION_INIT_ACK:
        raise ReplicationDurabilityError("destination initialization acknowledgement is invalid")
    bound = _validate_destination_root(Path(root), local_root=local_root)
    if bound.exists():
        if not bound.is_dir() or any(bound.iterdir()):
            raise ReplicationDurabilityError("destination must be a new empty child")
    else:
        _safe_parent(bound.parent)
        parent_fd, descriptors = _open_directory_chain(bound.parent, create=False)
        try:
            try:
                os.mkdir(bound.name, mode=0o700, dir_fd=parent_fd)
            except FileExistsError as exc:
                raise ReplicationDurabilityError("destination already exists") from exc
            _fsync_open_directory(parent_fd)
        except OSError as exc:
            raise ReplicationDurabilityError("destination initialization failed") from exc
        finally:
            failures: list[str] = []
            _close_descriptors(descriptors, failures)
            _finish_cleanup(None, failures)
    sentinel = sentinel_bytes or canonical_json_bytes(
        {
            "dataset": "stock-eva-market",
            "schema_version": 2,
        }
    )
    if not isinstance(sentinel, bytes) or not sentinel:
        raise ReplicationDurabilityError("destination sentinel is invalid")
    descriptor = DestinationDescriptor.from_root(
        bound,
        single_writer_host_id=single_writer_host_id,
        sentinel_sha256=_sha256_bytes(sentinel),
        mount_point=str(bound),
        fs_type="local",
        normalized_options=normalized_options,
        volume_id=volume_id,
        created_at=created_at,
        local_root=local_root,
        mount_inspector=mount_inspector,
    )
    replication_path = bound / DESTINATION_REPLICATION_DIR
    history = replication_path / DESTINATION_HISTORY_DIR
    head_history = replication_path / DESTINATION_HEAD_HISTORY_DIR
    staging = replication_path / DESTINATION_STAGING_DIR
    # All ancestors are created by no-follow descriptor traversal; none of
    # these operations resolve a user-controlled pathname after validation.
    for directory in (replication_path, history, head_history, staging):
        _fd, descriptors = _open_directory_chain(directory, create=True)
        failures: list[str] = []
        _close_descriptors(descriptors, failures)
        _finish_cleanup(None, failures)
    _install_no_replace(bound / DESTINATION_SENTINEL_NAME, sentinel)
    descriptor.persist()
    _install_no_replace(replication_path / DESTINATION_LOCK_NAME, b"")
    return descriptor


class ReplicationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    record_schema: Literal[DESTINATION_RECORD_SCHEMA] = DESTINATION_RECORD_SCHEMA
    schema_version: Literal[1] = 1
    replication_generation: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    destination_id: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")
    direction: Literal["local_to_nas"] = "local_to_nas"
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_sequence: int = Field(ge=1)
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    publication_binding_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    parent_record_hash: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    pointer_row_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    pointer_generation: str = Field(min_length=1, max_length=128)
    source_run_id: str = Field(min_length=1, max_length=128)
    source_trade_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    source_published_at: str = Field(min_length=20)
    pointer_db_device: int = Field(gt=0)
    pointer_db_inode: int = Field(gt=0)
    pointer_db_schema_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_manifest_canonical_sha256: str = Field(
        min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    source_manifest_bytes_sha256: str = Field(
        min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    source_object_set_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    destination_manifest_bytes_sha256: str = Field(
        min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    destination_object_set_sha256: str = Field(
        min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    object_inventory: tuple[ObjectInventoryEntry, ...]
    object_count: int = Field(ge=0)
    row_count: int = Field(ge=0)
    byte_count: int = Field(ge=0)
    descriptor_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    plan_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    created_at: str = Field(min_length=20)
    record_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    _trade_date_is_iso = field_validator("source_trade_date")(
        lambda value: _validate_iso_date(value, "source_trade_date")
    )
    _timestamps_are_utc = field_validator("source_published_at", "created_at")(
        lambda value, info: _validate_utc_timestamp(value, info.field_name)
    )

    @model_validator(mode="before")
    @classmethod
    def exact_fields(cls, value: object) -> object:
        if isinstance(value, Mapping) and set(value) != set(cls.model_fields):
            raise ValueError("replication record fields are not exact")
        return value

    def hash_preimage(self) -> dict[str, object]:
        values = self.model_dump(mode="json")
        values.pop("record_sha256")
        return values

    def verify_hashes(self) -> None:
        inventory = _object_inventory_projection(self.object_inventory)
        if tuple((item["relative_path"], item["object_sha256"]) for item in inventory) != tuple(
            sorted((item["relative_path"], item["object_sha256"]) for item in inventory)
        ):
            raise ReplicationDurabilityError("destination inventory is not sorted")
        object_set = domain_sha256(SOURCE_OBJECT_SET_DOMAIN, inventory)
        if (
            self.source_object_set_sha256 != object_set
            or self.destination_object_set_sha256 != object_set
        ):
            raise ReplicationDurabilityError("destination object set digest is invalid")
        if self.parent_record_hash != ZERO_SHA256:
            _validate_sha(self.parent_record_hash, "parent_record_hash")
        elif self.source_sequence != 1:
            raise ReplicationDurabilityError("destination lineage genesis is invalid")
        expected = domain_sha256("stock-eva/r2f4.3/replication-record/v1", self.hash_preimage())
        if expected != self.record_sha256:
            raise ReplicationDurabilityError("destination record hash is invalid")
        if (
            self.object_count != len(inventory)
            or self.byte_count != sum(int(item["size_bytes"]) for item in inventory)
            or self.row_count != sum(int(item["row_count"]) for item in inventory)
        ):
            raise ReplicationDurabilityError("destination record counts are invalid")


class DestinationHead(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    head_schema: Literal[DESTINATION_HEAD_SCHEMA] = DESTINATION_HEAD_SCHEMA
    schema_version: Literal[1] = 1
    destination_id: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")
    replication_generation: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    record_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    descriptor_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    direction: Literal["local_to_nas"] = "local_to_nas"
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_sequence: int = Field(ge=1)
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    publication_binding_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    parent_record_hash: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    manifest_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    object_set_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    head_version: int = Field(ge=1)
    updated_at: str = Field(min_length=20)
    head_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    _updated_at_is_utc = field_validator("updated_at")(
        lambda value: _validate_utc_timestamp(value, "updated_at")
    )

    @model_validator(mode="before")
    @classmethod
    def exact_fields(cls, value: object) -> object:
        if isinstance(value, Mapping) and set(value) != set(cls.model_fields):
            raise ValueError("destination head fields are not exact")
        return value

    def hash_preimage(self) -> dict[str, object]:
        values = self.model_dump(mode="json")
        values.pop("head_sha256")
        return values

    def verify_hash(self) -> None:
        if self.parent_record_hash != ZERO_SHA256:
            _validate_sha(self.parent_record_hash, "parent_record_hash")
        expected = domain_sha256("stock-eva/r2f4.3/replication-head/v1", self.hash_preimage())
        if expected != self.head_sha256:
            raise ReplicationDurabilityError("destination head hash is invalid")


@dataclass(frozen=True)
class DestinationReplicationResult:
    status: Literal["replicated", "already_replicated", "unavailable"]
    reason_code: ReplicationReason
    record: ReplicationRecord | None = None
    proof: VerifiedDestinationCommitProof | None = None
    destination_writes: bool = False


@dataclass
class _DestinationEffectContext:
    """Monotonic destination-effect state shared across cleanup boundaries."""

    destination_write_started: bool = False
    last_result: DestinationReplicationResult | None = None
    cleanup_failed: bool = False


class SentinelProjection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    dataset: Literal["stock-eva-market"] = "stock-eva-market"
    schema_version: Literal[2] = 2


class DestinationArchiveReader:
    """Descriptor-native read contract for the destination archive."""

    def __init__(
        self,
        descriptor: DestinationDescriptor | Path,
        *,
        mount_inspector: MountInspector | None = None,
    ) -> None:
        if isinstance(descriptor, DestinationDescriptor):
            self.descriptor = descriptor
        else:
            candidate = Path(descriptor)
            descriptor_path = (
                candidate / DESTINATION_DESCRIPTOR_NAME if candidate.is_dir() else candidate
            )
            self.descriptor = DestinationDescriptor.read(descriptor_path)
        self.mount_inspector = mount_inspector or SystemMountInspector()

    @contextmanager
    def root_session(self):
        with _destination_root_session(
            self.descriptor, mount_inspector=self.mount_inspector
        ) as root_fd:
            yield root_fd

    def _root_or_bound(self, root_dirfd: int | None):
        if root_dirfd is not None:
            self.descriptor.verify_root_fd(root_dirfd)
            persisted = _destination_verify_persisted_descriptor(self.descriptor, root_dirfd)
            _destination_live_mount_identity(persisted, root_dirfd, self.mount_inspector)
            return _null_context(root_dirfd)
        return self.root_session()

    def read_sentinel(self, root_dirfd: int | None = None) -> SentinelProjection:
        with self._root_or_bound(root_dirfd) as root_fd:
            result = _destination_read_at(
                root_fd,
                (DESTINATION_SENTINEL_NAME,),
                require_private_mode=False,
            )
            if result is None:
                raise ReplicationDurabilityError("destination sentinel is unavailable")
            payload, _ = result
            if _sha256_bytes(payload) != self.descriptor.sentinel_sha256:
                raise ReplicationDurabilityError("destination sentinel is invalid")
            try:
                sentinel_values = json.loads(payload)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ReplicationStateUnavailable("destination sentinel is invalid") from exc
            if sentinel_values != {"dataset": "stock-eva-market", "schema_version": 2}:
                raise ReplicationStateUnavailable("destination sentinel schema is invalid")
            return SentinelProjection.model_validate(sentinel_values)

    def read_head(
        self,
        root_dirfd: int | None = None,
        *,
        allow_pending_slot: bool = False,
    ) -> DestinationHead | None:
        with self._root_or_bound(root_dirfd) as root_fd:
            self._validate_replication_namespace(root_fd)
            result = _destination_read_at(
                root_fd,
                (DESTINATION_REPLICATION_DIR, "head.json"),
                optional=True,
            )
            if result is None:
                self._validate_head_history(
                    root_fd,
                    None,
                    allow_pending_slot=allow_pending_slot,
                )
                return None
            payload, _ = result
            try:
                values = json.loads(payload)
                head = DestinationHead.model_validate(values)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ReplicationStateUnavailable("destination head is unavailable") from exc
            if canonical_json_bytes(head.model_dump(mode="json")) != payload:
                raise ReplicationStateUnavailable("destination head is not canonical")
            head.verify_hash()
            if (
                head.destination_id != self.descriptor.destination_id
                or head.descriptor_sha256 != self.descriptor.descriptor_sha256
            ):
                raise ReplicationStateUnavailable("destination head trust identity is invalid")
            self._validate_head_history(
                root_fd,
                head,
                allow_pending_slot=allow_pending_slot,
            )
            return head

    def _validate_replication_namespace(self, root_fd: int) -> None:
        """Reject random head residues and unknown replication entries."""
        replication_fd, dirs = _destination_open_dirs(root_fd, (DESTINATION_REPLICATION_DIR,))
        primary: BaseException | None = None
        try:
            allowed = {
                DESTINATION_LOCK_NAME,
                DESTINATION_HISTORY_DIR,
                DESTINATION_HEAD_HISTORY_DIR,
                DESTINATION_STAGING_DIR,
                "head.json",
            }
            names = os.listdir(replication_fd)
            for name in names:
                if name not in allowed:
                    raise ReplicationStateUnavailable(
                        "destination replication namespace contains an unknown entry"
                    )
                info = os.stat(name, dir_fd=replication_fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise ReplicationStateUnavailable(
                        "destination replication namespace contains a symlink"
                    )
                if name in {
                    DESTINATION_HISTORY_DIR,
                    DESTINATION_HEAD_HISTORY_DIR,
                    DESTINATION_STAGING_DIR,
                }:
                    if not stat.S_ISDIR(info.st_mode):
                        raise ReplicationStateUnavailable(
                            "destination replication namespace directory is invalid"
                        )
                elif name == "head.json" or name == DESTINATION_LOCK_NAME:
                    if not stat.S_ISREG(info.st_mode):
                        raise ReplicationStateUnavailable(
                            "destination replication namespace file is invalid"
                        )
        except ReplicationStateUnavailable as exc:
            primary = exc
            raise
        except OSError as exc:
            primary = ReplicationStateUnavailable(
                "destination replication namespace is unavailable"
            )
            raise primary from exc
        finally:
            failures: list[str] = []
            _close_descriptors(dirs, failures)
            _finish_cleanup(primary, failures)

    def _validate_head_history(
        self,
        root_fd: int,
        current_head: DestinationHead | None,
        *,
        allow_pending_slot: bool = False,
        history_records: tuple[ReplicationRecord, ...] | None = None,
    ) -> None:
        """Validate deterministic immutable head-history slots end-to-end."""
        if history_records is None:
            history_records = self.list_verified_records(root_fd)
        records_by_sequence: dict[int, ReplicationRecord] = {}
        for record in history_records:
            if record.source_sequence in records_by_sequence:
                raise ReplicationStateUnavailable(
                    "destination head history has duplicate source sequence"
                )
            records_by_sequence[record.source_sequence] = record

        head_history_fd, dirs = _destination_open_dirs(
            root_fd, (DESTINATION_REPLICATION_DIR, DESTINATION_HEAD_HISTORY_DIR)
        )
        primary: BaseException | None = None
        try:
            names = os.listdir(head_history_fd)
            slot_sequences: dict[int, str] = {}
            for name in names:
                if not re.fullmatch(DESTINATION_HEAD_SLOT_PATTERN, name):
                    raise ReplicationStateUnavailable("destination head history entry is unknown")
                info = os.stat(name, dir_fd=head_history_fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                    raise ReplicationStateUnavailable("destination head history entry is unsafe")
                if stat.S_IMODE(info.st_mode) != 0o600:
                    raise ReplicationStateUnavailable(
                        "destination head history entry mode is invalid"
                    )
                sequence = int(name[5:25])
                if sequence < 1 or sequence in slot_sequences:
                    raise ReplicationStateUnavailable("destination head history slot is duplicated")
                slot_sequences[sequence] = name

            current_sequence = current_head.source_sequence if current_head is not None else 0
            expected_sequences = set(range(1, current_sequence + 1))
            actual_sequences = set(slot_sequences)
            pending_sequence = current_sequence + 1
            if allow_pending_slot and pending_sequence in actual_sequences:
                pending = {pending_sequence}
                if len(actual_sequences - expected_sequences) != 1:
                    raise ReplicationStateUnavailable(
                        "destination head history has ambiguous pending slot"
                    )
                actual_sequences -= pending
            elif actual_sequences - expected_sequences:
                raise ReplicationStateUnavailable(
                    "destination head history has an incomplete pending slot"
                )
            if actual_sequences != expected_sequences:
                raise ReplicationStateUnavailable("destination head history has a gap")

            # A complete reader view contains exactly the records represented by
            # the visible slot chain.  The only exception is the one deterministic
            # presealed next record, which a locked writer may resume.  Random
            # orphan generations are never silently ignored.
            record_sequences = set(records_by_sequence)
            expected_record_sequences = set(expected_sequences)
            if allow_pending_slot and pending_sequence in slot_sequences:
                expected_record_sequences.add(pending_sequence)
            if record_sequences != expected_record_sequences:
                raise ReplicationStateUnavailable(
                    "destination head history records are incomplete or contain an orphan"
                )
            for sequence in sorted(expected_record_sequences):
                record = records_by_sequence[sequence]
                if sequence == 1:
                    if record.parent_record_hash != ZERO_SHA256:
                        raise ReplicationStateUnavailable(
                            "destination head history genesis parent is invalid"
                        )
                    continue
                predecessor_record = records_by_sequence.get(sequence - 1)
                if (
                    predecessor_record is None
                    or record.parent_record_hash != predecessor_record.record_sha256
                    or record.source_instance_id != predecessor_record.source_instance_id
                    or record.source_instance_sha256 != predecessor_record.source_instance_sha256
                ):
                    raise ReplicationStateUnavailable(
                        "destination head history record parent is invalid"
                    )

            parsed_slots: dict[int, tuple[DestinationHead, os.stat_result, bytes]] = {}
            for sequence, name in slot_sequences.items():
                if sequence == pending_sequence and sequence not in expected_sequences:
                    if not allow_pending_slot:
                        raise ReplicationStateUnavailable(
                            "destination head history has an incomplete pending slot"
                        )
                result = _destination_read_at(head_history_fd, (name,))
                if result is None:
                    raise ReplicationStateUnavailable(
                        "destination head history slot is unavailable"
                    )
                payload, info = result
                try:
                    slot = DestinationHead.model_validate(json.loads(payload))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ReplicationStateUnavailable(
                        "destination head history slot is invalid"
                    ) from exc
                if canonical_json_bytes(slot.model_dump(mode="json")) != payload:
                    raise ReplicationStateUnavailable(
                        "destination head history slot is not canonical"
                    )
                try:
                    slot.verify_hash()
                except ReplicationDurabilityError as exc:
                    raise ReplicationStateUnavailable(
                        "destination head history slot hash is invalid"
                    ) from exc
                if (
                    slot.destination_id != self.descriptor.destination_id
                    or slot.descriptor_sha256 != self.descriptor.descriptor_sha256
                ):
                    raise ReplicationStateUnavailable(
                        "destination head history slot trust identity is invalid"
                    )
                parsed_slots[sequence] = (slot, info, payload)

            def verify_projection(
                slot: DestinationHead,
                record: ReplicationRecord,
                *,
                expected_head_version: int,
            ) -> None:
                # The slot name/record position is trusted lineage structure;
                # the serialized version is not.  Compare it independently
                # before rebuilding the expected projection so a rehashed
                # attacker cannot make an invalid slot self-consistent.
                if slot.head_version != expected_head_version:
                    raise ReplicationStateUnavailable("destination head history version is invalid")
                expected = _destination_head(
                    self.descriptor,
                    record,
                    head_version=expected_head_version,
                    updated_at=slot.updated_at,
                )
                if expected != slot:
                    raise ReplicationStateUnavailable(
                        "destination head history slot projection is invalid"
                    )

            for sequence in sorted(expected_sequences):
                slot, _info, _payload = parsed_slots[sequence]
                record_sequence = sequence if sequence == 1 else sequence - 1
                record = records_by_sequence.get(record_sequence)
                if record is None:
                    raise ReplicationStateUnavailable(
                        "destination head history slot record is missing"
                    )
                if slot.source_sequence != record_sequence:
                    raise ReplicationStateUnavailable(
                        "destination head history slot sequence is invalid"
                    )
                verify_projection(
                    slot,
                    record,
                    expected_head_version=record_sequence,
                )

            if current_head is not None:
                current_record = records_by_sequence.get(current_head.source_sequence)
                if current_record is None:
                    raise ReplicationStateUnavailable(
                        "destination head history current record is missing"
                    )
                verify_projection(
                    current_head,
                    current_record,
                    expected_head_version=current_head.source_sequence,
                )
                if current_head.head_version != current_head.source_sequence:
                    raise ReplicationStateUnavailable("destination head history version is invalid")
                if current_head.source_sequence == 1:
                    slot, slot_info, slot_payload = parsed_slots[1]
                    replication_fd = dirs[0]
                    head_info = os.stat("head.json", dir_fd=replication_fd, follow_symlinks=False)
                    if (
                        slot.source_sequence != 1
                        or slot_payload
                        != canonical_json_bytes(current_head.model_dump(mode="json"))
                        or slot_info.st_ino != head_info.st_ino
                        or slot_info.st_dev != head_info.st_dev
                        or slot_info.st_nlink != 2
                        or head_info.st_nlink != 2
                    ):
                        raise ReplicationStateUnavailable(
                            "destination head history genesis hardlink is invalid"
                        )
                elif 2 in parsed_slots:
                    first_slot, first_info, first_payload = parsed_slots[1]
                    second_slot, second_info, second_payload = parsed_slots[2]
                    if (
                        first_payload != second_payload
                        or first_info.st_ino != second_info.st_ino
                        or first_info.st_dev != second_info.st_dev
                        or first_info.st_nlink != 2
                        or second_info.st_nlink != 2
                    ):
                        raise ReplicationStateUnavailable(
                            "destination head history genesis hardlink is invalid"
                        )
                    if first_slot != second_slot:
                        raise ReplicationStateUnavailable(
                            "destination head history genesis projection is inconsistent"
                        )

                if current_head.source_sequence >= 2:
                    predecessor = parsed_slots[current_head.source_sequence][0]
                    if (
                        predecessor.source_sequence != current_head.source_sequence - 1
                        or current_head.parent_record_hash != predecessor.record_sha256
                        or current_head.source_instance_id != predecessor.source_instance_id
                        or current_head.source_instance_sha256 != predecessor.source_instance_sha256
                    ):
                        raise ReplicationStateUnavailable(
                            "destination head history current parent is invalid"
                        )
            if allow_pending_slot and pending_sequence in parsed_slots:
                candidate, _info, _payload = parsed_slots[pending_sequence]
                record = records_by_sequence.get(pending_sequence)
                if record is None:
                    raise ReplicationStateUnavailable(
                        "destination head history pending record is missing"
                    )
                verify_projection(
                    candidate,
                    record,
                    expected_head_version=pending_sequence,
                )
                if candidate.source_sequence != pending_sequence:
                    raise ReplicationStateUnavailable(
                        "destination head history pending sequence is invalid"
                    )
                if current_head is None:
                    if (
                        candidate.head_version != 1
                        or candidate.parent_record_hash != ZERO_SHA256
                        or candidate.source_sequence != 1
                    ):
                        raise ReplicationStateUnavailable(
                            "destination head history pending genesis is invalid"
                        )
                elif (
                    pending_sequence != current_head.source_sequence + 1
                    or candidate.head_version != current_head.head_version + 1
                    or candidate.parent_record_hash != current_head.record_sha256
                    or candidate.source_instance_id != current_head.source_instance_id
                    or candidate.source_instance_sha256 != current_head.source_instance_sha256
                    or record.parent_record_hash != current_head.record_sha256
                    or record.source_sequence != current_head.source_sequence + 1
                    or record.source_instance_id != current_head.source_instance_id
                    or record.source_instance_sha256 != current_head.source_instance_sha256
                ):
                    raise ReplicationStateUnavailable(
                        "destination head history pending parent is invalid"
                    )
        except ReplicationStateUnavailable as exc:
            primary = exc
            raise
        except OSError as exc:
            primary = ReplicationStateUnavailable("destination head history is unavailable")
            raise primary from exc
        finally:
            failures: list[str] = []
            _close_descriptors(dirs, failures)
            _finish_cleanup(primary, failures)

    def _open_generation(self, root_fd: int, generation: str) -> tuple[int, list[int]]:
        if not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{64}", generation):
            raise ReplicationDurabilityError("destination generation is invalid")
        return _destination_open_dirs(
            root_fd,
            (DESTINATION_REPLICATION_DIR, DESTINATION_HISTORY_DIR, generation),
        )

    def read_record(
        self, replication_generation: str | int, root_dirfd: int | str | None = None
    ) -> ReplicationRecord:
        # The low-level contract spells this as ``read_record(root_dirfd,
        # generation)`` while the ergonomic public form is
        # ``read_record(generation, root_dirfd=None)``.  Both use the same
        # held descriptor and never reopen a checked path.
        if isinstance(replication_generation, int) and isinstance(root_dirfd, str):
            replication_generation, root_dirfd = root_dirfd, replication_generation
        if not isinstance(replication_generation, str) or (
            root_dirfd is not None and not isinstance(root_dirfd, int)
        ):
            raise ReplicationDurabilityError("destination record arguments are invalid")
        with self._root_or_bound(root_dirfd) as root_fd:
            generation_fd, dirs = self._open_generation(root_fd, replication_generation)
            try:
                result = _destination_read_at(generation_fd, ("replication-record.json",))
                if result is None:
                    raise ReplicationStateUnavailable("destination record is unavailable")
                payload, _ = result
                try:
                    values = json.loads(payload)
                    if isinstance(values, Mapping):
                        values = dict(values)
                        values["object_inventory"] = tuple(values.get("object_inventory", ()))
                    record = ReplicationRecord.model_validate(values)
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ReplicationStateUnavailable("destination record is unavailable") from exc
                if canonical_json_bytes(record.model_dump(mode="json")) != payload:
                    raise ReplicationStateUnavailable("destination record is not canonical")
                if record.replication_generation != replication_generation:
                    raise ReplicationStateUnavailable("destination record generation is invalid")
                record.verify_hashes()
                if (
                    record.destination_id != self.descriptor.destination_id
                    or record.descriptor_sha256 != self.descriptor.descriptor_sha256
                ):
                    raise ReplicationStateUnavailable(
                        "destination record trust identity is invalid"
                    )
                return record
            finally:
                failures: list[str] = []
                _close_descriptors(dirs, failures)
                _finish_cleanup(None, failures)

    def read_manifest(self, generation_dirfd: int) -> bytes:
        result = _destination_read_at(
            generation_dirfd, ("manifest.json",), require_private_mode=False
        )
        if result is None:
            raise ReplicationStateUnavailable("destination manifest is unavailable")
        return result[0]

    def read_object(
        self, generation: str | int, relative_path: str, root_dirfd: int | None = None
    ) -> bytes:
        if isinstance(generation, int) and root_dirfd is None:
            # The low-level contract hands the reader an already-bound
            # generation fd; do not reopen the destination root for this
            # descriptor-native operation.
            return _destination_read_relative(generation, relative_path)
        with self._root_or_bound(root_dirfd) as root_fd:
            dirs: list[int] = []
            own_generation = False
            if isinstance(generation, int):
                generation_fd = generation
            else:
                generation_fd, dirs = self._open_generation(root_fd, generation)
                own_generation = True
            try:
                payload = _destination_read_relative(generation_fd, relative_path)
                return payload
            finally:
                if own_generation:
                    failures: list[str] = []
                    _close_descriptors(dirs, failures)
                    _finish_cleanup(None, failures)

    def verify_record(self, root_dirfd: int, replication_generation: str) -> ReplicationRecord:
        """Verify one immutable generation independently of ``head.json``."""
        self.read_sentinel(root_dirfd)
        record = self.read_record(replication_generation, root_dirfd)
        expected_generation = domain_sha256(
            "stock-eva/r2f4.3/replication-generation/v1",
            {
                "destination_id": record.destination_id,
                "source_instance_id": record.source_instance_id,
                "source_sequence": record.source_sequence,
                "checkpoint_id": record.checkpoint_id,
                "publication_binding_sha256": record.publication_binding_sha256,
                "source_object_set_sha256": record.source_object_set_sha256,
                "parent_record_hash": record.parent_record_hash,
                "plan_sha256": record.plan_sha256,
            },
        )
        if expected_generation != record.replication_generation:
            raise ReplicationStateUnavailable("destination generation identity is invalid")
        self._validate_history_namespace(root_dirfd)
        self._verify_parent_chain(root_dirfd, record)
        generation_fd, dirs = self._open_generation(root_dirfd, replication_generation)
        try:
            generation_sentinel = _destination_read_at(
                generation_fd,
                (DESTINATION_SENTINEL_NAME,),
                require_private_mode=False,
            )
            if (
                generation_sentinel is None
                or _sha256_bytes(generation_sentinel[0]) != self.descriptor.sentinel_sha256
            ):
                raise ReplicationStateUnavailable("destination generation sentinel mismatch")
            manifest = self.read_manifest(generation_fd)
            _parse_destination_manifest(
                manifest, expected_sha256=record.destination_manifest_bytes_sha256
            )
            expected_files = {
                "manifest.json",
                DESTINATION_SENTINEL_NAME,
                "replication-record.json",
                *(item.relative_path for item in record.object_inventory),
            }
            if _destination_list_files(generation_fd) != expected_files:
                raise ReplicationStateUnavailable("destination object inventory is incomplete")
            for item in record.object_inventory:
                payload = _destination_read_relative(generation_fd, item.relative_path)
                if len(payload) != item.size_bytes or _sha256_bytes(payload) != item.object_sha256:
                    raise ReplicationStateUnavailable("destination object verification failed")
        finally:
            failures: list[str] = []
            _close_descriptors(dirs, failures)
            _finish_cleanup(None, failures)
        return record

    def list_verified_records(self, root_dirfd: int) -> tuple[ReplicationRecord, ...]:
        """Strictly read every final history generation before a mutation."""
        history_fd, dirs = _destination_open_dirs(
            root_dirfd, (DESTINATION_REPLICATION_DIR, DESTINATION_HISTORY_DIR)
        )
        primary: BaseException | None = None
        try:
            names = os.listdir(history_fd)
        except OSError as exc:
            primary = ReplicationStateUnavailable("destination history listing is unavailable")
            raise primary from exc
        finally:
            failures: list[str] = []
            _close_descriptors(dirs, failures)
            _finish_cleanup(primary, failures)
        records: list[ReplicationRecord] = []
        for name in sorted(names):
            if re.fullmatch(r"[0-9a-f]{64}", name):
                records.append(self.verify_record(root_dirfd, name))
        return tuple(records)

    def verify_commit(
        self,
        *,
        intent_id: str | None = None,
        checkpoint_id: str,
        source_sequence: int,
        worker_id: str | None = None,
        state_version: int | None = None,
        destination_generation: str | None = None,
        now: str | None = None,
        allow_pending_slot: bool = False,
    ) -> VerifiedDestinationCommitProof:
        with self.root_session() as root_fd:
            self.read_sentinel(root_fd)
            head = self.read_head(root_fd, allow_pending_slot=allow_pending_slot)
            if head is None:
                raise ReplicationStateUnavailable("destination head is unavailable")
            if head.checkpoint_id != checkpoint_id or head.source_sequence != source_sequence:
                raise ReplicationStateUnavailable("destination proof checkpoint mismatch")
            generation = destination_generation or head.replication_generation
            if generation != head.replication_generation:
                raise ReplicationStateUnavailable("destination proof generation mismatch")
            record = self.read_record(generation, root_fd)
            if record.checkpoint_id != checkpoint_id or record.source_sequence != source_sequence:
                raise ReplicationStateUnavailable("destination proof record mismatch")
            if (
                head.record_sha256 != record.record_sha256
                or head.descriptor_sha256 != record.descriptor_sha256
                or head.parent_record_hash != record.parent_record_hash
                or head.manifest_sha256 != record.destination_manifest_bytes_sha256
                or head.object_set_sha256 != record.destination_object_set_sha256
            ):
                raise ReplicationStateUnavailable("destination head projection is inconsistent")
            self.verify_record(root_fd, generation)
            return VerifiedDestinationCommitProof(
                intent_id=intent_id,
                checkpoint_id=checkpoint_id,
                destination_id=self.descriptor.destination_id,
                source_sequence=source_sequence,
                destination_generation=generation,
                destination_record_sha256=record.record_sha256,
                destination_head_sha256=head.head_sha256,
                source_instance_id=record.source_instance_id,
                source_instance_sha256=record.source_instance_sha256,
                worker_id=worker_id,
                state_version=state_version,
                now=now or _utc_now(),
            )

    def _validate_history_namespace(self, root_fd: int) -> None:
        """Reject unallowlisted history entries before trusting a head."""
        history_fd, dirs = _destination_open_dirs(
            root_fd, (DESTINATION_REPLICATION_DIR, DESTINATION_HISTORY_DIR)
        )
        primary: BaseException | None = None
        try:
            for name in os.listdir(history_fd):
                if not name or name in {".", ".."} or "/" in name or "\\" in name:
                    raise ReplicationStateUnavailable("destination history entry is unsafe")
                info = os.stat(name, dir_fd=history_fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise ReplicationStateUnavailable("destination history entry is a symlink")
                if re.fullmatch(r"[0-9a-f]{64}", name):
                    if not stat.S_ISDIR(info.st_mode):
                        raise ReplicationStateUnavailable("destination generation entry is invalid")
                    continue
                # A crashed/incomplete destination generation is an orphan,
                # not a recoverable history record.  It must be quarantined
                # by an explicit operator before another writer can mutate
                # the namespace; silently cleaning it here would violate the
                # pre-mutation zero-write trust proof.
                if name.startswith(".") and name.endswith(".staging"):
                    raise ReplicationStateUnavailable("destination staging orphan is unsafe")
                raise ReplicationStateUnavailable("destination history entry is unknown")
        except ReplicationStateUnavailable as exc:
            primary = exc
            raise
        except OSError as exc:
            primary = ReplicationStateUnavailable("destination history listing is unavailable")
            raise primary from exc
        finally:
            failures: list[str] = []
            _close_descriptors(dirs, failures)
            _finish_cleanup(primary, failures)

    def _verify_parent_chain(self, root_fd: int, record: ReplicationRecord) -> None:
        """Verify each immutable parent record through the same root fd."""
        if record.parent_record_hash == ZERO_SHA256:
            if record.source_sequence != 1:
                raise ReplicationStateUnavailable("destination lineage genesis is invalid")
            return
        seen: set[str] = {record.record_sha256}
        current = record
        while current.parent_record_hash != ZERO_SHA256:
            parent_hash = current.parent_record_hash
            if parent_hash in seen:
                raise ReplicationStateUnavailable("destination lineage is cyclic")
            found: ReplicationRecord | None = None
            try:
                _replication_fd, dirs = _destination_open_dirs(
                    root_fd, (DESTINATION_REPLICATION_DIR, DESTINATION_HISTORY_DIR)
                )
                history_fd = dirs[-1]
                names = os.listdir(history_fd)
            finally:
                failures: list[str] = []
                _close_descriptors(dirs if "dirs" in locals() else [], failures)
                _finish_cleanup(None, failures)
            for name in names:
                if not re.fullmatch(r"[0-9a-f]{64}", name):
                    continue
                try:
                    candidate = self.read_record(name, root_fd)
                except ReplicationDurabilityError:
                    continue
                if candidate.record_sha256 == parent_hash:
                    if found is not None:
                        raise ReplicationStateUnavailable("destination lineage parent is ambiguous")
                    found = candidate
            if found is None:
                raise ReplicationStateUnavailable("destination lineage parent is missing")
            if (
                found.source_instance_id != record.source_instance_id
                or found.source_sequence >= current.source_sequence
            ):
                raise ReplicationStateUnavailable("destination lineage parent order is invalid")
            seen.add(found.record_sha256)
            current = found

    def contains_checkpoint(
        self,
        root_dirfd: int,
        *,
        checkpoint_id: str,
        source_instance_id: str,
        source_sequence: int,
    ) -> bool:
        """Return whether a complete, strictly readable history record matches.

        This is intentionally a descriptor-bound history lookup used only for
        partial-order decisions.  A higher destination sequence is
        ``DESTINATION_AHEAD`` only when this exact incoming checkpoint is an
        existing ancestor; an unrelated checkpoint is a conflict.
        """
        history_fd, dirs = _destination_open_dirs(
            root_dirfd, (DESTINATION_REPLICATION_DIR, DESTINATION_HISTORY_DIR)
        )
        primary: BaseException | None = None
        try:
            names = os.listdir(history_fd)
        except OSError as exc:
            primary = ReplicationStateUnavailable("destination history is unavailable")
            raise primary from exc
        finally:
            # The directory remains open only for enumeration.  Each candidate
            # is reopened through the original root descriptor below, never by
            # a path string outside the trust boundary.
            failures: list[str] = []
            _close_descriptors(dirs, failures)
            _finish_cleanup(primary, failures)
        for name in names:
            if not re.fullmatch(r"[0-9a-f]{64}", name):
                continue
            try:
                candidate = self.read_record(name, root_dirfd)
            except ReplicationDurabilityError:
                continue
            if (
                candidate.checkpoint_id == checkpoint_id
                and candidate.source_instance_id == source_instance_id
                and candidate.source_sequence == source_sequence
            ):
                return True
        return False

    def read_verified_archive(
        self,
        generation: str | None = None,
    ) -> VerifiedArchiveSnapshot:
        """Read one complete archive generation through one trusted session.

        ``generation`` is deliberately optional only for the current head.  A
        historical generation is never selected as a stale fallback: callers
        must name it explicitly and it must be present in the fully verified
        head/history chain.
        """
        with self.root_session() as root_fd:
            sentinel = self.read_sentinel(root_fd)
            sentinel_result = _destination_read_at(
                root_fd, (DESTINATION_SENTINEL_NAME,), require_private_mode=False
            )
            if sentinel_result is None:
                raise ReplicationStateUnavailable("destination sentinel is unavailable")
            head = self.read_head(root_fd)
            if head is None:
                raise ReplicationStateUnavailable("destination head is unavailable")
            records = self.list_verified_records(root_fd)
            records_by_generation = {item.replication_generation: item for item in records}
            selected_generation = (
                head.replication_generation if generation is None else generation
            )
            if selected_generation not in records_by_generation:
                raise ReplicationStateUnavailable("destination generation is unavailable")
            record = records_by_generation[selected_generation]
            if generation is not None and not isinstance(generation, str):
                raise ReplicationDurabilityError("destination generation is invalid")
            # Validate the complete selected generation, including its own
            # sentinel, manifest and exact object inventory, after validating
            # every record in the chain above.
            self.verify_record(root_fd, selected_generation)
            generation_fd, dirs = self._open_generation(root_fd, selected_generation)
            try:
                manifest = self.read_manifest(generation_fd)
                if _sha256_bytes(manifest) != record.destination_manifest_bytes_sha256:
                    raise ReplicationStateUnavailable("destination manifest hash mismatch")
                _validate_restore_manifest(manifest, record)
                for item in record.object_inventory:
                    payload = _destination_read_relative(generation_fd, item.relative_path)
                    if len(payload) != item.size_bytes or _sha256_bytes(payload) != item.object_sha256:
                        raise ReplicationStateUnavailable("destination object verification failed")
            finally:
                failures: list[str] = []
                _close_descriptors(dirs, failures)
                _finish_cleanup(None, failures)
            checkpoint = _checkpoint_from_record(record)
            checkpoint.verify_hashes()
            if checkpoint.checkpoint_id != record.checkpoint_id:
                raise ReplicationStateUnavailable("destination source checkpoint is invalid")
            return VerifiedArchiveSnapshot(
                sentinel=sentinel,
                sentinel_bytes=sentinel_result[0],
                head=head,
                record=record,
                manifest_bytes=manifest,
                checkpoint=checkpoint,
            )

    read_restore_snapshot = read_verified_archive


class DestinationCommitVerifier:
    """Fresh, descriptor-native verifier used by sidecar completion."""

    def __init__(
        self,
        descriptor: DestinationDescriptor | Path,
        *,
        mount_inspector: MountInspector | None = None,
    ) -> None:
        self.reader = DestinationArchiveReader(descriptor, mount_inspector=mount_inspector)

    def verify(self, proof: VerifiedDestinationCommitProof) -> VerifiedDestinationCommitProof:
        if not isinstance(proof, VerifiedDestinationCommitProof):
            raise ReplicationDurabilityError("destination completion proof is invalid")
        if (
            proof.intent_id is None
            or proof.worker_id is None
            or proof.state_version is None
            or proof.source_instance_id is None
            or proof.source_instance_sha256 is None
        ):
            raise ReplicationDurabilityError("destination completion proof is incomplete")
        try:
            refreshed = self.reader.verify_commit(
                intent_id=proof.intent_id,
                checkpoint_id=proof.checkpoint_id,
                source_sequence=proof.source_sequence,
                worker_id=proof.worker_id,
                state_version=proof.state_version,
                destination_generation=proof.destination_generation,
                now=proof.now,
            )
        except (ReplicationDurabilityError, ValueError) as exc:
            raise ReplicationStateUnavailable(
                "destination completion proof cannot be freshly verified"
            ) from exc
        if refreshed != proof:
            raise ReplicationStateUnavailable("destination completion proof is stale")
        # ``verify_commit`` performs a complete descriptor-native read, but a
        # completion caller must also prove the persisted descriptor and live
        # mount *after* that read returns.  This closes the interval in which
        # a remount/rebind could occur between the reader's final byte and the
        # sidecar completion write.
        try:
            with self.reader.root_session():
                pass
        except (ReplicationDurabilityError, ValueError) as exc:
            raise ReplicationStateUnavailable(
                "destination completion boundary is unavailable"
            ) from exc
        return refreshed


@dataclass(frozen=True)
class VerifiedArchiveSnapshot:
    """Self-contained, descriptor-verified archive input for restore."""

    sentinel: SentinelProjection
    sentinel_bytes: bytes
    head: DestinationHead
    record: ReplicationRecord
    manifest_bytes: bytes
    checkpoint: SourceCheckpoint


def _checkpoint_from_record(record: ReplicationRecord) -> SourceCheckpoint:
    """Reconstruct the source checkpoint without SQLite or provider access."""
    return build_source_checkpoint(
        source_instance_id=record.source_instance_id,
        source_instance_sha256=record.source_instance_sha256,
        publication_binding_sha256=record.publication_binding_sha256,
        pointer_row_sha256=record.pointer_row_sha256,
        pointer_generation=record.pointer_generation,
        source_run_id=record.source_run_id,
        source_trade_date=record.source_trade_date,
        source_published_at=record.source_published_at,
        pointer_db_device=record.pointer_db_device,
        pointer_db_inode=record.pointer_db_inode,
        pointer_db_schema_digest=record.pointer_db_schema_digest,
        manifest_canonical_sha256=record.source_manifest_canonical_sha256,
        source_manifest_bytes_sha256=record.source_manifest_bytes_sha256,
        source_object_set_sha256=record.source_object_set_sha256,
        object_inventory=record.object_inventory,
    )


def _validate_restore_manifest(payload: bytes, record: ReplicationRecord) -> None:
    """Validate the canonical dataset manifest against the closed inventory."""
    values = _parse_destination_manifest(payload, expected_sha256=record.destination_manifest_bytes_sha256)
    if values.get("dataset") != "stock-eva-market" or values.get("schema_version") != 2:
        raise ReplicationStateUnavailable("destination manifest schema is invalid")
    if set(values) != {"dataset", "schema_version", "generation", "files"}:
        raise ReplicationStateUnavailable("destination manifest schema is invalid")
    if not isinstance(values.get("generation"), str) or not values["generation"]:
        raise ReplicationStateUnavailable("destination manifest generation is invalid")
    files = values.get("files")
    if not isinstance(files, list) or not files:
        raise ReplicationStateUnavailable("destination manifest inventory is invalid")
    actual_by_path = {item.relative_path: item for item in record.object_inventory}
    if len(actual_by_path) != len(record.object_inventory) or len(files) != len(actual_by_path):
        raise ReplicationStateUnavailable("destination manifest inventory is incomplete")
    seen: set[str] = set()
    for item in files:
        if not isinstance(item, Mapping):
            raise ReplicationStateUnavailable("destination manifest file entry is invalid")
        try:
            relative = str(item["path"])
            if relative in seen:
                raise ReplicationStateUnavailable("destination manifest contains duplicate objects")
            seen.add(relative)
            expected = actual_by_path[relative]
            if (
                item.get("sha256") != expected.object_sha256
                or type(item.get("row_count")) is not int
                or item.get("row_count") != expected.row_count
                or item.get("trade_date") != expected.trade_date
                or item.get("source") != expected.source
            ):
                raise ReplicationStateUnavailable("destination manifest object metadata mismatch")
            ObjectInventoryEntry.model_validate(
                {
                    "relative_path": relative,
                    "object_sha256": expected.object_sha256,
                    "size_bytes": expected.size_bytes,
                    "row_count": expected.row_count,
                    "trade_date": expected.trade_date,
                    "source": expected.source,
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ReplicationStateUnavailable("destination manifest file entry is invalid") from exc
    if seen != set(actual_by_path):
        raise ReplicationStateUnavailable("destination manifest inventory is incomplete")


@contextmanager
def _null_context(value: int):
    yield value


def _destination_record_generation(
    *,
    descriptor: DestinationDescriptor,
    checkpoint: SourceCheckpoint,
    source_sequence: int,
    parent_record_hash: str,
    plan_sha256: str,
) -> str:
    return domain_sha256(
        "stock-eva/r2f4.3/replication-generation/v1",
        {
            "destination_id": descriptor.destination_id,
            "source_instance_id": checkpoint.source_instance_id,
            "source_sequence": source_sequence,
            "checkpoint_id": checkpoint.checkpoint_id,
            "publication_binding_sha256": checkpoint.publication_binding_sha256,
            "source_object_set_sha256": checkpoint.source_object_set_sha256,
            "parent_record_hash": parent_record_hash,
            "plan_sha256": plan_sha256,
        },
    )


def _destination_plan_hash(descriptor: DestinationDescriptor, checkpoint: SourceCheckpoint) -> str:
    return domain_sha256(
        "stock-eva/r2f4.3/replication-plan/v1",
        {
            "direction": "local_to_nas",
            "destination_id": descriptor.destination_id,
            "checkpoint_id": checkpoint.checkpoint_id,
            "object_inventory": _object_inventory_projection(checkpoint.object_inventory),
        },
    )


def _destination_record(
    descriptor: DestinationDescriptor,
    checkpoint: SourceCheckpoint,
    *,
    source_sequence: int,
    parent_record_hash: str,
    plan_sha256: str,
    destination_manifest_bytes_sha256: str,
    created_at: str,
    replication_generation: str,
) -> ReplicationRecord:
    inventory = tuple(checkpoint.object_inventory)
    values: dict[str, object] = {
        "record_schema": DESTINATION_RECORD_SCHEMA,
        "schema_version": 1,
        "replication_generation": replication_generation,
        "destination_id": descriptor.destination_id,
        "direction": "local_to_nas",
        "source_instance_id": checkpoint.source_instance_id,
        "source_instance_sha256": checkpoint.source_instance_sha256,
        "source_sequence": source_sequence,
        "checkpoint_id": checkpoint.checkpoint_id,
        "publication_binding_sha256": checkpoint.publication_binding_sha256,
        "parent_record_hash": parent_record_hash,
        "pointer_row_sha256": checkpoint.pointer_row_sha256,
        "pointer_generation": checkpoint.pointer_generation,
        "source_run_id": checkpoint.source_run_id,
        "source_trade_date": checkpoint.source_trade_date,
        "source_published_at": checkpoint.source_published_at,
        "pointer_db_device": checkpoint.pointer_db_device,
        "pointer_db_inode": checkpoint.pointer_db_inode,
        "pointer_db_schema_digest": checkpoint.pointer_db_schema_digest,
        "source_manifest_canonical_sha256": checkpoint.manifest_canonical_sha256,
        "source_manifest_bytes_sha256": checkpoint.source_manifest_bytes_sha256,
        "source_object_set_sha256": checkpoint.source_object_set_sha256,
        "destination_manifest_bytes_sha256": destination_manifest_bytes_sha256,
        "destination_object_set_sha256": checkpoint.source_object_set_sha256,
        "object_inventory": [item.model_dump(mode="json") for item in inventory],
        "object_count": len(inventory),
        "row_count": sum(item.row_count for item in inventory),
        "byte_count": sum(item.size_bytes for item in inventory),
        "descriptor_sha256": descriptor.descriptor_sha256,
        "plan_sha256": plan_sha256,
        "created_at": created_at,
    }
    values["record_sha256"] = domain_sha256("stock-eva/r2f4.3/replication-record/v1", values)
    values["object_inventory"] = inventory
    record = ReplicationRecord.model_validate(values)
    record.verify_hashes()
    return record


def _destination_head(
    descriptor: DestinationDescriptor,
    record: ReplicationRecord,
    *,
    head_version: int,
    updated_at: str,
) -> DestinationHead:
    values: dict[str, object] = {
        "head_schema": DESTINATION_HEAD_SCHEMA,
        "schema_version": 1,
        "destination_id": descriptor.destination_id,
        "replication_generation": record.replication_generation,
        "record_sha256": record.record_sha256,
        "descriptor_sha256": descriptor.descriptor_sha256,
        "direction": "local_to_nas",
        "source_instance_id": record.source_instance_id,
        "source_instance_sha256": record.source_instance_sha256,
        "source_sequence": record.source_sequence,
        "checkpoint_id": record.checkpoint_id,
        "publication_binding_sha256": record.publication_binding_sha256,
        "parent_record_hash": record.parent_record_hash,
        "manifest_sha256": record.destination_manifest_bytes_sha256,
        "object_set_sha256": record.destination_object_set_sha256,
        "head_version": head_version,
        "updated_at": updated_at,
    }
    values["head_sha256"] = domain_sha256("stock-eva/r2f4.3/replication-head/v1", values)
    head = DestinationHead.model_validate(values)
    head.verify_hash()
    return head


def build_replication_record(
    descriptor: DestinationDescriptor,
    checkpoint: SourceCheckpoint,
    *,
    source_sequence: int,
    parent_record_hash: str = ZERO_SHA256,
    destination_manifest_bytes_sha256: str | None = None,
    created_at: str | None = None,
) -> ReplicationRecord:
    """Build and seal a record using the normative closed hash preimage."""
    plan_sha256 = _destination_plan_hash(descriptor, checkpoint)
    generation = _destination_record_generation(
        descriptor=descriptor,
        checkpoint=checkpoint,
        source_sequence=source_sequence,
        parent_record_hash=parent_record_hash,
        plan_sha256=plan_sha256,
    )
    return _destination_record(
        descriptor,
        checkpoint,
        source_sequence=source_sequence,
        parent_record_hash=parent_record_hash,
        plan_sha256=plan_sha256,
        destination_manifest_bytes_sha256=(
            destination_manifest_bytes_sha256 or checkpoint.source_manifest_bytes_sha256
        ),
        created_at=created_at or _utc_now(),
        replication_generation=generation,
    )


def build_destination_head(
    descriptor: DestinationDescriptor,
    record: ReplicationRecord,
    *,
    head_version: int = 1,
    updated_at: str | None = None,
) -> DestinationHead:
    return _destination_head(
        descriptor,
        record,
        head_version=head_version,
        updated_at=updated_at or _utc_now(),
    )


class DestinationArchiveWriter:
    """Offline/fake destination writer with one descriptor-bound lock."""

    def __init__(
        self,
        descriptor: DestinationDescriptor | Path,
        *,
        source_root: Path,
        writer_host_id: str,
        mount_inspector: MountInspector | None = None,
    ) -> None:
        self.descriptor = (
            descriptor
            if isinstance(descriptor, DestinationDescriptor)
            else DestinationDescriptor.read(
                Path(descriptor) / DESTINATION_DESCRIPTOR_NAME
                if Path(descriptor).is_dir()
                else Path(descriptor)
            )
        )
        self.source_root = _physical_path(Path(source_root))
        self.writer_host_id = writer_host_id
        self.mount_inspector = mount_inspector or SystemMountInspector()
        if writer_host_id != self.descriptor.single_writer_host_id:
            raise ReplicationDurabilityError("destination writer host is not authorized")
        _validate_source_root(self.source_root)
        if (
            self.source_root == self.descriptor.root_path
            or self.source_root in self.descriptor.root_path.parents
            or self.descriptor.root_path in self.source_root.parents
        ):
            raise ReplicationDurabilityError("source and destination overlap")

    def _replicate_locked(
        self,
        root_fd: int,
        checkpoint: SourceCheckpoint,
        *,
        source_sequence: int,
        intent_id: str | None,
        claim_context: ReplicationClaimContext | None,
        now: str,
        lock_fd: int,
        lock_identity: list[int],
        replication_fd: int,
        effect_context: _DestinationEffectContext,
    ) -> DestinationReplicationResult:
        reader = DestinationArchiveReader(self.descriptor, mount_inspector=self.mount_inspector)
        proof_intent_id = claim_context.intent_id if claim_context is not None else intent_id
        proof_worker_id = claim_context.worker_id if claim_context is not None else None
        proof_state_version = claim_context.state_version if claim_context is not None else None
        head_committed = False
        destination_write_started = False

        def mark_effect() -> None:
            """Record an effect monotonically, including outer cleanup failures."""
            nonlocal destination_write_started
            destination_write_started = True
            effect_context.destination_write_started = True

        def finish(result: DestinationReplicationResult) -> DestinationReplicationResult:
            """Retain the primary result if a later cleanup step fails."""
            effect_context.last_result = result
            return result

        def check_boundary() -> None:
            """Freshly prove the persisted descriptor and live mount identity."""
            persisted = _destination_verify_persisted_descriptor(self.descriptor, root_fd)
            _destination_live_mount_identity(persisted, root_fd, self.mount_inspector)

        def install_head(
            payload: bytes,
            *,
            expected: bytes | None,
            slot_name: str,
        ) -> None:
            """Publish a presealed deterministic head-history slot."""
            nonlocal head_committed
            head_history_fd = _destination_open_dir(replication_fd, DESTINATION_HEAD_HISTORY_DIR)
            cleanup_failures: list[str] = []
            completed = False
            outcome: DestinationHeadInstallResult | None = None
            try:
                outcome = _destination_install_head_from_slot(
                    replication_fd,
                    head_history_fd,
                    slot_name,
                    payload,
                    expected=expected,
                )
                if outcome is not None and not outcome.linearized:
                    raise DestinationHeadInstallError(
                        "destination head install did not linearize",
                        linearized=False,
                    )
                completed = True
                head_committed = True
                mark_effect()
            except DestinationHeadInstallError as exc:
                if exc.cleanup_failed:
                    effect_context.cleanup_failed = True
                if exc.linearized:
                    head_committed = True
                    mark_effect()
                raise
            finally:
                _close_fd_best_effort(
                    head_history_fd, "destination_head_history_fd", cleanup_failures
                )
                if cleanup_failures:
                    effect_context.cleanup_failed = True
                    if completed:
                        raise DestinationHeadInstallError(
                            "destination head descriptor cleanup failed",
                            linearized=bool(outcome and outcome.linearized),
                            cleanup_failed=True,
                        )

        def prepare_head_slot(head: DestinationHead) -> tuple[str, bytes]:
            """Create or reuse the fixed, pre-fsynced candidate slot."""
            payload = canonical_json_bytes(head.model_dump(mode="json"))
            slot_name = _destination_head_slot_name(head.source_sequence)
            head_history_fd = _destination_open_dir(replication_fd, DESTINATION_HEAD_HISTORY_DIR)
            cleanup_failures: list[str] = []
            try:
                try:
                    created = _destination_write_head_slot(head_history_fd, slot_name, payload)
                except ReplicationCASConflict:
                    # A crash can leave a fully sealed deterministic candidate
                    # with an earlier ``updated_at``.  Reuse it only when its
                    # canonical projection is exactly the same immutable
                    # record/head identity; never rewrite a slot in place.
                    existing = _destination_read_at(head_history_fd, (slot_name,))
                    if existing is None:
                        raise
                    try:
                        existing_head = DestinationHead.model_validate(json.loads(existing[0]))
                        existing_head.verify_hash()
                    except (TypeError, ValueError, json.JSONDecodeError) as exc:
                        raise ReplicationCASConflict(
                            "destination head history slot conflicts"
                        ) from exc
                    if (
                        canonical_json_bytes(existing_head.model_dump(mode="json")) != existing[0]
                        or existing_head.destination_id != head.destination_id
                        or existing_head.descriptor_sha256 != head.descriptor_sha256
                        or existing_head.source_sequence != head.source_sequence
                        or existing_head.head_version != head.head_version
                        or existing_head.record_sha256 != head.record_sha256
                        or existing_head.parent_record_hash != head.parent_record_hash
                    ):
                        raise ReplicationCASConflict(
                            "destination head history slot conflicts"
                        ) from None
                    payload = existing[0]
                    created = False
                if created:
                    mark_effect()
            finally:
                _close_fd_best_effort(
                    head_history_fd, "destination_head_history_fd", cleanup_failures
                )
                if cleanup_failures:
                    effect_context.cleanup_failed = True
            return slot_name, payload

        _destination_assert_lock(replication_fd, lock_fd, lock_identity)
        try:
            check_boundary()
        except (ReplicationStateUnavailable, ReplicationDurabilityError) as exc:
            if isinstance(exc, ReplicationDurabilityError) and "unsupported" in str(exc):
                return finish(DestinationReplicationResult("unavailable", "MOUNT_UNSUPPORTED"))
            reason = (
                "DESTINATION_REBOUND"
                if "changed" in str(exc) or "outside live mount" in str(exc)
                else "DESTINATION_TRUST_FAILED"
            )
            return finish(DestinationReplicationResult("unavailable", reason))
        # The destination role/sentinel is part of the trust boundary and
        # must be proven before staging any new generation.  Otherwise a
        # tampered empty archive could advance its head and only be noticed by
        # the post-commit reader.
        try:
            reader._validate_replication_namespace(root_fd)
            reader.read_sentinel(root_fd)
            reader._validate_history_namespace(root_fd)
            history_records = reader.list_verified_records(root_fd)
            baseline = reader.read_head(root_fd, allow_pending_slot=True)
        except ReplicationStateUnavailable as exc:
            reason = "DESTINATION_CONFLICT" if "lineage" in str(exc) else "DESTINATION_TRUST_FAILED"
            return finish(DestinationReplicationResult("unavailable", reason))
        except ReplicationDurabilityError:
            reason = "DESTINATION_TRUST_FAILED"
            return finish(DestinationReplicationResult("unavailable", reason))

        def matches_checkpoint(candidate: ReplicationRecord, parent_record_hash: str) -> bool:
            """Prove a recovered record is the exact incoming checkpoint."""
            plan_sha256 = _destination_plan_hash(self.descriptor, checkpoint)
            expected_generation = _destination_record_generation(
                descriptor=self.descriptor,
                checkpoint=checkpoint,
                source_sequence=source_sequence,
                parent_record_hash=parent_record_hash,
                plan_sha256=plan_sha256,
            )
            if candidate.replication_generation != expected_generation:
                return False
            if (
                candidate.destination_manifest_bytes_sha256
                != checkpoint.source_manifest_bytes_sha256
            ):
                return False
            expected = _destination_record(
                self.descriptor,
                checkpoint,
                source_sequence=source_sequence,
                parent_record_hash=parent_record_hash,
                plan_sha256=plan_sha256,
                destination_manifest_bytes_sha256=checkpoint.source_manifest_bytes_sha256,
                created_at=candidate.created_at,
                replication_generation=expected_generation,
            )
            return expected.model_dump(mode="json") == candidate.model_dump(mode="json")

        if baseline is None:
            matching_orphans = [
                candidate
                for candidate in history_records
                if candidate.checkpoint_id == checkpoint.checkpoint_id
                and candidate.source_instance_id == checkpoint.source_instance_id
                and candidate.source_sequence == source_sequence
                and matches_checkpoint(candidate, ZERO_SHA256)
            ]
            if matching_orphans:
                if (
                    len(matching_orphans) != 1
                    or source_sequence != 1
                    or matching_orphans[0].parent_record_hash != ZERO_SHA256
                ):
                    raise ReplicationStateUnavailable("destination orphan generation conflicts")
                orphan = matching_orphans[0]
                check_boundary()
                _destination_assert_lock(replication_fd, lock_fd, lock_identity)
                recovered_head = _destination_head(
                    self.descriptor, orphan, head_version=1, updated_at=now
                )
                slot_name, recovered_payload = prepare_head_slot(recovered_head)
                install_head(
                    recovered_payload,
                    expected=None,
                    slot_name=slot_name,
                )
                try:
                    check_boundary()
                except (ReplicationStateUnavailable, ReplicationDurabilityError):
                    return finish(
                        DestinationReplicationResult(
                            "unavailable", "CONTROL_STATE_UNAVAILABLE", orphan, None, True
                        )
                    )
                try:
                    _destination_assert_lock(replication_fd, lock_fd, lock_identity)
                except (ReplicationStateUnavailable, ReplicationDurabilityError):
                    return finish(
                        DestinationReplicationResult(
                            "unavailable", "CONTROL_STATE_UNAVAILABLE", orphan, None, True
                        )
                    )
                try:
                    proof = reader.verify_commit(
                        intent_id=proof_intent_id,
                        checkpoint_id=checkpoint.checkpoint_id,
                        source_sequence=source_sequence,
                        destination_generation=orphan.replication_generation,
                        worker_id=proof_worker_id,
                        state_version=proof_state_version,
                        now=now,
                    )
                    check_boundary()
                except (ReplicationStateUnavailable, ReplicationDurabilityError):
                    return finish(
                        DestinationReplicationResult(
                            "unavailable", "CONTROL_STATE_UNAVAILABLE", orphan, None, True
                        )
                    )
                return finish(
                    DestinationReplicationResult("replicated", "NONE", orphan, proof, True)
                )
            if history_records or source_sequence != 1:
                # A sequence without a committed parent, or any unrelated
                # orphan generation, is not an admissible new genesis.
                raise ReplicationStateUnavailable("destination lineage genesis is invalid")
        if baseline is not None:
            baseline_record = reader.read_record(baseline.replication_generation, root_fd)
            if baseline_record.record_sha256 != baseline.record_sha256:
                raise ReplicationStateUnavailable("destination head record mismatch")
            reader.verify_commit(
                checkpoint_id=baseline.checkpoint_id,
                source_sequence=baseline.source_sequence,
                now=now,
                allow_pending_slot=True,
            )
            baseline_chain_hashes: set[str] = set()
            current_record = baseline_record
            records_by_hash = {record.record_sha256: record for record in history_records}
            if len(records_by_hash) != len(history_records):
                raise ReplicationStateUnavailable("destination history contains duplicate records")
            while True:
                if current_record.record_sha256 in baseline_chain_hashes:
                    raise ReplicationStateUnavailable("destination lineage is cyclic")
                baseline_chain_hashes.add(current_record.record_sha256)
                if current_record.parent_record_hash == ZERO_SHA256:
                    break
                current_record = records_by_hash.get(current_record.parent_record_hash)
                if current_record is None:
                    raise ReplicationStateUnavailable("destination lineage parent is missing")
            orphan_records = [
                record
                for record in history_records
                if record.record_sha256 not in baseline_chain_hashes
            ]
            if orphan_records:
                matching_orphans = [
                    record
                    for record in orphan_records
                    if record.checkpoint_id == checkpoint.checkpoint_id
                    and record.source_instance_id == checkpoint.source_instance_id
                    and record.source_sequence == source_sequence
                    and record.parent_record_hash == baseline.record_sha256
                    and matches_checkpoint(record, baseline.record_sha256)
                ]
                if (
                    len(orphan_records) != 1
                    or len(matching_orphans) != 1
                    or source_sequence != baseline.source_sequence + 1
                ):
                    raise ReplicationStateUnavailable("destination orphan generation conflicts")
                orphan = matching_orphans[0]
                check_boundary()
                _destination_assert_lock(replication_fd, lock_fd, lock_identity)
                recovered_head = _destination_head(
                    self.descriptor,
                    orphan,
                    head_version=baseline.head_version + 1,
                    updated_at=now,
                )
                baseline_bytes = canonical_json_bytes(baseline.model_dump(mode="json"))
                slot_name, recovered_payload = prepare_head_slot(recovered_head)
                install_head(
                    recovered_payload,
                    expected=baseline_bytes,
                    slot_name=slot_name,
                )
                try:
                    check_boundary()
                except (ReplicationStateUnavailable, ReplicationDurabilityError):
                    return finish(
                        DestinationReplicationResult(
                            "unavailable", "CONTROL_STATE_UNAVAILABLE", orphan, None, True
                        )
                    )
                try:
                    _destination_assert_lock(replication_fd, lock_fd, lock_identity)
                except (ReplicationStateUnavailable, ReplicationDurabilityError):
                    return finish(
                        DestinationReplicationResult(
                            "unavailable", "CONTROL_STATE_UNAVAILABLE", orphan, None, True
                        )
                    )
                try:
                    proof = reader.verify_commit(
                        intent_id=proof_intent_id,
                        checkpoint_id=checkpoint.checkpoint_id,
                        source_sequence=source_sequence,
                        destination_generation=orphan.replication_generation,
                        worker_id=proof_worker_id,
                        state_version=proof_state_version,
                        now=now,
                    )
                    check_boundary()
                except (ReplicationStateUnavailable, ReplicationDurabilityError):
                    return finish(
                        DestinationReplicationResult(
                            "unavailable", "CONTROL_STATE_UNAVAILABLE", orphan, None, True
                        )
                    )
                return finish(
                    DestinationReplicationResult("replicated", "NONE", orphan, proof, True)
                )
            if baseline.source_instance_id != checkpoint.source_instance_id:
                return finish(DestinationReplicationResult("unavailable", "DESTINATION_CONFLICT"))
            if baseline.source_sequence > source_sequence:
                if reader.contains_checkpoint(
                    root_fd,
                    checkpoint_id=checkpoint.checkpoint_id,
                    source_instance_id=checkpoint.source_instance_id,
                    source_sequence=source_sequence,
                ):
                    return finish(DestinationReplicationResult("unavailable", "DESTINATION_AHEAD"))
                return finish(DestinationReplicationResult("unavailable", "DESTINATION_CONFLICT"))
            if baseline.source_sequence == source_sequence:
                if baseline.checkpoint_id != checkpoint.checkpoint_id:
                    return finish(
                        DestinationReplicationResult("unavailable", "DESTINATION_CONFLICT")
                    )
                proof = reader.verify_commit(
                    intent_id=proof_intent_id,
                    checkpoint_id=checkpoint.checkpoint_id,
                    source_sequence=source_sequence,
                    worker_id=proof_worker_id,
                    state_version=proof_state_version,
                    now=now,
                )
                record = reader.read_record(baseline.replication_generation, root_fd)
                return finish(
                    DestinationReplicationResult(
                        "already_replicated", "ALREADY_REPLICATED", record, proof, False
                    )
                )
        parent_hash = baseline.record_sha256 if baseline is not None else ZERO_SHA256
        plan_sha256 = _destination_plan_hash(self.descriptor, checkpoint)
        generation = _destination_record_generation(
            descriptor=self.descriptor,
            checkpoint=checkpoint,
            source_sequence=source_sequence,
            parent_record_hash=parent_hash,
            plan_sha256=plan_sha256,
        )
        replication_fd = _destination_open_dir(root_fd, DESTINATION_REPLICATION_DIR)
        history_fd = _destination_open_dir(replication_fd, DESTINATION_HISTORY_DIR)
        staging_name = f".{generation}.staging"
        staging_fd: int | None = None
        linked = False
        record: ReplicationRecord | None = None
        try:
            check_boundary()
            try:
                os.mkdir(staging_name, mode=0o700, dir_fd=history_fd)
            except FileExistsError as exc:
                raise ReplicationDurabilityError("destination staging conflict") from exc
            mark_effect()
            check_boundary()
            staging_fd = _destination_open_dir(history_fd, staging_name)
            source_fd, source_dirs = _open_directory_chain(self.source_root, create=False)
            try:
                sentinel = _destination_read_relative(source_fd, DESTINATION_SENTINEL_NAME)
                if _sha256_bytes(sentinel) != self.descriptor.sentinel_sha256:
                    raise ReplicationDurabilityError("destination sentinel does not match source")
                manifest = _destination_read_relative(source_fd, "manifest.json")
                if _sha256_bytes(manifest) != checkpoint.source_manifest_bytes_sha256:
                    raise ReplicationDurabilityError("source manifest hash mismatch")
                _parse_destination_manifest(
                    manifest, expected_sha256=checkpoint.source_manifest_bytes_sha256
                )
                check_boundary()
                _destination_write_at(staging_fd, DESTINATION_SENTINEL_NAME, sentinel)
                mark_effect()
                check_boundary()
                _destination_write_at(staging_fd, "manifest.json", manifest)
                mark_effect()
                check_boundary()
                for item in checkpoint.object_inventory:
                    payload = _destination_read_relative(source_fd, item.relative_path)
                    if (
                        len(payload) != item.size_bytes
                        or _sha256_bytes(payload) != item.object_sha256
                    ):
                        raise ReplicationDurabilityError("source object verification failed")
                    target_fd = staging_fd
                    target_dirs: list[int] = []
                    parts = tuple(item.relative_path.split("/"))
                    try:
                        for part in parts[:-1]:
                            check_boundary()
                            target_fd = _destination_mkdir_at(target_fd, part)
                            target_dirs.append(target_fd)
                            mark_effect()
                            check_boundary()
                        check_boundary()
                        _destination_write_at(target_fd, parts[-1], payload)
                        mark_effect()
                        check_boundary()
                        readback = _destination_read_at(
                            target_fd, (parts[-1],), require_private_mode=False
                        )
                        if (
                            readback is None
                            or len(readback[0]) != item.size_bytes
                            or _sha256_bytes(readback[0]) != item.object_sha256
                        ):
                            raise ReplicationDurabilityError("destination object readback failed")
                        check_boundary()
                    finally:
                        failures: list[str] = []
                        _close_descriptors(target_dirs, failures)
                        _finish_cleanup(None, failures)
            finally:
                failures = []
                _close_descriptors(source_dirs, failures)
                _finish_cleanup(None, failures)
            record = _destination_record(
                self.descriptor,
                checkpoint,
                source_sequence=source_sequence,
                parent_record_hash=parent_hash,
                plan_sha256=plan_sha256,
                destination_manifest_bytes_sha256=_sha256_bytes(manifest),
                created_at=now,
                replication_generation=generation,
            )
            check_boundary()
            _destination_write_at(
                staging_fd,
                "replication-record.json",
                canonical_json_bytes(record.model_dump(mode="json")),
            )
            mark_effect()
            check_boundary()
            _fsync_open_directory(staging_fd)
            os.close(staging_fd)
            staging_fd = None
            check_boundary()
            _destination_rename_noreplace(history_fd, staging_name, generation)
            mark_effect()
            linked = True
            # The directory install is its own publication boundary.  A
            # remount or descriptor drift here must leave the immutable
            # generation intact but prevent head publication.
            check_boundary()
            _fsync_open_directory(history_fd)
            # Re-prove again after the history directory fsync; this is a
            # separate durability phase from the no-replace install.
            check_boundary()
            _destination_assert_lock(replication_fd, lock_fd, lock_identity)
            head = _destination_head(
                self.descriptor,
                record,
                head_version=baseline.head_version + 1 if baseline is not None else 1,
                updated_at=now,
            )
            slot_name, head_payload = prepare_head_slot(head)
            current_head_bytes = _destination_read_at(replication_fd, ("head.json",), optional=True)
            baseline_bytes = (
                canonical_json_bytes(baseline.model_dump(mode="json"))
                if baseline is not None
                else None
            )
            if (
                current_head_bytes[0] if current_head_bytes is not None else None
            ) != baseline_bytes:
                raise ReplicationCASConflict("destination head CAS conflict")
            check_boundary()
            install_head(
                head_payload,
                expected=baseline_bytes,
                slot_name=slot_name,
            )
            check_boundary()
            _destination_assert_lock(replication_fd, lock_fd, lock_identity)
            self.descriptor.verify_bound()
            proof = reader.verify_commit(
                intent_id=proof_intent_id,
                checkpoint_id=checkpoint.checkpoint_id,
                source_sequence=source_sequence,
                destination_generation=generation,
                worker_id=proof_worker_id,
                state_version=proof_state_version,
                now=now,
            )
            # The verifier's return is not the final trust point.  Re-read the
            # persisted descriptor and independent live mount identity before
            # allowing the caller to complete the local sidecar.
            check_boundary()
            return finish(DestinationReplicationResult("replicated", "NONE", record, proof, True))
        except (ReplicationCASConflict, ReplicationStateUnavailable) as exc:
            if head_committed:
                return finish(
                    DestinationReplicationResult(
                        "unavailable",
                        "CONTROL_STATE_UNAVAILABLE",
                        record,
                        None,
                        destination_writes=True,
                    )
                )
            reason = (
                "CAS_CONFLICT"
                if isinstance(exc, ReplicationCASConflict)
                else "DESTINATION_TRUST_FAILED"
            )
            if isinstance(exc, ReplicationStateUnavailable) and "lineage" in str(exc):
                reason = "DESTINATION_CONFLICT"
            if isinstance(exc, ReplicationStateUnavailable) and (
                "changed" in str(exc) or "rebound" in str(exc)
            ):
                reason = "DESTINATION_REBOUND"
            return finish(
                DestinationReplicationResult(
                    "unavailable", reason, destination_writes=destination_write_started
                )
            )
        except DestinationHeadInstallError as exc:
            if exc.linearized or head_committed:
                return finish(
                    DestinationReplicationResult(
                        "unavailable",
                        "CONTROL_STATE_UNAVAILABLE",
                        record,
                        None,
                        destination_write_started,
                    )
                )
            return finish(
                DestinationReplicationResult(
                    "unavailable", exc.reason_code, destination_writes=destination_write_started
                )
            )
        except ReplicationDurabilityError as exc:
            if head_committed:
                return finish(
                    DestinationReplicationResult(
                        "unavailable",
                        "CONTROL_STATE_UNAVAILABLE",
                        record,
                        None,
                        destination_writes=True,
                    )
                )
            reason = "MOUNT_UNSUPPORTED" if "unsupported" in str(exc) else "COPY_FAILED"
            return finish(
                DestinationReplicationResult(
                    "unavailable", reason, destination_writes=destination_write_started
                )
            )
        finally:
            cleanup_failures: list[str] = []
            if staging_fd is not None:
                _close_fd_best_effort(staging_fd, "destination_staging_fd", cleanup_failures)
            if not linked:
                try:
                    _destination_remove_tree_at(history_fd, staging_name)
                except (ReplicationDurabilityError, OSError):
                    cleanup_failures.append("destination_staging_cleanup")
            _close_fd_best_effort(history_fd, "destination_history_fd", cleanup_failures)
            _close_fd_best_effort(replication_fd, "destination_replication_fd", cleanup_failures)
            if cleanup_failures:
                effect_context.cleanup_failed = True
                primary = sys.exc_info()[1]
                if primary is not None:
                    primary.add_note("replication_cleanup=failed")
                elif effect_context.last_result is None:
                    _finish_cleanup(None, cleanup_failures)

    def replicate(
        self,
        checkpoint: SourceCheckpoint | Mapping[str, object],
        *,
        source_sequence: int,
        intent_id: str | None = None,
        claim_context: ReplicationClaimContext | None = None,
        now: str | None = None,
    ) -> DestinationReplicationResult:
        effect_context = _DestinationEffectContext()

        def result_with_effect(
            result: DestinationReplicationResult,
        ) -> DestinationReplicationResult:
            """Preserve the primary reason while carrying monotonic effects."""
            destination_writes = (
                result.destination_writes or effect_context.destination_write_started
            )
            if effect_context.cleanup_failed and result.status in {
                "replicated",
                "already_replicated",
            }:
                return DestinationReplicationResult(
                    "unavailable",
                    "CONTROL_STATE_UNAVAILABLE",
                    result.record,
                    None,
                    True,
                )
            return DestinationReplicationResult(
                result.status,
                result.reason_code,
                result.record,
                result.proof,
                destination_writes,
            )

        try:
            normalized = (
                checkpoint
                if isinstance(checkpoint, SourceCheckpoint)
                else SourceCheckpoint.model_validate(
                    {
                        **checkpoint,
                        "object_inventory": tuple(checkpoint.get("object_inventory", ())),
                    }
                    if isinstance(checkpoint, Mapping)
                    else checkpoint
                )
            )
            normalized.verify_hashes()
            if claim_context is not None and not isinstance(claim_context, ReplicationClaimContext):
                raise ReplicationDurabilityError("replication claim context is invalid")
            if claim_context is not None:
                if intent_id is not None and intent_id != claim_context.intent_id:
                    raise ReplicationDurabilityError("replication claim intent mismatch")
                if claim_context.checkpoint_id != normalized.checkpoint_id:
                    raise ReplicationDurabilityError("replication claim checkpoint mismatch")
                if (
                    claim_context.source_instance_id != normalized.source_instance_id
                    or claim_context.source_instance_sha256 != normalized.source_instance_sha256
                ):
                    raise ReplicationDurabilityError("replication claim source mismatch")
                intent_id = claim_context.intent_id
            elif intent_id is not None:
                raise ReplicationDurabilityError("replication claim context is required")
            if not isinstance(source_sequence, int) or source_sequence < 1:
                raise ReplicationDurabilityError("source sequence is invalid")
            occurred_at = now or _utc_now()
            _validate_utc_timestamp(occurred_at, "now")
            if self.writer_host_id != self.descriptor.single_writer_host_id:
                raise ReplicationDurabilityError("destination writer host is not authorized")
            self.descriptor.verify_bound()
            with _destination_root_session(
                self.descriptor, mount_inspector=self.mount_inspector
            ) as root_fd:
                replication_fd, lock_fd, lock_identity = _destination_lock(root_fd)
                result: DestinationReplicationResult | None = None
                try:
                    self.descriptor.verify_bound()
                    result = self._replicate_locked(
                        root_fd,
                        normalized,
                        source_sequence=source_sequence,
                        intent_id=intent_id,
                        claim_context=claim_context,
                        now=occurred_at,
                        lock_fd=lock_fd,
                        lock_identity=lock_identity,
                        replication_fd=replication_fd,
                        effect_context=effect_context,
                    )
                finally:
                    try:
                        _destination_unlock(replication_fd, lock_fd)
                    except ReplicationDurabilityError:
                        # Unlock/close is always best effort after a primary
                        # result.  A successful or already-replicated
                        # operation loses its proof when cleanup is uncertain;
                        # an unavailable primary keeps its own reason/effect.
                        effect_context.cleanup_failed = True
                        if result is None:
                            raise
                if result is None:
                    raise ReplicationDurabilityError("destination result is unavailable")
                return result_with_effect(result)
        except ReplicationCASConflict:
            if effect_context.last_result is not None:
                return result_with_effect(effect_context.last_result)
            return DestinationReplicationResult(
                "unavailable",
                "CAS_CONFLICT",
                destination_writes=effect_context.destination_write_started,
            )
        except DestinationHeadInstallError as exc:
            if exc.linearized:
                return DestinationReplicationResult(
                    "unavailable",
                    "CONTROL_STATE_UNAVAILABLE",
                    destination_writes=True,
                )
            return DestinationReplicationResult(
                "unavailable",
                exc.reason_code,
                destination_writes=effect_context.destination_write_started,
            )
        except ReplicationDurabilityError as exc:
            if effect_context.last_result is not None:
                if "cleanup failed" in str(exc):
                    effect_context.cleanup_failed = True
                return result_with_effect(effect_context.last_result)
            message = str(exc)
            reason: ReplicationReason = "DESTINATION_TRUST_FAILED"
            if "lock" in message or "unsupported" in message:
                reason = "MOUNT_UNSUPPORTED"
            elif "rebound" in message or "changed" in message:
                reason = "DESTINATION_REBOUND"
            elif "mount" in message:
                reason = "DESTINATION_MOUNT_UNAVAILABLE"
            elif "claim" in message:
                reason = "DESTINATION_TRUST_FAILED"
            elif "source" in message or "manifest" in message:
                reason = "SOURCE_UNAVAILABLE"
            elif "object" in message or "staging" in message or "copy" in message:
                reason = "COPY_FAILED"
            return DestinationReplicationResult(
                "unavailable", reason, destination_writes=effect_context.destination_write_started
            )

    execute = replicate


DestinationReplicationService = DestinationArchiveWriter
DestinationTrust = DestinationDescriptor
ReplicationDestinationWriter = DestinationArchiveWriter
DestinationWriter = DestinationArchiveWriter
DestinationDescriptor.from_path = DestinationDescriptor.from_root  # type: ignore[attr-defined]


def build_destination_descriptor(
    root: Path,
    *,
    single_writer_host_id: str = "local-host",
    normalized_options: str = "",
    volume_id: str | None = None,
    sentinel_sha256: str = ZERO_SHA256,
    local_root: Path | None = None,
    created_at: str | None = None,
    mount_inspector: MountInspector | None = None,
) -> DestinationDescriptor:
    """Small explicit-root factory used by offline drain fixtures."""
    return DestinationDescriptor.from_root(
        root,
        single_writer_host_id=single_writer_host_id,
        normalized_options=normalized_options,
        volume_id=volume_id,
        sentinel_sha256=sentinel_sha256,
        local_root=local_root,
        created_at=created_at,
        mount_inspector=mount_inspector,
    )


def _destination_head_slot_name(source_sequence: int) -> str:
    """Return the deterministic immutable slot for a source sequence."""
    if not isinstance(source_sequence, int) or not 1 <= source_sequence < 10**20:
        raise ReplicationDurabilityError("destination head history sequence is invalid")
    return f"slot-{source_sequence:020d}.json"


def _destination_write_head_slot(head_history_fd: int, slot_name: str, payload: bytes) -> bool:
    """O_EXCL-create and fsync one fixed head-history candidate slot.

    A pre-existing slot is reusable only when its descriptor-native bytes are
    identical.  There is deliberately no temporary basename and no unlink in
    this operation; a crash leaves a deterministic candidate for the next
    locked writer to validate and resume.
    """
    if not re.fullmatch(DESTINATION_HEAD_SLOT_PATTERN, slot_name):
        raise ReplicationDurabilityError("destination head history slot name is unsafe")
    fd: int | None = None
    primary: BaseException | None = None
    try:
        try:
            fd = os.open(
                slot_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
                dir_fd=head_history_fd,
            )
        except FileExistsError:
            existing = _destination_read_at(head_history_fd, (slot_name,))
            if existing is None or existing[0] != payload:
                raise ReplicationCASConflict("destination head history slot conflicts") from None
            return False
        _write_fully(fd, payload)
        os.fsync(fd)
        os.close(fd)
        fd = None
        _fsync_open_directory(head_history_fd)
        return True
    except (ReplicationCASConflict, ReplicationDurabilityError) as exc:
        primary = exc
        raise
    except OSError as exc:
        primary = ReplicationDurabilityError("destination head history slot install failed")
        raise primary from exc
    finally:
        failures: list[str] = []
        if fd is not None:
            _close_fd_best_effort(fd, "destination_head_history_slot_fd", failures)
        _finish_cleanup(primary, failures)


def _destination_install_head_from_slot(
    replication_fd: int,
    head_history_fd: int,
    slot_name: str,
    payload: bytes,
    *,
    expected: bytes | None,
) -> DestinationHeadInstallResult:
    """Publish a fixed candidate slot without creating/deleting a residue."""
    if not re.fullmatch(DESTINATION_HEAD_SLOT_PATTERN, slot_name):
        raise ReplicationDurabilityError("destination head history slot name is unsafe")
    linearized = False
    try:
        current = _destination_read_at(replication_fd, ("head.json",), optional=True)
        if (current[0] if current is not None else None) != expected:
            raise ReplicationCASConflict("destination head CAS conflict")
        candidate = _destination_read_at(head_history_fd, (slot_name,))
        if candidate is None or candidate[0] != payload:
            raise ReplicationCASConflict("destination head history candidate changed")
        if expected is None:
            os.link(
                slot_name,
                "head.json",
                src_dir_fd=head_history_fd,
                dst_dir_fd=replication_fd,
                follow_symlinks=False,
            )
        else:
            _destination_exchange_at(
                head_history_fd,
                slot_name,
                replication_fd,
                "head.json",
            )
        # The link or exchange syscall is the unique linearization point.
        linearized = True
        _fsync_open_directory(head_history_fd)
        _fsync_open_directory(replication_fd)
        readback = _destination_read_at(replication_fd, ("head.json",))
        if readback is None or readback[0] != payload:
            raise ReplicationDurabilityError("destination head readback failed")
        return DestinationHeadInstallResult(linearized=True)
    except ReplicationCASConflict:
        raise
    except ReplicationDurabilityError as exc:
        primary = DestinationHeadInstallError(
            "destination head install failed",
            linearized=linearized,
            reason_code="MOUNT_UNSUPPORTED" if "unsupported" in str(exc) else "COPY_FAILED",
        )
        raise primary from exc
    except OSError as exc:
        primary = DestinationHeadInstallError(
            "destination head install failed", linearized=linearized
        )
        raise primary from exc


def _complete_replication_with_proof(
    store: ImmutableReplicationSidecarStore,
    proof: VerifiedDestinationCommitProof,
) -> ReplicationHead:
    if not isinstance(proof, VerifiedDestinationCommitProof):
        raise ReplicationDurabilityError(
            "replication completion requires a dedicated verified destination proof"
        )
    if (
        proof.intent_id is None
        or proof.worker_id is None
        or proof.state_version is None
        or proof.source_instance_id is None
        or proof.source_instance_sha256 is None
    ):
        raise ReplicationDurabilityError("destination completion proof is incomplete")
    _validate_sha(proof.intent_id, "intent_id")
    _validate_sha(proof.checkpoint_id, "checkpoint_id")
    _validate_sha(proof.destination_generation, "destination_generation")
    _validate_sha(proof.destination_record_sha256, "destination_record_sha256")
    _validate_sha(proof.destination_head_sha256, "destination_head_sha256")
    _validate_utc_timestamp(proof.now, "now")

    def mutate(connection: sqlite3.Connection) -> _MutationOutcome:
        intent = connection.execute(
            """SELECT destination_id, source_sequence, checkpoint_id,
                              source_instance_id, source_instance_sha256
                         FROM replication_intents WHERE intent_id=?""",
            (proof.intent_id,),
        ).fetchone()
        if intent is None or intent != (
            proof.destination_id,
            proof.source_sequence,
            proof.checkpoint_id,
            proof.source_instance_id,
            proof.source_instance_sha256,
        ):
            raise ReplicationStateUnavailable("replication completion proof does not match intent")
        head = store._head_row(connection, proof.intent_id)
        if head[1] != "verifying" or head[2] != proof.state_version or head[4] != proof.worker_id:
            raise ReplicationCASConflict("replication completion lease changed")
        if head[5] is None or _timestamp_value(str(head[5])) <= _timestamp_value(proof.now):
            raise ReplicationCASConflict("replication completion lease expired")
        store._append_event_and_update_head(
            connection,
            intent_id=proof.intent_id,
            from_state="verifying",
            to_state="replicated",
            attempt=int(store._latest_event(connection, proof.intent_id)[6]),
            reason_code="NONE",
            occurred_at=proof.now,
            lease_owner=None,
            lease_until=None,
            next_attempt_at=proof.now,
            destination_replication_generation=proof.destination_generation,
            destination_record_sha256=proof.destination_record_sha256,
            destination_head_sha256=proof.destination_head_sha256,
            event_type="transition",
        )
        return _MutationOutcome(store._head_view(connection, proof.intent_id), True)

    result = store._mutate_generation(mutate)
    if not isinstance(result, ReplicationHead):
        raise ReplicationDurabilityError("replication completion result is invalid")
    return result


class RestorePlanResponse(BaseModel):
    """Bounded, path-free restore plan/effect projection."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    status: Literal["dry_run", "ready", "unavailable"]
    reason_code: ReplicationReason
    source_manifest_canonical_sha256: str | None = None
    source_object_set_sha256: str | None = None
    publication_binding_sha256: str | None = None
    source_instance_id: str | None = None
    source_sequence: int | None = Field(default=None, ge=1)
    checkpoint_id: str | None = None
    object_count: int = Field(ge=0, default=0)
    row_count: int = Field(ge=0, default=0)
    byte_count: int = Field(ge=0, default=0)
    rename_allowed: bool = False
    mode: Literal["plan", "execute"]
    execution_allowed: bool
    effects: ReplicationEffects
    provider_requests: Literal[0] = 0
    paths_exposed: Literal[False] = False

    @model_validator(mode="after")
    def validate_digests(self) -> RestorePlanResponse:
        for value, field in (
            (self.source_manifest_canonical_sha256, "source_manifest_canonical_sha256"),
            (self.source_object_set_sha256, "source_object_set_sha256"),
            (self.publication_binding_sha256, "publication_binding_sha256"),
            (self.source_instance_id, "source_instance_id"),
            (self.checkpoint_id, "checkpoint_id"),
        ):
            if value is not None:
                _validate_sha(value, field)
        return self


class RestoreReport(BaseModel):
    """Sanitized immutable restore evidence; it intentionally has no path."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    report_schema: Literal["stock-eva/r2f4.3/restore-report/v1"] = (
        "stock-eva/r2f4.3/restore-report/v1"
    )
    schema_version: Literal[1] = 1
    report_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    destination_id: str = Field(min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")
    source_replication_generation: str = Field(
        min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    source_record_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_sequence: int = Field(ge=1)
    checkpoint_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    object_count: int = Field(ge=0)
    row_count: int = Field(ge=0)
    byte_count: int = Field(ge=0)
    verification_state: Literal["verified", "failed"]
    reason_code: ReplicationReason
    started_at: str
    completed_at: str
    report_sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")

    _timestamps_are_utc = field_validator("started_at", "completed_at")(
        lambda value, info: _validate_utc_timestamp(value, info.field_name)
    )

    def verify_hash(self) -> None:
        values = self.model_dump(mode="json")
        actual = values.pop("report_sha256")
        expected = domain_sha256("stock-eva/r2f4.3/restore-report/v1", values)
        if actual != expected:
            raise ReplicationDurabilityError("restore report hash is invalid")


def _restore_destination_path(
    path: Path,
    *,
    archive_root: Path,
    canonical_roots: Iterable[Path],
) -> Path:
    """Validate a new temporary child without touching or creating it."""
    raw = str(path)
    if (
        not path.is_absolute()
        or any(token in raw for token in ("$", "~", "`"))
        or path == Path("/")
        or path == Path.home()
    ):
        raise ReplicationDurabilityError("restore destination path is invalid")
    normalized = _physical_path(Path(os.path.normpath(path)))
    roots = [_physical_path(Path(os.path.normpath(archive_root)))]
    roots.extend(_physical_path(Path(os.path.normpath(item))) for item in canonical_roots)
    for root in roots:
        if normalized == root or normalized in root.parents or root in normalized.parents:
            raise ReplicationDurabilityError("restore destination overlaps a protected root")
    _safe_parent(normalized.parent)
    try:
        info = os.lstat(normalized)
    except FileNotFoundError:
        return normalized
    except OSError as exc:
        raise ReplicationDurabilityError("restore destination is unavailable") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ReplicationDurabilityError("restore destination is unsafe")
    raise ReplicationDurabilityError("restore destination already exists")


def _validate_staged_restore_dataset(
    stage: Path,
    record: ReplicationRecord,
    manifest: bytes,
) -> None:
    """Run the strict canonical schema/row/partition checks in staging only."""
    _validate_restore_manifest(manifest, record)
    try:
        from backend.app.storage.dataset import NasMarketStore

        for item in record.object_inventory:
            if not item.relative_path.endswith(".parquet"):
                raise ReplicationStateUnavailable("restore object is not parquet")
            NasMarketStore._validate_parquet(
                stage / item.relative_path,
                item.row_count,
                expected_source=item.source,
                expected_trade_date=datetime.strptime(item.trade_date, "%Y-%m-%d").date(),
            )
    except ReplicationStateUnavailable:
        raise
    except Exception as exc:
        raise ReplicationStateUnavailable("restore object schema or rows are invalid") from exc


class RestoreService:
    """Descriptor-bound NAS-to-new-temporary-root restore operation."""

    def __init__(
        self,
        archive: DestinationDescriptor | Path,
        *,
        mount_inspector: MountInspector | None = None,
        canonical_roots: Iterable[Path] = (),
        representative_query: object | None = None,
    ) -> None:
        self.reader = DestinationArchiveReader(archive, mount_inspector=mount_inspector)
        self.canonical_roots = tuple(Path(item) for item in canonical_roots)
        self.representative_query = representative_query

    def _plan_response(
        self,
        *,
        snapshot: VerifiedArchiveSnapshot | None,
        mode: Literal["plan", "execute"],
        allowed: bool,
        status: Literal["dry_run", "ready", "unavailable"],
        reason_code: ReplicationReason,
        effects: ReplicationEffects,
        rename_allowed: bool = False,
    ) -> RestorePlanResponse:
        record = snapshot.record if snapshot is not None else None
        return RestorePlanResponse(
            status=status,
            reason_code=reason_code,
            source_manifest_canonical_sha256=(
                record.source_manifest_canonical_sha256 if record is not None else None
            ),
            source_object_set_sha256=record.source_object_set_sha256 if record is not None else None,
            publication_binding_sha256=(
                record.publication_binding_sha256 if record is not None else None
            ),
            source_instance_id=record.source_instance_id if record is not None else None,
            source_sequence=record.source_sequence if record is not None else None,
            checkpoint_id=record.checkpoint_id if record is not None else None,
            object_count=record.object_count if record is not None else 0,
            row_count=record.row_count if record is not None else 0,
            byte_count=record.byte_count if record is not None else 0,
            rename_allowed=rename_allowed,
            mode=mode,
            execution_allowed=allowed,
            effects=effects,
        )

    @staticmethod
    def _reason(exc: BaseException) -> ReplicationReason:
        message = str(exc).lower()
        if "query" in message:
            return "VERIFY_FAILED"
        if "conflict" in message:
            return "DESTINATION_CONFLICT"
        if "path" in message or "overlap" in message or "exists" in message:
            return "PATH_CHANGED" if "exists" in message else "PATH_INVALID"
        if "symlink" in message or "unsafe" in message:
            return "SYMLINK_UNSAFE"
        if "unsupported" in message or "mount" in message:
            return "MOUNT_UNSUPPORTED"
        if "object" in message or "manifest" in message or "schema" in message:
            return "VERIFY_FAILED"
        return "SOURCE_UNAVAILABLE"

    def _snapshot(self, generation: str | None) -> VerifiedArchiveSnapshot:
        return self.reader.read_verified_archive(generation=generation)

    def plan(
        self,
        destination: Path,
        *,
        generation: str | None = None,
    ) -> RestorePlanResponse:
        effects = ReplicationEffects(
            writes=False,
            canonical_writes=False,
            destination_writes=False,
            outbox_writes=False,
            restore_writes=False,
        )
        try:
            _restore_destination_path(
                Path(destination),
                archive_root=self.reader.descriptor.root_path,
                canonical_roots=self.canonical_roots,
            )
            snapshot = self._snapshot(generation)
        except Exception as exc:
            return self._plan_response(
                snapshot=None,
                mode="plan",
                allowed=False,
                status="unavailable",
                reason_code=self._reason(exc),
                effects=effects,
            )
        return self._plan_response(
            snapshot=snapshot,
            mode="plan",
            allowed=False,
            status="dry_run",
            reason_code="NONE",
            effects=effects,
            rename_allowed=True,
        )

    def execute(
        self,
        destination: Path,
        *,
        generation: str | None = None,
        representative_query: object | None = None,
    ) -> RestorePlanResponse:
        effects = ReplicationEffects(
            writes=False,
            canonical_writes=False,
            destination_writes=False,
            outbox_writes=False,
            restore_writes=False,
        )
        destination = Path(destination)
        try:
            final = _restore_destination_path(
                destination,
                archive_root=self.reader.descriptor.root_path,
                canonical_roots=self.canonical_roots,
            )
            snapshot = self._snapshot(generation)
        except (ReplicationDurabilityError, ValueError, OSError) as exc:
            return self._plan_response(
                snapshot=None,
                mode="execute",
                allowed=True,
                status="unavailable",
                reason_code=self._reason(exc),
                effects=effects,
            )

        parent_fd, parent_dirs = _open_directory_chain(final.parent, create=False)
        stage_name = f".{final.name}.restore-{secrets.token_hex(16)}.staging"
        stage_fd: int | None = None
        published = False
        restore_writes = False
        try:
            try:
                os.mkdir(stage_name, mode=0o700, dir_fd=parent_fd)
            except FileExistsError as exc:
                raise ReplicationDurabilityError("restore staging conflict") from exc
            restore_writes = True
            parent_identity = _directory_fingerprint(os.fstat(parent_fd))
            stage_fd = _destination_open_dir(parent_fd, stage_name)
            with self.reader.root_session() as source_root_fd:
                generation_fd, generation_dirs = self.reader._open_generation(
                    source_root_fd, snapshot.record.replication_generation
                )
                try:
                    for item in snapshot.record.object_inventory:
                        payload = _destination_read_relative(generation_fd, item.relative_path)
                        if len(payload) != item.size_bytes or _sha256_bytes(payload) != item.object_sha256:
                            raise ReplicationStateUnavailable("restore source object changed")
                        target_fd = stage_fd
                        target_dirs: list[int] = []
                        parts = tuple(item.relative_path.split("/"))
                        try:
                            for part in parts[:-1]:
                                target_fd = _destination_mkdir_at(target_fd, part)
                                target_dirs.append(target_fd)
                            _destination_write_at(target_fd, parts[-1], payload)
                            readback = _destination_read_at(
                                target_fd, (parts[-1],), require_private_mode=False
                            )
                            if (
                                readback is None
                                or len(readback[0]) != item.size_bytes
                                or _sha256_bytes(readback[0]) != item.object_sha256
                            ):
                                raise ReplicationStateUnavailable("restore object readback failed")
                        finally:
                            failures: list[str] = []
                            _close_descriptors(target_dirs, failures)
                            _finish_cleanup(None, failures)
                    # The publication metadata is written only after every
                    # immutable object has passed source and staging readback.
                    _destination_write_at(stage_fd, DESTINATION_SENTINEL_NAME, snapshot.sentinel_bytes)
                    _destination_write_at(stage_fd, "manifest.json", snapshot.manifest_bytes)
                    _validate_staged_restore_dataset(
                        final.parent / stage_name,
                        snapshot.record,
                        snapshot.manifest_bytes,
                    )
                    callback = representative_query or self.representative_query
                    if callback is not None:
                        if not callable(callback):
                            raise ReplicationDurabilityError("restore query validator is invalid")
                        callback(final.parent / stage_name)
                finally:
                    failures = []
                    _close_descriptors(generation_dirs, failures)
                    _finish_cleanup(None, failures)
            # Re-read the archive after copy.  This catches source drift before
            # the only visibility point without relying on mutable path state.
            fresh = self._snapshot(snapshot.record.replication_generation)
            if fresh.record.record_sha256 != snapshot.record.record_sha256:
                raise ReplicationStateUnavailable("restore source changed")
            _assert_directory_fingerprint(parent_fd, parent_identity)
            _fsync_open_directory(stage_fd)
            os.close(stage_fd)
            stage_fd = None
            _fsync_open_directory(parent_fd)
            _destination_rename_noreplace(parent_fd, stage_name, final.name)
            published = True
            _fsync_open_directory(parent_fd)
            # Post-rename verification is intentionally non-semantic: all
            # schema/count/query gates completed before this exchange.
            final_fd, final_dirs = _open_directory_chain(final, create=False)
            try:
                result = _destination_read_at(final_fd, ("manifest.json",), require_private_mode=False)
                if result is None or result[0] != snapshot.manifest_bytes:
                    raise ReplicationStateUnavailable("restore publication readback changed")
            finally:
                failures = []
                _close_descriptors(final_dirs, failures)
                _finish_cleanup(None, failures)
            effects = ReplicationEffects(
                writes=True,
                canonical_writes=False,
                destination_writes=False,
                outbox_writes=False,
                restore_writes=True,
            )
            return self._plan_response(
                snapshot=snapshot,
                mode="execute",
                allowed=True,
                status="ready",
                reason_code="NONE",
                effects=effects,
                rename_allowed=True,
            )
        except Exception as exc:
            effects = ReplicationEffects(
                writes=restore_writes,
                canonical_writes=False,
                destination_writes=False,
                outbox_writes=False,
                restore_writes=restore_writes,
            )
            return self._plan_response(
                snapshot=snapshot,
                mode="execute",
                allowed=True,
                status="unavailable",
                reason_code=self._reason(exc),
                effects=effects,
            )
        finally:
            failures: list[str] = []
            if stage_fd is not None:
                _close_fd_best_effort(stage_fd, "restore_staging_fd", failures)
            if not published:
                try:
                    _destination_remove_tree_at(parent_fd, stage_name)
                except (ReplicationDurabilityError, OSError):
                    failures.append("restore_staging_cleanup")
            _close_descriptors(parent_dirs, failures)
            _finish_cleanup(None, failures)

    def nas_to_temporary_root(
        self,
        destination: Path,
        *,
        execute: bool = False,
        generation: str | None = None,
        representative_query: object | None = None,
    ) -> RestorePlanResponse:
        return self.execute(destination, generation=generation, representative_query=representative_query) if execute else self.plan(destination, generation=generation)


VerifiedRestoreService = RestoreService
RestoreVerifier = RestoreService
VerifiedRestore = RestoreService
RestoreResult = RestorePlanResponse
RestorePlan = RestorePlanResponse
nas_to_temporary_root = RestoreService


__all__ = [
    "Effects",
    "SIDECAR_DDL",
    "SIDECAR_DDL_SHA256",
    "SIDECAR_SCHEMA_DIGEST",
    "SIDECAR_STAGING_NAME",
    "DATASET_IDENTITY_DOMAIN",
    "SOURCE_DATASET_DOMAIN",
    "SOURCE_INSTANCE_DOMAIN",
    "SOURCE_OBJECT_SET_DOMAIN",
    "GENERATION_PAYLOAD_DOMAIN",
    "GENERATION_TRUST_SCOPE",
    "JournalRecord",
    "ObjectInventoryEntry",
    "ReplicationDurabilityError",
    "DestinationHeadInstallError",
    "DestinationHeadInstallResult",
    "ReplicationCASConflict",
    "ReplicationClaim",
    "ReplicationClaimContext",
    "ReplicationHead",
    "ReplicationImportResult",
    "ReplicationIntent",
    "ReplicationOutboxResult",
    "ReplicationOutboxService",
    "ReplicationPostCommitConflict",
    "ReplicationEffects",
    "ReplicationReason",
    "ReplicationSidecarStore",
    "OutboxState",
    "RETRY_DELAYS_SECONDS",
    "MAX_REPLICATION_ATTEMPTS",
    "REPLICATION_LEASE_SECONDS",
    "ReplicationState",
    "ReplicationStateUnavailable",
    "ReplicationStatusResponse",
    "ReplicationStatus",
    "ReplicationStatusService",
    "VerifiedDestinationCommitProof",
    "DestinationDescriptor",
    "DestinationTrust",
    "DestinationArchiveReader",
    "DestinationCommitVerifier",
    "DestinationArchiveWriter",
    "DestinationReplicationService",
    "ReplicationDestinationWriter",
    "DestinationWriter",
    "DestinationReplicationResult",
    "ReplicationRecord",
    "DestinationHead",
    "SentinelProjection",
    "build_replication_record",
    "build_destination_head",
    "initialize_destination",
    "build_destination_descriptor",
    "DESTINATION_DESCRIPTOR_SCHEMA",
    "DESTINATION_SENTINEL_SCHEMA",
    "DESTINATION_RECORD_SCHEMA",
    "DESTINATION_HEAD_SCHEMA",
    "DESTINATION_INIT_ACK",
    "DESTINATION_DESCRIPTOR_NAME",
    "DESTINATION_SENTINEL_NAME",
    "DESTINATION_REPLICATION_DIR",
    "DESTINATION_HISTORY_DIR",
    "DESTINATION_STAGING_DIR",
    "DESTINATION_LOCK_NAME",
    "SourceCheckpoint",
    "SourceInstanceRecord",
    "SourceInstanceStore",
    "build_journal_record",
    "build_source_checkpoint",
    "canonical_json_bytes",
    "domain_sha256",
    "is_retryable_replication_reason",
    "normalized_ddl_bytes",
    "RestorePlanResponse",
    "RestoreReport",
    "VerifiedArchiveSnapshot",
    "RestoreService",
    "VerifiedRestoreService",
    "RestoreVerifier",
    "VerifiedRestore",
    "RestoreResult",
    "RestorePlan",
    "nas_to_temporary_root",
]


# Short aliases keep the public projection vocabulary aligned with the design
# document while retaining the explicit response class used by the API layer.
Effects = ReplicationEffects
ReplicationStatus = ReplicationStatusResponse
