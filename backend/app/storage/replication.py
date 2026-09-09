# ruff: noqa: E501

"""Local replication primitives.

This module deliberately contains only the durable local evidence boundary for
R2-F4.3.  It does not copy to a destination, restore a dataset, or contact a
provider.  The sidecar reader is read-only; initialization and journal writes
are explicit writer operations.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, Self, get_args

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.app.config import Settings
from backend.app.storage.layout import StorageLayout

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


class ReplicationStateUnavailable(ReplicationDurabilityError):
    """The local sidecar is missing, corrupt, or cannot be proven consistent."""


class ReplicationCASConflict(ReplicationDurabilityError):
    """A deterministic next generation already exists with no overwrite allowed."""


class ReplicationPostCommitConflict(ReplicationStateUnavailable):
    """A generation was linked, then its final namespace proof failed."""

    reason_code: Literal["CONTROL_STATE_UNAVAILABLE"] = "CONTROL_STATE_UNAVAILABLE"


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
        try:
            values = json.loads(payload)
            projection = dict(values["checkpoint_projection"])
            projection["object_inventory"] = tuple(projection.get("object_inventory", ()))
            values["checkpoint_projection"] = projection
            record = cls.model_validate(values)
        except (KeyError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ReplicationDurabilityError("replication journal is invalid") from exc
        if Path(path).name != f"{record.checkpoint_id}.json":
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


def _scan_generation_namespace(
    root_fd: int, *, require_genesis: bool = True, ignore_name: str | None = None
) -> tuple[
    dict[str, object] | None, bytes | None, _GenerationEntry | None, tuple[_GenerationEntry, ...]
]:
    try:
        names = os.listdir(root_fd)
    except OSError as exc:
        raise ReplicationStateUnavailable("replication sidecar namespace is unavailable") from exc
    allowed = {SIDECAR_LOCK_NAME, SIDECAR_GENESIS_NAME}
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
        if fingerprint is None or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise ReplicationStateUnavailable("replication sidecar generation is unsafe")
        entries.append(_GenerationEntry(sequence, name, payload, info, fingerprint))
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
        # ignored, including on an otherwise empty sidecar.
        genesis, genesis_payload, _latest, entries = _scan_generation_namespace(
            root_fd, require_genesis=False
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
    temp = f".{name}.{uuid.uuid4().hex}.tmp"
    temp_fd: int | None = None
    primary: BaseException | None = None
    failures: list[str] = []
    installed = False
    try:
        temp_fd = os.open(
            temp,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=root_fd,
        )
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
            os.link(temp, name, src_dir_fd=root_fd, dst_dir_fd=root_fd, follow_symlinks=False)
        except FileExistsError as exc:
            raise ReplicationCASConflict("replication generation CAS conflict") from exc
        installed = True
        os.unlink(temp, dir_fd=root_fd)
        temp = ""
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
        if temp:
            try:
                os.unlink(temp, dir_fd=root_fd)
            except FileNotFoundError:
                pass
            except OSError:
                failures.append("generation_temp_unlink")
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
    def __init__(self, path: Path) -> None:
        self.path = _sidecar_root(path)

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
                else SourceCheckpoint.model_validate(checkpoint)
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
            for name in sorted(os.listdir(root_fd)):
                if not isinstance(name, str) or not re.fullmatch(r"[0-9a-f]{64}\.json", name):
                    raise ReplicationStateUnavailable("replication journal namespace is invalid")
                path = root / name
                record = JournalRecord.read(path)
                records.append((record, name))
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
        result = self.import_journals(
            [record for record, _name in records],
            operation_day=operation_day,
            destination_id=destination_id,
            created_at=created_at,
        )
        # The generation is already durable.  Unlinking is an optimization and
        # is deliberately outside the transaction so a crash is harmless.
        unlink_failures: list[str] = []
        root_fd, descriptors = _open_directory_chain(root, create=False)
        try:
            for _record, name in records:
                try:
                    os.unlink(name, dir_fd=root_fd)
                except FileNotFoundError:
                    pass
                except OSError:
                    unlink_failures.append("journal_unlink")
            if not unlink_failures:
                _fsync_open_directory(root_fd)
        finally:
            _close_descriptors(descriptors, unlink_failures)
        if unlink_failures:
            raise ReplicationDurabilityError("replication journal cleanup is unavailable")
        return result

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


__all__ = [
    "Effects",
    "SIDECAR_DDL",
    "SIDECAR_DDL_SHA256",
    "SIDECAR_SCHEMA_DIGEST",
    "DATASET_IDENTITY_DOMAIN",
    "SOURCE_DATASET_DOMAIN",
    "SOURCE_INSTANCE_DOMAIN",
    "SOURCE_OBJECT_SET_DOMAIN",
    "GENERATION_PAYLOAD_DOMAIN",
    "GENERATION_TRUST_SCOPE",
    "JournalRecord",
    "ObjectInventoryEntry",
    "ReplicationDurabilityError",
    "ReplicationCASConflict",
    "ReplicationClaim",
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
    "SourceCheckpoint",
    "SourceInstanceRecord",
    "SourceInstanceStore",
    "build_journal_record",
    "build_source_checkpoint",
    "canonical_json_bytes",
    "domain_sha256",
    "is_retryable_replication_reason",
    "normalized_ddl_bytes",
]


# Short aliases keep the public projection vocabulary aligned with the design
# document while retaining the explicit response class used by the API layer.
Effects = ReplicationEffects
ReplicationStatus = ReplicationStatusResponse
