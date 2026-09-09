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
import secrets
import sqlite3
import stat
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
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
JOURNAL_SCHEMA_VERSION = 1

ReplicationState = Literal["disabled", "ready", "degraded", "unavailable"]
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
        connection.close()


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
    created_at TEXT NOT NULL
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
BEFORE UPDATE ON replication_sidecar_meta BEGIN
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
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass
        if isinstance(exc, ReplicationDurabilityError):
            raise
        raise ReplicationDurabilityError("replication path ancestor is unavailable") from exc


def _close_descriptors(descriptors: list[int]) -> None:
    for descriptor in reversed(descriptors):
        try:
            os.close(descriptor)
        except OSError:
            pass


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
    try:
        fd = os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=parent_fd,
        )
        return _read_descriptor(fd, require_private_mode=require_private_mode)
    except FileNotFoundError as exc:
        raise ReplicationDurabilityError("replication artifact is unreadable") from exc
    except OSError as exc:
        raise ReplicationDurabilityError("replication artifact read failed") from exc
    finally:
        if fd is not None:
            os.close(fd)
        _close_descriptors(descriptors)


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
        raise ReplicationDurabilityError("replication artifact read failed") from exc
    finally:
        if fd is not None:
            os.close(fd)
        _close_descriptors(descriptors)


def _stat_nofollow(path: Path) -> os.stat_result:
    path = _physical_path(path)
    _, descriptors = _open_directory_chain(path.parent)
    fd: int | None = None
    try:
        fd = os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=descriptors[-1],
        )
        return os.fstat(fd)
    except OSError as exc:
        raise ReplicationDurabilityError("replication artifact is unreadable") from exc
    finally:
        if fd is not None:
            os.close(fd)
        _close_descriptors(descriptors)


def _fsync_directory(path: Path) -> None:
    path = _physical_path(path)
    _, descriptors = _open_directory_chain(path)
    fd = descriptors[-1]
    try:
        _fsync_open_directory(fd)
    finally:
        _close_descriptors(descriptors)


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
        raise ReplicationDurabilityError("replication artifact fsync failed") from exc
    finally:
        if fd is not None:
            os.close(fd)
        _close_descriptors(descriptors)


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
    primary_error: BaseException | None = None
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
                os.close(target_fd)
                target_fd = None
            if existing == payload:
                return path
            raise ReplicationDurabilityError(
                "replication durability conflict: artifact already exists"
            )

        fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        try:
            _write_fully(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
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
                os.close(target_fd)
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
        cleanup_error: OSError | None = None
        try:
            os.unlink(temporary_name, dir_fd=parent_fd)
        except FileNotFoundError:
            pass
        except OSError as exc:
            cleanup_error = exc
        if target_fd is not None:
            try:
                os.close(target_fd)
            except OSError:
                pass
        _close_descriptors(descriptors)
        if cleanup_error is not None:
            error = ReplicationDurabilityError("replication temporary cleanup failed")
            if primary_error is not None:
                raise error from primary_error
            raise error from cleanup_error


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
        intent_preimage = {
            field: values[field] for field in columns if field not in {"intent_sha256"}
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
        final_occurred_at = rows[-1][9]
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


def _stat_optional_nofollow(path: Path) -> os.stat_result | None:
    """Stat an optional artifact without resolving any path component."""
    path = _physical_path(path)
    try:
        _, descriptors = _open_directory_chain(path.parent)
    except ReplicationDurabilityError:
        if not path.parent.exists():
            return None
        raise
    fd: int | None = None
    try:
        try:
            fd = os.open(
                path.name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptors[-1],
            )
        except FileNotFoundError:
            return None
        return os.fstat(fd)
    except OSError as exc:
        raise ReplicationDurabilityError("replication artifact stat failed") from exc
    finally:
        if fd is not None:
            os.close(fd)
        _close_descriptors(descriptors)


def _stat_optional_at(parent_fd: int, name: str) -> os.stat_result | None:
    """Stat an optional basename relative to an already trusted directory fd."""
    fd: int | None = None
    try:
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            return None
        return os.fstat(fd)
    except OSError as exc:
        raise ReplicationDurabilityError("replication artifact stat failed") from exc
    finally:
        if fd is not None:
            os.close(fd)


def _capture_sqlite_snapshot(
    path: Path,
) -> dict[
    str,
    tuple[Path, bytes | None, os.stat_result | None, tuple[int, int, int, int, str | None] | None],
]:
    path = _physical_path(path)
    try:
        main_payload, main_stat = _read_nofollow(path)
    except ReplicationDurabilityError as exc:
        raise ReplicationStateUnavailable(
            "replication sidecar source descriptor is unavailable"
        ) from exc
    snapshot: dict[
        str,
        tuple[
            Path, bytes | None, os.stat_result | None, tuple[int, int, int, int, str | None] | None
        ],
    ] = {"main": (path, main_payload, main_stat, _stat_fingerprint(main_stat, main_payload))}
    for name, sidecar in (("wal", Path(f"{path}-wal")), ("shm", Path(f"{path}-shm"))):
        try:
            sidecar_stat = _stat_optional_nofollow(sidecar)
        except ReplicationDurabilityError as exc:
            raise ReplicationStateUnavailable(
                "replication sidecar auxiliary descriptor is unavailable"
            ) from exc
        if sidecar_stat is not None:
            raise ReplicationStateUnavailable(f"replication sidecar {name.upper()} is present")
        snapshot[name] = (sidecar, None, None, None)
    return snapshot


def _assert_sqlite_snapshot_unchanged(
    snapshot: dict[
        str,
        tuple[
            Path, bytes | None, os.stat_result | None, tuple[int, int, int, int, str | None] | None
        ],
    ],
) -> None:
    for name, (path, _payload, _prior_stat, prior_fingerprint) in snapshot.items():
        if name == "main":
            try:
                current_payload, current_stat = _read_nofollow(path)
            except ReplicationDurabilityError as exc:
                raise ReplicationStateUnavailable(
                    "replication sidecar changed during read"
                ) from exc
            current_fingerprint = _stat_fingerprint(current_stat, current_payload)
        else:
            try:
                current_stat = _stat_optional_nofollow(path)
            except ReplicationDurabilityError as exc:
                raise ReplicationStateUnavailable(
                    "replication sidecar changed during read"
                ) from exc
            current_fingerprint = _stat_fingerprint(current_stat)
        if current_fingerprint != prior_fingerprint:
            raise ReplicationStateUnavailable("replication sidecar changed during read")


_SqliteFingerprint = tuple[int, int, int, int, str | None]
_SqliteSnapshot = dict[
    str, tuple[Path, bytes | None, os.stat_result | None, _SqliteFingerprint | None]
]


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
    except ReplicationStateUnavailable:
        if connection is not None:
            connection.close()
        raise
    except BaseException as exc:
        if connection is not None:
            connection.close()
        if isinstance(exc, ReplicationStateUnavailable):
            raise
        raise ReplicationStateUnavailable(
            "replication sidecar in-memory deserialize failed"
        ) from exc


def _open_verified_sqlite_readonly(path: Path) -> tuple[sqlite3.Connection, _SqliteSnapshot]:
    """Open a SELECT-only SQLite image without any filesystem database."""
    snapshot = _capture_sqlite_snapshot(path)
    main_payload = snapshot["main"][1]
    if main_payload is None:
        raise ReplicationStateUnavailable("replication sidecar source is empty")
    connection = _deserialize_sqlite_bytes(main_payload)
    try:
        _assert_sqlite_snapshot_unchanged(snapshot)
    except BaseException:
        connection.close()
        raise
    return connection, snapshot


@dataclass
class _WriterSession:
    path: Path
    parent_fd: int
    descriptors: list[int]
    target_fd: int | None
    payload: bytes | None
    stat: os.stat_result | None
    fingerprint: _SqliteFingerprint | None
    lock_fd: int | None = None
    lock_token: _WriterLockToken | None = None


class _WriterLockToken:
    """Opaque proof that a sidecar writer owns its descriptor-bound lock."""

    __slots__ = ("lock_dev", "lock_ino", "released")

    def __init__(self, info: os.stat_result) -> None:
        self.lock_dev = info.st_dev
        self.lock_ino = info.st_ino
        self.released = False


_WRITER_LOCK_SUFFIX = ".lock"


def _acquire_writer_lock(parent_fd: int, lock_name: str) -> tuple[int, _WriterLockToken]:
    """Acquire the fixed sidecar writer lock through a trusted parent dirfd."""
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
        return lock_fd, _WriterLockToken(after)
    except ReplicationDurabilityError:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(lock_fd)
            except OSError:
                pass
        raise
    except OSError as exc:
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
            try:
                os.close(lock_fd)
            except OSError:
                pass
        raise ReplicationDurabilityError("replication writer lock is unavailable") from exc


def _release_writer_lock(lock_fd: int, token: _WriterLockToken) -> None:
    if token.released:
        return
    token.released = True
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        os.close(lock_fd)
    except OSError:
        pass


def _require_writer_lock(session: _WriterSession) -> None:
    """Reject CAS helpers invoked without the opaque lock ownership proof."""
    token = session.lock_token
    if session.lock_fd is None or token is None:
        raise ReplicationDurabilityError("replication writer lock token is missing")
    if token.released:
        raise ReplicationDurabilityError("replication writer lock token is released")
    try:
        info = os.fstat(session.lock_fd)
        if (info.st_dev, info.st_ino) != (token.lock_dev, token.lock_ino):
            raise ReplicationDurabilityError("replication writer lock identity changed")
        # LOCK_NB is only a proof check: this descriptor already owns the
        # exclusive lock, and the call never releases or replaces it.
        fcntl.flock(session.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ReplicationDurabilityError:
        raise
    except OSError as exc:
        raise ReplicationDurabilityError("replication writer lock is not held") from exc


def _open_writer_session(path: Path) -> _WriterSession:
    """Open the fixed database basename through a trusted parent dirfd."""
    path = _physical_path(path)
    parent_fd, descriptors = _open_directory_chain(path.parent, create=True)
    target_fd: int | None = None
    lock_fd: int | None = None
    lock_token: _WriterLockToken | None = None
    try:
        lock_fd, lock_token = _acquire_writer_lock(parent_fd, f"{path.name}{_WRITER_LOCK_SUFFIX}")
        for suffix in ("-wal", "-shm"):
            try:
                auxiliary_stat = _stat_optional_at(parent_fd, f"{path.name}{suffix}")
            except ReplicationDurabilityError as exc:
                raise ReplicationStateUnavailable(
                    f"replication sidecar {suffix[1:].upper()} is unavailable"
                ) from exc
            if auxiliary_stat is not None:
                raise ReplicationStateUnavailable(
                    f"replication sidecar {suffix[1:].upper()} is present"
                )
        try:
            target_fd = os.open(
                path.name,
                os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            if lock_token is None or lock_fd is None:
                raise ReplicationDurabilityError("replication writer lock is unavailable") from None
            return _WriterSession(
                path, parent_fd, descriptors, None, None, None, None, lock_fd, lock_token
            )
        payload, info = _read_descriptor(target_fd)
        fingerprint = _stat_fingerprint(info, payload)
        if lock_token is None or lock_fd is None:
            raise ReplicationDurabilityError("replication writer lock is unavailable")
        return _WriterSession(
            path,
            parent_fd,
            descriptors,
            target_fd,
            payload,
            info,
            fingerprint,
            lock_fd,
            lock_token,
        )
    except BaseException:
        if target_fd is not None:
            try:
                os.close(target_fd)
            except OSError:
                pass
        if lock_fd is not None and lock_token is not None:
            _release_writer_lock(lock_fd, lock_token)
        _close_descriptors(descriptors)
        raise


def _close_writer_session(session: _WriterSession) -> None:
    if session.target_fd is not None:
        try:
            os.close(session.target_fd)
        except OSError:
            pass
    if session.lock_fd is not None and session.lock_token is not None:
        _release_writer_lock(session.lock_fd, session.lock_token)
    _close_descriptors(session.descriptors)


def _read_writer_target(session: _WriterSession) -> tuple[bytes, os.stat_result]:
    """Read the current fixed basename through the already-open parent dirfd."""
    fd: int | None = None
    try:
        fd = os.open(
            session.path.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=session.parent_fd,
        )
        return _read_descriptor(fd)
    except FileNotFoundError as exc:
        raise ReplicationDurabilityError("replication sidecar target disappeared") from exc
    except OSError as exc:
        raise ReplicationDurabilityError("replication sidecar target is unsafe") from exc
    finally:
        if fd is not None:
            os.close(fd)


def _assert_writer_session_stable(session: _WriterSession) -> None:
    """CAS-check both the held inode and the fixed directory entry."""
    _require_writer_lock(session)
    if session.fingerprint is None:
        try:
            _read_writer_target(session)
        except ReplicationDurabilityError as exc:
            if "disappeared" in str(exc):
                return
            raise
        raise ReplicationDurabilityError("replication sidecar target appeared during write")
    try:
        current_payload, current_stat = _read_writer_target(session)
    except ReplicationDurabilityError as exc:
        raise ReplicationDurabilityError(
            "replication sidecar identity changed during write"
        ) from exc
    current_fingerprint = _stat_fingerprint(current_stat, current_payload)
    if current_fingerprint != session.fingerprint:
        raise ReplicationDurabilityError("replication sidecar identity changed during write")
    if session.target_fd is None:
        raise ReplicationDurabilityError("replication sidecar descriptor is unavailable")
    held_payload, held_stat = _read_descriptor(session.target_fd)
    if _stat_fingerprint(held_stat, held_payload) != session.fingerprint:
        raise ReplicationDurabilityError("replication sidecar descriptor changed during write")


def _install_memory_snapshot(session: _WriterSession, payload: bytes) -> None:
    """Persist a private image using the held parent dirfd and a CAS."""
    _require_writer_lock(session)
    temporary_name = f".{session.path.name}.{uuid.uuid4().hex}.tmp"
    temporary_fd: int | None = None
    primary_error: BaseException | None = None
    try:
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
            dir_fd=session.parent_fd,
        )
        _write_fully(temporary_fd, payload)
        os.fsync(temporary_fd)
        os.close(temporary_fd)
        temporary_fd = None
        _assert_writer_session_stable(session)
        if session.fingerprint is None:
            try:
                os.link(
                    temporary_name,
                    session.path.name,
                    src_dir_fd=session.parent_fd,
                    dst_dir_fd=session.parent_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                existing, _ = _read_writer_target(session)
                if existing != payload:
                    raise ReplicationDurabilityError(
                        "replication sidecar target appeared with different bytes"
                    ) from exc
        elif session.payload == b"":
            os.replace(
                temporary_name,
                session.path.name,
                src_dir_fd=session.parent_fd,
                dst_dir_fd=session.parent_fd,
            )
        else:
            raise ReplicationDurabilityError("replication sidecar is already initialized")
        final_payload, _ = _read_writer_target(session)
        if final_payload != payload:
            raise ReplicationDurabilityError("replication sidecar install readback mismatch")
        _fsync_open_directory(session.parent_fd)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if temporary_fd is not None:
            try:
                os.close(temporary_fd)
            except OSError:
                pass
        cleanup_error: OSError | None = None
        try:
            os.unlink(temporary_name, dir_fd=session.parent_fd)
        except FileNotFoundError:
            pass
        except OSError as exc:
            cleanup_error = exc
        if cleanup_error is not None:
            error = ReplicationDurabilityError("replication temporary cleanup failed")
            if primary_error is not None:
                raise error from primary_error
            raise error from cleanup_error


class ReplicationSidecarStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    @staticmethod
    def _connect_writer(path: Path) -> sqlite3.Connection:
        """Compatibility probe returning an unpersisted descriptor-native image."""
        session = _open_writer_session(path)
        connection: sqlite3.Connection | None = None
        try:
            connection = (
                _deserialize_sqlite_bytes(session.payload)
                if session.payload
                else sqlite3.connect(":memory:")
            )
            _assert_writer_session_stable(session)
            return connection
        except ReplicationStateUnavailable:
            if connection is not None:
                connection.close()
            raise
        except ReplicationDurabilityError:
            if connection is not None:
                connection.close()
            raise
        except (OSError, sqlite3.Error) as exc:
            if connection is not None:
                connection.close()
            raise ReplicationDurabilityError("replication sidecar is not writable") from exc
        except BaseException:
            if connection is not None:
                connection.close()
            raise
        finally:
            _close_writer_session(session)

    def initialize(
        self,
        *,
        source_instance_id: str,
        source_instance_sha256: str,
        created_at: str | None = None,
    ) -> None:
        _validate_sha(source_instance_id, "source_instance_id")
        _validate_sha(source_instance_sha256, "source_instance_sha256")
        session = _open_writer_session(self.path)
        connection: sqlite3.Connection | None = None
        try:
            if session.payload:
                connection = _deserialize_sqlite_bytes(session.payload)
                connection.execute("PRAGMA query_only = ON")
                meta = connection.execute(
                    """SELECT source_instance_id, source_instance_sha256
                         FROM replication_sidecar_meta"""
                ).fetchall()
                _assert_writer_session_stable(session)
                if meta != [(source_instance_id, source_instance_sha256)]:
                    raise ReplicationDurabilityError(
                        "replication sidecar source identity conflicts"
                    )
                # Existing sidecars are never migrated by initialization.
                self.read_status()
                return
            connection = sqlite3.connect(":memory:")
            connection.executescript(SIDECAR_DDL)
            connection.execute(
                """INSERT INTO replication_sidecar_meta
                   (sidecar_id, schema_version, schema_identity, ddl_sha256, schema_digest,
                    source_instance_id, source_instance_sha256, created_at)
                   VALUES (1, 1, ?, ?, ?, ?, ?, ?)""",
                (
                    SIDECAR_SCHEMA_IDENTITY,
                    SIDECAR_DDL_SHA256,
                    SIDECAR_SCHEMA_DIGEST,
                    source_instance_id,
                    source_instance_sha256,
                    created_at or _utc_now(),
                ),
            )
            connection.commit()
            payload = connection.serialize(name="main")
            _install_memory_snapshot(session, payload)
        except ReplicationDurabilityError:
            raise
        except (OSError, sqlite3.Error) as exc:
            if connection is not None:
                connection.rollback()
            raise ReplicationDurabilityError("replication sidecar initialization failed") from exc
        finally:
            if connection is not None:
                connection.close()
            _close_writer_session(session)

    def _fsync_database(self) -> None:
        database_path = _physical_path(self.path)
        _fsync_nofollow(database_path)
        _fsync_directory(database_path.parent)

    def read_status(
        self,
        *,
        local_ready: bool = False,
        source_instance: SourceInstanceRecord | None = None,
    ) -> ReplicationStatusResponse:
        connection: sqlite3.Connection | None = None
        snapshot: _SqliteSnapshot | None = None
        try:
            connection, snapshot = _open_verified_sqlite_readonly(self.path)
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
            try:
                _validate_utc_timestamp(meta[0][6], "created_at")
            except (TypeError, ValueError) as exc:
                raise ReplicationStateUnavailable(
                    "replication sidecar timestamp is invalid"
                ) from exc
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
            invalid = set(counts) - {
                "pending",
                "copying",
                "verifying",
                "retry_wait",
                "replicated",
                "dead_letter",
            }
            if invalid:
                raise ReplicationStateUnavailable("replication sidecar state is invalid")
            _validate_event_reachability(
                connection,
                expected_source_instance_id=meta[0][4],
                expected_source_instance_sha256=meta[0][5],
            )
            replicated = connection.execute(
                """SELECT i.manifest_canonical_sha256, e.occurred_at
                     FROM replication_heads h
                     JOIN replication_intents i ON i.intent_id = h.intent_id
                     JOIN replication_attempt_events e
                       ON e.intent_id = h.intent_id AND e.event_sequence = h.last_event_sequence
                    WHERE h.current_state = 'replicated'
                    ORDER BY e.occurred_at DESC LIMIT 1"""
            ).fetchone()
            row = connection.execute(
                """SELECT destination_id, descriptor_sha256, head_sha256,
                          replication_generation, record_sha256, source_instance_id,
                          source_sequence, health_state, health_observed_at,
                          cache_version, updated_at
                     FROM replication_destination_cache
                    ORDER BY updated_at DESC LIMIT 1"""
            ).fetchone()
            if snapshot is None:
                raise ReplicationStateUnavailable("replication sidecar snapshot is unavailable")
            _assert_sqlite_snapshot_unchanged(snapshot)
        except (sqlite3.Error, OSError) as exc:
            raise ReplicationStateUnavailable from exc
        finally:
            if connection is not None:
                connection.close()
        if row is None:
            health, observed = "unknown", None
        else:
            (
                destination_id,
                descriptor_sha256,
                head_sha256,
                replication_generation,
                record_sha256,
                cache_source_instance_id,
                source_sequence,
                health,
                observed,
                cache_version,
                updated_at,
            ) = row
            if not isinstance(destination_id, str) or len(destination_id) != 32:
                raise ReplicationStateUnavailable("replication destination identity is invalid")
            for digest, name in (
                (descriptor_sha256, "descriptor_sha256"),
                (head_sha256, "head_sha256"),
                (replication_generation, "replication_generation"),
                (record_sha256, "record_sha256"),
                (cache_source_instance_id, "source_instance_id"),
            ):
                if digest is not None:
                    try:
                        _validate_sha(digest, name)
                    except (TypeError, ValueError) as exc:
                        raise ReplicationStateUnavailable(
                            "replication cache digest is invalid"
                        ) from exc
            if source_sequence is not None and (
                not isinstance(source_sequence, int) or source_sequence < 1
            ):
                raise ReplicationStateUnavailable("replication cache sequence is invalid")
            if health not in {"unknown", "healthy", "unavailable", "unsupported"}:
                raise ReplicationStateUnavailable("replication cache state is invalid")
            if (health == "unknown") != (observed is None):
                raise ReplicationStateUnavailable("replication cache timestamp is invalid")
            for timestamp, name in ((observed, "health_observed_at"), (updated_at, "updated_at")):
                if timestamp is not None:
                    try:
                        _validate_utc_timestamp(timestamp, name)
                    except (TypeError, ValueError) as exc:
                        raise ReplicationStateUnavailable(
                            "replication cache timestamp is invalid"
                        ) from exc
            if not isinstance(cache_version, int) or cache_version < 0:
                raise ReplicationStateUnavailable("replication cache version is invalid")
        try:
            result = ReplicationStatusResponse(
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
        except (TypeError, ValueError) as exc:
            raise ReplicationStateUnavailable("replication status projection is invalid") from exc
        return result


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
        sidecar = ReplicationSidecarStore(layout.replication_database)
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
    "JournalRecord",
    "ObjectInventoryEntry",
    "ReplicationDurabilityError",
    "ReplicationEffects",
    "ReplicationReason",
    "ReplicationSidecarStore",
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
    "normalized_ddl_bytes",
]


# Short aliases keep the public projection vocabulary aligned with the design
# document while retaining the explicit response class used by the API layer.
Effects = ReplicationEffects
ReplicationStatus = ReplicationStatusResponse
