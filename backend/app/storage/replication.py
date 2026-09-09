# ruff: noqa: E501

"""Local replication primitives.

This module deliberately contains only the durable local evidence boundary for
R2-F4.3.  It does not copy to a destination, restore a dataset, or contact a
provider.  The sidecar reader is read-only; initialization and journal writes
are explicit writer operations.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import stat
import uuid
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.app.config import Settings
from backend.app.storage.layout import StorageLayout

ZERO_SHA256 = "0" * 64
SIDECAR_SCHEMA_IDENTITY = "stock-eva/r2f4.3/replication-sidecar/v1"
SOURCE_INSTANCE_SCHEMA = "stock-eva/r2f4.3/source-instance/v2"
SOURCE_INSTANCE_DOMAIN = "stock-eva/r2f4.3/source-instance/v3"
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
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{field} must be a lower-case SHA-256 digest")


def _validate_utc_timestamp(value: str, field: str) -> str:
    if not value.endswith("Z"):
        raise ValueError(f"{field} must use a UTC Z timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must use UTC")
    return value


def _validate_iso_date(value: str, field: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date") from exc
    return value


def _safe_parent(path: Path) -> None:
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


def _read_nofollow(path: Path) -> tuple[bytes, os.stat_result]:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        raise ReplicationDurabilityError("replication artifact is unreadable") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ReplicationDurabilityError("replication artifact is not a regular file")
        if stat.S_IMODE(before.st_mode) != 0o600:
            raise ReplicationDurabilityError("replication artifact permissions are unsafe")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(fd)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ReplicationDurabilityError("replication artifact changed during read")
        return b"".join(chunks), after
    except OSError as exc:
        raise ReplicationDurabilityError("replication artifact read failed") from exc
    finally:
        os.close(fd)


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        raise ReplicationDurabilityError("replication parent is not durable") from exc
    try:
        os.fsync(fd)
    except OSError as exc:
        raise ReplicationDurabilityError("replication parent fsync failed") from exc
    finally:
        os.close(fd)


def _write_fully(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:
            raise ReplicationDurabilityError("replication artifact short write")
        offset += written


def _install_no_replace(path: Path, payload: bytes) -> Path:
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    _safe_parent(parent)
    if path.exists() or path.is_symlink():
        existing, _ = _read_nofollow(path)
        if existing == payload:
            return path
        raise ReplicationDurabilityError("replication durability conflict: artifact already exists")

    temporary = parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        try:
            _write_fully(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        _fsync_directory(parent)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as exc:
            existing, _ = _read_nofollow(path)
            if existing != payload:
                raise ReplicationDurabilityError(
                    "replication durability conflict: artifact already exists"
                ) from exc
        else:
            _fsync_directory(parent)
        return path
    except OSError as exc:
        raise ReplicationDurabilityError("replication artifact install failed") from exc
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise ReplicationDurabilityError("replication temporary cleanup failed") from exc


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
    outbox_schema_version: int | None
    pending_count: int
    copying_count: int
    verifying_count: int
    retry_wait_count: int
    dead_letter_count: int
    last_replicated_source_manifest_sha256: str | None
    last_replicated_at: str | None
    local_ready: bool
    destination_health: Literal["unknown", "healthy", "unavailable", "unsupported"]
    destination_health_observed_at: str | None
    queue_lag_seconds: int | None
    lag_seconds: int | None
    provider_requests: Literal[0] = 0
    mode: Literal["status"] = "status"
    execution_allowed: Literal[False] = False
    effects: ReplicationEffects
    paths_exposed: Literal[False] = False


class SourceInstanceRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_instance_schema: Literal[SOURCE_INSTANCE_SCHEMA] = SOURCE_INSTANCE_SCHEMA
    schema_version: Literal[2] = 2
    source_instance_id: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    canonical_root_path: str = Field(min_length=1)
    canonical_schema_digest: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source_instance_domain: str = Field(min_length=1)
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


class SourceInstanceStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def create(
        self,
        *,
        canonical_root_path: Path,
        canonical_schema_digest: str,
        source_instance_domain: str,
        source_instance_nonce: str | None = None,
        created_at: str | None = None,
    ) -> SourceInstanceRecord:
        canonical_root = Path(os.path.abspath(canonical_root_path))
        if not canonical_root.is_absolute() or canonical_root == Path("/"):
            raise ReplicationDurabilityError("canonical root is unsafe")
        expected_path = canonical_root / "_replication" / "source-instance.json"
        if Path(os.path.abspath(self.path)) != expected_path:
            raise ReplicationDurabilityError("source instance path is outside canonical layout")
        _validate_sha(canonical_schema_digest, "canonical_schema_digest")
        nonce = source_instance_nonce or secrets.token_hex(32)
        _validate_sha(nonce, "source_instance_nonce")
        dataset_identity = domain_sha256(
            "stock-eva/r2f4.3/dataset-identity/v1",
            {
                "canonical_root_path": str(canonical_root),
                "canonical_schema_digest": canonical_schema_digest,
                "source_instance_domain": source_instance_domain,
            },
        )
        source_instance_id = domain_sha256(
            SOURCE_INSTANCE_DOMAIN,
            {"dataset_identity": dataset_identity, "source_instance_nonce": nonce},
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
        payload = canonical_json_bytes(record.model_dump(mode="json"))
        _install_no_replace(self.path, payload)
        return self.read()

    def read(self) -> SourceInstanceRecord:
        payload, _ = _read_nofollow(self.path)
        try:
            record = SourceInstanceRecord.model_validate(json.loads(payload))
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ReplicationDurabilityError("source instance record is invalid") from exc
        if canonical_json_bytes(record.model_dump(mode="json")) != payload:
            raise ReplicationDurabilityError("source instance record is not canonical")
        record.verify_hash()
        return record


class ObjectInventoryEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    relative_path: str = Field(min_length=1)
    sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    byte_count: int = Field(ge=0)
    row_count: int = Field(ge=0)

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

    def hash_preimage(self) -> dict[str, object]:
        value = self.model_dump(mode="json")
        value.pop("checkpoint_id")
        value.pop("checkpoint_payload_sha256")
        return value

    def verify_hashes(self) -> None:
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
    inventory = tuple(
        item
        if isinstance(item, ObjectInventoryEntry)
        else ObjectInventoryEntry.model_validate(item)
        for item in object_inventory
    )
    if tuple(sorted(item.relative_path for item in inventory)) != tuple(
        item.relative_path for item in inventory
    ) or len({item.relative_path for item in inventory}) != len(inventory):
        raise ReplicationDurabilityError("source checkpoint inventory is not sorted and unique")
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
        "object_inventory": tuple(item.model_dump(mode="json") for item in inventory),
    }
    checkpoint_id = domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1", values)
    values["checkpoint_id"] = checkpoint_id
    values["checkpoint_payload_sha256"] = domain_sha256(
        "stock-eva/r2f4.3/source-checkpoint-payload/v1", values
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
    checkpoint_projection: dict[str, object]
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
        self.verify_hash()
        path = Path(root) / f"{self.checkpoint_id}.json"
        _install_no_replace(path, canonical_json_bytes(self.model_dump(mode="json")))
        return path

    @classmethod
    def read(cls, path: Path) -> Self:
        payload, _ = _read_nofollow(Path(path))
        try:
            record = cls.model_validate(json.loads(payload))
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ReplicationDurabilityError("replication journal is invalid") from exc
        if Path(path).name != f"{record.checkpoint_id}.json":
            raise ReplicationDurabilityError("replication journal filename is invalid")
        if canonical_json_bytes(record.model_dump(mode="json")) != payload:
            raise ReplicationDurabilityError("replication journal is not canonical")
        record.verify_hash()
        return record


def build_journal_record(
    *,
    checkpoint_id: str,
    source_instance_id: str,
    source_instance_sha256: str,
    publication_binding_sha256: str,
    source_published_at: str,
    checkpoint_projection: Mapping[str, object],
    created_at: str | None = None,
) -> JournalRecord:
    projection = dict(checkpoint_projection)
    checkpoint_payload_sha256 = domain_sha256(
        "stock-eva/r2f4.3/source-checkpoint-payload/v1", projection
    )
    values: dict[str, object] = {
        "journal_schema_version": JOURNAL_SCHEMA_VERSION,
        "checkpoint_id": checkpoint_id,
        "source_instance_id": source_instance_id,
        "source_instance_sha256": source_instance_sha256,
        "source_published_at": source_published_at,
        "checkpoint_projection": projection,
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


class ReplicationSidecarStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    @staticmethod
    def _connect_writer(path: Path) -> sqlite3.Connection:
        parent = path.parent
        parent.mkdir(parents=True, exist_ok=True)
        _safe_parent(parent)
        try:
            fd = os.open(
                path,
                os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
            )
        except OSError as exc:
            raise ReplicationDurabilityError("replication sidecar is not writable") from exc
        os.close(fd)
        try:
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            return connection
        except sqlite3.Error as exc:
            raise ReplicationDurabilityError("replication sidecar is not writable") from exc

    def initialize(
        self,
        *,
        source_instance_id: str,
        source_instance_sha256: str,
        created_at: str | None = None,
    ) -> None:
        _validate_sha(source_instance_id, "source_instance_id")
        _validate_sha(source_instance_sha256, "source_instance_sha256")
        try:
            existing_stat = os.lstat(self.path)
        except FileNotFoundError:
            existing_stat = None
        if existing_stat is not None and stat.S_ISLNK(existing_stat.st_mode):
            raise ReplicationDurabilityError("replication sidecar path is a symlink")
        existing = existing_stat is not None and existing_stat.st_size > 0
        if existing:
            connection: sqlite3.Connection | None = None
            try:
                connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
                meta = connection.execute(
                    """SELECT source_instance_id, source_instance_sha256
                         FROM replication_sidecar_meta"""
                ).fetchall()
            except sqlite3.Error as exc:
                raise ReplicationDurabilityError("existing replication sidecar is invalid") from exc
            finally:
                if connection is not None:
                    connection.close()
            if meta != [(source_instance_id, source_instance_sha256)]:
                raise ReplicationDurabilityError("replication sidecar source identity conflicts")
            # Existing sidecars are never migrated by initialization.
            self.read_status()
            return
        connection = self._connect_writer(self.path)
        try:
            connection.executescript(SIDECAR_DDL)
            connection.execute(
                """INSERT OR IGNORE INTO replication_sidecar_meta
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
        except sqlite3.Error as exc:
            connection.rollback()
            raise ReplicationDurabilityError("replication sidecar initialization failed") from exc
        finally:
            connection.close()
        self._fsync_database()

    def _fsync_database(self) -> None:
        if not self.path.exists():
            raise ReplicationDurabilityError("replication sidecar disappeared")
        for candidate in (self.path, Path(f"{self.path}-wal")):
            if not candidate.exists():
                continue
            try:
                fd = os.open(candidate, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except OSError as exc:
                raise ReplicationDurabilityError("replication sidecar fsync failed") from exc
        _fsync_directory(self.path.parent)

    def read_status(
        self,
        *,
        local_ready: bool = False,
        source_instance: SourceInstanceRecord | None = None,
    ) -> ReplicationStatusResponse:
        try:
            payload, before = _read_nofollow(self.path)
        except ReplicationDurabilityError as exc:
            raise ReplicationStateUnavailable from exc
        del payload
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
            connection.execute("PRAGMA query_only = ON")
            objects = {
                (row[0], row[1])
                for row in connection.execute(
                    "SELECT type, name FROM sqlite_master WHERE type IN ('table','index','trigger')"
                )
            }
            expected_tables = {
                "replication_sidecar_meta",
                "replication_intents",
                "replication_destination_cache",
                "replication_attempt_events",
                "replication_heads",
            }
            table_names = {name for kind, name in objects if kind == "table"}
            expected_triggers = {
                "replication_sidecar_meta_no_update",
                "replication_sidecar_meta_no_delete",
                "replication_intents_no_update",
                "replication_intents_no_delete",
                "replication_attempt_events_no_update",
                "replication_attempt_events_no_delete",
                "replication_heads_no_delete",
                "replication_heads_monotonic_cas",
            }
            trigger_names = {name for kind, name in objects if kind == "trigger"}
            if table_names != expected_tables or trigger_names != expected_triggers:
                raise ReplicationStateUnavailable("replication sidecar schema is incomplete")
            meta = connection.execute(
                """SELECT schema_version, schema_identity, ddl_sha256, schema_digest,
                          source_instance_id, source_instance_sha256
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
                """SELECT health_state, health_observed_at
                     FROM replication_destination_cache
                    ORDER BY updated_at DESC LIMIT 1"""
            ).fetchone()
            after = os.stat(self.path, follow_symlinks=False)
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise ReplicationStateUnavailable("replication sidecar changed during read")
        except (sqlite3.Error, OSError) as exc:
            raise ReplicationStateUnavailable from exc
        finally:
            if connection is not None:
                connection.close()
        health, observed = row if row else ("unknown", None)
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
            source_instance = SourceInstanceStore(layout.replication_source_instance).read()
            status = sidecar.read_status(
                local_ready=self.local_ready,
                source_instance=source_instance,
            )
            return status.model_copy(
                update={
                    "destination_configured": self.settings.replication_destination_root is not None
                }
            )
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
    return result.model_copy(
        update={
            "status": "unavailable",
            "reason_code": reason_code,
            "enabled": True,
        }
    )


__all__ = [
    "Effects",
    "SIDECAR_DDL",
    "SIDECAR_DDL_SHA256",
    "SIDECAR_SCHEMA_DIGEST",
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
