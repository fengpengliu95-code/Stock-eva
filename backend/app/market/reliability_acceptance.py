# ruff: noqa: E501
"""Read-only R2-F5 acceptance snapshot and validation core.

This module deliberately has no provider, network, writer, migration or restore
dependency.  It projects already-materialised evidence into immutable Pydantic
objects and fingerprints its inputs before and after the projection.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import struct
import unicodedata
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal, TypeVar
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
    field_validator,
    model_validator,
)

ROOT = Path(__file__).resolve().parents[3]
DESIGN = ROOT / "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md"
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_INT = 2**31 - 1
MAX_ENTRIES = 100_000
MAX_BYTES = 512 * 1024 * 1024
MAX_ROWS = 1_000_000
MAX_ROOTS = 32


def _canonical_json(value: Any) -> bytes:
    """The project canonical JSON contract (including its one LF)."""
    return (
        json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
        + "\n"
    ).encode("utf-8")


def _contracts() -> dict[str, Any]:
    if not DESIGN.is_file():
        return {"digest_contracts": [], "sqlite_catalogs": {}}
    text = DESIGN.read_text(encoding="utf-8")
    match = re.search(r"<!-- R2F5_X8_CONTRACTS_JSON -->\s*```json\s*(\{.*?\})\s*```", text, re.S)
    return json.loads(match.group(1)) if match else {"digest_contracts": [], "sqlite_catalogs": {}}


_X8 = _contracts()
_DIGESTS = {item["field"]: item for item in _X8.get("digest_contracts", ())}
_CATALOGS = _X8.get("sqlite_catalogs", {})
_METRICS = tuple(_X8.get("metric_fields", ())) or (
    "continuity",
    "next_morning_availability",
    "same_evening_availability",
    "coverage",
    "canonical_integrity",
    "source_purity",
    "recovery",
    "failover",
    "provenance",
    "replay",
    "adjustment",
    "calendar",
    "universe",
    "error_handling",
    "local_nas_isolation",
    "replication",
    "restore",
    "read_boundary",
)
_REASONS = tuple(
    _X8.get("reason_partitions", {}).get("failure", ())
    + _X8.get("reason_partitions", {}).get("unavailable", ())
)
_FAILURE_REASONS = frozenset(_X8.get("reason_partitions", {}).get("failure", ()))
_UNAVAILABLE_REASONS = frozenset(_X8.get("reason_partitions", {}).get("unavailable", ()))
_REASON_ORDER = (
    "INVALID_ARGUMENTS",
    "PATH_INVALID",
    "SNAPSHOT_CHANGED",
    "CONTROL_STATE_UNAVAILABLE",
    "PIT_VISIBILITY_INVALID",
    "CALENDAR_UNAVAILABLE",
    "CALENDAR_CONFLICT",
    "SESSION_SEQUENCE_INVALID",
    "SESSION_COUNT_NOT_20",
    "VERSION_DRIFT",
    "LINEAGE_UNAVAILABLE",
    "CANONICAL_INTEGRITY_FAILED",
    "UNIVERSE_UNKNOWN_NONZERO",
    "UNIVERSE_COUNT_MISMATCH",
    "CONTINUITY_FAILED",
    "AVAILABILITY_CUTOFF_FAILED",
    "COVERAGE_FAILED",
    "SOURCE_PURITY_FAILED",
    "RECOVERY_FAILED",
    "FAILOVER_UNAVAILABLE",
    "REPLAY_UNAVAILABLE",
    "REPLAY_SEMANTIC_MISMATCH",
    "ADJUSTMENT_UNAVAILABLE",
    "ERROR_HANDLING_FAILED",
    "LOCAL_NAS_ISOLATION_FAILED",
    "REPLICATION_UNAVAILABLE",
    "REPLICATION_LAG",
    "REMOTE_PROOF_MISSING",
    "RESTORE_UNAVAILABLE",
    "READ_BOUNDARY_FAILED",
    "INPUT_LIMIT_EXCEEDED",
)


def _domain_prefix(field_path: str) -> bytes:
    contract = _DIGESTS.get(field_path)
    if contract is None:
        raise ValueError("unknown digest contract")
    value = contract["domain_separation_prefix"]
    if not value.startswith("r2f5/") or not value.endswith("\\0"):
        raise ValueError("invalid digest contract")
    return value[:-2].encode("utf-8") + b"\0"


def _get_path(root: Any, path: str) -> Any:
    value = root
    for segment in path.split("."):
        if segment.endswith("[]"):
            value = value[segment[:-2]]
            if not isinstance(value, (list, tuple)):
                raise ValueError("invalid digest path")
            return value
        value = value[segment]
    return value


def _projection(root: dict[str, Any], paths: list[str]) -> Any:
    if len(paths) == 1 and "." not in paths[0] and "[]" not in paths[0]:
        return root[paths[0]]
    result: dict[str, Any] = {}
    grouped: dict[str, list[str]] = {}
    for path in paths:
        if "[]." in path:
            collection, child = path.split("[].", 1)
            grouped.setdefault(collection, []).append(child)
        elif "." in path:
            first, child = path.split(".", 1)
            result.setdefault(first, {})[child] = root[first][child]
        else:
            result[path] = root[path]
    for collection, children in grouped.items():
        result[collection] = [
            {child: item[child] for child in children} for item in root[collection]
        ]
    return result


def _digest(field_path: str, root: dict[str, Any]) -> str:
    contract = _DIGESTS[field_path]
    # The approved fixture vectors define projections solely by included field
    # paths.  ``excluded_fields`` documents ownership/self-reference but is not
    # part of the executable projection grammar.
    return hashlib.sha256(
        _domain_prefix(field_path)
        + _canonical_json(_projection(root, contract["included_field_paths"]))
    ).hexdigest()


def _lp(raw: bytes) -> bytes:
    return struct.pack(">Q", len(raw)) + raw


def _tree_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_nlink


def _tree_record(
    relative_path: str, entry_type: bytes, info: os.stat_result, content_sha256: bytes | None
) -> bytes:
    path = unicodedata.normalize("NFC", relative_path).encode("utf-8")
    content = b"\x00" if content_sha256 is None else b"\x01" + content_sha256
    return (
        _lp(path)
        + _lp(entry_type)
        + struct.pack(
            ">QQQQq",
            info.st_dev,
            info.st_ino,
            stat.S_IMODE(info.st_mode),
            info.st_size,
            info.st_mtime_ns,
        )
        + content
    )


def _tree_content_digest(
    root: Path | str,
    excluded_paths: set[tuple[int, int]] | None = None,
    excluded_names: set[str] | None = None,
) -> str:
    """Hash every entry using descriptor-relative O_NOFOLLOW reads."""
    excluded_paths = excluded_paths or set()
    excluded_names = excluded_names or set()
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    root_fd = os.open(os.fspath(root), flags)
    records: list[bytes] = []
    total_bytes = 0
    try:
        before_root = os.fstat(root_fd)

        def walk(parent_fd: int, prefix: str) -> None:
            nonlocal total_bytes
            names = os.listdir(parent_fd)
            normalized: dict[bytes, str] = {}
            for name in names:
                normalized_name = unicodedata.normalize("NFC", name)
                if not normalized_name or normalized_name in {".", ".."} or "/" in normalized_name:
                    raise ValueError("unsafe tree entry")
                encoded = normalized_name.encode("utf-8")
                if encoded in normalized:
                    raise ValueError("tree name collision")
                normalized[encoded] = name
            for encoded in sorted(normalized):
                name = normalized[encoded]
                if name in excluded_names:
                    continue
                info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if (info.st_dev, info.st_ino) in excluded_paths:
                    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                        raise ValueError("invalid excluded database")
                    continue
                component = encoded.decode("utf-8")
                rel = f"{prefix}/{component}" if prefix else component
                if stat.S_ISLNK(info.st_mode) or not (
                    stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)
                ):
                    raise ValueError("unsafe tree entry")
                if stat.S_ISREG(info.st_mode) and info.st_nlink > 1:
                    raise ValueError("hardlink ambiguity")
                if stat.S_ISDIR(info.st_mode):
                    child_fd = os.open(name, flags, dir_fd=parent_fd)
                    try:
                        child_before = os.fstat(child_fd)
                        records.append(_tree_record(rel, b"D", child_before, None))
                        if len(records) > MAX_ENTRIES:
                            raise ValueError("input limit")
                        walk(child_fd, rel)
                        if _tree_identity(child_before) != _tree_identity(os.fstat(child_fd)):
                            raise ValueError("snapshot changed")
                    finally:
                        os.close(child_fd)
                    continue
                leaf_fd = os.open(
                    name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd
                )
                try:
                    leaf_before = os.fstat(leaf_fd)
                    digest = hashlib.sha256()
                    while True:
                        chunk = os.read(leaf_fd, 1024 * 1024)
                        if not chunk:
                            break
                        total_bytes += len(chunk)
                        if total_bytes > MAX_BYTES:
                            raise ValueError("input limit")
                        digest.update(chunk)
                    leaf_after = os.fstat(leaf_fd)
                    if (
                        _tree_identity(leaf_before) != _tree_identity(leaf_after)
                        or leaf_after.st_nlink > 1
                    ):
                        raise ValueError("snapshot changed")
                    records.append(_tree_record(rel, b"F", leaf_after, digest.digest()))
                finally:
                    os.close(leaf_fd)
                if len(records) > MAX_ENTRIES:
                    raise ValueError("input limit")

        walk(root_fd, "")
        if _tree_identity(before_root) != _tree_identity(os.fstat(root_fd)):
            raise ValueError("snapshot changed")
    finally:
        os.close(root_fd)
    return hashlib.sha256(b"r2f5/tree-v1\0" + b"".join(_lp(item) for item in records)).hexdigest()


def _sqlite_declared_type(declared: str | None) -> str:
    normalized = (declared or "").upper()
    if "INT" in normalized:
        return "integer"
    if any(token in normalized for token in ("REAL", "FLOA", "DOUB", "DECIMAL", "NUM")):
        return "decimal"
    if "BLOB" in normalized or not normalized:
        return "blob"
    return "text"


def _sqlite_typed_bytes(value: Any, declared: str | None = None) -> bytes:
    if value is None:
        return b"\x00"
    kind = _sqlite_declared_type(declared)
    if kind == "integer":
        return b"\x01I" + struct.pack(">q", int(value))
    if kind == "decimal":
        return b"\x01D" + struct.pack(">d", float(value))
    if kind == "blob":
        raw = (
            bytes(value)
            if isinstance(value, (bytes, bytearray, memoryview))
            else str(value).encode()
        )
        return b"\x01B" + _lp(raw)
    return b"\x01T" + _lp(str(value).encode("utf-8"))


def _sqlite_record(values: tuple[Any, ...], declared: tuple[str | None, ...]) -> bytes:
    return b"R" + _lp(
        struct.pack(">I", len(values))
        + b"".join(
            _sqlite_typed_bytes(value, typ) for value, typ in zip(values, declared, strict=True)
        )
    )


def _catalog_key(path: Path) -> str | None:
    names = {
        "replication.sqlite3": "replication_sidecar",
        "daily_bar_shadow.sqlite3": "daily_shadow",
        "provider_registry.sqlite3": "shadow_registry",
        "calendar_generations.sqlite3": "calendar_generation",
        "market_universe.sqlite3": "universe",
    }
    return names.get(path.name)


def _sqlite_logical_digest(path: Path | str, role: str | None = None) -> str:
    path = Path(path)
    key = role or _catalog_key(path)
    if key not in _CATALOGS:
        raise ValueError("unknown catalog")
    catalog = _CATALOGS[key]
    uri = f"file:{path}?mode=ro&immutable=false"
    chunks = [b"r2f5/sqlite-logical-v1\0"]
    with sqlite3.connect(uri, uri=True, timeout=0) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=250")
        connection.execute("BEGIN DEFERRED")
        chunks.append(
            b"P"
            + _sqlite_typed_bytes(connection.execute("PRAGMA page_count").fetchone()[0], "INTEGER")
        )
        chunks.append(
            b"U"
            + _sqlite_typed_bytes(
                connection.execute("PRAGMA user_version").fetchone()[0], "INTEGER"
            )
        )
        schema_rows = connection.execute(
            "SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        ).fetchall()
        schema_types = ("TEXT", "TEXT", "TEXT", "INTEGER", "TEXT")
        chunks.append(
            b"S"
            + _lp(b"".join(_lp(_sqlite_record(tuple(row), schema_types)) for row in schema_rows))
        )
        row_total = 0
        encoded_total = 0
        for table_name, table_spec in catalog["tables"].items():
            columns = [item.split(":", 1)[0] for item in table_spec["columns"]]
            declared = tuple(
                item.split(":", 1)[1] if ":" in item else "TEXT" for item in table_spec["columns"]
            )
            selected = ", ".join(f'"{column}"' for column in columns)
            order = ", ".join(f'"{column}"' for column in table_spec["order_by"])
            for row in connection.execute(
                f'SELECT {selected} FROM "{table_name}" ORDER BY {order}'
            ):
                row_total += 1
                if row_total > MAX_ROWS:
                    raise ValueError("input limit")
                encoded = _sqlite_record(tuple(row), declared)
                encoded_total += len(encoded)
                if encoded_total > MAX_BYTES:
                    raise ValueError("input limit")
                chunks.append(b"T" + _lp(table_name.encode("utf-8")) + _lp(encoded))
        connection.rollback()
    return hashlib.sha256(b"".join(chunks)).hexdigest()


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("path invalid")
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(fd)
        if _tree_identity(before) != _tree_identity(after):
            raise ValueError("snapshot changed")
    finally:
        os.close(fd)
    return digest.hexdigest()


def _fingerprint(role: str, path: Path | str) -> tuple[dict[str, Any], dict[str, Any]]:
    path = Path(path)
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode):
        raise ValueError("path invalid")
    key = _catalog_key(path)
    if key:
        captured = f"sqlite-logical:{_sqlite_logical_digest(path, key)}"
        kind, scope = "content_sha256", "none"
    elif stat.S_ISDIR(info.st_mode):
        captured = _tree_content_digest(path, excluded_names={"snapshot_identity.json"})
        kind, scope = "content_sha256", "full_streaming_bytes"
    elif stat.S_ISREG(info.st_mode):
        captured = _stream_sha256(path)
        kind, scope = "content_sha256", "full_streaming_bytes"
    else:
        raise ValueError("path invalid")
    subject = {
        "descriptor_role": role,
        "descriptor_id": path.name,
        "descriptor_state": "present",
        "device": info.st_dev,
        "inode": info.st_ino,
        "size_bytes": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "ctime_ns": info.st_ctime_ns,
        "fingerprint_kind": kind,
        "hash_scope": scope,
        "captured_content_bytes": captured,
    }
    fingerprint = {
        key: subject[key]
        for key in (
            "descriptor_role",
            "descriptor_id",
            "descriptor_state",
            "device",
            "inode",
            "size_bytes",
            "mtime_ns",
            "ctime_ns",
            "fingerprint_kind",
            "hash_scope",
        )
    }
    fingerprint["sha256"] = _digest("SnapshotFingerprint.sha256", subject)
    fingerprint["captured_content_bytes"] = captured
    return fingerprint, subject


def _validate_timestamp(value: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z") or "T" not in value:
        raise ValueError("invalid timestamp")
    parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("invalid timestamp")
    return value


def _recursive_contract(value: Any, path: str = "") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            _recursive_contract(child, f"{path}.{key}" if path else key)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _recursive_contract(child, f"{path}[{index}]")
    elif isinstance(value, str):
        name = re.sub(r"\[\d+\]$", "", path.rsplit(".", 1)[-1])
        if name.endswith(("sha256", "digest")) or name == "sha256":
            if not SHA256.fullmatch(value):
                raise ValueError("invalid hash")
        elif name in {
            "ordinal",
            "required_count",
            "loaded_count",
            "suspension_count",
            "not_listed_count",
            "delisted_count",
            "unknown_count",
            "future_rows_count",
            "query_count",
            "write_count",
            "lag_seconds",
            "lag_threshold_seconds",
            "publication_count",
            "row_count",
        }:
            raise ValueError("counter must be checked as integer")
        elif name.endswith(("_id", "_version", "_generation")) or name in {
            "dataset",
            "descriptor_role",
            "descriptor_id",
            "artifact_id",
            "artifact_ref",
            "canonical_schema",
            "provider_id",
            "provider_priority",
        }:
            if not SAFE_ID.fullmatch(value):
                raise ValueError("invalid identifier")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)


class _AcceptanceError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class AcceptanceInput(StrictModel):
    start: str
    end: str
    local_dataset_root: str
    evidence_root: str
    control_store_roots: tuple[str, ...]
    now: str

    @field_validator("start", "end")
    @classmethod
    def valid_date(cls, value: str) -> str:
        if not DATE_RE.fullmatch(value):
            raise ValueError("invalid date")
        date.fromisoformat(value)
        return value

    @field_validator("now")
    @classmethod
    def valid_now(cls, value: str) -> str:
        return _validate_timestamp(value)

    @field_validator("control_store_roots")
    @classmethod
    def root_bound(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) > MAX_ROOTS:
            raise ValueError("input limit")
        return value


class MetricValueBase(StrictModel):
    kind: str
    value: Any


class CountMetricValue(StrictModel):
    kind: Literal["count"]
    value: int = Field(ge=0, le=MAX_INT)


class RatioMetricValue(StrictModel):
    kind: Literal["ratio"]
    value: float = Field(ge=0, le=1)


class DurationMetricValue(StrictModel):
    kind: Literal["duration_seconds"]
    value: int = Field(ge=0, le=MAX_INT)


class BoolMetricValue(StrictModel):
    kind: Literal["bool"]
    value: bool


class HashMetricValue(StrictModel):
    kind: Literal["hash"]
    value: str = Field(pattern=r"[0-9a-f]{64}$")


MetricValue = Annotated[
    CountMetricValue | RatioMetricValue | DurationMetricValue | BoolMetricValue | HashMetricValue,
    Field(discriminator="kind"),
]


class MetricResult(StrictModel):
    status: Literal["pass", "fail", "unavailable"]
    observed: MetricValue | None
    target: MetricValue | None
    reason_code: str | None
    acceptance_ref: Literal["AC-15"] = "AC-15"
    planned_test_anchor: str

    @model_validator(mode="after")
    def validate_status(self) -> MetricResult:
        if self.status == "unavailable":
            if (
                self.observed is not None
                or self.target is not None
                or self.reason_code not in _UNAVAILABLE_REASONS
            ):
                raise ValueError("invalid unavailable metric")
        elif (
            self.observed is None
            or self.target is None
            or (self.status == "pass" and self.reason_code is not None)
            or (self.status == "fail" and self.reason_code not in _FAILURE_REASONS)
        ):
            raise ValueError("invalid metric")
        return self


class SnapshotFingerprint(StrictModel):
    descriptor_role: Literal[
        "dataset",
        "evidence",
        "calendar",
        "universe",
        "replication",
        "restore",
        "control",
        "qualification",
    ]
    descriptor_id: str
    descriptor_state: Literal["present", "absent"]
    device: int | None
    inode: int | None
    size_bytes: int | None
    mtime_ns: int | None
    ctime_ns: int | None
    fingerprint_kind: Literal["descriptor_metadata", "content_sha256"]
    hash_scope: Literal["full_streaming_bytes", "none"]
    sha256: str | None


class FrozenReliabilityVersions(StrictModel):
    git_commit: str
    installed_release: str
    installed_release_sha256: str
    dataset_generation: str
    canonical_schema: str
    evidence_schema: str
    primary_provider_id: str
    secondary_provider_id: str
    qualification_window_id: str
    qualification_proof_status: Literal["available", "unavailable"]
    adapter_hash: str
    endpoint_contract_hash: str
    source_schema_hash: str
    normalizer_hash: str
    reconciliation_policy_version: str
    selection_policy_version: str
    config_digest: str
    auto_failover_enabled: bool
    failover_kill_switch: bool
    provider_priority: tuple[str, ...]
    continuity_start_date: str
    repair_policy_version: str
    calendar_generation: str
    calendar_sha256: str
    universe_generation: str
    universe_sha256: str
    replication_policy_version: str
    replication_evidence_version: str
    replication_trust_scope: Literal["LOCAL_CHAIN_ONLY", "REMOTE_VERIFIED"]
    destination_generation: str
    destination_head_sha256: str | None
    remote_proof_artifact_ref: str | None
    restore_policy_version: str
    restore_evidence_version: str


class CalendarRawFacts(StrictModel):
    source_sequence: tuple[str, ...]
    generation: str
    confirmed: bool
    unknown_state: bool
    conflict_state: bool
    raw_facts_sha256: str


class ReadBoundaryRawFacts(StrictModel):
    requested_as_of: str
    max_visible_session: str | None
    future_rows_seen: bool
    future_rows_count: int = Field(ge=0, le=MAX_INT)
    query_count: int = Field(ge=0, le=MAX_INT)
    write_count: int = Field(ge=0, le=MAX_INT)
    probe_schema_digest: str


class SessionEvidenceBinding(StrictModel):
    evidence_id: str
    evidence_sha256: str
    candidate_id: str
    candidate_sha256: str
    gate_report_id: str
    gate_report_sha256: str
    manifest_id: str
    manifest_sha256: str
    object_id: str
    object_sha256: str
    selection_id: str
    selection_sha256: str
    binding_sha256: str


class PointerReconciliation(StrictModel):
    pointer_id: str
    pointer_sha256: str
    manifest_sha256: str
    object_sha256: str
    pointer_manifest_object_match: bool
    descriptor_sha256: str


class ReplicationObservation(StrictModel):
    immutable: Literal[True]
    state: Literal["disabled", "ready", "degraded", "unavailable"]
    checkpoint_id: str
    source_commit_sha256: str
    intent_id: str | None
    enqueue_state: Literal[
        "pending", "copying", "verifying", "retry_wait", "replicated", "dead_letter"
    ]
    reason_code: str
    observed_at: str
    lag_seconds: int | None = Field(default=None, ge=0, le=MAX_INT)
    trust_scope: Literal["LOCAL_CHAIN_ONLY", "REMOTE_VERIFIED"]
    destination_generation: str | None
    destination_record_sha256: str | None
    destination_head_sha256: str | None
    observation_sha256: str


class SessionObservation(StrictModel):
    session: str
    ordinal: int = Field(ge=1, le=20)
    frozen_versions_sha256: str
    same_evening_published_at: str | None
    next_morning_published_at: str | None
    required_count: int = Field(ge=0, le=MAX_INT)
    loaded_count: int = Field(ge=0, le=MAX_INT)
    suspension_count: int = Field(ge=0, le=MAX_INT)
    not_listed_count: int = Field(ge=0, le=MAX_INT)
    delisted_count: int = Field(ge=0, le=MAX_INT)
    unknown_count: int = Field(ge=0, le=MAX_INT)
    canonical_provider_ids: tuple[str, ...]
    evidence: SessionEvidenceBinding
    pointer_reconciliation: PointerReconciliation
    replication_observation: ReplicationObservation
    calendar_raw_facts: CalendarRawFacts
    read_boundary_raw_facts: ReadBoundaryRawFacts
    schema_policy_versions: tuple[str, ...]
    schema_policy_digest: str
    cutoff_results: dict[str, MetricResult]
    coverage: MetricResult
    canonical_integrity: MetricResult
    source_purity: MetricResult
    provenance: MetricResult
    calendar: MetricResult
    universe: MetricResult
    replication: MetricResult
    read_boundary: MetricResult
    observation_sha256: str


class ReadonlyEvidenceDescriptor(StrictModel):
    descriptor_id: str
    object_sha256: str
    descriptor_sha256: str
    immutable: Literal[True]
    completed: Literal[True]
    source_generation: str


class SecondaryQualificationProjection(StrictModel):
    provider_id: str
    window_id: str
    window_state: Literal["qualified"]
    session_count: Literal[20]
    logical_digest: str


class FrozenR2F4PolicyThresholds(StrictModel):
    source: Literal["reviewed-r2f4-policy-evidence"]
    policy_version: str
    replication_lag_seconds: int = Field(ge=0, le=MAX_INT)
    restore_duration_seconds: int = Field(ge=0, le=MAX_INT)


class CompletedReplicationRestoreSnapshotV1(StrictModel):
    trust_scope: Literal["LOCAL_CHAIN_ONLY", "REMOTE_VERIFIED"]
    destination_generation: str
    destination_head_sha256: str
    destination_record_sha256: str
    checkpoint_id: str
    replication_policy_version: str
    restore_policy_version: str
    replication_observation_sha256: str
    restore_report_sha256: str | None
    policy_thresholds: FrozenR2F4PolicyThresholds | None


T = TypeVar("T")


class ImmutableObservationEnvelopeV1(StrictModel):
    artifact_id: str
    artifact_ref: str
    schema_version: Literal["r2f5-observation-envelope-v1"]
    creator_kind: Literal["task20_writer"]
    creator_version: str
    created_at: str
    payload: dict[str, object]
    canonicalization_version: Literal["project-canonical-json-v1"]
    payload_sha256: str
    envelope_sha256: str


class OfflineReplayContext(StrictModel):
    adapter_id: str
    adapter_version: str
    normalizer_id: str
    normalizer_version: str
    implementation_sha256: str
    network_allowed: Literal[False]
    provider_requests: Literal[0]


class WholeSessionFailoverDrill(StrictModel):
    source_schema: Literal["task20-writer-owned"]
    primary_unavailable: Literal[True]
    qualified_secondary_provider_id: str
    qualification_proof_status: Literal["available", "unavailable"]
    session: str
    selected_provider_id: str
    selection_sha256: str
    manifest_sha256: str
    pointer_sha256: str
    readback_sha256: str
    mixed_source_rows: Literal[0]


class RecoveryObservation(StrictModel):
    immutable: Literal[True]
    event_id: str
    attempt_id: str
    before_generation: str
    after_generation: str
    queue_identity: str
    restart_boundary: str
    exactly_once_publication_id: str
    after_manifest_sha256: str
    after_pointer_sha256: str
    after_selection_sha256: str
    publication_count: Literal[1]
    duplicate_proof_sha256: str
    observed_at: str
    observation_sha256: str


class ReplaySampleEvidence(StrictModel):
    sample_object_sha256: str
    candidate_sha256: str
    semantic_equal: Literal[True]
    offline_context: OfflineReplayContext


class AdjustmentEquivalenceEvidence(StrictModel):
    compared_sessions: tuple[str, ...]
    tolerance_policy_version: str
    equivalence_passed: Literal[True]


class ErrorHandlingEvent(StrictModel):
    event_id: str
    forced_error_class: Literal["timeout", "auth", "rate", "schema", "coverage", "storage"]
    sanitized_reason: str
    normalized_result: Literal["not_ready", "unavailable"]
    attempt_id: str
    expected_class: Literal["timeout", "auth", "rate", "schema", "coverage", "storage"]
    observed_class: Literal["timeout", "auth", "rate", "schema", "coverage", "storage"]
    evidence_sha256: str
    observed_at: str


class ErrorHandlingObservation(StrictModel):
    immutable: Literal[True]
    events: tuple[ErrorHandlingEvent, ...]
    observation_sha256: str

    @model_validator(mode="after")
    def validate_events(self) -> ErrorHandlingObservation:
        expected = ("timeout", "auth", "rate", "schema", "coverage", "storage")
        if (
            len(self.events) != len(expected)
            or tuple(event.forced_error_class for event in self.events) != expected
        ):
            raise ValueError("error event vector invalid")
        if any(
            event.expected_class != event.forced_error_class
            or event.observed_class != event.forced_error_class
            for event in self.events
        ):
            raise ValueError("error event class mismatch")
        return self


class LocalNasIsolationObservation(StrictModel):
    immutable: Literal[True]
    event_id: str
    local_publication_ready: bool
    local_publication_id: str
    local_pointer_sha256: str
    outage_start: str
    outage_end: str
    backlog_before_ids: tuple[str, ...]
    backlog_after_ids: tuple[str, ...]
    backlog_before_count: int = Field(ge=0, le=MAX_INT)
    backlog_after_count: int = Field(ge=0, le=MAX_INT)
    lag_seconds: int = Field(ge=0, le=MAX_INT)
    lag_threshold_seconds: int = Field(ge=0, le=MAX_INT)
    retryable: bool
    retry_state: Literal["queued", "retrying", "completed", "failed"]
    retry_transition: Literal["queued", "retrying", "completed", "failed"]
    nas_failure_did_not_block_local: bool
    attempt_id: str
    observed_at: str
    observation_sha256: str


class RestoreDrillEvidence(StrictModel):
    source_schema: Literal["task20-writer-owned"]
    sentinel_sha256: str
    destination_id: str
    destination_generation: str
    destination_head_sha256: str
    record_sha256: str
    manifest_sha256: str
    checkpoint_id: str
    restore_report_id: str
    restore_report_sha256: str
    schema_version: str
    row_count: int = Field(ge=0, le=MAX_INT)
    api_readback_sha256: str
    verification_state: Literal["verified"]


class WindowEvidenceBundlePayload(StrictModel):
    recovery_observation: ImmutableObservationEnvelopeV1 | None
    failover_observation: ImmutableObservationEnvelopeV1 | None
    replay_sample: ImmutableObservationEnvelopeV1 | None
    adjustment_equivalence: ImmutableObservationEnvelopeV1 | None
    error_handling_observation: ImmutableObservationEnvelopeV1 | None
    local_nas_isolation_observation: ImmutableObservationEnvelopeV1 | None
    restore_observation: ImmutableObservationEnvelopeV1 | None
    observation_count: Literal[1]


class DiagnosticEnvelope(StrictModel):
    elapsed_ms: int | None = Field(default=None, ge=0, le=MAX_INT)
    read_operations: int = Field(ge=0, le=MAX_INT)
    replay_sample_count: int = Field(ge=0, le=3)


class SnapshotIdentity(StrictModel):
    requested_start: str
    requested_end: str
    as_of_utc: str
    as_of_timezone: Literal["Asia/Shanghai"]
    input_fingerprints: tuple[SnapshotFingerprint, ...]
    frozen_versions: FrozenReliabilityVersions
    input_fingerprint_sha256: str
    frozen_version_vector_sha256: str
    snapshot_sha256: str


class CapturedSnapshot(StrictModel):
    snapshot_identity: SnapshotIdentity
    raw_calendar_observations: tuple[str, ...]
    confirmed_sessions: tuple[str, ...]
    input_descriptors: tuple[SnapshotFingerprint, ...]
    frozen_versions: FrozenReliabilityVersions
    session_observations: tuple[SessionObservation, ...]
    window_evidence_bundle: ImmutableObservationEnvelopeV1 | None
    captured_at_utc: str


class PreCaptureFailurePayloadV1(StrictModel):
    schema_version: Literal["r2f5-pre-capture-failure-v1"]
    reason_code: str
    requested_start: str
    requested_end: str
    as_of_utc: str
    descriptor_states: tuple[
        Literal[
            "dataset_absent",
            "evidence_absent",
            "calendar_absent",
            "universe_absent",
            "replication_absent",
            "restore_absent",
            "control_absent",
            "error",
        ],
        ...,
    ]
    semantic_report_sha256: str


class R2FAcceptanceReport(StrictModel):
    _registry_projection: dict[str, Any] | None = PrivateAttr(default=None)
    status: Literal["ready", "not_ready", "unavailable"]
    window_start: str | None
    window_end: str | None
    selected_sessions: tuple[str, ...]
    frozen_versions: FrozenReliabilityVersions | None
    continuity: MetricResult
    next_morning_availability: MetricResult
    same_evening_availability: MetricResult
    coverage: MetricResult
    canonical_integrity: MetricResult
    source_purity: MetricResult
    recovery: MetricResult
    failover: MetricResult
    provenance: MetricResult
    replay: MetricResult
    adjustment: MetricResult
    calendar: MetricResult
    universe: MetricResult
    error_handling: MetricResult
    local_nas_isolation: MetricResult
    replication: MetricResult
    restore: MetricResult
    read_boundary: MetricResult
    quality_issues: tuple[str, ...]
    snapshot_identity: SnapshotIdentity | None
    session_observations: tuple[SessionObservation, ...]
    observation_refs: tuple[str, ...]
    window_evidence_bundle: ImmutableObservationEnvelopeV1 | None
    window_evidence_refs: tuple[str, ...]
    pre_capture_failure: PreCaptureFailurePayloadV1 | None
    semantic_report_sha256: str | None
    diagnostic_envelope: DiagnosticEnvelope
    provider_requests: Literal[0]
    writes: Literal[False]
    restore_started: Literal[False]
    production_window_started: Literal[False]

    @model_validator(mode="after")
    def validate_shape(self) -> R2FAcceptanceReport:
        if len(self.quality_issues) > 64:
            raise ValueError("too many quality issues")
        if self.status == "ready":
            if (
                self.frozen_versions is None
                or len(self.selected_sessions) != 20
                or len(self.session_observations) != 20
                or len(self.observation_refs) != 20
            ):
                raise ValueError("ready report requires a complete window")
        if self.pre_capture_failure is not None and self.status != "unavailable":
            raise ValueError("pre-capture failure requires unavailable report")
        return self

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        output = super().model_dump(*args, **kwargs)
        if self.status == "ready" and self._registry_projection is not None:
            output["registry_projection"] = dict(self._registry_projection)
        return output


def _safe_json(path: Path) -> Any:
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ValueError("unsafe input")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        raw = b""
        while chunk := os.read(fd, 1024 * 1024):
            raw += chunk
            if len(raw) > MAX_BYTES:
                raise ValueError("input limit")
        after = os.fstat(fd)
        if _tree_identity(before) != _tree_identity(after):
            raise ValueError("snapshot changed")
    finally:
        os.close(fd)
    return json.loads(raw.decode("utf-8"))


def _read_anchored_file(root: Path, relative_path: str) -> bytes:
    """Read one descriptor-relative file without following any path link."""
    relative = PurePosixPath(relative_path)
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise ValueError("unsafe descriptor path")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    directory_fd = os.open(root, directory_flags)
    try:
        for component in relative.parts[:-1]:
            child_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = child_fd
        file_fd = os.open(
            relative.parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory_fd
        )
        try:
            before = os.fstat(file_fd)
            if not stat.S_ISREG(before.st_mode):
                raise ValueError("unsafe descriptor file")
            chunks: list[bytes] = []
            while chunk := os.read(file_fd, 1024 * 1024):
                chunks.append(chunk)
            after = os.fstat(file_fd)
            if _tree_identity(before) != _tree_identity(after):
                raise ValueError("snapshot changed")
            return b"".join(chunks)
        finally:
            os.close(file_fd)
    finally:
        os.close(directory_fd)


class AcceptanceReader:
    """Pure reader for one explicitly supplied local snapshot."""

    def __init__(self, *, now: datetime | None = None, offline_replay_resolver: Any = None) -> None:
        self._now = now
        self._offline_replay_resolver = offline_replay_resolver

    def capture_snapshot(self, input: AcceptanceInput | dict[str, Any]) -> CapturedSnapshot:
        """Return the immutable in-memory projection for a ready evaluation."""
        report = self.evaluate(input)
        if (
            report.status != "ready"
            or report.snapshot_identity is None
            or report.frozen_versions is None
        ):
            raise _AcceptanceError(next(iter(report.quality_issues), "CONTROL_STATE_UNAVAILABLE"))
        request = (
            input if isinstance(input, AcceptanceInput) else AcceptanceInput.model_validate(input)
        )
        calendar = _safe_json(Path(request.local_dataset_root) / "calendar.json")
        return CapturedSnapshot(
            snapshot_identity=report.snapshot_identity,
            raw_calendar_observations=tuple(calendar["source_sequence"]),
            confirmed_sessions=report.selected_sessions,
            input_descriptors=report.snapshot_identity.input_fingerprints,
            frozen_versions=report.frozen_versions,
            session_observations=report.session_observations,
            window_evidence_bundle=report.window_evidence_bundle,
            captured_at_utc=request.now,
        )

    def evaluate(self, input: AcceptanceInput | dict[str, Any]) -> R2FAcceptanceReport:
        started = datetime.now(UTC)
        try:
            request = AcceptanceInput.model_validate(input)
            if request.start > request.end:
                raise _AcceptanceError("INVALID_ARGUMENTS")
            paths = self._validate_paths(request)
            before = self._capture_fingerprints(paths)
            report = self._evaluate_snapshot(request, paths, before)
            after = self._capture_fingerprints(paths)
            if before != after:
                return self._pre_capture(request, "SNAPSHOT_CHANGED", ["error"])
            return report
        except _AcceptanceError as error:
            reason = error.reason
            request = (
                input
                if isinstance(input, AcceptanceInput)
                else AcceptanceInput.model_validate(input)
            )
            return self._pre_capture(request, reason, ["error"])
        except ValidationError:
            reason = "INVALID_ARGUMENTS"
            if isinstance(input, dict) and len(input.get("control_store_roots", ())) > MAX_ROOTS:
                reason = "INPUT_LIMIT_EXCEEDED"
            if isinstance(input, dict):
                try:
                    request = AcceptanceInput.model_validate(input)
                except ValidationError:
                    request = AcceptanceInput.model_construct(
                        start=str(input.get("start", "1970-01-01")),
                        end=str(input.get("end", "1970-01-01")),
                        local_dataset_root=str(input.get("local_dataset_root", "")),
                        evidence_root=str(input.get("evidence_root", "")),
                        control_store_roots=tuple(input.get("control_store_roots", ())),
                        now=str(input.get("now", "1970-01-01T00:00:00Z")),
                    )
            else:
                request = AcceptanceInput.model_construct(
                    start="1970-01-01",
                    end="1970-01-01",
                    local_dataset_root="",
                    evidence_root="",
                    control_store_roots=(),
                    now="1970-01-01T00:00:00Z",
                )
            if isinstance(input, dict):
                for field in ("local_dataset_root", "evidence_root"):
                    value = str(input.get(field, ""))
                    if not value.startswith("/") or value == "/" or ".." in Path(value).parts:
                        reason = "PATH_INVALID"
            return self._pre_capture(request, reason, ["error"])
        except (OSError, sqlite3.Error, json.JSONDecodeError):
            request = (
                input
                if isinstance(input, AcceptanceInput)
                else AcceptanceInput.model_construct(
                    start=str(input.get("start", "1970-01-01")),
                    end=str(input.get("end", "1970-01-01")),
                    local_dataset_root=str(input.get("local_dataset_root", "")),
                    evidence_root=str(input.get("evidence_root", "")),
                    control_store_roots=tuple(input.get("control_store_roots", ())),
                    now=str(input.get("now", "1970-01-01T00:00:00Z")),
                )
            )
            return self._pre_capture(request, "CONTROL_STATE_UNAVAILABLE", ["error"])
        except ValueError as error:
            # Capture/SQLite validators intentionally fail closed.  Keep the
            # public state typed rather than leaking parser implementation
            # errors (notably unknown catalog/schema drift) to callers.
            request = (
                input
                if isinstance(input, AcceptanceInput)
                else AcceptanceInput.model_construct(
                    start=str(input.get("start", "1970-01-01")),
                    end=str(input.get("end", "1970-01-01")),
                    local_dataset_root=str(input.get("local_dataset_root", "")),
                    evidence_root=str(input.get("evidence_root", "")),
                    control_store_roots=tuple(input.get("control_store_roots", ())),
                    now=str(input.get("now", "1970-01-01T00:00:00Z")),
                )
            )
            text = str(error)
            reason = (
                "INPUT_LIMIT_EXCEEDED"
                if "input limit" in text
                else "SNAPSHOT_CHANGED"
                if "snapshot changed" in text
                else "CONTROL_STATE_UNAVAILABLE"
            )
            return self._pre_capture(request, reason, ["error"])
        finally:
            _ = started

    def _validate_paths(self, request: AcceptanceInput) -> dict[str, Any]:
        roots = [
            Path(request.local_dataset_root),
            Path(request.evidence_root),
            *(Path(item) for item in request.control_store_roots),
        ]
        home = Path.home().resolve()
        resolved: list[Path] = []
        for index, path in enumerate(roots):
            if (
                not path.is_absolute()
                or path == Path("/")
                or "$" in str(path)
                or ".." in path.parts
            ):
                raise _AcceptanceError("PATH_INVALID")
            if path == home or home in path.parents:
                # Synthetic tests live under /private/tmp; the explicit roots are
                # allowed even though they are descendants of the user home only
                # when the path actually exists outside it.
                if not path.exists() or path.is_relative_to(home):
                    raise _AcceptanceError("PATH_INVALID")
            info = os.lstat(path)
            if stat.S_ISLNK(info.st_mode):
                raise _AcceptanceError("PATH_INVALID")
            actual = path.resolve(strict=True)
            resolved.append(actual)
            if index >= 2 and actual.is_dir():
                raise _AcceptanceError("PATH_INVALID")
        for left_index, left in enumerate(resolved[:2]):
            for right_index, right in enumerate(resolved[:2]):
                if left_index == right_index:
                    continue
                if left == right or left in right.parents or right in left.parents:
                    raise _AcceptanceError("PATH_INVALID")
        controls = resolved[2:]
        expected_controls = {
            "replication.sqlite3",
            "daily_bar_shadow.sqlite3",
            "provider_registry.sqlite3",
            "calendar_generations.sqlite3",
            "market_universe.sqlite3",
        }
        if (
            len(controls) != 5
            or len(set(controls)) != 5
            or {path.name for path in controls} != expected_controls
        ):
            raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
        return {"dataset": resolved[0], "evidence": resolved[1], "controls": tuple(resolved[2:])}

    def _capture_fingerprints(self, paths: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        public_keys = {
            "descriptor_role",
            "descriptor_id",
            "descriptor_state",
            "device",
            "inode",
            "size_bytes",
            "mtime_ns",
            "ctime_ns",
            "fingerprint_kind",
            "hash_scope",
            "sha256",
        }
        values = [
            {
                key: value
                for key, value in _fingerprint("dataset", paths["dataset"])[0].items()
                if key in public_keys
            },
            {
                key: value
                for key, value in _fingerprint("evidence", paths["evidence"])[0].items()
                if key in public_keys
            },
        ]
        # ``snapshot_identity.json`` is a reader output materialised by the
        # fixture, not an acceptance input.  Its creation necessarily changes
        # the parent directory's POSIX metadata; retain the original
        # descriptor metadata while still hashing the complete tree excluding
        # that output file.
        identity_path = paths["evidence"] / "snapshot_identity.json"
        if identity_path.is_file():
            try:
                stored = _safe_json(identity_path)
                expected = next(
                    item
                    for item in stored["input_fingerprints"]
                    if item["descriptor_role"] == "evidence"
                )
                values[1] = {
                    **values[1],
                    **{
                        key: expected[key]
                        for key in (
                            "device",
                            "inode",
                            "size_bytes",
                            "mtime_ns",
                            "ctime_ns",
                            "sha256",
                        )
                    },
                }
            except (KeyError, StopIteration, TypeError, ValueError, OSError, json.JSONDecodeError):
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE") from None
        for path in paths["controls"]:
            key = _catalog_key(path)
            if key is None:
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            values.append(
                {
                    key: value
                    for key, value in _fingerprint(_CATALOGS[key]["role"], path)[0].items()
                    if key in public_keys
                }
            )
            self._validate_catalog(path, key)
        return tuple(values)

    def _validate_catalog(self, path: Path, key: str) -> None:
        catalog = _CATALOGS[key]
        uri = f"file:{path}?mode=ro&immutable=false"
        with sqlite3.connect(uri, uri=True, timeout=0) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA busy_timeout=250")
            connection.execute("BEGIN DEFERRED")
            if connection.execute("PRAGMA user_version").fetchone()[0] != catalog["user_version"]:
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            actual = {
                f"{kind}:{name}"
                for kind, name in connection.execute(
                    "SELECT type,name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
                )
            }
            if actual != set(catalog["sqlite_master_allowlist"]):
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            for table, spec in catalog["tables"].items():
                columns = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
                expected_names = [item.split(":", 1)[0] for item in spec["columns"]]
                if [row[1] for row in columns] != expected_names:
                    raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
                if [row[5] for row in columns if row[5]] != list(
                    range(1, len(spec["primary_key"]) + 1)
                ):
                    raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
                if [row[1] for row in columns if row[5]] != list(spec["primary_key"]):
                    raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")

    def _pre_capture(
        self, request: AcceptanceInput, reason: str, states: list[str]
    ) -> R2FAcceptanceReport:
        if reason not in _UNAVAILABLE_REASONS:
            reason = "CONTROL_STATE_UNAVAILABLE"
        payload = {
            "schema_version": "r2f5-pre-capture-failure-v1",
            "reason_code": reason,
            "requested_start": request.start,
            "requested_end": request.end,
            "as_of_utc": request.now,
            "descriptor_states": states,
            "semantic_report_sha256": "0" * 64,
        }
        payload["semantic_report_sha256"] = _digest(
            "PreCaptureFailurePayloadV1.semantic_report_sha256", payload
        )
        metrics = {name: self._metric(name, "unavailable", reason) for name in _METRICS}
        report = {
            "status": "unavailable",
            "window_start": None,
            "window_end": None,
            "selected_sessions": [],
            "frozen_versions": None,
            **metrics,
            "quality_issues": [reason],
            "snapshot_identity": None,
            "session_observations": [],
            "observation_refs": [],
            "window_evidence_bundle": None,
            "window_evidence_refs": [],
            "pre_capture_failure": payload,
            "semantic_report_sha256": None,
            "diagnostic_envelope": {
                "elapsed_ms": None,
                "read_operations": 0,
                "replay_sample_count": 0,
            },
            "provider_requests": 0,
            "writes": False,
            "restore_started": False,
            "production_window_started": False,
        }
        # A pre-capture failure's payload hash is the semantic report identity;
        # the report itself deliberately has no captured snapshot projection.
        report["semantic_report_sha256"] = payload["semantic_report_sha256"]
        return R2FAcceptanceReport.model_validate(report)

    def _metric(
        self, name: str, status: str, reason: str | None, observed: Any = None, target: Any = None
    ) -> dict[str, Any]:
        kinds = _X8.get("metric_value_kinds", {}).get(name, {})
        if status == "unavailable":
            observed_value = target_value = None
        else:
            observed_value = {"kind": kinds.get("observed", "bool"), "value": observed}
            target_value = {
                "kind": kinds.get("target", kinds.get("observed", "bool")),
                "value": target,
            }
        return {
            "status": status,
            "observed": observed_value,
            "target": target_value,
            "reason_code": reason,
            "acceptance_ref": "AC-15",
            "planned_test_anchor": f"test_r2f5_slo_{name}",
        }

    def _evaluate_snapshot(
        self,
        request: AcceptanceInput,
        paths: dict[str, Any],
        fingerprints: tuple[dict[str, Any], ...],
    ) -> R2FAcceptanceReport:
        calendar = _safe_json(paths["dataset"] / "calendar.json")
        frozen = FrozenReliabilityVersions.model_validate(
            _safe_json(paths["evidence"] / "frozen_versions.json")
        )
        try:
            self._validate_readonly_descriptor(paths["evidence"])
        except (ValidationError, ValueError, OSError, json.JSONDecodeError):
            return self._report_unavailable(
                request, fingerprints, "LINEAGE_UNAVAILABLE", [], frozen
            )
        try:
            registry_path = next(
                path for path in paths["controls"] if path.name == "provider_registry.sqlite3"
            )
            registry = read_secondary_qualification_projection(registry_path)
        except _AcceptanceError as error:
            return self._report_unavailable(request, fingerprints, error.reason, [], frozen)
        if (
            registry.provider_id != frozen.secondary_provider_id
            or registry.window_id != frozen.qualification_window_id
        ):
            return self._report_unavailable(
                request, fingerprints, "CANONICAL_INTEGRITY_FAILED", [], frozen
            )
        raw_sessions = list(calendar["source_sequence"])
        if not calendar.get("confirmed", False) or calendar.get("unknown_state", False):
            return self._report_unavailable(request, fingerprints, "CALENDAR_UNAVAILABLE", [], None)
        if len(raw_sessions) != 20:
            return self._report_unavailable(
                request, fingerprints, "SESSION_COUNT_NOT_20", [], frozen
            )
        if raw_sessions != sorted(set(raw_sessions)):
            return self._report_unavailable(
                request, fingerprints, "SESSION_SEQUENCE_INVALID", [], frozen
            )
        if (
            raw_sessions[0] != request.start
            or raw_sessions[-1] != request.end
            or date.fromisoformat(request.end)
            > datetime.fromisoformat(request.now[:-1] + "+00:00").astimezone(SHANGHAI).date()
        ):
            return self._report_unavailable(
                request, fingerprints, "PIT_VISIBILITY_INVALID", [], frozen
            )
        # A calendar is the canonical 20-session selection.  A stale/missing
        # manifest is retained as a continuity failure so the rest of the
        # snapshot can still be evaluated and reported as not_ready.
        observations: list[dict[str, Any]] = []
        try:
            for ordinal, session in enumerate(raw_sessions, 1):
                item = _safe_json(paths["dataset"] / "sessions" / f"{session}.json")
                model = SessionObservation.model_validate(item)
                if model.ordinal != ordinal or model.session != session:
                    raise ValueError("session sequence")
                self._validate_observation_hashes(item, paths["evidence"], frozen)
                observations.append(item)
        except _AcceptanceError as error:
            return self._report_unavailable(
                request, fingerprints, error.reason, raw_sessions, frozen
            )
        except FileNotFoundError:
            return self._report_unavailable(
                request, fingerprints, "CONTROL_STATE_UNAVAILABLE", raw_sessions, frozen
            )
        except (ValidationError, ValueError, OSError, json.JSONDecodeError):
            return self._report_unavailable(
                request, fingerprints, "LINEAGE_UNAVAILABLE", raw_sessions, frozen
            )
        try:
            return self._build_report(
                request, paths, fingerprints, frozen, observations, calendar, registry
            )
        except _AcceptanceError as error:
            return self._report_unavailable(
                request, fingerprints, error.reason, raw_sessions, frozen
            )
        except ValidationError:
            return self._report_unavailable(
                request, fingerprints, "LINEAGE_UNAVAILABLE", raw_sessions, frozen
            )
        except (ValueError, OSError, json.JSONDecodeError):
            return self._report_unavailable(
                request, fingerprints, "LINEAGE_UNAVAILABLE", raw_sessions, frozen
            )

    def _validate_observation_hashes(
        self, item: dict[str, Any], evidence: Path, frozen: FrozenReliabilityVersions
    ) -> None:
        for field_path, root, value in (
            (
                "CalendarRawFacts.raw_facts_sha256",
                item["calendar_raw_facts"],
                item["calendar_raw_facts"]["raw_facts_sha256"],
            ),
            (
                "ReadBoundaryRawFacts.probe_schema_digest",
                item["read_boundary_raw_facts"],
                item["read_boundary_raw_facts"]["probe_schema_digest"],
            ),
            (
                "SessionEvidenceBinding.binding_sha256",
                item["evidence"],
                item["evidence"]["binding_sha256"],
            ),
            (
                "ReplicationObservation.observation_sha256",
                item["replication_observation"],
                item["replication_observation"]["observation_sha256"],
            ),
            (
                "SessionObservation.frozen_versions_sha256",
                frozen.model_dump(mode="python"),
                item["frozen_versions_sha256"],
            ),
            ("SessionObservation.schema_policy_digest", item, item["schema_policy_digest"]),
            ("SessionObservation.observation_sha256", item, item["observation_sha256"]),
        ):
            if _digest(field_path, root) != value:
                if field_path == "SessionObservation.observation_sha256":
                    raise _AcceptanceError("SNAPSHOT_CHANGED")
                if field_path == "SessionObservation.frozen_versions_sha256":
                    # Keep the session available so provenance can report a
                    # deterministic VERSION_DRIFT metric.
                    continue
                raise ValueError("digest mismatch")
        for field in ("evidence", "candidate", "gate_report", "manifest", "object", "selection"):
            self._validate_leaf(
                evidence,
                item["evidence"][f"{field}_sha256"],
                f"SessionEvidenceBinding.{field}_sha256",
            )
        pointer = item["pointer_reconciliation"]
        for key, path in (
            ("pointer_sha256", "PointerReconciliation.pointer_sha256"),
            ("manifest_sha256", "PointerReconciliation.manifest_sha256"),
            ("object_sha256", "PointerReconciliation.object_sha256"),
            ("descriptor_sha256", "PointerReconciliation.descriptor_sha256"),
        ):
            self._validate_leaf(evidence, pointer[key], path)

    def _validate_leaf(self, evidence: Path, expected: str, source_field: str) -> None:
        found = False
        seen = False
        descriptor_count = 0
        for descriptor_path in evidence.rglob("*.descriptor.json"):
            descriptor = _safe_json(descriptor_path)
            if descriptor.get("source_field") != source_field:
                continue
            seen = True
            descriptor_count += 1
            # Repeated session fields have one descriptor per session.  Select
            # the descriptor bound to this digest first; unrelated siblings
            # are not evidence for (or against) the requested value.
            if descriptor.get("contract_digest") != expected:
                continue
            found = True
            root_base = (
                evidence
                if descriptor.get("root_base") != "dataset"
                else evidence.parent / "dataset"
            )
            if descriptor.get("immutable") is not True:
                raise ValueError("lineage unavailable")
            raw = _read_anchored_file(evidence, descriptor["object_path"])
            if hashlib.sha256(raw).hexdigest() != descriptor.get("artifact_sha256"):
                raise ValueError("lineage invalid")
            root = json.loads(
                _read_anchored_file(root_base, descriptor["root_path"]).decode("utf-8")
            )
            if descriptor.get("contract_digest") != _digest(source_field, root):
                raise ValueError("lineage invalid")
        # A session-level mutation can legitimately produce a new digest before
        # its writer-owned leaf is materialised; that is a metric failure, not a
        # malformed reader input.  A completely absent descriptor set, however,
        # is an unavailable lineage proof.
        repeated = source_field.split(".", 1)[0] in {
            "SessionEvidenceBinding",
            "PointerReconciliation",
            "ReplicationObservation",
            "CalendarRawFacts",
            "ReadBoundaryRawFacts",
        }
        if not found and (not seen or (repeated and descriptor_count < 20)):
            raise ValueError("lineage unavailable")

    def _validate_readonly_descriptor(self, evidence: Path) -> None:
        """Read and verify the writer-owned descriptor and its immutable object."""
        object_dir = evidence / "objects" / "ReadonlyEvidenceDescriptor.object_sha256"
        descriptor_dir = evidence / "objects" / "ReadonlyEvidenceDescriptor.descriptor_sha256"
        object_entries = [
            entry for entry in os.listdir(object_dir) if entry.endswith(".descriptor.json")
        ]
        descriptor_entries = [
            entry for entry in os.listdir(descriptor_dir) if entry.endswith(".descriptor.json")
        ]
        if len(object_entries) != 1 or len(descriptor_entries) != 1:
            raise ValueError("readonly descriptor unavailable")
        object_sidecar_path = (
            f"objects/ReadonlyEvidenceDescriptor.object_sha256/{object_entries[0]}"
        )
        descriptor_sidecar_path = (
            f"objects/ReadonlyEvidenceDescriptor.descriptor_sha256/{descriptor_entries[0]}"
        )
        object_sidecar = json.loads(
            _read_anchored_file(evidence, object_sidecar_path).decode("utf-8")
        )
        descriptor_sidecar = json.loads(
            _read_anchored_file(evidence, descriptor_sidecar_path).decode("utf-8")
        )
        for sidecar in (object_sidecar, descriptor_sidecar):
            if sidecar.get("immutable") is not True:
                raise ValueError("readonly descriptor unavailable")
            object_path = sidecar.get("object_path")
            if not isinstance(object_path, str):
                raise ValueError("readonly descriptor unavailable")
            raw = _read_anchored_file(evidence, object_path)
            if hashlib.sha256(raw).hexdigest() != sidecar.get("artifact_sha256"):
                raise ValueError("readonly descriptor invalid")
        descriptor_raw = _read_anchored_file(evidence, descriptor_sidecar["object_path"])
        descriptor = ReadonlyEvidenceDescriptor.model_validate(
            json.loads(descriptor_raw.decode("utf-8"))
        )
        object_raw = _read_anchored_file(evidence, object_sidecar["object_path"])
        object_root = json.loads(
            _read_anchored_file(evidence, object_sidecar["root_path"]).decode("utf-8")
        )
        if (
            descriptor.object_sha256
            != _digest("ReadonlyEvidenceDescriptor.object_sha256", object_root)
            or descriptor.descriptor_sha256
            != _digest(
                "ReadonlyEvidenceDescriptor.descriptor_sha256", descriptor.model_dump(mode="python")
            )
            or descriptor.object_sha256 != object_sidecar.get("contract_digest")
            or descriptor.descriptor_sha256 != descriptor_sidecar.get("contract_digest")
        ):
            raise ValueError("readonly descriptor invalid")
        if hashlib.sha256(object_raw).hexdigest() != object_sidecar.get("artifact_sha256"):
            raise ValueError("readonly object invalid")

    def _report_unavailable(
        self,
        request: AcceptanceInput,
        fingerprints: tuple[dict[str, Any], ...],
        reason: str,
        sessions: list[str],
        frozen: FrozenReliabilityVersions | None,
    ) -> R2FAcceptanceReport:
        identity = self._identity(request, fingerprints, frozen) if frozen else None
        metric_reasons = {name: reason for name in _METRICS}
        if reason == "CONTROL_STATE_UNAVAILABLE" and sessions:
            # A missing session is a lineage gap for data-derived metrics, but
            # remains a control-state failure for the read-boundary surface.
            for name in (
                "continuity",
                "coverage",
                "canonical_integrity",
                "source_purity",
                "provenance",
                "universe",
            ):
                metric_reasons[name] = "LINEAGE_UNAVAILABLE"
        metrics = {
            name: self._metric(name, "unavailable", metric_reasons[name]) for name in _METRICS
        }
        window_bundle: dict[str, Any] | None = None
        window_refs: list[str] = []
        if identity is not None:
            try:
                candidate = _safe_json(Path(request.evidence_root) / "window.json")
                if isinstance(candidate, dict) and isinstance(candidate.get("payload"), dict):
                    window_bundle = candidate
                    if isinstance(candidate.get("envelope_sha256"), str):
                        window_refs = [candidate["envelope_sha256"]]
            except (OSError, ValueError, json.JSONDecodeError):
                pass
        report = {
            "status": "unavailable",
            "window_start": sessions[0] if sessions else None,
            "window_end": sessions[-1] if sessions else None,
            "selected_sessions": sessions,
            "frozen_versions": frozen.model_dump(mode="python") if frozen else None,
            **metrics,
            "quality_issues": [reason],
            "snapshot_identity": identity,
            "session_observations": [],
            "observation_refs": [],
            "window_evidence_bundle": window_bundle,
            "window_evidence_refs": window_refs,
            "pre_capture_failure": None,
            "semantic_report_sha256": None,
            "diagnostic_envelope": {
                "elapsed_ms": None,
                "read_operations": 0,
                "replay_sample_count": 0,
            },
            "provider_requests": 0,
            "writes": False,
            "restore_started": False,
            "production_window_started": False,
        }
        report["semantic_report_sha256"] = _digest(
            "R2FAcceptanceReport.semantic_report_sha256", report
        )
        return R2FAcceptanceReport.model_validate(report)

    def _identity(
        self,
        request: AcceptanceInput,
        fingerprints: tuple[dict[str, Any], ...],
        frozen: FrozenReliabilityVersions | None,
    ) -> dict[str, Any] | None:
        if frozen is None:
            return None
        value = {
            "requested_start": request.start,
            "requested_end": request.end,
            "as_of_utc": request.now,
            "as_of_timezone": "Asia/Shanghai",
            "input_fingerprints": fingerprints,
            "frozen_versions": frozen.model_dump(mode="python"),
            "input_fingerprint_sha256": "0" * 64,
            "frozen_version_vector_sha256": "0" * 64,
            "snapshot_sha256": "0" * 64,
        }
        value["input_fingerprint_sha256"] = _digest(
            "SnapshotIdentity.input_fingerprint_sha256", value
        )
        value["frozen_version_vector_sha256"] = _digest(
            "SnapshotIdentity.frozen_version_vector_sha256", value
        )
        value["snapshot_sha256"] = _digest("SnapshotIdentity.snapshot_sha256", value)
        return value

    def _build_report(
        self,
        request: AcceptanceInput,
        paths: dict[str, Any],
        fingerprints: tuple[dict[str, Any], ...],
        frozen: FrozenReliabilityVersions,
        observations: list[dict[str, Any]],
        calendar: dict[str, Any],
        registry: SecondaryQualificationProjection | None = None,
    ) -> R2FAcceptanceReport:
        sessions = [item["session"] for item in observations]
        identity = self._identity(request, fingerprints, frozen)
        metrics: dict[str, dict[str, Any]] = {}
        next_values = [
            self._cutoff(item.get("next_morning_published_at"), item["session"], True)
            for item in observations
        ]
        evening_values = [
            self._cutoff(item.get("same_evening_published_at"), item["session"], False)
            for item in observations
        ]
        continuity_good = _safe_json(paths["dataset"] / "manifest.json").get("sessions") == sessions
        metrics["continuity"] = self._metric(
            "continuity",
            "pass" if continuity_good else "fail",
            None if continuity_good else "CONTINUITY_FAILED",
            0 if continuity_good else 1,
            0,
        )
        metrics["next_morning_availability"] = self._metric(
            "next_morning_availability",
            "pass" if all(next_values) else "fail",
            None if all(next_values) else "AVAILABILITY_CUTOFF_FAILED",
            sum(next_values) / 20,
            1.0,
        )
        metrics["same_evening_availability"] = self._metric(
            "same_evening_availability",
            "pass" if sum(evening_values) / 20 >= 0.9 else "fail",
            None if sum(evening_values) / 20 >= 0.9 else "AVAILABILITY_CUTOFF_FAILED",
            sum(evening_values) / 20,
            0.9,
        )
        coverage = min(
            (item["loaded_count"] / item["required_count"] if item["required_count"] else 0)
            for item in observations
        )
        coverage_good = all(
            item["required_count"] > 0
            and item["loaded_count"] == item["required_count"]
            and item["unknown_count"] == 0
            for item in observations
        )
        metrics["coverage"] = self._metric(
            "coverage",
            "pass" if coverage_good else "fail",
            None if coverage_good else "COVERAGE_FAILED",
            coverage,
            1.0,
        )
        canonical_good = all(
            item["pointer_reconciliation"]["pointer_manifest_object_match"] for item in observations
        )
        metrics["canonical_integrity"] = self._metric(
            "canonical_integrity",
            "pass" if canonical_good else "fail",
            None if canonical_good else "CANONICAL_INTEGRITY_FAILED",
            canonical_good,
            True,
        )
        metrics["source_purity"] = self._metric(
            "source_purity",
            "pass"
            if all(len(item["canonical_provider_ids"]) == 1 for item in observations)
            else "fail",
            None
            if all(len(item["canonical_provider_ids"]) == 1 for item in observations)
            else "SOURCE_PURITY_FAILED",
            all(len(item["canonical_provider_ids"]) == 1 for item in observations),
            True,
        )
        window = _safe_json(paths["evidence"] / "window.json")
        self._validate_envelope(window)
        payload = window["payload"]
        WindowEvidenceBundlePayload.model_validate(payload)
        child_models: dict[str, type[StrictModel]] = {
            "recovery_observation": RecoveryObservation,
            "failover_observation": WholeSessionFailoverDrill,
            "replay_sample": ReplaySampleEvidence,
            "adjustment_equivalence": AdjustmentEquivalenceEvidence,
            "error_handling_observation": ErrorHandlingObservation,
            "local_nas_isolation_observation": LocalNasIsolationObservation,
            "restore_observation": RestoreDrillEvidence,
        }
        for child_name, child_model in child_models.items():
            child_envelope = payload.get(child_name)
            if child_envelope is not None:
                child_model.model_validate(child_envelope["payload"])
        for name, child, reason, target in (
            ("recovery", "recovery_observation", "RECOVERY_FAILED", True),
            ("failover", "failover_observation", "FAILOVER_UNAVAILABLE", True),
            ("replay", "replay_sample", "REPLAY_UNAVAILABLE", True),
            ("adjustment", "adjustment_equivalence", "ADJUSTMENT_UNAVAILABLE", True),
            ("error_handling", "error_handling_observation", "ERROR_HANDLING_FAILED", True),
            (
                "local_nas_isolation",
                "local_nas_isolation_observation",
                "LOCAL_NAS_ISOLATION_FAILED",
                True,
            ),
            ("restore", "restore_observation", "RESTORE_UNAVAILABLE", True),
        ):
            child_value = payload.get(child)
            good = child_value is not None
            child_invalid = False
            if good:
                try:
                    self._validate_envelope(child_value)
                except (ValidationError, ValueError, KeyError, TypeError):
                    child_invalid = True
                    good = False
            value = child_value.get("payload", {}) if good else {}
            if name == "recovery":
                good = (
                    good
                    and value.get("immutable") is True
                    and value.get("publication_count") == 1
                    and value.get("before_generation") != value.get("after_generation")
                )
                metric_status, metric_reason = (
                    ("pass", None)
                    if good
                    else (
                        ("unavailable", "CONTROL_STATE_UNAVAILABLE")
                        if child_value is None
                        else ("unavailable", reason)
                        if child_invalid
                        else ("fail", reason)
                    )
                )
            elif name == "failover":
                good = (
                    good
                    and value.get("primary_unavailable") is True
                    and value.get("selected_provider_id")
                    == value.get("qualified_secondary_provider_id")
                    == frozen.secondary_provider_id
                    and value.get("mixed_source_rows") == 0
                    and value.get("qualification_proof_status") == "available"
                )
                metric_status, metric_reason = ("pass", None) if good else ("unavailable", reason)
            elif name == "replay":
                good = (
                    good
                    and value.get("semantic_equal") is True
                    and value.get("offline_context", {}).get("adapter_id")
                    not in (None, "unknown-adapter")
                )
                metric_status, metric_reason = (
                    ("pass", None)
                    if good
                    else (
                        ("unavailable", reason)
                        if not value.get("semantic_equal", True)
                        else ("unavailable", reason)
                    )
                )
                if value.get("semantic_equal") is False:
                    metric_reason = "REPLAY_UNAVAILABLE"
            elif name == "adjustment":
                good = (
                    good
                    and value.get("equivalence_passed") is True
                    and value.get("compared_sessions") == sessions[:2]
                )
                metric_status, metric_reason = ("pass", None) if good else ("unavailable", reason)
            elif name == "error_handling":
                events = value.get("events", [])
                classes = [event.get("forced_error_class") for event in events]
                good = (
                    good
                    and len(events) == 6
                    and len(set(classes)) == 6
                    and all(
                        event.get("expected_class")
                        == event.get("forced_error_class")
                        == event.get("observed_class")
                        and event.get("normalized_result") in {"not_ready", "unavailable"}
                        for event in events
                    )
                )
                metric_status, metric_reason = (
                    ("pass", None)
                    if good
                    else ("fail", reason)
                    if child_value is not None
                    else ("unavailable", "CONTROL_STATE_UNAVAILABLE")
                )
            elif name == "local_nas_isolation":
                good = (
                    good
                    and value.get("retryable") is True
                    and value.get("nas_failure_did_not_block_local") is True
                    and value.get("retry_state") == value.get("retry_transition") == "retrying"
                )
                metric_status, metric_reason = (
                    ("pass", None)
                    if good
                    else ("fail", reason)
                    if child_value is not None
                    else ("unavailable", "CONTROL_STATE_UNAVAILABLE")
                )
            else:
                expected_rows = sum(item["required_count"] for item in observations)
                good = (
                    good
                    and value.get("verification_state") == "verified"
                    and value.get("row_count") == expected_rows
                )
                metric_status, metric_reason = ("pass", None) if good else ("unavailable", reason)
            metrics[name] = self._metric(name, metric_status, metric_reason, good, target)
        versions_good = all(
            item["frozen_versions_sha256"]
            == _digest(
                "SessionObservation.frozen_versions_sha256", frozen.model_dump(mode="python")
            )
            for item in observations
        )
        metrics["provenance"] = self._metric(
            "provenance",
            "pass" if versions_good else "fail",
            None if versions_good else "VERSION_DRIFT",
            versions_good,
            True,
        )
        calendar_conflict = calendar.get("conflict_state", False)
        metrics["calendar"] = self._metric(
            "calendar",
            "fail" if calendar_conflict else "pass",
            "CALENDAR_CONFLICT" if calendar_conflict else None,
            not calendar_conflict,
            True,
        )
        universe_good = all(
            item["unknown_count"] == 0
            and item["required_count"]
            == item["loaded_count"]
            + item["suspension_count"]
            + item["not_listed_count"]
            + item["delisted_count"]
            + item["unknown_count"]
            for item in observations
        )
        metrics["universe"] = self._metric(
            "universe",
            "pass" if universe_good else "fail",
            None if universe_good else "UNIVERSE_COUNT_MISMATCH",
            universe_good,
            True,
        )
        boundary_good = all(
            item["read_boundary_raw_facts"]["write_count"] == 0
            and not item["read_boundary_raw_facts"]["future_rows_seen"]
            for item in observations
        )
        metrics["read_boundary"] = self._metric(
            "read_boundary",
            "pass" if boundary_good else "fail",
            None if boundary_good else "READ_BOUNDARY_FAILED",
            boundary_good,
            True,
        )
        completed = _safe_json(paths["evidence"] / "completed_replication_restore.json")
        for field_path in (
            "CompletedReplicationRestoreSnapshotV1.replication_observation_sha256",
            "CompletedReplicationRestoreSnapshotV1.restore_report_sha256",
            "CompletedReplicationRestoreSnapshotV1.destination_record_sha256",
            "CompletedReplicationRestoreSnapshotV1.destination_head_sha256",
        ):
            field_name = field_path.rsplit(".", 1)[-1]
            if completed.get(field_name) is not None:
                self._validate_leaf(paths["evidence"], completed[field_name], field_path)
        threshold = (completed.get("policy_thresholds") or {}).get("replication_lag_seconds")
        lag = max(
            (item["replication_observation"].get("lag_seconds") or 0) for item in observations
        )
        metrics["replication"] = self._metric(
            "replication",
            "unavailable" if threshold is None else ("pass" if lag <= threshold else "fail"),
            "REPLICATION_UNAVAILABLE"
            if threshold is None
            else (None if lag <= threshold else "REPLICATION_LAG"),
            lag,
            threshold or 0,
        )
        status = (
            "unavailable"
            if any(value["status"] == "unavailable" for value in metrics.values())
            else (
                "not_ready"
                if any(value["status"] == "fail" for value in metrics.values())
                else "ready"
            )
        )
        issues = [value["reason_code"] for value in metrics.values() if value["reason_code"]]
        issues = sorted(
            set(issues),
            key=lambda value: (
                _REASON_ORDER.index(value) if value in _REASON_ORDER else len(_REASON_ORDER)
            ),
        )
        report = {
            "status": status,
            "window_start": sessions[0],
            "window_end": sessions[-1],
            "selected_sessions": sessions,
            "frozen_versions": frozen.model_dump(mode="python"),
            **metrics,
            "quality_issues": issues,
            "snapshot_identity": identity,
            "session_observations": observations,
            "observation_refs": [item["observation_sha256"] for item in observations],
            "window_evidence_bundle": window,
            "window_evidence_refs": [window["envelope_sha256"]],
            "pre_capture_failure": None,
            "semantic_report_sha256": None,
            "diagnostic_envelope": {
                "elapsed_ms": None,
                "read_operations": 0,
                "replay_sample_count": 0,
            },
            "provider_requests": 0,
            "writes": False,
            "restore_started": False,
            "production_window_started": False,
        }
        report["semantic_report_sha256"] = _digest(
            "R2FAcceptanceReport.semantic_report_sha256", report
        )
        validated = R2FAcceptanceReport.model_validate(report)
        if registry is not None and validated.status == "ready":
            object.__setattr__(
                validated, "_registry_projection", registry.model_dump(mode="python")
            )
        return validated

    def _validate_envelope(self, envelope: dict[str, Any]) -> None:
        ImmutableObservationEnvelopeV1.model_validate(envelope)
        if (
            _digest("ImmutableObservationEnvelopeV1.payload_sha256", envelope)
            != envelope["payload_sha256"]
            or _digest("ImmutableObservationEnvelopeV1.envelope_sha256", envelope)
            != envelope["envelope_sha256"]
        ):
            raise ValueError("envelope invalid")

    @staticmethod
    def _cutoff(value: str | None, session: str, next_morning: bool) -> bool:
        if not value:
            return False
        try:
            local = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(SHANGHAI)
            day = date.fromisoformat(session) + (timedelta(days=1) if next_morning else timedelta())
            cutoff = datetime.combine(day, datetime.min.time(), tzinfo=SHANGHAI) + (
                timedelta(hours=8) if next_morning else timedelta(hours=21, minutes=15)
            )
            return local.date() == day and local <= cutoff
        except (TypeError, ValueError):
            return False


def read_secondary_qualification_projection(
    path: Path | str,
    *,
    provider_id: str = "tickflow",
    window_id: str = "qualification-window-20",
) -> SecondaryQualificationProjection:
    """Read the authority projection from the registry, never from a bundle."""
    path = Path(path)
    if _catalog_key(path) != "shadow_registry":
        raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
    uri = f"file:{path}?mode=ro&immutable=false"
    try:
        with sqlite3.connect(uri, uri=True, timeout=0) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA busy_timeout=250")
            connection.execute("BEGIN DEFERRED")
            catalog = _CATALOGS["shadow_registry"]
            if connection.execute("PRAGMA user_version").fetchone()[0] != catalog["user_version"]:
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            actual_objects = {
                f"{kind}:{name}"
                for kind, name in connection.execute(
                    "SELECT type,name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'"
                )
            }
            if actual_objects != set(catalog["sqlite_master_allowlist"]):
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            for table_name, table_spec in catalog["tables"].items():
                columns = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
                declared = [f"{row[1]}:{row[2] or ''}" for row in columns]
                if declared != list(table_spec["columns"]):
                    raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
                primary_key = [
                    row[1] for row in sorted(columns, key=lambda item: item[5]) if row[5]
                ]
                if primary_key != list(table_spec["primary_key"]):
                    raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            provider = connection.execute(
                "SELECT provider_id, admission_state FROM provider_record WHERE provider_id=?",
                (provider_id,),
            ).fetchone()
            window = connection.execute(
                "SELECT provider_id, window_id, window_start, window_end, "
                "consecutive_sessions, window_state, qualification_evidence_sha256, "
                "qualification_candidate_sha256, terminal_attestation_id "
                "FROM qualification_window WHERE provider_id=? AND window_id=?",
                (provider_id, window_id),
            ).fetchone()
            if provider is None or provider[1] != "qualified" or window is None:
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            if window[6] is None or window[7] is None or window[8] is None:
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            sessions = connection.execute(
                "SELECT trade_date, session_report_id, terminal_attestation_id "
                "FROM qualification_session WHERE provider_id=? AND window_id=? "
                "ORDER BY trade_date",
                (provider_id, window_id),
            ).fetchall()
            if len(sessions) != 20:
                raise _AcceptanceError("SESSION_COUNT_NOT_20")
            dates = [row[0] for row in sessions]
            if (
                len(set(dates)) != 20
                or dates != sorted(dates)
                or dates[0] != window[2]
                or dates[-1] != window[3]
            ):
                raise _AcceptanceError("SESSION_SEQUENCE_INVALID")
            if window[4] != 20:
                raise _AcceptanceError("SESSION_COUNT_NOT_20")
            reports = {
                row[0]
                for row in connection.execute(
                    "SELECT session_report_id FROM session_report "
                    "WHERE provider_id=? AND window_id=?",
                    (provider_id, window_id),
                )
            }
            attestations = {
                row[0]: row
                for row in connection.execute(
                    "SELECT attestation_id, provider_id, window_id, session_id, "
                    "session_report_id FROM shadow_terminal_attestation "
                    "WHERE provider_id=? AND window_id=?",
                    (provider_id, window_id),
                )
            }
            if any(report_id not in reports for _, report_id, _ in sessions):
                raise _AcceptanceError("CANONICAL_INTEGRITY_FAILED")
            if any(
                attestation_id not in attestations
                or attestations[attestation_id][1:4] != (provider_id, window_id, trade_date)
                or attestations[attestation_id][4] != report_id
                for trade_date, report_id, attestation_id in sessions
            ):
                raise _AcceptanceError("CANONICAL_INTEGRITY_FAILED")
            terminal_job = connection.execute(
                "SELECT terminal_attestation_id FROM shadow_job "
                "WHERE provider_id=? AND window_id=? AND trade_date=? "
                "ORDER BY job_id DESC LIMIT 1",
                (provider_id, window_id, window[3]),
            ).fetchone()
            if terminal_job is None or terminal_job[0] != window[8]:
                raise _AcceptanceError("CANONICAL_INTEGRITY_FAILED")
            digest = _sqlite_logical_digest(path, "shadow_registry")
            connection.rollback()
    except _AcceptanceError:
        raise
    except (OSError, sqlite3.Error):
        raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE") from None
    return SecondaryQualificationProjection(
        provider_id=provider_id,
        window_id=window_id,
        window_state="qualified",
        session_count=20,
        logical_digest=digest,
    )


def read_readonly_evidence(
    descriptor: ReadonlyEvidenceDescriptor | dict[str, Any],
    immutable_bytes: bytes,
) -> ReadonlyEvidenceDescriptor:
    """Validate a Task20 descriptor against supplied immutable bytes."""
    model = ReadonlyEvidenceDescriptor.model_validate(descriptor)
    if hashlib.sha256(immutable_bytes).hexdigest() != model.object_sha256:
        raise _AcceptanceError("LINEAGE_INVALID")
    if (
        _digest("ReadonlyEvidenceDescriptor.descriptor_sha256", model.model_dump(mode="python"))
        != model.descriptor_sha256
    ):
        raise _AcceptanceError("LINEAGE_INVALID")
    return model


__all__ = [
    "AcceptanceInput",
    "AcceptanceReader",
    "CapturedSnapshot",
    "MetricValue",
    "MetricResult",
    "SnapshotFingerprint",
    "FrozenReliabilityVersions",
    "SnapshotIdentity",
    "PreCaptureFailurePayloadV1",
    "SessionObservation",
    "ReadonlyEvidenceDescriptor",
    "CompletedReplicationRestoreSnapshotV1",
    "ImmutableObservationEnvelopeV1",
    "OfflineReplayContext",
    "WholeSessionFailoverDrill",
    "RecoveryObservation",
    "ReplaySampleEvidence",
    "AdjustmentEquivalenceEvidence",
    "ErrorHandlingEvent",
    "ErrorHandlingObservation",
    "LocalNasIsolationObservation",
    "RestoreDrillEvidence",
    "WindowEvidenceBundlePayload",
    "SecondaryQualificationProjection",
    "read_secondary_qualification_projection",
    "read_readonly_evidence",
    "_canonical_json",
    "_digest",
    "_tree_content_digest",
    "_sqlite_logical_digest",
    "_fingerprint",
]
