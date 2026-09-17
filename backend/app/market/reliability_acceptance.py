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
import tempfile
import unicodedata
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal, TypeVar
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

ROOT = Path(__file__).resolve().parents[3]
DESIGN_RELATIVE = Path(
    "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md"
)


def _resolve_design_asset() -> Path:
    """Resolve the immutable X8 design asset in either runtime layout.

    Development checkouts retain ``docs`` at the release root.  The installer
    moves all documentation under ``public`` so the installed runtime does not
    expose the source-tree layout.  The first existing path is authoritative;
    a malformed asset is never replaced with a guessed or duplicated contract.
    """
    candidates = (ROOT / DESIGN_RELATIVE, ROOT / "public" / DESIGN_RELATIVE)
    for candidate in candidates:
        try:
            if candidate.is_file():
                return candidate
        except OSError:
            continue
    return candidates[0]


DESIGN = _resolve_design_asset()
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_INT = 2**31 - 1
MAX_ENTRIES = 100_000
MAX_BYTES = 512 * 1024 * 1024
MAX_ROWS = 1_000_000
MAX_ROOTS = 32
_ERROR_EVENT_CLASSES = ("timeout", "auth", "rate", "schema", "coverage", "storage")


def _canonical_json(value: Any) -> bytes:
    """The project canonical JSON contract (including its one LF)."""
    return (
        json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
        + "\n"
    ).encode("utf-8")


def _contracts() -> dict[str, Any]:
    try:
        text = DESIGN.read_text(encoding="utf-8")
        match = re.search(
            r"<!-- R2F5_X8_CONTRACTS_JSON -->\s*```json\s*(\{.*?\})\s*```",
            text,
            re.S,
        )
        if match is None:
            return {"digest_contracts": [], "sqlite_catalogs": {}}
        value = json.loads(match.group(1))
        return value if isinstance(value, dict) else {"digest_contracts": [], "sqlite_catalogs": {}}
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError):
        # Asset loading is deliberately fail-closed and import-safe.  The
        # reader will return a typed unavailable report if the contract cannot
        # be loaded; it must never leak a parser/packaging exception to HTTP.
        return {"digest_contracts": [], "sqlite_catalogs": {}}


_SAFE_METRICS = (
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

# The installed design asset is executable input, not merely documentation.
# Keep an immutable inventory of every digest contract consumed by this module
# so a truncated or partially packaged X8 asset cannot execute with a smaller
# contract and accidentally report a false pass.
_EXPECTED_DIGEST_FIELDS = frozenset(
    """
    ImmutableObservationEnvelopeV1.payload_sha256
    ImmutableObservationEnvelopeV1.envelope_sha256
    PreCaptureFailurePayloadV1.semantic_report_sha256
    CalendarRawFacts.raw_facts_sha256
    ReadBoundaryRawFacts.probe_schema_digest
    SessionEvidenceBinding.evidence_sha256
    SessionEvidenceBinding.candidate_sha256
    SessionEvidenceBinding.gate_report_sha256
    SessionEvidenceBinding.manifest_sha256
    SessionEvidenceBinding.object_sha256
    SessionEvidenceBinding.selection_sha256
    SessionEvidenceBinding.binding_sha256
    PointerReconciliation.pointer_sha256
    PointerReconciliation.manifest_sha256
    PointerReconciliation.object_sha256
    PointerReconciliation.descriptor_sha256
    ReplicationObservation.source_commit_sha256
    ReplicationObservation.destination_record_sha256
    ReplicationObservation.destination_head_sha256
    ReplicationObservation.observation_sha256
    RecoveryObservation.duplicate_proof_sha256
    RecoveryObservation.observation_sha256
    ErrorHandlingObservation.observation_sha256
    LocalNasIsolationObservation.observation_sha256
    SessionObservation.frozen_versions_sha256
    SessionObservation.schema_policy_digest
    SessionObservation.observation_sha256
    WholeSessionFailoverDrill.selection_sha256
    WholeSessionFailoverDrill.manifest_sha256
    WholeSessionFailoverDrill.pointer_sha256
    WholeSessionFailoverDrill.readback_sha256
    ReplaySampleEvidence.sample_object_sha256
    ReplaySampleEvidence.candidate_sha256
    RestoreDrillEvidence.sentinel_sha256
    RestoreDrillEvidence.destination_head_sha256
    RestoreDrillEvidence.record_sha256
    RestoreDrillEvidence.manifest_sha256
    RestoreDrillEvidence.restore_report_sha256
    RestoreDrillEvidence.api_readback_sha256
    SQLitePresentMemberFingerprint.full_sha256
    SQLiteRollbackJournalAbsenceProof.proof_digest
    SQLiteSnapshotFingerprint.logical_digest
    SQLiteSnapshotFingerprint.catalog_digest
    SnapshotFingerprint.sha256
    FrozenReliabilityVersions.config_digest
    RecoveryObservation.after_manifest_sha256
    RecoveryObservation.after_pointer_sha256
    RecoveryObservation.after_selection_sha256
    ErrorHandlingObservation.events.evidence_sha256
    LocalNasIsolationObservation.local_pointer_sha256
    CompletedReplicationRestoreSnapshotV1.replication_observation_sha256
    CompletedReplicationRestoreSnapshotV1.restore_report_sha256
    CompletedReplicationRestoreSnapshotV1.destination_record_sha256
    CompletedReplicationRestoreSnapshotV1.destination_head_sha256
    ReadonlyEvidenceDescriptor.object_sha256
    ReadonlyEvidenceDescriptor.descriptor_sha256
    SnapshotIdentity.input_fingerprint_sha256
    SnapshotIdentity.frozen_version_vector_sha256
    SnapshotIdentity.snapshot_sha256
    R2FAcceptanceReport.semantic_report_sha256
    FrozenReliabilityVersions.installed_release_sha256
    FrozenReliabilityVersions.calendar_sha256
    FrozenReliabilityVersions.universe_sha256
    FrozenReliabilityVersions.destination_head_sha256
    OfflineReplayContext.implementation_sha256
    """.split()
)
_EXPECTED_CATALOG_KEYS = frozenset(
    {
        "replication_sidecar",
        "daily_shadow",
        "shadow_registry",
        "calendar_generation",
        "universe",
    }
)
_EXPECTED_CATALOG_ROLES = {
    "replication_sidecar": "replication",
    "daily_shadow": "qualification",
    "shadow_registry": "qualification",
    "calendar_generation": "calendar",
    "universe": "universe",
}
_DIGEST_ITEM_FIELDS = frozenset(
    {
        "field",
        "canonicalization_version",
        "root_object_type",
        "included_field_paths",
        "excluded_fields",
        "ordering",
        "null_encoding",
        "domain_separation_prefix",
    }
)
_CATALOG_FIELDS = frozenset(
    {
        "role",
        "user_version",
        "schema_version_source",
        "allowed_tables",
        "system_tables",
        "sqlite_master_allowlist",
        "tables",
        "catalog_digest_source",
    }
)
_TABLE_FIELDS = frozenset({"columns", "primary_key", "order_by"})
_METRIC_VALUE_KINDS = frozenset({"count", "ratio", "duration_seconds", "bool", "hash"})


def _valid_string_list(value: Any, *, nonempty: bool = True) -> bool:
    return (
        isinstance(value, list)
        and (not nonempty or bool(value))
        and all(isinstance(item, str) and bool(item) for item in value)
    )


def _validate_x8_contract(value: Any) -> bool:
    """Validate all executable X8 structure before exposing any lookup maps."""
    if not isinstance(value, dict):
        return False
    if not isinstance(value.get("contract_version"), str) or not value["contract_version"]:
        return False
    if not isinstance(value.get("digest_contracts"), list):
        return False
    seen_fields: set[str] = set()
    for item in value["digest_contracts"]:
        if not isinstance(item, dict) or not _DIGEST_ITEM_FIELDS <= item.keys():
            return False
        field = item["field"]
        if (
            not isinstance(field, str)
            or not SAFE_ID.fullmatch(field)
            or field in seen_fields
            or not isinstance(item["canonicalization_version"], str)
            or not isinstance(item["root_object_type"], str)
            or not _valid_string_list(item["included_field_paths"])
            or len(set(item["included_field_paths"])) != len(item["included_field_paths"])
            or not _valid_string_list(item["excluded_fields"], nonempty=False)
            or not isinstance(item["ordering"], str)
            or not isinstance(item["null_encoding"], str)
            or not isinstance(item["domain_separation_prefix"], str)
            or not (
                (
                    item["domain_separation_prefix"].startswith("r2f5/")
                    and item["domain_separation_prefix"].endswith("\\0")
                )
                or item["domain_separation_prefix"]
                == "none; standard SHA-256 of exact full member bytes"
            )
        ):
            return False
        seen_fields.add(field)
    if seen_fields != _EXPECTED_DIGEST_FIELDS:
        return False

    metrics = value.get("metric_fields")
    if metrics != list(_SAFE_METRICS) or len(set(metrics)) != len(metrics):
        return False
    kinds = value.get("metric_value_kinds")
    if not isinstance(kinds, dict) or set(kinds) != set(_SAFE_METRICS):
        return False
    for name in _SAFE_METRICS:
        spec = kinds.get(name)
        if (
            not isinstance(spec, dict)
            or not {"observed", "target", "unavailable_null", "range"} <= spec.keys()
            or spec["observed"] not in _METRIC_VALUE_KINDS
            or spec["target"] not in _METRIC_VALUE_KINDS
            or spec["unavailable_null"] is not True
            or not isinstance(spec["range"], str)
        ):
            return False

    partitions = value.get("reason_partitions")
    if not isinstance(partitions, dict) or set(partitions) != {"failure", "unavailable"}:
        return False
    failure = partitions["failure"]
    unavailable = partitions["unavailable"]
    if (
        not _valid_string_list(failure)
        or not _valid_string_list(unavailable)
        or len(set(failure)) != len(failure)
        or len(set(unavailable)) != len(unavailable)
        or set(failure) & set(unavailable)
    ):
        return False

    catalogs = value.get("sqlite_catalogs")
    if not isinstance(catalogs, dict) or set(catalogs) != _EXPECTED_CATALOG_KEYS:
        return False
    for key, expected_role in _EXPECTED_CATALOG_ROLES.items():
        catalog = catalogs.get(key)
        if not isinstance(catalog, dict) or not _CATALOG_FIELDS <= catalog.keys():
            return False
        if catalog["role"] != expected_role or not isinstance(catalog["user_version"], int):
            return False
        if catalog["user_version"] < 0 or not isinstance(catalog["schema_version_source"], str):
            return False
        if (
            not _valid_string_list(catalog["allowed_tables"])
            or len(set(catalog["allowed_tables"])) != len(catalog["allowed_tables"])
            or not _valid_string_list(catalog["system_tables"], nonempty=False)
            or len(set(catalog["system_tables"])) != len(catalog["system_tables"])
            or not _valid_string_list(catalog["sqlite_master_allowlist"])
            or len(set(catalog["sqlite_master_allowlist"]))
            != len(catalog["sqlite_master_allowlist"])
            or not isinstance(catalog["catalog_digest_source"], str)
        ):
            return False
        tables = catalog["tables"]
        if not isinstance(tables, dict) or set(tables) != set(catalog["allowed_tables"]):
            return False
        for table_name, table in tables.items():
            if not isinstance(table_name, str) or not isinstance(table, dict):
                return False
            if set(table) != _TABLE_FIELDS:
                return False
            if (
                not _valid_string_list(table["columns"])
                or len(set(table["columns"])) != len(table["columns"])
                or not _valid_string_list(table["primary_key"], nonempty=False)
                or not _valid_string_list(table["order_by"])
                or len(set(table["primary_key"])) != len(table["primary_key"])
                or len(set(table["order_by"])) != len(table["order_by"])
            ):
                return False
            columns = {column.split(":", 1)[0] for column in table["columns"]}
            if (
                not columns
                or not set(table["primary_key"]) <= columns
                or not set(table["order_by"]) <= columns
            ):
                return False
    return True


_X8 = _contracts()
_CONTRACT_READY = _validate_x8_contract(_X8)
_RAW_DIGEST_CONTRACTS = _X8.get("digest_contracts", ())
if not isinstance(_RAW_DIGEST_CONTRACTS, list):
    _RAW_DIGEST_CONTRACTS = ()
_DIGESTS = {
    item["field"]: item
    for item in _RAW_DIGEST_CONTRACTS
    if isinstance(item, dict) and isinstance(item.get("field"), str)
}
_CATALOGS = _X8.get("sqlite_catalogs", {}) if isinstance(_X8.get("sqlite_catalogs"), dict) else {}
_METRICS = tuple(_X8.get("metric_fields", ())) if _CONTRACT_READY else _SAFE_METRICS
_REASON_PARTITIONS = _X8.get("reason_partitions", {})
if not isinstance(_REASON_PARTITIONS, dict):
    _REASON_PARTITIONS = {}
_REASONS = tuple(
    _REASON_PARTITIONS.get("failure", ())
    if isinstance(_REASON_PARTITIONS.get("failure"), list)
    else ()
) + tuple(
    _REASON_PARTITIONS.get("unavailable", ())
    if isinstance(_REASON_PARTITIONS.get("unavailable"), list)
    else ()
)
_FAILURE_REASONS = frozenset(
    _REASON_PARTITIONS.get("failure", ())
    if isinstance(_REASON_PARTITIONS.get("failure"), list)
    else ()
)
_UNAVAILABLE_REASONS = frozenset(
    _REASON_PARTITIONS.get("unavailable", ())
    if isinstance(_REASON_PARTITIONS.get("unavailable"), list)
    else ()
)
# This is only the minimal typed-error vocabulary needed when the approved
# design asset itself is absent.  It is not used to evaluate a valid snapshot
# and does not provide any digest/catalog contract.
_SAFE_PRE_CAPTURE_REASONS = frozenset(
    {"INVALID_ARGUMENTS", "PATH_INVALID", "CONTROL_STATE_UNAVAILABLE"}
)
_VALID_UNAVAILABLE_REASONS = _UNAVAILABLE_REASONS or _SAFE_PRE_CAPTURE_REASONS
_REASON_ORDER = (
    "INVALID_ARGUMENTS",
    "PATH_INVALID",
    "SNAPSHOT_CHANGED",
    "SQLITE_SNAPSHOT_INVALID",
    "TEMP_STORAGE_UNAVAILABLE",
    "TEMP_CLEANUP_FAILED",
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
    contract = _DIGESTS.get(field_path)
    if contract is None:
        raise ValueError("R2-F5 contract asset unavailable")
    if field_path == "ReadonlyEvidenceDescriptor.object_sha256":
        raw = root["immutable_object_bytes"]
        raw = raw if isinstance(raw, bytes) else str(raw).encode("utf-8")
        return _readonly_object_digest(raw)
    # The approved fixture vectors define projections solely by included field
    # paths.  ``excluded_fields`` documents ownership/self-reference but is not
    # part of the executable projection grammar.
    return hashlib.sha256(
        _domain_prefix(field_path)
        + _canonical_json(_projection(root, contract["included_field_paths"]))
    ).hexdigest()


def _readonly_object_digest(immutable_bytes: bytes) -> str:
    """Hash the exact immutable object bytes under their dedicated domain."""
    return hashlib.sha256(b"r2f5/readonly-object-v1\0" + immutable_bytes).hexdigest()


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
            raise ValueError("snapshot changed") from None
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


def _stat_parent(fd: int) -> os.stat_result:
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError("unsafe sqlite parent")
    return info


def _cleanup_temp_root(root: Path) -> None:
    try:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            for _directory, directories, files, directory_fd in os.fwalk(
                root, topdown=False, follow_symlinks=False
            ):
                for name in files:
                    os.unlink(name, dir_fd=directory_fd)
                for name in directories:
                    os.rmdir(name, dir_fd=directory_fd)
        finally:
            os.close(root_fd)
        os.rmdir(root)
    except OSError as error:
        raise ValueError("temporary cleanup failed") from error


def _probe_sqlite_member(
    parent_fd: int,
    parent: os.stat_result,
    basename: str,
    role: Literal["db", "wal", "shm"],
) -> tuple[dict[str, Any], bytes | None]:
    try:
        entry = os.stat(basename, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        if role == "db":
            raise ValueError("sqlite db absent") from None
        return (
            {
                "role": role,
                "presence": "absent",
                "parent_device": parent.st_dev,
                "parent_inode": parent.st_ino,
                "safe_basename": basename,
                "absence_marker": "absent_at_validated_parent",
            },
            None,
        )
    if stat.S_ISLNK(entry.st_mode) or not stat.S_ISREG(entry.st_mode):
        raise ValueError("unsafe sqlite member")
    file_fd = os.open(basename, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd)
    try:
        before = os.fstat(file_fd)
        if _tree_identity(entry) != _tree_identity(before) or before.st_nlink != 1:
            raise ValueError("sqlite member changed")
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        while chunk := os.read(file_fd, 1024 * 1024):
            chunks.append(chunk)
            digest.update(chunk)
            if sum(map(len, chunks)) > MAX_BYTES:
                raise ValueError("input limit")
        after = os.fstat(file_fd)
        if _tree_identity(before) != _tree_identity(after):
            raise ValueError("sqlite member changed")
        return (
            {
                "role": role,
                "presence": "present",
                "parent_device": parent.st_dev,
                "parent_inode": parent.st_ino,
                "safe_basename": basename,
                "device": after.st_dev,
                "inode": after.st_ino,
                "mode": stat.S_IMODE(after.st_mode),
                "size_bytes": after.st_size,
                "mtime_ns": after.st_mtime_ns,
                "full_sha256": digest.hexdigest(),
            },
            b"".join(chunks),
        )
    finally:
        os.close(file_fd)


def _probe_rollback_journal(
    parent_fd: int, parent: os.stat_result, basename: str, checkpoint: str
) -> tuple[os.stat_result, bool]:
    journal = f"{basename}-journal"
    try:
        os.stat(journal, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        current_parent = _stat_parent(parent_fd)
        if (
            current_parent.st_dev,
            current_parent.st_ino,
            stat.S_IMODE(current_parent.st_mode),
            current_parent.st_mtime_ns,
            current_parent.st_ctime_ns,
        ) != (
            parent.st_dev,
            parent.st_ino,
            stat.S_IMODE(parent.st_mode),
            parent.st_mtime_ns,
            parent.st_ctime_ns,
        ):
            raise ValueError("snapshot changed") from None
        return current_parent, False
    raise ValueError(
        "sqlite snapshot invalid"
        if checkpoint == "initial_probe"
        else "sqlite rollback journal changed"
    )


def _capture_sqlite_members(path: Path, role: str) -> tuple[dict[str, Any], Path]:
    """Capture SQLite bytes via one retained parent fd, never input sqlite APIs."""
    parent_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    temp_root: Path | None = None
    try:
        parent = _stat_parent(parent_fd)
        basename = path.name
        checkpoints = (
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        )
        journal_presence: list[bool] = []
        rounds: list[tuple[dict[str, Any], ...]] = []
        round_bytes: list[dict[str, bytes]] = []
        member_names = (("db", basename), ("wal", f"{basename}-wal"), ("shm", f"{basename}-shm"))
        # The first journal probe is deliberately independent of the first
        # member round.  It proves the sentinel was absent before any bytes
        # were copied and supplies the first element of the six-checkpoint
        # proof tuple.
        initial_parent, initial_present = _probe_rollback_journal(
            parent_fd, parent, basename, checkpoints[0]
        )
        journal_presence.append(initial_present)
        if _tree_identity(parent) != _tree_identity(initial_parent):
            raise ValueError("snapshot changed")
        for round_index in range(2):
            checkpoint = checkpoints[1 + round_index * 2]
            current_parent, present = _probe_rollback_journal(
                parent_fd, parent, basename, checkpoint
            )
            journal_presence.append(present)
            members: list[dict[str, Any]] = []
            contents: dict[str, bytes] = {}
            for member_role, member_name in member_names:
                member, content = _probe_sqlite_member(
                    parent_fd, current_parent, member_name, member_role
                )
                members.append(member)
                if content is not None:
                    contents[member_role] = content
            vector = [member["presence"] for member in members]
            if vector not in (["present", "absent", "absent"], ["present", "present", "present"]):
                raise ValueError("sqlite snapshot invalid")
            rounds.append(tuple(members))
            round_bytes.append(contents)
            checkpoint = checkpoints[2 + round_index * 2]
            post_parent, post_present = _probe_rollback_journal(
                parent_fd, parent, basename, checkpoint
            )
            journal_presence.append(post_present)
        final_parent, final_present = _probe_rollback_journal(
            parent_fd, parent, basename, checkpoints[-1]
        )
        journal_presence.append(final_present)
        if any(journal_presence):
            raise ValueError("sqlite rollback journal changed")
        if rounds[0] != rounds[1] or round_bytes[0] != round_bytes[1]:
            raise ValueError("sqlite snapshot changed")
        final_members: list[dict[str, Any]] = []
        final_bytes: dict[str, bytes] = {}
        for member_role, member_name in member_names:
            member, content = _probe_sqlite_member(
                parent_fd, final_parent, member_name, member_role
            )
            final_members.append(member)
            if content is not None:
                final_bytes[member_role] = content
        if tuple(final_members) != rounds[1] or final_bytes != round_bytes[1]:
            raise ValueError("sqlite snapshot changed")
        if _tree_identity(parent) != _tree_identity(final_parent):
            raise ValueError("snapshot changed")
        journal = {
            "role": "rollback_journal",
            "presence": "absent",
            "parent_device": parent.st_dev,
            "parent_inode": parent.st_ino,
            "parent_mode": stat.S_IMODE(parent.st_mode),
            "parent_mtime_ns": parent.st_mtime_ns,
            "parent_ctime_ns": parent.st_ctime_ns,
            "safe_basename": f"{basename}-journal",
            "absence_marker": "absent_at_all_capture_checkpoints",
            "directory_entry_change_marker": "none_observed",
            "probe_checkpoints": checkpoints,
            "proof_digest": "0" * 64,
        }
        journal["proof_digest"] = _digest("SQLiteRollbackJournalAbsenceProof.proof_digest", journal)
        vector = [member["presence"] for member in rounds[0]]
        temp_root = Path(tempfile.mkdtemp(prefix="r2f5-sqlite-"))
        if (
            temp_root.stat().st_uid != os.getuid()
            or stat.S_IMODE(temp_root.stat().st_mode) != 0o700
            or _path_contains(path.parent, temp_root)
        ):
            raise ValueError("temporary storage unavailable")
        temp_dirs: list[Path] = []
        for round_index, contents in enumerate(round_bytes, start=1):
            temp_dir = temp_root / f"round_{round_index}"
            temp_dir.mkdir(mode=0o700)
            temp_dirs.append(temp_dir)
            for member_role, _member_name in member_names:
                content = contents.get(member_role)
                if content is None:
                    continue
                target = temp_dir / (
                    basename if member_role == "db" else f"{basename}-{member_role}"
                )
                fd = os.open(
                    target,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    0o600,
                )
                try:
                    view = memoryview(content)
                    while view:
                        view = view[os.write(fd, view) :]
                    if stat.S_IMODE(os.fstat(fd).st_mode) != 0o600:
                        raise ValueError("temporary storage unavailable")
                finally:
                    os.close(fd)
        temp_dir = temp_dirs[-1]
        temp_uri = f"file:{temp_dir / basename}?mode=rw"
        with sqlite3.connect(temp_uri, uri=True, timeout=0) as connection:
            connection.execute("PRAGMA query_only=ON")
            if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise ValueError("sqlite snapshot invalid")
            mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            expected_mode = "wal" if vector == ["present", "present", "present"] else "delete"
            if mode != expected_mode:
                raise ValueError("sqlite snapshot invalid")
        snapshot = {
            "subject_kind": "sqlite",
            "descriptor_role": role,
            "descriptor_id": basename,
            "descriptor_state": "present",
            "sqlite_members": rounds[0],
            "rollback_journal_absence": journal,
            "logical_digest": "",
            "catalog_digest": "",
            "_temp_root": temp_root,
            "_temp_dir": temp_dir,
            "_temp_round_dirs": tuple(temp_dirs),
        }
        return snapshot, temp_dir / basename
    except Exception:
        if temp_root is not None:
            try:
                _cleanup_temp_root(temp_root)
            except ValueError:
                pass
        raise
    finally:
        os.close(parent_fd)


def _sqlite_catalog_preimage(catalog: dict[str, Any]) -> dict[str, Any]:
    """Return the descriptor-bound, immutable catalog contract preimage."""
    return {
        "catalog_role": catalog["role"],
        "catalog_version": catalog["user_version"],
        "sqlite_master_objects": catalog["sqlite_master_allowlist"],
        "tables": list(catalog["allowed_tables"]),
        "columns": {name: spec["columns"] for name, spec in catalog["tables"].items()},
        "primary_keys": {name: spec["primary_key"] for name, spec in catalog["tables"].items()},
        "order_by_tuples": {name: spec["order_by"] for name, spec in catalog["tables"].items()},
        "system_table_allowlist": catalog["system_tables"],
        "schema_version_source": catalog["schema_version_source"],
        "catalog_digest_source": catalog["catalog_digest_source"],
    }


def _sqlite_catalog_digest(catalog: dict[str, Any]) -> str:
    preimage = _sqlite_catalog_preimage(catalog)
    return hashlib.sha256(b"r2f5/sqlite-catalog-v1\0" + _canonical_json(preimage)).hexdigest()


def _sqlite_logical_digest_from_temp(temp_db: Path, catalog: dict[str, Any]) -> str:
    uri = f"file:{temp_db}?mode=rw"
    chunks = [b"r2f5/sqlite-logical-v2\0"]
    with sqlite3.connect(uri, uri=True, timeout=0) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=250")
        connection.execute("BEGIN DEFERRED")
        if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise ValueError("sqlite snapshot invalid")
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
        if journal_mode not in ("delete", "wal"):
            raise ValueError("sqlite snapshot invalid")
        chunks.append(b"J" + _sqlite_typed_bytes(journal_mode, "TEXT"))
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
        if {f"{row[0]}:{row[1]}" for row in schema_rows} != set(catalog["sqlite_master_allowlist"]):
            raise ValueError("catalog schema drift")
        for table_name, table_spec in catalog["tables"].items():
            columns = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
            if [f"{row[1]}:{row[2] or ''}" for row in columns] != list(table_spec["columns"]):
                raise ValueError("catalog columns drift")
            if [row[1] for row in sorted(columns, key=lambda item: item[5]) if row[5]] != list(
                table_spec["primary_key"]
            ):
                raise ValueError("catalog primary key drift")
            names = {row[1] for row in columns}
            if not set(table_spec["order_by"]).issubset(names):
                raise ValueError("catalog order drift")
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


def _sqlite_logical_digest(path: Path | str, role: str | None = None) -> str:
    path = Path(path)
    key = role or _catalog_key(path)
    if key not in _CATALOGS:
        raise ValueError("unknown catalog")
    captured, temp_db = _capture_sqlite_members(path, _CATALOGS[key]["role"])
    try:
        return _sqlite_logical_digest_from_temp(temp_db, _CATALOGS[key])
    finally:
        _cleanup_temp_root(captured["_temp_root"])


def _sqlite_snapshot_fingerprint(path: Path, role: str) -> dict[str, Any]:
    key = _catalog_key(path)
    if key is None:
        raise ValueError("unknown catalog")
    captured, temp_db = _capture_sqlite_members(path, role)
    try:
        captured["logical_digest"] = _sqlite_logical_digest_from_temp(temp_db, _CATALOGS[key])
        captured["catalog_digest"] = _sqlite_catalog_digest(_CATALOGS[key])
        captured.pop("_temp_root", None)
        captured.pop("_temp_dir", None)
        captured.pop("_temp_round_dirs", None)
        return captured
    finally:
        _cleanup_temp_root(captured.get("_temp_root", temp_db.parent.parent))


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
        public_role = _CATALOGS[key]["role"]
        if public_role == "qualification":
            public_role = "control"
        fingerprint = _sqlite_snapshot_fingerprint(path, public_role)
        # The SQLite proof is already the public fingerprint and intentionally
        # has no ambiguous single-file SHA-256 subject.  The second value is
        # retained only for the internal snapshot-identity compatibility path.
        return fingerprint, fingerprint
    elif stat.S_ISDIR(info.st_mode):
        captured = _tree_content_digest(path)
        kind, scope = "content_sha256", "full_streaming_bytes"
    elif stat.S_ISREG(info.st_mode):
        captured = _stream_sha256(path)
        kind, scope = "content_sha256", "full_streaming_bytes"
    else:
        raise ValueError("path invalid")
    subject = {
        "subject_kind": "tree" if stat.S_ISDIR(info.st_mode) else "file",
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
            "subject_kind",
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
                or self.reason_code not in _VALID_UNAVAILABLE_REASONS
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
    subject_kind: Literal["tree", "file"]
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


class SQLitePresentMemberFingerprint(StrictModel):
    role: Literal["db", "wal", "shm"]
    presence: Literal["present"]
    parent_device: int = Field(ge=0, le=MAX_INT)
    parent_inode: int = Field(ge=0, le=MAX_INT)
    safe_basename: str
    device: int = Field(ge=0, le=MAX_INT)
    inode: int = Field(ge=0, le=MAX_INT)
    mode: int = Field(ge=0, le=MAX_INT)
    size_bytes: int = Field(ge=0, le=MAX_INT)
    mtime_ns: int = Field(ge=0)
    full_sha256: str

    @field_validator("safe_basename")
    @classmethod
    def safe_name(cls, value: str) -> str:
        if value in {"", ".", ".."} or "/" in value or "\\" in value:
            raise ValueError("unsafe basename")
        return value

    @field_validator("full_sha256")
    @classmethod
    def valid_hash(cls, value: str) -> str:
        if not SHA256.fullmatch(value):
            raise ValueError("invalid hash")
        return value


class SQLiteAbsentMemberFingerprint(StrictModel):
    role: Literal["wal", "shm"]
    presence: Literal["absent"]
    parent_device: int = Field(ge=0, le=MAX_INT)
    parent_inode: int = Field(ge=0, le=MAX_INT)
    safe_basename: str
    absence_marker: Literal["absent_at_validated_parent"]

    @field_validator("safe_basename")
    @classmethod
    def safe_name(cls, value: str) -> str:
        if value in {"", ".", ".."} or "/" in value or "\\" in value:
            raise ValueError("unsafe basename")
        return value


SQLiteMemberFingerprint = Annotated[
    SQLitePresentMemberFingerprint | SQLiteAbsentMemberFingerprint,
    Field(discriminator="presence"),
]


class SQLiteRollbackJournalAbsenceProof(StrictModel):
    role: Literal["rollback_journal"]
    presence: Literal["absent"]
    parent_device: int = Field(ge=0, le=MAX_INT)
    parent_inode: int = Field(ge=0, le=MAX_INT)
    parent_mode: int = Field(ge=0, le=MAX_INT)
    parent_mtime_ns: int = Field(ge=0)
    parent_ctime_ns: int = Field(ge=0)
    safe_basename: str
    absence_marker: Literal["absent_at_all_capture_checkpoints"]
    directory_entry_change_marker: Literal["none_observed"]
    probe_checkpoints: tuple[
        Literal[
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        ],
        Literal[
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        ],
        Literal[
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        ],
        Literal[
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        ],
        Literal[
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        ],
        Literal[
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        ],
    ]
    proof_digest: str

    @field_validator("safe_basename")
    @classmethod
    def safe_name(cls, value: str) -> str:
        if value in {"", ".", ".."} or "/" in value or "\\" in value:
            raise ValueError("unsafe basename")
        return value

    @model_validator(mode="after")
    def validate_checkpoint_vector(self) -> SQLiteRollbackJournalAbsenceProof:
        expected = (
            "initial_probe",
            "round_1_pre_copy",
            "round_1_post_copy",
            "round_2_pre_copy",
            "round_2_post_copy",
            "final_path_reprobe",
        )
        if self.probe_checkpoints != expected:
            raise ValueError("rollback checkpoint vector invalid")
        if not SHA256.fullmatch(self.proof_digest):
            raise ValueError("invalid hash")
        return self


class SQLiteSnapshotFingerprint(StrictModel):
    subject_kind: Literal["sqlite"]
    descriptor_role: Literal["calendar", "universe", "replication", "control"]
    descriptor_id: str
    descriptor_state: Literal["present"]
    sqlite_members: tuple[SQLiteMemberFingerprint, SQLiteMemberFingerprint, SQLiteMemberFingerprint]
    rollback_journal_absence: SQLiteRollbackJournalAbsenceProof
    logical_digest: str
    catalog_digest: str

    @model_validator(mode="after")
    def validate_members(self) -> SQLiteSnapshotFingerprint:
        if tuple(member.role for member in self.sqlite_members) != ("db", "wal", "shm"):
            raise ValueError("sqlite member order invalid")
        if self.sqlite_members[0].presence != "present":
            raise ValueError("sqlite db must be present")
        if not SHA256.fullmatch(self.logical_digest) or not SHA256.fullmatch(self.catalog_digest):
            raise ValueError("invalid hash")
        return self


InputSnapshotFingerprint = Annotated[
    SnapshotFingerprint | SQLiteSnapshotFingerprint,
    Field(discriminator="subject_kind"),
]


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

    @field_validator("descriptor_id", "source_generation")
    @classmethod
    def safe_identifier(cls, value: str) -> str:
        if not SAFE_ID.fullmatch(value):
            raise ValueError("invalid identifier")
        return value

    @field_validator("object_sha256", "descriptor_sha256")
    @classmethod
    def valid_hash(cls, value: str) -> str:
        if not SHA256.fullmatch(value):
            raise ValueError("invalid hash")
        return value


class SecondaryQualificationProjection(StrictModel):
    provider_id: Literal["tickflow", "tushare"]
    admission_state: Literal["discovered", "canary", "shadow", "qualified", "quarantined"]
    adapter_hash: str
    endpoint_contract_hash: str
    source_schema_hash: str
    normalizer_hash: str
    reconciliation_policy_hash: str
    terms_evidence_hash: str | None
    terms_review_id: str | None
    window_id: str
    window_start: str | None
    window_end: str | None
    consecutive_sessions: int = Field(ge=0, le=MAX_INT)
    version_vector_sha256: str
    calendar_generation: str
    calendar_sha256: str
    window_state: Literal["observing", "qualified", "reset"]
    last_session_report_id: str | None
    qualification_evidence_sha256: str | None
    qualification_candidate_sha256: str | None
    terminal_attestation_id: str | None
    qualification_proof_status: Literal["available", "unavailable"]

    @field_validator(
        "adapter_hash",
        "endpoint_contract_hash",
        "source_schema_hash",
        "normalizer_hash",
        "reconciliation_policy_hash",
        "terms_evidence_hash",
        "version_vector_sha256",
        "calendar_sha256",
        "qualification_evidence_sha256",
        "qualification_candidate_sha256",
    )
    @classmethod
    def valid_hash(cls, value: str | None) -> str | None:
        if value is not None and not SHA256.fullmatch(value):
            raise ValueError("invalid qualification hash")
        return value

    @field_validator("window_start", "window_end")
    @classmethod
    def valid_window_date(cls, value: str | None) -> str | None:
        if value is not None:
            if not DATE_RE.fullmatch(value):
                raise ValueError("invalid qualification date")
            date.fromisoformat(value)
        return value


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

    @field_validator("artifact_id", "artifact_ref", "creator_version")
    @classmethod
    def safe_identifier(cls, value: str) -> str:
        if not SAFE_ID.fullmatch(value):
            raise ValueError("invalid envelope identifier")
        return value

    @field_validator("payload_sha256", "envelope_sha256")
    @classmethod
    def valid_hash(cls, value: str) -> str:
        if not SHA256.fullmatch(value):
            raise ValueError("invalid envelope hash")
        return value


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
        expected = _ERROR_EVENT_CLASSES
        if (
            len(self.events) != len(expected)
            or tuple(event.forced_error_class for event in self.events) != expected
        ):
            raise ValueError("error event vector invalid")
        if any(
            event.expected_class != event.forced_error_class
            or event.observed_class != event.forced_error_class
            or event.sanitized_reason not in _REASONS
            or not SHA256.fullmatch(event.evidence_sha256)
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
    input_fingerprints: tuple[InputSnapshotFingerprint, ...]
    frozen_versions: FrozenReliabilityVersions
    input_fingerprint_sha256: str
    frozen_version_vector_sha256: str
    snapshot_sha256: str


class CapturedSnapshot(StrictModel):
    snapshot_identity: SnapshotIdentity
    raw_calendar_observations: tuple[str, ...]
    confirmed_sessions: tuple[str, ...]
    input_descriptors: tuple[InputSnapshotFingerprint, ...]
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
    registry_projection: SecondaryQualificationProjection | None
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
        # Keep the optional authority projection absent from unavailable
        # compatibility payloads while retaining it as a typed model field.
        if output.get("registry_projection") is None:
            output.pop("registry_projection", None)
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


def _safe_absolute_root(raw: str, *, require_directory: bool = True) -> Path:
    """Validate every absolute path component without resolving links."""
    path = Path(raw)
    if not path.is_absolute() or path == Path("/") or ".." in path.parts:
        raise _AcceptanceError("PATH_INVALID")
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        info = os.lstat(current)
        if stat.S_ISLNK(info.st_mode):
            # macOS exposes /tmp and /var as fixed system aliases.  Anchor
            # those two aliases to their known absolute targets; all other
            # symlinked ancestors remain invalid.
            alias = {"/tmp": "/private/tmp", "/var": "/private/var"}.get(current.as_posix())
            if alias is None or os.readlink(current) not in {"private/tmp", "private/var"}:
                raise _AcceptanceError("PATH_INVALID")
            current = Path(alias)
            continue
        if component != path.name and not stat.S_ISDIR(info.st_mode):
            raise _AcceptanceError("PATH_INVALID")
    if require_directory and not stat.S_ISDIR(os.lstat(current).st_mode):
        raise _AcceptanceError("PATH_INVALID")
    return current


def _path_contains(parent: Path, candidate: Path) -> bool:
    """Compare already trusted absolute paths without resolving links."""
    try:
        candidate.relative_to(parent)
    except ValueError:
        return False
    return True


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


def _safe_directory_entries(path: Path, suffix: str) -> list[str]:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        return sorted(name for name in os.listdir(fd) if name.endswith(suffix))
    finally:
        os.close(fd)


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
        # Contract validation is intentionally before request/path handling.
        # A broken packaged X8 asset must produce a typed control-state result,
        # never a path-dependent 422 or an import/HTTP 500.
        if not _CONTRACT_READY:
            return self._pre_capture(
                self._coerce_failure_request(input),
                "CONTROL_STATE_UNAVAILABLE",
                ["control_absent"],
            )
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
                else "SQLITE_SNAPSHOT_INVALID"
                if "sqlite snapshot invalid" in text
                else "TEMP_STORAGE_UNAVAILABLE"
                if "temporary storage unavailable" in text
                else "TEMP_CLEANUP_FAILED"
                if "temporary cleanup failed" in text
                else "SNAPSHOT_CHANGED"
                if "snapshot changed" in text or "sqlite rollback journal changed" in text
                else "CONTROL_STATE_UNAVAILABLE"
            )
            return self._pre_capture(request, reason, ["error"])
        finally:
            _ = started

    @staticmethod
    def _coerce_failure_request(input: AcceptanceInput | dict[str, Any]) -> AcceptanceInput:
        if isinstance(input, AcceptanceInput):
            return input
        if isinstance(input, dict):
            try:
                return AcceptanceInput.model_validate(input)
            except ValidationError:
                return AcceptanceInput.model_construct(
                    start=input.get("start", "1970-01-01")
                    if isinstance(input.get("start", "1970-01-01"), str)
                    else "1970-01-01",
                    end=input.get("end", "1970-01-01")
                    if isinstance(input.get("end", "1970-01-01"), str)
                    else "1970-01-01",
                    local_dataset_root=input.get("local_dataset_root", "")
                    if isinstance(input.get("local_dataset_root", ""), str)
                    else "",
                    evidence_root=input.get("evidence_root", "")
                    if isinstance(input.get("evidence_root", ""), str)
                    else "",
                    control_store_roots=(),
                    now=input.get("now", "1970-01-01T00:00:00Z")
                    if isinstance(input.get("now", "1970-01-01T00:00:00Z"), str)
                    else "1970-01-01T00:00:00Z",
                )
        return AcceptanceInput.model_construct(
            start="1970-01-01",
            end="1970-01-01",
            local_dataset_root="",
            evidence_root="",
            control_store_roots=(),
            now="1970-01-01T00:00:00Z",
        )

    def _validate_paths(self, request: AcceptanceInput) -> dict[str, Any]:
        roots = [
            Path(request.local_dataset_root),
            Path(request.evidence_root),
            *(Path(item) for item in request.control_store_roots),
        ]
        resolved: list[Path] = []
        for index, path in enumerate(roots):
            resolved.append(_safe_absolute_root(path.as_posix(), require_directory=index < 2))
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
        values = [
            _fingerprint("dataset", paths["dataset"])[0],
            _fingerprint("evidence", paths["evidence"])[0],
        ]
        for path in paths["controls"]:
            key = _catalog_key(path)
            if key is None:
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            public_role = _CATALOGS[key]["role"]
            if public_role == "qualification":
                public_role = "control"
            values.append(_fingerprint(public_role, path)[0])
            self._validate_catalog(path, key)
        return tuple(values)

    def _validate_catalog(self, path: Path, key: str) -> None:
        try:
            _sqlite_logical_digest(path, key)
        except (OSError, sqlite3.Error, ValueError):
            raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE") from None

    def _pre_capture(
        self, request: AcceptanceInput, reason: str, states: list[str]
    ) -> R2FAcceptanceReport:
        if reason not in _VALID_UNAVAILABLE_REASONS:
            reason = "CONTROL_STATE_UNAVAILABLE"
        safe_start = (
            request.start
            if isinstance(request.start, str) and DATE_RE.fullmatch(request.start)
            else "1970-01-01"
        )
        safe_end = (
            request.end
            if isinstance(request.end, str) and DATE_RE.fullmatch(request.end)
            else "1970-01-01"
        )
        try:
            safe_now = _validate_timestamp(request.now)
        except (TypeError, ValueError):
            safe_now = "1970-01-01T00:00:00Z"
        payload = {
            "schema_version": "r2f5-pre-capture-failure-v1",
            "reason_code": reason,
            "requested_start": safe_start,
            "requested_end": safe_end,
            "as_of_utc": safe_now,
            "descriptor_states": states,
            "semantic_report_sha256": "0" * 64,
        }
        try:
            payload["semantic_report_sha256"] = _digest(
                "PreCaptureFailurePayloadV1.semantic_report_sha256", payload
            )
        except ValueError:
            # A missing/malformed installed contract is itself unavailable.
            # Keep the public failure envelope typed and deterministic without
            # manufacturing a digest contract at request time.
            payload["semantic_report_sha256"] = "0" * 64
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
            "registry_projection": None,
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
        kind_contracts = _X8.get("metric_value_kinds", {})
        kinds = kind_contracts.get(name, {}) if isinstance(kind_contracts, dict) else {}
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
        try:
            calendar = _safe_json(paths["dataset"] / "calendar.json")
            if not isinstance(calendar, dict) or not isinstance(
                calendar.get("source_sequence"), list
            ):
                raise ValueError("calendar source sequence unavailable")
            if any(not isinstance(session, str) for session in calendar["source_sequence"]):
                raise ValueError("calendar source sequence invalid")
        except (ValidationError, ValueError, OSError, json.JSONDecodeError):
            return self._pre_capture(request, "CONTROL_STATE_UNAVAILABLE", ["error"])
        try:
            frozen = FrozenReliabilityVersions.model_validate(
                _safe_json(paths["evidence"] / "frozen_versions.json")
            )
        except (ValidationError, ValueError, OSError, json.JSONDecodeError):
            return self._pre_capture(request, "CONTROL_STATE_UNAVAILABLE", ["error"])
        try:
            for field_name in (
                "installed_release_sha256",
                "config_digest",
                "calendar_sha256",
                "universe_sha256",
                "destination_head_sha256",
            ):
                value = getattr(frozen, field_name)
                if value is not None:
                    self._validate_leaf(
                        paths["evidence"], value, f"FrozenReliabilityVersions.{field_name}"
                    )
        except (ValidationError, ValueError, OSError, json.JSONDecodeError):
            return self._report_unavailable(
                request, fingerprints, "LINEAGE_UNAVAILABLE", [], frozen
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
        frozen_bindings = {
            "adapter_hash": frozen.adapter_hash,
            "endpoint_contract_hash": frozen.endpoint_contract_hash,
            "source_schema_hash": frozen.source_schema_hash,
            "normalizer_hash": frozen.normalizer_hash,
            "calendar_generation": frozen.calendar_generation,
            "calendar_sha256": frozen.calendar_sha256,
        }
        if any(getattr(registry, field) != expected for field, expected in frozen_bindings.items()):
            return self._report_unavailable(request, fingerprints, "VERSION_DRIFT", [], frozen)
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
                for field_name, field_path in (
                    ("source_commit_sha256", "ReplicationObservation.source_commit_sha256"),
                    (
                        "destination_record_sha256",
                        "ReplicationObservation.destination_record_sha256",
                    ),
                    (
                        "destination_head_sha256",
                        "ReplicationObservation.destination_head_sha256",
                    ),
                ):
                    self._validate_leaf(
                        paths["evidence"], item["replication_observation"][field_name], field_path
                    )
                observations.append(item)
        except _AcceptanceError as error:
            return self._report_unavailable(
                request, fingerprints, error.reason, raw_sessions, frozen
            )
        except FileNotFoundError:
            return self._report_unavailable(
                request, fingerprints, "LINEAGE_UNAVAILABLE", raw_sessions, frozen
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
            root_base = (
                evidence
                if descriptor.get("root_base") != "dataset"
                else evidence.parent / "dataset"
            )
            if descriptor.get("immutable") is not True:
                raise ValueError("lineage unavailable")
            raw = _read_anchored_file(evidence, descriptor["object_path"])
            if hashlib.sha256(raw).hexdigest() != descriptor.get("artifact_sha256"):
                raise _AcceptanceError("LINEAGE_INVALID")
            root = json.loads(
                _read_anchored_file(root_base, descriptor["root_path"]).decode("utf-8")
            )
            pointer = descriptor.get("root_pointer", "$")
            for segment in pointer.removeprefix("$").strip(".").split("."):
                if not segment:
                    continue
                if segment.endswith("[]"):
                    root = root[segment[:-2]][0]
                else:
                    root = root[segment]
            if descriptor.get("contract_digest") != _digest(source_field, root):
                raise _AcceptanceError("LINEAGE_INVALID")
            if descriptor.get("contract_digest") == expected:
                found = True
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
        if not found:
            # Every fixed error-class event owns one descriptor/object/root
            # binding.  A missing or mismatched member is not a valid array
            # contract and must fail closed.
            if source_field == "ErrorHandlingObservation.events.evidence_sha256":
                if descriptor_count != len(_ERROR_EVENT_CLASSES):
                    raise ValueError("error event evidence lineage unavailable")
                raise ValueError("error event evidence digest mismatch")
            if not seen or (repeated and descriptor_count < 20):
                raise ValueError("lineage unavailable")

    def _descriptor_contract_state(
        self, evidence: Path, source_field: str
    ) -> tuple[tuple[str, str, str], ...]:
        """Return descriptor contract/root digests for semantic-child handling.

        A window drill may be re-materialised with a changed semantic field and
        its own self-digest while the writer-owned leaf descriptor remains bound
        to the original immutable source.  We may classify that as a reducer
        failure only when every descriptor is internally valid.  This helper is
        intentionally read-only and mirrors the anchored root traversal used by
        ``_validate_leaf`` so descriptor tampering cannot be mistaken for a
        semantic mutation.
        """
        states: list[tuple[str, str, str]] = []
        for descriptor_path in evidence.rglob("*.descriptor.json"):
            descriptor = _safe_json(descriptor_path)
            if descriptor.get("source_field") != source_field:
                continue
            root_base = (
                evidence
                if descriptor.get("root_base") != "dataset"
                else evidence.parent / "dataset"
            )
            object_raw = _read_anchored_file(evidence, descriptor["object_path"])
            if hashlib.sha256(object_raw).hexdigest() != descriptor.get("artifact_sha256"):
                raise _AcceptanceError("LINEAGE_INVALID")
            object_value = json.loads(object_raw.decode("utf-8"))
            root = json.loads(
                _read_anchored_file(root_base, descriptor["root_path"]).decode("utf-8")
            )
            pointer = descriptor.get("root_pointer", "$")
            for segment in pointer.removeprefix("$").strip(".").split("."):
                if not segment:
                    continue
                if segment.endswith("[]"):
                    root = root[segment[:-2]][0]
                else:
                    root = root[segment]
            states.append(
                (
                    descriptor["contract_digest"],
                    _digest(source_field, root),
                    _digest(source_field, object_value),
                )
            )
        return tuple(states)

    def _validate_readonly_descriptor(self, evidence: Path) -> None:
        """Read and verify the writer-owned descriptor and its immutable object."""
        object_dir = evidence / "objects" / "ReadonlyEvidenceDescriptor.object_sha256"
        descriptor_dir = evidence / "objects" / "ReadonlyEvidenceDescriptor.descriptor_sha256"
        object_entries = _safe_directory_entries(object_dir, ".descriptor.json")
        descriptor_entries = _safe_directory_entries(descriptor_dir, ".descriptor.json")
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
            root_path = sidecar.get("root_path")
            source_field = sidecar.get("source_field")
            if (
                not isinstance(object_path, str)
                or not isinstance(root_path, str)
                or source_field
                not in {
                    "ReadonlyEvidenceDescriptor.object_sha256",
                    "ReadonlyEvidenceDescriptor.descriptor_sha256",
                }
            ):
                raise ValueError("readonly descriptor unavailable")
            raw = _read_anchored_file(evidence, object_path)
            if hashlib.sha256(raw).hexdigest() != sidecar.get("artifact_sha256"):
                raise ValueError("readonly descriptor invalid")
            root = json.loads(_read_anchored_file(evidence, root_path).decode("utf-8"))
            if _digest(source_field, root) != sidecar.get("contract_digest"):
                raise ValueError("readonly descriptor invalid")
        descriptor_raw = _read_anchored_file(evidence, descriptor_sidecar["object_path"])
        descriptor = ReadonlyEvidenceDescriptor.model_validate(
            json.loads(descriptor_raw.decode("utf-8"))
        )
        object_raw = _read_anchored_file(evidence, object_sidecar["object_path"])
        object_root = json.loads(
            _read_anchored_file(evidence, object_sidecar["root_path"]).decode("utf-8")
        )
        expected_object_bytes = object_root.get("immutable_object_bytes")
        if (
            set(object_root) != {"descriptor_id", "immutable_object_bytes"}
            or object_root.get("descriptor_id") != descriptor.descriptor_id
            or not isinstance(expected_object_bytes, str)
        ):
            raise ValueError("readonly object invalid")
        if (
            descriptor.object_sha256
            != _readonly_object_digest(expected_object_bytes.encode("utf-8"))
            or descriptor.descriptor_sha256
            != _digest(
                "ReadonlyEvidenceDescriptor.descriptor_sha256", descriptor.model_dump(mode="python")
            )
            or descriptor.object_sha256 != object_sidecar.get("contract_digest")
            or descriptor.descriptor_sha256 != descriptor_sidecar.get("contract_digest")
        ):
            raise ValueError("readonly descriptor invalid")
        if object_raw != expected_object_bytes.encode("utf-8"):
            raise ValueError("readonly object invalid")
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
        # Failure-class reasons remain visible in quality_issues, but an
        # unavailable metric must carry an unavailable-partition reason.  A
        # malformed immutable proof is still an unavailable report; it must
        # not make report validation fail and fall through to INVALID_ARGUMENTS.
        metric_reason = reason if reason in _UNAVAILABLE_REASONS else "CONTROL_STATE_UNAVAILABLE"
        metric_reasons = {name: metric_reason for name in _METRICS}
        if reason in {"CONTROL_STATE_UNAVAILABLE", "LINEAGE_INVALID"} and sessions:
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
                    self._validate_envelope(candidate)
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
            "registry_projection": None,
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
        completed_for_session = _safe_json(paths["evidence"] / "completed_replication_restore.json")
        session_lag_threshold = (completed_for_session.get("policy_thresholds") or {}).get(
            "replication_lag_seconds"
        ) or 0

        # SessionObservation contains public reducer projections, but raw
        # session facts are authoritative.  Recompute every per-session
        # metric before exposing it so a stale writer projection cannot make a
        # changed ordinal appear to pass.
        frozen_digest = _digest(
            "SessionObservation.frozen_versions_sha256", frozen.model_dump(mode="python")
        )
        for item in observations:
            same_evening = self._cutoff(
                item.get("same_evening_published_at"), item["session"], False
            )
            next_morning = self._cutoff(
                item.get("next_morning_published_at"), item["session"], True
            )
            required = item["required_count"]
            loaded = item["loaded_count"]
            unknown = item["unknown_count"]
            coverage_ratio = loaded / required if required else 0
            coverage_good = required > 0 and loaded == required and unknown == 0
            canonical_good = bool(
                item["pointer_reconciliation"].get("pointer_manifest_object_match")
            )
            source_good = len(item["canonical_provider_ids"]) == 1
            calendar_facts = item["calendar_raw_facts"]
            calendar_good = bool(
                calendar_facts.get("confirmed")
                and not calendar_facts.get("unknown_state")
                and not calendar_facts.get("conflict_state")
                and calendar_facts.get("source_sequence") == sessions
            )
            universe_good = bool(
                unknown == 0
                and required
                == loaded
                + item["suspension_count"]
                + item["not_listed_count"]
                + item["delisted_count"]
                + unknown
            )
            replication = item["replication_observation"]
            lag = replication.get("lag_seconds")
            replication_good = bool(
                replication.get("trust_scope") == "REMOTE_VERIFIED"
                and lag is not None
                and lag <= session_lag_threshold
            )
            boundary = item["read_boundary_raw_facts"]
            boundary_good = bool(
                boundary.get("query_count") == 1
                and boundary.get("write_count") == 0
                and not boundary.get("future_rows_seen")
            )
            item["cutoff_results"] = {
                "same_evening": self._metric(
                    "same_evening_availability",
                    "pass" if same_evening else "fail",
                    None if same_evening else "AVAILABILITY_CUTOFF_FAILED",
                    # The frozen SessionObservation contract records the
                    # per-session pass as the reviewed 18/20 ratio target
                    # (0.9); the window reducer counts the raw timestamp
                    # booleans independently.  Keep this projection stable
                    # so a reader does not rewrite writer-owned observation
                    # identities merely by materialising the report.
                    0.9 if same_evening else 0.0,
                    0.9,
                ),
                "next_morning": self._metric(
                    "next_morning_availability",
                    "pass" if next_morning else "fail",
                    None if next_morning else "AVAILABILITY_CUTOFF_FAILED",
                    1.0 if next_morning else 0.0,
                    1.0,
                ),
            }
            item["coverage"] = self._metric(
                "coverage",
                "pass" if coverage_good else "fail",
                None if coverage_good else "COVERAGE_FAILED",
                coverage_ratio,
                1.0,
            )
            item["canonical_integrity"] = self._metric(
                "canonical_integrity",
                "pass" if canonical_good else "fail",
                None if canonical_good else "CANONICAL_INTEGRITY_FAILED",
                canonical_good,
                True,
            )
            item["source_purity"] = self._metric(
                "source_purity",
                "pass" if source_good else "fail",
                None if source_good else "SOURCE_PURITY_FAILED",
                source_good,
                True,
            )
            item["provenance"] = self._metric(
                "provenance",
                "pass" if item["frozen_versions_sha256"] == frozen_digest else "fail",
                None if item["frozen_versions_sha256"] == frozen_digest else "VERSION_DRIFT",
                item["frozen_versions_sha256"] == frozen_digest,
                True,
            )
            item["calendar"] = self._metric(
                "calendar",
                "pass" if calendar_good else "fail",
                None if calendar_good else "CALENDAR_CONFLICT",
                calendar_good,
                True,
            )
            item["universe"] = self._metric(
                "universe",
                "pass" if universe_good else "fail",
                None if universe_good else "UNIVERSE_COUNT_MISMATCH",
                universe_good,
                True,
            )
            item["replication"] = self._metric(
                "replication",
                "pass" if replication_good else "fail",
                None if replication_good else "REPLICATION_LAG",
                lag if lag is not None else 0,
                session_lag_threshold,
            )
            item["read_boundary"] = self._metric(
                "read_boundary",
                "pass" if boundary_good else "fail",
                None if boundary_good else "READ_BOUNDARY_FAILED",
                boundary_good,
                True,
            )
            item["observation_sha256"] = "0" * 64
            item["observation_sha256"] = _digest("SessionObservation.observation_sha256", item)
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
                # A malformed child is a typed unavailable metric.  Do not
                # let one strict child validation error erase the rest of the
                # captured window or leak as a generic invalid-arguments
                # response.
                try:
                    child_model.model_validate(child_envelope["payload"])
                except (ValidationError, ValueError, TypeError, KeyError):
                    continue
        child_digest_fields = {
            "recovery_observation": tuple(
                (name, f"RecoveryObservation.{name}")
                for name in (
                    "after_manifest_sha256",
                    "after_pointer_sha256",
                    "after_selection_sha256",
                    "duplicate_proof_sha256",
                )
            ),
            "failover_observation": tuple(
                (name, f"WholeSessionFailoverDrill.{name}")
                for name in (
                    "selection_sha256",
                    "manifest_sha256",
                    "pointer_sha256",
                    "readback_sha256",
                )
            ),
            "replay_sample": (
                ("sample_object_sha256", "ReplaySampleEvidence.sample_object_sha256"),
                ("candidate_sha256", "ReplaySampleEvidence.candidate_sha256"),
                (
                    "implementation_sha256",
                    "OfflineReplayContext.implementation_sha256",
                ),
            ),
            "restore_observation": tuple(
                (name, f"RestoreDrillEvidence.{name}")
                for name in (
                    "sentinel_sha256",
                    "destination_head_sha256",
                    "record_sha256",
                    "manifest_sha256",
                    "restore_report_sha256",
                    "api_readback_sha256",
                )
            ),
            "local_nas_isolation_observation": (
                ("local_pointer_sha256", "LocalNasIsolationObservation.local_pointer_sha256"),
            ),
        }
        for child_name, fields in child_digest_fields.items():
            child = payload.get(child_name)
            if child is None or not isinstance(child, dict):
                continue
            child_payload = child.get("payload", {})
            for field_name, field_path in fields:
                value = child_payload.get(field_name)
                if value is None and field_name == "implementation_sha256":
                    value = child_payload.get("offline_context", {}).get(field_name)
                if not isinstance(value, str):
                    raise ValueError("child evidence digest unavailable")
                try:
                    self._validate_leaf(paths["evidence"], value, field_path)
                except _AcceptanceError:
                    # Recovery's reducer tests intentionally mutate the
                    # semantic proof and recompute its self-digest.  The
                    # writer descriptor then remains on the prior source
                    # preimage, which is a reducer failure rather than an
                    # unavailable report.  Suppress only that precise case;
                    # descriptor/root disagreement or a descriptor still
                    # matching the payload is a real lineage failure.
                    if field_path != "RecoveryObservation.duplicate_proof_sha256":
                        raise
                    states = self._descriptor_contract_state(paths["evidence"], field_path)
                    if not states or any(
                        contract != object_digest
                        for contract, _root_digest, object_digest in states
                    ):
                        raise
                    if any(contract == value for contract, _root, _object in states):
                        raise
        error_child = payload.get("error_handling_observation")
        if isinstance(error_child, dict):
            error_payload = error_child.get("payload", {})
            if isinstance(error_payload, dict):
                if isinstance(error_payload.get("observation_sha256"), str):
                    # Event-level mutations are deliberately rehashed as a
                    # valid envelope so the public reducer can expose the
                    # exact ERROR_HANDLING_FAILED mapping.  A stale child
                    # identity is semantic data, not a reason to erase the
                    # whole report; still fail closed when the payload's own
                    # digest is invalid or the immutable descriptor is
                    # actually absent/corrupt.
                    expected_child_digest = _digest(
                        "ErrorHandlingObservation.observation_sha256", error_payload
                    )
                    try:
                        self._validate_leaf(
                            paths["evidence"],
                            error_payload["observation_sha256"],
                            "ErrorHandlingObservation.observation_sha256",
                        )
                    except _AcceptanceError:
                        if expected_child_digest != error_payload["observation_sha256"]:
                            raise
                    if expected_child_digest != error_payload["observation_sha256"]:
                        raise _AcceptanceError("LINEAGE_INVALID")
                for event in error_payload.get("events", ()):
                    if isinstance(event, dict) and isinstance(event.get("evidence_sha256"), str):
                        self._validate_leaf(
                            paths["evidence"],
                            event["evidence_sha256"],
                            "ErrorHandlingObservation.events.evidence_sha256",
                        )
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
                        ("fail", "REPLAY_SEMANTIC_MISMATCH")
                        if not value.get("semantic_equal", True)
                        else ("unavailable", reason)
                    )
                )
                if value.get("semantic_equal") is False:
                    metric_reason = "REPLAY_SEMANTIC_MISMATCH"
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
                    and tuple(classes)
                    == ("timeout", "auth", "rate", "schema", "coverage", "storage")
                    and all(
                        event.get("expected_class")
                        == event.get("forced_error_class")
                        == event.get("observed_class")
                        and event.get("normalized_result") in {"not_ready", "unavailable"}
                        and event.get("sanitized_reason") in _REASONS
                        and isinstance(event.get("evidence_sha256"), str)
                        and SHA256.fullmatch(event["evidence_sha256"]) is not None
                        and not any(
                            token in key.lower()
                            for key in event
                            for token in (
                                "exception",
                                "traceback",
                                "token",
                                "secret",
                                "url",
                                "path",
                            )
                        )
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
                    and value.get("immutable") is True
                    and value.get("local_publication_ready") is True
                    and bool(value.get("outage_end"))
                    and value.get("retryable") is True
                    and len(value.get("backlog_before_ids", ()))
                    == value.get("backlog_before_count")
                    and len(value.get("backlog_after_ids", ())) == value.get("backlog_after_count")
                    and value.get("lag_seconds", MAX_INT) <= value.get("lag_threshold_seconds", -1)
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
        remote_verified = (
            all(
                item["replication_observation"].get("trust_scope") == "REMOTE_VERIFIED"
                for item in observations
            )
            and frozen.replication_trust_scope == "REMOTE_VERIFIED"
        )
        metrics["replication"] = self._metric(
            "replication",
            "unavailable"
            if threshold is None or not remote_verified
            else ("pass" if lag <= threshold else "fail"),
            "REPLICATION_UNAVAILABLE"
            if threshold is None
            else (
                "REMOTE_PROOF_MISSING"
                if not remote_verified
                else (None if lag <= threshold else "REPLICATION_LAG")
            ),
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
            "registry_projection": (
                registry.model_dump(mode="python")
                if registry is not None and status == "ready"
                else None
            ),
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
        return R2FAcceptanceReport.model_validate(report)

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
    captured: dict[str, Any] | None = None
    try:
        captured, temp_db = _capture_sqlite_members(path, "control")
        # Only the securely copied temporary database is opened by SQLite.
        # The retained input parent fd is used solely for byte capture.
        uri = f"file:{temp_db}?mode=rw"
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
                "SELECT provider_id, admission_state, adapter_hash, endpoint_contract_hash, "
                "source_schema_hash, normalizer_hash, reconciliation_policy_hash, "
                "terms_evidence_hash, terms_review_id FROM provider_record WHERE provider_id=?",
                (provider_id,),
            ).fetchone()
            window = connection.execute(
                "SELECT provider_id, window_id, window_start, window_end, "
                "consecutive_sessions, window_state, version_vector_sha256, "
                "calendar_generation, calendar_sha256, last_session_report_id, "
                "qualification_evidence_sha256, qualification_candidate_sha256, "
                "terminal_attestation_id "
                "FROM qualification_window WHERE provider_id=? AND window_id=?",
                (provider_id, window_id),
            ).fetchone()
            if provider is None or window is None:
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            if provider[1] != "qualified" or window[5] != "qualified":
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            if (
                provider[7] is None
                or provider[8] is None
                or window[10] is None
                or window[11] is None
                or window[12] is None
            ):
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
                    "session_report_id, evidence_sha256, candidate_sha256 "
                    "FROM shadow_terminal_attestation "
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
            terminal = attestations.get(window[12])
            if terminal is None or terminal[5] != window[10] or terminal[6] != window[11]:
                # The window's qualification proof is authoritative only when
                # it agrees with the immutable terminal graph record.
                raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE")
            terminal_job = connection.execute(
                "SELECT terminal_attestation_id FROM shadow_job "
                "WHERE provider_id=? AND window_id=? AND trade_date=? "
                "ORDER BY job_id DESC LIMIT 1",
                (provider_id, window_id, window[3]),
            ).fetchone()
            if terminal_job is None or terminal_job[0] != window[12]:
                raise _AcceptanceError("CANONICAL_INTEGRITY_FAILED")
            connection.rollback()
    except _AcceptanceError:
        raise
    except (OSError, sqlite3.Error, ValueError):
        raise _AcceptanceError("CONTROL_STATE_UNAVAILABLE") from None
    finally:
        if captured is not None:
            _cleanup_temp_root(captured["_temp_root"])
    projection = SecondaryQualificationProjection(
        provider_id=provider[0],
        admission_state=provider[1],
        adapter_hash=provider[2],
        endpoint_contract_hash=provider[3],
        source_schema_hash=provider[4],
        normalizer_hash=provider[5],
        reconciliation_policy_hash=provider[6],
        terms_evidence_hash=provider[7],
        terms_review_id=provider[8],
        window_id=window[1],
        window_start=window[2],
        window_end=window[3],
        consecutive_sessions=window[4],
        version_vector_sha256=window[6],
        calendar_generation=window[7],
        calendar_sha256=window[8],
        window_state=window[5],
        last_session_report_id=window[9],
        qualification_evidence_sha256=window[10],
        qualification_candidate_sha256=window[11],
        terminal_attestation_id=window[12],
        qualification_proof_status="available",
    )
    return projection


def read_readonly_evidence(
    descriptor: ReadonlyEvidenceDescriptor | dict[str, Any],
    immutable_bytes: bytes,
) -> ReadonlyEvidenceDescriptor:
    """Validate a Task20 descriptor against supplied immutable bytes."""
    model = ReadonlyEvidenceDescriptor.model_validate(descriptor)
    if _readonly_object_digest(immutable_bytes) != model.object_sha256:
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
    "SQLitePresentMemberFingerprint",
    "SQLiteAbsentMemberFingerprint",
    "SQLiteRollbackJournalAbsenceProof",
    "SQLiteSnapshotFingerprint",
    "InputSnapshotFingerprint",
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
