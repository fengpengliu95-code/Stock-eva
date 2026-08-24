"""Immutable, local provider evidence and replay boundary (R2-F2 Task 8).

This module deliberately owns the storage-facing projections.  It accepts only
the frozen provider contracts from ``providers.base`` and never stores a
provider response body, credentials, URL or exception text.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
import tempfile
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator

from backend.app.market.providers.base import (
    R2F2_ADAPTER_VERSION,
    R2F2_ENDPOINT_CONTRACT_VERSION,
    EndpointContractSummary,
    EvidenceObjectKind,
    ExpectedLogicalRequestPlan,
    InstrumentRole,
    PaginationPolicy,
    ProviderEndpoint,
    ProviderId,
    ProviderRawBatch,
    RequestCompletion,
    RequestRole,
    SafeFailureClass,
    SafeIdentifier,
    SafeRelativePath,
    SafeSha256,
    SafeSymbol,
    TransportLineageRef,
    TransportObservationAggregate,
    validate_safe_relative_path,
)

MAX_OBJECT_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_ROWS = 10_000_000
FACTOR_SCHEMA = (
    "symbol",
    "trade_date",
    "fore_adjust_factor",
    "back_adjust_factor",
    "evidence_kind",
    "evidence_effective_date",
    "evidence_observed_on",
    "source_row_hash",
    "observed_at",
)
FACTOR_RESOLUTION_SCHEMA = ("binding",)


class EvidenceError(RuntimeError):
    """Sanitized evidence failure; ``failure_class`` is safe to expose."""

    def __init__(self, message: str, failure_class: str = "storage") -> None:
        super().__init__(message)
        self.failure_class = failure_class


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _factor_value_semantic_hash(row: dict[str, Any]) -> str:
    """Hash the exact value selected by the BaoStock normalization contract."""
    return _digest(
        {
            "record_key": f"{row['symbol']}.{row['trade_date']}",
            "back_adjust_factor": row["back_adjust_factor"],
        }
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class _Immutable(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    def model_copy(self, *, update: dict[str, Any] | None = None, deep: bool = False):
        values = self.model_dump(mode="python")
        if update:
            values.update(update)
        return type(self).model_validate(values)


class FactorCacheSnapshotRecord(_Immutable):
    symbol: SafeSymbol
    trade_date: date
    fore_adjust_factor: float
    back_adjust_factor: float | None
    evidence_kind: str
    evidence_effective_date: date
    evidence_observed_on: date | None
    source_row_hash: SafeSha256
    observed_at: datetime
    row_fingerprint: SafeSha256

    @model_validator(mode="after")
    def validate_time(self) -> FactorCacheSnapshotRecord:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        return self


class FactorCacheSnapshotRecords(_Immutable):
    cache_schema: tuple[str, ...]
    rows: tuple[FactorCacheSnapshotRecord, ...]
    row_count: int = Field(ge=0, le=MAX_ROWS)
    records_sha256: SafeSha256
    before_fingerprint: SafeSha256
    after_fingerprint: SafeSha256

    @model_validator(mode="after")
    def validate_records(self) -> FactorCacheSnapshotRecords:
        if self.cache_schema != FACTOR_SCHEMA:
            raise ValueError("factor snapshot schema mismatch")
        if self.row_count != len(self.rows):
            raise ValueError("factor snapshot row count mismatch")
        if self.records_sha256 != _digest([row.model_dump(mode="json") for row in self.rows]):
            raise ValueError("factor snapshot records hash mismatch")
        if self.before_fingerprint != self.after_fingerprint:
            raise ValueError("factor snapshot changed during capture")
        symbols = tuple(row.symbol for row in self.rows)
        if symbols != tuple(sorted(set(symbols))):
            raise ValueError("factor snapshot rows must be ordered and unique")
        return self


def factor_snapshot_records_from_cache(
    rows: tuple[dict[str, object], ...] | list[dict[str, object]],
    *,
    before_fingerprint: str,
    after_fingerprint: str,
) -> FactorCacheSnapshotRecords:
    """Convert the strict factor-cache projection into its frozen evidence model."""
    records = []
    for item in rows:
        source_values = (
            item["symbol"],
            item["trade_date"],
            item["fore_adjust_factor"],
            item["back_adjust_factor"],
            item["evidence_kind"],
            item["evidence_effective_date"],
            item["evidence_observed_on"],
            item["source_row_hash"],
            item["observed_at"],
        )
        expected_fingerprint = _sha256_bytes(
            json.dumps(
                list(source_values),
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        )
        if str(item["row_fingerprint"]) != expected_fingerprint:
            raise EvidenceError("factor row fingerprint mismatch", "EVIDENCE_HASH_MISMATCH")
        records.append(
            FactorCacheSnapshotRecord(
                symbol=str(item["symbol"]),
                trade_date=date.fromisoformat(str(item["trade_date"])),
                fore_adjust_factor=float(item["fore_adjust_factor"]),
                back_adjust_factor=(
                    None
                    if item["back_adjust_factor"] is None
                    else float(item["back_adjust_factor"])
                ),
                evidence_kind=str(item["evidence_kind"]),
                evidence_effective_date=date.fromisoformat(str(item["evidence_effective_date"])),
                evidence_observed_on=(
                    None
                    if item["evidence_observed_on"] is None
                    else date.fromisoformat(str(item["evidence_observed_on"]))
                ),
                source_row_hash=str(item["source_row_hash"]),
                observed_at=datetime.fromisoformat(str(item["observed_at"])),
                row_fingerprint=str(item["row_fingerprint"]),
            )
        )
    records = sorted(records, key=lambda item: item.symbol)
    return FactorCacheSnapshotRecords(
        cache_schema=FACTOR_SCHEMA,
        rows=tuple(records),
        row_count=len(records),
        records_sha256=_digest([item.model_dump(mode="json") for item in records]),
        before_fingerprint=before_fingerprint,
        after_fingerprint=after_fingerprint,
    )


class FactorCacheSnapshotManifest(_Immutable):
    capture_id: SafeIdentifier
    object_id: SafeIdentifier
    relative_path: SafeRelativePath
    object_sha256: SafeSha256
    byte_count: int = Field(ge=0, le=MAX_OBJECT_BYTES)
    row_count: int = Field(ge=0, le=MAX_ROWS)
    schema_variant: str
    schema_hash: SafeSha256
    records_sha256: SafeSha256
    before_fingerprint: SafeSha256
    after_fingerprint: SafeSha256


class PublishedFactorCacheSnapshot(_Immutable):
    manifest: FactorCacheSnapshotManifest
    reader_identity: SafeSha256


class FactorResolutionSnapshotManifest(_Immutable):
    capture_id: SafeIdentifier
    object_id: SafeIdentifier
    relative_path: SafeRelativePath
    object_sha256: SafeSha256
    byte_count: int = Field(ge=0, le=MAX_OBJECT_BYTES)
    binding_count: int = Field(ge=0, le=MAX_ROWS)
    schema_variant: Literal["factor-resolution-snapshot.v1"]
    schema_hash: SafeSha256
    bindings_sha256: SafeSha256


class PublishedFactorResolutionSnapshot(_Immutable):
    manifest: FactorResolutionSnapshotManifest
    reader_identity: SafeSha256


class LiveFactorResolution(_Immutable):
    descriptor_id: SafeIdentifier
    object_sha256: SafeSha256
    endpoint: Literal[ProviderEndpoint.DAILY_FACTOR, ProviderEndpoint.ADJUST_FACTOR]
    row_key: SafeIdentifier
    schema_variant: str


class CacheFactorResolution(_Immutable):
    cache_object_id: SafeIdentifier
    cache_object_sha256: SafeSha256
    record_key: SafeIdentifier


class FactorResolutionBinding(_Immutable):
    plan_ordinal: int = Field(ge=0, le=4096)
    symbol: SafeSymbol
    trade_date: date
    selected_kind: Literal["factor_cache_snapshot", "daily_factor", "adjust_factor"]
    selected_value_semantic_hash: SafeSha256
    live: LiveFactorResolution | None = None
    cache: CacheFactorResolution | None = None
    resolution_sha256: SafeSha256

    @model_validator(mode="after")
    def validate_source(self) -> FactorResolutionBinding:
        if self.selected_kind == "factor_cache_snapshot":
            if self.cache is None or self.live is not None:
                raise ValueError("cache factor resolution requires cache only")
        elif self.live is None or self.cache is not None:
            raise ValueError("live factor resolution requires live only")
        values = self.model_dump(mode="json")
        values.pop("resolution_sha256", None)
        if self.resolution_sha256 != _digest(values):
            raise ValueError("factor resolution hash mismatch")
        return self


class EvidenceObjectDescriptor(_Immutable):
    object_id: SafeIdentifier
    object_kind: EvidenceObjectKind
    relative_path: SafeRelativePath
    sha256: SafeSha256
    schema_hash: SafeSha256
    row_count: int = Field(ge=0, le=MAX_ROWS)
    byte_count: int = Field(ge=0, le=MAX_OBJECT_BYTES)
    provider_id: ProviderId
    universe_id: SafeIdentifier
    refresh_id: SafeIdentifier
    capture_id: SafeIdentifier | None = None
    provider_session_id: SafeIdentifier | None = None
    root_request_id: SafeIdentifier | None = None
    page_request_id: SafeIdentifier | None = None
    endpoint: ProviderEndpoint | None = None
    request_role: RequestRole | None = None
    instrument_role: InstrumentRole | None = None
    shard_id: SafeIdentifier | None = None
    plan_ordinal: int | None = Field(default=None, ge=0, le=4096)
    attempt: int | None = Field(default=None, ge=1, le=20)
    page: int | None = Field(default=None, ge=1, le=16384)
    fields: tuple[str, ...] = ()
    adapter_version: str = R2F2_ADAPTER_VERSION
    endpoint_contract_version: str = R2F2_ENDPOINT_CONTRACT_VERSION
    schema_variant: str
    source_schema: str
    units: tuple[tuple[str, str], ...] = ()
    date_semantics: Literal["explicit_trade_date"] = "explicit_trade_date"
    pagination_policy: PaginationPolicy = PaginationPolicy.PROVIDER_TERMINAL
    normalization_clock_utc: datetime
    transport_observation_digest: SafeSha256 | None = None
    factor_snapshot_provenance_hash: SafeSha256 | None = None
    factor_resolution_provenance_hash: SafeSha256 | None = None

    @model_validator(mode="after")
    def validate_kind(self) -> EvidenceObjectDescriptor:
        if not self.fields or any(not str(field).strip() for field in self.fields):
            raise ValueError("descriptor fields are required")
        if not self.schema_variant.strip() or not self.source_schema.strip():
            raise ValueError("descriptor schema identity is required")
        if any(not str(name).strip() or not str(unit).strip() for name, unit in self.units):
            raise ValueError("descriptor units are invalid")
        if any(name not in self.fields for name, _ in self.units):
            raise ValueError("descriptor units do not match fields")
        page_fields = (
            self.provider_session_id,
            self.root_request_id,
            self.page_request_id,
            self.endpoint,
            self.request_role,
            self.instrument_role,
            self.shard_id,
            self.plan_ordinal,
            self.attempt,
            self.page,
            self.transport_observation_digest,
        )
        if self.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE:
            if self.capture_id is not None or any(value is None for value in page_fields):
                raise ValueError("raw page descriptor identity is incomplete")
            if self.page == 1 and self.page_request_id != self.root_request_id:
                raise ValueError("page one must reuse the query root request ID")
            if self.page > 1 and self.page_request_id == self.root_request_id:
                raise ValueError("nested pages require a fresh page request ID")
            if self.factor_snapshot_provenance_hash is not None:
                raise ValueError("raw page cannot carry factor provenance")
        elif self.object_kind is EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT:
            if self.fields != FACTOR_SCHEMA or self.units:
                raise ValueError("factor descriptor schema is invalid")
            if self.capture_id is None or any(value is not None for value in page_fields):
                raise ValueError("factor descriptor must be local and transport-free")
            if self.factor_snapshot_provenance_hash is None:
                raise ValueError("factor descriptor requires records hash")
        elif self.object_kind is EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT:
            if self.fields != ("binding",) or self.units:
                raise ValueError("factor resolution descriptor schema is invalid")
            if self.capture_id is None or any(value is not None for value in page_fields):
                raise ValueError("factor resolution descriptor must be local and transport-free")
            if self.factor_resolution_provenance_hash is None:
                raise ValueError("factor resolution descriptor requires bindings hash")
            if self.factor_snapshot_provenance_hash is not None:
                raise ValueError("factor resolution descriptor has wrong provenance")
        if self.normalization_clock_utc.tzinfo is None:
            raise ValueError("normalization clock must be timezone-aware")
        return self


class EvidenceManifest(_Immutable):
    evidence_id: SafeIdentifier
    provider_id: ProviderId
    adapter_version: str = R2F2_ADAPTER_VERSION
    endpoint_contract_version: str = R2F2_ENDPOINT_CONTRACT_VERSION
    trade_date: date
    refresh_id: SafeIdentifier
    universe_id: SafeIdentifier
    requested_at: datetime
    completed_at: datetime
    normalization_clock_utc: datetime
    logical_request_plan: ExpectedLogicalRequestPlan
    request_plan_hash: SafeSha256
    request_completions: tuple[RequestCompletion, ...]
    completion_hash: SafeSha256
    objects: tuple[EvidenceObjectDescriptor, ...]
    transport_lineage: tuple[TransportLineageRef, ...] = ()
    transport_observations: TransportObservationAggregate | None = None
    endpoint_summaries: tuple[EndpointContractSummary, ...] = ()
    factor_cache_snapshot: PublishedFactorCacheSnapshot | None = None
    factor_resolution_snapshot: PublishedFactorResolutionSnapshot | None = None
    factor_resolution: tuple[FactorResolutionBinding, ...] = Field(default=(), exclude=True)
    factor_resolution_sha256: SafeSha256
    request_count: int = Field(ge=1, le=4096)
    attempt_count: int = Field(ge=1, le=20000)
    failure_class: SafeFailureClass | None = None
    object_count: int = Field(ge=0, le=4096)
    row_count: int = Field(ge=0, le=MAX_ROWS)
    manifest_sha256: SafeSha256

    @model_validator(mode="after")
    def validate_counts(self) -> EvidenceManifest:
        if self.adapter_version != R2F2_ADAPTER_VERSION:
            raise ValueError("unknown evidence adapter version")
        if self.endpoint_contract_version != R2F2_ENDPOINT_CONTRACT_VERSION:
            raise ValueError("unknown evidence endpoint contract version")
        if self.request_plan_hash != self.logical_request_plan.request_plan_hash:
            raise ValueError("evidence request plan hash mismatch")
        if self.request_plan_hash != self.logical_request_plan.compute_hash():
            raise ValueError("evidence request plan content hash mismatch")
        completion_digest = _digest(
            [item.model_dump(mode="json") for item in self.request_completions]
        )
        if self.completion_hash != completion_digest:
            raise ValueError("evidence completion content hash mismatch")
        if self.object_count != len(self.objects):
            raise ValueError("evidence object count mismatch")
        raw_rows = sum(
            item.row_count
            for item in self.objects
            if item.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE
        )
        if self.row_count != raw_rows:
            raise ValueError("evidence row count mismatch")
        if self.factor_cache_snapshot is not None:
            factor = self.factor_cache_snapshot.manifest
            descriptor = tuple(
                item
                for item in self.objects
                if item.object_kind is EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT
            )
            if len(descriptor) != 1:
                raise ValueError("factor snapshot descriptor cardinality mismatch")
            item = descriptor[0]
            if (
                item.capture_id != factor.capture_id
                or item.object_id != factor.object_id
                or item.relative_path != factor.relative_path
                or item.sha256 != factor.object_sha256
                or item.byte_count != factor.byte_count
                or item.row_count != factor.row_count
                or item.schema_variant != factor.schema_variant
                or item.schema_hash != factor.schema_hash
                or item.factor_snapshot_provenance_hash != factor.records_sha256
            ):
                raise ValueError("factor descriptor and manifest identity mismatch")
            if not self.factor_resolution and self.factor_resolution_snapshot is None:
                raise ValueError("factor snapshot requires a resolution binding")
            for binding in self.factor_resolution:
                cache = getattr(binding, "cache", None)
                if (
                    cache is None
                    or cache.cache_object_id != factor.object_id
                    or cache.cache_object_sha256 != factor.object_sha256
                ):
                    raise ValueError("factor resolution is not descriptor-bound")
            if self.factor_resolution_snapshot is not None:
                resolution = self.factor_resolution_snapshot.manifest
                resolution_descriptor = tuple(
                    item
                    for item in self.objects
                    if item.object_kind is EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT
                )
                if len(resolution_descriptor) != 1:
                    raise ValueError("factor resolution descriptor cardinality mismatch")
                item = resolution_descriptor[0]
                if (
                    item.capture_id != resolution.capture_id
                    or item.object_id != resolution.object_id
                    or item.relative_path != resolution.relative_path
                    or item.sha256 != resolution.object_sha256
                    or item.byte_count != resolution.byte_count
                    or item.row_count != resolution.binding_count
                    or item.schema_variant != resolution.schema_variant
                    or item.schema_hash != resolution.schema_hash
                    or item.factor_resolution_provenance_hash != resolution.bindings_sha256
                ):
                    raise ValueError("factor resolution descriptor and manifest mismatch")
                if resolution.schema_hash != _digest(FACTOR_RESOLUTION_SCHEMA):
                    raise ValueError("factor resolution schema hash mismatch")
                if self.factor_resolution and resolution.binding_count != len(
                    self.factor_resolution
                ):
                    raise ValueError("factor resolution binding count mismatch")
            elif any(
                item.object_kind is EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT
                for item in self.objects
            ):
                raise ValueError("factor resolution descriptor is unbound")
        elif self.factor_resolution or self.factor_resolution_snapshot is not None:
            raise ValueError("factor resolution requires a published factor snapshot")
        if self.factor_resolution_snapshot is None and any(
            item.object_kind is EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT
            for item in self.objects
        ):
            raise ValueError("factor resolution descriptor requires a snapshot manifest")
        expected_factor_resolution_hash = (
            _digest([item.model_dump(mode="json") for item in self.factor_resolution])
            if self.factor_resolution
            else (
                self.factor_resolution_snapshot.manifest.bindings_sha256
                if self.factor_resolution_snapshot is not None
                else _digest([])
            )
        )
        if self.factor_resolution_sha256 != expected_factor_resolution_hash:
            raise ValueError("factor resolution aggregate hash mismatch")
        manifest = self.model_dump(mode="json")
        manifest.pop("manifest_sha256", None)
        if self.manifest_sha256 != _digest(manifest):
            raise ValueError("evidence manifest hash mismatch")
        return self


class PublishedEvidence(_Immutable):
    manifest: EvidenceManifest
    descriptors: tuple[EvidenceObjectDescriptor, ...]
    reader_identity: SafeSha256
    _reader: EvidenceReader | None = PrivateAttr(default=None)
    _closed: bool = PrivateAttr(default=False)

    def bind_reader(self, reader: EvidenceReader) -> PublishedEvidence:
        """Bind this immutable projection to the reader that verified its bytes."""
        object.__setattr__(self, "_reader", reader)
        return self

    def read_rows(self) -> tuple[dict[str, Any], ...]:
        if self._closed:
            raise EvidenceError("published evidence reader is closed", "EVIDENCE_ROOT_UNAVAILABLE")
        reader = self._reader
        if reader is None:
            raise EvidenceError(
                "published evidence reader is not bound", "EVIDENCE_MANIFEST_INVALID"
            )
        return reader.read_rows(self)

    def close(self) -> None:
        if self._closed:
            return
        object.__setattr__(self, "_closed", True)
        reader = self._reader
        if reader is not None:
            reader.close()

    def __enter__(self) -> PublishedEvidence:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class ReplayResult(_Immutable):
    status: Literal["ready", "unavailable", "error"]
    evidence_id: SafeIdentifier
    candidate_sha256: SafeSha256 | None = None
    semantic_hash: SafeSha256 | None = None
    byte_match: bool | None = None
    semantic_match: bool | None = None
    normalization_clock_utc: datetime | None = None
    row_count: int = Field(ge=0, le=MAX_ROWS)
    trade_date: date | None = None
    failure_class: SafeFailureClass | None = None


def _safe_parts(path: str) -> tuple[str, ...]:
    try:
        value = validate_safe_relative_path(path)
    except Exception as exc:
        raise EvidenceError("unsafe evidence path", "EVIDENCE_UNSAFE_PATH") from exc
    parts = tuple(value.split("/"))
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise EvidenceError("unsafe evidence path", "EVIDENCE_UNSAFE_PATH")
    return parts


def open_evidence_relative(root_dirfd: int, path: str, flags: int = os.O_RDONLY) -> int:
    """Open a descriptor relative to an already-open root without following links."""
    parts = _safe_parts(path)
    current = os.dup(root_dirfd)
    try:
        for component in parts[:-1]:
            nxt = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            os.close(current)
            current = nxt
        return os.open(parts[-1], flags | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=current)
    except (OSError, EvidenceError) as exc:
        raise EvidenceError("evidence path unavailable", "EVIDENCE_ROOT_UNAVAILABLE") from exc
    finally:
        try:
            os.close(current)
        except OSError:
            pass


def _open_root_dir(root: Path, *, create: bool) -> int:
    """Walk every root ancestor with O_NOFOLLOW and return its directory fd."""
    absolute = root.absolute()
    parts = absolute.parts
    if any(part in {"", ".", ".."} for part in parts):
        raise EvidenceError("evidence root is unsafe", "EVIDENCE_UNSAFE_PATH")
    current = os.open(os.sep, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        for component in parts[1:]:
            try:
                child = os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=current,
                )
            except FileNotFoundError:
                if not create:
                    raise EvidenceError(
                        "evidence root unavailable", "EVIDENCE_ROOT_UNAVAILABLE"
                    ) from None
                try:
                    os.mkdir(component, 0o700, dir_fd=current)
                except FileExistsError:
                    pass
                child = os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=current,
                )
            os.close(current)
            current = child
        return current
    except EvidenceError:
        os.close(current)
        raise
    except OSError as exc:
        os.close(current)
        raise EvidenceError("evidence root unavailable", "EVIDENCE_ROOT_UNAVAILABLE") from exc


def _stat_fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read_descriptor_fd(root_fd: int, relative: str, limit: int) -> bytes:
    descriptor = open_evidence_relative(root_fd, relative)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise EvidenceError("evidence object exceeds bound", "EVIDENCE_OBJECT_OVERSIZE")
        payload = bytearray()
        while len(payload) <= limit:
            chunk = os.read(descriptor, min(1024 * 1024, limit + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if _stat_fingerprint(before) != _stat_fingerprint(after):
            raise EvidenceError("evidence object changed during read", "EVIDENCE_HASH_MISMATCH")
        if len(payload) > limit:
            raise EvidenceError("evidence object exceeds bound", "EVIDENCE_OBJECT_OVERSIZE")
        return bytes(payload)
    finally:
        os.close(descriptor)


def _read_descriptor(root: Path, relative: str, limit: int) -> bytes:
    root_fd = _open_root_dir(root, create=False)
    try:
        return _read_descriptor_fd(root_fd, relative, limit)
    finally:
        os.close(root_fd)


def _read_existing_fd(descriptor: int, expected: bytes) -> None:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != len(expected):
        raise EvidenceError("content-addressed path is unsafe", "EVIDENCE_UNSAFE_PATH")
    actual = bytearray()
    while len(actual) < len(expected):
        chunk = os.read(descriptor, len(expected) - len(actual))
        if not chunk:
            break
        actual.extend(chunk)
    if bytes(actual) != expected:
        raise EvidenceError("content-addressed collision", "EVIDENCE_HASH_MISMATCH")


def _open_or_create_directory(parent: int, component: str) -> int:
    try:
        return os.open(
            component,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=parent,
        )
    except FileNotFoundError:
        try:
            os.mkdir(component, 0o700, dir_fd=parent)
        except FileExistsError:
            pass
        try:
            return os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
        except OSError as exc:
            raise EvidenceError(
                "content-addressed parent is unsafe", "EVIDENCE_UNSAFE_PATH"
            ) from exc


def _atomic_create_relative(
    root: Path,
    relative: str,
    payload: bytes,
    *,
    root_fd: int | None = None,
) -> None:
    """Create a content-addressed object through descriptor-bound directories."""
    parts = _safe_parts(relative)
    owned_root_fd = root_fd is None
    opened_root_fd = _open_root_dir(root, create=False) if owned_root_fd else os.dup(root_fd)
    parent = os.dup(opened_root_fd)
    try:
        for component in parts[:-1]:
            child = _open_or_create_directory(parent, component)
            os.close(parent)
            parent = child
        name = parts[-1]
        try:
            existing = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
        except FileNotFoundError:
            existing = None
        if existing is not None:
            try:
                _read_existing_fd(existing, payload)
            finally:
                os.close(existing)
            return

        temporary_name = f".{name}.{secrets.token_hex(8)}.partial"
        temporary = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=parent,
        )
        try:
            view = memoryview(payload)
            while view:
                written = os.write(temporary, view)
                view = view[written:]
            os.fsync(temporary)
        finally:
            os.close(temporary)
        try:
            os.link(
                temporary_name,
                name,
                src_dir_fd=parent,
                dst_dir_fd=parent,
                follow_symlinks=False,
            )
            os.fsync(parent)
        except FileExistsError:
            existing = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent,
            )
            try:
                _read_existing_fd(existing, payload)
            finally:
                os.close(existing)
        finally:
            try:
                os.unlink(temporary_name, dir_fd=parent)
            except FileNotFoundError:
                pass
    except OSError as exc:
        raise EvidenceError("content-addressed path is unsafe", "EVIDENCE_UNSAFE_PATH") from exc
    finally:
        os.close(parent)
        os.close(opened_root_fd)


def _parquet_bytes(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> bytes:
    with tempfile.TemporaryDirectory(prefix="stock-eva-evidence-") as directory:
        output = Path(directory) / "object.parquet"
        connection = duckdb.connect(":memory:")
        try:
            definitions = ", ".join(f'"{field}" VARCHAR' for field in fields)
            connection.execute(f"CREATE TABLE source ({definitions})")
            values = [
                [None if row.get(field) is None else str(row.get(field)) for field in fields]
                for row in rows
            ]
            if values:
                connection.executemany(
                    f"INSERT INTO source VALUES ({','.join('?' for _ in fields)})", values
                )
            connection.execute("COPY source TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(output)])
        except Exception as exc:
            raise EvidenceError(
                "evidence parquet write failed", "EVIDENCE_SCHEMA_MISMATCH"
            ) from exc
        finally:
            connection.close()
        return output.read_bytes()


class EvidenceReader:
    """Descriptor-bound, read-only evidence reader."""

    def __init__(self, root: Path | str, *, max_object_bytes: int = MAX_OBJECT_BYTES) -> None:
        self.root = Path(root)
        self.max_object_bytes = max_object_bytes
        self._held_root_fd: int | None = None
        self._held_root_identity: tuple[int, int] | None = None
        self._closed = False

    def close(self) -> None:
        self._closed = True
        if self._held_root_fd is not None:
            try:
                os.close(self._held_root_fd)
            except OSError:
                pass
            self._held_root_fd = None
            self._held_root_identity = None

    def __del__(self) -> None:
        self.close()

    def _assert_root_identity(self, root_fd: int, identity: tuple[int, int]) -> None:
        current = os.fstat(root_fd)
        if (current.st_dev, current.st_ino) != identity:
            raise EvidenceError("evidence root changed during read", "EVIDENCE_HASH_MISMATCH")
        try:
            configured = os.stat(self.root, follow_symlinks=False)
        except OSError as exc:
            raise EvidenceError("evidence root unavailable", "EVIDENCE_ROOT_UNAVAILABLE") from exc
        if (configured.st_dev, configured.st_ino) != identity:
            raise EvidenceError("evidence root was replaced", "EVIDENCE_HASH_MISMATCH")

    def _open_read_root(self) -> tuple[int, tuple[int, int]]:
        if self._closed:
            raise EvidenceError("evidence reader is closed", "EVIDENCE_ROOT_UNAVAILABLE")
        root_fd = _open_root_dir(self.root, create=False)
        metadata = os.fstat(root_fd)
        identity = (metadata.st_dev, metadata.st_ino)
        self._assert_root_identity(root_fd, identity)
        return root_fd, identity

    def read(self, evidence_id: str) -> PublishedEvidence:
        if self._closed:
            raise EvidenceError("evidence reader is closed", "EVIDENCE_ROOT_UNAVAILABLE")
        if "/" in evidence_id or ".." in evidence_id:
            raise EvidenceError("unsafe evidence ID", "EVIDENCE_UNSAFE_PATH")
        if self._held_root_fd is not None:
            os.close(self._held_root_fd)
            self._held_root_fd = None
            self._held_root_identity = None
        root_fd, root_identity = self._open_read_root()
        try:
            payload = _read_descriptor_fd(
                root_fd, f"manifests/{evidence_id}.json", MAX_MANIFEST_BYTES
            )
            try:
                raw = json.loads(payload.decode("utf-8"))
                if raw.get("evidence_id") != evidence_id:
                    raise EvidenceError(
                        "requested evidence ID does not match manifest ID",
                        "EVIDENCE_HASH_MISMATCH",
                    )
                if raw.get("manifest_sha256") != _digest(
                    {key: value for key, value in raw.items() if key != "manifest_sha256"}
                ):
                    raise EvidenceError("evidence manifest hash mismatch", "EVIDENCE_HASH_MISMATCH")
                manifest = EvidenceManifest.model_validate(raw)
            except EvidenceError:
                raise
            except Exception as exc:
                raise EvidenceError(
                    "evidence manifest invalid", "EVIDENCE_MANIFEST_INVALID"
                ) from exc
            identity_probe = manifest.model_dump(mode="json")
            identity_probe.pop("evidence_id", None)
            identity_probe.pop("manifest_sha256", None)
            expected_evidence_id = f"ev-{_digest(identity_probe)[:24]}"
            if manifest.evidence_id != expected_evidence_id:
                raise EvidenceError(
                    "evidence ID content identity mismatch", "EVIDENCE_HASH_MISMATCH"
                )
            factor_rows: tuple[dict[str, Any], ...] = ()
            factor_resolution: tuple[FactorResolutionBinding, ...] = ()
            for descriptor in manifest.objects:
                self._assert_root_identity(root_fd, root_identity)
                data = _read_descriptor_fd(root_fd, descriptor.relative_path, self.max_object_bytes)
                if len(data) != descriptor.byte_count or _sha256_bytes(data) != descriptor.sha256:
                    raise EvidenceError("evidence object hash mismatch", "EVIDENCE_HASH_MISMATCH")
                if descriptor.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE:
                    self._validate_parquet(data, descriptor)
                elif descriptor.object_kind is EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT:
                    factor_resolution = self._validate_factor_resolution_snapshot(data, descriptor)
                else:
                    self._validate_factor_snapshot(data, descriptor)
                    factor_rows = tuple(json.loads(data.decode("utf-8"))["rows"])
            if manifest.factor_resolution_snapshot is not None:
                if not factor_resolution:
                    raise EvidenceError(
                        "factor resolution snapshot is missing", "EVIDENCE_MANIFEST_INVALID"
                    )
                if _digest([item.model_dump(mode="json") for item in factor_resolution]) != (
                    manifest.factor_resolution_sha256
                ):
                    raise EvidenceError(
                        "factor resolution snapshot hash mismatch", "EVIDENCE_HASH_MISMATCH"
                    )
                if (
                    len(factor_resolution)
                    != manifest.factor_resolution_snapshot.manifest.binding_count
                ):
                    raise EvidenceError(
                        "factor resolution snapshot count mismatch", "EVIDENCE_MANIFEST_INVALID"
                    )
                object.__setattr__(manifest, "factor_resolution", factor_resolution)
            self._validate_typed_graph(manifest)
            if manifest.factor_cache_snapshot is not None:
                expected_reader_identity = _digest(
                    manifest.factor_cache_snapshot.manifest.model_dump(mode="json")
                )
                if manifest.factor_cache_snapshot.reader_identity != expected_reader_identity:
                    raise EvidenceError(
                        "factor snapshot reader identity mismatch",
                        "EVIDENCE_HASH_MISMATCH",
                    )
            if manifest.factor_resolution_snapshot is not None:
                resolution = manifest.factor_resolution_snapshot
                expected_reader_identity = _digest(resolution.manifest.model_dump(mode="json"))
                if resolution.reader_identity != expected_reader_identity:
                    raise EvidenceError(
                        "factor resolution reader identity mismatch", "EVIDENCE_HASH_MISMATCH"
                    )
                if resolution.manifest.schema_hash != _digest(FACTOR_RESOLUTION_SCHEMA):
                    raise EvidenceError(
                        "factor resolution schema hash mismatch", "EVIDENCE_SCHEMA_MISMATCH"
                    )
            if manifest.factor_resolution:
                keys = [f"{row['symbol']}.{row['trade_date']}" for row in factor_rows]
                if len(keys) != len(set(keys)):
                    raise EvidenceError(
                        "factor snapshot has duplicate record keys", "EVIDENCE_MANIFEST_INVALID"
                    )
                factor_requests = tuple(
                    item
                    for item in manifest.logical_request_plan.requests
                    if item.endpoint is ProviderEndpoint.DAILY_FACTOR
                )
                for binding in manifest.factor_resolution:
                    if factor_requests and binding.plan_ordinal >= manifest.request_count:
                        raise EvidenceError(
                            "factor resolution plan ordinal is unbound",
                            "EVIDENCE_MANIFEST_INVALID",
                        )
                    if factor_requests:
                        logical = manifest.logical_request_plan.requests[binding.plan_ordinal]
                        expected_endpoint = (
                            ProviderEndpoint.DAILY_FACTOR
                            if binding.selected_kind == "factor_cache_snapshot"
                            else binding.live.endpoint
                        )
                        if logical.endpoint is not expected_endpoint:
                            raise EvidenceError(
                                "factor resolution endpoint is unbound",
                                "EVIDENCE_MANIFEST_INVALID",
                            )
                        if binding.symbol not in logical.symbols:
                            raise EvidenceError(
                                "factor resolution symbol is outside its plan",
                                "EVIDENCE_MANIFEST_INVALID",
                            )
                        if binding.trade_date != manifest.trade_date:
                            raise EvidenceError(
                                "factor resolution date is outside its plan",
                                "EVIDENCE_MANIFEST_INVALID",
                            )
                    if binding.selected_kind == "factor_cache_snapshot":
                        if binding.cache is None or binding.cache.record_key not in set(keys):
                            raise EvidenceError(
                                "factor resolution record key is unbound",
                                "EVIDENCE_MANIFEST_INVALID",
                            )
                        selected = next(
                            row
                            for row in factor_rows
                            if f"{row['symbol']}.{row['trade_date']}" == binding.cache.record_key
                        )
                        if binding.selected_value_semantic_hash != _factor_value_semantic_hash(
                            selected
                        ):
                            raise EvidenceError(
                                "factor selected value hash mismatch", "EVIDENCE_HASH_MISMATCH"
                            )
                if len(manifest.factor_resolution) != len(
                    set(
                        binding.cache.record_key
                        for binding in manifest.factor_resolution
                        if binding.cache is not None
                    )
                ):
                    raise EvidenceError(
                        "factor resolution has duplicate bindings", "EVIDENCE_MANIFEST_INVALID"
                    )
            self._assert_root_identity(root_fd, root_identity)
            identity = _digest(manifest.model_dump(mode="json"))
            evidence = PublishedEvidence(
                manifest=manifest, descriptors=manifest.objects, reader_identity=identity
            ).bind_reader(self)
            self._held_root_fd = root_fd
            self._held_root_identity = root_identity
            root_fd = -1
            return evidence
        finally:
            if root_fd >= 0:
                os.close(root_fd)

    @staticmethod
    def _validate_typed_graph(manifest: EvidenceManifest) -> None:
        """Re-check nested typed joins after JSON deserialization."""
        if manifest.request_count != manifest.logical_request_plan.request_count:
            raise EvidenceError("evidence request count mismatch", "EVIDENCE_MANIFEST_INVALID")
        if tuple(item.plan_ordinal for item in manifest.request_completions) != tuple(
            range(manifest.request_count)
        ):
            raise EvidenceError("evidence completions are not ordered", "EVIDENCE_MANIFEST_INVALID")
        if manifest.attempt_count != sum(
            len(item.attempts) for item in manifest.request_completions
        ):
            raise EvidenceError("evidence attempt count mismatch", "EVIDENCE_MANIFEST_INVALID")
        if manifest.factor_resolution and manifest.factor_resolution_sha256 != _digest(
            [item.model_dump(mode="json") for item in manifest.factor_resolution]
        ):
            raise EvidenceError("factor resolution hash mismatch", "EVIDENCE_HASH_MISMATCH")
        from backend.app.market.providers.base import endpoint_contract_for

        for descriptor in manifest.objects:
            if descriptor.object_kind is not EvidenceObjectKind.RAW_ENDPOINT_PAGE:
                continue
            if descriptor.plan_ordinal is None or descriptor.plan_ordinal >= manifest.request_count:
                raise EvidenceError(
                    "descriptor plan ordinal is unbound", "EVIDENCE_MANIFEST_INVALID"
                )
            logical = manifest.logical_request_plan.requests[descriptor.plan_ordinal]
            try:
                contract = endpoint_contract_for(
                    logical.endpoint, logical.instrument_role, logical.schema_variant
                )
            except ValueError as exc:
                raise EvidenceError(
                    "descriptor endpoint contract is unknown", "EVIDENCE_MANIFEST_INVALID"
                ) from exc
            expected = (
                manifest.provider_id,
                manifest.universe_id,
                manifest.refresh_id,
                manifest.adapter_version,
                manifest.endpoint_contract_version,
                logical.endpoint,
                logical.request_role,
                logical.instrument_role,
                logical.schema_variant,
                contract.schema_variant,
                contract.fields,
                contract.units,
                contract.date_semantics,
                contract.pagination_policy,
            )
            actual = (
                descriptor.provider_id,
                descriptor.universe_id,
                descriptor.refresh_id,
                descriptor.adapter_version,
                descriptor.endpoint_contract_version,
                descriptor.endpoint,
                descriptor.request_role,
                descriptor.instrument_role,
                descriptor.schema_variant,
                descriptor.source_schema,
                descriptor.fields,
                descriptor.units,
                descriptor.date_semantics,
                descriptor.pagination_policy,
            )
            if actual != expected:
                raise EvidenceError("descriptor typed graph mismatch", "EVIDENCE_MANIFEST_INVALID")

            completion = manifest.request_completions[descriptor.plan_ordinal]
            if completion.successful_attempt is None:
                raise EvidenceError(
                    "descriptor has no successful attempt", "EVIDENCE_MANIFEST_INVALID"
                )
            final = next(
                item
                for item in completion.attempts
                if item.attempt == completion.successful_attempt
            )
            expected_page_request = dict(final.page_request_ids).get(descriptor.page)
            if (
                descriptor.attempt != final.attempt
                or descriptor.root_request_id != final.root_request_id
                or descriptor.provider_session_id != final.provider_session_id
                or descriptor.page_request_id != expected_page_request
            ):
                raise EvidenceError(
                    "descriptor completion join mismatch", "EVIDENCE_MANIFEST_INVALID"
                )
            if manifest.transport_observations is None:
                raise EvidenceError(
                    "descriptor observations are missing", "EVIDENCE_MANIFEST_INVALID"
                )
            matching_lineage = tuple(
                item
                for item in manifest.transport_lineage
                if (
                    item.refresh_id == descriptor.refresh_id
                    and item.provider_session_id == descriptor.provider_session_id
                    and item.root_request_id == descriptor.root_request_id
                    and item.page_request_id == descriptor.page_request_id
                    and item.endpoint == descriptor.endpoint
                    and item.plan_ordinal == descriptor.plan_ordinal
                    and item.attempt == descriptor.attempt
                    and item.page == descriptor.page
                    and item.observation_digest == descriptor.transport_observation_digest
                )
            )
            matching_observation = tuple(
                item
                for item in manifest.transport_observations.observations
                if (
                    item.refresh_id == descriptor.refresh_id
                    and item.provider_session_id == descriptor.provider_session_id
                    and item.request_id == descriptor.page_request_id
                    and item.endpoint == descriptor.endpoint
                    and item.plan_ordinal == descriptor.plan_ordinal
                    and item.attempt == descriptor.attempt
                    and item.page == descriptor.page
                    and item.protocol_stage.value == "complete"
                    and item.outcome.value == "success"
                    and item.end_marker_seen
                    and item.observation_digest == descriptor.transport_observation_digest
                )
            )
            if len(matching_lineage) != 1 or len(matching_observation) != 1:
                raise EvidenceError(
                    "descriptor lineage join is not one-to-one", "EVIDENCE_MANIFEST_INVALID"
                )

        raw_descriptors = tuple(
            item
            for item in manifest.objects
            if item.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE
        )
        if len(manifest.transport_lineage) != len(raw_descriptors) or len(
            {
                (
                    item.refresh_id,
                    item.provider_session_id,
                    item.root_request_id,
                    item.page_request_id,
                    item.endpoint,
                    item.plan_ordinal,
                    item.attempt,
                    item.page,
                    item.observation_digest,
                )
                for item in manifest.transport_lineage
            }
        ) != len(raw_descriptors):
            raise EvidenceError("lineage cardinality mismatch", "EVIDENCE_MANIFEST_INVALID")

    def read_rows(
        self,
        evidence: PublishedEvidence,
        descriptor: EvidenceObjectDescriptor | None = None,
    ) -> tuple[dict[str, Any], ...]:
        """Read descriptor-bound pages, or one page for the legacy test helper."""
        if self._closed or evidence._closed:
            raise EvidenceError("published evidence reader is closed", "EVIDENCE_ROOT_UNAVAILABLE")
        if descriptor is None:
            owns_fd = self._held_root_fd is None
            if owns_fd:
                root_fd, root_identity = self._open_read_root()
            else:
                root_fd = self._held_root_fd
                root_identity = self._held_root_identity
                assert root_fd is not None and root_identity is not None
            pages = []
            factor_fields: tuple[str, ...] = ()
            factor_rows: list[dict[str, Any]] = []
            try:
                for item in evidence.descriptors:
                    self._assert_root_identity(root_fd, root_identity)
                    data = _read_descriptor_fd(root_fd, item.relative_path, self.max_object_bytes)
                    if len(data) != item.byte_count or _sha256_bytes(data) != item.sha256:
                        raise EvidenceError(
                            "evidence object hash mismatch", "EVIDENCE_HASH_MISMATCH"
                        )
                    if item.object_kind is EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT:
                        self._validate_factor_snapshot(data, item)
                        factor_fields = item.fields
                        factor_rows.extend(json.loads(data.decode("utf-8"))["rows"])
                    elif item.object_kind is EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT:
                        self._validate_factor_resolution_snapshot(data, item)
                    else:
                        self._validate_parquet(data, item)
                        with tempfile.NamedTemporaryFile(suffix=".parquet") as handle:
                            handle.write(data)
                            handle.flush()
                            connection = duckdb.connect(":memory:")
                            try:
                                decoded = connection.execute(
                                    "SELECT * FROM read_parquet(?)", [handle.name]
                                ).fetchall()
                            finally:
                                connection.close()
                        pages.append(
                            {
                                "fields": item.fields,
                                "rows": tuple(decoded),
                            }
                        )
                if factor_rows and evidence.manifest.factor_resolution:
                    rows_by_key = {
                        f"{row['symbol']}.{row['trade_date']}": row for row in factor_rows
                    }
                    factor_fields = ("code", "dividOperateDate", "backAdjustFactor")
                    factor_rows = [
                        rows_by_key[binding.cache.record_key]
                        for binding in evidence.manifest.factor_resolution
                        if binding.cache is not None and binding.cache.record_key in rows_by_key
                    ]
                    factor_rows = [
                        {
                            "code": row["symbol"],
                            "dividOperateDate": row["trade_date"],
                            "backAdjustFactor": row["back_adjust_factor"],
                        }
                        for row in factor_rows
                    ]
                if factor_rows:
                    for page in pages:
                        page["factor_fields"] = factor_fields
                        page["factor_rows"] = tuple(
                            tuple(row.get(field) for field in factor_fields) for row in factor_rows
                        )
                self._assert_root_identity(root_fd, root_identity)
                return tuple(pages)
            finally:
                if owns_fd:
                    os.close(root_fd)
        if descriptor not in evidence.descriptors:
            raise EvidenceError("descriptor is not bound to evidence", "EVIDENCE_MANIFEST_INVALID")
        if self._held_root_fd is None:
            bound_reader = evidence._reader
            if bound_reader is None or bound_reader is self:
                raise EvidenceError(
                    "published evidence has no held root", "EVIDENCE_ROOT_UNAVAILABLE"
                )
            return bound_reader.read_rows(evidence, descriptor)
        if evidence.reader_identity != _digest(evidence.manifest.model_dump(mode="json")):
            raise EvidenceError("published evidence identity mismatch", "EVIDENCE_HASH_MISMATCH")
        assert self._held_root_identity is not None
        self._assert_root_identity(self._held_root_fd, self._held_root_identity)
        data = _read_descriptor_fd(
            self._held_root_fd, descriptor.relative_path, self.max_object_bytes
        )
        if len(data) != descriptor.byte_count or _sha256_bytes(data) != descriptor.sha256:
            raise EvidenceError("evidence object hash mismatch", "EVIDENCE_HASH_MISMATCH")
        if descriptor.object_kind is EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT:
            self._validate_factor_snapshot(data, descriptor)
            raw = json.loads(data.decode("utf-8"))
            return tuple(raw["rows"])
        if descriptor.object_kind is EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT:
            return tuple(
                item.model_dump(mode="json")
                for item in self._validate_factor_resolution_snapshot(data, descriptor)
            )
        self._validate_parquet(data, descriptor)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as handle:
            handle.write(data)
            handle.flush()
            connection = duckdb.connect(":memory:")
            try:
                result = connection.execute(
                    "SELECT * FROM read_parquet(?)", [handle.name]
                ).fetchall()
            except Exception as exc:
                raise EvidenceError(
                    "evidence parquet is invalid", "EVIDENCE_SCHEMA_MISMATCH"
                ) from exc
            finally:
                connection.close()
        return tuple(dict(zip(descriptor.fields, row, strict=True)) for row in result)

    @staticmethod
    def _validate_parquet(data: bytes, descriptor: EvidenceObjectDescriptor) -> None:
        with tempfile.NamedTemporaryFile(suffix=".parquet") as handle:
            handle.write(data)
            handle.flush()
            connection = duckdb.connect(":memory:")
            try:
                columns = tuple(
                    row[0]
                    for row in connection.execute(
                        "DESCRIBE SELECT * FROM read_parquet(?)", [handle.name]
                    ).fetchall()
                )
                count = int(
                    connection.execute(
                        "SELECT count(*) FROM read_parquet(?)", [handle.name]
                    ).fetchone()[0]
                )
            except Exception as exc:
                raise EvidenceError(
                    "evidence parquet is invalid", "EVIDENCE_SCHEMA_MISMATCH"
                ) from exc
            finally:
                connection.close()
        if (
            count != descriptor.row_count
            or columns != descriptor.fields
            or descriptor.schema_hash != _digest(descriptor.fields)
        ):
            raise EvidenceError("evidence parquet schema/row mismatch", "EVIDENCE_SCHEMA_MISMATCH")

    @staticmethod
    def _validate_factor_snapshot(data: bytes, descriptor: EvidenceObjectDescriptor) -> None:
        try:
            raw = json.loads(data.decode("utf-8"))
            schema = tuple(raw["schema"])
            rows = tuple(raw["rows"])
            records_sha256 = str(raw["records_sha256"])
            if schema != FACTOR_SCHEMA or descriptor.fields != FACTOR_SCHEMA:
                raise ValueError("factor schema")
            if _digest(schema) != descriptor.schema_hash:
                raise ValueError("factor schema hash")
            if len(rows) != descriptor.row_count:
                raise ValueError("factor row count")
            if records_sha256 != descriptor.factor_snapshot_provenance_hash:
                raise ValueError("factor records binding")
            if records_sha256 != _digest(list(rows)):
                raise ValueError("factor records hash")
            for row in rows:
                required = (
                    "symbol",
                    "trade_date",
                    "fore_adjust_factor",
                    "evidence_kind",
                    "evidence_effective_date",
                    "source_row_hash",
                    "observed_at",
                    "row_fingerprint",
                )
                if set(row) != set(FACTOR_SCHEMA) | {"row_fingerprint"} or any(
                    row[field] in (None, "") for field in required
                ):
                    raise ValueError("factor row schema")
                expected_fingerprint = _sha256_bytes(
                    json.dumps(
                        [
                            (
                                datetime.fromisoformat(
                                    str(row[field]).replace("Z", "+00:00")
                                ).isoformat()
                                if field == "observed_at"
                                else str(row[field])
                                if field
                                in {
                                    "trade_date",
                                    "evidence_effective_date",
                                    "evidence_observed_on",
                                }
                                else row[field]
                            )
                            for field in FACTOR_SCHEMA
                        ],
                        ensure_ascii=False,
                        separators=(",", ":"),
                        default=str,
                    ).encode("utf-8")
                )
                if row["row_fingerprint"] != expected_fingerprint:
                    raise ValueError("factor row fingerprint")
        except Exception as exc:
            raise EvidenceError("factor snapshot is invalid", "EVIDENCE_SCHEMA_MISMATCH") from exc

    @staticmethod
    def _validate_factor_resolution_snapshot(
        data: bytes, descriptor: EvidenceObjectDescriptor
    ) -> tuple[FactorResolutionBinding, ...]:
        try:
            payload = json.loads(data.decode("utf-8"))
            if payload.get("schema") != "factor-resolution-snapshot.v1":
                raise ValueError("factor resolution schema")
            bindings = tuple(
                FactorResolutionBinding.model_validate(item) for item in payload["bindings"]
            )
            bindings_sha256 = payload.get("bindings_sha256")
            if bindings_sha256 != _digest([item.model_dump(mode="json") for item in bindings]):
                raise ValueError("factor resolution hash")
            if len(data) != descriptor.byte_count or _sha256_bytes(data) != descriptor.sha256:
                raise ValueError("factor resolution object hash")
            if len(bindings) != descriptor.row_count:
                raise ValueError("factor resolution count")
            if descriptor.factor_resolution_provenance_hash != bindings_sha256:
                raise ValueError("factor resolution provenance")
            if descriptor.fields != FACTOR_RESOLUTION_SCHEMA:
                raise ValueError("factor resolution fields")
            if descriptor.schema_hash != _digest(FACTOR_RESOLUTION_SCHEMA):
                raise ValueError("factor resolution schema hash")
            return bindings
        except Exception as exc:
            raise EvidenceError(
                "factor resolution snapshot is invalid", "EVIDENCE_SCHEMA_MISMATCH"
            ) from exc

    def replay(
        self,
        evidence_id: str,
        *,
        compare_candidate_sha: str | None = None,
        compare_semantic_sha: str | None = None,
        compare_adapter_version: str | None = None,
        adapter: object | None = None,
        factor_cache: object | None = None,
    ) -> ReplayResult:
        try:
            evidence = self.read(evidence_id)
        except EvidenceError as exc:
            return ReplayResult(
                status="unavailable",
                evidence_id=evidence_id,
                row_count=0,
                failure_class=exc.failure_class,
            )
        manifest = evidence.manifest
        if (
            compare_adapter_version is not None
            and compare_adapter_version != manifest.adapter_version
        ):
            evidence.close()
            return ReplayResult(
                status="error",
                evidence_id=evidence_id,
                row_count=0,
                failure_class="REPLAY_NONDETERMINISTIC",
            )
        try:
            evidence.read_rows()
            if factor_cache is not None:
                # Replay is immutable: this argument is accepted only as a sentinel
                # in tests and is deliberately never dereferenced.
                del factor_cache
            selected_adapter = adapter
            if selected_adapter is None:
                from backend.app.market.providers.baostock import BaoStockProviderAdapter

                selected_adapter = BaoStockProviderAdapter(client=None)
            normalize = getattr(selected_adapter, "normalize", None)
            if not callable(normalize):
                raise EvidenceError(
                    "typed evidence adapter is required", "EVIDENCE_SCHEMA_MISMATCH"
                )
            normalized = normalize(
                evidence,
                normalization_clock_utc=manifest.normalization_clock_utc,
            )
            if not isinstance(normalized, tuple):
                raise EvidenceError("adapter normalization failed", "EVIDENCE_SCHEMA_MISMATCH")
        except EvidenceError as exc:
            evidence.close()
            return ReplayResult(
                status="error",
                evidence_id=evidence_id,
                row_count=0,
                failure_class=exc.failure_class,
            )
        except Exception:
            evidence.close()
            return ReplayResult(
                status="error",
                evidence_id=evidence_id,
                row_count=0,
                failure_class="EVIDENCE_SCHEMA_MISMATCH",
            )
        candidate = _digest([_dump_json(item) for item in normalized])
        semantic = _digest(
            [
                {
                    "trade_date": item.trade_date.isoformat(),
                    "symbol": item.symbol,
                    "open": item.open,
                    "high": item.high,
                    "low": item.low,
                    "close": item.close,
                    "adjust_factor": item.adjust_factor,
                    "is_suspended": item.is_suspended,
                }
                for item in normalized
            ]
        )
        byte_match = None if compare_candidate_sha is None else candidate == compare_candidate_sha
        semantic_match = None if compare_semantic_sha is None else semantic == compare_semantic_sha
        if byte_match is False:
            evidence.close()
            return ReplayResult(
                status="error",
                evidence_id=evidence_id,
                candidate_sha256=candidate,
                semantic_hash=semantic,
                byte_match=False,
                semantic_match=semantic_match,
                normalization_clock_utc=manifest.normalization_clock_utc,
                row_count=manifest.row_count,
                trade_date=manifest.trade_date,
                failure_class="REPLAY_NONDETERMINISTIC",
            )
        if semantic_match is False:
            evidence.close()
            return ReplayResult(
                status="error",
                evidence_id=evidence_id,
                candidate_sha256=candidate,
                semantic_hash=semantic,
                byte_match=byte_match,
                semantic_match=False,
                normalization_clock_utc=manifest.normalization_clock_utc,
                row_count=manifest.row_count,
                trade_date=manifest.trade_date,
                failure_class="REPLAY_SEMANTIC_MISMATCH",
            )
        evidence.close()
        return ReplayResult(
            status="ready",
            evidence_id=evidence_id,
            candidate_sha256=candidate,
            semantic_hash=semantic,
            byte_match=byte_match,
            semantic_match=semantic_match,
            normalization_clock_utc=manifest.normalization_clock_utc,
            row_count=manifest.row_count,
            trade_date=manifest.trade_date,
        )


class EvidenceStore:
    """Writer for content-addressed source evidence."""

    def __init__(
        self,
        root: Path | str,
        *,
        max_object_bytes: int = MAX_OBJECT_BYTES,
        max_rows: int = MAX_ROWS,
    ) -> None:
        self.root = Path(root)
        self.max_object_bytes = max_object_bytes
        self.max_rows = max_rows

    def _prepare_root(self) -> None:
        root_fd = self._prepare_root_fd()
        os.close(root_fd)

    def _prepare_root_fd(self) -> int:
        root_fd = _open_root_dir(self.root, create=True)
        try:
            for name in (
                "objects",
                "manifests",
                "gates",
                "candidates",
                "selections",
                "staging",
                "orphan-audit",
            ):
                child = _open_or_create_directory(root_fd, name)
                os.close(child)
        except BaseException:
            os.close(root_fd)
            raise
        return root_fd

    @staticmethod
    def _unlink_relative_fd(root_fd: int, relative: str) -> None:
        parts = _safe_parts(relative)
        parent = os.dup(root_fd)
        try:
            for component in parts[:-1]:
                child = os.open(
                    component,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                    dir_fd=parent,
                )
                os.close(parent)
                parent = child
            os.unlink(parts[-1], dir_fd=parent)
        finally:
            os.close(parent)

    @staticmethod
    def _relative_exists_fd(root_fd: int, relative: str) -> bool:
        try:
            descriptor = open_evidence_relative(root_fd, relative)
        except EvidenceError:
            return False
        os.close(descriptor)
        return True

    def publish_factor_snapshot(
        self,
        records: FactorCacheSnapshotRecords,
        *,
        capture_id: str,
        object_id: str | None = None,
    ) -> tuple[FactorCacheSnapshotManifest, EvidenceObjectDescriptor]:
        """Publish one descriptor-bound, immutable factor snapshot object."""
        factor, descriptor, payload = self._build_factor_snapshot(
            records, capture_id=capture_id, object_id=object_id
        )
        if len(payload) > self.max_object_bytes:
            raise EvidenceError("factor object exceeds bound", "EVIDENCE_OBJECT_OVERSIZE")
        root_fd = self._prepare_root_fd()
        try:
            _atomic_create_relative(self.root, descriptor.relative_path, payload, root_fd=root_fd)
        finally:
            os.close(root_fd)
        return factor, descriptor

    @staticmethod
    def _build_factor_snapshot(
        records: FactorCacheSnapshotRecords,
        *,
        capture_id: str,
        object_id: str | None = None,
    ) -> tuple[FactorCacheSnapshotManifest, EvidenceObjectDescriptor, bytes]:
        payload = _canonical(
            {
                "schema": FACTOR_SCHEMA,
                "rows": [item.model_dump(mode="json") for item in records.rows],
                "records_sha256": records.records_sha256,
            }
        )
        object_sha = _sha256_bytes(payload)
        object_id = object_id or f"factor-{object_sha[:24]}"
        relative = f"objects/{object_sha[:2]}/{object_id}.json"
        schema_hash = _digest(FACTOR_SCHEMA)
        factor = FactorCacheSnapshotManifest(
            capture_id=capture_id,
            object_id=object_id,
            relative_path=relative,
            object_sha256=object_sha,
            byte_count=len(payload),
            row_count=records.row_count,
            schema_variant="factor-cache-snapshot.v1",
            schema_hash=schema_hash,
            records_sha256=records.records_sha256,
            before_fingerprint=records.before_fingerprint,
            after_fingerprint=records.after_fingerprint,
        )
        descriptor = EvidenceObjectDescriptor(
            object_id=object_id,
            object_kind=EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT,
            relative_path=relative,
            sha256=object_sha,
            schema_hash=schema_hash,
            row_count=records.row_count,
            byte_count=len(payload),
            provider_id=ProviderId.BAOSTOCK,
            universe_id="factor-cache",
            refresh_id=capture_id,
            capture_id=capture_id,
            fields=FACTOR_SCHEMA,
            schema_variant="factor-cache-snapshot.v1",
            source_schema="factor-cache-snapshot.v1",
            normalization_clock_utc=(
                records.rows[-1].observed_at if records.rows else datetime(1970, 1, 1, tzinfo=UTC)
            ),
            factor_snapshot_provenance_hash=records.records_sha256,
        )
        return factor, descriptor, payload

    @staticmethod
    def _build_factor_resolution_snapshot(
        bindings: tuple[FactorResolutionBinding, ...],
        *,
        capture_id: str,
    ) -> tuple[FactorResolutionSnapshotManifest, EvidenceObjectDescriptor, bytes]:
        serialized = [item.model_dump(mode="json") for item in bindings]
        bindings_sha256 = _digest(serialized)
        payload = _canonical(
            {
                "schema": "factor-resolution-snapshot.v1",
                "bindings": serialized,
                "bindings_sha256": bindings_sha256,
            }
        )
        object_sha = _sha256_bytes(payload)
        object_id = f"factor-resolution-{object_sha[:24]}"
        relative = f"objects/{object_sha[:2]}/{object_id}.json"
        schema_hash = _digest(FACTOR_RESOLUTION_SCHEMA)
        manifest = FactorResolutionSnapshotManifest(
            capture_id=capture_id,
            object_id=object_id,
            relative_path=relative,
            object_sha256=object_sha,
            byte_count=len(payload),
            binding_count=len(bindings),
            schema_variant="factor-resolution-snapshot.v1",
            schema_hash=schema_hash,
            bindings_sha256=bindings_sha256,
        )
        descriptor = EvidenceObjectDescriptor(
            object_id=object_id,
            object_kind=EvidenceObjectKind.FACTOR_RESOLUTION_SNAPSHOT,
            relative_path=relative,
            sha256=object_sha,
            schema_hash=schema_hash,
            row_count=len(bindings),
            byte_count=len(payload),
            provider_id=ProviderId.BAOSTOCK,
            universe_id="factor-resolution",
            refresh_id=capture_id,
            capture_id=capture_id,
            fields=FACTOR_RESOLUTION_SCHEMA,
            schema_variant="factor-resolution-snapshot.v1",
            source_schema="factor-resolution-snapshot.v1",
            normalization_clock_utc=datetime(1970, 1, 1, tzinfo=UTC),
            factor_resolution_provenance_hash=bindings_sha256,
        )
        return manifest, descriptor, payload

    def publish(
        self,
        batch: ProviderRawBatch,
        *,
        evidence_id: str | None = None,
        factor_records: FactorCacheSnapshotRecords | None = None,
        capture_id: str | None = None,
        factor_resolution: tuple[FactorResolutionBinding, ...] = (),
    ) -> EvidenceManifest:
        self._validate_publishable_batch(batch)
        descriptors: list[EvidenceObjectDescriptor] = []
        writes: list[tuple[str, bytes]] = []
        factor_snapshot: PublishedFactorCacheSnapshot | None = None
        factor_descriptor: EvidenceObjectDescriptor | None = None
        factor_resolution_snapshot: PublishedFactorResolutionSnapshot | None = None
        factor_resolution_descriptor: EvidenceObjectDescriptor | None = None
        if factor_records is not None:
            if capture_id is None:
                raise EvidenceError(
                    "factor capture identity is required", "EVIDENCE_MANIFEST_INVALID"
                )
            if not factor_resolution:
                raise EvidenceError(
                    "factor resolution binding is required", "EVIDENCE_MANIFEST_INVALID"
                )
            factor_manifest, factor_descriptor, factor_payload = self._build_factor_snapshot(
                factor_records, capture_id=capture_id
            )
            expected_keys = {
                f"{row.symbol}.{row.trade_date.isoformat()}" for row in factor_records.rows
            }
            binding_keys = tuple(
                binding.cache.record_key
                for binding in factor_resolution
                if binding.cache is not None
            )
            if (
                len(binding_keys) != len(factor_resolution)
                or len(binding_keys) != len(set(binding_keys))
                or set(binding_keys) != expected_keys
            ):
                raise EvidenceError(
                    "factor resolution cardinality is not exact", "EVIDENCE_MANIFEST_INVALID"
                )
            if len(factor_payload) > self.max_object_bytes:
                raise EvidenceError("factor object exceeds bound", "EVIDENCE_OBJECT_OVERSIZE")
            factor_snapshot = PublishedFactorCacheSnapshot(
                manifest=factor_manifest,
                reader_identity=_digest(factor_manifest.model_dump(mode="json")),
            )
            writes.append((factor_descriptor.relative_path, factor_payload))
            for binding in factor_resolution:
                cache = binding.cache
                if (
                    binding.selected_kind != "factor_cache_snapshot"
                    or cache is None
                    or cache.cache_object_id != factor_descriptor.object_id
                    or cache.cache_object_sha256 != factor_descriptor.sha256
                ):
                    raise EvidenceError(
                        "factor resolution is not descriptor-bound", "EVIDENCE_MANIFEST_INVALID"
                    )
                if cache.record_key not in {
                    f"{row.symbol}.{row.trade_date.isoformat()}" for row in factor_records.rows
                }:
                    raise EvidenceError(
                        "factor resolution record key is unbound", "EVIDENCE_MANIFEST_INVALID"
                    )
                selected = next(
                    row
                    for row in factor_records.rows
                    if f"{row.symbol}.{row.trade_date.isoformat()}" == cache.record_key
                )
                selected_data = selected.model_dump(mode="python")
                if (
                    binding.symbol != selected.symbol
                    or binding.trade_date != selected.trade_date
                    or binding.selected_value_semantic_hash
                    != _factor_value_semantic_hash(selected_data)
                ):
                    raise EvidenceError(
                        "factor selected value binding is invalid", "EVIDENCE_HASH_MISMATCH"
                    )
                factor_requests = tuple(
                    item
                    for item in batch.logical_request_plan.requests
                    if item.endpoint is ProviderEndpoint.DAILY_FACTOR
                )
                if factor_requests:
                    if binding.plan_ordinal >= batch.logical_request_plan.request_count:
                        raise EvidenceError(
                            "factor resolution plan ordinal is unbound",
                            "EVIDENCE_MANIFEST_INVALID",
                        )
                    logical = batch.logical_request_plan.requests[binding.plan_ordinal]
                    if logical.endpoint is not ProviderEndpoint.DAILY_FACTOR:
                        raise EvidenceError(
                            "factor resolution endpoint is unbound",
                            "EVIDENCE_MANIFEST_INVALID",
                        )
                    if (
                        binding.symbol not in logical.symbols
                        or binding.trade_date != batch.request.trade_date
                    ):
                        raise EvidenceError(
                            "factor resolution is outside its plan",
                            "EVIDENCE_MANIFEST_INVALID",
                        )
        for item in batch.endpoint_batches:
            fields = tuple(item.fields)
            rows = [row.model_dump(mode="python") for row in item.rows]
            rows = [{key: value for key, value in row.items() if key in fields} for row in rows]
            payload = _parquet_bytes(rows, fields)
            if len(payload) > self.max_object_bytes or len(rows) > self.max_rows:
                raise EvidenceError("evidence object exceeds bound", "EVIDENCE_OBJECT_OVERSIZE")
            sha = _sha256_bytes(payload)
            object_id = f"obj-{sha[:24]}"
            relative = f"objects/{sha[:2]}/{object_id}.parquet"
            lineage = item.lineage
            descriptor = EvidenceObjectDescriptor(
                object_id=object_id,
                object_kind=EvidenceObjectKind.RAW_ENDPOINT_PAGE,
                relative_path=relative,
                sha256=sha,
                schema_hash=_digest(fields),
                row_count=len(rows),
                byte_count=len(payload),
                provider_id=batch.provider_id,
                universe_id=batch.request.universe_id,
                refresh_id=batch.request.refresh_id,
                provider_session_id=lineage.provider_session_id,
                root_request_id=lineage.root_request_id,
                page_request_id=lineage.page_request_id,
                endpoint=item.endpoint,
                request_role=item.request_role,
                instrument_role=item.instrument_role,
                shard_id=item.shard_id,
                plan_ordinal=item.plan_ordinal,
                attempt=lineage.attempt,
                page=lineage.page,
                fields=fields,
                adapter_version=batch.adapter_version,
                endpoint_contract_version=batch.endpoint_contract_version,
                schema_variant=item.schema_variant,
                source_schema=item.source_schema,
                units=item.units,
                date_semantics=item.date_semantics,
                pagination_policy=item.pagination_policy,
                normalization_clock_utc=batch.normalization_clock_utc,
                transport_observation_digest=lineage.observation_digest,
            )
            descriptors.append(descriptor)
            writes.append((relative, payload))
        if factor_descriptor is not None:
            descriptors.append(factor_descriptor)
        resolution_bindings = tuple(factor_resolution or getattr(batch, "factor_resolution", ()))
        if resolution_bindings:
            if factor_snapshot is None or capture_id is None:
                raise EvidenceError(
                    "factor resolution requires a factor snapshot",
                    "EVIDENCE_MANIFEST_INVALID",
                )
            resolution_manifest, factor_resolution_descriptor, resolution_payload = (
                self._build_factor_resolution_snapshot(resolution_bindings, capture_id=capture_id)
            )
            factor_resolution_snapshot = PublishedFactorResolutionSnapshot(
                manifest=resolution_manifest,
                reader_identity=_digest(resolution_manifest.model_dump(mode="json")),
            )
            descriptors.append(factor_resolution_descriptor)
            writes.append((factor_resolution_descriptor.relative_path, resolution_payload))
        manifest_id = evidence_id or "ev-pending"
        manifest_data = dict(
            evidence_id=manifest_id,
            provider_id=batch.provider_id,
            adapter_version=batch.adapter_version,
            endpoint_contract_version=batch.endpoint_contract_version,
            trade_date=batch.request.trade_date,
            refresh_id=batch.request.refresh_id,
            universe_id=batch.request.universe_id,
            requested_at=batch.started_at,
            completed_at=batch.completed_at,
            normalization_clock_utc=batch.normalization_clock_utc,
            logical_request_plan=batch.logical_request_plan,
            request_plan_hash=batch.request_plan_hash,
            request_completions=batch.request_completions,
            completion_hash=batch.completion_hash,
            objects=tuple(descriptors),
            transport_lineage=batch.transport_lineage,
            transport_observations=batch.transport_observations,
            endpoint_summaries=batch.endpoint_summaries,
            factor_cache_snapshot=factor_snapshot,
            factor_resolution_snapshot=factor_resolution_snapshot,
            factor_resolution=resolution_bindings,
            factor_resolution_sha256=(
                _digest([item.model_dump(mode="json") for item in resolution_bindings])
                if resolution_bindings
                else _digest([])
            ),
            request_count=batch.logical_request_plan.request_count,
            attempt_count=sum(len(item.attempts) for item in batch.request_completions),
            failure_class=None,
            object_count=len(descriptors),
            row_count=sum(
                item.row_count
                for item in descriptors
                if item.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE
            ),
        )
        identity_probe = EvidenceManifest.model_construct(
            **manifest_data,
            manifest_sha256="0" * 64,
        ).model_dump(mode="json")
        identity_probe.pop("evidence_id", None)
        identity_probe.pop("manifest_sha256", None)
        expected_manifest_id = f"ev-{_digest(identity_probe)[:24]}"
        if evidence_id is not None and evidence_id != expected_manifest_id:
            raise EvidenceError("evidence ID is not content addressed", "EVIDENCE_HASH_MISMATCH")
        manifest_id = expected_manifest_id
        manifest_data["evidence_id"] = manifest_id
        provisional = EvidenceManifest.model_construct(
            **manifest_data,
            manifest_sha256="0" * 64,
        )
        provisional_json = provisional.model_dump(mode="json")
        provisional_json.pop("manifest_sha256", None)
        manifest_hash = _digest(provisional_json)
        manifest_data["manifest_sha256"] = manifest_hash
        manifest = EvidenceManifest.model_validate(manifest_data)
        payload = _canonical(manifest.model_dump(mode="json"))
        if len(payload) > MAX_MANIFEST_BYTES:
            raise EvidenceError("evidence manifest exceeds bound", "EVIDENCE_OBJECT_OVERSIZE")
        writes.append((f"manifests/{manifest_id}.json", payload))
        root_fd = self._prepare_root_fd()
        root_metadata = os.fstat(root_fd)
        root_identity = (root_metadata.st_dev, root_metadata.st_ino)
        created: list[str] = []
        preexisting = {
            relative for relative, _content in writes if self._relative_exists_fd(root_fd, relative)
        }
        try:
            for relative, content in writes:
                try:
                    current = os.fstat(root_fd)
                    configured = os.stat(self.root, follow_symlinks=False)
                except OSError as exc:
                    raise EvidenceError(
                        "evidence root changed during write", "EVIDENCE_HASH_MISMATCH"
                    ) from exc
                if (current.st_dev, current.st_ino) != root_identity or (
                    configured.st_dev,
                    configured.st_ino,
                ) != root_identity:
                    raise EvidenceError("evidence root was replaced", "EVIDENCE_HASH_MISMATCH")
                _atomic_create_relative(self.root, relative, content, root_fd=root_fd)
                created.append(relative)
            current = os.fstat(root_fd)
            configured = os.stat(self.root, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != root_identity or (
                configured.st_dev,
                configured.st_ino,
            ) != root_identity:
                raise EvidenceError("evidence root was replaced", "EVIDENCE_HASH_MISMATCH")
        except BaseException as exc:
            # Best-effort rollback protects pre-existing content-addressed objects.
            expected_payloads = dict(writes)
            for relative in reversed(tuple(expected_payloads)):
                if relative in preexisting:
                    continue
                try:
                    descriptor = open_evidence_relative(root_fd, relative)
                    try:
                        expected = expected_payloads[relative]
                        _read_existing_fd(descriptor, expected)
                    finally:
                        os.close(descriptor)
                    self._unlink_relative_fd(root_fd, relative)
                except (OSError, EvidenceError):
                    pass
            if isinstance(exc, KeyboardInterrupt):
                os.close(root_fd)
                raise
            os.close(root_fd)
            raise EvidenceError("evidence publication failed", "EVIDENCE_WRITE_FAILED") from exc
        os.close(root_fd)
        return manifest

    @staticmethod
    def _validate_publishable_batch(batch: ProviderRawBatch) -> None:
        """Close the plan/completion/page/observation joins before any mkdir/write."""
        if batch.failure_class is not None:
            raise EvidenceError("failed provider batch cannot publish", "internal")
        plan = batch.logical_request_plan
        if batch.request_plan_hash != plan.compute_hash():
            raise EvidenceError("logical request plan hash mismatch", "EVIDENCE_HASH_MISMATCH")
        completions = tuple(batch.request_completions)
        if batch.completion_hash != _digest([item.model_dump(mode="json") for item in completions]):
            raise EvidenceError("completion hash mismatch", "EVIDENCE_HASH_MISMATCH")
        if tuple(item.plan_ordinal for item in completions) != tuple(range(plan.request_count)):
            raise EvidenceError(
                "request completion cardinality is invalid", "EVIDENCE_MANIFEST_INVALID"
            )
        observations = tuple(getattr(batch.transport_observations, "observations", ()))

        def stage_value(item: Any) -> str:
            return str(getattr(item.protocol_stage, "value", item.protocol_stage)).lower()

        def outcome_value(item: Any) -> str:
            return str(getattr(item.outcome, "value", item.outcome)).lower()

        observed_by_key = {
            (
                item.refresh_id,
                item.provider_session_id,
                item.request_id,
                item.endpoint,
                item.plan_ordinal,
                item.attempt,
                item.page,
                item.protocol_stage,
            ): item
            for item in observations
        }
        expected_batches: list[tuple[int, int, int]] = []
        for completion in completions:
            if completion.final_outcome.value != "success":
                raise EvidenceError("ultimate failure cannot publish evidence", "internal")
            final = completion.attempts[-1]
            operation = observed_by_key.get(
                (
                    batch.request.refresh_id,
                    final.provider_session_id,
                    final.root_request_id,
                    plan.requests[completion.plan_ordinal].endpoint,
                    completion.plan_ordinal,
                    final.attempt,
                    1,
                    "operation",
                )
            )
            if (
                operation is None
                or operation.observation_digest != final.operation_observation_digest
            ):
                # Enum values are accepted in the key by Pydantic, but model_construct
                # fixtures may carry their string representation.
                operation = next(
                    (
                        item
                        for item in observations
                        if item.refresh_id == batch.request.refresh_id
                        and item.provider_session_id == final.provider_session_id
                        and item.request_id == final.root_request_id
                        and item.plan_ordinal == completion.plan_ordinal
                        and item.attempt == final.attempt
                        and item.page == 1
                        and stage_value(item) == "operation"
                    ),
                    None,
                )
                if (
                    operation is None
                    or operation.observation_digest != final.operation_observation_digest
                ):
                    # Legacy task fixtures may intentionally omit transport aggregates; in
                    # that narrow case the lineage refs still have to be complete.
                    if observations:
                        raise EvidenceError(
                            "operation observation is unbound", "EVIDENCE_MANIFEST_INVALID"
                        )
            for page, page_request_id in final.page_request_ids:
                expected_batches.append((completion.plan_ordinal, final.attempt, page))
                if observations:
                    expected = next(
                        (
                            item
                            for item in observations
                            if item.refresh_id == batch.request.refresh_id
                            and item.provider_session_id == final.provider_session_id
                            and item.request_id == page_request_id
                            and item.plan_ordinal == completion.plan_ordinal
                            and item.attempt == final.attempt
                            and item.page == page
                            and stage_value(item) == "complete"
                            and outcome_value(item) == "success"
                        ),
                        None,
                    )
                    if expected is None or not expected.end_marker_seen:
                        raise EvidenceError(
                            "page observation is unbound", "EVIDENCE_MANIFEST_INVALID"
                        )
        actual_batches = tuple(
            (item.plan_ordinal, item.lineage.attempt, item.lineage.page)
            for item in batch.endpoint_batches
        )
        lineage_by_key = {
            (
                item.refresh_id,
                item.provider_session_id,
                item.root_request_id,
                item.page_request_id,
                item.endpoint,
                item.plan_ordinal,
                item.attempt,
                item.page,
            ): item
            for item in batch.transport_lineage
        }
        for item in batch.endpoint_batches:
            lineage = item.lineage
            bound = lineage_by_key.get(
                (
                    lineage.refresh_id,
                    lineage.provider_session_id,
                    lineage.root_request_id,
                    lineage.page_request_id,
                    lineage.endpoint,
                    lineage.plan_ordinal,
                    lineage.attempt,
                    lineage.page,
                )
            )
            if bound is None or bound.observation_digest != lineage.observation_digest:
                raise EvidenceError("page lineage digest is unbound", "EVIDENCE_HASH_MISMATCH")
            if not any(
                item.observation_digest == lineage.observation_digest
                and stage_value(item) == "complete"
                for item in observations
            ):
                raise EvidenceError("page lineage is not COMPLETE", "EVIDENCE_MANIFEST_INVALID")
        if actual_batches != tuple(sorted(actual_batches)) or len(actual_batches) != len(
            set(actual_batches)
        ):
            raise EvidenceError(
                "evidence pages are unordered or duplicated", "EVIDENCE_MANIFEST_INVALID"
            )
        if any(
            item.plan_ordinal >= len(completions)
            or item.lineage.attempt != completions[item.plan_ordinal].successful_attempt
            or item.lineage.root_request_id
            != completions[item.plan_ordinal].successful_root_request_id
            or item.lineage.page not in completions[item.plan_ordinal].attempts[-1].observed_pages
            for item in batch.endpoint_batches
        ):
            raise EvidenceError(
                "descriptor belongs to a non-final attempt", "EVIDENCE_MANIFEST_INVALID"
            )
        expected_set = tuple(sorted(expected_batches))
        if actual_batches != expected_set:
            raise EvidenceError(
                "evidence page set does not exactly match plan", "EVIDENCE_MANIFEST_INVALID"
            )
        if not observations:
            raise EvidenceError("transport observations are required", "EVIDENCE_MANIFEST_INVALID")

    def read(self, evidence_id: str) -> PublishedEvidence:
        """Return evidence bound to one held root FD; the caller must close it."""
        return EvidenceReader(self.root, max_object_bytes=self.max_object_bytes).read(evidence_id)

    def replay(
        self,
        evidence_id: str,
        *,
        compare_candidate_sha: str | None = None,
        compare_semantic_sha: str | None = None,
        compare_adapter_version: str | None = None,
        adapter: object | None = None,
        factor_cache: object | None = None,
    ) -> ReplayResult:
        reader = EvidenceReader(self.root, max_object_bytes=self.max_object_bytes)
        try:
            return reader.replay(
                evidence_id,
                compare_candidate_sha=compare_candidate_sha,
                compare_semantic_sha=compare_semantic_sha,
                compare_adapter_version=compare_adapter_version,
                adapter=adapter,
                factor_cache=factor_cache,
            )
        finally:
            reader.close()


def _dump_json(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_dump_json(item) for item in value]
    if isinstance(value, list):
        return [_dump_json(item) for item in value]
    if isinstance(value, dict):
        return {key: _dump_json(item) for key, item in value.items()}
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def replay_cli_payload(
    root: Path | str,
    evidence_id: str,
    compare_candidate_sha: str | None = None,
    compare_semantic_sha: str | None = None,
    compare_adapter_version: str | None = None,
) -> dict[str, Any]:
    """Return the strictly bounded public replay projection."""
    result = EvidenceReader(root).replay(
        evidence_id,
        compare_candidate_sha=compare_candidate_sha,
        compare_semantic_sha=compare_semantic_sha,
        compare_adapter_version=compare_adapter_version,
    )
    return result.model_dump(mode="json")


__all__ = [
    "EvidenceError",
    "EvidenceManifest",
    "EvidenceObjectDescriptor",
    "EvidenceReader",
    "EvidenceStore",
    "CacheFactorResolution",
    "FactorResolutionBinding",
    "FactorCacheSnapshotManifest",
    "FactorCacheSnapshotRecord",
    "FactorCacheSnapshotRecords",
    "PublishedEvidence",
    "PublishedFactorCacheSnapshot",
    "ReplayResult",
    "LiveFactorResolution",
    "factor_snapshot_records_from_cache",
    "open_evidence_relative",
    "replay_cli_payload",
]
