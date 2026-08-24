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
from pathlib import Path
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

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
        if self.normalization_clock_utc.tzinfo is None:
            raise ValueError("normalization clock must be timezone-aware")
        return self


class EvidenceManifest(_Immutable):
    evidence_id: SafeIdentifier
    provider_id: ProviderId
    adapter_version: str = R2F2_ADAPTER_VERSION
    endpoint_contract_version: str = R2F2_ENDPOINT_CONTRACT_VERSION
    trade_date: date
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
    factor_resolution: tuple[FactorResolutionBinding, ...] = ()
    factor_resolution_sha256: SafeSha256
    request_count: int = Field(ge=1, le=4096)
    attempt_count: int = Field(ge=1, le=20000)
    failure_class: SafeFailureClass | None = None
    object_count: int = Field(ge=0, le=4096)
    row_count: int = Field(ge=0, le=MAX_ROWS)
    manifest_sha256: SafeSha256

    @model_validator(mode="after")
    def validate_counts(self) -> EvidenceManifest:
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
                or item.schema_hash != factor.schema_hash
                or item.factor_snapshot_provenance_hash != factor.records_sha256
            ):
                raise ValueError("factor descriptor and manifest identity mismatch")
            if not self.factor_resolution:
                raise ValueError("factor snapshot requires a resolution binding")
            for binding in self.factor_resolution:
                cache = getattr(binding, "cache", None)
                if (
                    cache is None
                    or cache.cache_object_id != factor.object_id
                    or cache.cache_object_sha256 != factor.object_sha256
                ):
                    raise ValueError("factor resolution is not descriptor-bound")
        if self.factor_resolution:
            if self.factor_resolution_sha256 != _digest(
                [item.model_dump(mode="json") for item in self.factor_resolution]
            ):
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


def _read_descriptor(root: Path, relative: str, limit: int) -> bytes:
    root_fd = _open_root_dir(root, create=False)
    try:
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
            return bytes(payload)
        finally:
            os.close(descriptor)
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


def _atomic_create_relative(root: Path, relative: str, payload: bytes) -> None:
    """Create a content-addressed object through descriptor-bound directories."""
    parts = _safe_parts(relative)
    root_fd = _open_root_dir(root, create=False)
    parent = os.dup(root_fd)
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
        os.close(root_fd)


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

    def read(self, evidence_id: str) -> PublishedEvidence:
        if "/" in evidence_id or ".." in evidence_id:
            raise EvidenceError("unsafe evidence ID", "EVIDENCE_UNSAFE_PATH")
        payload = _read_descriptor(self.root, f"manifests/{evidence_id}.json", MAX_MANIFEST_BYTES)
        try:
            raw = json.loads(payload.decode("utf-8"))
            if raw.get("manifest_sha256") != _digest(
                {key: value for key, value in raw.items() if key != "manifest_sha256"}
            ):
                raise EvidenceError("evidence manifest hash mismatch", "EVIDENCE_HASH_MISMATCH")
            manifest = EvidenceManifest.model_validate(raw)
        except EvidenceError:
            raise
        except Exception as exc:
            raise EvidenceError("evidence manifest invalid", "EVIDENCE_MANIFEST_INVALID") from exc
        for descriptor in manifest.objects:
            data = _read_descriptor(self.root, descriptor.relative_path, self.max_object_bytes)
            if len(data) != descriptor.byte_count or _sha256_bytes(data) != descriptor.sha256:
                raise EvidenceError("evidence object hash mismatch", "EVIDENCE_HASH_MISMATCH")
            if descriptor.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE:
                self._validate_parquet(data, descriptor)
            else:
                self._validate_factor_snapshot(data, descriptor)
        identity = _digest(manifest.model_dump(mode="json"))
        return PublishedEvidence(
            manifest=manifest, descriptors=manifest.objects, reader_identity=identity
        )

    def read_rows(
        self,
        evidence: PublishedEvidence,
        descriptor: EvidenceObjectDescriptor,
    ) -> tuple[dict[str, Any], ...]:
        """Read one descriptor-bound page for the canonical normalizer seam."""
        if descriptor not in evidence.descriptors:
            raise EvidenceError("descriptor is not bound to evidence", "EVIDENCE_MANIFEST_INVALID")
        data = _read_descriptor(self.root, descriptor.relative_path, self.max_object_bytes)
        if len(data) != descriptor.byte_count or _sha256_bytes(data) != descriptor.sha256:
            raise EvidenceError("evidence object hash mismatch", "EVIDENCE_HASH_MISMATCH")
        if descriptor.object_kind is EvidenceObjectKind.FACTOR_CACHE_SNAPSHOT:
            self._validate_factor_snapshot(data, descriptor)
            raw = json.loads(data.decode("utf-8"))
            return tuple(raw["rows"])
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
        if count != descriptor.row_count or columns != descriptor.fields:
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

    def replay(
        self,
        evidence_id: str,
        *,
        compare_candidate_sha: str | None = None,
        normalizer: object | None = None,
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
        try:
            for descriptor in evidence.descriptors:
                self.read_rows(evidence, descriptor)
            if normalizer is not None:
                normalize = getattr(normalizer, "normalize", None)
                if not callable(normalize) or not isinstance(
                    normalize(evidence), PublishedEvidence
                ):
                    raise EvidenceError(
                        "typed evidence adapter is required", "EVIDENCE_SCHEMA_MISMATCH"
                    )
        except EvidenceError as exc:
            return ReplayResult(
                status="error",
                evidence_id=evidence_id,
                row_count=0,
                failure_class=exc.failure_class,
            )
        except Exception:
            return ReplayResult(
                status="error",
                evidence_id=evidence_id,
                row_count=0,
                failure_class="EVIDENCE_SCHEMA_MISMATCH",
            )
        semantic = _digest(
            {
                "trade_date": manifest.trade_date.isoformat(),
                "universe_id": manifest.universe_id,
                "rows": manifest.row_count,
                "objects": [item.sha256 for item in manifest.objects],
            }
        )
        candidate = manifest.manifest_sha256
        return ReplayResult(
            status="ready",
            evidence_id=evidence_id,
            candidate_sha256=candidate,
            semantic_hash=semantic,
            byte_match=None
            if compare_candidate_sha is None
            else candidate == compare_candidate_sha,
            semantic_match=(
                True if compare_candidate_sha is None else candidate == compare_candidate_sha
            ),
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
        finally:
            os.close(root_fd)

    def publish_factor_snapshot(
        self,
        records: FactorCacheSnapshotRecords,
        *,
        capture_id: str,
        object_id: str | None = None,
    ) -> tuple[FactorCacheSnapshotManifest, EvidenceObjectDescriptor]:
        """Publish one descriptor-bound, immutable factor snapshot object."""
        self._prepare_root()
        payload = _canonical(
            {
                "schema": FACTOR_SCHEMA,
                "rows": [item.model_dump(mode="json") for item in records.rows],
                "records_sha256": records.records_sha256,
            }
        )
        if len(payload) > self.max_object_bytes:
            raise EvidenceError("factor object exceeds bound", "EVIDENCE_OBJECT_OVERSIZE")
        object_sha = _sha256_bytes(payload)
        object_id = object_id or f"factor-{object_sha[:24]}"
        relative = f"objects/{object_sha[:2]}/{object_id}.json"
        _atomic_create_relative(self.root, relative, payload)
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
        return factor, descriptor

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
        self._prepare_root()
        descriptors: list[EvidenceObjectDescriptor] = []
        factor_snapshot = None
        if factor_records is not None:
            if capture_id is None:
                raise EvidenceError(
                    "factor capture identity is required", "EVIDENCE_MANIFEST_INVALID"
                )
            factor_manifest, factor_descriptor = self.publish_factor_snapshot(
                factor_records, capture_id=capture_id
            )
            factor_snapshot = PublishedFactorCacheSnapshot(
                manifest=factor_manifest,
                reader_identity=_digest(factor_manifest.model_dump(mode="json")),
            )
            if not factor_resolution:
                raise EvidenceError(
                    "factor resolution binding is required", "EVIDENCE_MANIFEST_INVALID"
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
            _atomic_create_relative(self.root, relative, payload)
            lineage = item.lineage
            descriptors.append(
                EvidenceObjectDescriptor(
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
            )
        if factor_records is not None:
            descriptors.append(factor_descriptor)
        manifest_id = evidence_id or f"ev-{_digest([item.sha256 for item in descriptors])[:24]}"
        manifest_data = dict(
            evidence_id=manifest_id,
            provider_id=batch.provider_id,
            adapter_version=batch.adapter_version,
            endpoint_contract_version=batch.endpoint_contract_version,
            trade_date=batch.request.trade_date,
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
            factor_resolution=(factor_resolution or getattr(batch, "factor_resolution", ())),
            factor_resolution_sha256=(
                getattr(batch, "factor_resolution_sha256", "0" * 64)
                if not factor_resolution
                else _digest([item.model_dump(mode="json") for item in factor_resolution])
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
        _atomic_create_relative(self.root, f"manifests/{manifest_id}.json", payload)
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
        return EvidenceReader(self.root, max_object_bytes=self.max_object_bytes).read(evidence_id)

    def replay(
        self,
        evidence_id: str,
        *,
        compare_candidate_sha: str | None = None,
        normalizer: object | None = None,
    ) -> ReplayResult:
        return EvidenceReader(self.root, max_object_bytes=self.max_object_bytes).replay(
            evidence_id,
            compare_candidate_sha=compare_candidate_sha,
            normalizer=normalizer,
        )


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
    return value


def replay_cli_payload(
    root: Path | str, evidence_id: str, compare_candidate_sha: str | None = None
) -> dict[str, Any]:
    """Return the strictly bounded public replay projection."""
    result = EvidenceReader(root).replay(evidence_id, compare_candidate_sha=compare_candidate_sha)
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
