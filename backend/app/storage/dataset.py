"""Immutable Parquet dataset publishing and manifest-only reads.

The NAS is deliberately never used as a mutable DuckDB/SQLite database.  The
control database remains local; only checksum-verified Parquet objects and a
small published manifest live under the configured dataset root.
"""

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import threading
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

import duckdb
from pydantic import ValidationError

from backend.app.market.candidates import SessionSelection
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore
from backend.app.storage.models import (
    DatasetManifest,
    DatasetSentinel,
    VerifiedReadySessionInventory,
)

SENTINEL_NAME = ".stock-eva-dataset.json"
MANIFEST_NAME = "manifest.json"
DATASET = "stock-eva-market"
SCHEMA_VERSION = 2
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_R2F2_LINEAGE_FIELDS = (
    "provider_id",
    "universe_id",
    "evidence_id",
    "evidence_sha256",
    "candidate_id",
    "candidate_manifest_sha256",
    "gate_report_sha256",
    "adapter_version",
    "source_schema_version",
)
_MAX_METADATA_BYTES = 1024 * 1024
_READ_CHUNK_BYTES = 1024 * 1024
_VALIDATED_OBJECTS_LIMIT = 1024
_VALIDATED_OBJECTS: OrderedDict[tuple[str, str, int], tuple[int, int, int, int, int]] = (
    OrderedDict()
)
_VALIDATION_LOCK = threading.Lock()
_EXPECTED_PARQUET_TYPES = {
    "trade_date": "DATE",
    "symbol": "VARCHAR",
    "security_type": "VARCHAR",
    "exchange": "VARCHAR",
    "board": "VARCHAR",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "preclose": "DOUBLE",
    "volume": "DOUBLE",
    "amount": "DOUBLE",
    "turnover_rate": "DOUBLE",
    "pct_change": "DOUBLE",
    "adjust_factor": "DOUBLE",
    "price_adjustment": "VARCHAR",
    "is_trading": "BOOLEAN",
    "is_suspended": "BOOLEAN",
    "is_st": "BOOLEAN",
    "source": "VARCHAR",
    "source_record_id": "VARCHAR",
    "ingested_at": "TIMESTAMP WITH TIME ZONE",
    "quality_status": "VARCHAR",
    "quality_issues": "JSON",
}


class DatasetError(ValueError):
    pass


class DatasetPublicationBusy(DatasetError):
    pass


@dataclass(frozen=True)
class PublishedReadSnapshot:
    """One verified manifest/object view, safe to bind to a single read."""

    identity: str
    paths: tuple[Path, ...]
    fingerprints: tuple[tuple[int, int, int, int, int], ...]
    generation: str = ""
    trade_dates: tuple[date, ...] = ()
    manifest_identity: str = ""
    partitions: tuple[tuple[str, date], ...] = ()


class _ManifestLock:
    """Local cross-process lease, matching the refresh lock's nonblocking semantics."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.descriptor: int | None = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise DatasetPublicationBusy("NAS manifest publication already running") from exc
        self.descriptor = descriptor
        return self

    def __exit__(self, *_args) -> None:
        if self.descriptor is not None:
            fcntl.flock(self.descriptor, fcntl.LOCK_UN)
            os.close(self.descriptor)
            self.descriptor = None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stat_fingerprint(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read_bounded_json(path: Path) -> object:
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not nofollow:
        raise DatasetError("published dataset metadata cannot be opened safely")
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | nofollow)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise DatasetError("published dataset metadata cannot be opened safely")
        if before.st_size > _MAX_METADATA_BYTES:
            raise DatasetError("published dataset metadata exceeds size limit")
        chunks: list[bytes] = []
        remaining = _MAX_METADATA_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > _MAX_METADATA_BYTES:
            raise DatasetError("published dataset metadata exceeds size limit")
        after = os.fstat(descriptor)
        if _stat_fingerprint(before) != _stat_fingerprint(after):
            raise DatasetError("published dataset metadata changed during read")
    finally:
        os.close(descriptor)
    return json.loads(payload.decode("utf-8"))


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _safe_relative(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise DatasetError("manifest contains unsafe file path")
    if any(part in {"_staging", "quarantine"} for part in path.parts) or path.suffix != ".parquet":
        raise DatasetError("manifest references a non-published parquet file")
    return path


class DatasetPublication:
    """Five-stage, same-share publication implementation for immutable files."""

    def stage_and_validate(self, source: Path, staging_root: Path) -> tuple[Path, str, int]:
        if not source.is_file() or source.suffix != ".parquet":
            raise DatasetError("staged source must be a parquet file")
        generation_root = staging_root / f"generation-{uuid4().hex}"
        generation_root.mkdir(parents=True, exist_ok=False)
        staged = generation_root / "bars.parquet"
        shutil.copyfile(source, staged)
        checksum = _sha256(staged)
        connection = duckdb.connect(":memory:")
        try:
            rows = int(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(staged)]
                ).fetchone()[0]
            )
            if rows < 1:
                raise DatasetError("staged parquet contains no rows")
        finally:
            connection.close()
        return staged, checksum, rows

    def upload_partial(self, staged: Path, root: Path, generation: str) -> Path:
        partial = root / "_staging" / generation / f"{staged.name}.partial"
        partial.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(staged, partial)
        return partial

    def readback_and_verify(self, partial: Path, checksum: str, expected_rows: int) -> None:
        if _sha256(partial) != checksum:
            raise DatasetError("NAS readback checksum mismatch")
        connection = duckdb.connect(":memory:")
        try:
            rows = int(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(partial)]
                ).fetchone()[0]
            )
        finally:
            connection.close()
        if rows != expected_rows:
            raise DatasetError("NAS readback row count mismatch")

    def publish_by_atomic_rename(self, partial: Path, final: Path, root: Path) -> None:
        partial_in_root = partial.resolve().is_relative_to(root.resolve())
        final_in_root = final.resolve().is_relative_to(root.resolve())
        if not partial_in_root or not final_in_root:
            # Both paths must reside below the same dataset root. This guard
            # rejects accidental cross-volume copy-and-delete publication.
            raise DatasetError("publication paths do not share a dataset root")
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(partial, final)

    def publish_manifest(self, root: Path, manifest: dict[str, object]) -> None:
        _atomic_json(root / MANIFEST_NAME, manifest)


class NasMarketStore:
    """Read market bars from the current manifest while retaining local audit state."""

    def __init__(
        self,
        control: MarketStore,
        root: Path,
        staging_root: Path,
        *,
        manifest_lock_path: Path | None = None,
    ) -> None:
        self.control = control
        self.root = root
        self.staging_root = staging_root
        self.manifest_lock_path = manifest_lock_path or staging_root / "nas-manifest.lock"
        self.publisher = DatasetPublication()

    def _ensure_writable(self) -> None:
        if self.control.read_only:
            raise RuntimeError("read-only market store cannot run writer lifecycle")

    @staticmethod
    def _validate_publication_lineage(lineage: dict[str, object] | None) -> dict[str, object]:
        if lineage is None:
            return {}
        if set(lineage) != set(_R2F2_LINEAGE_FIELDS):
            raise DatasetError("new canonical publication requires complete lineage")
        if any(lineage.get(field) is None for field in _R2F2_LINEAGE_FIELDS):
            raise DatasetError("new canonical publication requires complete lineage")
        if lineage.get("provider_id") != "baostock":
            raise DatasetError("canonical publication provider lineage is unsupported")
        for field in (
            "evidence_sha256",
            "candidate_manifest_sha256",
            "gate_report_sha256",
        ):
            if not isinstance(lineage.get(field), str) or not _SHA256.fullmatch(lineage[field]):
                raise DatasetError("canonical publication lineage hash is invalid")
        return dict(lineage)

    @property
    def path(self) -> Path:
        return self.control.path

    @property
    def temp_directory(self) -> Path:
        return self.control.temp_directory

    def _manifest(self) -> dict[str, object]:
        try:
            sentinel_payload = _read_bounded_json(self.root / SENTINEL_NAME)
            manifest_payload = _read_bounded_json(self.root / MANIFEST_NAME)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise DatasetError("published dataset metadata is unavailable") from exc
        if not isinstance(sentinel_payload, dict) or not isinstance(manifest_payload, dict):
            raise DatasetError("published dataset metadata is unavailable")
        if (
            sentinel_payload.get("dataset") != DATASET
            or sentinel_payload.get("schema_version") != SCHEMA_VERSION
            or manifest_payload.get("dataset") != DATASET
            or manifest_payload.get("schema_version") != SCHEMA_VERSION
        ):
            raise DatasetError("published manifest has an unsupported schema")
        try:
            DatasetSentinel.model_validate(sentinel_payload)
            DatasetManifest.model_validate(manifest_payload)
        except ValidationError as exc:
            raise DatasetError("published dataset metadata is unavailable") from exc
        # Legacy manifests predate R2-F2 provenance and may omit ``source``.
        # Add the validated incumbent identity only to this read projection;
        # the immutable manifest bytes are never rewritten.
        payload = dict(manifest_payload)
        files = [
            (
                {**item, "source": item.get("source", "baostock")}
                if isinstance(item, dict) and "source" not in item
                else item
            )
            for item in manifest_payload["files"]
        ]
        payload["files"] = files
        seen_paths: set[Path] = set()
        seen_partitions: set[tuple[str, date]] = set()
        for item in files:
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                raise DatasetError("published manifest has invalid file metadata")
            relative = _safe_relative(item["path"])
            if not isinstance(item.get("sha256"), str) or not _SHA256.fullmatch(item["sha256"]):
                raise DatasetError("published manifest has invalid checksum metadata")
            if type(item.get("row_count")) is not int or item["row_count"] < 1:
                raise DatasetError("published manifest has invalid row count metadata")
            if item.get("source") != "baostock" or not isinstance(item.get("trade_date"), str):
                raise DatasetError("published manifest has invalid market partition metadata")
            lineage_present = any(field in item for field in _R2F2_LINEAGE_FIELDS)
            if lineage_present:
                self._validate_publication_lineage(
                    {field: item.get(field) for field in _R2F2_LINEAGE_FIELDS}
                )
            try:
                trade_date = date.fromisoformat(item["trade_date"])
            except ValueError as exc:
                raise DatasetError("published manifest has invalid trade date metadata") from exc
            partition = (item["source"], trade_date)
            if relative in seen_paths:
                raise DatasetError("published manifest contains a duplicate path")
            if partition in seen_partitions:
                raise DatasetError("published manifest contains a duplicate partition")
            seen_paths.add(relative)
            seen_partitions.add(partition)
        return payload

    @staticmethod
    def _validate_parquet(
        path: Path,
        expected_rows: int,
        *,
        expected_source: str | None = None,
        expected_trade_date: date | None = None,
    ) -> None:
        connection = duckdb.connect(":memory:")
        try:
            schema = tuple(
                (row[0], row[1])
                for row in connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                    [str(path)],
                ).fetchall()
            )
            if schema != tuple(_EXPECTED_PARQUET_TYPES.items()):
                raise DatasetError("published parquet has an incompatible schema")
            rows = int(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
                    [str(path)],
                ).fetchone()[0]
            )
            if (expected_source is None) != (expected_trade_date is None):
                raise DatasetError("published parquet validation contract is invalid")
            invalid_partition_rows = 0
            if expected_source is not None and expected_trade_date is not None:
                invalid_partition_rows = int(
                    connection.execute(
                        """SELECT count(*)
                           FROM read_parquet(?, hive_partitioning=false)
                           WHERE source IS DISTINCT FROM ?
                              OR trade_date IS DISTINCT FROM ?""",
                        [str(path), expected_source, expected_trade_date],
                    ).fetchone()[0]
                )
        except duckdb.Error as exc:
            raise DatasetError("published parquet cannot be read") from exc
        finally:
            connection.close()
        if rows != expected_rows:
            raise DatasetError("published parquet row count mismatch")
        if invalid_partition_rows:
            raise DatasetError("published parquet partition does not match manifest")

    @staticmethod
    def _fingerprint(path: Path) -> tuple[int, int, int, int, int]:
        value = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(value.st_mode):
            raise DatasetError("published object cannot be opened safely")
        return _stat_fingerprint(value)

    def _open_published_object(self, relative: Path) -> int:
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        directory = getattr(os, "O_DIRECTORY", 0)
        if not nofollow or not directory:
            raise DatasetError("published object cannot be opened safely")
        flags = os.O_RDONLY | os.O_CLOEXEC | nofollow
        directory_descriptor: int | None = None
        try:
            directory_descriptor = os.open(
                self.root.resolve(strict=True),
                flags | directory,
            )
            for component in relative.parts[:-1]:
                next_descriptor = os.open(
                    component,
                    flags | directory,
                    dir_fd=directory_descriptor,
                )
                os.close(directory_descriptor)
                directory_descriptor = next_descriptor
            descriptor = os.open(
                relative.name,
                flags,
                dir_fd=directory_descriptor,
            )
        except FileNotFoundError as exc:
            raise DatasetError("published manifest references a missing parquet file") from exc
        except OSError as exc:
            raise DatasetError("published object cannot be opened safely") from exc
        finally:
            if directory_descriptor is not None:
                os.close(directory_descriptor)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            os.close(descriptor)
            raise DatasetError("published object cannot be opened safely")
        return descriptor

    def _validate_published_object(
        self,
        descriptor: int,
        item: dict[str, object],
        expected_fingerprint: tuple[int, int, int, int, int],
    ) -> None:
        before = os.fstat(descriptor)
        if _stat_fingerprint(before) != expected_fingerprint:
            raise DatasetError("published object changed during validation")
        digest = hashlib.sha256()
        os.lseek(descriptor, 0, os.SEEK_SET)
        for chunk in iter(lambda: os.read(descriptor, _READ_CHUNK_BYTES), b""):
            digest.update(chunk)
        if digest.hexdigest() != item["sha256"]:
            raise DatasetError("published parquet checksum mismatch")
        descriptor_path = Path("/dev/fd") / str(descriptor)
        if not descriptor_path.exists():
            raise DatasetError("published object descriptor validation is unavailable")
        self._validate_parquet(
            descriptor_path,
            int(item["row_count"]),
            expected_source=str(item["source"]),
            expected_trade_date=date.fromisoformat(str(item["trade_date"])),
        )
        after = os.fstat(descriptor)
        if _stat_fingerprint(before) != _stat_fingerprint(after):
            raise DatasetError("published object changed during validation")

    def _read_snapshot(
        self,
        *,
        force_validation: bool = False,
        verify_checksums: bool = False,
    ) -> PublishedReadSnapshot:
        try:
            manifest = self._manifest()
            paths: list[Path] = []
            items: list[dict[str, object]] = manifest["files"]
            for item in items:
                relative = _safe_relative(item["path"])
                path = self.root / relative
                try:
                    if path.resolve(strict=True) != self.root.resolve() / relative:
                        raise DatasetError("manifest contains unsafe file path")
                except FileNotFoundError as exc:
                    raise DatasetError(
                        "published manifest references a missing parquet file"
                    ) from exc
                paths.append(path)
            fingerprints = [self._fingerprint(path) for path in paths]
            validation_needed = []
            for item, path, fingerprint in zip(items, paths, fingerprints, strict=True):
                key = (
                    str(self.root.resolve() / _safe_relative(str(item["path"]))),
                    str(item["sha256"]),
                    int(item["row_count"]),
                )
                with _VALIDATION_LOCK:
                    cached = _VALIDATED_OBJECTS.get(key)
                    if cached == fingerprint and not force_validation:
                        _VALIDATED_OBJECTS.move_to_end(key)
                if force_validation or verify_checksums or cached != fingerprint:
                    validation_needed.append((item, path, fingerprint, key))

            # Hashing and DuckDB validation are deliberately outside the process-global
            # LRU lock. Strict snapshots revalidate every object; ordinary readers
            # validate only unseen or metadata-changed objects.
            for item, _path, fingerprint, key in validation_needed:
                descriptor = self._open_published_object(_safe_relative(str(item["path"])))
                try:
                    self._validate_published_object(descriptor, item, fingerprint)
                finally:
                    os.close(descriptor)
                with _VALIDATION_LOCK:
                    _VALIDATED_OBJECTS[key] = fingerprint
                    _VALIDATED_OBJECTS.move_to_end(key)
                    while len(_VALIDATED_OBJECTS) > _VALIDATED_OBJECTS_LIMIT:
                        _VALIDATED_OBJECTS.popitem(last=False)

            content = json.dumps(manifest, separators=(",", ":"), sort_keys=True)
            manifest_identity = hashlib.sha256(content.encode()).hexdigest()
            partitions = tuple(
                sorted(
                    (str(item["source"]), date.fromisoformat(str(item["trade_date"])))
                    for item in items
                )
            )
            return PublishedReadSnapshot(
                identity=f"{self.root.resolve()}:{manifest_identity}",
                paths=tuple(paths),
                fingerprints=tuple(fingerprints),
                generation=str(manifest["generation"]),
                trade_dates=tuple(sorted({trade_date for _, trade_date in partitions})),
                manifest_identity=manifest_identity,
                partitions=partitions,
            )
        except OSError as exc:
            raise DatasetError("published NAS dataset is unavailable") from exc

    def _paths(self, *, force_validation: bool = False) -> list[Path]:
        return list(self._read_snapshot(force_validation=force_validation).paths)

    def validate_readiness(self) -> None:
        """Force manifest/hash/schema validation before exposing a NAS reader."""
        self._paths(force_validation=True)

    def ensure_readiness(self) -> None:
        """Validate a dataset generation once per process, then reuse that proof."""
        self._paths()

    def _query(self, sql: str, parameters: list[object] | None = None):
        snapshot = self._read_snapshot()
        return self._query_snapshot(snapshot, sql, parameters)

    def _query_snapshot(
        self,
        snapshot: PublishedReadSnapshot,
        sql: str,
        parameters: list[object] | None = None,
    ):
        if not snapshot.paths:
            return [], []
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = duckdb.connect(":memory:")
            cursor = connection.execute(
                f"WITH daily_bars AS (SELECT * FROM read_parquet(?)) {sql}",
                [[str(path) for path in snapshot.paths], *(parameters or [])],
            )
            columns = [item[0] for item in cursor.description]
            rows = cursor.fetchall()
            self._assert_snapshot_unchanged(snapshot)
            return rows, columns
        except (duckdb.Error, OSError) as exc:
            raise DatasetError("published NAS dataset cannot be queried") from exc
        finally:
            if connection is not None:
                connection.close()

    def _assert_snapshot_unchanged(self, snapshot: PublishedReadSnapshot) -> None:
        try:
            current = tuple(self._fingerprint(path) for path in snapshot.paths)
        except OSError as exc:
            raise DatasetError("published snapshot changed during query") from exc
        if current != snapshot.fingerprints:
            raise DatasetError("published snapshot changed during query")

    def read_identity(self) -> str:
        """Return the checked immutable publication identity for read-result caches.

        This deliberately runs the normal manifest and object-presence checks on
        every call. A cache may skip a repeat query, but it must not turn a
        missing/corrupt manifest or missing published object into a stale result.
        """
        return self._read_snapshot(verify_checksums=True).identity

    def read_snapshot(self, *, verify_checksums: bool = False) -> PublishedReadSnapshot:
        """Capture one checked manifest/object view for a bound reader query."""
        return self._read_snapshot(verify_checksums=verify_checksums)

    def verified_ready_session_inventory(
        self,
        source: Literal["baostock"] = "baostock",
        *,
        now: datetime | None = None,
    ) -> VerifiedReadySessionInventory:
        """Return sessions proven by one strict immutable manifest snapshot."""
        if source != "baostock":
            raise DatasetError("published inventory source is unsupported")
        verified_at = now or datetime.now(UTC)
        if verified_at.utcoffset() is None:
            raise DatasetError("inventory verification time must be timezone-aware")
        snapshot = self._read_snapshot(verify_checksums=True)
        try:
            return VerifiedReadySessionInventory(
                manifest_generation=snapshot.generation,
                manifest_identity=snapshot.manifest_identity,
                sessions=tuple(
                    trade_date
                    for partition_source, trade_date in snapshot.partitions
                    if partition_source == source
                ),
                verified_at=verified_at,
            )
        except ValidationError as exc:
            raise DatasetError("published inventory metadata is invalid") from exc

    def exists(self) -> bool:
        return bool(self._paths())

    def latest_refresh(self):
        return self.control.latest_refresh()

    def published_refresh(self):
        return self.control.published_refresh()

    def scheduler_state(self):
        return self.control.scheduler_state()

    def save_scheduler_state(self, state) -> None:
        self._ensure_writable()
        self.control.save_scheduler_state(state)

    def list_refreshes(self, limit: int = 100):
        return self.control.list_refreshes(limit)

    def _daily_bar_from_row(self, row) -> DailyBar:
        return MarketStore._daily_bar_from_row(row)

    def bars_for(self, trade_date, source: str) -> list[dict[str, object]]:
        rows, columns = self._query(
            """
            SELECT symbol, security_type, close, pct_change, amount, is_suspended,
                   quality_status, quality_issues
            FROM daily_bars WHERE trade_date = ? AND source = ? ORDER BY symbol
            """,
            [trade_date, source],
        )
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def available_dates(self, source: str = "baostock") -> list[date]:
        rows, _ = self._query(
            "SELECT DISTINCT trade_date FROM daily_bars WHERE source = ? ORDER BY trade_date",
            [source],
        )
        return [row[0] for row in rows]

    def manifest_dates(self, source: str = "baostock") -> list[date]:
        """Read immutable resume checkpoints without scanning every Parquet file."""
        return sorted(
            date.fromisoformat(item["trade_date"])
            for item in self._manifest()["files"]
            if item["source"] == source
        )

    def reconcile_control_pointer(self, source: str = "baostock") -> RefreshResult | None:
        """Repair a local pointer after a manifest-first publication crash."""
        self._ensure_writable()
        manifest = self._manifest()
        try:
            self.control.initialize_schema()
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise DatasetError("local control schema cannot be initialized") from exc
        entries = [item for item in manifest["files"] if item["source"] == source]
        if not entries:
            return None
        latest = max(entries, key=lambda item: item["trade_date"])
        trade_date = date.fromisoformat(latest["trade_date"])
        current = self.control.published_refresh()
        if current is not None and current.requested_date == trade_date:
            return current

        # Trust only the immutable object that the manifest points to. A local
        # pointer must never be reconstructed from metadata alone.
        path = self.root / _safe_relative(latest["path"])
        if not path.is_file() or _sha256(path) != latest["sha256"]:
            raise DatasetError("published parquet checksum mismatch during reconciliation")
        self._validate_parquet(path, latest["row_count"])

        request_key = f"nas-manifest-reconcile:{source}:{trade_date.isoformat()}:{latest['sha256']}"
        now = datetime.now(UTC)
        result = RefreshResult(
            run_id=hashlib.sha256(request_key.encode()).hexdigest()[:24],
            request_key=request_key,
            run_kind="backfill",
            requested_date=trade_date,
            source=source,
            status="ready",
            requested_count=latest["row_count"],
            succeeded_count=latest["row_count"],
            coverage_ratio=1,
            started_at=now,
            completed_at=now,
        )
        self.control.save_external_publication(result)
        return result

    def canonical_bars(self, trade_date, source: str = "baostock") -> list[DailyBar]:
        rows, _ = self._query(
            """SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                      close, preclose, volume, amount, turnover_rate, pct_change,
                      adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                      source, source_record_id, ingested_at, quality_status, quality_issues
               FROM daily_bars WHERE trade_date = ? AND source = ? ORDER BY symbol""",
            [trade_date, source],
        )
        return [self._daily_bar_from_row(row) for row in rows]

    def symbol_bars(self, symbol, start, end, source: str = "baostock") -> list[DailyBar]:
        if start > end:
            return []
        rows, _ = self._query(
            """SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                      close, preclose, volume, amount, turnover_rate, pct_change,
                      adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                      source, source_record_id, ingested_at, quality_status, quality_issues
               FROM daily_bars WHERE symbol = ? AND trade_date BETWEEN ? AND ? AND source = ?
               ORDER BY trade_date""",
            [symbol, start, end, source],
        )
        return [self._daily_bar_from_row(row) for row in rows]

    def symbols_bars(
        self,
        symbols: list[str],
        start,
        end,
        source: str = "baostock",
    ) -> dict[str, list[DailyBar]]:
        normalized = sorted(set(symbols))
        result = {symbol: [] for symbol in normalized}
        if not normalized or start > end:
            return result
        rows, _ = self._query(
            """SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                      close, preclose, volume, amount, turnover_rate, pct_change,
                      adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                      source, source_record_id, ingested_at, quality_status, quality_issues
               FROM daily_bars WHERE symbol = ANY(?) AND trade_date BETWEEN ? AND ? AND source = ?
               ORDER BY symbol, trade_date""",
            [normalized, start, end, source],
        )
        for row in rows:
            result[row[1]].append(self._daily_bar_from_row(row))
        return result

    def present_points(
        self,
        trading_dates: list,
        symbols: list[str],
        source: str = "baostock",
    ) -> set[tuple]:
        if not trading_dates or not symbols:
            return set()
        rows, _ = self._query(
            """SELECT trade_date, symbol FROM daily_bars
               WHERE trade_date = ANY(?) AND symbol = ANY(?) AND source = ?""",
            [list(dict.fromkeys(trading_dates)), sorted(set(symbols)), source],
        )
        return set(rows)

    def _publish_bars(
        self,
        bars: list[DailyBar],
        *,
        publication_lineage: dict[str, object] | None = None,
    ) -> None:
        self._ensure_writable()
        if not bars:
            raise DatasetError("cannot publish an empty bar set")
        by_date: dict[tuple[date, str], list[DailyBar]] = defaultdict(list)
        for bar in bars:
            by_date[(bar.trade_date, bar.source)].append(bar)
        lineage = self._validate_publication_lineage(publication_lineage)
        with _ManifestLock(self.manifest_lock_path):
            baseline = self._manifest()
            baseline_generation = baseline["generation"]
            self._publish_locked(by_date, baseline_generation, lineage=lineage)

    def _publish_locked(
        self,
        by_date: dict[tuple[date, str], list[DailyBar]],
        baseline_generation: object,
        *,
        lineage: dict[str, object] | None = None,
    ) -> None:
        staged_entries: list[tuple[Path, Path, Path, dict[str, object]]] = []
        scratch_paths: list[Path] = []
        try:
            # All partitions must complete local staging, NAS upload and
            # readback before there is any published-manifest mutation.
            for (trade_date, source), incoming in sorted(by_date.items()):
                existing = self.canonical_bars(trade_date, source)
                merged = {(bar.symbol, bar.source): bar for bar in existing}
                merged.update({(bar.symbol, bar.source): bar for bar in incoming})
                scratch = self.staging_root / f"scratch-{uuid4().hex}.duckdb"
                scratch_paths.append(scratch)
                scratch_store = MarketStore(
                    scratch,
                    temp_directory=self.staging_root / "duckdb-tmp",
                )
                scratch_store.upsert_bars(list(merged.values()))
                exported = scratch_store.export_date(
                    trade_date, self.staging_root / "exports", source
                )
                staged, checksum, row_count = self.publisher.stage_and_validate(
                    exported, self.staging_root
                )
                generation = f"generation-{trade_date:%Y%m%d}-{checksum[:12]}"
                partial = self.publisher.upload_partial(staged, self.root, generation)
                self.publisher.readback_and_verify(partial, checksum, row_count)
                relative = (
                    Path("bars")
                    / f"source={source}"
                    / f"year={trade_date:%Y}"
                    / f"month={trade_date:%m}"
                    / f"date={trade_date.isoformat()}_{checksum[:12]}.parquet"
                )
                staged_entries.append(
                    (
                        staged,
                        partial,
                        self.root / relative,
                        {
                            "path": str(relative),
                            "sha256": checksum,
                            "trade_date": trade_date.isoformat(),
                            "source": source,
                            "row_count": row_count,
                            **(lineage or {}),
                        },
                    )
                )

            # Rename is same-share/same-root. If interrupted before the final
            # manifest update, renamed files are unreferenced and cannot leak.
            for _staged, partial, final, _entry in staged_entries:
                self.publisher.publish_by_atomic_rename(partial, final, self.root)
            manifest = self._manifest()
            if manifest["generation"] != baseline_generation:
                raise DatasetError("published manifest changed during staging")
            replaced = {
                (entry["trade_date"], entry["source"])
                for _staged, _partial, _final, entry in staged_entries
            }
            old_files = [
                item
                for item in manifest["files"]
                if (item.get("trade_date"), item.get("source")) not in replaced
            ]
            old_files.extend(entry for _staged, _partial, _final, entry in staged_entries)
            manifest["generation"] = f"generation-{uuid4().hex}"
            manifest["files"] = sorted(old_files, key=lambda item: str(item["path"]))
            self.publisher.publish_manifest(self.root, manifest)
            for _staged, partial, _final, _entry in staged_entries:
                partial.parent.rmdir()
        finally:
            shutil.rmtree(self.staging_root / "exports", ignore_errors=True)
            for staged, _partial, _final, _entry in staged_entries:
                shutil.rmtree(staged.parent, ignore_errors=True)
            for scratch in scratch_paths:
                scratch.unlink(missing_ok=True)
                scratch.with_name(f"{scratch.name}.wal").unlink(missing_ok=True)

    def upsert_bars(self, bars: list[DailyBar]) -> None:
        self._publish_bars(bars)

    def save_refresh(
        self,
        bars: list[DailyBar],
        result: RefreshResult,
        *,
        publish: bool | None = None,
        publication_lineage: dict[str, object] | None = None,
        selection: SessionSelection | None = None,
    ) -> None:
        self._ensure_writable()
        publish = result.status == "ready" if publish is None else publish
        if publish:
            MarketStore._validate_ready_publication(result)
            MarketStore._validate_local_publication_bars(bars, result)
            if publication_lineage is not None:
                lineage = self._validate_publication_lineage(publication_lineage)
                if selection is None:
                    raise DatasetError("canonical pointer requires a verified selection")
                if (
                    selection.evidence_sha256 != lineage["evidence_sha256"]
                    or selection.candidate_manifest_sha256 != lineage["candidate_manifest_sha256"]
                    or selection.gate_report_sha256 != lineage["gate_report_sha256"]
                    or selection.selected_provider_id.value != lineage["provider_id"]
                    or selection.trade_date != result.requested_date
                ):
                    raise DatasetError("canonical selection lineage does not match publication")
            self._publish_bars(bars, publication_lineage=publication_lineage)
            self.control.save_external_publication(result)
        else:
            self.control.save_refresh([], result, publish=False)

    def export_date(self, trade_date, output_root: Path, source: str = "baostock") -> Path:
        """Export a published partition locally without modifying the NAS dataset."""
        self._ensure_writable()
        # Export has the same trust boundary as read APIs: never copy a file
        # merely because its path appears in a manifest.
        self._paths()
        manifest = self._manifest()
        matches = [
            item
            for item in manifest["files"]
            if item.get("trade_date") == trade_date.isoformat() and item.get("source") == source
        ]
        if len(matches) != 1:
            raise DatasetError("exactly one published partition is required for export")
        source_path = self.root / _safe_relative(str(matches[0]["path"]))
        output = output_root / f"date={trade_date.isoformat()}" / "bars.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, output)
        return output
