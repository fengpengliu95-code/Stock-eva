"""Durable, isolated shadow outbox and lease state machine."""

# SQL statements are kept as reviewable one-line projections of the frozen DDL.
# ruff: noqa: E501

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import secrets
import sqlite3
import stat
from collections import deque
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from .providers.registry import ShadowRegistry
from .shadow_calendar import ConfirmedSessionSnapshot
from .shadow_evidence import ShadowAttemptReport

_CANONICAL_PARQUET_SCHEMA = (
    ("trade_date", "DATE"),
    ("symbol", "VARCHAR"),
    ("security_type", "VARCHAR"),
    ("exchange", "VARCHAR"),
    ("board", "VARCHAR"),
    ("open", "DOUBLE"),
    ("high", "DOUBLE"),
    ("low", "DOUBLE"),
    ("close", "DOUBLE"),
    ("preclose", "DOUBLE"),
    ("volume", "DOUBLE"),
    ("amount", "DOUBLE"),
    ("turnover_rate", "DOUBLE"),
    ("pct_change", "DOUBLE"),
    ("adjust_factor", "DOUBLE"),
    ("price_adjustment", "VARCHAR"),
    ("is_trading", "BOOLEAN"),
    ("is_suspended", "BOOLEAN"),
    ("is_st", "BOOLEAN"),
    ("source", "VARCHAR"),
    ("source_record_id", "VARCHAR"),
    ("ingested_at", "TIMESTAMP WITH TIME ZONE"),
    ("quality_status", "VARCHAR"),
    ("quality_issues", "JSON"),
)
_SAFE_GENERATION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ShadowJobUnavailable(RuntimeError):
    """The shadow outbox is unavailable; callers must preserve canonical outcome."""


class ShadowJob(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job_id: str
    provider_id: str
    window_id: str
    trade_date: str
    universe_id: str
    canonical_manifest_generation: str
    canonical_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    version_vector_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    run_status: str
    lease_owner: str | None = None
    lease_expires_at: str | None = None
    attempt_count: int = Field(ge=0)
    state_version: int = Field(ge=0)
    successful_evidence_sha256: str | None = None
    successful_candidate_sha256: str | None = None
    completion_sha256: str | None = None
    terminal_attestation_id: str | None = None


class ShadowSessionReport(BaseModel):
    """Immutable sanitized session projection used by the qualification window."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    session_report_id: str
    provider_id: str
    job_id: str
    window_id: str
    session_id: str
    trade_date: str
    outcome: str
    calendar_generation: str
    calendar_sha256: str
    universe_sha256: str
    version_vector_sha256: str
    report_ref: str
    report_sha256: str
    report_version: int = 1


class CanonicalManifestUnavailable(RuntimeError):
    """Published canonical manifest did not satisfy the scanner contract."""


class CanonicalOutcomeScanner:
    """Descriptor-bound scanner for the canonical DatasetManifest only."""

    def __init__(
        self,
        root: Path | str,
        *,
        shadow_start_date: date,
        shadow_end_date: date | None = None,
        provider_id: str,
        window_id: str,
    ) -> None:
        self.root = Path(root)
        self.shadow_start_date = shadow_start_date
        self.shadow_end_date = shadow_end_date
        self.provider_id = provider_id
        self.window_id = window_id

    def _open_relative(self, relative: Path) -> int:
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise CanonicalManifestUnavailable("canonical object unavailable")
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        directory = getattr(os, "O_DIRECTORY", 0)
        if not nofollow or not directory:
            raise CanonicalManifestUnavailable("canonical object unavailable")
        root_fd = -1
        try:
            root_fd = os.open(os.sep, os.O_RDONLY | directory | nofollow | os.O_CLOEXEC)
            for component in self.root.parts[1:]:
                next_fd = os.open(
                    component,
                    os.O_RDONLY | directory | nofollow | os.O_CLOEXEC,
                    dir_fd=root_fd,
                )
                os.close(root_fd)
                root_fd = next_fd
        except OSError as exc:
            if root_fd >= 0:
                os.close(root_fd)
            raise CanonicalManifestUnavailable("canonical object unavailable") from exc
        current_fd = root_fd
        try:
            for component in relative.parts[:-1]:
                next_fd = os.open(
                    component,
                    os.O_RDONLY | directory | nofollow | os.O_CLOEXEC,
                    dir_fd=current_fd,
                )
                os.close(current_fd)
                current_fd = next_fd
            return os.open(
                relative.parts[-1],
                os.O_RDONLY | nofollow | os.O_CLOEXEC,
                dir_fd=current_fd,
            )
        except OSError as exc:
            raise CanonicalManifestUnavailable("canonical object unavailable") from exc
        finally:
            if current_fd != root_fd:
                os.close(current_fd)
            else:
                os.close(root_fd)

    @staticmethod
    def _read_fd(fd: int) -> tuple[bytes, tuple[int, int, int, int], str]:
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode):
                raise CanonicalManifestUnavailable("canonical manifest unavailable")
            raw = os.pread(fd, 1_048_577, 0)
            after = os.fstat(fd)
            before_fp = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
            after_fp = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            if len(raw) > 1_048_576 or before_fp != after_fp:
                raise CanonicalManifestUnavailable("canonical manifest unavailable")
            return raw, before_fp, hashlib.sha256(raw).hexdigest()
        except OSError as exc:
            raise CanonicalManifestUnavailable("canonical manifest unavailable") from exc

    def _read(self, relative: Path) -> tuple[bytes, tuple[int, int, int, int]]:
        fd = self._open_relative(relative)
        try:
            raw, fingerprint, _ = self._read_fd(fd)
            return raw, fingerprint
        finally:
            os.close(fd)

    def _validate_object(self, relative: Path, item: dict[str, Any], trade_date: date) -> None:
        fd = self._open_relative(relative)
        try:
            raw, _, digest = self._read_fd(fd)
            if digest != item["sha256"]:
                raise CanonicalManifestUnavailable("canonical object checksum mismatch")
            query_fd = os.dup(fd)
            try:
                connection = duckdb.connect(":memory:")
                try:
                    descriptor = f"/dev/fd/{query_fd}"
                    schema = tuple(
                        (row[0], row[1])
                        for row in connection.execute(
                            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                            [descriptor],
                        ).fetchall()
                    )
                    if schema != _CANONICAL_PARQUET_SCHEMA:
                        raise CanonicalManifestUnavailable("canonical object schema mismatch")
                    row_count = int(
                        connection.execute(
                            "SELECT count(*) FROM read_parquet(?, hive_partitioning=false)",
                            [descriptor],
                        ).fetchone()[0]
                    )
                    if row_count != item["row_count"] or row_count < 1:
                        raise CanonicalManifestUnavailable("canonical object row count mismatch")
                    invalid = int(
                        connection.execute(
                            """SELECT count(*) FROM read_parquet(?, hive_partitioning=false)
                               WHERE source IS DISTINCT FROM ? OR trade_date IS DISTINCT FROM ?""",
                            [descriptor, "baostock", trade_date],
                        ).fetchone()[0]
                    )
                    if invalid:
                        raise CanonicalManifestUnavailable("canonical object partition mismatch")
                finally:
                    connection.close()
            except duckdb.Error as exc:
                raise CanonicalManifestUnavailable("canonical object cannot be read") from exc
            finally:
                os.close(query_fd)
            if len(raw) == 0:
                raise CanonicalManifestUnavailable("canonical object is empty")
        finally:
            os.close(fd)

    def scan(self) -> tuple[dict[str, Any], ...]:
        if self.provider_id not in {"tickflow", "tushare"}:
            return ()
        try:
            current = Path(self.root.anchor)
            for part in self.root.parts[1:]:
                current /= part
                if stat.S_ISLNK(os.lstat(current).st_mode):
                    return ()
        except OSError:
            return ()
        try:
            sentinel_raw, _ = self._read(Path(".stock-eva-dataset.json"))
            manifest_raw, before = self._read(Path("manifest.json"))
            _after_raw, after = self._read(Path("manifest.json"))
        except CanonicalManifestUnavailable:
            return ()
        if before != after:
            return ()
        try:
            sentinel = json.loads(sentinel_raw)
            manifest = json.loads(manifest_raw)
        except (UnicodeError, json.JSONDecodeError):
            return ()
        if not isinstance(sentinel, dict) or sentinel != {
            "dataset": "stock-eva-market",
            "schema_version": 2,
        }:
            return ()
        if not isinstance(manifest, dict) or set(manifest) != {
            "dataset",
            "schema_version",
            "generation",
            "files",
        }:
            return ()
        if (
            manifest.get("dataset") != "stock-eva-market"
            or manifest.get("schema_version") != 2
            or not isinstance(manifest.get("generation"), str)
            or _SAFE_GENERATION.fullmatch(manifest.get("generation", "")) is None
            or not isinstance(manifest.get("files"), list)
        ):
            return ()
        manifest_sha = hashlib.sha256(manifest_raw).hexdigest()
        files = manifest["files"]
        if not files:
            return ()
        results: list[dict[str, Any]] = []
        for item in files:
            if not isinstance(item, dict) or item.get("source") != "baostock":
                return ()
            required = {"path", "sha256", "row_count", "trade_date", "source"}
            if not required <= set(item):
                return ()
            try:
                trade_date = date.fromisoformat(str(item["trade_date"]))
            except ValueError:
                return ()
            if (
                trade_date < self.shadow_start_date
                or (self.shadow_end_date is not None and trade_date > self.shadow_end_date)
                or not isinstance(item["path"], str)
                or not isinstance(item["sha256"], str)
                or len(item["sha256"]) != 64
                or item["sha256"] != item["sha256"].lower()
                or any(char not in "0123456789abcdef" for char in item["sha256"])
                or not isinstance(item["row_count"], int)
                or item["row_count"] < 1
                or Path(item["path"]).suffix.lower() != ".parquet"
                or not isinstance(item.get("source_schema_version"), str)
                or not item["source_schema_version"]
            ):
                return ()
            relative = Path(item["path"])
            if relative.is_absolute() or ".." in relative.parts:
                return ()
            expected_path = (
                Path("bars")
                / "source=baostock"
                / f"year={trade_date:%Y}"
                / f"month={trade_date:%m}"
            )
            if relative.parent != expected_path or relative.name != (
                f"date={trade_date.isoformat()}_{item['sha256'][:12]}.parquet"
            ):
                return ()
            try:
                parent = self.root
                for part in relative.parts[:-1]:
                    parent /= part
                    if stat.S_ISLNK(os.lstat(parent).st_mode):
                        return ()
            except OSError:
                return ()
            try:
                self._validate_object(relative, item, trade_date)
            except CanonicalManifestUnavailable:
                return ()
            lineage = {
                key: item.get(key)
                for key in (
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
            }
            if lineage["provider_id"] != "baostock" or any(
                not isinstance(value, str) or not value for value in lineage.values()
            ):
                return ()
            if any(
                not _SHA256.fullmatch(lineage[field])
                for field in (
                    "evidence_sha256",
                    "candidate_manifest_sha256",
                    "gate_report_sha256",
                )
            ):
                return ()
            universe_id = str(lineage["universe_id"])
            key_bytes = f"stock-eva/r2f3/shadow-job/v1\n{self.provider_id}|{self.window_id}|{trade_date.isoformat()}|{universe_id}|{manifest['generation']}|{manifest_sha}".encode()
            results.append(
                {
                    "job_id": hashlib.sha256(key_bytes).hexdigest()[:32],
                    "provider_id": self.provider_id,
                    "window_id": self.window_id,
                    "trade_date": trade_date.isoformat(),
                    "universe_id": universe_id,
                    "canonical_manifest_generation": manifest["generation"],
                    "canonical_manifest_sha256": manifest_sha,
                    "version_vector_sha256": hashlib.sha256(
                        json.dumps(lineage, sort_keys=True, separators=(",", ":")).encode()
                    ).hexdigest(),
                    "files": (item,),
                }
            )
        return tuple(results)

    def enqueue(self, job_store, *, limit: int = 64) -> int:
        """Scan the canonical descriptor, then enqueue only validated identities."""
        if type(job_store) is not ShadowJobStore:
            raise TypeError("canonical scanner requires ShadowJobStore")
        descriptors = self.scan()[: max(0, limit)]
        return job_store._enqueue_canonical_descriptors(descriptors)


class ShadowBundlePublisher:
    """No-follow, locked, atomic report bundle publisher."""

    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.lock_path = self.root / "bundles.lock"

    @staticmethod
    def _open_child_directory(parent_fd: int, name: str) -> int:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            return os.open(name, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            try:
                os.mkdir(name, 0o700, dir_fd=parent_fd)
            except FileExistsError:
                pass
            return os.open(name, flags, dir_fd=parent_fd)

    @staticmethod
    def _write_new_file(directory_fd: int, name: str, payload: bytes) -> None:
        fd = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=directory_fd,
        )
        try:
            offset = 0
            while offset < len(payload):
                written = os.write(fd, payload[offset:])
                if written <= 0:
                    raise OSError("shadow bundle short write")
                offset += written
            os.fsync(fd)
            checked = os.fstat(fd)
            if not stat.S_ISREG(checked.st_mode) or checked.st_size != len(payload):
                raise OSError("shadow bundle file changed")
        finally:
            os.close(fd)

    def _open_directory_chain(self) -> tuple[int, int, int, int]:
        if not self.root.is_absolute():
            raise ShadowJobUnavailable("shadow bundle root unavailable")
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        current = os.open(os.sep, flags)
        bundles_fd = staging_fd = lock_fd = -1
        try:
            for component in self.root.parts[1:]:
                next_fd = self._open_child_directory(current, component)
                os.close(current)
                current = next_fd
            bundles_fd = self._open_child_directory(current, "bundles")
            staging_fd = self._open_child_directory(current, "staging")
            lock_fd = os.open(
                "bundles.lock",
                os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
                dir_fd=current,
            )
            return current, bundles_fd, staging_fd, lock_fd
        except Exception:
            for descriptor in (lock_fd, staging_fd, bundles_fd):
                if descriptor >= 0:
                    os.close(descriptor)
            os.close(current)
            raise

    def publish(self, identity: str, payload: dict[str, Any]) -> Path:
        if not self.root.is_absolute() or not identity or "/" in identity or "\\" in identity:
            raise ShadowJobUnavailable("shadow bundle root unavailable")
        current = Path(self.root.anchor)
        for part in self.root.parts[1:]:
            current /= part
            try:
                if stat.S_ISLNK(os.lstat(current).st_mode):
                    raise ShadowJobUnavailable("shadow bundle root unavailable")
            except FileNotFoundError:
                continue
        root_fd, bundles_fd, staging_fd, lock_fd = self._open_directory_chain()
        bundles = self.root / "bundles"
        staging_root = self.root / "staging"
        nonce = f"{identity}-{secrets.token_hex(8)}"
        staging = staging_root / nonce
        destination = bundles / identity
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            if destination.exists():
                try:
                    if stat.S_ISLNK(os.lstat(destination).st_mode):
                        raise ShadowJobUnavailable("shadow bundle identity conflict")
                except FileNotFoundError:
                    pass
                marker = destination / "COMMIT"
                raw = (
                    json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                    + "\n"
                )
                expected = hashlib.sha256(raw.encode()).hexdigest() + "\n"
                try:
                    marker_fd = os.open(
                        marker,
                        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
                    )
                    try:
                        marker_raw = os.read(marker_fd, 128).decode()
                    finally:
                        os.close(marker_fd)
                except OSError as exc:
                    raise ShadowJobUnavailable("shadow bundle identity conflict") from exc
                if marker_raw != expected:
                    raise ShadowJobUnavailable("shadow bundle identity conflict")
                return destination
            os.mkdir(staging.name, 0o700, dir_fd=staging_fd)
            raw = (
                json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode()
            marker = hashlib.sha256(raw).hexdigest() + "\n"
            staging_dir_fd = os.open(
                staging.name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=staging_fd,
            )
            try:
                self._write_new_file(staging_dir_fd, "report.json", raw)
                self._write_new_file(staging_dir_fd, "COMMIT", marker.encode())
                os.fsync(staging_dir_fd)
            finally:
                os.close(staging_dir_fd)
            try:
                import ctypes

                libc = ctypes.CDLL(None, use_errno=True)
                renameatx_np = libc.renameatx_np
                parent_fd = os.dup(bundles_fd)
                source_fd = os.dup(staging_fd)
                try:
                    if (
                        renameatx_np(
                            source_fd,
                            staging.name.encode(),
                            parent_fd,
                            destination.name.encode(),
                            0x00000004,
                        )
                        != 0
                    ):
                        raise OSError(ctypes.get_errno(), "exclusive bundle rename failed")
                finally:
                    os.close(source_fd)
                    os.close(parent_fd)
            except AttributeError:
                if destination.exists():
                    raise ShadowJobUnavailable("shadow bundle identity conflict") from None
                os.rename(staging, destination)
            except OSError as exc:
                if destination.exists() and not stat.S_ISLNK(os.lstat(destination).st_mode):
                    marker = destination / "COMMIT"
                    expected = hashlib.sha256(raw).hexdigest() + "\n"
                    try:
                        marker_fd = os.open(
                            marker,
                            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
                        )
                        try:
                            marker_raw = os.read(marker_fd, 128).decode()
                        finally:
                            os.close(marker_fd)
                    except OSError:
                        marker_raw = ""
                    if marker_raw == expected:
                        return destination
                raise ShadowJobUnavailable("shadow bundle publish unavailable") from exc
            dir_fd = os.open(bundles, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
            return destination
        finally:
            if staging.exists():
                for path in staging.iterdir():
                    path.unlink()
                staging.rmdir()
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
            os.close(staging_fd)
            os.close(bundles_fd)
            os.close(root_fd)


class ShadowOutcomeReport(BaseModel):
    """Sanitized immutable scheduler-run projection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    report_version: int = 1
    job_id: str | None = None
    status: str
    state_version: int | None = None
    reason_code: str | None = None


class ShadowOutcomeReporter:
    """Publish every scheduler exit through the sole immutable bundle publisher."""

    _STATUSES = frozenset(
        {
            "idle",
            "busy",
            "cancelled",
            "no_worker",
            "worker_exception",
            "missing_context",
            "success",
            "failure",
            "budget_exhausted",
            "unavailable",
            "pending",
        }
    )

    def __init__(self, publisher: ShadowBundlePublisher):
        if type(publisher) is not ShadowBundlePublisher:
            raise TypeError("shadow outcome reporter requires ShadowBundlePublisher")
        self.publisher = publisher

    def publish(
        self,
        *,
        status: str,
        job_id: str | None = None,
        state_version: int | None = None,
        reason_code: str | None = None,
    ) -> Path:
        if status not in self._STATUSES:
            raise ShadowJobUnavailable("shadow outcome status unavailable")
        report = ShadowOutcomeReport(
            job_id=job_id,
            status=status,
            state_version=state_version,
            reason_code=reason_code,
        )
        identity = "outcome-{}-{}-{}".format(
            job_id or "run",
            state_version if state_version is not None else "none",
            status,
        )
        return self.publisher.publish(identity, report.model_dump(mode="json"))


def _safe(value: str) -> str:
    if (
        not value
        or len(value) > 160
        or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
            for c in value
        )
    ):
        raise ValueError("shadow identity is invalid")
    return value


class ShadowJobStore:
    """Registry-backed outbox.  It never touches canonical storage."""

    def __init__(self, registry: ShadowRegistry, shadow_root: Path | str | None = None):
        if type(registry) is not ShadowRegistry:
            raise TypeError("shadow job store requires ShadowRegistry")
        self.registry = registry
        self.shadow_root = Path(shadow_root) if shadow_root is not None else None

    def _row(self, connection: sqlite3.Connection, job_id: str) -> ShadowJob | None:
        row = connection.execute(
            "SELECT job_id,provider_id,window_id,trade_date,universe_id,canonical_manifest_generation,canonical_manifest_sha256,version_vector_sha256,run_status,lease_owner,lease_expires_at,attempt_count,state_version,successful_evidence_sha256,successful_candidate_sha256,completion_sha256,terminal_attestation_id FROM shadow_job WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        fields = (
            "job_id",
            "provider_id",
            "window_id",
            "trade_date",
            "universe_id",
            "canonical_manifest_generation",
            "canonical_manifest_sha256",
            "version_vector_sha256",
            "run_status",
            "lease_owner",
            "lease_expires_at",
            "attempt_count",
            "state_version",
            "successful_evidence_sha256",
            "successful_candidate_sha256",
            "completion_sha256",
            "terminal_attestation_id",
        )
        return ShadowJob.model_validate(dict(zip(fields, row, strict=True)))

    def get(self, job_id: str) -> ShadowJob | None:
        with self.registry._lock(shared=True):
            connection = self.registry._connection_for_read()
            try:
                return self._row(connection, _safe(job_id))
            finally:
                if not self.registry._memory:
                    connection.close()

    def enqueue(
        self,
        *,
        job_id: str,
        provider_id: str,
        window_id: str,
        trade_date: str,
        universe_id: str,
        canonical_manifest_generation: str,
        canonical_manifest_sha256: str,
        version_vector_sha256: str,
    ) -> ShadowJob:
        values = (
            _safe(job_id),
            _safe(provider_id),
            _safe(window_id),
            trade_date,
            _safe(universe_id),
            _safe(canonical_manifest_generation),
            canonical_manifest_sha256,
            version_vector_sha256,
        )

        def save(connection):
            connection.execute(
                "INSERT OR IGNORE INTO shadow_job (job_id,provider_id,window_id,trade_date,universe_id,canonical_manifest_generation,canonical_manifest_sha256,version_vector_sha256,run_status,lease_owner,lease_expires_at,attempt_count,state_version,successful_evidence_sha256,successful_candidate_sha256,completion_sha256,terminal_attestation_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (*values, "pending", None, None, 0, 0, None, None, None, None),
            )
            row = self._row(connection, job_id)
            if row is None:
                raise ShadowJobUnavailable("shadow job unavailable")
            return row

        return self.registry._with_transaction(save)

    create = enqueue

    def lease(
        self, job_id: str, *, owner: str, now: datetime | None = None, lease_seconds: int = 300
    ) -> ShadowJob:
        owner = _safe(owner)
        instant = now or datetime.now(UTC)
        expires = instant + timedelta(seconds=max(1, lease_seconds))

        def acquire(connection):
            row = self._row(connection, _safe(job_id))
            if row is None or row.run_status not in {"pending", "leased"}:
                raise ShadowJobUnavailable("shadow job is not leasable")
            if (
                row.run_status == "leased"
                and row.lease_expires_at
                and row.lease_expires_at > instant.isoformat()
            ):
                raise ShadowJobUnavailable("shadow job is leased")
            cursor = connection.execute(
                "UPDATE shadow_job SET run_status='leased',lease_owner=?,lease_expires_at=?,attempt_count=attempt_count+1,state_version=state_version+1 WHERE job_id=? AND state_version=? AND run_status IN ('pending','leased')",
                (owner, expires.isoformat(), job_id, row.state_version),
            )
            if cursor.rowcount != 1:
                raise ShadowJobUnavailable("shadow job lease conflict")
            return self._row(connection, job_id)

        return self.registry._with_transaction(acquire)

    acquire_lease = lease

    def reclaim_expired(self, *, now: datetime | None = None, limit: int = 64) -> int:
        instant = (now or datetime.now(UTC)).isoformat()

        def recover(connection):
            rows = connection.execute(
                "SELECT job_id,state_version FROM shadow_job WHERE run_status='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at<=? ORDER BY job_id LIMIT ?",
                (instant, max(0, limit)),
            ).fetchall()
            changed = 0
            for job_id, version in rows:
                changed += connection.execute(
                    "UPDATE shadow_job SET run_status='pending',lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND state_version=? AND run_status='leased'",
                    (job_id, version),
                ).rowcount
            return changed

        return self.registry._with_transaction(recover)

    recover_expired_leases = reclaim_expired

    def cancel(self, job_id: str, *, expected_state_version: int) -> ShadowJob:
        def update(connection):
            cursor = connection.execute(
                "UPDATE shadow_job SET run_status='cancelled',lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND state_version=? AND run_status IN ('pending','leased','pending_normalization')",
                (_safe(job_id), expected_state_version),
            )
            if cursor.rowcount != 1:
                raise ShadowJobUnavailable("shadow job state conflict")
            return self._row(connection, job_id)

        return self.registry._with_transaction(update)

    def release_after_worker(self, job: ShadowJob, *, status: str) -> ShadowJob | None:
        """CAS-release a leased job on every worker exit; never leave a stale lease."""
        if status not in {"pending", "failed", "cancelled", "unavailable"}:
            raise ValueError("invalid shadow worker release status")

        def update(connection):
            cursor = connection.execute(
                "UPDATE shadow_job SET run_status=?,lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND provider_id=? AND window_id=? AND run_status='leased' AND state_version=?",
                (status, job.job_id, job.provider_id, job.window_id, job.state_version),
            )
            if cursor.rowcount != 1:
                return self._row(connection, job.job_id)
            return self._row(connection, job.job_id)

        return self.registry._with_transaction(update)

    def persist_outcome(
        self,
        *,
        plan,
        session_id: str,
        outcome: str,
        trade_date: str,
        calendar_generation: str,
        calendar_sha256: str,
        universe_sha256: str,
        version_vector_sha256: str,
        expected_job_state_version: int | None = None,
        expected_window_state_version: int | None = None,
        failure_class: str | None = None,
        snapshot: ConfirmedSessionSnapshot | None = None,
        bundle_publisher: ShadowBundlePublisher | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> tuple[ShadowAttemptReport, ...]:
        """Persist one complete sanitized failure graph and atomically reset the window.

        Success/evidence/candidate transitions remain owned by Task 11/12 and the
        terminal writer.  This method deliberately refuses to attach any object for
        non-success outcomes.
        """
        if outcome not in {"failure", "skip", "unavailable", "mismatch"}:
            raise ShadowJobUnavailable("shadow outcome requires terminal writer")
        if type(snapshot) is not ConfirmedSessionSnapshot:
            raise ShadowJobUnavailable("confirmed session snapshot is required")
        if (
            snapshot.provider_id != plan.provider_id
            or snapshot.window_id != plan.window_id
            or snapshot.universe_sha256 != universe_sha256
            or snapshot.calendar_generation != calendar_generation
            or snapshot.calendar_sha256 != calendar_sha256
            or trade_date not in {item.isoformat() for item in snapshot.confirmed_next_sessions}
        ):
            raise ShadowJobUnavailable("confirmed session snapshot mismatch")
        now = started_at or datetime.now(UTC)
        finished = completed_at or now
        if now.tzinfo is None or finished.tzinfo is None or finished < now:
            raise ShadowJobUnavailable("shadow report timing unavailable")
        reports = tuple(
            ShadowAttemptReport(
                report_id=f"report-{session_id}-{request.ordinal:06d}",
                attempt_id=f"attempt-{session_id}-{request.ordinal:06d}",
                job_id=plan.job_id,
                provider_id=plan.provider_id,
                window_id=plan.window_id,
                session_id=session_id,
                logical_request_ordinal=request.ordinal,
                request_id=request.request_id,
                endpoint_class=request.endpoint_class,
                outcome=outcome,
                started_at=now.astimezone(UTC).isoformat(),
                completed_at=finished.astimezone(UTC).isoformat(),
                failure_class=failure_class or outcome,
                durable_report_ref=f"reports/{session_id}",
            )
            for request in plan.requests
        )
        report_id = f"session-report-{session_id}-v1"
        report_raw = json.dumps(
            [item.model_dump(mode="json") for item in reports],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        report_sha = hashlib.sha256(report_raw.encode()).hexdigest()
        terminal_status = "unavailable" if outcome == "unavailable" else "failed"
        publisher = bundle_publisher or (
            ShadowBundlePublisher(self.shadow_root) if self.shadow_root else None
        )
        if publisher is None:
            raise ShadowJobUnavailable("shadow report bundle publisher unavailable")
        bundle_path = publisher.publish(
            report_id,
            {
                "report_version": 1,
                "outcome": outcome,
                "reports": [item.model_dump(mode="json") for item in reports],
                "report_sha256": report_sha,
            },
        )
        try:
            report_fd = os.open(
                bundle_path / "report.json",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
            )
            try:
                bundle_raw = os.read(report_fd, 1_048_577)
            finally:
                os.close(report_fd)
            marker_fd = os.open(
                bundle_path / "COMMIT",
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_CLOEXEC,
            )
            try:
                marker = os.read(marker_fd, 128).decode()
            finally:
                os.close(marker_fd)
            if hashlib.sha256(bundle_raw).hexdigest() + "\n" != marker:
                raise ShadowJobUnavailable("shadow report bundle hash mismatch")
        except OSError as exc:
            raise ShadowJobUnavailable("shadow report bundle unavailable") from exc

        def save(connection):
            job = self._row(connection, plan.job_id)
            window = connection.execute(
                "SELECT state_version FROM qualification_window WHERE provider_id=? AND window_id=?",
                (plan.provider_id, plan.window_id),
            ).fetchone()
            if job is None or window is None:
                raise ShadowJobUnavailable("shadow lifecycle unavailable")
            if (
                expected_job_state_version is not None
                and job.state_version != expected_job_state_version
            ):
                raise ShadowJobUnavailable("shadow job CAS conflict")
            if (
                expected_window_state_version is not None
                and window[0] != expected_window_state_version
            ):
                raise ShadowJobUnavailable("shadow window CAS conflict")
            changed = connection.execute(
                "UPDATE shadow_job SET run_status=?,lease_owner=NULL,lease_expires_at=NULL,state_version=state_version+1 WHERE job_id=? AND provider_id=? AND window_id=? AND state_version=? AND run_status IN ('leased','pending','pending_normalization')",
                (terminal_status, plan.job_id, plan.provider_id, plan.window_id, job.state_version),
            )
            if changed.rowcount != 1:
                raise ShadowJobUnavailable("shadow job state changed")
            new_state = job.state_version + 1
            for report in reports:
                connection.execute(
                    "INSERT INTO shadow_attempt_report (attempt_id,report_id,job_id,provider_id,window_id,session_id,request_id,endpoint,endpoint_class,logical_request_ordinal,attempt_number,version_vector_sha256,outcome,started_at,completed_at,coverage_expected,coverage_observed,request_count,retry_count,rate_limit_count,failure_class,page_identities_json,page_count,row_count,terminal_marker,durable_report_ref,report_sha256,evidence_refs_json,evidence_id,evidence_sha256,candidate_sha256,terminal_session_report_id,state_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        report.attempt_id,
                        report.report_id,
                        report.job_id,
                        report.provider_id,
                        report.window_id,
                        report.session_id,
                        report.request_id,
                        report.endpoint_class,
                        report.endpoint_class,
                        report.logical_request_ordinal,
                        1,
                        version_vector_sha256,
                        report.outcome,
                        report.started_at,
                        report.completed_at,
                        0,
                        0,
                        0,
                        0,
                        0,
                        report.failure_class,
                        "[]",
                        0,
                        0,
                        0,
                        report.durable_report_ref,
                        report.report_sha256,
                        "[]",
                        None,
                        None,
                        None,
                        None,
                        new_state,
                    ),
                )
            connection.execute(
                "INSERT INTO session_report (session_report_id,provider_id,job_id,window_id,session_id,successful_attempt_id,evidence_id,candidate_id,terminal_attestation_id,report_version,trade_date,outcome,calendar_generation,calendar_sha256,universe_sha256,version_vector_sha256,evidence_sha256,candidate_sha256,report_ref,report_sha256,state_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    report_id,
                    plan.provider_id,
                    plan.job_id,
                    plan.window_id,
                    session_id,
                    None,
                    None,
                    None,
                    None,
                    1,
                    trade_date,
                    outcome,
                    calendar_generation,
                    calendar_sha256,
                    universe_sha256,
                    version_vector_sha256,
                    None,
                    None,
                    f"reports/{report_id}",
                    report_sha,
                    new_state,
                ),
            )
            reset = connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',last_session_report_id=NULL,qualification_evidence_sha256=NULL,qualification_candidate_sha256=NULL,terminal_attestation_id=NULL,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (plan.provider_id, plan.window_id, window[0]),
            )
            if reset.rowcount != 1:
                raise ShadowJobUnavailable("shadow window CAS conflict")
            return reports

        return self.registry._with_transaction(save)

    persist_failure = persist_outcome

    def _enqueue_canonical_descriptors(
        self,
        manifests,
        *,
        provider_id: str | None = None,
        window_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Attach every ready canonical manifest identity idempotently.

        ``manifests`` is an injected iterable of immutable, already-read descriptors;
        this method never opens a canonical root or asks a provider for a fresh value.
        """
        count = 0
        for manifest in manifests:
            if not isinstance(manifest, dict):
                continue
            try:
                if provider_id is not None and manifest["provider_id"] != provider_id:
                    continue
                if window_id is not None and manifest["window_id"] != window_id:
                    continue
                trade_date = str(manifest["trade_date"])
                if start_date is not None and trade_date < start_date:
                    continue
                if end_date is not None and trade_date > end_date:
                    continue
                self.enqueue(
                    job_id=str(manifest["job_id"]),
                    provider_id=str(manifest["provider_id"]),
                    window_id=str(manifest["window_id"]),
                    trade_date=trade_date,
                    universe_id=str(manifest["universe_id"]),
                    canonical_manifest_generation=str(manifest["canonical_manifest_generation"]),
                    canonical_manifest_sha256=str(manifest["canonical_manifest_sha256"]),
                    version_vector_sha256=str(manifest["version_vector_sha256"]),
                )
                count += 1
            except (KeyError, TypeError, ValueError, ShadowJobUnavailable):
                continue
        return count


class ShadowHandoff:
    """Post-lock bounded in-memory handoff; offer never touches SQLite or callbacks."""

    def __init__(
        self,
        job_store: ShadowJobStore | None = None,
        enqueue_outcome: Callable[[Any], Any] | None = None,
        *,
        maxsize: int = 64,
    ):
        self.job_store = job_store
        self.enqueue_outcome = enqueue_outcome
        self._offered: set[str] = set()
        self._queue = deque(maxlen=max(1, maxsize))

    def offer(self, outcome: Any, *, wait_budget: float = 0, nonblocking: bool = True) -> bool:
        if wait_budget != 0 or nonblocking is not True:
            raise ValueError("shadow handoff must be nonblocking with zero wait")
        key = getattr(getattr(outcome, "result", None), "run_id", None) or getattr(
            getattr(outcome, "decision", None), "target_session", None
        )
        key = str(key or "none")
        if key in self._offered:
            return False
        if len(self._queue) >= self._queue.maxlen:
            return False
        self._queue.append((key, outcome))
        self._offered.add(key)
        return True

    def drain(self, *, limit: int = 64) -> int:
        """Drain outside the canonical lock; callback failures leave scanner recovery."""
        count = 0
        while self._queue and count < max(0, limit):
            _key, outcome = self._queue.popleft()
            if self.enqueue_outcome is not None:
                self.enqueue_outcome(outcome)
            count += 1
        return count


__all__ = [
    "CanonicalManifestUnavailable",
    "ConfirmedSessionSnapshot",
    "CanonicalOutcomeScanner",
    "ShadowAttemptReport",
    "ShadowBundlePublisher",
    "ShadowHandoff",
    "ShadowJob",
    "ShadowJobStore",
    "ShadowJobUnavailable",
    "ShadowOutcomeReport",
    "ShadowOutcomeReporter",
    "ShadowSessionReport",
]
