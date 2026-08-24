"""Writer-owned, descriptor-bound registry for R2-F3 shadow admission."""

# The frozen SQL source is intentionally kept byte-readable for independent review.
# ruff: noqa: E501

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.market.shadow_registry_schema import (
    MIGRATION_ID,
    initialize_registry,
    shadow_sha256_canonical_json,
    shadow_validate_terminal_graph,
)

from .shadow_contracts import (
    MAX_TERMS_EVIDENCE_BYTES,
    AdmissionState,
    ShadowProviderId,
    ShadowProviderRecord,
    TermsEvidence,
)
from .shadow_contracts import (
    exact_credential_env as _exact_credential_env,
)
from .shadow_contracts import (
    make_token as _make_token,
)


class RegistryUnavailable(RuntimeError):
    """A sanitized unavailable result; no local path or SQL is exposed."""


class TermsEvidenceUnavailable(RegistryUnavailable):
    pass


def exact_credential_env(provider_id: str, *, requested: str | None = None) -> str:
    return _exact_credential_env(provider_id, requested=requested)


def make_token(
    provider_record: ShadowProviderRecord,
    terms_evidence: TermsEvidence,
    *,
    environ=None,
) -> str:
    return _make_token(
        provider_record,
        terms_evidence,
        environ=environ,
    )


def resolve_provider_credential(
    provider_record: ShadowProviderRecord,
    terms_evidence: TermsEvidence,
    *,
    environ=None,
) -> str:
    return _make_token(
        provider_record,
        terms_evidence,
        environ=environ,
    )


class QualificationWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_id: ShadowProviderId
    window_id: str
    window_state: str = "observing"
    consecutive_sessions: int = Field(ge=0)
    version_vector_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    calendar_generation: str
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_version: int = Field(ge=0)


class ConfirmedCalendarSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider_id: ShadowProviderId
    window_id: str
    version_vector_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    calendar_generation: str
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    universe_id: str
    adapter_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reconciliation_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sessions: tuple[date, ...]

    @model_validator(mode="after")
    def validate_sessions(self) -> ConfirmedCalendarSnapshot:
        if tuple(sorted(set(self.sessions))) != self.sessions:
            raise ValueError("confirmed sessions must be sorted and unique")
        return self


class ConfirmedCalendarReader(Protocol):
    def read(self, provider_id: str, window_id: str) -> ConfirmedCalendarSnapshot: ...


def _authorizer(role: str):
    writes = frozenset(
        {
            sqlite3.SQLITE_INSERT,
            sqlite3.SQLITE_UPDATE,
            sqlite3.SQLITE_DELETE,
            sqlite3.SQLITE_ALTER_TABLE,
            sqlite3.SQLITE_DROP_TABLE,
        }
    )

    def check(action, arg1, arg2, dbname, source):
        if action in writes and (
            role == "reader"
            or (
                arg1 in {"shadow_attempt_report", "session_report", "shadow_terminal_attestation"}
                and role != "terminal_writer"
            )
        ):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    return check


class ShadowRegistryTerminalWriter:
    """The only public writable terminal connection bootstrap."""

    @staticmethod
    def open(path: Path | str, *, allow_memory: bool = False) -> sqlite3.Connection:
        if str(path) == ":memory:" and not allow_memory:
            raise ValueError("registry writer requires an explicit file path")
        connection = sqlite3.connect(str(path))
        connection.create_function(
            "shadow_sha256_canonical_json", 2, shadow_sha256_canonical_json, deterministic=True
        )
        connection.create_function(
            "shadow_validate_terminal_graph", 8, shadow_validate_terminal_graph, deterministic=True
        )
        connection.set_authorizer(_authorizer("terminal_writer"))
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=250")
        return connection


class ShadowRegistry:
    def __init__(self, path: Path | str, *, terms_object_root: Path | None = None):
        self.path = Path(path) if str(path) != ":memory:" else Path(":memory:")
        self._memory = str(path) == ":memory:"
        self.terms_object_root = terms_object_root or (
            Path(path).parent / "terms"
            if not self._memory
            else Path(tempfile.mkdtemp(prefix="stock-eva-shadow-terms-"))
        )

    @classmethod
    def in_memory(cls) -> ShadowRegistry:
        return cls(":memory:")

    @staticmethod
    def allowed_provider_ids() -> tuple[str, ...]:
        return tuple(item.value for item in ShadowProviderId)

    @staticmethod
    def lock_order() -> tuple[str, str]:
        return ("bundle_publish_release", "registry_cas")

    @property
    def lock_path(self) -> Path:
        return self.path.with_name(self.path.name + ".lock")

    def initialize(self) -> None:
        if self._memory:
            connection = ShadowRegistryTerminalWriter.open(":memory:", allow_memory=True)
            initialize_registry(connection)
            connection.close()
            # A memory registry is intentionally used by tests through one connection below.
            self._memory_connection = ShadowRegistryTerminalWriter.open(
                ":memory:", allow_memory=True
            )
            initialize_registry(self._memory_connection)
            return
        physical_path = self._trusted_physical_path(self.path)
        self._validate_existing_ancestors(physical_path.parent)
        if not physical_path.parent.is_dir():
            raise RegistryUnavailable("registry parent unavailable")
        parent_info = physical_path.parent.stat()
        if parent_info.st_uid != os.getuid() or parent_info.st_mode & 0o777 != 0o700:
            raise RegistryUnavailable("registry parent permissions unavailable")
        if physical_path.is_symlink() or self._trusted_physical_path(self.lock_path).is_symlink():
            raise RegistryUnavailable("registry basename unavailable")
        parent_fd = os.open(
            physical_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            self._open_or_create_private_file(parent_fd, physical_path.name + ".lock")
        finally:
            os.close(parent_fd)
        with self._lock(shared=False):
            parent_fd = os.open(
                physical_path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                self._open_or_create_private_file(parent_fd, physical_path.name)
            finally:
                os.close(parent_fd)
            guard_fd, guarded_path, before = self._open_guard_descriptor()
            connection = None
            try:
                connection = self._open_writer(physical_path)
                initialize_registry(connection)
            finally:
                if connection is not None:
                    connection.close()
                try:
                    self._verify_guard_descriptor(guard_fd, guarded_path, before)
                finally:
                    os.close(guard_fd)

    @staticmethod
    def _open_or_create_private_file(parent_fd: int, name: str) -> None:
        flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
        try:
            try:
                fd = os.open(name, flags, dir_fd=parent_fd)
            except FileNotFoundError:
                fd = os.open(
                    name,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=parent_fd,
                )
        except OSError as exc:
            raise RegistryUnavailable("registry basename unavailable") from exc
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o777 != 0o600:
                raise RegistryUnavailable("registry permissions unavailable")
        finally:
            os.close(fd)

    def _has_schema(self, connection: sqlite3.Connection) -> bool:
        try:
            return (
                connection.execute("SELECT 1 FROM schema_migration LIMIT 1").fetchone() is not None
            )
        except sqlite3.DatabaseError:
            return False

    @contextmanager
    def _lock(self, *, shared: bool) -> Iterator[None]:
        if self._memory:
            yield
            return
        lock_path = self._trusted_physical_path(self.lock_path)
        self._validate_existing_ancestors(lock_path.parent)
        if (
            lock_path.is_symlink()
            or not lock_path.is_file()
            or lock_path.stat().st_mode & 0o777 != 0o600
        ):
            raise RegistryUnavailable("registry lock unavailable")
        fd = os.open(
            lock_path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
            yield
        except (OSError, EOFError) as exc:
            raise RegistryUnavailable("registry lock unavailable") from exc
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _open_writer(self, path: Path | None = None) -> sqlite3.Connection:
        if not self._memory:
            path = path or self._trusted_physical_path(self.path)
            if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o777 != 0o600:
                raise RegistryUnavailable("registry permissions unavailable")
        return getattr(self, "_memory_connection", None) or ShadowRegistryTerminalWriter.open(
            path or self.path
        )

    def _open_guard_descriptor(self) -> tuple[int, Path, tuple[int, int, int, int]]:
        physical_path = self._trusted_physical_path(self.path)
        self._validate_existing_ancestors(physical_path.parent)
        parent_fd = os.open(
            physical_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            fd = os.open(
                physical_path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        finally:
            os.close(parent_fd)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o777 != 0o600:
            os.close(fd)
            raise RegistryUnavailable("registry permissions unavailable")
        return fd, physical_path, (info.st_dev, info.st_ino, info.st_mode, info.st_ctime_ns)

    @staticmethod
    def _verify_guard_descriptor(fd: int, path: Path, before: tuple[int, int, int, int]) -> None:
        after = os.fstat(fd)
        current = os.stat(path)
        after_identity = (after.st_dev, after.st_ino, after.st_mode, after.st_ctime_ns)
        current_identity = (current.st_dev, current.st_ino, current.st_mode, current.st_ctime_ns)
        if (
            path.is_symlink()
            or after_identity[:3] != before[:3]
            or current_identity[:3] != before[:3]
        ):
            raise RegistryUnavailable("registry basename changed during transaction")

    @staticmethod
    def _validate_existing_ancestors(path: Path) -> None:
        from pathlib import PurePath

        absolute = Path(os.path.abspath(path))
        current = Path(absolute.anchor)
        for component in PurePath(absolute).parts[1:]:
            current /= component
            if current.is_symlink():
                # macOS exposes the system temporary directory through the
                # stable /var -> /private/var alias.  Treat only that
                # platform-owned alias as trusted; caller-controlled
                # symlinked ancestors remain rejected before any mkdir/write.
                if current == Path("/var") and current.resolve() == Path("/private/var"):
                    continue
                raise RegistryUnavailable("registry ancestor unavailable")
            if current.exists() and not current.is_dir():
                raise RegistryUnavailable("registry ancestor unavailable")

    @staticmethod
    def _trusted_physical_path(path: Path) -> Path:
        absolute = Path(os.path.abspath(path))
        if absolute.parts[1:2] == ("tmp",) and Path("/tmp").is_symlink():
            return Path("/private/tmp", *absolute.parts[2:])
        if absolute.parts[1:2] == ("var",) and Path("/var").is_symlink():
            return Path("/private/var", *absolute.parts[2:])
        return absolute

    def _connection_for_read(self) -> sqlite3.Connection:
        if self._memory:
            return self._memory_connection
        physical_path = self._trusted_physical_path(self.path)
        self._validate_existing_ancestors(physical_path.parent)
        if physical_path.is_symlink() or not physical_path.parent.is_dir():
            raise RegistryUnavailable("registry unavailable")
        if physical_path.with_name(physical_path.name + "-journal").exists():
            raise RegistryUnavailable("registry journal unavailable")
        parent_fd = os.open(
            physical_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            fd = os.open(
                physical_path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            try:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode) or before.st_mode & 0o777 != 0o600:
                    raise RegistryUnavailable("registry unavailable")
                data = os.read(fd, 32 * 1024 * 1024 + 1)
                after = os.fstat(fd)
            finally:
                os.close(fd)
        except (OSError, RegistryUnavailable) as exc:
            raise RegistryUnavailable("registry unavailable") from exc
        finally:
            os.close(parent_fd)
        if (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise RegistryUnavailable("registry changed during read")
        if len(data) > 32 * 1024 * 1024 or not data.startswith(b"SQLite format 3"):
            raise RegistryUnavailable("registry schema unavailable")
        connection = sqlite3.connect(":memory:")
        if not hasattr(connection, "deserialize"):
            connection.close()
            raise RegistryUnavailable("registry reader unavailable")
        try:
            connection.deserialize(data)
        except sqlite3.DatabaseError as exc:
            connection.close()
            raise RegistryUnavailable("registry schema unavailable") from exc
        connection.set_authorizer(_authorizer("reader"))
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            if (
                connection.execute(
                    "SELECT migration_id FROM schema_migration ORDER BY schema_version DESC LIMIT 1"
                ).fetchone()[0]
                != MIGRATION_ID
            ):
                raise RegistryUnavailable("registry migration unavailable")
        except (sqlite3.DatabaseError, TypeError) as exc:
            connection.close()
            raise RegistryUnavailable("registry schema unavailable") from exc
        return connection

    def _with_transaction(self, callback):
        with self._lock(shared=False):
            guard_fd = None
            connection = None
            try:
                if self._memory:
                    connection = self._open_writer()
                else:
                    guard_fd, physical_path, before = self._open_guard_descriptor()
                    connection = self._open_writer(physical_path)
                connection.execute("BEGIN IMMEDIATE")
                result = callback(connection)
                connection.commit()
                if guard_fd is not None:
                    self._verify_guard_descriptor(guard_fd, physical_path, before)
                return result
            except RegistryUnavailable:
                if connection is not None:
                    connection.rollback()
                raise
            except ValueError:
                if connection is not None:
                    connection.rollback()
                raise
            except (sqlite3.DatabaseError, OSError) as exc:
                if connection is not None:
                    connection.rollback()
                raise RegistryUnavailable("registry transaction unavailable") from exc
            finally:
                if not self._memory:
                    if connection is not None:
                        connection.close()
                    if guard_fd is not None:
                        os.close(guard_fd)

    @staticmethod
    def _record_values(record: ShadowProviderRecord) -> tuple[object, ...]:
        return tuple(
            record.model_dump(mode="json").get(key)
            for key in (
                "provider_id",
                "admission_state",
                "adapter_hash",
                "endpoint_contract_hash",
                "source_schema_hash",
                "normalizer_hash",
                "reconciliation_policy_hash",
                "terms_evidence_hash",
                "terms_review_id",
                "credential_env_name",
                "intended_use",
                "retention_decision",
                "quota_contract",
                "required_fields_json",
                "unit_contract_json",
                "state_version",
                "quarantine_reason",
            )
        )

    def put_provider(self, record: ShadowProviderRecord) -> None:
        def save(connection):
            connection.execute(
                "INSERT INTO provider_record VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                self._record_values(record),
            )

        self._with_transaction(save)

    def _record_from_row(self, row) -> ShadowProviderRecord:
        names = (
            "provider_id",
            "admission_state",
            "adapter_hash",
            "endpoint_contract_hash",
            "source_schema_hash",
            "normalizer_hash",
            "reconciliation_policy_hash",
            "terms_evidence_hash",
            "terms_review_id",
            "credential_env_name",
            "intended_use",
            "retention_decision",
            "quota_contract",
            "required_fields_json",
            "unit_contract_json",
            "state_version",
            "quarantine_reason",
        )
        return ShadowProviderRecord.model_validate(dict(zip(names, row, strict=True)))

    def _query_record(self, connection, provider_id: str) -> ShadowProviderRecord:
        row = connection.execute(
            "SELECT * FROM provider_record WHERE provider_id=?", (provider_id,)
        ).fetchone()
        if row is None:
            raise RegistryUnavailable("provider unavailable")
        return self._record_from_row(row)

    def read_status(self, provider_id: str) -> ShadowProviderRecord:
        if provider_id not in self.allowed_provider_ids():
            raise RegistryUnavailable("provider unavailable")
        with self._lock(shared=True):
            connection = self._connection_for_read()
            try:
                return self._query_record(connection, provider_id)
            finally:
                if not self._memory:
                    connection.close()

    def transition(
        self, provider_id: str, state: AdmissionState | str, *, expected_state_version: int
    ) -> ShadowProviderRecord:
        state = AdmissionState(state)
        allowed = {
            AdmissionState.DISCOVERED: {AdmissionState.CANARY},
            AdmissionState.CANARY: {AdmissionState.SHADOW, AdmissionState.QUARANTINED},
            AdmissionState.SHADOW: {AdmissionState.QUARANTINED},
            AdmissionState.QUALIFIED: {AdmissionState.QUARANTINED},
            AdmissionState.QUARANTINED: set(),
        }

        def update(connection):
            current = self._query_record(connection, provider_id)
            if state not in allowed[current.admission_state]:
                raise ValueError("closed admission transition")
            if current.state_version != expected_state_version:
                raise RegistryUnavailable("stale registry state")
            if state == AdmissionState.QUALIFIED and current.terms_evidence_hash is None:
                raise ValueError("terms evidence is required")
            if state is AdmissionState.QUARANTINED:
                vector = connection.execute(
                    "SELECT version_vector_sha256 FROM qualification_window WHERE provider_id=?",
                    (provider_id,),
                ).fetchone()
                connection.execute(
                    "INSERT OR REPLACE INTO quarantine_snapshot VALUES (?,?,?,?)",
                    (
                        provider_id,
                        current.adapter_hash,
                        current.terms_evidence_hash,
                        vector[0] if vector else None,
                    ),
                )
                connection.execute(
                    "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',state_version=state_version+1 WHERE provider_id=?",
                    (provider_id,),
                )
            cursor = connection.execute(
                "UPDATE provider_record SET admission_state=?, state_version=state_version+1 WHERE provider_id=? AND state_version=?",
                (state.value, provider_id, expected_state_version),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale registry state")
            return self._query_record(connection, provider_id).model_copy()

        return self._with_transaction(update)

    def reopen_quarantined(
        self, record: ShadowProviderRecord, *, version_vector_sha256: str
    ) -> ShadowProviderRecord:
        current = self.read_status(record.provider_id.value)
        if current.admission_state != AdmissionState.QUARANTINED:
            raise ValueError("provider is not quarantined")
        with self._lock(shared=True):
            connection = self._connection_for_read()
            try:
                snapshot = connection.execute(
                    "SELECT adapter_hash,terms_evidence_hash,version_vector_sha256 FROM quarantine_snapshot WHERE provider_id=?",
                    (record.provider_id.value,),
                ).fetchone()
            finally:
                if not self._memory:
                    connection.close()
        if snapshot is None or (
            record.adapter_hash == snapshot[0]
            or record.terms_evidence_hash == snapshot[1]
            or version_vector_sha256 == snapshot[2]
        ):
            raise RegistryUnavailable("new reviewed quarantine version required")
        values = record.model_copy(
            update={
                "state_version": current.state_version,
                "admission_state": AdmissionState.CANARY,
            }
        )

        def update(connection):
            cursor = connection.execute(
                "UPDATE provider_record SET admission_state=?,adapter_hash=?,endpoint_contract_hash=?,source_schema_hash=?,normalizer_hash=?,reconciliation_policy_hash=?,terms_evidence_hash=?,terms_review_id=?,state_version=state_version+1,quarantine_reason=NULL WHERE provider_id=? AND state_version=?",
                (
                    values.admission_state.value,
                    values.adapter_hash,
                    values.endpoint_contract_hash,
                    values.source_schema_hash,
                    values.normalizer_hash,
                    values.reconciliation_policy_hash,
                    values.terms_evidence_hash,
                    values.terms_review_id,
                    values.provider_id.value,
                    current.state_version,
                ),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale registry state")
            connection.execute(
                "DELETE FROM quarantine_snapshot WHERE provider_id=?", (values.provider_id.value,)
            )
            return self._query_record(connection, values.provider_id.value)

        return self._with_transaction(update)

    def put_terms_evidence(self, evidence: TermsEvidence) -> None:
        if not evidence._content_bytes:
            raise TermsEvidenceUnavailable("terms object unavailable")
        terms_root = self._trusted_physical_path(self.terms_object_root)
        self._validate_existing_ancestors(terms_root)
        if not terms_root.exists():
            terms_root.mkdir(parents=True, exist_ok=True)
        if terms_root.is_symlink():
            raise TermsEvidenceUnavailable("terms object root unavailable")
        target = terms_root / evidence.content_object_relpath
        self._validate_existing_ancestors(target.parent)
        if not target.parent.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
        if target.parent.is_symlink():
            raise TermsEvidenceUnavailable("terms object path unavailable")
        parent_fd = os.open(
            target.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            try:
                fd = os.open(
                    target.name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=parent_fd
                )
            except FileNotFoundError:
                fd = os.open(
                    target.name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=parent_fd,
                )
                try:
                    written = 0
                    while written < len(evidence._content_bytes):
                        written += os.write(fd, evidence._content_bytes[written:])
                    os.fsync(fd)
                finally:
                    os.close(fd)
                fd = -1
            if fd >= 0:
                try:
                    before = os.fstat(fd)
                    if not stat.S_ISREG(before.st_mode) or before.st_mode & 0o777 != 0o600:
                        raise TermsEvidenceUnavailable("terms object permissions unavailable")
                    current_bytes = os.read(fd, len(evidence._content_bytes) + 1)
                    after = os.fstat(fd)
                    if current_bytes != evidence._content_bytes or (
                        before.st_dev,
                        before.st_ino,
                        before.st_mode,
                        before.st_size,
                        before.st_mtime_ns,
                        before.st_ctime_ns,
                    ) != (
                        after.st_dev,
                        after.st_ino,
                        after.st_mode,
                        after.st_size,
                        after.st_mtime_ns,
                        after.st_ctime_ns,
                    ):
                        raise TermsEvidenceUnavailable("terms object changed")
                finally:
                    os.close(fd)
        except (FileExistsError, OSError) as exc:
            if isinstance(exc, TermsEvidenceUnavailable):
                raise
            raise TermsEvidenceUnavailable("terms object unavailable") from exc
        finally:
            os.close(parent_fd)
        values = (
            evidence.terms_evidence_id,
            evidence.provider_id.value,
            json_array(evidence.official_url_allowlist),
            evidence.content_object_relpath,
            evidence.content_bytes_sha256,
            evidence.contract_version,
            evidence.as_of_date,
            evidence.reviewer,
            evidence.review_id,
            evidence.approved_intended_use,
            evidence.approved_retention,
            evidence.approved_credential_mode,
            evidence.approved_quota_decision,
            evidence.manifest_sha256,
        )
        self._with_transaction(
            lambda connection: connection.execute(
                "INSERT INTO terms_evidence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", values
            )
        )

    def attach_terms_to_provider(
        self,
        provider_id: str,
        evidence: TermsEvidence,
        *,
        expected_state_version: int | None = None,
        expected_window_state_version: int | None = None,
    ) -> ShadowProviderRecord:
        if evidence.provider_id.value != provider_id:
            raise ValueError("terms/provider binding mismatch")

        def update(connection):
            current = self._query_record(connection, provider_id)
            expected = (
                current.state_version if expected_state_version is None else expected_state_version
            )
            if current.state_version != expected:
                raise RegistryUnavailable("stale registry state")
            window_row = connection.execute(
                "SELECT state_version FROM qualification_window WHERE provider_id=?",
                (provider_id,),
            ).fetchone()
            if window_row is not None and expected_window_state_version is None:
                raise RegistryUnavailable("qualification window CAS is required")
            if window_row is not None and window_row[0] != expected_window_state_version:
                raise RegistryUnavailable("stale qualification state")
            changed = (current.terms_evidence_hash, current.terms_review_id) != (
                evidence.manifest_sha256,
                evidence.review_id,
            )
            cursor = connection.execute(
                "UPDATE provider_record SET terms_evidence_hash=?,terms_review_id=?,state_version=state_version+1 WHERE provider_id=? AND state_version=?",
                (evidence.manifest_sha256, evidence.review_id, provider_id, expected),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale registry state")
            if changed and window_row is not None:
                reset = connection.execute(
                    "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',state_version=state_version+1 WHERE provider_id=? AND state_version=?",
                    (provider_id, expected_window_state_version),
                )
                if reset.rowcount != 1:
                    raise RegistryUnavailable("qualification reset unavailable")
            return self._query_record(connection, provider_id)

        return self._with_transaction(update)

    def update_provider_config(
        self,
        record: ShadowProviderRecord,
        *,
        expected_state_version: int,
        expected_window_state_version: int | None = None,
    ) -> ShadowProviderRecord:
        """CAS a reviewed material contract change and reset its window atomically."""

        def update(connection):
            current = self._query_record(connection, record.provider_id.value)
            if current.state_version != expected_state_version:
                raise RegistryUnavailable("stale registry state")
            window_row = connection.execute(
                "SELECT state_version FROM qualification_window WHERE provider_id=?",
                (record.provider_id.value,),
            ).fetchone()
            if window_row is not None and expected_window_state_version is None:
                raise RegistryUnavailable("qualification window CAS is required")
            if window_row is not None and window_row[0] != expected_window_state_version:
                raise RegistryUnavailable("stale qualification state")
            changed = current.model_dump(mode="json")
            proposed = record.model_dump(mode="json")
            changed.pop("state_version", None)
            proposed.pop("state_version", None)
            if changed == proposed:
                return current
            values = (
                record.admission_state.value,
                record.adapter_hash,
                record.endpoint_contract_hash,
                record.source_schema_hash,
                record.normalizer_hash,
                record.reconciliation_policy_hash,
                record.terms_evidence_hash,
                record.terms_review_id,
                record.credential_env_name,
                record.intended_use,
                record.retention_decision,
                record.quota_contract,
                record.required_fields_json,
                record.unit_contract_json,
                record.provider_id.value,
                expected_state_version,
            )
            cursor = connection.execute(
                "UPDATE provider_record SET admission_state=?,adapter_hash=?,endpoint_contract_hash=?,source_schema_hash=?,normalizer_hash=?,reconciliation_policy_hash=?,terms_evidence_hash=?,terms_review_id=?,credential_env_name=?,intended_use=?,retention_decision=?,quota_contract=?,required_fields_json=?,unit_contract_json=?,state_version=state_version+1 WHERE provider_id=? AND state_version=?",
                values,
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale registry state")
            reset = connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',state_version=state_version+1 WHERE provider_id=? AND state_version=?",
                (record.provider_id.value, expected_window_state_version),
            )
            if reset.rowcount != 1:
                raise RegistryUnavailable("qualification reset unavailable")
            return self._query_record(connection, record.provider_id.value)

        return self._with_transaction(update)

    def read_window(self, provider_id: str, window_id: str) -> QualificationWindow:
        with self._lock(shared=True):
            connection = self._connection_for_read()
            try:
                return self._window(connection, provider_id, window_id)
            finally:
                if not self._memory:
                    connection.close()

    def promote_qualified(
        self,
        provider_id: str,
        window_id: str,
        *,
        terminal_attestation_id: str,
        evidence_sha256: str,
        candidate_sha256: str,
        session_report_id: str,
    ) -> ShadowProviderRecord:
        def promote(connection):
            current = self._query_record(connection, provider_id)
            window = self._window(connection, provider_id, window_id)
            if current.admission_state is not AdmissionState.SHADOW:
                raise RegistryUnavailable("provider is not shadow-qualified")
            if window.consecutive_sessions != 20:
                raise RegistryUnavailable("qualification window unavailable")
            attestation = connection.execute(
                "SELECT 1 FROM shadow_terminal_attestation WHERE attestation_id=? AND provider_id=? AND job_id IS NOT NULL AND window_id=? AND evidence_sha256=? AND candidate_sha256=? AND session_report_id=?",
                (
                    terminal_attestation_id,
                    provider_id,
                    window_id,
                    evidence_sha256,
                    candidate_sha256,
                    session_report_id,
                ),
            ).fetchone()
            if attestation is None:
                raise RegistryUnavailable("terminal attestation unavailable")
            cursor = connection.execute(
                "UPDATE provider_record SET admission_state='qualified',state_version=state_version+1 WHERE provider_id=? AND admission_state='shadow' AND state_version=?",
                (provider_id, current.state_version),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale registry state")
            window_cursor = connection.execute(
                "UPDATE qualification_window SET window_state='qualified',last_session_report_id=?,qualification_evidence_sha256=?,qualification_candidate_sha256=?,terminal_attestation_id=?,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (
                    session_report_id,
                    evidence_sha256,
                    candidate_sha256,
                    terminal_attestation_id,
                    provider_id,
                    window_id,
                    window.state_version,
                ),
            )
            if window_cursor.rowcount != 1:
                raise RegistryUnavailable("stale qualification state")
            return self._query_record(connection, provider_id)

        return self._with_transaction(promote)

    def read_terms_evidence(self, terms_evidence_id: str) -> TermsEvidence:
        with self._lock(shared=True):
            connection = self._connection_for_read()
            try:
                row = connection.execute(
                    "SELECT * FROM terms_evidence WHERE terms_evidence_id=?", (terms_evidence_id,)
                ).fetchone()
                if row is None:
                    raise RegistryUnavailable("terms evidence unavailable")
            finally:
                if not self._memory:
                    connection.close()
        terms_root = self._trusted_physical_path(self.terms_object_root)
        target = terms_root / row[3]
        try:
            self._validate_existing_ancestors(terms_root)
            self._validate_existing_ancestors(target.parent)
            if target.is_symlink() or not target.parent.is_dir():
                raise RegistryUnavailable("terms evidence changed")
            parent_fd = os.open(
                target.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                fd = os.open(
                    target.name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent_fd,
                )
                try:
                    before = os.fstat(fd)
                    content = os.read(fd, MAX_TERMS_EVIDENCE_BYTES + 1)
                    after = os.fstat(fd)
                finally:
                    os.close(fd)
            finally:
                os.close(parent_fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_mode & 0o777 != 0o600
                or before.st_size > MAX_TERMS_EVIDENCE_BYTES
                or (
                    before.st_dev,
                    before.st_ino,
                    before.st_mode,
                    before.st_size,
                    before.st_mtime_ns,
                    before.st_ctime_ns,
                )
                != (
                    after.st_dev,
                    after.st_ino,
                    after.st_mode,
                    after.st_size,
                    after.st_mtime_ns,
                    after.st_ctime_ns,
                )
                or hashlib.sha256(content).hexdigest() != row[4]
            ):
                raise RegistryUnavailable("terms evidence changed")
            evidence = TermsEvidence.build(
                content_bytes=content,
                official_url_allowlist=tuple(json.loads(row[2])),
                terms_evidence_id=row[0],
                provider_id=row[1],
                content_object_relpath=row[3],
                contract_version=row[5],
                as_of_date=row[6],
                reviewer=row[7],
                review_id=row[8],
                approved_intended_use=row[9],
                approved_retention=row[10],
                approved_credential_mode=row[11],
                approved_quota_decision=row[12],
            )
            if evidence.manifest_sha256 != row[13]:
                raise RegistryUnavailable("terms evidence hash unavailable")
            return evidence
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise RegistryUnavailable("terms evidence unavailable") from exc

    def ensure_window(
        self,
        provider_id: str,
        version_vector_sha256: str,
        calendar_generation: str,
        calendar_sha256: str,
    ) -> QualificationWindow:
        if provider_id not in self.allowed_provider_ids():
            raise ValueError("unknown shadow provider")
        window_id = f"{provider_id}-window-1"

        def save(connection):
            connection.execute(
                "INSERT OR IGNORE INTO qualification_window VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    provider_id,
                    window_id,
                    None,
                    None,
                    0,
                    version_vector_sha256,
                    calendar_generation,
                    calendar_sha256,
                    "observing",
                    None,
                    None,
                    None,
                    None,
                    0,
                ),
            )
            return self._window(connection, provider_id, window_id)

        return self._with_transaction(save)

    def _window(self, connection, provider_id: str, window_id: str) -> QualificationWindow:
        row = connection.execute(
            "SELECT provider_id,window_id,window_state,consecutive_sessions,version_vector_sha256,calendar_generation,calendar_sha256,state_version FROM qualification_window WHERE provider_id=? AND window_id=?",
            (provider_id, window_id),
        ).fetchone()
        if row is None:
            raise RegistryUnavailable("qualification window unavailable")
        return QualificationWindow.model_validate(
            dict(
                zip(
                    (
                        "provider_id",
                        "window_id",
                        "window_state",
                        "consecutive_sessions",
                        "version_vector_sha256",
                        "calendar_generation",
                        "calendar_sha256",
                        "state_version",
                    ),
                    row,
                    strict=True,
                )
            )
        )

    def qualify_window(
        self,
        provider_id: str,
        window_id: str,
        *,
        calendar_reader: ConfirmedCalendarReader,
        expected_provider_state_version: int,
        expected_window_state_version: int,
    ) -> QualificationWindow:
        try:
            snapshot = calendar_reader.read(provider_id, window_id)
        except (OSError, ValueError, TypeError, RegistryUnavailable) as exc:
            raise RegistryUnavailable("confirmed calendar snapshot unavailable") from exc
        if not isinstance(snapshot, ConfirmedCalendarSnapshot):
            raise RegistryUnavailable("confirmed calendar snapshot unavailable")

        def update(connection):
            provider = self._query_record(connection, provider_id)
            current = self._window(connection, provider_id, window_id)
            if provider.admission_state is not AdmissionState.SHADOW:
                raise RegistryUnavailable("provider is not shadow-qualified")
            if provider.state_version != expected_provider_state_version:
                raise RegistryUnavailable("stale registry state")
            if current.state_version != expected_window_state_version:
                raise RegistryUnavailable("stale qualification state")
            if (
                snapshot.provider_id != provider.provider_id
                or snapshot.window_id != window_id
                or snapshot.version_vector_sha256 != current.version_vector_sha256
                or snapshot.calendar_generation != current.calendar_generation
                or snapshot.calendar_sha256 != current.calendar_sha256
                or snapshot.adapter_hash != provider.adapter_hash
                or snapshot.reconciliation_policy_hash != provider.reconciliation_policy_hash
                or len(snapshot.sessions) < 20
            ):
                raise RegistryUnavailable("confirmed calendar snapshot unavailable")
            expected_sessions = snapshot.sessions[-20:]
            rows = connection.execute(
                """
                SELECT s.trade_date
                FROM session_report s
                JOIN shadow_terminal_attestation t
                  ON t.session_report_id=s.session_report_id
                 AND t.provider_id=s.provider_id AND t.job_id=s.job_id
                 AND t.window_id=s.window_id AND t.session_id=s.session_id
                 AND t.attestation_id=s.terminal_attestation_id
                JOIN shadow_job j
                  ON j.job_id=s.job_id AND j.provider_id=s.provider_id
                 AND j.window_id=s.window_id
                WHERE s.provider_id=? AND s.window_id=? AND s.outcome='success'
                  AND s.version_vector_sha256=? AND j.version_vector_sha256=?
                  AND j.universe_id=?
                ORDER BY s.trade_date
                """,
                (
                    provider_id,
                    window_id,
                    snapshot.version_vector_sha256,
                    snapshot.version_vector_sha256,
                    snapshot.universe_id,
                ),
            ).fetchall()
            observed_sessions = tuple(date.fromisoformat(row[0]) for row in rows)
            if observed_sessions[-20:] != expected_sessions or len(observed_sessions) < 20:
                raise RegistryUnavailable("terminal calendar sequence unavailable")
            cursor = connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=20,window_start=?,window_end=?,window_state='observing',state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (
                    expected_sessions[0].isoformat(),
                    expected_sessions[-1].isoformat(),
                    provider_id,
                    window_id,
                    expected_window_state_version,
                ),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale qualification state")
            return self._window(connection, provider_id, window_id)

        return self._with_transaction(update)

    def reset_window_if_version_changed(
        self,
        provider_id: str,
        window_id: str,
        version_vector_sha256: str,
        calendar_generation: str,
        calendar_sha256: str,
        *,
        expected_provider_state_version: int,
        expected_window_state_version: int,
    ) -> QualificationWindow:
        def update(connection):
            provider = self._query_record(connection, provider_id)
            current = self._window(connection, provider_id, window_id)
            expected_provider = expected_provider_state_version
            expected_window = expected_window_state_version
            if (
                provider.state_version != expected_provider
                or current.state_version != expected_window
            ):
                raise RegistryUnavailable("stale qualification state")
            if (
                current.version_vector_sha256,
                current.calendar_generation,
                current.calendar_sha256,
            ) == (version_vector_sha256, calendar_generation, calendar_sha256):
                return current
            provider_cursor = connection.execute(
                "UPDATE provider_record SET state_version=state_version+1 WHERE provider_id=? AND state_version=?",
                (provider_id, expected_provider),
            )
            if provider_cursor.rowcount != 1:
                raise RegistryUnavailable("stale registry state")
            cursor = connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',version_vector_sha256=?,calendar_generation=?,calendar_sha256=?,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (
                    version_vector_sha256,
                    calendar_generation,
                    calendar_sha256,
                    provider_id,
                    window_id,
                    expected_window,
                ),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale qualification state")
            return self._window(connection, provider_id, window_id)

        return self._with_transaction(update)

    def attach_session(self, window_provider: str, window_id: str, *, provider_id: str) -> None:
        if provider_id != window_provider:
            raise ValueError("composite provider/window binding mismatch")
        try:
            self.read_status(provider_id)
        except RegistryUnavailable:
            raise ValueError("provider/window unavailable") from None


def json_array(values: tuple[str, ...]) -> str:
    import json

    return json.dumps(list(values), ensure_ascii=False, separators=(",", ":"))
