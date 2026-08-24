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
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from backend.app.market.shadow_registry_schema import (
    MIGRATION_ID,
    initialize_registry,
    shadow_sha256_canonical_json,
    shadow_validate_terminal_graph,
)

from .shadow_contracts import (
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


def make_token(provider_id: str, **kwargs) -> str:
    return _make_token(provider_id, **kwargs)


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
    def open(path: Path | str) -> sqlite3.Connection:
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
            connection = ShadowRegistryTerminalWriter.open(":memory:")
            initialize_registry(connection)
            connection.close()
            # A memory registry is intentionally used by tests through one connection below.
            self._memory_connection = ShadowRegistryTerminalWriter.open(":memory:")
            initialize_registry(self._memory_connection)
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink() or self.lock_path.is_symlink():
            raise RegistryUnavailable("registry path unavailable")
        if self.path.exists() and self.path.stat().st_mode & 0o777 != 0o600:
            raise RegistryUnavailable("registry permissions unavailable")
        if not self.lock_path.exists():
            self.lock_path.touch(mode=0o600)
        os.chmod(self.lock_path, 0o600)
        connection = self._open_writer()
        try:
            if not self._has_schema(connection):
                initialize_registry(connection)
        finally:
            connection.close()
        os.chmod(self.path, 0o600)

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
        if not self.lock_path.is_file() or self.lock_path.stat().st_mode & 0o777 != 0o600:
            raise RegistryUnavailable("registry lock unavailable")
        fd = os.open(self.lock_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
            yield
        except (OSError, EOFError) as exc:
            raise RegistryUnavailable("registry lock unavailable") from exc
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _open_writer(self) -> sqlite3.Connection:
        if not self._memory and self.path.exists() and self.path.stat().st_mode & 0o777 != 0o600:
            raise RegistryUnavailable("registry permissions unavailable")
        return getattr(self, "_memory_connection", None) or ShadowRegistryTerminalWriter.open(
            self.path
        )

    def _connection_for_read(self) -> sqlite3.Connection:
        if self._memory:
            return self._memory_connection
        if self.path.is_symlink() or not self.path.parent.is_dir():
            raise RegistryUnavailable("registry unavailable")
        if self.path.with_name(self.path.name + "-journal").exists():
            raise RegistryUnavailable("registry journal unavailable")
        parent_fd = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            fd = os.open(
                self.path.name,
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
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
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
                connection.execute("SELECT migration_id FROM schema_migration").fetchone()[0]
                != MIGRATION_ID
            ):
                raise RegistryUnavailable("registry migration unavailable")
        except (sqlite3.DatabaseError, TypeError) as exc:
            connection.close()
            raise RegistryUnavailable("registry schema unavailable") from exc
        return connection

    def _with_transaction(self, callback):
        with self._lock(shared=False):
            connection = self._open_writer()
            try:
                connection.execute("BEGIN IMMEDIATE")
                result = callback(connection)
                connection.commit()
                return result
            except RegistryUnavailable:
                connection.rollback()
                raise
            except (sqlite3.DatabaseError, OSError) as exc:
                connection.rollback()
                raise RegistryUnavailable("registry transaction unavailable") from exc
            finally:
                if not self._memory:
                    connection.close()

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
            AdmissionState.SHADOW: {AdmissionState.QUALIFIED, AdmissionState.QUARANTINED},
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
            cursor = connection.execute(
                "UPDATE provider_record SET admission_state=?, state_version=state_version+1 WHERE provider_id=? AND state_version=?",
                (state.value, provider_id, expected_state_version),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("stale registry state")
            return self._query_record(connection, provider_id).model_copy()

        return self._with_transaction(update)

    def reopen_quarantined(self, record: ShadowProviderRecord) -> ShadowProviderRecord:
        current = self.read_status(record.provider_id.value)
        if current.admission_state != AdmissionState.QUARANTINED:
            raise ValueError("provider is not quarantined")
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
            return self._query_record(connection, values.provider_id.value)

        return self._with_transaction(update)

    def put_terms_evidence(self, evidence: TermsEvidence) -> None:
        if not evidence._content_bytes:
            raise TermsEvidenceUnavailable("terms object unavailable")
        self.terms_object_root.mkdir(parents=True, exist_ok=True)
        target = self.terms_object_root / evidence.content_object_relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and target.read_bytes() != evidence._content_bytes:
            raise TermsEvidenceUnavailable("terms object changed")
        if not target.exists():
            target.write_bytes(evidence._content_bytes)
            os.chmod(target, 0o600)
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
        self, provider_id: str, evidence: TermsEvidence
    ) -> ShadowProviderRecord:
        if evidence.provider_id.value != provider_id:
            raise ValueError("terms/provider binding mismatch")

        def update(connection):
            cursor = connection.execute(
                "UPDATE provider_record SET terms_evidence_hash=?,terms_review_id=? WHERE provider_id=?",
                (evidence.manifest_sha256, evidence.review_id, provider_id),
            )
            if cursor.rowcount != 1:
                raise RegistryUnavailable("provider unavailable")
            return self._query_record(connection, provider_id)

        return self._with_transaction(update)

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
        target = self.terms_object_root / row[3]
        try:
            if target.is_symlink() or not target.parent.is_dir():
                raise RegistryUnavailable("terms evidence changed")
            parent_fd = os.open(target.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                fd = os.open(
                    target.name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=parent_fd,
                )
                try:
                    before = os.fstat(fd)
                    content = os.read(fd, 64 * 1024 * 1024 + 1)
                    after = os.fstat(fd)
                finally:
                    os.close(fd)
            finally:
                os.close(parent_fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or (before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns)
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

    def record_session(
        self, provider_id: str, window_id: str, *, success: bool
    ) -> QualificationWindow:
        def update(connection):
            current = self._window(connection, provider_id, window_id)
            count = current.consecutive_sessions + 1 if success else 0
            state = "qualified" if count >= 20 else "observing" if success else "reset"
            # A real terminal report supplies evidence/candidate/attestation fields; this
            # lightweight evaluator only records the observation count until that boundary.
            if state == "qualified":
                state = "observing"
            cursor = connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=?,window_state=?,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (count, state, provider_id, window_id, current.state_version),
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
    ) -> QualificationWindow:
        def update(connection):
            current = self._window(connection, provider_id, window_id)
            if (
                current.version_vector_sha256,
                current.calendar_generation,
                current.calendar_sha256,
            ) == (version_vector_sha256, calendar_generation, calendar_sha256):
                return current
            connection.execute(
                "UPDATE qualification_window SET consecutive_sessions=0,window_state='reset',version_vector_sha256=?,calendar_generation=?,calendar_sha256=?,state_version=state_version+1 WHERE provider_id=? AND window_id=? AND state_version=?",
                (
                    version_vector_sha256,
                    calendar_generation,
                    calendar_sha256,
                    provider_id,
                    window_id,
                    current.state_version,
                ),
            )
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
