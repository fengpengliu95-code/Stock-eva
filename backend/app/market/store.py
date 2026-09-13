import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
from contextlib import contextmanager
from ctypes import CDLL, c_char_p, c_int, c_uint, set_errno
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

import duckdb
from pydantic import ValidationError

from backend.app.market.continuity import (
    RepairAttempt,
    RepairFinalizationOutcome,
    RepairJob,
    RepairLease,
    RepairQueueConflictError,
    RepairQueueError,
    RepairQueueSnapshot,
    RepairRetryPolicy,
    repair_job_id,
    repair_request_key,
    require_safe_identifier,
    require_universe_id,
    require_utc,
)
from backend.app.market.models import DailyBar, RefreshResult

MAX_SYMBOL_RANGE_QUERY = 200
_DUCKDB_SAME_DATABASE_CONFIGURATION_ERROR = (
    "Connection Error: Can't open a connection to same database file with a different "
    "configuration than existing connections"
)
_REPAIR_JOB_COLUMNS = (
    "job_id",
    "trade_date",
    "universe_id",
    "state",
    "state_version",
    "attempt_count",
    "abandoned_attempt_count",
    "next_attempt_at",
    "lease_id",
    "lease_owner",
    "lease_expires_at",
    "last_attempt_id",
    "last_failure_stage",
    "last_failure_class",
    "created_at",
    "updated_at",
    "published_at",
)
_REPAIR_ATTEMPT_COLUMNS = (
    "attempt_id",
    "job_id",
    "attempt_number",
    "lease_id",
    "lease_owner",
    "outcome",
    "refresh_run_id",
    "failure_stage",
    "failure_class",
    "retryable",
    "started_at",
    "completed_at",
)
_REPAIR_JOBS_DDL = """
CREATE TABLE IF NOT EXISTS repair_jobs (
    job_id VARCHAR PRIMARY KEY,
    trade_date DATE NOT NULL,
    universe_id VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    state_version BIGINT NOT NULL,
    attempt_count INTEGER NOT NULL,
    abandoned_attempt_count INTEGER NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    lease_id VARCHAR UNIQUE,
    lease_owner VARCHAR,
    lease_expires_at TIMESTAMPTZ,
    last_attempt_id VARCHAR,
    last_failure_stage VARCHAR,
    last_failure_class VARCHAR,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    UNIQUE (trade_date, universe_id),
    CHECK (state IN ('pending', 'leased', 'retry_wait', 'published', 'dead_letter')),
    CHECK (state_version >= 1),
    CHECK (attempt_count >= 0),
    CHECK (abandoned_attempt_count >= 0 AND abandoned_attempt_count <= attempt_count),
    CHECK (
        (state = 'leased' AND lease_id IS NOT NULL AND lease_owner IS NOT NULL
         AND lease_expires_at IS NOT NULL)
        OR
        (state <> 'leased' AND lease_id IS NULL AND lease_owner IS NULL
         AND lease_expires_at IS NULL)
    ),
    CHECK (
        (state = 'published' AND published_at IS NOT NULL)
        OR (state <> 'published' AND published_at IS NULL)
    )
)
"""
_REPAIR_ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS repair_attempts (
    attempt_id VARCHAR PRIMARY KEY,
    job_id VARCHAR NOT NULL,
    attempt_number INTEGER NOT NULL,
    lease_id VARCHAR NOT NULL UNIQUE,
    lease_owner VARCHAR NOT NULL,
    outcome VARCHAR NOT NULL,
    refresh_run_id VARCHAR,
    failure_stage VARCHAR,
    failure_class VARCHAR,
    retryable BOOLEAN,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    UNIQUE (job_id, attempt_number),
    CHECK (attempt_number > 0),
    CHECK (outcome IN ('running', 'succeeded', 'failed', 'abandoned')),
    CHECK (
        (outcome = 'running' AND completed_at IS NULL AND retryable IS NULL)
        OR
        (outcome <> 'running' AND completed_at IS NOT NULL AND retryable IS NOT NULL)
    )
)
"""
_CONTINUITY_SCHEMA_META_DDL = """
CREATE TABLE IF NOT EXISTS continuity_schema_meta (
    singleton INTEGER PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    schema_hash VARCHAR NOT NULL,
    CHECK (singleton = 1),
    CHECK (schema_version = 1)
)
"""
_CONTINUITY_DDL = (
    _REPAIR_JOBS_DDL,
    _REPAIR_ATTEMPTS_DDL,
    _CONTINUITY_SCHEMA_META_DDL,
)
_CONTINUITY_SCHEMA_HASH = hashlib.sha256(
    "\n".join(" ".join(statement.split()) for statement in _CONTINUITY_DDL).encode()
).hexdigest()
_CONTINUITY_TABLES = ("continuity_schema_meta", "repair_attempts", "repair_jobs")
_SCHEMA_MIGRATION_DATABASE = "market.duckdb"
_SCHEMA_MIGRATION_RANDOM_DATABASE_PATTERN = re.compile(r"market\.[0-9a-f]{32}\.duckdb")
_RENAME_SWAP = 0x00000002
_RENAME_NOFOLLOW_ANY = 0x00000010
_RENAME_RESOLVE_BENEATH = 0x00000020
_SCHEMA_MIGRATION_CHILD = """
import os
import sys
os.fchdir(int(sys.argv[1]))
from backend.app.market.store import _bound_schema_migration_child
raise SystemExit(_bound_schema_migration_child(sys.argv[2:]))
"""


def _normalize_schema_expression(value: str | None) -> str | None:
    return None if value is None else re.sub(r"\s+", " ", value).strip()


def _continuity_schema_signature(connection: duckdb.DuckDBPyConnection) -> tuple:
    tables = tuple(
        (
            table,
            tuple(
                (row[1], row[2], row[3], row[4], row[5])
                for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
            ),
        )
        for table in _CONTINUITY_TABLES
    )
    constraints = tuple(
        sorted(
            (
                row[0],
                row[1],
                _normalize_schema_expression(row[2]),
                _normalize_schema_expression(row[3]),
                tuple(row[4] or ()),
            )
            for row in connection.execute(
                """
                SELECT table_name, constraint_type, constraint_text, expression,
                       constraint_column_names
                FROM duckdb_constraints()
                WHERE table_name IN ('continuity_schema_meta', 'repair_attempts', 'repair_jobs')
                """
            ).fetchall()
        )
    )
    return tables, constraints


@lru_cache(maxsize=1)
def _expected_continuity_schema_signature() -> tuple:
    connection = duckdb.connect(":memory:")
    try:
        for statement in _CONTINUITY_DDL:
            connection.execute(statement)
        return _continuity_schema_signature(connection)
    finally:
        connection.close()


def _schema_migration_database_name_is_safe(name: str) -> bool:
    return name == _SCHEMA_MIGRATION_DATABASE or bool(
        _SCHEMA_MIGRATION_RANDOM_DATABASE_PATTERN.fullmatch(name)
    )


def _schema_migration_artifact_names(database_name: str) -> set[str]:
    if not _schema_migration_database_name_is_safe(database_name):
        raise ValueError("invalid schema migration database name")
    return {database_name, f"{database_name}.wal"}


def _fingerprint_descriptor(descriptor: int) -> tuple[int, int, int, int, str]:
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("schema migration input is not regular")
    digest = hashlib.sha256()
    offset = 0
    while offset < before.st_size:
        chunk = os.pread(descriptor, min(1024 * 1024, before.st_size - offset), offset)
        if not chunk:
            raise ValueError("schema migration input changed")
        digest.update(chunk)
        offset += len(chunk)
    after = os.fstat(descriptor)
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError("schema migration input changed")
    return (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
        digest.hexdigest(),
    )


def _portable_fingerprint(
    fingerprint: tuple[int, int, int, int, str],
) -> tuple[int, int, int, str]:
    return fingerprint[0], fingerprint[1], fingerprint[2], fingerprint[4]


def _bound_schema_artifacts(database_name: str) -> list[dict[str, int | str]]:
    artifacts: list[dict[str, int | str]] = []
    for name in sorted(_schema_migration_artifact_names(database_name)):
        try:
            descriptor = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError:
            continue
        try:
            fingerprint = _portable_fingerprint(_fingerprint_descriptor(descriptor))
            artifacts.append(
                {
                    "name": name,
                    "device": fingerprint[0],
                    "inode": fingerprint[1],
                    "size": fingerprint[2],
                    "sha256": fingerprint[3],
                }
            )
        finally:
            os.close(descriptor)
    return artifacts


def _bound_schema_migration_child(arguments: list[str]) -> int:
    """Run only after the child has fchdir'd to the inherited private directory fd."""
    connection = None
    database_name: str | None = None
    status = "error"
    try:
        if not arguments or arguments[0] not in {"initialize_and_migrate", "migrate"}:
            raise ValueError("invalid schema migration action")
        action = arguments[0]
        if action == "initialize_and_migrate":
            if len(arguments) != 1 or os.listdir("."):
                raise ValueError("invalid new schema migration input")
            database_name = f"market.{secrets.token_hex(16)}.duckdb"
        else:
            if len(arguments) != 6:
                raise ValueError("invalid schema migration input")
            database_name = arguments[1]
            if not _schema_migration_database_name_is_safe(database_name):
                raise ValueError("invalid schema migration database name")
            if any(not re.fullmatch(r"0|[1-9][0-9]*", value) for value in arguments[2:5]):
                raise ValueError("invalid schema migration fingerprint")
            expected = (
                int(arguments[2]),
                int(arguments[3]),
                int(arguments[4]),
                arguments[5],
            )
            if (
                expected[0] < 0
                or expected[1] <= 0
                or expected[2] < 0
                or not re.fullmatch(r"[0-9a-f]{64}", expected[3])
            ):
                raise ValueError("invalid schema migration fingerprint")
            # The parent protects this private directory entry against replacement.
            # An actively malicious same-UID writer to the already-open inode is
            # outside the isolation that this writer lock can provide.
            input_descriptor = os.open(
                database_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                if _portable_fingerprint(_fingerprint_descriptor(input_descriptor)) != expected:
                    raise ValueError("schema migration input changed")
                entry = os.stat(database_name, follow_symlinks=False)
                if (entry.st_dev, entry.st_ino) != expected[:2]:
                    raise ValueError("schema migration input changed")
            finally:
                os.close(input_descriptor)
        connection = duckdb.connect(database_name)
        connection.execute("SET temp_directory = '.'")
        connection.begin()
        MarketStore._initialize_base_schema_on_connection(connection)
        MarketStore._initialize_continuity_schema_on_connection(connection)
        connection.commit()
        connection.close()
        connection = None
        validation = duckdb.connect(database_name, read_only=True)
        try:
            MarketStore._require_continuity_schema(validation)
        finally:
            validation.close()
        status = "ready"
    except Exception:
        if connection is not None:
            try:
                connection.rollback()
            except duckdb.Error:
                pass
    finally:
        if connection is not None:
            try:
                connection.close()
            except duckdb.Error:
                pass
    print(
        json.dumps(
            {
                "artifacts": (
                    _bound_schema_artifacts(database_name) if database_name is not None else []
                ),
                "database_name": database_name,
                "status": status,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return 0 if status == "ready" else 1


class MarketStoreReadError(RuntimeError):
    """A SELECT-only control database cannot be read safely."""


class MarketStore:
    def __init__(
        self,
        path: Path,
        *,
        temp_directory: Path | None = None,
        read_only: bool = False,
    ) -> None:
        self.path = path
        self.temp_directory = temp_directory or path.parent / ".duckdb-tmp"
        self.read_only = read_only

    def _open_writer_connection(
        self,
        *,
        path: Path | None = None,
        temp_directory: Path | None = None,
        create_directories: bool = True,
    ) -> duckdb.DuckDBPyConnection:
        if self.read_only:
            raise RuntimeError("read-only market store cannot open a writer connection")
        database_path = path or self.path
        temporary = temp_directory or self.temp_directory
        if create_directories:
            database_path.parent.mkdir(parents=True, exist_ok=True)
            temporary.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(database_path))
        connection.execute("SET temp_directory = ?", [str(temporary)])
        return connection

    def _connect(self) -> duckdb.DuckDBPyConnection:
        connection = self._open_writer_connection()
        self._initialize_base_schema_on_connection(connection)
        return connection

    @staticmethod
    def _initialize_base_schema_on_connection(
        connection: duckdb.DuckDBPyConnection,
    ) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_bars (
                trade_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                security_type VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                board VARCHAR NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                preclose DOUBLE NOT NULL,
                volume DOUBLE NOT NULL,
                amount DOUBLE NOT NULL,
                turnover_rate DOUBLE,
                pct_change DOUBLE,
                adjust_factor DOUBLE,
                price_adjustment VARCHAR NOT NULL,
                is_trading BOOLEAN NOT NULL,
                is_suspended BOOLEAN NOT NULL,
                is_st BOOLEAN NOT NULL,
                source VARCHAR NOT NULL,
                source_record_id VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                quality_status VARCHAR NOT NULL,
                quality_issues JSON NOT NULL,
                PRIMARY KEY (trade_date, symbol, source)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_runs (
                run_id VARCHAR PRIMARY KEY,
                request_key VARCHAR,
                run_kind VARCHAR NOT NULL DEFAULT 'daily',
                requested_date DATE NOT NULL,
                source VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                requested_count INTEGER NOT NULL,
                succeeded_count INTEGER NOT NULL,
                coverage_ratio DOUBLE,
                failed_symbols JSON NOT NULL,
                quality_issues JSON NOT NULL,
                error_message VARCHAR,
                failure_stage VARCHAR,
                failure_class VARCHAR,
                retryable BOOLEAN,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS published_snapshots (
                singleton INTEGER PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                published_at TIMESTAMPTZ NOT NULL,
                CHECK (singleton = 1)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS published_daily_bars (
                publication_run_id VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                security_type VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                board VARCHAR NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                preclose DOUBLE NOT NULL,
                volume DOUBLE NOT NULL,
                amount DOUBLE NOT NULL,
                turnover_rate DOUBLE,
                pct_change DOUBLE,
                adjust_factor DOUBLE,
                price_adjustment VARCHAR NOT NULL,
                is_trading BOOLEAN NOT NULL,
                is_suspended BOOLEAN NOT NULL,
                is_st BOOLEAN NOT NULL,
                source VARCHAR NOT NULL,
                source_record_id VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                quality_status VARCHAR NOT NULL,
                quality_issues JSON NOT NULL,
                PRIMARY KEY (trade_date, symbol, source)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS market_automation_state (
                singleton INTEGER PRIMARY KEY,
                target_session DATE,
                refresh_state VARCHAR NOT NULL,
                attempt_count INTEGER NOT NULL,
                last_attempt_at TIMESTAMPTZ,
                last_success_at TIMESTAMPTZ,
                next_retry_at TIMESTAMPTZ,
                calendar_status VARCHAR NOT NULL,
                error_code VARCHAR,
                CHECK (singleton = 1)
            )
            """
        )
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS coverage_ratio DOUBLE"
        )
        connection.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS request_key VARCHAR")
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS run_kind VARCHAR DEFAULT 'daily'"
        )
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS quality_issues JSON DEFAULT '[]'"
        )
        # Legacy databases that lost these columns must be restored to the
        # same nullability/default contract as a newly-created canonical
        # schema before descriptor-bound pointer reads.
        connection.execute("ALTER TABLE refresh_runs ALTER COLUMN run_kind SET NOT NULL")
        connection.execute("ALTER TABLE refresh_runs ALTER COLUMN quality_issues SET NOT NULL")
        connection.execute("ALTER TABLE refresh_runs ALTER COLUMN quality_issues DROP DEFAULT")
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS failure_stage VARCHAR"
        )
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS failure_class VARCHAR"
        )
        connection.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS retryable BOOLEAN")

    def initialize_schema(self) -> None:
        """Explicitly initialize or migrate schema for a writer-owned store."""
        connection = self._connect()
        connection.close()

    def initialize_all_writer_schema(self, *, staging_directory: Path) -> None:
        """Atomically migrate base and continuity schema under the caller-owned lock."""
        if self.read_only:
            raise RepairQueueError("read-only market store cannot migrate writer schema")
        staging_fd = -1
        target_parent_fd = -1
        created_directories: list[tuple[int, str, tuple[int, int]]] = []
        succeeded = False
        try:
            staging_fd, staging_identity, _ = self._open_directory_chain(
                staging_directory, create=False
            )
            target_parent_fd, target_parent_identity, created_directories = (
                self._open_directory_chain(self.path.parent, create=True)
            )
            self._require_directory_path_identity(staging_directory, staging_identity)
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            try:
                target_stat = os.stat(
                    self.path.name,
                    dir_fd=target_parent_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                target_stat = None
            if target_stat is not None:
                if not stat.S_ISREG(target_stat.st_mode):
                    raise RepairQueueError("market schema target is not a regular file")
                self._initialize_all_existing_writer_schema(
                    staging_directory,
                    staging_identity=staging_identity,
                    target_parent_fd=target_parent_fd,
                    target_parent_identity=target_parent_identity,
                    expected_inode=(target_stat.st_dev, target_stat.st_ino),
                )
            else:
                self._initialize_all_new_writer_schema(
                    staging_directory,
                    staging_parent_fd=staging_fd,
                    staging_parent_identity=staging_identity,
                    target_parent_fd=target_parent_fd,
                    target_parent_identity=target_parent_identity,
                )
            succeeded = True
        except RepairQueueError:
            raise
        except (duckdb.Error, OSError, TypeError, ValueError):
            raise RepairQueueError("market schema migration failed") from None
        finally:
            if target_parent_fd >= 0:
                os.close(target_parent_fd)
            if staging_fd >= 0:
                os.close(staging_fd)
            self._finish_created_directories(created_directories, remove=not succeeded)

    @classmethod
    def _open_directory_chain(
        cls,
        path: Path,
        *,
        create: bool,
    ) -> tuple[int, tuple[int, int], list[tuple[int, str, tuple[int, int]]]]:
        absolute = Path(os.path.abspath(path))
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(absolute.anchor, flags)
        created: list[tuple[int, str, tuple[int, int]]] = []
        try:
            for component in absolute.parts[1:]:
                try:
                    child = os.open(component, flags, dir_fd=descriptor)
                except FileNotFoundError:
                    if not create:
                        raise
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                    child_stat = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                    identity = (child_stat.st_dev, child_stat.st_ino)
                    created.append((os.dup(descriptor), component, identity))
                    child = os.open(component, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            directory_stat = os.fstat(descriptor)
            if not stat.S_ISDIR(directory_stat.st_mode):
                raise RepairQueueError("market schema migration directory is unsafe")
            return descriptor, (directory_stat.st_dev, directory_stat.st_ino), created
        except Exception:
            os.close(descriptor)
            cls._finish_created_directories(created, remove=True)
            raise

    @classmethod
    def _require_directory_path_identity(
        cls,
        path: Path,
        expected_identity: tuple[int, int],
    ) -> None:
        descriptor = -1
        try:
            descriptor, identity, _ = cls._open_directory_chain(path, create=False)
            if identity != expected_identity:
                raise RepairQueueConflictError("market schema directory changed during migration")
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _finish_created_directories(
        created: list[tuple[int, str, tuple[int, int]]],
        *,
        remove: bool,
    ) -> None:
        for parent_fd, name, expected_identity in reversed(created):
            try:
                if remove:
                    try:
                        current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if (
                        stat.S_ISDIR(current.st_mode)
                        and (current.st_dev, current.st_ino) == expected_identity
                    ):
                        try:
                            os.rmdir(name, dir_fd=parent_fd)
                        except OSError:
                            pass
            finally:
                os.close(parent_fd)

    @staticmethod
    def _require_bound_regular_file(
        parent_fd: int,
        name: str,
        expected_inode: tuple[int, int],
    ) -> None:
        try:
            target_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            raise RepairQueueConflictError(
                "market schema target changed during migration"
            ) from None
        if (
            not stat.S_ISREG(target_stat.st_mode)
            or (target_stat.st_dev, target_stat.st_ino) != expected_inode
        ):
            raise RepairQueueConflictError("market schema target changed during migration")

    @classmethod
    def _chmod_bound_file(
        cls,
        parent_fd: int,
        name: str,
        expected_inode: tuple[int, int],
        mode: int,
    ) -> None:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        try:
            target_stat = os.fstat(descriptor)
            if (target_stat.st_dev, target_stat.st_ino) != expected_inode:
                raise RepairQueueConflictError("market schema target changed during migration")
            os.fchmod(descriptor, mode)
        finally:
            os.close(descriptor)
        cls._require_bound_regular_file(parent_fd, name, expected_inode)

    @staticmethod
    def _run_bound_schema_migration_child(
        directory_fd: int,
        action: str,
        *,
        database_name: str | None = None,
        expected_fingerprint: tuple[int, int, int, str] | None = None,
    ) -> tuple[bool, str | None, dict[str, tuple[int, int, int, str]]]:
        if action not in {"initialize_and_migrate", "migrate"}:
            raise RepairQueueError("market schema migration action is invalid")
        if action == "initialize_and_migrate":
            if database_name is not None or expected_fingerprint is not None:
                raise RepairQueueError("market schema migration input is invalid")
            child_arguments = [action]
        else:
            if (
                database_name is None
                or expected_fingerprint is None
                or not _schema_migration_database_name_is_safe(database_name)
            ):
                raise RepairQueueError("market schema migration input is invalid")
            child_arguments = [
                action,
                database_name,
                *(str(value) for value in expected_fingerprint),
            ]
        project_root = str(Path(__file__).resolve().parents[3])
        environment = {
            "LC_ALL": "C",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONPATH": project_root,
        }
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    _SCHEMA_MIGRATION_CHILD,
                    str(directory_fd),
                    *child_arguments,
                ],
                pass_fds=(directory_fd,),
                close_fds=True,
                capture_output=True,
                text=True,
                timeout=30,
                env=environment,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            raise RepairQueueError("market schema migration child failed") from None
        try:
            payload = json.loads(result.stdout)
            if set(payload) != {"artifacts", "database_name", "status"}:
                raise ValueError
            status = payload["status"]
            observed_database_name = payload["database_name"]
            if status not in {"ready", "error"} or not isinstance(payload["artifacts"], list):
                raise ValueError
            if observed_database_name is not None and (
                not isinstance(observed_database_name, str)
                or not _schema_migration_database_name_is_safe(observed_database_name)
            ):
                raise ValueError
            artifacts: dict[str, tuple[int, int, int, str]] = {}
            for artifact in payload["artifacts"]:
                if not isinstance(artifact, dict) or set(artifact) != {
                    "device",
                    "inode",
                    "name",
                    "sha256",
                    "size",
                }:
                    raise ValueError
                name = artifact["name"]
                device = artifact["device"]
                inode = artifact["inode"]
                size = artifact["size"]
                sha256 = artifact["sha256"]
                if (
                    observed_database_name is None
                    or name not in _schema_migration_artifact_names(observed_database_name)
                    or name in artifacts
                    or not isinstance(device, int)
                    or isinstance(device, bool)
                    or device < 0
                    or not isinstance(inode, int)
                    or isinstance(inode, bool)
                    or inode <= 0
                    or not isinstance(size, int)
                    or isinstance(size, bool)
                    or size < 0
                    or not isinstance(sha256, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", sha256)
                ):
                    raise ValueError
                artifacts[name] = (device, inode, size, sha256)
        except (json.JSONDecodeError, TypeError, ValueError):
            raise RepairQueueError("market schema migration child failed") from None
        ready = result.returncode == 0 and status == "ready"
        if result.returncode == 0 and status != "ready":
            raise RepairQueueError("market schema migration child failed")
        if ready and observed_database_name is None:
            raise RepairQueueError("market schema migration child failed")
        if (
            ready
            and action == "initialize_and_migrate"
            and not (
                isinstance(observed_database_name, str)
                and _SCHEMA_MIGRATION_RANDOM_DATABASE_PATTERN.fullmatch(observed_database_name)
            )
        ):
            raise RepairQueueError("market schema migration child failed")
        if ready and action == "migrate" and observed_database_name != database_name:
            raise RepairQueueError("market schema migration child failed")
        return ready, observed_database_name, artifacts

    @staticmethod
    def _set_bound_directory_flags(
        directory_fd: int,
        expected_identity: tuple[int, int],
        flags: int,
    ) -> None:
        before = os.fstat(directory_fd)
        if not stat.S_ISDIR(before.st_mode) or (before.st_dev, before.st_ino) != expected_identity:
            raise RepairQueueConflictError("market schema staging directory changed")
        try:
            fchflags = CDLL(None, use_errno=True).fchflags
        except AttributeError:
            raise RepairQueueError("secure market schema staging is unavailable") from None
        fchflags.argtypes = [c_int, c_uint]
        fchflags.restype = c_int
        set_errno(0)
        if fchflags(directory_fd, flags) != 0:
            raise RepairQueueError("secure market schema staging failed")
        after = os.fstat(directory_fd)
        if (after.st_dev, after.st_ino) != expected_identity or getattr(
            after, "st_flags", None
        ) != flags:
            raise RepairQueueConflictError("market schema staging flags changed")

    @staticmethod
    def _atomic_exchange_bound_files(
        source_fd: int,
        source_name: str,
        target_fd: int,
        target_name: str,
    ) -> None:
        if (
            not _schema_migration_database_name_is_safe(source_name)
            or target_name in {"", ".", ".."}
            or "/" in target_name
            or "\x00" in target_name
        ):
            raise RepairQueueError("market schema exchange target is invalid")
        try:
            renameatx = CDLL(None, use_errno=True).renameatx_np
        except AttributeError:
            raise RepairQueueError("atomic market schema exchange is unavailable") from None
        renameatx.argtypes = [c_int, c_char_p, c_int, c_char_p, c_uint]
        renameatx.restype = c_int
        flags = _RENAME_SWAP | _RENAME_NOFOLLOW_ANY | _RENAME_RESOLVE_BENEATH
        set_errno(0)
        if (
            renameatx(
                source_fd,
                source_name.encode(),
                target_fd,
                target_name.encode(),
                flags,
            )
            != 0
        ):
            raise RepairQueueError("atomic market schema exchange failed")

    @staticmethod
    def _bound_file_fingerprint(
        descriptor: int,
    ) -> tuple[int, int, int, int, str]:
        try:
            return _fingerprint_descriptor(descriptor)
        except ValueError:
            raise RepairQueueConflictError("market schema file changed") from None

    @classmethod
    def _bound_path_fingerprint(
        cls,
        parent_fd: int,
        name: str,
    ) -> tuple[int, int, int, int, str]:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        try:
            return cls._bound_file_fingerprint(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _copy_bound_regular_file(
        cls,
        source_fd: int,
        expected_fingerprint: tuple[int, int, int, int, str],
        destination_parent_fd: int,
        destination_name: str,
    ) -> tuple[int, int, int, str]:
        destination_fd = -1
        try:
            before = cls._bound_file_fingerprint(source_fd)
            if before != expected_fingerprint:
                raise RepairQueueConflictError("market schema source changed during copy")
            destination_fd = os.open(
                destination_name,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=destination_parent_fd,
            )
            offset = 0
            while offset < before[2]:
                chunk = os.pread(source_fd, min(1024 * 1024, before[2] - offset), offset)
                if not chunk:
                    raise RepairQueueConflictError("market schema source changed during copy")
                written = 0
                while written < len(chunk):
                    count = os.write(destination_fd, chunk[written:])
                    if count <= 0:
                        raise RepairQueueError("market schema copy failed")
                    written += count
                offset += len(chunk)
            os.fsync(destination_fd)
            destination_stat = os.fstat(destination_fd)
            after = cls._bound_file_fingerprint(source_fd)
            if before != after:
                raise RepairQueueConflictError("market schema source changed during copy")
            copied = cls._bound_file_fingerprint(destination_fd)
            if copied[0:3] != (
                destination_stat.st_dev,
                destination_stat.st_ino,
                destination_stat.st_size,
            ):
                raise RepairQueueConflictError("market schema copy changed")
            return _portable_fingerprint(copied)
        finally:
            if destination_fd >= 0:
                os.close(destination_fd)

    @staticmethod
    def _merge_registered_artifacts(
        registered: dict[str, tuple[int, int, int, str]],
        observed: dict[str, tuple[int, int, int, str]],
    ) -> None:
        for name, identity in observed.items():
            prior = registered.get(name)
            if prior is not None and prior[:2] != identity[:2]:
                raise RepairQueueConflictError("market schema artifact identity changed")
            registered[name] = identity

    @classmethod
    def _require_registered_artifacts(
        cls,
        directory_fd: int,
        registered: dict[str, tuple[int, int, int, str]],
        database_name: str,
    ) -> None:
        names = set(os.listdir(directory_fd))
        if names != set(registered) or database_name not in names:
            raise RepairQueueConflictError("market schema staging artifacts changed")
        if names - _schema_migration_artifact_names(database_name):
            raise RepairQueueConflictError("market schema staging artifacts changed")
        for name, fingerprint in registered.items():
            cls._require_bound_regular_file(directory_fd, name, fingerprint[:2])
            observed = _portable_fingerprint(cls._bound_path_fingerprint(directory_fd, name))
            if observed != fingerprint:
                raise RepairQueueConflictError("market schema staging artifact changed")

    @classmethod
    def _remove_registered_artifacts(
        cls,
        directory_fd: int,
        registered: dict[str, tuple[int, int, int, str]],
    ) -> None:
        for name, fingerprint in sorted(registered.items(), reverse=True):
            try:
                cls._require_bound_regular_file(directory_fd, name, fingerprint[:2])
                if (
                    _portable_fingerprint(cls._bound_path_fingerprint(directory_fd, name))
                    != fingerprint
                ):
                    continue
            except RepairQueueError:
                continue
            try:
                os.unlink(name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass

    def _create_bound_staging_root(
        self,
        staging_parent_fd: int,
    ) -> tuple[str, int, tuple[int, int]]:
        staging_name = f".{self.path.name}.{uuid4().hex}.migration"
        os.mkdir(staging_name, mode=0o700, dir_fd=staging_parent_fd)
        root_stat = os.stat(staging_name, dir_fd=staging_parent_fd, follow_symlinks=False)
        staging_identity = (root_stat.st_dev, root_stat.st_ino)
        staging_root_fd = os.open(
            staging_name,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=staging_parent_fd,
        )
        return staging_name, staging_root_fd, staging_identity

    @staticmethod
    def _remove_bound_staging_root(
        staging_parent_fd: int,
        staging_name: str,
        staging_identity: tuple[int, int],
    ) -> None:
        try:
            current = os.stat(
                staging_name,
                dir_fd=staging_parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            return
        if stat.S_ISDIR(current.st_mode) and (current.st_dev, current.st_ino) == staging_identity:
            try:
                os.rmdir(staging_name, dir_fd=staging_parent_fd)
            except OSError:
                pass

    @classmethod
    def _rollback_uncommitted_exchange(
        cls,
        *,
        staging_root_fd: int,
        database_name: str,
        target_parent_fd: int,
        target_name: str,
        migrated_fingerprint: tuple[int, int, int, str],
    ) -> tuple[int, int, int, str]:
        try:
            target_fingerprint = _portable_fingerprint(
                cls._bound_path_fingerprint(target_parent_fd, target_name)
            )
            displaced_fingerprint = _portable_fingerprint(
                cls._bound_path_fingerprint(staging_root_fd, database_name)
            )
            if target_fingerprint != migrated_fingerprint:
                raise RepairQueueConflictError("market schema exchange cannot be rolled back")
            cls._atomic_exchange_bound_files(
                staging_root_fd,
                database_name,
                target_parent_fd,
                target_name,
            )
            if (
                _portable_fingerprint(cls._bound_path_fingerprint(target_parent_fd, target_name))
                != displaced_fingerprint
                or _portable_fingerprint(
                    cls._bound_path_fingerprint(staging_root_fd, database_name)
                )
                != migrated_fingerprint
            ):
                raise RepairQueueConflictError("market schema exchange rollback changed")
            os.fsync(target_parent_fd)
            return displaced_fingerprint
        except (OSError, RepairQueueError):
            raise RepairQueueConflictError("market schema exchange recovery is uncertain") from None

    def _initialize_all_existing_writer_schema(
        self,
        staging_directory: Path,
        *,
        staging_identity: tuple[int, int],
        target_parent_fd: int,
        target_parent_identity: tuple[int, int],
        expected_inode: tuple[int, int],
    ) -> None:
        staging_parent_fd = -1
        staging_root_fd = -1
        staging_name: str | None = None
        staging_root_identity: tuple[int, int] | None = None
        registered: dict[str, tuple[int, int, int, str]] = {}
        original_flags = 0
        flags_may_be_protected = False
        flag_restore_failed = False
        source_fd = -1
        try:
            staging_parent_fd, current_staging_identity, _ = self._open_directory_chain(
                staging_directory, create=False
            )
            if current_staging_identity != staging_identity:
                raise RepairQueueConflictError("market schema directory changed during migration")
            staging_name, staging_root_fd, staging_root_identity = self._create_bound_staging_root(
                staging_parent_fd
            )
            original_flags = getattr(os.fstat(staging_root_fd), "st_flags", 0)
            flags_may_be_protected = True
            self._set_bound_directory_flags(
                staging_root_fd,
                staging_root_identity,
                original_flags | stat.UF_APPEND,
            )
            self._require_directory_path_identity(staging_directory, staging_identity)
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            self._require_bound_regular_file(target_parent_fd, self.path.name, expected_inode)
            source_fd = os.open(
                self.path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=target_parent_fd,
            )
            source_fingerprint = self._bound_file_fingerprint(source_fd)
            if source_fingerprint[:2] != expected_inode:
                raise RepairQueueConflictError("market schema source changed before copy")
            copied_fingerprint = self._copy_bound_regular_file(
                source_fd,
                source_fingerprint,
                staging_root_fd,
                _SCHEMA_MIGRATION_DATABASE,
            )
            registered[_SCHEMA_MIGRATION_DATABASE] = copied_fingerprint
            ready, database_name, observed = self._run_bound_schema_migration_child(
                staging_root_fd,
                "migrate",
                database_name=_SCHEMA_MIGRATION_DATABASE,
                expected_fingerprint=copied_fingerprint,
            )
            if database_name != _SCHEMA_MIGRATION_DATABASE:
                raise RepairQueueError("market schema migration child failed")
            self._merge_registered_artifacts(registered, observed)
            if not ready:
                raise RepairQueueError("market schema migration child failed")
            self._require_registered_artifacts(
                staging_root_fd,
                registered,
                _SCHEMA_MIGRATION_DATABASE,
            )
            migrated_fingerprint = registered[_SCHEMA_MIGRATION_DATABASE]
            self._require_directory_path_identity(staging_directory, staging_identity)
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            self._require_bound_regular_file(target_parent_fd, self.path.name, expected_inode)
            try:
                self._set_bound_directory_flags(
                    staging_root_fd,
                    staging_root_identity,
                    original_flags,
                )
            except RepairQueueError:
                flag_restore_failed = True
                raise
            flags_may_be_protected = False
            removable = {
                name: identity
                for name, identity in registered.items()
                if name != _SCHEMA_MIGRATION_DATABASE
            }
            self._remove_registered_artifacts(staging_root_fd, removable)
            for name in removable:
                registered.pop(name, None)
            self._require_registered_artifacts(
                staging_root_fd,
                registered,
                _SCHEMA_MIGRATION_DATABASE,
            )
            self._chmod_bound_file(
                staging_root_fd,
                _SCHEMA_MIGRATION_DATABASE,
                migrated_fingerprint[:2],
                0o600,
            )
            migrated_fd = os.open(
                _SCHEMA_MIGRATION_DATABASE,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=staging_root_fd,
            )
            try:
                if (
                    _portable_fingerprint(self._bound_file_fingerprint(migrated_fd))
                    != migrated_fingerprint
                ):
                    raise RepairQueueConflictError("market schema staging artifact changed")
                os.fsync(migrated_fd)
            finally:
                os.close(migrated_fd)
            os.fsync(staging_root_fd)
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            self._require_bound_regular_file(target_parent_fd, self.path.name, expected_inode)
            if self._bound_file_fingerprint(source_fd) != source_fingerprint:
                raise RepairQueueConflictError("market schema original evidence changed")
            if _portable_fingerprint(
                self._bound_path_fingerprint(target_parent_fd, self.path.name)
            ) != _portable_fingerprint(source_fingerprint):
                raise RepairQueueConflictError("market schema original evidence changed")
            exchange_completed = False
            try:
                self._atomic_exchange_bound_files(
                    staging_root_fd,
                    _SCHEMA_MIGRATION_DATABASE,
                    target_parent_fd,
                    self.path.name,
                )
                exchange_completed = True
                if (
                    _portable_fingerprint(
                        self._bound_path_fingerprint(target_parent_fd, self.path.name)
                    )
                    != migrated_fingerprint
                    or _portable_fingerprint(
                        self._bound_path_fingerprint(
                            staging_root_fd,
                            _SCHEMA_MIGRATION_DATABASE,
                        )
                    )
                    != _portable_fingerprint(source_fingerprint)
                    or self._bound_file_fingerprint(source_fd) != source_fingerprint
                ):
                    raise RepairQueueConflictError("market schema exchange result changed")
                self._require_directory_path_identity(
                    self.path.parent,
                    target_parent_identity,
                )
                os.fsync(target_parent_fd)
            except (OSError, RepairQueueError):
                if exchange_completed:
                    try:
                        self._rollback_uncommitted_exchange(
                            staging_root_fd=staging_root_fd,
                            database_name=_SCHEMA_MIGRATION_DATABASE,
                            target_parent_fd=target_parent_fd,
                            target_name=self.path.name,
                            migrated_fingerprint=migrated_fingerprint,
                        )
                    except RepairQueueError:
                        registered.clear()
                        raise
                    registered[_SCHEMA_MIGRATION_DATABASE] = migrated_fingerprint
                    raise RepairQueueConflictError(
                        "market schema target changed during exchange"
                    ) from None
                raise
            registered[_SCHEMA_MIGRATION_DATABASE] = _portable_fingerprint(source_fingerprint)
        finally:
            if flags_may_be_protected and not flag_restore_failed and staging_root_fd >= 0:
                try:
                    self._set_bound_directory_flags(
                        staging_root_fd,
                        staging_root_identity,
                        original_flags,
                    )
                except RepairQueueError:
                    flag_restore_failed = True
            if staging_root_fd >= 0:
                if not flag_restore_failed:
                    self._remove_registered_artifacts(staging_root_fd, registered)
                os.close(staging_root_fd)
            if (
                staging_parent_fd >= 0
                and staging_name is not None
                and staging_root_identity is not None
                and not flag_restore_failed
            ):
                self._remove_bound_staging_root(
                    staging_parent_fd, staging_name, staging_root_identity
                )
            if staging_parent_fd >= 0:
                os.close(staging_parent_fd)
            if source_fd >= 0:
                os.close(source_fd)

    def _initialize_all_new_writer_schema(
        self,
        staging_directory: Path,
        *,
        staging_parent_fd: int,
        staging_parent_identity: tuple[int, int],
        target_parent_fd: int,
        target_parent_identity: tuple[int, int],
    ) -> None:
        staging_name: str | None = None
        staging_root: Path | None = None
        staging_root_fd = -1
        staging_identity: tuple[int, int] | None = None
        published_inode: tuple[int, int] | None = None
        staging_owned = False
        target_published = False
        publication_complete = False
        registered: dict[str, tuple[int, int, int, str]] = {}
        original_flags = 0
        flags_may_be_protected = False
        flag_restore_failed = False
        try:
            staging_name, staging_root_fd, staging_identity = self._create_bound_staging_root(
                staging_parent_fd
            )
            staging_root = staging_directory / staging_name
            staging_owned = True
            original_flags = getattr(os.fstat(staging_root_fd), "st_flags", 0)
            flags_may_be_protected = True
            self._set_bound_directory_flags(
                staging_root_fd,
                staging_identity,
                original_flags | stat.UF_APPEND,
            )
            self._require_directory_path_identity(staging_directory, staging_parent_identity)
            assert staging_root is not None
            self._require_directory_path_identity(staging_root, staging_identity)
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            ready, database_name, observed = self._run_bound_schema_migration_child(
                staging_root_fd,
                "initialize_and_migrate",
            )
            self._merge_registered_artifacts(registered, observed)
            if not ready or database_name is None:
                raise RepairQueueError("market schema migration child failed")
            self._require_registered_artifacts(
                staging_root_fd,
                registered,
                database_name,
            )
            published_fingerprint = registered[database_name]
            published_inode = published_fingerprint[:2]
            self._require_directory_path_identity(staging_directory, staging_parent_identity)
            self._require_directory_path_identity(staging_root, staging_identity)
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            try:
                self._set_bound_directory_flags(
                    staging_root_fd,
                    staging_identity,
                    original_flags,
                )
            except RepairQueueError:
                flag_restore_failed = True
                raise
            flags_may_be_protected = False
            removable = {
                name: identity for name, identity in registered.items() if name != database_name
            }
            self._remove_registered_artifacts(staging_root_fd, removable)
            for name in removable:
                registered.pop(name, None)
            self._require_registered_artifacts(staging_root_fd, registered, database_name)
            self._chmod_bound_file(
                staging_root_fd,
                database_name,
                published_inode,
                0o600,
            )
            published_fd = os.open(
                database_name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=staging_root_fd,
            )
            try:
                if (
                    _portable_fingerprint(self._bound_file_fingerprint(published_fd))
                    != published_fingerprint
                ):
                    raise RepairQueueConflictError("market schema staging artifact changed")
                os.fsync(published_fd)
            finally:
                os.close(published_fd)
            os.fsync(staging_root_fd)
            os.link(
                database_name,
                self.path.name,
                src_dir_fd=staging_root_fd,
                dst_dir_fd=target_parent_fd,
                follow_symlinks=False,
            )
            target_published = True
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            self._require_bound_regular_file(target_parent_fd, self.path.name, published_inode)
            self._remove_registered_artifacts(staging_root_fd, registered)
            registered.clear()
            os.close(staging_root_fd)
            staging_root_fd = -1
            os.rmdir(staging_name, dir_fd=staging_parent_fd)
            staging_owned = False
            self._require_directory_path_identity(staging_directory, staging_parent_identity)
            self._require_directory_path_identity(self.path.parent, target_parent_identity)
            self._require_bound_regular_file(target_parent_fd, self.path.name, published_inode)
            publication_complete = True
        finally:
            if flags_may_be_protected and not flag_restore_failed and staging_root_fd >= 0:
                try:
                    self._set_bound_directory_flags(
                        staging_root_fd,
                        staging_identity,
                        original_flags,
                    )
                except RepairQueueError:
                    flag_restore_failed = True
            if target_published and not publication_complete and published_inode is not None:
                try:
                    self._require_bound_regular_file(
                        target_parent_fd, self.path.name, published_inode
                    )
                except RepairQueueError:
                    pass
                else:
                    try:
                        os.unlink(self.path.name, dir_fd=target_parent_fd)
                    except FileNotFoundError:
                        pass
            if staging_root_fd >= 0:
                if not flag_restore_failed:
                    self._remove_registered_artifacts(staging_root_fd, registered)
                os.close(staging_root_fd)
            if (
                staging_owned
                and staging_identity is not None
                and staging_name is not None
                and not flag_restore_failed
            ):
                self._remove_bound_staging_root(staging_parent_fd, staging_name, staging_identity)

    def _connect_continuity_writer(self) -> duckdb.DuckDBPyConnection:
        if self.read_only:
            raise RepairQueueError("read-only market store cannot write repair queue")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        try:
            connection = duckdb.connect(str(self.path))
            connection.execute("SET temp_directory = ?", [str(self.temp_directory)])
            return connection
        except (duckdb.Error, OSError, TypeError, ValueError):
            raise RepairQueueError("repair queue database operation failed") from None

    @staticmethod
    def _continuity_schema_is_valid(connection: duckdb.DuckDBPyConnection) -> bool:
        try:
            if _continuity_schema_signature(connection) != (
                _expected_continuity_schema_signature()
            ):
                return False
            rows = connection.execute(
                """
                SELECT singleton, schema_version, schema_hash
                FROM continuity_schema_meta
                """
            ).fetchall()
        except duckdb.Error:
            return False
        return rows == [(1, 1, _CONTINUITY_SCHEMA_HASH)]

    @classmethod
    def _require_continuity_schema(cls, connection: duckdb.DuckDBPyConnection) -> None:
        if not cls._continuity_schema_is_valid(connection):
            raise RepairQueueError("repair queue schema is unavailable")

    def initialize_continuity_schema(self) -> None:
        """Create additive queue tables while the caller owns market-refresh.lock."""
        connection = self._connect_continuity_writer()
        try:
            connection.begin()
            self._initialize_continuity_schema_on_connection(connection)
            connection.commit()
        except RepairQueueError:
            connection.rollback()
            raise
        except (duckdb.Error, OSError, TypeError, ValueError):
            connection.rollback()
            raise RepairQueueError("repair queue schema migration failed") from None
        finally:
            connection.close()

    @classmethod
    def _initialize_continuity_schema_on_connection(
        cls,
        connection: duckdb.DuckDBPyConnection,
    ) -> None:
        for statement in _CONTINUITY_DDL:
            connection.execute(statement)
        connection.execute(
            """
            INSERT OR REPLACE INTO continuity_schema_meta (
                singleton, schema_version, schema_hash
            ) VALUES (1, 1, ?)
            """,
            [_CONTINUITY_SCHEMA_HASH],
        )
        cls._require_continuity_schema(connection)
        cls._validate_queue_on_connection(connection)

    @staticmethod
    def _repair_job_from_row(row) -> RepairJob:
        return RepairJob(
            job_id=row[0],
            trade_date=row[1],
            universe_id=row[2],
            state=row[3],
            state_version=row[4],
            attempt_count=row[5],
            abandoned_attempt_count=row[6],
            next_attempt_at=row[7],
            lease_id=row[8],
            lease_owner=row[9],
            lease_expires_at=row[10],
            last_attempt_id=row[11],
            last_failure_stage=row[12],
            last_failure_class=row[13],
            created_at=row[14],
            updated_at=row[15],
            published_at=row[16],
        )

    @staticmethod
    def _repair_attempt_from_row(row) -> RepairAttempt:
        return RepairAttempt(
            attempt_id=row[0],
            job_id=row[1],
            attempt_number=row[2],
            lease_id=row[3],
            lease_owner=row[4],
            outcome=row[5],
            refresh_run_id=row[6],
            failure_stage=row[7],
            failure_class=row[8],
            retryable=row[9],
            started_at=row[10],
            completed_at=row[11],
        )

    @staticmethod
    def _validated_lease(value: RepairLease) -> RepairLease:
        if not isinstance(value, RepairLease):
            raise RepairQueueError("repair lease contract is invalid")
        try:
            return RepairLease.model_validate(value.model_dump(mode="python", round_trip=True))
        except ValidationError as exc:
            raise RepairQueueError("repair lease contract is invalid") from exc

    @staticmethod
    def _validated_retry_policy(value: RepairRetryPolicy) -> RepairRetryPolicy:
        if not isinstance(value, RepairRetryPolicy):
            raise RepairQueueError("repair retry policy is invalid")
        try:
            return RepairRetryPolicy.model_validate(
                value.model_dump(mode="python", round_trip=True)
            )
        except ValidationError as exc:
            raise RepairQueueError("repair retry policy is invalid") from exc

    @staticmethod
    def _validated_refresh_result(value: RefreshResult) -> RefreshResult:
        if not isinstance(value, RefreshResult):
            raise RepairQueueError("repair refresh result contract is invalid")
        try:
            result = RefreshResult.model_validate(value.model_dump(mode="python", round_trip=True))
        except ValidationError as exc:
            raise RepairQueueError("repair refresh result contract is invalid") from exc
        started_at = require_utc(result.started_at)
        completed_at = require_utc(result.completed_at)
        if completed_at < started_at:
            raise RepairQueueError("repair refresh result timestamps are invalid")
        return result

    @classmethod
    def _validate_job_attempts_on_connection(
        cls,
        connection: duckdb.DuckDBPyConnection,
        job: RepairJob,
    ) -> None:
        attempts = tuple(
            cls._repair_attempt_from_row(row)
            for row in connection.execute(
                f"SELECT {', '.join(_REPAIR_ATTEMPT_COLUMNS)} FROM repair_attempts "
                "WHERE job_id = ? ORDER BY attempt_number",
                [job.job_id],
            ).fetchall()
        )
        RepairQueueSnapshot(status="ready", jobs=(job,), attempts=attempts)

    @classmethod
    def _validate_queue_on_connection(
        cls,
        connection: duckdb.DuckDBPyConnection,
    ) -> None:
        jobs = tuple(
            cls._repair_job_from_row(row)
            for row in connection.execute(
                f"SELECT {', '.join(_REPAIR_JOB_COLUMNS)} FROM repair_jobs "
                "ORDER BY trade_date, universe_id"
            ).fetchall()
        )
        attempts = tuple(
            cls._repair_attempt_from_row(row)
            for row in connection.execute(
                f"SELECT {', '.join(_REPAIR_ATTEMPT_COLUMNS)} FROM repair_attempts "
                "ORDER BY job_id, attempt_number"
            ).fetchall()
        )
        RepairQueueSnapshot(status="ready", jobs=jobs, attempts=attempts)

    def repair_queue_snapshot(self) -> RepairQueueSnapshot:
        """Read queue evidence without creating or migrating any file or table."""
        unavailable = RepairQueueSnapshot(
            status="unavailable",
            reason_code="CONTROL_STATE_UNAVAILABLE",
        )
        if not self.path.is_file():
            return unavailable
        connection = None
        try:
            connection = duckdb.connect(str(self.path), read_only=True)
            if not self._continuity_schema_is_valid(connection):
                return unavailable
            jobs = tuple(
                self._repair_job_from_row(row)
                for row in connection.execute(
                    f"SELECT {', '.join(_REPAIR_JOB_COLUMNS)} "
                    "FROM repair_jobs ORDER BY trade_date, universe_id"
                ).fetchall()
            )
            attempts = tuple(
                self._repair_attempt_from_row(row)
                for row in connection.execute(
                    f"SELECT {', '.join(_REPAIR_ATTEMPT_COLUMNS)} "
                    "FROM repair_attempts ORDER BY started_at, attempt_id"
                ).fetchall()
            )
            return RepairQueueSnapshot(status="ready", jobs=jobs, attempts=attempts)
        except (duckdb.Error, OSError, TypeError, ValueError, ValidationError):
            return unavailable
        finally:
            if connection is not None:
                connection.close()

    def enqueue_repair_jobs(
        self,
        missing_dates,
        *,
        universe_id: str,
        now: datetime,
    ) -> list[RepairJob]:
        """Insert new deterministic jobs while the caller owns market-refresh.lock."""
        timestamp = require_utc(now)
        universe_id = require_universe_id(universe_id)
        dates: list[date] = []
        for item in missing_dates:
            if not isinstance(item, date) or isinstance(item, datetime):
                raise RepairQueueError("repair queue trade date is invalid")
            dates.append(item)
        dates = sorted(set(dates))
        connection = self._connect_continuity_writer()
        inserted: list[RepairJob] = []
        try:
            connection.begin()
            self._require_continuity_schema(connection)
            self._validate_queue_on_connection(connection)
            for trade_date in dates:
                job_id = repair_job_id(trade_date, universe_id)
                existing = connection.execute(
                    "SELECT 1 FROM repair_jobs WHERE job_id = ?",
                    [job_id],
                ).fetchone()
                if existing is not None:
                    continue
                connection.execute(
                    """
                    INSERT INTO repair_jobs (
                        job_id, trade_date, universe_id, state, state_version,
                        attempt_count, abandoned_attempt_count, created_at, updated_at
                    ) VALUES (?, ?, ?, 'pending', 1, 0, 0, ?, ?)
                    """,
                    [job_id, trade_date, universe_id, timestamp, timestamp],
                )
                inserted.append(
                    RepairJob(
                        job_id=job_id,
                        trade_date=trade_date,
                        universe_id=universe_id,
                        state="pending",
                        state_version=1,
                        attempt_count=0,
                        abandoned_attempt_count=0,
                        created_at=timestamp,
                        updated_at=timestamp,
                    )
                )
            connection.commit()
            return inserted
        except RepairQueueError:
            connection.rollback()
            raise
        except (duckdb.Error, OSError, TypeError, ValueError, ValidationError):
            connection.rollback()
            raise RepairQueueError("repair queue database operation failed") from None
        finally:
            connection.close()

    def claim_repair_job(
        self,
        job_id: str,
        *,
        owner: str,
        expected_version: int,
        now: datetime,
        lease_seconds: int,
    ) -> RepairLease | None:
        """CAS one eligible job while the caller owns market-refresh.lock."""
        job_id = require_safe_identifier(job_id)
        owner = require_safe_identifier(owner)
        timestamp = require_utc(now)
        if (
            not isinstance(expected_version, int)
            or isinstance(expected_version, bool)
            or expected_version < 1
            or not isinstance(lease_seconds, int)
            or isinstance(lease_seconds, bool)
            or not 60 <= lease_seconds <= 86400
        ):
            raise RepairQueueError("repair queue claim policy is invalid")
        lease_id = f"lease-{uuid4().hex}"
        attempt_id = f"attempt-{uuid4().hex}"
        expires_at = timestamp + timedelta(seconds=lease_seconds)
        connection = self._connect_continuity_writer()
        try:
            connection.begin()
            self._require_continuity_schema(connection)
            self._validate_queue_on_connection(connection)
            row = connection.execute(
                f"SELECT {', '.join(_REPAIR_JOB_COLUMNS)} FROM repair_jobs WHERE job_id = ?",
                [job_id],
            ).fetchone()
            if row is None:
                connection.rollback()
                return None
            job = self._repair_job_from_row(row)
            if timestamp < job.created_at or timestamp < job.updated_at:
                raise RepairQueueError("repair queue claim time is invalid")
            eligible = (
                job.state in {"pending", "retry_wait"}
                and job.state_version == expected_version
                and (job.next_attempt_at is None or job.next_attempt_at <= timestamp)
            )
            if not eligible:
                connection.rollback()
                return None
            attempt_number = job.attempt_count + 1
            updated = connection.execute(
                f"""
                UPDATE repair_jobs
                SET state = 'leased', state_version = state_version + 1,
                    attempt_count = ?, next_attempt_at = NULL,
                    lease_id = ?, lease_owner = ?, lease_expires_at = ?,
                    last_attempt_id = ?, updated_at = ?
                WHERE job_id = ?
                  AND state_version = ?
                  AND state IN ('pending', 'retry_wait')
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                RETURNING {", ".join(_REPAIR_JOB_COLUMNS)}
                """,
                [
                    attempt_number,
                    lease_id,
                    owner,
                    expires_at,
                    attempt_id,
                    timestamp,
                    job_id,
                    expected_version,
                    timestamp,
                ],
            ).fetchone()
            if updated is None:
                connection.rollback()
                return None
            connection.execute(
                """
                INSERT INTO repair_attempts (
                    attempt_id, job_id, attempt_number, lease_id, lease_owner,
                    outcome, started_at
                ) VALUES (?, ?, ?, ?, ?, 'running', ?)
                """,
                [attempt_id, job_id, attempt_number, lease_id, owner, timestamp],
            )
            claimed_job = self._repair_job_from_row(updated)
            result = RepairLease(
                job_id=job_id,
                attempt_id=attempt_id,
                lease_id=lease_id,
                owner=owner,
                target_session=job.trade_date,
                state_version=claimed_job.state_version,
                expires_at=expires_at,
            )
            self._validate_job_attempts_on_connection(connection, claimed_job)
            connection.commit()
            return result
        except RepairQueueError:
            connection.rollback()
            raise
        except (duckdb.Error, OSError, TypeError, ValueError, ValidationError):
            connection.rollback()
            raise RepairQueueError("repair queue database operation failed") from None
        finally:
            connection.close()

    def finalize_repair_attempt(
        self,
        lease: RepairLease,
        *,
        outcome: RepairFinalizationOutcome,
        refresh_result: RefreshResult,
        now: datetime,
        retry_policy: RepairRetryPolicy,
    ) -> RepairJob:
        """CAS one terminal attempt while the caller owns market-refresh.lock."""
        lease = self._validated_lease(lease)
        refresh_result = self._validated_refresh_result(refresh_result)
        retry_policy = self._validated_retry_policy(retry_policy)
        if outcome not in {"succeeded", "failed"}:
            raise RepairQueueError("repair queue finalization outcome is invalid")
        timestamp = require_utc(now)
        if (
            refresh_result.run_kind != "repair"
            or refresh_result.requested_date != lease.target_session
        ):
            raise RepairQueueError("repair refresh result does not match lease")
        refresh_run_id = require_safe_identifier(refresh_result.run_id)
        if outcome == "succeeded":
            if refresh_result.status != "ready":
                raise RepairQueueError("successful repair requires ready refresh evidence")
            next_state = "published"
            next_attempt_at = None
            published_at = timestamp
            attempt_outcome = "succeeded"
            failure_stage = None
            failure_class = None
            retryable = False
        else:
            if (
                refresh_result.status == "ready"
                or refresh_result.failure_stage is None
                or refresh_result.failure_class is None
                or refresh_result.retryable is None
            ):
                raise RepairQueueError("failed repair requires sanitized failure evidence")
            retryable = refresh_result.retryable
            failure_stage = refresh_result.failure_stage
            failure_class = refresh_result.failure_class
            attempt_outcome = "failed"
            published_at = None
            next_state = "retry_wait" if retryable else "dead_letter"
            next_attempt_at = None
        connection = self._connect_continuity_writer()
        try:
            connection.begin()
            self._require_continuity_schema(connection)
            self._validate_queue_on_connection(connection)
            row = connection.execute(
                f"SELECT {', '.join(_REPAIR_JOB_COLUMNS)} FROM repair_jobs "
                "WHERE job_id = ? AND state = 'leased' AND state_version = ? "
                "AND lease_id = ? AND lease_owner = ? AND last_attempt_id = ? "
                "AND lease_expires_at = ? AND lease_expires_at > ?",
                [
                    lease.job_id,
                    lease.state_version,
                    lease.lease_id,
                    lease.owner,
                    lease.attempt_id,
                    lease.expires_at,
                    timestamp,
                ],
            ).fetchone()
            if row is None:
                raise RepairQueueConflictError("repair queue lease no longer matches")
            job = self._repair_job_from_row(row)
            attempt_row = connection.execute(
                f"SELECT {', '.join(_REPAIR_ATTEMPT_COLUMNS)} FROM repair_attempts "
                "WHERE attempt_id = ? AND job_id = ? AND lease_id = ? "
                "AND lease_owner = ? AND outcome = 'running'",
                [lease.attempt_id, lease.job_id, lease.lease_id, lease.owner],
            ).fetchone()
            if attempt_row is None:
                raise RepairQueueConflictError("repair queue attempt no longer matches")
            attempt = self._repair_attempt_from_row(attempt_row)
            if (
                job.trade_date != lease.target_session
                or refresh_result.requested_date != job.trade_date
            ):
                raise RepairQueueError("repair target does not match database job")
            if (
                timestamp < job.created_at
                or timestamp < job.updated_at
                or timestamp < attempt.started_at
            ):
                raise RepairQueueError("repair queue finalization time is invalid")
            if refresh_result.request_key != repair_request_key(job.trade_date):
                raise RepairQueueError("repair publication request key does not match job")
            if (
                refresh_result.started_at < job.created_at
                or refresh_result.started_at < job.updated_at
                or refresh_result.started_at < attempt.started_at
                or refresh_result.completed_at > timestamp
            ):
                raise RepairQueueError("repair refresh evidence time is invalid")
            if outcome == "failed":
                if retryable and job.attempt_count < retry_policy.max_attempts:
                    next_attempt_at = timestamp + retry_policy.delay_after(job.attempt_count)
                else:
                    next_state = "dead_letter"
            attempt_updated = connection.execute(
                f"""
                UPDATE repair_attempts
                SET outcome = ?, refresh_run_id = ?, failure_stage = ?,
                    failure_class = ?, retryable = ?, completed_at = ?
                WHERE attempt_id = ? AND job_id = ? AND lease_id = ?
                  AND lease_owner = ? AND outcome = 'running'
                RETURNING {", ".join(_REPAIR_ATTEMPT_COLUMNS)}
                """,
                [
                    attempt_outcome,
                    refresh_run_id,
                    failure_stage,
                    failure_class,
                    retryable,
                    timestamp,
                    lease.attempt_id,
                    lease.job_id,
                    lease.lease_id,
                    lease.owner,
                ],
            ).fetchone()
            if attempt_updated is None:
                raise RepairQueueConflictError("repair queue attempt no longer matches")
            terminal_attempt = self._repair_attempt_from_row(attempt_updated)
            updated_row = connection.execute(
                f"""
                UPDATE repair_jobs
                SET state = ?, state_version = state_version + 1,
                    next_attempt_at = ?, lease_id = NULL, lease_owner = NULL,
                    lease_expires_at = NULL, last_failure_stage = ?,
                    last_failure_class = ?, updated_at = ?, published_at = ?
                WHERE job_id = ? AND state = 'leased' AND state_version = ?
                  AND lease_id = ? AND lease_owner = ? AND last_attempt_id = ?
                  AND lease_expires_at = ? AND lease_expires_at > ?
                RETURNING {", ".join(_REPAIR_JOB_COLUMNS)}
                """,
                [
                    next_state,
                    next_attempt_at,
                    failure_stage,
                    failure_class,
                    timestamp,
                    published_at,
                    lease.job_id,
                    lease.state_version,
                    lease.lease_id,
                    lease.owner,
                    lease.attempt_id,
                    lease.expires_at,
                    timestamp,
                ],
            ).fetchone()
            if updated_row is None:
                raise RepairQueueConflictError("repair queue lease no longer matches")
            result = self._repair_job_from_row(updated_row)
            self._validate_job_attempts_on_connection(connection, result)
            if terminal_attempt.attempt_id != result.last_attempt_id:
                raise RepairQueueError("repair queue terminal attempt is inconsistent")
            connection.commit()
            return result
        except (RepairQueueConflictError, RepairQueueError):
            connection.rollback()
            raise
        except (duckdb.Error, OSError, TypeError, ValueError, ValidationError):
            connection.rollback()
            raise RepairQueueError("repair queue database operation failed") from None
        finally:
            connection.close()

    def reap_expired_repair_leases(
        self,
        *,
        now: datetime,
        retry_policy: RepairRetryPolicy,
    ) -> list[RepairJob]:
        """Audit expired leases while the caller owns market-refresh.lock."""
        timestamp = require_utc(now)
        retry_policy = self._validated_retry_policy(retry_policy)
        connection = self._connect_continuity_writer()
        recovered: list[RepairJob] = []
        try:
            connection.begin()
            self._require_continuity_schema(connection)
            self._validate_queue_on_connection(connection)
            rows = connection.execute(
                f"SELECT {', '.join(_REPAIR_JOB_COLUMNS)} FROM repair_jobs "
                "WHERE state = 'leased' AND lease_expires_at <= ? ORDER BY trade_date",
                [timestamp],
            ).fetchall()
            for row in rows:
                job = self._repair_job_from_row(row)
                if job.lease_id is None or job.lease_owner is None or job.last_attempt_id is None:
                    raise RepairQueueError("expired repair lease is invalid")
                attempt_row = connection.execute(
                    f"SELECT {', '.join(_REPAIR_ATTEMPT_COLUMNS)} FROM repair_attempts "
                    "WHERE attempt_id = ? AND job_id = ? AND lease_id = ? "
                    "AND lease_owner = ? AND outcome = 'running'",
                    [job.last_attempt_id, job.job_id, job.lease_id, job.lease_owner],
                ).fetchone()
                if attempt_row is None:
                    raise RepairQueueConflictError("expired repair attempt no longer matches")
                attempt = self._repair_attempt_from_row(attempt_row)
                if (
                    timestamp < job.created_at
                    or timestamp < job.updated_at
                    or timestamp < attempt.started_at
                ):
                    raise RepairQueueError("expired repair lease time is invalid")
                next_state = (
                    "retry_wait" if job.attempt_count < retry_policy.max_attempts else "dead_letter"
                )
                next_attempt_at = (
                    timestamp + retry_policy.delay_after(job.attempt_count)
                    if next_state == "retry_wait"
                    else None
                )
                attempt_updated = connection.execute(
                    f"""
                    UPDATE repair_attempts
                    SET outcome = 'abandoned', retryable = TRUE, completed_at = ?
                    WHERE attempt_id = ? AND job_id = ? AND lease_id = ?
                      AND lease_owner = ? AND outcome = 'running'
                    RETURNING {", ".join(_REPAIR_ATTEMPT_COLUMNS)}
                    """,
                    [
                        timestamp,
                        job.last_attempt_id,
                        job.job_id,
                        job.lease_id,
                        job.lease_owner,
                    ],
                ).fetchone()
                if attempt_updated is None:
                    raise RepairQueueConflictError("expired repair attempt no longer matches")
                terminal_attempt = self._repair_attempt_from_row(attempt_updated)
                updated_row = connection.execute(
                    f"""
                    UPDATE repair_jobs
                    SET state = ?, state_version = state_version + 1,
                        abandoned_attempt_count = abandoned_attempt_count + 1,
                        next_attempt_at = ?, lease_id = NULL, lease_owner = NULL,
                        lease_expires_at = NULL, updated_at = ?
                    WHERE job_id = ? AND state = 'leased' AND state_version = ?
                      AND lease_id = ? AND lease_owner = ? AND last_attempt_id = ?
                      AND lease_expires_at <= ?
                    RETURNING {", ".join(_REPAIR_JOB_COLUMNS)}
                    """,
                    [
                        next_state,
                        next_attempt_at,
                        timestamp,
                        job.job_id,
                        job.state_version,
                        job.lease_id,
                        job.lease_owner,
                        job.last_attempt_id,
                        timestamp,
                    ],
                ).fetchone()
                if updated_row is None:
                    raise RepairQueueConflictError("expired repair lease no longer matches")
                recovered_job = self._repair_job_from_row(updated_row)
                self._validate_job_attempts_on_connection(connection, recovered_job)
                if terminal_attempt.attempt_id != recovered_job.last_attempt_id:
                    raise RepairQueueError("expired repair attempt is inconsistent")
                recovered.append(recovered_job)
            connection.commit()
            return recovered
        except (RepairQueueConflictError, RepairQueueError):
            connection.rollback()
            raise
        except (duckdb.Error, OSError, TypeError, ValueError, ValidationError):
            connection.rollback()
            raise RepairQueueError("repair queue database operation failed") from None
        finally:
            connection.close()

    def reconcile_published_repair_jobs(
        self,
        verified_dates,
        *,
        now: datetime,
    ) -> list[RepairJob]:
        """Promote only caller-verified dates while market-refresh.lock is held."""
        timestamp = require_utc(now)
        dates: list[date] = []
        for item in verified_dates:
            if not isinstance(item, date) or isinstance(item, datetime):
                raise RepairQueueError("repair queue trade date is invalid")
            dates.append(item)
        dates = sorted(set(dates))
        if not dates:
            return []
        connection = self._connect_continuity_writer()
        reconciled: list[RepairJob] = []
        try:
            connection.begin()
            self._require_continuity_schema(connection)
            self._validate_queue_on_connection(connection)
            rows = connection.execute(
                f"SELECT {', '.join(_REPAIR_JOB_COLUMNS)} FROM repair_jobs "
                "WHERE trade_date = ANY(?) AND state <> 'published' "
                "ORDER BY trade_date, universe_id",
                [dates],
            ).fetchall()
            for row in rows:
                job = self._repair_job_from_row(row)
                if timestamp < job.created_at or timestamp < job.updated_at:
                    raise RepairQueueError("repair reconciliation time is invalid")
                terminal_attempt: RepairAttempt | None = None
                if job.state == "leased":
                    if (
                        job.lease_id is None
                        or job.lease_owner is None
                        or job.last_attempt_id is None
                    ):
                        raise RepairQueueError("leased repair job is invalid")
                    attempt_row = connection.execute(
                        f"SELECT {', '.join(_REPAIR_ATTEMPT_COLUMNS)} FROM repair_attempts "
                        "WHERE attempt_id = ? AND job_id = ? AND lease_id = ? "
                        "AND lease_owner = ? AND outcome = 'running'",
                        [job.last_attempt_id, job.job_id, job.lease_id, job.lease_owner],
                    ).fetchone()
                    if attempt_row is None:
                        raise RepairQueueConflictError("repair queue attempt no longer matches")
                    attempt = self._repair_attempt_from_row(attempt_row)
                    if timestamp < attempt.started_at:
                        raise RepairQueueError("repair reconciliation time is invalid")
                    attempt_updated = connection.execute(
                        f"""
                        UPDATE repair_attempts
                        SET outcome = 'abandoned', failure_stage = NULL,
                            failure_class = NULL, retryable = TRUE, completed_at = ?
                        WHERE attempt_id = ? AND job_id = ? AND lease_id = ?
                          AND lease_owner = ? AND outcome = 'running'
                        RETURNING {", ".join(_REPAIR_ATTEMPT_COLUMNS)}
                        """,
                        [
                            timestamp,
                            job.last_attempt_id,
                            job.job_id,
                            job.lease_id,
                            job.lease_owner,
                        ],
                    ).fetchone()
                    if attempt_updated is None:
                        raise RepairQueueConflictError("repair queue attempt no longer matches")
                    terminal_attempt = self._repair_attempt_from_row(attempt_updated)
                updated_row = connection.execute(
                    f"""
                    UPDATE repair_jobs
                    SET state = 'published', state_version = state_version + 1,
                        abandoned_attempt_count = abandoned_attempt_count + ?,
                        next_attempt_at = NULL, lease_id = NULL, lease_owner = NULL,
                        lease_expires_at = NULL, last_failure_stage = NULL,
                        last_failure_class = NULL, updated_at = ?, published_at = ?
                    WHERE job_id = ? AND state_version = ? AND state = ?
                      AND lease_id IS NOT DISTINCT FROM ?
                      AND lease_owner IS NOT DISTINCT FROM ?
                      AND last_attempt_id IS NOT DISTINCT FROM ?
                    RETURNING {", ".join(_REPAIR_JOB_COLUMNS)}
                    """,
                    [
                        1 if job.state == "leased" else 0,
                        timestamp,
                        timestamp,
                        job.job_id,
                        job.state_version,
                        job.state,
                        job.lease_id,
                        job.lease_owner,
                        job.last_attempt_id,
                    ],
                ).fetchone()
                if updated_row is None:
                    raise RepairQueueConflictError("repair queue state no longer matches")
                reconciled_job = self._repair_job_from_row(updated_row)
                self._validate_job_attempts_on_connection(connection, reconciled_job)
                if (
                    terminal_attempt is not None
                    and terminal_attempt.attempt_id != reconciled_job.last_attempt_id
                ):
                    raise RepairQueueError("repair reconciliation attempt is inconsistent")
                reconciled.append(reconciled_job)
            connection.commit()
            return reconciled
        except (RepairQueueConflictError, RepairQueueError):
            connection.rollback()
            raise
        except (duckdb.Error, OSError, TypeError, ValueError, ValidationError):
            connection.rollback()
            raise RepairQueueError("repair queue database operation failed") from None
        finally:
            connection.close()

    def _connect_reader(self) -> duckdb.DuckDBPyConnection | None:
        if not self.path.is_file():
            return None
        try:
            return duckdb.connect(str(self.path), read_only=self.read_only)
        except duckdb.ConnectionException as exc:
            if not self.read_only or str(exc).strip() != _DUCKDB_SAME_DATABASE_CONFIGURATION_ERROR:
                raise
            # DuckDB forbids mixed access-mode handles to one database in the
            # same process. Reopen with the writer's configuration so SELECT-only
            # readers retain MVCC access to the last committed snapshot.
            return duckdb.connect(str(self.path))

    @contextmanager
    def _reader(self):
        connection = None
        try:
            connection = self._connect_reader()
            yield connection
        except MarketStoreReadError:
            raise
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise MarketStoreReadError("market control database cannot be read") from exc
        finally:
            if connection is not None:
                connection.close()

    def exists(self) -> bool:
        return self.path.is_file()

    def save_refresh(
        self,
        bars: list[DailyBar],
        result: RefreshResult,
        *,
        publish: bool | None = None,
        publication_lineage: dict[str, object] | None = None,
        selection: object | None = None,
    ) -> None:
        publish = result.status == "ready" if publish is None else publish
        if publish:
            self._validate_ready_publication(result)
            self._validate_local_publication_bars(bars, result)
            if publication_lineage is not None:
                from backend.app.market.candidates import PublishedSelection

                if not isinstance(selection, PublishedSelection):
                    raise ValueError("canonical publication requires a verified selection")
                selection.verify()
                selected = selection.selection
                if (
                    selected.evidence_sha256 != publication_lineage.get("evidence_sha256")
                    or selected.candidate_manifest_sha256
                    != publication_lineage.get("candidate_manifest_sha256")
                    or selected.gate_report_sha256 != publication_lineage.get("gate_report_sha256")
                    or selected.trade_date != result.requested_date
                    or selected.selected_candidate_id != publication_lineage.get("candidate_id")
                    or selected.selected_provider_id.value != publication_lineage.get("provider_id")
                    or selected.universe_id != publication_lineage.get("universe_id")
                ):
                    raise ValueError("canonical publication lineage does not match selection")
        self._save_refresh(bars, result, publish=publish, capture_published_bars=publish)

    def save_external_publication(self, result: RefreshResult) -> None:
        """Persist a trusted external-dataset pointer without local bar materialization."""
        self._validate_ready_publication(result)
        self._save_refresh([], result, publish=True, capture_published_bars=False)

    @staticmethod
    def _validate_ready_publication(result: RefreshResult) -> None:
        try:
            validated = RefreshResult.model_validate(result.model_dump())
        except ValueError as exc:
            raise ValueError("ready publication must be complete") from exc
        if validated.status != "ready":
            raise ValueError("ready publication must be complete")

    @staticmethod
    def publication_bar_quality_issue(bar: DailyBar) -> str | None:
        """Return why a canonical bar cannot belong to a complete publication."""
        if bar.is_trading:
            if bar.is_suspended:
                return "active_bar_marked_suspended"
            if bar.quality_status != "ready" or bar.quality_issues:
                return "active_bar_not_ready"
            return None
        if not bar.is_suspended:
            return "non_trading_bar_not_suspended"
        if (
            bar.security_type != "stock"
            or bar.volume != 0
            or bar.amount != 0
            or bar.quality_status != "partial"
            or bar.quality_issues != ["suspended_placeholder"]
        ):
            return "invalid_suspended_placeholder"
        return None

    @staticmethod
    def _validate_local_publication_bars(
        bars: list[DailyBar],
        result: RefreshResult,
    ) -> None:
        if not bars:
            raise ValueError("ready local publication requires non-empty bars")
        partitions = {(bar.trade_date, bar.source) for bar in bars}
        expected_partition = {(result.requested_date, result.source)}
        if partitions != expected_partition:
            raise ValueError("ready local publication requires one explicit trade-date partition")
        identities = [(bar.trade_date, bar.symbol, bar.source) for bar in bars]
        if len(set(identities)) != len(identities):
            raise ValueError("ready local publication contains duplicate symbols")
        symbols = {bar.symbol for bar in bars}
        if len(symbols) != result.succeeded_count:
            raise ValueError("ready local publication count does not match refresh audit")
        invalid = [
            (bar.symbol, issue)
            for bar in bars
            if (issue := MarketStore.publication_bar_quality_issue(bar)) is not None
        ]
        if invalid:
            symbol, issue = invalid[0]
            raise ValueError(f"invalid ready publication bar {symbol}: {issue}")

    def _save_refresh(
        self,
        bars: list[DailyBar],
        result: RefreshResult,
        *,
        publish: bool,
        capture_published_bars: bool,
    ) -> None:
        connection = self._connect()
        try:
            connection.begin()
            self._upsert_bars(connection, bars)
            if capture_published_bars:
                self._replace_published_bars(
                    connection,
                    bars,
                    publication_run_id=result.run_id,
                )
            connection.execute("DELETE FROM refresh_runs WHERE run_id = ?", [result.run_id])
            connection.execute(
                """
                INSERT INTO refresh_runs (
                    run_id, request_key, run_kind, requested_date, source, status, requested_count,
                    succeeded_count, coverage_ratio, failed_symbols, quality_issues,
                    error_message, failure_stage, failure_class, retryable,
                    started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    result.run_id,
                    result.request_key,
                    result.run_kind,
                    result.requested_date,
                    result.source,
                    result.status,
                    result.requested_count,
                    result.succeeded_count,
                    result.coverage_ratio,
                    json.dumps(result.failed_symbols, ensure_ascii=False),
                    json.dumps(result.quality_issues, ensure_ascii=False),
                    result.error_message,
                    result.failure_stage,
                    result.failure_class,
                    result.retryable,
                    result.started_at,
                    result.completed_at,
                ],
            )
            if publish:
                current = connection.execute(
                    """
                    SELECT trade_date FROM published_snapshots
                    WHERE singleton = 1
                    """
                ).fetchone()
                if current is None or current[0] <= result.requested_date:
                    connection.execute("DELETE FROM published_snapshots WHERE singleton = 1")
                    connection.execute(
                        """
                        INSERT INTO published_snapshots (
                            singleton, run_id, trade_date, published_at
                        ) VALUES (1, ?, ?, ?)
                        """,
                        [result.run_id, result.requested_date, result.completed_at],
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _upsert_bars(
        connection: duckdb.DuckDBPyConnection,
        bars: list[DailyBar],
    ) -> None:
        if not bars:
            return
        deduplicated = {(bar.trade_date, bar.symbol, bar.source): bar for bar in bars}
        rows = [
            [
                bar.trade_date,
                bar.symbol,
                bar.security_type,
                bar.exchange,
                bar.board,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.preclose,
                bar.volume,
                bar.amount,
                bar.turnover_rate,
                bar.pct_change,
                bar.adjust_factor,
                bar.price_adjustment,
                bar.is_trading,
                bar.is_suspended,
                bar.is_st,
                bar.source,
                bar.source_record_id,
                bar.ingested_at,
                bar.quality_status,
                json.dumps(bar.quality_issues, ensure_ascii=False),
            ]
            for bar in deduplicated.values()
        ]
        columns = [list(column) for column in zip(*rows, strict=True)]
        connection.execute(
            """
            INSERT OR REPLACE INTO daily_bars
            SELECT
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?)
            """,
            columns,
        )

    @staticmethod
    def _replace_published_bars(
        connection: duckdb.DuckDBPyConnection,
        bars: list[DailyBar],
        *,
        publication_run_id: str,
    ) -> None:
        deduplicated = {(bar.trade_date, bar.symbol, bar.source): bar for bar in bars}
        touched_partitions = sorted({(bar.trade_date, bar.source) for bar in deduplicated.values()})
        for trade_date, source in touched_partitions:
            connection.execute(
                """
                DELETE FROM published_daily_bars
                WHERE trade_date = ? AND source = ?
                """,
                [trade_date, source],
            )
        rows = [
            [
                publication_run_id,
                bar.trade_date,
                bar.symbol,
                bar.security_type,
                bar.exchange,
                bar.board,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.preclose,
                bar.volume,
                bar.amount,
                bar.turnover_rate,
                bar.pct_change,
                bar.adjust_factor,
                bar.price_adjustment,
                bar.is_trading,
                bar.is_suspended,
                bar.is_st,
                bar.source,
                bar.source_record_id,
                bar.ingested_at,
                bar.quality_status,
                json.dumps(bar.quality_issues, ensure_ascii=False),
            ]
            for bar in deduplicated.values()
        ]
        columns = [list(column) for column in zip(*rows, strict=True)]
        connection.execute(
            """
            INSERT INTO published_daily_bars
            SELECT
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?)
            """,
            columns,
        )

    def upsert_bars(self, bars: list[DailyBar]) -> None:
        connection = self._connect()
        try:
            connection.begin()
            self._upsert_bars(connection, bars)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def present_points(
        self,
        trading_dates: list,
        symbols: list[str],
        source: str = "baostock",
    ) -> set[tuple]:
        if not trading_dates or not symbols or not self.exists():
            return set()
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol FROM daily_bars
                WHERE trade_date = ANY(?)
                  AND symbol = ANY(?)
                  AND source = ?
                """,
                [list(dict.fromkeys(trading_dates)), sorted(set(symbols)), source],
            ).fetchall()
        return set(rows)

    def count_bars(self) -> int:
        if not self.exists():
            return 0
        with self._reader() as connection:
            assert connection is not None
            return connection.execute("SELECT count(*) FROM daily_bars").fetchone()[0]

    def latest_refresh(self) -> RefreshResult | None:
        if not self.exists():
            return None
        with self._reader() as connection:
            assert connection is not None
            row = connection.execute(
                """
                SELECT run_id, requested_date, source, status, requested_count,
                       succeeded_count, coverage_ratio, failed_symbols, quality_issues,
                       error_message, started_at, completed_at, request_key, run_kind,
                       failure_stage, failure_class, retryable
                FROM refresh_runs
                ORDER BY completed_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._refresh_result_from_row(row)

    @staticmethod
    def _refresh_result_from_row(row) -> RefreshResult:
        return RefreshResult(
            run_id=row[0],
            requested_date=row[1],
            source=row[2],
            status=row[3],
            requested_count=row[4],
            succeeded_count=row[5],
            coverage_ratio=row[6],
            failed_symbols=json.loads(row[7]),
            quality_issues=json.loads(row[8]),
            error_message=row[9],
            started_at=row[10],
            completed_at=row[11],
            request_key=row[12],
            run_kind=row[13],
            # Compatibility for read-only consumers that still project the
            # pre-R2-F0 fourteen-column refresh audit shape.
            failure_stage=row[14] if len(row) > 14 else None,
            failure_class=row[15] if len(row) > 15 else None,
            retryable=row[16] if len(row) > 16 else None,
        )

    def published_refresh(self) -> RefreshResult | None:
        if not self.exists():
            return None
        with self._reader() as connection:
            assert connection is not None
            row = connection.execute(
                """
                SELECT r.run_id, r.requested_date, r.source, r.status,
                       r.requested_count, r.succeeded_count, r.coverage_ratio,
                       r.failed_symbols, r.quality_issues, r.error_message,
                       r.started_at, r.completed_at, r.request_key, r.run_kind,
                       r.failure_stage, r.failure_class, r.retryable
                FROM published_snapshots p
                JOIN refresh_runs r ON r.run_id = p.run_id
                WHERE p.singleton = 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._refresh_result_from_row(row)

    def save_scheduler_state(self, state) -> None:
        connection = self._connect()
        try:
            connection.execute("DELETE FROM market_automation_state WHERE singleton = 1")
            connection.execute(
                """
                INSERT INTO market_automation_state (
                    singleton, target_session, refresh_state, attempt_count,
                    last_attempt_at, last_success_at, next_retry_at,
                    calendar_status, error_code
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    state.target_session,
                    state.refresh_state,
                    state.attempt_count,
                    state.last_attempt_at,
                    state.last_success_at,
                    state.next_retry_at,
                    state.calendar_status,
                    state.error_code,
                ],
            )
        finally:
            connection.close()

    def scheduler_state(self):
        if not self.exists():
            return None
        with self._reader() as connection:
            assert connection is not None
            row = connection.execute(
                """
                SELECT target_session, refresh_state, attempt_count,
                       last_attempt_at, last_success_at, next_retry_at,
                       calendar_status, error_code
                FROM market_automation_state
                WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            return None
        from backend.app.market.automation import SchedulerState

        return SchedulerState(
            target_session=row[0],
            refresh_state=row[1],
            attempt_count=row[2],
            last_attempt_at=row[3],
            last_success_at=row[4],
            next_retry_at=row[5],
            calendar_status=row[6],
            error_code=row[7],
        )

    def list_refreshes(self, limit: int = 100) -> list[RefreshResult]:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT run_id, requested_date, source, status, requested_count,
                       succeeded_count, coverage_ratio, failed_symbols, quality_issues,
                       error_message, started_at, completed_at, request_key, run_kind,
                       failure_stage, failure_class, retryable
                FROM refresh_runs
                ORDER BY completed_at DESC, run_id
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        return [self._refresh_result_from_row(row) for row in rows]

    def bars_for(self, trade_date, source: str) -> list[dict[str, object]]:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            cursor = connection.execute(
                """
                SELECT symbol, security_type, close, pct_change, amount, is_suspended,
                       quality_status, quality_issues
                FROM daily_bars
                WHERE trade_date = ? AND source = ?
                ORDER BY symbol
                """,
                [trade_date, source],
            )
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def available_dates(self, source: str = "baostock") -> list:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            return [
                row[0]
                for row in connection.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM daily_bars
                    WHERE source = ?
                    ORDER BY trade_date
                    """,
                    [source],
                ).fetchall()
            ]

    def canonical_bars(self, trade_date, source: str = "baostock") -> list[DailyBar]:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                       close, preclose, volume, amount, turnover_rate, pct_change,
                       adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                       source, source_record_id, ingested_at, quality_status, quality_issues
                FROM daily_bars
                WHERE trade_date = ? AND source = ?
                ORDER BY symbol
                """,
                [trade_date, source],
            ).fetchall()
        return [self._daily_bar_from_row(row) for row in rows]

    def symbol_bars(self, symbol, start, end, source: str = "baostock") -> list[DailyBar]:
        if start > end or not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                       close, preclose, volume, amount, turnover_rate, pct_change,
                       adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                       source, source_record_id, ingested_at, quality_status, quality_issues
                FROM daily_bars
                WHERE symbol = ?
                  AND trade_date BETWEEN ? AND ?
                  AND source = ?
                ORDER BY trade_date
                """,
                [symbol, start, end, source],
            ).fetchall()
        return [self._daily_bar_from_row(row) for row in rows]

    def symbols_bars(
        self,
        symbols: list[str],
        start,
        end,
        source: str = "baostock",
    ) -> dict[str, list[DailyBar]]:
        normalized = sorted(set(symbols))
        if len(normalized) > MAX_SYMBOL_RANGE_QUERY:
            raise ValueError(
                f"a symbol range query accepts at most {MAX_SYMBOL_RANGE_QUERY} symbols"
            )
        grouped: dict[str, list[DailyBar]] = {symbol: [] for symbol in normalized}
        if not normalized or start > end or not self.exists():
            return grouped
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                       close, preclose, volume, amount, turnover_rate, pct_change,
                       adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                       source, source_record_id, ingested_at, quality_status, quality_issues
                FROM daily_bars
                WHERE symbol = ANY(?)
                  AND trade_date BETWEEN ? AND ?
                  AND source = ?
                ORDER BY symbol, trade_date
                """,
                [normalized, start, end, source],
            ).fetchall()
        for row in rows:
            grouped[row[1]].append(self._daily_bar_from_row(row))
        return grouped

    @staticmethod
    def _daily_bar_from_row(row) -> DailyBar:
        return DailyBar(
            trade_date=row[0],
            symbol=row[1],
            security_type=row[2],
            exchange=row[3],
            board=row[4],
            open=row[5],
            high=row[6],
            low=row[7],
            close=row[8],
            preclose=row[9],
            volume=row[10],
            amount=row[11],
            turnover_rate=row[12],
            pct_change=row[13],
            adjust_factor=row[14],
            price_adjustment=row[15],
            is_trading=row[16],
            is_suspended=row[17],
            is_st=row[18],
            source=row[19],
            source_record_id=row[20],
            ingested_at=row[21],
            quality_status=row[22],
            quality_issues=json.loads(row[23]),
        )

    def export_date(self, trade_date, output_root: Path, source: str = "baostock") -> Path:
        output = output_root / f"date={trade_date.isoformat()}" / "bars.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        escaped_output = str(output).replace("'", "''")
        connection = self._connect()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT * FROM daily_bars
                    WHERE trade_date = ? AND source = ?
                    ORDER BY symbol
                ) TO '{escaped_output}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [trade_date, source],
            )
        finally:
            connection.close()
        return output
