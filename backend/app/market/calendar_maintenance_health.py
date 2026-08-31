import os
import re
import sqlite3
import stat
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from backend.app.market.provider_health import (
    CircuitState,
    EndpointHealth,
    ProviderEndpoint,
    ProviderHealth,
    ProviderHealthError,
    ProviderOutcomeConflictError,
    SQLiteProviderHealthStore,
    _Circuit,
    _close,
    _health,
    _open,
    _Outcome,
    _provider_health,
    _require_aware,
    _require_market_endpoint,
    _require_policy,
    _require_safe_id,
    _validate_outcome,
)
from backend.app.market.provider_transport import NormalizedTransportError

_MAX_SCHEMA_SQL = 8192
_MAX_FIELD_TEXT = 128
_SAFE_FILE_MODE = stat.S_IWGRP | stat.S_IWOTH
_IDENTITY = tuple[int, int]
_CONTENT_SIGNATURE = tuple[int, int, int]


def _normalized_sql(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).lower()


_EXPECTED_SCHEMA: dict[tuple[str, str, str], str | None] = {
    (
        "table",
        "endpoint_circuits",
        "endpoint_circuits",
    ): """CREATE TABLE endpoint_circuits (
        endpoint TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        consecutive_failures INTEGER NOT NULL,
        opened_at TEXT,
        cooldown_until TEXT,
        probe_lease_id TEXT UNIQUE,
        probe_owner TEXT,
        probe_expires_at TEXT
    )""",
    (
        "table",
        "endpoint_outcomes",
        "endpoint_outcomes",
    ): """CREATE TABLE endpoint_outcomes (
        refresh_id TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        success INTEGER NOT NULL,
        normalized_error TEXT,
        observed_at TEXT NOT NULL,
        PRIMARY KEY (refresh_id, endpoint)
    )""",
    (
        "table",
        "provider_health_meta",
        "provider_health_meta",
    ): """CREATE TABLE provider_health_meta (
        schema_version INTEGER NOT NULL
    )""",
    (
        "table",
        "sqlite_sequence",
        "sqlite_sequence",
    ): "CREATE TABLE sqlite_sequence(name,seq)",
    (
        "table",
        "transport_observations",
        "transport_observations",
    ): """CREATE TABLE transport_observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        refresh_id TEXT NOT NULL,
        provider_session_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        provider_id TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        page INTEGER NOT NULL,
        protocol_stage TEXT NOT NULL,
        elapsed_ms INTEGER NOT NULL,
        recv_calls INTEGER NOT NULL,
        response_bytes INTEGER NOT NULL,
        end_marker_seen INTEGER NOT NULL,
        provider_code TEXT,
        normalized_error TEXT,
        outcome TEXT NOT NULL,
        observed_at TEXT NOT NULL
    )""",
    ("index", "sqlite_autoindex_endpoint_circuits_1", "endpoint_circuits"): None,
    ("index", "sqlite_autoindex_endpoint_circuits_2", "endpoint_circuits"): None,
    ("index", "sqlite_autoindex_endpoint_outcomes_1", "endpoint_outcomes"): None,
}


def _field_case(column: str, max_length: int = _MAX_FIELD_TEXT) -> str:
    return (
        f"CASE WHEN typeof({column}) = 'text' AND length({column}) <= {max_length} "
        f"THEN {column} ELSE NULL END"
    )


def _content_signature(value: os.stat_result) -> _CONTENT_SIGNATURE:
    return (value.st_size, value.st_mtime_ns, value.st_ctime_ns)


class CalendarMaintenanceHealthStore(SQLiteProviderHealthStore):
    """Existing-only, read-safe health adapter for calendar maintenance."""

    def __init__(
        self,
        path: Path,
        *,
        failure_threshold: int = 3,
        cooldown_seconds: float = 900,
        probe_lease_seconds: float = 120,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        _require_policy(failure_threshold, cooldown_seconds, probe_lease_seconds)
        self.path = Path(os.path.abspath(os.fspath(path)))
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = float(cooldown_seconds)
        self.probe_lease_seconds = float(probe_lease_seconds)
        self._clock = clock or (lambda: datetime.now().astimezone())
        self._validated_identity: _IDENTITY | None = None

    def initialize(self) -> None:
        raise ProviderHealthError("calendar maintenance health cannot initialize state")

    def reset(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise ProviderHealthError("calendar maintenance health cannot reset state")

    def acquire_probe(self, endpoint: ProviderEndpoint, *, owner: str) -> None:
        del endpoint, owner
        raise ProviderHealthError("calendar maintenance health cannot acquire probes")

    def resolve_probe(
        self,
        lease_id: str,
        *,
        owner: str,
        success: bool,
        normalized_error: Any = None,
    ) -> None:
        del lease_id, owner, success, normalized_error
        raise ProviderHealthError("calendar maintenance health cannot resolve probes")

    def provider_health(self) -> ProviderHealth:
        return self.provider_health_snapshot()

    def endpoint_health(self, endpoint: ProviderEndpoint) -> EndpointHealth:
        endpoint = _require_market_endpoint(endpoint)
        with self._database() as connection:
            circuits = self._read_circuits(connection)
            try:
                circuit = circuits[endpoint]
            except (KeyError, TypeError):
                raise ProviderHealthError("provider circuit requires a market endpoint") from None
        return _health(endpoint, circuit)

    def provider_health_snapshot(self) -> ProviderHealth:
        with self._database() as connection:
            circuits = self._read_circuits(connection)
        endpoints = tuple(_health(endpoint, circuits[endpoint]) for endpoint in ProviderEndpoint)
        return _provider_health(endpoints)

    def record_endpoint_outcome(
        self,
        refresh_id: str,
        endpoint: ProviderEndpoint,
        *,
        success: bool,
        normalized_error: NormalizedTransportError | None = None,
        observed_at: datetime | None = None,
    ) -> EndpointHealth:
        """Record one terminal outcome without reading unbounded audit text."""
        refresh_id = _require_safe_id(refresh_id)
        endpoint = _require_market_endpoint(endpoint)
        outcome = _validate_outcome(success=success, normalized_error=normalized_error)
        now = _require_aware(observed_at or self._clock())
        with self._database(write=True) as connection:
            existing = connection.execute(
                """
                SELECT
                    typeof(success),
                    CASE WHEN typeof(success) = 'integer' THEN success ELSE NULL END,
                    typeof(normalized_error),
                    length(normalized_error),
                    CASE WHEN typeof(normalized_error) = 'text'
                              AND length(normalized_error) <= ?
                         THEN normalized_error ELSE NULL END
                FROM endpoint_outcomes
                WHERE refresh_id = ? AND endpoint = ?
                """,
                (_MAX_FIELD_TEXT, refresh_id, endpoint.value),
            ).fetchone()
            if existing is not None:
                success_type, existing_success, error_type, error_length, error_value = existing
                if (
                    success_type != "integer"
                    or type(existing_success) is not int
                    or existing_success not in (0, 1)
                    or error_type not in ("text", "null")
                    or (
                        error_length is not None
                        and (not isinstance(error_length, int) or error_length > _MAX_FIELD_TEXT)
                    )
                    or (error_type == "text" and not isinstance(error_value, str))
                    or (error_type == "null" and error_value is not None)
                ):
                    raise ProviderHealthError("provider endpoint outcome is unavailable")
                try:
                    existing_error = (
                        NormalizedTransportError(error_value) if error_value is not None else None
                    )
                except (TypeError, ValueError):
                    raise ProviderHealthError("provider endpoint outcome is unavailable") from None
                existing_outcome = _Outcome(
                    success=bool(existing_success),
                    normalized_error=existing_error,
                )
                if existing_outcome.success and existing_outcome.normalized_error is not None:
                    raise ProviderHealthError("provider endpoint outcome is unavailable")
                if not existing_outcome.success and existing_outcome.normalized_error is None:
                    raise ProviderHealthError("provider endpoint outcome is unavailable")
                if existing_outcome != outcome:
                    raise ProviderOutcomeConflictError(
                        "provider endpoint outcome is already resolved"
                    )
                return self._health_from_connection(connection, endpoint)

            circuit = self._circuit_from_connection(connection, endpoint)
            if circuit.state != CircuitState.CLOSED:
                raise ProviderHealthError("provider circuit requires probe resolution")
            if outcome.success:
                _close(circuit)
            else:
                circuit.consecutive_failures += 1
                if circuit.consecutive_failures >= self.failure_threshold:
                    _open(circuit, now, self.cooldown_seconds)
            self._save_circuit(connection, endpoint, circuit)
            connection.execute(
                """
                INSERT INTO endpoint_outcomes (
                    refresh_id, endpoint, success, normalized_error, observed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    refresh_id,
                    endpoint.value,
                    int(outcome.success),
                    outcome.normalized_error.value if outcome.normalized_error else None,
                    now.isoformat(),
                ],
            )
            return _health(endpoint, circuit)

    def _validate_path(self) -> tuple[os.stat_result, _IDENTITY, _CONTENT_SIGNATURE]:
        try:
            for ancestor in self.path.parents:
                ancestor_stat = os.lstat(ancestor)
                if stat.S_ISLNK(ancestor_stat.st_mode):
                    raise ProviderHealthError("provider health path is unsafe")

            parent_stat = os.lstat(self.path.parent)
            if (
                not stat.S_ISDIR(parent_stat.st_mode)
                or parent_stat.st_uid != os.geteuid()
                or parent_stat.st_mode & _SAFE_FILE_MODE
            ):
                raise ProviderHealthError("provider health path is unsafe")

            path_stat = os.lstat(self.path)
            if (
                stat.S_ISLNK(path_stat.st_mode)
                or not stat.S_ISREG(path_stat.st_mode)
                or path_stat.st_uid != os.geteuid()
                or path_stat.st_nlink != 1
                or path_stat.st_mode & _SAFE_FILE_MODE
            ):
                raise ProviderHealthError("provider health path is unsafe")
            identity = (path_stat.st_dev, path_stat.st_ino)
            if self._validated_identity is not None and identity != self._validated_identity:
                raise ProviderHealthError("provider health path was replaced")

            for suffix in ("-journal", "-wal", "-shm"):
                try:
                    os.lstat(self.path.with_name(self.path.name + suffix))
                except FileNotFoundError:
                    continue
                raise ProviderHealthError("provider health sidecar is present")
            return path_stat, identity, _content_signature(path_stat)
        except ProviderHealthError:
            raise
        except (OSError, TypeError, ValueError):
            raise ProviderHealthError("provider health database operation failed") from None

    def _verify_bound_path(self, identity: _IDENTITY, *, expected_nlink: int = 1) -> None:
        try:
            path_stat = os.lstat(self.path)
            if (
                stat.S_ISLNK(path_stat.st_mode)
                or not stat.S_ISREG(path_stat.st_mode)
                or path_stat.st_uid != os.geteuid()
                or path_stat.st_nlink != expected_nlink
                or path_stat.st_mode & _SAFE_FILE_MODE
                or (path_stat.st_dev, path_stat.st_ino) != identity
            ):
                raise ProviderHealthError("provider health path was replaced")
        except ProviderHealthError:
            raise
        except (OSError, TypeError, ValueError):
            raise ProviderHealthError("provider health database operation failed") from None

    def _verify_read_binding(
        self,
        fd: int,
        identity: _IDENTITY,
        content: _CONTENT_SIGNATURE,
    ) -> None:
        try:
            path_stat, path_identity, path_content = self._validate_path()
            descriptor_stat = os.fstat(fd)
        except ProviderHealthError:
            raise
        except OSError:
            raise ProviderHealthError("provider health database operation failed") from None
        if (
            path_identity != identity
            or path_content != content
            or (descriptor_stat.st_dev, descriptor_stat.st_ino) != identity
            or _content_signature(descriptor_stat) != content
            or path_stat.st_nlink != 1
        ):
            raise ProviderHealthError("provider health path changed during read")

    def _create_write_alias(self, identity: _IDENTITY) -> Path:
        for _ in range(4):
            alias = self.path.with_name(f".{self.path.name}.maintenance-{uuid4().hex}")
            try:
                os.link(self.path, alias, follow_symlinks=False)
            except FileExistsError:
                continue
            except (OSError, TypeError, ValueError):
                raise ProviderHealthError("provider health database operation failed") from None
            try:
                alias_stat = os.lstat(alias)
                path_stat = os.lstat(self.path)
            except OSError:
                self._cleanup_write_alias(alias, identity)
                raise ProviderHealthError("provider health database operation failed") from None
            if (
                not stat.S_ISREG(alias_stat.st_mode)
                or (alias_stat.st_dev, alias_stat.st_ino) != identity
                or (path_stat.st_dev, path_stat.st_ino) != identity
                or alias_stat.st_nlink != 2
                or path_stat.st_nlink != 2
            ):
                self._cleanup_write_alias(alias, identity)
                raise ProviderHealthError("provider health path was replaced")
            return alias
        raise ProviderHealthError("provider health database operation failed")

    @staticmethod
    def _cleanup_write_alias(alias: Path, identity: _IDENTITY) -> None:
        try:
            alias_stat = os.lstat(alias)
            if (alias_stat.st_dev, alias_stat.st_ino) == identity:
                os.unlink(alias)
        except (FileNotFoundError, OSError):
            pass

    def _bind_fd(
        self,
        fd: int,
        path_stat: os.stat_result,
        identity: _IDENTITY,
        *,
        expected_nlink: int = 1,
    ) -> None:
        try:
            descriptor_stat = os.fstat(fd)
        except OSError:
            raise ProviderHealthError("provider health database operation failed") from None
        if (
            not stat.S_ISREG(descriptor_stat.st_mode)
            or descriptor_stat.st_uid != os.geteuid()
            or descriptor_stat.st_nlink != expected_nlink
            or descriptor_stat.st_mode & _SAFE_FILE_MODE
            or (descriptor_stat.st_dev, descriptor_stat.st_ino) != identity
            or (path_stat.st_dev, path_stat.st_ino) != identity
        ):
            raise ProviderHealthError("provider health path was replaced")
        if self._validated_identity is None:
            self._validated_identity = identity

    def _verify_write_alias(self, alias: Path, identity: _IDENTITY) -> None:
        try:
            alias_stat = os.lstat(alias)
            if (
                not stat.S_ISREG(alias_stat.st_mode)
                or alias_stat.st_uid != os.geteuid()
                or alias_stat.st_nlink != 2
                or alias_stat.st_mode & _SAFE_FILE_MODE
                or (alias_stat.st_dev, alias_stat.st_ino) != identity
            ):
                raise ProviderHealthError("provider health path was replaced")
        except ProviderHealthError:
            raise
        except (OSError, TypeError, ValueError):
            raise ProviderHealthError("provider health database operation failed") from None

    @contextmanager
    def _database(self, *, write: bool = False):
        connection: sqlite3.Connection | None = None
        fd: int | None = None
        alias: Path | None = None
        identity: _IDENTITY | None = None
        try:
            path_stat, identity, content = self._validate_path()
            flags = (os.O_RDWR if write else os.O_RDONLY) | os.O_NOFOLLOW | os.O_NONBLOCK
            fd = os.open(self.path, flags)
            self._bind_fd(fd, path_stat, identity)
            mode = "rw" if write else "ro"
            if write:
                alias = self._create_write_alias(identity)
                database_uri = f"{alias.as_uri()}?mode={mode}"
            else:
                database_uri = f"file:///dev/fd/{fd}?mode={mode}"
            connection = sqlite3.connect(
                database_uri,
                uri=True,
                timeout=0,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            if write:
                self._verify_write_alias(alias, identity)
            else:
                self._verify_read_binding(fd, identity, content)
            connection.execute("PRAGMA busy_timeout = 0")
            self._verify_bound_path(identity, expected_nlink=2 if write else 1)
            self._bind_fd(
                fd,
                os.lstat(self.path),
                identity,
                expected_nlink=2 if write else 1,
            )
            if write:
                connection.execute("BEGIN IMMEDIATE")
            else:
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
            self._prove_existing_health(connection)
            yield connection
            if write:
                self._verify_bound_path(identity, expected_nlink=2)
            else:
                self._verify_read_binding(fd, identity, content)
            if write:
                self._verify_write_alias(alias, identity)
                self._bind_fd(
                    fd,
                    os.lstat(self.path),
                    identity,
                    expected_nlink=2,
                )
            self._prove_existing_health(connection)
            if write:
                connection.execute("COMMIT")
            else:
                connection.execute("ROLLBACK")
            if write:
                self._verify_bound_path(identity, expected_nlink=2)
            else:
                self._verify_read_binding(fd, identity, content)
            if write:
                self._verify_write_alias(alias, identity)
        except ProviderHealthError:
            if connection is not None and connection.in_transaction:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            raise
        except (OSError, sqlite3.Error):
            if connection is not None and connection.in_transaction:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            raise ProviderHealthError("provider health database operation failed") from None
        finally:
            if connection is not None:
                connection.close()
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if alias is not None and identity is not None:
                self._cleanup_write_alias(alias, identity)

    def _prove_existing_health(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            """
            SELECT
                CASE WHEN length(type) <= 32 THEN type ELSE NULL END,
                CASE WHEN length(name) <= 128 THEN name ELSE NULL END,
                CASE WHEN length(tbl_name) <= 128 THEN tbl_name ELSE NULL END,
                length(sql),
                CASE WHEN sql IS NULL THEN NULL ELSE substr(sql, 1, ?) END
            FROM sqlite_master
            LIMIT 9
            """,
            (_MAX_SCHEMA_SQL + 1,),
        ).fetchall()
        if len(rows) != len(_EXPECTED_SCHEMA):
            raise ProviderHealthError("provider health schema is unavailable")
        actual: dict[tuple[str, str, str], str | None] = {}
        for row in rows:
            object_type, name, table_name, sql_length, sql = row
            if not all(isinstance(value, str) for value in (object_type, name, table_name)):
                raise ProviderHealthError("provider health schema is unavailable")
            if not isinstance(sql_length, (int, type(None))) or (
                sql_length is not None and sql_length > _MAX_SCHEMA_SQL
            ):
                raise ProviderHealthError("provider health schema is unavailable")
            if sql is not None and not isinstance(sql, str):
                raise ProviderHealthError("provider health schema is unavailable")
            actual[(object_type, name, table_name)] = (
                _normalized_sql(sql) if sql is not None else None
            )
        expected = {
            key: _normalized_sql(sql) if sql is not None else None
            for key, sql in _EXPECTED_SCHEMA.items()
        }
        if actual != expected:
            raise ProviderHealthError("provider health schema is unavailable")

        metadata = connection.execute(
            """
            SELECT typeof(schema_version),
                   CASE WHEN typeof(schema_version) = 'integer'
                        THEN schema_version ELSE NULL END
            FROM provider_health_meta
            LIMIT 2
            """
        ).fetchall()
        if len(metadata) != 1 or metadata[0][0] != "integer" or metadata[0][1] != 1:
            raise ProviderHealthError("provider health metadata is unavailable")
        self._read_circuits(connection)

    def _read_circuits(self, connection: sqlite3.Connection) -> dict[ProviderEndpoint, _Circuit]:
        rows = connection.execute(
            f"""
            SELECT
                typeof(endpoint), typeof(state), typeof(consecutive_failures),
                typeof(opened_at), typeof(cooldown_until), typeof(probe_lease_id),
                typeof(probe_owner), typeof(probe_expires_at),
                length(endpoint), length(state), length(consecutive_failures),
                length(opened_at), length(cooldown_until), length(probe_lease_id),
                length(probe_owner), length(probe_expires_at),
                {_field_case("endpoint")}, {_field_case("state")},
                CASE WHEN typeof(consecutive_failures) = 'integer'
                     THEN consecutive_failures ELSE NULL END,
                {_field_case("opened_at")}, {_field_case("cooldown_until")},
                {_field_case("probe_lease_id")}, {_field_case("probe_owner")},
                {_field_case("probe_expires_at")}
            FROM endpoint_circuits
            LIMIT 7
            """
        ).fetchall()
        if len(rows) != len(tuple(ProviderEndpoint)):
            raise ProviderHealthError("provider circuit state is unavailable")

        circuits: dict[ProviderEndpoint, _Circuit] = {}
        for row in rows:
            types = tuple(row[:8])
            lengths = tuple(row[8:16])
            (
                endpoint_value,
                state_value,
                failures,
                opened,
                cooldown,
                lease_id,
                owner,
                expires,
            ) = row[16:]
            if types[0] != "text" or types[1] != "text" or types[2] != "integer":
                raise ProviderHealthError("provider circuit state is unavailable")
            if any(value not in ("text", "null") for value in types[3:]):
                raise ProviderHealthError("provider circuit state is unavailable")
            if any(
                value is not None and (not isinstance(value, int) or value > _MAX_FIELD_TEXT)
                for value in lengths
            ):
                raise ProviderHealthError("provider circuit state is unavailable")
            if not isinstance(endpoint_value, str) or not isinstance(state_value, str):
                raise ProviderHealthError("provider circuit state is unavailable")
            if type(failures) is not int or failures < 0:
                raise ProviderHealthError("provider circuit state is unavailable")
            try:
                endpoint = ProviderEndpoint(endpoint_value)
                state = CircuitState(state_value)
            except (TypeError, ValueError):
                raise ProviderHealthError("provider circuit state is unavailable") from None
            if endpoint in circuits:
                raise ProviderHealthError("provider circuit state is unavailable")

            opened_at = self._parse_time(opened)
            cooldown_until = self._parse_time(cooldown)
            probe_expires_at = self._parse_time(expires)
            if state == CircuitState.CLOSED:
                if (
                    opened_at is not None
                    or cooldown_until is not None
                    or lease_id is not None
                    or owner is not None
                    or probe_expires_at is not None
                ):
                    raise ProviderHealthError("provider circuit state is unavailable")
            elif state == CircuitState.OPEN:
                if (
                    failures < 1
                    or opened_at is None
                    or cooldown_until is None
                    or cooldown_until < opened_at
                    or lease_id is not None
                    or owner is not None
                    or probe_expires_at is not None
                ):
                    raise ProviderHealthError("provider circuit state is unavailable")
            else:
                if (
                    failures < 1
                    or opened_at is None
                    or cooldown_until is None
                    or cooldown_until < opened_at
                    or probe_expires_at is None
                ):
                    raise ProviderHealthError("provider circuit state is unavailable")
                try:
                    _require_safe_id(lease_id)
                    _require_safe_id(owner)
                except ProviderHealthError:
                    raise ProviderHealthError("provider circuit state is unavailable") from None

            circuits[endpoint] = _Circuit(
                state=state,
                consecutive_failures=failures,
                opened_at=opened_at,
                cooldown_until=cooldown_until,
                probe_lease_id=lease_id,
                probe_owner=owner,
                probe_expires_at=probe_expires_at,
            )
        if set(circuits) != set(ProviderEndpoint):
            raise ProviderHealthError("provider circuit state is unavailable")
        return circuits

    @staticmethod
    def _parse_time(value: Any) -> datetime | None:
        if value is None:
            return None
        if not isinstance(value, str) or len(value) > _MAX_FIELD_TEXT:
            raise ProviderHealthError("provider circuit state is unavailable")
        try:
            return _require_aware(datetime.fromisoformat(value))
        except (TypeError, ValueError):
            raise ProviderHealthError("provider circuit state is unavailable") from None

    def _circuit_from_connection(
        self,
        connection: sqlite3.Connection,
        endpoint: ProviderEndpoint,
    ) -> _Circuit:
        if not isinstance(endpoint, ProviderEndpoint):
            raise ProviderHealthError("provider circuit requires a market endpoint")
        try:
            return self._read_circuits(connection)[endpoint]
        except KeyError:
            raise ProviderHealthError("provider circuit state is unavailable") from None
