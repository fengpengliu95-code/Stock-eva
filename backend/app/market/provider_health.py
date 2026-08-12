import math
import re
import sqlite3
import threading
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from backend.app.market.provider_transport import (
    ClassificationEndpoint,
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
    TransportOutcome,
)

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_BUSY_TIMEOUT_MILLISECONDS = 5_000


class CircuitState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class ProviderHealthError(RuntimeError):
    pass


class ProviderOutcomeConflictError(ProviderHealthError):
    pass


class EndpointHealth(BaseModel):
    model_config = ConfigDict(frozen=True)

    endpoint: ProviderEndpoint
    state: CircuitState
    consecutive_failures: int = Field(ge=0)
    opened_at: datetime | None = None
    cooldown_until: datetime | None = None
    probe_lease_id: str | None = None
    probe_owner: str | None = None
    probe_expires_at: datetime | None = None


class ProviderHealth(BaseModel):
    model_config = ConfigDict(frozen=True)

    state: CircuitState
    endpoints: tuple[EndpointHealth, ...]
    blocking_endpoints: tuple[ProviderEndpoint, ...]
    probe_endpoints: tuple[ProviderEndpoint, ...]


class ProbeLease(BaseModel):
    model_config = ConfigDict(frozen=True)

    lease_id: str
    endpoint: ProviderEndpoint
    owner: str
    expires_at: datetime


@dataclass(frozen=True)
class _Outcome:
    success: bool
    normalized_error: NormalizedTransportError | None


@dataclass
class _Circuit:
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    opened_at: datetime | None = None
    cooldown_until: datetime | None = None
    probe_lease_id: str | None = None
    probe_owner: str | None = None
    probe_expires_at: datetime | None = None


def _require_policy(
    failure_threshold: int,
    cooldown_seconds: float,
    probe_lease_seconds: float,
) -> None:
    if isinstance(failure_threshold, bool) or failure_threshold < 1:
        raise ProviderHealthError("provider circuit policy is invalid")
    for value in (cooldown_seconds, probe_lease_seconds):
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise ProviderHealthError("provider circuit policy is invalid")


def _require_market_endpoint(endpoint: Any) -> ProviderEndpoint:
    if not isinstance(endpoint, ProviderEndpoint):
        raise ProviderHealthError("provider circuit requires a market endpoint")
    return endpoint


def _require_safe_id(value: Any) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise ProviderHealthError("provider health identifier is invalid")
    return value


def _require_aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ProviderHealthError("provider health timestamp is invalid")
    return value


def _validate_outcome(
    *,
    success: bool,
    normalized_error: Any,
) -> _Outcome:
    if not isinstance(success, bool):
        raise ProviderHealthError("provider endpoint outcome is invalid")
    if success:
        if normalized_error is not None:
            raise ProviderHealthError("provider endpoint outcome is invalid")
        return _Outcome(success=True, normalized_error=None)
    if not isinstance(normalized_error, NormalizedTransportError):
        raise ProviderHealthError("provider endpoint outcome is invalid")
    return _Outcome(success=False, normalized_error=normalized_error)


def _health(endpoint: ProviderEndpoint, circuit: _Circuit) -> EndpointHealth:
    return EndpointHealth(
        endpoint=endpoint,
        state=circuit.state,
        consecutive_failures=circuit.consecutive_failures,
        opened_at=circuit.opened_at,
        cooldown_until=circuit.cooldown_until,
        probe_lease_id=circuit.probe_lease_id,
        probe_owner=circuit.probe_owner,
        probe_expires_at=circuit.probe_expires_at,
    )


def _provider_health(endpoints: tuple[EndpointHealth, ...]) -> ProviderHealth:
    blocking = tuple(item.endpoint for item in endpoints if item.state == CircuitState.OPEN)
    probing = tuple(item.endpoint for item in endpoints if item.state == CircuitState.HALF_OPEN)
    if blocking:
        state = CircuitState.OPEN
    elif probing:
        state = CircuitState.HALF_OPEN
    else:
        state = CircuitState.CLOSED
    return ProviderHealth(
        state=state,
        endpoints=endpoints,
        blocking_endpoints=blocking,
        probe_endpoints=probing,
    )


class _ProviderHealthStore:
    failure_threshold: int
    cooldown_seconds: float
    probe_lease_seconds: float

    def initialize(self) -> None:
        raise NotImplementedError

    def record_observation(self, observation: TransportObservation) -> None:
        raise NotImplementedError

    def list_observations(self) -> list[TransportObservation]:
        raise NotImplementedError

    def record_endpoint_outcome(
        self,
        refresh_id: str,
        endpoint: ProviderEndpoint,
        *,
        success: bool,
        normalized_error: NormalizedTransportError | None = None,
        observed_at: datetime | None = None,
    ) -> EndpointHealth:
        raise NotImplementedError

    def record_terminal_failure(
        self,
        refresh_id: str,
        endpoint: ProviderEndpoint,
        normalized_error: NormalizedTransportError,
        *,
        observed_at: datetime | None = None,
    ) -> EndpointHealth:
        return self.record_endpoint_outcome(
            refresh_id,
            endpoint,
            success=False,
            normalized_error=normalized_error,
            observed_at=observed_at,
        )

    def record_terminal_success(
        self,
        refresh_id: str,
        endpoint: ProviderEndpoint,
        *,
        observed_at: datetime | None = None,
    ) -> EndpointHealth:
        return self.record_endpoint_outcome(
            refresh_id,
            endpoint,
            success=True,
            observed_at=observed_at,
        )

    def endpoint_health(self, endpoint: ProviderEndpoint) -> EndpointHealth:
        raise NotImplementedError

    def provider_health(self) -> ProviderHealth:
        endpoints = tuple(self.endpoint_health(endpoint) for endpoint in ProviderEndpoint)
        return _provider_health(endpoints)

    def provider_health_snapshot(self) -> ProviderHealth:
        """Return persisted state without reclaiming an expired probe lease."""
        raise NotImplementedError

    def acquire_probe(self, endpoint: ProviderEndpoint, *, owner: str) -> ProbeLease | None:
        raise NotImplementedError

    def resolve_probe(
        self,
        lease_id: str,
        *,
        owner: str,
        success: bool,
        normalized_error: NormalizedTransportError | None = None,
    ) -> EndpointHealth:
        raise NotImplementedError


class InMemoryProviderHealthStore(_ProviderHealthStore):
    def __init__(
        self,
        *,
        failure_threshold: int = 3,
        cooldown_seconds: float = 900,
        probe_lease_seconds: float = 120,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        _require_policy(failure_threshold, cooldown_seconds, probe_lease_seconds)
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = float(cooldown_seconds)
        self.probe_lease_seconds = float(probe_lease_seconds)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._observations: list[TransportObservation] = []
        self._outcomes: dict[tuple[str, ProviderEndpoint], _Outcome] = {}
        self._circuits = {endpoint: _Circuit() for endpoint in ProviderEndpoint}
        self._lock = threading.RLock()

    def initialize(self) -> None:
        return None

    def record_observation(self, observation: TransportObservation) -> None:
        if not isinstance(observation, TransportObservation):
            raise ProviderHealthError("provider transport observation is invalid")
        with self._lock:
            self._observations.append(observation)

    def list_observations(self) -> list[TransportObservation]:
        with self._lock:
            return list(self._observations)

    def record_endpoint_outcome(
        self,
        refresh_id: str,
        endpoint: ProviderEndpoint,
        *,
        success: bool,
        normalized_error: NormalizedTransportError | None = None,
        observed_at: datetime | None = None,
    ) -> EndpointHealth:
        refresh_id = _require_safe_id(refresh_id)
        endpoint = _require_market_endpoint(endpoint)
        outcome = _validate_outcome(success=success, normalized_error=normalized_error)
        now = _require_aware(observed_at or self._clock())
        key = (refresh_id, endpoint)
        with self._lock:
            existing = self._outcomes.get(key)
            if existing is not None:
                if existing != outcome:
                    raise ProviderOutcomeConflictError(
                        "provider endpoint outcome is already resolved"
                    )
                return _health(endpoint, self._circuits[endpoint])
            circuit = self._circuits[endpoint]
            if circuit.state != CircuitState.CLOSED:
                raise ProviderHealthError("provider circuit requires probe resolution")
            self._apply_outcome(circuit, outcome, now)
            self._outcomes[key] = outcome
            return _health(endpoint, circuit)

    def endpoint_health(self, endpoint: ProviderEndpoint) -> EndpointHealth:
        endpoint = _require_market_endpoint(endpoint)
        with self._lock:
            self._reap_expired_probes(_require_aware(self._clock()))
            return _health(endpoint, self._circuits[endpoint])

    def provider_health(self) -> ProviderHealth:
        with self._lock:
            self._reap_expired_probes(_require_aware(self._clock()))
            endpoints = tuple(
                _health(endpoint, self._circuits[endpoint]) for endpoint in ProviderEndpoint
            )
            return _provider_health(endpoints)

    def provider_health_snapshot(self) -> ProviderHealth:
        with self._lock:
            endpoints = tuple(
                _health(endpoint, self._circuits[endpoint]) for endpoint in ProviderEndpoint
            )
            return _provider_health(endpoints)

    def acquire_probe(self, endpoint: ProviderEndpoint, *, owner: str) -> ProbeLease | None:
        endpoint = _require_market_endpoint(endpoint)
        owner = _require_safe_id(owner)
        now = _require_aware(self._clock())
        with self._lock:
            self._reap_expired_probes(now)
            if any(circuit.state == CircuitState.HALF_OPEN for circuit in self._circuits.values()):
                return None
            circuit = self._circuits[endpoint]
            eligible = (
                circuit.state == CircuitState.OPEN
                and circuit.cooldown_until is not None
                and circuit.cooldown_until <= now
            )
            if not eligible:
                return None
            lease_id = uuid4().hex
            expires_at = now + timedelta(seconds=self.probe_lease_seconds)
            circuit.state = CircuitState.HALF_OPEN
            circuit.probe_lease_id = lease_id
            circuit.probe_owner = owner
            circuit.probe_expires_at = expires_at
            return ProbeLease(
                lease_id=lease_id,
                endpoint=endpoint,
                owner=owner,
                expires_at=expires_at,
            )

    def _reap_expired_probes(self, now: datetime) -> None:
        for circuit in self._circuits.values():
            if (
                circuit.state == CircuitState.HALF_OPEN
                and circuit.probe_expires_at is not None
                and circuit.probe_expires_at <= now
            ):
                circuit.state = CircuitState.OPEN
                circuit.probe_lease_id = None
                circuit.probe_owner = None
                circuit.probe_expires_at = None

    def resolve_probe(
        self,
        lease_id: str,
        *,
        owner: str,
        success: bool,
        normalized_error: NormalizedTransportError | None = None,
    ) -> EndpointHealth:
        lease_id = _require_safe_id(lease_id)
        owner = _require_safe_id(owner)
        outcome = _validate_outcome(success=success, normalized_error=normalized_error)
        now = _require_aware(self._clock())
        with self._lock:
            match = next(
                (
                    (endpoint, circuit)
                    for endpoint, circuit in self._circuits.items()
                    if circuit.probe_lease_id == lease_id
                ),
                None,
            )
            if (
                match is None
                or match[1].probe_owner != owner
                or match[1].probe_expires_at is None
                or match[1].probe_expires_at <= now
            ):
                raise ProviderHealthError("provider probe lease owner does not match")
            endpoint, circuit = match
            self._apply_probe_outcome(circuit, outcome, now)
            return _health(endpoint, circuit)

    def _apply_outcome(self, circuit: _Circuit, outcome: _Outcome, now: datetime) -> None:
        if outcome.success:
            _close(circuit)
            return
        circuit.consecutive_failures += 1
        if circuit.consecutive_failures >= self.failure_threshold:
            _open(circuit, now, self.cooldown_seconds)

    def _apply_probe_outcome(self, circuit: _Circuit, outcome: _Outcome, now: datetime) -> None:
        if outcome.success:
            _close(circuit)
            return
        circuit.consecutive_failures = max(
            circuit.consecutive_failures + 1,
            self.failure_threshold,
        )
        _open(circuit, now, self.cooldown_seconds)


def _close(circuit: _Circuit) -> None:
    circuit.state = CircuitState.CLOSED
    circuit.consecutive_failures = 0
    circuit.opened_at = None
    circuit.cooldown_until = None
    circuit.probe_lease_id = None
    circuit.probe_owner = None
    circuit.probe_expires_at = None


def _open(circuit: _Circuit, now: datetime, cooldown_seconds: float) -> None:
    circuit.state = CircuitState.OPEN
    circuit.opened_at = now
    circuit.cooldown_until = now + timedelta(seconds=cooldown_seconds)
    circuit.probe_lease_id = None
    circuit.probe_owner = None
    circuit.probe_expires_at = None


class SQLiteProviderHealthStore(_ProviderHealthStore):
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
        self.path = Path(path)
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = float(cooldown_seconds)
        self.probe_lease_seconds = float(probe_lease_seconds)
        self._clock = clock or (lambda: datetime.now(UTC))

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=_BUSY_TIMEOUT_MILLISECONDS / 1000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MILLISECONDS}")
        return connection

    @contextmanager
    def _database(self, *, write: bool = False):
        connection = None
        try:
            connection = self._connect()
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.execute("COMMIT")
        except ProviderHealthError:
            if connection is not None and connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        except (OSError, sqlite3.Error, TypeError, ValueError):
            if connection is not None and connection.in_transaction:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
            raise ProviderHealthError("provider health database operation failed") from None
        finally:
            if connection is not None:
                connection.close()

    def initialize(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise ProviderHealthError("provider health database operation failed") from None
        with self._database(write=True) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_health_meta (
                    schema_version INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS transport_observations (
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
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS endpoint_outcomes (
                    refresh_id TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    normalized_error TEXT,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY (refresh_id, endpoint)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS endpoint_circuits (
                    endpoint TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    consecutive_failures INTEGER NOT NULL,
                    opened_at TEXT,
                    cooldown_until TEXT,
                    probe_lease_id TEXT UNIQUE,
                    probe_owner TEXT,
                    probe_expires_at TEXT
                )
                """
            )
            connection.execute(
                "INSERT INTO provider_health_meta (schema_version) "
                "SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM provider_health_meta)"
            )
            for endpoint in ProviderEndpoint:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO endpoint_circuits (
                        endpoint, state, consecutive_failures
                    ) VALUES (?, ?, 0)
                    """,
                    [endpoint.value, CircuitState.CLOSED.value],
                )

    def record_observation(self, observation: TransportObservation) -> None:
        if not isinstance(observation, TransportObservation):
            raise ProviderHealthError("provider transport observation is invalid")
        with self._database(write=True) as connection:
            connection.execute(
                """
                INSERT INTO transport_observations (
                    refresh_id, provider_session_id, request_id, provider_id,
                    endpoint, attempt, page, protocol_stage, elapsed_ms,
                    recv_calls, response_bytes, end_marker_seen, provider_code,
                    normalized_error, outcome, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    observation.refresh_id,
                    observation.provider_session_id,
                    observation.request_id,
                    observation.provider_id,
                    observation.endpoint.value,
                    observation.attempt,
                    observation.page,
                    observation.protocol_stage.value,
                    observation.elapsed_ms,
                    observation.recv_calls,
                    observation.response_bytes,
                    int(observation.end_marker_seen),
                    observation.provider_code,
                    (
                        observation.normalized_error.value
                        if observation.normalized_error is not None
                        else None
                    ),
                    observation.outcome.value,
                    observation.observed_at.isoformat(),
                ],
            )

    def list_observations(self) -> list[TransportObservation]:
        with self._database() as connection:
            rows = connection.execute(
                """
                SELECT refresh_id, provider_session_id, request_id, provider_id,
                       endpoint, attempt, page, protocol_stage, elapsed_ms,
                       recv_calls, response_bytes, end_marker_seen, provider_code,
                       normalized_error, outcome, observed_at
                FROM transport_observations ORDER BY id
                """
            ).fetchall()
        try:
            return [self._observation_from_row(row) for row in rows]
        except (TypeError, ValueError):
            raise ProviderHealthError("provider health database operation failed") from None

    @staticmethod
    def _observation_from_row(row: sqlite3.Row) -> TransportObservation:
        try:
            endpoint: ProviderEndpoint | ClassificationEndpoint = ProviderEndpoint(row["endpoint"])
        except ValueError:
            endpoint = ClassificationEndpoint(row["endpoint"])
        return TransportObservation(
            refresh_id=row["refresh_id"],
            provider_session_id=row["provider_session_id"],
            request_id=row["request_id"],
            provider_id=row["provider_id"],
            endpoint=endpoint,
            attempt=row["attempt"],
            page=row["page"],
            protocol_stage=ProtocolStage(row["protocol_stage"]),
            elapsed_ms=row["elapsed_ms"],
            recv_calls=row["recv_calls"],
            response_bytes=row["response_bytes"],
            end_marker_seen=bool(row["end_marker_seen"]),
            provider_code=row["provider_code"],
            normalized_error=(
                NormalizedTransportError(row["normalized_error"])
                if row["normalized_error"] is not None
                else None
            ),
            outcome=TransportOutcome(row["outcome"]),
            observed_at=datetime.fromisoformat(row["observed_at"]),
        )

    def record_endpoint_outcome(
        self,
        refresh_id: str,
        endpoint: ProviderEndpoint,
        *,
        success: bool,
        normalized_error: NormalizedTransportError | None = None,
        observed_at: datetime | None = None,
    ) -> EndpointHealth:
        refresh_id = _require_safe_id(refresh_id)
        endpoint = _require_market_endpoint(endpoint)
        outcome = _validate_outcome(success=success, normalized_error=normalized_error)
        now = _require_aware(observed_at or self._clock())
        with self._database(write=True) as connection:
            existing = connection.execute(
                """
                SELECT success, normalized_error FROM endpoint_outcomes
                WHERE refresh_id = ? AND endpoint = ?
                """,
                [refresh_id, endpoint.value],
            ).fetchone()
            if existing is not None:
                existing_outcome = _Outcome(
                    success=bool(existing["success"]),
                    normalized_error=(
                        NormalizedTransportError(existing["normalized_error"])
                        if existing["normalized_error"] is not None
                        else None
                    ),
                )
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

    def endpoint_health(self, endpoint: ProviderEndpoint) -> EndpointHealth:
        endpoint = _require_market_endpoint(endpoint)
        with self._database(write=True) as connection:
            self._reap_expired_probes(connection, _require_aware(self._clock()))
            return self._health_from_connection(connection, endpoint)

    def provider_health(self) -> ProviderHealth:
        with self._database(write=True) as connection:
            self._reap_expired_probes(connection, _require_aware(self._clock()))
            endpoints = tuple(
                self._health_from_connection(connection, endpoint) for endpoint in ProviderEndpoint
            )
        return _provider_health(endpoints)

    def provider_health_snapshot(self) -> ProviderHealth:
        with self._database() as connection:
            endpoints = tuple(
                self._health_from_connection(connection, endpoint) for endpoint in ProviderEndpoint
            )
        return _provider_health(endpoints)

    def acquire_probe(self, endpoint: ProviderEndpoint, *, owner: str) -> ProbeLease | None:
        endpoint = _require_market_endpoint(endpoint)
        owner = _require_safe_id(owner)
        now = _require_aware(self._clock())
        with self._database(write=True) as connection:
            self._reap_expired_probes(connection, now)
            active_probe = connection.execute(
                "SELECT 1 FROM endpoint_circuits WHERE state = ? LIMIT 1",
                [CircuitState.HALF_OPEN.value],
            ).fetchone()
            if active_probe is not None:
                return None
            circuit = self._circuit_from_connection(connection, endpoint)
            eligible = (
                circuit.state == CircuitState.OPEN
                and circuit.cooldown_until is not None
                and circuit.cooldown_until <= now
            )
            if not eligible:
                return None
            lease_id = uuid4().hex
            expires_at = now + timedelta(seconds=self.probe_lease_seconds)
            circuit.state = CircuitState.HALF_OPEN
            circuit.probe_lease_id = lease_id
            circuit.probe_owner = owner
            circuit.probe_expires_at = expires_at
            self._save_circuit(connection, endpoint, circuit)
            return ProbeLease(
                lease_id=lease_id,
                endpoint=endpoint,
                owner=owner,
                expires_at=expires_at,
            )

    def _reap_expired_probes(
        self,
        connection: sqlite3.Connection,
        now: datetime,
    ) -> None:
        rows = connection.execute(
            "SELECT * FROM endpoint_circuits WHERE state = ?",
            [CircuitState.HALF_OPEN.value],
        ).fetchall()
        for row in rows:
            circuit = self._circuit_from_row(row)
            if circuit.probe_expires_at is not None and circuit.probe_expires_at <= now:
                circuit.state = CircuitState.OPEN
                circuit.probe_lease_id = None
                circuit.probe_owner = None
                circuit.probe_expires_at = None
                self._save_circuit(
                    connection,
                    ProviderEndpoint(row["endpoint"]),
                    circuit,
                )

    def resolve_probe(
        self,
        lease_id: str,
        *,
        owner: str,
        success: bool,
        normalized_error: NormalizedTransportError | None = None,
    ) -> EndpointHealth:
        lease_id = _require_safe_id(lease_id)
        owner = _require_safe_id(owner)
        outcome = _validate_outcome(success=success, normalized_error=normalized_error)
        now = _require_aware(self._clock())
        with self._database(write=True) as connection:
            row = connection.execute(
                "SELECT * FROM endpoint_circuits WHERE probe_lease_id = ?",
                [lease_id],
            ).fetchone()
            if (
                row is None
                or row["probe_owner"] != owner
                or row["probe_expires_at"] is None
                or datetime.fromisoformat(row["probe_expires_at"]) <= now
            ):
                raise ProviderHealthError("provider probe lease owner does not match")
            endpoint = ProviderEndpoint(row["endpoint"])
            circuit = self._circuit_from_row(row)
            if outcome.success:
                _close(circuit)
            else:
                circuit.consecutive_failures = max(
                    circuit.consecutive_failures + 1,
                    self.failure_threshold,
                )
                _open(circuit, now, self.cooldown_seconds)
            self._save_circuit(connection, endpoint, circuit)
            return _health(endpoint, circuit)

    def _health_from_connection(
        self,
        connection: sqlite3.Connection,
        endpoint: ProviderEndpoint,
    ) -> EndpointHealth:
        return _health(endpoint, self._circuit_from_connection(connection, endpoint))

    def _circuit_from_connection(
        self,
        connection: sqlite3.Connection,
        endpoint: ProviderEndpoint,
    ) -> _Circuit:
        row = connection.execute(
            "SELECT * FROM endpoint_circuits WHERE endpoint = ?",
            [endpoint.value],
        ).fetchone()
        if row is None:
            raise ProviderHealthError("provider circuit state is unavailable")
        return self._circuit_from_row(row)

    @staticmethod
    def _circuit_from_row(row: sqlite3.Row) -> _Circuit:
        return _Circuit(
            state=CircuitState(row["state"]),
            consecutive_failures=row["consecutive_failures"],
            opened_at=(
                datetime.fromisoformat(row["opened_at"]) if row["opened_at"] is not None else None
            ),
            cooldown_until=(
                datetime.fromisoformat(row["cooldown_until"])
                if row["cooldown_until"] is not None
                else None
            ),
            probe_lease_id=row["probe_lease_id"],
            probe_owner=row["probe_owner"],
            probe_expires_at=(
                datetime.fromisoformat(row["probe_expires_at"])
                if row["probe_expires_at"] is not None
                else None
            ),
        )

    @staticmethod
    def _save_circuit(
        connection: sqlite3.Connection,
        endpoint: ProviderEndpoint,
        circuit: _Circuit,
    ) -> None:
        connection.execute(
            """
            UPDATE endpoint_circuits
            SET state = ?, consecutive_failures = ?, opened_at = ?,
                cooldown_until = ?, probe_lease_id = ?, probe_owner = ?,
                probe_expires_at = ?
            WHERE endpoint = ?
            """,
            [
                circuit.state.value,
                circuit.consecutive_failures,
                circuit.opened_at.isoformat() if circuit.opened_at else None,
                circuit.cooldown_until.isoformat() if circuit.cooldown_until else None,
                circuit.probe_lease_id,
                circuit.probe_owner,
                circuit.probe_expires_at.isoformat() if circuit.probe_expires_at else None,
                endpoint.value,
            ],
        )
