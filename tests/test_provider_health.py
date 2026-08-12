import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.config import Settings
from backend.app.market.provider_health import (
    CircuitState,
    InMemoryProviderHealthStore,
    ProviderHealthError,
    ProviderOutcomeConflictError,
    SQLiteProviderHealthStore,
)
from backend.app.market.provider_transport import (
    ClassificationEndpoint,
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
    TransportOutcome,
)
from backend.app.storage.layout import StorageLayout


class MutableClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 12, 12, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def observation(
    *,
    request_id: str = "request-1",
    endpoint=ProviderEndpoint.ALL_STOCK,
    stage: ProtocolStage = ProtocolStage.RECEIVE,
    error: NormalizedTransportError | None = NormalizedTransportError.RECV_TIMEOUT,
    observed_at: datetime | None = None,
) -> TransportObservation:
    return TransportObservation(
        refresh_id="refresh-1",
        provider_session_id="session-1",
        request_id=request_id,
        endpoint=endpoint,
        attempt=2,
        page=3,
        protocol_stage=stage,
        elapsed_ms=30_001,
        recv_calls=4,
        response_bytes=8192,
        end_marker_seen=False,
        provider_code="10002008" if error is not None else "0",
        normalized_error=error,
        outcome=TransportOutcome.ERROR if error is not None else TransportOutcome.SUCCESS,
        observed_at=observed_at or datetime(2026, 8, 12, 12, tzinfo=UTC),
    )


def make_store(kind: str, tmp_path: Path, clock: MutableClock, **overrides):
    options = {
        "failure_threshold": 3,
        "cooldown_seconds": 60,
        "probe_lease_seconds": 10,
        "clock": clock,
        **overrides,
    }
    if kind == "memory":
        store = InMemoryProviderHealthStore(**options)
    else:
        store = SQLiteProviderHealthStore(tmp_path / "provider-health.sqlite3", **options)
    store.initialize()
    return store


def open_endpoint(store, endpoint: ProviderEndpoint, *, prefix: str = "refresh") -> None:
    for index in range(store.failure_threshold):
        store.record_terminal_failure(
            f"{prefix}-{index}",
            endpoint,
            NormalizedTransportError.RECV_TIMEOUT,
        )


def test_sqlite_audit_round_trip_uses_only_explicit_safe_columns(tmp_path: Path) -> None:
    clock = MutableClock()
    path = tmp_path / "provider-health.sqlite3"
    store = SQLiteProviderHealthStore(path, clock=clock)
    assert path.exists() is False
    store.initialize()
    rows = [
        observation(),
        observation(
            request_id="classification-request",
            endpoint=ClassificationEndpoint.INDUSTRY,
            stage=ProtocolStage.COMPLETE,
            error=None,
        ),
    ]

    for row in rows:
        store.record_observation(row)

    assert store.list_observations() == rows
    connection = sqlite3.connect(path)
    try:
        schemas = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        columns = {
            table: [row[1] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()]
            for table in (
                "transport_observations",
                "endpoint_outcomes",
                "endpoint_circuits",
            )
        }
    finally:
        connection.close()

    assert columns["transport_observations"] == [
        "id",
        "refresh_id",
        "provider_session_id",
        "request_id",
        "provider_id",
        "endpoint",
        "attempt",
        "page",
        "protocol_stage",
        "elapsed_ms",
        "recv_calls",
        "response_bytes",
        "end_marker_seen",
        "provider_code",
        "normalized_error",
        "outcome",
        "observed_at",
    ]
    serialized_schema = json.dumps({"schemas": schemas, "columns": columns}).lower()
    for forbidden in ("payload", "token", "url", "raw_message", "exception"):
        assert forbidden not in serialized_schema


def test_sqlite_initialize_is_additive_idempotent_and_restart_safe(tmp_path: Path) -> None:
    path = tmp_path / "provider-health.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE legacy_marker (value TEXT NOT NULL)")
    connection.execute("INSERT INTO legacy_marker VALUES ('keep')")
    connection.commit()
    connection.close()
    clock = MutableClock()
    first = SQLiteProviderHealthStore(path, clock=clock)

    first.initialize()
    first.initialize()
    first.record_terminal_failure(
        "refresh-1",
        ProviderEndpoint.TRADE_DATES,
        NormalizedTransportError.CONNECT_ERROR,
    )
    restarted = SQLiteProviderHealthStore(path, clock=clock)
    restarted.initialize()

    assert restarted.endpoint_health(ProviderEndpoint.TRADE_DATES).consecutive_failures == 1
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("SELECT value FROM legacy_marker").fetchone() == ("keep",)
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] > 0
    finally:
        connection.close()


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_observations_never_implicitly_count_attempt_or_pagination_events(
    kind: str,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    store = make_store(kind, tmp_path, clock)
    endpoint = ProviderEndpoint.ALL_STOCK
    store.record_observation(observation(request_id="attempt-1", error=None))
    store.record_observation(observation(request_id="attempt-2", error=None))
    store.record_observation(
        observation(
            request_id="page-2",
            stage=ProtocolStage.PAGINATION,
            error=NormalizedTransportError.PAGINATION_STALLED,
        )
    )

    assert store.endpoint_health(endpoint).consecutive_failures == 0
    store.record_terminal_failure(
        "refresh-1",
        endpoint,
        NormalizedTransportError.PAGINATION_STALLED,
    )
    store.record_terminal_failure(
        "refresh-1",
        endpoint,
        NormalizedTransportError.PAGINATION_STALLED,
    )

    assert store.endpoint_health(endpoint).consecutive_failures == 1


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_final_endpoint_outcome_is_single_assignment_and_conflicts_fail_closed(
    kind: str,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    store = make_store(kind, tmp_path, clock)
    endpoint = ProviderEndpoint.DAILY_ASTOCK

    store.record_endpoint_outcome(
        "refresh-final",
        endpoint,
        success=False,
        normalized_error=NormalizedTransportError.EOF,
    )
    store.record_endpoint_outcome(
        "refresh-final",
        endpoint,
        success=False,
        normalized_error=NormalizedTransportError.EOF,
    )
    with pytest.raises(ProviderOutcomeConflictError, match="already resolved"):
        store.record_terminal_success("refresh-final", endpoint)

    assert store.endpoint_health(endpoint).consecutive_failures == 1


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_circuit_opens_at_threshold_and_persists_timestamps(
    kind: str,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    store = make_store(kind, tmp_path, clock)
    endpoint = ProviderEndpoint.ADJUST_FACTOR

    store.record_terminal_failure("refresh-1", endpoint, NormalizedTransportError.CONNECT_ERROR)
    store.record_terminal_failure("refresh-2", endpoint, NormalizedTransportError.RECV_TIMEOUT)
    assert store.endpoint_health(endpoint).state == CircuitState.CLOSED
    store.record_terminal_failure("refresh-3", endpoint, NormalizedTransportError.EOF)

    health = store.endpoint_health(endpoint)
    assert health.state == CircuitState.OPEN
    assert health.consecutive_failures == 3
    assert health.opened_at == clock.now
    assert health.cooldown_until == clock.now + timedelta(seconds=60)
    if kind == "sqlite":
        restarted = SQLiteProviderHealthStore(
            tmp_path / "provider-health.sqlite3",
            failure_threshold=3,
            cooldown_seconds=60,
            probe_lease_seconds=10,
            clock=clock,
        )
        restarted.initialize()
        assert restarted.endpoint_health(endpoint) == health


def test_sqlite_probe_lease_is_atomic_across_threads_and_store_instances(tmp_path: Path) -> None:
    clock = MutableClock()
    path = tmp_path / "provider-health.sqlite3"
    first = SQLiteProviderHealthStore(
        path,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
        clock=clock,
    )
    second = SQLiteProviderHealthStore(
        path,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
        clock=clock,
    )
    first.initialize()
    second.initialize()
    endpoint = ProviderEndpoint.INDEX_HISTORY
    first.record_terminal_failure("refresh-open", endpoint, NormalizedTransportError.RECV_TIMEOUT)
    assert first.acquire_probe(endpoint, owner="worker-before-cooldown") is None
    clock.advance(60)
    barrier = threading.Barrier(2)

    def acquire(store, owner):
        barrier.wait()
        return store.acquire_probe(endpoint, owner=owner)

    with ThreadPoolExecutor(max_workers=2) as executor:
        leases = list(
            executor.map(
                lambda args: acquire(*args),
                [(first, "worker-a"), (second, "worker-b")],
            )
        )

    winners = [lease for lease in leases if lease is not None]
    assert len(winners) == 1
    lease = winners[0]
    restarted = SQLiteProviderHealthStore(
        path,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
        clock=clock,
    )
    restarted.initialize()
    leased_health = restarted.endpoint_health(endpoint)
    assert leased_health.state == CircuitState.HALF_OPEN
    assert leased_health.probe_lease_id == lease.lease_id
    assert leased_health.probe_owner == lease.owner
    assert leased_health.probe_expires_at == lease.expires_at
    with pytest.raises(ProviderHealthError, match="lease owner"):
        second.resolve_probe(
            lease.lease_id,
            owner="not-the-owner",
            success=True,
        )
    assert first.endpoint_health(endpoint).state == CircuitState.HALF_OPEN


def test_in_memory_probe_lease_is_atomic_across_threads(tmp_path: Path) -> None:
    clock = MutableClock()
    store = make_store(
        "memory",
        tmp_path,
        clock,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
    )
    endpoint = ProviderEndpoint.INDEX_HISTORY
    open_endpoint(store, endpoint)
    clock.advance(60)
    barrier = threading.Barrier(4)

    def acquire(owner: str):
        barrier.wait()
        return store.acquire_probe(endpoint, owner=owner)

    with ThreadPoolExecutor(max_workers=4) as executor:
        leases = list(executor.map(acquire, [f"worker-{index}" for index in range(4)]))

    assert len([lease for lease in leases if lease is not None]) == 1


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_provider_allows_only_one_cross_endpoint_probe_and_releases_expired_lease(
    kind: str,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    first = make_store(
        kind,
        tmp_path,
        clock,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
    )
    second = (
        SQLiteProviderHealthStore(
            tmp_path / "provider-health.sqlite3",
            failure_threshold=1,
            cooldown_seconds=60,
            probe_lease_seconds=10,
            clock=clock,
        )
        if kind == "sqlite"
        else first
    )
    second.initialize()
    endpoints = (ProviderEndpoint.ALL_STOCK, ProviderEndpoint.TRADE_DATES)
    for endpoint in endpoints:
        open_endpoint(first, endpoint, prefix=f"open-{endpoint.value}")
    clock.advance(60)
    barrier = threading.Barrier(2)

    def acquire(store, endpoint, owner):
        barrier.wait()
        return store.acquire_probe(endpoint, owner=owner)

    with ThreadPoolExecutor(max_workers=2) as executor:
        leases = list(
            executor.map(
                lambda args: acquire(*args),
                [
                    (first, endpoints[0], "worker-all-stock"),
                    (second, endpoints[1], "worker-trade-dates"),
                ],
            )
        )

    winners = [lease for lease in leases if lease is not None]
    assert len(winners) == 1
    winner = winners[0]
    loser_endpoint = next(endpoint for endpoint in endpoints if endpoint != winner.endpoint)
    assert first.acquire_probe(loser_endpoint, owner="blocked-worker") is None
    clock.advance(11)
    replacement = second.acquire_probe(loser_endpoint, owner="replacement-worker")
    assert replacement is not None
    assert replacement.endpoint == loser_endpoint
    assert replacement.lease_id != winner.lease_id
    assert first.endpoint_health(winner.endpoint).state == CircuitState.OPEN


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_probe_lease_expiry_success_close_and_failure_reopen(
    kind: str,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    store = make_store(
        kind,
        tmp_path,
        clock,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
    )
    endpoint = ProviderEndpoint.DAILY_FACTOR
    open_endpoint(store, endpoint)
    clock.advance(60)
    first = store.acquire_probe(endpoint, owner="worker-a")
    assert first is not None
    clock.advance(11)
    second = store.acquire_probe(endpoint, owner="worker-b")
    assert second is not None
    assert second.lease_id != first.lease_id
    with pytest.raises(ProviderHealthError, match="lease owner"):
        store.resolve_probe(first.lease_id, owner="worker-a", success=True)

    closed = store.resolve_probe(second.lease_id, owner="worker-b", success=True)
    assert closed.state == CircuitState.CLOSED
    assert closed.consecutive_failures == 0
    store.record_terminal_failure("refresh-reopen", endpoint, NormalizedTransportError.RECV_TIMEOUT)
    clock.advance(60)
    third = store.acquire_probe(endpoint, owner="worker-c")
    assert third is not None
    failed_at = clock.now
    reopened = store.resolve_probe(
        third.lease_id,
        owner="worker-c",
        success=False,
        normalized_error=NormalizedTransportError.RECV_TIMEOUT,
    )
    assert reopened.state == CircuitState.OPEN
    assert reopened.consecutive_failures == 2
    assert reopened.opened_at == failed_at
    assert reopened.cooldown_until == failed_at + timedelta(seconds=60)


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
@pytest.mark.parametrize("initial_state", [CircuitState.OPEN, CircuitState.HALF_OPEN])
def test_late_refresh_outcome_cannot_bypass_open_or_half_open_probe_resolution(
    kind: str,
    initial_state: CircuitState,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    store = make_store(
        kind,
        tmp_path,
        clock,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
    )
    endpoint = ProviderEndpoint.ALL_STOCK
    open_endpoint(store, endpoint)
    if initial_state == CircuitState.HALF_OPEN:
        clock.advance(60)
        assert store.acquire_probe(endpoint, owner="probe-owner") is not None
    before = store.endpoint_health(endpoint)

    with pytest.raises(ProviderHealthError, match="probe resolution"):
        store.record_terminal_success("late-refresh-success", endpoint)
    assert store.endpoint_health(endpoint) == before
    with pytest.raises(ProviderHealthError, match="probe resolution"):
        store.record_terminal_failure(
            "late-refresh-failure",
            endpoint,
            NormalizedTransportError.RECV_TIMEOUT,
        )
    after = store.endpoint_health(endpoint)
    assert after == before
    assert after.cooldown_until == before.cooldown_until
    assert after.consecutive_failures == before.consecutive_failures


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_terminal_success_resets_pre_threshold_failure_count(
    kind: str,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    store = make_store(kind, tmp_path, clock)
    endpoint = ProviderEndpoint.TRADE_DATES
    store.record_terminal_failure(
        "refresh-failure", endpoint, NormalizedTransportError.CONNECT_ERROR
    )
    assert store.endpoint_health(endpoint).consecutive_failures == 1

    store.record_terminal_success("refresh-success", endpoint)

    health = store.endpoint_health(endpoint)
    assert health.state == CircuitState.CLOSED
    assert health.consecutive_failures == 0


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_provider_aggregation_uses_only_six_required_market_endpoints(
    kind: str,
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    store = make_store(
        kind,
        tmp_path,
        clock,
        failure_threshold=1,
        cooldown_seconds=60,
        probe_lease_seconds=10,
    )
    store.record_observation(
        observation(
            request_id="classification-only",
            endpoint=ClassificationEndpoint.INDUSTRY,
        )
    )
    assert store.provider_health().state == CircuitState.CLOSED
    open_endpoint(store, ProviderEndpoint.ALL_STOCK, prefix="all-stock")
    open_endpoint(store, ProviderEndpoint.TRADE_DATES, prefix="trade-dates")
    provider = store.provider_health()
    assert provider.state == CircuitState.OPEN
    assert provider.blocking_endpoints == (
        ProviderEndpoint.TRADE_DATES,
        ProviderEndpoint.ALL_STOCK,
    )
    assert provider.probe_endpoints == ()
    assert {item.endpoint for item in provider.endpoints} == set(ProviderEndpoint)
    clock.advance(60)
    lease = store.acquire_probe(ProviderEndpoint.ALL_STOCK, owner="probe-worker")
    assert lease is not None
    provider = store.provider_health()
    assert provider.state == CircuitState.OPEN
    assert provider.blocking_endpoints == (ProviderEndpoint.TRADE_DATES,)
    assert provider.probe_endpoints == (ProviderEndpoint.ALL_STOCK,)
    assert (
        store.acquire_probe(
            ProviderEndpoint.TRADE_DATES,
            owner="second-probe-must-wait",
        )
        is None
    )
    store.resolve_probe(lease.lease_id, owner="probe-worker", success=True)
    trade_lease = store.acquire_probe(
        ProviderEndpoint.TRADE_DATES,
        owner="trade-probe-worker",
    )
    assert trade_lease is not None
    provider = store.provider_health()
    assert provider.state == CircuitState.HALF_OPEN
    assert provider.blocking_endpoints == ()
    assert provider.probe_endpoints == (ProviderEndpoint.TRADE_DATES,)
    store.resolve_probe(
        trade_lease.lease_id,
        owner="trade-probe-worker",
        success=True,
    )
    assert store.provider_health().state == CircuitState.CLOSED


@pytest.mark.parametrize(
    ("success", "error"),
    [
        (False, "private-token raw exception"),
        (False, None),
        (True, NormalizedTransportError.EOF),
    ],
)
def test_terminal_outcomes_reject_non_normalized_or_inconsistent_input(
    tmp_path: Path,
    success,
    error,
) -> None:
    store = make_store("memory", tmp_path, MutableClock())

    with pytest.raises(ProviderHealthError) as caught:
        store.record_endpoint_outcome(
            "refresh-invalid",
            ProviderEndpoint.TRADE_DATES,
            success=success,
            normalized_error=error,
        )

    assert "private-token" not in str(caught.value)
    assert "raw exception" not in str(caught.value)


def test_terminal_outcomes_reject_classification_endpoint(tmp_path: Path) -> None:
    store = make_store("memory", tmp_path, MutableClock())

    with pytest.raises(ProviderHealthError, match="market endpoint"):
        store.record_terminal_failure(
            "refresh-classification",
            ClassificationEndpoint.INDUSTRY,
            NormalizedTransportError.RECV_TIMEOUT,
        )


def test_sqlite_errors_are_stable_and_do_not_disclose_path_sql_or_exception(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import backend.app.market.provider_health as provider_health

    path = tmp_path / "private-token" / "provider-health.sqlite3"

    def fail_connect(*_args, **_kwargs):
        raise sqlite3.OperationalError(f"SELECT secret FROM {path} raw exception")

    monkeypatch.setattr(provider_health.sqlite3, "connect", fail_connect)
    store = SQLiteProviderHealthStore(path)

    with pytest.raises(ProviderHealthError) as caught:
        store.initialize()

    assert str(caught.value) == "provider health database operation failed"
    assert str(path) not in str(caught.value)
    assert "SELECT" not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "name",
    ["", "../health.sqlite3", "/tmp/health.sqlite3", "nested/health.sqlite3", "health.db"],
)
def test_provider_health_database_name_is_a_safe_sqlite_basename(name: str) -> None:
    with pytest.raises(ValidationError, match="provider health database name"):
        Settings(_env_file=None, provider_health_database_name=name)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider_circuit_failure_threshold", 0),
        ("provider_circuit_cooldown_seconds", 0),
        ("provider_circuit_probe_lease_seconds", -1),
    ],
)
def test_provider_circuit_configuration_requires_positive_values(field: str, value) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


def test_provider_health_layout_is_local_and_directory_creation_does_not_create_database(
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        local_control_dir=tmp_path / "control",
        provider_health_database_name="provider-health.sqlite3",
    )
    layout = StorageLayout(settings)

    assert layout.local_paths.provider_health_database == (
        tmp_path / "control" / "provider-health.sqlite3"
    )
    assert layout.provider_health_database == layout.local_paths.provider_health_database
    layout.ensure_local_runtime_dirs()
    assert settings.local_control_dir.is_dir()
    assert layout.provider_health_database.exists() is False


def test_provider_health_defaults_do_not_change_transport_retry_policy() -> None:
    settings = Settings(_env_file=None)

    assert settings.provider_circuit_failure_threshold == 3
    assert settings.provider_circuit_cooldown_seconds > 0
    assert settings.provider_circuit_probe_lease_seconds > 0
    assert settings.baostock_socket_timeout_seconds == 30.0
