import os
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

from backend.app.market.provider_health import (
    CircuitState,
    ProviderHealthError,
    SQLiteProviderHealthStore,
)
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    TransportObservation,
    TransportOutcome,
)


def initialized(tmp_path: Path) -> Path:
    path = tmp_path / "provider-health.sqlite3"
    SQLiteProviderHealthStore(path).initialize()
    path.chmod(0o644)
    return path


def tree_with_metadata(root: Path) -> dict[str, tuple[bytes, int]]:
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file()
    }


def test_foreign_alias_swap_before_sqlite_connect_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    original = initialized(tmp_path)
    foreign = tmp_path / "foreign.sqlite3"
    SQLiteProviderHealthStore(foreign).initialize()
    original_bytes = original.read_bytes()
    foreign_bytes = foreign.read_bytes()
    retained = tmp_path / "retained-original-alias.sqlite3"
    connect = sqlite3.connect
    replaced: list[Path] = []

    def replace_alias_then_connect(database, *args, **kwargs):
        if ".maintenance-" in str(database):
            alias = Path(unquote(urlsplit(str(database)).path))
            alias.rename(retained)
            foreign.rename(alias)
            replaced.append(alias)
        return connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", replace_alias_then_connect)
    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(original).record_terminal_success(
            "must-not-write-foreign", ProviderEndpoint.TRADE_DATES
        )
    assert len(replaced) == 1
    assert replaced[0].read_bytes() == foreign_bytes
    assert retained.read_bytes() == original_bytes
    assert original.read_bytes() == original_bytes


def test_foreign_alias_replacement_during_link_is_not_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    original = initialized(tmp_path)
    foreign = tmp_path / "foreign.sqlite3"
    SQLiteProviderHealthStore(foreign).initialize()
    original_bytes = original.read_bytes()
    foreign_bytes = foreign.read_bytes()
    retained = tmp_path / "retained-original-alias.sqlite3"
    link = os.link
    replaced: list[Path] = []

    def link_then_replace(source, destination, *args, **kwargs):
        result = link(source, destination, *args, **kwargs)
        if ".maintenance-" in str(destination):
            alias = Path(destination)
            alias.rename(retained)
            foreign.rename(alias)
            replaced.append(alias)
        return result

    monkeypatch.setattr(os, "link", link_then_replace)
    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(original).record_terminal_success(
            "must-not-delete-foreign", ProviderEndpoint.TRADE_DATES
        )
    assert len(replaced) == 1
    assert replaced[0].exists()
    assert replaced[0].read_bytes() == foreign_bytes
    assert retained.read_bytes() == original_bytes
    assert original.read_bytes() == original_bytes


@pytest.mark.parametrize("error_type", [TypeError, ValueError])
def test_database_context_preserves_programmer_errors(
    tmp_path: Path, error_type: type[Exception]
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path)
    original_error = error_type("private programmer defect")
    with pytest.raises(error_type) as captured:
        with store._database():
            raise original_error
    assert captured.value is original_error


def test_calendar_maintenance_health_does_not_create_missing_database(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = tmp_path / "missing" / "provider-health.sqlite3"
    store = CalendarMaintenanceHealthStore(path)

    with pytest.raises(ProviderHealthError):
        store.provider_health_snapshot()

    assert not path.exists()


def test_calendar_maintenance_health_reads_existing_closed_state_without_writing(
    tmp_path: Path,
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    before = path.read_bytes()
    health = CalendarMaintenanceHealthStore(path).provider_health_snapshot()

    assert health.state == CircuitState.CLOSED
    assert len(health.endpoints) == len(tuple(ProviderEndpoint))
    assert path.read_bytes() == before


def test_provider_health_is_readonly_and_does_not_reap_expired_half_open(
    tmp_path: Path,
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE endpoint_circuits
            SET state='HALF_OPEN', consecutive_failures=3,
                opened_at='2026-08-30T08:00:00+08:00',
                cooldown_until='2026-08-30T08:01:00+08:00',
                probe_lease_id='lease-1', probe_owner='owner-1',
                probe_expires_at='2026-08-30T08:02:00+08:00'
            WHERE endpoint='trade_dates'
            """
        )
    before = path.read_bytes()

    health = CalendarMaintenanceHealthStore(path).provider_health()

    assert health.state == CircuitState.HALF_OPEN
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE provider_health_meta SET schema_version='bad'",
        "UPDATE provider_health_meta SET schema_version=1.5",
        "UPDATE provider_health_meta SET schema_version=x'01'",
        "UPDATE provider_health_meta SET schema_version=2",
        "DELETE FROM provider_health_meta",
        "INSERT INTO provider_health_meta VALUES (1)",
    ],
)
def test_calendar_maintenance_health_requires_exact_metadata(tmp_path: Path, mutation: str) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(mutation)
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()
    assert tree_with_metadata(tmp_path) == before


@pytest.mark.parametrize(
    "mutation",
    [
        "DELETE FROM endpoint_circuits WHERE endpoint='trade_dates'",
        "INSERT INTO endpoint_circuits VALUES ('unknown','CLOSED',0,NULL,NULL,NULL,NULL,NULL)",
        "UPDATE endpoint_circuits SET endpoint=NULL WHERE endpoint='trade_dates'",
        "UPDATE endpoint_circuits SET state='INVALID' WHERE endpoint='trade_dates'",
        "UPDATE endpoint_circuits SET consecutive_failures=0.5 WHERE endpoint='trade_dates'",
        "UPDATE endpoint_circuits SET consecutive_failures='bad' WHERE endpoint='trade_dates'",
    ],
)
def test_calendar_maintenance_health_requires_exact_circuit_rows_and_types(
    tmp_path: Path, mutation: str
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(mutation)
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()
    assert tree_with_metadata(tmp_path) == before


def test_closed_state_preserves_nonzero_failure_count_and_offset_timestamps(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE endpoint_circuits SET consecutive_failures=7 WHERE endpoint='trade_dates'"
        )
    endpoint = CalendarMaintenanceHealthStore(path).endpoint_health(ProviderEndpoint.TRADE_DATES)

    assert endpoint.state == CircuitState.CLOSED
    assert endpoint.consecutive_failures == 7


def test_open_state_accepts_aware_iso_offsets(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            UPDATE endpoint_circuits
            SET state='OPEN', consecutive_failures=1,
                opened_at='2026-08-31T08:00:00+08:00',
                cooldown_until='2026-08-31T08:01:00+08:00'
            WHERE endpoint='trade_dates'
            """
        )
    endpoint = CalendarMaintenanceHealthStore(path).endpoint_health(ProviderEndpoint.TRADE_DATES)

    assert endpoint.state == CircuitState.OPEN
    assert endpoint.opened_at == datetime.fromisoformat("2026-08-31T08:00:00+08:00")


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE endpoint_circuits SET opened_at='2026-08-31T08:00:00' WHERE endpoint='trade_dates'",
        "UPDATE endpoint_circuits SET state='OPEN' WHERE endpoint='trade_dates'",
        (
            "UPDATE endpoint_circuits SET state='HALF_OPEN', consecutive_failures=1, "
            "opened_at='2026-08-31T08:00:00+00:00', "
            "cooldown_until='2026-08-31T08:01:00+00:00' WHERE endpoint='trade_dates'"
        ),
        (
            "UPDATE endpoint_circuits SET state='OPEN', consecutive_failures=1, "
            "opened_at='2026-08-31T08:01:00+00:00', "
            "cooldown_until='2026-08-31T08:00:00+00:00' WHERE endpoint='trade_dates'"
        ),
    ],
)
def test_invalid_circuit_state_timing_or_leases_fail_closed(tmp_path: Path, mutation: str) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(mutation)
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()
    assert tree_with_metadata(tmp_path) == before


@pytest.mark.parametrize(
    "mutation",
    [
        "CREATE INDEX foreign_idx ON endpoint_outcomes (success)",
        "CREATE TRIGGER foreign_trigger AFTER INSERT ON endpoint_outcomes BEGIN SELECT 1; END",
    ],
)
def test_calendar_maintenance_health_rejects_foreign_schema_objects(
    tmp_path: Path, mutation: str
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(mutation)
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()
    assert tree_with_metadata(tmp_path) == before


def test_calendar_maintenance_health_reuses_guarded_terminal_writer(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path, failure_threshold=1)
    store.record_terminal_failure(
        "maintenance-refresh",
        ProviderEndpoint.TRADE_DATES,
        NormalizedTransportError.RECV_TIMEOUT,
        observed_at=datetime(2026, 8, 31, tzinfo=UTC),
    )

    assert store.provider_health_snapshot().state == CircuitState.OPEN


def test_terminal_success_and_observation_round_trip_use_existing_database(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path, failure_threshold=2)
    observation = TransportObservation(
        refresh_id="refresh-1",
        provider_session_id="session-1",
        request_id="request-1",
        endpoint=ProviderEndpoint.TRADE_DATES,
        attempt=1,
        page=1,
        protocol_stage=ProtocolStage.COMPLETE,
        elapsed_ms=5,
        recv_calls=1,
        response_bytes=20,
        end_marker_seen=True,
        provider_code="0",
        outcome=TransportOutcome.SUCCESS,
        observed_at=datetime(2026, 8, 31, tzinfo=UTC),
    )

    store.record_observation(observation)
    store.record_terminal_failure(
        "refresh-1", ProviderEndpoint.TRADE_DATES, NormalizedTransportError.EOF
    )
    closed = store.record_terminal_success("refresh-2", ProviderEndpoint.TRADE_DATES)

    assert store.list_observations() == [observation]
    assert closed.state == CircuitState.CLOSED
    assert closed.consecutive_failures == 0


def test_terminal_outcome_is_idempotent_and_conflicts_fail_closed(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore
    from backend.app.market.provider_health import ProviderOutcomeConflictError

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path)
    first = store.record_terminal_failure(
        "refresh-1", ProviderEndpoint.TRADE_DATES, NormalizedTransportError.EOF
    )
    repeated = store.record_terminal_failure(
        "refresh-1", ProviderEndpoint.TRADE_DATES, NormalizedTransportError.EOF
    )

    assert repeated == first
    with pytest.raises(ProviderOutcomeConflictError):
        store.record_terminal_success("refresh-1", ProviderEndpoint.TRADE_DATES)


@pytest.mark.parametrize(
    "success,normalized_error",
    [
        (2, None),
        ("bad", None),
        (0, None),
        (1, "EOF"),
        (0, "not-an-error"),
        (0, "x" * 129),
    ],
)
def test_selected_persisted_outcome_fields_are_bounded_and_validated(
    tmp_path: Path, success, normalized_error
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO endpoint_outcomes VALUES (?, ?, ?, ?, ?)",
            (
                "refresh-1",
                "trade_dates",
                success,
                normalized_error,
                "2026-08-31T08:00:00+00:00",
            ),
        )
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).record_terminal_success(
            "refresh-1", ProviderEndpoint.TRADE_DATES
        )
    assert tree_with_metadata(tmp_path) == before


@pytest.mark.parametrize("kind", ["fifo", "symlink"])
def test_calendar_maintenance_health_rejects_non_regular_or_symlink_path(
    tmp_path: Path, kind: str
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = tmp_path / "unsafe.sqlite3"
    if kind == "fifo":
        os.mkfifo(path)
    else:
        path.symlink_to(initialized(tmp_path))

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()


def test_calendar_maintenance_health_refuses_inode_replacement_after_validation(
    tmp_path: Path,
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path)
    store.provider_health_snapshot()
    moved = tmp_path / "old.sqlite3"
    path.rename(moved)
    SQLiteProviderHealthStore(path).initialize()
    path.chmod(0o644)

    with pytest.raises(ProviderHealthError):
        store.provider_health_snapshot()


def test_audit_after_path_missing_never_recreates_database(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path, failure_threshold=1)
    store.record_terminal_failure(
        "audit-refresh", ProviderEndpoint.TRADE_DATES, NormalizedTransportError.EOF
    )
    path.unlink()
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        store.record_terminal_success("must-not-recreate", ProviderEndpoint.TRADE_DATES)

    assert tree_with_metadata(tmp_path) == before
    assert not path.exists()


def test_audit_after_path_replacement_never_adopts_new_inode(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path)
    store.provider_health_snapshot()
    old = tmp_path / "old-health.sqlite3"
    path.rename(old)
    SQLiteProviderHealthStore(path).initialize()
    path.chmod(0o644)
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        store.record_terminal_success("must-not-adopt", ProviderEndpoint.TRADE_DATES)

    assert tree_with_metadata(tmp_path) == before


def test_oversized_health_field_is_rejected_without_echoing_marker(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    marker = "OVERSIZED_SECRET_MARKER"
    path = initialized(tmp_path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE endpoint_circuits SET probe_owner=? WHERE endpoint='trade_dates'",
            (marker * 20,),
        )
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError) as captured:
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()

    assert marker not in str(captured.value)
    assert tree_with_metadata(tmp_path) == before


def test_connection_open_path_swap_is_rejected_without_state_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    replacement = tmp_path / "replacement.sqlite3"
    SQLiteProviderHealthStore(replacement).initialize()
    original_connect = sqlite3.connect
    swapped = False

    def swap_on_descriptor(database, *args, **kwargs):
        nonlocal swapped
        if not swapped and "/dev/fd/" in str(database):
            path.rename(tmp_path / "retained-health.sqlite3")
            replacement.rename(path)
            path.chmod(0o644)
            swapped = True
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", swap_on_descriptor)
    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()
    assert swapped


def test_calendar_maintenance_health_true_exclusive_lock_is_bounded(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    connection = sqlite3.connect(path, timeout=0)
    try:
        connection.execute("BEGIN EXCLUSIVE")
        started = time.monotonic()
        with pytest.raises(ProviderHealthError):
            CalendarMaintenanceHealthStore(path).provider_health_snapshot()
        assert time.monotonic() - started < 1
    finally:
        connection.rollback()
        connection.close()


def test_calendar_maintenance_health_write_requires_file_permission(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    path.chmod(0o444)
    try:
        with pytest.raises(ProviderHealthError):
            CalendarMaintenanceHealthStore(path).record_terminal_success(
                "refresh-1", ProviderEndpoint.TRADE_DATES
            )
    finally:
        path.chmod(0o644)


def test_calendar_maintenance_health_rejects_symlink_parent(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    real_path = real_parent / "health.sqlite3"
    SQLiteProviderHealthStore(real_path).initialize()
    real_path.chmod(0o644)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(linked_parent / "health.sqlite3").provider_health_snapshot()


def test_calendar_maintenance_health_rejects_hardlink(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    os.link(path, tmp_path / "foreign-hardlink.sqlite3")
    before = tree_with_metadata(tmp_path)

    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()

    assert tree_with_metadata(tmp_path) == before


def test_calendar_maintenance_health_rejects_other_writable_file(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    path.chmod(0o666)
    before = tree_with_metadata(tmp_path)
    try:
        with pytest.raises(ProviderHealthError):
            CalendarMaintenanceHealthStore(path).provider_health_snapshot()
        assert tree_with_metadata(tmp_path) == before
    finally:
        path.chmod(0o644)


def test_calendar_maintenance_health_rejects_unsafe_parent_permissions(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    parent = tmp_path / "unsafe-parent"
    parent.mkdir()
    path = parent / "health.sqlite3"
    SQLiteProviderHealthStore(path).initialize()
    path.chmod(0o644)
    parent.chmod(0o777)
    try:
        with pytest.raises(ProviderHealthError):
            CalendarMaintenanceHealthStore(path).provider_health_snapshot()
    finally:
        parent.chmod(0o700)


def test_calendar_maintenance_health_rejects_unreadable_ancestor(tmp_path: Path) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    parent = tmp_path / "unreadable-parent"
    parent.mkdir()
    path = parent / "health.sqlite3"
    SQLiteProviderHealthStore(path).initialize()
    path.chmod(0o644)
    parent.chmod(0o000)
    try:
        with pytest.raises(ProviderHealthError):
            CalendarMaintenanceHealthStore(path).provider_health_snapshot()
    finally:
        parent.chmod(0o700)


@pytest.mark.parametrize("suffix", ["-journal", "-wal", "-shm"])
def test_calendar_maintenance_health_rejects_existing_sidecar(tmp_path: Path, suffix: str) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    path.with_name(path.name + suffix).touch()
    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).provider_health_snapshot()


def test_calendar_maintenance_health_rejects_in_place_change_during_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    store = CalendarMaintenanceHealthStore(path)
    read_circuits = store._read_circuits
    calls: list[None] = []

    def change_after_query(connection):
        result = read_circuits(connection)
        calls.append(None)
        if len(calls) == 2:
            with path.open("r+b") as changed:
                changed.write(b"changed-in-place")
        return result

    monkeypatch.setattr(store, "_read_circuits", change_after_query)
    with pytest.raises(ProviderHealthError):
        store.provider_health_snapshot()
    assert len(calls) >= 2


@pytest.mark.parametrize("method", ["initialize", "reset", "acquire_probe", "resolve_probe"])
def test_calendar_maintenance_health_disallows_state_management(
    tmp_path: Path, method: str
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    store = CalendarMaintenanceHealthStore(initialized(tmp_path))
    with pytest.raises(ProviderHealthError):
        if method == "acquire_probe":
            store.acquire_probe(ProviderEndpoint.TRADE_DATES, owner="owner")
        elif method == "resolve_probe":
            store.resolve_probe("lease", owner="owner", success=True)
        else:
            getattr(store, method)()


def test_commit_failure_rolls_back_and_cleans_owned_write_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.app.market.calendar_maintenance_health import CalendarMaintenanceHealthStore

    path = initialized(tmp_path)
    before = tree_with_metadata(tmp_path)
    original_connect = sqlite3.connect

    class CommitFailureConnection:
        def __init__(self, connection):
            self._connection = connection

        @property
        def row_factory(self):
            return self._connection.row_factory

        @row_factory.setter
        def row_factory(self, value):
            self._connection.row_factory = value

        def __getattr__(self, name):
            return getattr(self._connection, name)

        def execute(self, sql, *args, **kwargs):
            if sql == "COMMIT":
                raise sqlite3.OperationalError("simulated commit failure")
            return self._connection.execute(sql, *args, **kwargs)

        def close(self):
            return self._connection.close()

    def connect_with_commit_failure(database, *args, **kwargs):
        connection = original_connect(database, *args, **kwargs)
        if "mode=rw" in str(database):
            return CommitFailureConnection(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect_with_commit_failure)
    with pytest.raises(ProviderHealthError):
        CalendarMaintenanceHealthStore(path).record_terminal_failure(
            "must-rollback", ProviderEndpoint.TRADE_DATES, NormalizedTransportError.EOF
        )

    assert tree_with_metadata(tmp_path) == before
    assert list(tmp_path.glob("*.maintenance-*")) == []
    assert list(tmp_path.glob("*.sqlite3-journal")) == []
