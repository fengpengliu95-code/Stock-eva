import asyncio
import json
import sqlite3
import sys
import threading
from contextlib import closing, contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import httpx
import pytest

import backend.app.cli as cli
import backend.app.market.calendar_sync as calendar_sync_module
from backend.app.api.market import get_calendar_sync_store, get_market_store
from backend.app.config import Settings
from backend.app.main import app
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.baostock_vendor import emit_terminal_observation
from backend.app.market.calendar import TradingCalendar, get_trading_calendar
from backend.app.market.calendar_sync import (
    CalendarSyncPlan,
    CalendarSyncPlanError,
    CalendarSyncPolicy,
    CalendarSyncResult,
    CalendarSyncService,
    CalendarSyncState,
    CalendarSyncStore,
    CalendarSyncStoreReadError,
    read_calendar_conflict,
    run_calendar_sync_loop,
)
from backend.app.market.provider_health import (
    CircuitState,
    InMemoryProviderHealthStore,
    ProviderHealthError,
    SQLiteProviderHealthStore,
)
from backend.app.market.provider_transport import (
    NormalizedTransportError,
    ProtocolStage,
    ProviderEndpoint,
    provider_session_scope,
    refresh_scope,
    request_scope,
)
from backend.app.market.store import MarketStore
from tests.test_baostock_provider import FIXTURE_PATH, FakeBaoStock, FakeResult


def test_calendar_sync_store_rejects_aliases_without_touching_target(tmp_path: Path) -> None:
    target = tmp_path / "target.sqlite3"
    target.write_bytes(b"owned bytes")
    before = target.read_bytes()
    symlink = tmp_path / "symlink.sqlite3"
    symlink.symlink_to(target)
    hardlink = tmp_path / "hardlink.sqlite3"
    hardlink.hardlink_to(target)

    for path in (symlink, hardlink):
        store = CalendarSyncStore(path, initialize=False)
        with pytest.raises(CalendarSyncStoreReadError):
            store.state()
        with pytest.raises(CalendarSyncStoreReadError):
            store.initialize_for_write()

    assert target.read_bytes() == before


def test_calendar_sync_execute_revalidates_plan_before_dependencies() -> None:
    class ForbiddenCalendar:
        def snapshot(self):
            raise AssertionError("calendar must not be read")

    service = CalendarSyncService(object(), ForbiddenCalendar(), object())
    invalid = CalendarSyncPlan.model_construct(
        mode="invalid",
        reason="test",
        requested_at=datetime(2026, 8, 3, tzinfo=UTC),
        range_start=date(2026, 8, 3),
        range_end=date(2026, 8, 3),
        authority_years=[],
        authority_checksum="safe",
        sources=[],
    )

    with pytest.raises(CalendarSyncPlanError, match="calendar sync plan is invalid"):
        service.execute(invalid)


def test_calendar_sync_execute_rejects_reverse_range_before_dependencies() -> None:
    class ForbiddenCalendar:
        def snapshot(self):
            raise AssertionError("calendar must not be read")

    service = CalendarSyncService(object(), ForbiddenCalendar(), object())
    invalid = CalendarSyncPlan.model_construct(
        mode="light",
        reason="test",
        requested_at=datetime(2026, 8, 3, tzinfo=UTC),
        range_start=date(2026, 8, 4),
        range_end=date(2026, 8, 3),
        authority_years=[2026],
        authority_checksum="a" * 64,
        sources=[],
    )

    with pytest.raises(CalendarSyncPlanError, match="calendar sync plan is invalid"):
        service.execute(invalid)


@pytest.mark.parametrize("suffix", ["-journal", "-wal", "-shm"])
def test_calendar_sync_store_rejects_existing_sidecar_without_mutation(
    tmp_path: Path, suffix: str
) -> None:
    path = tmp_path / "calendar.sqlite3"
    CalendarSyncStore(path)
    sidecar = path.with_name(path.name + suffix)
    sidecar.write_bytes(b"untrusted sidecar")
    before = sidecar.read_bytes()

    with pytest.raises(CalendarSyncStoreReadError):
        CalendarSyncStore(path, initialize=False).state()

    assert sidecar.read_bytes() == before


def test_calendar_sync_writer_does_not_create_database_beside_sidecar(tmp_path: Path) -> None:
    path = tmp_path / "calendar.sqlite3"
    sidecar = path.with_name(path.name + "-journal")
    sidecar.write_bytes(b"untrusted sidecar")

    with pytest.raises(CalendarSyncStoreReadError):
        CalendarSyncStore(path, initialize=False).initialize_for_write()

    assert not path.exists()
    assert sidecar.read_bytes() == b"untrusted sidecar"


def test_calendar_sync_read_helpers_do_not_create_missing_database(tmp_path: Path) -> None:
    path = tmp_path / "calendar.sqlite3"
    store = CalendarSyncStore(path, initialize=False)
    before = sorted(tmp_path.iterdir())

    with pytest.raises(CalendarSyncStoreReadError):
        store.run("missing")
    with pytest.raises(CalendarSyncStoreReadError):
        store.observations("missing")

    assert sorted(tmp_path.iterdir()) == before


def test_calendar_sync_missing_state_requires_safe_existing_parent(tmp_path: Path) -> None:
    safe = tmp_path / "safe"
    safe.mkdir(mode=0o700)
    assert (
        CalendarSyncStore(safe / "missing.sqlite3", initialize=False).state() == CalendarSyncState()
    )

    missing_parent = tmp_path / "missing-parent"
    with pytest.raises(CalendarSyncStoreReadError):
        CalendarSyncStore(missing_parent / "calendar.sqlite3", initialize=False).state()
    assert not missing_parent.exists()

    unsafe = tmp_path / "unsafe"
    unsafe.mkdir(mode=0o777)
    unsafe.chmod(0o777)
    with pytest.raises(CalendarSyncStoreReadError):
        CalendarSyncStore(unsafe / "calendar.sqlite3", initialize=False).state()
    assert list(unsafe.iterdir()) == []


def test_calendar_sync_missing_parent_exception_rejects_linked_ancestor(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    path = linked / "missing-parent" / "calendar.sqlite3"

    with pytest.raises(CalendarSyncStoreReadError):
        CalendarSyncStore(path, initialize=False, missing_parent_is_empty=True).state()

    assert not path.exists()


def test_calendar_sync_writer_rejects_path_swap_after_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "calendar.sqlite3"
    CalendarSyncStore(path)
    replacement = tmp_path / "replacement.sqlite3"
    CalendarSyncStore(replacement)
    validated = tmp_path / "validated.sqlite3"
    swapped = tmp_path / "swapped.sqlite3"
    real_connect = sqlite3.connect

    def swapping_connect(database, *args, **kwargs):
        if Path(database) == path:
            path.rename(validated)
            replacement.rename(path)
            connection = real_connect(database, *args, **kwargs)
            path.rename(swapped)
            validated.rename(path)
            return connection
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(calendar_sync_module.sqlite3, "connect", swapping_connect)

    with pytest.raises(CalendarSyncStoreReadError):
        CalendarSyncStore(path, initialize=False).initialize_for_write()

    assert path.stat().st_ino != swapped.stat().st_ino


def test_calendar_sync_store_serializes_concurrent_writer_identity_checks(tmp_path: Path) -> None:
    path = tmp_path / "calendar.sqlite3"
    store = CalendarSyncStore(path)
    barrier = threading.Barrier(16)
    failures: list[Exception] = []

    def connect_once() -> None:
        try:
            barrier.wait()
            with closing(store._connect()):
                pass
        except Exception as error:
            failures.append(error)

    workers = [threading.Thread(target=connect_once) for _ in range(16)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=5)

    assert not failures
    assert all(not worker.is_alive() for worker in workers)


@pytest.mark.parametrize("reader", [False, True])
def test_calendar_sync_connection_lock_releases_after_unexpected_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, reader: bool
) -> None:
    path = tmp_path / "calendar.sqlite3"
    store = CalendarSyncStore(path)
    real_connect = sqlite3.connect
    monkeypatch.setattr(
        calendar_sync_module.sqlite3,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("injected")),
    )

    with pytest.raises(RuntimeError, match="injected"):
        (store._connect_reader if reader else store._connect)()

    monkeypatch.setattr(calendar_sync_module.sqlite3, "connect", real_connect)
    failures: list[Exception] = []

    def reconnect() -> None:
        try:
            with closing(store._connect_reader() if reader else store._connect()):
                pass
        except Exception as error:
            failures.append(error)

    worker = threading.Thread(target=reconnect)
    worker.start()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert not failures


def test_calendar_loop_cancellation_drains_started_runtime_worker() -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocking_runtime():
        started.set()
        release.wait(timeout=5)
        finished.set()
        return SimpleNamespace(outcome="NOT_DUE")

    async def exercise() -> None:
        task = asyncio.create_task(
            run_calendar_sync_loop(
                object(),
                asyncio.Event(),
                clock=lambda: datetime(2026, 8, 3, tzinfo=UTC),
                runtime_maintenance=blocking_runtime,
            )
        )
        await asyncio.to_thread(started.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()

    asyncio.run(exercise())


SHANGHAI = ZoneInfo("Asia/Shanghai")


def synthetic_calendar() -> TradingCalendar:
    return TradingCalendar.from_dict(
        {
            "year": 2026,
            "status": "confirmed",
            "published_on": "2025-12-22",
            "sources": [
                {
                    "exchange": "SSE",
                    "title": "Synthetic 2026 closure notice",
                    "url": "https://example.test/sse-2026",
                    "notice_no": "SSE-2026",
                }
            ],
            "closed_dates": ["2026-01-01", "2026-02-16"],
        }
    )


class CalendarProvider:
    def __init__(self, sessions: list[date]) -> None:
        self.sessions = sessions
        self.calls: list[tuple[date, date]] = []

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        self.calls.append((start_date, end_date))
        return [item for item in self.sessions if start_date <= item <= end_date]


class ObservedCalendarProvider(CalendarProvider):
    @contextmanager
    def refresh_operation(self, refresh_id: str):
        with refresh_scope(refresh_id):
            yield

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        with provider_session_scope("calendar-session"):
            with request_scope(ProviderEndpoint.TRADE_DATES, attempt=1):
                emit_terminal_observation(
                    started_at=0,
                    protocol_stage=ProtocolStage.OPERATION,
                    recv_calls=1,
                    response_bytes=32,
                    end_marker_seen=True,
                    provider_code="0",
                    normalized_error=None,
                )
        return super().trading_dates(start_date, end_date)


class RecordingHealthStore(InMemoryProviderHealthStore):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.terminal_outcomes = []

    def record_endpoint_outcome(
        self,
        refresh_id,
        endpoint,
        *,
        success,
        normalized_error=None,
        observed_at=None,
    ):
        self.terminal_outcomes.append((refresh_id, endpoint, success, normalized_error))
        return super().record_endpoint_outcome(
            refresh_id,
            endpoint,
            success=success,
            normalized_error=normalized_error,
            observed_at=observed_at,
        )


class SnapshotCalendar:
    def __init__(self, snapshots) -> None:
        self.snapshots = list(snapshots)
        self.calls = 0

    def snapshot(self):
        self.calls += 1
        return self.snapshots.pop(0) if len(self.snapshots) > 1 else self.snapshots[0]


class CalendarSnapshot:
    def __init__(self, calendar, *, bundled_sha256=None, generation_sha256=None) -> None:
        self.configs = calendar.configs
        self._calendar = calendar
        self.bundled_sha256 = bundled_sha256
        self.generation_sha256 = generation_sha256

    def session_status(self, value):
        return self._calendar.session_status(value)


def _snapshot_calendar(*, bundled_sha256=None, generation_sha256=None):
    return CalendarSnapshot(
        synthetic_calendar(),
        bundled_sha256=bundled_sha256,
        generation_sha256=generation_sha256,
    )


def _calendar_control_counts(path: Path) -> tuple[int, int]:
    with sqlite3.connect(path) as connection:
        runs = connection.execute("SELECT COUNT(*) FROM calendar_sync_runs").fetchone()[0]
        state = connection.execute("SELECT COUNT(*) FROM calendar_sync_state").fetchone()[0]
    return runs, state


def test_calendar_sync_pins_one_snapshot_for_plan_and_one_for_execute(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = SnapshotCalendar([_snapshot_calendar()])
    service = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2026, 7, 24)]),
    )

    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2026, 7, 24),
        end_date=date(2026, 7, 24),
    )
    assert calendar.calls == 1
    result = service.execute(plan)

    assert result.status == "ready"
    assert calendar.calls == 2


def test_calendar_sync_stale_plan_fails_before_provider_health_or_state_writes(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = SnapshotCalendar(
        [
            _snapshot_calendar(generation_sha256="a" * 64),
            _snapshot_calendar(generation_sha256="b" * 64),
        ]
    )

    class UnexpectedProvider(CalendarProvider):
        def trading_dates(self, start_date, end_date):
            raise AssertionError("stale plan must not call provider")

    class UnexpectedHealth:
        def provider_health(self):
            raise AssertionError("stale plan must not inspect health")

    service = CalendarSyncService(
        store,
        calendar,
        UnexpectedProvider([]),
        health_store=UnexpectedHealth(),
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2026, 7, 24),
        end_date=date(2026, 7, 24),
    )
    before_bytes = store.path.read_bytes()
    before_state = store.state()

    result = service.execute(plan)

    assert result.status == "error"
    assert result.failure_code == "CALENDAR_AUTHORITY_CHANGED"
    assert store.path.read_bytes() == before_bytes
    assert store.state() == before_state
    with pytest.raises(KeyError):
        store.run(result.run_id)


def test_calendar_sync_stale_plan_does_not_initialize_missing_store(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "calendar.sqlite3"
    store = CalendarSyncStore(path, initialize=False)
    calendar = SnapshotCalendar(
        [
            _snapshot_calendar(generation_sha256="a" * 64),
            _snapshot_calendar(generation_sha256="b" * 64),
        ]
    )

    class UnexpectedProvider(CalendarProvider):
        def trading_dates(self, start_date, end_date):
            raise AssertionError("stale plan must not call provider")

    service = CalendarSyncService(store, calendar, UnexpectedProvider([]))
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2026, 7, 24),
        end_date=date(2026, 7, 24),
    )
    result = service.execute(plan)

    assert result.failure_code == "CALENDAR_AUTHORITY_CHANGED"
    assert not path.parent.exists()


def test_calendar_sync_checksum_binds_verified_generation_and_bundled_identities(
    tmp_path: Path,
) -> None:
    first = SnapshotCalendar(
        [_snapshot_calendar(bundled_sha256="1" * 64, generation_sha256="a" * 64)]
    )
    second = SnapshotCalendar(
        [_snapshot_calendar(bundled_sha256="1" * 64, generation_sha256="b" * 64)]
    )
    first_service = CalendarSyncService(CalendarSyncStore(tmp_path / "first.sqlite3"), first, None)
    second_service = CalendarSyncService(
        CalendarSyncStore(tmp_path / "second.sqlite3"), second, None
    )
    now = datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI)

    first_plan = first_service.plan(now=now, mode="light")
    second_plan = second_service.plan(now=now, mode="light")

    assert first_plan.authority_checksum != second_plan.authority_checksum


def test_calendar_sync_mixed_known_unknown_observation_is_observed_only(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        CalendarProvider([date(2026, 12, 31)]),
    )
    plan = service.plan(
        now=datetime(2027, 1, 4, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2026, 12, 31),
        end_date=date(2027, 1, 1),
    )

    result = service.execute(plan)

    assert result.status == "observed_only"
    assert result.conflicts == []


def test_calendar_sync_execute_keeps_one_snapshot_after_provider_changes_live_authority(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    first_snapshot = _snapshot_calendar(generation_sha256="a" * 64)
    later_snapshot = _snapshot_calendar(generation_sha256="b" * 64)

    class MutableLiveCalendar:
        def __init__(self) -> None:
            self.current = first_snapshot
            self.calls = 0

        def snapshot(self):
            self.calls += 1
            return self.current

    live = MutableLiveCalendar()

    class MutatingProvider(CalendarProvider):
        def trading_dates(self, start_date, end_date):
            live.current = later_snapshot
            return super().trading_dates(start_date, end_date)

    provider = MutatingProvider([date(2026, 12, 30), date(2026, 12, 31)])
    service = CalendarSyncService(
        store, live, provider, clock=lambda: datetime(2026, 12, 31, tzinfo=UTC)
    )
    plan = service.plan(
        now=datetime(2026, 12, 31, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2026, 12, 30),
        end_date=date(2026, 12, 31),
    )

    result = service.execute(plan)
    assert result.status == "ready"
    assert live.calls == 2
    assert [item.official_status for item in store.observations(result.run_id)] == [
        "open",
        "open",
    ]
    with sqlite3.connect(store.path) as connection:
        payload = connection.execute(
            "SELECT payload_json FROM calendar_authority_versions WHERE checksum = ?",
            (plan.authority_checksum,),
        ).fetchone()[0]
    assert json.loads(payload)["generation_sha256"] == "a" * 64

    stale = service.execute(plan)
    assert stale.status == "error"
    assert stale.failure_code == "CALENDAR_AUTHORITY_CHANGED"
    assert live.calls == 3


def test_mixed_unknown_is_observed_only_without_replacing_last_good_and_conflict_wins(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = synthetic_calendar()
    good_service = CalendarSyncService(store, calendar, CalendarProvider([date(2026, 12, 31)]))
    good_plan = good_service.plan(
        now=datetime(2026, 12, 31, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2026, 12, 31),
        end_date=date(2026, 12, 31),
    )
    good = good_service.execute(good_plan)
    last_good_run_id = store.state().last_good_run_id

    mixed_service = CalendarSyncService(store, calendar, CalendarProvider([date(2026, 12, 31)]))
    mixed_plan = mixed_service.plan(
        now=datetime(2027, 1, 1, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2026, 12, 31),
        end_date=date(2027, 1, 1),
    )
    mixed = mixed_service.execute(mixed_plan)
    assert mixed.status == "observed_only"
    assert store.state().last_good_run_id == last_good_run_id == good.run_id

    conflict_service = CalendarSyncService(store, calendar, CalendarProvider([]))
    conflict = conflict_service.execute(
        conflict_service.plan(
            now=datetime(2027, 1, 1, 16, 30, tzinfo=SHANGHAI),
            mode="light",
            start_date=date(2026, 12, 31),
            end_date=date(2027, 1, 1),
        )
    )
    assert conflict.status == "quarantined"
    assert store.state().last_good_run_id == last_good_run_id


def test_calendar_sync_result_failure_code_is_strict_and_ordinary_results_serialize_null() -> None:
    fields = {
        "run_id": "run",
        "mode": "light",
        "status": "ready",
        "range_start": date(2026, 7, 24),
        "range_end": date(2026, 7, 24),
        "fetched_at": datetime(2026, 7, 24, tzinfo=UTC),
        "completed_at": datetime(2026, 7, 24, tzinfo=UTC),
        "observed_open_count": 1,
        "authority_checksum": "a" * 64,
    }
    ordinary = CalendarSyncResult(**fields)
    assert ordinary.failure_code is None
    assert '"failure_code":null' in ordinary.model_dump_json()

    with pytest.raises(ValueError):
        CalendarSyncResult(**fields, failure_code="NOT_A_FAILURE")
    with pytest.raises(ValueError):
        CalendarSyncResult(**fields, failure_code="CALENDAR_AUTHORITY_CHANGED")


def test_calendar_sync_schedule_has_monthly_full_startup_and_1630_light() -> None:
    policy = CalendarSyncPolicy()

    monthly = policy.decide(
        datetime(2026, 8, 1, 4, 5, tzinfo=SHANGHAI),
        state=None,
    )
    startup = policy.decide(
        datetime(2026, 8, 3, 9, 0, tzinfo=SHANGHAI),
        state=None,
        startup=True,
    )
    daily = policy.decide(
        datetime(2026, 8, 3, 16, 30, tzinfo=SHANGHAI),
        state=None,
    )

    assert monthly.action == "full"
    assert monthly.reason == "monthly_reconciliation"
    assert startup.action == "light"
    assert startup.reason == "startup_check"
    assert daily.action == "light"
    assert daily.reason == "daily_1630_check"


def test_bundled_calendar_supports_cross_year_strategy_history() -> None:
    calendar = get_trading_calendar()

    assert calendar.session_status(date(2025, 1, 1)) == "closed"
    assert calendar.session_status(date(2025, 1, 2)) == "open"
    assert calendar.session_status(date(2025, 10, 8)) == "closed"
    assert calendar.previous_session(date(2026, 1, 5)) == date(2025, 12, 31)
    assert calendar.latest_expected_session(datetime(2026, 1, 5, 18, 9, tzinfo=SHANGHAI)) == date(
        2025, 12, 31
    )

    current = date(2025, 1, 1)
    end = date(2026, 7, 24)
    confirmed_open = 0
    while current <= end:
        confirmed_open += calendar.session_status(current) == "open"
        current += timedelta(days=1)
    assert confirmed_open >= 260


def test_read_calendar_conflict_is_dynamic_select_only_and_fail_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control" / "calendar.sqlite3"
    CalendarSyncStore(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO calendar_sync_state (singleton, payload_json) VALUES (1, ?)",
            ('{"conflict_detected": false}',),
        )

    assert read_calendar_conflict(path) is False

    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT OR REPLACE INTO calendar_sync_state (singleton, payload_json) VALUES (1, ?)",
            ('{"conflict_detected": true}',),
        )
    assert read_calendar_conflict(path) is True

    with pytest.raises(CalendarSyncStoreReadError):
        read_calendar_conflict(tmp_path / "missing.sqlite3")

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE calendar_sync_state")
    with pytest.raises(CalendarSyncStoreReadError):
        read_calendar_conflict(path)


def test_calendar_sync_schedule_does_not_repeat_completed_slots(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = CalendarProvider([date(2026, 8, 3)])
    service = CalendarSyncService(store, synthetic_calendar(), provider)
    now = datetime(2026, 8, 3, 16, 30, tzinfo=SHANGHAI)
    result = service.execute(service.plan(now=now, mode="light"))

    decision = CalendarSyncPolicy().decide(now, state=store.state())

    assert result.status == "ready"
    assert decision.action == "none"
    assert decision.next_sync_at == datetime(2026, 8, 4, 16, 30, tzinfo=SHANGHAI)


def test_successful_sync_versions_observations_and_notice_metadata(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = CalendarProvider([date(2026, 7, 23), date(2026, 7, 24)])
    service = CalendarSyncService(store, synthetic_calendar(), provider)
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="full",
        start_date=date(2026, 7, 23),
        end_date=date(2026, 7, 24),
    )

    result = service.execute(plan)
    state = store.state()
    run = store.run(result.run_id)
    observations = store.observations(result.run_id)

    assert result.status == "ready"
    assert state.last_good_run_id == result.run_id
    assert state.last_full_sync_at == result.completed_at
    assert state.conflict_detected is False
    assert [item.session_date for item in observations] == [
        date(2026, 7, 23),
        date(2026, 7, 24),
    ]
    assert all(item.provider_is_open for item in observations)
    assert run.authority_checksum
    assert run.sources[0]["url"] == "https://example.test/sse-2026"
    assert run.sources[0]["notice_no"] == "SSE-2026"
    assert run.range_start == date(2026, 7, 23)
    assert run.range_end == date(2026, 7, 24)


def test_cross_year_sync_uses_both_confirmed_authority_versions(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = get_trading_calendar()
    service = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2025, 12, 31), date(2026, 1, 5)]),
    )
    plan = service.plan(
        now=datetime(2026, 1, 5, 16, 30, tzinfo=SHANGHAI),
        mode="full",
        start_date=date(2025, 12, 31),
        end_date=date(2026, 1, 5),
    )

    result = service.execute(plan)

    assert result.status == "ready"
    assert plan.authority_years == [2025, 2026]
    assert {
        item.session_date: item.official_status for item in store.observations(result.run_id)
    } == {
        date(2025, 12, 31): "open",
        date(2026, 1, 1): "closed",
        date(2026, 1, 2): "closed",
        date(2026, 1, 3): "closed",
        date(2026, 1, 4): "closed",
        date(2026, 1, 5): "open",
    }


def test_conflict_is_quarantined_without_replacing_last_known_good(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = synthetic_calendar()
    good = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2026, 7, 23), date(2026, 7, 24)]),
    )
    range_kwargs = {
        "now": datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        "mode": "full",
        "start_date": date(2026, 7, 23),
        "end_date": date(2026, 7, 24),
    }
    good_result = good.execute(good.plan(**range_kwargs))

    conflicting = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2026, 7, 23)]),
    )
    conflict_result = conflicting.execute(conflicting.plan(**range_kwargs))
    state = store.state()

    assert conflict_result.status == "quarantined"
    assert conflict_result.conflicts == [
        {
            "date": "2026-07-24",
            "official": "open",
            "provider": "closed",
        }
    ]
    assert state.last_good_run_id == good_result.run_id
    assert state.conflict_detected is True
    assert state.conflict_run_id == conflict_result.run_id
    assert store.run(conflict_result.run_id).status == "quarantined"


def test_unknown_year_is_observed_but_never_promoted_to_authority(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        CalendarProvider([date(2027, 1, 4)]),
    )
    plan = service.plan(
        now=datetime(2027, 1, 4, 16, 30, tzinfo=SHANGHAI),
        mode="light",
        start_date=date(2027, 1, 4),
        end_date=date(2027, 1, 4),
    )

    result = service.execute(plan)
    observation = store.observations(result.run_id)[0]

    assert result.status == "observed_only"
    assert observation.official_status == "unknown"
    assert store.state().last_good_run_id is None
    assert synthetic_calendar().session_status(date(2027, 1, 4)) == "unknown"


def test_provider_failure_is_audited_once_and_preserves_last_known_good(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    calendar = synthetic_calendar()
    good = CalendarSyncService(
        store,
        calendar,
        CalendarProvider([date(2026, 7, 24)]),
    )
    good_result = good.execute(
        good.plan(
            now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
            mode="light",
            start_date=date(2026, 7, 24),
            end_date=date(2026, 7, 24),
        )
    )

    class FailingProvider:
        def trading_dates(self, start_date, end_date):
            raise OSError("synthetic transport failure")

    failing = CalendarSyncService(store, calendar, FailingProvider())
    failed = failing.execute(
        failing.plan(
            now=datetime(2026, 7, 25, 16, 30, tzinfo=SHANGHAI),
            mode="light",
            start_date=date(2026, 7, 25),
            end_date=date(2026, 7, 25),
        )
    )

    assert failed.status == "error"
    assert store.run(failed.run_id).status == "error"
    assert store.state().last_good_run_id == good_result.run_id
    assert (
        CalendarSyncPolicy()
        .decide(
            datetime(2026, 7, 25, 16, 31, tzinfo=SHANGHAI),
            state=store.state(),
        )
        .action
        == "none"
    )


def test_calendar_execute_is_skipped_while_provider_circuit_is_not_closed(
    tmp_path: Path,
) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = ObservedCalendarProvider([date(2026, 7, 24)])
    health = InMemoryProviderHealthStore(failure_threshold=1)
    health.record_terminal_failure(
        "open-calendar",
        ProviderEndpoint.TRADE_DATES,
        NormalizedTransportError.RECV_TIMEOUT,
    )
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "skipped_circuit_open"
    assert provider.calls == []
    assert store.state().last_attempt_at is None


def test_calendar_terminal_observation_updates_breaker_before_calendar_state_save(
    tmp_path: Path,
) -> None:
    health = InMemoryProviderHealthStore(failure_threshold=1)
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = ObservedCalendarProvider([date(2026, 7, 24)])
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    assert service.execute(plan).status == "ready"
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).consecutive_failures == 0
    assert len(health.list_observations()) == 1

    class FailingObservedProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date, end_date):
            with provider_session_scope("failed-calendar-session"):
                with request_scope(ProviderEndpoint.TRADE_DATES, attempt=2):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=ProtocolStage.OPERATION,
                        recv_calls=1,
                        response_bytes=0,
                        end_marker_seen=False,
                        provider_code=None,
                        normalized_error=NormalizedTransportError.RECV_TIMEOUT,
                    )
            raise OSError("private-token raw calendar failure")

    failed_store = CalendarSyncStore(tmp_path / "failed-calendar.sqlite3")
    failed_service = CalendarSyncService(
        failed_store,
        synthetic_calendar(),
        FailingObservedProvider([]),
        health_store=health,
    )
    failed = failed_service.execute(
        failed_service.plan(
            now=datetime(2026, 7, 25, 16, 30, tzinfo=SHANGHAI),
            mode="light",
        )
    )
    assert failed.status == "error"
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert failed_store.run(failed.run_id).status == "error"


def test_calendar_complete_success_evidence_records_terminal_success(tmp_path: Path) -> None:
    health = RecordingHealthStore(failure_threshold=1)
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        ObservedCalendarProvider([date(2026, 7, 24)]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "ready"
    assert health.terminal_outcomes == [(result.run_id, ProviderEndpoint.TRADE_DATES, True, None)]
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.CLOSED


@pytest.mark.parametrize("malformation", ["invalid-date", "missing-schema"])
def test_real_provider_calendar_high_level_failure_trips_circuit_without_state_write(
    tmp_path: Path,
    malformation: str,
) -> None:
    class MalformedCalendarClient(FakeBaoStock):
        def query_trade_dates(self, **_kwargs):
            if malformation == "missing-schema":
                return FakeResult(
                    ["is_trading_day"],
                    [["1"]],
                )
            return FakeResult(
                ["calendar_date", "is_trading_day"],
                [["private-token-not-a-date", "1"]],
            )

    health = RecordingHealthStore(failure_threshold=1)
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = BaoStockProvider(
        client=MalformedCalendarClient(json.loads(FIXTURE_PATH.read_text())),
        max_attempts=1,
        min_request_interval_seconds=0,
    )
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )
    before_bytes = store.path.read_bytes()
    before_state = store.state()
    before_counts = _calendar_control_counts(store.path)

    result = service.execute(plan)

    assert result.status == "error"
    observations = health.list_observations()
    assert len(observations) == 1
    assert observations[0].protocol_stage == ProtocolStage.OPERATION
    assert observations[0].normalized_error is None
    assert health.terminal_outcomes == [
        (
            result.run_id,
            ProviderEndpoint.TRADE_DATES,
            False,
            NormalizedTransportError.PROTOCOL_ERROR,
        )
    ]
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert health.provider_health().state == CircuitState.OPEN
    assert store.path.read_bytes() == before_bytes
    assert store.state() == before_state
    assert _calendar_control_counts(store.path) == before_counts
    with pytest.raises(KeyError):
        store.run(result.run_id)
    assert "private-token" not in result.model_dump_json()


@pytest.mark.parametrize(
    "invalid_evidence",
    ["second-session", "attempt-two", "multiple-operation"],
)
def test_calendar_rejects_inconsistent_success_evidence_without_state_write(
    tmp_path: Path,
    invalid_evidence: str,
) -> None:
    class InvalidEvidenceProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date: date, end_date: date) -> list[date]:
            stage = (
                ProtocolStage.OPERATION
                if invalid_evidence == "multiple-operation"
                else ProtocolStage.COMPLETE
            )
            session = (
                "extra-session" if invalid_evidence == "second-session" else "calendar-session"
            )
            with provider_session_scope(session):
                with request_scope(
                    ProviderEndpoint.TRADE_DATES,
                    attempt=2 if invalid_evidence == "attempt-two" else 1,
                ):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=stage,
                        recv_calls=1,
                        response_bytes=32,
                        end_marker_seen=True,
                        provider_code="0",
                        normalized_error=None,
                    )
            return super().trading_dates(start_date, end_date)

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    before_bytes = store.path.read_bytes()
    before_state = store.state()
    before_counts = _calendar_control_counts(store.path)
    health = RecordingHealthStore(failure_threshold=1)
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        InvalidEvidenceProvider([date(2026, 7, 24)]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "error"
    assert health.terminal_outcomes == [
        (
            result.run_id,
            ProviderEndpoint.TRADE_DATES,
            False,
            NormalizedTransportError.PROTOCOL_ERROR,
        )
    ]
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert health.provider_health().state == CircuitState.OPEN
    assert store.path.read_bytes() == before_bytes
    assert store.state() == before_state
    assert _calendar_control_counts(store.path) == before_counts


def test_calendar_health_audit_failure_prevents_calendar_state_write(tmp_path: Path) -> None:
    class FailingAudit(InMemoryProviderHealthStore):
        def record_observation(self, observation) -> None:
            del observation
            raise ProviderHealthError("token=/private/provider-health.sqlite3")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        ObservedCalendarProvider([date(2026, 7, 24)]),
        health_store=FailingAudit(),
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    with pytest.raises(ProviderHealthError):
        service.execute(plan)

    assert store.state().last_attempt_at is None


def test_calendar_unclassified_failure_resolves_breaker_without_state_write(
    tmp_path: Path,
) -> None:
    class UnclassifiedProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date, end_date):
            del start_date, end_date
            with provider_session_scope("runtime-calendar-session"):
                with request_scope(ProviderEndpoint.TRADE_DATES, attempt=1):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=ProtocolStage.OPERATION,
                        recv_calls=1,
                        response_bytes=32,
                        end_marker_seen=True,
                        provider_code="0",
                        normalized_error=None,
                    )
            raise RuntimeError("private-token /private/calendar provider failure")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    health = RecordingHealthStore(failure_threshold=1)
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        UnclassifiedProvider([]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    result = service.execute(plan)

    assert result.status == "error"
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert health.provider_health().state == CircuitState.OPEN
    assert health.terminal_outcomes == [
        (
            result.run_id,
            ProviderEndpoint.TRADE_DATES,
            False,
            NormalizedTransportError.PROTOCOL_ERROR,
        )
    ]
    assert store.state().last_attempt_at is None
    with pytest.raises(KeyError):
        store.run(result.run_id)
    serialized = result.model_dump_json()
    assert "private-token" not in serialized
    assert "/private/calendar" not in serialized


def test_calendar_unclassified_zero_observation_failure_does_not_kill_loop(
    tmp_path: Path,
) -> None:
    class ZeroObservationFailure(CalendarProvider):
        def trading_dates(self, start_date, end_date):
            self.calls.append((start_date, end_date))
            raise AssertionError("private-token zero-observation failure")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    health = InMemoryProviderHealthStore(failure_threshold=1)
    provider = ZeroObservationFailure([])
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        provider,
        clock=lambda: datetime(2026, 7, 24, 8, tzinfo=UTC),
        health_store=health,
    )
    stop = asyncio.Event()
    clock_calls = 0

    def loop_clock() -> datetime:
        nonlocal clock_calls
        clock_calls += 1
        if clock_calls >= 2:
            stop.set()
        return datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI)

    asyncio.run(
        run_calendar_sync_loop(
            service,
            stop,
            clock=loop_clock,
            poll_seconds=0.001,
        )
    )

    assert len(provider.calls) == 1
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN
    assert store.state().last_attempt_at is None


def test_calendar_unclassified_failure_propagates_health_outcome_write_error(
    tmp_path: Path,
) -> None:
    class FailingOutcome(InMemoryProviderHealthStore):
        def record_terminal_failure(self, *args, **kwargs):
            del args, kwargs
            raise ProviderHealthError("token=/private/provider-health.sqlite3")

    class UnclassifiedProvider(ObservedCalendarProvider):
        def trading_dates(self, start_date, end_date):
            del start_date, end_date
            with provider_session_scope("runtime-calendar-session"):
                with request_scope(ProviderEndpoint.TRADE_DATES, attempt=1):
                    emit_terminal_observation(
                        started_at=0,
                        protocol_stage=ProtocolStage.OPERATION,
                        recv_calls=0,
                        response_bytes=0,
                        end_marker_seen=False,
                        provider_code=None,
                        normalized_error=NormalizedTransportError.PROTOCOL_ERROR,
                    )
            raise RuntimeError("private-token provider failure")

    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        UnclassifiedProvider([]),
        health_store=FailingOutcome(failure_threshold=1),
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    with pytest.raises(ProviderHealthError):
        service.execute(plan)

    assert store.state().last_attempt_at is None


def test_calendar_success_without_operation_observation_fails_closed(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    health = InMemoryProviderHealthStore(failure_threshold=1)
    service = CalendarSyncService(
        store,
        synthetic_calendar(),
        CalendarProvider([date(2026, 7, 24)]),
        health_store=health,
    )
    plan = service.plan(
        now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        mode="light",
    )

    with pytest.raises(ProviderHealthError, match="audit is incomplete"):
        service.execute(plan)

    assert store.state().last_attempt_at is None
    assert health.endpoint_health(ProviderEndpoint.TRADE_DATES).state == CircuitState.OPEN


def test_calendar_loop_checks_startup_then_daily_slot(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    provider = CalendarProvider([date(2026, 7, 24)])
    service = CalendarSyncService(store, synthetic_calendar(), provider)
    stop = asyncio.Event()
    moments = iter(
        [
            datetime(2026, 7, 24, 9, 0, tzinfo=SHANGHAI),
            datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
        ]
    )

    async def exercise() -> None:
        calls = 0

        def clock() -> datetime:
            nonlocal calls
            calls += 1
            if calls >= 2:
                stop.set()
            return next(moments)

        await run_calendar_sync_loop(
            service,
            stop,
            clock=clock,
            poll_seconds=0.001,
        )

    asyncio.run(exercise())

    assert len(provider.calls) == 2
    assert store.state().last_startup_slot_date == date(2026, 7, 24)
    assert store.state().last_light_slot_date == date(2026, 7, 24)


def test_calendar_sync_plan_fails_closed_for_unavailable_calendar(tmp_path: Path) -> None:
    class NoCallProvider:
        def trading_dates(self, *_args) -> list[date]:
            raise AssertionError("unavailable calendar must not reach provider")

    class NoControlRead:
        def state(self):
            raise AssertionError("unavailable calendar must not read legacy control")

    path = tmp_path / "calendar.sqlite3"
    service = CalendarSyncService(
        NoControlRead(),
        TradingCalendar([]),
        NoCallProvider(),
    )

    assert (
        service.plan(
            now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
            mode="auto",
            startup=True,
        )
        is None
    )
    assert not path.exists()


def test_calendar_loop_invokes_one_runtime_maintenance_callback_per_poll(tmp_path: Path) -> None:
    store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(store, synthetic_calendar(), CalendarProvider([]))
    stop = asyncio.Event()
    calls: list[int] = []

    async def exercise() -> None:
        def clock() -> datetime:
            stop.set()
            return datetime(2026, 7, 24, 9, 0, tzinfo=SHANGHAI)

        def runtime():
            calls.append(1)
            return SimpleNamespace(outcome="NOT_DUE")

        await run_calendar_sync_loop(
            service,
            stop,
            clock=clock,
            runtime_maintenance=runtime,
            poll_seconds=0.001,
        )

    asyncio.run(exercise())
    assert calls == [1]


def test_calendar_loop_runtime_failure_does_not_kill_or_fall_through_to_legacy_provider() -> None:
    stop = asyncio.Event()
    calls: list[str] = []

    class NoLegacyPlan:
        def plan(self, **_kwargs):
            calls.append("legacy-plan")
            raise AssertionError("runtime failure must block legacy planning")

    def broken_runtime() -> None:
        calls.append("runtime")
        stop.set()
        raise RuntimeError("private provider detail")

    async def exercise() -> None:
        await run_calendar_sync_loop(
            NoLegacyPlan(),
            stop,
            clock=lambda: (_ for _ in ()).throw(AssertionError("clock must not run")),
            runtime_maintenance=broken_runtime,
            poll_seconds=0.001,
        )

    asyncio.run(exercise())
    assert calls == ["runtime"]


@pytest.mark.parametrize("runtime_result", [None, SimpleNamespace(outcome="UNKNOWN")])
def test_calendar_loop_unprovable_runtime_result_blocks_legacy_plan(runtime_result) -> None:
    stop = asyncio.Event()

    class NoLegacyPlan:
        def plan(self, **_kwargs):
            raise AssertionError("unprovable runtime result must block legacy planning")

    def runtime():
        stop.set()
        return runtime_result

    asyncio.run(
        run_calendar_sync_loop(
            NoLegacyPlan(),
            stop,
            clock=lambda: (_ for _ in ()).throw(AssertionError("clock must not run")),
            runtime_maintenance=runtime,
            poll_seconds=0.001,
        )
    )


def test_calendar_loop_retries_after_runtime_exception_on_next_poll() -> None:
    stop = asyncio.Event()
    events: list[str] = []

    class LegacyPlan:
        def plan(self, **_kwargs):
            events.append("legacy-plan")
            stop.set()
            return None

    runtime_calls = 0

    def runtime():
        nonlocal runtime_calls
        runtime_calls += 1
        events.append(f"runtime-{runtime_calls}")
        if runtime_calls == 1:
            raise RuntimeError("private provider detail")
        return SimpleNamespace(outcome="NOT_DUE")

    asyncio.run(
        run_calendar_sync_loop(
            LegacyPlan(),
            stop,
            clock=lambda: datetime(2026, 7, 24, 9, 0, tzinfo=SHANGHAI),
            runtime_maintenance=runtime,
            poll_seconds=0.001,
        )
    )
    assert events == ["runtime-1", "runtime-2", "legacy-plan"]


def test_calendar_sync_cli_defaults_to_network_free_plan(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-sync"])
    control = tmp_path / "control"
    control.mkdir(mode=0o700)
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=control,
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
    )
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    monkeypatch.setattr(cli, "get_settings", lambda: settings)

    def unexpected_provider(*args, **kwargs):
        raise AssertionError("dry-run calendar sync must not construct provider")

    monkeypatch.setattr(cli, "BaoStockProvider", unexpected_provider)

    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry-run"
    assert payload["writes_calendar_state"] is False
    assert payload["network_requests"] == 0
    assert payload["execute_requires"] == "--execute"
    assert not (settings.local_control_dir / settings.calendar_sync_database_name).exists()
    assert not (settings.local_control_dir / settings.provider_health_database_name).exists()
    assert sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*")) == before


@pytest.mark.parametrize("mode", ["light", "auto"])
def test_calendar_sync_cli_rejects_reverse_range_before_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    mode: str,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "calendar-sync",
            "--mode",
            mode,
            "--start",
            "2026-08-04",
            "--end",
            "2026-08-03",
            "--execute",
        ],
    )

    def forbidden_settings():
        raise AssertionError("invalid range must be rejected before dependencies")

    monkeypatch.setattr(cli, "get_settings", forbidden_settings)
    before = sorted(tmp_path.rglob("*"))

    assert cli.main() == 2
    assert json.loads(capsys.readouterr().out) == {
        "status": "error",
        "error_code": "INVALID_CALENDAR_SYNC_RANGE",
        "network_requests": 0,
        "writes_calendar_state": False,
        "canonical_writes": False,
    }
    assert sorted(tmp_path.rglob("*")) == before


def test_calendar_sync_cli_execute_uses_persistent_health_gate_before_provider_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=None,
        local_market_dataset_root=None,
        provider_circuit_failure_threshold=1,
    )
    health = SQLiteProviderHealthStore(
        settings.local_control_dir / settings.provider_health_database_name,
        failure_threshold=1,
        cooldown_seconds=settings.provider_circuit_cooldown_seconds,
        probe_lease_seconds=settings.provider_circuit_probe_lease_seconds,
    )
    health.initialize()
    health.record_terminal_failure(
        "open-cli-calendar",
        ProviderEndpoint.TRADE_DATES,
        NormalizedTransportError.RECV_TIMEOUT,
    )
    calls = 0

    class NoCallProvider:
        def __init__(self, **_kwargs) -> None:
            pass

        def trading_dates(self, *_args) -> list[date]:
            nonlocal calls
            calls += 1
            raise AssertionError("open circuit must skip calendar provider")

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "BaoStockProvider", NoCallProvider)
    monkeypatch.setattr(
        cli,
        "get_market_clock",
        lambda: lambda: datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "calendar-sync",
            "--mode",
            "light",
            "--start",
            "2026-07-24",
            "--end",
            "2026-07-24",
            "--execute",
        ],
    )

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "skipped_circuit_open"
    assert payload["writes_calendar_state"] is False
    assert calls == 0


def test_calendar_sync_cli_revalidates_result_before_publication(monkeypatch, capsys, tmp_path):
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=None,
        local_market_dataset_root=None,
    )
    moment = datetime(2026, 7, 24, 8, tzinfo=UTC)
    valid = CalendarSyncResult(
        run_id="safe-run",
        mode="light",
        status="ready",
        range_start=date(2026, 7, 24),
        range_end=date(2026, 7, 24),
        fetched_at=moment,
        completed_at=moment,
        observed_open_count=1,
        authority_checksum="a" * 64,
    )
    malicious = valid.model_copy(update={"failure_code": "CALENDAR_AUTHORITY_CHANGED"})

    class FakeStore:
        def __init__(self, _path, *, initialize):
            pass

    class FakeService:
        def __init__(self, *_args, **_kwargs):
            pass

        def plan(self, **_kwargs):
            return SimpleNamespace(mode="light", model_dump=lambda **_dump: {})

        def execute(self, _plan):
            return malicious

    class FakeHealth:
        def __init__(self, *_args, **_kwargs):
            pass

        def initialize(self):
            pass

    class FakeProvider:
        def __init__(self, **_kwargs):
            pass

    class FakeLock:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class ReadyPreflight:
        def __init__(self, _settings):
            pass

        def inspect(self):
            return SimpleNamespace(market_data_available=True, mode="local")

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr(cli, "CalendarSyncStore", FakeStore)
    monkeypatch.setattr(cli, "CalendarSyncService", FakeService)
    monkeypatch.setattr(cli, "SQLiteProviderHealthStore", FakeHealth)
    monkeypatch.setattr(cli, "BaoStockProvider", FakeProvider)
    monkeypatch.setattr(cli, "RefreshRunLock", FakeLock)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-sync", "--execute"])

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "status": "error",
        "error_code": "INVALID_CALENDAR_SYNC_RESULT",
        "network_requests": None,
        "writes_calendar_state": False,
        "canonical_writes": False,
    }


def test_market_status_exposes_calendar_maintenance_without_client_input(
    tmp_path: Path,
) -> None:
    market_store = MarketStore(tmp_path / "market.duckdb")
    sync_store = CalendarSyncStore(tmp_path / "calendar.sqlite3")
    service = CalendarSyncService(
        sync_store,
        synthetic_calendar(),
        CalendarProvider([date(2026, 7, 23)]),
        clock=lambda: datetime(2026, 7, 24, 8, tzinfo=UTC),
    )
    result = service.execute(
        service.plan(
            now=datetime(2026, 7, 24, 16, 30, tzinfo=SHANGHAI),
            mode="light",
            start_date=date(2026, 7, 24),
            end_date=date(2026, 7, 24),
        )
    )
    app.dependency_overrides[get_market_store] = lambda: market_store
    app.dependency_overrides[get_calendar_sync_store] = lambda: sync_store

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            return await client.get("/api/v1/market/status")

    try:
        response = asyncio.run(send())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["calendar_status"] == "conflict"
    assert (
        datetime.fromisoformat(payload["calendar_last_sync_at"].replace("Z", "+00:00"))
        == result.completed_at
    )
    assert payload["calendar_conflict_detected"] is True
    assert (
        datetime.fromisoformat(payload["calendar_conflict_at"].replace("Z", "+00:00"))
        == result.completed_at
    )
    assert payload["calendar_next_sync_at"] is not None
