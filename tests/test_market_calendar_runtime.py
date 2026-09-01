from __future__ import annotations

import inspect
from datetime import UTC, date, datetime, timedelta

import pytest
from pydantic import ValidationError

from backend.app import config as config_module
from backend.app.config import (
    CalendarRuntimeSettings,
    Settings,
    get_calendar_runtime_settings,
)
from backend.app.market.calendar import (
    LiveTradingCalendar,
    TradingCalendar,
    build_live_trading_calendar,
    get_trading_calendar,
)
from backend.app.market.calendar_generation import (
    EMPTY_CALENDAR,
    CalendarGenerationStore,
    CalendarMachineDayV1,
    body_sha256,
    build_observation,
    build_schedule,
    build_source,
)
from backend.app.storage.layout import StorageLayout

RUNTIME_ENV_KEYS = (
    "STOCK_EVA_CALENDAR_RUNTIME_ENABLED",
    "STOCK_EVA_CALENDAR_GENERATION_DATABASE_NAME",
    "STOCK_EVA_CALENDAR_SYNC_DATABASE_NAME",
    "STOCK_EVA_LOCAL_CONTROL_DIR",
    "STOCK_EVA_LOCAL_LOCK_DIR",
    "STOCK_EVA_PROVIDER_HEALTH_DATABASE_NAME",
    "STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    "STOCK_EVA_AUTO_REFRESH_MIN_REQUEST_INTERVAL_SECONDS",
    "STOCK_EVA_PROVIDER_CIRCUIT_FAILURE_THRESHOLD",
    "STOCK_EVA_PROVIDER_CIRCUIT_COOLDOWN_SECONDS",
    "STOCK_EVA_PROVIDER_CIRCUIT_PROBE_LEASE_SECONDS",
)


def _promote_2027(path):
    now = datetime(2026, 12, 22, 1, tzinfo=UTC)
    bodies = (b"runtime-sse-2027", b"runtime-szse-2027")
    schedules = tuple(
        build_schedule(
            exchange=exchange,
            year=2027,
            coverage_start=date(2027, 1, 1),
            coverage_end=date(2027, 12, 31),
            title="Reviewed runtime calendar 2027",
            notice_no="runtime-2027",
            official_url=f"https://{host}/runtime-2027",
            published_on=date(2026, 12, 20),
            body_sha256=body_sha256(body),
            closed_dates=(date(2027, 1, 1),),
            review_id="runtime-independent-review",
            reviewed_on=datetime(2026, 12, 21, 1, tzinfo=UTC),
        )
        for exchange, host, body in zip(
            ("SSE", "SZSE"), ("www.sse.com.cn", "www.szse.cn"), bodies, strict=True
        )
    )
    source = build_source(
        schema_version=1,
        rule_version="cn-a-share-weekends-closed-v1",
        year=2027,
        schedules=schedules,
    )
    start, end = date(2027, 1, 1), date(2027, 12, 31)
    closed = set(source.schedules[0].closed_dates)
    days = tuple(
        CalendarMachineDayV1(
            date=start + timedelta(days=offset),
            is_open=(start + timedelta(days=offset)).weekday() < 5
            and start + timedelta(days=offset) not in closed,
        )
        for offset in range((end - start).days + 1)
    )
    machine = build_observation(
        provider="baostock",
        contract_version="r2f4.1-baostock-calendar-days-v1",
        range_start=start,
        range_end=end,
        observed_at=now + timedelta(minutes=30),
        days=days,
    )
    store = CalendarGenerationStore(path)
    assert store.stage_execute(source, now).outcome == "STAGED"
    attempt = store.reserve_attempt(source, now)
    result = store.promote(attempt, source, bodies, machine, now + timedelta(hours=1))
    assert result.outcome == "PROMOTED"
    return result.generation_sha256


def test_runtime_loader_reads_exact_allowlist_and_anchors_paths(tmp_path, monkeypatch):
    values = {
        "STOCK_EVA_CALENDAR_RUNTIME_ENABLED": "true",
        "STOCK_EVA_CALENDAR_GENERATION_DATABASE_NAME": "runtime.sqlite3",
        "STOCK_EVA_CALENDAR_SYNC_DATABASE_NAME": "sync.sqlite3",
        "STOCK_EVA_LOCAL_CONTROL_DIR": "control",
        "STOCK_EVA_LOCAL_LOCK_DIR": "locks",
        "STOCK_EVA_PROVIDER_HEALTH_DATABASE_NAME": "health.sqlite3",
        "STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS": "12.5",
        "STOCK_EVA_AUTO_REFRESH_MIN_REQUEST_INTERVAL_SECONDS": "0.25",
        "STOCK_EVA_PROVIDER_CIRCUIT_FAILURE_THRESHOLD": "4",
        "STOCK_EVA_PROVIDER_CIRCUIT_COOLDOWN_SECONDS": "600",
        "STOCK_EVA_PROVIDER_CIRCUIT_PROBE_LEASE_SECONDS": "90",
    }
    monkeypatch.chdir(tmp_path)
    projected = get_calendar_runtime_settings(environ=values)
    assert projected.calendar_runtime_enabled is True
    assert projected.local_control_dir == (tmp_path / "control").absolute()
    assert projected.local_lock_dir == (tmp_path / "locks").absolute()
    assert projected.provider_circuit_failure_threshold == 4


def test_default_factory_reads_only_exact_environment_allowlist(monkeypatch):
    class AuditedEnvironment:
        def __init__(self):
            self.calls = []

        def get(self, key, default=None):
            assert key in RUNTIME_ENV_KEYS
            assert key not in self.calls
            self.calls.append(key)
            return default

    audited = AuditedEnvironment()
    projected = get_calendar_runtime_settings(environ=audited)
    monkeypatch.setattr(
        config_module,
        "get_settings",
        lambda: (_ for _ in ()).throw(AssertionError("default factory loaded BaseSettings")),
    )
    monkeypatch.setattr(config_module, "get_calendar_runtime_settings", lambda: projected)

    calendar = get_trading_calendar()

    assert isinstance(calendar, LiveTradingCalendar)
    assert tuple(audited.calls) == RUNTIME_ENV_KEYS


def test_default_calendar_factory_has_no_dependency_configuration_parameter():
    assert tuple(inspect.signature(get_trading_calendar).parameters) == ()


def test_runtime_settings_reject_unsafe_or_duplicate_database_names():
    with pytest.raises(ValidationError):
        CalendarRuntimeSettings(calendar_generation_database_name="nested/runtime.sqlite3")
    with pytest.raises(ValidationError):
        CalendarRuntimeSettings(
            calendar_generation_database_name="CALENDAR.sqlite3",
            calendar_sync_database_name="calendar.SQLITE3",
        )


def test_settings_and_layout_expose_additive_generation_database(tmp_path):
    settings = Settings(
        _env_file=None,
        local_control_dir=tmp_path / "control",
        calendar_generation_database_name="generations.sqlite3",
    )
    assert StorageLayout(settings).calendar_generation_database == (
        tmp_path / "control" / "generations.sqlite3"
    )
    projected = build_live_trading_calendar(settings)
    assert isinstance(projected, LiveTradingCalendar)
    assert projected.settings.local_control_dir == (tmp_path / "control").absolute()


def test_concrete_snapshot_returns_identity():
    calendar = TradingCalendar.from_dict(
        {
            "year": 2026,
            "status": "confirmed",
            "published_on": "2025-12-01",
            "sources": [],
            "closed_dates": [],
        }
    )
    assert calendar.snapshot() is calendar


def test_disabled_live_calendar_never_reads_runtime_store(tmp_path, monkeypatch):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=False,
        local_control_dir=tmp_path / "control",
    )

    class ForbiddenStore:
        def __init__(self, _path):
            raise AssertionError("disabled runtime opened generation store")

    monkeypatch.setattr(
        "backend.app.market.calendar_generation.CalendarGenerationStore", ForbiddenStore
    )
    live = LiveTradingCalendar(settings)
    snapshot = live.snapshot()
    assert snapshot.status == "confirmed"
    assert snapshot is live.snapshot()
    assert not (tmp_path / "control").exists()


def test_enabled_missing_runtime_is_empty_and_does_not_initialize(tmp_path):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    snapshot = LiveTradingCalendar(settings).snapshot()
    assert snapshot is EMPTY_CALENDAR
    assert snapshot.status == "unavailable"
    assert not (tmp_path / "control").exists()


def test_enabled_empty_initialized_runtime_composes_bundled_calendar(tmp_path):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    path = tmp_path / "control" / settings.calendar_generation_database_name
    path.parent.mkdir()
    path.parent.chmod(0o700)
    store = CalendarGenerationStore(path)
    connection = store._connect()
    try:
        store._initialize(connection)
        store._assert_writer_identity(connection)
        connection.commit()
    finally:
        store._close_connection(connection)
    lock = path.with_name(path.name + ".lock")
    lock.touch(mode=0o600)
    assert LiveTradingCalendar(settings).snapshot().status == "confirmed"


def test_long_lived_calendar_sees_promotion_then_fails_closed_without_stale_fallback(tmp_path):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    path = settings.local_control_dir / settings.calendar_generation_database_name
    live = LiveTradingCalendar(settings)
    assert live.snapshot() is EMPTY_CALENDAR

    generation_sha256 = _promote_2027(path)
    promoted = live.snapshot()
    assert promoted.generation_sha256 == generation_sha256
    assert promoted.session_status(date(2027, 1, 1)) == "closed"
    assert promoted.session_status(date(2027, 1, 4)) == "open"

    path.write_bytes(b"corrupt after a valid operation")
    before = (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns)
    assert live.snapshot() is EMPTY_CALENDAR
    assert (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns) == before

    path.unlink()
    assert live.snapshot() is EMPTY_CALENDAR
    assert not path.exists()


def test_enabled_runtime_rejects_symlink_without_following_or_repairing(tmp_path):
    target = tmp_path / "target" / "calendar_generations.sqlite3"
    _promote_2027(target)
    control = tmp_path / "control"
    control.mkdir()
    linked = control / "calendar_generations.sqlite3"
    linked.symlink_to(target)
    before = linked.lstat()
    live = LiveTradingCalendar(
        CalendarRuntimeSettings(calendar_runtime_enabled=True, local_control_dir=control)
    )

    assert live.snapshot() is EMPTY_CALENDAR
    after = linked.lstat()
    assert (after.st_mode, after.st_size, after.st_mtime_ns) == (
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
    )
    assert linked.is_symlink()


def test_live_range_operation_pins_one_concrete_snapshot(monkeypatch):
    first = TradingCalendar.from_dict(
        {
            "year": 2027,
            "status": "confirmed",
            "published_on": "2026-12-20",
            "sources": [],
            "closed_dates": ["2027-01-01"],
        }
    )
    live = LiveTradingCalendar(CalendarRuntimeSettings())
    snapshots = iter((first, EMPTY_CALENDAR))
    calls = []

    def changing_snapshot():
        calls.append(None)
        return next(snapshots)

    monkeypatch.setattr(live, "snapshot", changing_snapshot)
    opened = live.confirmed_open_sessions(date(2027, 1, 1), date(2027, 1, 5))

    assert opened == (date(2027, 1, 4), date(2027, 1, 5))
    assert len(calls) == 1
