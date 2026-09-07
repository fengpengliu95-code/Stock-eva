from __future__ import annotations

import asyncio
import inspect
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError

from backend.app import cli as cli_module
from backend.app import config as config_module
from backend.app.api import market as market_api
from backend.app.config import (
    CalendarRuntimeSettings,
    Settings,
    get_calendar_runtime_settings,
)
from backend.app.main import app
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
from backend.app.market.calendar_runtime import (
    CalendarGenerationStatusV1,
    _last_outcome,
    _latest_candidate_conflict,
    build_calendar_generation_status,
)
from backend.app.market.provider_health import SQLiteProviderHealthStore
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


def _write_source_package(directory: Path, now: datetime, *, conflict: bool = False) -> Path:
    bodies = (b"source-sse-2027", b"source-szse-2027")
    closed = ((date(2027, 1, 1),), (date(2027, 1, 4),)) if conflict else ((date(2027, 1, 1),),) * 2
    schedules = tuple(
        build_schedule(
            exchange=exchange,
            year=2027,
            coverage_start=date(2027, 1, 1),
            coverage_end=date(2027, 12, 31),
            title="Source package calendar",
            notice_no="source-2027",
            official_url=f"https://{host}/source-2027",
            published_on=date(2026, 8, 1),
            body_sha256=body_sha256(body),
            closed_dates=closures,
            review_id="source-test",
            reviewed_on=now,
        )
        for exchange, host, body, closures in zip(
            ("SSE", "SZSE"),
            ("www.sse.com.cn", "www.szse.cn"),
            bodies,
            closed,
            strict=True,
        )
    )
    source = build_source(
        schema_version=1,
        rule_version="cn-a-share-weekends-closed-v1",
        year=2027,
        schedules=schedules,
    )
    path = directory / ("conflict.json" if conflict else "source.json")
    path.write_text(json.dumps(source.model_dump(mode="json")), encoding="utf-8")
    return path


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


def test_generation_status_is_frozen_extra_forbid_and_has_exact_contract():
    status = CalendarGenerationStatusV1(
        schema_version=1,
        runtime_enabled=False,
        status="disabled",
        generation_sha256=None,
        covered_years=(),
        covered_through=None,
        current_year_ready=False,
        next_year=2027,
        next_year_status="not_due",
        last_outcome=None,
        conflict_detected=False,
        provider_requests=0,
        canonical_writes=False,
    )
    assert tuple(CalendarGenerationStatusV1.model_fields) == (
        "schema_version",
        "runtime_enabled",
        "status",
        "generation_sha256",
        "covered_years",
        "covered_through",
        "current_year_ready",
        "next_year",
        "next_year_status",
        "last_outcome",
        "conflict_detected",
        "provider_requests",
        "canonical_writes",
    )
    with pytest.raises(ValidationError):
        CalendarGenerationStatusV1.model_validate({**status.model_dump(), "unexpected": True})
    with pytest.raises(ValidationError):
        CalendarGenerationStatusV1.model_validate(
            {**status.model_dump(), "generation_sha256": "a" * 64}
        )
    with pytest.raises(ValidationError):
        CalendarGenerationStatusV1.model_validate(
            {**status.model_dump(), "last_outcome": "PROMOTED"}
        )
    with pytest.raises(ValidationError):
        CalendarGenerationStatusV1.model_validate(
            {**status.model_dump(), "conflict_detected": True}
        )


def test_generation_status_disabled_never_reads_runtime_store(tmp_path, monkeypatch):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=False,
        local_control_dir=tmp_path / "control",
    )
    monkeypatch.setattr(
        "backend.app.market.calendar_runtime.CalendarGenerationStore",
        lambda _path: pytest.fail("disabled status opened runtime store"),
    )
    status = build_calendar_generation_status(settings, now=datetime(2026, 9, 1, 12, tzinfo=UTC))
    assert status.status == "disabled"
    assert status.runtime_enabled is False
    assert status.covered_years == (2025, 2026)
    assert status.current_year_ready is True
    assert status.next_year_status == "not_due"
    assert status.provider_requests == 0
    assert status.canonical_writes is False
    historical = build_calendar_generation_status(
        settings, now=datetime(2025, 9, 1, 12, tzinfo=UTC)
    )
    assert historical.covered_years == (2025, 2026)
    assert historical.next_year_status == "confirmed"
    assert not (tmp_path / "control").exists()


def test_generation_status_enabled_missing_is_unavailable_and_empty(tmp_path):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    status = build_calendar_generation_status(settings, now=datetime(2026, 9, 1, 12, tzinfo=UTC))
    assert status.status == "unavailable"
    assert status.covered_years == ()
    assert status.covered_through is None
    assert status.generation_sha256 is None
    assert status.current_year_ready is False
    assert not (tmp_path / "control").exists()


def test_generation_status_valid_empty_store_composes_verified_bundled_coverage(tmp_path):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    path = tmp_path / "control" / settings.calendar_generation_database_name
    path.parent.mkdir(mode=0o700)
    store = CalendarGenerationStore(path)
    connection = store._connect()
    try:
        store._initialize(connection)
        store._assert_writer_identity(connection)
        connection.commit()
    finally:
        store._close_connection(connection)
    path.with_name(path.name + ".lock").touch(mode=0o600)
    status = build_calendar_generation_status(settings, now=datetime(2026, 9, 1, 12, tzinfo=UTC))
    assert status.status == "ready"
    assert status.covered_years == (2025, 2026)
    assert status.covered_through == "2026-12-31"
    assert status.current_year_ready is True
    assert status.next_year_status == "not_due"
    assert status.provider_requests == 0
    assert status.canonical_writes is False
    historical_status = build_calendar_generation_status(
        settings, now=datetime(2025, 9, 1, 12, tzinfo=UTC)
    )
    assert historical_status.next_year == 2026
    assert historical_status.next_year_status == "confirmed"


def test_calendar_stage_default_plan_is_zero_write_and_zero_network(tmp_path, monkeypatch, capsys):
    now = datetime(2026, 9, 1, 12, tzinfo=UTC)
    bodies = (b"cli-sse-2027", b"cli-szse-2027")
    schedules = tuple(
        build_schedule(
            exchange=exchange,
            year=2027,
            coverage_start=date(2027, 1, 1),
            coverage_end=date(2027, 12, 31),
            title="CLI runtime calendar",
            notice_no="cli-2027",
            official_url=f"https://{host}/cli-2027",
            published_on=date(2026, 8, 1),
            body_sha256=body_sha256(body),
            closed_dates=(),
            review_id="cli-test",
            reviewed_on=now,
        )
        for exchange, host, body in zip(
            ("SSE", "SZSE"), ("www.sse.com.cn", "www.szse.cn"), bodies, strict=True
        )
    )
    package = tmp_path / "calendar.json"
    package.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "rule_version": "cn-a-share-weekends-closed-v1",
                "year": 2027,
                "schedules": [item.model_dump(mode="json") for item in schedules],
                "source_sha256": build_source(schedules=schedules, year=2027).source_sha256,
            }
        ),
        encoding="utf-8",
    )
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "_calendar_runtime_now", lambda: now)
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "calendar-generation-stage", "--source", str(package)],
    )
    assert cli_module.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "STAGED"
    assert payload["network_requests"] == 0
    assert payload["writes_calendar_state"] is False
    assert payload["canonical_writes"] is False
    assert not (tmp_path / "control").exists()


@pytest.mark.parametrize("kind", ["malformed", "duplicate", "symlink"])
def test_calendar_stage_invalid_input_exits_two_before_control_access(
    kind, monkeypatch, capsys, tmp_path
):
    source = tmp_path / "source.json"
    if kind == "malformed":
        source.write_text("not-json", encoding="utf-8")
    elif kind == "duplicate":
        source.write_text('{"schema_version": 1, "schema_version": 1}', encoding="utf-8")
    else:
        target = tmp_path / "target.json"
        target.write_text("not-json", encoding="utf-8")
        source.symlink_to(target)
    monkeypatch.setattr(
        cli_module,
        "get_calendar_runtime_settings",
        lambda: pytest.fail("invalid source reached runtime settings"),
    )
    monkeypatch.setattr(
        sys, "argv", ["stock-eva", "calendar-generation-stage", "--source", str(source)]
    )
    assert cli_module.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "SOURCE_INVALID"
    assert not (tmp_path / "control").exists()


def test_calendar_stage_execute_idempotent_and_quarantine_exit_matrix(
    monkeypatch, capsys, tmp_path
):
    now = datetime(2026, 9, 1, 12, tzinfo=UTC)
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    source = _write_source_package(tmp_path, now)
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "_calendar_runtime_now", lambda: now)
    argv = ["stock-eva", "calendar-generation-stage", "--source", str(source), "--execute"]
    monkeypatch.setattr(sys, "argv", argv)
    assert cli_module.main() == 0
    first = json.loads(capsys.readouterr().out)
    assert first["outcome"] == "STAGED"
    assert first["writes_calendar_state"] is True
    assert cli_module.main() == 0
    second = json.loads(capsys.readouterr().out)
    assert second["outcome"] == "ALREADY_STAGED"
    assert second["writes_calendar_state"] is False

    conflict_settings = settings.model_copy(update={"local_control_dir": tmp_path / "control-2"})
    conflict = _write_source_package(tmp_path, now, conflict=True)
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: conflict_settings)
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "calendar-generation-stage", "--source", str(conflict), "--execute"],
    )
    assert cli_module.main() == 1
    quarantined = json.loads(capsys.readouterr().out)
    assert quarantined["outcome"] == "SOURCE_CONFLICT"
    assert quarantined["admission"] == "quarantined"
    assert quarantined["writes_calendar_state"] is True


def test_calendar_generation_api_is_standalone_and_returns_disabled_status(monkeypatch, tmp_path):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=False,
        local_control_dir=tmp_path / "control",
    )
    monkeypatch.setattr(
        market_api,
        "get_calendar_runtime_settings",
        lambda: pytest.fail("API used an unexpected settings path"),
    )
    app.dependency_overrides[market_api.get_calendar_generation_api_settings] = lambda: settings

    async def request():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/market/calendar-generation")

    try:
        response = asyncio.run(request())
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json()["status"] == "disabled"
    assert response.json()["provider_requests"] == 0
    assert response.json()["canonical_writes"] is False
    assert not (tmp_path / "control").exists()


def test_calendar_generation_api_dependency_invalid_env_is_sanitized_422(monkeypatch):
    monkeypatch.setenv("STOCK_EVA_CALENDAR_RUNTIME_ENABLED", "not-a-bool")
    monkeypatch.setattr(market_api, "get_settings", lambda: pytest.fail("get_settings used"))
    app.dependency_overrides.clear()

    async def request():
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/market/calendar-generation?execute=true")

    try:
        response = asyncio.run(request())
    finally:
        app.dependency_overrides.clear()
    assert response.status_code == 422
    assert response.json() == {"detail": {"code": "invalid_calendar_runtime_configuration"}}
    assert "not-a-bool" not in response.text
    assert "STOCK_EVA" not in response.text


def test_calendar_generation_api_declares_no_query_parameters():
    operation = app.openapi()["paths"]["/api/v1/market/calendar-generation"]["get"]
    assert "parameters" not in operation


@pytest.mark.parametrize(
    ("admission", "expected"),
    [("awaiting_machine", "STAGED"), ("quarantined", "SOURCE_CONFLICT")],
)
def test_status_last_outcome_uses_latest_candidate_without_attempt(admission, expected):
    candidate = SimpleNamespace(
        staging_sequence=1,
        staged_at=datetime(2026, 9, 1, 1, tzinfo=UTC),
        admission=admission,
    )
    assert _last_outcome(SimpleNamespace(candidates=(candidate,), attempts=())) == expected


def test_status_last_outcome_newer_valid_candidate_clears_older_conflict():
    old = SimpleNamespace(
        staging_sequence=1,
        staged_at=datetime(2026, 9, 1, 1, tzinfo=UTC),
        admission="quarantined",
    )
    new = SimpleNamespace(
        staging_sequence=2,
        staged_at=datetime(2026, 9, 1, 2, tzinfo=UTC),
        admission="awaiting_machine",
    )
    control = SimpleNamespace(candidates=(old, new), attempts=())
    assert _last_outcome(control) == "STAGED"


def test_status_last_outcome_maps_running_and_resolves_ties():
    started = datetime(2026, 9, 1, 1, tzinfo=UTC)
    lower = SimpleNamespace(
        started_at=started,
        target_year=2026,
        slot_date=date(2026, 9, 1),
        outcome="PROMOTED",
    )
    higher = SimpleNamespace(
        started_at=started,
        target_year=2027,
        slot_date=date(2026, 9, 1),
        outcome="RUNNING",
    )
    assert _last_outcome(SimpleNamespace(candidates=(), attempts=(lower, higher))) == (
        "ALREADY_ATTEMPTED"
    )


def test_status_last_outcome_same_timestamp_distinguishes_attempt_source_from_new_candidate():
    timestamp = datetime(2026, 9, 1, 1, tzinfo=UTC)
    attempt = SimpleNamespace(
        started_at=timestamp,
        target_year=2027,
        slot_date=date(2026, 9, 1),
        source_sha256="a" * 64,
        outcome="PROMOTED",
    )
    attempted = SimpleNamespace(
        staging_sequence=1,
        staged_at=timestamp,
        source_sha256="a" * 64,
        admission="awaiting_machine",
    )
    newer = SimpleNamespace(
        staging_sequence=2,
        staged_at=timestamp,
        source_sha256="b" * 64,
        admission="quarantined",
    )
    assert _last_outcome(SimpleNamespace(candidates=(attempted,), attempts=(attempt,))) == (
        "PROMOTED"
    )
    assert (
        _last_outcome(SimpleNamespace(candidates=(attempted, newer), attempts=(attempt,)))
        == "SOURCE_CONFLICT"
    )


def test_status_conflict_only_uses_latest_candidate_per_year(monkeypatch):
    def fake_source(payload):
        return SimpleNamespace(year=int(payload), source_sha256=f"{int(payload):064d}")

    monkeypatch.setattr(
        "backend.app.market.calendar_runtime.CalendarSourceBundleV1.model_validate_json",
        staticmethod(fake_source),
    )
    old = SimpleNamespace(
        canonical_source_bytes=b"2027", staging_sequence=1, admission="quarantined"
    )
    new = SimpleNamespace(
        canonical_source_bytes=b"2027", staging_sequence=2, admission="awaiting_machine"
    )
    other = SimpleNamespace(
        canonical_source_bytes=b"2028", staging_sequence=3, admission="quarantined"
    )
    assert _latest_candidate_conflict(SimpleNamespace(candidates=(old, new))) is False
    assert _latest_candidate_conflict(SimpleNamespace(candidates=(old, new, other))) is True


def test_cli_maintenance_execute_receives_fresh_clock_after_plan_snapshot(
    monkeypatch, capsys, tmp_path
):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    times = iter(
        (
            datetime(2026, 9, 1, 1, 0, tzinfo=UTC),
            datetime(2026, 9, 1, 1, 1, tzinfo=UTC),
            datetime(2026, 9, 1, 1, 2, tzinfo=UTC),
        )
    )
    observed = {}

    class FakeMaintenanceService:
        def __init__(self, *_args, clock, **_kwargs):
            self.clock = clock

        def execute(self, *, target_year=None):
            observed["times"] = (self.clock(), self.clock())
            return SimpleNamespace(
                outcome="NOT_DUE",
                target_year=target_year,
                source_sha256=None,
                generation_sha256=None,
                next_year_status="not_due",
                network_requests=0,
                official_requests=0,
                machine_requests=0,
                writes_calendar_state=False,
            )

    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "_calendar_runtime_now", lambda: next(times))
    monkeypatch.setattr(cli_module, "CalendarMaintenanceService", FakeMaintenanceService)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-maintenance", "--execute"])
    assert cli_module.main() == 0
    assert observed["times"] == (
        datetime(2026, 9, 1, 1, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 1, 2, tzinfo=UTC),
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "NOT_DUE"
    assert payload["network_requests"] == 0
    assert payload["canonical_writes"] is False


@pytest.mark.parametrize(
    ("runtime_status", "expected_exit"),
    [("disabled", 0), ("ready", 0), ("unavailable", 1)],
)
def test_cli_calendar_generation_status_exit_matrix(
    runtime_status, expected_exit, monkeypatch, capsys, tmp_path
):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=runtime_status != "disabled",
        local_control_dir=tmp_path / "control",
    )
    status = CalendarGenerationStatusV1(
        runtime_enabled=settings.calendar_runtime_enabled,
        status=runtime_status,
        generation_sha256=None,
        covered_years=(),
        covered_through=None,
        current_year_ready=False,
        next_year=2027,
        next_year_status="not_due",
        last_outcome=None,
        conflict_detected=False,
    )
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: settings)
    monkeypatch.setattr(
        cli_module, "build_calendar_generation_status", lambda *_args, **_kwargs: status
    )
    monkeypatch.setattr(cli_module, "get_settings", lambda: pytest.fail("general settings used"))
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-generation-status"])
    assert cli_module.main() == expected_exit
    assert json.loads(capsys.readouterr().out)["status"] == runtime_status


def test_cli_calendar_generation_status_invalid_config_is_exit_two(monkeypatch, capsys):
    monkeypatch.setattr(
        cli_module,
        "get_calendar_runtime_settings",
        lambda: (_ for _ in ()).throw(ValueError("private config")),
    )
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-generation-status"])
    assert cli_module.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "canonical_writes": False,
        "error_code": "INVALID_CALENDAR_RUNTIME_CONFIGURATION",
        "provider_requests": 0,
        "status": "unavailable",
    }
    assert "private config" not in json.dumps(payload)


def test_calendar_control_cli_lane_precedes_general_initialization(monkeypatch, capsys, tmp_path):
    """The three standalone commands must not enter the market/provider lane."""

    def fail(*args, **kwargs):
        del args, kwargs
        pytest.fail("general market initialization was reached")

    monkeypatch.setattr(cli_module, "get_settings", fail)
    monkeypatch.setattr(cli_module.StoragePreflight, "inspect", fail)
    monkeypatch.setattr(cli_module, "MarketStore", fail)
    monkeypatch.setattr(cli_module, "BaoStockProvider", fail)
    monkeypatch.setattr(StorageLayout, "ensure_local_runtime_dirs", fail)
    monkeypatch.setattr("backend.app.market.calendar_maintenance.httpx.Client", fail)

    disabled = CalendarRuntimeSettings(
        calendar_runtime_enabled=False,
        local_control_dir=tmp_path / "status-control",
    )
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: disabled)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-generation-status"])
    assert cli_module.main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "disabled"

    malformed = tmp_path / "malformed.json"
    malformed.write_text("not-json", encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        ["stock-eva", "calendar-generation-stage", "--source", str(malformed)],
    )
    assert cli_module.main() == 2
    assert json.loads(capsys.readouterr().out)["outcome"] == "SOURCE_INVALID"

    enabled = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "maintenance-control",
    )
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: enabled)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-maintenance"])
    assert cli_module.main() == 1
    assert json.loads(capsys.readouterr().out)["outcome"] == "CONTROL_STATE_UNAVAILABLE"


@pytest.mark.parametrize(
    ("outcome", "expected_exit"),
    [
        ("PROMOTED", 0),
        ("NOT_DUE", 0),
        ("SOURCE_PENDING", 0),
        ("ALREADY_ATTEMPTED", 0),
        ("SOURCE_CONFLICT", 1),
        ("OFFICIAL_UNAVAILABLE", 1),
        ("MACHINE_UNAVAILABLE", 1),
        ("SKIPPED_CIRCUIT_OPEN", 1),
        ("CONTROL_STATE_UNAVAILABLE", 1),
    ],
)
def test_cli_calendar_maintenance_execute_exit_and_safe_payload_matrix(
    outcome, expected_exit, monkeypatch, capsys, tmp_path
):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )

    class FakeMaintenanceService:
        def __init__(self, *_args, **_kwargs):
            pass

        def execute(self, *, target_year=None):
            return SimpleNamespace(
                outcome=outcome,
                target_year=target_year,
                source_sha256="a" * 64,
                generation_sha256="b" * 64 if outcome == "PROMOTED" else None,
                next_year_status="pending",
                network_requests=3,
                official_requests=2,
                machine_requests=1,
                writes_calendar_state=True,
            )

    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "CalendarMaintenanceService", FakeMaintenanceService)
    monkeypatch.setattr(cli_module, "get_settings", lambda: pytest.fail("general settings used"))
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-maintenance", "--execute"])
    assert cli_module.main() == expected_exit
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == outcome
    assert payload["network_requests"] == 3
    assert payload["official_requests"] == 2
    assert payload["machine_requests"] == 1
    assert payload["canonical_writes"] is False
    assert "control" not in json.dumps(payload)


def test_cli_calendar_maintenance_plan_missing_control_is_write_free(monkeypatch, capsys, tmp_path):
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: settings)
    monkeypatch.setattr(
        cli_module, "_calendar_runtime_now", lambda: datetime(2026, 9, 1, 12, tzinfo=UTC)
    )
    monkeypatch.setattr(cli_module, "get_settings", lambda: pytest.fail("general settings used"))
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-maintenance"])
    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "CONTROL_STATE_UNAVAILABLE"
    assert payload["network_requests"] == 0
    assert payload["writes_calendar_state"] is False
    assert payload["canonical_writes"] is False
    assert not (tmp_path / "control").exists()


def test_cli_calendar_maintenance_valid_plan_is_zero_network_and_no_provider(
    monkeypatch, capsys, tmp_path
):
    now = datetime(2026, 9, 1, 12, tzinfo=UTC)
    settings = CalendarRuntimeSettings(
        calendar_runtime_enabled=True,
        local_control_dir=tmp_path / "control",
    )
    source = _write_source_package(tmp_path, now)
    source_model = build_source(
        schema_version=1,
        rule_version="cn-a-share-weekends-closed-v1",
        year=2027,
        schedules=tuple(
            build_schedule(
                exchange=exchange,
                year=2027,
                coverage_start=date(2027, 1, 1),
                coverage_end=date(2027, 12, 31),
                title="Source package calendar",
                notice_no="source-2027",
                official_url=f"https://{host}/source-2027",
                published_on=date(2026, 8, 1),
                body_sha256=body_sha256(body),
                closed_dates=(date(2027, 1, 1),),
                review_id="source-test",
                reviewed_on=now,
            )
            for exchange, host, body in zip(
                ("SSE", "SZSE"),
                ("www.sse.com.cn", "www.szse.cn"),
                (b"source-sse-2027", b"source-szse-2027"),
                strict=True,
            )
        ),
    )
    generation_store = CalendarGenerationStore(
        settings.local_control_dir / settings.calendar_generation_database_name
    )
    assert generation_store.stage_execute(source_model, now).outcome == "STAGED"
    health_path = settings.local_control_dir / settings.provider_health_database_name
    health_path.parent.mkdir(mode=0o700, exist_ok=True)
    SQLiteProviderHealthStore(health_path).initialize()
    monkeypatch.setattr(cli_module, "get_calendar_runtime_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "_calendar_runtime_now", lambda: now)
    monkeypatch.setattr(cli_module, "get_settings", lambda: pytest.fail("general settings used"))
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-maintenance"])
    assert cli_module.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == "NOT_DUE"
    assert payload["network_requests"] == 0
    assert payload["writes_calendar_state"] is False
    assert payload["canonical_writes"] is False
    assert source.exists()


def test_cli_calendar_sync_runtime_runs_once_then_rejects_stale_legacy_plan(
    monkeypatch, capsys, tmp_path
):
    settings = Settings(
        _env_file=None,
        calendar_runtime_enabled=True,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=None,
        local_market_dataset_root=None,
    )
    first = TradingCalendar.from_dict(
        {
            "year": 2026,
            "status": "confirmed",
            "published_on": "2025-12-20",
            "sources": [],
            "closed_dates": ["2026-01-01"],
        }
    )
    second = TradingCalendar.from_dict(
        {
            "year": 2026,
            "status": "confirmed",
            "published_on": "2025-12-20",
            "sources": [],
            "closed_dates": ["2026-01-02"],
        }
    )
    events: list[str] = []

    class Live:
        def __init__(self):
            self.snapshots = [first, second]

        def snapshot(self):
            events.append("snapshot")
            return self.snapshots.pop(0)

    live = Live()

    class RuntimeService:
        def execute(self):
            events.append("runtime")
            return SimpleNamespace(
                outcome="NOT_DUE",
                target_year=2026,
                source_sha256=None,
                generation_sha256=None,
                next_year_status="not_due",
                network_requests=0,
                official_requests=0,
                machine_requests=0,
                writes_calendar_state=False,
            )

    class NoWriteCalendarStore:
        def __init__(self, _path, *, initialize):
            events.append(f"legacy-store:{initialize}")

        def initialize_for_write(self):
            events.append("legacy-store-init")

    class NoCallProvider:
        def trading_dates(self, *_args):
            events.append("provider-request")
            raise AssertionError("stale plan must not request provider")

    class NoInitHealth:
        def __init__(self, *_args, **_kwargs):
            events.append("legacy-health")

        def initialize(self):
            events.append("legacy-health-init")

        def provider_health(self):
            return SimpleNamespace(state="CLOSED")

    class NoLock:
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

    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr(cli_module, "_calendar_runtime_service", lambda _settings: RuntimeService())
    monkeypatch.setattr(cli_module, "build_live_trading_calendar", lambda _settings: live)
    monkeypatch.setattr(cli_module, "CalendarSyncStore", NoWriteCalendarStore)
    monkeypatch.setattr(cli_module, "SQLiteProviderHealthStore", NoInitHealth)
    monkeypatch.setattr(cli_module, "BaoStockProvider", lambda **_kwargs: NoCallProvider())
    monkeypatch.setattr(cli_module, "RefreshRunLock", NoLock)
    monkeypatch.setattr(
        cli_module,
        "get_market_clock",
        lambda: lambda: datetime(2026, 7, 24, 16, 30, tzinfo=UTC),
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

    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["failure_code"] == "CALENDAR_AUTHORITY_CHANGED"
    assert payload["writes_calendar_state"] is False
    assert payload["calendar_maintenance"]["outcome"] == "NOT_DUE"
    assert events[:2] == ["runtime", "snapshot"]
    assert events.count("snapshot") == 2
    assert "provider-request" not in events


def test_cli_calendar_sync_runtime_unavailable_blocks_legacy_construction(
    monkeypatch, capsys, tmp_path
):
    settings = Settings(
        _env_file=None,
        calendar_runtime_enabled=True,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=None,
        local_market_dataset_root=None,
    )
    events: list[str] = []

    class RuntimeService:
        def execute(self):
            events.append("runtime")
            return SimpleNamespace(
                outcome="CONTROL_STATE_UNAVAILABLE",
                target_year=2026,
                source_sha256=None,
                generation_sha256=None,
                next_year_status="not_due",
                network_requests=0,
                official_requests=0,
                machine_requests=0,
                writes_calendar_state=False,
            )

    class UnavailableLive:
        def snapshot(self):
            events.append("snapshot")
            return EMPTY_CALENDAR

    class Forbidden:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("legacy calendar/provider/health must not be constructed")

    class ReadyPreflight:
        def __init__(self, _settings):
            pass

        def inspect(self):
            return SimpleNamespace(market_data_available=True, mode="local")

    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr(cli_module, "_calendar_runtime_service", lambda _settings: RuntimeService())
    monkeypatch.setattr(
        cli_module, "build_live_trading_calendar", lambda _settings: UnavailableLive()
    )
    monkeypatch.setattr(cli_module, "CalendarSyncStore", Forbidden)
    monkeypatch.setattr(cli_module, "SQLiteProviderHealthStore", Forbidden)
    monkeypatch.setattr(cli_module, "BaoStockProvider", Forbidden)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-sync", "--execute"])

    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "status": "unavailable",
        "outcome": "CONTROL_STATE_UNAVAILABLE",
        "network_requests": 0,
        "writes_calendar_state": False,
        "canonical_writes": False,
    }
    assert events == ["runtime", "snapshot"]


@pytest.mark.parametrize(
    "outcome",
    [
        "OFFICIAL_UNAVAILABLE",
        "MACHINE_UNAVAILABLE",
        "SKIPPED_CIRCUIT_OPEN",
        "CONTROL_STATE_UNAVAILABLE",
    ],
)
def test_cli_calendar_sync_runtime_terminal_failure_is_not_reported_success(
    outcome, monkeypatch, capsys, tmp_path
):
    settings = Settings(
        _env_file=None,
        calendar_runtime_enabled=True,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=None,
        local_market_dataset_root=None,
    )
    calendar = TradingCalendar.from_dict(
        {
            "year": 2026,
            "status": "confirmed",
            "published_on": "2025-12-20",
            "sources": [],
            "closed_dates": [],
        }
    )

    class RuntimeService:
        def execute(self):
            return SimpleNamespace(
                outcome=outcome,
                target_year=2026,
                source_sha256="a" * 64,
                generation_sha256=None,
                next_year_status="pending",
                network_requests=3,
                official_requests=2,
                machine_requests=1,
                writes_calendar_state=True,
            )

    class Live:
        def snapshot(self):
            return calendar

    class NoWriteCalendarStore:
        def __init__(self, _path, *, initialize):
            assert initialize is False

    class NoLegacyPlan:
        def __init__(self, *_args, **_kwargs):
            self.policy = SimpleNamespace()

        def initialize_for_execution(self):
            pass

        def plan(self, **_kwargs):
            return None

    class ReadyPreflight:
        def __init__(self, _settings):
            pass

        def inspect(self):
            return SimpleNamespace(market_data_available=True, mode="local")

    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr(cli_module, "_calendar_runtime_service", lambda _settings: RuntimeService())
    monkeypatch.setattr(cli_module, "build_live_trading_calendar", lambda _settings: Live())
    monkeypatch.setattr(cli_module, "CalendarSyncStore", NoWriteCalendarStore)
    monkeypatch.setattr(cli_module, "CalendarSyncService", NoLegacyPlan)
    monkeypatch.setattr(sys, "argv", ["stock-eva", "calendar-sync", "--execute"])

    assert cli_module.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "not-due"
    assert payload["calendar_maintenance"]["outcome"] == outcome
    assert payload["calendar_maintenance"]["network_requests"] == 3
    assert payload["calendar_maintenance"]["writes_calendar_state"] is True


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
