import asyncio
import json
import multiprocessing
import os
import socket
import sqlite3
import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import backend.app.main as main_module
from backend.app.blocking import BlockingOperation
from backend.app.config import Settings
from backend.app.market.automation import RefreshRunLock
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.calendar import SHANGHAI
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore
from backend.app.provider_process import (
    ProviderProcessCommand,
    ProviderProcessError,
    ProviderProcessRunner,
    ProviderProcessTestConfig,
    run_provider_process,
)
from backend.app.storage.models import StorageReadiness
from tests.test_market_provider_contract import _CompleteSdkClient


class _ProductionSocketSdkClient(_CompleteSdkClient):
    """Complete offline SDK analogue whose requests cross a local socket."""

    def __init__(self, config: ProviderProcessTestConfig) -> None:
        super().__init__(response_date=(config.response_date or date(2026, 8, 20)).isoformat())
        self.config = config

    def _record(self, name: str, *args: object) -> None:
        left, right = socket.socketpair()
        self.context = type("Context", (), {"default_socket": left})()
        self.config.marker.write_text(f"{name}-in-flight")

        def respond() -> None:
            if self.config.scenario == "production_timeout":
                return
            right.sendall(b"synthetic")

        sender = threading.Thread(target=respond)
        sender.start()
        try:
            left.recv(32)
        finally:
            left.close()
            right.close()
            sender.join(timeout=1)
        super()._record(name, *args)


def production_provider_factory(config: ProviderProcessTestConfig, **kwargs):
    """Spawn-importable factory that still constructs the real provider wrapper."""
    return BaoStockProvider(client=_ProductionSocketSdkClient(config), **kwargs)


def _settings(tmp_path: Path, **updates) -> Settings:
    configured = Settings(
        _env_file=None,
        auto_refresh_enabled=True,
        calendar_runtime_enabled=False,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=None,
        local_market_dataset_root=None,
        replication_enabled=False,
    )
    return configured.model_copy(update=updates)


def _command(
    settings: Settings,
    kind: str,
    scenario: str,
    marker: Path,
    *,
    release: Path | None = None,
    timeout: float = 0.1,
) -> ProviderProcessCommand:
    return ProviderProcessCommand(
        kind=kind,
        settings=settings.model_dump(mode="python"),
        startup=True,
        test=ProviderProcessTestConfig(
            scenario=scenario,
            marker=marker,
            release=release,
            socket_timeout_seconds=timeout,
        ),
    )


def _children() -> set[int | None]:
    return {child.pid for child in multiprocessing.active_children()}


class _ReadyPreflight:
    def __init__(self, _settings) -> None:
        pass

    def inspect(self) -> StorageReadiness:
        return StorageReadiness(
            mode="local",
            status="ready",
            market_data_available=True,
            serving_source="local",
            mount_type="local",
            sentinel_status="ready",
            manifest_status="ready",
            dataset_generation=None,
        )


def _wait_for(path: Path, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists()


def _install_synthetic_runner(monkeypatch, config: ProviderProcessTestConfig) -> None:
    monkeypatch.setattr(
        main_module,
        "provider_process_runner_factory",
        lambda kind, settings: ProviderProcessRunner(
            kind, settings, test=config, clock=_fixed_clock()
        ),
    )


def _production_config(
    marker: Path,
    *,
    timeout: bool = False,
    response_date: date = date(2026, 8, 20),
) -> ProviderProcessTestConfig:
    return ProviderProcessTestConfig(
        scenario="production_timeout" if timeout else "production_success",
        marker=marker,
        socket_timeout_seconds=0.1,
        production_provider_factory=(
            "tests.test_background_baostock_execution:production_provider_factory"
        ),
        child_settings_overrides={"calendar_runtime_enabled": False},
        response_date=response_date,
    )


def _fixed_clock():
    return lambda: datetime(2026, 8, 20, 18, 10, tzinfo=SHANGHAI)


def _publish_baseline(settings: Settings) -> MarketStore:
    settings.market_data_dir.mkdir(parents=True, exist_ok=True)
    store = MarketStore(settings.market_data_dir / settings.market_database_name)
    observed = datetime(2026, 7, 23, 10, tzinfo=UTC)
    bar = DailyBar(
        trade_date=date(2026, 7, 23),
        symbol="sh.600000",
        security_type="stock",
        exchange="sh",
        board="main",
        open=10,
        high=11,
        low=9,
        close=10.5,
        preclose=10,
        volume=100,
        amount=1000,
        turnover_rate=1,
        pct_change=5,
        adjust_factor=1,
        is_trading=True,
        is_suspended=False,
        is_st=False,
        source_record_id="baseline",
        ingested_at=observed,
        quality_status="ready",
    )
    store.save_refresh(
        [bar],
        RefreshResult(
            run_id="baseline",
            requested_date=bar.trade_date,
            source="baostock",
            status="ready",
            requested_count=1,
            succeeded_count=1,
            coverage_ratio=1,
            started_at=observed,
            completed_at=observed,
        ),
        publish=True,
    )
    return store


def test_real_main_lifespan_runs_market_and_calendar_children_while_health_responds(
    tmp_path: Path, monkeypatch
) -> None:
    marker = tmp_path / "in-flight"
    release = tmp_path / "release"
    settings = _settings(tmp_path, calendar_runtime_enabled=True)
    monkeypatch.setattr(main_module, "settings", settings)
    monkeypatch.setattr(main_module, "StoragePreflight", _ReadyPreflight)
    monkeypatch.setattr(main_module, "background_poll_seconds", 0.05)
    _install_synthetic_runner(
        monkeypatch,
        ProviderProcessTestConfig(
            scenario="timeout",
            marker=marker,
            release=release,
            socket_timeout_seconds=5,
        ),
    )
    before = _children()

    with TestClient(main_module.app) as client:
        _wait_for(marker)
        started = time.monotonic()
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        assert time.monotonic() - started < 1
        release.write_text("release")
        _wait_for(marker.with_name(f"{marker.name}-market"))
        _wait_for(marker.with_name(f"{marker.name}-calendar"))

    assert _children() == before


def test_main_lifespan_spawn_runs_production_market_composition(
    tmp_path: Path, monkeypatch
) -> None:
    marker = tmp_path / "production-market"
    settings = _settings(tmp_path)
    monkeypatch.setattr(main_module, "settings", settings)
    monkeypatch.setattr(main_module, "StoragePreflight", _ReadyPreflight)
    monkeypatch.setattr(main_module, "get_market_clock", _fixed_clock)
    monkeypatch.setattr(main_module, "background_poll_seconds", 0.05)
    _install_synthetic_runner(monkeypatch, _production_config(marker))
    before = _children()

    with TestClient(main_module.app):
        deadline = time.monotonic() + 60
        command_marker = marker.with_name(f"{marker.name}-command-market")
        payload = None
        while time.monotonic() < deadline:
            if command_marker.exists():
                payload = json.loads(command_marker.read_text())
                if payload.get("result", {}).get("status") == "ready":
                    break
            time.sleep(0.02)

        assert marker.exists(), "production provider requests must cross the synthetic socket"
        assert payload is not None
        assert payload["result"]["status"] == "ready"

    store = MarketStore(
        settings.market_data_dir / settings.market_database_name,
        read_only=True,
    )
    pointer = store.published_refresh()
    state = store.scheduler_state()
    assert pointer is not None
    assert pointer.requested_date == date(2026, 8, 20)
    assert pointer.status == "ready"
    assert state is not None and state.refresh_state == "success"
    assert _children() == before


def test_calendar_only_spawn_runs_production_sync_composition(tmp_path: Path, monkeypatch) -> None:
    marker = tmp_path / "production-calendar"
    settings = _settings(
        tmp_path,
        auto_refresh_enabled=False,
        calendar_runtime_enabled=True,
    )
    monkeypatch.setattr(main_module, "settings", settings)
    monkeypatch.setattr(main_module, "get_market_clock", _fixed_clock)
    monkeypatch.setattr(main_module, "background_poll_seconds", 0.05)
    _install_synthetic_runner(monkeypatch, _production_config(marker))
    database = settings.local_control_dir / settings.calendar_sync_database_name
    before = _children()

    with TestClient(main_module.app):
        deadline = time.monotonic() + 20
        run_count = 0
        while time.monotonic() < deadline:
            if database.exists():
                try:
                    with sqlite3.connect(database, timeout=0) as connection:
                        run_count = connection.execute(
                            "SELECT COUNT(*) FROM calendar_sync_runs"
                        ).fetchone()[0]
                except sqlite3.Error:
                    pass
            if run_count:
                break
            time.sleep(0.02)

        assert marker.exists(), "calendar provider request must cross the synthetic socket"
        assert run_count >= 1

    assert _children() == before


def test_production_timeout_preserves_pointer_and_releases_all_resources(
    tmp_path: Path, monkeypatch
) -> None:
    marker = tmp_path / "production-timeout"
    settings = _settings(tmp_path)
    store = _publish_baseline(settings)
    pointer_before = store.published_refresh()
    monkeypatch.setattr(main_module, "settings", settings)
    monkeypatch.setattr(main_module, "StoragePreflight", _ReadyPreflight)
    monkeypatch.setattr(main_module, "get_market_clock", _fixed_clock)
    monkeypatch.setattr(main_module, "background_poll_seconds", 60)
    _install_synthetic_runner(monkeypatch, _production_config(marker, timeout=True))
    before = _children()

    try:
        with TestClient(main_module.app):
            _wait_for(marker)
            time.sleep(0.5)
    except ProviderProcessError:
        # The calendar lane surfaces a completed provider failure at shutdown.
        pass

    pointer_after = MarketStore(store.path, read_only=True).published_refresh()
    assert pointer_before is not None
    assert pointer_after is not None
    assert pointer_after.run_id == pointer_before.run_id
    assert _children() == before
    with RefreshRunLock(settings.local_lock_dir / "market-refresh.lock"):
        pass


def test_real_main_calendar_only_lifespan_uses_spawned_provider(
    tmp_path: Path, monkeypatch
) -> None:
    marker = tmp_path / "calendar"
    settings = _settings(
        tmp_path,
        auto_refresh_enabled=False,
        calendar_runtime_enabled=True,
    )
    monkeypatch.setattr(main_module, "settings", settings)
    _install_synthetic_runner(
        monkeypatch,
        ProviderProcessTestConfig(scenario="success", marker=marker),
    )
    before = _children()

    with TestClient(main_module.app):
        _wait_for(marker.with_name(f"{marker.name}-calendar"))

    assert _children() == before


def test_real_main_provider_failure_preserves_canonical_pointer(
    tmp_path: Path, monkeypatch
) -> None:
    marker = tmp_path / "failure"
    settings = _settings(tmp_path)
    store = _publish_baseline(settings)
    pointer_before = store.published_refresh()
    monkeypatch.setattr(main_module, "settings", settings)
    monkeypatch.setattr(main_module, "StoragePreflight", _ReadyPreflight)
    monkeypatch.setattr(main_module, "background_poll_seconds", 0.05)
    _install_synthetic_runner(
        monkeypatch,
        ProviderProcessTestConfig(scenario="unserializable_failure", marker=marker),
    )

    try:
        with TestClient(main_module.app):
            _wait_for(marker)
            time.sleep(0.1)
    except ProviderProcessError:
        pass

    pointer_after = MarketStore(store.path, read_only=True).published_refresh()
    assert pointer_before is not None
    assert pointer_after is not None
    assert pointer_after.run_id == pointer_before.run_id


@pytest.mark.parametrize(
    ("scenario", "failure_type"),
    (("timeout", "_OperationDeadlineExceeded"), ("unserializable_failure", "RuntimeError")),
)
def test_provider_failure_uses_structured_envelope_and_reaps_child(
    tmp_path: Path,
    scenario: str,
    failure_type: str,
) -> None:
    async def exercise() -> None:
        before = _children()
        with pytest.raises(ProviderProcessError) as captured:
            await run_provider_process(
                _command(_settings(tmp_path), "market", scenario, tmp_path / scenario)
            )
        assert failure_type in captured.value.failure_type
        assert _children() == before

    asyncio.run(exercise())


def test_production_child_uses_scheduler_timestamp_across_close_boundary(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        settings = _settings(tmp_path)
        marker = tmp_path / "slot-boundary"
        before_close = datetime(2026, 8, 20, 15, 59, tzinfo=SHANGHAI)
        after_close = datetime(2026, 8, 20, 18, 10, tzinfo=SHANGHAI)

        await run_provider_process(
            ProviderProcessCommand(
                kind="market",
                settings=settings.model_dump(mode="python"),
                requested_at=before_close,
                test=_production_config(marker, response_date=date(2026, 8, 19)),
            )
        )
        store = MarketStore(settings.market_data_dir / settings.market_database_name)
        before_pointer = store.published_refresh()
        assert before_pointer is not None
        assert before_pointer.requested_date == date(2026, 8, 19)

        await run_provider_process(
            ProviderProcessCommand(
                kind="market",
                settings=settings.model_dump(mode="python"),
                requested_at=after_close,
                test=_production_config(marker, response_date=date(2026, 8, 20)),
            )
        )
        pointer = store.published_refresh()
        assert pointer is not None
        assert pointer.requested_date == after_close.date()

    asyncio.run(exercise())


def test_production_composition_timeout_is_structured_and_reaped(tmp_path: Path) -> None:
    async def exercise() -> None:
        settings = _settings(tmp_path)
        before = _children()
        with pytest.raises(ProviderProcessError) as captured:
            await run_provider_process(
                ProviderProcessCommand(
                    kind="market",
                    settings=settings.model_dump(mode="python"),
                    requested_at=datetime(2026, 8, 20, 18, 10, tzinfo=SHANGHAI),
                    test=_production_config(tmp_path / "production-timeout", timeout=True),
                )
            )
        assert captured.value.failure_type in {"CommandExit", "internal"}
        assert _children() == before

    asyncio.run(exercise())


def test_child_crash_reports_eof_and_reaps_child(tmp_path: Path) -> None:
    async def exercise() -> None:
        before = _children()
        with pytest.raises(ProviderProcessError) as captured:
            await run_provider_process(
                _command(_settings(tmp_path), "market", "crash", tmp_path / "crash")
            )
        assert captured.value.failure_type == "ChildEOF"
        assert _children() == before

    asyncio.run(exercise())


def test_shutdown_cancels_child_and_releases_refresh_lock(tmp_path: Path) -> None:
    async def exercise() -> None:
        settings = _settings(tmp_path)
        marker = tmp_path / "blocked"
        before = _children()
        task = asyncio.create_task(
            run_provider_process(
                _command(settings, "market", "timeout", marker, timeout=30),
                cancel_grace_seconds=0.05,
            )
        )
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert marker.exists()
        started = time.monotonic()
        task.cancel()
        result = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert time.monotonic() - started < 1
        assert _children() == before

        # A new child can acquire the same inter-process refresh lock immediately.
        completed = tmp_path / "after-cancel"
        await run_provider_process(_command(settings, "market", "success", completed))
        assert completed.with_name(f"{completed.name}-market").read_text() == "complete"
        assert _children() == before

    asyncio.run(exercise())


def test_concurrent_market_calendar_and_repeated_scheduling_leave_no_children(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        settings = _settings(tmp_path)
        before = _children()
        for iteration in range(2):
            marker = tmp_path / f"iteration-{iteration}"
            results = await asyncio.gather(
                run_provider_process(_command(settings, "market", "success", marker)),
                run_provider_process(_command(settings, "calendar", "success", marker)),
            )
            statuses = {result.status for result in results}
            assert "ok" in statuses
            assert statuses <= {"ok", "busy"}
            for kind in ("market", "calendar"):
                completed = marker.with_name(f"{marker.name}-{kind}")
                if not completed.exists():
                    result = await run_provider_process(_command(settings, kind, "success", marker))
                    assert result.status == "ok"
            assert marker.with_name(f"{marker.name}-market").exists()
            assert marker.with_name(f"{marker.name}-calendar").exists()
            assert _children() == before

    asyncio.run(exercise())


def test_replication_drain_remains_in_parent_thread_boundary(tmp_path: Path) -> None:
    async def exercise() -> None:
        observed = []

        def run_once() -> None:
            observed.append(os.getpid())

        runner = ProviderProcessRunner("market", _settings(tmp_path))
        await runner(BlockingOperation.REPLICATION_DRAIN, run_once)
        assert observed == [os.getpid()]

    asyncio.run(exercise())


def test_runner_dispatch_is_explicit_and_independent_of_callback_name(tmp_path: Path) -> None:
    async def exercise() -> None:
        observed = []

        def run_due_once() -> None:
            observed.append(os.getpid())

        settings = _settings(tmp_path)
        parent_runner = ProviderProcessRunner("market", settings)
        await parent_runner(BlockingOperation.REPLICATION_DRAIN, run_due_once)
        assert observed == [os.getpid()]

        marker = tmp_path / "renamed-callback"
        child_runner = ProviderProcessRunner(
            "market",
            settings,
            test=ProviderProcessTestConfig(scenario="success", marker=marker),
        )

        def renamed_callback(_scheduled_at) -> None:
            raise AssertionError("provider operation must not execute in the parent")

        await child_runner(
            BlockingOperation.MARKET_REFRESH,
            renamed_callback,
            datetime(2026, 8, 20, 18, 10, tzinfo=SHANGHAI),
            scheduled_at=datetime(2026, 8, 20, 18, 10, tzinfo=SHANGHAI),
        )
        assert marker.with_name(f"{marker.name}-market").exists()

        with pytest.raises(ValueError, match="invalid for market lane"):
            await parent_runner(BlockingOperation.CALENDAR_SYNC, run_due_once)

    asyncio.run(exercise())
