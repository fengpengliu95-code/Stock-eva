import asyncio
import multiprocessing
import os
import time
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import backend.app.main as main_module
from backend.app.config import Settings
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
        lambda kind, settings: ProviderProcessRunner(kind, settings, test=config),
    )


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

    with pytest.raises(ProviderProcessError):
        with TestClient(main_module.app):
            _wait_for(marker)
            time.sleep(0.1)

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
        await runner(run_once)
        assert observed == [os.getpid()]

    asyncio.run(exercise())
