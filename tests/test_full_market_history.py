import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import backend.app.cli as cli
from backend.app.cli import build_parser
from backend.app.config import Settings
from backend.app.market.automation import RefreshAlreadyRunning
from backend.app.market.baostock import ProviderBatch
from backend.app.market.full_history import FullMarketHistoryService
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore
from backend.app.storage.models import StorageReadiness
from tests.test_market_automation import fixture_bars


def nas_root(tmp_path: Path) -> Path:
    root = tmp_path / "nas"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 1}),
        encoding="utf-8",
    )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 1,
                "generation": "empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    return root


def ready_result(trade_date: date) -> RefreshResult:
    bars = fixture_bars(trade_date)
    return RefreshResult(
        run_id=f"seed-{trade_date}",
        request_key=f"seed:{trade_date}",
        requested_date=trade_date,
        source="baostock",
        status="ready",
        requested_count=len(bars),
        succeeded_count=len(bars),
        coverage_ratio=1,
        started_at=datetime(2026, 7, 1, tzinfo=UTC),
        completed_at=datetime(2026, 7, 1, 0, 1, tzinfo=UTC),
    )


class FullProvider:
    def __init__(self, dates: list[date], *, fail_dates: set[date] | None = None) -> None:
        self.dates = dates
        self.fail_dates = set(fail_dates or ())
        self.fetch_calls: list[date] = []
        self.calendar_calls = 0

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        self.calendar_calls += 1
        return [item for item in self.dates if start_date <= item <= end_date]

    def fetch(self, trade_date: date, symbols=None) -> ProviderBatch:
        assert symbols is None
        self.fetch_calls.append(trade_date)
        bars = fixture_bars(trade_date)
        expected = [item.symbol for item in bars]
        if trade_date in self.fail_dates:
            bars = [item for item in bars if item.symbol != "sz.399001"]
        return ProviderBatch(
            bars=bars,
            expected_symbols=expected,
            failed_symbols=[],
        )


def store_for(tmp_path: Path) -> NasMarketStore:
    return NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        nas_root(tmp_path),
        tmp_path / "staging",
    )


def test_plan_and_execution_resume_from_manifest_without_replacing_dates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dates = [date(2026, 7, 21), date(2026, 7, 22), date(2026, 7, 23)]
    store = store_for(tmp_path)
    store.save_refresh(
        fixture_bars(dates[0]),
        ready_result(dates[0]),
        publish=True,
    )
    provider = FullProvider(dates)
    service = FullMarketHistoryService(store, provider)

    first_plan = service.plan(
        start_date=dates[0],
        end_date=dates[-1],
        max_sessions=3,
    )
    second_plan = service.plan(
        start_date=dates[0],
        end_date=dates[-1],
        max_sessions=3,
    )

    assert first_plan == second_plan
    assert first_plan.already_published_dates == [dates[0]]
    assert first_plan.pending_dates == dates[1:]
    assert first_plan.pending_sessions == 2

    original_publish = store.publisher.publish_manifest
    manifest_writes = 0

    def count_publish(*args, **kwargs):
        nonlocal manifest_writes
        manifest_writes += 1
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(store.publisher, "publish_manifest", count_publish)
    result = service.execute(first_plan)

    assert result.status == "ready"
    assert result.published_sessions == 3
    assert result.skipped_sessions == 1
    assert provider.fetch_calls == dates[1:]
    assert manifest_writes == 2
    assert store.manifest_dates() == dates
    assert all(item.run_kind == "backfill" for item in result.date_results)
    assert [item.run_id for item in result.date_results] == [
        service.daily_run_id(first_plan, item) for item in dates[1:]
    ]

    resumed = service.execute(first_plan)

    assert resumed.status == "ready"
    assert resumed.skipped_sessions == 3
    assert resumed.date_results == []
    assert provider.fetch_calls == dates[1:]
    assert manifest_writes == 2


def test_partial_date_never_publishes_and_older_resume_does_not_regress_pointer(
    tmp_path: Path,
) -> None:
    dates = [date(2026, 7, 22), date(2026, 7, 23)]
    store = store_for(tmp_path)
    failing = FullProvider(dates, fail_dates={dates[0]})
    first_service = FullMarketHistoryService(store, failing)
    plan = first_service.plan(
        start_date=dates[0],
        end_date=dates[-1],
        max_sessions=2,
    )

    first = first_service.execute(plan)

    assert first.status == "partial"
    assert first.failed_dates == [dates[0]]
    assert store.manifest_dates() == [dates[1]]
    assert store.published_refresh().requested_date == dates[1]

    recovery = FullMarketHistoryService(store, FullProvider(dates))
    resume_plan = recovery.plan(
        start_date=dates[0],
        end_date=dates[-1],
        max_sessions=2,
    )
    resumed = recovery.execute(resume_plan)

    assert resume_plan.pending_dates == [dates[0]]
    assert resumed.status == "ready"
    assert store.manifest_dates() == dates
    assert store.published_refresh().requested_date == dates[1]


def test_resume_repairs_local_pointer_when_manifest_publish_won_the_crash(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 23)
    store = store_for(tmp_path)
    store._publish_bars(fixture_bars(trade_date))
    assert store.published_refresh() is None

    provider = FullProvider([trade_date])
    service = FullMarketHistoryService(store, provider)
    plan = service.plan(
        start_date=trade_date,
        end_date=trade_date,
        max_sessions=1,
    )
    resumed = service.execute(plan)

    assert resumed.status == "ready"
    assert resumed.skipped_sessions == 1
    assert resumed.date_results == []
    assert provider.fetch_calls == []
    pointer = store.published_refresh()
    assert pointer is not None
    assert pointer.requested_date == trade_date
    assert pointer.request_key.startswith("nas-manifest-reconcile:baostock:")


def test_full_history_cli_is_dry_run_unless_explicit_execution_flag_is_present() -> None:
    parser = build_parser()
    planned = parser.parse_args(
        [
            "full-market-backfill",
            "--start",
            "2026-01-01",
            "--end",
            "2026-07-23",
        ]
    )
    executable = parser.parse_args(
        [
            "full-market-backfill",
            "--start",
            "2026-01-01",
            "--end",
            "2026-07-23",
            "--execute-all-main-board-history",
        ]
    )

    assert planned.execute_all_main_board_history is False
    assert executable.execute_all_main_board_history is True
    assert planned.max_sessions == 3000


def test_full_history_cli_dry_run_queries_only_calendar_and_never_fetches(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    root = nas_root(tmp_path)
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=root,
    )

    class ReadyPreflight:
        def __init__(self, _settings) -> None:
            pass

        def inspect(self):
            return StorageReadiness(
                mode="nas",
                status="ready",
                market_data_available=True,
                serving_source="nas",
                mount_type="smbfs",
                sentinel_status="ready",
                manifest_status="ready",
                dataset_generation="empty",
            )

    class CalendarOnlyProvider:
        calendar_calls = 0
        fetch_calls = 0

        def __init__(self, **_kwargs) -> None:
            pass

        def trading_dates(self, start_date, end_date):
            self.calendar_calls += 1
            return [date(2026, 7, 22), date(2026, 7, 23)]

        def fetch(self, *_args, **_kwargs):
            self.fetch_calls += 1
            raise AssertionError("dry-run must not fetch market rows")

    provider = CalendarOnlyProvider()
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr(cli, "BaoStockProvider", lambda **_kwargs: provider)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "full-market-backfill",
            "--start",
            "2026-07-22",
            "--end",
            "2026-07-23",
        ],
    )

    assert cli.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry-run"
    assert payload["pending_sessions"] == 2
    assert payload["writes_market_data"] is False
    assert provider.calendar_calls == 1
    assert provider.fetch_calls == 0
    assert json.loads((root / "manifest.json").read_text())["files"] == []


def test_full_history_cli_execution_acquires_lock_before_calendar_request(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    root = nas_root(tmp_path)
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=root,
    )

    class ReadyPreflight:
        def __init__(self, _settings) -> None:
            pass

        def inspect(self):
            return StorageReadiness(
                mode="nas",
                status="ready",
                market_data_available=True,
                serving_source="nas",
                mount_type="smbfs",
                sentinel_status="ready",
                manifest_status="ready",
                dataset_generation="empty",
            )

    class UnusedProvider:
        calendar_calls = 0

        def __init__(self, **_kwargs) -> None:
            pass

        def trading_dates(self, *_args):
            self.calendar_calls += 1
            raise AssertionError("calendar request must be inside the refresh lock")

    class BusyLock:
        def __init__(self, _path) -> None:
            pass

        def __enter__(self):
            raise RefreshAlreadyRunning("busy")

        def __exit__(self, *_args) -> None:
            pass

    provider = UnusedProvider()
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr(cli, "BaoStockProvider", lambda **_kwargs: provider)
    monkeypatch.setattr(cli, "RefreshRunLock", BusyLock)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "full-market-backfill",
            "--start",
            "2026-07-22",
            "--end",
            "2026-07-23",
            "--execute-all-main-board-history",
        ],
    )

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["quality_issues"] == ["refresh_already_running"]
    assert provider.calendar_calls == 0
