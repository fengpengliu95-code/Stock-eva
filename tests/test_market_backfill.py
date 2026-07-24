import importlib.util
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import backend.app.cli as cli
import backend.app.market.backfill as backfill
from backend.app.cli import build_parser
from backend.app.market.baostock import ProviderRangeBatch
from backend.app.market.store import MarketStore
from tests.test_strategy_engine import bar, combined_rule


def test_cli_exposes_safe_backfill_and_daily_audit_commands() -> None:
    help_text = build_parser().format_help()

    assert "backfill" in help_text
    assert "refresh-runs" in help_text


def test_backfill_service_module_exists() -> None:
    assert importlib.util.find_spec("backend.app.market.backfill") is not None


def trading_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


class RangeProvider:
    def __init__(self, dates: list[date], fail_once_for: str | None = None) -> None:
        self.dates = dates
        self.fail_once_for = fail_once_for
        self.calls: list[tuple[date, date, tuple[str, ...]]] = []
        self.failures = 0

    def trading_dates(self, start_date: date, end_date: date) -> list[date]:
        return [item for item in self.dates if start_date <= item <= end_date]

    def fetch_range(self, start_date: date, end_date: date, *, symbols):
        symbols = tuple(symbols)
        self.calls.append((start_date, end_date, symbols))
        if self.fail_once_for in symbols and self.failures == 0:
            self.failures += 1
            raise RuntimeError("controlled provider failure")
        selected_dates = [
            item for item in self.dates if start_date <= item <= end_date
        ]
        bars = []
        for trade_date in selected_dates:
            for symbol in symbols:
                bars.append(
                    bar(trade_date, 10).model_copy(
                        update={
                            "symbol": symbol,
                            "exchange": symbol[:2],
                            "source_record_id": f"fixture:{symbol}:{trade_date}",
                            "ingested_at": datetime(2026, 1, 1, tzinfo=UTC),
                        }
                    )
                )
        return ProviderRangeBatch(
            bars=bars,
            trading_dates=selected_dates,
            expected_symbols=list(symbols),
            failed_symbols=[],
        )


class PartialRangeProvider(RangeProvider):
    def fetch_range(self, start_date: date, end_date: date, *, symbols):
        result = super().fetch_range(start_date, end_date, symbols=symbols)
        return ProviderRangeBatch(
            bars=result.bars[:1],
            trading_dates=result.trading_dates,
            expected_symbols=result.expected_symbols,
            failed_symbols=[],
        )


def test_strategy_minimum_history_window_counts_cross_previous_day() -> None:
    minimum = getattr(backfill, "minimum_effective_days", None)

    assert callable(minimum)
    assert minimum(combined_rule().ast) == 21


def test_backfill_plan_chunks_symbols_and_dates_with_hard_batch_cap(
    tmp_path: Path,
) -> None:
    dates = trading_days(date(2026, 1, 1), 60)
    provider = RangeProvider(dates)
    service_type = getattr(backfill, "BackfillService", None)

    assert service_type is not None
    service = service_type(MarketStore(tmp_path / "market.duckdb"), provider)
    plan = service.plan(
        start_date=dates[0],
        end_date=dates[-1],
        symbols=["sh.600000", "sz.000001"],
        symbol_batch_size=1,
        date_batch_size=20,
        max_batches=6,
    )

    assert len(plan.trading_dates) == 60
    assert plan.total_batches == 6
    assert plan.requested_points == 120
    assert plan.estimated_provider_requests == 18
    with pytest.raises(ValueError, match="batch cap"):
        service.plan(
            start_date=dates[0],
            end_date=dates[-1],
            symbols=["sh.600000", "sz.000001"],
            symbol_batch_size=1,
            date_batch_size=20,
            max_batches=5,
        )


def test_backfill_is_resumable_audited_and_reports_coverage(tmp_path: Path) -> None:
    dates = trading_days(date(2026, 1, 1), 2)
    provider = RangeProvider(dates, fail_once_for="sz.000001")
    service_type = getattr(backfill, "BackfillService", None)

    assert service_type is not None
    service = service_type(
        MarketStore(tmp_path / "market.duckdb"),
        provider,
        sleep_fn=lambda _: None,
    )
    plan = service.plan(
        start_date=dates[0],
        end_date=dates[-1],
        symbols=["sh.600000", "sz.000001"],
        symbol_batch_size=1,
        date_batch_size=2,
        max_batches=2,
    )

    first = service.execute(plan, min_request_interval_seconds=0)
    second = service.execute(plan, min_request_interval_seconds=0)

    assert first.status == "partial"
    assert first.completed_batches == 1
    assert first.failed_batches == 1
    assert first.coverage_ratio == pytest.approx(0.5)
    assert second.run_id == first.run_id == plan.run_id
    assert second.status == "ready"
    assert second.completed_batches == 2
    assert second.failed_batches == 0
    assert second.coverage_ratio == 1
    assert len(provider.calls) == 3
    assert service.audit.get_run(plan.run_id) == second


def test_partial_batch_reports_loaded_point_coverage(tmp_path: Path) -> None:
    dates = trading_days(date(2026, 1, 1), 2)
    service_type = getattr(backfill, "BackfillService", None)

    assert service_type is not None
    service = service_type(
        MarketStore(tmp_path / "market.duckdb"),
        PartialRangeProvider(dates),
        sleep_fn=lambda _: None,
    )
    plan = service.plan(
        start_date=dates[0],
        end_date=dates[-1],
        symbols=["sh.600000"],
        symbol_batch_size=1,
        date_batch_size=2,
        max_batches=1,
    )

    result = service.execute(plan, min_request_interval_seconds=0)

    assert result.status == "partial"
    assert result.loaded_points == 1
    assert result.coverage_ratio == pytest.approx(0.5)


def test_effective_window_uses_calendar_and_can_target_60_days() -> None:
    dates = trading_days(date(2026, 1, 1), 80)
    provider = RangeProvider(dates)
    resolver = getattr(backfill, "resolve_effective_window", None)

    assert callable(resolver)
    selected = resolver(provider, end_date=dates[-1], effective_days=60)

    assert selected == dates[-60:]
    assert len(selected) == 60


def test_all_main_board_refresh_defaults_to_network_free_dry_run(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "refresh",
            "--date",
            "2026-07-23",
            "--all-main-board",
        ],
    )

    def unexpected_refresh(*args, **kwargs):
        raise AssertionError("all-main-board refresh must require explicit confirmation")

    monkeypatch.setattr(cli.MarketRefreshService, "refresh", unexpected_refresh)

    assert cli.main() == 0
    output = capsys.readouterr().out
    assert '"status": "dry-run"' in output
    assert '"scope": "all-main-board"' in output


def test_backfill_calendar_failure_returns_safe_cli_error(
    monkeypatch,
    capsys,
) -> None:
    class CalendarFailure:
        def __init__(self, **kwargs) -> None:
            pass

        def trading_dates(self, start_date, end_date):
            raise RuntimeError("private provider detail")

    monkeypatch.setattr(cli, "BaoStockProvider", CalendarFailure)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "backfill",
            "--symbols",
            "sh.600000",
            "--end",
            "2026-07-23",
            "--effective-days",
            "60",
        ],
    )

    assert cli.main() == 1
    output = capsys.readouterr().out
    assert '"status": "error"' in output
    assert '"quality_issues": ["calendar_or_plan_error"]' in output
    assert "private provider detail" not in output
