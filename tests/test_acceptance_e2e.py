import importlib.util
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore


def bar(
    trade_date: date,
    symbol: str,
    close: float,
    *,
    volume: float,
) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        symbol=symbol,
        security_type="stock",
        exchange=symbol[:2],
        board="main",
        open=close,
        high=close,
        low=close,
        close=close,
        preclose=close,
        volume=volume,
        amount=close * volume,
        turnover_rate=1,
        pct_change=0,
        adjust_factor=1,
        is_trading=True,
        is_suspended=False,
        is_st=False,
        source_record_id=f"acceptance:{symbol}:{trade_date}",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        quality_status="ready",
    )


def market_with_candidate(tmp_path: Path) -> MarketStore:
    store = MarketStore(tmp_path / "market.duckdb")
    start = date(2026, 1, 1)
    for offset in range(21):
        trade_date = start + timedelta(days=offset)
        bars = [
            bar(
                trade_date,
                "sh.600000",
                30 if offset == 20 else 10,
                volume=200 if offset == 20 else 100,
            ),
            bar(trade_date, "sz.000001", 10, volume=100),
        ]
        store.save_refresh(
            bars,
            RefreshResult(
                run_id=f"acceptance-{trade_date}",
                request_key=f"acceptance:{trade_date}",
                requested_date=trade_date,
                source="baostock",
                status="ready",
                requested_count=2,
                succeeded_count=2,
                coverage_ratio=1,
                started_at=datetime(2026, 1, 1, tzinfo=UTC),
                completed_at=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            publish=True,
        )
    return store


class ReadOnlyMarket:
    def __init__(self, store: MarketStore) -> None:
        self.store = store

    def exists(self):
        return self.store.exists()

    def published_refresh(self):
        return self.store.published_refresh()

    def available_dates(self, source="baostock"):
        return self.store.available_dates(source)

    def canonical_bars(self, trade_date, source="baostock"):
        return self.store.canonical_bars(trade_date, source)

    def symbols_bars(self, symbols, start, end, source="baostock"):
        return self.store.symbols_bars(symbols, start, end, source)


def test_acceptance_uses_temporary_user_data_and_replays_idempotently(
    tmp_path: Path,
) -> None:
    assert importlib.util.find_spec("backend.app.acceptance") is not None
    from backend.app.acceptance import run_real_e2e_acceptance

    real_user_database = tmp_path / "real-user.sqlite3"
    real_user_database.write_bytes(b"do-not-touch")
    report = run_real_e2e_acceptance(
        ReadOnlyMarket(market_with_candidate(tmp_path)),
        temporary_parent=tmp_path,
    )

    assert report.status == "ready"
    assert report.as_of_date == date(2026, 1, 21)
    assert report.strategy_run_reused is True
    assert report.alert_evaluation_reused is True
    assert report.candidate_symbols == ["sh.600000"]
    assert report.alert_state == "triggered"
    assert report.portfolio_coverage == "1/1"
    assert report.user_database_persisted is False
    assert "规则候选" in report.disclaimer
    assert "投资建议" in report.disclaimer
    assert real_user_database.read_bytes() == b"do-not-touch"
    assert list(tmp_path.glob("stock-eva-real-e2e-*")) == []
