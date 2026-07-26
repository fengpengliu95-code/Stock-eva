import asyncio
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path

import httpx

from backend.app.api.market import get_market_store
from backend.app.api.strategy import get_strategy_store
from backend.app.main import app
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.run import StrategyRunService
from backend.app.strategy.store import StrategyStore

SIGNAL_DATE = date(2026, 7, 23)


def positive_close_rule() -> dict[str, object]:
    return {
        "op": "compare",
        "left": {"type": "field", "name": "close"},
        "operator": ">",
        "right": {"type": "constant", "value": 0},
    }


def stock_bar(symbol: str) -> DailyBar:
    return DailyBar(
        trade_date=SIGNAL_DATE,
        symbol=symbol,
        security_type="stock",
        exchange=symbol[:2],
        board="main",
        open=10,
        high=10,
        low=10,
        close=10,
        preclose=10,
        volume=100,
        amount=1_000,
        turnover_rate=1,
        pct_change=0,
        adjust_factor=1,
        is_trading=True,
        is_suspended=False,
        is_st=False,
        source_record_id=f"fixture:{symbol}:{SIGNAL_DATE}",
        ingested_at=datetime(2026, 7, 23, 10, tzinfo=UTC),
        quality_status="ready",
    )


def published_refresh() -> RefreshResult:
    return RefreshResult(
        run_id="published-all-main-board",
        request_key="daily:baostock:2026-07-23:all-main-board",
        requested_date=SIGNAL_DATE,
        source="baostock",
        status="ready",
        requested_count=403,
        succeeded_count=403,
        coverage_ratio=1,
        started_at=datetime(2026, 7, 23, 10, tzinfo=UTC),
        completed_at=datetime(2026, 7, 23, 10, 1, tzinfo=UTC),
    )


class PublishedMarket:
    def __init__(
        self,
        symbols: list[str],
        *,
        fail_batch_start: str | None = None,
    ) -> None:
        self._symbols = symbols
        self.fail_batch_start = fail_batch_start
        self.batch_sizes: list[int] = []

    def published_refresh(self) -> RefreshResult:
        return published_refresh()

    def canonical_bars(self, trade_date: date, source: str = "baostock"):
        assert trade_date == SIGNAL_DATE
        assert source == "baostock"
        return [stock_bar(symbol) for symbol in self._symbols]

    def available_dates(self, source: str = "baostock") -> list[date]:
        assert source == "baostock"
        return [SIGNAL_DATE]

    def symbols_bars(
        self,
        symbols: list[str],
        start: date,
        end: date,
        source: str = "baostock",
    ) -> dict[str, list[DailyBar]]:
        assert start == SIGNAL_DATE
        assert end == SIGNAL_DATE
        assert source == "baostock"
        self.batch_sizes.append(len(symbols))
        if symbols[0] == self.fail_batch_start:
            raise OSError("private filesystem detail must not escape")
        return {symbol: [stock_bar(symbol)] for symbol in symbols}


def main_board_symbols(count: int) -> list[str]:
    return [f"sh.{600000 + index:06d}" for index in range(count)]


def make_strategy(store: StrategyStore):
    return store.create_strategy(
        "全市场正收盘价校验",
        validate_rule(positive_close_rule()),
    )


def test_all_main_board_scan_batches_and_persists_progress(tmp_path: Path) -> None:
    symbols = main_board_symbols(401)
    market = PublishedMarket(symbols)
    store = StrategyStore(tmp_path / "user.sqlite3")
    strategy, version = make_strategy(store)

    run = StrategyRunService(store, market).run_all_main_board(
        strategy.id,
        version=version.version,
    )
    stored = store.get_run(run.id)

    assert market.batch_sizes == [200, 200, 1]
    assert run.scope == "all_main_board"
    assert run.as_of_date == SIGNAL_DATE
    assert run.status == "completed"
    assert run.total_symbols == 401
    assert run.batch_size == 200
    assert run.total_batches == 3
    assert run.completed_batches == 3
    assert run.failed_batches == 0
    assert run.failed_symbols == []
    assert len(run.results) == 401
    assert all(result.status == "matched" for result in run.results)
    assert [batch.status for batch in run.batches] == [
        "completed",
        "completed",
        "completed",
    ]
    assert stored == run


def test_all_main_board_scan_isolates_failed_batch_without_leaking_error(
    tmp_path: Path,
) -> None:
    symbols = main_board_symbols(401)
    market = PublishedMarket(symbols, fail_batch_start=symbols[200])
    store = StrategyStore(tmp_path / "user.sqlite3")
    strategy, version = make_strategy(store)

    run = StrategyRunService(store, market).run_all_main_board(
        strategy.id,
        version=version.version,
    )

    assert market.batch_sizes == [200, 200, 1]
    assert run.status == "partial"
    assert run.completed_batches == 2
    assert run.failed_batches == 1
    assert run.failed_symbols == symbols[200:400]
    assert len(run.results) == 201
    failed = run.batches[1]
    assert failed.status == "error"
    assert failed.error_code == "batch_evaluation_failed"
    assert "filesystem" not in run.model_dump_json()


def test_all_main_board_scan_persists_all_batches_failed_status(tmp_path: Path) -> None:
    symbols = main_board_symbols(200)
    market = PublishedMarket(symbols, fail_batch_start=symbols[0])
    store = StrategyStore(tmp_path / "user.sqlite3")
    strategy, version = make_strategy(store)

    created = StrategyRunService(store, market).run_all_main_board(
        strategy.id,
        version=version.version,
    )
    stored = store.get_run(created.id)

    assert stored.status == "error"
    assert stored.completed_batches == 0
    assert stored.failed_batches == 1
    assert stored.failed_symbols == symbols


def test_all_main_board_scan_reuses_caller_idempotency_key(tmp_path: Path) -> None:
    symbols = main_board_symbols(201)
    market = PublishedMarket(symbols)
    store = StrategyStore(tmp_path / "user.sqlite3")
    strategy, version = make_strategy(store)
    service = StrategyRunService(store, market)

    first = service.run_all_main_board(
        strategy.id,
        version=version.version,
        idempotency_key="after-close:2026-07-23:strategy-v1",
    )
    second = service.run_all_main_board(
        strategy.id,
        version=version.version,
        idempotency_key="after-close:2026-07-23:strategy-v1",
    )

    assert first.id == second.id
    assert first.idempotency_key == "after-close:2026-07-23:strategy-v1"
    assert market.batch_sizes == [200, 1]
    assert len(store.list_runs(strategy.id)) == 1


def test_strategy_store_migrates_existing_run_metadata_for_idempotency(
    tmp_path: Path,
) -> None:
    database = tmp_path / "user.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE strategy_run_metadata (
            run_id TEXT PRIMARY KEY,
            scope TEXT NOT NULL,
            total_symbols INTEGER NOT NULL,
            batch_size INTEGER NOT NULL,
            total_batches INTEGER NOT NULL,
            completed_batches INTEGER NOT NULL,
            failed_batches INTEGER NOT NULL,
            failed_symbols TEXT NOT NULL,
            batches TEXT NOT NULL
        )
        """
    )
    connection.commit()
    connection.close()

    StrategyStore(database).list_strategies()

    connection = sqlite3.connect(database)
    try:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(strategy_run_metadata)").fetchall()
        }
    finally:
        connection.close()
    assert "idempotency_key" in columns


def test_all_main_board_api_is_explicit_and_uses_published_session(
    tmp_path: Path,
) -> None:
    symbols = main_board_symbols(201)
    market = PublishedMarket(symbols)
    store = StrategyStore(tmp_path / "user.sqlite3")
    strategy, version = make_strategy(store)
    app.dependency_overrides[get_strategy_store] = lambda: store
    app.dependency_overrides[get_market_store] = lambda: market

    async def request(method: str, path: str, **kwargs) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    try:
        response = asyncio.run(
            request(
                "POST",
                f"/api/v1/strategies/{strategy.id}/runs/all-main-board",
                json={"version": version.version},
            )
        )
        rejected_client_scope = asyncio.run(
            request(
                "POST",
                f"/api/v1/strategies/{strategy.id}/runs/all-main-board",
                json={
                    "version": version.version,
                    "as_of_date": SIGNAL_DATE.isoformat(),
                    "symbols": [symbols[0]],
                },
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201
    assert response.json()["scope"] == "all_main_board"
    assert response.json()["as_of_date"] == SIGNAL_DATE.isoformat()
    assert response.json()["total_symbols"] == 201
    assert response.json()["total_batches"] == 2
    assert rejected_client_scope.status_code == 422
