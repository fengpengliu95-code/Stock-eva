from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from backend.app.market.models import DailyBar
from backend.app.market.store import MarketStore
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.engine import StrategyEngine


class ProbedConnection:
    def __init__(self, connection, owner: "CountingMarketStore") -> None:
        self.connection = connection
        self.owner = owner
        self.cursor = connection

    def execute(self, query, parameters=None):
        self.owner.execute_count += 1
        self.cursor = (
            self.connection.execute(query)
            if parameters is None
            else self.connection.execute(query, parameters)
        )
        return self

    def executemany(self, query, parameters):
        self.owner.executemany_count += 1
        self.cursor = self.connection.executemany(query, parameters)
        return self

    def fetchall(self):
        rows = self.cursor.fetchall()
        self.owner.fetched_rows += len(rows)
        return rows

    def fetchone(self):
        return self.cursor.fetchone()

    @property
    def description(self):
        return self.cursor.description

    def __getattr__(self, name):
        return getattr(self.connection, name)


class CountingMarketStore(MarketStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.connection_count = 0
        self.execute_count = 0
        self.executemany_count = 0
        self.fetched_rows = 0

    def _connect(self):
        self.connection_count += 1
        return ProbedConnection(super()._connect(), self)

    def _connect_reader(self):
        self.connection_count += 1
        connection = super()._connect_reader()
        return None if connection is None else ProbedConnection(connection, self)

    def reset_probe(self) -> None:
        self.connection_count = 0
        self.execute_count = 0
        self.executemany_count = 0
        self.fetched_rows = 0


def seed_multi_year_market(store: MarketStore) -> None:
    connection = store._connect()
    try:
        connection.execute(
            """
            INSERT INTO daily_bars
            SELECT
                DATE '2023-01-02' + CAST(day_index * 7 AS INTEGER),
                'sh.' || lpad(CAST(600000 + symbol_index AS VARCHAR), 6, '0'),
                'stock',
                'sh',
                'main',
                10,
                11,
                9,
                10.5 + day_index / 1000,
                10,
                1000,
                10500,
                1.0,
                5.0,
                1.25,
                'none',
                true,
                false,
                false,
                'baostock',
                'synthetic:' || day_index || ':' || symbol_index,
                TIMESTAMPTZ '2026-07-24 00:00:00+00',
                'ready',
                '[]'
            FROM range(0, 156) days(day_index)
            CROSS JOIN range(0, 200) symbols(symbol_index)
            """
        )
    finally:
        connection.close()


def positive_close_rule():
    return validate_rule(
        {
            "op": "compare",
            "left": {"type": "field", "name": "close"},
            "operator": ">",
            "right": {"type": "constant", "value": 0},
        }
    )


def synthetic_bars() -> list[DailyBar]:
    result: list[DailyBar] = []
    for day_index in range(156):
        trade_date = date(2023, 1, 2) + timedelta(days=day_index * 7)
        for symbol_index in range(20):
            symbol = f"sh.{600000 + symbol_index:06d}"
            result.append(
                DailyBar(
                    trade_date=trade_date,
                    symbol=symbol,
                    security_type="stock",
                    exchange="sh",
                    board="main",
                    open=10,
                    high=11,
                    low=9,
                    close=10.5,
                    preclose=10,
                    volume=1000,
                    amount=10500,
                    turnover_rate=1,
                    pct_change=5,
                    adjust_factor=1,
                    price_adjustment="none",
                    is_trading=True,
                    is_suspended=False,
                    is_st=False,
                    source="baostock",
                    source_record_id=f"synthetic:{day_index}:{symbol_index}",
                    ingested_at=datetime(2026, 7, 24, tzinfo=UTC),
                    quality_status="ready",
                    quality_issues=[],
                )
            )
    return result


def test_symbol_range_query_uses_one_connection_for_multi_year_dataset(
    tmp_path: Path,
) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    seed_multi_year_market(store)
    store.reset_probe()

    bars = store.symbol_bars(
        "sh.600042",
        date(2023, 1, 2),
        date(2025, 12, 31),
        source="baostock",
    )

    assert store.connection_count == 1
    assert len(bars) == 156
    assert {bar.symbol for bar in bars} == {"sh.600042"}
    assert bars[0].trade_date == date(2023, 1, 2)
    assert bars[-1].trade_date == date(2025, 12, 22)
    assert all(bar.quality_status == "ready" for bar in bars)
    assert all(bar.quality_issues == [] for bar in bars)


def test_symbol_range_query_pushes_inclusive_dates_into_store_filter(
    tmp_path: Path,
) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    seed_multi_year_market(store)
    store.reset_probe()

    bars = store.symbol_bars(
        "sh.600042",
        date(2024, 1, 1),
        date(2024, 1, 31),
        source="baostock",
    )

    assert store.connection_count == 1
    assert [bar.trade_date for bar in bars] == [
        date(2024, 1, 1),
        date(2024, 1, 8),
        date(2024, 1, 15),
        date(2024, 1, 22),
        date(2024, 1, 29),
    ]


def test_multi_symbol_range_query_is_bounded_and_uses_one_query(
    tmp_path: Path,
) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    seed_multi_year_market(store)
    store.reset_probe()
    symbols = [f"sh.{600000 + index:06d}" for index in range(20)]

    grouped = store.symbols_bars(
        symbols,
        date(2023, 1, 2),
        date(2025, 12, 31),
        source="baostock",
    )

    assert store.connection_count == 1
    assert store.execute_count == 1
    assert store.fetched_rows == 156 * 20
    assert list(grouped) == symbols
    assert all(len(grouped[symbol]) == 156 for symbol in symbols)
    assert all(bar.trade_date <= date(2025, 12, 31) for bars in grouped.values() for bar in bars)
    with pytest.raises(ValueError, match="at most 200"):
        store.symbols_bars(
            [f"sh.{index:06d}" for index in range(201)],
            date(2023, 1, 2),
            date(2025, 12, 31),
        )


def test_strategy_batches_multi_symbol_history_without_changing_results(
    tmp_path: Path,
) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    seed_multi_year_market(store)
    store.reset_probe()
    symbols = [f"sh.{600000 + index:06d}" for index in range(20)]

    evaluation = StrategyEngine(store).evaluate(
        positive_close_rule(),
        symbols=symbols,
        as_of_date=date(2025, 12, 22),
    )

    assert store.connection_count == 2
    assert len(evaluation.results) == 20
    assert all(result.status == "matched" for result in evaluation.results)
    assert all(result.matched is True for result in evaluation.results)
    assert evaluation.data_fingerprint == (
        "dfaee1163953054ba1620eacfc29662782e91766238c3281fbc798121e1af525"
    )


def test_strategy_chunks_large_symbol_sets_instead_of_querying_each_symbol(
    tmp_path: Path,
) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    seed_multi_year_market(store)
    store.reset_probe()
    symbols = [f"sh.{600000 + index:06d}" for index in range(201)]

    evaluation = StrategyEngine(store).evaluate(
        positive_close_rule(),
        symbols=symbols,
        as_of_date=date(2025, 12, 22),
    )

    assert store.connection_count == 3  # one calendar query and two bounded history batches
    assert len(evaluation.results) == 201
    assert sum(result.matched is True for result in evaluation.results) == 200
    assert evaluation.results[-1].quality_issues == ["missing_signal_date"]


def test_present_points_filters_exact_sets_inside_duckdb(tmp_path: Path) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    seed_multi_year_market(store)
    store.reset_probe()
    dates = [date(2023, 1, 2), date(2025, 12, 22)]
    symbols = ["sh.600000", "sh.600042"]

    points = store.present_points(dates, symbols)

    assert points == {
        (date(2023, 1, 2), "sh.600000"),
        (date(2023, 1, 2), "sh.600042"),
        (date(2025, 12, 22), "sh.600000"),
        (date(2025, 12, 22), "sh.600042"),
    }
    assert store.connection_count == 1
    assert store.execute_count == 1
    assert store.fetched_rows == 4


def test_canonical_upsert_uses_one_set_based_statement_and_remains_idempotent(
    tmp_path: Path,
) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    bars = synthetic_bars()
    store.reset_probe()

    store.upsert_bars(bars)

    assert store.connection_count == 1
    assert store.execute_count == 1
    assert store.executemany_count == 0
    assert store.count_bars() == len(bars)

    store.reset_probe()
    store.upsert_bars(bars)

    assert store.connection_count == 1
    assert store.execute_count == 1
    assert store.executemany_count == 0
    assert store.count_bars() == len(bars)


def test_canonical_batch_upsert_preserves_last_duplicate_row(tmp_path: Path) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    original = synthetic_bars()[0]
    replacement = original.model_copy(
        update={"close": 99, "source_record_id": "synthetic:replacement"}
    )
    store.reset_probe()

    store.upsert_bars([original, replacement])

    assert store.execute_count == 1
    stored = store.symbol_bars(
        original.symbol,
        original.trade_date,
        original.trade_date,
    )
    assert len(stored) == 1
    assert stored[0].close == 99
    assert stored[0].source_record_id == "synthetic:replacement"
