from datetime import date
from pathlib import Path

from backend.app.market.store import MarketStore


class CountingMarketStore(MarketStore):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.connection_count = 0

    def _connect(self):
        self.connection_count += 1
        return super()._connect()


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


def test_symbol_range_query_uses_one_connection_for_multi_year_dataset(
    tmp_path: Path,
) -> None:
    store = CountingMarketStore(tmp_path / "market.duckdb")
    seed_multi_year_market(store)
    store.connection_count = 0

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
    store.connection_count = 0

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
