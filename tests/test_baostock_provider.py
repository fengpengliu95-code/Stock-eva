import json
from datetime import date
from pathlib import Path

from backend.app.market.baostock import BaoStockProvider

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "baostock_daily.json"


class FakeResult:
    def __init__(self, fields, rows, error_code="0", error_msg="") -> None:
        self.fields = fields
        self.rows = iter(rows)
        self.error_code = error_code
        self.error_msg = error_msg
        self.current = None

    def next(self) -> bool:
        self.current = next(self.rows, None)
        return self.current is not None

    def get_row_data(self):
        return self.current


class FakeBaoStock:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.logged_out = False

    def login(self):
        return FakeResult([], [])

    def logout(self):
        self.logged_out = True

    def query_history_k_data_plus(self, code, fields, **kwargs):
        names = fields.split(",")
        rows = [
            row
            for row in self.payload["daily_rows"]
            if row[self.payload["daily_fields"].index("code")] == code
        ]
        positions = [self.payload["daily_fields"].index(name) for name in names]
        return FakeResult(names, [[row[position] for position in positions] for row in rows])

    def query_adjust_factor(self, code, **kwargs):
        rows = [row for row in self.payload["factor_rows"] if row[0] == code]
        return FakeResult(self.payload["factor_fields"], rows)

    def query_daily_history_k_AStock(self, **kwargs):
        return FakeResult(self.payload["daily_fields"], self.payload["daily_rows"])

    def query_daily_adjust_factor(self, **kwargs):
        target = kwargs["date"]
        date_index = self.payload["factor_fields"].index("dividOperateDate")
        rows = [row for row in self.payload["factor_rows"] if row[date_index] == target]
        return FakeResult(self.payload["factor_fields"], rows)

    def query_trade_dates(self, **kwargs):
        return FakeResult(
            ["calendar_date", "is_trading_day"],
            [["2026-07-23", "1"]],
        )

    def query_all_stock(self, **kwargs):
        code_index = self.payload["daily_fields"].index("code")
        return FakeResult(
            ["code", "tradeStatus", "code_name"],
            [[row[code_index], "1", row[code_index]] for row in self.payload["daily_rows"]],
        )


def test_provider_logs_out_and_returns_canonical_explicit_slice() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    client = FakeBaoStock(payload)
    provider = BaoStockProvider(client=client)

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000", "sh.000001"])

    assert [bar.symbol for bar in batch.bars] == ["sh.600000", "sh.000001"]
    assert batch.failed_symbols == []
    assert batch.bars[0].adjust_factor == 1.25
    assert client.logged_out is True


def test_provider_bulk_slice_filters_main_board_and_adds_indexes() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    provider = BaoStockProvider(client=FakeBaoStock(payload))

    batch = provider.fetch(date(2026, 7, 23))

    assert len([bar for bar in batch.bars if bar.security_type == "stock"]) == 3
    assert [bar.symbol for bar in batch.bars if bar.security_type == "index"] == [
        "sh.000001",
        "sz.399001",
    ]


def test_universe_inspection_reads_metadata_without_fetching_market_rows() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())

    class MetadataOnlyClient(FakeBaoStock):
        def query_daily_history_k_AStock(self, **kwargs):
            raise AssertionError("universe inspection must not fetch daily rows")

        def query_daily_adjust_factor(self, **kwargs):
            raise AssertionError("universe inspection must not fetch factors")

        def query_history_k_data_plus(self, *args, **kwargs):
            raise AssertionError("universe inspection must not fetch indexes")

    provider = BaoStockProvider(client=MetadataOnlyClient(payload))

    inspection = provider.inspect_main_board(date(2026, 7, 23))

    assert inspection.main_board_count == 3
    assert inspection.shanghai_count == 2
    assert inspection.shenzhen_count == 1
    assert inspection.total_expected_count == 5
    assert inspection.metadata_provider_requests == 2
    assert inspection.main_board_symbols == (
        "sh.600000",
        "sh.600001",
        "sz.000001",
    )


def test_provider_applies_minimum_interval_to_every_operation() -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    clock = [0.0]
    sleeps: list[float] = []

    def monotonic() -> float:
        return clock[0]

    def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    provider = BaoStockProvider(
        client=FakeBaoStock(payload),
        min_request_interval_seconds=0.5,
        monotonic_fn=monotonic,
        sleep_fn=sleeper,
    )

    provider.inspect_main_board(date(2026, 7, 23))

    assert sleeps == [0.5]
