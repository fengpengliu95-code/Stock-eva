import asyncio
from datetime import UTC, date, datetime, timedelta

import httpx
import pytest

from backend.app.api.market import get_market_store
from backend.app.main import app
from backend.app.market.models import DailyBar

SYMBOL = "sh.600000"
START = date(2025, 1, 1)


class RecordingMarketStore:
    def __init__(self, bars: list[DailyBar]) -> None:
        self.bars = bars
        self.queries: list[tuple[str, date, date, str]] = []

    def symbol_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        source: str = "baostock",
    ) -> list[DailyBar]:
        self.queries.append((symbol, start, end, source))
        return [
            bar
            for bar in self.bars
            if bar.symbol == symbol and start <= bar.trade_date <= end and bar.source == source
        ]


def bar(
    index: int,
    close: float,
    *,
    factor: float | None = 1.0,
    suspended: bool = False,
) -> DailyBar:
    trade_date = START + timedelta(days=index)
    return DailyBar(
        trade_date=trade_date,
        symbol=SYMBOL,
        security_type="stock",
        exchange="sh",
        board="main",
        open=max(close - 0.5, close / 2),
        high=close + 1,
        low=max(close - 1, close / 2),
        close=close,
        preclose=max(close - 1, close / 2),
        volume=1_000 + index,
        amount=(1_000 + index) * close,
        turnover_rate=1,
        pct_change=0,
        adjust_factor=factor,
        is_trading=not suspended,
        is_suspended=suspended,
        is_st=False,
        source_record_id=f"fixture:{SYMBOL}:{trade_date}",
        ingested_at=datetime(2025, 1, 1, tzinfo=UTC),
        quality_status="partial" if suspended else "ready",
        quality_issues=["suspended_placeholder"] if suspended else [],
    )


def request(store: RecordingMarketStore, path: str) -> httpx.Response:
    app.dependency_overrides[get_market_store] = lambda: store

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    try:
        return asyncio.run(send())
    finally:
        app.dependency_overrides.clear()


def analysis_path(
    *,
    symbol: str = SYMBOL,
    start: date = START,
    end: date = START + timedelta(days=259),
) -> str:
    return f"/api/v1/securities/{symbol}/analysis?start={start}&end={end}"


def test_analysis_contract_and_fixed_indicator_goldens() -> None:
    store = RecordingMarketStore([bar(index, index + 1) for index in range(260)])

    response = request(store, analysis_path())

    assert response.status_code == 200
    payload = response.json()
    assert set(payload) == {
        "symbol",
        "status",
        "as_of",
        "source",
        "price_adjustment",
        "formula_version",
        "quality_issues",
        "series",
    }
    assert payload["symbol"] == SYMBOL
    assert payload["status"] == "ready"
    assert payload["as_of"] == str(START + timedelta(days=259))
    assert payload["source"] == "baostock"
    assert payload["price_adjustment"] == "qfq"
    assert payload["formula_version"] == "ta-lib-0.7.0-r0-v1"
    assert payload["quality_issues"] == []
    assert len(payload["series"]) == 260

    series = payload["series"]
    assert set(series[-1]) == {
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "ma120",
        "ma250",
        "macd",
        "macd_signal",
        "macd_hist",
        "rsi14",
    }
    assert series[3]["ma5"] is None
    assert series[4]["ma5"] == pytest.approx(3)
    assert series[8]["ma10"] is None
    assert series[9]["ma10"] == pytest.approx(5.5)
    assert series[248]["ma250"] is None
    assert series[249]["ma250"] == pytest.approx(125.5)
    assert series[13]["rsi14"] is None
    assert series[14]["rsi14"] == pytest.approx(100)
    assert series[32]["macd"] is None
    assert series[33]["macd"] == pytest.approx(7)
    assert series[-1]["macd_signal"] == pytest.approx(7)
    assert series[-1]["macd_hist"] == pytest.approx(0, abs=1e-12)


def test_moving_averages_equal_independent_trailing_means() -> None:
    closes = [10 + ((index * 7) % 13) / 4 for index in range(80)]
    store = RecordingMarketStore([bar(index, close) for index, close in enumerate(closes)])

    response = request(
        store,
        analysis_path(end=START + timedelta(days=len(closes) - 1)),
    )

    assert response.status_code == 200
    series = response.json()["series"]
    for period in (5, 10, 20, 60):
        field = f"ma{period}"
        for index, point in enumerate(series):
            if index < period - 1:
                assert point[field] is None
            else:
                expected = sum(closes[index - period + 1 : index + 1]) / period
                assert point[field] == pytest.approx(expected)


def test_qfq_prices_use_adjust_factors_and_suspensions_are_excluded() -> None:
    store = RecordingMarketStore(
        [
            bar(0, 10, factor=1),
            bar(1, 99, factor=1.5, suspended=True),
            bar(2, 10, factor=2),
        ]
    )

    response = request(store, analysis_path(end=START + timedelta(days=2)))

    assert response.status_code == 200
    series = response.json()["series"]
    assert [point["trade_date"] for point in series] == [
        str(START),
        str(START + timedelta(days=2)),
    ]
    assert series[0]["open"] == pytest.approx(4.75)
    assert series[0]["high"] == pytest.approx(5.5)
    assert series[0]["low"] == pytest.approx(4.5)
    assert series[0]["close"] == pytest.approx(5)
    assert series[0]["volume"] == 1_000
    assert series[0]["amount"] == 10_000


def test_end_is_a_hard_as_of_boundary() -> None:
    end = START + timedelta(days=39)
    original = [bar(index, index + 1) for index in range(40)]
    store = RecordingMarketStore(original + [bar(40, 10_000)])

    response = request(store, analysis_path(end=end))

    assert response.status_code == 200
    payload = response.json()
    assert payload["as_of"] == str(end)
    assert max(point["trade_date"] for point in payload["series"]) == str(end)
    assert store.queries == [(SYMBOL, START, end, "baostock")]


def test_empty_window_has_explainable_response() -> None:
    store = RecordingMarketStore([])

    history = request(
        store,
        f"/api/v1/market/history/{SYMBOL}?start={START}&end={START}&adjustment=qfq",
    )
    unadjusted_history = request(
        store,
        f"/api/v1/market/history/{SYMBOL}?start={START}&end={START}&adjustment=none",
    )
    analysis = request(store, analysis_path(end=START))

    assert history.status_code == 409
    assert history.json()["detail"] == f"{SYMBOL} {START} no market data"
    assert unadjusted_history.status_code == 200
    assert unadjusted_history.json() == []
    assert analysis.status_code == 200
    assert analysis.json() == {
        "symbol": SYMBOL,
        "status": "empty",
        "as_of": None,
        "source": "baostock",
        "price_adjustment": "qfq",
        "formula_version": "ta-lib-0.7.0-r0-v1",
        "quality_issues": ["no_market_data"],
        "series": [],
    }


def test_all_suspended_history_compatibility_and_analysis_empty() -> None:
    store = RecordingMarketStore([bar(0, 10, factor=1, suspended=True)])

    qfq_history = request(
        store,
        f"/api/v1/market/history/{SYMBOL}?start={START}&end={START}&adjustment=qfq",
    )
    unadjusted_history = request(
        store,
        f"/api/v1/market/history/{SYMBOL}?start={START}&end={START}&adjustment=none",
    )
    response = request(store, analysis_path(end=START))

    assert qfq_history.status_code == 200
    assert qfq_history.json() == []
    assert unadjusted_history.status_code == 200
    assert unadjusted_history.json() == []
    assert response.status_code == 200
    assert response.json()["status"] == "empty"
    assert response.json()["as_of"] is None
    assert response.json()["quality_issues"] == ["no_effective_trading_data"]
    assert response.json()["series"] == []
    assert len(store.queries) == 3


def test_analysis_all_suspended_missing_factor_is_empty_with_one_query() -> None:
    store = RecordingMarketStore([bar(0, 10, factor=None, suspended=True)])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 200
    assert response.json()["quality_issues"] == ["no_effective_trading_data"]
    assert response.json()["series"] == []
    assert store.queries == [(SYMBOL, START, START, "baostock")]


def test_missing_adjust_factor_fails_closed() -> None:
    store = RecordingMarketStore([bar(0, 10, factor=None)])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 409
    assert response.json()["detail"] == f"{SYMBOL} {START} missing adjust_factor"


@pytest.mark.parametrize("factor", [float("nan"), float("inf"), float("-inf"), 0.0, -1.0])
def test_invalid_adjust_factor_fails_closed(factor: float) -> None:
    store = RecordingMarketStore([bar(0, 10, factor=factor)])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 409
    assert SYMBOL in response.json()["detail"]
    assert str(START) in response.json()["detail"]
    assert "invalid adjust_factor" in response.json()["detail"]


@pytest.mark.parametrize(
    ("first_factor", "last_factor"),
    [(1e308, 1e-308), (1e-308, 1e308)],
)
def test_invalid_qfq_multiplier_fails_closed(
    first_factor: float,
    last_factor: float,
) -> None:
    store = RecordingMarketStore(
        [
            bar(0, 10, factor=first_factor),
            bar(1, 10, factor=last_factor),
        ]
    )

    response = request(store, analysis_path(end=START + timedelta(days=1)))

    assert response.status_code == 409
    assert "invalid qfq multiplier" in response.json()["detail"]


def test_qfq_price_overflow_fails_closed() -> None:
    enormous = bar(0, 1e308, factor=2).model_copy(update={"amount": 1e308})
    store = RecordingMarketStore([enormous, bar(1, 10, factor=1)])

    response = request(store, analysis_path(end=START + timedelta(days=1)))

    assert response.status_code == 409
    assert "invalid qfq open" in response.json()["detail"]


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "preclose"])
@pytest.mark.parametrize("value", [0.0, -1.0])
def test_non_positive_market_price_fails_closed(field: str, value: float) -> None:
    invalid = bar(0, 10).model_copy(update={field: value})
    store = RecordingMarketStore([invalid])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 409
    assert f"invalid {field}" in response.json()["detail"]


def test_qfq_price_underflow_to_zero_fails_closed() -> None:
    tiny = bar(0, 1, factor=5e-324).model_copy(
        update={
            "open": 0.25,
            "high": 0.25,
            "low": 0.25,
            "close": 0.25,
            "preclose": 0.25,
            "amount": 250,
        }
    )
    store = RecordingMarketStore([tiny, bar(1, 10, factor=1)])

    response = request(store, analysis_path(end=START + timedelta(days=1)))

    assert response.status_code == 409
    assert "invalid qfq open=0.0" in response.json()["detail"]


def test_zero_volume_and_amount_are_valid() -> None:
    inactive_values = bar(0, 10).model_copy(update={"volume": 0.0, "amount": 0.0})
    store = RecordingMarketStore([inactive_values])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 200
    assert response.json()["series"][0]["volume"] == 0
    assert response.json()["series"][0]["amount"] == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open", float("nan")),
        ("high", float("inf")),
        ("low", float("-inf")),
        ("close", float("nan")),
        ("preclose", float("inf")),
        ("volume", float("-inf")),
        ("amount", float("inf")),
        ("volume", -1.0),
        ("amount", -1.0),
    ],
)
def test_invalid_market_value_fails_closed(field: str, value: float) -> None:
    invalid = bar(0, 10).model_copy(update={field: value})
    store = RecordingMarketStore([invalid])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 409
    assert SYMBOL in response.json()["detail"]
    assert str(START) in response.json()["detail"]
    assert f"invalid {field}" in response.json()["detail"]


@pytest.mark.parametrize(
    ("updates", "expected_detail"),
    [
        ({"quality_status": "partial"}, "status=partial"),
        ({"quality_status": "error"}, "status=error"),
        ({"quality_issues": ["bad_price_source"]}, "issues=bad_price_source"),
    ],
)
def test_non_ready_effective_bar_fails_closed(
    updates: dict[str, object],
    expected_detail: str,
) -> None:
    invalid = bar(0, 10).model_copy(update=updates)
    store = RecordingMarketStore([invalid])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert SYMBOL in detail
    assert str(START) in detail
    assert "quality not ready" in detail
    assert expected_detail in detail


@pytest.mark.parametrize("symbol", ["600000", "xx.600000", "sh.60000", "SH.600000"])
def test_invalid_symbol_is_rejected_without_querying_store(symbol: str) -> None:
    store = RecordingMarketStore([])

    response = request(store, analysis_path(symbol=symbol, end=START))

    assert response.status_code == 422
    assert store.queries == []


def test_start_after_end_is_rejected_without_querying_store() -> None:
    store = RecordingMarketStore([])

    response = request(
        store,
        analysis_path(
            start=START + timedelta(days=1),
            end=START,
        ),
    )

    assert response.status_code == 422
    assert store.queries == []


def test_start_and_end_are_required_dates() -> None:
    store = RecordingMarketStore([])

    missing = request(store, f"/api/v1/securities/{SYMBOL}/analysis")
    malformed = request(
        store,
        f"/api/v1/securities/{SYMBOL}/analysis?start=2025-01-01&end=not-a-date",
    )

    assert missing.status_code == 422
    assert malformed.status_code == 422
    assert store.queries == []
