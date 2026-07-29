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
        open=close - 0.5,
        high=close + 1,
        low=close - 1,
        close=close,
        preclose=close - 1,
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

    response = request(store, analysis_path(end=START))

    assert response.status_code == 200
    assert response.json() == {
        "symbol": SYMBOL,
        "status": "empty",
        "as_of": None,
        "source": "baostock",
        "price_adjustment": "qfq",
        "formula_version": "ta-lib-0.7.0-r0-v1",
        "quality_issues": ["no_market_data"],
        "series": [],
    }


def test_missing_adjust_factor_fails_closed() -> None:
    store = RecordingMarketStore([bar(0, 10, factor=None)])

    response = request(store, analysis_path(end=START))

    assert response.status_code == 409
    assert response.json()["detail"] == f"{SYMBOL} {START} missing adjust_factor"


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
