from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.engine import StrategyEngine

SYMBOL = "sh.600000"


def bar(
    trade_date: date,
    close: float,
    volume: float = 100,
    *,
    suspended: bool = False,
    factor: float | None = 1.0,
    quality: str = "ready",
) -> DailyBar:
    return DailyBar(
        trade_date=trade_date,
        symbol=SYMBOL,
        security_type="stock",
        exchange="sh",
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
        adjust_factor=factor,
        is_trading=not suspended,
        is_suspended=suspended,
        is_st=False,
        source_record_id=f"fixture:{SYMBOL}:{trade_date}",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        quality_status=quality,
        quality_issues=[] if quality == "ready" else ["fixture_quality_error"],
    )


def save_history(store: MarketStore, bars: list[DailyBar]) -> None:
    for index, item in enumerate(bars):
        store.save_refresh(
            [item],
            RefreshResult(
                run_id=f"strategy-{index}",
                requested_date=item.trade_date,
                source="baostock",
                status="ready",
                requested_count=1,
                succeeded_count=1,
                coverage_ratio=1,
                started_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index),
                completed_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index),
            ),
        )


def combined_rule():
    return validate_rule(
        {
            "op": "and",
            "args": [
                {
                    "op": "crosses_above",
                    "left": {
                        "type": "indicator",
                        "name": "SMA",
                        "field": "close",
                        "period": 5,
                    },
                    "right": {
                        "type": "indicator",
                        "name": "SMA",
                        "field": "close",
                        "period": 20,
                    },
                },
                {
                    "op": "compare",
                    "left": {"type": "indicator", "name": "volume_ratio_5d"},
                    "operator": ">",
                    "right": {"type": "constant", "value": 1.5},
                },
            ],
        }
    )


def crossing_history(start: date) -> list[DailyBar]:
    bars = [bar(start + timedelta(days=index), 10) for index in range(20)]
    bars.append(bar(start + timedelta(days=20), 30, volume=200))
    return bars


def test_cross_and_volume_ratio_use_only_prior_effective_days(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    history = crossing_history(date(2026, 1, 1))
    save_history(store, history)

    evaluation = StrategyEngine(store).evaluate(
        combined_rule(),
        symbols=[SYMBOL],
        as_of_date=history[-1].trade_date,
    )
    result = evaluation.results[0]

    assert result.status == "matched"
    assert result.matched is True
    cross, ratio = result.explanation["children"]
    assert cross["previous"]["left"] == pytest.approx(10)
    assert cross["previous"]["right"] == pytest.approx(10)
    assert cross["current"]["left"] == pytest.approx(14)
    assert cross["current"]["right"] == pytest.approx(11)
    assert ratio["left"]["value"] == pytest.approx(2)
    assert ratio["left"]["window"]["prior_effective_days"] == 5
    assert result.signal_date == history[-1].trade_date
    assert result.quality_status == "ready"


@pytest.mark.parametrize(
    ("updates", "expected_issue"),
    [
        ({"suspended": True}, "suspended_on_signal_date"),
        ({"factor": None}, "missing_adjust_factor"),
        ({"quality": "partial"}, "quality_not_ready"),
    ],
)
def test_signal_date_suspension_factor_and_quality_exclude_symbol(
    tmp_path: Path,
    updates: dict[str, object],
    expected_issue: str,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    history = crossing_history(date(2026, 1, 1))
    last = history[-1]
    history[-1] = bar(
        last.trade_date,
        last.close,
        last.volume,
        suspended=bool(updates.get("suspended", False)),
        factor=updates.get("factor", 1.0),
        quality=str(updates.get("quality", "ready")),
    )
    save_history(store, history)

    result = (
        StrategyEngine(store)
        .evaluate(
            combined_rule(),
            symbols=[SYMBOL],
            as_of_date=last.trade_date,
        )
        .results[0]
    )

    assert result.status == "excluded"
    assert result.matched is None
    assert expected_issue in result.quality_issues


def test_future_rows_do_not_change_as_of_result_or_fingerprint(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    history = crossing_history(date(2026, 1, 1))
    signal_date = history[-1].trade_date
    save_history(store, history)
    engine = StrategyEngine(store)

    before = engine.evaluate(combined_rule(), symbols=[SYMBOL], as_of_date=signal_date)
    save_history(store, [bar(signal_date + timedelta(days=1), 1, volume=10_000)])
    after = engine.evaluate(combined_rule(), symbols=[SYMBOL], as_of_date=signal_date)

    assert before.data_fingerprint == after.data_fingerprint
    assert before.results == after.results


def test_strategy_normalizes_cumulative_back_factors_to_signal_date(
    tmp_path: Path,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    start = date(2026, 1, 1)
    history = [
        bar(start, 10, factor=1.0),
        bar(start + timedelta(days=1), 10, factor=2.0),
    ]
    save_history(store, history)
    rule = validate_rule(
        {
            "op": "compare",
            "left": {
                "type": "indicator",
                "name": "SMA",
                "field": "close",
                "period": 2,
            },
            "operator": "<",
            "right": {"type": "constant", "value": 8},
        }
    )

    result = (
        StrategyEngine(store)
        .evaluate(
            rule,
            symbols=[SYMBOL],
            as_of_date=history[-1].trade_date,
        )
        .results[0]
    )

    assert result.status == "matched"
    assert result.explanation["left"]["value"] == pytest.approx(7.5)


def test_repeated_evaluation_is_reproducible(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    history = crossing_history(date(2026, 1, 1))
    save_history(store, history)
    engine = StrategyEngine(store)

    first = engine.evaluate(
        combined_rule(),
        symbols=[SYMBOL],
        as_of_date=history[-1].trade_date,
    )
    second = engine.evaluate(
        combined_rule(),
        symbols=[SYMBOL],
        as_of_date=history[-1].trade_date,
    )

    assert first.model_dump() == second.model_dump()


def test_crosses_below_uses_previous_greater_or_equal_boundary(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    start = date(2026, 1, 1)
    history = [bar(start + timedelta(days=index), 10) for index in range(20)]
    history.append(bar(start + timedelta(days=20), 0))
    save_history(store, history)
    rule = validate_rule(
        {
            "op": "crosses_below",
            "left": {
                "type": "indicator",
                "name": "SMA",
                "field": "close",
                "period": 5,
            },
            "right": {
                "type": "indicator",
                "name": "SMA",
                "field": "close",
                "period": 20,
            },
        }
    )

    result = (
        StrategyEngine(store)
        .evaluate(
            rule,
            symbols=[SYMBOL],
            as_of_date=history[-1].trade_date,
        )
        .results[0]
    )

    assert result.status == "matched"
    assert result.explanation["previous"]["left"] == pytest.approx(10)
    assert result.explanation["previous"]["right"] == pytest.approx(10)


def test_ema_and_rsi_have_deterministic_effective_day_windows(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    start = date(2026, 1, 1)
    history = [bar(start + timedelta(days=index), 10 + index) for index in range(16)]
    save_history(store, history)
    rule = validate_rule(
        {
            "op": "and",
            "args": [
                {
                    "op": "compare",
                    "left": {
                        "type": "indicator",
                        "name": "EMA",
                        "field": "close",
                        "period": 5,
                    },
                    "operator": ">",
                    "right": {"type": "constant", "value": 20},
                },
                {
                    "op": "compare",
                    "left": {
                        "type": "indicator",
                        "name": "RSI",
                        "field": "close",
                        "period": 14,
                    },
                    "operator": "==",
                    "right": {"type": "constant", "value": 100},
                },
            ],
        }
    )

    result = (
        StrategyEngine(store)
        .evaluate(
            rule,
            symbols=[SYMBOL],
            as_of_date=history[-1].trade_date,
        )
        .results[0]
    )

    assert result.status == "matched"
    ema, rsi = result.explanation["children"]
    assert ema["left"]["window"]["effective_days"] == 16
    assert rsi["left"]["window"]["effective_days"] == 15
