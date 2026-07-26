import sqlite3
from datetime import date
from pathlib import Path

import pytest

from backend.app.market.baostock import DAILY_FIELDS, BaoStockProvider
from backend.app.market.factor_cache import AdjustmentFactorCache, FactorCacheError

FACTOR_FIELDS = [
    "code",
    "dividOperateDate",
    "foreAdjustFactor",
    "backAdjustFactor",
    "adjustFactor",
]
STOCKS = ("sh.600000", "sz.000001")
INDEXES = ("sh.000001", "sz.399001")


class Result:
    def __init__(self, fields, rows, *, error_code="0") -> None:
        self.fields = fields
        self._rows = iter(rows)
        self.error_code = error_code
        self.error_msg = "synthetic failure" if error_code != "0" else ""
        self._current = None

    def next(self):
        self._current = next(self._rows, None)
        return self._current is not None

    def get_row_data(self):
        return self._current


def daily_row(trade_date: date, symbol: str) -> list[str]:
    return [
        trade_date.isoformat(),
        symbol,
        "10",
        "11",
        "9",
        "10",
        "9.5",
        "100",
        "1000",
        "3",
        "1",
        "1",
        "5.26",
        "0",
    ]


class FactorClient:
    def __init__(
        self,
        trade_date: date,
        *,
        bootstrap_factors: dict[str, float | None],
        daily_events: dict[date, dict[str, float]] | None = None,
        failed_once: set[str] | None = None,
        trading_dates: list[date] | None = None,
    ) -> None:
        self.trade_date = trade_date
        self.bootstrap_factors = bootstrap_factors
        self.daily_events = daily_events or {}
        self.failed_once = set(failed_once or ())
        self.trading_date_values = trading_dates or [trade_date]
        self.adjust_calls: list[tuple[str, str]] = []
        self.daily_factor_calls: list[date] = []

    def login(self):
        return Result([], [])

    def logout(self):
        return Result([], [])

    def query_trade_dates(self, **kwargs):
        start = date.fromisoformat(kwargs["start_date"])
        end = date.fromisoformat(kwargs["end_date"])
        rows = [
            [item.isoformat(), "1"] for item in self.trading_date_values if start <= item <= end
        ]
        return Result(["calendar_date", "is_trading_day"], rows)

    def query_all_stock(self, **kwargs):
        return Result(
            ["code", "tradeStatus", "code_name"],
            [[symbol, "1", symbol] for symbol in STOCKS],
        )

    def query_daily_history_k_AStock(self, **kwargs):
        target = date.fromisoformat(kwargs["date"])
        return Result(
            DAILY_FIELDS.split(","),
            [daily_row(target, symbol) for symbol in STOCKS],
        )

    def query_daily_adjust_factor(self, **kwargs):
        target = date.fromisoformat(kwargs["date"])
        self.daily_factor_calls.append(target)
        rows = [
            [symbol, target.isoformat(), str(factor), "1", "1"]
            for symbol, factor in self.daily_events.get(target, {}).items()
        ]
        return Result(FACTOR_FIELDS, rows)

    def query_adjust_factor(self, symbol, **kwargs):
        self.adjust_calls.append((symbol, kwargs["end_date"]))
        if symbol in self.failed_once:
            self.failed_once.remove(symbol)
            return Result(FACTOR_FIELDS, [], error_code="100")
        factor = self.bootstrap_factors.get(symbol)
        rows = [[symbol, "2020-01-01", str(factor), "1", "1"]] if factor is not None else []
        return Result(FACTOR_FIELDS, rows)

    def query_history_k_data_plus(self, symbol, fields, **kwargs):
        target = date.fromisoformat(kwargs["start_date"])
        return Result(fields.split(","), [daily_row(target, symbol)])


def provider(client: FactorClient, cache: Path) -> BaoStockProvider:
    return BaoStockProvider(
        client=client,
        factor_cache_path=str(cache),
        max_attempts=1,
        min_request_interval_seconds=0,
    )


def stock_factors(batch) -> dict[str, float | None]:
    return {bar.symbol: bar.adjust_factor for bar in batch.bars if bar.symbol in STOCKS}


def test_first_full_refresh_bootstraps_each_symbol_and_resumes_from_sqlite(
    tmp_path: Path,
) -> None:
    target = date(2026, 7, 23)
    cache = tmp_path / "factors.sqlite3"
    first_client = FactorClient(
        target,
        bootstrap_factors={"sh.600000": 1.0, "sz.000001": 1.1},
        failed_once={"sz.000001"},
    )

    first = provider(first_client, cache).fetch(target)

    assert stock_factors(first) == {"sh.600000": 1.0, "sz.000001": None}
    assert first_client.adjust_calls == [
        ("sh.600000", "2026-07-23"),
        ("sz.000001", "2026-07-23"),
    ]

    retry_client = FactorClient(
        target,
        bootstrap_factors={"sh.600000": 1.0, "sz.000001": 1.1},
    )
    retry = provider(retry_client, cache).fetch(target)

    assert stock_factors(retry) == {"sh.600000": 1.0, "sz.000001": 1.1}
    assert retry_client.adjust_calls == [("sz.000001", "2026-07-23")]


def test_next_session_uses_daily_events_and_carries_unaffected_factor(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 7, 23)
    next_date = date(2026, 7, 24)
    cache = tmp_path / "factors.sqlite3"
    first_client = FactorClient(
        first_date,
        bootstrap_factors={"sh.600000": 1.0, "sz.000001": 1.1},
    )
    provider(first_client, cache).fetch(first_date)
    next_client = FactorClient(
        next_date,
        bootstrap_factors={"sh.600000": 99.0, "sz.000001": 99.0},
        daily_events={next_date: {"sz.000001": 1.25}},
        trading_dates=[next_date],
    )

    batch = provider(next_client, cache).fetch(next_date)

    assert stock_factors(batch) == {"sh.600000": 1.0, "sz.000001": 1.25}
    assert next_client.adjust_calls == []
    assert next_client.daily_factor_calls == [next_date]


def test_forward_gap_queries_each_missing_trading_session_before_carry(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 7, 23)
    middle_date = date(2026, 7, 24)
    target = date(2026, 7, 27)
    cache = tmp_path / "factors.sqlite3"
    provider(
        FactorClient(
            first_date,
            bootstrap_factors={"sh.600000": 1.0, "sz.000001": 1.1},
        ),
        cache,
    ).fetch(first_date)
    client = FactorClient(
        target,
        bootstrap_factors={"sh.600000": 99.0, "sz.000001": 99.0},
        daily_events={middle_date: {"sh.600000": 1.2}},
        trading_dates=[middle_date, target],
    )

    batch = provider(client, cache).fetch(target)

    assert client.daily_factor_calls == [middle_date, target]
    assert client.adjust_calls == []
    assert stock_factors(batch) == {"sh.600000": 1.2, "sz.000001": 1.1}
    assert AdjustmentFactorCache(cache).stream_through() == target


def test_historical_backfill_bootstraps_once_then_uses_point_in_time_daily_events(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "factors.sqlite3"
    cache = AdjustmentFactorCache(cache_path)
    observed_on = date(2026, 7, 24)
    cache.record_bootstrap(
        "sh.600000",
        observed_on,
        FACTOR_FIELDS,
        [
            ["sh.600000", "2020-01-01", "99.0", "1", "1"],
        ],
    )
    cache.record_bootstrap(
        "sz.000001",
        observed_on,
        FACTOR_FIELDS,
        [["sz.000001", "2020-01-01", "99.0", "1", "1"]],
    )
    cache.record_daily_events(observed_on, FACTOR_FIELDS, [], advance_stream=True)
    first = date(2025, 7, 1)
    second = date(2025, 7, 2)
    client = FactorClient(
        second,
        bootstrap_factors={"sh.600000": 1.0, "sz.000001": 1.1},
        daily_events={second: {"sh.600000": 1.2}},
        trading_dates=[first, second],
    )
    historical_provider = provider(client, cache_path)

    first_batch = historical_provider.fetch(first)
    second_batch = historical_provider.fetch(second)

    assert stock_factors(first_batch) == {"sh.600000": 1.0, "sz.000001": 1.1}
    assert stock_factors(second_batch) == {"sh.600000": 1.2, "sz.000001": 1.1}
    assert client.daily_factor_calls == [first, second]
    assert client.adjust_calls == [
        ("sh.600000", first.isoformat()),
        ("sz.000001", first.isoformat()),
    ]
    with sqlite3.connect(cache_path) as connection:
        evidence = connection.execute(
            """
            SELECT evidence_kind, evidence_effective_date, evidence_observed_on
            FROM factor_snapshots
            WHERE symbol = 'sh.600000' AND trade_date = ?
            """,
            (second.isoformat(),),
        ).fetchone()
    assert evidence == ("daily_event", second.isoformat(), second.isoformat())


def test_historical_carry_requires_a_complete_daily_event_observation(
    tmp_path: Path,
) -> None:
    cache = AdjustmentFactorCache(tmp_path / "factors.sqlite3")
    first = date(2025, 7, 1)
    second = date(2025, 7, 2)
    stream_start = date(2026, 7, 24)
    cache.record_bootstrap(
        "sh.600000",
        first,
        FACTOR_FIELDS,
        [["sh.600000", "2020-01-01", "1.0", "1", "1"]],
    )
    cache.record_daily_events(stream_start, FACTOR_FIELDS, [], advance_stream=True)

    assert cache.materialize_from_stream(["sh.600000"], second) == {}

    cache.record_daily_events(second, FACTOR_FIELDS, [], advance_stream=False)

    assert cache.materialize_from_stream(["sh.600000"], second) == {"sh.600000": 1.0}


def test_newer_cache_is_never_reused_for_an_older_target(tmp_path: Path) -> None:
    newer = date(2026, 7, 24)
    older = date(2026, 7, 23)
    cache = tmp_path / "factors.sqlite3"
    provider(
        FactorClient(
            newer,
            bootstrap_factors={"sh.600000": 1.5, "sz.000001": 1.6},
        ),
        cache,
    ).fetch(newer)
    old_client = FactorClient(
        older,
        bootstrap_factors={"sh.600000": 1.0, "sz.000001": 1.1},
    )

    batch = provider(old_client, cache).fetch(older)

    assert stock_factors(batch) == {"sh.600000": 1.0, "sz.000001": 1.1}
    assert old_client.adjust_calls == [
        ("sh.600000", "2026-07-23"),
        ("sz.000001", "2026-07-23"),
    ]


def test_empty_authoritative_factor_stays_missing_instead_of_guessing_one(
    tmp_path: Path,
) -> None:
    target = date(2026, 7, 23)
    client = FactorClient(
        target,
        bootstrap_factors={"sh.600000": 1.0, "sz.000001": None},
    )

    batch = provider(client, tmp_path / "factors.sqlite3").fetch(target)

    missing = next(bar for bar in batch.bars if bar.symbol == "sz.000001")
    assert missing.adjust_factor is None
    assert missing.quality_status == "partial"
    assert "missing_adjust_factor" in missing.quality_issues


@pytest.mark.parametrize("invalid_factor", ["nan", "inf", "-inf"])
def test_bootstrap_rejects_every_non_finite_factor(
    tmp_path: Path,
    invalid_factor: str,
) -> None:
    cache = AdjustmentFactorCache(tmp_path / "factors.sqlite3")

    with pytest.raises(FactorCacheError, match="finite and positive"):
        cache.record_bootstrap(
            "sh.600000",
            date(2026, 7, 23),
            FACTOR_FIELDS,
            [["sh.600000", "2020-01-01", invalid_factor, "1", "1"]],
        )

    assert cache.exact_snapshots(["sh.600000"], date(2026, 7, 23)) == {}


@pytest.mark.parametrize("invalid_factor", ["nan", "inf", "-inf"])
def test_daily_event_rejects_every_non_finite_factor_without_advancing_stream(
    tmp_path: Path,
    invalid_factor: str,
) -> None:
    cache = AdjustmentFactorCache(tmp_path / "factors.sqlite3")
    target = date(2026, 7, 23)

    with pytest.raises(FactorCacheError, match="finite and positive"):
        cache.record_daily_events(
            target,
            FACTOR_FIELDS,
            [["sh.600000", target.isoformat(), invalid_factor, "1", "1"]],
            advance_stream=True,
        )

    assert cache.stream_through() is None
    assert cache.exact_snapshots(["sh.600000"], target) == {}
