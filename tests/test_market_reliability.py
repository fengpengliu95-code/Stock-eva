import asyncio
import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import httpx
import pytest

from backend.app.api.market import get_market_store
from backend.app.main import app
from backend.app.market.automation import run_publication_refresh
from backend.app.market.baostock import BaoStockError, BaoStockProvider, ProviderBatch
from backend.app.market.continuity import repair_request_key
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.refresh import MarketRefreshService
from backend.app.market.series import DataQualityError, PriceSeriesService
from backend.app.market.service import MarketSummaryService
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "baostock_daily.json"


def fixture_bars():
    payload = json.loads(FIXTURE_PATH.read_text())
    return normalize_baostock_rows(
        fields=payload["daily_fields"],
        rows=payload["daily_rows"],
        factor_fields=payload["factor_fields"],
        factor_rows=payload["factor_rows"],
        ingested_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )


class FixedBatchProvider:
    """A provider-shaped fixture that always returns one captured batch."""

    def __init__(self, batch: ProviderBatch) -> None:
        trade_dates = {bar.trade_date for bar in batch.bars}
        if not trade_dates:
            raise ValueError("fixed provider batch must contain bars")
        if len(trade_dates) != 1:
            raise ValueError("fixed provider batch must contain one trade date")
        self.batch = batch
        self.trade_date = trade_dates.pop()

    def fetch(self, trade_date, symbols=None):
        assert trade_date == self.trade_date
        assert symbols is None
        return self.batch


def immutable_dataset_store(tmp_path: Path) -> tuple[NasMarketStore, Path]:
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )
    return (
        NasMarketStore(
            MarketStore(tmp_path / "control" / "market.duckdb"),
            root,
            tmp_path / "staging",
        ),
        root,
    )


@pytest.fixture
def incident_provider() -> FixedBatchProvider:
    """Reproduce the 2026-08-11 full-universe suspension-factor incident shape."""
    fields = [
        "date",
        "code",
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "volume",
        "amount",
        "adjustflag",
        "turn",
        "tradestatus",
        "pctChg",
        "isST",
    ]
    rows = [
        [
            "2026-08-11",
            "sh.600984",
            "0",
            "0",
            "0",
            "0",
            "0",
            "",
            "",
            "3",
            "",
            "0",
            "",
            "0",
        ],
        [
            "2026-08-11",
            "sh.603221",
            "0",
            "0",
            "0",
            "0",
            "0",
            "",
            "",
            "3",
            "",
            "0",
            "",
            "0",
        ],
        [
            "2026-08-11",
            "sh.600000",
            "10.00",
            "10.60",
            "9.90",
            "10.50",
            "10.00",
            "1000",
            "10200",
            "3",
            "1.20",
            "1",
            "5.00",
            "0",
        ],
        [
            "2026-08-11",
            "sh.000001",
            "3500.00",
            "3520.00",
            "3480.00",
            "3510.00",
            "3490.00",
            "300000",
            "450000000",
            "3",
            "",
            "1",
            "0.57",
            "0",
        ],
        [
            "2026-08-11",
            "sz.399001",
            "11000.00",
            "11050.00",
            "10800.00",
            "10900.00",
            "11020.00",
            "400000",
            "550000000",
            "3",
            "",
            "1",
            "-1.09",
            "0",
        ],
    ]
    bars = normalize_baostock_rows(
        fields=fields,
        rows=rows,
        factor_fields=[
            "code",
            "dividOperateDate",
            "foreAdjustFactor",
            "backAdjustFactor",
            "adjustFactor",
        ],
        factor_rows=[["sh.600000", "2026-01-01", "1", "1", "1"]],
        ingested_at=datetime(2026, 8, 11, 20, 0, tzinfo=UTC),
    )
    expected_symbols = [bar.symbol for bar in bars]
    return FixedBatchProvider(
        ProviderBatch(bars=bars, expected_symbols=expected_symbols, failed_symbols=[])
    )


class Result:
    def __init__(self, fields, rows, error_code="0", error_msg="") -> None:
        self.fields = fields
        self.rows = iter(rows)
        self.error_code = error_code
        self.error_msg = error_msg
        self.current = None

    def next(self):
        self.current = next(self.rows, None)
        return self.current is not None

    def get_row_data(self):
        return self.current


class ClosedDayClient:
    history_calls = 0

    def login(self):
        return Result([], [])

    def logout(self):
        return None

    def query_trade_dates(self, **kwargs):
        return Result(["calendar_date", "is_trading_day"], [["2026-07-25", "0"]])

    def query_history_k_data_plus(self, *args, **kwargs):
        self.history_calls += 1
        raise AssertionError("closed dates must not query bars")


def test_provider_rejects_non_trading_day_before_querying_bars() -> None:
    client = ClosedDayClient()

    with pytest.raises(BaoStockError, match="not a trading day"):
        BaoStockProvider(client=client).fetch(date(2026, 7, 25), symbols=["sh.600000"])

    assert client.history_calls == 0


def test_provider_bounds_explicit_symbol_requests() -> None:
    provider = BaoStockProvider(client=ClosedDayClient(), max_explicit_symbols=2)

    with pytest.raises(BaoStockError, match="at most 2"):
        provider.fetch(
            date(2026, 7, 23),
            symbols=["sh.600000", "sz.000001", "sh.000001"],
        )


class FailingSymbolClient(ClosedDayClient):
    def query_trade_dates(self, **kwargs):
        return Result(["calendar_date", "is_trading_day"], [["2026-07-23", "1"]])

    def query_history_k_data_plus(self, *args, **kwargs):
        self.history_calls += 1
        return Result([], [], error_code="1001", error_msg="temporary failure")


def test_provider_retries_each_request_at_most_twice_then_degrades() -> None:
    client = FailingSymbolClient()
    provider = BaoStockProvider(
        client=client,
        max_attempts=2,
        min_request_interval_seconds=0,
    )

    batch = provider.fetch(date(2026, 7, 23), symbols=["sh.600000"])

    assert client.history_calls == 2
    assert batch.expected_symbols == ["sh.600000"]
    assert batch.bars == []
    assert batch.failed_symbols == ["sh.600000"]


class RangeClient(ClosedDayClient):
    def __init__(self) -> None:
        self.login_calls = 0
        self.history_calls = 0
        self.factor_calls = 0

    def login(self):
        self.login_calls += 1
        return Result([], [])

    def query_trade_dates(self, **kwargs):
        return Result(
            ["calendar_date", "is_trading_day"],
            [
                ["2026-07-21", "1"],
                ["2026-07-22", "0"],
                ["2026-07-23", "1"],
            ],
        )

    def query_history_k_data_plus(self, symbol, fields, **kwargs):
        self.history_calls += 1
        return Result(
            fields.split(","),
            [
                [
                    "2026-07-21",
                    symbol,
                    "10",
                    "10",
                    "10",
                    "10",
                    "10",
                    "100",
                    "1000",
                    "3",
                    "1",
                    "1",
                    "0",
                    "0",
                ],
                [
                    "2026-07-23",
                    symbol,
                    "11",
                    "11",
                    "11",
                    "11",
                    "10",
                    "120",
                    "1320",
                    "3",
                    "1",
                    "1",
                    "10",
                    "0",
                ],
            ],
        )

    def query_adjust_factor(self, symbol, **kwargs):
        self.factor_calls += 1
        return Result(
            [
                "code",
                "dividOperateDate",
                "foreAdjustFactor",
                "backAdjustFactor",
                "adjustFactor",
            ],
            [[symbol, "2026-01-01", "1", "1", "1"]],
        )


def test_provider_fetches_date_range_with_one_history_request_per_symbol() -> None:
    client = RangeClient()
    provider = BaoStockProvider(client=client, min_request_interval_seconds=0)

    assert callable(getattr(provider, "fetch_range", None))
    batch = provider.fetch_range(
        date(2026, 7, 21),
        date(2026, 7, 23),
        symbols=["sh.600000", "sz.000001"],
    )

    assert batch.trading_dates == [date(2026, 7, 21), date(2026, 7, 23)]
    assert len(batch.bars) == 4
    assert batch.failed_symbols == []
    assert client.login_calls == 1
    assert client.history_calls == 2
    assert client.factor_calls == 2


class PartialProvider:
    def __init__(self, bars) -> None:
        self.bars = bars

    def fetch(self, trade_date, symbols=None):
        return ProviderBatch(
            bars=self.bars,
            expected_symbols=["sh.600000", "sz.000001", "sh.000001"],
            failed_symbols=["sh.000001"],
        )


def test_refresh_coverage_uses_expected_universe_and_is_strict(tmp_path: Path) -> None:
    bars = [bar for bar in fixture_bars() if bar.symbol in {"sh.600000", "sz.000001"}]
    store = MarketStore(tmp_path / "market.duckdb")

    result = MarketRefreshService(store, PartialProvider(bars)).refresh(
        date(2026, 7, 23),
        symbols=["sh.600000", "sz.000001", "sh.000001"],
    )

    assert result.status == "partial"
    assert result.requested_count == 3
    assert result.succeeded_count == 2
    assert result.coverage_ratio == pytest.approx(2 / 3)
    summary = MarketSummaryService(store).latest(expected_session=date(2026, 7, 23))
    assert summary.completeness.is_complete is False
    assert summary.completeness.coverage_ratio == pytest.approx(2 / 3)


def test_daily_refresh_run_id_is_idempotent_for_same_target(tmp_path: Path) -> None:
    bars = [bar for bar in fixture_bars() if bar.symbol in {"sh.600000", "sz.000001"}]
    store = MarketStore(tmp_path / "market.duckdb")
    service = MarketRefreshService(store, PartialProvider(bars))

    first = service.refresh(
        date(2026, 7, 23),
        symbols=["sz.000001", "sh.600000"],
    )
    second = service.refresh(
        date(2026, 7, 23),
        symbols=["sh.600000", "sz.000001"],
    )

    assert first.run_id == second.run_id
    assert first.request_key == second.request_key
    assert first.run_kind == second.run_kind == "daily"


def save_fixture(store: MarketStore, bars, run_id: str, trade_date: date) -> None:
    store.save_refresh(
        bars,
        RefreshResult(
            run_id=run_id,
            requested_date=trade_date,
            source="baostock",
            status="ready",
            requested_count=len(bars),
            succeeded_count=len(bars),
            coverage_ratio=1.0,
            started_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
            completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
        ),
    )


def test_suspended_rows_without_factor_can_form_a_complete_publication(
    tmp_path: Path,
    incident_provider: FixedBatchProvider,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")

    result = run_publication_refresh(
        store,
        incident_provider,
        trade_date=date(2026, 8, 11),
        required_symbols={"sh.600000"},
    )

    assert result.quality_issues == []
    assert result.status == "ready"
    assert store.published_refresh().run_id == result.run_id


@pytest.mark.parametrize("field", ["volume", "amount"])
def test_suspended_placeholder_with_nonzero_activity_is_rejected(field: str) -> None:
    suspended = next(bar for bar in fixture_bars() if bar.symbol == "sh.600001").model_copy(
        update={field: 1.0}
    )

    assert MarketStore.publication_bar_quality_issue(suspended) == "invalid_suspended_placeholder"


def test_suspended_index_placeholder_is_rejected() -> None:
    suspended_index = next(bar for bar in fixture_bars() if bar.symbol == "sh.000001").model_copy(
        update={
            "is_trading": False,
            "is_suspended": True,
            "quality_status": "partial",
            "quality_issues": ["suspended_placeholder"],
            "volume": 0.0,
            "amount": 0.0,
        }
    )

    assert (
        MarketStore.publication_bar_quality_issue(suspended_index)
        == "invalid_suspended_placeholder"
    )


def test_factor_is_required_for_every_non_suspended_stock(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE_PATH.read_text())
    active_without_factor = normalize_baostock_rows(
        fields=payload["daily_fields"],
        rows=[payload["daily_rows"][0]],
        ingested_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )[0]
    assert active_without_factor.quality_issues == ["missing_adjust_factor"]

    bars = [
        active_without_factor if bar.symbol == active_without_factor.symbol else bar
        for bar in fixture_bars()
    ]
    store = MarketStore(tmp_path / "market.duckdb")
    provider = FixedBatchProvider(
        ProviderBatch(
            bars=bars,
            expected_symbols=[bar.symbol for bar in bars],
            failed_symbols=[],
        )
    )

    result = run_publication_refresh(
        store,
        provider,
        trade_date=date(2026, 7, 23),
        required_symbols={"sh.600000"},
    )

    assert result.status == "partial"
    assert "missing_adjust_factor:sh.600000" in result.quality_issues
    assert store.published_refresh() is None


def test_active_row_without_factor_still_blocks_publication(
    tmp_path: Path,
    incident_provider: FixedBatchProvider,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    active_missing_factor = [
        bar.model_copy(update={"adjust_factor": None})
        if bar.is_trading and bar.security_type == "stock"
        else bar
        for bar in incident_provider.batch.bars
    ]
    provider = FixedBatchProvider(
        ProviderBatch(
            bars=active_missing_factor,
            expected_symbols=incident_provider.batch.expected_symbols,
            failed_symbols=[],
        )
    )

    result = run_publication_refresh(
        store,
        provider,
        trade_date=date(2026, 8, 11),
        required_symbols={"sh.600000"},
    )

    assert result.status == "partial"
    assert "missing_adjust_factor:sh.600000" in result.quality_issues
    assert store.published_refresh() is None


def test_failed_incident_candidate_preserves_existing_pointer_and_objects(
    tmp_path: Path,
    incident_provider: FixedBatchProvider,
) -> None:
    store, dataset_root = immutable_dataset_store(tmp_path)
    baseline = fixture_bars()
    baseline_provider = FixedBatchProvider(
        ProviderBatch(
            bars=baseline,
            expected_symbols=[bar.symbol for bar in baseline],
            failed_symbols=[],
        )
    )
    published = run_publication_refresh(
        store,
        baseline_provider,
        trade_date=date(2026, 7, 23),
        required_symbols={"sh.600000"},
    )
    assert published.status == "ready"
    pointer_before = store.published_refresh().model_dump(mode="json")
    manifest_before = (dataset_root / "manifest.json").read_bytes()
    manifest = json.loads(manifest_before)
    objects_before = {
        item["path"]: (dataset_root / item["path"]).read_bytes() for item in manifest["files"]
    }
    assert objects_before
    assert set(objects_before) == {item["path"] for item in manifest["files"]}
    active_missing_factor = [
        bar.model_copy(update={"adjust_factor": None})
        if bar.is_trading and bar.security_type == "stock"
        else bar
        for bar in incident_provider.batch.bars
    ]
    failed_provider = FixedBatchProvider(
        ProviderBatch(
            bars=active_missing_factor,
            expected_symbols=incident_provider.batch.expected_symbols,
            failed_symbols=[],
        )
    )

    failed = run_publication_refresh(
        store,
        failed_provider,
        trade_date=date(2026, 8, 11),
        required_symbols={"sh.600000"},
    )

    assert failed.status == "partial"
    assert "missing_adjust_factor:sh.600000" in failed.quality_issues
    assert store.published_refresh().model_dump(mode="json") == pointer_before
    assert (dataset_root / "manifest.json").read_bytes() == manifest_before
    assert {path: (dataset_root / path).read_bytes() for path in objects_before} == objects_before


def test_older_repair_extends_manifest_without_regressing_latest_pointer(
    tmp_path: Path,
) -> None:
    store, dataset_root = immutable_dataset_store(tmp_path)
    latest = date(2026, 8, 11)
    older = date(2026, 8, 10)
    latest_provider = FixedBatchProvider(
        ProviderBatch(
            bars=[bar.model_copy(update={"trade_date": latest}) for bar in fixture_bars()],
            expected_symbols=[bar.symbol for bar in fixture_bars()],
            failed_symbols=[],
        )
    )
    older_provider = FixedBatchProvider(
        ProviderBatch(
            bars=[bar.model_copy(update={"trade_date": older}) for bar in fixture_bars()],
            expected_symbols=[bar.symbol for bar in fixture_bars()],
            failed_symbols=[],
        )
    )
    published = run_publication_refresh(
        store,
        latest_provider,
        trade_date=latest,
        required_symbols={"sh.600000"},
    )
    assert published.status == "ready"

    repaired = run_publication_refresh(
        store,
        older_provider,
        trade_date=older,
        required_symbols={"sh.600000"},
        request_key=repair_request_key(older),
        run_id="repair-2026-08-10-attempt-1",
        run_kind="repair",
    )

    assert repaired.status == "ready"
    assert store.published_refresh().requested_date == latest
    assert set(store.verified_ready_session_inventory().sessions) == {older, latest}
    persisted = next(item for item in store.list_refreshes() if item.run_id == repaired.run_id)
    assert persisted.run_kind == "repair"
    assert persisted.request_key == repair_request_key(older)


def test_history_is_queryable_by_date_and_exports_one_parquet_partition(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    bars = fixture_bars()
    save_fixture(store, bars, "partition-fixture", date(2026, 7, 23))

    assert store.available_dates() == [date(2026, 7, 23)]
    assert len(store.canonical_bars(date(2026, 7, 23), source="baostock")) == len(bars)

    output = store.export_date(date(2026, 7, 23), tmp_path / "exports")

    assert output == tmp_path / "exports" / "date=2026-07-23" / "bars.parquet"
    assert duckdb.sql(f"SELECT count(*) FROM read_parquet('{output}')").fetchone()[0] == len(bars)


def test_qfq_series_multiplies_prices_and_excludes_suspensions(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    normal = next(bar for bar in fixture_bars() if bar.symbol == "sh.600000")
    suspended = normal.model_copy(
        update={
            "trade_date": date(2026, 7, 24),
            "is_trading": False,
            "is_suspended": True,
            "adjust_factor": None,
            "volume": 0.0,
            "amount": 0.0,
            "quality_status": "partial",
            "quality_issues": ["suspended_placeholder"],
        }
    )
    save_fixture(store, [normal], "series-one", normal.trade_date)
    save_fixture(store, [suspended], "series-two", suspended.trade_date)

    series = PriceSeriesService(store).read(
        "sh.600000",
        start=date(2026, 7, 23),
        end=date(2026, 7, 24),
        adjustment="qfq",
    )

    assert len(series) == 1
    assert series[0].close == pytest.approx(10.5)
    assert series[0].adjust_factor == pytest.approx(1)


def test_qfq_series_returns_empty_when_all_rows_are_suspended(
    tmp_path: Path,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    suspended = next(bar for bar in fixture_bars() if bar.symbol == "sh.600000").model_copy(
        update={
            "is_trading": False,
            "is_suspended": True,
            "adjust_factor": 1.0,
            "volume": 0.0,
            "amount": 0.0,
            "quality_status": "partial",
            "quality_issues": ["suspended_placeholder"],
        }
    )
    save_fixture(store, [suspended], "all-suspended", suspended.trade_date)

    service = PriceSeriesService(store)
    result = service.read_result(
        suspended.symbol,
        start=suspended.trade_date,
        end=suspended.trade_date,
        adjustment="qfq",
    )
    series = service.read(
        suspended.symbol,
        start=suspended.trade_date,
        end=suspended.trade_date,
        adjustment="qfq",
    )

    assert result.points == []
    assert result.empty_reason == "no_effective_trading_data"
    assert series == []


def test_qfq_series_normalizes_cumulative_back_factor_to_requested_end(
    tmp_path: Path,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    first = next(bar for bar in fixture_bars() if bar.symbol == "sh.600000").model_copy(
        update={"adjust_factor": 1.0}
    )
    second = first.model_copy(
        update={
            "trade_date": date(2026, 7, 24),
            "close": 12.0,
            "preclose": 10.0,
            "adjust_factor": 1.2,
        }
    )
    save_fixture(store, [first], "back-factor-one", first.trade_date)
    save_fixture(store, [second], "back-factor-two", second.trade_date)

    series = PriceSeriesService(store).read(
        "sh.600000",
        start=first.trade_date,
        end=second.trade_date,
        adjustment="qfq",
    )

    assert [point.adjust_factor for point in series] == pytest.approx([1 / 1.2, 1])
    assert [point.close for point in series] == pytest.approx([10.5 / 1.2, 12])
    assert series[0].volume == 1000
    assert series[0].price_adjustment == "qfq"
    assert series[0].source == "baostock"


def test_qfq_series_refuses_missing_factor(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    normal = next(bar for bar in fixture_bars() if bar.symbol == "sh.600000")
    missing = normal.model_copy(update={"adjust_factor": None})
    save_fixture(store, [missing], "missing-factor", normal.trade_date)

    with pytest.raises(DataQualityError, match="missing adjust_factor"):
        PriceSeriesService(store).read(
            "sh.600000",
            start=normal.trade_date,
            end=normal.trade_date,
            adjustment="qfq",
        )


def test_history_api_exposes_dates_and_deterministic_qfq_series(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    normal = next(bar for bar in fixture_bars() if bar.symbol == "sh.600000")
    save_fixture(store, [normal], "history-api", normal.trade_date)
    app.dependency_overrides[get_market_store] = lambda: store

    async def send(path: str) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    try:
        dates = asyncio.run(send("/api/v1/market/history/dates"))
        series = asyncio.run(
            send("/api/v1/market/history/sh.600000?start=2026-07-23&end=2026-07-23&adjustment=qfq")
        )
    finally:
        app.dependency_overrides.clear()

    assert dates.status_code == 200
    assert dates.json() == ["2026-07-23"]
    assert series.status_code == 200
    assert series.json()[0]["close"] == pytest.approx(10.5)
    assert series.json()[0]["price_adjustment"] == "qfq"


def test_freshness_is_not_claimed_without_backend_expected_session(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    bars = fixture_bars()
    save_fixture(store, bars, "freshness-fixture", date(2026, 7, 23))

    unevaluated = MarketSummaryService(store).latest()
    fresh = MarketSummaryService(store).latest(expected_session=date(2026, 7, 23))

    assert unevaluated.status == "partial"
    assert unevaluated.freshness.evaluated is False
    assert "freshness_not_evaluated" in unevaluated.quality_issues
    assert fresh.status == "ready"
    assert fresh.freshness.is_fresh is True
