import asyncio
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx

from backend.app.api.market import get_market_store
from backend.app.main import app
from backend.app.market.automation import get_market_clock
from backend.app.market.calendar import SHANGHAI
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.service import MarketSummaryService
from backend.app.market.store import MarketStore

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "baostock_daily.json"


def fixture_payload() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text())


def canonical_fixture():
    payload = fixture_payload()
    return normalize_baostock_rows(
        fields=payload["daily_fields"],
        rows=payload["daily_rows"],
        factor_fields=payload["factor_fields"],
        factor_rows=payload["factor_rows"],
        ingested_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )


def test_baostock_normalization_preserves_raw_prices_and_marks_suspension() -> None:
    rows = canonical_fixture()
    normal = next(row for row in rows if row.symbol == "sh.600000")
    suspended = next(row for row in rows if row.symbol == "sh.600001")

    assert normal.close == 10.5
    assert normal.price_adjustment == "none"
    assert normal.adjust_factor == 1.25
    assert normal.source == "baostock"
    assert normal.is_suspended is False

    assert suspended.is_suspended is True
    assert suspended.is_trading is False
    assert suspended.quality_status == "partial"
    assert "suspended_placeholder" in suspended.quality_issues


def test_duckdb_upsert_is_idempotent(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    rows = canonical_fixture()
    result = RefreshResult(
        run_id="fixture-run",
        requested_date=date(2026, 7, 23),
        source="baostock",
        status="ready",
        requested_count=len(rows),
        succeeded_count=len(rows),
        failed_symbols=[],
        started_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
    )

    store.save_refresh(rows, result)
    store.save_refresh(rows, result)

    assert store.count_bars() == len(rows)
    assert store.latest_refresh().run_id == "fixture-run"


def test_market_summary_reports_breadth_indexes_and_partial_state(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    rows = canonical_fixture()
    result = RefreshResult(
        run_id="partial-fixture",
        requested_date=date(2026, 7, 23),
        source="baostock",
        status="partial",
        requested_count=6,
        succeeded_count=5,
        failed_symbols=["sz.000002"],
        started_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
    )
    store.save_refresh(rows, result)

    summary = MarketSummaryService(store).latest()

    assert summary.status == "partial"
    assert summary.as_of == date(2026, 7, 23)
    assert summary.source == "baostock"
    assert summary.completeness.requested == 6
    assert summary.completeness.loaded == 5
    assert summary.completeness.failed_symbols == ["sz.000002"]
    assert summary.breadth.advancing == 1
    assert summary.breadth.declining == 1
    assert summary.breadth.suspended == 1
    assert summary.turnover.amount == 33_700
    assert [item.symbol for item in summary.indexes] == ["sh.000001", "sz.399001"]


def test_market_summary_api_exposes_partial_semantics(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    rows = canonical_fixture()
    result = RefreshResult(
        run_id="api-fixture",
        requested_date=date(2026, 7, 23),
        source="baostock",
        status="partial",
        requested_count=6,
        succeeded_count=5,
        failed_symbols=["sz.000002"],
        started_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
    )
    store.save_refresh(rows, result)
    app.dependency_overrides[get_market_store] = lambda: store
    app.dependency_overrides[get_market_clock] = lambda: (
        lambda: datetime(2026, 7, 24, 10, tzinfo=SHANGHAI)
    )

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/market/summary")

    try:
        response = asyncio.run(send())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["status"] == "partial"
    assert response.json()["completeness"]["failed_symbols"] == ["sz.000002"]
    assert len(response.json()["indexes"]) == 2


def test_empty_market_store_is_explicit(tmp_path: Path) -> None:
    summary = MarketSummaryService(MarketStore(tmp_path / "market.duckdb")).latest()

    assert summary.status == "empty"
    assert summary.as_of is None
    assert summary.source is None
    assert summary.completeness.loaded == 0
    assert summary.indexes == []


def test_failed_attempt_does_not_replace_last_published_snapshot(tmp_path: Path) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    rows = canonical_fixture()
    ready = RefreshResult(
        run_id="stale-fixture",
        requested_date=date(2026, 7, 23),
        source="baostock",
        status="ready",
        requested_count=len(rows),
        succeeded_count=len(rows),
        started_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
    )
    store.save_refresh(rows, ready)

    stale = MarketSummaryService(store).latest(expected_session=date(2026, 7, 24))

    assert stale.status == "stale"
    assert "data_older_than_expected" in stale.quality_issues

    failed = ready.model_copy(
        update={
            "run_id": "failed-fixture",
            "status": "error",
            "succeeded_count": 0,
            "error_message": "provider unavailable",
            "completed_at": datetime(2026, 7, 24, 11, 1, tzinfo=UTC),
        }
    )
    store.save_refresh([], failed)

    preserved = MarketSummaryService(store).latest()

    assert preserved.as_of == date(2026, 7, 23)
    assert preserved.completeness.loaded == len(rows)
    assert len(preserved.indexes) == 2
    assert store.latest_refresh().status == "error"
    assert store.published_refresh().run_id == "stale-fixture"
