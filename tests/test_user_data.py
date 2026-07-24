import asyncio
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from backend.app.api.market import get_market_store
from backend.app.api.user import get_user_store
from backend.app.main import app
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.store import MarketStore
from backend.app.user.models import PositionCreate, PositionUpdate
from backend.app.user.portfolio import PortfolioValuationService
from backend.app.user.store import ConflictError, UserStore

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "baostock_daily.json"


def canonical_fixture():
    payload = json.loads(FIXTURE_PATH.read_text())
    return normalize_baostock_rows(
        fields=payload["daily_fields"],
        rows=payload["daily_rows"],
        factor_fields=payload["factor_fields"],
        factor_rows=payload["factor_rows"],
        ingested_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )


def ready_market_store(path: Path) -> MarketStore:
    store = MarketStore(path)
    bars = canonical_fixture()
    store.save_refresh(
        bars,
        RefreshResult(
            run_id="portfolio-market",
            requested_date=date(2026, 7, 23),
            source="baostock",
            status="ready",
            requested_count=len(bars),
            succeeded_count=len(bars),
            coverage_ratio=1.0,
            started_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
            completed_at=datetime(2026, 7, 24, 10, 1, tzinfo=UTC),
        ),
    )
    return store


def position(symbol: str = "sh.600000") -> PositionCreate:
    return PositionCreate(
        symbol=symbol,
        quantity=Decimal("100"),
        avg_cost=Decimal("9.80"),
        as_of_date=date(2026, 7, 23),
        today_buy_qty=Decimal("20"),
    )


def test_position_crud_uses_optimistic_version_guard(tmp_path: Path) -> None:
    store = UserStore(tmp_path / "user.sqlite3")
    created = store.create_position(position())

    assert created.version == 1
    assert store.get_position(created.id) == created

    updated = store.update_position(
        created.id,
        PositionUpdate(
            quantity=Decimal("120"),
            avg_cost=Decimal("9.90"),
            as_of_date=date(2026, 7, 23),
            today_buy_qty=Decimal("20"),
            expected_version=1,
        ),
    )

    assert updated.quantity == Decimal("120")
    assert updated.version == 2
    with pytest.raises(ConflictError):
        store.update_position(
            created.id,
            PositionUpdate(
                quantity=Decimal("130"),
                avg_cost=Decimal("10"),
                as_of_date=date(2026, 7, 23),
                today_buy_qty=Decimal("0"),
                expected_version=1,
            ),
        )
    with pytest.raises(ConflictError):
        store.delete_position(created.id, expected_version=1)

    store.delete_position(created.id, expected_version=2)
    assert store.list_positions() == []


def test_watchlist_items_are_deduplicated_and_delete_is_idempotent(tmp_path: Path) -> None:
    store = UserStore(tmp_path / "user.sqlite3")
    watchlist = store.create_watchlist("核心观察")

    first, first_created = store.add_watchlist_item(watchlist.id, "sh.600000")
    second, second_created = store.add_watchlist_item(watchlist.id, "sh.600000")

    assert first == second
    assert first_created is True
    assert second_created is False
    assert store.list_watchlist_items(watchlist.id) == [first]

    store.remove_watchlist_item(watchlist.id, "sh.600000")
    store.remove_watchlist_item(watchlist.id, "sh.600000")
    assert store.list_watchlist_items(watchlist.id) == []


def test_portfolio_valuation_is_partial_for_suspended_and_missing_prices(
    tmp_path: Path,
) -> None:
    user_store = UserStore(tmp_path / "user.sqlite3")
    user_store.create_position(position("sh.600000"))
    user_store.create_position(position("sh.600001"))
    user_store.create_position(position("sz.000002"))
    market_store = ready_market_store(tmp_path / "market.duckdb")

    valuation = PortfolioValuationService(user_store, market_store).latest(
        expected_date=date(2026, 7, 23)
    )

    assert valuation.status == "partial"
    assert valuation.data_date == date(2026, 7, 23)
    assert valuation.coverage.covered == 1
    assert valuation.coverage.total == 3
    assert valuation.coverage.suspended_symbols == ["sh.600001"]
    assert valuation.coverage.missing_symbols == ["sz.000002"]
    assert valuation.covered_market_value == Decimal("1050.00")
    assert valuation.covered_unrealized_pnl == Decimal("70.00")
    suspended = next(item for item in valuation.items if item.symbol == "sh.600001")
    assert suspended.status == "suspended"
    assert suspended.market_value is None
    assert suspended.unrealized_pnl is None
    assert "未计交易费用" in valuation.valuation_basis


def test_empty_portfolio_and_market_error_are_distinct(tmp_path: Path) -> None:
    user_store = UserStore(tmp_path / "user.sqlite3")
    empty = PortfolioValuationService(
        user_store,
        MarketStore(tmp_path / "missing.duckdb"),
    ).latest(expected_date=date(2026, 7, 23))
    assert empty.status == "empty"

    user_store.create_position(position())
    error = PortfolioValuationService(
        user_store,
        MarketStore(tmp_path / "missing.duckdb"),
    ).latest(expected_date=date(2026, 7, 23))
    assert error.status == "error"
    assert error.coverage.covered == 0
    assert error.covered_unrealized_pnl is None


def test_user_api_exposes_crud_conflict_and_portfolio_coverage(tmp_path: Path) -> None:
    user_store = UserStore(tmp_path / "user.sqlite3")
    market_store = ready_market_store(tmp_path / "market.duckdb")
    app.dependency_overrides[get_user_store] = lambda: user_store
    app.dependency_overrides[get_market_store] = lambda: market_store

    async def request(method: str, path: str, **kwargs) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    try:
        created = asyncio.run(
            request(
                "POST",
                "/api/v1/portfolio/positions",
                json={
                    "symbol": "sh.600000",
                    "quantity": "100",
                    "avg_cost": "9.80",
                    "as_of_date": "2026-07-23",
                    "today_buy_qty": "20",
                },
            )
        )
        position_id = created.json()["id"]
        conflict = asyncio.run(
            request(
                "PUT",
                f"/api/v1/portfolio/positions/{position_id}",
                json={
                    "quantity": "120",
                    "avg_cost": "9.90",
                    "as_of_date": "2026-07-23",
                    "today_buy_qty": "20",
                    "expected_version": 99,
                },
            )
        )
        valuation = asyncio.run(
            request(
                "GET",
                "/api/v1/portfolio/valuation?expected_date=2026-07-23",
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert created.status_code == 201
    assert conflict.status_code == 409
    assert valuation.status_code == 200
    assert valuation.json()["status"] == "ready"
    assert valuation.json()["coverage"] == {
        "covered": 1,
        "total": 1,
        "missing_symbols": [],
        "suspended_symbols": [],
    }
