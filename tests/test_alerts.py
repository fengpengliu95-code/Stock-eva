import asyncio
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import httpx
import pytest

from backend.app.config import Settings, get_settings
from backend.app.main import app
from backend.app.market.store import MarketStore
from tests.test_strategy_engine import SYMBOL, bar, crossing_history, save_history
from tests.test_strategy_store import cross_rule_ast


def request(method: str, path: str, **kwargs) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(send())


@pytest.fixture
def alert_environment(tmp_path: Path):
    settings = Settings(
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
    )
    app.dependency_overrides[get_settings] = lambda: settings
    try:
        yield settings
    finally:
        app.dependency_overrides.clear()


def seed_rule_and_watchlist(settings: Settings) -> tuple[str, str, str]:
    created_strategy = request(
        "POST",
        "/api/v1/strategies",
        json={"name": "收盘均线交叉", "rule": cross_rule_ast()},
    )
    assert created_strategy.status_code == 201
    strategy = created_strategy.json()["strategy"]
    version = created_strategy.json()["version"]

    created_watchlist = request(
        "POST",
        "/api/v1/watchlists",
        json={"name": "预警观察"},
    )
    assert created_watchlist.status_code == 201
    watchlist = created_watchlist.json()
    added = request(
        "POST",
        f"/api/v1/watchlists/{watchlist['id']}/items",
        json={"symbol": SYMBOL},
    )
    assert added.status_code == 201

    rule = request(
        "POST",
        "/api/v1/alerts/rules",
        json={
            "name": "观察均线交叉",
            "watchlist_id": watchlist["id"],
            "strategy_id": strategy["id"],
            "strategy_version": version["version"],
        },
    )
    assert rule.status_code == 201
    return rule.json()["id"], strategy["id"], version["id"]


def market_store(settings: Settings) -> MarketStore:
    return MarketStore(settings.market_data_dir / settings.market_database_name)


def test_alert_evaluation_is_post_close_idempotent_and_audited(
    alert_environment: Settings,
) -> None:
    settings = alert_environment
    history = crossing_history(date(2026, 1, 1))
    signal_date = history[-1].trade_date
    save_history(market_store(settings), history)
    rule_id, _, version_id = seed_rule_and_watchlist(settings)

    first = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": signal_date.isoformat()},
    )
    second = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": signal_date.isoformat()},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    first_event = first.json()["events"][0]
    second_event = second.json()["events"][0]
    assert first.json()["status"] == "ready"
    assert first_event["state"] == "triggered"
    assert first_event["source"] == "baostock"
    assert first_event["quality_status"] == "ready"
    assert first_event["signal_date"] == signal_date.isoformat()
    assert first_event["idempotency_key"] == f"{version_id}:{SYMBOL}:{signal_date}"
    assert first_event["id"] == second_event["id"]
    assert [item["to_state"] for item in first_event["transitions"]] == [
        "pending",
        "eligible",
        "triggered",
    ]
    assert second_event["transitions"] == first_event["transitions"]

    listing = request("GET", "/api/v1/alerts/events")
    assert listing.status_code == 200
    assert listing.json()["status"] == "ready"
    assert listing.json()["unacknowledged"] == 1
    assert listing.json()["items"][0]["explanation"]["data_date"] == (
        signal_date.isoformat()
    )


def test_alert_manual_acknowledge_suppress_restore_and_re_evaluate(
    alert_environment: Settings,
) -> None:
    settings = alert_environment
    history = crossing_history(date(2026, 2, 1))
    signal_date = history[-1].trade_date
    save_history(market_store(settings), history)
    rule_id, _, _ = seed_rule_and_watchlist(settings)
    event = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": signal_date.isoformat()},
    ).json()["events"][0]

    acknowledged = request(
        "POST",
        f"/api/v1/alerts/events/{event['id']}/acknowledge",
    )
    suppressed = request(
        "POST",
        f"/api/v1/alerts/events/{event['id']}/suppress",
    )
    restored = request(
        "POST",
        f"/api/v1/alerts/events/{event['id']}/restore",
    )
    re_evaluated = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": signal_date.isoformat()},
    )

    assert acknowledged.json()["state"] == "acknowledged"
    assert suppressed.json()["state"] == "suppressed"
    assert restored.json()["state"] == "eligible"
    assert re_evaluated.json()["events"][0]["state"] == "triggered"
    states = [
        item["to_state"]
        for item in re_evaluated.json()["events"][0]["transitions"]
    ]
    assert states == [
        "pending",
        "eligible",
        "triggered",
        "acknowledged",
        "suppressed",
        "eligible",
        "triggered",
    ]


@pytest.mark.parametrize(
    ("updates", "expected_issue"),
    [
        ({"suspended": True}, "suspended_on_signal_date"),
        ({"quality": "partial"}, "quality_not_ready"),
    ],
)
def test_alert_suppresses_suspension_and_bad_quality(
    alert_environment: Settings,
    updates: dict[str, object],
    expected_issue: str,
) -> None:
    settings = alert_environment
    history = crossing_history(date(2026, 3, 1))
    last = history[-1]
    history[-1] = bar(
        last.trade_date,
        last.close,
        last.volume,
        suspended=bool(updates.get("suspended", False)),
        quality=str(updates.get("quality", "ready")),
    )
    save_history(market_store(settings), history)
    rule_id, _, _ = seed_rule_and_watchlist(settings)

    response = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": last.trade_date.isoformat()},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "partial"
    event = response.json()["events"][0]
    assert event["state"] == "suppressed"
    assert expected_issue in event["quality_issues"]
    assert "triggered" not in [
        item["to_state"] for item in event["transitions"]
    ]


def test_alert_rejects_future_signal_date_without_creating_event(
    alert_environment: Settings,
) -> None:
    settings = alert_environment
    rule_id, _, _ = seed_rule_and_watchlist(settings)
    future = date.today() + timedelta(days=1)

    response = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": future.isoformat()},
    )
    listing = request("GET", "/api/v1/alerts/events")

    assert response.status_code == 422
    assert listing.status_code == 200
    assert listing.json() == {
        "status": "empty",
        "unacknowledged": 0,
        "items": [],
        "quality_issues": [],
    }


def test_alert_invalid_transition_is_conflict(
    alert_environment: Settings,
) -> None:
    settings = alert_environment
    history = crossing_history(date(2026, 4, 1))
    last = history[-1]
    history[-1] = bar(last.trade_date, last.close, last.volume, suspended=True)
    save_history(market_store(settings), history)
    rule_id, _, _ = seed_rule_and_watchlist(settings)
    event = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": last.trade_date.isoformat()},
    ).json()["events"][0]

    response = request(
        "POST",
        f"/api/v1/alerts/events/{event['id']}/acknowledge",
    )

    assert response.status_code == 409


def test_alert_evaluation_failure_is_persisted_as_error(
    alert_environment: Settings,
) -> None:
    settings = alert_environment
    rule_id, _, _ = seed_rule_and_watchlist(settings)
    database = settings.user_data_dir / settings.user_database_name
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "UPDATE strategy_versions SET rule_ast = ?",
            ['{"op":"eval","source":"close > 0"}'],
        )
        connection.commit()
    finally:
        connection.close()

    response = request(
        "POST",
        f"/api/v1/alerts/rules/{rule_id}/evaluate",
        json={"signal_date": "2026-07-23"},
    )
    listing = request("GET", "/api/v1/alerts/events")

    assert response.status_code == 200
    assert response.json()["status"] == "error"
    assert response.json()["quality_issues"] == ["evaluation_error"]
    event = response.json()["events"][0]
    assert event["state"] == "error"
    assert event["quality_status"] == "error"
    assert event["source"] is None
    assert event["explanation"] == {
        "data_date": "2026-07-23",
        "quality_status": "error",
        "status": "error",
    }
    assert listing.json()["status"] == "partial"
