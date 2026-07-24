import asyncio
from datetime import date
from pathlib import Path

import httpx
import pytest

from backend.app.api.market import get_market_store
from backend.app.api.strategy import get_strategy_store
from backend.app.main import app
from backend.app.market.store import MarketStore
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.run import StrategyRunService
from backend.app.strategy.store import StrategyConflictError, StrategyStore
from tests.test_strategy_engine import SYMBOL, crossing_history, save_history


def cross_rule_ast() -> dict[str, object]:
    return {
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
    }


def test_strategy_versions_are_immutable_and_optimistically_guarded(tmp_path: Path) -> None:
    store = StrategyStore(tmp_path / "user.sqlite3")
    first_rule = validate_rule(cross_rule_ast())
    strategy, version_one = store.create_strategy("MA crossover", first_rule)
    second_ast = {
        "op": "compare",
        "left": {"type": "indicator", "name": "RSI", "field": "close", "period": 14},
        "operator": "<",
        "right": {"type": "constant", "value": 30},
    }
    version_two = store.add_version(
        strategy.id,
        expected_current_version=1,
        rule=validate_rule(second_ast),
    )

    assert strategy.current_version == 1
    assert version_one.version == 1
    assert version_two.version == 2
    assert store.get_version(strategy.id, 1).rule_ast == first_rule.ast
    assert store.get_strategy(strategy.id).current_version == 2
    with pytest.raises(StrategyConflictError):
        store.add_version(
            strategy.id,
            expected_current_version=1,
            rule=first_rule,
        )


def test_run_records_results_and_replays_reproducibly(tmp_path: Path) -> None:
    strategy_store = StrategyStore(tmp_path / "user.sqlite3")
    market_store = MarketStore(tmp_path / "market.duckdb")
    history = crossing_history(date(2026, 1, 1))
    save_history(market_store, history)
    strategy, version = strategy_store.create_strategy(
        "MA crossover",
        validate_rule(cross_rule_ast()),
    )
    service = StrategyRunService(strategy_store, market_store)

    first = service.run(
        strategy.id,
        version=version.version,
        symbols=[SYMBOL],
        as_of_date=history[-1].trade_date,
    )
    second = service.run(
        strategy.id,
        version=version.version,
        symbols=[SYMBOL],
        as_of_date=history[-1].trade_date,
    )

    assert first.status == "completed"
    assert first.results[0].status == "matched"
    assert first.id != second.id
    assert first.rule_hash == second.rule_hash
    assert first.data_fingerprint == second.data_fingerprint
    assert first.results == second.results
    assert store_run_ids(strategy_store, strategy.id) == [first.id, second.id]


def store_run_ids(store: StrategyStore, strategy_id: str) -> list[str]:
    return [run.id for run in store.list_runs(strategy_id)]


def test_strategy_api_validates_creates_and_runs_version(tmp_path: Path) -> None:
    strategy_store = StrategyStore(tmp_path / "user.sqlite3")
    market_store = MarketStore(tmp_path / "market.duckdb")
    history = crossing_history(date(2026, 1, 1))
    save_history(market_store, history)
    app.dependency_overrides[get_strategy_store] = lambda: strategy_store
    app.dependency_overrides[get_market_store] = lambda: market_store

    async def request(method: str, path: str, **kwargs) -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.request(method, path, **kwargs)

    try:
        invalid = asyncio.run(
            request(
                "POST",
                "/api/v1/strategies/validate",
                json={"rule": {"op": "eval", "source": "close > 0"}},
            )
        )
        created = asyncio.run(
            request(
                "POST",
                "/api/v1/strategies",
                json={"name": "MA crossover", "rule": cross_rule_ast()},
            )
        )
        strategy_id = created.json()["strategy"]["id"]
        run = asyncio.run(
            request(
                "POST",
                f"/api/v1/strategies/{strategy_id}/runs",
                json={
                    "version": 1,
                    "symbols": [SYMBOL],
                    "as_of_date": history[-1].trade_date.isoformat(),
                },
            )
        )
    finally:
        app.dependency_overrides.clear()

    assert invalid.status_code == 422
    assert created.status_code == 201
    assert created.json()["version"]["rule_hash"]
    assert run.status_code == 201
    assert run.json()["status"] == "completed"
    assert run.json()["results"][0]["status"] == "matched"
