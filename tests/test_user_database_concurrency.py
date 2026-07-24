from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

from backend.app.alert.store import AlertStore
from backend.app.strategy.store import StrategyStore
from backend.app.user.store import UserStore


def run_after(
    start: Barrier,
    call: Callable[[], list[Any]],
) -> list[Any]:
    start.wait()
    return call()


def test_shared_user_database_initializes_safely_under_concurrency(
    tmp_path: Path,
) -> None:
    for attempt in range(30):
        database_path = tmp_path / str(attempt) / "user.sqlite3"
        start = Barrier(3)
        calls = [
            UserStore(database_path).list_positions,
            StrategyStore(database_path).list_strategies,
            AlertStore(database_path).list_rules,
        ]

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(run_after, start, call)
                for call in calls
            ]
            results = [future.result() for future in futures]

        assert results == [[], [], []]
