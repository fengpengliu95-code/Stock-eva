from datetime import date
from types import SimpleNamespace

import pytest

from backend.app.orchestration.adapters import (
    AlertCatalogAdapter,
    AlertRunnerAdapter,
    StrategyCatalogAdapter,
    StrategyRunnerAdapter,
    build_after_close_pipeline,
)
from backend.app.orchestration.after_close import (
    AfterClosePipelineService,
    AlertWorkItem,
    StrategyWorkItem,
)


def test_strategy_catalog_uses_each_saved_strategy_current_immutable_version() -> None:
    versions = {
        ("strategy-1", 2): SimpleNamespace(id="version-1-2"),
        ("strategy-2", 4): SimpleNamespace(id="version-2-4"),
    }
    store = SimpleNamespace(
        list_strategies=lambda: [
            SimpleNamespace(id="strategy-1", current_version=2),
            SimpleNamespace(id="strategy-2", current_version=4),
        ],
        get_version=lambda strategy_id, version: versions[(strategy_id, version)],
    )

    assert StrategyCatalogAdapter(store).list_enabled() == [
        StrategyWorkItem(
            strategy_id="strategy-1",
            version=2,
            version_id="version-1-2",
        ),
        StrategyWorkItem(
            strategy_id="strategy-2",
            version=4,
            version_id="version-2-4",
        ),
    ]


def test_strategy_runner_forwards_pipeline_idempotency_key() -> None:
    calls = []
    service = SimpleNamespace(
        run_all_main_board=lambda strategy_id, **kwargs: (
            calls.append((strategy_id, kwargs))
            or SimpleNamespace(id="run-1", status="completed", failed_batches=0)
        )
    )
    item = StrategyWorkItem(
        strategy_id="strategy-1",
        version=2,
        version_id="version-1-2",
    )

    result = StrategyRunnerAdapter(service).run(
        item,
        trade_date=date(2026, 7, 24),
        idempotency_key="after-close:strategy:version-1-2:2026-07-24",
    )

    assert result == "run-1"
    assert calls == [
        (
            "strategy-1",
            {
                "version": 2,
                "idempotency_key": "after-close:strategy:version-1-2:2026-07-24",
            },
        )
    ]


@pytest.mark.parametrize(
    ("status", "failed_batches"),
    [("error", 0), ("partial", 1)],
)
def test_strategy_runner_rejects_failed_scan(
    status: str,
    failed_batches: int,
) -> None:
    service = SimpleNamespace(
        run_all_main_board=lambda *_args, **_kwargs: SimpleNamespace(
            id="run-failed",
            status=status,
            failed_batches=failed_batches,
        )
    )

    with pytest.raises(RuntimeError, match="market scan did not complete"):
        StrategyRunnerAdapter(service).run(
            StrategyWorkItem(
                strategy_id="strategy-1",
                version=1,
                version_id="version-1",
            ),
            trade_date=date(2026, 7, 24),
            idempotency_key="key",
        )


def test_alert_catalog_and_runner_use_all_saved_rules() -> None:
    alert_store = SimpleNamespace(
        list_rules=lambda: [
            SimpleNamespace(id="rule-1"),
            SimpleNamespace(id="rule-2"),
        ]
    )
    calls = []
    alert_service = SimpleNamespace(
        evaluate=lambda rule_id, **kwargs: (
            calls.append((rule_id, kwargs)) or SimpleNamespace(status="ready")
        )
    )

    items = AlertCatalogAdapter(alert_store).list_enabled()
    result = AlertRunnerAdapter(alert_service).run(
        items[0],
        trade_date=date(2026, 7, 24),
        idempotency_key="task-key",
    )

    assert items == [
        AlertWorkItem(rule_id="rule-1"),
        AlertWorkItem(rule_id="rule-2"),
    ]
    assert result == "alert-evaluation:rule-1:2026-07-24"
    assert calls == [("rule-1", {"signal_date": date(2026, 7, 24)})]


def test_alert_runner_marks_evaluation_error_as_task_failure() -> None:
    service = SimpleNamespace(evaluate=lambda *_args, **_kwargs: SimpleNamespace(status="error"))

    with pytest.raises(RuntimeError, match="alert evaluation did not complete"):
        AlertRunnerAdapter(service).run(
            AlertWorkItem(rule_id="rule-1"),
            trade_date=date(2026, 7, 24),
            idempotency_key="task-key",
        )


def test_factory_builds_local_pipeline_around_shared_private_database(
    tmp_path,
) -> None:
    pipeline = build_after_close_pipeline(
        tmp_path / "user.sqlite3",
        SimpleNamespace(),
    )

    assert isinstance(pipeline, AfterClosePipelineService)
    assert pipeline.task_store.path == tmp_path / "user.sqlite3"
