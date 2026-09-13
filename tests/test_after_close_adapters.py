import sqlite3
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.app.config import Settings
from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore
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
from backend.app.storage.dataset import NasMarketStore


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


def test_factory_with_snapshot_settings_does_not_create_derived_database(tmp_path) -> None:
    settings = Settings(
        local_control_dir=tmp_path / "control",
        market_data_dir=tmp_path / "market",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=None,
        nas_market_dataset_root=None,
    )

    pipeline = build_after_close_pipeline(
        tmp_path / "user.sqlite3",
        SimpleNamespace(),
        settings=settings,
    )

    assert pipeline.regime_runner is not None
    assert not (settings.local_control_dir / settings.regime_snapshot_database_name).exists()


def test_factory_ready_manifest_captures_one_derived_snapshot_idempotently(tmp_path) -> None:
    as_of = date(2026, 7, 24)
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2}'
    )
    (root / "manifest.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2,"generation":"empty","files":[]}'
    )
    settings = Settings(
        local_control_dir=tmp_path / "control",
        market_data_dir=tmp_path / "market",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        user_data_dir=tmp_path / "user",
        local_market_dataset_root=root,
        nas_market_dataset_root=None,
    )
    store = NasMarketStore(
        MarketStore(settings.market_data_dir / settings.market_database_name),
        root,
        settings.local_staging_dir,
    )
    bar = DailyBar(
        trade_date=as_of,
        symbol="sh.600000",
        security_type="stock",
        exchange="sh",
        board="main",
        open=10,
        high=10,
        low=10,
        close=10,
        preclose=10,
        volume=1,
        amount=1,
        turnover_rate=1,
        pct_change=0,
        adjust_factor=1,
        is_trading=True,
        is_suspended=False,
        is_st=False,
        source_record_id="fixture",
        ingested_at=datetime(2026, 7, 24, tzinfo=UTC),
        quality_status="ready",
    )
    result = RefreshResult(
        run_id="ready",
        requested_date=as_of,
        source="baostock",
        status="ready",
        requested_count=1,
        succeeded_count=1,
        coverage_ratio=1,
        started_at=datetime(2026, 7, 24, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 1, tzinfo=UTC),
    )
    store.save_refresh([bar], result, publish=True, lineage_input={"mode": "legacy"})
    user_database = settings.user_data_dir / settings.user_database_name
    pipeline = build_after_close_pipeline(user_database, store, settings=settings)
    assert pipeline.run_after_publication(result).status == "completed"
    assert pipeline.run_after_publication(result).status == "completed"
    database = settings.local_control_dir / settings.regime_snapshot_database_name
    connection = sqlite3.connect(database)
    assert connection.execute("SELECT count(*) FROM regime_snapshots").fetchone()[0] == 1
    connection.close()

    connection = sqlite3.connect(user_database)
    task = connection.execute(
        """
        SELECT kind, status, result_reference, error_code, attempt_count
        FROM after_close_tasks WHERE kind = 'regime'
        """
    ).fetchone()
    schema = "\n".join(
        row[0]
        for row in connection.execute(
            "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name"
        ).fetchall()
    )
    connection.close()

    kind, status, result_reference, error_code, attempt_count = task
    assert (kind, status, error_code, attempt_count) == (
        "regime",
        "completed",
        None,
        1,
    )
    assert result_reference.startswith("regime-snapshot-")
    assert "result_payload" not in schema
    assert "content_hash" not in schema
    assert "dataset_identity_hash" not in schema
    private_bytes = b"".join(
        path.read_bytes()
        for path in sorted(user_database.parent.glob(f"{user_database.name}*"))
        if path.is_file()
    )
    assert str(root).encode() not in private_bytes
    assert b"result_payload" not in private_bytes
    assert b"dataset_identity_hash" not in private_bytes
