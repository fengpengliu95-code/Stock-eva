from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.app.alert.store import AlertStore
from backend.app.market.automation import MarketAutomationService
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore
from backend.app.orchestration.after_close import (
    AfterClosePipelineService,
    AfterCloseTaskStore,
    AlertWorkItem,
    StrategyWorkItem,
)

TRADE_DATE = date(2026, 7, 24)


def refresh_result(
    *,
    run_id: str = "publication-ready",
    requested_date: date = TRADE_DATE,
    status: str = "ready",
) -> RefreshResult:
    now = datetime(2026, 7, 24, 10, tzinfo=UTC)
    return RefreshResult(
        run_id=run_id,
        request_key=f"daily:{requested_date}",
        run_kind="daily",
        requested_date=requested_date,
        source="baostock",
        status=status,
        requested_count=2,
        succeeded_count=2 if status == "ready" else 1,
        coverage_ratio=1 if status == "ready" else 0.5,
        started_at=now,
        completed_at=now,
    )


@dataclass
class PublishedStore:
    published: RefreshResult | None

    def published_refresh(self) -> RefreshResult | None:
        return self.published


class RecordingStrategyRunner:
    def __init__(self, *, fail_keys: set[str] | None = None) -> None:
        self.calls: list[tuple[StrategyWorkItem, date, str]] = []
        self.fail_keys = fail_keys or set()
        self.result_by_key: dict[str, str] = {}

    def run(
        self,
        item: StrategyWorkItem,
        *,
        trade_date: date,
        idempotency_key: str,
    ) -> str:
        self.calls.append((item, trade_date, idempotency_key))
        if item.version_id in self.fail_keys:
            raise RuntimeError("private implementation detail")
        return self.result_by_key.setdefault(
            idempotency_key,
            f"strategy-run-{len(self.result_by_key) + 1}",
        )


class RecordingAlertRunner:
    def __init__(self, *, fail_keys: set[str] | None = None) -> None:
        self.calls: list[tuple[AlertWorkItem, date, str]] = []
        self.fail_keys = fail_keys or set()
        self.result_by_key: dict[str, str] = {}

    def run(
        self,
        item: AlertWorkItem,
        *,
        trade_date: date,
        idempotency_key: str,
    ) -> str:
        self.calls.append((item, trade_date, idempotency_key))
        if item.rule_id in self.fail_keys:
            raise RuntimeError("private implementation detail")
        return self.result_by_key.setdefault(
            idempotency_key,
            f"alert-evaluation-{len(self.result_by_key) + 1}",
        )


def build_service(
    tmp_path: Path,
    *,
    published: RefreshResult | None,
    strategies: list[StrategyWorkItem] | None = None,
    alerts: list[AlertWorkItem] | None = None,
    strategy_runner: RecordingStrategyRunner | None = None,
    alert_runner: RecordingAlertRunner | None = None,
) -> tuple[
    AfterClosePipelineService,
    AfterCloseTaskStore,
    RecordingStrategyRunner,
    RecordingAlertRunner,
]:
    task_store = AfterCloseTaskStore(tmp_path / "user.sqlite3")
    selected_strategy_runner = strategy_runner or RecordingStrategyRunner()
    selected_alert_runner = alert_runner or RecordingAlertRunner()
    service = AfterClosePipelineService(
        market_store=PublishedStore(published),
        task_store=task_store,
        list_strategies=lambda: strategies or [],
        strategy_runner=selected_strategy_runner,
        list_alert_rules=lambda: alerts or [],
        alert_runner=selected_alert_runner,
    )
    return service, task_store, selected_strategy_runner, selected_alert_runner


def test_ready_published_snapshot_runs_strategies_before_alerts(tmp_path: Path) -> None:
    sequence: list[str] = []

    class OrderedStrategyRunner(RecordingStrategyRunner):
        def run(self, *args, **kwargs) -> str:
            sequence.append("strategy")
            return super().run(*args, **kwargs)

    class OrderedAlertRunner(RecordingAlertRunner):
        def run(self, *args, **kwargs) -> str:
            sequence.append("alert")
            return super().run(*args, **kwargs)

    strategy_runner = OrderedStrategyRunner()
    alert_runner = OrderedAlertRunner()
    service, task_store, _, _ = build_service(
        tmp_path,
        published=refresh_result(),
        strategies=[
            StrategyWorkItem(
                strategy_id="strategy-1",
                version=2,
                version_id="strategy-version-2",
            )
        ],
        alerts=[AlertWorkItem(rule_id="alert-rule-1")],
        strategy_runner=strategy_runner,
        alert_runner=alert_runner,
    )

    outcome = service.run_after_publication(refresh_result())

    assert outcome.triggered is True
    assert outcome.status == "completed"
    assert outcome.completed_tasks == 2
    assert sequence == ["strategy", "alert"]
    assert task_store.list_tasks(TRADE_DATE)[0].kind == "strategy"
    assert all(task.status == "completed" for task in task_store.list_tasks(TRADE_DATE))


def test_non_ready_or_unpublished_or_old_result_never_triggers(tmp_path: Path) -> None:
    scenarios = [
        (refresh_result(status="partial"), refresh_result(status="partial"), "not_ready"),
        (refresh_result(), None, "not_published"),
        (
            refresh_result(requested_date=date(2026, 7, 23)),
            refresh_result(requested_date=date(2026, 7, 24)),
            "not_current_publication",
        ),
        (
            refresh_result(run_id="old-ready"),
            refresh_result(run_id="new-ready"),
            "not_current_publication",
        ),
    ]
    for index, (result, published, expected_reason) in enumerate(scenarios):
        service, task_store, strategy_runner, alert_runner = build_service(
            tmp_path / str(index),
            published=published,
            strategies=[
                StrategyWorkItem(
                    strategy_id="strategy-1",
                    version=1,
                    version_id="strategy-version-1",
                )
            ],
            alerts=[AlertWorkItem(rule_id="alert-rule-1")],
        )

        outcome = service.run_after_publication(result)

        assert outcome.triggered is False
        assert outcome.reason == expected_reason
        assert strategy_runner.calls == []
        assert alert_runner.calls == []
        assert task_store.list_tasks(result.requested_date) == []


def test_same_strategy_version_and_rule_are_idempotent_for_date(tmp_path: Path) -> None:
    service, task_store, strategy_runner, alert_runner = build_service(
        tmp_path,
        published=refresh_result(),
        strategies=[
            StrategyWorkItem(
                strategy_id="strategy-1",
                version=3,
                version_id="version-immutable-3",
            )
        ],
        alerts=[AlertWorkItem(rule_id="alert-rule-1")],
    )

    first = service.run_after_publication(refresh_result())
    second = service.run_after_publication(refresh_result())

    assert first.status == "completed"
    assert second.status == "completed"
    assert second.completed_tasks == 2
    assert len(strategy_runner.calls) == 1
    assert len(alert_runner.calls) == 1
    tasks = task_store.list_tasks(TRADE_DATE)
    assert len(tasks) == 2
    assert all(task.attempt_count == 1 for task in tasks)


def test_failure_is_isolated_audited_and_retryable_without_rerunning_success(
    tmp_path: Path,
) -> None:
    strategy_runner = RecordingStrategyRunner(fail_keys={"bad-version"})
    service, task_store, _, alert_runner = build_service(
        tmp_path,
        published=refresh_result(),
        strategies=[
            StrategyWorkItem(
                strategy_id="bad",
                version=1,
                version_id="bad-version",
            ),
            StrategyWorkItem(
                strategy_id="good",
                version=1,
                version_id="good-version",
            ),
        ],
        alerts=[AlertWorkItem(rule_id="alert-rule-1")],
        strategy_runner=strategy_runner,
    )

    first = service.run_after_publication(refresh_result())

    assert first.status == "partial"
    assert first.failed_tasks == 1
    assert len(alert_runner.calls) == 1
    failed = next(task for task in task_store.list_tasks(TRADE_DATE) if task.status == "error")
    assert failed.error_code == "strategy_execution_failed"
    assert "private implementation detail" not in repr(failed)

    strategy_runner.fail_keys.clear()
    second = service.run_after_publication(refresh_result())

    assert second.status == "completed"
    assert len(strategy_runner.calls) == 3
    assert len(alert_runner.calls) == 1
    tasks = task_store.list_tasks(TRADE_DATE)
    assert next(task for task in tasks if task.work_key == "bad-version").attempt_count == 2
    assert next(task for task in tasks if task.work_key == "good-version").attempt_count == 1


def test_disabled_work_items_are_not_run(tmp_path: Path) -> None:
    service, task_store, strategy_runner, alert_runner = build_service(
        tmp_path,
        published=refresh_result(),
        strategies=[
            StrategyWorkItem(
                strategy_id="disabled",
                version=1,
                version_id="disabled-version",
                enabled=False,
            )
        ],
        alerts=[AlertWorkItem(rule_id="disabled-rule", enabled=False)],
    )

    outcome = service.run_after_publication(refresh_result())

    assert outcome.status == "empty"
    assert strategy_runner.calls == []
    assert alert_runner.calls == []
    assert task_store.list_tasks(TRADE_DATE) == []


def test_market_automation_calls_pipeline_only_after_ready_publish(
    tmp_path: Path,
) -> None:
    from tests.test_market_automation import (
        CompleteProvider,
        fixture_bars,
        synthetic_calendar,
    )

    class Recorder:
        def __init__(self) -> None:
            self.results: list[RefreshResult] = []

        def run_after_publication(self, result: RefreshResult) -> None:
            self.results.append(result)

    store = MarketStore(tmp_path / "market.duckdb")
    recorder = Recorder()
    service = MarketAutomationService(
        store,
        CompleteProvider(fixture_bars(TRADE_DATE)),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        post_publish=recorder,
    )

    outcome = service.run_due_once(datetime(2026, 7, 24, 18, 10, tzinfo=ZoneInfo("Asia/Shanghai")))

    assert outcome.result is not None
    assert outcome.result.status == "ready"
    assert [item.run_id for item in recorder.results] == [outcome.result.run_id]


def test_market_publication_remains_success_when_post_publish_pipeline_raises(
    tmp_path: Path,
) -> None:
    from tests.test_market_automation import (
        CompleteProvider,
        fixture_bars,
        synthetic_calendar,
    )

    class FailingPipeline:
        def run_after_publication(self, _result: RefreshResult) -> None:
            raise RuntimeError("synthetic private pipeline failure")

    store = MarketStore(tmp_path / "market.duckdb")
    service = MarketAutomationService(
        store,
        CompleteProvider(fixture_bars(TRADE_DATE)),
        synthetic_calendar(),
        required_symbols=lambda: {"sh.600000"},
        post_publish=FailingPipeline(),
    )

    outcome = service.run_due_once(datetime(2026, 7, 24, 18, 10, tzinfo=ZoneInfo("Asia/Shanghai")))

    assert outcome.result is not None
    assert outcome.result.status == "ready"
    assert outcome.state.refresh_state == "success"
    assert store.published_refresh().run_id == outcome.result.run_id


def test_market_automation_replays_post_publish_idempotently_after_restart(
    tmp_path: Path,
) -> None:
    from tests.test_market_automation import save_published, synthetic_calendar

    class Recorder:
        def __init__(self) -> None:
            self.results: list[RefreshResult] = []

        def run_after_publication(self, result: RefreshResult) -> None:
            self.results.append(result)

    class Provider:
        def trading_dates(self, *_args):
            raise AssertionError("published catch-up must not access the provider")

        def fetch(self, *_args, **_kwargs):
            raise AssertionError("published catch-up must not access the provider")

    store = MarketStore(tmp_path / "market.duckdb")
    save_published(store, TRADE_DATE)
    recorder = Recorder()
    service = MarketAutomationService(
        store,
        Provider(),
        synthetic_calendar(),
        required_symbols=set,
        post_publish=recorder,
    )

    outcome = service.run_due_once(datetime(2026, 7, 24, 19, 0, tzinfo=ZoneInfo("Asia/Shanghai")))

    assert outcome.decision.action == "none"
    assert outcome.state.refresh_state == "success"
    assert len(recorder.results) == 1
    assert recorder.results[0].run_id == store.published_refresh().run_id


def test_overlapping_alert_rules_keep_separate_idempotent_events(
    tmp_path: Path,
) -> None:
    alert_store = AlertStore(tmp_path / "user.sqlite3")
    first_rule = alert_store.create_rule(
        name="first",
        watchlist_id="watchlist-1",
        strategy_id="strategy-1",
        strategy_version=1,
        strategy_version_id="strategy-version-1",
    )
    second_rule = alert_store.create_rule(
        name="second",
        watchlist_id="watchlist-2",
        strategy_id="strategy-1",
        strategy_version=1,
        strategy_version_id="strategy-version-1",
    )
    arguments = {
        "symbol": "sh.600000",
        "signal_date": TRADE_DATE,
        "source": "baostock",
        "quality_status": "ready",
        "quality_issues": [],
        "explanation": {"status": "matched"},
        "data_fingerprint": "fingerprint",
    }

    first = alert_store.create_pending(rule=first_rule, **arguments)
    second = alert_store.create_pending(rule=second_rule, **arguments)
    repeated_second = alert_store.create_pending(rule=second_rule, **arguments)

    assert first.id != second.id
    assert first.alert_rule_id == first_rule.id
    assert second.alert_rule_id == second_rule.id
    assert repeated_second.id == second.id
