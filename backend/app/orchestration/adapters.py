"""Adapters from local Stock EVA stores/services to after-close protocols."""

from datetime import date
from pathlib import Path

from backend.app.orchestration.after_close import (
    AfterClosePipelineService,
    AfterCloseTaskStore,
    AlertWorkItem,
    StrategyWorkItem,
)


class StrategyCatalogAdapter:
    def __init__(self, strategy_store) -> None:
        self.strategy_store = strategy_store

    def list_enabled(self) -> list[StrategyWorkItem]:
        items = []
        for strategy in self.strategy_store.list_strategies():
            version = self.strategy_store.get_version(
                strategy.id,
                strategy.current_version,
            )
            items.append(
                StrategyWorkItem(
                    strategy_id=strategy.id,
                    version=strategy.current_version,
                    version_id=version.id,
                )
            )
        return items


class StrategyRunnerAdapter:
    def __init__(self, strategy_run_service) -> None:
        self.strategy_run_service = strategy_run_service

    def run(
        self,
        item: StrategyWorkItem,
        *,
        trade_date: date,
        idempotency_key: str,
    ) -> str:
        del trade_date
        result = self.strategy_run_service.run_all_main_board(
            item.strategy_id,
            version=item.version,
            idempotency_key=idempotency_key,
        )
        if result.status == "error" or result.failed_batches:
            raise RuntimeError("market scan did not complete")
        return result.id


class AlertCatalogAdapter:
    def __init__(self, alert_store) -> None:
        self.alert_store = alert_store

    def list_enabled(self) -> list[AlertWorkItem]:
        return [AlertWorkItem(rule_id=rule.id) for rule in self.alert_store.list_rules()]


class AlertRunnerAdapter:
    def __init__(self, alert_service) -> None:
        self.alert_service = alert_service

    def run(
        self,
        item: AlertWorkItem,
        *,
        trade_date: date,
        idempotency_key: str,
    ) -> str:
        del idempotency_key
        result = self.alert_service.evaluate(
            item.rule_id,
            signal_date=trade_date,
        )
        if result.status == "error":
            raise RuntimeError("alert evaluation did not complete")
        return f"alert-evaluation:{item.rule_id}:{trade_date.isoformat()}"


def build_after_close_pipeline(
    user_database: Path,
    market_store,
) -> AfterClosePipelineService:
    from backend.app.alert.service import AlertService
    from backend.app.alert.store import AlertStore
    from backend.app.strategy.run import StrategyRunService
    from backend.app.strategy.store import StrategyStore
    from backend.app.user.store import UserStore

    strategy_store = StrategyStore(user_database)
    alert_store = AlertStore(user_database)
    user_store = UserStore(user_database)
    return AfterClosePipelineService(
        market_store=market_store,
        task_store=AfterCloseTaskStore(user_database),
        list_strategies=StrategyCatalogAdapter(strategy_store).list_enabled,
        strategy_runner=StrategyRunnerAdapter(StrategyRunService(strategy_store, market_store)),
        list_alert_rules=AlertCatalogAdapter(alert_store).list_enabled,
        alert_runner=AlertRunnerAdapter(
            AlertService(
                alert_store,
                user_store,
                strategy_store,
                market_store,
            )
        ),
    )
