from datetime import date

from backend.app.alert.models import (
    AlertEvaluationResponse,
    AlertEventListResponse,
    AlertEventRecord,
    AlertRuleRecord,
)
from backend.app.alert.store import AlertStore
from backend.app.market.store import MarketStore
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.engine import StrategyEngine
from backend.app.strategy.store import StrategyStore
from backend.app.user.store import NotFoundError, UserStore


class AlertService:
    def __init__(
        self,
        alert_store: AlertStore,
        user_store: UserStore,
        strategy_store: StrategyStore,
        market_store: MarketStore | None,
    ) -> None:
        self.alert_store = alert_store
        self.user_store = user_store
        self.strategy_store = strategy_store
        self.market_store = market_store

    def create_rule(
        self,
        *,
        name: str,
        watchlist_id: str,
        strategy_id: str,
        strategy_version: int,
    ) -> AlertRuleRecord:
        if not any(item.id == watchlist_id for item in self.user_store.list_watchlists()):
            raise NotFoundError("watchlist not found")
        version = self.strategy_store.get_version(strategy_id, strategy_version)
        return self.alert_store.create_rule(
            name=name,
            watchlist_id=watchlist_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            strategy_version_id=version.id,
        )

    def evaluate(
        self,
        rule_id: str,
        *,
        signal_date: date,
    ) -> AlertEvaluationResponse:
        if self.market_store is None:
            raise RuntimeError("market storage is unavailable")
        if signal_date > date.today():
            raise ValueError("future signal dates are not allowed")
        rule = self.alert_store.get_rule(rule_id)
        symbols = [item.symbol for item in self.user_store.list_watchlist_items(rule.watchlist_id)]
        if not symbols:
            return AlertEvaluationResponse(
                status="empty",
                signal_date=signal_date,
                events=[],
                quality_issues=["watchlist_empty"],
            )
        try:
            version = self.strategy_store.get_version(
                rule.strategy_id,
                rule.strategy_version,
            )
            evaluation = StrategyEngine(self.market_store).evaluate(
                validate_rule(version.rule_ast),
                symbols=symbols,
                as_of_date=signal_date,
            )
        except Exception:
            events = [self._record_error(rule, symbol, signal_date) for symbol in symbols]
            return AlertEvaluationResponse(
                status="error",
                signal_date=signal_date,
                events=events,
                quality_issues=["evaluation_error"],
            )
        events = [
            self._record_result(
                rule,
                result,
                evaluation.data_fingerprint,
            )
            for result in evaluation.results
        ]
        issues = sorted({issue for event in events for issue in event.quality_issues})
        if all(event.state == "error" for event in events):
            status = "error"
        elif any(event.state in {"suppressed", "error"} for event in events):
            status = "partial"
        else:
            status = "ready"
        return AlertEvaluationResponse(
            status=status,
            signal_date=signal_date,
            events=events,
            quality_issues=issues,
        )

    def _record_error(
        self,
        rule: AlertRuleRecord,
        symbol: str,
        signal_date: date,
    ) -> AlertEventRecord:
        key = self.alert_store.event_idempotency_key(rule, symbol, signal_date)
        existing = self.alert_store.get_event_by_key(key)
        if existing is not None:
            return existing
        event = self.alert_store.create_pending(
            rule=rule,
            symbol=symbol,
            signal_date=signal_date,
            source=None,
            quality_status="error",
            quality_issues=["evaluation_error"],
            explanation={
                "status": "error",
                "data_date": signal_date.isoformat(),
                "quality_status": "error",
            },
            data_fingerprint=None,
        )
        return self.alert_store.transition(
            event.id,
            to_state="error",
            actor="system",
            reason="evaluation_error",
        )

    def _record_result(
        self,
        rule: AlertRuleRecord,
        result,
        data_fingerprint: str,
    ) -> AlertEventRecord:
        key = self.alert_store.event_idempotency_key(
            rule,
            result.symbol,
            result.signal_date,
        )
        existing = self.alert_store.get_event_by_key(key)
        if existing is not None and existing.state != "eligible":
            return existing
        event = existing or self.alert_store.create_pending(
            rule=rule,
            symbol=result.symbol,
            signal_date=result.signal_date,
            source="baostock",
            quality_status=result.quality_status,
            quality_issues=result.quality_issues,
            explanation=result.explanation,
            data_fingerprint=data_fingerprint,
        )
        if result.status in {"excluded", "insufficient_data"}:
            return self.alert_store.transition(
                event.id,
                to_state="suppressed",
                actor="system",
                reason=result.quality_issues[0] if result.quality_issues else result.status,
            )
        if event.state == "pending":
            event = self.alert_store.transition(
                event.id,
                to_state="eligible",
                actor="system",
                reason="post_close_data_ready",
            )
        if result.matched:
            return self.alert_store.transition(
                event.id,
                to_state="triggered",
                actor="system",
                reason="strategy_condition_matched",
            )
        return self.alert_store.transition(
            event.id,
            to_state="resolved",
            actor="system",
            reason="strategy_condition_not_matched",
        )

    def list_events(self) -> AlertEventListResponse:
        events = self.alert_store.list_events()
        if not events:
            return AlertEventListResponse(
                status="empty",
                unacknowledged=0,
                items=[],
                quality_issues=[],
            )
        issues = sorted(
            {
                issue
                for event in events
                if event.state in {"suppressed", "error"}
                for issue in event.quality_issues
            }
        )
        status = (
            "partial"
            if any(event.state in {"suppressed", "error"} for event in events)
            else "ready"
        )
        return AlertEventListResponse(
            status=status,
            unacknowledged=sum(event.state == "triggered" for event in events),
            items=events,
            quality_issues=issues,
        )
