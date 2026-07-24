from datetime import date

from backend.app.market.store import MarketStore
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.engine import StrategyEngine
from backend.app.strategy.models import StrategyRunRecord
from backend.app.strategy.store import StrategyStore

MAX_RUN_SYMBOLS = 20


class StrategyRunService:
    def __init__(self, strategy_store: StrategyStore, market_store: MarketStore) -> None:
        self.strategy_store = strategy_store
        self.market_store = market_store

    def run(
        self,
        strategy_id: str,
        *,
        version: int,
        symbols: list[str],
        as_of_date: date,
    ) -> StrategyRunRecord:
        unique_symbols = sorted(set(symbols))
        if not unique_symbols:
            raise ValueError("at least one symbol is required")
        if len(unique_symbols) > MAX_RUN_SYMBOLS:
            raise ValueError(f"a run accepts at most {MAX_RUN_SYMBOLS} symbols")
        stored_version = self.strategy_store.get_version(strategy_id, version)
        rule = validate_rule(stored_version.rule_ast)
        if rule.rule_hash != stored_version.rule_hash:
            raise ValueError("stored strategy hash mismatch")
        evaluation = StrategyEngine(self.market_store).evaluate(
            rule,
            symbols=unique_symbols,
            as_of_date=as_of_date,
        )
        run_status = (
            "partial"
            if any(
                result.status in {"excluded", "insufficient_data"}
                for result in evaluation.results
            )
            else "completed"
        )
        return self.strategy_store.save_run(
            strategy_id=strategy_id,
            strategy_version=version,
            as_of_date=as_of_date,
            status=run_status,
            rule_hash=evaluation.rule_hash,
            data_fingerprint=evaluation.data_fingerprint,
            symbols=unique_symbols,
            results=evaluation.results,
        )
