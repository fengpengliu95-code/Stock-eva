import hashlib
import json
from datetime import date

from backend.app.market.store import MarketStore
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.engine import StrategyEngine
from backend.app.strategy.models import StrategyRunBatchRecord, StrategyRunRecord
from backend.app.strategy.store import StrategyStore

MAX_RUN_SYMBOLS = 20
MAX_MARKET_SCAN_BATCH_SIZE = 200


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
                result.status in {"excluded", "insufficient_data"} for result in evaluation.results
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

    def run_all_main_board(
        self,
        strategy_id: str,
        *,
        version: int,
        idempotency_key: str | None = None,
    ) -> StrategyRunRecord:
        stored_version = self.strategy_store.get_version(strategy_id, version)
        rule = validate_rule(stored_version.rule_ast)
        if rule.rule_hash != stored_version.rule_hash:
            raise ValueError("stored strategy hash mismatch")

        published = self.market_store.published_refresh()
        if published is None or published.status != "ready":
            raise ValueError("a ready published market snapshot is required")
        as_of_date = published.requested_date
        scan_key = idempotency_key or (
            f"strategy-scan:{strategy_id}:v{version}:{as_of_date.isoformat()}:all-main-board"
        )
        if not scan_key.strip() or len(scan_key) > 240:
            raise ValueError("idempotency key must contain 1 to 240 characters")
        existing = self.strategy_store.find_run_by_idempotency_key(scan_key)
        if existing is not None:
            if (
                existing.strategy_id != strategy_id
                or existing.strategy_version != version
                or existing.as_of_date != as_of_date
                or existing.scope != "all_main_board"
            ):
                raise ValueError("idempotency key belongs to a different strategy scan")
            return existing
        symbols = sorted(
            {
                bar.symbol
                for bar in self.market_store.canonical_bars(
                    as_of_date,
                    source="baostock",
                )
                if bar.security_type == "stock" and bar.board == "main"
            }
        )
        if not symbols:
            raise ValueError("published market snapshot has no main-board stocks")

        engine = StrategyEngine(self.market_store)
        results = []
        batches: list[StrategyRunBatchRecord] = []
        failed_symbols: list[str] = []
        fingerprint_parts: list[dict[str, object]] = []
        for offset in range(0, len(symbols), MAX_MARKET_SCAN_BATCH_SIZE):
            batch_symbols = symbols[offset : offset + MAX_MARKET_SCAN_BATCH_SIZE]
            batch_index = len(batches) + 1
            try:
                evaluation = engine.evaluate(
                    rule,
                    symbols=batch_symbols,
                    as_of_date=as_of_date,
                )
            except Exception:
                failed_symbols.extend(batch_symbols)
                batches.append(
                    StrategyRunBatchRecord(
                        index=batch_index,
                        status="error",
                        symbol_count=len(batch_symbols),
                        first_symbol=batch_symbols[0],
                        last_symbol=batch_symbols[-1],
                        error_code="batch_evaluation_failed",
                    )
                )
                fingerprint_parts.append(
                    {
                        "batch": batch_index,
                        "symbols": batch_symbols,
                        "status": "error",
                    }
                )
                continue

            results.extend(evaluation.results)
            batches.append(
                StrategyRunBatchRecord(
                    index=batch_index,
                    status="completed",
                    symbol_count=len(batch_symbols),
                    first_symbol=batch_symbols[0],
                    last_symbol=batch_symbols[-1],
                    matched_count=sum(result.status == "matched" for result in evaluation.results),
                    not_matched_count=sum(
                        result.status == "not_matched" for result in evaluation.results
                    ),
                    excluded_count=sum(
                        result.status == "excluded" for result in evaluation.results
                    ),
                    insufficient_count=sum(
                        result.status == "insufficient_data" for result in evaluation.results
                    ),
                    data_fingerprint=evaluation.data_fingerprint,
                )
            )
            fingerprint_parts.append(
                {
                    "batch": batch_index,
                    "symbols": batch_symbols,
                    "status": "completed",
                    "data_fingerprint": evaluation.data_fingerprint,
                }
            )

        completed_batches = sum(batch.status == "completed" for batch in batches)
        if completed_batches == 0:
            run_status = "error"
        elif failed_symbols or any(
            result.status in {"excluded", "insufficient_data"} for result in results
        ):
            run_status = "partial"
        else:
            run_status = "completed"
        data_fingerprint = hashlib.sha256(
            json.dumps(
                fingerprint_parts,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return self.strategy_store.save_run(
            strategy_id=strategy_id,
            strategy_version=version,
            as_of_date=as_of_date,
            status=run_status,
            rule_hash=rule.rule_hash,
            data_fingerprint=data_fingerprint,
            symbols=symbols,
            results=results,
            scope="all_main_board",
            batch_size=MAX_MARKET_SCAN_BATCH_SIZE,
            batches=batches,
            failed_symbols=failed_symbols,
            idempotency_key=scan_key,
        )
