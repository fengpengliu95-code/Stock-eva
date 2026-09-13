import hashlib
from datetime import UTC, date, datetime

from backend.app.market.baostock import BaoStockProvider
from backend.app.market.failures import (
    MarketFailure,
    legacy_failure_quality_issues,
    market_failure_from_exception,
    public_failure_message,
)
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import DatasetError, LineageInput, NasMarketStore


class MarketRefreshService:
    def __init__(
        self,
        store: MarketStore,
        provider: BaoStockProvider | None = None,
        *,
        lineage_input: LineageInput | dict[str, object] | None = None,
    ) -> None:
        self.store = store
        self.provider = provider or BaoStockProvider()
        if isinstance(store, NasMarketStore) and lineage_input is None:
            raise DatasetError("source lineage input is required for NAS publication")
        self.lineage_input = lineage_input

    def refresh(self, trade_date: date, symbols: list[str] | None = None) -> RefreshResult:
        started_at = datetime.now(UTC)
        target = "all-main-board" if symbols is None else ",".join(sorted(set(symbols)))
        request_key = f"daily:baostock:{trade_date.isoformat()}:{target}"
        run_id = hashlib.sha256(request_key.encode()).hexdigest()[:24]
        failure_stage = "fetch"
        requested_count = len(symbols or [])
        succeeded_count = 0
        failures = list(symbols or [])

        def failed_result(failure: MarketFailure) -> RefreshResult:
            result = RefreshResult(
                run_id=run_id,
                request_key=request_key,
                requested_date=trade_date,
                source="baostock",
                status="error",
                requested_count=requested_count,
                succeeded_count=succeeded_count,
                coverage_ratio=0.0,
                failed_symbols=failures,
                quality_issues=legacy_failure_quality_issues(failure),
                error_message=public_failure_message(failure),
                failure_stage=failure.failure_stage,
                failure_class=failure.failure_class,
                retryable=failure.retryable,
                started_at=started_at,
                completed_at=datetime.now(UTC),
            )
            if failure.failure_stage != "publish":
                try:
                    if isinstance(self.store, NasMarketStore):
                        self.store.save_refresh(
                            [], result, publish=False, lineage_input=self.lineage_input
                        )
                    else:
                        self.store.save_refresh([], result, publish=False)
                except Exception as error:
                    storage_failure = market_failure_from_exception(
                        error,
                        stage="publish",
                        default_class="storage",
                        default_retryable=False,
                    )
                    return failed_result(storage_failure)
            return result

        try:
            batch = self.provider.fetch(trade_date, symbols=symbols)
            failure_stage = "validate"
            missing_factors = [
                bar.symbol
                for bar in batch.bars
                if not bar.is_suspended and "missing_adjust_factor" in bar.quality_issues
            ]
            loaded_symbols = {bar.symbol for bar in batch.bars}
            expected_symbols = set(batch.expected_symbols)
            failures = sorted((expected_symbols - loaded_symbols) | set(batch.failed_symbols))
            quality_issues = [
                f"missing_adjust_factor:{symbol}" for symbol in sorted(set(missing_factors))
            ]
            batch_failure = getattr(batch, "failure", None)
            if batch_failure is not None:
                quality_issues.extend(legacy_failure_quality_issues(batch_failure))
            requested_count = len(expected_symbols)
            succeeded_count = len(loaded_symbols & expected_symbols)
            coverage_ratio = succeeded_count / requested_count if requested_count else 0.0
            status = "partial" if failures or quality_issues or coverage_ratio < 1.0 else "ready"
            publication_failure = None
            if status == "partial":
                publication_failure = batch_failure or MarketFailure(
                    failure_stage="validate",
                    failure_class="coverage" if failures or coverage_ratio < 1 else "semantic",
                    retryable=bool(failures or coverage_ratio < 1),
                )
            result = RefreshResult(
                run_id=run_id,
                request_key=request_key,
                requested_date=trade_date,
                source="baostock",
                status=status,
                requested_count=requested_count,
                succeeded_count=succeeded_count,
                coverage_ratio=coverage_ratio,
                failed_symbols=failures,
                quality_issues=quality_issues,
                failure_stage=(
                    publication_failure.failure_stage if publication_failure is not None else None
                ),
                failure_class=(
                    publication_failure.failure_class if publication_failure is not None else None
                ),
                retryable=(
                    publication_failure.retryable if publication_failure is not None else None
                ),
                started_at=started_at,
                completed_at=datetime.now(UTC),
            )
            failure_stage = "publish"
            if isinstance(self.store, NasMarketStore):
                self.store.save_refresh(
                    batch.bars,
                    result,
                    publish=False,
                    lineage_input=self.lineage_input,
                )
            else:
                self.store.save_refresh(batch.bars, result, publish=False)
            return result
        except Exception as error:
            failure = market_failure_from_exception(
                error,
                stage=failure_stage,
                default_class="storage" if failure_stage == "publish" else "internal",
                default_retryable=False,
            )
            return failed_result(failure)
