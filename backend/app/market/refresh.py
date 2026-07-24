import hashlib
from datetime import UTC, date, datetime

from backend.app.market.baostock import BaoStockProvider
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore


class MarketRefreshService:
    def __init__(self, store: MarketStore, provider: BaoStockProvider | None = None) -> None:
        self.store = store
        self.provider = provider or BaoStockProvider()

    def refresh(self, trade_date: date, symbols: list[str] | None = None) -> RefreshResult:
        started_at = datetime.now(UTC)
        target = "all-main-board" if symbols is None else ",".join(sorted(set(symbols)))
        request_key = f"daily:baostock:{trade_date.isoformat()}:{target}"
        run_id = hashlib.sha256(request_key.encode()).hexdigest()[:24]
        try:
            batch = self.provider.fetch(trade_date, symbols=symbols)
            missing_factors = [
                bar.symbol for bar in batch.bars if "missing_adjust_factor" in bar.quality_issues
            ]
            loaded_symbols = {bar.symbol for bar in batch.bars}
            expected_symbols = set(batch.expected_symbols)
            failures = sorted((expected_symbols - loaded_symbols) | set(batch.failed_symbols))
            quality_issues = [
                f"missing_adjust_factor:{symbol}" for symbol in sorted(set(missing_factors))
            ]
            requested_count = len(expected_symbols)
            succeeded_count = len(loaded_symbols & expected_symbols)
            coverage_ratio = (
                succeeded_count / requested_count if requested_count else 0.0
            )
            status = "partial" if failures or quality_issues or coverage_ratio < 1.0 else "ready"
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
                started_at=started_at,
                completed_at=datetime.now(UTC),
            )
            self.store.save_refresh(batch.bars, result)
            return result
        except Exception as exc:
            result = RefreshResult(
                run_id=run_id,
                request_key=request_key,
                requested_date=trade_date,
                source="baostock",
                status="error",
                requested_count=len(symbols or []),
                succeeded_count=0,
                coverage_ratio=0.0,
                failed_symbols=list(symbols or []),
                quality_issues=["provider_error"],
                error_message=str(exc),
                started_at=started_at,
                completed_at=datetime.now(UTC),
            )
            self.store.save_refresh([], result)
            return result
