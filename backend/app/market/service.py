from datetime import date

from backend.app.market.models import (
    Breadth,
    Completeness,
    Freshness,
    IndexSnapshot,
    MarketSummary,
    TurnoverOverview,
)
from backend.app.market.store import MarketStore


class MarketSummaryService:
    def __init__(self, store: MarketStore) -> None:
        self.store = store

    def latest(self, expected_session: date | None = None) -> MarketSummary:
        if not self.store.exists():
            return self._empty()
        published = self.store.published_refresh()
        refresh = published or self.store.latest_refresh()
        if refresh is None:
            return self._empty()

        if refresh.status == "error":
            return MarketSummary(
                status="error",
                as_of=refresh.requested_date,
                source=refresh.source,
                completeness=Completeness(
                    requested=refresh.requested_count,
                    loaded=0,
                    failed=len(refresh.failed_symbols),
                    failed_symbols=refresh.failed_symbols,
                    coverage_ratio=0.0,
                    is_complete=False,
                ),
                freshness=Freshness(),
                breadth=Breadth(),
                turnover=TurnoverOverview(),
                indexes=[],
                quality_issues=["refresh_error"],
            )

        bars = self.store.bars_for(refresh.requested_date, refresh.source)
        status = refresh.status
        issues = list(refresh.quality_issues)
        if expected_session is None:
            freshness = Freshness()
            issues.append("freshness_not_evaluated")
            issues.append("calendar_unavailable")
            if status == "ready":
                status = "partial"
        else:
            lag = max((expected_session - refresh.requested_date).days, 0)
            is_fresh = refresh.requested_date >= expected_session
            freshness = Freshness(
                evaluated=True,
                expected_as_of=expected_session,
                is_fresh=is_fresh,
                lag_calendar_days=lag,
            )
            if not is_fresh:
                status = "stale"
                issues.append("data_older_than_expected")
        if refresh.failed_symbols:
            issues.append("incomplete_symbol_coverage")

        coverage_ratio = refresh.coverage_ratio
        if coverage_ratio is None:
            coverage_ratio = (
                refresh.succeeded_count / refresh.requested_count
                if refresh.requested_count
                else 0.0
            )
        is_complete = coverage_ratio >= 1.0 and not refresh.failed_symbols
        stocks = [row for row in bars if row["security_type"] == "stock"]
        trading = [row for row in stocks if not row["is_suspended"]]
        breadth = Breadth(
            advancing=sum((row["pct_change"] or 0) > 0 for row in trading),
            declining=sum((row["pct_change"] or 0) < 0 for row in trading),
            unchanged=sum(row["pct_change"] == 0 for row in trading),
            suspended=sum(bool(row["is_suspended"]) for row in stocks),
            eligible=len(trading),
        )
        indexes = [
            IndexSnapshot(
                symbol=str(row["symbol"]),
                close=float(row["close"]),
                pct_change=row["pct_change"],
                is_suspended=bool(row["is_suspended"]),
            )
            for row in bars
            if row["security_type"] == "index"
        ]
        return MarketSummary(
            status=status,
            as_of=refresh.requested_date,
            source=refresh.source,
            completeness=Completeness(
                requested=refresh.requested_count,
                loaded=refresh.succeeded_count,
                failed=len(refresh.failed_symbols),
                failed_symbols=refresh.failed_symbols,
                coverage_ratio=coverage_ratio,
                is_complete=is_complete,
            ),
            freshness=freshness,
            breadth=breadth,
            turnover=TurnoverOverview(
                amount=sum(float(row["amount"]) for row in trading) if trading else None,
                coverage=len(trading),
            ),
            indexes=indexes,
            quality_issues=issues,
        )

    @staticmethod
    def _empty() -> MarketSummary:
        return MarketSummary(
            status="empty",
            as_of=None,
            source=None,
            completeness=Completeness(),
            freshness=Freshness(),
            breadth=Breadth(),
            turnover=TurnoverOverview(),
            indexes=[],
            quality_issues=["no_market_data"],
        )
