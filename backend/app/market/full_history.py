"""Resumable, per-session full-main-board publication into a NAS dataset."""

import hashlib
from datetime import date
from typing import Literal

from pydantic import BaseModel

from backend.app.market.automation import run_publication_refresh
from backend.app.market.models import RefreshResult


class FullMarketHistoryPlan(BaseModel):
    run_id: str
    request_key: str
    start_date: date
    end_date: date
    trading_dates: list[date]
    already_published_dates: list[date]
    pending_dates: list[date]
    total_sessions: int
    pending_sessions: int
    estimated_provider_requests_lower_bound: int


class FullMarketHistoryResult(BaseModel):
    run_id: str
    status: Literal["ready", "partial", "error"]
    total_sessions: int
    published_sessions: int
    skipped_sessions: int
    failed_dates: list[date]
    date_results: list[RefreshResult]


class FullMarketHistoryService:
    """Publish one immutable, strictly validated manifest partition per date."""

    def __init__(self, store, provider) -> None:
        self.store = store
        self.provider = provider

    def _published_dates(self) -> set[date]:
        reader = getattr(self.store, "manifest_dates", None)
        if reader is None:
            reader = self.store.available_dates
        return set(reader("baostock"))

    def plan(
        self,
        *,
        start_date: date,
        end_date: date,
        max_sessions: int,
    ) -> FullMarketHistoryPlan:
        if start_date > end_date:
            raise ValueError("start date must not be after end date")
        if max_sessions < 1:
            raise ValueError("max sessions must be positive")
        trading_dates = self.provider.trading_dates(start_date, end_date)
        if not trading_dates:
            raise ValueError("date range contains no trading days")
        trading_dates = sorted(set(trading_dates))
        if len(trading_dates) > max_sessions:
            raise ValueError(
                f"planned {len(trading_dates)} sessions exceeds session cap {max_sessions}"
            )
        published = self._published_dates()
        already_published = [item for item in trading_dates if item in published]
        pending = [item for item in trading_dates if item not in published]
        request_key = (
            f"full-market-history:baostock:{trading_dates[0].isoformat()}:"
            f"{trading_dates[-1].isoformat()}:schema-v1"
        )
        return FullMarketHistoryPlan(
            run_id=hashlib.sha256(request_key.encode()).hexdigest()[:24],
            request_key=request_key,
            start_date=trading_dates[0],
            end_date=trading_dates[-1],
            trading_dates=trading_dates,
            already_published_dates=already_published,
            pending_dates=pending,
            total_sessions=len(trading_dates),
            pending_sessions=len(pending),
            # Calendar/universe/daily/factors/two indexes form a conservative
            # seven-request floor after the first factor bootstrap.
            estimated_provider_requests_lower_bound=1 + len(pending) * 7,
        )

    @staticmethod
    def daily_request_key(plan: FullMarketHistoryPlan, trade_date: date) -> str:
        return f"{plan.request_key}:date:{trade_date.isoformat()}"

    @classmethod
    def daily_run_id(cls, plan: FullMarketHistoryPlan, trade_date: date) -> str:
        return hashlib.sha256(cls.daily_request_key(plan, trade_date).encode()).hexdigest()[:24]

    def execute(self, plan: FullMarketHistoryPlan) -> FullMarketHistoryResult:
        reconciler = getattr(self.store, "reconcile_control_pointer", None)
        if reconciler is not None:
            reconciler("baostock")
        published = self._published_dates()
        skipped = sum(item in published for item in plan.trading_dates)
        results: list[RefreshResult] = []
        failed: list[date] = []
        for trade_date in plan.trading_dates:
            # Re-read manifest state on every iteration. A completed partition
            # is the resume checkpoint and is never replaced by this workflow.
            published = self._published_dates()
            if trade_date in published:
                continue
            result = run_publication_refresh(
                self.store,
                self.provider,
                trade_date=trade_date,
                required_symbols=set(),
                request_key=self.daily_request_key(plan, trade_date),
                run_id=self.daily_run_id(plan, trade_date),
                run_kind="backfill",
            )
            results.append(result)
            if result.status == "ready":
                published.add(trade_date)
            else:
                failed.append(trade_date)
        completed = sum(item in published for item in plan.trading_dates)
        status: Literal["ready", "partial", "error"]
        if completed == plan.total_sessions:
            status = "ready"
        elif completed:
            status = "partial"
        else:
            status = "error"
        return FullMarketHistoryResult(
            run_id=plan.run_id,
            status=status,
            total_sessions=plan.total_sessions,
            published_sessions=completed,
            skipped_sessions=skipped,
            failed_dates=failed,
            date_results=results,
        )
