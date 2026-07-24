"""Deterministic after-close publication and scheduling policy."""

import asyncio
import hashlib
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal

from pydantic import BaseModel

from backend.app.market.baostock import INDEX_SYMBOLS
from backend.app.market.calendar import SHANGHAI, TradingCalendar
from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore

RefreshState = Literal[
    "disabled",
    "idle",
    "scheduled",
    "running",
    "retry_wait",
    "success",
    "delayed",
    "error",
]


class SchedulerState(BaseModel):
    target_session: date | None = None
    refresh_state: RefreshState = "idle"
    attempt_count: int = 0
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    next_retry_at: datetime | None = None
    calendar_status: Literal["confirmed", "conflict", "unavailable"] = "confirmed"
    error_code: str | None = None


class ScheduleDecision(BaseModel):
    action: Literal["run", "wait", "none"]
    target_session: date | None
    refresh_state: RefreshState
    next_run_at: datetime | None = None


class AutomationOutcome(BaseModel):
    decision: ScheduleDecision
    state: SchedulerState
    result: RefreshResult | None = None


class SchedulePolicy:
    RETRY_TIMES = (time(18, 40), time(19, 20), time(20, 10), time(21, 0))
    CORRECTION_TIME = time(7, 15)

    def __init__(self, calendar: TradingCalendar) -> None:
        self.calendar = calendar

    @staticmethod
    def availability_for(session: date) -> datetime:
        return datetime.combine(session, time(18, 10), tzinfo=SHANGHAI)

    def next_retry_after(self, session: date, after: datetime) -> datetime | None:
        local = after.astimezone(SHANGHAI)
        slots = [
            datetime.combine(session, item, tzinfo=SHANGHAI)
            for item in self.RETRY_TIMES
        ]
        slots.append(
            datetime.combine(
                session + timedelta(days=1),
                self.CORRECTION_TIME,
                tzinfo=SHANGHAI,
            )
        )
        return next((slot for slot in slots if slot > local), None)

    def decide(
        self,
        now: datetime,
        *,
        published_as_of: date | None,
        state: SchedulerState | None,
    ) -> ScheduleDecision:
        local = now.astimezone(SHANGHAI)
        target = self.calendar.latest_expected_session(local)
        if target is None:
            return ScheduleDecision(
                action="none",
                target_session=None,
                refresh_state="error",
            )
        if published_as_of is not None and published_as_of >= target:
            return ScheduleDecision(
                action="none",
                target_session=target,
                refresh_state="success",
            )
        available_at = self.availability_for(target)
        if local < available_at:
            return ScheduleDecision(
                action="wait",
                target_session=target,
                refresh_state="scheduled",
                next_run_at=available_at,
            )
        if state is None or state.target_session != target or state.attempt_count == 0:
            return ScheduleDecision(
                action="run",
                target_session=target,
                refresh_state="running",
            )
        if state.next_retry_at is not None:
            if local >= state.next_retry_at.astimezone(SHANGHAI):
                return ScheduleDecision(
                    action="run",
                    target_session=target,
                    refresh_state="running",
                )
            return ScheduleDecision(
                action="wait",
                target_session=target,
                refresh_state="retry_wait",
                next_run_at=state.next_retry_at,
            )
        return ScheduleDecision(
            action="none",
            target_session=target,
            refresh_state="delayed",
        )


def _publication_issues(batch, required_symbols: set[str]) -> list[str]:
    loaded = {bar.symbol for bar in batch.bars}
    expected = set(batch.expected_symbols)
    issues = [
        f"missing_symbol:{symbol}"
        for symbol in sorted((expected - loaded) | set(batch.failed_symbols))
    ]
    issues.extend(
        f"required_index_missing:{symbol}"
        for symbol in INDEX_SYMBOLS
        if symbol not in loaded
    )
    issues.extend(
        f"required_symbol_missing:{symbol}"
        for symbol in sorted(required_symbols - loaded)
    )
    issues.extend(
        f"missing_adjust_factor:{bar.symbol}"
        for bar in batch.bars
        if bar.security_type == "stock"
        and not bar.is_suspended
        and bar.adjust_factor is None
    )
    issues.extend(
        f"quality_error:{bar.symbol}"
        for bar in batch.bars
        if bar.quality_status == "error"
    )
    return list(dict.fromkeys(issues))


def run_publication_refresh(
    store: MarketStore,
    provider,
    *,
    trade_date: date,
    required_symbols: set[str],
) -> RefreshResult:
    """Fetch to canonical staging, validate all gates, then move the pointer."""
    started_at = datetime.now(UTC)
    request_key = f"daily:baostock:{trade_date.isoformat()}:all-main-board"
    request_prefix = hashlib.sha256(request_key.encode()).hexdigest()[:12]
    run_id = f"{request_prefix}-{uuid.uuid4().hex[:12]}"
    try:
        batch = provider.fetch(trade_date, symbols=None)
        loaded = {bar.symbol for bar in batch.bars}
        expected = set(batch.expected_symbols)
        issues = _publication_issues(batch, required_symbols)
        failures = sorted((expected - loaded) | set(batch.failed_symbols))
        requested_count = len(expected)
        succeeded_count = len(expected & loaded)
        coverage = succeeded_count / requested_count if requested_count else 0.0
        if coverage < 1:
            issues.append("incomplete_symbol_coverage")
        status = "ready" if requested_count and not issues else "partial"
        result = RefreshResult(
            run_id=run_id,
            request_key=request_key,
            requested_date=trade_date,
            source="baostock",
            status=status,
            requested_count=requested_count,
            succeeded_count=succeeded_count,
            coverage_ratio=coverage,
            failed_symbols=failures,
            quality_issues=list(dict.fromkeys(issues)),
            started_at=started_at,
            completed_at=datetime.now(UTC),
        )
        # A failed validation remains auditable, but cannot mutate the canonical
        # rows behind an existing published pointer for the same session.
        store.save_refresh(
            batch.bars if status == "ready" else [],
            result,
            publish=status == "ready",
        )
        return result
    except Exception:
        result = RefreshResult(
            run_id=run_id,
            request_key=request_key,
            requested_date=trade_date,
            source="baostock",
            status="error",
            requested_count=0,
            succeeded_count=0,
            coverage_ratio=0,
            quality_issues=["provider_error"],
            error_message="provider request failed",
            started_at=started_at,
            completed_at=datetime.now(UTC),
        )
        store.save_refresh([], result, publish=False)
        return result


class MarketAutomationService:
    def __init__(
        self,
        store: MarketStore,
        provider,
        calendar: TradingCalendar,
        *,
        required_symbols: Callable[[], set[str]],
    ) -> None:
        self.store = store
        self.provider = provider
        self.calendar = calendar
        self.required_symbols = required_symbols
        self.policy = SchedulePolicy(calendar)

    def run_due_once(self, now: datetime) -> AutomationOutcome:
        local = now.astimezone(SHANGHAI)
        published = self.store.published_refresh()
        current = self.store.scheduler_state()
        decision = self.policy.decide(
            local,
            published_as_of=published.requested_date if published else None,
            state=current,
        )
        if decision.action != "run":
            state = current or SchedulerState(
                target_session=decision.target_session,
                refresh_state=decision.refresh_state,
                next_retry_at=decision.next_run_at,
            )
            if decision.action == "wait" and current is None:
                self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)

        target = decision.target_session
        if target is None:
            state = SchedulerState(
                refresh_state="error",
                calendar_status="unavailable",
                error_code="calendar_unavailable",
            )
            self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)

        attempt_count = (
            current.attempt_count + 1
            if current is not None and current.target_session == target
            else 1
        )
        running = SchedulerState(
            target_session=target,
            refresh_state="running",
            attempt_count=attempt_count,
            last_attempt_at=local,
            last_success_at=current.last_success_at if current else None,
        )
        self.store.save_scheduler_state(running)

        try:
            provider_sessions = self.provider.trading_dates(target, target)
        except Exception:
            state = self._failed_state(
                running,
                local,
                error_code="calendar_validation_unavailable",
                calendar_status="unavailable",
            )
            self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)
        if provider_sessions != [target]:
            state = running.model_copy(
                update={
                    "refresh_state": "error",
                    "calendar_status": "conflict",
                    "error_code": "calendar_provider_conflict",
                }
            )
            self.store.save_scheduler_state(state)
            return AutomationOutcome(decision=decision, state=state)

        result = run_publication_refresh(
            self.store,
            self.provider,
            trade_date=target,
            required_symbols=self.required_symbols(),
        )
        if result.status == "ready":
            state = running.model_copy(
                update={
                    "refresh_state": "success",
                    "last_success_at": result.completed_at,
                    "calendar_status": "confirmed",
                    "next_retry_at": None,
                    "error_code": None,
                }
            )
        else:
            state = self._failed_state(
                running,
                local,
                error_code=(
                    "publication_incomplete"
                    if result.status == "partial"
                    else "provider_error"
                ),
                calendar_status="confirmed",
            )
        self.store.save_scheduler_state(state)
        return AutomationOutcome(decision=decision, state=state, result=result)

    def _failed_state(
        self,
        running: SchedulerState,
        now: datetime,
        *,
        error_code: str,
        calendar_status: Literal["confirmed", "conflict", "unavailable"],
    ) -> SchedulerState:
        next_retry = self.policy.next_retry_after(running.target_session, now)
        return running.model_copy(
            update={
                "refresh_state": "retry_wait" if next_retry else "delayed",
                "next_retry_at": next_retry,
                "calendar_status": calendar_status,
                "error_code": error_code,
            }
        )


def collect_required_symbols(user_store) -> set[str]:
    symbols = {item.symbol for item in user_store.list_positions()}
    for watchlist in user_store.list_watchlists():
        symbols.update(
            item.symbol
            for item in user_store.list_watchlist_items(watchlist.id)
        )
    return symbols


async def run_automation_loop(
    service: MarketAutomationService,
    stop: asyncio.Event,
    *,
    clock,
    poll_seconds: float = 60,
) -> None:
    """Re-check regularly so process start and wake both perform catch-up."""
    while not stop.is_set():
        await asyncio.to_thread(service.run_due_once, clock())
        if stop.is_set():
            break
        try:
            await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
        except TimeoutError:
            continue


def get_market_clock():
    return lambda: datetime.now(SHANGHAI)
