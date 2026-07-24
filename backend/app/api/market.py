from collections.abc import Callable
from datetime import date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request

from backend.app.api.storage import get_storage_readiness
from backend.app.config import Settings, get_settings
from backend.app.market.automation import get_market_clock
from backend.app.market.calendar import SHANGHAI, TradingCalendar, get_trading_calendar
from backend.app.market.models import MarketDataStatus, MarketSummary, PriceSeriesPoint
from backend.app.market.series import DataQualityError, PriceSeriesService
from backend.app.market.service import MarketSummaryService
from backend.app.market.store import MarketStore
from backend.app.storage.layout import StorageLayout
from backend.app.storage.models import StorageReadiness

router = APIRouter(prefix="/market", tags=["market"])


def get_market_store(
    settings: Annotated[Settings, Depends(get_settings)],
    readiness: Annotated[StorageReadiness, Depends(get_storage_readiness)],
) -> MarketStore:
    if not readiness.market_data_available:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "market_storage_unavailable",
                "storage_status": readiness.status,
                "reason_code": readiness.reason_code,
            },
        )
    layout = StorageLayout(settings)
    layout.ensure_local_runtime_dirs()
    return MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
    )


@router.get("/summary", response_model=MarketSummary)
def market_summary(
    request: Request,
    store: Annotated[MarketStore, Depends(get_market_store)],
    calendar: Annotated[TradingCalendar, Depends(get_trading_calendar)],
    clock: Annotated[Callable[[], datetime], Depends(get_market_clock)],
) -> MarketSummary:
    if request.query_params:
        raise HTTPException(
            status_code=422,
            detail="market summary does not accept client freshness parameters",
        )
    expected_session = calendar.latest_expected_session(clock())
    return MarketSummaryService(store).latest(expected_session=expected_session)


@router.get("/status", response_model=MarketDataStatus)
def market_status(
    store: Annotated[MarketStore, Depends(get_market_store)],
    calendar: Annotated[TradingCalendar, Depends(get_trading_calendar)],
    clock: Annotated[Callable[[], datetime], Depends(get_market_clock)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> MarketDataStatus:
    now = clock().astimezone(SHANGHAI)
    expected = calendar.latest_expected_session(now)
    published = store.published_refresh()
    scheduler = store.scheduler_state()
    calendar_status = (
        scheduler.calendar_status
        if scheduler is not None and scheduler.calendar_status != "confirmed"
        else (
            "confirmed"
            if calendar.session_status(now.date()) != "unknown"
            else "unavailable"
        )
    )
    refresh_state = (
        scheduler.refresh_state
        if settings.auto_refresh_enabled and scheduler is not None
        else "idle" if settings.auto_refresh_enabled else "disabled"
    )
    phase = calendar.market_phase(now)
    if phase == "after_close_waiting":
        if refresh_state == "running":
            phase = "refreshing"
        elif (
            expected is not None
            and published is not None
            and published.requested_date >= expected
        ):
            phase = "complete"
        elif refresh_state in {"delayed", "error"}:
            phase = "delayed"
    return MarketDataStatus(
        market_phase=phase,
        calendar_status=calendar_status,
        latest_expected_session=expected,
        published_as_of=published.requested_date if published else None,
        refresh_state=refresh_state,
        last_success_at=(
            scheduler.last_success_at
            if scheduler is not None and scheduler.last_success_at is not None
            else published.completed_at if published else None
        ),
        next_retry_at=scheduler.next_retry_at if scheduler else None,
        calendar_sources=[
            item.model_dump()
            for item in calendar.sources_for(now.year)
        ],
    )


@router.get("/history/dates", response_model=list[date])
def market_history_dates(
    store: Annotated[MarketStore, Depends(get_market_store)],
) -> list[date]:
    return store.available_dates(source="baostock")


@router.get("/history/{symbol}", response_model=list[PriceSeriesPoint])
def market_history(
    symbol: str,
    start: date,
    end: date,
    store: Annotated[MarketStore, Depends(get_market_store)],
    adjustment: Literal["none", "qfq"] = "qfq",
) -> list[PriceSeriesPoint]:
    if start > end:
        raise HTTPException(status_code=422, detail="start must be on or before end")
    try:
        return PriceSeriesService(store).read(
            symbol,
            start=start,
            end=end,
            adjustment=adjustment,
        )
    except DataQualityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
