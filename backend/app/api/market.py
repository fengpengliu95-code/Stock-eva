from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException

from backend.app.config import Settings, get_settings
from backend.app.market.models import MarketSummary, PriceSeriesPoint
from backend.app.market.series import DataQualityError, PriceSeriesService
from backend.app.market.service import MarketSummaryService
from backend.app.market.store import MarketStore

router = APIRouter(prefix="/market", tags=["market"])


def get_market_store(settings: Annotated[Settings, Depends(get_settings)]) -> MarketStore:
    return MarketStore(settings.market_data_dir / settings.market_database_name)


@router.get("/summary", response_model=MarketSummary)
def market_summary(
    store: Annotated[MarketStore, Depends(get_market_store)],
    expected_date: date | None = None,
) -> MarketSummary:
    return MarketSummaryService(store).latest(expected_date=expected_date)


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
