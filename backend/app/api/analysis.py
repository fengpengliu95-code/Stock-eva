from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path

from backend.app.analysis.models import SecurityAnalysisResponse
from backend.app.analysis.service import SecurityAnalysisService
from backend.app.api.market import get_market_store
from backend.app.market.series import DataQualityError
from backend.app.market.store import MarketStore

router = APIRouter(prefix="/securities", tags=["analysis"])


@router.get("/{symbol}/analysis", response_model=SecurityAnalysisResponse)
def security_analysis(
    symbol: Annotated[str, Path(pattern=r"^(?:sh|sz)\.\d{6}$")],
    start: date,
    end: date,
    store: Annotated[MarketStore, Depends(get_market_store)],
) -> SecurityAnalysisResponse:
    if start > end:
        raise HTTPException(status_code=422, detail="start must be on or before end")
    try:
        return SecurityAnalysisService(store).read(
            symbol,
            start=start,
            end=end,
        )
    except DataQualityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
