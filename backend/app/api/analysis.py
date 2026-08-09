from datetime import date, datetime
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Path

from backend.app.analysis.models import SecurityAnalysisResponse
from backend.app.analysis.service import SecurityAnalysisService
from backend.app.api.market import get_market_store
from backend.app.config import Settings, get_settings
from backend.app.market.series import DataQualityError
from backend.app.market.store import MarketStore
from backend.app.regime.models import MarketRegimeResult
from backend.app.regime.service import MarketRegimeService
from backend.app.regime.snapshots import (
    MarketRegimeSnapshot,
    RegimeSnapshotStore,
    RegimeSnapshotUnavailable,
)
from backend.app.regime.store import (
    MarketReadUnavailable,
    MarketRegimeStore,
    market_regime_store_from_settings,
)
from backend.app.storage.layout import StorageLayout

router = APIRouter(prefix="/securities", tags=["analysis"])
market_regime_router = APIRouter(prefix="/analysis", tags=["analysis"])


def get_regime_today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def get_market_regime_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> MarketRegimeStore:
    return market_regime_store_from_settings(settings)


def get_regime_snapshot_store(
    settings: Annotated[Settings, Depends(get_settings)],
) -> RegimeSnapshotStore:
    return RegimeSnapshotStore(StorageLayout(settings).regime_snapshot_database)


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


@market_regime_router.get(
    "/market-regime",
    response_model=MarketRegimeResult,
)
def market_regime(
    as_of: date,
    store: Annotated[MarketRegimeStore, Depends(get_market_regime_store)],
    today: Annotated[date, Depends(get_regime_today)],
) -> MarketRegimeResult:
    if as_of > today:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "future_as_of",
                "as_of": as_of.isoformat(),
                "today": today.isoformat(),
                "timezone": "Asia/Shanghai",
            },
        )
    try:
        return MarketRegimeService().evaluate(store.read(as_of))
    except MarketReadUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": exc.code,
                "storage_status": "unavailable",
            },
        ) from exc


@market_regime_router.get(
    "/market-regime/snapshots/{as_of}",
    response_model=MarketRegimeSnapshot,
)
def market_regime_snapshot(
    as_of: date,
    store: Annotated[RegimeSnapshotStore, Depends(get_regime_snapshot_store)],
    today: Annotated[date, Depends(get_regime_today)],
) -> MarketRegimeSnapshot:
    if as_of > today:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "future_as_of",
                "as_of": as_of.isoformat(),
                "today": today.isoformat(),
                "timezone": "Asia/Shanghai",
            },
        )
    try:
        snapshot = store.read_exact(as_of, "market-regime-v1")
    except RegimeSnapshotUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "regime_snapshot_store_unavailable",
                "storage_status": "unavailable",
            },
        ) from exc
    if snapshot is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "regime_snapshot_not_found",
                "as_of": as_of.isoformat(),
            },
        )
    return snapshot
