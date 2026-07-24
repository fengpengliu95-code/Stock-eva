from collections.abc import Callable
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status

from backend.app.api.market import get_market_store
from backend.app.config import Settings, get_settings
from backend.app.market.automation import get_market_clock
from backend.app.market.calendar import TradingCalendar, get_trading_calendar
from backend.app.market.store import MarketStore
from backend.app.user.models import (
    PortfolioValuation,
    Position,
    PositionCreate,
    PositionUpdate,
    Watchlist,
    WatchlistCreate,
    WatchlistItem,
    WatchlistItemCreate,
)
from backend.app.user.portfolio import PortfolioValuationService
from backend.app.user.store import (
    ConflictError,
    DuplicateError,
    NotFoundError,
    UserStore,
)

portfolio_router = APIRouter(prefix="/portfolio", tags=["portfolio"])
watchlist_router = APIRouter(prefix="/watchlists", tags=["watchlists"])


def get_user_store(settings: Annotated[Settings, Depends(get_settings)]) -> UserStore:
    return UserStore(settings.user_data_dir / settings.user_database_name)


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, NotFoundError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, (ConflictError, DuplicateError)):
        return HTTPException(status_code=409, detail=str(exc))
    return HTTPException(status_code=400, detail=str(exc))


@portfolio_router.post(
    "/positions",
    response_model=Position,
    status_code=status.HTTP_201_CREATED,
)
def create_position(
    data: PositionCreate,
    store: Annotated[UserStore, Depends(get_user_store)],
) -> Position:
    try:
        return store.create_position(data)
    except (DuplicateError, ValueError) as exc:
        raise _http_error(exc) from exc


@portfolio_router.get("/positions", response_model=list[Position])
def list_positions(
    store: Annotated[UserStore, Depends(get_user_store)],
) -> list[Position]:
    return store.list_positions()


@portfolio_router.get("/positions/{identifier}", response_model=Position)
def get_position(
    identifier: str,
    store: Annotated[UserStore, Depends(get_user_store)],
) -> Position:
    try:
        return store.get_position(identifier)
    except NotFoundError as exc:
        raise _http_error(exc) from exc


@portfolio_router.put("/positions/{identifier}", response_model=Position)
def update_position(
    identifier: str,
    data: PositionUpdate,
    store: Annotated[UserStore, Depends(get_user_store)],
) -> Position:
    try:
        return store.update_position(identifier, data)
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc


@portfolio_router.delete(
    "/positions/{identifier}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_position(
    identifier: str,
    store: Annotated[UserStore, Depends(get_user_store)],
    expected_version: int = Query(ge=1),
) -> Response:
    try:
        store.delete_position(identifier, expected_version)
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@portfolio_router.get("/valuation", response_model=PortfolioValuation)
def portfolio_valuation(
    request: Request,
    user_store: Annotated[UserStore, Depends(get_user_store)],
    market_store: Annotated[MarketStore, Depends(get_market_store)],
    calendar: Annotated[TradingCalendar, Depends(get_trading_calendar)],
    clock: Annotated[Callable[[], datetime], Depends(get_market_clock)],
) -> PortfolioValuation:
    if request.query_params:
        raise HTTPException(
            status_code=422,
            detail="portfolio valuation does not accept client freshness parameters",
        )
    return PortfolioValuationService(user_store, market_store).latest(
        calendar.latest_expected_session(clock())
    )


@watchlist_router.post(
    "",
    response_model=Watchlist,
    status_code=status.HTTP_201_CREATED,
)
def create_watchlist(
    data: WatchlistCreate,
    store: Annotated[UserStore, Depends(get_user_store)],
) -> Watchlist:
    try:
        return store.create_watchlist(data.name)
    except (DuplicateError, ValueError) as exc:
        raise _http_error(exc) from exc


@watchlist_router.get("", response_model=list[Watchlist])
def list_watchlists(
    store: Annotated[UserStore, Depends(get_user_store)],
) -> list[Watchlist]:
    return store.list_watchlists()


@watchlist_router.delete(
    "/{identifier}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_watchlist(
    identifier: str,
    store: Annotated[UserStore, Depends(get_user_store)],
    expected_version: int = Query(ge=1),
) -> Response:
    try:
        store.delete_watchlist(identifier, expected_version)
    except (NotFoundError, ConflictError) as exc:
        raise _http_error(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@watchlist_router.get("/{identifier}/items", response_model=list[WatchlistItem])
def list_watchlist_items(
    identifier: str,
    store: Annotated[UserStore, Depends(get_user_store)],
) -> list[WatchlistItem]:
    try:
        return store.list_watchlist_items(identifier)
    except NotFoundError as exc:
        raise _http_error(exc) from exc


@watchlist_router.post("/{identifier}/items")
def add_watchlist_item(
    identifier: str,
    data: WatchlistItemCreate,
    response: Response,
    store: Annotated[UserStore, Depends(get_user_store)],
) -> dict[str, object]:
    try:
        item, created = store.add_watchlist_item(identifier, data.symbol)
    except NotFoundError as exc:
        raise _http_error(exc) from exc
    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return {"created": created, "item": item}


@watchlist_router.delete(
    "/{identifier}/items/{symbol}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def remove_watchlist_item(
    identifier: str,
    symbol: str,
    store: Annotated[UserStore, Depends(get_user_store)],
) -> Response:
    try:
        normalized = WatchlistItemCreate(symbol=symbol).symbol
        store.remove_watchlist_item(identifier, normalized)
    except (NotFoundError, ValueError) as exc:
        raise _http_error(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
