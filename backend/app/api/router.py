from fastapi import APIRouter

from backend.app.api.alert import router as alert_router
from backend.app.api.analysis import router as analysis_router
from backend.app.api.classification import router as classification_router
from backend.app.api.health import router as health_router
from backend.app.api.market import router as market_router
from backend.app.api.storage import router as storage_router
from backend.app.api.strategy import router as strategy_router
from backend.app.api.strategy import run_router as strategy_run_router
from backend.app.api.user import portfolio_router, watchlist_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(health_router)
api_router.include_router(storage_router)
api_router.include_router(market_router)
api_router.include_router(portfolio_router)
api_router.include_router(watchlist_router)
api_router.include_router(strategy_router)
api_router.include_router(strategy_run_router)
api_router.include_router(alert_router)
api_router.include_router(analysis_router)
api_router.include_router(classification_router)
