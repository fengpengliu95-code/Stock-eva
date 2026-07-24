import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.api.router import api_router
from backend.app.config import get_settings
from backend.app.market.automation import (
    MarketAutomationService,
    collect_required_symbols,
    get_market_clock,
    run_automation_loop,
)
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.calendar import get_trading_calendar
from backend.app.market.store import MarketStore
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import StoragePreflight
from backend.app.user.store import UserStore

settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not settings.auto_refresh_enabled:
        yield
        return
    readiness = StoragePreflight(settings).inspect()
    if not readiness.market_data_available:
        yield
        return
    layout = StorageLayout(settings)
    layout.ensure_local_runtime_dirs()
    market_store = MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
    )
    user_store = UserStore(
        layout.local_paths.user_database
    )
    service = MarketAutomationService(
        market_store,
        BaoStockProvider(
            min_request_interval_seconds=(
                settings.auto_refresh_min_request_interval_seconds
            )
        ),
        get_trading_calendar(),
        required_symbols=lambda: collect_required_symbols(user_store),
        lock_path=layout.market_refresh_lock,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_automation_loop(
            service,
            stop,
            clock=get_market_clock(),
        )
    )
    try:
        yield
    finally:
        stop.set()
        await task


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)
app.include_router(api_router)
