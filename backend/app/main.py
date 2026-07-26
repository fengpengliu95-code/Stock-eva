import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

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
from backend.app.storage.dataset import DatasetError, NasMarketStore
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
    control_store = MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
    )
    market_store = (
        NasMarketStore(
            control_store,
            settings.nas_market_dataset_root,
            layout.local_paths.staging,
        )
        if readiness.mode == "nas"
        else control_store
    )
    if isinstance(market_store, NasMarketStore):
        try:
            market_store.validate_readiness()
        except DatasetError:
            yield
            return
    user_store = UserStore(layout.local_paths.user_database)
    service = MarketAutomationService(
        market_store,
        BaoStockProvider(
            min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
            factor_cache_path=str(layout.local_paths.factor_cache_database),
            socket_timeout_seconds=settings.baostock_socket_timeout_seconds,
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


@app.exception_handler(DatasetError)
async def nas_dataset_error_handler(_, __: DatasetError) -> JSONResponse:
    """An SMB disconnect must degrade market consumers instead of returning 500."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": {
                "code": "market_storage_unavailable",
                "storage_status": "unavailable",
                "reason_code": "nas_dataset_read_failed",
            }
        },
    )
