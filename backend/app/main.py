import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.app.api.router import api_router
from backend.app.config import get_settings
from backend.app.market.automation import (
    BaoStockProbeRunner,
    MarketAutomationService,
    RefreshAlreadyRunning,
    RefreshRunLock,
    canonical_refresh_callback,
    collect_required_symbols,
    get_market_clock,
    run_automation_loop,
)
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.calendar import get_trading_calendar
from backend.app.market.calendar_sync import (
    CalendarSyncService,
    CalendarSyncStore,
    read_calendar_conflict,
    run_calendar_sync_loop,
)
from backend.app.market.continuity import (
    ContinuityInventory,
    ContinuityRepairExecutor,
    RepairClaimCoordinator,
    RepairRetryPolicy,
)
from backend.app.market.provider_health import SQLiteProviderHealthStore
from backend.app.market.providers.baostock import BaoStockProviderAdapter
from backend.app.market.store import MarketStore, MarketStoreReadError
from backend.app.orchestration.adapters import build_after_close_pipeline
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import StoragePreflight, configured_market_dataset_root
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
    health_store = SQLiteProviderHealthStore(
        layout.provider_health_database,
        failure_threshold=settings.provider_circuit_failure_threshold,
        cooldown_seconds=settings.provider_circuit_cooldown_seconds,
        probe_lease_seconds=settings.provider_circuit_probe_lease_seconds,
    )
    health_store.initialize()
    control_store = MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
    )
    dataset_root = configured_market_dataset_root(settings)
    market_store = (
        NasMarketStore(
            control_store,
            dataset_root,
            layout.local_paths.staging,
        )
        if readiness.mode in {"nas", "local_dataset"} and dataset_root is not None
        else control_store
    )
    if isinstance(market_store, NasMarketStore):
        try:
            market_store.validate_readiness()
            market_store.reconcile_control_pointer()
        except DatasetError:
            yield
            return
    user_store = UserStore(layout.local_paths.user_database)
    trading_calendar = get_trading_calendar()
    provider = BaoStockProvider(
        min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
        factor_cache_path=str(layout.local_paths.factor_cache_database),
        socket_timeout_seconds=settings.baostock_socket_timeout_seconds,
    )
    canonical_refresh = None
    if hasattr(provider, "client") and hasattr(provider, "factor_cache"):
        canonical_adapter = BaoStockProviderAdapter(
            client=provider.client,
            factor_cache=provider.factor_cache,
            min_request_interval_seconds=settings.auto_refresh_min_request_interval_seconds,
            socket_timeout_seconds=settings.baostock_socket_timeout_seconds,
        )
        canonical_refresh = canonical_refresh_callback(
            control_store,
            canonical_adapter,
            evidence_root=layout.provider_evidence_root,
            lock_path=layout.market_refresh_lock,
            factor_cache=provider.factor_cache,
        )
    continuity = None
    repair_executor = None
    if (
        settings.market_repair_enabled
        and settings.market_continuity_start_date is not None
        and isinstance(market_store, NasMarketStore)
    ):
        queue_store = market_store.control
        calendar_sync_path = layout.local_paths.control / settings.calendar_sync_database_name
        continuity = ContinuityInventory(
            calendar=trading_calendar,
            inventory_reader=market_store,
            inventory_mode="immutable_dataset",
            calendar_conflict=lambda: read_calendar_conflict(calendar_sync_path),
        )
        coordinator = RepairClaimCoordinator(
            store=queue_store,
            health_store=health_store,
            lock_path=layout.market_refresh_lock,
            owner="auto-refresh-repair",
            lease_seconds=settings.market_repair_lease_seconds,
        )
        repair_executor = ContinuityRepairExecutor(
            coordinator=coordinator,
            queue_store=queue_store,
            canonical_store=market_store,
            provider=provider,
            inventory_reader=market_store,
            required_symbols=lambda: collect_required_symbols(user_store),
            clock=get_market_clock(),
            retry_policy=RepairRetryPolicy(
                max_attempts=settings.market_repair_max_attempts,
                base_seconds=settings.market_repair_retry_base_seconds,
            ),
            scanner=continuity,
            configured_start=settings.market_continuity_start_date,
            queue_schema_initializer=queue_store.initialize_continuity_schema,
            health_store=health_store,
            post_publish=build_after_close_pipeline(
                layout.local_paths.user_database,
                market_store,
                settings=settings,
            ),
        )
    service = MarketAutomationService(
        market_store,
        provider,
        trading_calendar,
        required_symbols=lambda: collect_required_symbols(user_store),
        lock_path=layout.market_refresh_lock,
        post_publish=build_after_close_pipeline(
            layout.local_paths.user_database,
            market_store,
            settings=settings,
        ),
        health_store=health_store,
        probe_runner=BaoStockProbeRunner(
            lambda *, max_attempts: BaoStockProvider(
                max_attempts=max_attempts,
                min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
                socket_timeout_seconds=settings.baostock_socket_timeout_seconds,
            )
        ),
        continuity=coordinator if repair_executor is not None else None,
        repair_enabled=(repair_executor is not None),
        repair_executor=repair_executor,
        canonical_refresh=canonical_refresh,
    )
    calendar_sync_path = layout.local_paths.control / settings.calendar_sync_database_name
    calendar_service = CalendarSyncService(
        # The loop may plan a repair before calendar control state exists.  Its reader must
        # never create that state; the explicit execution callback below owns initialization.
        CalendarSyncStore(calendar_sync_path, initialize=False),
        get_trading_calendar(),
        BaoStockProvider(
            min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
            socket_timeout_seconds=settings.baostock_socket_timeout_seconds,
        ),
        health_store=health_store,
    )

    def execute_calendar_plan(plan):
        try:
            with RefreshRunLock(layout.market_refresh_lock):
                writer_service = CalendarSyncService(
                    CalendarSyncStore(calendar_sync_path),
                    calendar_service.calendar,
                    calendar_service.provider,
                    health_store=health_store,
                )
                return writer_service.execute(plan)
        except RefreshAlreadyRunning:
            return None

    stop = asyncio.Event()
    market_task = asyncio.create_task(
        run_automation_loop(
            service,
            stop,
            clock=get_market_clock(),
        )
    )
    calendar_task = asyncio.create_task(
        run_calendar_sync_loop(
            calendar_service,
            stop,
            clock=get_market_clock(),
            execute_plan=execute_calendar_plan,
        )
    )
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(market_task, calendar_task)


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


@app.exception_handler(MarketStoreReadError)
async def market_store_read_error_handler(_, __: MarketStoreReadError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "detail": {
                "code": "market_storage_unavailable",
                "storage_status": "unavailable",
                "reason_code": "market_control_read_failed",
            }
        },
    )
