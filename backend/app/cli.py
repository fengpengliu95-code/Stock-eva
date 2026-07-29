import argparse
import json
import signal
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from time import perf_counter

from backend.app.classification.provider import (
    BaoStockClassificationProvider,
    ClassificationProviderError,
)
from backend.app.classification.service import ClassificationConflictError
from backend.app.classification.store import ClassificationStore
from backend.app.classification.sync import run_classification_sync
from backend.app.config import get_settings
from backend.app.market.automation import (
    MarketAutomationService,
    RefreshAlreadyRunning,
    RefreshRunLock,
    collect_required_symbols,
    get_market_clock,
    run_publication_refresh,
)
from backend.app.market.backfill import (
    BackfillService,
    minimum_effective_days,
    resolve_effective_window,
)
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.calendar import get_trading_calendar
from backend.app.market.calendar_sync import CalendarSyncService, CalendarSyncStore
from backend.app.market.factor_cache import AdjustmentFactorCache
from backend.app.market.full_history import FullMarketHistoryService
from backend.app.market.refresh import MarketRefreshService
from backend.app.market.store import MarketStore
from backend.app.orchestration.adapters import build_after_close_pipeline
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.initialize import (
    DatasetInitializationError,
    EmptyDatasetInitializer,
)
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import StoragePreflight, configured_market_dataset_root
from backend.app.strategy.store import StrategyStore
from backend.app.user.backup import PrivateBackupError, PrivateBackupService
from backend.app.user.store import UserStore


def _probe_timeout_value(value: str) -> int:
    seconds = int(value)
    if not 1 <= seconds <= 60:
        raise argparse.ArgumentTypeError("probe timeout must be between 1 and 60 seconds")
    return seconds


def _socket_timeout_value(value: str) -> float:
    seconds = float(value)
    if not 1 <= seconds <= 120:
        raise argparse.ArgumentTypeError("socket timeout must be between 1 and 120 seconds")
    return seconds


class _ProbeTimeoutInterrupt(BaseException):
    pass


@contextmanager
def _probe_timeout(seconds: int):
    def raise_timeout(_signum, _frame):
        raise _ProbeTimeoutInterrupt

    previous = signal.signal(signal.SIGALRM, raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        try:
            yield
        except _ProbeTimeoutInterrupt as exc:
            raise TimeoutError("metadata probe timed out") from exc
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stock EVA after-close data tasks")
    subparsers = parser.add_subparsers(dest="command", required=True)
    refresh = subparsers.add_parser("refresh", help="refresh one completed trading date")
    refresh.add_argument("--date", required=True, type=date.fromisoformat, dest="trade_date")
    target = refresh.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--all-main-board",
        action="store_true",
        help="load Shanghai/Shenzhen main-board stocks and the two summary indexes",
    )
    target.add_argument(
        "--symbols",
        nargs="+",
        help="controlled symbol slice, for example sh.600000 sh.000001",
    )
    refresh.add_argument(
        "--dry-run",
        action="store_true",
        help="print the request plan without calling BaoStock",
    )
    refresh.add_argument(
        "--execute-all-main-board",
        action="store_true",
        help="required confirmation for a real all-main-board refresh",
    )
    refresh.add_argument(
        "--inspect-universe",
        action="store_true",
        help="dry-run metadata probe: calendar and main-board universe only",
    )
    refresh.add_argument(
        "--probe-timeout-seconds",
        type=_probe_timeout_value,
        default=30,
        help="hard timeout for the optional metadata probe (1-60 seconds)",
    )
    refresh.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    backfill = subparsers.add_parser(
        "backfill",
        help="plan or execute a resumable explicit-symbol historical backfill",
    )
    backfill.add_argument("--symbols", nargs="+", required=True)
    backfill.add_argument("--start", type=date.fromisoformat)
    backfill.add_argument("--end", required=True, type=date.fromisoformat)
    backfill.add_argument("--effective-days", type=int)
    backfill.add_argument("--strategy-id")
    backfill.add_argument("--strategy-version", type=int)
    backfill.add_argument("--symbol-batch-size", type=int, default=5)
    backfill.add_argument("--date-batch-size", type=int, default=60)
    backfill.add_argument("--max-batches", type=int, default=20)
    backfill.add_argument("--min-request-interval", type=float, default=0.5)
    backfill.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    backfill.add_argument(
        "--execute",
        action="store_true",
        help="perform network requests; omitted means dry-run",
    )
    full_history = subparsers.add_parser(
        "full-market-backfill",
        help="plan or execute resumable full-main-board history into NAS",
    )
    full_history.add_argument("--start", required=True, type=date.fromisoformat)
    full_history.add_argument("--end", required=True, type=date.fromisoformat)
    full_history.add_argument("--max-sessions", type=int, default=3000)
    full_history.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    full_history.add_argument(
        "--execute-all-main-board-history",
        action="store_true",
        help="required confirmation for real full-market historical publication",
    )
    subparsers.add_parser(
        "refresh-runs",
        help="list auditable daily refresh run records",
    )
    automation_once = subparsers.add_parser(
        "auto-refresh-once",
        help="evaluate the backend schedule once without a client-provided date",
    )
    automation_once.add_argument(
        "--execute",
        action="store_true",
        help="perform a due refresh; omitted means a network-free plan",
    )
    automation_once.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    calendar_sync = subparsers.add_parser(
        "calendar-sync",
        help="plan or execute versioned BaoStock checks of the official calendar",
    )
    calendar_sync.add_argument(
        "--mode",
        choices=("auto", "full", "light"),
        default="auto",
        help="auto follows monthly/startup/daily policy",
    )
    calendar_sync.add_argument("--start", type=date.fromisoformat)
    calendar_sync.add_argument("--end", type=date.fromisoformat)
    calendar_sync.add_argument(
        "--startup",
        action="store_true",
        help="evaluate the startup light-check slot when mode is auto",
    )
    calendar_sync.add_argument(
        "--execute",
        action="store_true",
        help="perform the BaoStock observation and persist audit state",
    )
    calendar_sync.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    storage_init = subparsers.add_parser(
        "storage-init",
        help="initialize a new empty NAS market dataset",
    )
    storage_init.add_argument(
        "--initialize-empty-nas-dataset",
        action="store_true",
        help="required acknowledgement; refuses an existing path or share root",
    )
    export = subparsers.add_parser("export", help="export one stored date to Parquet")
    export.add_argument("--date", required=True, type=date.fromisoformat, dest="trade_date")
    classification_sync = subparsers.add_parser(
        "classification-sync",
        help="plan or execute a point-in-time security/index/sector metadata sync",
    )
    classification_sync.add_argument(
        "--as-of",
        required=True,
        type=date.fromisoformat,
        dest="as_of",
    )
    classification_sync.add_argument(
        "--execute",
        action="store_true",
        help="perform bounded BaoStock metadata reads and publish; omitted means dry-run",
    )
    classification_sync.add_argument(
        "--socket-timeout-seconds",
        type=_socket_timeout_value,
        help="BaoStock socket timeout; defaults to STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS",
    )
    private_backup = subparsers.add_parser(
        "backup-private-data",
        help="create a consistent local SQLite snapshot with bounded retention",
    )
    private_backup.add_argument(
        "--backup-root",
        type=Path,
        default=(Path.home() / "Library/Application Support/Stock EVA/backups"),
    )
    private_backup.add_argument("--keep-daily", type=int, default=7)
    private_backup.add_argument("--keep-weekly", type=int, default=4)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    if args.command == "classification-sync":
        store = ClassificationStore(
            settings.market_data_dir / settings.classification_database_name,
            temp_directory=settings.local_temp_dir / "classification-duckdb",
        )
        provider = (
            BaoStockClassificationProvider(
                min_request_interval_seconds=(
                    settings.auto_refresh_min_request_interval_seconds
                ),
                socket_timeout_seconds=(
                    args.socket_timeout_seconds
                    or settings.baostock_socket_timeout_seconds
                ),
            )
            if args.execute
            else None
        )
        try:
            result = run_classification_sync(
                as_of=args.as_of,
                execute=args.execute,
                store=store,
                provider=provider,
            )
        except (ClassificationProviderError, ClassificationConflictError, ValueError):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "as_of": args.as_of.isoformat(),
                        "quality_issues": ["classification_sync_failed"],
                        "writes_classification_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
        return 0 if result.status in {"dry-run", "ready"} else 1
    if args.command == "backup-private-data":
        try:
            outcome = PrivateBackupService(
                settings.user_data_dir / settings.user_database_name,
                args.backup_root,
                keep_daily=args.keep_daily,
                keep_weekly=args.keep_weekly,
            ).run(datetime.now(UTC))
        except (PrivateBackupError, ValueError):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "error_code": "private_backup_failed",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "status": outcome.status,
                    "integrity_check": outcome.integrity_check,
                    "completed_at": outcome.completed_at.isoformat(),
                    "daily_snapshot": outcome.daily_backup.name,
                    "weekly_snapshot": outcome.weekly_backup.name,
                    "writes_market_data": False,
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "storage-init":
        if not args.initialize_empty_nas_dataset:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "reason_code": "explicit_initialization_required",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        try:
            root = EmptyDatasetInitializer(settings).initialize()
        except DatasetInitializationError as error:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "reason_code": str(error),
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        readiness = StoragePreflight(settings).inspect()
        print(
            json.dumps(
                {
                    "status": "ready",
                    "dataset_root": str(root),
                    "storage": readiness.model_dump(mode="json"),
                    "writes_market_data": True,
                },
                ensure_ascii=False,
            )
        )
        return 0
    readiness = StoragePreflight(settings).inspect()
    if (
        settings.nas_market_dataset_root is not None
        and not readiness.market_data_available
        and args.command
        in {
            "refresh",
            "backfill",
            "full-market-backfill",
            "auto-refresh-once",
            "export",
        }
    ):
        print(
            json.dumps(
                {
                    "status": "error",
                    "quality_issues": ["market_storage_unavailable"],
                    "storage_status": readiness.status,
                    "reason_code": readiness.reason_code,
                    "writes_market_data": False,
                },
                ensure_ascii=False,
            )
        )
        return 1
    layout = StorageLayout(settings)
    layout.ensure_local_runtime_dirs()
    socket_timeout_seconds = (
        getattr(args, "socket_timeout_seconds", None) or settings.baostock_socket_timeout_seconds
    )
    if args.command == "calendar-sync":
        if (args.start is None) != (args.end is None):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "message": "--start and --end must be used together",
                        "writes_calendar_state": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 2
        calendar_store = CalendarSyncStore(
            layout.local_paths.control / settings.calendar_sync_database_name,
            initialize=args.execute,
        )
        now = get_market_clock()()
        planning_service = CalendarSyncService(
            calendar_store,
            get_trading_calendar(),
            None,
        )
        plan = planning_service.plan(
            now=now,
            mode=args.mode,
            startup=args.startup,
            start_date=args.start,
            end_date=args.end,
        )
        if not args.execute:
            decision = planning_service.policy.decide(
                now,
                state=calendar_store.state(),
                startup=args.startup,
            )
            print(
                json.dumps(
                    {
                        "status": "dry-run",
                        "action": plan.mode if plan is not None else "none",
                        "plan": (plan.model_dump(mode="json") if plan is not None else None),
                        "next_sync_at": (
                            decision.next_sync_at.isoformat()
                            if decision.next_sync_at is not None
                            else None
                        ),
                        "network_requests": 0,
                        "writes_calendar_state": False,
                        "execute_requires": "--execute",
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        if plan is None:
            print(
                json.dumps(
                    {
                        "status": "not-due",
                        "writes_calendar_state": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        provider = BaoStockProvider(
            min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
            socket_timeout_seconds=socket_timeout_seconds,
        )
        try:
            with RefreshRunLock(layout.market_refresh_lock):
                result = CalendarSyncService(
                    calendar_store,
                    get_trading_calendar(),
                    provider,
                ).execute(plan)
        except RefreshAlreadyRunning:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["refresh_already_running"],
                        "writes_calendar_state": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["calendar_sync_failed"],
                        "writes_calendar_state": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    **result.model_dump(mode="json"),
                    "writes_calendar_state": True,
                },
                ensure_ascii=False,
            )
        )
        return 1 if result.status == "quarantined" else 0
    control_store = MarketStore(
        layout.local_paths.market_database,
        temp_directory=layout.duckdb_temporary,
    )
    dataset_root = configured_market_dataset_root(settings)
    store = (
        NasMarketStore(
            control_store,
            dataset_root,
            layout.local_paths.staging,
        )
        if readiness.mode in {"nas", "local_dataset"} and dataset_root is not None
        else control_store
    )
    if isinstance(store, NasMarketStore):
        try:
            store.validate_readiness()
            store.reconcile_control_pointer()
        except DatasetError:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["market_storage_unavailable"],
                        "reason_code": "nas_dataset_read_failed",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
    if args.command == "full-market-backfill":
        if not isinstance(store, NasMarketStore):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["nas_market_dataset_required"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        provider = BaoStockProvider(
            min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
            factor_cache_path=str(layout.local_paths.factor_cache_database),
            socket_timeout_seconds=socket_timeout_seconds,
        )
        service = FullMarketHistoryService(store, provider)
        if not args.execute_all_main_board_history:
            try:
                plan = service.plan(
                    start_date=args.start,
                    end_date=args.end,
                    max_sessions=args.max_sessions,
                )
            except Exception:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "quality_issues": ["calendar_or_plan_error"],
                            "writes_market_data": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            payload = plan.model_dump(mode="json")
            payload.update(
                {
                    "status": "dry-run",
                    "scope": "all-main-board-history",
                    "writes_market_data": False,
                    "network_note": ("one BaoStock calendar query was used to build this plan"),
                    "execute_requires": "--execute-all-main-board-history",
                }
            )
            print(json.dumps(payload, ensure_ascii=False))
            return 0
        try:
            with RefreshRunLock(layout.market_refresh_lock):
                plan = service.plan(
                    start_date=args.start,
                    end_date=args.end,
                    max_sessions=args.max_sessions,
                )
                result = service.execute(plan)
        except RefreshAlreadyRunning:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["refresh_already_running"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except DatasetError:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["nas_dataset_operation_failed"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["history_execution_failed"],
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
        return 0 if result.status == "ready" else 1
    if args.command == "export":
        try:
            output = store.export_date(args.trade_date, layout.local_paths.staging / "exports")
        except DatasetError:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["market_storage_unavailable"],
                        "reason_code": "nas_dataset_read_failed",
                        "writes_market_data": False,
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        print(json.dumps({"status": "ready", "path": str(output)}, ensure_ascii=False))
        return 0
    if args.command == "refresh-runs":
        print(
            json.dumps(
                [item.model_dump(mode="json") for item in store.list_refreshes()],
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "auto-refresh-once":
        calendar = get_trading_calendar()
        provider = BaoStockProvider(
            min_request_interval_seconds=(settings.auto_refresh_min_request_interval_seconds),
            factor_cache_path=str(layout.local_paths.factor_cache_database),
            socket_timeout_seconds=socket_timeout_seconds,
        )
        user_store = UserStore(layout.local_paths.user_database)
        service = MarketAutomationService(
            store,
            provider,
            calendar,
            required_symbols=lambda: collect_required_symbols(user_store),
            lock_path=layout.market_refresh_lock,
            post_publish=build_after_close_pipeline(
                layout.local_paths.user_database,
                store,
            ),
        )
        now = get_market_clock()()
        if not args.execute:
            published = store.published_refresh()
            decision = service.policy.decide(
                now,
                published_as_of=(published.requested_date if published is not None else None),
                state=store.scheduler_state(),
            )
            print(
                json.dumps(
                    {
                        "status": "dry-run",
                        "action": decision.action,
                        "target_session": (
                            decision.target_session.isoformat()
                            if decision.target_session is not None
                            else None
                        ),
                        "refresh_state": decision.refresh_state,
                        "next_run_at": (
                            decision.next_run_at.isoformat()
                            if decision.next_run_at is not None
                            else None
                        ),
                        "writes_market_data": False,
                        "execute_requires": "--execute",
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        outcome = service.run_due_once(now)
        print(json.dumps(outcome.model_dump(mode="json"), ensure_ascii=False))
        return 1 if outcome.state.refresh_state in {"retry_wait", "delayed", "error"} else 0
    if args.command == "backfill":
        if (args.start is None) == (args.effective_days is None):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "message": "provide exactly one of --start or --effective-days",
                    },
                    ensure_ascii=False,
                )
            )
            return 2
        if (args.strategy_id is None) != (args.strategy_version is None):
            print(
                json.dumps(
                    {
                        "status": "error",
                        "message": "--strategy-id and --strategy-version must be used together",
                    },
                    ensure_ascii=False,
                )
            )
            return 2
        provider = BaoStockProvider(
            min_request_interval_seconds=max(args.min_request_interval, 0.2),
            socket_timeout_seconds=socket_timeout_seconds,
        )
        selected_dates = None
        start_date = args.start
        if args.effective_days is not None:
            try:
                selected_dates = resolve_effective_window(
                    provider,
                    end_date=args.end,
                    effective_days=args.effective_days,
                )
            except Exception:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "quality_issues": ["calendar_or_plan_error"],
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            start_date = selected_dates[0]
        service = BackfillService(store, provider)
        try:
            plan = service.plan(
                start_date=start_date,
                end_date=args.end,
                symbols=args.symbols,
                symbol_batch_size=args.symbol_batch_size,
                date_batch_size=args.date_batch_size,
                max_batches=args.max_batches,
                trading_dates=selected_dates,
            )
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["calendar_or_plan_error"],
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        required_history = None
        if args.strategy_id is not None:
            strategy_store = StrategyStore(layout.local_paths.user_database)
            version = strategy_store.get_version(
                args.strategy_id,
                args.strategy_version,
            )
            required_history = minimum_effective_days(version.rule_ast)
            if len(plan.trading_dates) < required_history:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "message": (
                                f"strategy requires {required_history} effective days; "
                                f"plan has {len(plan.trading_dates)}"
                            ),
                        },
                        ensure_ascii=False,
                    )
                )
                return 2
        if not args.execute:
            payload = plan.model_dump(mode="json")
            payload.update(
                {
                    "status": "dry-run",
                    "network_note": "one BaoStock calendar query was used to build this plan",
                    "strategy_minimum_effective_days": required_history,
                }
            )
            print(json.dumps(payload, ensure_ascii=False))
            return 0
        result = service.execute(
            plan,
            min_request_interval_seconds=args.min_request_interval,
        )
        print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
        return 0 if result.status in {"ready", "partial"} else 1
    if args.all_main_board and (args.dry_run or not args.execute_all_main_board):
        inspection = None
        elapsed = None
        if args.inspect_universe:
            calendar = get_trading_calendar()
            if calendar.session_status(args.trade_date) != "open":
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "quality_issues": ["official_calendar_not_confirmed_open"],
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            provider = BaoStockProvider(
                min_request_interval_seconds=0.5,
                socket_timeout_seconds=socket_timeout_seconds,
            )
            started = perf_counter()
            try:
                with _probe_timeout(args.probe_timeout_seconds):
                    inspection = provider.inspect_main_board(args.trade_date)
            except Exception:
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "quality_issues": ["universe_inspection_failed"],
                            "probe_timeout_seconds": args.probe_timeout_seconds,
                            "writes_market_data": False,
                        },
                        ensure_ascii=False,
                    )
                )
                return 1
            elapsed = round(perf_counter() - started, 3)
        base_estimated_requests = (
            inspection.metadata_provider_requests + 5 if inspection is not None else 7
        )
        factor_cache_path = layout.local_paths.factor_cache_database
        cached_factor_count = (
            (
                len(
                    AdjustmentFactorCache(factor_cache_path).exact_snapshots(
                        inspection.main_board_symbols,
                        args.trade_date,
                    )
                )
                if factor_cache_path.exists()
                else 0
            )
            if inspection is not None
            else None
        )
        factor_bootstrap_remaining = (
            inspection.main_board_count - cached_factor_count
            if inspection is not None and cached_factor_count is not None
            else None
        )
        estimated_requests = (
            base_estimated_requests + factor_bootstrap_remaining
            if factor_bootstrap_remaining is not None
            else base_estimated_requests
        )
        estimated_seconds_lower_bound = (
            round(
                elapsed / inspection.metadata_provider_requests * base_estimated_requests
                + (factor_bootstrap_remaining or 0) * 0.5,
                1,
            )
            if inspection is not None
            and elapsed is not None
            and inspection.metadata_provider_requests
            else None
        )
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "scope": "all-main-board",
                    "trade_date": args.trade_date.isoformat(),
                    "batch_cap": 1,
                    "request_batches": 1,
                    "requested_count": (
                        inspection.total_expected_count if inspection is not None else None
                    ),
                    "main_board_count": (
                        inspection.main_board_count if inspection is not None else None
                    ),
                    "shanghai_count": (
                        inspection.shanghai_count if inspection is not None else None
                    ),
                    "shenzhen_count": (
                        inspection.shenzhen_count if inspection is not None else None
                    ),
                    "metadata_provider_requests": (
                        inspection.metadata_provider_requests if inspection is not None else 0
                    ),
                    "metadata_probe_elapsed_seconds": elapsed,
                    "cached_factor_count": cached_factor_count,
                    "factor_bootstrap_remaining": factor_bootstrap_remaining,
                    "estimated_provider_requests": estimated_requests,
                    "provider_requests_with_retries_upper_bound": (estimated_requests * 2),
                    "min_request_interval_seconds": 0.5,
                    "minimum_throttle_elapsed_seconds": round(
                        (estimated_requests - 1) * 0.5,
                        1,
                    ),
                    "estimated_execution_seconds_lower_bound": (estimated_seconds_lower_bound),
                    "recommended_resource_window_seconds": (
                        3600 if factor_bootstrap_remaining else 900
                    ),
                    "writes_market_data": False,
                    "execute_requires": "--execute-all-main-board",
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.dry_run:
        symbols = sorted(set(args.symbols))
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "scope": "explicit-symbols",
                    "trade_date": args.trade_date.isoformat(),
                    "symbols": symbols,
                    "requested_count": len(symbols),
                    "batch_cap": 1,
                    "estimated_provider_requests_upper_bound": 1 + 2 * len(symbols),
                },
                ensure_ascii=False,
            )
        )
        return 0
    if args.all_main_board:
        calendar = get_trading_calendar()
        if calendar.session_status(args.trade_date) != "open":
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["official_calendar_not_confirmed_open"],
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        try:
            with RefreshRunLock(layout.market_refresh_lock):
                provider = BaoStockProvider(
                    min_request_interval_seconds=(
                        settings.auto_refresh_min_request_interval_seconds
                    ),
                    factor_cache_path=str(layout.local_paths.factor_cache_database),
                    socket_timeout_seconds=socket_timeout_seconds,
                )
                if provider.trading_dates(args.trade_date, args.trade_date) != [args.trade_date]:
                    raise ValueError("calendar conflict")
                user_store = UserStore(layout.local_paths.user_database)
                result = run_publication_refresh(
                    store,
                    provider,
                    trade_date=args.trade_date,
                    required_symbols=collect_required_symbols(user_store),
                )
        except RefreshAlreadyRunning:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["refresh_already_running"],
                    },
                    ensure_ascii=False,
                )
            )
            return 1
        except Exception:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "quality_issues": ["calendar_provider_conflict"],
                    },
                    ensure_ascii=False,
                )
            )
            return 1
    else:
        result = MarketRefreshService(
            store,
            BaoStockProvider(socket_timeout_seconds=socket_timeout_seconds),
        ).refresh(args.trade_date, symbols=args.symbols)
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
    return 0 if result.status in {"ready", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
