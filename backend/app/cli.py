import argparse
import json
from datetime import date

from backend.app.config import get_settings
from backend.app.market.backfill import (
    BackfillService,
    minimum_effective_days,
    resolve_effective_window,
)
from backend.app.market.baostock import BaoStockProvider
from backend.app.market.refresh import MarketRefreshService
from backend.app.market.store import MarketStore
from backend.app.strategy.store import StrategyStore


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
        "--execute",
        action="store_true",
        help="perform network requests; omitted means dry-run",
    )
    subparsers.add_parser(
        "refresh-runs",
        help="list auditable daily refresh run records",
    )
    export = subparsers.add_parser("export", help="export one stored date to Parquet")
    export.add_argument("--date", required=True, type=date.fromisoformat, dest="trade_date")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    settings = get_settings()
    store = MarketStore(settings.market_data_dir / settings.market_database_name)
    if args.command == "export":
        output = store.export_date(args.trade_date, settings.market_data_dir / "exports")
        print(json.dumps({"status": "ready", "path": str(output)}, ensure_ascii=False))
        return 0
    if args.command == "refresh-runs":
        print(
            json.dumps(
                [
                    item.model_dump(mode="json")
                    for item in store.list_refreshes()
                ],
                ensure_ascii=False,
            )
        )
        return 0
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
            min_request_interval_seconds=max(args.min_request_interval, 0.2)
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
            strategy_store = StrategyStore(
                settings.user_data_dir / settings.user_database_name
            )
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
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "scope": "all-main-board",
                    "trade_date": args.trade_date.isoformat(),
                    "batch_cap": 1,
                    "estimated_provider_requests_upper_bound": 6,
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
    result = MarketRefreshService(store).refresh(
        args.trade_date,
        symbols=None if args.all_main_board else args.symbols,
    )
    print(json.dumps(result.model_dump(mode="json"), ensure_ascii=False))
    return 0 if result.status in {"ready", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
