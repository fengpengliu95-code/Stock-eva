"""Read-only NAS / temporary-user-data end-to-end acceptance workflow."""

import argparse
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from pydantic import BaseModel

from backend.app.alert.service import AlertService
from backend.app.alert.store import AlertStore
from backend.app.config import Settings
from backend.app.market.automation import RefreshAlreadyRunning, RefreshRunLock
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.preflight import StoragePreflight
from backend.app.strategy.dsl import validate_rule
from backend.app.strategy.run import StrategyRunService
from backend.app.strategy.store import StrategyStore
from backend.app.user.models import PositionCreate
from backend.app.user.portfolio import PortfolioValuationService
from backend.app.user.store import UserStore

DISCLAIMER = "规则候选，仅用于研究，不构成投资建议。"
MINIMUM_EFFECTIVE_DAYS = 21


class AcceptanceError(RuntimeError):
    pass


class AcceptanceReport(BaseModel):
    status: Literal["ready"]
    as_of_date: date
    strategy_status: Literal["completed", "partial"]
    strategy_run_reused: bool
    scanned_symbols: int
    candidate_count: int
    candidate_symbols: list[str]
    alert_evaluation_reused: bool
    alert_state: str
    portfolio_coverage: str
    user_database_persisted: Literal[False] = False
    disclaimer: str = DISCLAIMER


def acceptance_rule_ast() -> dict[str, object]:
    return {
        "op": "and",
        "args": [
            {
                "op": "compare",
                "left": {"type": "indicator", "name": "volume_ratio_5d"},
                "operator": ">",
                "right": {"type": "constant", "value": 1},
            },
            {
                "op": "crosses_above",
                "left": {
                    "type": "indicator",
                    "name": "SMA",
                    "field": "close",
                    "period": 5,
                },
                "right": {
                    "type": "indicator",
                    "name": "SMA",
                    "field": "close",
                    "period": 20,
                },
            },
        ],
    }


def run_real_e2e_acceptance(
    market_store,
    *,
    temporary_parent: Path | None = None,
) -> AcceptanceReport:
    published = market_store.published_refresh()
    if published is None or published.status != "ready":
        raise AcceptanceError("ready_published_snapshot_required")
    as_of_date = published.requested_date
    effective_dates = [
        item
        for item in market_store.available_dates("baostock")
        if item <= as_of_date
    ]
    if len(effective_dates) < MINIMUM_EFFECTIVE_DAYS:
        raise AcceptanceError("at_least_21_effective_days_required")
    latest_bars = [
        bar
        for bar in market_store.canonical_bars(as_of_date, "baostock")
        if bar.security_type == "stock" and bar.board == "main"
    ]
    if not latest_bars:
        raise AcceptanceError("published_snapshot_has_no_main_board_stocks")
    latest_by_symbol = {bar.symbol: bar for bar in latest_bars}

    with TemporaryDirectory(
        prefix="stock-eva-real-e2e-",
        dir=temporary_parent,
    ) as raw_workspace:
        user_database = Path(raw_workspace) / "acceptance-user.sqlite3"
        user_store = UserStore(user_database)
        strategy_store = StrategyStore(user_database)
        alert_store = AlertStore(user_database)

        strategy, version = strategy_store.create_strategy(
            "E2E 量比与均线交叉候选",
            validate_rule(acceptance_rule_ast()),
        )
        scan_key = (
            f"acceptance:{strategy.id}:v{version.version}:"
            f"{as_of_date.isoformat()}:all-main-board"
        )
        scan_service = StrategyRunService(strategy_store, market_store)
        first_scan = scan_service.run_all_main_board(
            strategy.id,
            version=version.version,
            idempotency_key=scan_key,
        )
        second_scan = scan_service.run_all_main_board(
            strategy.id,
            version=version.version,
            idempotency_key=scan_key,
        )
        if first_scan.id != second_scan.id:
            raise AcceptanceError("strategy_scan_idempotency_failed")
        if first_scan.status == "error" or first_scan.failed_batches:
            raise AcceptanceError("strategy_scan_batch_failed")

        candidates = sorted(
            result.symbol
            for result in first_scan.results
            if result.status == "matched"
        )
        sample_symbol = candidates[0] if candidates else sorted(latest_by_symbol)[0]
        sample_bar = latest_by_symbol[sample_symbol]
        user_store.create_position(
            PositionCreate(
                symbol=sample_symbol,
                quantity=Decimal("100"),
                avg_cost=Decimal(str(sample_bar.close)),
                as_of_date=as_of_date,
            )
        )
        watchlist = user_store.create_watchlist("E2E 临时观察")
        user_store.add_watchlist_item(watchlist.id, sample_symbol)

        alert_service = AlertService(
            alert_store,
            user_store,
            strategy_store,
            market_store,
        )
        alert_rule = alert_service.create_rule(
            name="E2E 收盘候选预警",
            watchlist_id=watchlist.id,
            strategy_id=strategy.id,
            strategy_version=version.version,
        )
        first_alert = alert_service.evaluate(alert_rule.id, signal_date=as_of_date)
        second_alert = alert_service.evaluate(alert_rule.id, signal_date=as_of_date)
        first_event_ids = [event.id for event in first_alert.events]
        second_event_ids = [event.id for event in second_alert.events]
        if (
            not first_event_ids
            or first_event_ids != second_event_ids
            or [
                event.idempotency_key for event in first_alert.events
            ]
            != [
                event.idempotency_key for event in second_alert.events
            ]
        ):
            raise AcceptanceError("alert_evaluation_idempotency_failed")

        valuation = PortfolioValuationService(user_store, market_store).latest(
            expected_session=as_of_date
        )
        if valuation.coverage.covered != 1 or valuation.coverage.total != 1:
            raise AcceptanceError("temporary_portfolio_valuation_failed")

        return AcceptanceReport(
            status="ready",
            as_of_date=as_of_date,
            strategy_status=first_scan.status,
            strategy_run_reused=True,
            scanned_symbols=first_scan.total_symbols,
            candidate_count=len(candidates),
            candidate_symbols=candidates[:50],
            alert_evaluation_reused=True,
            alert_state=first_alert.events[0].state,
            portfolio_coverage="1/1",
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run read-only NAS market / temporary-user-data E2E acceptance"
    )
    parser.add_argument("--nas-root", required=True, type=Path)
    parser.add_argument(
        "--refresh-lock",
        type=Path,
        default=Path("var/locks/market-refresh.lock"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        with RefreshRunLock(args.refresh_lock):
            with TemporaryDirectory(prefix="stock-eva-real-e2e-control-") as raw:
                workspace = Path(raw)
                settings = Settings(
                    _env_file=None,
                    market_data_dir=workspace / "market",
                    user_data_dir=workspace / "user",
                    local_control_dir=workspace / "control",
                    local_staging_dir=workspace / "staging",
                    local_lock_dir=workspace / "locks",
                    local_temp_dir=workspace / "tmp",
                    nas_market_dataset_root=args.nas_root,
                )
                readiness = StoragePreflight(settings).inspect()
                if not readiness.market_data_available:
                    raise AcceptanceError("nas_market_storage_not_ready")
                control = MarketStore(
                    workspace / "control" / "market.duckdb",
                    temp_directory=workspace / "tmp" / "duckdb",
                )
                market = NasMarketStore(
                    control,
                    args.nas_root,
                    workspace / "staging",
                )
                market.validate_readiness()
                market.reconcile_control_pointer()
                report = run_real_e2e_acceptance(
                    market,
                    temporary_parent=workspace,
                )
    except RefreshAlreadyRunning:
        payload = {"status": "blocked", "reason_code": "market_refresh_running"}
        print(json.dumps(payload, ensure_ascii=False))
        return 2
    except (AcceptanceError, DatasetError) as error:
        payload = {"status": "not_ready", "reason_code": str(error)}
        print(json.dumps(payload, ensure_ascii=False))
        return 1
    except Exception:
        payload = {"status": "error", "reason_code": "acceptance_execution_failed"}
        print(json.dumps(payload, ensure_ascii=False))
        return 1
    print(json.dumps(report.model_dump(mode="json"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
