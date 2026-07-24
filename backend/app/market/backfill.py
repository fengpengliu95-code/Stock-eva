"""Controlled, resumable historical market-data backfill."""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from itertools import product
from time import sleep
from typing import Literal

import duckdb
from pydantic import BaseModel

from backend.app.market.models import RefreshResult
from backend.app.market.store import MarketStore


class BackfillBatchPlan(BaseModel):
    index: int
    start_date: date
    end_date: date
    trading_dates: list[date]
    symbols: list[str]
    requested_points: int
    estimated_provider_requests: int


class BackfillPlan(BaseModel):
    run_id: str
    request_key: str
    start_date: date
    end_date: date
    symbols: list[str]
    trading_dates: list[date]
    symbol_batch_size: int
    date_batch_size: int
    total_batches: int
    requested_points: int
    estimated_provider_requests: int
    batches: list[BackfillBatchPlan]


class BackfillRunRecord(BaseModel):
    run_id: str
    request_key: str
    status: Literal["planned", "running", "ready", "partial", "error"]
    start_date: date
    end_date: date
    symbols: list[str]
    trading_dates: list[date]
    total_batches: int
    completed_batches: int
    failed_batches: int
    requested_points: int
    loaded_points: int
    coverage_ratio: float
    started_at: datetime
    completed_at: datetime | None


def minimum_effective_days(rule: dict[str, object]) -> int:
    def operand_days(operand: dict[str, object]) -> int:
        if operand["type"] in {"constant", "field"}:
            return 1
        name = operand["name"]
        if name == "volume_ratio_5d":
            return 6
        period = int(operand["period"])
        return period + 1 if name == "RSI" else period

    operation = rule["op"]
    if operation in {"and", "or"}:
        return max(minimum_effective_days(item) for item in rule["args"])
    if operation == "not":
        return minimum_effective_days(rule["arg"])
    required = max(
        operand_days(rule["left"]),
        operand_days(rule["right"]),
    )
    return required + 1 if operation in {"crosses_above", "crosses_below"} else required


def resolve_effective_window(
    provider,
    *,
    end_date: date,
    effective_days: int,
) -> list[date]:
    if effective_days < 1:
        raise ValueError("effective days must be positive")
    lookback_start = end_date - timedelta(days=effective_days * 3)
    trading_dates = provider.trading_dates(lookback_start, end_date)
    if len(trading_dates) < effective_days:
        raise ValueError(
            f"calendar returned {len(trading_dates)} trading days; "
            f"{effective_days} required"
        )
    return trading_dates[-effective_days:]


def _chunks(values: list, size: int) -> list[list]:
    if size < 1:
        raise ValueError("batch sizes must be positive")
    return [values[index : index + size] for index in range(0, len(values), size)]


class BackfillAuditStore:
    def __init__(self, path, *, temp_directory=None) -> None:
        self.path = path
        self.temp_directory = temp_directory

    def _connect(self):
        connection = duckdb.connect(str(self.path))
        if self.temp_directory is not None:
            self.temp_directory.mkdir(parents=True, exist_ok=True)
            connection.execute(
                "SET temp_directory = ?",
                [str(self.temp_directory)],
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS backfill_runs (
                run_id VARCHAR PRIMARY KEY,
                request_key VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                symbols JSON NOT NULL,
                trading_dates JSON NOT NULL,
                total_batches INTEGER NOT NULL,
                requested_points INTEGER NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS backfill_batches (
                run_id VARCHAR NOT NULL,
                batch_index INTEGER NOT NULL,
                status VARCHAR NOT NULL,
                requested_points INTEGER NOT NULL,
                loaded_points INTEGER NOT NULL,
                error_code VARCHAR,
                completed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (run_id, batch_index)
            )
            """
        )
        return connection

    def initialize(self, plan: BackfillPlan) -> None:
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO backfill_runs VALUES (
                    ?, ?, 'planned', ?, ?, ?, ?, ?, ?, ?, NULL
                )
                """,
                [
                    plan.run_id,
                    plan.request_key,
                    plan.start_date,
                    plan.end_date,
                    json.dumps(plan.symbols, ensure_ascii=False),
                    json.dumps(
                        [item.isoformat() for item in plan.trading_dates],
                        ensure_ascii=False,
                    ),
                    plan.total_batches,
                    plan.requested_points,
                    datetime.now(UTC),
                ],
            )
        finally:
            connection.close()

    def mark_running(self, run_id: str) -> None:
        connection = self._connect()
        try:
            connection.execute(
                "UPDATE backfill_runs SET status = 'running', completed_at = NULL "
                "WHERE run_id = ?",
                [run_id],
            )
        finally:
            connection.close()

    def ready_batch_indexes(self, run_id: str) -> set[int]:
        connection = self._connect()
        try:
            return {
                row[0]
                for row in connection.execute(
                    """
                    SELECT batch_index FROM backfill_batches
                    WHERE run_id = ? AND status = 'ready'
                    """,
                    [run_id],
                ).fetchall()
            }
        finally:
            connection.close()

    def record_batch(
        self,
        run_id: str,
        batch_index: int,
        *,
        status: str,
        requested_points: int,
        loaded_points: int,
        error_code: str | None = None,
    ) -> None:
        connection = self._connect()
        try:
            connection.execute(
                "DELETE FROM backfill_batches WHERE run_id = ? AND batch_index = ?",
                [run_id, batch_index],
            )
            connection.execute(
                "INSERT INTO backfill_batches VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    run_id,
                    batch_index,
                    status,
                    requested_points,
                    loaded_points,
                    error_code,
                    datetime.now(UTC),
                ],
            )
        finally:
            connection.close()

    def finalize(
        self,
        plan: BackfillPlan,
        *,
        loaded_points: int,
    ) -> BackfillRunRecord:
        connection = self._connect()
        try:
            coverage = (
                loaded_points / plan.requested_points
                if plan.requested_points
                else 0.0
            )
            status = "ready" if coverage >= 1 else "partial" if loaded_points else "error"
            connection.execute(
                """
                UPDATE backfill_runs
                SET status = ?, completed_at = ?
                WHERE run_id = ?
                """,
                [status, datetime.now(UTC), plan.run_id],
            )
        finally:
            connection.close()
        return self.get_run(plan.run_id)

    def get_run(self, run_id: str) -> BackfillRunRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM backfill_runs WHERE run_id = ?",
                [run_id],
            ).fetchone()
            if row is None:
                raise ValueError("backfill run not found")
            batch_rows = connection.execute(
                """
                SELECT status, loaded_points FROM backfill_batches
                WHERE run_id = ?
                """,
                [run_id],
            ).fetchall()
        finally:
            connection.close()
        requested = row[8]
        loaded = sum(item[1] for item in batch_rows)
        completed = sum(item[0] == "ready" for item in batch_rows)
        coverage = loaded / requested if requested else 0.0
        return BackfillRunRecord(
            run_id=row[0],
            request_key=row[1],
            status=row[2],
            start_date=row[3],
            end_date=row[4],
            symbols=json.loads(row[5]),
            trading_dates=[
                date.fromisoformat(item) for item in json.loads(row[6])
            ],
            total_batches=row[7],
            completed_batches=completed,
            failed_batches=row[7] - completed,
            requested_points=requested,
            loaded_points=loaded,
            coverage_ratio=coverage,
            started_at=row[9],
            completed_at=row[10],
        )


class BackfillService:
    def __init__(
        self,
        store: MarketStore,
        provider,
        *,
        sleep_fn: Callable[[float], None] = sleep,
    ) -> None:
        self.store = store
        self.provider = provider
        self.sleep_fn = sleep_fn
        self.audit = BackfillAuditStore(
            store.path,
            temp_directory=store.temp_directory,
        )

    def plan(
        self,
        *,
        start_date: date,
        end_date: date,
        symbols: list[str],
        symbol_batch_size: int,
        date_batch_size: int,
        max_batches: int,
        trading_dates: list[date] | None = None,
    ) -> BackfillPlan:
        normalized_symbols = sorted(set(symbols))
        if not normalized_symbols:
            raise ValueError("backfill requires explicit symbols")
        if len(normalized_symbols) > 20:
            raise ValueError("backfill accepts at most 20 explicit symbols")
        trading_dates = trading_dates or self.provider.trading_dates(
            start_date,
            end_date,
        )
        if not trading_dates:
            raise ValueError("date range contains no trading days")
        date_batches = _chunks(trading_dates, date_batch_size)
        symbol_batches = _chunks(normalized_symbols, symbol_batch_size)
        combinations = list(product(date_batches, symbol_batches))
        if len(combinations) > max_batches:
            raise ValueError(
                f"planned {len(combinations)} batches exceeds batch cap {max_batches}"
            )
        batches: list[BackfillBatchPlan] = []
        for index, (dates, batch_symbols) in enumerate(combinations):
            batches.append(
                BackfillBatchPlan(
                    index=index,
                    start_date=dates[0],
                    end_date=dates[-1],
                    trading_dates=dates,
                    symbols=batch_symbols,
                    requested_points=len(dates) * len(batch_symbols),
                    estimated_provider_requests=1 + 2 * len(batch_symbols),
                )
            )
        request_key = (
            f"backfill:baostock:{trading_dates[0]}:{trading_dates[-1]}:"
            f"{','.join(normalized_symbols)}:{symbol_batch_size}:{date_batch_size}"
        )
        run_id = hashlib.sha256(request_key.encode()).hexdigest()[:24]
        return BackfillPlan(
            run_id=run_id,
            request_key=request_key,
            start_date=trading_dates[0],
            end_date=trading_dates[-1],
            symbols=normalized_symbols,
            trading_dates=trading_dates,
            symbol_batch_size=symbol_batch_size,
            date_batch_size=date_batch_size,
            total_batches=len(batches),
            requested_points=len(trading_dates) * len(normalized_symbols),
            estimated_provider_requests=sum(
                item.estimated_provider_requests for item in batches
            ),
            batches=batches,
        )

    def execute(
        self,
        plan: BackfillPlan,
        *,
        min_request_interval_seconds: float,
    ) -> BackfillRunRecord:
        if min_request_interval_seconds < 0:
            raise ValueError("request interval cannot be negative")
        self.audit.initialize(plan)
        self.audit.mark_running(plan.run_id)
        ready_indexes = self.audit.ready_batch_indexes(plan.run_id)
        attempted = 0
        for batch in plan.batches:
            if batch.index in ready_indexes:
                continue
            if attempted:
                self.sleep_fn(min_request_interval_seconds)
            attempted += 1
            try:
                result = self.provider.fetch_range(
                    batch.start_date,
                    batch.end_date,
                    symbols=batch.symbols,
                )
                self.store.upsert_bars(result.bars)
                loaded = len(
                    {
                        (item.trade_date, item.symbol)
                        for item in result.bars
                        if item.trade_date in batch.trading_dates
                        and item.symbol in batch.symbols
                    }
                )
                batch_status = (
                    "ready"
                    if loaded == batch.requested_points
                    and not result.failed_symbols
                    else "partial"
                )
                self.audit.record_batch(
                    plan.run_id,
                    batch.index,
                    status=batch_status,
                    requested_points=batch.requested_points,
                    loaded_points=loaded,
                    error_code=None if batch_status == "ready" else "incomplete_coverage",
                )
            except Exception:
                self.audit.record_batch(
                    plan.run_id,
                    batch.index,
                    status="error",
                    requested_points=batch.requested_points,
                    loaded_points=0,
                    error_code="provider_error",
                )
        loaded_points = len(
            self.store.present_points(plan.trading_dates, plan.symbols)
        )
        self._save_daily_audits(plan)
        return self.audit.finalize(plan, loaded_points=loaded_points)

    def _save_daily_audits(self, plan: BackfillPlan) -> None:
        present = self.store.present_points(plan.trading_dates, plan.symbols)
        now = datetime.now(UTC)
        for trade_date in plan.trading_dates:
            loaded_symbols = {
                symbol
                for stored_date, symbol in present
                if stored_date == trade_date
            }
            failures = sorted(set(plan.symbols) - loaded_symbols)
            loaded = len(loaded_symbols)
            status = "ready" if not failures else "partial" if loaded else "error"
            request_key = f"{plan.request_key}:date:{trade_date}"
            self.store.save_refresh(
                [],
                RefreshResult(
                    run_id=hashlib.sha256(request_key.encode()).hexdigest()[:24],
                    request_key=request_key,
                    run_kind="backfill",
                    requested_date=trade_date,
                    source="baostock",
                    status=status,
                    requested_count=len(plan.symbols),
                    succeeded_count=loaded,
                    coverage_ratio=loaded / len(plan.symbols),
                    failed_symbols=failures,
                    quality_issues=[] if not failures else ["incomplete_symbol_coverage"],
                    started_at=now,
                    completed_at=datetime.now(UTC),
                ),
                publish=False,
            )
