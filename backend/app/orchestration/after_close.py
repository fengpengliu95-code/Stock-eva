"""Idempotent post-publication strategy and alert orchestration."""

import sqlite3
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, Field

from backend.app.market.models import RefreshResult
from backend.app.user.database import user_database_initialization


class StrategyWorkItem(BaseModel):
    strategy_id: str
    version: int = Field(ge=1)
    version_id: str
    enabled: bool = True


class AlertWorkItem(BaseModel):
    rule_id: str
    enabled: bool = True


class StrategyWorkRunner(Protocol):
    def run(
        self,
        item: StrategyWorkItem,
        *,
        trade_date: date,
        idempotency_key: str,
    ) -> str | None: ...


class AlertWorkRunner(Protocol):
    def run(
        self,
        item: AlertWorkItem,
        *,
        trade_date: date,
        idempotency_key: str,
    ) -> str | None: ...


class RegimeSnapshotWorkRunner(Protocol):
    def run(self, result: RefreshResult, *, idempotency_key: str) -> str | None: ...


TaskKind = Literal["regime", "strategy", "alert"]
TaskStatus = Literal["running", "completed", "error"]


class AfterCloseTaskRecord(BaseModel):
    trade_date: date
    kind: TaskKind
    work_key: str
    idempotency_key: str
    status: TaskStatus
    attempt_count: int = Field(ge=1)
    result_reference: str | None = None
    error_code: str | None = None
    started_at: datetime
    completed_at: datetime | None = None
    updated_at: datetime


class AfterClosePipelineOutcome(BaseModel):
    trade_date: date
    publication_run_id: str
    triggered: bool
    status: Literal["skipped", "empty", "completed", "partial"]
    reason: str | None = None
    completed_tasks: int = Field(default=0, ge=0)
    failed_tasks: int = Field(default=0, ge=0)


class AfterCloseTaskStore:
    """Audit store in the local private SQLite database."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        with user_database_initialization(self.path):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS after_close_pipeline_runs (
                    trade_date TEXT PRIMARY KEY,
                    publication_run_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    completed_tasks INTEGER NOT NULL,
                    failed_tasks INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS after_close_tasks (
                    trade_date TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    work_key TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL,
                    result_reference TEXT,
                    error_code TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (trade_date, kind, work_key)
                );
                """
            )
        return connection

    @staticmethod
    def _task(row: sqlite3.Row) -> AfterCloseTaskRecord:
        return AfterCloseTaskRecord(**dict(row))

    def start_pipeline(self, trade_date: date, publication_run_id: str) -> None:
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO after_close_pipeline_runs (
                    trade_date, publication_run_id, status,
                    completed_tasks, failed_tasks, started_at,
                    completed_at, updated_at
                ) VALUES (?, ?, 'running', 0, 0, ?, NULL, ?)
                ON CONFLICT(trade_date) DO UPDATE SET
                    publication_run_id = excluded.publication_run_id,
                    status = 'running',
                    completed_at = NULL,
                    updated_at = excluded.updated_at
                """,
                [trade_date.isoformat(), publication_run_id, now, now],
            )
            connection.commit()
        finally:
            connection.close()

    def finish_pipeline(
        self,
        trade_date: date,
        *,
        status: Literal["empty", "completed", "partial"],
        completed_tasks: int,
        failed_tasks: int,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE after_close_pipeline_runs
                SET status = ?, completed_tasks = ?, failed_tasks = ?,
                    completed_at = ?, updated_at = ?
                WHERE trade_date = ?
                """,
                [
                    status,
                    completed_tasks,
                    failed_tasks,
                    now,
                    now,
                    trade_date.isoformat(),
                ],
            )
            connection.commit()
        finally:
            connection.close()

    def claim_task(
        self,
        *,
        trade_date: date,
        kind: TaskKind,
        work_key: str,
        idempotency_key: str,
    ) -> AfterCloseTaskRecord:
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM after_close_tasks
                WHERE trade_date = ? AND kind = ? AND work_key = ?
                """,
                [trade_date.isoformat(), kind, work_key],
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO after_close_tasks (
                        trade_date, kind, work_key, idempotency_key, status,
                        attempt_count, result_reference, error_code,
                        started_at, completed_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'running', 1, NULL, NULL, ?, NULL, ?)
                    """,
                    [
                        trade_date.isoformat(),
                        kind,
                        work_key,
                        idempotency_key,
                        now,
                        now,
                    ],
                )
            elif row["status"] != "completed":
                connection.execute(
                    """
                    UPDATE after_close_tasks
                    SET status = 'running',
                        attempt_count = attempt_count + 1,
                        result_reference = NULL,
                        error_code = NULL,
                        started_at = ?,
                        completed_at = NULL,
                        updated_at = ?
                    WHERE trade_date = ? AND kind = ? AND work_key = ?
                    """,
                    [
                        now,
                        now,
                        trade_date.isoformat(),
                        kind,
                        work_key,
                    ],
                )
            connection.commit()
            claimed = connection.execute(
                """
                SELECT * FROM after_close_tasks
                WHERE trade_date = ? AND kind = ? AND work_key = ?
                """,
                [trade_date.isoformat(), kind, work_key],
            ).fetchone()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        if claimed is None:
            raise RuntimeError("after-close task claim was not persisted")
        return self._task(claimed)

    def finish_task(
        self,
        *,
        trade_date: date,
        kind: TaskKind,
        work_key: str,
        status: Literal["completed", "error"],
        result_reference: str | None = None,
        error_code: str | None = None,
    ) -> AfterCloseTaskRecord:
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute(
                """
                UPDATE after_close_tasks
                SET status = ?, result_reference = ?, error_code = ?,
                    completed_at = ?, updated_at = ?
                WHERE trade_date = ? AND kind = ? AND work_key = ?
                """,
                [
                    status,
                    result_reference,
                    error_code,
                    now,
                    now,
                    trade_date.isoformat(),
                    kind,
                    work_key,
                ],
            )
            connection.commit()
            row = connection.execute(
                """
                SELECT * FROM after_close_tasks
                WHERE trade_date = ? AND kind = ? AND work_key = ?
                """,
                [trade_date.isoformat(), kind, work_key],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise RuntimeError("after-close task was not found")
        return self._task(row)

    def list_tasks(self, trade_date: date) -> list[AfterCloseTaskRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM after_close_tasks
                WHERE trade_date = ?
                ORDER BY CASE kind WHEN 'regime' THEN 0 WHEN 'strategy' THEN 1 ELSE 2 END, work_key
                """,
                [trade_date.isoformat()],
            ).fetchall()
        finally:
            connection.close()
        return [self._task(row) for row in rows]


class AfterClosePipelineService:
    """Run private research workflows only for the exact ready publication."""

    def __init__(
        self,
        *,
        market_store,
        task_store: AfterCloseTaskStore,
        list_strategies: Callable[[], Iterable[StrategyWorkItem]],
        strategy_runner: StrategyWorkRunner,
        list_alert_rules: Callable[[], Iterable[AlertWorkItem]],
        alert_runner: AlertWorkRunner,
        regime_runner: RegimeSnapshotWorkRunner | None = None,
    ) -> None:
        self.market_store = market_store
        self.task_store = task_store
        self.list_strategies = list_strategies
        self.strategy_runner = strategy_runner
        self.list_alert_rules = list_alert_rules
        self.alert_runner = alert_runner
        self.regime_runner = regime_runner

    def run_after_publication(
        self,
        result: RefreshResult,
    ) -> AfterClosePipelineOutcome:
        if result.status != "ready":
            return self._skipped(result, "not_ready")
        published = self.market_store.published_refresh()
        if published is None or published.status != "ready":
            return self._skipped(result, "not_published")
        if published.requested_date != result.requested_date or published.run_id != result.run_id:
            return self._skipped(result, "not_current_publication")

        self.task_store.start_pipeline(result.requested_date, result.run_id)
        strategies = [item for item in self.list_strategies() if item.enabled]
        alerts = [item for item in self.list_alert_rules() if item.enabled]

        if self.regime_runner is not None:
            self._run_task(
                trade_date=result.requested_date,
                kind="regime",
                work_key="market-regime-v1",
                runner=lambda key: self.regime_runner.run(result, idempotency_key=key),
                error_code="regime_snapshot_capture_failed",
            )

        for item in strategies:
            self._run_task(
                trade_date=result.requested_date,
                kind="strategy",
                work_key=item.version_id,
                runner=lambda key, selected=item: self.strategy_runner.run(
                    selected,
                    trade_date=result.requested_date,
                    idempotency_key=key,
                ),
                error_code="strategy_execution_failed",
            )
        for item in alerts:
            self._run_task(
                trade_date=result.requested_date,
                kind="alert",
                work_key=item.rule_id,
                runner=lambda key, selected=item: self.alert_runner.run(
                    selected,
                    trade_date=result.requested_date,
                    idempotency_key=key,
                ),
                error_code="alert_evaluation_failed",
            )

        tasks = self.task_store.list_tasks(result.requested_date)
        completed = sum(task.status == "completed" for task in tasks)
        failed = sum(task.status == "error" for task in tasks)
        if not tasks:
            status = "empty"
        elif failed:
            status = "partial"
        else:
            status = "completed"
        self.task_store.finish_pipeline(
            result.requested_date,
            status=status,
            completed_tasks=completed,
            failed_tasks=failed,
        )
        return AfterClosePipelineOutcome(
            trade_date=result.requested_date,
            publication_run_id=result.run_id,
            triggered=True,
            status=status,
            completed_tasks=completed,
            failed_tasks=failed,
        )

    def _run_task(
        self,
        *,
        trade_date: date,
        kind: TaskKind,
        work_key: str,
        runner: Callable[[str], str | None],
        error_code: str,
    ) -> None:
        idempotency_key = f"after-close:{kind}:{work_key}:{trade_date.isoformat()}"
        task = self.task_store.claim_task(
            trade_date=trade_date,
            kind=kind,
            work_key=work_key,
            idempotency_key=idempotency_key,
        )
        if task.status == "completed":
            return
        try:
            result_reference = runner(idempotency_key)
        except Exception:
            self.task_store.finish_task(
                trade_date=trade_date,
                kind=kind,
                work_key=work_key,
                status="error",
                error_code=error_code,
            )
            return
        self.task_store.finish_task(
            trade_date=trade_date,
            kind=kind,
            work_key=work_key,
            status="completed",
            result_reference=result_reference,
        )

    @staticmethod
    def _skipped(
        result: RefreshResult,
        reason: str,
    ) -> AfterClosePipelineOutcome:
        return AfterClosePipelineOutcome(
            trade_date=result.requested_date,
            publication_run_id=result.run_id,
            triggered=False,
            status="skipped",
            reason=reason,
        )
