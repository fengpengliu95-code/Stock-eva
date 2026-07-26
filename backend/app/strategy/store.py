import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.strategy.dsl import ValidatedRule
from backend.app.strategy.models import (
    SignalResult,
    StrategyRecord,
    StrategyRunBatchRecord,
    StrategyRunRecord,
    StrategyVersionRecord,
)
from backend.app.user.database import user_database_initialization


class StrategyStoreError(RuntimeError):
    pass


class StrategyNotFoundError(StrategyStoreError):
    pass


class StrategyConflictError(StrategyStoreError):
    pass


class StrategyStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        with user_database_initialization(self.path):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS strategies (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    current_version INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS strategy_versions (
                    id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    rule_ast TEXT NOT NULL,
                    rule_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (strategy_id, version)
                );

                CREATE TABLE IF NOT EXISTS strategy_runs (
                    id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
                    strategy_version INTEGER NOT NULL,
                    as_of_date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    rule_hash TEXT NOT NULL,
                    data_fingerprint TEXT NOT NULL,
                    symbols TEXT NOT NULL,
                    results TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS strategy_run_metadata (
                    run_id TEXT PRIMARY KEY
                        REFERENCES strategy_runs(id) ON DELETE CASCADE,
                    scope TEXT NOT NULL,
                    total_symbols INTEGER NOT NULL,
                    batch_size INTEGER NOT NULL,
                    total_batches INTEGER NOT NULL,
                    completed_batches INTEGER NOT NULL,
                    failed_batches INTEGER NOT NULL,
                    failed_symbols TEXT NOT NULL,
                    batches TEXT NOT NULL,
                    idempotency_key TEXT
                );
                """
            )
            metadata_columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(strategy_run_metadata)").fetchall()
            }
            if "idempotency_key" not in metadata_columns:
                connection.execute(
                    "ALTER TABLE strategy_run_metadata ADD COLUMN idempotency_key TEXT"
                )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS strategy_run_idempotency_key
                ON strategy_run_metadata(idempotency_key)
                WHERE idempotency_key IS NOT NULL
                """
            )
        return connection

    @staticmethod
    def _strategy(row: sqlite3.Row) -> StrategyRecord:
        return StrategyRecord(**dict(row))

    @staticmethod
    def _version(row: sqlite3.Row) -> StrategyVersionRecord:
        return StrategyVersionRecord(
            id=row["id"],
            strategy_id=row["strategy_id"],
            version=row["version"],
            rule_ast=json.loads(row["rule_ast"]),
            rule_hash=row["rule_hash"],
            created_at=row["created_at"],
        )

    def create_strategy(
        self,
        name: str,
        rule: ValidatedRule,
    ) -> tuple[StrategyRecord, StrategyVersionRecord]:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("strategy name cannot be empty")
        strategy_id = str(uuid4())
        version_id = str(uuid4())
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO strategies VALUES (?, ?, 1, ?, ?)",
                [strategy_id, normalized_name, now, now],
            )
            connection.execute(
                "INSERT INTO strategy_versions VALUES (?, ?, 1, ?, ?, ?)",
                [
                    version_id,
                    strategy_id,
                    rule.canonical_json,
                    rule.rule_hash,
                    now,
                ],
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            raise StrategyConflictError(f"strategy already exists: {normalized_name}") from exc
        finally:
            connection.close()
        return self.get_strategy(strategy_id), self.get_version(strategy_id, 1)

    def get_strategy(self, strategy_id: str) -> StrategyRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM strategies WHERE id = ?",
                [strategy_id],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise StrategyNotFoundError("strategy not found")
        return self._strategy(row)

    def list_strategies(self) -> list[StrategyRecord]:
        connection = self._connect()
        try:
            rows = connection.execute("SELECT * FROM strategies ORDER BY name, id").fetchall()
        finally:
            connection.close()
        return [self._strategy(row) for row in rows]

    def get_version(self, strategy_id: str, version: int) -> StrategyVersionRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT * FROM strategy_versions
                WHERE strategy_id = ? AND version = ?
                """,
                [strategy_id, version],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise StrategyNotFoundError("strategy version not found")
        return self._version(row)

    def add_version(
        self,
        strategy_id: str,
        *,
        expected_current_version: int,
        rule: ValidatedRule,
    ) -> StrategyVersionRecord:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT current_version FROM strategies WHERE id = ?",
                [strategy_id],
            ).fetchone()
            if row is None:
                raise StrategyNotFoundError("strategy not found")
            if row["current_version"] != expected_current_version:
                raise StrategyConflictError("strategy version conflict")
            next_version = expected_current_version + 1
            version_id = str(uuid4())
            now = datetime.now(UTC).isoformat()
            connection.execute(
                "INSERT INTO strategy_versions VALUES (?, ?, ?, ?, ?, ?)",
                [
                    version_id,
                    strategy_id,
                    next_version,
                    rule.canonical_json,
                    rule.rule_hash,
                    now,
                ],
            )
            cursor = connection.execute(
                """
                UPDATE strategies
                SET current_version = ?, updated_at = ?
                WHERE id = ? AND current_version = ?
                """,
                [next_version, now, strategy_id, expected_current_version],
            )
            if cursor.rowcount != 1:
                raise StrategyConflictError("strategy version conflict")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_version(strategy_id, next_version)

    def save_run(
        self,
        *,
        strategy_id: str,
        strategy_version: int,
        as_of_date,
        status: str,
        rule_hash: str,
        data_fingerprint: str,
        symbols: list[str],
        results: list[SignalResult],
        scope: str = "explicit",
        batch_size: int | None = None,
        batches: list[StrategyRunBatchRecord] | None = None,
        failed_symbols: list[str] | None = None,
        idempotency_key: str | None = None,
    ) -> StrategyRunRecord:
        identifier = str(uuid4())
        created_at = datetime.now(UTC).isoformat()
        persisted_batches = batches or [
            StrategyRunBatchRecord(
                index=1,
                status="completed",
                symbol_count=len(symbols),
                first_symbol=symbols[0],
                last_symbol=symbols[-1],
                matched_count=sum(result.status == "matched" for result in results),
                not_matched_count=sum(result.status == "not_matched" for result in results),
                excluded_count=sum(result.status == "excluded" for result in results),
                insufficient_count=sum(result.status == "insufficient_data" for result in results),
                data_fingerprint=data_fingerprint,
            )
        ]
        failed = failed_symbols or []
        connection = self._connect()
        idempotency_conflict = False
        try:
            connection.execute(
                """
                INSERT INTO strategy_runs (
                    id, strategy_id, strategy_version, as_of_date, status,
                    rule_hash, data_fingerprint, symbols, results, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    identifier,
                    strategy_id,
                    strategy_version,
                    as_of_date.isoformat(),
                    status,
                    rule_hash,
                    data_fingerprint,
                    json.dumps(symbols, ensure_ascii=False),
                    json.dumps(
                        [result.model_dump(mode="json") for result in results],
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    created_at,
                ],
            )
            connection.execute(
                """
                INSERT INTO strategy_run_metadata (
                    run_id, scope, total_symbols, batch_size, total_batches,
                    completed_batches, failed_batches, failed_symbols, batches,
                    idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    identifier,
                    scope,
                    len(symbols),
                    batch_size if batch_size is not None else len(symbols),
                    len(persisted_batches),
                    sum(batch.status == "completed" for batch in persisted_batches),
                    sum(batch.status == "error" for batch in persisted_batches),
                    json.dumps(failed, ensure_ascii=False),
                    json.dumps(
                        [batch.model_dump(mode="json") for batch in persisted_batches],
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    idempotency_key,
                ],
            )
            connection.commit()
        except sqlite3.IntegrityError:
            connection.rollback()
            if idempotency_key is None:
                raise
            idempotency_conflict = True
        finally:
            connection.close()
        if idempotency_conflict:
            existing = self.find_run_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing
            raise StrategyConflictError("strategy run idempotency conflict")
        return self.get_run(identifier)

    @staticmethod
    def _run(row: sqlite3.Row) -> StrategyRunRecord:
        return StrategyRunRecord(
            id=row["id"],
            strategy_id=row["strategy_id"],
            strategy_version=row["strategy_version"],
            as_of_date=row["as_of_date"],
            status=row["status"],
            rule_hash=row["rule_hash"],
            data_fingerprint=row["data_fingerprint"],
            symbols=json.loads(row["symbols"]),
            results=[SignalResult.model_validate(result) for result in json.loads(row["results"])],
            created_at=row["created_at"],
            scope=row["scan_scope"] or "explicit",
            total_symbols=(
                row["scan_total_symbols"]
                if row["scan_total_symbols"] is not None
                else len(json.loads(row["symbols"]))
            ),
            batch_size=row["scan_batch_size"] or len(json.loads(row["symbols"])),
            total_batches=row["scan_total_batches"] or 1,
            completed_batches=(
                row["scan_completed_batches"] if row["scan_completed_batches"] is not None else 1
            ),
            failed_batches=(
                row["scan_failed_batches"] if row["scan_failed_batches"] is not None else 0
            ),
            failed_symbols=(
                json.loads(row["scan_failed_symbols"])
                if row["scan_failed_symbols"] is not None
                else []
            ),
            batches=(
                [
                    StrategyRunBatchRecord.model_validate(batch)
                    for batch in json.loads(row["scan_batches"])
                ]
                if row["scan_batches"] is not None
                else []
            ),
            idempotency_key=row["scan_idempotency_key"],
        )

    def find_run_by_idempotency_key(
        self,
        idempotency_key: str,
    ) -> StrategyRunRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT run_id
                FROM strategy_run_metadata
                WHERE idempotency_key = ?
                """,
                [idempotency_key],
            ).fetchone()
        finally:
            connection.close()
        return self.get_run(row["run_id"]) if row is not None else None

    def get_run(self, run_id: str) -> StrategyRunRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT r.*,
                       m.scope AS scan_scope,
                       m.total_symbols AS scan_total_symbols,
                       m.batch_size AS scan_batch_size,
                       m.total_batches AS scan_total_batches,
                       m.completed_batches AS scan_completed_batches,
                       m.failed_batches AS scan_failed_batches,
                       m.failed_symbols AS scan_failed_symbols,
                       m.batches AS scan_batches,
                       m.idempotency_key AS scan_idempotency_key
                FROM strategy_runs r
                LEFT JOIN strategy_run_metadata m ON m.run_id = r.id
                WHERE r.id = ?
                """,
                [run_id],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise StrategyNotFoundError("strategy run not found")
        return self._run(row)

    def list_runs(self, strategy_id: str) -> list[StrategyRunRecord]:
        self.get_strategy(strategy_id)
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT r.*,
                       m.scope AS scan_scope,
                       m.total_symbols AS scan_total_symbols,
                       m.batch_size AS scan_batch_size,
                       m.total_batches AS scan_total_batches,
                       m.completed_batches AS scan_completed_batches,
                       m.failed_batches AS scan_failed_batches,
                       m.failed_symbols AS scan_failed_symbols,
                       m.batches AS scan_batches,
                       m.idempotency_key AS scan_idempotency_key
                FROM strategy_runs r
                LEFT JOIN strategy_run_metadata m ON m.run_id = r.id
                WHERE r.strategy_id = ?
                ORDER BY r.created_at, r.id
                """,
                [strategy_id],
            ).fetchall()
        finally:
            connection.close()
        return [self._run(row) for row in rows]
