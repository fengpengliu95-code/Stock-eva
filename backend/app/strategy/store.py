import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.strategy.dsl import ValidatedRule
from backend.app.strategy.models import (
    SignalResult,
    StrategyRecord,
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
            rows = connection.execute(
                "SELECT * FROM strategies ORDER BY name, id"
            ).fetchall()
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
    ) -> StrategyRunRecord:
        identifier = str(uuid4())
        created_at = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO strategy_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            connection.commit()
        finally:
            connection.close()
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
            results=[
                SignalResult.model_validate(result)
                for result in json.loads(row["results"])
            ],
            created_at=row["created_at"],
        )

    def get_run(self, run_id: str) -> StrategyRunRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM strategy_runs WHERE id = ?",
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
                SELECT * FROM strategy_runs
                WHERE strategy_id = ?
                ORDER BY created_at, id
                """,
                [strategy_id],
            ).fetchall()
        finally:
            connection.close()
        return [self._run(row) for row in rows]
