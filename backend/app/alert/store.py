import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

from backend.app.alert.models import (
    AlertEventRecord,
    AlertRuleRecord,
    AlertTransitionRecord,
)
from backend.app.user.database import user_database_initialization


class AlertStoreError(RuntimeError):
    pass


class AlertNotFoundError(AlertStoreError):
    pass


class AlertConflictError(AlertStoreError):
    pass


ALLOWED_TRANSITIONS = {
    "pending": {"eligible", "suppressed", "error"},
    "eligible": {"triggered", "resolved", "suppressed", "error"},
    "triggered": {"acknowledged", "suppressed", "resolved"},
    "acknowledged": {"suppressed", "resolved"},
    "suppressed": {"eligible"},
    "resolved": set(),
    "error": set(),
}


class AlertStore:
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
                CREATE TABLE IF NOT EXISTS alert_rules (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    watchlist_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    strategy_version INTEGER NOT NULL,
                    strategy_version_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alert_events (
                    id TEXT PRIMARY KEY,
                    alert_rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
                    strategy_id TEXT NOT NULL,
                    strategy_version INTEGER NOT NULL,
                    strategy_version_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    signal_date TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    source TEXT,
                    quality_status TEXT NOT NULL,
                    quality_issues TEXT NOT NULL,
                    explanation TEXT NOT NULL,
                    data_fingerprint TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (strategy_version_id, symbol, signal_date)
                );

                CREATE TABLE IF NOT EXISTS alert_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_event_id TEXT NOT NULL REFERENCES alert_events(id) ON DELETE CASCADE,
                    from_state TEXT,
                    to_state TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @staticmethod
    def _rule(row: sqlite3.Row) -> AlertRuleRecord:
        return AlertRuleRecord(**dict(row))

    def create_rule(
        self,
        *,
        name: str,
        watchlist_id: str,
        strategy_id: str,
        strategy_version: int,
        strategy_version_id: str,
    ) -> AlertRuleRecord:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("alert rule name cannot be empty")
        identifier = str(uuid4())
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute(
                "INSERT INTO alert_rules VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    identifier,
                    normalized_name,
                    watchlist_id,
                    strategy_id,
                    strategy_version,
                    strategy_version_id,
                    now,
                ],
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            raise AlertConflictError(
                f"alert rule already exists: {normalized_name}"
            ) from exc
        finally:
            connection.close()
        return self.get_rule(identifier)

    def get_rule(self, identifier: str) -> AlertRuleRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM alert_rules WHERE id = ?",
                [identifier],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise AlertNotFoundError("alert rule not found")
        return self._rule(row)

    def list_rules(self) -> list[AlertRuleRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM alert_rules ORDER BY name, id"
            ).fetchall()
        finally:
            connection.close()
        return [self._rule(row) for row in rows]

    @staticmethod
    def _transition(row: sqlite3.Row) -> AlertTransitionRecord:
        return AlertTransitionRecord(**dict(row))

    def _transitions(
        self,
        connection: sqlite3.Connection,
        event_id: str,
    ) -> list[AlertTransitionRecord]:
        rows = connection.execute(
            """
            SELECT * FROM alert_transitions
            WHERE alert_event_id = ?
            ORDER BY id
            """,
            [event_id],
        ).fetchall()
        return [self._transition(row) for row in rows]

    def _event(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> AlertEventRecord:
        return AlertEventRecord(
            id=row["id"],
            alert_rule_id=row["alert_rule_id"],
            strategy_id=row["strategy_id"],
            strategy_version=row["strategy_version"],
            strategy_version_id=row["strategy_version_id"],
            symbol=row["symbol"],
            signal_date=row["signal_date"],
            idempotency_key=row["idempotency_key"],
            state=row["state"],
            source=row["source"],
            quality_status=row["quality_status"],
            quality_issues=json.loads(row["quality_issues"]),
            explanation=json.loads(row["explanation"]),
            data_fingerprint=row["data_fingerprint"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            transitions=self._transitions(connection, row["id"]),
        )

    def get_event(self, identifier: str) -> AlertEventRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM alert_events WHERE id = ?",
                [identifier],
            ).fetchone()
            if row is None:
                raise AlertNotFoundError("alert event not found")
            return self._event(connection, row)
        finally:
            connection.close()

    def get_event_by_key(self, idempotency_key: str) -> AlertEventRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM alert_events WHERE idempotency_key = ?",
                [idempotency_key],
            ).fetchone()
            return self._event(connection, row) if row is not None else None
        finally:
            connection.close()

    def list_events(self) -> list[AlertEventRecord]:
        connection = self._connect()
        try:
            rows = connection.execute(
                """
                SELECT * FROM alert_events
                ORDER BY signal_date DESC, created_at DESC, symbol
                """
            ).fetchall()
            return [self._event(connection, row) for row in rows]
        finally:
            connection.close()

    def create_pending(
        self,
        *,
        rule: AlertRuleRecord,
        symbol: str,
        signal_date: date,
        source: str | None,
        quality_status: str,
        quality_issues: list[str],
        explanation: dict[str, object],
        data_fingerprint: str | None,
    ) -> AlertEventRecord:
        idempotency_key = f"{rule.strategy_version_id}:{symbol}:{signal_date}"
        existing = self.get_event_by_key(idempotency_key)
        if existing is not None:
            return existing
        identifier = str(uuid4())
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO alert_events VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    identifier,
                    rule.id,
                    rule.strategy_id,
                    rule.strategy_version,
                    rule.strategy_version_id,
                    symbol,
                    signal_date.isoformat(),
                    idempotency_key,
                    source,
                    quality_status,
                    json.dumps(quality_issues, ensure_ascii=False),
                    json.dumps(explanation, ensure_ascii=False, sort_keys=True),
                    data_fingerprint,
                    now,
                    now,
                ],
            )
            connection.execute(
                """
                INSERT INTO alert_transitions (
                    alert_event_id, from_state, to_state, actor, reason, created_at
                ) VALUES (?, NULL, 'pending', 'system', 'evaluation_started', ?)
                """,
                [identifier, now],
            )
            connection.commit()
        except sqlite3.IntegrityError:
            connection.rollback()
            existing = self.get_event_by_key(idempotency_key)
            if existing is None:
                raise
            return existing
        finally:
            connection.close()
        return self.get_event(identifier)

    def transition(
        self,
        identifier: str,
        *,
        to_state: str,
        actor: str,
        reason: str,
    ) -> AlertEventRecord:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state FROM alert_events WHERE id = ?",
                [identifier],
            ).fetchone()
            if row is None:
                raise AlertNotFoundError("alert event not found")
            current = row["state"]
            if to_state not in ALLOWED_TRANSITIONS[current]:
                raise AlertConflictError(
                    f"invalid alert transition: {current} -> {to_state}"
                )
            if to_state == "triggered" and actor != "system":
                raise AlertConflictError(
                    "triggered state requires system post-close evaluation"
                )
            now = datetime.now(UTC).isoformat()
            connection.execute(
                "UPDATE alert_events SET state = ?, updated_at = ? WHERE id = ?",
                [to_state, now, identifier],
            )
            connection.execute(
                """
                INSERT INTO alert_transitions (
                    alert_event_id, from_state, to_state, actor, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [identifier, current, to_state, actor, reason, now],
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_event(identifier)
