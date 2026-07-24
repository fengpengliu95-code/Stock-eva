import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from backend.app.user.models import (
    Position,
    PositionCreate,
    PositionUpdate,
    Watchlist,
    WatchlistItem,
)


class UserDataError(RuntimeError):
    pass


class NotFoundError(UserDataError):
    pass


class ConflictError(UserDataError):
    pass


class DuplicateError(UserDataError):
    pass


class UserStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL UNIQUE,
                quantity TEXT NOT NULL,
                avg_cost TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                today_buy_qty TEXT NOT NULL,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS watchlists (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS watchlist_items (
                id TEXT PRIMARY KEY,
                watchlist_id TEXT NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
                symbol TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (watchlist_id, symbol)
            );
            """
        )
        return connection

    @staticmethod
    def _position(row: sqlite3.Row) -> Position:
        return Position(
            id=row["id"],
            symbol=row["symbol"],
            quantity=Decimal(row["quantity"]),
            avg_cost=Decimal(row["avg_cost"]),
            as_of_date=row["as_of_date"],
            today_buy_qty=Decimal(row["today_buy_qty"]),
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def create_position(self, data: PositionCreate) -> Position:
        now = datetime.now(UTC).isoformat()
        identifier = str(uuid4())
        connection = self._connect()
        try:
            connection.execute(
                """
                INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                [
                    identifier,
                    data.symbol,
                    str(data.quantity),
                    str(data.avg_cost),
                    data.as_of_date.isoformat(),
                    str(data.today_buy_qty),
                    now,
                    now,
                ],
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            raise DuplicateError(f"position already exists for {data.symbol}") from exc
        finally:
            connection.close()
        return self.get_position(identifier)

    def get_position(self, identifier: str) -> Position:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM positions WHERE id = ?",
                [identifier],
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise NotFoundError("position not found")
        return self._position(row)

    def list_positions(self) -> list[Position]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM positions ORDER BY symbol"
            ).fetchall()
        finally:
            connection.close()
        return [self._position(row) for row in rows]

    def update_position(self, identifier: str, data: PositionUpdate) -> Position:
        connection = self._connect()
        try:
            cursor = connection.execute(
                """
                UPDATE positions
                SET quantity = ?, avg_cost = ?, as_of_date = ?, today_buy_qty = ?,
                    version = version + 1, updated_at = ?
                WHERE id = ? AND version = ?
                """,
                [
                    str(data.quantity),
                    str(data.avg_cost),
                    data.as_of_date.isoformat(),
                    str(data.today_buy_qty),
                    datetime.now(UTC).isoformat(),
                    identifier,
                    data.expected_version,
                ],
            )
            connection.commit()
            if cursor.rowcount != 1:
                self._raise_missing_or_conflict(connection, identifier, "positions")
        finally:
            connection.close()
        return self.get_position(identifier)

    def delete_position(self, identifier: str, expected_version: int) -> None:
        connection = self._connect()
        try:
            cursor = connection.execute(
                "DELETE FROM positions WHERE id = ? AND version = ?",
                [identifier, expected_version],
            )
            connection.commit()
            if cursor.rowcount != 1:
                self._raise_missing_or_conflict(connection, identifier, "positions")
        finally:
            connection.close()

    @staticmethod
    def _raise_missing_or_conflict(
        connection: sqlite3.Connection,
        identifier: str,
        table: str,
    ) -> None:
        exists = connection.execute(
            f"SELECT 1 FROM {table} WHERE id = ?",
            [identifier],
        ).fetchone()
        if exists:
            raise ConflictError("version conflict")
        raise NotFoundError("record not found")

    @staticmethod
    def _watchlist(row: sqlite3.Row) -> Watchlist:
        return Watchlist(**dict(row))

    def create_watchlist(self, name: str) -> Watchlist:
        normalized_name = name.strip()
        if not normalized_name:
            raise ValueError("watchlist name cannot be empty")
        identifier = str(uuid4())
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            connection.execute(
                "INSERT INTO watchlists VALUES (?, ?, 1, ?, ?)",
                [identifier, normalized_name, now, now],
            )
            connection.commit()
            row = connection.execute(
                "SELECT * FROM watchlists WHERE id = ?",
                [identifier],
            ).fetchone()
        except sqlite3.IntegrityError as exc:
            raise DuplicateError(f"watchlist already exists: {normalized_name}") from exc
        finally:
            connection.close()
        return self._watchlist(row)

    def list_watchlists(self) -> list[Watchlist]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM watchlists ORDER BY name, id"
            ).fetchall()
        finally:
            connection.close()
        return [self._watchlist(row) for row in rows]

    def delete_watchlist(self, identifier: str, expected_version: int) -> None:
        connection = self._connect()
        try:
            cursor = connection.execute(
                "DELETE FROM watchlists WHERE id = ? AND version = ?",
                [identifier, expected_version],
            )
            connection.commit()
            if cursor.rowcount != 1:
                self._raise_missing_or_conflict(connection, identifier, "watchlists")
        finally:
            connection.close()

    @staticmethod
    def _watchlist_item(row: sqlite3.Row) -> WatchlistItem:
        return WatchlistItem(**dict(row))

    def add_watchlist_item(self, watchlist_id: str, symbol: str) -> tuple[WatchlistItem, bool]:
        connection = self._connect()
        try:
            if connection.execute(
                "SELECT 1 FROM watchlists WHERE id = ?",
                [watchlist_id],
            ).fetchone() is None:
                raise NotFoundError("watchlist not found")
            existing = connection.execute(
                "SELECT * FROM watchlist_items WHERE watchlist_id = ? AND symbol = ?",
                [watchlist_id, symbol],
            ).fetchone()
            if existing is not None:
                return self._watchlist_item(existing), False
            identifier = str(uuid4())
            cursor = connection.execute(
                "INSERT OR IGNORE INTO watchlist_items VALUES (?, ?, ?, ?)",
                [identifier, watchlist_id, symbol, datetime.now(UTC).isoformat()],
            )
            connection.commit()
            row = connection.execute(
                "SELECT * FROM watchlist_items WHERE watchlist_id = ? AND symbol = ?",
                [watchlist_id, symbol],
            ).fetchone()
            return self._watchlist_item(row), cursor.rowcount == 1
        finally:
            connection.close()

    def list_watchlist_items(self, watchlist_id: str) -> list[WatchlistItem]:
        connection = self._connect()
        try:
            if connection.execute(
                "SELECT 1 FROM watchlists WHERE id = ?",
                [watchlist_id],
            ).fetchone() is None:
                raise NotFoundError("watchlist not found")
            rows = connection.execute(
                """
                SELECT * FROM watchlist_items
                WHERE watchlist_id = ?
                ORDER BY symbol
                """,
                [watchlist_id],
            ).fetchall()
        finally:
            connection.close()
        return [self._watchlist_item(row) for row in rows]

    def remove_watchlist_item(self, watchlist_id: str, symbol: str) -> None:
        connection = self._connect()
        try:
            if connection.execute(
                "SELECT 1 FROM watchlists WHERE id = ?",
                [watchlist_id],
            ).fetchone() is None:
                raise NotFoundError("watchlist not found")
            connection.execute(
                "DELETE FROM watchlist_items WHERE watchlist_id = ? AND symbol = ?",
                [watchlist_id, symbol],
            )
            connection.commit()
        finally:
            connection.close()
