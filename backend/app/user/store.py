from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sqlite3
import stat
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.app.security_identity import derive_security_identity
from backend.app.user.database import user_database_initialization
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


_USER_SNAPSHOT_DOMAIN = "stock-eva/r2f4.2/"
_USER_SYMBOL = r"^(sh|sz)\.[0-9]{6}$"
_ADMISSION_SECRET = object()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def _user_digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        (_USER_SNAPSHOT_DOMAIN + domain).encode("ascii")
        + b"\n"
        + _canonical_json(value).encode("utf-8")
    ).hexdigest()


class RequiredUserSymbolSnapshot(BaseModel):
    """The single writer-owned user-symbol observation used by universe admission."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1
    snapshot_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    symbols: tuple[str, ...]
    roles_by_symbol: tuple[tuple[str, tuple[Literal["position", "watchlist"], ...]], ...]
    captured_at: datetime
    source: Literal["UserStore.capture_required_symbol_snapshot_existing"]
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("captured_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("user snapshot timestamp must be timezone aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _identity(self) -> RequiredUserSymbolSnapshot:
        import re

        if self.symbols != tuple(sorted(set(self.symbols))):
            raise ValueError("user snapshot symbols must be sorted and unique")
        if tuple(symbol for symbol, _ in self.roles_by_symbol) != self.symbols:
            raise ValueError("user snapshot roles must cover symbols exactly")
        if any(
            re.fullmatch(_USER_SYMBOL, symbol) is None
            or not roles
            or roles != tuple(sorted(set(roles)))
            for symbol, roles in self.roles_by_symbol
        ):
            raise ValueError("user snapshot role identity invalid")
        # The token is intentionally supplied by the private read wrapper; the
        # model's digest is checked there using the exact required-symbol preimage.
        if self.snapshot_id != self.snapshot_sha256[:32]:
            raise ValueError("user snapshot id is not digest-derived")
        if not self.snapshot_sha256:
            raise ValueError("user snapshot digest is empty")
        return self


class RequiredUserSymbolSnapshotRead:
    """Opaque trusted admission produced only by the existing-store capture."""

    __slots__ = ("snapshot", "snapshot_token")

    def __init__(
        self,
        snapshot: RequiredUserSymbolSnapshot,
        snapshot_token: str,
        *,
        _secret: object | None = None,
    ) -> None:
        if _secret is not _ADMISSION_SECRET:
            raise TypeError("user snapshot admission is writer-owned")
        if not isinstance(snapshot, RequiredUserSymbolSnapshot):
            raise TypeError("invalid user snapshot")
        if not isinstance(snapshot_token, str) or len(snapshot_token) != 64:
            raise TypeError("invalid user snapshot token")
        expected = _user_digest(
            "required-symbol-snapshot/v1",
            {
                "schema_version": 1,
                "snapshot_token": snapshot_token,
                "symbols": [
                    {"symbol": symbol, "roles": list(roles)}
                    for symbol, roles in snapshot.roles_by_symbol
                ],
            },
        )
        if snapshot.snapshot_sha256 != expected:
            raise TypeError("user snapshot digest mismatch")
        self.snapshot = snapshot
        self.snapshot_token = snapshot_token


class UserStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._descriptor_fds: dict[int, int] = {}

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

    def _connect_existing(self) -> sqlite3.Connection:
        """Open only an already existing, exact user DB; never initialize it."""
        if not self.path.exists() or self.path.is_symlink() or not self.path.is_file():
            raise UserDataError("existing user database unavailable")
        descriptor: int | None = None
        connection: sqlite3.Connection | None = None
        try:
            descriptor = os.open(
                self.path,
                os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
            )
            descriptor_stat = os.fstat(descriptor)
            path_stat = os.stat(self.path, follow_symlinks=False)
            if not stat.S_ISREG(descriptor_stat.st_mode) or (
                descriptor_stat.st_dev,
                descriptor_stat.st_ino,
            ) != (path_stat.st_dev, path_stat.st_ino):
                raise UserDataError("existing user database descriptor changed")
            # SQLite cannot resolve a WAL sidecar when opened through /dev/fd/N
            # (it would look for `/dev/fd/N-wal`). Resolve the already-open
            # descriptor to its canonical path, then keep the descriptor alive
            # for the entire descriptor-bound transaction. The fstat/lstat
            # checks above and below make a pathname swap fail closed while the
            # connection is being admitted; subsequent reads use the opened
            # inode, not a re-resolved user-supplied path.
            getpath = getattr(fcntl, "F_GETPATH", None)
            if getpath is not None:
                opened_path = fcntl.fcntl(descriptor, getpath, b"\0" * 1024)
                opened_path = opened_path.split(b"\0", 1)[0].decode()
            else:
                opened_path = os.readlink(f"/proc/self/fd/{descriptor}")
            connection = sqlite3.connect(opened_path, timeout=0)
            opened_stat = os.stat(opened_path, follow_symlinks=False)
            if (opened_stat.st_dev, opened_stat.st_ino) != (
                descriptor_stat.st_dev,
                descriptor_stat.st_ino,
            ):
                raise UserDataError("existing user database descriptor changed")
            self._descriptor_fds[id(connection)] = descriptor
            descriptor = None
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 0")
            objects = connection.execute(
                "SELECT type,name,tbl_name FROM sqlite_master "
                "WHERE type IN ('table','view','trigger') ORDER BY type,name"
            ).fetchall()
            if {(row[0], row[1]) for row in objects} != {
                ("table", "positions"),
                ("table", "watchlists"),
                ("table", "watchlist_items"),
            }:
                raise UserDataError("user database schema is not exact")
            expected = {
                "positions": {
                    "id",
                    "symbol",
                    "quantity",
                    "avg_cost",
                    "as_of_date",
                    "today_buy_qty",
                    "version",
                    "created_at",
                    "updated_at",
                },
                "watchlists": {"id", "name", "version", "created_at", "updated_at"},
                "watchlist_items": {"id", "watchlist_id", "symbol", "created_at"},
            }
            for table, columns in expected.items():
                found = {
                    row[1] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if found != columns:
                    raise UserDataError("user database columns are not exact")
            return connection
        except (OSError, sqlite3.Error, UserDataError):
            try:
                if connection is not None:
                    owned = self._descriptor_fds.pop(id(connection), None)
                    connection.close()
                    if owned is not None:
                        os.close(owned)
            finally:
                if descriptor is not None:
                    os.close(descriptor)
            raise

    def capture_required_symbol_snapshot_existing(self) -> RequiredUserSymbolSnapshotRead:
        """Capture holdings/watchlist symbols in one existing-DB transaction.

        This is deliberately separate from ``_connect``: a missing or malformed
        DB must be unavailable, never initialized or migrated as a side effect.
        """
        connection = self._connect_existing()
        try:
            connection.execute("BEGIN IMMEDIATE")
            positions = connection.execute(
                "SELECT id,symbol,quantity,avg_cost,as_of_date,today_buy_qty,version,"
                "created_at,updated_at FROM positions ORDER BY id"
            ).fetchall()
            watchlists = connection.execute(
                "SELECT id,name,version,created_at,updated_at FROM watchlists ORDER BY id"
            ).fetchall()
            watchlist_ids = tuple(row["id"] for row in watchlists)
            items = connection.execute(
                "SELECT id,watchlist_id,symbol,created_at FROM watchlist_items ORDER BY id"
            ).fetchall()
            if any(row["watchlist_id"] not in watchlist_ids for row in items):
                raise UserDataError("watchlist item has no parent")

            roles: dict[str, set[str]] = {}
            user_rows: list[dict[str, object]] = []
            for row in positions:
                symbol = row["symbol"]
                if not isinstance(symbol, str) or re.fullmatch(_USER_SYMBOL, symbol) is None:
                    raise UserDataError("user symbol is unsafe")
                identity = derive_security_identity(symbol, "1")
                if identity.security_type != "stock" or identity.board not in {
                    "main",
                    "chinext",
                    "star",
                }:
                    raise UserDataError("user symbol is not an eligible A-share stock")
                roles.setdefault(symbol, set()).add("position")
                user_rows.append({"table": "positions", "row": dict(row)})
            for row in watchlists:
                user_rows.append({"table": "watchlists", "row": dict(row)})
            for row in items:
                symbol = row["symbol"]
                if not isinstance(symbol, str) or re.fullmatch(_USER_SYMBOL, symbol) is None:
                    raise UserDataError("user symbol is unsafe")
                identity = derive_security_identity(symbol, "1")
                if identity.security_type != "stock" or identity.board not in {
                    "main",
                    "chinext",
                    "star",
                }:
                    raise UserDataError("user symbol is not an eligible A-share stock")
                roles.setdefault(symbol, set()).add("watchlist")
                user_rows.append({"table": "watchlist_items", "row": dict(row)})
            symbols = tuple(sorted(roles))
            roles_by_symbol = tuple((symbol, tuple(sorted(roles[symbol]))) for symbol in symbols)
            token = _user_digest("user-rows/v1", {"schema_version": 1, "user_rows": user_rows})
            snapshot_sha = _user_digest(
                "required-symbol-snapshot/v1",
                {
                    "schema_version": 1,
                    "snapshot_token": token,
                    "symbols": [
                        {"symbol": symbol, "roles": list(role_values)}
                        for symbol, role_values in roles_by_symbol
                    ],
                },
            )
            snapshot = RequiredUserSymbolSnapshot(
                snapshot_id=snapshot_sha[:32],
                symbols=symbols,
                roles_by_symbol=roles_by_symbol,
                captured_at=datetime.now(UTC),
                source="UserStore.capture_required_symbol_snapshot_existing",
                snapshot_sha256=snapshot_sha,
            )
            connection.commit()
            return RequiredUserSymbolSnapshotRead(snapshot, token, _secret=_ADMISSION_SECRET)
        except Exception as exc:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
            if isinstance(exc, UserDataError):
                raise
            raise UserDataError("user snapshot capture unavailable") from exc
        finally:
            descriptor = self._descriptor_fds.pop(id(connection), None)
            connection.close()
            if descriptor is not None:
                os.close(descriptor)

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
            rows = connection.execute("SELECT * FROM positions ORDER BY symbol").fetchall()
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
            rows = connection.execute("SELECT * FROM watchlists ORDER BY name, id").fetchall()
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
            if (
                connection.execute(
                    "SELECT 1 FROM watchlists WHERE id = ?",
                    [watchlist_id],
                ).fetchone()
                is None
            ):
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
            if (
                connection.execute(
                    "SELECT 1 FROM watchlists WHERE id = ?",
                    [watchlist_id],
                ).fetchone()
                is None
            ):
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
            if (
                connection.execute(
                    "SELECT 1 FROM watchlists WHERE id = ?",
                    [watchlist_id],
                ).fetchone()
                is None
            ):
                raise NotFoundError("watchlist not found")
            connection.execute(
                "DELETE FROM watchlist_items WHERE watchlist_id = ? AND symbol = ?",
                [watchlist_id, symbol],
            )
            connection.commit()
        finally:
            connection.close()
