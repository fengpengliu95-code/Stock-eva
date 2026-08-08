import hashlib
import json
import os
import sqlite3
import stat
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.app.portfolio.models import (
    PortfolioDailySnapshot,
    PortfolioDailySnapshotCreate,
    PortfolioDailySnapshotUpdate,
    PortfolioPositionInput,
    PortfolioSnapshotContent,
    PortfolioSnapshotWriteResult,
)
from backend.app.user.database import user_database_initialization

_TABLE = "portfolio_daily_snapshots"
_EXPECTED_COLUMNS = {
    "as_of",
    "revision",
    "snapshot_id",
    "source",
    "cash",
    "manual_nav",
    "positions_json",
    "content_hash",
    "recorded_at",
}


class PortfolioLedgerError(RuntimeError):
    pass


class PortfolioLedgerConflict(PortfolioLedgerError):
    pass


class PortfolioLedgerUnavailable(PortfolioLedgerError):
    pass


class PortfolioEvidenceCutoffInvalid(PortfolioLedgerError):
    pass


@dataclass(frozen=True)
class PortfolioRiskReadView:
    snapshot: PortfolioDailySnapshot | None
    history: list[PortfolioDailySnapshot]
    evidence_cutoff_at: datetime


def portfolio_evidence_cutoff(
    as_of: date,
    known_at: datetime | None = None,
) -> datetime:
    """Resolve the knowledge boundary independently from the effective date."""
    if known_at is None:
        return datetime.combine(
            as_of,
            time.max,
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ).astimezone(UTC)
    if known_at.utcoffset() is None:
        raise PortfolioEvidenceCutoffInvalid("known_at must include a timezone offset")
    return known_at.astimezone(UTC)


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def _canonical_content(data: PortfolioSnapshotContent) -> dict[str, object]:
    return {
        "as_of": data.as_of.isoformat(),
        "cash": _decimal_text(data.cash),
        "manual_nav": _decimal_text(data.manual_nav),
        "positions": [
            {
                "symbol": item.symbol,
                "quantity": _decimal_text(item.quantity),
                "avg_cost": _decimal_text(item.avg_cost),
            }
            for item in sorted(data.positions, key=lambda value: value.symbol)
        ],
    }


def _content_hash(data: PortfolioSnapshotContent) -> str:
    encoded = json.dumps(
        _canonical_content(data),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def _snapshot_id(as_of: date, revision: int, content_hash: str) -> str:
    encoded = f"{as_of.isoformat()}:{revision}:{content_hash}"
    return f"portfolio-snapshot-{hashlib.sha256(encoded.encode()).hexdigest()[:24]}"


class PortfolioLedger:
    """Append-only manual daily snapshots in the dedicated private SQLite file."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = path
        self.clock = clock or (lambda: datetime.now(UTC))

    def _connect_writer(self) -> sqlite3.Connection:
        with user_database_initialization(self.path):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            metadata = self._safe_target_metadata()
            if metadata is None:
                flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
                flags |= getattr(os, "O_NOFOLLOW", 0)
                try:
                    descriptor = os.open(self.path, flags, 0o600)
                except FileExistsError:
                    metadata = self._safe_target_metadata()
                except OSError as exc:
                    raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
                else:
                    os.close(descriptor)
                    metadata = self._safe_target_metadata()
            if metadata is None:
                raise PortfolioLedgerUnavailable("portfolio ledger is unavailable")
            try:
                os.chmod(self.path, 0o600, follow_symlinks=False)
            except (OSError, NotImplementedError) as exc:
                raise PortfolioLedgerUnavailable(
                    "portfolio ledger permissions are unavailable"
                ) from exc
            metadata = self._safe_target_metadata()
            if metadata is None or stat.S_IMODE(metadata.st_mode) != 0o600:
                raise PortfolioLedgerUnavailable("portfolio ledger permissions must be 0600")
            connection = sqlite3.connect(self.path, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
            if str(journal_mode).lower() != "delete":
                connection.close()
                raise PortfolioLedgerUnavailable("portfolio ledger requires rollback journal mode")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_TABLE} (
                    as_of TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision >= 1),
                    snapshot_id TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL CHECK (source = 'manual'),
                    cash TEXT,
                    manual_nav TEXT,
                    positions_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY (as_of, revision)
                )
                """
            )
            self._validate_schema(connection)
        return connection

    def _connect_reader(self) -> sqlite3.Connection | None:
        metadata = self._safe_target_metadata()
        if metadata is None:
            return None
        if stat.S_IMODE(metadata.st_mode) != 0o600:
            raise PortfolioLedgerUnavailable("portfolio ledger permissions must be 0600")
        rollback_journal = self.path.with_name(f"{self.path.name}-journal")
        try:
            journal_metadata = rollback_journal.lstat()
        except FileNotFoundError:
            journal_metadata = None
        except OSError as exc:
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
        if journal_metadata is not None:
            if stat.S_ISLNK(journal_metadata.st_mode) or not stat.S_ISREG(journal_metadata.st_mode):
                raise PortfolioLedgerUnavailable(
                    "portfolio ledger journal must be a regular non-symlink file"
                )
            if stat.S_IMODE(journal_metadata.st_mode) != 0o600:
                raise PortfolioLedgerUnavailable(
                    "portfolio ledger journal permissions must be 0600"
                )
            if journal_metadata.st_size > 0:
                raise PortfolioLedgerUnavailable(
                    "portfolio ledger has an active or hot rollback journal"
                )
        try:
            with self.path.open("rb") as database:
                database.seek(18)
                journal_header = database.read(2)
        except OSError as exc:
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
        if journal_header != b"\x01\x01":
            raise PortfolioLedgerUnavailable(
                "portfolio ledger is not a checkpointed rollback-journal database"
            )
        try:
            connection = sqlite3.connect(
                f"file:{self.path.resolve()}?mode=ro",
                uri=True,
                timeout=5,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            return connection
        except sqlite3.Error as exc:
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc

    def _safe_target_metadata(self) -> os.stat_result | None:
        try:
            metadata = self.path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise PortfolioLedgerUnavailable("portfolio ledger must be a regular non-symlink file")
        return metadata

    @staticmethod
    def _table_exists(connection: sqlite3.Connection) -> bool:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            [_TABLE],
        ).fetchone()
        return row is not None

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        if not PortfolioLedger._table_exists(connection):
            return
        columns = {
            row[1] for row in connection.execute(f"PRAGMA table_info('{_TABLE}')").fetchall()
        }
        if columns != _EXPECTED_COLUMNS:
            raise PortfolioLedgerUnavailable("portfolio ledger schema is invalid")

    def create(
        self,
        data: PortfolioDailySnapshotCreate,
    ) -> PortfolioSnapshotWriteResult:
        return self._write(data, expected_revision=None, mode="create")

    def revise(
        self,
        data: PortfolioDailySnapshotUpdate,
    ) -> PortfolioSnapshotWriteResult:
        return self._write(
            PortfolioDailySnapshotCreate.model_validate(
                data.model_dump(exclude={"expected_revision"})
            ),
            expected_revision=data.expected_revision,
            mode="revise",
        )

    def _write(
        self,
        data: PortfolioDailySnapshotCreate,
        *,
        expected_revision: int | None,
        mode: str,
    ) -> PortfolioSnapshotWriteResult:
        digest = _content_hash(data)
        connection: sqlite3.Connection | None = None
        try:
            connection = self._connect_writer()
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                f"SELECT * FROM {_TABLE} WHERE as_of = ? ORDER BY revision DESC LIMIT 1",
                [data.as_of.isoformat()],
            ).fetchone()
            if current is not None and current["content_hash"] == digest:
                connection.commit()
                return PortfolioSnapshotWriteResult(
                    write_status="idempotent",
                    snapshot=self._snapshot(current),
                )
            if mode == "create" and current is not None:
                raise PortfolioLedgerConflict(
                    "expected_revision is required to change an existing daily snapshot"
                )
            if mode == "revise" and current is None:
                raise PortfolioLedgerConflict("revision conflict: daily snapshot does not exist")
            current_revision = int(current["revision"]) if current is not None else 0
            if expected_revision is not None and expected_revision != current_revision:
                raise PortfolioLedgerConflict("revision conflict")
            revision = current_revision + 1
            identifier = _snapshot_id(data.as_of, revision, digest)
            recorded_at = self.clock()
            if recorded_at.utcoffset() is None:
                recorded_at = recorded_at.replace(tzinfo=UTC)
            recorded_at = recorded_at.astimezone(UTC)
            canonical = _canonical_content(data)
            connection.execute(
                f"""
                INSERT INTO {_TABLE} (
                    as_of, revision, snapshot_id, source, cash, manual_nav,
                    positions_json, content_hash, recorded_at
                ) VALUES (?, ?, ?, 'manual', ?, ?, ?, ?, ?)
                """,
                [
                    data.as_of.isoformat(),
                    revision,
                    identifier,
                    canonical["cash"],
                    canonical["manual_nav"],
                    json.dumps(
                        canonical["positions"],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    digest,
                    recorded_at.isoformat(),
                ],
            )
            row = connection.execute(
                f"SELECT * FROM {_TABLE} WHERE as_of = ? AND revision = ?",
                [data.as_of.isoformat(), revision],
            ).fetchone()
            connection.commit()
            return PortfolioSnapshotWriteResult(
                write_status="created" if revision == 1 else "revised",
                snapshot=self._snapshot(row),
            )
        except PortfolioLedgerConflict:
            if connection is not None:
                connection.rollback()
            raise
        except PortfolioLedgerUnavailable:
            if connection is not None:
                connection.rollback()
            raise
        except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
            if connection is not None:
                connection.rollback()
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
        finally:
            if connection is not None:
                connection.close()

    @staticmethod
    def _cutoff_parameter(cutoff: datetime) -> str:
        return cutoff.astimezone(UTC).isoformat()

    def read_selected(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
    ) -> PortfolioDailySnapshot | None:
        cutoff = portfolio_evidence_cutoff(as_of, known_at)
        return self._read_one(
            f"""
            SELECT * FROM {_TABLE}
            WHERE as_of = (
                SELECT max(as_of) FROM {_TABLE}
                WHERE as_of <= ?
                  AND julianday(recorded_at) <= julianday(?)
            )
              AND julianday(recorded_at) <= julianday(?)
            ORDER BY revision DESC
            LIMIT 1
            """,
            [
                as_of.isoformat(),
                self._cutoff_parameter(cutoff),
                self._cutoff_parameter(cutoff),
            ],
        )

    def read_revision(
        self,
        as_of: date,
        revision: int,
        *,
        known_at: datetime | None = None,
    ) -> PortfolioDailySnapshot | None:
        cutoff = portfolio_evidence_cutoff(as_of, known_at)
        return self._read_one(
            f"""
            SELECT * FROM {_TABLE}
            WHERE as_of = ? AND revision = ?
              AND julianday(recorded_at) <= julianday(?)
            """,
            [as_of.isoformat(), revision, self._cutoff_parameter(cutoff)],
        )

    def _read_one(
        self,
        sql: str,
        parameters: list[object],
    ) -> PortfolioDailySnapshot | None:
        connection = self._connect_reader()
        if connection is None:
            return None
        try:
            if not self._table_exists(connection):
                return None
            self._validate_schema(connection)
            row = connection.execute(sql, parameters).fetchone()
            return self._snapshot(row) if row is not None else None
        except PortfolioLedgerUnavailable:
            raise
        except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
        finally:
            connection.close()

    def read_history(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
    ) -> list[PortfolioDailySnapshot]:
        cutoff = portfolio_evidence_cutoff(as_of, known_at)
        connection = self._connect_reader()
        if connection is None:
            return []
        try:
            if not self._table_exists(connection):
                return []
            self._validate_schema(connection)
            rows = connection.execute(
                f"""
                SELECT snapshots.*
                FROM {_TABLE} AS snapshots
                JOIN (
                    SELECT as_of, max(revision) AS revision
                    FROM {_TABLE}
                    WHERE as_of <= ?
                      AND julianday(recorded_at) <= julianday(?)
                    GROUP BY as_of
                ) AS selected
                  ON selected.as_of = snapshots.as_of
                 AND selected.revision = snapshots.revision
                ORDER BY snapshots.as_of
                """,
                [as_of.isoformat(), self._cutoff_parameter(cutoff)],
            ).fetchall()
            return [self._snapshot(row) for row in rows]
        except PortfolioLedgerUnavailable:
            raise
        except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
        finally:
            connection.close()

    def read_risk_view(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
    ) -> PortfolioRiskReadView:
        """Read selected snapshot and history from one rollback-journal view."""
        cutoff = portfolio_evidence_cutoff(as_of, known_at)
        connection = self._connect_reader()
        if connection is None:
            return PortfolioRiskReadView(None, [], cutoff)
        cutoff_text = self._cutoff_parameter(cutoff)
        try:
            connection.execute("BEGIN")
            if not self._table_exists(connection):
                connection.commit()
                return PortfolioRiskReadView(None, [], cutoff)
            self._validate_schema(connection)
            selected_row = connection.execute(
                f"""
                SELECT * FROM {_TABLE}
                WHERE as_of = (
                    SELECT max(as_of) FROM {_TABLE}
                    WHERE as_of <= ?
                      AND julianday(recorded_at) <= julianday(?)
                )
                  AND julianday(recorded_at) <= julianday(?)
                ORDER BY revision DESC
                LIMIT 1
                """,
                [as_of.isoformat(), cutoff_text, cutoff_text],
            ).fetchone()
            snapshot = self._snapshot(selected_row) if selected_row is not None else None
            history_rows = connection.execute(
                f"""
                SELECT snapshots.*
                FROM {_TABLE} AS snapshots
                JOIN (
                    SELECT as_of, max(revision) AS revision
                    FROM {_TABLE}
                    WHERE as_of <= ?
                      AND julianday(recorded_at) <= julianday(?)
                    GROUP BY as_of
                ) AS selected
                  ON selected.as_of = snapshots.as_of
                 AND selected.revision = snapshots.revision
                ORDER BY snapshots.as_of
                """,
                [as_of.isoformat(), cutoff_text],
            ).fetchall()
            history = [self._snapshot(row) for row in history_rows]
            connection.commit()
            return PortfolioRiskReadView(snapshot, history, cutoff)
        except PortfolioLedgerUnavailable:
            connection.rollback()
            raise
        except (sqlite3.Error, OSError, TypeError, ValueError) as exc:
            connection.rollback()
            raise PortfolioLedgerUnavailable("portfolio ledger is unavailable") from exc
        finally:
            connection.close()

    @staticmethod
    def _snapshot(row: sqlite3.Row) -> PortfolioDailySnapshot:
        positions = [
            PortfolioPositionInput(
                symbol=item["symbol"],
                quantity=Decimal(item["quantity"]),
                avg_cost=(Decimal(item["avg_cost"]) if item.get("avg_cost") is not None else None),
            )
            for item in json.loads(row["positions_json"])
        ]
        snapshot = PortfolioDailySnapshot(
            as_of=row["as_of"],
            cash=Decimal(row["cash"]) if row["cash"] is not None else None,
            positions=positions,
            manual_nav=(Decimal(row["manual_nav"]) if row["manual_nav"] is not None else None),
            snapshot_id=row["snapshot_id"],
            revision=int(row["revision"]),
            previous_revision=(int(row["revision"]) - 1 if int(row["revision"]) > 1 else None),
            content_hash=row["content_hash"],
            recorded_at=row["recorded_at"],
            source=row["source"],
        )
        if snapshot.recorded_at.utcoffset() is None:
            raise PortfolioLedgerUnavailable("portfolio ledger recorded_at is timezone-naive")
        snapshot = snapshot.model_copy(update={"recorded_at": snapshot.recorded_at.astimezone(UTC)})
        expected_hash = _content_hash(
            PortfolioDailySnapshotCreate(
                as_of=snapshot.as_of,
                cash=snapshot.cash,
                positions=snapshot.positions,
                manual_nav=snapshot.manual_nav,
            )
        )
        if snapshot.content_hash != expected_hash or snapshot.snapshot_id != _snapshot_id(
            snapshot.as_of,
            snapshot.revision,
            snapshot.content_hash,
        ):
            raise PortfolioLedgerUnavailable("portfolio ledger row failed integrity validation")
        return snapshot
