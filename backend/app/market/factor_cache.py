"""Persistent, point-in-time-safe BaoStock adjustment-factor cache."""

import hashlib
import math
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from datetime import UTC, date, datetime
from pathlib import Path


class FactorCacheError(RuntimeError):
    pass


class AdjustmentFactorCache:
    """Cache exact daily factor snapshots and authoritative daily events.

    Bootstrap snapshots are keyed by their target date.  Consequently a cache
    populated for a newer date is never reused for an older refresh.  Once a
    baseline exists, a complete BaoStock daily corporate-action observation can
    advance it without repeating one request per security. Historical dates
    outside the forward stream require their own completed daily observation.
    """

    FACTOR_FIELDS = (
        "code",
        "dividOperateDate",
        "foreAdjustFactor",
        "backAdjustFactor",
        "adjustFactor",
    )

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = str(path) if path is not None else ":memory:"
        self._memory_connection: sqlite3.Connection | None = None
        if self.path == ":memory:":
            self._memory_connection = sqlite3.connect(":memory:")
        else:
            cache_path = Path(self.path)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        if self._memory_connection is not None:
            return self._memory_connection
        return sqlite3.connect(self.path, timeout=30)

    def _close(self, connection: sqlite3.Connection) -> None:
        if connection is not self._memory_connection:
            connection.close()

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS factor_snapshots (
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    fore_adjust_factor REAL NOT NULL,
                    back_adjust_factor REAL,
                    evidence_kind TEXT NOT NULL,
                    evidence_effective_date TEXT NOT NULL,
                    evidence_observed_on TEXT,
                    source_row_hash TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, trade_date)
                );
                CREATE TABLE IF NOT EXISTS factor_events (
                    symbol TEXT NOT NULL,
                    effective_date TEXT NOT NULL,
                    observed_on TEXT NOT NULL,
                    fore_adjust_factor REAL NOT NULL,
                    back_adjust_factor REAL,
                    source_row_hash TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, effective_date, observed_on)
                );
                CREATE TABLE IF NOT EXISTS event_stream_state (
                    source TEXT PRIMARY KEY,
                    start_date TEXT NOT NULL,
                    through_date TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS daily_event_observations (
                    trade_date TEXT PRIMARY KEY,
                    observed_at TEXT NOT NULL
                );
                """
            )
            snapshot_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(factor_snapshots)").fetchall()
            }
            if "evidence_observed_on" not in snapshot_columns:
                connection.execute(
                    "ALTER TABLE factor_snapshots ADD COLUMN evidence_observed_on TEXT"
                )
            if "back_adjust_factor" not in snapshot_columns:
                connection.execute(
                    "ALTER TABLE factor_snapshots ADD COLUMN back_adjust_factor REAL"
                )
            event_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(factor_events)").fetchall()
            }
            if "back_adjust_factor" not in event_columns:
                connection.execute("ALTER TABLE factor_events ADD COLUMN back_adjust_factor REAL")
            connection.execute(
                """
                UPDATE factor_snapshots AS snapshots
                SET evidence_observed_on = COALESCE(
                    (
                        SELECT MAX(events.observed_on)
                        FROM factor_events AS events
                        WHERE events.symbol = snapshots.symbol
                          AND events.effective_date =
                              snapshots.evidence_effective_date
                          AND events.source_row_hash =
                              snapshots.source_row_hash
                    ),
                    snapshots.trade_date
                )
                WHERE evidence_observed_on IS NULL
                """
            )
            connection.commit()
        finally:
            self._close(connection)

    @staticmethod
    def _row_hash(row: Sequence[str]) -> str:
        payload = "\x1f".join(row).encode()
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _parse_factor_rows(
        fields: Sequence[str],
        rows: Sequence[Sequence[str]],
        *,
        through_date: date,
    ) -> list[tuple[str, date, float, float, str]]:
        required = {
            "code",
            "dividOperateDate",
            "foreAdjustFactor",
            "backAdjustFactor",
        }
        if not required.issubset(fields):
            raise FactorCacheError("BaoStock factor response has unsupported fields")
        positions = {field: fields.index(field) for field in required}
        parsed: list[tuple[str, date, float, float, str]] = []
        for row in rows:
            if len(row) != len(fields):
                raise FactorCacheError("BaoStock factor field and row lengths differ")
            symbol = row[positions["code"]].lower()
            effective_date = date.fromisoformat(row[positions["dividOperateDate"]])
            if effective_date > through_date:
                raise FactorCacheError("BaoStock factor response contains future data")
            factor = float(row[positions["foreAdjustFactor"]])
            back_factor = float(row[positions["backAdjustFactor"]])
            if (
                not math.isfinite(factor)
                or factor <= 0
                or not math.isfinite(back_factor)
                or back_factor <= 0
            ):
                raise FactorCacheError("BaoStock factor must be finite and positive")
            parsed.append(
                (
                    symbol,
                    effective_date,
                    factor,
                    back_factor,
                    AdjustmentFactorCache._row_hash(row),
                )
            )
        return parsed

    def exact_snapshots(
        self,
        symbols: Sequence[str],
        trade_date: date,
    ) -> dict[str, float]:
        unique = list(dict.fromkeys(symbol.lower() for symbol in symbols))
        if not unique:
            return {}
        placeholders = ",".join("?" for _ in unique)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"""
                SELECT symbol, back_adjust_factor
                FROM factor_snapshots
                WHERE trade_date = ? AND symbol IN ({placeholders})
                  AND back_adjust_factor IS NOT NULL
                """,
                [trade_date.isoformat(), *unique],
            ).fetchall()
        finally:
            self._close(connection)
        return {str(symbol): float(factor) for symbol, factor in rows}

    @staticmethod
    def snapshot_row_fingerprint(row: Sequence[object]) -> str:
        """Return a stable digest for the exact snapshot projection.

        The helper is deliberately independent of the cache writer.  It hashes
        only the nine columns exposed by ``exact_snapshot_records`` and uses a
        canonical JSON representation so an equivalent SQLite value has the
        same identity across processes.
        """
        import json

        if len(row) != 9:
            raise FactorCacheError("factor snapshot row shape is invalid")
        payload = json.dumps(
            list(row), ensure_ascii=False, sort_keys=False, separators=(",", ":"), default=str
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def exact_snapshot_records(
        self,
        symbols: Sequence[str],
        trade_date: date,
    ) -> tuple[dict[str, object], ...]:
        """Read exact factor snapshot rows without initializing or mutating the cache.

        This is intentionally a separate API from ``exact_snapshots``: callers
        receive the complete current table projection, including provenance,
        and can fingerprint it before and after an online capture.  The method
        performs one SELECT and never invokes any cache writer or schema setup.
        """
        unique = tuple(sorted(set(str(symbol).lower() for symbol in symbols)))
        if not unique:
            return ()
        placeholders = ",".join("?" for _ in unique)
        connection = self._connect()
        try:
            rows = connection.execute(
                f"""
                SELECT symbol, trade_date, fore_adjust_factor, back_adjust_factor,
                       evidence_kind, evidence_effective_date, evidence_observed_on,
                       source_row_hash, observed_at
                FROM factor_snapshots
                WHERE trade_date = ? AND symbol IN ({placeholders})
                ORDER BY symbol, trade_date
                """,
                [trade_date.isoformat(), *unique],
            ).fetchall()
        except sqlite3.Error as exc:
            raise FactorCacheError("factor snapshot read failed") from exc
        finally:
            self._close(connection)
        result: list[dict[str, object]] = []
        for row in rows:
            values = tuple(row)
            result.append(
                {
                    "symbol": str(values[0]),
                    "trade_date": str(values[1]),
                    "fore_adjust_factor": float(values[2]),
                    "back_adjust_factor": (None if values[3] is None else float(values[3])),
                    "evidence_kind": str(values[4]),
                    "evidence_effective_date": str(values[5]),
                    "evidence_observed_on": (None if values[6] is None else str(values[6])),
                    "source_row_hash": str(values[7]),
                    "observed_at": str(values[8]),
                    "row_fingerprint": self.snapshot_row_fingerprint(values),
                }
            )
        return tuple(result)

    def record_bootstrap(
        self,
        symbol: str,
        trade_date: date,
        fields: Sequence[str],
        rows: Sequence[Sequence[str]],
        *,
        allow_empty_history: bool = False,
    ) -> float | None:
        """Persist one completed per-symbol query immediately for resume."""
        symbol = symbol.lower()
        parsed = [
            item
            for item in self._parse_factor_rows(
                fields,
                rows,
                through_date=trade_date,
            )
            if item[0] == symbol
        ]
        if not parsed:
            if not allow_empty_history:
                return None
            now = datetime.now(UTC).isoformat()
            response_hash = hashlib.sha256(
                (
                    "\x1f".join(fields)
                    + "\x1e"
                    + symbol
                    + "\x1e"
                    + trade_date.isoformat()
                    + "\x1eEMPTY"
                ).encode()
            ).hexdigest()
            connection = self._connect()
            try:
                with connection:
                    connection.execute(
                        """
                        INSERT OR REPLACE INTO factor_snapshots (
                            symbol, trade_date, fore_adjust_factor,
                            back_adjust_factor, evidence_kind,
                            evidence_effective_date, evidence_observed_on,
                            source_row_hash, observed_at
                        ) VALUES (?, ?, 1, 1, 'bootstrap_no_events', ?, ?, ?, ?)
                        """,
                        (
                            symbol,
                            trade_date.isoformat(),
                            trade_date.isoformat(),
                            trade_date.isoformat(),
                            response_hash,
                            now,
                        ),
                    )
            finally:
                self._close(connection)
            return 1.0
        latest = max(parsed, key=lambda item: item[1])
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            with connection:
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO factor_events (
                        symbol, effective_date, observed_on, fore_adjust_factor,
                        back_adjust_factor, source_row_hash, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            item_symbol,
                            effective.isoformat(),
                            trade_date.isoformat(),
                            fore_factor,
                            back_factor,
                            row_hash,
                            now,
                        )
                        for (
                            item_symbol,
                            effective,
                            fore_factor,
                            back_factor,
                            row_hash,
                        ) in parsed
                    ],
                )
                connection.execute(
                    """
                    INSERT OR REPLACE INTO factor_snapshots (
                        symbol, trade_date, fore_adjust_factor, evidence_kind,
                        back_adjust_factor,
                        evidence_effective_date, evidence_observed_on,
                        source_row_hash, observed_at
                    ) VALUES (?, ?, ?, 'bootstrap', ?, ?, ?, ?, ?)
                    """,
                    (
                        symbol,
                        trade_date.isoformat(),
                        latest[2],
                        latest[3],
                        latest[1].isoformat(),
                        trade_date.isoformat(),
                        latest[4],
                        now,
                    ),
                )
        finally:
            self._close(connection)
        return latest[3]

    def stream_through(self) -> date | None:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT through_date
                FROM event_stream_state
                WHERE source = 'baostock'
                """
            ).fetchone()
        finally:
            self._close(connection)
        return date.fromisoformat(row[0]) if row is not None else None

    def record_daily_events(
        self,
        trade_date: date,
        fields: Sequence[str],
        rows: Sequence[Sequence[str]],
        *,
        advance_stream: bool,
    ) -> None:
        """Atomically record a complete daily event response and its watermark."""
        parsed = self._parse_factor_rows(fields, rows, through_date=trade_date)
        if any(effective != trade_date for _, effective, _, _, _ in parsed):
            raise FactorCacheError("daily factor response contains another effective date")
        now = datetime.now(UTC).isoformat()
        connection = self._connect()
        try:
            with connection:
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO factor_events (
                        symbol, effective_date, observed_on, fore_adjust_factor,
                        back_adjust_factor, source_row_hash, observed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            symbol,
                            effective.isoformat(),
                            trade_date.isoformat(),
                            fore_factor,
                            back_factor,
                            row_hash,
                            now,
                        )
                        for symbol, effective, fore_factor, back_factor, row_hash in parsed
                    ],
                )
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO factor_snapshots (
                        symbol, trade_date, fore_adjust_factor, evidence_kind,
                        back_adjust_factor,
                        evidence_effective_date, evidence_observed_on,
                        source_row_hash, observed_at
                    ) VALUES (?, ?, ?, 'daily_event', ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            symbol,
                            trade_date.isoformat(),
                            fore_factor,
                            back_factor,
                            effective.isoformat(),
                            trade_date.isoformat(),
                            row_hash,
                            now,
                        )
                        for symbol, effective, fore_factor, back_factor, row_hash in parsed
                    ],
                )
                connection.execute(
                    """
                    INSERT OR REPLACE INTO daily_event_observations (
                        trade_date, observed_at
                    ) VALUES (?, ?)
                    """,
                    (trade_date.isoformat(), now),
                )
                if advance_stream:
                    current = connection.execute(
                        """
                        SELECT start_date, through_date
                        FROM event_stream_state
                        WHERE source = 'baostock'
                        """
                    ).fetchone()
                    start = date.fromisoformat(current[0]) if current else trade_date
                    through = (
                        max(date.fromisoformat(current[1]), trade_date) if current else trade_date
                    )
                    connection.execute(
                        """
                        INSERT OR REPLACE INTO event_stream_state (
                            source, start_date, through_date, observed_at
                        ) VALUES ('baostock', ?, ?, ?)
                        """,
                        (start.isoformat(), through.isoformat(), now),
                    )
        finally:
            self._close(connection)

    def materialize_from_stream(
        self,
        symbols: Sequence[str],
        trade_date: date,
    ) -> dict[str, float]:
        """Materialize from a prior baseline plus point-in-time event evidence."""
        unique = list(dict.fromkeys(symbol.lower() for symbol in symbols))
        if not unique:
            return {}
        connection = self._connect()
        try:
            state = connection.execute(
                """
                SELECT start_date, through_date
                FROM event_stream_state
                WHERE source = 'baostock'
                """
            ).fetchone()
            in_stream = False
            if state is not None:
                stream_start = date.fromisoformat(state[0])
                stream_through = date.fromisoformat(state[1])
                in_stream = stream_start <= trade_date <= stream_through
            observed = connection.execute(
                """
                SELECT 1 FROM daily_event_observations
                WHERE trade_date = ?
                """,
                (trade_date.isoformat(),),
            ).fetchone()
            if not in_stream and observed is None:
                return {}
            now = datetime.now(UTC).isoformat()
            resolved: dict[str, float] = {}
            with connection:
                for symbol in unique:
                    exact = connection.execute(
                        """
                        SELECT back_adjust_factor
                        FROM factor_snapshots
                        WHERE symbol = ? AND trade_date = ?
                          AND back_adjust_factor IS NOT NULL
                        """,
                        (symbol, trade_date.isoformat()),
                    ).fetchone()
                    if exact is not None:
                        resolved[symbol] = float(exact[0])
                        continue
                    baseline = connection.execute(
                        """
                        SELECT trade_date, fore_adjust_factor, back_adjust_factor,
                               evidence_effective_date, evidence_observed_on,
                               source_row_hash
                        FROM factor_snapshots
                        WHERE symbol = ? AND trade_date < ?
                          AND back_adjust_factor IS NOT NULL
                        ORDER BY trade_date DESC
                        LIMIT 1
                        """,
                        (
                            symbol,
                            trade_date.isoformat(),
                        ),
                    ).fetchone()
                    if baseline is None:
                        continue
                    event = connection.execute(
                        """
                        SELECT effective_date, fore_adjust_factor, back_adjust_factor,
                               observed_on, source_row_hash
                        FROM factor_events
                        WHERE symbol = ?
                          AND effective_date > ?
                          AND effective_date <= ?
                          AND observed_on <= ?
                          AND back_adjust_factor IS NOT NULL
                        ORDER BY effective_date DESC, observed_on DESC
                        LIMIT 1
                        """,
                        (
                            symbol,
                            baseline[0],
                            trade_date.isoformat(),
                            trade_date.isoformat(),
                        ),
                    ).fetchone()
                    if event is None:
                        fore_factor = float(baseline[1])
                        back_factor = float(baseline[2])
                        effective = str(baseline[3])
                        observed_on = str(baseline[4])
                        row_hash = str(baseline[5])
                        kind = "daily_carry"
                    else:
                        effective = str(event[0])
                        fore_factor = float(event[1])
                        back_factor = float(event[2])
                        observed_on = str(event[3])
                        row_hash = str(event[4])
                        kind = "daily_event"
                    connection.execute(
                        """
                        INSERT INTO factor_snapshots (
                            symbol, trade_date, fore_adjust_factor, evidence_kind,
                            back_adjust_factor,
                            evidence_effective_date, evidence_observed_on,
                            source_row_hash, observed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            symbol,
                            trade_date.isoformat(),
                            fore_factor,
                            kind,
                            back_factor,
                            effective,
                            observed_on,
                            row_hash,
                            now,
                        ),
                    )
                    resolved[symbol] = back_factor
            return resolved
        finally:
            self._close(connection)

    def normalized_rows(
        self,
        symbols: Sequence[str],
        trade_date: date,
    ) -> tuple[list[str], list[list[str]]]:
        snapshots = self.exact_snapshots(symbols, trade_date)
        return list(self.FACTOR_FIELDS), [
            [
                symbol,
                trade_date.isoformat(),
                format(factor, ".17g"),
                format(factor, ".17g"),
                "",
            ]
            for symbol, factor in sorted(snapshots.items())
        ]

    def close(self) -> None:
        if self._memory_connection is not None:
            with closing(self._memory_connection):
                pass
            self._memory_connection = None
