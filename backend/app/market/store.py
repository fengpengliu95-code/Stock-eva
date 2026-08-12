import json
from contextlib import contextmanager
from pathlib import Path

import duckdb

from backend.app.market.models import DailyBar, RefreshResult

MAX_SYMBOL_RANGE_QUERY = 200
_DUCKDB_SAME_DATABASE_CONFIGURATION_ERROR = (
    "Connection Error: Can't open a connection to same database file with a different "
    "configuration than existing connections"
)


class MarketStoreReadError(RuntimeError):
    """A SELECT-only control database cannot be read safely."""


class MarketStore:
    def __init__(
        self,
        path: Path,
        *,
        temp_directory: Path | None = None,
        read_only: bool = False,
    ) -> None:
        self.path = path
        self.temp_directory = temp_directory or path.parent / ".duckdb-tmp"
        self.read_only = read_only

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self.read_only:
            raise RuntimeError("read-only market store cannot open a writer connection")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.temp_directory.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(self.path))
        connection.execute("SET temp_directory = ?", [str(self.temp_directory)])
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_bars (
                trade_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                security_type VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                board VARCHAR NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                preclose DOUBLE NOT NULL,
                volume DOUBLE NOT NULL,
                amount DOUBLE NOT NULL,
                turnover_rate DOUBLE,
                pct_change DOUBLE,
                adjust_factor DOUBLE,
                price_adjustment VARCHAR NOT NULL,
                is_trading BOOLEAN NOT NULL,
                is_suspended BOOLEAN NOT NULL,
                is_st BOOLEAN NOT NULL,
                source VARCHAR NOT NULL,
                source_record_id VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                quality_status VARCHAR NOT NULL,
                quality_issues JSON NOT NULL,
                PRIMARY KEY (trade_date, symbol, source)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS refresh_runs (
                run_id VARCHAR PRIMARY KEY,
                request_key VARCHAR,
                run_kind VARCHAR NOT NULL DEFAULT 'daily',
                requested_date DATE NOT NULL,
                source VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                requested_count INTEGER NOT NULL,
                succeeded_count INTEGER NOT NULL,
                coverage_ratio DOUBLE,
                failed_symbols JSON NOT NULL,
                quality_issues JSON NOT NULL,
                error_message VARCHAR,
                failure_stage VARCHAR,
                failure_class VARCHAR,
                retryable BOOLEAN,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS published_snapshots (
                singleton INTEGER PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                published_at TIMESTAMPTZ NOT NULL,
                CHECK (singleton = 1)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS published_daily_bars (
                publication_run_id VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                security_type VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                board VARCHAR NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                preclose DOUBLE NOT NULL,
                volume DOUBLE NOT NULL,
                amount DOUBLE NOT NULL,
                turnover_rate DOUBLE,
                pct_change DOUBLE,
                adjust_factor DOUBLE,
                price_adjustment VARCHAR NOT NULL,
                is_trading BOOLEAN NOT NULL,
                is_suspended BOOLEAN NOT NULL,
                is_st BOOLEAN NOT NULL,
                source VARCHAR NOT NULL,
                source_record_id VARCHAR NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                quality_status VARCHAR NOT NULL,
                quality_issues JSON NOT NULL,
                PRIMARY KEY (trade_date, symbol, source)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS market_automation_state (
                singleton INTEGER PRIMARY KEY,
                target_session DATE,
                refresh_state VARCHAR NOT NULL,
                attempt_count INTEGER NOT NULL,
                last_attempt_at TIMESTAMPTZ,
                last_success_at TIMESTAMPTZ,
                next_retry_at TIMESTAMPTZ,
                calendar_status VARCHAR NOT NULL,
                error_code VARCHAR,
                CHECK (singleton = 1)
            )
            """
        )
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS coverage_ratio DOUBLE"
        )
        connection.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS request_key VARCHAR")
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS run_kind VARCHAR DEFAULT 'daily'"
        )
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS quality_issues JSON DEFAULT '[]'"
        )
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS failure_stage VARCHAR"
        )
        connection.execute(
            "ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS failure_class VARCHAR"
        )
        connection.execute("ALTER TABLE refresh_runs ADD COLUMN IF NOT EXISTS retryable BOOLEAN")
        return connection

    def initialize_schema(self) -> None:
        """Explicitly initialize or migrate schema for a writer-owned store."""
        connection = self._connect()
        connection.close()

    def _connect_reader(self) -> duckdb.DuckDBPyConnection | None:
        if not self.path.is_file():
            return None
        try:
            return duckdb.connect(str(self.path), read_only=self.read_only)
        except duckdb.ConnectionException as exc:
            if not self.read_only or str(exc).strip() != _DUCKDB_SAME_DATABASE_CONFIGURATION_ERROR:
                raise
            # DuckDB forbids mixed access-mode handles to one database in the
            # same process. Reopen with the writer's configuration so SELECT-only
            # readers retain MVCC access to the last committed snapshot.
            return duckdb.connect(str(self.path))

    @contextmanager
    def _reader(self):
        connection = None
        try:
            connection = self._connect_reader()
            yield connection
        except MarketStoreReadError:
            raise
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise MarketStoreReadError("market control database cannot be read") from exc
        finally:
            if connection is not None:
                connection.close()

    def exists(self) -> bool:
        return self.path.is_file()

    def save_refresh(
        self,
        bars: list[DailyBar],
        result: RefreshResult,
        *,
        publish: bool | None = None,
    ) -> None:
        publish = result.status == "ready" if publish is None else publish
        if publish:
            self._validate_ready_publication(result)
            self._validate_local_publication_bars(bars, result)
        self._save_refresh(bars, result, publish=publish, capture_published_bars=publish)

    def save_external_publication(self, result: RefreshResult) -> None:
        """Persist a trusted external-dataset pointer without local bar materialization."""
        self._validate_ready_publication(result)
        self._save_refresh([], result, publish=True, capture_published_bars=False)

    @staticmethod
    def _validate_ready_publication(result: RefreshResult) -> None:
        try:
            validated = RefreshResult.model_validate(result.model_dump())
        except ValueError as exc:
            raise ValueError("ready publication must be complete") from exc
        if validated.status != "ready":
            raise ValueError("ready publication must be complete")

    @staticmethod
    def publication_bar_quality_issue(bar: DailyBar) -> str | None:
        """Return why a canonical bar cannot belong to a complete publication."""
        if bar.is_trading:
            if bar.is_suspended:
                return "active_bar_marked_suspended"
            if bar.quality_status != "ready" or bar.quality_issues:
                return "active_bar_not_ready"
            return None
        if not bar.is_suspended:
            return "non_trading_bar_not_suspended"
        if (
            bar.security_type != "stock"
            or bar.volume != 0
            or bar.amount != 0
            or bar.quality_status != "partial"
            or bar.quality_issues != ["suspended_placeholder"]
        ):
            return "invalid_suspended_placeholder"
        return None

    @staticmethod
    def _validate_local_publication_bars(
        bars: list[DailyBar],
        result: RefreshResult,
    ) -> None:
        if not bars:
            raise ValueError("ready local publication requires non-empty bars")
        partitions = {(bar.trade_date, bar.source) for bar in bars}
        expected_partition = {(result.requested_date, result.source)}
        if partitions != expected_partition:
            raise ValueError("ready local publication requires one explicit trade-date partition")
        identities = [(bar.trade_date, bar.symbol, bar.source) for bar in bars]
        if len(set(identities)) != len(identities):
            raise ValueError("ready local publication contains duplicate symbols")
        symbols = {bar.symbol for bar in bars}
        if len(symbols) != result.succeeded_count:
            raise ValueError("ready local publication count does not match refresh audit")
        invalid = [
            (bar.symbol, issue)
            for bar in bars
            if (issue := MarketStore.publication_bar_quality_issue(bar)) is not None
        ]
        if invalid:
            symbol, issue = invalid[0]
            raise ValueError(f"invalid ready publication bar {symbol}: {issue}")

    def _save_refresh(
        self,
        bars: list[DailyBar],
        result: RefreshResult,
        *,
        publish: bool,
        capture_published_bars: bool,
    ) -> None:
        connection = self._connect()
        try:
            connection.begin()
            self._upsert_bars(connection, bars)
            if capture_published_bars:
                self._replace_published_bars(
                    connection,
                    bars,
                    publication_run_id=result.run_id,
                )
            connection.execute("DELETE FROM refresh_runs WHERE run_id = ?", [result.run_id])
            connection.execute(
                """
                INSERT INTO refresh_runs (
                    run_id, request_key, run_kind, requested_date, source, status, requested_count,
                    succeeded_count, coverage_ratio, failed_symbols, quality_issues,
                    error_message, failure_stage, failure_class, retryable,
                    started_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    result.run_id,
                    result.request_key,
                    result.run_kind,
                    result.requested_date,
                    result.source,
                    result.status,
                    result.requested_count,
                    result.succeeded_count,
                    result.coverage_ratio,
                    json.dumps(result.failed_symbols, ensure_ascii=False),
                    json.dumps(result.quality_issues, ensure_ascii=False),
                    result.error_message,
                    result.failure_stage,
                    result.failure_class,
                    result.retryable,
                    result.started_at,
                    result.completed_at,
                ],
            )
            if publish:
                current = connection.execute(
                    """
                    SELECT trade_date FROM published_snapshots
                    WHERE singleton = 1
                    """
                ).fetchone()
                if current is None or current[0] <= result.requested_date:
                    connection.execute("DELETE FROM published_snapshots WHERE singleton = 1")
                    connection.execute(
                        """
                        INSERT INTO published_snapshots (
                            singleton, run_id, trade_date, published_at
                        ) VALUES (1, ?, ?, ?)
                        """,
                        [result.run_id, result.requested_date, result.completed_at],
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _upsert_bars(
        connection: duckdb.DuckDBPyConnection,
        bars: list[DailyBar],
    ) -> None:
        if not bars:
            return
        deduplicated = {(bar.trade_date, bar.symbol, bar.source): bar for bar in bars}
        rows = [
            [
                bar.trade_date,
                bar.symbol,
                bar.security_type,
                bar.exchange,
                bar.board,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.preclose,
                bar.volume,
                bar.amount,
                bar.turnover_rate,
                bar.pct_change,
                bar.adjust_factor,
                bar.price_adjustment,
                bar.is_trading,
                bar.is_suspended,
                bar.is_st,
                bar.source,
                bar.source_record_id,
                bar.ingested_at,
                bar.quality_status,
                json.dumps(bar.quality_issues, ensure_ascii=False),
            ]
            for bar in deduplicated.values()
        ]
        columns = [list(column) for column in zip(*rows, strict=True)]
        connection.execute(
            """
            INSERT OR REPLACE INTO daily_bars
            SELECT
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?)
            """,
            columns,
        )

    @staticmethod
    def _replace_published_bars(
        connection: duckdb.DuckDBPyConnection,
        bars: list[DailyBar],
        *,
        publication_run_id: str,
    ) -> None:
        deduplicated = {(bar.trade_date, bar.symbol, bar.source): bar for bar in bars}
        touched_partitions = sorted({(bar.trade_date, bar.source) for bar in deduplicated.values()})
        for trade_date, source in touched_partitions:
            connection.execute(
                """
                DELETE FROM published_daily_bars
                WHERE trade_date = ? AND source = ?
                """,
                [trade_date, source],
            )
        rows = [
            [
                publication_run_id,
                bar.trade_date,
                bar.symbol,
                bar.security_type,
                bar.exchange,
                bar.board,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.preclose,
                bar.volume,
                bar.amount,
                bar.turnover_rate,
                bar.pct_change,
                bar.adjust_factor,
                bar.price_adjustment,
                bar.is_trading,
                bar.is_suspended,
                bar.is_st,
                bar.source,
                bar.source_record_id,
                bar.ingested_at,
                bar.quality_status,
                json.dumps(bar.quality_issues, ensure_ascii=False),
            ]
            for bar in deduplicated.values()
        ]
        columns = [list(column) for column in zip(*rows, strict=True)]
        connection.execute(
            """
            INSERT INTO published_daily_bars
            SELECT
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?), unnest(?), unnest(?), unnest(?), unnest(?), unnest(?),
                unnest(?)
            """,
            columns,
        )

    def upsert_bars(self, bars: list[DailyBar]) -> None:
        connection = self._connect()
        try:
            connection.begin()
            self._upsert_bars(connection, bars)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def present_points(
        self,
        trading_dates: list,
        symbols: list[str],
        source: str = "baostock",
    ) -> set[tuple]:
        if not trading_dates or not symbols or not self.exists():
            return set()
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol FROM daily_bars
                WHERE trade_date = ANY(?)
                  AND symbol = ANY(?)
                  AND source = ?
                """,
                [list(dict.fromkeys(trading_dates)), sorted(set(symbols)), source],
            ).fetchall()
        return set(rows)

    def count_bars(self) -> int:
        if not self.exists():
            return 0
        with self._reader() as connection:
            assert connection is not None
            return connection.execute("SELECT count(*) FROM daily_bars").fetchone()[0]

    def latest_refresh(self) -> RefreshResult | None:
        if not self.exists():
            return None
        with self._reader() as connection:
            assert connection is not None
            row = connection.execute(
                """
                SELECT run_id, requested_date, source, status, requested_count,
                       succeeded_count, coverage_ratio, failed_symbols, quality_issues,
                       error_message, started_at, completed_at, request_key, run_kind,
                       failure_stage, failure_class, retryable
                FROM refresh_runs
                ORDER BY completed_at DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._refresh_result_from_row(row)

    @staticmethod
    def _refresh_result_from_row(row) -> RefreshResult:
        return RefreshResult(
            run_id=row[0],
            requested_date=row[1],
            source=row[2],
            status=row[3],
            requested_count=row[4],
            succeeded_count=row[5],
            coverage_ratio=row[6],
            failed_symbols=json.loads(row[7]),
            quality_issues=json.loads(row[8]),
            error_message=row[9],
            started_at=row[10],
            completed_at=row[11],
            request_key=row[12],
            run_kind=row[13],
            # Compatibility for read-only consumers that still project the
            # pre-R2-F0 fourteen-column refresh audit shape.
            failure_stage=row[14] if len(row) > 14 else None,
            failure_class=row[15] if len(row) > 15 else None,
            retryable=row[16] if len(row) > 16 else None,
        )

    def published_refresh(self) -> RefreshResult | None:
        if not self.exists():
            return None
        with self._reader() as connection:
            assert connection is not None
            row = connection.execute(
                """
                SELECT r.run_id, r.requested_date, r.source, r.status,
                       r.requested_count, r.succeeded_count, r.coverage_ratio,
                       r.failed_symbols, r.quality_issues, r.error_message,
                       r.started_at, r.completed_at, r.request_key, r.run_kind,
                       r.failure_stage, r.failure_class, r.retryable
                FROM published_snapshots p
                JOIN refresh_runs r ON r.run_id = p.run_id
                WHERE p.singleton = 1
                """
            ).fetchone()
        if row is None:
            return None
        return self._refresh_result_from_row(row)

    def save_scheduler_state(self, state) -> None:
        connection = self._connect()
        try:
            connection.execute("DELETE FROM market_automation_state WHERE singleton = 1")
            connection.execute(
                """
                INSERT INTO market_automation_state (
                    singleton, target_session, refresh_state, attempt_count,
                    last_attempt_at, last_success_at, next_retry_at,
                    calendar_status, error_code
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    state.target_session,
                    state.refresh_state,
                    state.attempt_count,
                    state.last_attempt_at,
                    state.last_success_at,
                    state.next_retry_at,
                    state.calendar_status,
                    state.error_code,
                ],
            )
        finally:
            connection.close()

    def scheduler_state(self):
        if not self.exists():
            return None
        with self._reader() as connection:
            assert connection is not None
            row = connection.execute(
                """
                SELECT target_session, refresh_state, attempt_count,
                       last_attempt_at, last_success_at, next_retry_at,
                       calendar_status, error_code
                FROM market_automation_state
                WHERE singleton = 1
                """
            ).fetchone()
        if row is None:
            return None
        from backend.app.market.automation import SchedulerState

        return SchedulerState(
            target_session=row[0],
            refresh_state=row[1],
            attempt_count=row[2],
            last_attempt_at=row[3],
            last_success_at=row[4],
            next_retry_at=row[5],
            calendar_status=row[6],
            error_code=row[7],
        )

    def list_refreshes(self, limit: int = 100) -> list[RefreshResult]:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT run_id, requested_date, source, status, requested_count,
                       succeeded_count, coverage_ratio, failed_symbols, quality_issues,
                       error_message, started_at, completed_at, request_key, run_kind,
                       failure_stage, failure_class, retryable
                FROM refresh_runs
                ORDER BY completed_at DESC, run_id
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        return [self._refresh_result_from_row(row) for row in rows]

    def bars_for(self, trade_date, source: str) -> list[dict[str, object]]:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            cursor = connection.execute(
                """
                SELECT symbol, security_type, close, pct_change, amount, is_suspended,
                       quality_status, quality_issues
                FROM daily_bars
                WHERE trade_date = ? AND source = ?
                ORDER BY symbol
                """,
                [trade_date, source],
            )
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def available_dates(self, source: str = "baostock") -> list:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            return [
                row[0]
                for row in connection.execute(
                    """
                    SELECT DISTINCT trade_date
                    FROM daily_bars
                    WHERE source = ?
                    ORDER BY trade_date
                    """,
                    [source],
                ).fetchall()
            ]

    def canonical_bars(self, trade_date, source: str = "baostock") -> list[DailyBar]:
        if not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                       close, preclose, volume, amount, turnover_rate, pct_change,
                       adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                       source, source_record_id, ingested_at, quality_status, quality_issues
                FROM daily_bars
                WHERE trade_date = ? AND source = ?
                ORDER BY symbol
                """,
                [trade_date, source],
            ).fetchall()
        return [self._daily_bar_from_row(row) for row in rows]

    def symbol_bars(self, symbol, start, end, source: str = "baostock") -> list[DailyBar]:
        if start > end or not self.exists():
            return []
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                       close, preclose, volume, amount, turnover_rate, pct_change,
                       adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                       source, source_record_id, ingested_at, quality_status, quality_issues
                FROM daily_bars
                WHERE symbol = ?
                  AND trade_date BETWEEN ? AND ?
                  AND source = ?
                ORDER BY trade_date
                """,
                [symbol, start, end, source],
            ).fetchall()
        return [self._daily_bar_from_row(row) for row in rows]

    def symbols_bars(
        self,
        symbols: list[str],
        start,
        end,
        source: str = "baostock",
    ) -> dict[str, list[DailyBar]]:
        normalized = sorted(set(symbols))
        if len(normalized) > MAX_SYMBOL_RANGE_QUERY:
            raise ValueError(
                f"a symbol range query accepts at most {MAX_SYMBOL_RANGE_QUERY} symbols"
            )
        grouped: dict[str, list[DailyBar]] = {symbol: [] for symbol in normalized}
        if not normalized or start > end or not self.exists():
            return grouped
        with self._reader() as connection:
            assert connection is not None
            rows = connection.execute(
                """
                SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                       close, preclose, volume, amount, turnover_rate, pct_change,
                       adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                       source, source_record_id, ingested_at, quality_status, quality_issues
                FROM daily_bars
                WHERE symbol = ANY(?)
                  AND trade_date BETWEEN ? AND ?
                  AND source = ?
                ORDER BY symbol, trade_date
                """,
                [normalized, start, end, source],
            ).fetchall()
        for row in rows:
            grouped[row[1]].append(self._daily_bar_from_row(row))
        return grouped

    @staticmethod
    def _daily_bar_from_row(row) -> DailyBar:
        return DailyBar(
            trade_date=row[0],
            symbol=row[1],
            security_type=row[2],
            exchange=row[3],
            board=row[4],
            open=row[5],
            high=row[6],
            low=row[7],
            close=row[8],
            preclose=row[9],
            volume=row[10],
            amount=row[11],
            turnover_rate=row[12],
            pct_change=row[13],
            adjust_factor=row[14],
            price_adjustment=row[15],
            is_trading=row[16],
            is_suspended=row[17],
            is_st=row[18],
            source=row[19],
            source_record_id=row[20],
            ingested_at=row[21],
            quality_status=row[22],
            quality_issues=json.loads(row[23]),
        )

    def export_date(self, trade_date, output_root: Path, source: str = "baostock") -> Path:
        output = output_root / f"date={trade_date.isoformat()}" / "bars.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        escaped_output = str(output).replace("'", "''")
        connection = self._connect()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT * FROM daily_bars
                    WHERE trade_date = ? AND source = ?
                    ORDER BY symbol
                ) TO '{escaped_output}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [trade_date, source],
            )
        finally:
            connection.close()
        return output
