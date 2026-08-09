import hashlib
import json
import math
import threading
from collections import OrderedDict, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from statistics import fmean, pstdev
from typing import Protocol

import duckdb

from backend.app.config import Settings
from backend.app.market.models import DailyBar
from backend.app.market.store import MarketStore
from backend.app.regime.models import (
    EXPECTED_BOARDS,
    EXPECTED_INDEX_SERIES,
    ActualMarketScope,
    EvidenceItem,
    MarketRegimeInput,
    RegimeComponentInput,
    SourceLineage,
)
from backend.app.storage.dataset import DatasetError, NasMarketStore, PublishedReadSnapshot
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import configured_market_dataset_root

REGIME_LOOKBACK_SESSIONS = 130
CANONICAL_SOURCE_VERSION = "canonical-market-v2"

_BAR_COLUMNS = """
    trade_date, symbol, security_type, exchange, board, open, high, low,
    close, preclose, volume, amount, turnover_rate, pct_change,
    adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
    source, source_record_id, ingested_at, quality_status, quality_issues
"""

# R1-B never consumes intraday OHLC, volume, turnover or price-adjustment
# metadata. Keep immutable Parquet reads to the fields used by its formulas,
# scope and PIT checks; published objects have already been schema/checksum
# validated before this trusted projection is constructed.
_REGIME_BAR_COLUMNS = """
    trade_date, symbol, security_type, exchange, board, close, preclose,
    amount, pct_change, adjust_factor, is_trading, is_suspended, is_st,
    source_record_id, ingested_at, quality_status, quality_issues
"""

_REFRESH_COLUMNS = """
    run_id, requested_date, source, status, requested_count,
    succeeded_count, coverage_ratio, failed_symbols, quality_issues,
    error_message, started_at, completed_at, request_key, run_kind
"""


class MarketBarsReader(Protocol):
    def bars_through(
        self,
        as_of: date,
        *,
        max_sessions: int,
    ) -> list[DailyBar]: ...


class MarketReadUnavailable(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ReadOnlyDuckDbMarketReader:
    """SELECT-only local reader; missing storage remains missing."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def bars_through(
        self,
        as_of: date,
        *,
        max_sessions: int,
    ) -> list[DailyBar]:
        if not self.path.is_file():
            return []
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = MarketStore(self.path, read_only=True)._connect_reader()
            assert connection is not None
            rows = connection.execute(
                f"""
                SELECT {_BAR_COLUMNS}
                FROM daily_bars
                WHERE source = 'baostock'
                  AND trade_date IN (
                      SELECT DISTINCT trade_date
                      FROM daily_bars
                      WHERE source = 'baostock' AND trade_date <= ?
                      ORDER BY trade_date DESC
                      LIMIT ?
                  )
                  AND trade_date <= ?
                ORDER BY trade_date, symbol
                """,
                [as_of, max_sessions, as_of],
            ).fetchall()
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise MarketReadUnavailable("market_storage_unavailable") from exc
        finally:
            if connection is not None:
                connection.close()
        return [MarketStore._daily_bar_from_row(row) for row in rows]


class PublishedSnapshotDuckDbMarketReader:
    """Read only writer-captured rows attached to an explicit published pointer."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def bars_through(
        self,
        as_of: date,
        *,
        max_sessions: int,
    ) -> list[DailyBar]:
        if not self.path.is_file():
            return []
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = MarketStore(self.path, read_only=True)._connect_reader()
            assert connection is not None
            tables = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                    """
                ).fetchall()
            }
            if not {"published_snapshots", "published_daily_bars", "refresh_runs"} <= tables:
                raise MarketReadUnavailable("market_publication_state_unavailable")
            pointer = connection.execute(
                """
                SELECT run_id, trade_date
                FROM published_snapshots
                WHERE singleton = 1
                """
            ).fetchone()
            if pointer is None:
                raise MarketReadUnavailable("market_publication_state_unavailable")
            pointer_run_id, pointer_date = pointer

            inventory = connection.execute(
                """
                SELECT
                    trade_date,
                    count(DISTINCT source) AS source_count,
                    min(source) AS source,
                    count(DISTINCT publication_run_id) AS run_count,
                    min(publication_run_id) AS publication_run_id,
                    count(*) AS row_count,
                    count(DISTINCT symbol) AS symbol_count
                FROM published_daily_bars
                GROUP BY trade_date
                ORDER BY trade_date
                """
            ).fetchall()
            if not inventory:
                raise MarketReadUnavailable("market_publication_state_unavailable")
            run_ids = sorted({row[4] for row in inventory})
            audit_rows = connection.execute(
                f"""
                SELECT {_REFRESH_COLUMNS}
                FROM refresh_runs
                WHERE run_id = ANY(?)
                """,
                [run_ids],
            ).fetchall()
            audits = {}
            try:
                for row in audit_rows:
                    audit = MarketStore._refresh_result_from_row(row)
                    MarketStore._validate_ready_publication(audit)
                    audits[audit.run_id] = audit
            except (TypeError, ValueError) as exc:
                raise MarketReadUnavailable("market_publication_state_unavailable") from exc
            if set(audits) != set(run_ids):
                raise MarketReadUnavailable("market_publication_state_unavailable")
            for (
                trade_date,
                source_count,
                source,
                run_count,
                run_id,
                row_count,
                symbol_count,
            ) in inventory:
                if (
                    source_count != 1
                    or source != "baostock"
                    or run_count != 1
                    or row_count != symbol_count
                ):
                    raise MarketReadUnavailable("market_publication_state_unavailable")
                audit = audits[run_id]
                if (
                    audit.requested_date != trade_date
                    or audit.source != source
                    or row_count != audit.succeeded_count
                ):
                    raise MarketReadUnavailable("market_publication_state_unavailable")

            latest_date, _, _, _, latest_run_id, _, _ = inventory[-1]
            if pointer_date != latest_date or pointer_run_id != latest_run_id:
                raise MarketReadUnavailable("market_publication_state_unavailable")

            published_as_of = min(as_of, pointer_date)
            published_count = connection.execute(
                """
                SELECT count(*)
                FROM published_daily_bars
                WHERE source = 'baostock' AND trade_date <= ?
                """,
                [published_as_of],
            ).fetchone()[0]
            if not published_count:
                raise MarketReadUnavailable("market_publication_state_unavailable")
            rows = connection.execute(
                f"""
                SELECT publication_run_id, {_BAR_COLUMNS}
                FROM published_daily_bars
                WHERE source = 'baostock'
                  AND trade_date IN (
                      SELECT DISTINCT trade_date
                      FROM published_daily_bars
                      WHERE source = 'baostock'
                        AND trade_date <= ?
                        AND security_type = 'stock'
                        AND board = 'main'
                        AND quality_status = 'ready'
                        AND is_trading = TRUE
                        AND is_suspended = FALSE
                        AND close > 0
                        AND preclose > 0
                        AND adjust_factor > 0
                        AND amount >= 0
                      ORDER BY trade_date DESC
                      LIMIT ?
                  )
                  AND trade_date <= ?
                ORDER BY trade_date, symbol
                """,
                [published_as_of, max_sessions, published_as_of],
            ).fetchall()
            partitions: dict[tuple[date, str], list[tuple[str, str]]] = defaultdict(list)
            for row in rows:
                partitions[(row[1], row[20])].append((row[0], row[2]))
            for (trade_date, source), partition_rows in partitions.items():
                run_ids = {row[0] for row in partition_rows}
                symbols = {row[1] for row in partition_rows}
                if len(run_ids) != 1:
                    raise MarketReadUnavailable("market_publication_state_unavailable")
                run_id = next(iter(run_ids))
                audit = audits.get(run_id)
                if (
                    audit is None
                    or audit.requested_date != trade_date
                    or audit.source != source
                    or len(symbols) != audit.succeeded_count
                    or len(partition_rows) != audit.succeeded_count
                ):
                    raise MarketReadUnavailable("market_publication_state_unavailable")
            bars = [MarketStore._daily_bar_from_row(row[1:]) for row in rows]
        except MarketReadUnavailable:
            raise
        except (duckdb.Error, OSError, TypeError, ValueError) as exc:
            raise MarketReadUnavailable("market_storage_unavailable") from exc
        finally:
            if connection is not None:
                connection.close()
        return bars


class PublishedDatasetMarketReader:
    """Manifest-only Parquet reader with an SQL as-of predicate."""

    def __init__(
        self,
        *,
        control_path: Path,
        control_temp: Path,
        dataset_root: Path,
        staging_root: Path,
    ) -> None:
        self.dataset_root = dataset_root
        self.store = NasMarketStore(
            MarketStore(
                control_path,
                temp_directory=control_temp,
                read_only=True,
            ),
            dataset_root,
            staging_root,
        )

    def bars_through(
        self,
        as_of: date,
        *,
        max_sessions: int,
    ) -> list[DailyBar]:
        return self.bars_through_snapshot(self.cache_snapshot(), as_of, max_sessions=max_sessions)

    def cache_snapshot(self) -> PublishedReadSnapshot:
        """Capture one checked immutable view for cache identity and query binding."""
        try:
            return self.store.read_snapshot()
        except DatasetError as exc:
            raise MarketReadUnavailable("market_storage_unavailable") from exc

    def bars_through_snapshot(
        self,
        snapshot: PublishedReadSnapshot,
        as_of: date,
        *,
        max_sessions: int,
    ) -> list[DailyBar]:
        try:
            rows, _ = self.store._query_snapshot(
                snapshot,
                f"""
                SELECT {_REGIME_BAR_COLUMNS}
                FROM daily_bars
                WHERE source = 'baostock'
                  AND trade_date IN (
                      SELECT DISTINCT trade_date
                      FROM daily_bars
                      WHERE source = 'baostock' AND trade_date <= ?
                      ORDER BY trade_date DESC
                      LIMIT ?
                  )
                  AND trade_date <= ?
                ORDER BY trade_date, symbol
                """,
                [as_of, max_sessions, as_of],
            )
        except DatasetError as exc:
            raise MarketReadUnavailable("market_storage_unavailable") from exc
        return [self._trusted_regime_bar_from_row(row) for row in rows]

    @staticmethod
    def _trusted_regime_bar_from_row(row) -> DailyBar:
        """Avoid repeat model validation for checksum-verified immutable rows."""
        return DailyBar.model_construct(
            trade_date=row[0],
            symbol=row[1],
            security_type=row[2],
            exchange=row[3],
            board=row[4],
            close=row[5],
            preclose=row[6],
            amount=row[7],
            pct_change=row[8],
            adjust_factor=row[9],
            is_trading=row[10],
            is_suspended=row[11],
            is_st=row[12],
            source_record_id=row[13],
            ingested_at=row[14],
            quality_status=row[15],
            quality_issues=json.loads(row[16]),
            # These fields are intentionally outside the R1-B projection.
            open=0.0,
            high=0.0,
            low=0.0,
            volume=0.0,
            turnover_rate=None,
            price_adjustment="none",
            source="baostock",
        )

    def cache_identity(self) -> str:
        """Preflight the current immutable publication before a cached read."""
        return self.cache_snapshot().identity


def market_regime_store_from_settings(settings: Settings) -> "MarketRegimeStore":
    layout = StorageLayout(settings)
    dataset_root = configured_market_dataset_root(settings)
    if dataset_root is None:
        reader: MarketBarsReader = ReadOnlyDuckDbMarketReader(layout.local_paths.market_database)
    else:
        reader = PublishedDatasetMarketReader(
            control_path=layout.local_paths.market_database,
            control_temp=layout.duckdb_temporary,
            dataset_root=dataset_root,
            staging_root=layout.local_paths.staging,
        )
    return MarketRegimeStore(reader)


class MarketRegimeStore:
    """Build deterministic inputs from canonical rows at or before one as-of date."""

    _INPUT_CACHE_LIMIT = 128
    _input_cache: OrderedDict[tuple[str, date, str | None], MarketRegimeInput] = OrderedDict()
    _input_cache_lock = threading.Lock()

    def __init__(self, reader: MarketBarsReader) -> None:
        self.reader = reader

    def read(
        self,
        as_of: date,
        *,
        known_at: datetime | None = None,
    ) -> MarketRegimeInput:
        snapshot, cache_key = self._cache_key(as_of, known_at)
        if cache_key is not None:
            cached = self._cached_input(cache_key)
            if cached is not None:
                return cached
        if snapshot is not None:
            raw_bars = self.reader.bars_through_snapshot(
                snapshot,
                as_of,
                max_sessions=REGIME_LOOKBACK_SESSIONS,
            )
        else:
            raw_bars = self.reader.bars_through(
                as_of,
                max_sessions=REGIME_LOOKBACK_SESSIONS,
            )
        future_rows = [bar for bar in raw_bars if bar.trade_date > as_of]
        post_cutoff_rows = []
        if known_at is not None:
            cutoff = known_at.astimezone(UTC)
            post_cutoff_rows = [
                bar
                for bar in raw_bars
                if bar.ingested_at.utcoffset() is None or bar.ingested_at.astimezone(UTC) > cutoff
            ]
        bars = [
            bar
            for bar in raw_bars
            if bar.trade_date <= as_of
            and (
                known_at is None
                or (
                    bar.ingested_at.utcoffset() is not None
                    and bar.ingested_at.astimezone(UTC) <= cutoff
                )
            )
        ]
        dates = sorted({bar.trade_date for bar in bars})
        if len(dates) > REGIME_LOOKBACK_SESSIONS:
            allowed = set(dates[-REGIME_LOOKBACK_SESSIONS:])
            bars = [bar for bar in bars if bar.trade_date in allowed]
            dates = dates[-REGIME_LOOKBACK_SESSIONS:]
        quality_issues = ["future_market_rows_discarded"] if future_rows else []
        if post_cutoff_rows:
            quality_issues.append("post_cutoff_market_rows_discarded")
        if not bars:
            result = self._empty(as_of, quality_issues=quality_issues)
            self._cache_input(cache_key, result)
            return result

        data_as_of = dates[-1]
        current = [bar for bar in bars if bar.trade_date == data_as_of]
        lineage = self._lineage(bars)
        scope = self._scope(current)
        result = MarketRegimeInput(
            as_of=as_of,
            data_as_of=data_as_of,
            trend=self._trend(bars, data_as_of, lineage),
            breadth=self._breadth(bars, data_as_of, lineage),
            liquidity=self._liquidity(bars, data_as_of, lineage),
            risk=self._risk(bars, data_as_of, lineage),
            leadership=self._leadership(lineage),
            actual_market_scope=scope,
            source_lineage=[lineage],
            quality_issues=quality_issues,
        )
        self._cache_input(cache_key, result)
        return result

    def _cache_key(
        self,
        as_of: date,
        known_at: datetime | None,
    ) -> tuple[object | None, tuple[str, date, str | None] | None]:
        cache_snapshot = getattr(self.reader, "cache_snapshot", None)
        if not callable(cache_snapshot):
            return None, None
        snapshot = cache_snapshot()
        known_at_key = None if known_at is None else known_at.astimezone(UTC).isoformat()
        return snapshot, (str(snapshot.identity), as_of, known_at_key)

    @classmethod
    def _cached_input(
        cls,
        key: tuple[str, date, str | None],
    ) -> MarketRegimeInput | None:
        with cls._input_cache_lock:
            result = cls._input_cache.get(key)
            if result is None:
                return None
            cls._input_cache.move_to_end(key)
            return result.model_copy(deep=True)

    @classmethod
    def _cache_input(
        cls,
        key: tuple[str, date, str | None] | None,
        result: MarketRegimeInput,
    ) -> None:
        if key is None:
            return
        with cls._input_cache_lock:
            cls._input_cache[key] = result.model_copy(deep=True)
            cls._input_cache.move_to_end(key)
            while len(cls._input_cache) > cls._INPUT_CACHE_LIMIT:
                cls._input_cache.popitem(last=False)

    def _empty(
        self,
        as_of: date,
        *,
        quality_issues: list[str],
    ) -> MarketRegimeInput:
        return MarketRegimeInput(
            as_of=as_of,
            data_as_of=None,
            trend=self._missing_component(
                "trend",
                "trend-price-ma-v1",
                ["trend.representative_indexes"],
            ),
            breadth=self._missing_component(
                "breadth",
                "breadth-price-v1",
                ["breadth.market_breadth"],
            ),
            liquidity=self._missing_component(
                "liquidity",
                "liquidity-price-volume-v1",
                ["liquidity.turnover_history"],
            ),
            risk=self._missing_component(
                "risk",
                "risk-price-v1",
                ["risk.market_risk"],
            ),
            leadership=self._missing_component(
                "leadership",
                "leadership-sector-v1",
                [
                    "leadership.sector_persistence",
                    "leadership.leader_diffusion",
                ],
            ),
            actual_market_scope=self._scope([]),
            quality_issues=quality_issues or ["no_market_data_at_or_before_as_of"],
        )

    @staticmethod
    def _missing_component(
        name: str,
        formula_version: str,
        missing_inputs: list[str],
        *,
        lineage: list[SourceLineage] | None = None,
        quality_issues: list[str] | None = None,
    ) -> RegimeComponentInput:
        return RegimeComponentInput(
            name=name,
            score=None,
            formula_version=formula_version,
            quality_status="missing",
            missing_inputs=missing_inputs,
            quality_issues=quality_issues or [],
            source_lineage=lineage or [],
        )

    @staticmethod
    def _lineage(bars: list[DailyBar]) -> SourceLineage:
        stable_rows = [
            {
                "trade_date": bar.trade_date.isoformat(),
                "symbol": bar.symbol,
                "close": bar.close,
                "preclose": bar.preclose,
                "amount": bar.amount,
                "adjust_factor": bar.adjust_factor,
                "quality_status": bar.quality_status,
                "quality_issues": bar.quality_issues,
                "source_record_id": bar.source_record_id,
            }
            for bar in sorted(bars, key=lambda item: (item.trade_date, item.symbol))
        ]
        content = json.dumps(
            stable_rows,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        dates = [bar.trade_date for bar in bars]
        return SourceLineage(
            source="baostock",
            source_version=CANONICAL_SOURCE_VERSION,
            earliest_input_date=min(dates),
            latest_input_date=max(dates),
            record_count=len(bars),
            content_hash=hashlib.sha256(content.encode()).hexdigest(),
        )

    @staticmethod
    def _scope(current: list[DailyBar]) -> ActualMarketScope:
        observed_boards = sorted(
            {
                board
                for bar in current
                if bar.security_type == "stock" and (board := _scope_board(bar)) is not None
            }
        )
        observed_indexes = sorted(
            {bar.symbol for bar in current if bar.symbol in EXPECTED_INDEX_SERIES}
        )
        missing_boards = sorted(set(EXPECTED_BOARDS) - set(observed_boards))
        missing_indexes = sorted(set(EXPECTED_INDEX_SERIES) - set(observed_indexes))
        observed_universe_count = len(
            {bar.symbol for bar in current if bar.security_type == "stock"}
        )
        return ActualMarketScope(
            expected_boards=list(EXPECTED_BOARDS),
            observed_boards=observed_boards,
            missing_boards=missing_boards,
            expected_index_series=list(EXPECTED_INDEX_SERIES),
            observed_index_series=observed_indexes,
            missing_index_series=missing_indexes,
            coverage_basis="symbol_presence_only",
            coverage_evidence_status="unavailable",
            observed_universe_count=observed_universe_count,
            index_coverage_ratio=len(observed_indexes) / len(EXPECTED_INDEX_SERIES),
            scope_status="narrow_provisional",
            can_support_full_a_share_conclusion=False,
            conclusion_disclaimer=(
                "Symbol presence is observed, but authoritative point-in-time "
                "expected-universe coverage has not been audited. This result is "
                "narrow-scope and cannot support a full A-share bull/bear conclusion."
            ),
        )

    def _trend(
        self,
        bars: list[DailyBar],
        as_of: date,
        lineage: SourceLineage,
    ) -> RegimeComponentInput:
        histories = _histories(bar for bar in bars if bar.symbol in EXPECTED_INDEX_SERIES)
        signals: list[float] = []
        supporting: list[EvidenceItem] = []
        contrary: list[EvidenceItem] = []
        missing: list[str] = []
        for symbol in EXPECTED_INDEX_SERIES:
            points = _valid_bars(histories.get(symbol, []))
            if not points or points[-1].trade_date != as_of:
                missing.append(f"trend.index:{symbol}")
                continue
            closes = [bar.close for bar in points]
            if len(closes) < 125:
                missing.append(f"trend.warmup:{symbol}")
                continue
            metrics = {
                "above_ma20": _sign(closes[-1] - fmean(closes[-20:])),
                "above_ma60": _sign(closes[-1] - fmean(closes[-60:])),
                "above_ma120": _sign(closes[-1] - fmean(closes[-120:])),
                "ma20_slope_5d": _sign(fmean(closes[-20:]) - fmean(closes[-25:-5])),
            }
            for metric, signal in metrics.items():
                signals.append(signal * 100)
                evidence = EvidenceItem(
                    component="trend",
                    code=f"{symbol}:{metric}",
                    detail=f"{symbol} {metric} signal",
                    value=signal,
                    as_of=as_of,
                    source="baostock",
                )
                if signal > 0:
                    supporting.append(evidence)
                elif signal < 0:
                    contrary.append(evidence)
        if not signals:
            return self._missing_component(
                "trend",
                "trend-price-ma-v1",
                missing or ["trend.representative_indexes"],
                lineage=[lineage],
            )
        return RegimeComponentInput(
            name="trend",
            score=round(fmean(signals), 4),
            formula_version="trend-price-ma-v1",
            quality_status="degraded" if missing else "ready",
            missing_inputs=missing,
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            source_lineage=[lineage],
        )

    def _breadth(
        self,
        bars: list[DailyBar],
        as_of: date,
        lineage: SourceLineage,
    ) -> RegimeComponentInput:
        histories = _histories(bar for bar in bars if bar.security_type == "stock")
        current = [
            bar
            for rows in histories.values()
            for bar in rows[-1:]
            if bar.trade_date == as_of and _bar_is_return_candidate(bar)
        ]
        if not current:
            return self._missing_component(
                "breadth",
                "breadth-price-v1",
                ["breadth.market_breadth"],
                lineage=[lineage],
            )

        signals: list[tuple[str, float, float]] = []
        returns, quality_issues = _finite_returns(current, component="breadth")
        missing: list[str] = []
        if returns:
            ratio = sum(value > 0 for value in returns) / len(returns)
            signals.append(("advancing_ratio", _ratio_score(ratio), ratio))
        else:
            missing.append("breadth.advancing_declining_returns")

        adjusted = {
            symbol: _adjusted_closes(rows, as_of=as_of) for symbol, rows in histories.items()
        }
        for period in (20, 60):
            eligible = [values for values in adjusted.values() if len(values) >= period]
            if eligible:
                ratio = sum(values[-1] > fmean(values[-period:]) for values in eligible)
                ratio /= len(eligible)
                signals.append((f"above_ma{period}_ratio", _ratio_score(ratio), ratio))
            else:
                missing.append(f"breadth.ma{period}")
        eligible_60 = [values for values in adjusted.values() if len(values) >= 60]
        if eligible_60:
            high_ratio = sum(values[-1] >= max(values[-60:-1]) for values in eligible_60) / len(
                eligible_60
            )
            low_ratio = sum(values[-1] <= min(values[-60:-1]) for values in eligible_60) / len(
                eligible_60
            )
            spread_score = _clamp((high_ratio - low_ratio) * 500)
            signals.append(("new_high_low_spread", spread_score, high_ratio - low_ratio))
        else:
            missing.append("breadth.new_high_low")

        if not signals:
            return self._missing_component(
                "breadth",
                "breadth-price-v1",
                ["breadth.market_breadth"],
                lineage=[lineage],
                quality_issues=quality_issues,
            )
        supporting, contrary = _metric_evidence(
            "breadth",
            signals,
            as_of=as_of,
        )
        return RegimeComponentInput(
            name="breadth",
            score=round(fmean(score for _, score, _ in signals), 4),
            formula_version="breadth-price-v1",
            quality_status="degraded" if missing or quality_issues else "ready",
            missing_inputs=missing,
            quality_issues=quality_issues,
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            source_lineage=[lineage],
        )

    def _liquidity(
        self,
        bars: list[DailyBar],
        as_of: date,
        lineage: SourceLineage,
    ) -> RegimeComponentInput:
        daily_amount: dict[date, float] = defaultdict(float)
        for bar in bars:
            if bar.security_type == "stock" and _bar_is_usable(bar):
                daily_amount[bar.trade_date] += bar.amount
        dates = sorted(daily_amount)
        if len(dates) < 6 or dates[-1] != as_of:
            return self._missing_component(
                "liquidity",
                "liquidity-price-volume-v1",
                ["liquidity.turnover_history"],
                lineage=[lineage],
            )
        prior = [daily_amount[item] for item in dates[-6:-1]]
        if not prior or fmean(prior) <= 0:
            return self._missing_component(
                "liquidity",
                "liquidity-price-volume-v1",
                ["liquidity.turnover_history"],
                lineage=[lineage],
            )
        ratio = daily_amount[dates[-1]] / fmean(prior)
        score = _clamp((ratio - 1) * 100)
        supporting, contrary = _metric_evidence(
            "liquidity",
            [("turnover_5d_ratio", score, ratio)],
            as_of=as_of,
        )
        return RegimeComponentInput(
            name="liquidity",
            score=round(score, 4),
            formula_version="liquidity-price-volume-v1",
            quality_status="degraded",
            missing_inputs=["liquidity.fund_flow_evidence"],
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            source_lineage=[lineage],
        )

    def _risk(
        self,
        bars: list[DailyBar],
        as_of: date,
        lineage: SourceLineage,
    ) -> RegimeComponentInput:
        index_histories = _histories(bar for bar in bars if bar.symbol in EXPECTED_INDEX_SERIES)
        volatilities: list[float] = []
        drawdowns: list[float] = []
        index_series_missing = False
        volatility_warmup = False
        drawdown_warmup = False
        for symbol in EXPECTED_INDEX_SERIES:
            rows = index_histories.get(symbol, [])
            valid = _valid_bars(rows)
            if not valid or valid[-1].trade_date != as_of:
                index_series_missing = True
                continue
            if len(valid) < 21:
                volatility_warmup = True
            if len(valid) < 60:
                drawdown_warmup = True
            closes = [bar.close for bar in valid]
            if len(valid) >= 21:
                returns = [
                    closes[index] / closes[index - 1] - 1
                    for index in range(len(closes) - 20, len(closes))
                ]
                volatilities.append(pstdev(returns))
            if len(valid) >= 60:
                window = closes[-60:]
                running_high = window[0]
                worst = 0.0
                for close in window:
                    running_high = max(running_high, close)
                    worst = min(worst, close / running_high - 1)
                drawdowns.append(abs(worst))

        current_stock_bars = [
            bar
            for bar in bars
            if bar.trade_date == as_of
            and bar.security_type == "stock"
            and _bar_is_return_candidate(bar)
        ]
        current_returns, quality_issues = _finite_returns(
            current_stock_bars,
            component="risk",
        )
        metrics: list[tuple[str, float, float]] = []
        missing: list[str] = []
        if index_series_missing:
            missing.append("risk.representative_indexes")
        if volatilities:
            value = fmean(volatilities)
            metrics.append(("index_volatility_20d", _low_risk_score(value, 0.01, 0.03), value))
        if not volatilities or volatility_warmup:
            missing.append("risk.index_volatility_20d_warmup")
        if drawdowns:
            value = max(drawdowns)
            metrics.append(("index_drawdown_60d", _low_risk_score(value, 0.05, 0.20), value))
        if not drawdowns or drawdown_warmup:
            missing.append("risk.index_drawdown_60d_warmup")
        if len(current_returns) >= 2:
            value = pstdev(current_returns)
            metrics.append(("cross_section_dispersion", _low_risk_score(value, 0.015, 0.04), value))
        else:
            missing.append("risk.cross_section_dispersion")
        if not metrics:
            return self._missing_component(
                "risk",
                "risk-price-v1",
                ["risk.market_risk"],
                lineage=[lineage],
                quality_issues=quality_issues,
            )
        supporting, contrary = _metric_evidence("risk", metrics, as_of=as_of)
        return RegimeComponentInput(
            name="risk",
            score=round(fmean(score for _, score, _ in metrics), 4),
            formula_version="risk-price-v1",
            quality_status="degraded" if missing or quality_issues else "ready",
            missing_inputs=missing,
            quality_issues=quality_issues,
            supporting_evidence=supporting,
            contrary_evidence=contrary,
            source_lineage=[lineage],
        )

    def _leadership(self, lineage: SourceLineage) -> RegimeComponentInput:
        return self._missing_component(
            "leadership",
            "leadership-sector-v1",
            [
                "leadership.sector_persistence",
                "leadership.leader_diffusion",
            ],
            lineage=[lineage],
        )


def _scope_board(bar: DailyBar) -> str | None:
    code = bar.symbol.split(".", 1)[-1]
    if bar.exchange == "sz" and code.startswith(("300", "301")):
        return "chinext"
    if bar.exchange == "sh" and code.startswith(("688", "689")):
        return "star"
    if bar.board == "main":
        return "sse_main" if bar.exchange == "sh" else "szse_main"
    if bar.board in {"chinext", "star"}:
        return bar.board
    return None


def _histories(bars) -> dict[str, list[DailyBar]]:
    result: dict[str, list[DailyBar]] = defaultdict(list)
    for bar in bars:
        result[bar.symbol].append(bar)
    for rows in result.values():
        rows.sort(key=lambda item: item.trade_date)
    return dict(result)


def _bar_is_usable(bar: DailyBar) -> bool:
    return (
        bar.quality_status == "ready"
        and not bar.quality_issues
        and not bar.is_suspended
        and bar.close > 0
        and bar.preclose > 0
        and math.isfinite(bar.close)
        and math.isfinite(bar.preclose)
        and math.isfinite(bar.amount)
        and bar.amount >= 0
    )


def _bar_is_return_candidate(bar: DailyBar) -> bool:
    close = _finite_float(bar.close)
    return (
        bar.quality_status == "ready"
        and not bar.quality_issues
        and not bar.is_suspended
        and close is not None
        and close > 0
    )


def _finite_returns(
    bars: list[DailyBar],
    *,
    component: str,
) -> tuple[list[float], list[str]]:
    returns: list[float] = []
    quality_issues: set[str] = set()
    for bar in bars:
        preclose = _finite_float(bar.preclose)
        if preclose is None or preclose <= 0:
            quality_issues.add(f"{component}.invalid_return_input_excluded")
            continue

        pct_change = _finite_float(bar.pct_change)
        if pct_change is not None:
            returns.append(pct_change / 100)
            continue

        close = _finite_float(bar.close)
        if close is not None:
            fallback = close / preclose - 1
            if math.isfinite(fallback):
                returns.append(fallback)
                if bar.pct_change is not None:
                    quality_issues.add(f"{component}.pct_change_fallback_used")
                continue
        quality_issues.add(f"{component}.invalid_return_input_excluded")
    return returns, sorted(quality_issues)


def _finite_float(value) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (OverflowError, TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _valid_bars(bars: list[DailyBar]) -> list[DailyBar]:
    return [bar for bar in bars if _bar_is_usable(bar)]


def _adjusted_closes(bars: list[DailyBar], *, as_of: date) -> list[float]:
    valid = [
        bar
        for bar in bars
        if _bar_is_usable(bar)
        and bar.adjust_factor is not None
        and math.isfinite(bar.adjust_factor)
        and bar.adjust_factor > 0
    ]
    if not valid or valid[-1].trade_date != as_of:
        return []
    target = valid[-1].adjust_factor
    assert target is not None
    return [
        bar.close * float(bar.adjust_factor) / target
        for bar in valid
        if bar.adjust_factor is not None
    ]


def _sign(value: float) -> float:
    return 1.0 if value > 0 else -1.0 if value < 0 else 0.0


def _clamp(value: float) -> float:
    return max(-100.0, min(100.0, value))


def _ratio_score(value: float) -> float:
    return _clamp((value - 0.5) * 200)


def _low_risk_score(value: float, good: float, bad: float) -> float:
    if value <= good:
        return 100.0
    if value >= bad:
        return -100.0
    return 100 - 200 * (value - good) / (bad - good)


def _metric_evidence(
    component: str,
    metrics: list[tuple[str, float, float]],
    *,
    as_of: date,
) -> tuple[list[EvidenceItem], list[EvidenceItem]]:
    supporting: list[EvidenceItem] = []
    contrary: list[EvidenceItem] = []
    for code, score, raw_value in metrics:
        evidence = EvidenceItem(
            component=component,
            code=code,
            detail=f"{component} {code} deterministic input",
            value=raw_value,
            as_of=as_of,
            source="baostock",
        )
        if score > 0:
            supporting.append(evidence)
        elif score < 0:
            contrary.append(evidence)
    return supporting, contrary
