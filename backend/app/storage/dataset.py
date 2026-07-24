"""Immutable Parquet dataset publishing and manifest-only reads.

The NAS is deliberately never used as a mutable DuckDB/SQLite database.  The
control database remains local; only checksum-verified Parquet objects and a
small published manifest live under the configured dataset root.
"""

import fcntl
import hashlib
import json
import os
import re
import shutil
from collections import defaultdict
from datetime import date
from pathlib import Path
from uuid import uuid4

import duckdb

from backend.app.market.models import DailyBar, RefreshResult
from backend.app.market.store import MarketStore

SENTINEL_NAME = ".stock-eva-dataset.json"
MANIFEST_NAME = "manifest.json"
DATASET = "stock-eva-market"
SCHEMA_VERSION = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_PARQUET_TYPES = {
    "trade_date": "DATE",
    "symbol": "VARCHAR",
    "security_type": "VARCHAR",
    "exchange": "VARCHAR",
    "board": "VARCHAR",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "preclose": "DOUBLE",
    "volume": "DOUBLE",
    "amount": "DOUBLE",
    "turnover_rate": "DOUBLE",
    "pct_change": "DOUBLE",
    "adjust_factor": "DOUBLE",
    "price_adjustment": "VARCHAR",
    "is_trading": "BOOLEAN",
    "is_suspended": "BOOLEAN",
    "is_st": "BOOLEAN",
    "source": "VARCHAR",
    "source_record_id": "VARCHAR",
    "ingested_at": "TIMESTAMP WITH TIME ZONE",
    "quality_status": "VARCHAR",
    "quality_issues": "JSON",
}


class DatasetError(ValueError):
    pass


class DatasetPublicationBusy(DatasetError):
    pass


class _ManifestLock:
    """Local cross-process lease, matching the refresh lock's nonblocking semantics."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.descriptor: int | None = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise DatasetPublicationBusy("NAS manifest publication already running") from exc
        self.descriptor = descriptor
        return self

    def __exit__(self, *_args) -> None:
        if self.descriptor is not None:
            fcntl.flock(self.descriptor, fcntl.LOCK_UN)
            os.close(self.descriptor)
            self.descriptor = None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _safe_relative(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        raise DatasetError("manifest contains unsafe file path")
    if any(part in {"_staging", "quarantine"} for part in path.parts) or path.suffix != ".parquet":
        raise DatasetError("manifest references a non-published parquet file")
    return path


class DatasetPublication:
    """Five-stage, same-share publication implementation for immutable files."""

    def stage_and_validate(self, source: Path, staging_root: Path) -> tuple[Path, str, int]:
        if not source.is_file() or source.suffix != ".parquet":
            raise DatasetError("staged source must be a parquet file")
        generation_root = staging_root / f"generation-{uuid4().hex}"
        generation_root.mkdir(parents=True, exist_ok=False)
        staged = generation_root / "bars.parquet"
        shutil.copyfile(source, staged)
        checksum = _sha256(staged)
        connection = duckdb.connect(":memory:")
        try:
            rows = int(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(staged)]
                ).fetchone()[0]
            )
            if rows < 1:
                raise DatasetError("staged parquet contains no rows")
        finally:
            connection.close()
        return staged, checksum, rows

    def upload_partial(self, staged: Path, root: Path, generation: str) -> Path:
        partial = root / "_staging" / generation / f"{staged.name}.partial"
        partial.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(staged, partial)
        return partial

    def readback_and_verify(self, partial: Path, checksum: str, expected_rows: int) -> None:
        if _sha256(partial) != checksum:
            raise DatasetError("NAS readback checksum mismatch")
        connection = duckdb.connect(":memory:")
        try:
            rows = int(
                connection.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(partial)]
                ).fetchone()[0]
            )
        finally:
            connection.close()
        if rows != expected_rows:
            raise DatasetError("NAS readback row count mismatch")

    def publish_by_atomic_rename(self, partial: Path, final: Path, root: Path) -> None:
        partial_in_root = partial.resolve().is_relative_to(root.resolve())
        final_in_root = final.resolve().is_relative_to(root.resolve())
        if not partial_in_root or not final_in_root:
            # Both paths must reside below the same dataset root. This guard
            # rejects accidental cross-volume copy-and-delete publication.
            raise DatasetError("publication paths do not share a dataset root")
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(partial, final)

    def publish_manifest(self, root: Path, manifest: dict[str, object]) -> None:
        _atomic_json(root / MANIFEST_NAME, manifest)


class NasMarketStore:
    """Read market bars from the current manifest while retaining local audit state."""

    def __init__(
        self,
        control: MarketStore,
        root: Path,
        staging_root: Path,
        *,
        manifest_lock_path: Path | None = None,
    ) -> None:
        self.control = control
        self.root = root
        self.staging_root = staging_root
        self.manifest_lock_path = manifest_lock_path or staging_root / "nas-manifest.lock"
        self.publisher = DatasetPublication()

    @property
    def path(self) -> Path:
        return self.control.path

    @property
    def temp_directory(self) -> Path:
        return self.control.temp_directory

    def _manifest(self) -> dict[str, object]:
        try:
            payload = json.loads((self.root / MANIFEST_NAME).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DatasetError("published manifest is unavailable") from exc
        if payload.get("dataset") != DATASET or payload.get("schema_version") != SCHEMA_VERSION:
            raise DatasetError("published manifest has an unsupported schema")
        files = payload.get("files")
        if not isinstance(files, list):
            raise DatasetError("published manifest has invalid files")
        for item in files:
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                raise DatasetError("published manifest has invalid file metadata")
            _safe_relative(item["path"])
            if not isinstance(item.get("sha256"), str) or not _SHA256.fullmatch(item["sha256"]):
                raise DatasetError("published manifest has invalid checksum metadata")
            if not isinstance(item.get("row_count"), int) or item["row_count"] < 1:
                raise DatasetError("published manifest has invalid row count metadata")
            if item.get("source") != "baostock" or not isinstance(item.get("trade_date"), str):
                raise DatasetError("published manifest has invalid market partition metadata")
            try:
                date.fromisoformat(item["trade_date"])
            except ValueError as exc:
                raise DatasetError("published manifest has invalid trade date metadata") from exc
        return payload

    @staticmethod
    def _validate_parquet(path: Path, expected_rows: int) -> None:
        connection = duckdb.connect(":memory:")
        try:
            schema = {
                row[0]: row[1]
                for row in connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
                ).fetchall()
            }
            if any(
                schema.get(column) != data_type
                for column, data_type in _EXPECTED_PARQUET_TYPES.items()
            ):
                raise DatasetError("published parquet has an incompatible schema")
            rows = int(
                connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[
                    0
                ]
            )
        except duckdb.Error as exc:
            raise DatasetError("published parquet cannot be read") from exc
        finally:
            connection.close()
        if rows != expected_rows:
            raise DatasetError("published parquet row count mismatch")

    def _paths(self) -> list[Path]:
        try:
            paths: list[Path] = []
            for item in self._manifest()["files"]:
                relative = _safe_relative(item["path"])
                path = self.root / relative
                if not path.is_file():
                    raise DatasetError("published manifest references a missing parquet file")
                if _sha256(path) != item["sha256"]:
                    raise DatasetError("published parquet checksum mismatch")
                self._validate_parquet(path, item["row_count"])
                paths.append(path)
            return paths
        except OSError as exc:
            raise DatasetError("published NAS dataset is unavailable") from exc

    def validate_readiness(self) -> None:
        """Force manifest/hash/schema validation before exposing a NAS reader."""
        self._paths()

    def _query(self, sql: str, parameters: list[object] | None = None):
        paths = self._paths()
        if not paths:
            return [], []
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = duckdb.connect(":memory:")
            cursor = connection.execute(
                f"WITH daily_bars AS (SELECT * FROM read_parquet(?)) {sql}",
                [[str(path) for path in paths], *(parameters or [])],
            )
            columns = [item[0] for item in cursor.description]
            return cursor.fetchall(), columns
        except (duckdb.Error, OSError) as exc:
            raise DatasetError("published NAS dataset cannot be queried") from exc
        finally:
            if connection is not None:
                connection.close()

    def exists(self) -> bool:
        return bool(self._paths())

    def latest_refresh(self):
        return self.control.latest_refresh()

    def published_refresh(self):
        return self.control.published_refresh()

    def scheduler_state(self):
        return self.control.scheduler_state()

    def save_scheduler_state(self, state) -> None:
        self.control.save_scheduler_state(state)

    def list_refreshes(self, limit: int = 100):
        return self.control.list_refreshes(limit)

    def _daily_bar_from_row(self, row) -> DailyBar:
        return MarketStore._daily_bar_from_row(row)

    def bars_for(self, trade_date, source: str) -> list[dict[str, object]]:
        rows, columns = self._query(
            """
            SELECT symbol, security_type, close, pct_change, amount, is_suspended,
                   quality_status, quality_issues
            FROM daily_bars WHERE trade_date = ? AND source = ? ORDER BY symbol
            """,
            [trade_date, source],
        )
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def available_dates(self, source: str = "baostock") -> list[date]:
        rows, _ = self._query(
            "SELECT DISTINCT trade_date FROM daily_bars WHERE source = ? ORDER BY trade_date",
            [source],
        )
        return [row[0] for row in rows]

    def canonical_bars(self, trade_date, source: str = "baostock") -> list[DailyBar]:
        rows, _ = self._query(
            """SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                      close, preclose, volume, amount, turnover_rate, pct_change,
                      adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                      source, source_record_id, ingested_at, quality_status, quality_issues
               FROM daily_bars WHERE trade_date = ? AND source = ? ORDER BY symbol""",
            [trade_date, source],
        )
        return [self._daily_bar_from_row(row) for row in rows]

    def symbol_bars(self, symbol, start, end, source: str = "baostock") -> list[DailyBar]:
        if start > end:
            return []
        rows, _ = self._query(
            """SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                      close, preclose, volume, amount, turnover_rate, pct_change,
                      adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                      source, source_record_id, ingested_at, quality_status, quality_issues
               FROM daily_bars WHERE symbol = ? AND trade_date BETWEEN ? AND ? AND source = ?
               ORDER BY trade_date""",
            [symbol, start, end, source],
        )
        return [self._daily_bar_from_row(row) for row in rows]

    def symbols_bars(
        self,
        symbols: list[str],
        start,
        end,
        source: str = "baostock",
    ) -> dict[str, list[DailyBar]]:
        normalized = sorted(set(symbols))
        result = {symbol: [] for symbol in normalized}
        if not normalized or start > end:
            return result
        rows, _ = self._query(
            """SELECT trade_date, symbol, security_type, exchange, board, open, high, low,
                      close, preclose, volume, amount, turnover_rate, pct_change,
                      adjust_factor, price_adjustment, is_trading, is_suspended, is_st,
                      source, source_record_id, ingested_at, quality_status, quality_issues
               FROM daily_bars WHERE symbol = ANY(?) AND trade_date BETWEEN ? AND ? AND source = ?
               ORDER BY symbol, trade_date""",
            [normalized, start, end, source],
        )
        for row in rows:
            result[row[1]].append(self._daily_bar_from_row(row))
        return result

    def present_points(
        self,
        trading_dates: list,
        symbols: list[str],
        source: str = "baostock",
    ) -> set[tuple]:
        if not trading_dates or not symbols:
            return set()
        rows, _ = self._query(
            """SELECT trade_date, symbol FROM daily_bars
               WHERE trade_date = ANY(?) AND symbol = ANY(?) AND source = ?""",
            [list(dict.fromkeys(trading_dates)), sorted(set(symbols)), source],
        )
        return set(rows)

    def _publish_bars(self, bars: list[DailyBar]) -> None:
        if not bars:
            raise DatasetError("cannot publish an empty bar set")
        by_date: dict[tuple[date, str], list[DailyBar]] = defaultdict(list)
        for bar in bars:
            by_date[(bar.trade_date, bar.source)].append(bar)
        with _ManifestLock(self.manifest_lock_path):
            baseline = self._manifest()
            baseline_generation = baseline["generation"]
            self._publish_locked(by_date, baseline_generation)

    def _publish_locked(
        self,
        by_date: dict[tuple[date, str], list[DailyBar]],
        baseline_generation: object,
    ) -> None:
        staged_entries: list[tuple[Path, Path, Path, dict[str, object]]] = []
        scratch_paths: list[Path] = []
        try:
            # All partitions must complete local staging, NAS upload and
            # readback before there is any published-manifest mutation.
            for (trade_date, source), incoming in sorted(by_date.items()):
                existing = self.canonical_bars(trade_date, source)
                merged = {(bar.symbol, bar.source): bar for bar in existing}
                merged.update({(bar.symbol, bar.source): bar for bar in incoming})
                scratch = self.staging_root / f"scratch-{uuid4().hex}.duckdb"
                scratch_paths.append(scratch)
                scratch_store = MarketStore(
                    scratch,
                    temp_directory=self.staging_root / "duckdb-tmp",
                )
                scratch_store.upsert_bars(list(merged.values()))
                exported = scratch_store.export_date(
                    trade_date, self.staging_root / "exports", source
                )
                staged, checksum, row_count = self.publisher.stage_and_validate(
                    exported, self.staging_root
                )
                generation = f"generation-{trade_date:%Y%m%d}-{checksum[:12]}"
                partial = self.publisher.upload_partial(staged, self.root, generation)
                self.publisher.readback_and_verify(partial, checksum, row_count)
                relative = (
                    Path("bars")
                    / f"source={source}"
                    / f"year={trade_date:%Y}"
                    / f"month={trade_date:%m}"
                    / f"date={trade_date.isoformat()}_{checksum[:12]}.parquet"
                )
                staged_entries.append(
                    (
                        staged,
                        partial,
                        self.root / relative,
                        {
                            "path": str(relative),
                            "sha256": checksum,
                            "trade_date": trade_date.isoformat(),
                            "source": source,
                            "row_count": row_count,
                        },
                    )
                )

            # Rename is same-share/same-root. If interrupted before the final
            # manifest update, renamed files are unreferenced and cannot leak.
            for _staged, partial, final, _entry in staged_entries:
                self.publisher.publish_by_atomic_rename(partial, final, self.root)
            manifest = self._manifest()
            if manifest["generation"] != baseline_generation:
                raise DatasetError("published manifest changed during staging")
            replaced = {
                (entry["trade_date"], entry["source"])
                for _staged, _partial, _final, entry in staged_entries
            }
            old_files = [
                item
                for item in manifest["files"]
                if (item.get("trade_date"), item.get("source")) not in replaced
            ]
            old_files.extend(entry for _staged, _partial, _final, entry in staged_entries)
            manifest["generation"] = f"generation-{uuid4().hex}"
            manifest["files"] = sorted(old_files, key=lambda item: str(item["path"]))
            self.publisher.publish_manifest(self.root, manifest)
            for _staged, partial, _final, _entry in staged_entries:
                partial.parent.rmdir()
        finally:
            shutil.rmtree(self.staging_root / "exports", ignore_errors=True)
            for staged, _partial, _final, _entry in staged_entries:
                shutil.rmtree(staged.parent, ignore_errors=True)
            for scratch in scratch_paths:
                scratch.unlink(missing_ok=True)
                scratch.with_name(f"{scratch.name}.wal").unlink(missing_ok=True)

    def upsert_bars(self, bars: list[DailyBar]) -> None:
        self._publish_bars(bars)

    def save_refresh(
        self,
        bars: list[DailyBar],
        result: RefreshResult,
        *,
        publish: bool | None = None,
    ) -> None:
        publish = result.status == "ready" if publish is None else publish
        if publish:
            if result.status != "ready":
                raise ValueError("only ready refreshes can be published")
            self._publish_bars(bars)
        self.control.save_refresh([], result, publish=publish)

    def export_date(self, trade_date, output_root: Path, source: str = "baostock") -> Path:
        """Export a published partition locally without modifying the NAS dataset."""
        # Export has the same trust boundary as read APIs: never copy a file
        # merely because its path appears in a manifest.
        self._paths()
        manifest = self._manifest()
        matches = [
            item
            for item in manifest["files"]
            if item.get("trade_date") == trade_date.isoformat() and item.get("source") == source
        ]
        if len(matches) != 1:
            raise DatasetError("exactly one published partition is required for export")
        source_path = self.root / _safe_relative(str(matches[0]["path"]))
        output = output_root / f"date={trade_date.isoformat()}" / "bars.parquet"
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, output)
        return output
