"""Explicit, audited ingestion for optional AKShare supplemental datasets.

This module is intentionally separate from the BaoStock canonical bar store.
Product GET requests only read the immutable manifest and never construct a
provider or perform network I/O.
"""

import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.app.market.akshare_supplemental import (
    AKShareSupplementalProvider,
    SupplementalDataError,
    SupplementalSourceUnavailableError,
)
from backend.app.market.supplemental import (
    IndustryClassificationRecord,
    ReportedFundFlowPoint,
    SupplementalMarketResponse,
    supplemental_capabilities,
)
from backend.app.storage.preflight import MountInspector, SystemMountInspector

DATASET_NAME = "stock-eva-supplemental"
SCHEMA_VERSION = 1
SENTINEL_NAME = ".stock-eva-supplemental.json"
PINNED_PROVIDER_VERSION = "akshare-1.18.78-contract"
_KINDS = {
    "industry_classification",
    "market_fund_flow",
    "sector_fund_flow",
}
_COLUMNS = (
    "dataset_kind",
    "trade_date",
    "scope",
    "scope_name",
    "source_symbol",
    "industry_code",
    "industry_name",
    "effective_from",
    "source_updated_on",
    "reported_main_net_inflow",
    "reported_main_net_inflow_ratio",
    "reported_super_large_net_inflow",
    "reported_super_large_net_inflow_ratio",
    "reported_large_net_inflow",
    "reported_large_net_inflow_ratio",
    "reported_medium_net_inflow",
    "reported_medium_net_inflow_ratio",
    "reported_small_net_inflow",
    "reported_small_net_inflow_ratio",
    "source",
    "upstream",
    "endpoint",
    "observed_at",
    "quality_status",
)
_TABLE_SQL = """
CREATE TABLE supplemental (
    dataset_kind VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    scope VARCHAR,
    scope_name VARCHAR,
    source_symbol VARCHAR,
    industry_code VARCHAR,
    industry_name VARCHAR,
    effective_from DATE,
    source_updated_on DATE,
    reported_main_net_inflow DOUBLE,
    reported_main_net_inflow_ratio DOUBLE,
    reported_super_large_net_inflow DOUBLE,
    reported_super_large_net_inflow_ratio DOUBLE,
    reported_large_net_inflow DOUBLE,
    reported_large_net_inflow_ratio DOUBLE,
    reported_medium_net_inflow DOUBLE,
    reported_medium_net_inflow_ratio DOUBLE,
    reported_small_net_inflow DOUBLE,
    reported_small_net_inflow_ratio DOUBLE,
    source VARCHAR NOT NULL,
    upstream VARCHAR NOT NULL,
    endpoint VARCHAR NOT NULL,
    observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    quality_status VARCHAR NOT NULL
)
"""


class SupplementalDatasetError(ValueError):
    pass


class SupplementalIngestionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    through_date: date
    include_market_flow: bool = False
    include_classification: bool = False
    sector_names: list[str] = Field(default_factory=list, max_length=10)
    canary: bool = False

    @field_validator("sector_names")
    @classmethod
    def normalize_sectors(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            item = value.strip()
            if not item:
                raise ValueError("sector name must not be blank")
            if item not in seen:
                normalized.append(item)
                seen.add(item)
        return normalized

    @model_validator(mode="after")
    def require_target(self):
        if not (self.include_market_flow or self.include_classification or self.sector_names):
            raise ValueError("at least one audited supplemental dataset is required")
        return self

    @property
    def dataset_kinds(self) -> list[str]:
        kinds: list[str] = []
        if self.include_classification:
            kinds.append("industry_classification")
        if self.include_market_flow:
            kinds.append("market_fund_flow")
        if self.sector_names:
            kinds.append("sector_fund_flow")
        return sorted(kinds)

    @property
    def request_key(self) -> str:
        canonical = json.dumps(
            {
                "provider_contract": PINNED_PROVIDER_VERSION,
                "through_date": self.through_date.isoformat(),
                "datasets": self.dataset_kinds,
                "sector_names": sorted(self.sector_names),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


class SupplementalRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    request_key: str
    through_date: date
    status: Literal["running", "ready", "error"]
    datasets: list[str]
    record_count: int = 0
    started_at: datetime
    completed_at: datetime | None = None
    generation: str | None = None
    error_code: str | None = None


class _FileLock:
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
            raise SupplementalDatasetError("supplemental ingestion already running") from exc
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
    with temporary.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class SupplementalDatasetInitializer:
    """Explicitly initialize an empty supplemental root without network I/O."""

    def __init__(
        self,
        root: Path,
        *,
        require_smb: bool,
        mount_inspector: MountInspector | None = None,
    ) -> None:
        self.root = root
        self.require_smb = require_smb
        self.mount_inspector = mount_inspector or SystemMountInspector()

    def initialize(self) -> Path:
        if self.require_smb:
            if not self.root.is_absolute():
                raise SupplementalDatasetError("supplemental NAS root must be absolute")
            if not self.root.parent.is_dir():
                raise SupplementalDatasetError("supplemental NAS parent is unavailable")
            mount = self.mount_inspector.find_mount(self.root.parent)
            if (
                mount is None
                or mount.filesystem_type.lower() not in {"smbfs", "cifs"}
                or self.root == mount.mount_point
                or mount.mount_point not in self.root.parents
            ):
                raise SupplementalDatasetError("supplemental NAS SMB mount is unavailable")
        else:
            self.root.parent.mkdir(parents=True, exist_ok=True)

        if self.root.exists():
            SupplementalStore(
                self.root,
                self.root.parent / ".supplemental-init-audit-unused.sqlite3",
            ).validate_initialized()
            return self.root

        try:
            self.root.mkdir()
        except FileExistsError as exc:
            raise SupplementalDatasetError(
                "supplemental dataset root changed during initialization"
            ) from exc
        try:
            _atomic_json(
                self.root / SENTINEL_NAME,
                {"dataset": DATASET_NAME, "schema_version": SCHEMA_VERSION},
            )
            _atomic_json(
                self.root / "manifest.json",
                {
                    "dataset": DATASET_NAME,
                    "schema_version": SCHEMA_VERSION,
                    "generation": f"generation-{uuid4().hex}",
                    "published_at": None,
                    "files": [],
                },
            )
        except Exception:
            # The root was created by this call and contains no user data. A
            # partial initialization must never be accepted by execute/GET.
            raise
        return self.root


class SupplementalStore:
    """Local audit store plus immutable local-or-NAS Parquet dataset."""

    def __init__(
        self,
        root: Path,
        audit_database: Path,
        *,
        staging_root: Path | None = None,
        lock_path: Path | None = None,
    ) -> None:
        self.root = root
        self.audit_database = audit_database
        self.staging_root = staging_root or audit_database.parent / "supplemental-staging"
        self.lock_path = lock_path or audit_database.parent / "supplemental-ingestion.lock"

    def _connect(self) -> sqlite3.Connection:
        self.audit_database.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.audit_database)
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS supplemental_runs (
                run_id TEXT PRIMARY KEY,
                request_key TEXT NOT NULL,
                through_date TEXT NOT NULL,
                status TEXT NOT NULL,
                datasets_json TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                generation TEXT,
                error_code TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS supplemental_ready_request
            ON supplemental_runs(request_key) WHERE status = 'ready'
            """
        )
        return connection

    @staticmethod
    def _run(row: sqlite3.Row) -> SupplementalRun:
        return SupplementalRun(
            run_id=row["run_id"],
            request_key=row["request_key"],
            through_date=date.fromisoformat(row["through_date"]),
            status=row["status"],
            datasets=json.loads(row["datasets_json"]),
            record_count=row["record_count"],
            started_at=datetime.fromisoformat(row["started_at"]),
            completed_at=(
                datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None
            ),
            generation=row["generation"],
            error_code=row["error_code"],
        )

    def ready_run(self, request_key: str) -> SupplementalRun | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM supplemental_runs
                WHERE request_key = ? AND status = 'ready'
                ORDER BY completed_at DESC LIMIT 1
                """,
                (request_key,),
            ).fetchone()
        return self._run(row) if row else None

    def begin(self, request: SupplementalIngestionRequest) -> SupplementalRun:
        started_at = datetime.now(UTC)
        run = SupplementalRun(
            run_id=uuid4().hex,
            request_key=request.request_key,
            through_date=request.through_date,
            status="running",
            datasets=request.dataset_kinds,
            started_at=started_at,
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO supplemental_runs(
                    run_id, request_key, through_date, status, datasets_json,
                    record_count, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.request_key,
                    run.through_date.isoformat(),
                    run.status,
                    json.dumps(run.datasets, separators=(",", ":")),
                    0,
                    run.started_at.isoformat(),
                ),
            )
        return run

    def finish(
        self,
        run: SupplementalRun,
        *,
        status: Literal["ready", "error"],
        record_count: int = 0,
        generation: str | None = None,
        error_code: str | None = None,
    ) -> SupplementalRun:
        completed_at = datetime.now(UTC)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE supplemental_runs
                SET status = ?, record_count = ?, completed_at = ?,
                    generation = ?, error_code = ?
                WHERE run_id = ?
                """,
                (
                    status,
                    record_count,
                    completed_at.isoformat(),
                    generation,
                    error_code,
                    run.run_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM supplemental_runs WHERE run_id = ?",
                (run.run_id,),
            ).fetchone()
        assert row is not None
        return self._run(row)

    def list_runs(self, limit: int = 100) -> list[SupplementalRun]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM supplemental_runs
                ORDER BY started_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._run(row) for row in rows]

    def _manifest(self, *, allow_missing: bool) -> dict[str, object]:
        path = self.root / "manifest.json"
        sentinel = self.root / SENTINEL_NAME
        if not self.root.exists() and allow_missing:
            return {
                "dataset": DATASET_NAME,
                "schema_version": SCHEMA_VERSION,
                "generation": "empty",
                "files": [],
            }
        try:
            sentinel_payload = json.loads(sentinel.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SupplementalDatasetError("supplemental dataset sentinel is unavailable") from exc
        if sentinel_payload != {
            "dataset": DATASET_NAME,
            "schema_version": SCHEMA_VERSION,
        }:
            raise SupplementalDatasetError("supplemental dataset sentinel is invalid")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SupplementalDatasetError("supplemental manifest is unavailable") from exc
        if (
            payload.get("dataset") != DATASET_NAME
            or payload.get("schema_version") != SCHEMA_VERSION
            or not isinstance(payload.get("generation"), str)
            or not isinstance(payload.get("files"), list)
        ):
            raise SupplementalDatasetError("supplemental manifest is invalid")
        for item in payload["files"]:
            self._validate_manifest_item(item)
        return payload

    def validate_initialized(self) -> None:
        self._manifest(allow_missing=False)

    @staticmethod
    def _validate_manifest_item(item) -> None:
        if not isinstance(item, dict) or item.get("dataset_kind") not in _KINDS:
            raise SupplementalDatasetError("supplemental manifest file is invalid")
        relative = Path(str(item.get("path", "")))
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.suffix != ".parquet"
            or "_staging" in relative.parts
            or "quarantine" in relative.parts
        ):
            raise SupplementalDatasetError("supplemental manifest path is unsafe")
        checksum = item.get("sha256")
        if (
            not isinstance(checksum, str)
            or len(checksum) != 64
            or any(character not in "0123456789abcdef" for character in checksum)
        ):
            raise SupplementalDatasetError("supplemental manifest checksum is invalid")
        if not isinstance(item.get("row_count"), int) or item["row_count"] < 1:
            raise SupplementalDatasetError("supplemental manifest row count is invalid")
        try:
            as_of = date.fromisoformat(str(item["as_of"]))
            minimum = date.fromisoformat(str(item["min_trade_date"]))
            maximum = date.fromisoformat(str(item["max_trade_date"]))
        except (KeyError, ValueError) as exc:
            raise SupplementalDatasetError("supplemental manifest as-of date is invalid") from exc
        if minimum > maximum or maximum > as_of:
            raise SupplementalDatasetError("supplemental manifest date range is invalid")

    def publish(
        self,
        run: SupplementalRun,
        rows_by_kind: dict[str, list[dict[str, object]]],
    ) -> str:
        with _FileLock(self.lock_path):
            baseline = self._manifest(allow_missing=False)
            baseline_generation = str(baseline["generation"])
            self.staging_root.mkdir(parents=True, exist_ok=True)
            local_generation = self.staging_root / run.run_id
            local_generation.mkdir(exist_ok=False)
            nas_staging = self.root / "_staging" / run.run_id
            final_entries: list[dict[str, object]] = []
            try:
                for kind, rows in sorted(rows_by_kind.items()):
                    staged = local_generation / f"{kind}.parquet"
                    self._write_parquet(staged, rows)
                    checksum = _sha256(staged)
                    self._validate_parquet(
                        staged,
                        kind,
                        len(rows),
                        as_of=run.through_date,
                    )
                    partial = nas_staging / f"{kind}.parquet.partial"
                    partial.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(staged, partial)
                    if _sha256(partial) != checksum:
                        raise SupplementalDatasetError("supplemental readback checksum mismatch")
                    self._validate_parquet(
                        partial,
                        kind,
                        len(rows),
                        as_of=run.through_date,
                    )
                    final = (
                        self.root
                        / "data"
                        / f"dataset={kind}"
                        / f"as_of={run.through_date.isoformat()}"
                        / f"{checksum}.parquet"
                    )
                    final.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(partial, final)
                    final_entries.append(
                        {
                            "dataset_kind": kind,
                            "path": str(final.relative_to(self.root)),
                            "sha256": checksum,
                            "row_count": len(rows),
                            "as_of": run.through_date.isoformat(),
                            "min_trade_date": min(row["trade_date"] for row in rows).isoformat(),
                            "max_trade_date": max(row["trade_date"] for row in rows).isoformat(),
                            "source": "akshare",
                            "provider_contract": PINNED_PROVIDER_VERSION,
                        }
                    )

                current = self._manifest(allow_missing=False)
                if str(current["generation"]) != baseline_generation:
                    raise SupplementalDatasetError("supplemental manifest changed during staging")
                replaced = set(rows_by_kind)
                retained = [
                    item for item in current["files"] if item["dataset_kind"] not in replaced
                ]
                generation = f"generation-{uuid4().hex}"
                payload = {
                    "dataset": DATASET_NAME,
                    "schema_version": SCHEMA_VERSION,
                    "generation": generation,
                    "published_at": datetime.now(UTC).isoformat(),
                    "files": sorted(
                        [*retained, *final_entries],
                        key=lambda item: str(item["dataset_kind"]),
                    ),
                }
                _atomic_json(self.root / "manifest.json", payload)
                return generation
            finally:
                shutil.rmtree(local_generation, ignore_errors=True)
                shutil.rmtree(nas_staging, ignore_errors=True)

    @staticmethod
    def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
        if not rows:
            raise SupplementalDatasetError("supplemental dataset contains no records")
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(_TABLE_SQL)
            placeholders = ",".join("?" for _ in _COLUMNS)
            connection.executemany(
                f"INSERT INTO supplemental VALUES ({placeholders})",
                [tuple(row.get(column) for column in _COLUMNS) for row in rows],
            )
            connection.execute(
                "COPY supplemental TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(path)],
            )
        except duckdb.Error as exc:
            raise SupplementalDatasetError("supplemental parquet staging failed") from exc
        finally:
            connection.close()

    @staticmethod
    def _validate_parquet(
        path: Path,
        kind: str,
        expected_rows: int,
        *,
        as_of: date,
    ) -> None:
        identity = (
            "(source_symbol, effective_from, industry_code)"
            if kind == "industry_classification"
            else "(trade_date, scope, scope_name)"
        )
        connection = duckdb.connect(":memory:")
        try:
            schema = {
                row[0]
                for row in connection.execute(
                    """
                    DESCRIBE SELECT * FROM read_parquet(
                        ?, hive_partitioning = false
                    )
                    """,
                    [str(path)],
                ).fetchall()
            }
            row = connection.execute(
                f"""
                SELECT count(*), count(DISTINCT dataset_kind),
                       min(dataset_kind), count_if(trade_date IS NULL),
                       count_if(source != 'akshare'),
                       count_if(endpoint IS NULL OR observed_at IS NULL),
                       count(DISTINCT {identity}),
                       max(trade_date)
                FROM read_parquet(?, hive_partitioning = false)
                """,
                [str(path)],
            ).fetchone()
        except duckdb.Error as exc:
            raise SupplementalDatasetError("supplemental parquet validation failed") from exc
        finally:
            connection.close()
        assert row is not None
        if schema != set(_COLUMNS):
            raise SupplementalDatasetError("supplemental parquet schema is invalid")
        if row[:7] != (expected_rows, 1, kind, 0, 0, 0, expected_rows):
            raise SupplementalDatasetError("supplemental parquet contents are invalid")
        if row[7] is None or row[7] > as_of:
            raise SupplementalDatasetError("supplemental parquet exceeds the requested date")

    def read_response(self, *, enabled: bool) -> SupplementalMarketResponse:
        if not enabled:
            return SupplementalMarketResponse(
                status="not_configured",
                provider="akshare",
                as_of=None,
                market_flow=None,
                sector_flows=[],
                industry_classifications=[],
                capabilities=supplemental_capabilities(),
                quality_issues=["akshare_supplemental_disabled"],
            )
        try:
            manifest = self._manifest(allow_missing=True)
            entries = manifest["files"]
            if not entries:
                return SupplementalMarketResponse(
                    status="empty",
                    provider="akshare",
                    as_of=None,
                    market_flow=None,
                    sector_flows=[],
                    industry_classifications=[],
                    capabilities=supplemental_capabilities(),
                    quality_issues=["supplemental_not_published"],
                )
            paths: list[Path] = []
            for item in entries:
                path = self.root / item["path"]
                if not path.is_file() or _sha256(path) != item["sha256"]:
                    raise SupplementalDatasetError("published supplemental object is invalid")
                self._validate_parquet(
                    path,
                    str(item["dataset_kind"]),
                    int(item["row_count"]),
                    as_of=date.fromisoformat(str(item["as_of"])),
                )
                paths.append(path)
            return self._query_response(paths, entries)
        except (OSError, duckdb.Error, SupplementalDatasetError):
            return SupplementalMarketResponse(
                status="error",
                provider="akshare",
                as_of=None,
                market_flow=None,
                sector_flows=[],
                industry_classifications=[],
                capabilities=supplemental_capabilities(),
                quality_issues=["supplemental_published_snapshot_unavailable"],
            )

    @staticmethod
    def _query_response(
        paths: list[Path],
        entries: list[dict[str, object]],
    ) -> SupplementalMarketResponse:
        connection = duckdb.connect(":memory:")
        try:
            rows = connection.execute(
                """
                SELECT * FROM read_parquet(?, hive_partitioning = false)
                ORDER BY dataset_kind, trade_date, scope_name, source_symbol
                """,
                [[str(path) for path in paths]],
            ).fetchall()
            columns = [item[0] for item in connection.description]
        finally:
            connection.close()
        records = [dict(zip(columns, row, strict=True)) for row in rows]
        market_records = [
            _fund_flow_from_row(row) for row in records if row["dataset_kind"] == "market_fund_flow"
        ]
        sector_history = [
            _fund_flow_from_row(row) for row in records if row["dataset_kind"] == "sector_fund_flow"
        ]
        classification_history = [
            _classification_from_row(row)
            for row in records
            if row["dataset_kind"] == "industry_classification"
        ]
        latest_sector: dict[str, ReportedFundFlowPoint] = {}
        for point in sector_history:
            current = latest_sector.get(point.scope_name)
            if current is None or point.trade_date > current.trade_date:
                latest_sector[point.scope_name] = point
        latest_classification: dict[str, IndustryClassificationRecord] = {}
        for item in classification_history:
            current = latest_classification.get(item.source_symbol)
            if current is None or (
                item.effective_from,
                item.source_updated_on,
                item.industry_code,
            ) > (
                current.effective_from,
                current.source_updated_on,
                current.industry_code,
            ):
                latest_classification[item.source_symbol] = item
        dates = {date.fromisoformat(str(item["as_of"])) for item in entries}
        quality_issues = ["supplemental_dataset_dates_differ"] if len(dates) > 1 else []
        return SupplementalMarketResponse(
            status="partial" if quality_issues else "ready",
            provider="akshare",
            as_of=max(dates),
            market_flow=market_records[-1] if market_records else None,
            sector_flows=[latest_sector[name] for name in sorted(latest_sector)],
            industry_classifications=[
                latest_classification[symbol] for symbol in sorted(latest_classification)
            ],
            capabilities=supplemental_capabilities(),
            quality_issues=quality_issues,
        )


class SupplementalIngestionService:
    def __init__(
        self,
        store: SupplementalStore,
        provider: AKShareSupplementalProvider,
    ) -> None:
        self.store = store
        self.provider = provider

    @staticmethod
    def plan(request: SupplementalIngestionRequest) -> dict[str, object]:
        return {
            "mode": "dry_run",
            "provider_contract": PINNED_PROVIDER_VERSION,
            "through_date": request.through_date.isoformat(),
            "datasets": request.dataset_kinds,
            "sector_names": request.sector_names,
            "network_requests": (
                int(request.include_market_flow)
                + int(request.include_classification)
                + len(request.sector_names)
            ),
            "requires": ["--canary", "--execute"],
        }

    def execute(self, request: SupplementalIngestionRequest) -> SupplementalRun:
        if not request.canary:
            raise ValueError("explicit canary authorization is required")
        existing = self.store.ready_run(request.request_key)
        if existing is not None:
            return existing
        run = self.store.begin(request)
        try:
            rows_by_kind = self._fetch(request)
            generation = self.store.publish(run, rows_by_kind)
            return self.store.finish(
                run,
                status="ready",
                record_count=sum(len(rows) for rows in rows_by_kind.values()),
                generation=generation,
            )
        except SupplementalDataError:
            self.store.finish(
                run,
                status="error",
                error_code="SUPPLEMENTAL_SOURCE_INVALID",
            )
            raise
        except SupplementalSourceUnavailableError:
            self.store.finish(
                run,
                status="error",
                error_code="SUPPLEMENTAL_SOURCE_UNAVAILABLE",
            )
            raise
        except Exception:
            self.store.finish(
                run,
                status="error",
                error_code="SUPPLEMENTAL_PUBLISH_FAILED",
            )
            raise

    def _fetch(
        self,
        request: SupplementalIngestionRequest,
    ) -> dict[str, list[dict[str, object]]]:
        rows: dict[str, list[dict[str, object]]] = {}
        if request.include_classification:
            classifications = self.provider.classification_history(
                through_date=request.through_date
            )
            rows["industry_classification"] = [
                _classification_row(item) for item in classifications
            ]
        if request.include_market_flow:
            market = self.provider.market_fund_flow_history(through_date=request.through_date)
            rows["market_fund_flow"] = [_fund_flow_row(item) for item in market]
        if request.sector_names:
            sector_records: list[ReportedFundFlowPoint] = []
            for sector_name in request.sector_names:
                sector_records.extend(
                    self.provider.sector_fund_flow_history(
                        sector_name,
                        through_date=request.through_date,
                    )
                )
            rows["sector_fund_flow"] = [_fund_flow_row(item) for item in sector_records]
        if any(not values for values in rows.values()):
            raise SupplementalDataError("audited supplemental dataset returned no dated records")
        return rows


def _base_row() -> dict[str, object]:
    return dict.fromkeys(_COLUMNS)


def _classification_row(item: IndustryClassificationRecord) -> dict[str, object]:
    row = _base_row()
    row.update(
        {
            "dataset_kind": "industry_classification",
            # This is the source's explicit classification effective date, not
            # an inferred exchange session or an observation timestamp.
            "trade_date": item.effective_from,
            "source_symbol": item.source_symbol,
            "industry_code": item.industry_code,
            "industry_name": item.industry_name,
            "effective_from": item.effective_from,
            "source_updated_on": item.source_updated_on,
            "source": item.source,
            "upstream": item.upstream,
            "endpoint": item.source_endpoint,
            "observed_at": item.observed_at,
            "quality_status": item.quality_status,
        }
    )
    return row


def _fund_flow_row(item: ReportedFundFlowPoint) -> dict[str, object]:
    row = _base_row()
    values = item.model_dump()
    row.update(
        {
            "dataset_kind": ("market_fund_flow" if item.scope == "market" else "sector_fund_flow"),
            "trade_date": item.trade_date,
            "scope": item.scope,
            "scope_name": item.scope_name,
            **{key: values[key] for key in values if key.startswith("reported_")},
            "source": item.source,
            "upstream": item.upstream,
            "endpoint": item.source_endpoint,
            "observed_at": item.observed_at,
            "quality_status": item.quality_status,
        }
    )
    return row


def _fund_flow_from_row(row: dict[str, object]) -> ReportedFundFlowPoint:
    return ReportedFundFlowPoint(
        trade_date=row["trade_date"],
        scope=row["scope"],
        scope_name=row["scope_name"],
        **{key: row[key] for key in _COLUMNS if key.startswith("reported_")},
        source=row["source"],
        upstream=row["upstream"],
        source_endpoint=row["endpoint"],
        observed_at=row["observed_at"],
        quality_status=row["quality_status"],
    )


def _classification_from_row(
    row: dict[str, object],
) -> IndustryClassificationRecord:
    return IndustryClassificationRecord(
        source_symbol=row["source_symbol"],
        industry_code=row["industry_code"],
        industry_name=row["industry_name"],
        effective_from=row["effective_from"],
        source_updated_on=row["source_updated_on"],
        source_endpoint=row["endpoint"],
        observed_at=row["observed_at"],
    )


def supplemental_dataset_root(settings) -> Path:
    if settings.supplemental_data_dir is not None:
        return settings.supplemental_data_dir
    if settings.nas_market_dataset_root is not None:
        return settings.nas_market_dataset_root.parent / "stock-eva-supplemental"
    return Path("var/supplemental")
