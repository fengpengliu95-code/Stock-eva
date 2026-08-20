import asyncio
import hashlib
import json
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import httpx
import pytest

import backend.app.cli as cli_module
import backend.app.main as main_module
import backend.app.market.store as market_store_module
from backend.app.classification.models import TAXONOMY_BAOSTOCK_INDUSTRY
from backend.app.config import Settings, get_settings
from backend.app.main import app
from backend.app.market.calendar_sync import CalendarSyncStore
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.store import MarketStore, MarketStoreReadError
from backend.app.storage.dataset import DatasetError, NasMarketStore
from backend.app.storage.models import StorageReadiness
from backend.app.user.store import UserStore

AS_OF = date(2026, 7, 23)


def _settings(tmp_path: Path, dataset_root: Path) -> Settings:
    runtime = tmp_path / "runtime"
    return Settings(
        _env_file=None,
        market_data_dir=runtime / "market",
        user_data_dir=runtime / "user",
        local_control_dir=runtime / "control",
        local_staging_dir=runtime / "staging",
        local_lock_dir=runtime / "locks",
        local_temp_dir=runtime / "temp",
        local_market_dataset_root=dataset_root,
        nas_market_dataset_root=None,
    )


def _empty_dataset(root: Path) -> None:
    root.mkdir(parents=True)
    (root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}),
        encoding="utf-8",
    )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
        ),
        encoding="utf-8",
    )


def _bars():
    return normalize_baostock_rows(
        fields=[
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
            "adjustflag",
            "turn",
            "tradestatus",
            "pctChg",
            "isST",
        ],
        rows=[
            [
                AS_OF.isoformat(),
                "sh.600000",
                "10",
                "11",
                "9",
                "10.5",
                "10",
                "100",
                "1000",
                "3",
                "1",
                "1",
                "5",
                "0",
            ]
        ],
        factor_fields=[
            "code",
            "dividOperateDate",
            "foreAdjustFactor",
            "backAdjustFactor",
            "adjustFactor",
        ],
        factor_rows=[["sh.600000", "2026-01-01", "1.25", "0.8", "1"]],
        ingested_at=datetime(2026, 7, 24, tzinfo=UTC),
    )


def _ready_result(*, run_id: str = "ready-20260723") -> RefreshResult:
    return RefreshResult(
        run_id=run_id,
        request_key=f"daily:baostock:{AS_OF}:fixture",
        requested_date=AS_OF,
        source="baostock",
        status="ready",
        requested_count=1,
        succeeded_count=1,
        coverage_ratio=1,
        started_at=datetime(2026, 7, 24, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 1, tzinfo=UTC),
    )


def _publish_fixture(settings: Settings, dataset_root: Path) -> Path:
    _empty_dataset(dataset_root)
    control_path = settings.market_data_dir / settings.market_database_name
    store = NasMarketStore(
        MarketStore(control_path, temp_directory=settings.local_temp_dir / "writer-duckdb"),
        dataset_root,
        settings.local_staging_dir,
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    CalendarSyncStore(settings.local_control_dir / settings.calendar_sync_database_name)
    UserStore(settings.user_data_dir / settings.user_database_name).list_positions()
    return control_path


def _api_get(path: str, settings: Settings, *, raise_app_exceptions: bool = True):
    app.dependency_overrides[get_settings] = lambda: settings

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(
            app=app,
            raise_app_exceptions=raise_app_exceptions,
        )
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    try:
        return asyncio.run(send())
    finally:
        app.dependency_overrides.clear()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _schema(path: Path) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        tables = [
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_name"
            ).fetchall()
        ]
        return tuple(
            (
                table,
                tuple(
                    (row[1], row[2])
                    for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
                ),
            )
            for table in tables
        )
    finally:
        connection.close()


def _sqlite_schema(path: Path) -> tuple[tuple[str, str | None], ...]:
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    try:
        return tuple(
            connection.execute(
                "SELECT name, sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        )
    finally:
        connection.close()


def _tree(root: Path) -> tuple[tuple[str, str, int, int], ...]:
    return tuple(
        (
            str(path.relative_to(root)),
            "dir" if path.is_dir() else _sha256(path),
            path.stat().st_mtime_ns,
            path.stat().st_size,
        )
        for path in sorted(root.rglob("*"))
    )


@dataclass(frozen=True)
class _StorageFingerprint:
    control_hash: str
    control_mtime_ns: int
    control_size: int
    control_schema: tuple[tuple[str, tuple[tuple[str, str], ...]], ...]
    dataset_tree: tuple[tuple[str, str, int, int], ...]
    calendar_hash: str | None = None
    calendar_mtime_ns: int | None = None
    calendar_schema: tuple[tuple[str, str | None], ...] | None = None


def _fingerprint(
    control: Path,
    dataset_root: Path,
    calendar_path: Path | None = None,
) -> _StorageFingerprint:
    return _StorageFingerprint(
        control_hash=_sha256(control),
        control_mtime_ns=control.stat().st_mtime_ns,
        control_size=control.stat().st_size,
        control_schema=_schema(control),
        dataset_tree=_tree(dataset_root),
        calendar_hash=(
            _sha256(calendar_path)
            if calendar_path is not None and calendar_path.is_file()
            else None
        ),
        calendar_mtime_ns=(
            calendar_path.stat().st_mtime_ns
            if calendar_path is not None and calendar_path.is_file()
            else None
        ),
        calendar_schema=(
            _sqlite_schema(calendar_path)
            if calendar_path is not None and calendar_path.is_file()
            else None
        ),
    )


def test_market_consuming_gets_do_not_change_control_or_dataset(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    settings = _settings(tmp_path, dataset_root)
    control = _publish_fixture(settings, dataset_root)
    # Simulate an older valid control database. GET must not perform an implicit migration.
    connection = duckdb.connect(str(control))
    try:
        connection.execute("DROP TABLE published_daily_bars")
    finally:
        connection.close()
    calendar_path = settings.local_control_dir / settings.calendar_sync_database_name
    before = _fingerprint(control, dataset_root, calendar_path)

    paths = [
        "/api/v1/health",
        "/api/v1/storage/readiness",
        "/api/v1/market/summary",
        "/api/v1/market/status",
        "/api/v1/market/supplemental",
        "/api/v1/market/history/dates",
        f"/api/v1/market/history/sh.600000?start={AS_OF}&end={AS_OF}",
        f"/api/v1/securities/sh.600000/analysis?start={AS_OF}&end={AS_OF}",
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        f"/api/v1/analysis/market-regime/snapshots/{AS_OF}",
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
        f"/api/v1/classification/securities?as_of={AS_OF}&symbol=sh.600000",
        f"/api/v1/analysis/fund-flow-evidence?as_of={AS_OF}",
        "/api/v1/portfolio/valuation",
        "/api/v1/strategies",
    ]
    responses = [_api_get(path, settings, raise_app_exceptions=False) for path in paths]

    assert all(response.status_code < 500 for response in responses), [
        (path, response.status_code, response.text)
        for path, response in zip(paths, responses, strict=True)
    ]
    assert _fingerprint(control, dataset_root, calendar_path) == before


def test_market_get_dependency_never_reconciles_control_pointer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dataset_root = tmp_path / "dataset"
    settings = _settings(tmp_path, dataset_root)
    _publish_fixture(settings, dataset_root)

    def unexpected_reconcile(*_args, **_kwargs):
        raise AssertionError("HTTP GET must not reconcile market control state")

    monkeypatch.setattr(NasMarketStore, "reconcile_control_pointer", unexpected_reconcile)

    response = _api_get("/api/v1/market/summary", settings, raise_app_exceptions=False)

    assert response.status_code == 200


def test_market_status_exposes_unavailable_continuity_additives_without_migration(
    tmp_path: Path,
) -> None:
    dataset_root = tmp_path / "dataset"
    settings = _settings(tmp_path, dataset_root).model_copy(
        update={"market_continuity_start_date": date(2026, 7, 20)}
    )
    control = _publish_fixture(settings, dataset_root)
    before = _fingerprint(control, dataset_root)

    response = _api_get("/api/v1/market/status", settings, raise_app_exceptions=False)

    assert response.status_code == 200
    payload = response.json()
    assert payload["continuity_status"] == "unavailable"
    assert payload["continuity_start_date"] == "2026-07-20"
    assert payload["missing_session_count"] > 0
    assert payload["repair_execution_enabled"] is False
    assert payload["continuity_reason_code"] == "CONTROL_STATE_UNAVAILABLE"
    assert _fingerprint(control, dataset_root) == before


def test_market_status_local_mutable_mode_reports_manifest_unavailable_without_writes(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, tmp_path / "unused").model_copy(
        update={
            "local_market_dataset_root": None,
            "market_continuity_start_date": date(2026, 7, 20),
        }
    )
    runtime = tmp_path / "runtime"

    response = _api_get("/api/v1/market/status", settings, raise_app_exceptions=False)

    assert response.status_code == 200
    assert response.json()["continuity_status"] == "unavailable"
    assert response.json()["continuity_reason_code"] == "MANIFEST_INVENTORY_UNAVAILABLE"
    assert not runtime.exists()


def test_market_continuity_execute_enqueues_only_after_strict_rescan(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    target = date(2026, 7, 22)
    dataset_root = tmp_path / "dataset"
    settings = _settings(tmp_path, dataset_root).model_copy(
        update={
            "market_continuity_start_date": target,
        }
    )
    control = _publish_fixture(settings, dataset_root)
    before_dataset = _fingerprint(control, dataset_root)
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "market-continuity",
            "--start",
            target.isoformat(),
            "--end",
            target.isoformat(),
            "--execute",
        ],
    )
    monkeypatch.setattr(
        cli_module,
        "BaoStockProvider",
        lambda **_: pytest.fail("continuity enqueue must not construct a provider"),
    )
    monkeypatch.setattr(
        NasMarketStore,
        "reconcile_control_pointer",
        lambda *_args, **_kwargs: pytest.fail("continuity enqueue must not reconcile pointer"),
    )

    assert cli_module.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "gaps"
    assert payload["writes_control_state"] is True
    assert payload["provider_requests"] == 0
    assert payload["writes_parquet"] is False
    assert payload["writes_manifest"] is False
    assert payload["writes_pointer"] is False
    assert payload["created_count"] == 1
    after_dataset = _fingerprint(control, dataset_root)
    assert after_dataset.dataset_tree == before_dataset.dataset_tree
    assert after_dataset.calendar_hash == before_dataset.calendar_hash
    assert after_dataset.calendar_mtime_ns == before_dataset.calendar_mtime_ns
    assert after_dataset.calendar_schema == before_dataset.calendar_schema
    with duckdb.connect(str(control), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM repair_jobs").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM repair_attempts").fetchone()[0] == 0


def test_missing_market_storage_gets_create_no_runtime_paths(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    _empty_dataset(dataset_root)
    settings = _settings(tmp_path, dataset_root)
    runtime = tmp_path / "runtime"

    paths = [
        "/api/v1/market/summary",
        "/api/v1/market/status",
        "/api/v1/market/supplemental",
        "/api/v1/market/history/dates",
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        f"/api/v1/analysis/market-regime/snapshots/{AS_OF}",
        f"/api/v1/analysis/sector-rotation?as_of={AS_OF}&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
    ]
    responses = [_api_get(path, settings, raise_app_exceptions=False) for path in paths]

    assert all(response.status_code < 500 for response in responses)
    assert [response.status_code for response in responses[-3:]] == [200, 404, 200]
    assert [response.json()["status"] for response in responses[-3::2]] == [
        "empty",
        "empty",
    ]
    assert not runtime.exists()

    enabled = settings.model_copy(update={"akshare_supplemental_enabled": True})
    response = _api_get("/api/v1/market/supplemental", enabled)
    assert response.status_code == 200
    assert not runtime.exists()


def test_corrupt_control_schema_is_503_without_implicit_migration(tmp_path: Path) -> None:
    dataset_root = tmp_path / "dataset"
    _empty_dataset(dataset_root)
    settings = _settings(tmp_path, dataset_root)
    control = settings.market_data_dir / settings.market_database_name
    control.parent.mkdir(parents=True)
    connection = duckdb.connect(str(control))
    try:
        connection.execute("CREATE TABLE unrelated(value INTEGER)")
    finally:
        connection.close()
    before = _fingerprint(control, dataset_root)

    response = _api_get("/api/v1/market/status", settings, raise_app_exceptions=False)

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "market_storage_unavailable",
        "storage_status": "unavailable",
        "reason_code": "market_control_read_failed",
    }
    assert _fingerprint(control, dataset_root) == before


@pytest.mark.parametrize(
    "failure",
    ["missing_table", "invalid_payload", "exclusive_lock"],
)
def test_calendar_status_read_failures_are_structured_and_read_only(
    tmp_path: Path,
    failure: str,
) -> None:
    dataset_root = tmp_path / "dataset"
    settings = _settings(tmp_path, dataset_root)
    control = _publish_fixture(settings, dataset_root)
    calendar_path = settings.local_control_dir / settings.calendar_sync_database_name
    connection = sqlite3.connect(calendar_path)
    if failure == "missing_table":
        connection.execute("DROP TABLE calendar_sync_state")
        connection.commit()
    elif failure == "invalid_payload":
        connection.execute(
            "INSERT OR REPLACE INTO calendar_sync_state (singleton, payload_json) VALUES (1, '[]')"
        )
        connection.commit()
    before = _fingerprint(control, dataset_root, calendar_path)
    if failure == "exclusive_lock":
        connection.execute("BEGIN EXCLUSIVE")
    else:
        connection.close()

    try:
        response = _api_get(
            "/api/v1/market/status",
            settings,
            raise_app_exceptions=False,
        )
    finally:
        if failure == "exclusive_lock":
            connection.rollback()
            connection.close()

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "calendar_control_read_failed",
        "storage_status": "unavailable",
    }
    assert _fingerprint(control, dataset_root, calendar_path) == before


def test_calendar_select_only_reader_supports_relative_database_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    writer = CalendarSyncStore(Path("control/calendar.sqlite3"))
    before = _sha256(writer.path)

    state = CalendarSyncStore(writer.path, initialize=False).state()

    assert state.last_attempt_at is None
    assert _sha256(writer.path) == before


@pytest.mark.parametrize(
    "dataset_state",
    [
        "missing_root",
        "missing_sentinel",
        "invalid_sentinel",
        "missing_manifest",
        "manifest_array",
        "missing_generation",
        "wrong_generation_type",
        "wrong_file_type",
    ],
)
def test_direct_analysis_readers_fail_closed_for_unavailable_dataset_metadata(
    tmp_path: Path,
    dataset_state: str,
) -> None:
    dataset_root = tmp_path / "dataset"
    if dataset_state != "missing_root":
        dataset_root.mkdir()
        if dataset_state != "missing_sentinel":
            sentinel: object = {
                "dataset": "stock-eva-market",
                "schema_version": 2,
            }
            if dataset_state == "invalid_sentinel":
                sentinel = []
            (dataset_root / ".stock-eva-dataset.json").write_text(
                json.dumps(sentinel),
                encoding="utf-8",
            )
        if dataset_state not in {"missing_sentinel", "missing_manifest"}:
            manifest: object = {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "generation-empty",
                "files": [],
            }
            if dataset_state == "manifest_array":
                manifest = []
            elif dataset_state == "missing_generation":
                del manifest["generation"]
            elif dataset_state == "wrong_generation_type":
                manifest["generation"] = 7
            elif dataset_state == "wrong_file_type":
                manifest["files"] = [
                    {
                        "path": "bars/source=baostock/date=2026-07-23.parquet",
                        "sha256": "0" * 64,
                        "trade_date": AS_OF.isoformat(),
                        "source": "baostock",
                        "row_count": "1",
                    }
                ]
            (dataset_root / "manifest.json").write_text(
                json.dumps(manifest),
                encoding="utf-8",
            )
    settings = _settings(tmp_path, dataset_root)
    before = _tree(dataset_root) if dataset_root.exists() else None

    responses = [
        _api_get(
            f"/api/v1/analysis/market-regime?as_of={AS_OF}",
            settings,
            raise_app_exceptions=False,
        ),
        _api_get(
            f"/api/v1/analysis/sector-rotation?as_of={AS_OF}"
            f"&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}",
            settings,
            raise_app_exceptions=False,
        ),
    ]

    assert [response.status_code for response in responses] == [503, 503]
    assert [response.json()["detail"] for response in responses] == [
        {"code": "market_storage_unavailable", "storage_status": "unavailable"},
        {"code": "market_storage_unavailable", "storage_status": "unavailable"},
    ]
    assert not (tmp_path / "runtime").exists()
    if before is None:
        assert not dataset_root.exists()
    else:
        assert _tree(dataset_root) == before


@pytest.mark.parametrize(
    "manifest_attack",
    ["boolean_row_count", "duplicate_path", "duplicate_partition"],
)
@pytest.mark.parametrize(
    "analysis_path",
    [
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        (
            f"/api/v1/analysis/sector-rotation?as_of={AS_OF}"
            f"&taxonomy_id={TAXONOMY_BAOSTOCK_INDUSTRY}"
        ),
    ],
)
def test_direct_analysis_readers_reject_ambiguous_manifest_inventory(
    tmp_path: Path,
    manifest_attack: str,
    analysis_path: str,
) -> None:
    dataset_root = tmp_path / "dataset"
    settings = _settings(tmp_path, dataset_root)
    control = _publish_fixture(settings, dataset_root)
    manifest_path = dataset_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    original = manifest["files"][0]
    duplicate = dict(original)
    if manifest_attack == "boolean_row_count":
        original["row_count"] = True
    elif manifest_attack == "duplicate_path":
        duplicate["trade_date"] = (AS_OF - timedelta(days=1)).isoformat()
        manifest["files"].append(duplicate)
    else:
        source_path = dataset_root / original["path"]
        duplicate_path = source_path.with_name(f"duplicate-{source_path.name}")
        shutil.copyfile(source_path, duplicate_path)
        duplicate["path"] = str(duplicate_path.relative_to(dataset_root))
        manifest["files"].append(duplicate)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    before = _fingerprint(control, dataset_root)

    response = _api_get(
        analysis_path,
        settings,
        raise_app_exceptions=False,
    )

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "code": "market_storage_unavailable",
        "storage_status": "unavailable",
    }
    assert _fingerprint(control, dataset_root) == before


def test_local_mode_missing_and_corrupt_storage_never_initialize_on_get(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path, tmp_path / "unused").model_copy(
        update={"local_market_dataset_root": None}
    )
    runtime = tmp_path / "runtime"

    missing = _api_get("/api/v1/market/status", settings)

    assert missing.status_code == 200
    assert not runtime.exists()

    control = settings.market_data_dir / settings.market_database_name
    control.parent.mkdir(parents=True)
    connection = duckdb.connect(str(control))
    try:
        connection.execute("CREATE TABLE unrelated(value INTEGER)")
    finally:
        connection.close()
    before = (_sha256(control), control.stat().st_mtime_ns, _schema(control))

    corrupt = _api_get(
        "/api/v1/market/status",
        settings,
        raise_app_exceptions=False,
    )
    regime = _api_get(
        f"/api/v1/analysis/market-regime?as_of={AS_OF}",
        settings,
        raise_app_exceptions=False,
    )

    assert corrupt.status_code == 503
    assert corrupt.json()["detail"]["reason_code"] == "market_control_read_failed"
    assert regime.status_code == 503
    assert regime.json()["detail"]["code"] == "market_storage_unavailable"
    assert (_sha256(control), control.stat().st_mtime_ns, _schema(control)) == before


def test_explicit_writer_and_reconciler_still_own_schema_and_pointer(tmp_path: Path) -> None:
    local_path = tmp_path / "local" / "market.duckdb"
    writer = MarketStore(local_path, temp_directory=tmp_path / "local-temp")

    writer.save_refresh(_bars(), _ready_result(run_id="local-ready"), publish=True)

    assert writer.published_refresh().run_id == "local-ready"
    assert {table for table, _columns in _schema(local_path)} >= {
        "daily_bars",
        "published_daily_bars",
        "published_snapshots",
        "refresh_runs",
    }

    dataset_root = tmp_path / "dataset"
    _empty_dataset(dataset_root)
    publisher = NasMarketStore(
        MarketStore(tmp_path / "publisher.duckdb"),
        dataset_root,
        tmp_path / "publisher-staging",
    )
    publisher.save_refresh(_bars(), _ready_result(run_id="dataset-ready"), publish=True)
    repaired_control = MarketStore(tmp_path / "repaired" / "market.duckdb")

    repaired = NasMarketStore(
        repaired_control,
        dataset_root,
        tmp_path / "repair-staging",
    ).reconcile_control_pointer()

    assert repaired is not None
    assert repaired.run_id == repaired_control.published_refresh().run_id
    assert repaired.requested_date == AS_OF


def test_writable_reconciler_repairs_legacy_control_schema_before_pointer_read(
    tmp_path: Path,
) -> None:
    dataset_root = tmp_path / "dataset"
    _empty_dataset(dataset_root)
    publisher = NasMarketStore(
        MarketStore(tmp_path / "publisher.duckdb"),
        dataset_root,
        tmp_path / "publisher-staging",
    )
    publisher._publish_bars(_bars())
    dataset_before = _tree(dataset_root)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    initialized = control._connect()
    initialized.close()
    connection = duckdb.connect(str(control.path))
    try:
        connection.execute("DROP TABLE published_snapshots")
        connection.execute("DROP TABLE published_daily_bars")
        connection.execute("ALTER TABLE refresh_runs DROP COLUMN coverage_ratio")
        connection.execute("ALTER TABLE refresh_runs DROP COLUMN request_key")
        connection.execute("ALTER TABLE refresh_runs DROP COLUMN run_kind")
        connection.execute("ALTER TABLE refresh_runs DROP COLUMN quality_issues")
    finally:
        connection.close()

    repaired = NasMarketStore(
        control,
        dataset_root,
        tmp_path / "repair-staging",
    ).reconcile_control_pointer()

    assert repaired is not None
    assert repaired.requested_date == AS_OF
    assert control.published_refresh().run_id == repaired.run_id
    assert {table for table, _columns in _schema(control.path)} >= {
        "published_snapshots",
        "published_daily_bars",
        "refresh_runs",
    }
    assert _tree(dataset_root) == dataset_before


def test_reconciler_maps_control_schema_initialization_failure_to_dataset_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dataset_root = tmp_path / "dataset"
    _empty_dataset(dataset_root)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    store = NasMarketStore(control, dataset_root, tmp_path / "staging")

    def fail_initialization() -> None:
        raise OSError("fixture control unavailable")

    monkeypatch.setattr(control, "initialize_schema", fail_initialization, raising=False)

    with pytest.raises(DatasetError, match="control schema"):
        store.reconcile_control_pointer()

    assert not control.path.exists()
    assert not (tmp_path / "staging").exists()


def test_lifespan_writer_owner_repairs_legacy_control_schema(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dataset_root = tmp_path / "dataset"
    _empty_dataset(dataset_root)
    publisher = NasMarketStore(
        MarketStore(tmp_path / "publisher.duckdb"),
        dataset_root,
        tmp_path / "publisher-staging",
    )
    publisher._publish_bars(_bars())
    settings = _settings(tmp_path, dataset_root).model_copy(update={"auto_refresh_enabled": True})
    control_path = settings.market_data_dir / settings.market_database_name
    control = MarketStore(control_path)
    initialized = control._connect()
    initialized.close()
    connection = duckdb.connect(str(control_path))
    try:
        connection.execute("DROP TABLE published_snapshots")
    finally:
        connection.close()

    class ReadyPreflight:
        def __init__(self, _settings) -> None:
            pass

        def inspect(self) -> StorageReadiness:
            return StorageReadiness(
                mode="local_dataset",
                status="ready",
                market_data_available=True,
                serving_source="local",
                mount_type="local",
                sentinel_status="ready",
                manifest_status="ready",
                dataset_generation="fixture",
            )

    async def idle_loop(_service, stop, **_kwargs) -> None:
        await stop.wait()

    monkeypatch.setattr(main_module, "settings", settings)
    monkeypatch.setattr(main_module, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr(main_module, "BaoStockProvider", lambda **_kwargs: object())
    monkeypatch.setattr(
        main_module,
        "build_after_close_pipeline",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(main_module, "run_automation_loop", idle_loop)
    monkeypatch.setattr(main_module, "run_calendar_sync_loop", idle_loop)

    async def run_lifespan() -> None:
        async with main_module.lifespan(None):
            pass

    asyncio.run(run_lifespan())

    pointer = MarketStore(control_path, read_only=True).published_refresh()
    assert pointer is not None
    assert pointer.requested_date == AS_OF


def test_market_store_reader_sees_previous_commit_during_writer_transaction(
    tmp_path: Path,
) -> None:
    store = MarketStore(tmp_path / "market.duckdb")
    store.save_refresh(_bars(), _ready_result(), publish=True)
    reader = MarketStore(store.path, read_only=True)
    writer = store._connect()
    try:
        writer.begin()
        writer.execute("UPDATE daily_bars SET close = 99 WHERE symbol = 'sh.600000'")

        assert reader.symbol_bars("sh.600000", AS_OF, AS_OF)[0].close == 10.5

        writer.commit()
    finally:
        writer.close()

    assert reader.symbol_bars("sh.600000", AS_OF, AS_OF)[0].close == 99


def test_read_only_local_store_uses_duckdb_read_only_and_rejects_writer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "market.duckdb"
    MarketStore(path).save_refresh(_bars(), _ready_result(), publish=True)
    before = (_sha256(path), path.stat().st_mtime_ns, _schema(path))
    real_connect = market_store_module.duckdb.connect
    calls: list[dict[str, object]] = []

    def recording_connect(*args, **kwargs):
        calls.append(dict(kwargs))
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(market_store_module.duckdb, "connect", recording_connect)
    reader = MarketStore(path, read_only=True)

    assert reader.symbol_bars("sh.600000", AS_OF, AS_OF)[0].close == 10.5
    assert calls == [{"read_only": True}]
    with pytest.raises(RuntimeError, match="read-only market store"):
        reader._connect()
    assert (_sha256(path), path.stat().st_mtime_ns, _schema(path)) == before


def test_only_exact_duckdb_configuration_mismatch_uses_select_only_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "market.duckdb"
    MarketStore(path).save_refresh(_bars(), _ready_result(), publish=True)
    real_connect = market_store_module.duckdb.connect
    calls: list[dict[str, object]] = []

    def configuration_mismatch_then_connect(*args, **kwargs):
        calls.append(dict(kwargs))
        if len(calls) == 1:
            raise duckdb.ConnectionException(
                "Connection Error: Can't open a connection to same database file with a "
                "different configuration than existing connections"
            )
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(
        market_store_module.duckdb,
        "connect",
        configuration_mismatch_then_connect,
    )

    assert MarketStore(path, read_only=True).available_dates() == [AS_OF]
    assert calls == [{"read_only": True}, {}]


@pytest.mark.parametrize(
    "error",
    [
        duckdb.IOException("fixture read-only open failed"),
        duckdb.ConnectionException(
            "Connection Error: Can't open a connection to same database file with a different "
            "configuration than existing connections: not-the-exact-error"
        ),
    ],
)
def test_read_only_open_errors_other_than_exact_configuration_mismatch_fail_closed(
    tmp_path: Path,
    monkeypatch,
    error: duckdb.Error,
) -> None:
    path = tmp_path / "market.duckdb"
    MarketStore(path).save_refresh(_bars(), _ready_result(), publish=True)
    calls = 0

    def unavailable(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise error

    monkeypatch.setattr(market_store_module.duckdb, "connect", unavailable)

    with pytest.raises(MarketStoreReadError, match="cannot be read"):
        MarketStore(path, read_only=True).available_dates()

    assert calls == 1


def test_read_only_dataset_store_rejects_writer_lifecycle_before_any_mutation(
    tmp_path: Path,
) -> None:
    dataset_root = tmp_path / "dataset"
    _empty_dataset(dataset_root)
    control = tmp_path / "control" / "market.duckdb"
    staging = tmp_path / "staging"
    store = NasMarketStore(
        MarketStore(control, read_only=True),
        dataset_root,
        staging,
    )
    before = _tree(dataset_root)

    with pytest.raises(RuntimeError, match="read-only market store"):
        store.save_refresh(_bars(), _ready_result(), publish=True)
    with pytest.raises(RuntimeError, match="read-only market store"):
        store.reconcile_control_pointer()

    assert _tree(dataset_root) == before
    assert not control.exists()
    assert not staging.exists()
