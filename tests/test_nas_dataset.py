import asyncio
import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import httpx
import pytest
from pydantic import ValidationError

from backend.app import cli
from backend.app.api.market import get_market_store
from backend.app.config import Settings
from backend.app.main import app
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.service import MarketSummaryService
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import DatasetError, NasMarketStore, _ManifestLock, _sha256
from backend.app.storage.mirror import MarketDatasetMirror
from backend.app.storage.models import StorageReadiness


def _bars() -> list:
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
                "2026-07-23",
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
            ],
            [
                "2026-07-23",
                "sh.000001",
                "3500",
                "3520",
                "3480",
                "3510",
                "3490",
                "100",
                "1000",
                "3",
                "",
                "1",
                "0.57",
                "0",
            ],
            [
                "2026-07-23",
                "sz.399001",
                "11000",
                "11050",
                "10800",
                "10900",
                "11020",
                "100",
                "1000",
                "3",
                "",
                "1",
                "-1.09",
                "0",
            ],
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


def _root(tmp_path: Path) -> Path:
    root = (tmp_path / "nas-dataset").resolve()
    root.mkdir()
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
    return root


def _ready_result() -> RefreshResult:
    return RefreshResult(
        run_id="nas-ready",
        request_key="daily:baostock:2026-07-23:all-main-board",
        requested_date=date(2026, 7, 23),
        source="baostock",
        status="ready",
        requested_count=3,
        succeeded_count=3,
        coverage_ratio=1.0,
        started_at=datetime(2026, 7, 24, tzinfo=UTC),
        completed_at=datetime(2026, 7, 24, 1, tzinfo=UTC),
    )


def _bars_on(trade_date: date) -> list:
    return [bar.model_copy(update={"trade_date": trade_date}) for bar in _bars()]


def _ready_result_on(trade_date: date) -> RefreshResult:
    return _ready_result().model_copy(
        update={
            "run_id": f"nas-ready-{trade_date.isoformat()}",
            "request_key": f"daily:baostock:{trade_date.isoformat()}:all-main-board",
            "requested_date": trade_date,
        }
    )


def _dataset_fingerprint(root: Path) -> tuple[tuple[str, str, int, int], ...]:
    entries: list[tuple[str, str, int, int]] = []
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        if path.is_dir():
            entries.append((relative, "directory", 0, path.stat().st_mtime_ns))
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        stat = path.stat()
        entries.append((relative, digest, stat.st_size, stat.st_mtime_ns))
    return tuple(entries)


def test_verified_ready_inventory_enumerates_all_current_manifest_partitions(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    store = NasMarketStore(control, root, tmp_path / "staging")
    sessions = (date(2026, 7, 21), date(2026, 7, 23))
    for session in sessions:
        store.save_refresh(
            _bars_on(session),
            _ready_result_on(session),
            publish=True,
        )
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    verified_at = datetime(2026, 8, 13, 9, 30, tzinfo=UTC)

    inventory = store.verified_ready_session_inventory(now=verified_at)

    assert inventory.status == "ready"
    assert inventory.source == "baostock"
    assert inventory.manifest_generation == manifest["generation"]
    assert inventory.sessions == sessions
    assert inventory.verified_at == verified_at
    assert len(inventory.manifest_identity) == 64
    assert "/" not in inventory.manifest_identity
    assert "path" not in inventory.model_dump(mode="json")
    assert control.published_refresh().requested_date == sessions[-1]
    with pytest.raises(ValidationError, match="frozen"):
        inventory.sessions = ()


@pytest.mark.parametrize("corruption", ["missing", "wrong_hash"])
def test_verified_ready_inventory_rejects_missing_or_wrong_hash_object(
    tmp_path: Path,
    corruption: str,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    store.verified_ready_session_inventory()
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parquet = root / manifest["files"][0]["path"]
    if corruption == "missing":
        parquet.unlink()
        expected = "missing parquet"
    else:
        parquet.write_bytes(b"corrupt-after-success")
        expected = "checksum mismatch"
    before = _dataset_fingerprint(root)

    with pytest.raises(DatasetError, match=expected):
        store.verified_ready_session_inventory()

    assert _dataset_fingerprint(root) == before


@pytest.mark.parametrize("corruption", ["wrong_schema", "wrong_row_count"])
def test_verified_ready_inventory_rejects_wrong_schema_and_row_count(
    tmp_path: Path,
    corruption: str,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    store.verified_ready_session_inventory()
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if corruption == "wrong_row_count":
        manifest["files"][0]["row_count"] += 1
        expected = "row count mismatch"
    else:
        bad_file = root / "bars" / "wrong-schema.parquet"
        connection = duckdb.connect(":memory:")
        try:
            connection.execute(
                "COPY (SELECT 1 AS wrong_column) TO ? (FORMAT PARQUET)",
                [str(bad_file)],
            )
        finally:
            connection.close()
        manifest["files"][0].update(
            {
                "path": "bars/wrong-schema.parquet",
                "sha256": _sha256(bad_file),
                "row_count": 1,
            }
        )
        expected = "incompatible schema"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    before = _dataset_fingerprint(root)

    with pytest.raises(DatasetError, match=expected):
        store.verified_ready_session_inventory()

    assert _dataset_fingerprint(root) == before


def test_verified_ready_inventory_is_bound_to_one_manifest_snapshot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    sessions = (date(2026, 7, 21), date(2026, 7, 23))
    for session in sessions:
        store.save_refresh(
            _bars_on(session),
            _ready_result_on(session),
            publish=True,
        )
    manifest_path = root / "manifest.json"
    captured = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_identity = hashlib.sha256(
        json.dumps(captured, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    original_manifest = store._manifest
    calls = 0

    def switch_generation_after_capture() -> dict[str, object]:
        nonlocal calls
        calls += 1
        snapshot = original_manifest()
        replacement = {
            "dataset": "stock-eva-market",
            "schema_version": 2,
            "generation": "generation-after-capture",
            "files": [],
        }
        manifest_path.write_text(json.dumps(replacement), encoding="utf-8")
        return snapshot

    monkeypatch.setattr(store, "_manifest", switch_generation_after_capture)

    inventory = store.verified_ready_session_inventory()

    assert calls == 1
    assert inventory.manifest_generation == captured["generation"]
    assert inventory.manifest_identity == expected_identity
    assert inventory.sessions == sessions
    assert json.loads(manifest_path.read_text())["generation"] == "generation-after-capture"


def test_lightweight_manifest_dates_is_not_the_continuity_inventory_contract(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    store.verified_ready_session_inventory()
    parquet = next((root / "bars").rglob("*.parquet"))
    parquet.write_bytes(b"corrupt-after-inventory")

    assert store.manifest_dates() == [date(2026, 7, 23)]
    with pytest.raises(DatasetError, match="checksum mismatch"):
        store.verified_ready_session_inventory()


def test_verified_ready_inventory_accepts_valid_empty_manifest(tmp_path: Path) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )

    inventory = store.verified_ready_session_inventory()

    assert inventory.status == "ready"
    assert inventory.sessions == ()


def test_verified_ready_inventory_rejects_symlink_escape_without_writes(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    published = root / manifest["files"][0]["path"]
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(published.read_bytes())
    escaped = root / "bars" / "escaped.parquet"
    escaped.symlink_to(outside)
    manifest["files"][0].update(
        {
            "path": "bars/escaped.parquet",
            "sha256": _sha256(outside),
        }
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    before = _dataset_fingerprint(root)

    with pytest.raises(DatasetError, match="unsafe file path"):
        store.verified_ready_session_inventory()

    assert _dataset_fingerprint(root) == before


def test_verified_ready_inventory_rejects_path_like_generation_without_writes(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["generation"] = "/private/dataset-generation"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    before = _dataset_fingerprint(root)

    with pytest.raises(DatasetError, match="inventory metadata"):
        store.verified_ready_session_inventory()

    assert _dataset_fingerprint(root) == before


def test_verified_local_mirror_is_idempotent_and_manifest_readable(tmp_path: Path) -> None:
    source = _root(tmp_path)
    source_store = NasMarketStore(
        MarketStore(tmp_path / "source-control.duckdb"),
        source,
        tmp_path / "source-staging",
    )
    source_store.save_refresh(_bars(), _ready_result(), publish=True)
    destination = tmp_path / "local-mirror"

    first = MarketDatasetMirror(source, destination).sync()
    second = MarketDatasetMirror(source, destination).sync()

    assert first.status == "copied"
    assert first.file_count == 1
    assert first.row_count == 3
    assert first.copied_bytes > 0
    assert second.status == "reused"
    assert second.copied_bytes == 0
    mirrored = NasMarketStore(
        MarketStore(tmp_path / "mirror-control.duckdb"),
        destination,
        tmp_path / "mirror-staging",
    )
    mirrored.validate_readiness()
    assert mirrored.manifest_dates() == [date(2026, 7, 23)]
    mirrored.reconcile_control_pointer()
    summary = MarketSummaryService(mirrored).latest(expected_session=date(2026, 7, 23))
    assert summary.status == "ready"
    assert summary.as_of == date(2026, 7, 23)


def test_older_archive_never_replaces_a_newer_local_dataset(tmp_path: Path) -> None:
    source = _root(tmp_path)
    source_store = NasMarketStore(
        MarketStore(tmp_path / "source-control.duckdb"),
        source,
        tmp_path / "source-staging",
    )
    source_store.save_refresh(_bars(), _ready_result(), publish=True)
    destination = tmp_path / "local-mirror"
    MarketDatasetMirror(source, destination).sync()
    destination_store = NasMarketStore(
        MarketStore(tmp_path / "destination-control.duckdb"),
        destination,
        tmp_path / "destination-staging",
    )
    destination_store.save_refresh(
        _bars_on(date(2026, 7, 24)),
        _ready_result_on(date(2026, 7, 24)),
        publish=True,
    )

    result = MarketDatasetMirror(source, destination).sync()

    assert result.status == "destination_newer"
    assert destination_store.manifest_dates() == [
        date(2026, 7, 23),
        date(2026, 7, 24),
    ]


def test_interrupted_mirror_keeps_the_previous_manifest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = _root(tmp_path)
    source_store = NasMarketStore(
        MarketStore(tmp_path / "source-control.duckdb"),
        source,
        tmp_path / "source-staging",
    )
    source_store.save_refresh(_bars(), _ready_result(), publish=True)
    destination = tmp_path / "local-mirror"
    MarketDatasetMirror(source, destination).sync()
    original_manifest = (destination / "manifest.json").read_bytes()
    source_store.save_refresh(
        _bars_on(date(2026, 7, 24)),
        _ready_result_on(date(2026, 7, 24)),
        publish=True,
    )
    real_replace = os.replace

    def interrupt_manifest(source_path, destination_path) -> None:
        if Path(destination_path).name == "manifest.json":
            raise OSError("simulated interruption before manifest publication")
        real_replace(source_path, destination_path)

    monkeypatch.setattr("backend.app.storage.mirror.os.replace", interrupt_manifest)

    with pytest.raises(OSError, match="simulated interruption"):
        MarketDatasetMirror(source, destination).sync()

    assert (destination / "manifest.json").read_bytes() == original_manifest
    unchanged = NasMarketStore(
        MarketStore(tmp_path / "unchanged-control.duckdb"),
        destination,
        tmp_path / "unchanged-staging",
    )
    unchanged.validate_readiness()
    assert unchanged.manifest_dates() == [date(2026, 7, 23)]


def test_invalid_source_never_replaces_a_valid_local_mirror(tmp_path: Path) -> None:
    source = _root(tmp_path)
    source_store = NasMarketStore(
        MarketStore(tmp_path / "source-control.duckdb"),
        source,
        tmp_path / "source-staging",
    )
    source_store.save_refresh(_bars(), _ready_result(), publish=True)
    destination = tmp_path / "local-mirror"
    MarketDatasetMirror(source, destination).sync()
    original = (destination / "manifest.json").read_bytes()
    source_file = next((source / "bars").rglob("*.parquet"))
    source_file.write_bytes(b"corrupt")

    with pytest.raises(DatasetError):
        MarketDatasetMirror(source, destination).sync()

    assert (destination / "manifest.json").read_bytes() == original
    NasMarketStore(
        MarketStore(tmp_path / "mirror-control.duckdb"),
        destination,
        tmp_path / "mirror-staging",
    ).validate_readiness()


def test_nas_manifest_publication_is_readable_and_control_pointer_is_local(tmp_path: Path) -> None:
    root = _root(tmp_path)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    store = NasMarketStore(control, root, tmp_path / "staging")

    store.save_refresh(_bars(), _ready_result(), publish=True)

    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["files"]) == 1
    assert manifest["files"][0]["path"].startswith("bars/source=baostock/")
    assert "_staging" not in manifest["files"][0]["path"]
    assert list((root / "_staging").rglob("*.partial")) == []
    assert store.published_refresh().run_id == "nas-ready"
    assert control.count_bars() == 0
    assert [
        bar.symbol for bar in store.symbol_bars("sh.600000", date(2026, 7, 23), date(2026, 7, 23))
    ] == ["sh.600000"]
    summary = MarketSummaryService(store).latest(expected_session=date(2026, 7, 23))
    assert summary.status == "ready"
    assert [item.symbol for item in summary.indexes] == ["sh.000001", "sz.399001"]


def test_reader_reuses_a_validated_generation_across_requests(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    original = NasMarketStore._validate_parquet
    calls = 0

    def counted(path: Path, expected_rows: int) -> None:
        nonlocal calls
        calls += 1
        original(path, expected_rows)

    monkeypatch.setattr(NasMarketStore, "_validate_parquet", staticmethod(counted))
    first = store.bars_for(date(2026, 7, 23), "baostock")
    second_store = NasMarketStore(
        MarketStore(tmp_path / "second-control.duckdb"),
        root,
        tmp_path / "second-staging",
    )
    second = second_store.bars_for(date(2026, 7, 23), "baostock")

    assert len(first) == len(second) == 3
    assert calls == 1
    second_store.validate_readiness()
    assert calls == 2


def test_failed_readback_never_moves_manifest_or_published_pointer(
    tmp_path: Path, monkeypatch
) -> None:
    root = _root(tmp_path)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    store = NasMarketStore(control, root, tmp_path / "staging")
    original_manifest = (root / "manifest.json").read_text(encoding="utf-8")

    def interrupted(*_args, **_kwargs) -> None:
        raise DatasetError("simulated_readback_failure")

    monkeypatch.setattr(store.publisher, "readback_and_verify", interrupted)
    with pytest.raises(DatasetError, match="simulated_readback_failure"):
        store.save_refresh(_bars(), _ready_result(), publish=True)

    assert (root / "manifest.json").read_text(encoding="utf-8") == original_manifest
    assert control.published_refresh() is None


def test_reader_rejects_partial_or_unsafe_manifest_without_local_fallback(tmp_path: Path) -> None:
    root = _root(tmp_path)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "bad",
                "files": [{"path": "_staging/untrusted.parquet"}],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(DatasetError, match="non-published"):
        NasMarketStore(control, root, tmp_path / "staging").available_dates()


def test_multi_date_publication_replaces_manifest_once(tmp_path: Path, monkeypatch) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    first = _bars()[0]
    second = first.model_copy(
        update={
            "trade_date": date(2026, 7, 24),
            "source_record_id": "sh.600000:2026-07-24",
        }
    )
    original_publish = store.publisher.publish_manifest
    calls = 0

    def record_publish(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(store.publisher, "publish_manifest", record_publish)
    store.upsert_bars([first, second])

    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert calls == 1
    assert {item["trade_date"] for item in manifest["files"]} == {
        "2026-07-23",
        "2026-07-24",
    }


def test_api_maps_tampered_nas_dataset_to_safe_503(tmp_path: Path) -> None:
    root = _root(tmp_path)
    control = MarketStore(tmp_path / "control" / "market.duckdb")
    store = NasMarketStore(control, root, tmp_path / "staging")
    store.save_refresh(_bars(), _ready_result(), publish=True)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["files"][0]["sha256"] = "0" * 64
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    app.dependency_overrides[get_market_store] = lambda: store

    async def request_summary():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/api/v1/market/summary")

    try:
        response = asyncio.run(request_summary())
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "market_storage_unavailable"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda item: item.pop("sha256"), "checksum metadata"),
        (lambda item: item.update({"row_count": 999}), "row count mismatch"),
        (
            lambda item: item.update({"path": "bars/_staging/untrusted.parquet"}),
            "non-published",
        ),
        (
            lambda item: item.update({"path": "bars/year=2026/quarantine/untrusted.parquet"}),
            "non-published",
        ),
    ],
)
def test_reader_rejects_required_manifest_integrity_metadata(
    tmp_path: Path,
    mutation,
    message: str,
) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    mutation(manifest["files"][0])
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(DatasetError, match=message):
        store.validate_readiness()


def test_reader_rejects_schema_mismatch_and_export_revalidates(tmp_path: Path) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    store.save_refresh(_bars(), _ready_result(), publish=True)
    bad_file = root / "bars" / "bad.parquet"
    bad_file.parent.mkdir(parents=True, exist_ok=True)
    import duckdb

    connection = duckdb.connect(":memory:")
    try:
        connection.execute("COPY (SELECT 1 AS wrong_column) TO ? (FORMAT PARQUET)", [str(bad_file)])
    finally:
        connection.close()
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["files"] = [
        {
            "path": "bars/bad.parquet",
            "sha256": _sha256(bad_file),
            "trade_date": "2026-07-23",
            "source": "baostock",
            "row_count": 1,
        }
    ]
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(DatasetError, match="incompatible schema"):
        store.export_date(date(2026, 7, 23), tmp_path / "export")


def test_reader_rejects_legacy_fore_factor_manifest_v1(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["schema_version"] = 1
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )

    with pytest.raises(DatasetError, match="unsupported schema"):
        store.validate_readiness()


def test_manifest_lock_and_generation_cas_prevent_lost_updates(tmp_path: Path, monkeypatch) -> None:
    root = _root(tmp_path)
    store = NasMarketStore(
        MarketStore(tmp_path / "control" / "market.duckdb"),
        root,
        tmp_path / "staging",
    )
    with _ManifestLock(store.manifest_lock_path):
        with pytest.raises(DatasetError, match="already running"):
            store.upsert_bars([_bars()[0]])

    original_readback = store.publisher.readback_and_verify

    def change_generation(*args, **kwargs):
        original_readback(*args, **kwargs)
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        manifest["generation"] = "generation-interloper"
        (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    monkeypatch.setattr(store.publisher, "readback_and_verify", change_generation)
    with pytest.raises(DatasetError, match="changed during staging"):
        store.upsert_bars([_bars()[0]])
    assert json.loads((root / "manifest.json").read_text(encoding="utf-8"))["generation"] == (
        "generation-interloper"
    )


def test_cli_rejects_invalid_nas_dataset_without_traceback(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    root = _root(tmp_path)
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "invalid",
                "files": [
                    {
                        "path": "bars/missing.parquet",
                        "trade_date": "2026-07-23",
                        "source": "baostock",
                        "row_count": 1,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    settings = Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "temp",
        nas_market_dataset_root=root,
    )

    class ReadyPreflight:
        def __init__(self, _settings) -> None:
            pass

        def inspect(self) -> StorageReadiness:
            return StorageReadiness(
                mode="nas",
                status="ready",
                market_data_available=True,
                serving_source="nas",
                sentinel_status="ready",
                manifest_status="ready",
            )

    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(cli, "StoragePreflight", ReadyPreflight)
    monkeypatch.setattr("sys.argv", ["stock-eva", "refresh-runs"])

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["quality_issues"] == ["market_storage_unavailable"]
    assert payload["writes_market_data"] is False
