import asyncio
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from backend.app.api.market import get_market_store
from backend.app.main import app
from backend.app.market.models import RefreshResult
from backend.app.market.normalize import normalize_baostock_rows
from backend.app.market.service import MarketSummaryService
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import DatasetError, NasMarketStore


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
        json.dumps({"dataset": "stock-eva-market", "schema_version": 1}),
        encoding="utf-8",
    )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 1,
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
                "schema_version": 1,
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
