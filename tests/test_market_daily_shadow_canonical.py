from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from backend.app.market.daily_shadow_canonical import (
    DailyCanonicalReader,
    PublishedDailyCanonicalProjection,
)
from backend.app.market.daily_shadow_models import DailyCanonicalLineageState

TRADE_DATE = date(2026, 8, 10)
PARQUET_SCHEMA = (
    ("trade_date", "DATE"),
    ("symbol", "VARCHAR"),
    ("security_type", "VARCHAR"),
    ("exchange", "VARCHAR"),
    ("board", "VARCHAR"),
    ("open", "DOUBLE"),
    ("high", "DOUBLE"),
    ("low", "DOUBLE"),
    ("close", "DOUBLE"),
    ("preclose", "DOUBLE"),
    ("volume", "DOUBLE"),
    ("amount", "DOUBLE"),
    ("turnover_rate", "DOUBLE"),
    ("pct_change", "DOUBLE"),
    ("adjust_factor", "DOUBLE"),
    ("price_adjustment", "VARCHAR"),
    ("is_trading", "BOOLEAN"),
    ("is_suspended", "BOOLEAN"),
    ("is_st", "BOOLEAN"),
    ("source", "VARCHAR"),
    ("source_record_id", "VARCHAR"),
    ("ingested_at", "TIMESTAMP WITH TIME ZONE"),
    ("quality_status", "VARCHAR"),
    ("quality_issues", "JSON"),
)


def _row(
    symbol: str,
    *,
    security_type: str = "stock",
    exchange: str | None = None,
    is_trading: bool = True,
    is_suspended: bool = False,
    quality_status: str = "ready",
    close: float = 10.2,
) -> tuple[object, ...]:
    exchange = exchange or symbol[:2]
    return (
        TRADE_DATE,
        symbol,
        security_type,
        exchange,
        "main",
        10.0,
        10.4,
        9.9,
        close,
        9.8,
        1000.0,
        10200.0,
        1.0,
        4.0,
        None if security_type == "index" else 1.0,
        "none",
        is_trading,
        is_suspended,
        False,
        "baostock",
        f"baostock:{symbol}:{TRADE_DATE.isoformat()}",
        datetime(2026, 8, 10, 10, tzinfo=UTC),
        quality_status,
        "[]",
    )


def _tree(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    }


def _write_dataset(
    root: Path,
    *,
    rows: tuple[tuple[object, ...], ...] | None = None,
    descriptor_updates: dict[str, object] | None = None,
    manifest_updates: dict[str, object] | None = None,
) -> tuple[Path, dict[str, object]]:
    rows = rows or (
        _row("sh.600000"),
        _row("sz.000001"),
        _row("sh.600001", is_trading=False, is_suspended=True),
        _row("sh.000001", security_type="index"),
    )
    root.mkdir(parents=True)
    (root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2}, sort_keys=True),
        encoding="utf-8",
    )
    parquet = root / "bars" / "source=baostock" / "year=2026" / "month=08" / "pending.parquet"
    parquet.parent.mkdir(parents=True)
    connection = duckdb.connect(":memory:")
    try:
        columns = ",".join(f'"{name}" {kind}' for name, kind in PARQUET_SCHEMA)
        connection.execute(f"CREATE TABLE bars ({columns})")
        connection.executemany(
            f"INSERT INTO bars VALUES ({','.join('?' for _ in PARQUET_SCHEMA)})", rows
        )
        connection.execute("COPY bars TO ? (FORMAT PARQUET)", [str(parquet)])
    finally:
        connection.close()
    digest = hashlib.sha256(parquet.read_bytes()).hexdigest()
    final = parquet.with_name(f"date={TRADE_DATE.isoformat()}_{digest[:12]}.parquet")
    parquet.rename(final)
    descriptor: dict[str, object] = {
        "path": str(final.relative_to(root)),
        "row_count": len(rows),
        "sha256": digest,
        "source": "baostock",
        "trade_date": TRADE_DATE.isoformat(),
    }
    descriptor.update(descriptor_updates or {})
    manifest: dict[str, object] = {
        "dataset": "stock-eva-market",
        "files": [descriptor],
        "generation": "generation-r2f3-daily-shadow-legacy",
        "schema_version": 2,
    }
    manifest.update(manifest_updates or {})
    (root / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return final, manifest


def test_legacy_manifest_projects_exact_active_stock_universe_without_writes(tmp_path):
    root = tmp_path / "dataset"
    _write_dataset(root)
    before = _tree(root)

    result = DailyCanonicalReader(root, trade_date=TRADE_DATE).read()

    assert result.status == "ready"
    assert result.snapshot is not None
    snapshot = result.snapshot
    assert snapshot.lineage_state == DailyCanonicalLineageState.LEGACY_UNAVAILABLE
    assert snapshot.trade_date == TRADE_DATE
    assert snapshot.eligible_symbol_count == 2
    assert snapshot.excluded_symbol_count == 2
    assert tuple(row.symbol for row in snapshot.rows) == ("sh.600000", "sz.000001")
    assert len(snapshot.manifest_sha256) == 64
    assert len(snapshot.partition_sha256) == 64
    assert len(snapshot.canonical_universe_sha256) == 64
    assert len(snapshot.canonical_exclusion_sha256) == 64
    assert len(snapshot.symbol_mapping_sha256) == 64
    assert _tree(root) == before


def test_complete_r2f2_lineage_is_accepted_but_partial_lineage_fails_closed(tmp_path):
    lineage = {
        "provider_id": "baostock",
        "universe_id": "all-main-board",
        "evidence_id": "evidence-1",
        "evidence_sha256": "1" * 64,
        "candidate_id": "candidate-1",
        "candidate_manifest_sha256": "2" * 64,
        "gate_report_sha256": "3" * 64,
        "adapter_version": "baostock-evidence-v1",
        "source_schema_version": "daily_astock.v1",
    }
    complete = tmp_path / "complete"
    _write_dataset(complete, descriptor_updates=lineage)
    result = DailyCanonicalReader(complete, trade_date=TRADE_DATE).read()
    assert result.status == "ready"
    assert result.snapshot is not None
    assert result.snapshot.lineage_state == DailyCanonicalLineageState.R2F2_VERIFIED

    partial = tmp_path / "partial"
    _write_dataset(partial, descriptor_updates={"provider_id": "baostock"})
    before = _tree(partial)
    result = DailyCanonicalReader(partial, trade_date=TRADE_DATE).read()
    assert result.status == "unavailable"
    assert result.unavailable_reason == "CANONICAL_DESCRIPTOR_INVALID"
    assert _tree(partial) == before


@pytest.mark.parametrize(
    ("descriptor_updates", "manifest_updates"),
    [
        ({"row_count": 99}, {}),
        ({"sha256": "0" * 64}, {}),
        ({"source": "tickflow"}, {}),
        ({"trade_date": "2026-08-09"}, {}),
        ({"extra": "not-allowed"}, {}),
        ({}, {"extra": "not-allowed"}),
    ],
)
def test_descriptor_schema_hash_count_source_and_date_fail_closed(
    tmp_path, descriptor_updates, manifest_updates
):
    root = tmp_path / "dataset"
    _write_dataset(
        root,
        descriptor_updates=descriptor_updates,
        manifest_updates=manifest_updates,
    )
    before = _tree(root)
    result = DailyCanonicalReader(root, trade_date=TRADE_DATE).read()
    assert result.status == "unavailable"
    assert result.snapshot is None
    assert _tree(root) == before


@pytest.mark.parametrize(
    "rows",
    [
        (_row("sh.600000"), _row("sh.600000")),
        (_row("sh.600000", exchange="SSE"),),
        (_row("bj.830001", exchange="bj"),),
        (_row("sh.600000", security_type="fund"),),
        (_row("sh.600000", quality_status="bad"),),
        (_row("sh.600000", close=float("nan")),),
    ],
)
def test_unknown_or_invalid_partition_semantics_fail_closed(tmp_path, rows):
    root = tmp_path / "dataset"
    _write_dataset(root, rows=rows)
    result = DailyCanonicalReader(root, trade_date=TRADE_DATE).read()
    assert result.status == "unavailable"
    assert result.snapshot is None


def test_unsafe_partition_path_and_symlink_object_fail_closed(tmp_path):
    root = tmp_path / "dataset"
    partition, _ = _write_dataset(root)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][0]["path"] = "../escape.parquet"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True))
    assert DailyCanonicalReader(root, trade_date=TRADE_DATE).read().status == "unavailable"

    safe = tmp_path / "safe"
    partition, _ = _write_dataset(safe)
    real = partition.with_name("real.parquet")
    partition.rename(real)
    partition.symlink_to(real.name)
    assert DailyCanonicalReader(safe, trade_date=TRADE_DATE).read().status == "unavailable"


def test_published_projection_detects_manifest_change_and_closes(tmp_path):
    root = tmp_path / "dataset"
    _write_dataset(root)
    opened = PublishedDailyCanonicalProjection.open(
        DailyCanonicalReader(root, trade_date=TRADE_DATE)
    )
    assert isinstance(opened, PublishedDailyCanonicalProjection)
    manifest = root / "manifest.json"
    os.utime(manifest, None)
    assert opened.verify().status == "unavailable"
    opened.close()
    assert opened.closed is True


def test_snapshot_hashes_are_deterministic_for_input_row_order(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    rows = (_row("sz.000001"), _row("sh.600000"), _row("sh.000001", security_type="index"))
    _write_dataset(first, rows=rows)
    _write_dataset(second, rows=tuple(reversed(rows)))
    left = DailyCanonicalReader(first, trade_date=TRADE_DATE).read().snapshot
    right = DailyCanonicalReader(second, trade_date=TRADE_DATE).read().snapshot
    assert left is not None and right is not None
    assert left.canonical_universe_sha256 == right.canonical_universe_sha256
    assert left.canonical_exclusion_sha256 == right.canonical_exclusion_sha256
    assert left.symbol_mapping_sha256 == right.symbol_mapping_sha256
    assert left.ohlc_sha256 == right.ohlc_sha256
