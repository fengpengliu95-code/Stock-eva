import hashlib
import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from backend.app.config import Settings
from backend.app.regime.models import (
    ActualMarketScope,
    ComponentScore,
    EvidenceItem,
    MarketRegimeResult,
    RegimeConfidence,
    SourceLineage,
)
from backend.app.regime.snapshots import (
    RegimeSnapshotConflict,
    RegimeSnapshotStore,
    RegimeSnapshotUnavailable,
    SnapshotCaptureRequest,
)
from backend.app.storage.layout import StorageLayout

AS_OF = date(2026, 7, 29)
HASH = "a" * 64


def _result(*, as_of: date = AS_OF) -> MarketRegimeResult:
    lineage = SourceLineage(
        source="fixture",
        source_version="fixture-v1",
        earliest_input_date=as_of,
        latest_input_date=as_of,
        record_count=1,
        content_hash=HASH,
    )
    evidence = EvidenceItem(
        component="trend",
        code="fixture",
        detail="fixture evidence",
        value=1.0,
        as_of=as_of,
        source="fixture",
    )
    scope = ActualMarketScope(
        expected_boards=["sse_main"],
        observed_boards=["sse_main"],
        missing_boards=[],
        expected_index_series=["sh.000001"],
        observed_index_series=["sh.000001"],
        missing_index_series=[],
        observed_universe_count=1,
        index_coverage_ratio=1.0,
        conclusion_disclaimer="Fixture scope only.",
    )
    component = ComponentScore(
        name="trend",
        score=1.0,
        weight=1.0,
        weighted_score=1.0,
        formula_version="fixture-v1",
        quality_status="ready",
        missing_inputs=[],
        quality_issues=[],
        supporting_evidence=[evidence],
        contrary_evidence=[],
        source_lineage=[lineage],
    )
    return MarketRegimeResult(
        result_id="regime-fixture",
        formula_version="market-regime-v1",
        as_of=as_of,
        data_as_of=as_of,
        status="ready",
        quality_status="ready",
        strategic_state="range",
        tactical_state="neutral",
        total_score=1.0,
        component_scores=[component],
        weights={"trend": 1.0},
        thresholds={"neutral": 0.0},
        confidence=RegimeConfidence(value=1.0, level="high"),
        missing_inputs=[],
        supporting_evidence=[evidence],
        contrary_evidence=[],
        quality_issues=[],
        actual_market_scope=scope,
        source_lineage=[lineage],
    )


def _request(*, result: MarketRegimeResult | None = None) -> SnapshotCaptureRequest:
    return SnapshotCaptureRequest(
        result=result or _result(),
        capture_mode="post_hoc_backfill",
        evidence_cutoff_at=datetime(2026, 7, 29, 8, tzinfo=UTC),
        dataset_generation="fixture-generation",
        dataset_identity=Path("/private/fixture/market.parquet").as_posix(),
    )


def _inventory(path: Path) -> dict[str, bytes]:
    return {
        candidate.name: candidate.read_bytes()
        for candidate in sorted(path.parent.glob(f"{path.name}*"))
    }


def _store(tmp_path: Path) -> RegimeSnapshotStore:
    settings = Settings(local_control_dir=tmp_path / "control")
    return RegimeSnapshotStore(StorageLayout(settings).regime_snapshot_database)


def test_snapshot_store_inserts_once_and_exact_repeat_is_byte_stable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    request = _request()

    inserted, created = store.capture(request)
    before = _inventory(store.path)
    repeated, repeated_created = store.capture(request)

    assert created is True
    assert repeated_created is False
    assert repeated == inserted
    assert _inventory(store.path) == before


def test_snapshot_store_rejects_divergent_same_date_without_mutation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.capture(_request())
    before = _inventory(store.path)
    changed = _result()
    changed.total_score = 2.0

    with pytest.raises(RegimeSnapshotConflict):
        store.capture(_request(result=changed))

    assert _inventory(store.path) == before


def test_snapshot_reader_missing_file_does_not_create_parent(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "snapshots.sqlite3"

    assert RegimeSnapshotStore(path).read_exact(AS_OF, "market-regime-v1") is None
    assert not path.parent.exists()


@pytest.mark.parametrize("mutation", ["schema", "payload", "hash", "future_lineage", "lock"])
def test_snapshot_reader_rejects_bad_schema_payload_hash_future_lineage_and_lock(
    tmp_path: Path, mutation: str
) -> None:
    store = _store(tmp_path)
    store.capture(_request())
    if mutation == "schema":
        connection = sqlite3.connect(store.path)
        connection.execute("DROP TABLE regime_snapshots")
        connection.commit()
        connection.close()
    elif mutation == "payload":
        connection = sqlite3.connect(store.path)
        connection.execute("UPDATE regime_snapshots SET result_payload = ?", ["[]"])
        connection.commit()
        connection.close()
    elif mutation == "hash":
        connection = sqlite3.connect(store.path)
        connection.execute("UPDATE regime_snapshots SET content_hash = ?", ["b" * 64])
        connection.commit()
        connection.close()
    elif mutation == "future_lineage":
        payload = _result().model_dump(mode="json")
        payload["source_lineage"][0]["latest_input_date"] = "2026-07-30"
        connection = sqlite3.connect(store.path)
        connection.execute(
            "UPDATE regime_snapshots SET result_payload = ?",
            [json.dumps(payload)],
        )
        connection.commit()
        connection.close()
    else:
        connection = sqlite3.connect(store.path)
        connection.execute("BEGIN EXCLUSIVE")

    try:
        with pytest.raises(RegimeSnapshotUnavailable):
            store.read_exact(AS_OF, "market-regime-v1")
    finally:
        if mutation == "lock":
            connection.rollback()
            connection.close()


def test_snapshot_metadata_never_contains_dataset_path(tmp_path: Path) -> None:
    store = _store(tmp_path)
    snapshot, _ = store.capture(_request())
    serialized = snapshot.model_dump_json()

    assert "/private/fixture" not in serialized
    assert snapshot.dataset_identity_hash == hashlib.sha256(
        b"/private/fixture/market.parquet"
    ).hexdigest()


def test_snapshot_store_uses_explicit_canonical_result_payload_columns(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    snapshot, _ = store.capture(_request())

    connection = sqlite3.connect(store.path)
    columns = [row[1] for row in connection.execute("PRAGMA table_info(regime_snapshots)")]
    row = connection.execute(
        """
        SELECT snapshot_id, as_of, formula_version, result_id, data_as_of, capture_mode,
               evidence_cutoff_at, recorded_at, dataset_generation, dataset_identity_hash,
               content_hash, result_payload
        FROM regime_snapshots
        """
    ).fetchone()
    connection.close()

    assert columns == [
        "snapshot_id",
        "as_of",
        "formula_version",
        "result_id",
        "data_as_of",
        "capture_mode",
        "evidence_cutoff_at",
        "recorded_at",
        "dataset_generation",
        "dataset_identity_hash",
        "content_hash",
        "result_payload",
    ]
    assert row == (
        snapshot.snapshot_id,
        snapshot.as_of.isoformat(),
        snapshot.formula_version,
        snapshot.result_id,
        snapshot.data_as_of.isoformat(),
        snapshot.capture_mode,
        snapshot.evidence_cutoff_at.isoformat(),
        snapshot.recorded_at.isoformat(),
        snapshot.dataset_generation,
        snapshot.dataset_identity_hash,
        snapshot.content_hash,
        json.dumps(
            snapshot.result.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
