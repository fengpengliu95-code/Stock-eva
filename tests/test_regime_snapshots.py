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
    RegimeSnapshotCaptureService,
    RegimeSnapshotConflict,
    RegimeSnapshotStore,
    RegimeSnapshotUnavailable,
    SnapshotCaptureRequest,
    _canonical,
    _hash,
)
from backend.app.regime.store import MarketRegimeStore
from backend.app.storage.dataset import PublishedReadSnapshot
from backend.app.storage.layout import StorageLayout

AS_OF = date(2026, 7, 29)
HASH = "a" * 64
VALID_GENERATION = "generation-" + "a" * 32
FUTURE_RESULT_BOUNDARIES = (
    "top_earliest_lineage",
    "top_latest_lineage",
    "component_earliest_lineage",
    "component_latest_lineage",
    "top_supporting_evidence",
    "top_contrary_evidence",
    "component_supporting_evidence",
    "component_contrary_evidence",
)


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


def _request(
    *,
    result: MarketRegimeResult | None = None,
    dataset_generation: str = VALID_GENERATION,
) -> SnapshotCaptureRequest:
    return SnapshotCaptureRequest(
        result=result or _result(),
        capture_mode="post_hoc_backfill",
        evidence_cutoff_at=datetime(2026, 7, 29, 8, tzinfo=UTC),
        dataset_generation=dataset_generation,
        dataset_identity=Path("/private/fixture/market.parquet").as_posix(),
    )


def _result_with_future_boundary(boundary: str) -> MarketRegimeResult:
    result = _result()
    future = AS_OF.replace(day=30)
    if boundary in {"top_earliest_lineage", "top_latest_lineage"}:
        field = "earliest_input_date" if "earliest" in boundary else "latest_input_date"
        update = {field: future}
        if field == "earliest_input_date":
            update["latest_input_date"] = future
        lineage = result.source_lineage[0].model_copy(update=update)
        return result.model_copy(update={"source_lineage": [lineage]})
    if boundary in {"component_earliest_lineage", "component_latest_lineage"}:
        field = "earliest_input_date" if "earliest" in boundary else "latest_input_date"
        update = {field: future}
        if field == "earliest_input_date":
            update["latest_input_date"] = future
        lineage = result.component_scores[0].source_lineage[0].model_copy(update=update)
        component = result.component_scores[0].model_copy(update={"source_lineage": [lineage]})
        return result.model_copy(update={"component_scores": [component]})

    evidence = result.supporting_evidence[0].model_copy(update={"as_of": future})
    if boundary == "top_supporting_evidence":
        return result.model_copy(update={"supporting_evidence": [evidence]})
    if boundary == "top_contrary_evidence":
        return result.model_copy(update={"contrary_evidence": [evidence]})
    field = boundary.removeprefix("component_")
    component = result.component_scores[0].model_copy(update={field: [evidence]})
    return result.model_copy(update={"component_scores": [component]})


def _mutate_payload_future_boundary(
    payload: dict[str, object],
    boundary: str,
) -> None:
    future = "2026-07-30"
    if boundary in {"top_earliest_lineage", "top_latest_lineage"}:
        field = "earliest_input_date" if "earliest" in boundary else "latest_input_date"
        payload["source_lineage"][0][field] = future
        if field == "earliest_input_date":
            payload["source_lineage"][0]["latest_input_date"] = future
        return
    if boundary in {"component_earliest_lineage", "component_latest_lineage"}:
        field = "earliest_input_date" if "earliest" in boundary else "latest_input_date"
        payload["component_scores"][0]["source_lineage"][0][field] = future
        if field == "earliest_input_date":
            payload["component_scores"][0]["source_lineage"][0]["latest_input_date"] = future
        return

    evidence = dict(payload["supporting_evidence"][0])
    evidence["as_of"] = future
    if boundary == "top_supporting_evidence":
        payload["supporting_evidence"] = [evidence]
    elif boundary == "top_contrary_evidence":
        payload["contrary_evidence"] = [evidence]
    else:
        field = boundary.removeprefix("component_")
        payload["component_scores"][0][field] = [evidence]


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
    assert (
        snapshot.dataset_identity_hash
        == hashlib.sha256(b"/private/fixture/market.parquet").hexdigest()
    )


@pytest.mark.parametrize(
    "dataset_generation",
    [VALID_GENERATION, "generation-20260729-080000"],
)
def test_snapshot_accepts_only_real_publisher_generation_shapes(
    tmp_path: Path,
    dataset_generation: str,
) -> None:
    snapshot, created = _store(tmp_path).capture(_request(dataset_generation=dataset_generation))

    assert created is True
    assert snapshot.dataset_generation == dataset_generation


@pytest.mark.parametrize(
    "boundary",
    FUTURE_RESULT_BOUNDARIES,
)
def test_snapshot_capture_rejects_future_nested_result_dates_without_writing(
    tmp_path: Path,
    boundary: str,
) -> None:
    result = _result_with_future_boundary(boundary)
    request = SnapshotCaptureRequest.model_construct(
        result=result,
        capture_mode="post_hoc_backfill",
        evidence_cutoff_at=datetime(2026, 7, 29, 8, tzinfo=UTC),
        dataset_generation=VALID_GENERATION,
        dataset_identity="/private/fixture/market.parquet",
    )
    store = _store(tmp_path)

    with pytest.raises(ValueError, match="must not exceed as_of"):
        store.capture(request)

    assert not store.path.exists()


@pytest.mark.parametrize("boundary", FUTURE_RESULT_BOUNDARIES)
def test_snapshot_reader_rejects_rehashed_future_nested_result_dates(
    tmp_path: Path,
    boundary: str,
) -> None:
    store = _store(tmp_path)
    store.capture(_request())
    connection = sqlite3.connect(store.path)
    row = connection.execute(
        """
        SELECT snapshot_id, as_of, formula_version, result_id, data_as_of, capture_mode,
               evidence_cutoff_at, recorded_at, dataset_generation, dataset_identity_hash,
               result_payload
        FROM regime_snapshots
        """
    ).fetchone()
    result_payload = json.loads(row[-1])
    _mutate_payload_future_boundary(result_payload, boundary)
    fields = dict(
        zip(
            (
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
                "result_payload",
            ),
            (*row[:-1], _canonical(result_payload)),
            strict=True,
        )
    )
    connection.execute(
        "UPDATE regime_snapshots SET result_payload = ?, content_hash = ?",
        [fields["result_payload"], _hash(fields)],
    )
    connection.commit()
    connection.close()

    with pytest.raises(RegimeSnapshotUnavailable):
        store.read_exact(AS_OF, "market-regime-v1")


@pytest.mark.parametrize(
    "unsafe_generation",
    ["/private/generation secret", "credential-secret-token"],
)
def test_snapshot_rejects_non_publisher_dataset_generation_on_write_and_read(
    tmp_path: Path,
    unsafe_generation: str,
) -> None:
    store = _store(tmp_path)
    unsafe_request = _request().model_copy(update={"dataset_generation": unsafe_generation})
    with pytest.raises(ValueError):
        store.capture(unsafe_request)
    assert not store.path.exists()

    store.capture(_request())
    connection = sqlite3.connect(store.path)
    row = connection.execute(
        """
        SELECT snapshot_id, as_of, formula_version, result_id, data_as_of, capture_mode,
               evidence_cutoff_at, recorded_at, dataset_identity_hash, result_payload
        FROM regime_snapshots
        """
    ).fetchone()
    fields = {
        "snapshot_id": row[0],
        "as_of": row[1],
        "formula_version": row[2],
        "result_id": row[3],
        "data_as_of": row[4],
        "capture_mode": row[5],
        "evidence_cutoff_at": row[6],
        "recorded_at": row[7],
        "dataset_generation": unsafe_generation,
        "dataset_identity_hash": row[8],
        "result_payload": row[9],
    }
    connection.execute(
        "UPDATE regime_snapshots SET dataset_generation = ?, content_hash = ?",
        [fields["dataset_generation"], _hash(fields)],
    )
    connection.commit()
    connection.close()

    with pytest.raises(RegimeSnapshotUnavailable):
        store.read_exact(AS_OF, "market-regime-v1")


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


def test_backfill_binds_all_dates_to_one_strict_publication_snapshot(tmp_path: Path) -> None:
    dates = (date(2026, 7, 28), AS_OF)

    class ImmutableReader:
        def __init__(self) -> None:
            self.snapshots = 0
            self.queries: list[tuple[PublishedReadSnapshot, date]] = []

        def cache_snapshot(self) -> PublishedReadSnapshot:
            self.snapshots += 1
            return PublishedReadSnapshot(
                identity="/private/dataset:fixture",
                paths=(),
                fingerprints=(),
                generation=VALID_GENERATION,
                trade_dates=dates,
            )

        def bars_through_snapshot(
            self,
            snapshot: PublishedReadSnapshot,
            as_of: date,
            *,
            max_sessions: int,
        ) -> list[object]:
            assert max_sessions == 130
            self.queries.append((snapshot, as_of))
            return []

    reader = ImmutableReader()
    service = RegimeSnapshotCaptureService(
        regime_store=MarketRegimeStore(reader),
        snapshot_store=_store(tmp_path),
    )

    outcome = service.capture_range(
        dates[0],
        dates[-1],
        evidence_cutoff_at=datetime(2026, 7, 30, tzinfo=UTC),
    )

    assert outcome.inserted == 2
    assert outcome.selected_dates == dates
    assert reader.snapshots == 1
    assert [as_of for _snapshot, as_of in reader.queries] == list(dates)
    assert len({id(snapshot) for snapshot, _as_of in reader.queries}) == 1
