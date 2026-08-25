import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from backend.app.market.candidates import (
    R2F2_GATE_ORDER,
    CandidateGateReport,
    CandidateManifest,
    GateOutcome,
    SessionSelection,
)
from backend.app.market.canonical_comparison import (
    CanonicalCandidateReader,
    PublishedCanonicalComparison,
    UnavailableReason,
)
from backend.app.market.evidence import EvidenceStore
from backend.app.market.models import DailyBar
from tests.test_market_provider_contract import _provider_raw_batch


def digest(value):
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def write_canonical_fixture(root: Path):
    dataset = root / "dataset"
    candidate_root = root / "candidate"
    evidence_root = root / "evidence"
    dataset.mkdir()
    candidate_root.mkdir()
    evidence_root.mkdir()
    trade_date = date(2026, 8, 20)
    evidence_store = EvidenceStore(evidence_root)
    evidence_manifest = evidence_store.publish(_provider_raw_batch())
    evidence_id = evidence_manifest.evidence_id
    evidence_sha = evidence_manifest.manifest_sha256
    universe = evidence_manifest.universe_id
    symbol = "sh.600000"
    ingested = datetime(2026, 8, 20, 8, tzinfo=UTC)
    bar = DailyBar(
        trade_date=trade_date,
        symbol=symbol,
        security_type="stock",
        exchange="sh",
        board="main",
        open=10,
        high=11,
        low=9,
        close=10.5,
        preclose=10,
        volume=1000,
        amount=20000,
        turnover_rate=1,
        pct_change=5,
        adjust_factor=1,
        is_trading=True,
        is_suspended=False,
        is_st=False,
        source_record_id="r1",
        ingested_at=ingested,
        quality_status="ready",
    )
    normalized_raw = (
        json.dumps([bar.model_dump(mode="json")], sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    normalized_sha = hashlib.sha256(normalized_raw).hexdigest()
    candidate_values = {
        "candidate_id": "candidate-canonical-0001",
        "trade_date": trade_date.isoformat(),
        "universe_id": universe,
        "provider_id": "baostock",
        "evidence_id": evidence_id,
        "evidence_sha256": evidence_sha,
        "normalized_object_relative_path": "bundles/candidate-canonical-0001/normalized.json",
        "normalized_object_sha256": normalized_sha,
        "gate_report_relative_path": "bundles/candidate-canonical-0001/gate.json",
        "gate_report_sha256": "0" * 64,
        "factor_resolution_sha256": evidence_manifest.factor_resolution_sha256,
        "adapter_version": "r2f2.v1",
        "source_schema_version": "daily_astock.v1",
        "row_count": 1,
        "required_symbol_count": 1,
        "status": "accepted",
    }
    outcomes = tuple(
        GateOutcome(
            gate_name=name,
            gate_version="r2f2.v1",
            verdict="pass",
            referenced_hashes=(evidence_sha,),
        )
        for name in R2F2_GATE_ORDER
    )
    gate_values = {
        "gate_report_id": "gate-canonical-0001",
        "candidate_id": candidate_values["candidate_id"],
        "gate_version": "r2f2.v1",
        "outcomes": [item.model_dump(mode="json") for item in outcomes],
        "verdict": "pass",
        "created_at": "2026-08-20T08:00:00Z",
    }
    gate_values["aggregate_sha256"] = digest(gate_values)
    gate = CandidateGateReport.model_validate(gate_values)
    candidate_values["gate_report_sha256"] = gate.aggregate_sha256
    candidate_values["manifest_sha256"] = digest(candidate_values)
    candidate = CandidateManifest.model_validate(candidate_values)
    selection_values = {
        "selection_id": "selection-canonical-0001",
        "trade_date": trade_date.isoformat(),
        "universe_id": universe,
        "selected_candidate_id": candidate.candidate_id,
        "selected_provider_id": "baostock",
        "reason": "primary_ready",
        "fallback_from": None,
        "evidence_sha256": evidence_sha,
        "candidate_manifest_sha256": candidate.manifest_sha256,
        "gate_report_sha256": gate.aggregate_sha256,
        "selected_at": "2026-08-20T08:00:00Z",
    }
    selection_values["selection_sha256"] = digest(selection_values)
    selection = SessionSelection.model_validate(selection_values)
    bundle = candidate_root / "bundles" / candidate.candidate_id
    bundle.mkdir(parents=True)
    (bundle / "normalized.json").write_bytes(normalized_raw)
    (bundle / "gate.json").write_text(
        json.dumps(gate.model_dump(mode="json"), sort_keys=True, separators=(",", ":")) + "\n"
    )
    (bundle / "candidate.json").write_text(
        json.dumps(candidate.model_dump(mode="json"), sort_keys=True, separators=(",", ":")) + "\n"
    )
    (bundle / "selection.json").write_text(
        json.dumps(selection.model_dump(mode="json"), sort_keys=True, separators=(",", ":")) + "\n"
    )
    partition = dataset / "partitions" / "2026-08-20.parquet"
    partition.parent.mkdir()
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE bars (trade_date DATE, symbol VARCHAR, open DOUBLE, "
            "high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE, amount DOUBLE)"
        )
        connection.execute(
            "INSERT INTO bars VALUES ('2026-08-20', 'sh.600000', 10, 11, 9, 10.5, 1000, 20000)"
        )
        connection.execute("COPY bars TO ? (FORMAT PARQUET)", [str(partition)])
    finally:
        connection.close()
    partition_sha = hashlib.sha256(partition.read_bytes()).hexdigest()
    manifest = {
        "dataset": "stock-eva-market",
        "schema_version": 2,
        "generation": "g1",
        "files": [
            {
                "path": "partitions/2026-08-20.parquet",
                "sha256": partition_sha,
                "row_count": 1,
                "source": "baostock",
                "trade_date": "2026-08-20",
                "lineage": {
                    "candidate_id": candidate.candidate_id,
                    "candidate_manifest_sha256": candidate.manifest_sha256,
                    "evidence_id": evidence_id,
                    "evidence_sha256": evidence_sha,
                    "gate_report_sha256": gate.aggregate_sha256,
                    "factor_resolution_sha256": candidate.factor_resolution_sha256,
                    "normalized_object_sha256": candidate.normalized_object_sha256,
                    "selection_id": selection.selection_id,
                    "selection_sha256": selection.selection_sha256,
                },
            }
        ],
    }
    (dataset / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
    )
    return dataset, candidate_root, evidence_root


def test_canonical_comparison_snapshot_binds_manifest_partition_and_r2f2_lineage(tmp_path):
    dataset, candidate, evidence = write_canonical_fixture(tmp_path)
    result = CanonicalCandidateReader(dataset, candidate, evidence_root=evidence).read()
    assert result.status == "ready"
    assert result.snapshot.trade_date_partition_sha256
    assert result.snapshot.r2f2_publication_lineage["provider_id"] == "baostock"
    assert (
        result.snapshot.r2f2_publication_lineage["candidate_manifest_sha256"]
        == result.snapshot.candidate_sha256
    )


def test_canonical_comparison_bidirectional_hashes(
    tmp_path,
):
    dataset, candidate, evidence = write_canonical_fixture(tmp_path)
    snapshot = CanonicalCandidateReader(dataset, candidate, evidence_root=evidence).read().snapshot
    bundle = candidate / "bundles" / "candidate-canonical-0001"
    assert (
        snapshot.selection_sha256
        == json.loads((bundle / "selection.json").read_text())["selection_sha256"]
    )
    assert (
        snapshot.gate_sha256 == json.loads((bundle / "gate.json").read_text())["aggregate_sha256"]
    )
    assert (
        snapshot.normalized_sha256
        == json.loads((bundle / "candidate.json").read_text())["normalized_object_sha256"]
    )


def test_canonical_comparison_drift_returns_unavailable_and_zero_write(tmp_path):
    dataset, candidate, evidence = write_canonical_fixture(tmp_path)
    reader = CanonicalCandidateReader(dataset, candidate, evidence_root=evidence)
    assert reader.read().status == "ready"
    before = (dataset / "manifest.json").read_bytes()
    (dataset / "manifest.json").write_text(before.decode().replace('"g1"', '"g2"'))
    result = reader.verify()
    assert result.status == "unavailable"
    assert (dataset / "manifest.json").read_bytes() != before
    assert not (dataset / "writes").exists()


def test_canonical_candidate_reader_rejects_root_bundle_and_fd_toctou(tmp_path):
    dataset, candidate, evidence = write_canonical_fixture(tmp_path)
    bundled = tmp_path / "foreign-bundles" / "candidate-canonical-0001"
    bundled.parent.mkdir()
    bundled.mkdir()
    existing = candidate / "bundles" / "candidate-canonical-0001"
    for name in ("candidate.json", "gate.json", "normalized.json", "selection.json"):
        (bundled / name).write_bytes((existing / name).read_bytes())
    assert (
        CanonicalCandidateReader(dataset, tmp_path, evidence_root=evidence).read().status
        == "unavailable"
    )
    (existing / "normalized.json").unlink()
    (existing / "normalized.json").symlink_to(bundled / "normalized.json")
    assert (
        CanonicalCandidateReader(dataset, candidate, evidence_root=evidence).read().status
        == "unavailable"
    )


def test_published_canonical_comparison_verify_before_after_and_close_capability(tmp_path):
    dataset, candidate, evidence = write_canonical_fixture(tmp_path)
    capability = PublishedCanonicalComparison.open(
        CanonicalCandidateReader(dataset, candidate, evidence_root=evidence)
    )
    assert isinstance(capability, PublishedCanonicalComparison)
    assert capability.verify().status == "ready"
    assert capability.verify().status == "ready"
    assert capability.verify_before_reconcile and capability.verify_after_reconcile
    capability.close()
    assert capability.verify().status == "unavailable"


def test_canonical_comparison_rejects_mixed_generation_or_lineage(tmp_path):
    dataset, candidate, evidence = write_canonical_fixture(tmp_path)
    manifest = json.loads((dataset / "manifest.json").read_text())
    manifest["files"][0]["lineage"]["evidence_sha256"] = "t" * 64
    (dataset / "manifest.json").write_text(json.dumps(manifest) + "\n")
    result = CanonicalCandidateReader(dataset, candidate, evidence_root=evidence).read()
    assert (
        result.status == "unavailable"
        and result.unavailable_reason == UnavailableReason.SELECTION_BINDING_INVALID
    )


def test_task12_candidate_attach_requires_evidence_ready_and_keeps_job_nonterminal(tmp_path):
    dataset, candidate, evidence = write_canonical_fixture(tmp_path)
    result = CanonicalCandidateReader(dataset, candidate, evidence_root=evidence).read()
    assert result.status == "ready"


def test_canonical_reader_uses_production_dataset_files_and_candidate_bundle_layout(tmp_path):
    dataset, candidate_root, evidence = write_canonical_fixture(tmp_path)
    result = CanonicalCandidateReader(dataset, candidate_root, evidence_root=evidence).read()
    assert result.status == "ready"
