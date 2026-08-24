from datetime import UTC, date, datetime

import pytest

from backend.app.market.candidates import (
    R2F2_GATE_ORDER,
    CandidateGateReport,
    CandidateStore,
    GateName,
    GateOutcome,
    SelectionReason,
    SessionSelection,
    build_candidate_manifest,
    build_gate_report,
    evaluate_candidate_gates,
    select_primary_candidate,
)
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore

NOW = datetime(2026, 7, 24, 10, tzinfo=UTC)
SHA = "a" * 64


def _outcomes(verdict: str = "pass"):
    return tuple(
        GateOutcome(
            gate_name=name,
            gate_version="r2f2-gates.v1",
            verdict=verdict,
            bounded_metrics=(("rows", 1),),
            referenced_hashes=(SHA,),
        )
        for name in R2F2_GATE_ORDER
    )


def test_gate_report_requires_exact_ordered_complete_gate_aggregate() -> None:
    report = build_gate_report(candidate_id="candidate-1", outcomes=_outcomes(), created_at=NOW)
    assert report.verdict == "pass"
    assert tuple(item.gate_name for item in report.outcomes) == R2F2_GATE_ORDER
    assert len(report.aggregate_sha256) == 64

    with pytest.raises(ValueError):
        CandidateGateReport.model_validate(
            report.model_dump(mode="json") | {"outcomes": report.outcomes[:-1]}
        )


def test_gate_report_rejects_reordered_or_changed_aggregate() -> None:
    report = build_gate_report(candidate_id="candidate-1", outcomes=_outcomes(), created_at=NOW)
    reordered = (report.outcomes[1], report.outcomes[0], *report.outcomes[2:])
    with pytest.raises(ValueError):
        CandidateGateReport.model_validate(report.model_dump(mode="json") | {"outcomes": reordered})
    with pytest.raises(ValueError):
        CandidateGateReport.model_validate(
            report.model_dump(mode="json") | {"aggregate_sha256": "b" * 64}
        )


def test_qualified_fallback_is_reserved_and_rejected_by_r2f2_writers() -> None:
    assert SelectionReason.QUALIFIED_FALLBACK.value == "qualified_fallback"
    with pytest.raises(ValueError, match="fallback"):
        select_primary_candidate(
            trade_date=date(2026, 7, 23),
            universe_id="main-board",
            candidates=(),
            reason=SelectionReason.QUALIFIED_FALLBACK,
            selected_at=NOW,
        )


def test_selection_requires_one_complete_baostock_candidate() -> None:
    candidate = {
        "candidate_id": "candidate-1",
        "trade_date": date(2026, 7, 23),
        "universe_id": "main-board",
        "provider_id": "baostock",
        "evidence_id": "evidence-1",
        "evidence_sha256": SHA,
        "normalized_object_relative_path": "candidates/one.parquet",
        "normalized_object_sha256": SHA,
        "gate_report_relative_path": "gates/one.json",
        "gate_report_sha256": SHA,
        "factor_resolution_sha256": SHA,
        "adapter_version": "r2f2.v1",
        "source_schema_version": "daily.v1",
        "row_count": 1,
        "required_symbol_count": 1,
        "status": "accepted",
    }
    candidate = build_candidate_manifest(**candidate)
    selection = select_primary_candidate(
        trade_date=date(2026, 7, 23),
        universe_id="main-board",
        candidates=(candidate,),
        selected_at=NOW,
    )
    assert isinstance(selection, SessionSelection)
    assert selection.reason is SelectionReason.PRIMARY_READY
    assert selection.selected_provider_id.value == "baostock"
    assert selection.fallback_from is None


def test_selection_rejects_mixed_provider_or_symbol_level_partition() -> None:
    base = {
        "candidate_id": "candidate-1",
        "trade_date": date(2026, 7, 23),
        "universe_id": "main-board",
        "provider_id": "baostock",
        "evidence_id": "evidence-1",
        "evidence_sha256": SHA,
        "normalized_object_relative_path": "candidates/one.parquet",
        "normalized_object_sha256": SHA,
        "gate_report_relative_path": "gates/one.json",
        "gate_report_sha256": SHA,
        "factor_resolution_sha256": SHA,
        "adapter_version": "r2f2.v1",
        "source_schema_version": "daily.v1",
        "row_count": 1,
        "required_symbol_count": 1,
        "status": "accepted",
        "manifest_sha256": SHA,
    }
    with pytest.raises(ValueError, match="exactly one"):
        select_primary_candidate(
            trade_date=date(2026, 7, 23),
            universe_id="main-board",
            candidates=(base, base | {"candidate_id": "candidate-2"}),
            selected_at=NOW,
        )
    with pytest.raises(ValueError, match="provider"):
        select_primary_candidate(
            trade_date=date(2026, 7, 23),
            universe_id="main-board",
            candidates=(base | {"provider_id": "tushare"},),
            selected_at=NOW,
        )


def test_selection_model_rejects_non_null_fallback() -> None:
    with pytest.raises(ValueError):
        SessionSelection(
            selection_id="selection-1",
            trade_date=date(2026, 7, 23),
            universe_id="main-board",
            selected_candidate_id="candidate-1",
            selected_provider_id="baostock",
            reason="primary_ready",
            fallback_from="baostock",
            evidence_sha256=SHA,
            candidate_manifest_sha256=SHA,
            gate_report_sha256=SHA,
            selected_at=NOW,
            selection_sha256=SHA,
        )


def test_selection_publish_order_writes_selection_before_canonical_callback(tmp_path) -> None:
    outcomes = _outcomes()
    report = build_gate_report(candidate_id="candidate-1", outcomes=outcomes, created_at=NOW)
    candidate = build_candidate_manifest(
        candidate_id="candidate-1",
        trade_date=date(2026, 7, 23),
        universe_id="main-board",
        provider_id="baostock",
        evidence_id="evidence-1",
        evidence_sha256=SHA,
        normalized_object_relative_path="candidates/one.parquet",
        normalized_object_sha256=SHA,
        gate_report_relative_path="gates/one.json",
        gate_report_sha256=report.aggregate_sha256,
        factor_resolution_sha256=SHA,
        adapter_version="r2f2.v1",
        source_schema_version="daily.v1",
        row_count=1,
        required_symbol_count=1,
    )
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
    )
    observations = []

    def canonical_callback(_selection):
        assert (tmp_path / f"selections/{selection.selection_id}.json").is_file()
        observations.append("canonical")

    CandidateStore(tmp_path).publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        after_selection=canonical_callback,
    )
    assert observations == ["canonical"]


def test_missing_legacy_source_decodes_in_memory_without_rewriting(tmp_path) -> None:
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2}', encoding="utf-8"
    )
    original = b'{"dataset":"stock-eva-market","schema_version":2,"generation":"legacy","files":[]}'
    manifest = root / "manifest.json"
    manifest.write_bytes(original)
    store = NasMarketStore(MarketStore(tmp_path / "control.duckdb"), root, tmp_path / "staging")
    assert store._manifest()["files"] == []
    assert manifest.read_bytes() == original


def test_canonical_manifest_lineage_is_all_or_nothing(tmp_path) -> None:
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2}', encoding="utf-8"
    )
    (root / "manifest.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2,"generation":"g",'
        '"files":[{"path":"bars/x.parquet","sha256":"'
        + SHA
        + '","trade_date":"2026-07-23","source":"baostock","row_count":1,'
        '"candidate_id":"candidate-1"}]}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="complete lineage"):
        NasMarketStore(
            MarketStore(tmp_path / "control.duckdb"), root, tmp_path / "staging"
        )._manifest()


def test_candidate_lineage_mismatch_is_rejected_before_selection_write(tmp_path) -> None:
    report = build_gate_report(candidate_id="candidate-1", outcomes=_outcomes(), created_at=NOW)
    candidate = build_candidate_manifest(
        candidate_id="candidate-1",
        trade_date=date(2026, 7, 23),
        universe_id="main-board",
        provider_id="baostock",
        evidence_id="evidence-1",
        evidence_sha256=SHA,
        normalized_object_relative_path="candidates/one.parquet",
        normalized_object_sha256=SHA,
        gate_report_relative_path="gates/one.json",
        gate_report_sha256=report.aggregate_sha256,
        factor_resolution_sha256=SHA,
        adapter_version="r2f2.v1",
        source_schema_version="daily.v1",
        row_count=1,
        required_symbol_count=1,
    )
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
    )

    class Evidence:
        manifest = type(
            "Manifest",
            (),
            {
                "evidence_id": "evidence-other",
                "manifest_sha256": SHA,
                "trade_date": candidate.trade_date,
                "universe_id": candidate.universe_id,
                "provider_id": "baostock",
                "adapter_version": "r2f2.v1",
                "factor_resolution_sha256": SHA,
            },
        )()

    with pytest.raises(ValueError, match="evidence_id"):
        CandidateStore(tmp_path).publish_chain(
            report=report,
            candidate=candidate,
            selection=selection,
            evidence=Evidence(),
        )
    assert not (tmp_path / "selections").exists()


def test_semantic_factor_and_suspension_gates_fail_closed() -> None:
    report = evaluate_candidate_gates(
        candidate_id="candidate-1",
        trade_date=date(2026, 7, 23),
        required_symbols=("sh.600000", "sh.600001", "sh.000001"),
        rows=(
            {
                "symbol": "sh.600000",
                "trade_date": date(2026, 7, 23),
                "security_type": "stock",
                "is_suspended": False,
                "adjust_factor": None,
            },
            {
                "symbol": "sh.600001",
                "trade_date": date(2026, 7, 23),
                "security_type": "stock",
                "is_suspended": True,
                "volume": 1,
                "amount": 1,
            },
            {
                "symbol": "sh.000001",
                "trade_date": date(2026, 7, 23),
                "security_type": "index",
                "is_suspended": True,
                "volume": 0,
                "amount": 0,
            },
        ),
        evidence=None,
        created_at=NOW,
    )
    by_name = {item.gate_name: item for item in report.outcomes}
    assert by_name[GateName.SEMANTIC].verdict == "fail"
    assert by_name[GateName.FACTOR].verdict == "fail"
    assert by_name[GateName.SUSPENSION].verdict == "fail"
    assert report.verdict == "fail"
