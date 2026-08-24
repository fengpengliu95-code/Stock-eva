import gc
import hashlib
import json
import os
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from backend.app.market.automation import canonical_refresh_callback
from backend.app.market.candidates import (
    R2F2_GATE_ORDER,
    CandidateGateReport,
    CandidateManifest,
    CandidateStore,
    GateName,
    GateOutcome,
    PublishedCandidateRejection,
    PublishedSelection,
    SelectionReason,
    SessionSelection,
    build_candidate_manifest,
    build_gate_report,
    evaluate_candidate_gates,
    select_primary_candidate,
    validate_candidate_lineage,
)
from backend.app.market.evidence import EvidenceError, EvidenceStore, PublishedEvidence
from backend.app.market.factor_cache import AdjustmentFactorCache
from backend.app.market.models import DailyBar
from backend.app.market.providers.baostock import BaoStockProviderAdapter
from backend.app.market.store import MarketStore
from backend.app.storage.dataset import NasMarketStore
from tests.test_market_provider_contract import _CompleteSdkClient

NOW = datetime(2026, 7, 24, 10, tzinfo=UTC)
SHA = "a" * 64
_LAST_NORMALIZED_PAYLOAD: bytes | None = None


def _outcomes(verdict: str = "pass", referenced_hash: str = SHA):
    return tuple(
        GateOutcome(
            gate_name=name,
            gate_version="r2f2-gates.v1",
            verdict=verdict,
            bounded_metrics=(("rows", 1), ("required_symbols", 1)),
            referenced_hashes=(referenced_hash,),
        )
        for name in R2F2_GATE_ORDER
    )


def test_gate_report_requires_exact_ordered_complete_gate_aggregate(tmp_path) -> None:
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


def test_qualified_fallback_is_reserved_and_rejected_by_r2f2_writers(tmp_path) -> None:
    assert SelectionReason.QUALIFIED_FALLBACK.value == "qualified_fallback"
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    with pytest.raises(ValueError, match="fallback"):
        select_primary_candidate(
            trade_date=candidate.trade_date,
            universe_id=candidate.universe_id,
            candidates=(candidate,),
            reason=SelectionReason.QUALIFIED_FALLBACK,
            selected_at=NOW,
            gate_report=report,
            evidence=evidence,
        )
    evidence.close()


def test_selection_requires_one_complete_baostock_candidate(tmp_path) -> None:
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    assert isinstance(selection, SessionSelection)
    assert selection.reason is SelectionReason.PRIMARY_READY
    assert selection.selected_provider_id.value == "baostock"
    assert selection.fallback_from is None


def test_selection_rejects_mixed_provider_or_symbol_level_partition(tmp_path) -> None:
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    with pytest.raises(ValueError, match="exactly one"):
        select_primary_candidate(
            trade_date=candidate.trade_date,
            universe_id=candidate.universe_id,
            candidates=(candidate, candidate),
            selected_at=NOW,
            gate_report=report,
            evidence=evidence,
        )
    evidence.close()


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


def test_selection_publish_order_never_moves_pointer_early(tmp_path) -> None:
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    published = CandidateStore(tmp_path).publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        evidence=evidence,
        normalized_payload=_normalized_payload(),
    )
    assert isinstance(published, PublishedSelection)
    assert published.selection.selection_id == selection.selection_id
    published.verify()


def test_missing_legacy_source_decodes_as_baostock_in_memory(tmp_path) -> None:
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2}', encoding="utf-8"
    )
    original = b'{"dataset":"stock-eva-market","schema_version":2,"generation":"legacy","files":[]}'
    manifest = root / "manifest.json"
    manifest.write_bytes(original)
    before = _tree_fingerprint(root)
    store = NasMarketStore(MarketStore(tmp_path / "control.duckdb"), root, tmp_path / "staging")
    assert store._manifest()["files"] == []
    assert manifest.read_bytes() == original
    assert _tree_fingerprint(root) == before


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
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )

    with pytest.raises(ValueError, match="evidence_id"):
        CandidateStore(tmp_path).publish_chain(
            report=report,
            candidate=build_candidate_manifest(
                **(
                    {
                        key: value
                        for key, value in candidate.model_dump(mode="python").items()
                        if key != "manifest_sha256"
                    }
                    | {"evidence_id": "evidence-other"}
                )
            ),
            selection=selection,
            evidence=evidence,
            normalized_payload=_normalized_payload(),
        )
    assert not (tmp_path / "selections").exists()


def test_semantic_gate_rejects_active_missing_factor_nonzero_suspended_activity_and_suspended_index(
    tmp_path,
) -> None:
    evidence, _report, _candidate = _real_candidate_bundle(tmp_path)
    report = evaluate_candidate_gates(
        candidate_id="candidate-1",
        trade_date=evidence.manifest.trade_date,
        required_symbols=("sh.600000", "sh.600001", "sh.000001"),
        rows=(
            {
                "symbol": "sh.600000",
                "trade_date": evidence.manifest.trade_date,
                "security_type": "stock",
                "is_suspended": False,
                "adjust_factor": None,
            },
            {
                "symbol": "sh.600001",
                "trade_date": evidence.manifest.trade_date,
                "security_type": "stock",
                "is_suspended": True,
                "volume": 1,
                "amount": 1,
            },
            {
                "symbol": "sh.000001",
                "trade_date": evidence.manifest.trade_date,
                "security_type": "index",
                "is_suspended": True,
                "volume": 0,
                "amount": 0,
            },
        ),
        evidence=evidence,
        created_at=NOW,
    )
    by_name = {item.gate_name: item for item in report.outcomes}
    assert by_name[GateName.SEMANTIC].verdict == "fail"
    # The real five-request fixture carries an authoritative factor snapshot
    # for the active stock, so this scenario fails on semantic/suspension gates.
    assert by_name[GateName.FACTOR].verdict == "pass"
    assert by_name[GateName.SUSPENSION].verdict == "fail"
    evidence.close()


def _real_candidate_bundle(tmp_path):
    trade_date = date(2026, 8, 20)
    factor_cache = AdjustmentFactorCache(":memory:")
    factor_cache.record_bootstrap(
        "sh.600000",
        trade_date,
        factor_cache.FACTOR_FIELDS,
        [["sh.600000", trade_date.isoformat(), "1", "1", "1"]],
    )
    adapter = BaoStockProviderAdapter(
        client=_CompleteSdkClient(response_date=trade_date.isoformat()),
        max_attempts=1,
        min_request_interval_seconds=0,
        factor_cache=factor_cache,
    )
    callback = canonical_refresh_callback(
        MarketStore(tmp_path / "control.duckdb"),
        adapter,
        evidence_root=tmp_path / "evidence",
        lock_path=tmp_path / "locks" / "refresh.lock",
        factor_cache=factor_cache,
    )
    result = callback(
        trade_date=trade_date,
        required_symbols={"sh.600000"},
        request_key="daily:2026-08-20",
        run_id="refresh-real",
    )
    assert result.status == "ready"
    evidence_root = tmp_path / "evidence"
    manifest_path = next(evidence_root.glob("manifests/*.json"))
    evidence_store = EvidenceStore(evidence_root)
    evidence = evidence_store.read(json.loads(manifest_path.read_text())["evidence_id"])
    bundle = next(evidence_root.glob("bundles/*"))
    candidate = CandidateManifest.model_validate(
        json.loads((bundle / "candidate.json").read_text())
    )
    report = CandidateGateReport.model_validate(json.loads((bundle / "gate.json").read_text()))
    global _LAST_NORMALIZED_PAYLOAD
    _LAST_NORMALIZED_PAYLOAD = (bundle / "normalized.json").read_bytes()
    return evidence, report, candidate


def _normalized_payload() -> bytes:
    if _LAST_NORMALIZED_PAYLOAD is not None:
        return _LAST_NORMALIZED_PAYLOAD
    return json.dumps(
        [
            {
                "trade_date": "2026-08-20",
                "symbol": "sh.600000",
                "security_type": "stock",
                "exchange": "sh",
                "board": "main",
                "open": 10.0,
                "high": 10.0,
                "low": 10.0,
                "close": 10.0,
                "preclose": 10.0,
                "volume": 0.0,
                "amount": 0.0,
                "turnover_rate": None,
                "pct_change": None,
                "adjust_factor": None,
                "price_adjustment": "none",
                "is_trading": False,
                "is_suspended": True,
                "is_st": False,
                "source": "baostock",
                "source_record_id": "sh.600000-2026-08-20",
                "ingested_at": "2026-08-20T08:00:00Z",
                "quality_status": "partial",
                "quality_issues": ["suspended_placeholder"],
            }
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _tree_fingerprint(root: Path) -> dict[str, tuple[bytes, int]]:
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file()
    }


def test_legacy_baostock_rows_and_manifests_decode_without_rewrite(tmp_path):
    evidence, _report, _candidate = _real_candidate_bundle(tmp_path)
    assert evidence.manifest.provider_id.value == "baostock"
    assert evidence.read_rows()
    evidence.close()


def test_new_candidate_references_exact_evidence_universe_and_gate_hashes(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    assert candidate.evidence_id == evidence.manifest.evidence_id
    assert candidate.evidence_sha256 == evidence.manifest.manifest_sha256
    assert candidate.gate_report_sha256 == report.aggregate_sha256
    evidence.close()


def test_every_candidate_gate_has_immutable_pass_or_fail_record(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    evaluated = evaluate_candidate_gates(
        candidate_id=candidate.candidate_id,
        trade_date=evidence.manifest.trade_date,
        required_symbols=("sh.600000",),
        rows=({"symbol": "sh.600000", "trade_date": evidence.manifest.trade_date},),
        evidence=evidence,
        created_at=NOW,
    )
    assert len(evaluated.outcomes) == 10
    assert all(item.verdict in {"pass", "fail"} for item in evaluated.outcomes)
    assert report.outcomes[0].gate_name is GateName.TRANSPORT_COMPLETE
    evidence.close()


def test_selection_references_one_complete_candidate_only(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    assert selection.selected_candidate_id == candidate.candidate_id
    evidence.close()


def test_new_canonical_manifest_requires_lineage_for_new_entries(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    root = tmp_path / "candidates"
    root.mkdir()
    with pytest.raises(AttributeError, match="publish_selection"):
        CandidateStore(root).publish_selection(
            selection,
            candidate=candidate,
            gate_report=report,
            evidence=evidence,
        )
    assert not (root / "selections").exists()
    assert not (root / "bundles").exists()
    evidence.close()


def test_lineage_mismatch_preserves_old_pointer_and_candidate_history(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    with pytest.raises(ValueError):
        CandidateStore(tmp_path / "candidates").publish_chain(
            report=report,
            candidate=candidate.model_copy(update={"evidence_id": "evidence-other"}),
            selection=selection,
            evidence=evidence,
        )
    assert not (tmp_path / "candidates" / "selections").exists()
    evidence.close()


def test_candidate_rejects_any_lineage_provider_schema_universe_or_hash_mismatch(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    for field, value in (
        ("provider_id", "tushare"),
        ("universe_id", "other"),
        ("adapter_version", "other.v1"),
    ):
        with pytest.raises(ValueError):
            validate_candidate_lineage(
                candidate.model_copy(update={field: value}), evidence, report
            )
    evidence.close()


def test_factor_resolution_binds_published_snapshot_or_raw_endpoint(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    values = {
        key: value
        for key, value in candidate.model_dump(mode="python").items()
        if key != "manifest_sha256"
    }
    values["factor_resolution_sha256"] = "f" * 64
    forged = build_candidate_manifest(**values)
    with pytest.raises(ValueError, match="factor"):
        validate_candidate_lineage(forged, evidence, report)
    evidence.close()


def test_factor_resolution_requires_mutually_exclusive_live_cache_fields_and_hashes(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    assert evidence.manifest.factor_resolution
    assert candidate.factor_resolution_sha256 == evidence.manifest.factor_resolution_sha256
    with pytest.raises(ValueError):
        validate_candidate_lineage(
            candidate.model_copy(update={"factor_resolution_sha256": "f" * 64}),
            evidence,
            report,
        )
    evidence.close()


def test_existing_market_analysis_alert_user_and_api_json_remain_compatible(tmp_path):
    from datetime import datetime

    from backend.app.alert.models import AlertRuleRecord
    from backend.app.analysis.models import SecurityAnalysisResponse
    from backend.app.market.models import MarketSummary
    from backend.app.user.models import PositionCreate

    analysis = SecurityAnalysisResponse.model_validate(
        {
            "symbol": "sh.600000",
            "status": "empty",
            "as_of": None,
            "formula_version": "analysis.v1",
            "series": [],
        }
    )
    alert = AlertRuleRecord.model_validate(
        {
            "id": "alert-1",
            "name": "close",
            "watchlist_id": "watch-1",
            "strategy_id": "strategy-1",
            "strategy_version": 1,
            "strategy_version_id": "strategy-1.v1",
            "created_at": NOW,
        }
    )
    position = PositionCreate.model_validate(
        {
            "symbol": "SH.600000",
            "quantity": "10",
            "avg_cost": "10.5",
            "as_of_date": "2026-08-20",
        }
    )
    summary = MarketSummary.model_validate(
        {
            "status": "empty",
            "as_of": None,
            "source": None,
            "completeness": {"requested": 0, "loaded": 0, "failed": 0},
            "freshness": {"evaluated": False},
            "breadth": {},
            "turnover": {"coverage": 0},
            "indexes": [],
        }
    )
    for model in (analysis, alert, position, summary):
        assert type(model).model_validate_json(model.model_dump_json()) == model
    assert datetime.fromisoformat(alert.model_dump(mode="json")["created_at"])


def test_get_reads_and_legacy_manifest_reads_are_write_free(tmp_path):
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        '{"dataset":"stock-eva-market","schema_version":2}', encoding="utf-8"
    )
    manifest = root / "manifest.json"
    original = b'{"dataset":"stock-eva-market","schema_version":2,"generation":"legacy","files":[]}'
    manifest.write_bytes(original)
    before = _tree_fingerprint(root)
    store = NasMarketStore(MarketStore(tmp_path / "control.duckdb"), root, tmp_path / "staging")
    assert store._manifest()["files"] == []
    assert manifest.read_bytes() == original
    assert _tree_fingerprint(root) == before


def test_candidate_store_rejects_symlinked_artifact_directory(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "candidate-root"
    root.mkdir()
    (root / "bundles").symlink_to(outside, target_is_directory=True)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    with pytest.raises(EvidenceError):
        CandidateStore(root).publish_chain(
            report=report,
            candidate=candidate,
            selection=selection,
            evidence=evidence,
            normalized_payload=_normalized_payload(),
        )
    assert not list(outside.iterdir())
    evidence.close()


def test_candidate_store_cas_collision_is_rejected_without_overwrite(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    store = CandidateStore(tmp_path / "candidate-root")
    (tmp_path / "candidate-root").mkdir()
    path = store.publish_gate_report(report)
    path.write_bytes(b"forged")
    with pytest.raises(ValueError):
        store.publish_gate_report(report)
    evidence.close()


def test_candidate_callback_api_is_removed_and_bundle_is_atomic(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    (tmp_path / "candidate-root").mkdir()
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )

    with pytest.raises(TypeError, match="after_selection"):
        CandidateStore(tmp_path / "candidate-root").publish_chain(
            report=report,
            candidate=candidate,
            selection=selection,
            evidence=evidence,
            normalized_payload=_normalized_payload(),
            after_selection=lambda _selection: None,
        )
    assert not (tmp_path / "candidate-root" / "bundles").exists()
    evidence.close()


def test_candidate_store_fails_closed_on_selection_path_tamper(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    root = tmp_path / "candidate-root"
    root.mkdir()
    published = CandidateStore(root).publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        evidence=evidence,
        normalized_payload=_normalized_payload(),
    )
    assert published is not None
    selection_path = root / f"bundles/{candidate.candidate_id}/selection.json"
    selection_path.write_bytes(b"tampered")
    with pytest.raises(EvidenceError):
        published.verify()
    published.close()
    evidence.close()


def test_candidate_store_walks_each_ancestor_without_following_symlink(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    outside = tmp_path / "outside-root"
    outside.mkdir()
    ancestor = tmp_path / "ancestor-link"
    ancestor.symlink_to(outside, target_is_directory=True)
    with pytest.raises(EvidenceError):
        CandidateStore(ancestor / "candidate-root").publish_chain(
            report=report,
            candidate=candidate,
            selection=selection,
            evidence=evidence,
            normalized_payload=_normalized_payload(),
        )
    assert not list(outside.iterdir())
    evidence.close()


def test_publish_recomputes_gates_from_normalized_payload(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    forged = json.loads(_normalized_payload())
    forged[0]["is_suspended"] = False
    forged[0]["is_trading"] = True
    forged[0]["quality_status"] = "ready"
    forged[0]["quality_issues"] = []
    payload = json.dumps(forged, sort_keys=True, separators=(",", ":")).encode()
    values = {
        key: value
        for key, value in candidate.model_dump(mode="python").items()
        if key != "manifest_sha256"
    }
    values["normalized_object_sha256"] = hashlib.sha256(payload).hexdigest()
    forged_candidate = build_candidate_manifest(**values)
    with pytest.raises(ValueError, match="accepted candidate requires a selection"):
        CandidateStore(tmp_path).publish_chain(
            report=report,
            candidate=forged_candidate,
            selection=None,
            evidence=evidence,
            normalized_payload=payload,
        )
    assert not (tmp_path / "bundles").exists()
    evidence.close()


def test_published_selection_model_copy_cannot_forge_capability(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    published = CandidateStore(tmp_path).publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        evidence=evidence,
        normalized_payload=_normalized_payload(),
    )
    assert published is not None
    forged = published.model_copy(update={"selection_sha256": "b" * 64})
    with pytest.raises(EvidenceError):
        forged.verify()
    published.close()
    evidence.close()


def test_rejected_candidate_publishes_complete_audit_without_selection(tmp_path):
    evidence, _report, candidate = _real_candidate_bundle(tmp_path)
    payload_values = json.loads(_normalized_payload())
    payload_values[0]["is_suspended"] = False
    payload_values[0]["is_trading"] = True
    payload_values[0]["adjust_factor"] = None
    payload_values[0]["quality_status"] = "ready"
    payload_values[0]["quality_issues"] = []
    payload = json.dumps(payload_values, sort_keys=True, separators=(",", ":")).encode()
    rejected_report = evaluate_candidate_gates(
        candidate_id=candidate.candidate_id,
        trade_date=candidate.trade_date,
        required_symbols=("sh.000001", "sh.600000", "sz.399001"),
        rows=tuple(DailyBar.model_validate(row) for row in payload_values),
        evidence=evidence,
        created_at=NOW,
    )
    values = {
        key: value
        for key, value in candidate.model_dump(mode="python").items()
        if key != "manifest_sha256"
    }
    values.update(
        {
            "normalized_object_sha256": hashlib.sha256(payload).hexdigest(),
            "gate_report_sha256": rejected_report.aggregate_sha256,
            "status": "rejected",
        }
    )
    rejected = build_candidate_manifest(**values)
    result = CandidateStore(tmp_path).publish_chain(
        report=rejected_report,
        candidate=rejected,
        selection=None,
        evidence=evidence,
        normalized_payload=payload,
    )
    assert isinstance(result, PublishedCandidateRejection)
    bundle = tmp_path / "bundles" / candidate.candidate_id
    assert (bundle / "gate.json").is_file()
    assert (bundle / "candidate.json").is_file()
    assert (bundle / "normalized.json").is_file()
    assert not (bundle / "selection.json").exists()
    assert result.gate_report.verdict == "fail"
    evidence.close()


def test_evaluator_reuses_bound_reader_without_reopening_root(tmp_path, monkeypatch):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    reader = evidence._reader
    assert reader is not None

    def replay_bomb(*_args, **_kwargs):
        raise AssertionError("evaluator must not replay through a second reader")

    monkeypatch.setattr(reader, "replay", replay_bomb)
    evaluate_candidate_gates(
        candidate_id=candidate.candidate_id,
        trade_date=candidate.trade_date,
        required_symbols=("sh.600000",),
        rows=(json.loads(_normalized_payload())[0],),
        evidence=evidence,
        created_at=report.created_at,
    )
    evidence.close()


def test_evaluator_rejects_root_rename_and_same_content_copy(tmp_path, monkeypatch):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    original_read_rows = PublishedEvidence.read_rows
    swapped = False

    def swap_after_read(instance):
        nonlocal swapped
        result = original_read_rows(instance)
        if not swapped:
            swapped = True
            root = Path(instance._reader.root)
            moved = root.with_name("evidence-original")
            root.rename(moved)
            shutil.copytree(moved, root)
        return result

    monkeypatch.setattr(PublishedEvidence, "read_rows", swap_after_read)
    with pytest.raises(ValueError, match="readback"):
        evaluate_candidate_gates(
            candidate_id=candidate.candidate_id,
            trade_date=candidate.trade_date,
            required_symbols=("sh.600000",),
            rows=(json.loads(_normalized_payload())[0],),
            evidence=evidence,
            created_at=report.created_at,
        )
    evidence.close()


def test_normalized_subset_or_universe_substitution_is_rejected(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    substituted = json.loads(_normalized_payload())
    substituted[0]["symbol"] = "sh.600001"
    substituted[0]["source_record_id"] = "sh.600001-2026-08-20"
    payload = json.dumps(substituted, sort_keys=True, separators=(",", ":")).encode()
    values = {
        key: value
        for key, value in candidate.model_dump(mode="python").items()
        if key != "manifest_sha256"
    }
    values["normalized_object_sha256"] = hashlib.sha256(payload).hexdigest()
    substituted_candidate = build_candidate_manifest(**values)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(substituted_candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    with pytest.raises(ValueError, match="universe|gate report"):
        CandidateStore(tmp_path).publish_chain(
            report=report,
            candidate=substituted_candidate,
            selection=selection,
            evidence=evidence,
            normalized_payload=payload,
        )
    evidence.close()


def test_publish_requires_approved_five_request_plan_shape(tmp_path):
    evidence, _report, _candidate = _real_candidate_bundle(tmp_path)
    with pytest.raises(ValueError, match="logical request plan"):
        from types import SimpleNamespace

        from backend.app.market.candidates import _validate_approved_plan

        malformed = SimpleNamespace(
            manifest=SimpleNamespace(
                logical_request_plan=SimpleNamespace(
                    requests=evidence.manifest.logical_request_plan.requests[:1]
                ),
                trade_date=evidence.manifest.trade_date,
            )
        )
        _validate_approved_plan(malformed)
    evidence.close()


def test_post_rename_root_failure_rolls_back_owned_bundle(tmp_path, monkeypatch):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    store = CandidateStore(tmp_path)
    calls = 0

    def fail_after_rename(_fd, _identity):
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise EvidenceError("post-rename root replacement", "EVIDENCE_HASH_MISMATCH")

    monkeypatch.setattr(store, "_assert_root", fail_after_rename)
    with pytest.raises(EvidenceError, match="post-rename"):
        store.publish_chain(
            report=report,
            candidate=candidate,
            selection=selection,
            evidence=evidence,
            normalized_payload=_normalized_payload(),
        )
    assert not (tmp_path / "bundles" / candidate.candidate_id).exists()
    evidence.close()


def test_post_rename_root_replacement_quarantines_copy(tmp_path, monkeypatch):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    store = CandidateStore(tmp_path)
    calls = 0

    def replace_after_rename(_fd, _identity):
        nonlocal calls
        calls += 1
        if calls >= 2:
            old = tmp_path.with_name("candidate-root-original")
            tmp_path.rename(old)
            shutil.copytree(old, tmp_path)
            raise EvidenceError("post-rename root replacement", "EVIDENCE_HASH_MISMATCH")

    monkeypatch.setattr(store, "_assert_root", replace_after_rename)
    with pytest.raises(EvidenceError, match="post-rename"):
        store.publish_chain(
            report=report,
            candidate=candidate,
            selection=selection,
            evidence=evidence,
            normalized_payload=_normalized_payload(),
        )
    assert not (tmp_path / "bundles" / candidate.candidate_id).exists()
    assert list((tmp_path / "orphan-audit").iterdir())
    evidence.close()


def test_selection_gc_closes_held_root_fd(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    published = CandidateStore(tmp_path).publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        evidence=evidence,
        normalized_payload=_normalized_payload(),
    )
    assert published is not None and published._root_fd is not None
    fd = published._root_fd
    del published
    gc.collect()
    with pytest.raises(OSError):
        os.fstat(fd)
    evidence.close()


def test_cas_existing_winner_is_preserved_when_replayed(tmp_path):
    evidence, report, candidate = _real_candidate_bundle(tmp_path)
    selection = select_primary_candidate(
        trade_date=candidate.trade_date,
        universe_id=candidate.universe_id,
        candidates=(candidate,),
        selected_at=NOW,
        gate_report=report,
        evidence=evidence,
    )
    store = CandidateStore(tmp_path)
    first = store.publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        evidence=evidence,
        normalized_payload=_normalized_payload(),
    )
    assert first is not None
    selection_bytes = (
        tmp_path / "bundles" / candidate.candidate_id / "selection.json"
    ).read_bytes()
    second = store.publish_chain(
        report=report,
        candidate=candidate,
        selection=selection,
        evidence=evidence,
        normalized_payload=_normalized_payload(),
    )
    assert second is not None
    assert (
        tmp_path / "bundles" / candidate.candidate_id / "selection.json"
    ).read_bytes() == selection_bytes
    first.close()
    second.close()
    evidence.close()
