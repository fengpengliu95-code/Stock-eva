from datetime import date

import pytest

from backend.app.market.providers.registry import ShadowRegistry
from backend.app.market.shadow_candidates import (
    ShadowCandidateReader,
    ShadowCandidateStore,
    ShadowCandidateUnavailable,
)
from backend.app.market.shadow_evidence import (
    ShadowAttempt,
    ShadowCompletion,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowLogicalRequest,
    ShadowLogicalRequestPlan,
    ShadowRequestCompletion,
)
from backend.app.market.shadow_normalize import REVIEWED_TUSHARE_UNIT_CONTRACT, normalize_tushare


def bindings(tmp_path, *, provider="tushare", universe="u"):
    request = ShadowLogicalRequest(
        job_id="job-1",
        provider_id=provider,
        window_id="window-1",
        ordinal=0,
        request_id="request-0",
        endpoint="daily",
        endpoint_class="daily",
        role="daily",
        trade_date=date(2026, 8, 20),
        symbol_or_index_shard="symbols",
        schema_contract_hash="a" * 64,
        unit_contract_hash="b" * 64,
    )
    plan = ShadowLogicalRequestPlan(
        job_id="job-1", provider_id=provider, window_id="window-1", requests=(request,)
    )
    page = {"page_identity": "request-0:page-000001", "rows": [{"symbol": "sz.000001"}]}
    request_completion = ShadowRequestCompletion(
        ordinal=0,
        request_id="request-0",
        endpoint="daily",
        final_attempt_id="attempt-1",
        pages=(page,),
    )
    completion = ShadowCompletion(
        job_id="job-1",
        provider_id=provider,
        window_id="window-1",
        session_id="session-1",
        evidence_id="ev-1",
        request_plan_sha256=plan.request_plan_sha256,
        requests=(request_completion,),
    )
    ShadowEvidenceStore(tmp_path / "evidence").publish(
        plan=plan,
        completion=completion,
        attempts=(
            ShadowAttempt(
                attempt_id="attempt-1",
                job_id="job-1",
                provider_id=provider,
                ordinal=0,
                outcome="success",
                rows=({"symbol": "sz.000001"},),
                pages=(page,),
            ),
        ),
    )
    reader = ShadowEvidenceReader(tmp_path / "evidence")
    registry = ShadowRegistry.in_memory()
    registry.initialize()
    connection = registry._memory_connection
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute(
        "INSERT INTO shadow_job (job_id,provider_id,window_id,trade_date,universe_id,"
        "canonical_manifest_generation,canonical_manifest_sha256,version_vector_sha256,"
        "run_status,attempt_count,state_version) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            "job-1",
            provider,
            "window-1",
            "2026-08-20",
            universe,
            "g1",
            "m" * 64,
            "v" * 64,
            "pending_normalization",
            1,
            1,
        ),
    )
    evidence = reader.read("ev-1")
    connection.execute(
        "INSERT INTO shadow_evidence_ref (evidence_id,job_id,provider_id,window_id,session_id,"
        "completion_sha256,evidence_sha256,bundle_ref,bundle_sha256,attached_session_report_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,NULL)",
        (
            "ev-1",
            "job-1",
            provider,
            "window-1",
            "session-1",
            evidence.completion_sha256,
            evidence.manifest_sha256,
            "bundles/ev-1",
            evidence.manifest_sha256,
        ),
    )
    connection.commit()
    return reader, registry


def candidate(complete=True):
    payload = {
        "daily": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260820",
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "pre_close": 10,
                "vol": 100,
                "amount": 20,
            }
        ],
        "universe": [{"ts_code": "000001.SZ", "list_status": "L"}],
        "indexes": [],
        "adj_factor": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260820",
                "anchor_date": "2026-08-20",
                "adj_factor": 1,
                "prev_adj_factor": 1,
                "factor_semantics": "multiplicative_back_adjust",
            }
        ],
        "suspend_d": [],
        "stock_basic": [{"ts_code": "000001.SZ", "list_status": "L"}],
        "units": {
            "vol": "lots",
            "amount": "thousand_cny",
            "factor_semantics": "multiplicative_back_adjust",
            "factor_anchor": "trade_date",
            "factor_direction": "back_adjust",
        },
        "job_id": "job-1",
        "window_id": "window-1",
        "session_id": "session-1",
        "version_vector_sha256": "v" * 64,
    }
    value = normalize_tushare(
        payload,
        trade_date=date(2026, 8, 20),
        universe_id="u",
        contract=REVIEWED_TUSHARE_UNIT_CONTRACT,
    )
    return value.__class__.model_validate(
        {**value.model_dump(mode="json"), "complete": complete, "normalized_sha256": "0" * 64}
    )


def test_shadow_quality_report_and_candidate_bundle_are_complete_or_unavailable(
    tmp_path, monkeypatch
):
    reader, registry = bindings(tmp_path)
    store = ShadowCandidateStore(tmp_path / "shadow")
    manifest = store.publish(
        candidate(), evidence_reader=reader, evidence_id="ev-1", registry=registry
    )
    bundle = ShadowCandidateReader(
        tmp_path / "shadow", evidence_reader=reader, registry=registry
    ).read(manifest.candidate_id)
    assert bundle.manifest.evidence_sha256 == reader.read("ev-1").manifest_sha256
    assert bundle.quality_report.verdict == "pass"


def test_candidate_store_recomputes_quality_and_forged_or_incomplete_pass_has_zero_writes(
    tmp_path, monkeypatch
):
    reader, registry = bindings(tmp_path)
    root = tmp_path / "shadow"
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateStore(root).publish(
            candidate(complete=False), evidence_reader=reader, evidence_id="ev-1", registry=registry
        )
    assert not root.exists()


def test_candidate_store_rejects_evidence_provider_date_universe_or_plan_drift(
    tmp_path, monkeypatch
):
    reader, registry = bindings(tmp_path, provider="tickflow")
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateStore(tmp_path / "shadow").publish(
            candidate(), evidence_reader=reader, evidence_id="ev-1", registry=registry
        )
    reader, registry = bindings(tmp_path / "other", universe="other")
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateStore(tmp_path / "shadow2").publish(
            candidate(), evidence_reader=reader, evidence_id="ev-1", registry=registry
        )


def test_candidate_bundle_reader_rejects_symlink_extra_file_and_mutation(tmp_path):
    reader, registry = bindings(tmp_path)
    root = tmp_path / "shadow"
    manifest = ShadowCandidateStore(root).publish(
        candidate(), evidence_reader=reader, evidence_id="ev-1", registry=registry
    )
    bundle = root / "bundles" / manifest.candidate_id
    (bundle / "extra").write_text("x")
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateReader(root, evidence_reader=reader, registry=registry).read(
            manifest.candidate_id
        )


def test_candidate_store_and_reader_require_real_evidence_reader_and_registry(tmp_path):
    root = tmp_path / "shadow"
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateStore(root).publish(
            candidate(), evidence_reader=object(), evidence_id="ev-1", registry=object()
        )
    with pytest.raises(ShadowCandidateUnavailable):
        ShadowCandidateReader(root, evidence_reader=object(), registry=object())
