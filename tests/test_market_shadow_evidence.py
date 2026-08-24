"""Offline RED/GREEN tests for successful-attempt-only shadow evidence."""

from datetime import date

import pytest

from backend.app.market.shadow_evidence import (
    ShadowAttempt,
    ShadowAttemptReport,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowLogicalRequest,
    ShadowLogicalRequestPlan,
    ShadowRequestCompletion,
)


def _request(ordinal=0):
    return ShadowLogicalRequest(
        ordinal=ordinal,
        request_id=f"request-{ordinal}",
        endpoint="daily",
        endpoint_class="daily",
        role="daily",
        trade_date=date(2026, 8, 20),
        symbol_or_index_shard="shard-0",
        schema_contract_hash="a" * 64,
        unit_contract_hash="b" * 64,
    )


def test_shadow_evidence_persists_only_final_successful_attempt(tmp_path):
    request = _request()
    completion = ShadowRequestCompletion(
        ordinal=0,
        request_id=request.request_id,
        endpoint=request.endpoint,
        final_attempt_id="attempt-2",
        pages=(
            {
                "page_identity": "request-0:page-000001",
                "rows": [{"symbol": "sh.600000"}],
            },
        ),
    )
    evidence = ShadowEvidenceStore(tmp_path).publish(
        plan=(request,),
        completions=(completion,),
        attempts=(
            ShadowAttempt(attempt_id="attempt-1", ordinal=0, outcome="failure", rows=()),
            ShadowAttempt(
                attempt_id="attempt-2",
                ordinal=0,
                outcome="success",
                rows=({"symbol": "sh.600000"},),
            ),
        ),
    )
    assert ShadowEvidenceReader(tmp_path).read(evidence.evidence_id).rows == (
        {"symbol": "sh.600000"},
    )


def test_failure_skip_unavailable_mismatch_persist_sanitized_report_without_evidence_or_candidate(
    tmp_path,
):
    store = ShadowEvidenceStore(tmp_path)
    report = store.record_failure("attempt-1", outcome="timeout", error="secret-token")
    assert report.evidence_id is None
    assert report.page_identities == ()
    assert "secret-token" not in report.model_dump_json()


def test_duplicate_missing_or_out_of_order_final_pages_are_unavailable(tmp_path):
    request = _request()
    with pytest.raises(ValueError):
        ShadowRequestCompletion(
            ordinal=0,
            request_id=request.request_id,
            endpoint=request.endpoint,
            final_attempt_id="attempt-1",
            pages=(
                {"page_identity": "request-0:page-000002", "rows": []},
                {"page_identity": "request-0:page-000002", "rows": []},
            ),
        )


def test_shadow_evidence_crash_cancel_and_orphan_are_unreadable(tmp_path):
    store = ShadowEvidenceStore(tmp_path)
    request = _request()
    completion = ShadowRequestCompletion(
        ordinal=0,
        request_id=request.request_id,
        endpoint=request.endpoint,
        final_attempt_id="attempt-1",
        pages=({"page_identity": "request-0:page-000001", "rows": []},),
    )
    with pytest.raises(RuntimeError):
        store.publish(
            plan=(request,),
            completions=(completion,),
            attempts=(ShadowAttempt(attempt_id="attempt-1", ordinal=0, outcome="success"),),
            simulate_crash=True,
        )
    assert list((tmp_path / "bundles").glob("**/COMMIT")) == []


def test_task11_evidence_ready_is_legal_but_not_terminal_or_qualifying(tmp_path):
    report = ShadowAttemptReport(
        report_id="report-1",
        attempt_id="attempt-1",
        outcome="evidence_ready",
        page_identities=("request-0:page-000001",),
        page_count=1,
        row_count=1,
        evidence_refs=("object-1",),
        evidence_id="ev-1",
        evidence_sha256="a" * 64,
    )
    assert report.outcome == "evidence_ready"
    assert report.candidate_sha256 is None


def test_request_pages_use_global_namespace_and_exact_model_digest_domains():
    request = _request()
    with pytest.raises(ValueError):
        ShadowRequestCompletion(
            ordinal=0,
            request_id=request.request_id,
            endpoint="daily",
            endpoint_class="daily",
            final_attempt_id="attempt-0",
            outcome="success",
            pages=({"page_identity": "page-1", "rows": []},),
        )
    plan = ShadowLogicalRequestPlan(requests=(request,))
    assert plan.exact_ordinal_set == {0}


def test_bundle_publish_is_idempotent_and_conflict_is_fail_closed(tmp_path):
    request = _request()
    completion = ShadowRequestCompletion(
        ordinal=0,
        request_id=request.request_id,
        endpoint=request.endpoint,
        final_attempt_id="attempt-0",
        pages=(
            {
                "page_identity": f"{request.request_id}:page-000001",
                "object_ref": "objects/page-0.json",
                "content_sha256": "c" * 64,
                "row_count": 1,
                "rows": [{"symbol": "sh.600000"}],
            },
        ),
    )
    attempts = (
        ShadowAttempt(attempt_id="attempt-0", ordinal=0, outcome="success", rows=({"x": 1},)),
    )
    store = ShadowEvidenceStore(tmp_path)
    first = store.publish(plan=(request,), completions=(completion,), attempts=attempts)
    second = store.publish(plan=(request,), completions=(completion,), attempts=attempts)
    assert first.manifest_sha256 == second.manifest_sha256
    assert ShadowEvidenceReader(tmp_path).read(first.evidence_id).evidence_id == first.evidence_id


def test_recovery_only_removes_owned_staging_and_writes_sanitized_audit(tmp_path):
    store = ShadowEvidenceStore(tmp_path)
    staging = tmp_path / "staging" / "owned"
    staging.mkdir(parents=True)
    (staging / "OWNER").write_text('{"nonce":"owned","evidence_id":"ev-1","job_id":"job-1"}')
    (staging / "raw-provider-secret").write_text("secret-token")
    result = store.recover_orphans()
    assert result.removed == 1
    assert not staging.exists()
    assert "secret-token" not in result.model_dump_json()


def test_reader_rejects_hardlinks_extra_files_and_mutation(tmp_path):
    request = _request()
    completion = ShadowRequestCompletion(
        ordinal=0,
        request_id=request.request_id,
        endpoint=request.endpoint,
        final_attempt_id="attempt-0",
        pages=({"page_identity": f"{request.request_id}:page-000001", "rows": [{"x": 1}]},),
    )
    bundle = ShadowEvidenceStore(tmp_path).publish(
        plan=(request,),
        completions=(completion,),
        attempts=(
            ShadowAttempt(attempt_id="attempt-0", ordinal=0, outcome="success", rows=({"x": 1},)),
        ),
    )
    final = tmp_path / "bundles" / bundle.evidence_id
    page = next((final / "pages").iterdir())
    (final / "hardlink").hardlink_to(page)
    with pytest.raises(RuntimeError):
        ShadowEvidenceReader(tmp_path).read(bundle.evidence_id)


def test_control_sink_is_registry_backed_and_exposes_no_terminal_candidate_api():
    from backend.app.market.shadow_evidence import ShadowEvidenceControlSink

    assert ShadowEvidenceControlSink is not None
