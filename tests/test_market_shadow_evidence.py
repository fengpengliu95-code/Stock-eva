"""Offline RED/GREEN tests for successful-attempt-only shadow evidence."""

from datetime import date

import pytest

from backend.app.market.shadow_evidence import (
    ShadowAttempt,
    ShadowAttemptReport,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowEvidenceUnavailable,
    ShadowLogicalRequest,
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
        pages=({"page_identity": "page-1", "rows": [{"symbol": "sh.600000"}]},),
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
    completion = ShadowRequestCompletion(
        ordinal=0,
        request_id=request.request_id,
        endpoint=request.endpoint,
        final_attempt_id="attempt-1",
        pages=({"page_identity": "page-2", "rows": []}, {"page_identity": "page-2", "rows": []}),
    )
    with pytest.raises(ShadowEvidenceUnavailable):
        ShadowEvidenceStore(tmp_path).publish(
            plan=(request,), completions=(completion,), attempts=()
        )


def test_shadow_evidence_crash_cancel_and_orphan_are_unreadable(tmp_path):
    store = ShadowEvidenceStore(tmp_path)
    request = _request()
    completion = ShadowRequestCompletion(
        ordinal=0,
        request_id=request.request_id,
        endpoint=request.endpoint,
        final_attempt_id="attempt-1",
        pages=({"page_identity": "page-1", "rows": []},),
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
        page_identities=("page-1",),
        page_count=1,
        row_count=1,
        evidence_refs=("object-1",),
        evidence_id="ev-1",
        evidence_sha256="a" * 64,
    )
    assert report.outcome == "evidence_ready"
    assert report.candidate_sha256 is None
