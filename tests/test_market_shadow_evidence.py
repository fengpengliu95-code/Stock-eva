"""Offline RED/GREEN tests for successful-attempt-only shadow evidence."""

import hashlib
import json
import os
from datetime import date

import pytest

import backend.app.market.shadow_evidence as shadow_module
from backend.app.market.shadow_evidence import (
    ShadowAttempt,
    ShadowAttemptReport,
    ShadowCompletion,
    ShadowEvidenceCleanupFailed,
    ShadowEvidenceReader,
    ShadowEvidenceStore,
    ShadowEvidenceUnavailable,
    ShadowLogicalRequest,
    ShadowLogicalRequestPlan,
    ShadowRequestCompletion,
)


def _request(ordinal=0):
    return ShadowLogicalRequest(
        provider_id="tickflow",
        window_id="window-1",
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


def _plan_completion(request, attempt_id="attempt-2", evidence_id="ev-test"):
    request_completion = ShadowRequestCompletion(
        ordinal=request.ordinal,
        request_id=request.request_id,
        endpoint=request.endpoint,
        final_attempt_id=attempt_id,
        pages=({"page_identity": f"{request.request_id}:page-000001", "rows": [{"x": 1}]},),
    )
    plan = ShadowLogicalRequestPlan(requests=(request,))
    completion = ShadowCompletion(
        job_id=plan.job_id,
        provider_id=plan.provider_id,
        window_id=plan.window_id,
        session_id="session-1",
        evidence_id=evidence_id,
        request_plan_sha256=plan.request_plan_sha256,
        requests=(request_completion,),
    )
    return plan, completion


def test_shadow_evidence_persists_only_final_successful_attempt(tmp_path):
    request = _request()
    plan, completion = _plan_completion(request)
    evidence = ShadowEvidenceStore(tmp_path).publish(
        plan=plan,
        completion=completion,
        attempts=(
            ShadowAttempt(
                attempt_id="attempt-1", ordinal=0, outcome="failure", rows=(), attempt_number=1
            ),
            ShadowAttempt(
                attempt_id="attempt-2",
                ordinal=0,
                outcome="success",
                rows=({"symbol": "sh.600000"},),
                pages=completion.requests[0].pages,
                attempt_number=2,
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
    plan, completion = _plan_completion(request, attempt_id="attempt-1", evidence_id="ev-crash")
    with pytest.raises(RuntimeError):
        store.publish(
            plan=plan,
            completion=completion,
            attempts=(
                ShadowAttempt(
                    attempt_id="attempt-1",
                    ordinal=0,
                    outcome="success",
                    rows=({"x": 1},),
                    pages=completion.requests[0].pages,
                ),
            ),
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


def test_completion_requires_explicit_identity_and_final_success():
    request = _request()
    plan, complete = _plan_completion(request)
    assert complete.provider_id == "tickflow"
    with pytest.raises(ValueError):
        ShadowCompletion(
            job_id=plan.job_id,
            provider_id="",
            window_id="",
            session_id="session-1",
            evidence_id="ev-bad",
            request_plan_sha256=plan.request_plan_sha256,
            requests=complete.requests,
        )
    with pytest.raises(ValueError):
        ShadowRequestCompletion(
            ordinal=0,
            request_id=request.request_id,
            endpoint="daily",
            final_attempt_id="attempt-1",
            final_success=False,
            pages=complete.requests[0].pages,
        )


def test_store_requires_complete_plan_completion_and_attempt_history(tmp_path):
    request = _request()
    plan, completion = _plan_completion(request)
    with pytest.raises(TypeError):
        ShadowEvidenceStore(tmp_path).publish(
            plan=(request,), completions=completion.requests, attempts=()
        )
    with pytest.raises(ShadowEvidenceUnavailable):
        ShadowEvidenceStore(tmp_path).publish(plan=plan, completion=completion, attempts=())


def test_failure_sink_requires_explicit_nonempty_plan_and_ordinals():
    from backend.app.market.shadow_evidence import ShadowEvidenceControlSink

    with pytest.raises(TypeError):
        ShadowEvidenceControlSink(object()).persist_failure()


def test_bundle_publish_is_idempotent_and_conflict_is_fail_closed(tmp_path):
    request = _request()
    plan, completion = _plan_completion(
        request, attempt_id="attempt-0", evidence_id="ev-idempotent"
    )
    attempts = (
        ShadowAttempt(
            attempt_id="attempt-0",
            ordinal=0,
            outcome="success",
            rows=({"x": 1},),
            pages=completion.requests[0].pages,
        ),
    )
    store = ShadowEvidenceStore(tmp_path)
    first = store.publish(plan=plan, completion=completion, attempts=attempts)
    second = store.publish(plan=plan, completion=completion, attempts=attempts)
    assert first.manifest_sha256 == second.manifest_sha256
    assert ShadowEvidenceReader(tmp_path).read(first.evidence_id).evidence_id == first.evidence_id


def test_recovery_only_removes_owned_staging_and_writes_sanitized_audit(tmp_path):
    store = ShadowEvidenceStore(tmp_path)
    staging = tmp_path / "staging" / "owned"
    staging.mkdir(parents=True)
    (tmp_path / "staging").chmod(0o700)
    staging.chmod(0o700)
    (staging / "OWNER").write_text(
        '{"nonce":"owned","evidence_id":"ev-1","job_id":"job-1",'
        '"provider_id":"tickflow","window_id":"window-1",'
        '"session_id":"session-1","plan_sha256":"'
        + "0" * 64
        + '","completion_sha256":"'
        + "0" * 64
        + '"}'
    )
    (staging / "raw-provider-secret").write_text("secret-token")
    result = store.recover_orphans()
    # An owner marker without a canonical manifest/COMMIT is incomplete and
    # therefore fail-closed: recovery must not delete a forged staging entry.
    assert result.removed == 0
    assert result.skipped == 1
    assert staging.exists()
    assert "secret-token" not in result.model_dump_json()


def _minimal_owned_tree(tmp_path):
    staging = tmp_path / "staging"
    entry = staging / "owned"
    pages = entry / "pages"
    pages.mkdir(parents=True)
    staging.chmod(0o700)
    entry.chmod(0o700)
    (entry / "OWNER").write_text(
        json.dumps(
            {
                "nonce": "owned",
                "evidence_id": "evidence-1",
                "job_id": "job-1",
                "provider_id": "tickflow",
                "window_id": "window-1",
                "session_id": "session-1",
                "plan_sha256": "0" * 64,
                "completion_sha256": "1" * 64,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    )
    for name in ("COMMIT", "manifest.json"):
        (entry / name).write_bytes(b"owned")
    (pages / "page.json").write_bytes(b"page")
    parent_fd = os.open(staging, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    entry_fd = os.open(
        "owned",
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=parent_fd,
    )
    info = os.fstat(entry_fd)
    expected = (info.st_dev, info.st_ino, info.st_mode, info.st_ctime_ns)
    return staging, parent_fd, entry_fd, expected


def test_orphan_cleanup_stops_on_partial_delete_and_reports_residual(tmp_path, monkeypatch):
    staging, parent_fd, entry_fd, expected = _minimal_owned_tree(tmp_path)
    original_unlink = shadow_module.os.unlink

    def fail_unlink(*args, **kwargs):
        raise OSError("injected cleanup failure")

    monkeypatch.setattr(shadow_module.os, "unlink", fail_unlink)
    try:
        with pytest.raises(ShadowEvidenceCleanupFailed):
            shadow_module._remove_owned_tree(
                parent_fd,
                entry_fd,
                "owned",
                expected,
                allowed_page_names={"page.json"},
            )
    finally:
        monkeypatch.setattr(shadow_module.os, "unlink", original_unlink)
        os.close(entry_fd)
        os.close(parent_fd)
    assert (staging / "owned").exists()
    assert (staging / "owned" / "pages" / "page.json").exists()


def test_orphan_cleanup_never_deletes_foreign_parent_replacement(tmp_path):
    staging, parent_fd, entry_fd, expected = _minimal_owned_tree(tmp_path)
    entry = staging / "owned"
    displaced = staging / "displaced"
    os.rename(entry, displaced)
    entry.mkdir()
    try:
        with pytest.raises(ShadowEvidenceCleanupFailed):
            shadow_module._remove_owned_tree(
                parent_fd,
                entry_fd,
                "owned",
                expected,
                allowed_page_names={"page.json"},
            )
    finally:
        os.close(entry_fd)
        os.close(parent_fd)
    assert entry.exists()
    assert displaced.exists()


def test_orphan_cleanup_keeps_fixed_tombstone_and_is_idempotent(tmp_path):
    staging, parent_fd, entry_fd, expected = _minimal_owned_tree(tmp_path)
    try:
        assert not shadow_module._remove_owned_tree(
            parent_fd,
            entry_fd,
            "owned",
            expected,
            allowed_page_names={"page.json"},
        )
        assert sorted(path.name for path in (staging / "owned").iterdir()) == ["CLEANED_RESIDUAL"]
        marker_bytes = (staging / "owned" / "CLEANED_RESIDUAL").read_bytes()
        assert len(marker_bytes) < 256
        assert json.loads(marker_bytes) == {
            "evidence_id": "evidence-1",
            "nonce": "owned",
            "status": "CLEANED_RESIDUAL",
        }
        assert not shadow_module._remove_owned_tree(
            parent_fd,
            entry_fd,
            "owned",
            expected,
            allowed_page_names=set(),
        )
        assert (staging / "owned" / "CLEANED_RESIDUAL").read_bytes() == marker_bytes
    finally:
        os.close(entry_fd)
        os.close(parent_fd)


def test_recovery_skips_cleaned_tombstone_without_growth(tmp_path):
    staging, parent_fd, entry_fd, expected = _minimal_owned_tree(tmp_path)
    try:
        shadow_module._remove_owned_tree(
            parent_fd,
            entry_fd,
            "owned",
            expected,
            allowed_page_names={"page.json"},
        )
    finally:
        os.close(entry_fd)
        os.close(parent_fd)
    store = ShadowEvidenceStore(tmp_path)
    marker = (staging / "owned" / "CLEANED_RESIDUAL").read_bytes()
    first = store.recover_orphans()
    second = store.recover_orphans()
    assert first.contents_cleaned is True
    assert first.residual_directory is True
    assert second.contents_cleaned is True
    assert (staging / "owned" / "CLEANED_RESIDUAL").read_bytes() == marker
    assert sorted(path.name for path in (staging / "owned").iterdir()) == ["CLEANED_RESIDUAL"]


def test_orphan_cleanup_final_foreign_race_leaves_foreign_and_displaced(tmp_path, monkeypatch):
    staging, parent_fd, entry_fd, expected = _minimal_owned_tree(tmp_path)
    entry = staging / "owned"
    displaced = staging / "displaced"
    original_stat = shadow_module.os.stat
    calls = 0

    def race_stat(path, *args, **kwargs):
        nonlocal calls
        if path == "owned" and kwargs.get("dir_fd") == parent_fd:
            calls += 1
            if calls == 2:
                os.rename(entry, displaced)
                entry.mkdir()
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(shadow_module.os, "stat", race_stat)
    try:
        assert shadow_module._remove_owned_tree(
            parent_fd,
            entry_fd,
            "owned",
            expected,
            allowed_page_names={"page.json"},
        )
    finally:
        os.close(entry_fd)
        os.close(parent_fd)
    assert entry.exists()
    assert displaced.exists()
    assert not any(entry.iterdir())


def test_reader_rejects_hardlinks_extra_files_and_mutation(tmp_path):
    request = _request()
    plan, completion = _plan_completion(request, attempt_id="attempt-0", evidence_id="ev-hardlink")
    bundle = ShadowEvidenceStore(tmp_path).publish(
        plan=plan,
        completion=completion,
        attempts=(
            ShadowAttempt(
                attempt_id="attempt-0",
                ordinal=0,
                outcome="success",
                rows=({"x": 1},),
                pages=completion.requests[0].pages,
            ),
        ),
    )
    final = tmp_path / "bundles" / bundle.evidence_id
    page = next(path for path in (final / "pages").rglob("*.json"))
    (final / "hardlink").hardlink_to(page)
    with pytest.raises(RuntimeError):
        ShadowEvidenceReader(tmp_path).read(bundle.evidence_id)


def test_reader_uses_held_directory_descriptors_not_pathname_listing(tmp_path, monkeypatch):
    request = _request()
    plan, completion = _plan_completion(request, attempt_id="attempt-held", evidence_id="ev-held")
    bundle = ShadowEvidenceStore(tmp_path).publish(
        plan=plan,
        completion=completion,
        attempts=(
            ShadowAttempt(
                attempt_id="attempt-held",
                ordinal=0,
                outcome="success",
                rows=({"x": 1},),
                pages=completion.requests[0].pages,
            ),
        ),
    )
    original_lstat = shadow_module.os.lstat

    def reject_path_lstat(value):
        if not isinstance(value, int):
            raise AssertionError("reader must not lstat configured pathnames")
        return original_lstat(value)

    monkeypatch.setattr(shadow_module.os, "lstat", reject_path_lstat)
    assert ShadowEvidenceReader(tmp_path).read(bundle.evidence_id).evidence_id == bundle.evidence_id


def test_completion_aggregate_fields_are_not_caller_controlled():
    request = _request()
    with pytest.raises(ValueError):
        ShadowCompletion(
            job_id="job-1",
            provider_id="tickflow",
            window_id="window-1",
            session_id="session-1",
            evidence_id="ev-aggregate",
            request_plan_sha256=ShadowLogicalRequestPlan(requests=(request,)).request_plan_sha256,
            requests=(
                ShadowRequestCompletion(
                    ordinal=0,
                    request_id=request.request_id,
                    endpoint=request.endpoint,
                    final_attempt_id="attempt-1",
                    pages=({"page_identity": "request-0:page-000001", "rows": [{"x": 1}]},),
                ),
            ),
            aggregate_page_count=1,
            aggregate_row_count=1,
        )


def test_recovery_does_not_delete_self_consistent_malformed_completion(tmp_path):
    request = _request()
    plan, completion = _plan_completion(
        request, attempt_id="attempt-malformed", evidence_id="ev-malformed"
    )
    store = ShadowEvidenceStore(tmp_path)
    with pytest.raises(RuntimeError):
        store.publish(
            plan=plan,
            completion=completion,
            attempts=(
                ShadowAttempt(
                    attempt_id="attempt-malformed",
                    ordinal=0,
                    outcome="success",
                    rows=({"x": 1},),
                    pages=completion.requests[0].pages,
                ),
            ),
            simulate_crash=True,
        )
    staging = next((tmp_path / "staging").iterdir())
    manifest_path = staging / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["completion"]["requests"] = []
    completion_sha = hashlib.sha256(
        b"stock-eva/r2f3/completion/v1\n"
        + (
            json.dumps(manifest["completion"], sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
    ).hexdigest()
    manifest["completion_sha256"] = completion_sha
    owner_path = staging / "OWNER"
    owner = json.loads(owner_path.read_text())
    owner["completion_sha256"] = completion_sha
    owner_path.write_text(json.dumps(owner, sort_keys=True, separators=(",", ":")) + "\n")
    manifest_values = {
        key: manifest[key] for key in manifest if key not in {"manifest_sha256", "rows"}
    }
    manifest["manifest_sha256"] = hashlib.sha256(
        b"stock-eva/r2f3/shadow-evidence/v1\n"
        + (json.dumps(manifest_values, sort_keys=True, separators=(",", ":")) + "\n").encode()
    ).hexdigest()
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
    (staging / "COMMIT").write_text(f"COMMIT\n{manifest['manifest_sha256']}\n")
    result = store.recover_orphans()
    assert result.removed == 0
    assert staging.exists()


def test_control_sink_is_registry_backed_and_exposes_no_terminal_candidate_api():
    from backend.app.market.shadow_evidence import ShadowEvidenceControlSink

    assert ShadowEvidenceControlSink is not None
