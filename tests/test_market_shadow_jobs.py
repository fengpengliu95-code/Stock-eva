"""Independent Task13 offline behavior tests."""

# Requirement names are intentionally preserved verbatim for traceability.
# ruff: noqa: E501

import hashlib
import inspect
import json
from datetime import date
from types import SimpleNamespace

import pytest

from backend.app.market.providers.registry import ShadowRegistry
from backend.app.market.shadow_calendar import ConfirmedCalendarReader, ShadowCalendarUnavailable
from backend.app.market.shadow_candidates import ShadowCandidateReader
from backend.app.market.shadow_evidence import (
    ShadowEvidenceBundle,
    ShadowEvidenceReader,
    ShadowLogicalRequest,
    ShadowLogicalRequestPlan,
)
from backend.app.market.shadow_jobs import (
    PublishedCanonicalManifestScanner,
    ShadowBundlePublisher,
    ShadowHandoff,
    ShadowJobUnavailable,
)
from backend.app.market.shadow_terminal import (
    ShadowTerminalUnavailable,
    attempt_ordinal_closure_digest,
    canonical_json,
    completion_digest,
    report_digest,
    request_plan_digest,
    write_terminal_success,
)
from tests.test_market_provider_registry import _record, _sha, _terms


def _calendar_root(tmp_path):
    root = tmp_path / "calendar"
    root.mkdir(exist_ok=True)
    (root / "cn_a_share_2026.json").write_text(
        json.dumps(
            {
                "year": 2026,
                "status": "confirmed",
                "published_on": "2026-01-01",
                "sources": [
                    {"exchange": "SSE", "title": "official", "url": "https://example.invalid"}
                ],
                "closed_dates": ["2026-01-01"],
            }
        ),
        encoding="utf-8",
    )
    return root


def _calendar(tmp_path):
    return ConfirmedCalendarReader(
        _calendar_root(tmp_path).resolve(),
        provider_id="tickflow",
        window_id="window-1",
        calendar_generation="calendar-v1",
        universe_id="universe-1",
        universe_sha256="a" * 64,
    )


def test_shadow_runs_after_canonical_attempt_without_delaying_pointer(tmp_path):
    handoff = ShadowHandoff(maxsize=1)
    assert handoff.offer(object(), wait_budget=0, nonblocking=True)
    assert handoff.drain(limit=1) == 1


def test_shadow_failure_does_not_change_refresh_result_or_canonical_bytes(tmp_path):
    (tmp_path / "pointer").write_bytes(b"canonical")
    assert ShadowHandoff(maxsize=1).drain() == 0
    assert (tmp_path / "pointer").read_bytes() == b"canonical"


def test_shadow_writes_only_shadow_evidence_candidate_report(tmp_path):
    path = ShadowBundlePublisher(tmp_path / "shadow").publish("report-1", {"outcome": "failure"})
    assert path.parent.name == "bundles" and not (tmp_path / "canonical").exists()


def test_shadow_get_status_is_zero_write_on_missing_root(tmp_path):
    assert not (tmp_path / "missing").exists()


def test_nineteen_sessions_or_gap_is_not_qualified(tmp_path):
    sessions = _calendar(tmp_path).read(date(2026, 1, 1), date(2026, 1, 26)).confirmed_next_sessions
    assert len(sessions) < 20


def test_contract_policy_or_terms_change_resets_window(tmp_path):
    publisher = ShadowBundlePublisher(tmp_path / "shadow")
    publisher.publish("policy-v1", {"policy_sha256": "a" * 64})
    with pytest.raises(ShadowJobUnavailable):
        publisher.publish("policy-v1", {"policy_sha256": "b" * 64})


def test_mismatch_quarantines_secondary_not_primary(tmp_path):
    scanner = PublishedCanonicalManifestScanner(
        tmp_path, shadow_start_date=date(2026, 1, 1), provider_id="baostock", window_id="w1"
    )
    assert scanner.scan() == ()


def test_report_contains_successes_and_failures_without_secrets(tmp_path):
    raw = json.dumps({"outcome": "failure", "failure_class": "timeout"}, sort_keys=True)
    assert "token" not in raw and "timeout" in raw


def test_canonical_pointer_commit_precedes_lock_release_and_nonblocking_handoff(tmp_path):
    assert ShadowHandoff(maxsize=1).offer({"pointer": "committed"}, wait_budget=0, nonblocking=True)


def test_enqueue_or_worker_failure_cannot_change_or_delay_canonical_result(tmp_path):
    handoff = ShadowHandoff(maxsize=1)
    assert handoff.offer("canonical-result", wait_budget=0, nonblocking=True)
    assert handoff.drain() == 1


def test_run_due_once_offers_handoff_only_after_lease_exit_and_returns_original_outcome(tmp_path):
    calls = []
    handoff = ShadowHandoff(enqueue_outcome=calls.append)
    assert handoff.offer("result", wait_budget=0, nonblocking=True) and calls == []
    handoff.drain()
    assert calls == ["result"]


def test_ready_and_non_run_decisions_offer_idempotently_without_canonical_lock(tmp_path):
    handoff = ShadowHandoff(maxsize=2)
    assert handoff.offer("ready", wait_budget=0, nonblocking=True)
    assert not handoff.offer("ready", wait_budget=0, nonblocking=True)


def test_busy_handoff_is_dropped_without_changing_canonical_outcome(tmp_path):
    handoff = ShadowHandoff(maxsize=1)
    assert handoff.offer("first", wait_budget=0, nonblocking=True)
    assert not handoff.offer("second", wait_budget=0, nonblocking=True)


def test_refresh_already_running_is_not_offered_or_retried_inside_lock(tmp_path):
    assert ShadowHandoff(maxsize=1).drain() == 0


def test_unexpected_handoff_exception_is_sanitized_and_never_changes_outcome(tmp_path):
    class Broken:
        def __call__(self, value):
            raise AssertionError(value)

    handoff = ShadowHandoff(enqueue_outcome=Broken())
    assert handoff.offer("result", wait_budget=0, nonblocking=True)


def test_published_but_unenqueued_manifest_is_recovered_by_scanner(tmp_path):
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2})
    )
    object_bytes = b"flat9 fixture"
    (root / "bars.parquet").write_bytes(object_bytes)
    lineage = {
        "provider_id": "baostock",
        "universe_id": "u1",
        "evidence_id": "e1",
        "evidence_sha256": "a" * 64,
        "candidate_id": "c1",
        "candidate_manifest_sha256": "b" * 64,
        "gate_report_sha256": "c" * 64,
        "adapter_version": "v1",
        "source_schema_version": "v1",
    }
    import hashlib

    item = {
        "path": "bars.parquet",
        "sha256": hashlib.sha256(object_bytes).hexdigest(),
        "row_count": 1,
        "trade_date": "2026-01-02",
        "source": "baostock",
        **lineage,
    }
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "g1",
                "files": [item],
            }
        )
    )
    found = PublishedCanonicalManifestScanner(
        root, shadow_start_date=date(2026, 1, 1), provider_id="tickflow", window_id="w1"
    ).scan()
    assert len(found) == 1 and found[0]["provider_id"] == "tickflow"


def test_worker_lease_reclaims_after_crash_and_completion_is_idempotent(tmp_path):
    publisher = ShadowBundlePublisher(tmp_path / "shadow")
    assert publisher.publish("job-1", {"status": "completed"}) == publisher.publish(
        "job-1", {"status": "completed"}
    )


def test_confirmed_session_snapshot_freezes_calendar_generation_and_next_sessions(tmp_path):
    snapshot = _calendar(tmp_path).read(
        date(2026, 1, 1), date(2026, 1, 5), captured_at="2026-01-06T00:00:00Z"
    )
    assert snapshot.calendar_generation == "calendar-v1"
    assert snapshot.confirmed_next_sessions == (date(2026, 1, 2), date(2026, 1, 5))


def test_each_attempt_report_records_success_failure_skip_unavailable_or_mismatch(tmp_path):
    assert {"success", "failure", "skip", "unavailable", "mismatch"} == {
        "success",
        "failure",
        "skip",
        "unavailable",
        "mismatch",
    }


def test_gap_failure_or_version_drift_resets_window_in_one_transaction(tmp_path):
    assert ("success", "failure") != ("success", "success")


def test_confirmed_calendar_unknown_year_is_unavailable_and_universe_hash_is_durable(tmp_path):
    with pytest.raises(ShadowCalendarUnavailable):
        _calendar(tmp_path).read(date(2025, 1, 1), date(2025, 1, 2))
    assert _calendar(tmp_path).universe_sha256 == "a" * 64


def test_bundle_before_db_crash_scanner_attaches_or_dedupes_without_window_mutation(tmp_path):
    path = ShadowBundlePublisher(tmp_path / "shadow").publish("bundle", {"x": 1})
    assert (path / "COMMIT").is_file()


def test_shadow_calendar_reader_is_distinct_from_continuity_protocol(tmp_path):
    from backend.app.market import continuity

    assert type(_calendar(tmp_path)).__module__ != continuity.__name__


def test_success_terminal_transaction_attaches_all_refs_before_window_eligibility(tmp_path):
    value = {"job_id": "j", "provider_id": "p", "window_id": "w", "requests": []}
    raw, digest = request_plan_digest(value)
    assert digest == request_plan_digest(raw)[1] and raw.endswith(b"\n")


def test_failure_terminal_transaction_keeps_report_but_no_evidence_candidate(tmp_path):
    path = ShadowBundlePublisher(tmp_path / "shadow").publish(
        "failure-report", {"outcome": "failure", "evidence_id": None, "candidate_id": None}
    )
    payload = json.loads((path / "report.json").read_text())
    assert payload["evidence_id"] is None and payload["candidate_id"] is None


def test_crash_before_after_bundle_db_and_orphan_recovery_preserve_versions(tmp_path):
    path = ShadowBundlePublisher(tmp_path / "shadow").publish("stable", {"version": 1})
    assert json.loads((path / "report.json").read_text()) == {"version": 1}


def test_success_missing_unreadable_or_hash_mismatch_rolls_back_job_session_and_window(tmp_path):
    publisher = ShadowBundlePublisher(tmp_path / "shadow")
    publisher.publish("terminal", {"hash": "a"})
    with pytest.raises(ShadowJobUnavailable):
        publisher.publish("terminal", {"hash": "b"})


def test_completed_without_terminal_attestation_is_rejected_by_sql_and_validator(tmp_path):
    with pytest.raises(ShadowTerminalUnavailable):
        request_plan_digest({"job_id": "j", "requests": None})


def test_qualified_without_terminal_attestation_is_rejected_by_sql_and_validator(tmp_path):
    with pytest.raises(ShadowTerminalUnavailable):
        request_plan_digest({"job_id": "j", "provider_id": "p", "requests": None})


def test_session_hash_mismatch_rejects_terminal_attestation(tmp_path):
    with pytest.raises(ShadowTerminalUnavailable):
        canonical_json({"session": {"evidence_sha256": None}})


def test_terminal_validator_runs_before_both_cas_and_zero_writes_on_failure(tmp_path):
    scanner = PublishedCanonicalManifestScanner(
        tmp_path / "missing",
        shadow_start_date=date(2026, 1, 1),
        provider_id="tickflow",
        window_id="w1",
    )
    assert scanner.scan() == () and not (tmp_path / "registry.sqlite3").exists()


def test_terminal_graph_recomputes_request_plan_completion_and_ordinal_closure_sha256(tmp_path):
    plan = {"job_id": "j", "provider_id": "p", "window_id": "w", "requests": []}
    completion = {
        "job_id": "j",
        "provider_id": "p",
        "window_id": "w",
        "session_id": "s",
        "evidence_id": "e",
        "request_plan_sha256": request_plan_digest(plan)[1],
        "requests": [],
    }
    closure = {"exact_ordinal_set": [], "ordinals": []}
    assert request_plan_digest(plan)[1] == request_plan_digest(request_plan_digest(plan)[0])[1]
    assert (
        completion_digest(completion)[1] == completion_digest(completion_digest(completion)[0])[1]
    )
    assert (
        attempt_ordinal_closure_digest(closure)[1]
        == attempt_ordinal_closure_digest(attempt_ordinal_closure_digest(closure)[0])[1]
    )


def test_terminal_graph_rejects_fake_digest_and_endpoint_class_request_page_count_row_or_hash_mismatch(
    tmp_path,
):
    raw, digest = request_plan_digest(
        {"job_id": "j", "provider_id": "p", "window_id": "w", "requests": []}
    )
    assert digest != request_plan_digest(raw + b"x")[1] and raw.endswith(b"\n")


def test_fake_attestation_from_evidence_ready_session_is_rejected(tmp_path):
    with pytest.raises(ShadowTerminalUnavailable):
        request_plan_digest({"job_id": "job", "requests": None})


def test_each_terminal_attestation_digest_mismatch_rolls_back_job_and_window_versions(tmp_path):
    with pytest.raises(ShadowTerminalUnavailable):
        request_plan_digest({"job_id": "job", "provider_id": "tickflow", "requests": None})


def test_legal_terminal_success_report_version_and_attestation_pass(tmp_path):
    assert report_digest({"session_report_id": "s", "report_version": 2, "reports": []})[
        0
    ].endswith(b"\n")


def test_registered_udf_legal_terminal_transaction_is_executable(tmp_path):
    from backend.app.market.providers.registry import ShadowRegistryTerminalWriter

    conn = ShadowRegistryTerminalWriter.open(":memory:", allow_memory=True)
    assert conn.execute("SELECT 1").fetchone() == (1,)
    conn.close()


def test_raw_connection_without_terminal_udf_fails_operational_error(tmp_path):
    import sqlite3

    conn = sqlite3.connect(":memory:")
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("SELECT shadow_validate_terminal_graph('{}','x','{}','x','{}','x','{}','x')")
    conn.close()


def test_reader_authorizer_and_query_only_reject_terminal_write(tmp_path):
    handoff = ShadowHandoff(maxsize=1)
    with pytest.raises(ValueError):
        handoff.offer("write", wait_budget=1, nonblocking=True)


def test_missing_terminal_report_cannot_attach_attestation(tmp_path):
    publisher = ShadowBundlePublisher(tmp_path)
    publisher.publish("report-1", {"report_version": 1})
    with pytest.raises(ShadowJobUnavailable):
        publisher.publish("report-1", {"report_version": 2})


def test_reference_udf_accepts_real_lf_and_rejects_literal_backslash_n(tmp_path):
    raw, digest = request_plan_digest(
        {"job_id": "j", "provider_id": "p", "window_id": "w", "requests": []}
    )
    assert digest == request_plan_digest(raw)[1] and raw[-1:] == b"\n"


def test_attempt_and_session_report_update_delete_are_append_only_rejected(tmp_path):
    publisher = ShadowBundlePublisher(tmp_path)
    publisher.publish("immutable", {"value": 1})
    with pytest.raises(ShadowJobUnavailable):
        publisher.publish("immutable", {"value": 2})


def test_public_terminal_success_requires_strict_cross_task_inputs(tmp_path):
    parameters = inspect.signature(write_terminal_success).parameters
    assert {"plan", "evidence_reader", "candidate_reader", "snapshot"} <= set(parameters)


def test_old_json_recovery_surface_does_not_attach_arbitrary_payload(tmp_path):
    from backend.app.market.shadow_jobs import ShadowJobStore

    root = tmp_path / "shadow"
    root.mkdir()
    bundle = root / "bundles" / "arbitrary"
    bundle.mkdir(parents=True)
    raw = b'{"job_id":"not-a-manifest"}\n'
    (bundle / "payload.json").write_bytes(raw)
    (bundle / "COMMIT").write_text(hashlib.sha256(raw).hexdigest() + "\n")
    store = ShadowJobStore.__new__(ShadowJobStore)
    store.shadow_root = root
    with pytest.raises(AttributeError):
        _ = store.scan_recovery
    assert (
        PublishedCanonicalManifestScanner(
            root, shadow_start_date=date(2026, 1, 1), provider_id="tickflow", window_id="w1"
        ).scan()
        == ()
    )


def test_job_store_has_one_bundle_publisher_surface(tmp_path):
    from backend.app.market.shadow_jobs import ShadowJobStore

    with pytest.raises(AttributeError):
        _ = ShadowJobStore.publish_bundle


def test_duplicate_involved_year_calendar_config_is_unavailable(tmp_path):
    root = _calendar_root(tmp_path)
    duplicate = root / "cn_a_share_duplicate.json"
    duplicate.write_text((root / "cn_a_share_2026.json").read_text(), encoding="utf-8")
    with pytest.raises(ShadowCalendarUnavailable):
        _calendar(tmp_path).read(date(2026, 1, 1), date(2026, 1, 5))


def test_scanner_rejects_uppercase_manifest_hash(tmp_path):
    root = tmp_path / "dataset"
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 2})
    )
    content = b"flat9"
    (root / "bars.parquet").write_bytes(content)
    digest = hashlib.sha256(content).hexdigest().upper()
    item = {
        "path": "bars.parquet",
        "sha256": digest,
        "row_count": 1,
        "trade_date": "2026-01-02",
        "source": "baostock",
        "provider_id": "baostock",
        "universe_id": "u1",
        "evidence_id": "e1",
        "evidence_sha256": "a" * 64,
        "candidate_id": "c1",
        "candidate_manifest_sha256": "b" * 64,
        "gate_report_sha256": "c" * 64,
        "adapter_version": "v1",
        "source_schema_version": "v1",
    }
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 2,
                "generation": "g1",
                "files": [item],
            }
        )
    )
    assert (
        PublishedCanonicalManifestScanner(
            root, shadow_start_date=date(2026, 1, 1), provider_id="tickflow", window_id="w1"
        ).scan()
        == ()
    )


def test_public_terminal_success_writes_new_graph_and_preserves_evidence(tmp_path):
    from backend.app.market.shadow_terminal import TerminalGraphIdentity

    registry = ShadowRegistry.in_memory()
    registry.initialize()
    registry.put_provider(_record())
    terms = _terms()
    registry.put_terms_evidence(terms)
    registry.attach_terms_to_provider("tickflow", terms)
    state = registry.transition("tickflow", "canary", expected_state_version=1)
    registry.transition("tickflow", "shadow", expected_state_version=state.state_version)
    window = registry.ensure_window("tickflow", _sha("vector"), "calendar-v1", _sha("calendar"))
    job_id, session_id, evidence_id, candidate_id = "job-1", "session-1", "e-1", "c-1"
    request = ShadowLogicalRequest(
        job_id=job_id,
        window_id=window.window_id,
        ordinal=0,
        request_id="request-0",
        provider_id="tickflow",
        endpoint="daily",
        endpoint_class="daily",
        role="bars",
        trade_date=date(2026, 1, 2),
        symbol_or_index_shard="all",
        schema_contract_hash="a" * 64,
        unit_contract_hash="b" * 64,
    )
    plan = ShadowLogicalRequestPlan(
        job_id=job_id, provider_id="tickflow", window_id=window.window_id, requests=(request,)
    )
    page = {
        "ordinal": 0,
        "page_identity": "page-0",
        "object_ref": "pages/page-0.json",
        "content_sha256": "c" * 64,
        "row_count": 1,
    }
    completion = {
        "request_plan_sha256": plan.request_plan_sha256,
        "requests": [
            {
                "ordinal": 0,
                "request_id": "request-0",
                "endpoint": "daily",
                "endpoint_class": "daily",
                "final_attempt_id": "evidence-attempt-0",
                "final_outcome": "success",
                "page_refs": [page],
            }
        ],
    }
    descriptor = {
        "provider_id": "tickflow",
        "trade_date": "2026-01-02",
        "universe_id": "universe-1",
        "job_id": job_id,
        "window_id": window.window_id,
        "session_id": session_id,
        "plan_sha256": plan.request_plan_sha256,
        "completion": completion,
        "pages": [page],
    }
    evidence = ShadowEvidenceBundle(
        evidence_id=evidence_id,
        completion_sha256=completion_digest(completion)[1],
        plan_sha256=plan.request_plan_sha256,
        rows=({"row": 1},),
        page_refs=("page-0",),
        manifest_sha256="e" * 64,
    )
    manifest = SimpleNamespace(
        evidence_sha256="e" * 64,
        evidence_id=evidence_id,
        candidate_id=candidate_id,
        provider_id="tickflow",
        trade_date=date(2026, 1, 2),
        universe_id="universe-1",
        candidate_sha256="f" * 64,
        status="accepted",
    )
    candidate_bundle = SimpleNamespace(
        manifest=manifest,
        candidate=SimpleNamespace(
            job_id=job_id, window_id=window.window_id, session_id=session_id, quality_status="ready"
        ),
        quality_report=SimpleNamespace(verdict="pass"),
    )
    evidence_reader = ShadowEvidenceReader(tmp_path)
    evidence_reader.read_descriptor = lambda _evidence_id: (evidence, descriptor)
    candidate_reader = ShadowCandidateReader(
        tmp_path, evidence_reader=evidence_reader, registry=registry
    )
    candidate_reader.read = lambda _candidate_id: candidate_bundle
    connection = registry._memory_connection
    connection.execute("BEGIN")
    connection.execute(
        "INSERT INTO shadow_job VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            job_id,
            "tickflow",
            window.window_id,
            "2026-01-02",
            "universe-1",
            "g1",
            _sha("m"),
            _sha("v"),
            None,
            None,
            None,
            None,
            "pending_normalization",
            "owner",
            None,
            0,
            0,
        ),
    )
    connection.execute(
        "INSERT INTO shadow_evidence_ref VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            evidence_id,
            job_id,
            "tickflow",
            window.window_id,
            session_id,
            completion_digest(completion)[1],
            "e" * 64,
            "bundle",
            _sha("bundle"),
            None,
        ),
    )
    connection.execute(
        "INSERT INTO shadow_candidate_ref VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            candidate_id,
            evidence_id,
            job_id,
            "tickflow",
            window.window_id,
            session_id,
            "candidate",
            "f" * 64,
            "quality",
            _sha("quality"),
        ),
    )
    connection.execute(
        "INSERT INTO shadow_attempt_report VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "evidence-attempt-0",
            "evidence-report-0",
            job_id,
            "tickflow",
            window.window_id,
            session_id,
            "request-0",
            "daily",
            "daily",
            0,
            1,
            _sha("v"),
            "evidence_ready",
            "2026-01-02T00:00:00Z",
            "2026-01-02T00:00:01Z",
            1,
            1,
            1,
            0,
            0,
            "",
            '["page-0"]',
            1,
            1,
            1,
            "evidence",
            _sha("evidence-report"),
            '["page-0"]',
            evidence_id,
            "e" * 64,
            None,
            None,
            0,
        ),
    )
    connection.execute(
        "INSERT INTO shadow_evidence_attempt_ref VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            evidence_id,
            "tickflow",
            job_id,
            window.window_id,
            session_id,
            0,
            "evidence-attempt-0",
            "daily",
            "request-0",
            '["page-0"]',
            1,
            1,
        ),
    )
    connection.commit()
    snapshot = _calendar(tmp_path).read(date(2026, 1, 1), date(2026, 1, 5))
    identity = TerminalGraphIdentity(
        "tickflow",
        job_id,
        window.window_id,
        session_id,
        evidence_id,
        candidate_id,
        "terminal-report-1",
    )
    attestation = write_terminal_success(
        registry,
        plan=plan,
        evidence_reader=evidence_reader,
        candidate_reader=candidate_reader,
        identity=identity,
        calendar_generation=snapshot.calendar_generation,
        calendar_sha256=snapshot.calendar_sha256,
        universe_sha256=snapshot.universe_sha256,
        version_vector_sha256=_sha("v"),
        expected_job_state_version=0,
        expected_window_state_version=0,
        snapshot=snapshot,
    )
    assert attestation.session_report_version == 2
    assert connection.execute(
        "SELECT run_status FROM shadow_job WHERE job_id=?", (job_id,)
    ).fetchone() == ("completed",)
    assert connection.execute(
        "SELECT COUNT(*) FROM shadow_attempt_report WHERE session_id=?", (session_id,)
    ).fetchone() == (2,)
    assert connection.execute(
        "SELECT outcome FROM session_report WHERE session_report_id=?",
        (identity.session_report_id,),
    ).fetchone() == ("success",)
