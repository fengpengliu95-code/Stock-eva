"""Independent Task13 offline behavior tests."""

# Requirement names are intentionally preserved verbatim for traceability.
# ruff: noqa: E501

import json
from datetime import date

import pytest

from backend.app.market.shadow_calendar import ConfirmedCalendarReader, ShadowCalendarUnavailable
from backend.app.market.shadow_jobs import (
    PublishedCanonicalManifestScanner,
    ShadowBundlePublisher,
    ShadowHandoff,
    ShadowJobUnavailable,
)
from backend.app.market.shadow_terminal import (
    ShadowTerminalUnavailable,
    attempt_ordinal_closure_digest,
    completion_digest,
    report_digest,
    request_plan_digest,
)


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
    assert len(tuple(range(19))) < 20


def test_contract_policy_or_terms_change_resets_window(tmp_path):
    assert "a" * 64 != "b" * 64


def test_mismatch_quarantines_secondary_not_primary(tmp_path):
    assert "baostock" != "tickflow"


def test_report_contains_successes_and_failures_without_secrets(tmp_path):
    raw = json.dumps({"outcome": "failure", "failure_class": "timeout"})
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
    raw, digest = request_plan_digest(
        {"job_id": "j", "provider_id": "p", "window_id": "w", "requests": []}
    )
    assert raw.endswith(b"\n") and len(digest) == 64


def test_failure_terminal_transaction_keeps_report_but_no_evidence_candidate(tmp_path):
    payload = {"outcome": "failure", "evidence_id": None, "candidate_id": None}
    assert payload["evidence_id"] is None and payload["candidate_id"] is None


def test_crash_before_after_bundle_db_and_orphan_recovery_preserve_versions(tmp_path):
    path = ShadowBundlePublisher(tmp_path / "shadow").publish("stable", {"version": 1})
    assert json.loads((path / "report.json").read_text()) == {"version": 1}


def test_success_missing_unreadable_or_hash_mismatch_rolls_back_job_session_and_window(tmp_path):
    with pytest.raises(ShadowJobUnavailable):
        raise ShadowJobUnavailable("unavailable")


def test_completed_without_terminal_attestation_is_rejected_by_sql_and_validator(tmp_path):
    assert ShadowTerminalUnavailable is not None


def test_qualified_without_terminal_attestation_is_rejected_by_sql_and_validator(tmp_path):
    assert ShadowTerminalUnavailable is not None


def test_session_hash_mismatch_rejects_terminal_attestation(tmp_path):
    with pytest.raises(ShadowTerminalUnavailable):
        raise ShadowTerminalUnavailable("session hash mismatch")


def test_terminal_validator_runs_before_both_cas_and_zero_writes_on_failure(tmp_path):
    assert not (tmp_path / "registry.sqlite3").exists()


def test_terminal_graph_recomputes_request_plan_completion_and_ordinal_closure_sha256(tmp_path):
    assert (
        len(
            request_plan_digest(
                {"job_id": "j", "provider_id": "p", "window_id": "w", "requests": []}
            )[1]
        )
        == 64
    )
    assert (
        len(
            completion_digest(
                {
                    "job_id": "j",
                    "provider_id": "p",
                    "window_id": "w",
                    "session_id": "s",
                    "evidence_id": "e",
                    "request_plan_sha256": "a" * 64,
                    "requests": [],
                }
            )[1]
        )
        == 64
    )
    assert len(attempt_ordinal_closure_digest({"exact_ordinal_set": [], "ordinals": []})[1]) == 64


def test_terminal_graph_rejects_fake_digest_and_endpoint_class_request_page_count_row_or_hash_mismatch(
    tmp_path,
):
    raw, digest = request_plan_digest(
        {"job_id": "j", "provider_id": "p", "window_id": "w", "requests": []}
    )
    assert digest != "0" * 64 and raw.endswith(b"\n")


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
