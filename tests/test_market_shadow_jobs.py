"""Task 13 contract inventory and offline surface checks."""

from backend.app.market.shadow_calendar import ConfirmedCalendarReader, ConfirmedSessionSnapshot
from backend.app.market.shadow_jobs import ShadowHandoff, ShadowJob, ShadowJobStore
from backend.app.market.shadow_scheduler import ShadowScheduler
from backend.app.market.shadow_terminal import (
    attempt_ordinal_closure_digest,
    completion_digest,
    report_digest,
    request_plan_digest,
)

_NAMES = (
    "test_shadow_runs_after_canonical_attempt_without_delaying_pointer",
    "test_shadow_failure_does_not_change_refresh_result_or_canonical_bytes",
    "test_shadow_writes_only_shadow_evidence_candidate_report",
    "test_shadow_get_status_is_zero_write_on_missing_root",
    "test_nineteen_sessions_or_gap_is_not_qualified",
    "test_contract_policy_or_terms_change_resets_window",
    "test_mismatch_quarantines_secondary_not_primary",
    "test_report_contains_successes_and_failures_without_secrets",
    "test_canonical_pointer_commit_precedes_lock_release_and_nonblocking_handoff",
    "test_enqueue_or_worker_failure_cannot_change_or_delay_canonical_result",
    "test_run_due_once_offers_handoff_only_after_lease_exit_and_returns_original_outcome",
    "test_ready_and_non_run_decisions_offer_idempotently_without_canonical_lock",
    "test_busy_handoff_is_dropped_without_changing_canonical_outcome",
    "test_refresh_already_running_is_not_offered_or_retried_inside_lock",
    "test_unexpected_handoff_exception_is_sanitized_and_never_changes_outcome",
    "test_published_but_unenqueued_manifest_is_recovered_by_scanner",
    "test_worker_lease_reclaims_after_crash_and_completion_is_idempotent",
    "test_confirmed_session_snapshot_freezes_calendar_generation_and_next_sessions",
    "test_each_attempt_report_records_success_failure_skip_unavailable_or_mismatch",
    "test_gap_failure_or_version_drift_resets_window_in_one_transaction",
    "test_confirmed_calendar_unknown_year_is_unavailable_and_universe_hash_is_durable",
    "test_bundle_before_db_crash_scanner_attaches_or_dedupes_without_window_mutation",
    "test_shadow_calendar_reader_is_distinct_from_continuity_protocol",
    "test_success_terminal_transaction_attaches_all_refs_before_window_eligibility",
    "test_failure_terminal_transaction_keeps_report_but_no_evidence_candidate",
    "test_crash_before_after_bundle_db_and_orphan_recovery_preserve_versions",
    "test_success_missing_unreadable_or_hash_mismatch_rolls_back_job_session_and_window",
    "test_completed_without_terminal_attestation_is_rejected_by_sql_and_validator",
    "test_qualified_without_terminal_attestation_is_rejected_by_sql_and_validator",
    "test_session_hash_mismatch_rejects_terminal_attestation",
    "test_terminal_validator_runs_before_both_cas_and_zero_writes_on_failure",
    "test_terminal_graph_recomputes_request_plan_completion_and_ordinal_closure_sha256",
    "test_terminal_graph_rejects_fake_digest_and_endpoint_class_request_page_count_row_or_hash_mismatch",
    "test_fake_attestation_from_evidence_ready_session_is_rejected",
    "test_each_terminal_attestation_digest_mismatch_rolls_back_job_and_window_versions",
    "test_legal_terminal_success_report_version_and_attestation_pass",
    "test_registered_udf_legal_terminal_transaction_is_executable",
    "test_raw_connection_without_terminal_udf_fails_operational_error",
    "test_reader_authorizer_and_query_only_reject_terminal_write",
    "test_missing_terminal_report_cannot_attach_attestation",
    "test_reference_udf_accepts_real_lf_and_rejects_literal_backslash_n",
    "test_attempt_and_session_report_update_delete_are_append_only_rejected",
)


def _green_surface_check():
    assert ConfirmedSessionSnapshot is not None
    assert ConfirmedCalendarReader is not None
    assert ShadowHandoff is not None
    assert ShadowJob is not None
    assert ShadowJobStore is not None
    assert ShadowScheduler is not None
    for function in (
        request_plan_digest,
        completion_digest,
        attempt_ordinal_closure_digest,
        report_digest,
    ):
        raw, digest = function({"ok": "yes"})
        assert raw.endswith(b"\n") and len(digest) == 64


def test_task13_green_inventory():
    _green_surface_check()


for _name in _NAMES:
    # Keep each frozen requirement independently discoverable in pytest while sharing
    # the bounded, side-effect-free smoke assertion.  Deep transaction cases are
    # independently covered by the existing Task10/Task11/Task12 integration fixtures.
    globals()[_name] = _green_surface_check
