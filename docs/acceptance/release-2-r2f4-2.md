# Release 2 / R2-F4.2 Exact-session Universe

## Scope and status

This document records the Step 6-7 implementation boundary for the exact-session Universe
sidecar. It is an additive control/read-only surface and is not a production enablement claim.
The canonical BaoStock refresh path, Normalize → Quality Gate → Immutable Parquet → SHA-256 →
Manifest → Atomic Publish chain is unchanged. `market_universe_mode` remains `off` by default;
secondary publication, automatic failover and provider switching remain disabled.

## Read-only API and CLI

`GET /api/v1/market/universe?trade_date=YYYY-MM-DD` and
`stock-eva market-universe --date YYYY-MM-DD` emit the same bounded status projection. They read
only a strictly validated, already-created sidecar. They never initialize/migrate the sidecar,
read credentials, construct a provider, access UserStore, or write database, Parquet, manifest,
pointer or canonical state. Every successful or safe response includes `provider_requests=0` and
`writes=false`; no payload, path, SQL, token, URL or raw exception is exposed.

The status decision is deterministic: lexical-invalid date → HTTP 422/CLI 2 without I/O;
lexically-valid date with an unprovable sidecar → HTTP 503/CLI 3; only after sidecar proof is a
future date rejected as `PIT_VISIBILITY_INVALID` (HTTP 422/CLI 2). A valid head on another date is
`stale/DATE_MISMATCH`; a persisted source digest change is stale or, when paired with a durable
terminal attempt, blocked according to the allowlisted terminal matrix. An initialized empty head
is `unavailable/CONTROL_STATE_UNAVAILABLE`, while a success without a promoted head is a control
failure rather than an inferred current state.

## No-migration rehearsal

The offline rehearsal uses a synthetic promoted classification/evidence fixture to populate an
isolated sidecar, reads the status projection, and fingerprints the pre-existing canonical files,
manifest and pointer before/after. The expected result is byte/inode identity for the canonical
chain and no new control directory from a missing-sidecar GET/CLI. No historical partition is
relabeled, no old date is presented as a new date, no provider is replaced, and no real network,
production, NAS or credential operation is part of this rehearsal. A passing rehearsal is
compatibility evidence only; it is not a production GO decision.

## Release-candidate evidence record (offline only)

This record is intentionally `In Review`; it is not a GO decision. Every anchor below is a real
test or named static check in this repository. Results are from the isolated release-candidate
focused run; no real provider, network, production, NAS, credential or LaunchAgent operation was
used.

### Functional requirements

| ID | Result | Evidence anchor |
|---|---|---|
| FR-1 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| FR-2 | PASS | `tests/test_market_universe.py::test_sidecar_initializes_without_head_and_cas_promotes_then_rejects_tamper` |
| FR-3 | PASS | `tests/test_market_automation.py::test_calendar_fails_closed_outside_confirmed_year` |
| FR-4 | PASS | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| FR-5 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| FR-6 | PASS | `tests/test_market_automation.py::test_required_symbols_include_positions_and_all_watchlists` |
| FR-7 | PASS | `tests/test_market_universe.py::test_required_index_identity_and_state_are_closed` |
| FR-8 | PASS | `tests/test_market_universe.py::test_builder_rejects_authority_bundle_from_wrong_classification_generation` |
| FR-9 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| FR-10 | PASS | `tests/test_market_universe.py::test_raw_gate_unknown_and_state_mismatch_fail_closed` |
| FR-11 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| FR-12 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| FR-13 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| FR-14 | PASS | `tests/test_market_universe.py::test_unknown_one_loaded_match_rejects_contract_publication` |
| FR-15 | PASS | `tests/test_market_universe.py::test_raw_batch_validator_rejects_partial_candidate_before_normalize` |
| FR-16 | PASS | `tests/test_market_universe.py::test_raw_batch_validator_rejects_partial_candidate_before_normalize` |
| FR-17 | PASS | `tests/test_market_universe.py::test_raw_batch_validator_rejects_partial_candidate_before_normalize` |
| FR-18 | PASS | `tests/test_market_universe.py::test_sidecar_cas_conflict_does_not_change_head` |
| FR-19 | PASS | `tests/test_market_universe_staged.py::test_attempt_plan_hash_preimage_uses_canonical_utc_z_timestamp` |
| FR-20 | PASS | `tests/test_market_universe_status.py::test_api_and_cli_project_the_same_promoted_snapshot` |
| FR-21 | PASS | `tests/test_market_universe_status.py::test_market_universe_cli_parser_and_invalid_date_are_read_only` |
| FR-22 | PASS | `tests/test_market_universe_staged.py::test_shadow_maintenance_hook_is_last_and_consumes_once` |
| FR-23 | PASS | `tests/test_market_universe_staged.py::test_maintenance_service_promotes_one_reviewed_session_and_restart_is_noop` |
| FR-24 | PASS | `tests/test_market_universe_staged.py::test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison` |
| FR-25 | PASS | `tests/test_market_universe_staged.py::test_release_candidate_removes_runner_and_raw_symbol_public_fields` |
| FR-26 | PASS | `tests/test_market_universe_status.py::test_no_migration_rehearsal_preserves_canonical_files_and_pointer` |
| FR-27 | PASS | `tests/test_market_automation.py::test_automation_outcome_is_typed_and_legacy_dump_omits_only_new_null_field` |

### Acceptance criteria

| ID | Result | Evidence anchor |
|---|---|---|
| AC-1 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| AC-2 | PASS | `tests/test_market_automation.py::test_calendar_fails_closed_outside_confirmed_year` |
| AC-3 | PASS | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| AC-4 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| AC-5 | PASS | `tests/test_market_universe.py::test_unknown_one_loaded_match_rejects_contract_publication` |
| AC-6 | PASS | `tests/test_market_universe.py::test_required_index_identity_and_state_are_closed` |
| AC-7 | PASS | `tests/test_market_automation.py::test_required_symbols_include_positions_and_all_watchlists` |
| AC-8 | PASS | `tests/test_market_universe.py::test_required_index_identity_and_state_are_closed` |
| AC-9 | PASS | `tests/test_market_universe.py::test_unknown_one_loaded_match_rejects_contract_publication` |
| AC-10 | PASS | `tests/test_market_universe.py::test_builder_rejects_authority_bundle_from_wrong_classification_generation` |
| AC-11 | PASS | `tests/test_market_universe.py::test_raw_batch_validator_rejects_partial_candidate_before_normalize` |
| AC-12 | PASS | `tests/test_market_automation.py::test_automatic_due_refresh_uses_canonical_callback_and_never_legacy_fetch` |
| AC-13 | PASS | `tests/test_market_universe_staged.py::test_durable_attempt_claim_is_restart_safe_and_allows_first_empty_sidecar` |
| AC-14 | PASS | `tests/test_market_universe_status.py::test_api_and_cli_project_the_same_promoted_snapshot` |
| AC-15 | PASS | `tests/test_market_universe_staged.py::test_release_candidate_removes_runner_and_raw_symbol_public_fields` |
| AC-16 | PASS | `tests/test_market_universe_staged.py::test_maintenance_service_promotes_one_reviewed_session_and_restart_is_noop` |
| AC-17 | PASS | `tests/test_market_universe_staged.py::test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison` |
| AC-18 | PASS | `tests/test_market_automation.py::test_automation_outcome_is_typed_and_legacy_dump_omits_only_new_null_field` |
| AC-19 | PASS | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| AC-20 | PASS | `tests/test_market_universe.py::test_raw_gate_unknown_and_state_mismatch_fail_closed` |

### Edge cases

| ID | Result | Evidence anchor |
|---|---|---|
| EC-1 | PASS | `tests/test_market_universe_status.py::test_market_universe_cli_parser_and_invalid_date_are_read_only` |
| EC-2 | PASS | `tests/test_market_universe_status.py::test_empty_or_missing_sidecar_is_control_error_without_initialization` |
| EC-3 | PASS | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| EC-4 | PASS | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| EC-5 | PASS | `tests/test_market_universe.py::test_builder_rejects_authority_bundle_from_wrong_classification_generation` |
| EC-6 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| EC-7 | PASS | `tests/test_market_universe.py::test_required_index_identity_and_state_are_closed` |
| EC-8 | PASS | `tests/test_market_universe.py::test_raw_gate_unknown_and_state_mismatch_fail_closed` |
| EC-9 | PASS | `tests/test_market_universe.py::test_member_and_count_hashes_are_deterministic_and_equation_is_fail_closed` |
| EC-10 | PASS | `tests/test_market_universe.py::test_requested_unverified_source_is_never_publishable` |
| EC-11 | PASS | `tests/test_market_automation.py::test_automation_does_not_disguise_snapshot_failure_or_touch_store_or_provider` |
| EC-12 | PASS | `tests/test_market_universe.py::test_sidecar_reader_rejects_missing_bidirectional_role_link` |
| EC-13 | PASS | `tests/test_market_universe.py::test_required_index_identity_and_state_are_closed` |
| EC-14 | PASS | `tests/test_market_universe.py::test_raw_batch_validator_rejects_partial_candidate_before_normalize` |
| EC-15 | PASS | `tests/test_market_universe.py::test_raw_gate_unknown_and_state_mismatch_fail_closed` |
| EC-16 | PASS | `tests/test_market_universe.py::test_raw_batch_validator_rejects_partial_candidate_before_normalize` |
| EC-17 | PASS | `tests/test_market_universe.py::test_raw_batch_validator_rejects_partial_candidate_before_normalize` |
| EC-18 | PASS | `tests/test_market_universe.py::test_sidecar_cas_conflict_does_not_change_head` |
| EC-19 | PASS | `tests/test_market_universe.py::test_sidecar_initializes_without_head_and_cas_promotes_then_rejects_tamper` |
| EC-20 | PASS | `tests/test_market_universe_staged.py::test_maintenance_service_promotes_one_reviewed_session_and_restart_is_noop` |
| EC-21 | PASS | `tests/test_market_universe_staged.py::test_disabled_maintenance_is_zero_hook_and_preserves_legacy_tick` |
| EC-22 | PASS | `tests/test_market_universe_staged.py::test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison` |
| EC-23 | PASS | `tests/test_market_universe.py::test_raw_gate_unknown_and_state_mismatch_fail_closed` |
| EC-24 | PASS | `tests/test_market_universe_staged.py::test_attempt_dedup_identity_is_four_fields_but_plan_hash_is_complete` |

### Gate commands and review boundary

The focused and full offline suites, formatting, compilation and diff checks are required evidence:

```text
.venv/bin/pytest -q tests/test_market_universe.py tests/test_market_universe_staged.py tests/test_market_universe_status.py tests/test_market_automation.py
.venv/bin/pytest -q --basetemp=/tmp/stock-eva-r2f4-2-full-final
.venv/bin/ruff check backend/app/market/automation.py backend/app/market/universe.py backend/app/market/universe_status.py tests/test_market_universe.py tests/test_market_universe_staged.py tests/test_market_universe_status.py
.venv/bin/ruff format --check backend/app/market/automation.py backend/app/market/universe.py backend/app/market/universe_status.py tests/test_market_universe.py tests/test_market_universe_staged.py tests/test_market_universe_status.py
.venv/bin/python -m compileall -q backend/app
git diff --check
```

The exact implementation commit is recorded by the release handoff after final independent review.
Until that review completes, this document remains `In Review`; no production enablement,
secondary qualification, automatic failover or release GO is asserted.
