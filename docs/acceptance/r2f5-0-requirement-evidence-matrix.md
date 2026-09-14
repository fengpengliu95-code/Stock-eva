# R2-F5.0 requirement-evidence matrix (SPEC CANDIDATE)

This is the normative planning crosswalk for Task 19. It is intentionally an evidence plan, not
an implementation or acceptance result. Every row is unique, every acceptance criterion names
its FR/NFR parent(s), and every anchor is `PLANNED`. No anchor below is claimed to exist, execute,
pass, or establish Task 20 production soak.

The design is the requirement text authority. The matrix is the sole ID/parent/anchor crosswalk;
the validator checks that IDs occur exactly once in this table, that design requirements exist,
that AC parents are FR/NFR only, and that no result language is smuggled into this candidate.

| ID | Unique testable requirement summary | Parent FR/NFR refs | Planned test anchors | Stage/status |
| --- | --- | --- | --- | --- |
| FR-1 | Read existing allowlisted roots/control stores without create, initialize, migrate, repair, delete, provider, or canonical writer calls | — | `PLANNED::test_r2f5_read_only_boundary` | SPEC CANDIDATE; PLANNED |
| FR-2 | Reject relative/root/home/mutable-root/symlink/overlap/unresolved paths before enumeration | — | `PLANNED::test_r2f5_path_validation_rejects_mutable_aliases` | SPEC CANDIDATE; PLANNED |
| FR-3 | Fingerprint every input before and after; any descriptor/size/time/hash change invalidates the report | — | `PLANNED::test_r2f5_success_preserves_all_input_fingerprints`, `PLANNED::test_r2f5_error_preserves_all_input_fingerprints` | SPEC CANDIDATE; PLANNED |
| FR-4 | Capture one strict immutable in-memory snapshot and never initialize a missing control DB | — | `PLANNED::test_r2f5_missing_control_db_is_not_initialized` | SPEC CANDIDATE; PLANNED |
| FR-5 | Select exactly 20 distinct confirmed consecutive sessions visible to the Shanghai trusted clock | — | `PLANNED::test_r2f5_exactly_20_confirmed_sessions_only` | SPEC CANDIDATE; PLANNED |
| FR-6 | Report missing-middle, unknown, duplicate, non-advancing and later-repaired availability explicitly | — | `PLANNED::test_r2f5_rejects_19_21_duplicate_future_and_missing_middle` | SPEC CANDIDATE; PLANNED |
| FR-7 | Freeze exact provider/adapter/endpoint/policy/schema/calendar/universe/replication/restore versions | — | `PLANNED::test_r2f5_version_vector_is_frozen_across_window`, `PLANNED::test_r2f5_version_drift_is_not_coerced_to_ready` | SPEC CANDIDATE; PLANNED |
| FR-8 | Apply inclusive same-evening 21:15 and next-morning 08:00 Asia/Shanghai cutoffs | — | `PLANNED::test_r2f5_cutoffs_are_inclusive_in_shanghai`, `PLANNED::test_r2f5_late_evening_and_next_morning_fail` | SPEC CANDIDATE; PLANNED |
| FR-9 | Compute continuity, availability, coverage, source purity and integrity against every mandatory target | — | `PLANNED::test_r2f5_ready_requires_every_mandatory_metric` | SPEC CANDIDATE; PLANNED |
| FR-10 | Require final-success raw evidence, candidate/gate, manifest/object and SessionSelection lineage | — | `PLANNED::test_r2f5_lineage_requires_evidence_candidate_gate_selection` | SPEC CANDIDATE; PLANNED |
| FR-11 | Replay at most three existing raw objects offline without provider/network/candidate mutation | — | `PLANNED::test_r2f5_replay_is_bounded_and_offline` | SPEC CANDIDATE; PLANNED |
| FR-12 | Require strict calendar/universe states; unknown/conflict/count mismatch never guesses or publishes | — | `PLANNED::test_r2f5_calendar_and_universe_gates_fail_closed` | SPEC CANDIDATE; PLANNED |
| FR-13 | Consume replication/restore drill evidence without draining, copying, mounting or restoring | — | `PLANNED::test_r2f5_acceptance_never_drains_or_restores` | SPEC CANDIDATE; PLANNED |
| FR-14 | Use only ready/not_ready/unavailable with ready only when all mandatory rows pass | — | `PLANNED::test_r2f5_not_ready_and_unavailable_are_distinct` | SPEC CANDIDATE; PLANNED |
| FR-15 | CLI emits one sanitized JSON report with exits 0 valid, 1 unavailable, 2 invalid | — | `PLANNED::test_r2f5_cli_contract_and_exit_mapping` | SPEC CANDIDATE; PLANNED |
| FR-16 | Add one read-only API with configured roots and unchanged existing response models | — | `PLANNED::test_r2f5_cli_and_api_have_identical_report_contract` | SPEC CANDIDATE; PLANNED |
| FR-17 | Every result includes provider_requests=0, writes=false, restore_started=false and production_window_started=false | — | `PLANNED::test_r2f5_zero_provider_and_mutation_markers` | SPEC CANDIDATE; PLANNED |
| FR-18 | Never claim Task 20 elapsed production soak or R2-F5/Release 2 GO from an offline report | — | `PLANNED::test_r2f5_offline_ready_never_claims_task20_or_release2` | SPEC CANDIDATE; PLANNED |
| NFR-1 | Preserve predecessor readers, public models, tables, manifests, pointers, partitions and schemas | — | `PLANNED::test_r2f5_existing_readers_remain_compatible` | SPEC CANDIDATE; PLANNED |
| NFR-2 | Use read-only SQLite and descriptor-bound no-follow reads and close resources on all paths | — | `PLANNED::test_r2f5_read_connections_are_read_only_and_closed` | SPEC CANDIDATE; PLANNED |
| NFR-3 | Keep failures deterministic, bounded, sanitized and zero-write | — | `PLANNED::test_r2f5_missing_corrupt_locked_states_fail_closed` | SPEC CANDIDATE; PLANNED |
| NFR-4 | Complete reference 20-session/100000-row evaluation within 10000 ms or bounded-fail | — | `PLANNED::test_r2f5_reference_fixture_reports_elapsed_within_bound` | SPEC CANDIDATE; PLANNED |
| NFR-5 | Give every FR/NFR/AC/EC a unique planned test anchor without claiming it passed | — | `PLANNED::test_r2f5_crosswalk_is_exact` | SPEC CANDIDATE; PLANNED |
| NFR-6 | Keep public errors to allowlisted reason/count/hash/time fields with no path/token/SQL/raw text | — | `PLANNED::test_r2f5_path_and_exception_redaction_is_stable` | SPEC CANDIDATE; PLANNED |
| NFR-7 | Enforce bounded object bytes, row counts, 20 sessions and at most three replay samples | — | `PLANNED::test_r2f5_bounded_replay_and_row_limits` | SPEC CANDIDATE; PLANNED |
| NFR-8 | Bind snapshot identity to range, Shanghai clock, fingerprints and frozen versions | — | `PLANNED::test_r2f5_snapshot_identity_rejects_changed_bound_fields` | SPEC CANDIDATE; PLANNED |
| NFR-9 | Run RED, GREEN, focused/full/static and protected compatibility checks before implementation GO | — | `PLANNED::test_r2f5_verification_commands_are_documented` | SPEC CANDIDATE; PLANNED |
| NFR-10 | Require separate human/installed-release/terms/Task20 authority for production mutation and soak | — | `PLANNED::test_r2f5_production_gates_are_false` | SPEC CANDIDATE; PLANNED |
| AC-1 | Exactly 20 confirmed consecutive sessions can be ready; 19/21/duplicate/future/middle-gap cannot | FR-4, FR-5, FR-6, FR-14 | `PLANNED::test_r2f5_exactly_20_confirmed_sessions_only`, `PLANNED::test_r2f5_rejects_19_21_duplicate_future_and_missing_middle` | SPEC CANDIDATE; PLANNED |
| AC-2 | Inclusive Shanghai cutoff boundaries pass while 21:16 and 08:01 fail their metric | FR-8, FR-9 | `PLANNED::test_r2f5_cutoffs_are_inclusive_in_shanghai`, `PLANNED::test_r2f5_late_evening_and_next_morning_fail` | SPEC CANDIDATE; PLANNED |
| AC-3 | Any material frozen-version drift produces not_ready/VERSION_DRIFT | FR-7, NFR-8 | `PLANNED::test_r2f5_version_drift_is_not_coerced_to_ready` | SPEC CANDIDATE; PLANNED |
| AC-4 | 100% legal coverage and zero mixed-source rows pass; unknown/count mismatch or mixed source fails | FR-9, FR-12 | `PLANNED::test_r2f5_coverage_requires_exact_legal_universe`, `PLANNED::test_r2f5_mixed_source_partition_fails_closed` | SPEC CANDIDATE; PLANNED |
| AC-5 | Complete lineage plus bounded offline semantic replay passes; missing/corrupt binding remains unavailable | FR-10, FR-11 | `PLANNED::test_r2f5_lineage_requires_evidence_candidate_gate_selection`, `PLANNED::test_r2f5_corrupt_lineage_fails_closed` | SPEC CANDIDATE; PLANNED |
| AC-6 | Replication lag/missing restore drill is visible and cannot initiate drain or restore | FR-13, FR-17 | `PLANNED::test_r2f5_replication_lag_is_visible`, `PLANNED::test_r2f5_restore_requires_completed_immutable_drill`, `PLANNED::test_r2f5_acceptance_never_drains_or_restores` | SPEC CANDIDATE; PLANNED |
| AC-7 | Success/error evaluation preserves every input fingerprint and missing DB remains absent | FR-1, FR-3, FR-4, NFR-2 | `PLANNED::test_r2f5_success_preserves_all_input_fingerprints`, `PLANNED::test_r2f5_error_preserves_all_input_fingerprints`, `PLANNED::test_r2f5_missing_control_db_is_not_initialized` | SPEC CANDIDATE; PLANNED |
| AC-8 | Invalid paths/config and raw diagnostic inputs fail before enumeration with redacted 2/422 output | FR-2, NFR-6 | `PLANNED::test_r2f5_path_validation_rejects_mutable_aliases`, `PLANNED::test_r2f5_path_and_exception_redaction_is_stable` | SPEC CANDIDATE; PLANNED |
| AC-9 | API and CLI agree on report/markers/reason order and both perform zero initialization/provider calls | FR-15, FR-16, FR-17 | `PLANNED::test_r2f5_cli_and_api_have_identical_report_contract`, `PLANNED::test_r2f5_api_is_read_only_and_sanitized` | SPEC CANDIDATE; PLANNED |
| AC-10 | All mandatory pass gives ready; any provable failure gives not_ready; unprovable input gives unavailable | FR-9, FR-14, NFR-3 | `PLANNED::test_r2f5_ready_requires_every_mandatory_metric`, `PLANNED::test_r2f5_not_ready_and_unavailable_are_distinct` | SPEC CANDIDATE; PLANNED |
| AC-11 | In-bound reference fixture records counters and <=10000 ms; over-bound input bounded-fails | NFR-4, NFR-7 | `PLANNED::test_r2f5_reference_fixture_reports_elapsed_within_bound`, `PLANNED::test_r2f5_bounded_replay_and_row_limits` | SPEC CANDIDATE; PLANNED |
| AC-12 | Crosswalk, focused/full/static and protected golden checks preserve predecessor compatibility | NFR-1, NFR-9 | `PLANNED::test_r2f5_protected_golden_objects_are_unchanged`, `PLANNED::test_r2f5_existing_readers_remain_compatible` | SPEC CANDIDATE; PLANNED |
| AC-13 | Offline ready metadata still states production_window_started=false, Task20 pending and R2-F5.0 NO-GO | FR-18, NFR-10 | `PLANNED::test_r2f5_offline_ready_never_claims_task20_or_release2`, `PLANNED::test_r2f5_production_gates_are_false` | SPEC CANDIDATE; PLANNED |
| AC-14 | Repeated identical captured bytes/clock give deterministic JSON, reason order and zero-write markers | FR-14, FR-17, NFR-3 | `PLANNED::test_r2f5_report_digest_and_reason_order_are_deterministic`, `PLANNED::test_r2f5_zero_provider_and_mutation_markers` | SPEC CANDIDATE; PLANNED |
| EC-1 | 19 confirmed sessions returns SESSION_COUNT_NOT_20 without inference or padding | FR-5, FR-14 | `PLANNED::test_r2f5_rejects_19_21_duplicate_future_and_missing_middle` | SPEC CANDIDATE; PLANNED |
| EC-2 | Duplicate/out-of-order raw dates return unavailable/SESSION_SEQUENCE_INVALID before sorted derivation | FR-5, FR-6 | `PLANNED::test_r2f5_rejects_19_21_duplicate_future_and_missing_middle` | SPEC CANDIDATE; PLANNED |
| EC-3 | Future date rejects with PIT_VISIBILITY_INVALID after strict source proof | FR-5, FR-6 | `PLANNED::test_r2f5_future_date_is_not_visible` | SPEC CANDIDATE; PLANNED |
| EC-4 | Unknown/conflicting calendar returns CALENDAR_UNAVAILABLE/CALENDAR_CONFLICT without guesses | FR-5, FR-12 | `PLANNED::test_r2f5_calendar_unknown_and_conflict_fail_closed` | SPEC CANDIDATE; PLANNED |
| EC-5 | Universe unknown/count/index/generation mismatch prevents ready | FR-9, FR-12 | `PLANNED::test_r2f5_calendar_and_universe_gates_fail_closed` | SPEC CANDIDATE; PLANNED |
| EC-6 | Non-Shanghai or naive timestamp cannot bypass timezone-aware cutoff | FR-8, NFR-2 | `PLANNED::test_r2f5_non_shanghai_timestamp_is_normalized_or_unavailable` | SPEC CANDIDATE; PLANNED |
| EC-7 | Later repair does not rewrite an original late/missing availability timestamp | FR-6, FR-9 | `PLANNED::test_r2f5_late_publication_is_not_rewritten_by_repair` | SPEC CANDIDATE; PLANNED |
| EC-8 | Any provider/adapter/endpoint/policy/calendar/universe/replication/restore drift is not_ready | FR-7, NFR-8 | `PLANNED::test_r2f5_version_drift_is_not_coerced_to_ready` | SPEC CANDIDATE; PLANNED |
| EC-9 | Missing/corrupt/partial/wrong-date/provider/mixed lineage is unavailable | FR-10, NFR-3 | `PLANNED::test_r2f5_corrupt_lineage_fails_closed` | SPEC CANDIDATE; PLANNED |
| EC-10 | Missing/oversized/malformed/semantic-mismatch replay is bounded REPLAY_UNAVAILABLE | FR-11, NFR-7 | `PLANNED::test_r2f5_bounded_replay_and_row_limits` | SPEC CANDIDATE; PLANNED |
| EC-11 | Missing/corrupt/locked/ahead/lagged/orphan replication evidence is read-only unavailable/degraded | FR-13, NFR-3 | `PLANNED::test_r2f5_replication_lag_is_visible`, `PLANNED::test_r2f5_missing_corrupt_locked_states_fail_closed` | SPEC CANDIDATE; PLANNED |
| EC-12 | Missing/nonterminal/hash-invalid/changing/unsafe restore drill is unavailable without destination creation | FR-13, NFR-3 | `PLANNED::test_r2f5_restore_requires_completed_immutable_drill` | SPEC CANDIDATE; PLANNED |
| EC-13 | Replaced/symlinked/permission-denied/changed input returns SNAPSHOT_CHANGED or unavailable | FR-2, FR-3, NFR-2 | `PLANNED::test_r2f5_snapshot_change_fails_closed` | SPEC CANDIDATE; PLANNED |
| EC-14 | Relative/root/home/mutable/variable/overlap path rejects before stat/enumeration | FR-2, NFR-5 | `PLANNED::test_r2f5_path_validation_rejects_mutable_aliases` | SPEC CANDIDATE; PLANNED |
| EC-15 | Absent/locked SQLite returns unavailable without initialize/migrate/retry takeover | FR-1, FR-4, NFR-2 | `PLANNED::test_r2f5_missing_corrupt_locked_states_fail_closed` | SPEC CANDIDATE; PLANNED |
| EC-16 | Exception path/token/SQL/URL/provider text is reduced to allowlisted reason/detail | FR-14, NFR-6 | `PLANNED::test_r2f5_path_and_exception_redaction_is_stable` | SPEC CANDIDATE; PLANNED |
| EC-17 | Object/row/sample/time bound stops safely and reports counters | FR-11, NFR-4, NFR-7 | `PLANNED::test_r2f5_bounded_replay_and_row_limits`, `PLANNED::test_r2f5_reference_fixture_reports_elapsed_within_bound` | SPEC CANDIDATE; PLANNED |
| EC-18 | Malformed dates/missing args/disallowed overrides return 422/2 without filesystem/provider I/O | FR-15, FR-16, NFR-5 | `PLANNED::test_r2f5_cli_contract_and_exit_mapping`, `PLANNED::test_r2f5_api_invalid_arguments_are_zero_io` | SPEC CANDIDATE; PLANNED |
| FR-19 | Expose separate MetricResult fields for all 17 roadmap SLO dimensions with observed value, target, status and reason | FR-9 | `PLANNED::test_r2f5_report_has_every_roadmap_slo_metric`, `PLANNED::test_r2f5_each_slo_threshold_is_explicit` | SPEC CANDIDATE; PLANNED |
| FR-20 | Require immutable secondary qualification/admission hashes and failover proof; purity alone cannot pass | FR-10 | `PLANNED::test_r2f5_baostock_only_cannot_pass_failover`, `PLANNED::test_r2f5_failover_requires_qualified_whole_session_drill` | SPEC CANDIDATE; PLANNED |
| FR-21 | Read replication/restore snapshots with trust scope, destination generation/head proof, remote verification and thresholds | FR-13 | `PLANNED::test_r2f5_replication_restore_reader_is_strictly_read_only` | SPEC CANDIDATE; PLANNED |
| FR-22 | Replay only with injected frozen offline adapter/normalizer identity and zero provider/network construction | FR-11 | `PLANNED::test_r2f5_replay_uses_injected_offline_identity`, `PLANNED::test_r2f5_replay_rejects_unknown_identity_without_provider` | SPEC CANDIDATE; PLANNED |
| FR-23 | Require complete non-null ready-time version vector and equality across all 20 observations | FR-7 | `PLANNED::test_r2f5_frozen_versions_are_complete_and_nonnull_when_ready` | SPEC CANDIDATE; PLANNED |
| FR-24 | Preserve raw calendar order/duplicates and bind SnapshotIdentity to range, Shanghai clock, fingerprints and versions | FR-3, FR-5 | `PLANNED::test_r2f5_raw_calendar_order_and_duplicates_fail_closed`, `PLANNED::test_r2f5_snapshot_identity_binds_all_inputs_and_clock` | SPEC CANDIDATE; PLANNED |
| FR-25 | Return exactly 20 ordered per-session observations or digest-bound references with independent evidence fields | FR-9 | `PLANNED::test_r2f5_per_session_observation_cardinality_and_order` | SPEC CANDIDATE; PLANNED |
| FR-26 | Exclude elapsed/read/replay counters from semantic canonical JSON and digest | FR-16 | `PLANNED::test_r2f5_semantic_digest_excludes_volatile_envelope` | SPEC CANDIDATE; PLANNED |
| FR-27 | Enforce closed enums/reasons, safe IDs/hashes, nonnegative bounds and bounded quality issues | FR-14 | `PLANNED::test_r2f5_report_types_are_closed_and_bounded` | SPEC CANDIDATE; PLANNED |
| FR-28 | Enforce exact reason precedence and invalidate the whole report on concurrent snapshot change | FR-3, FR-6 | `PLANNED::test_r2f5_reason_precedence_is_exact`, `PLANNED::test_r2f5_snapshot_change_fails_closed` | SPEC CANDIDATE; PLANNED |
| NFR-11 | Preserve roadmap continuity, availability, coverage and purity thresholds without invention or relaxation | FR-9 | `PLANNED::test_r2f5_each_slo_threshold_is_explicit` | SPEC CANDIDATE; PLANNED |
| NFR-12 | Require reviewed R2-F4 numeric lag/duration thresholds; missing values or LOCAL_CHAIN_ONLY cannot pass remote | FR-13 | `PLANNED::test_r2f5_local_chain_only_cannot_satisfy_remote_acceptance` | SPEC CANDIDATE; PLANNED |
| NFR-13 | Fail closed on any snapshot descriptor/content/version change and never mix retries | FR-3, FR-4 | `PLANNED::test_r2f5_snapshot_identity_rejects_changed_bound_fields` | SPEC CANDIDATE; PLANNED |
| NFR-14 | Encode semantic report as canonical UTF-8 JSON with domain-separated SHA-256 and volatile envelope excluded | FR-16 | `PLANNED::test_r2f5_semantic_digest_excludes_volatile_envelope` | SPEC CANDIDATE; PLANNED |
| NFR-15 | Keep API/CLI additive and predecessor response/schema contracts unchanged | FR-15, FR-16 | `PLANNED::test_r2f5_acceptance_surface_is_additive`, `PLANNED::test_r2f5_predecessor_payloads_are_byte_compatible` | SPEC CANDIDATE; PLANNED |
| AC-15 | All 17 roadmap dimensions appear as independent MetricResult fields with explicit thresholds | FR-9, FR-19, NFR-11 | `PLANNED::test_r2f5_report_has_every_roadmap_slo_metric`, `PLANNED::test_r2f5_each_slo_threshold_is_explicit` | SPEC CANDIDATE; PLANNED |
| AC-16 | BaoStock-only/no secondary/no whole-session drill cannot pass failover; qualified proof requires whole-session purity | FR-20, FR-21 | `PLANNED::test_r2f5_baostock_only_cannot_pass_failover`, `PLANNED::test_r2f5_failover_requires_qualified_whole_session_drill` | SPEC CANDIDATE; PLANNED |
| AC-17 | LOCAL_CHAIN_ONLY/missing threshold or remote proof is unavailable/not_ready and starts no writer operation | FR-21, NFR-12, NFR-13 | `PLANNED::test_r2f5_local_chain_only_cannot_satisfy_remote_acceptance`, `PLANNED::test_r2f5_replication_restore_reader_is_strictly_read_only` | SPEC CANDIDATE; PLANNED |
| AC-18 | Frozen offline replay identity permits deterministic normalization with zero provider/network; unknown identity rejects first | FR-22, NFR-13 | `PLANNED::test_r2f5_replay_uses_injected_offline_identity`, `PLANNED::test_r2f5_replay_rejects_unknown_identity_without_provider` | SPEC CANDIDATE; PLANNED |
| AC-19 | Complete required frozen vector is non-null and equal across 20 observations | FR-23, NFR-14 | `PLANNED::test_r2f5_frozen_versions_are_complete_and_nonnull_when_ready`, `PLANNED::test_r2f5_version_vector_drift_is_unavailable` | SPEC CANDIDATE; PLANNED |
| AC-20 | Raw duplicate/out-of-order calendar fails before sorting and SnapshotIdentity binds every input/clock/version | FR-24, FR-28, NFR-13 | `PLANNED::test_r2f5_raw_calendar_order_and_duplicates_fail_closed`, `PLANNED::test_r2f5_snapshot_identity_binds_all_inputs_and_clock` | SPEC CANDIDATE; PLANNED |
| AC-21 | Candidate report contains exactly 20 ordered per-session observations/refs with independent metric evidence | FR-25, NFR-14 | `PLANNED::test_r2f5_per_session_observation_cardinality_and_order` | SPEC CANDIDATE; PLANNED |
| AC-22 | Changing only elapsed/read/replay diagnostics leaves semantic JSON bytes and digest unchanged | FR-26, NFR-14 | `PLANNED::test_r2f5_semantic_digest_excludes_volatile_envelope` | SPEC CANDIDATE; PLANNED |
| AC-23 | Invalid enum/reason/hash/ID/negative/oversized fields reject safely; additive predecessor contracts remain unchanged | FR-27, NFR-15 | `PLANNED::test_r2f5_report_types_are_closed_and_bounded`, `PLANNED::test_r2f5_acceptance_surface_is_additive` | SPEC CANDIDATE; PLANNED |
| EC-19 | Missing SLO field or threshold returns unavailable and cannot infer pass from another dimension | FR-19, NFR-11 | `PLANNED::test_r2f5_missing_slo_metric_fails_closed` | SPEC CANDIDATE; PLANNED |
| EC-20 | BaoStock-only/unqualified secondary returns failover not_ready/unavailable regardless of purity | FR-20 | `PLANNED::test_r2f5_baostock_only_cannot_pass_failover` | SPEC CANDIDATE; PLANNED |
| EC-21 | Incomplete forced-failover proof cannot pass or mutate a pointer | FR-21 | `PLANNED::test_r2f5_failover_requires_qualified_whole_session_drill` | SPEC CANDIDATE; PLANNED |
| EC-22 | LOCAL_CHAIN_ONLY or missing reviewed lag/duration policy cannot pass remote acceptance | FR-21, NFR-12 | `PLANNED::test_r2f5_local_chain_only_cannot_satisfy_remote_acceptance` | SPEC CANDIDATE; PLANNED |
| EC-23 | Unknown/default network-capable replay identity is rejected before construction | FR-22 | `PLANNED::test_r2f5_replay_rejects_unknown_identity_without_provider` | SPEC CANDIDATE; PLANNED |
| EC-24 | Missing RELEASE/dataset/admission/config/policy/calendar/universe/replication/restore identity rejects ready | FR-23, NFR-14 | `PLANNED::test_r2f5_frozen_versions_are_complete_and_nonnull_when_ready` | SPEC CANDIDATE; PLANNED |
| EC-25 | Duplicate/out-of-order raw calendar remains unmodified and unavailable before sorted derivation | FR-24, FR-28 | `PLANNED::test_r2f5_raw_calendar_order_and_duplicates_fail_closed` | SPEC CANDIDATE; PLANNED |
| EC-26 | Volatile diagnostics or absent identity/observation makes semantic report incomplete/non-deterministic | FR-25, FR-26, NFR-14 | `PLANNED::test_r2f5_semantic_digest_excludes_volatile_envelope`, `PLANNED::test_r2f5_per_session_observation_cardinality_and_order` | SPEC CANDIDATE; PLANNED |

## Mandatory SLO crosswalk

Each roadmap SLO has an independent report field, threshold source and planned anchor. These are
not additional requirement IDs; FR-19/NFR-11 own the inventory contract.

| Metric field | Exact target | Threshold source | Planned anchor |
| --- | --- | --- | --- |
| `continuity` | zero missing canonical dates in 20 sessions | roadmap Section 10 | `PLANNED::test_r2f5_continuity_metric` |
| `next_morning_availability` | 20/20 by 08:00 Shanghai next day | roadmap Section 10 | `PLANNED::test_r2f5_next_morning_metric` |
| `same_evening_availability` | at least 18/20 by 21:15 Shanghai | roadmap Section 10 | `PLANNED::test_r2f5_same_evening_metric` |
| `coverage` | 100% legal universe each session | roadmap Section 10 | `PLANNED::test_r2f5_coverage_metric` |
| `canonical_integrity` | pointer/manifest/object hashes reconcile every session | roadmap Section 10 | `PLANNED::test_r2f5_canonical_pointer_manifest_hash_metric` |
| `source_purity` | zero mixed-provider canonical partitions | roadmap Section 10 | `PLANNED::test_r2f5_source_purity_metric` |
| `recovery` | restart queue and exactly-once later publication | roadmap Section 10 | `PLANNED::test_r2f5_recovery_metric` |
| `failover` | qualified secondary whole-session drill, zero mixed rows | roadmap Section 10 + R2-F4 evidence | `PLANNED::test_r2f5_failover_metric` |
| `provenance` | complete raw/provider/version/hash/gate/selection lineage | roadmap Section 10 | `PLANNED::test_r2f5_provenance_metric` |
| `replay` | bounded sample semantically identical offline | roadmap Section 10 | `PLANNED::test_r2f5_replay_metric` |
| `adjustment` | declared adjusted-return tolerance | reviewed R2-F4 policy; missing is unavailable | `PLANNED::test_r2f5_adjustment_metric` |
| `calendar` | warning/acquisition observable; unknown/conflict fail closed | roadmap Section 10 | `PLANNED::test_r2f5_calendar_metric` |
| `universe` | required/loaded/suspension/listing counts reconcile; unknown zero | roadmap Section 10 | `PLANNED::test_r2f5_universe_metric` |
| `error_handling` | timeout/auth/rate/schema/coverage/storage sanitized categories | roadmap Section 10 | `PLANNED::test_r2f5_error_handling_metric` |
| `local_nas_isolation` | NAS outage leaves local ready and retryable backlog | roadmap Section 10 + R2-F4.3 | `PLANNED::test_r2f5_local_nas_isolation_metric` |
| `replication` | lag/remote verification satisfy frozen R2-F4 numeric policy | reviewed R2-F4 policy; missing is unavailable | `PLANNED::test_r2f5_replication_metric` |
| `restore` | restore/readback satisfies frozen R2-F4 duration policy | reviewed R2-F4 policy; missing is unavailable | `PLANNED::test_r2f5_restore_metric` |

## Crosswalk interpretation

`PLANNED` means a future RED/GREEN or static test anchor is named for implementation planning.
It is not a test result. The only current verification claims are that this table and its design
are syntactically/crosswalk valid at base commit; Task 20 and production soak remain unstarted.
