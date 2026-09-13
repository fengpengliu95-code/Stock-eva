# R2-F4.3 replication and restore — authoritative normative specification

Status: RC / Candidate/NO-GO pending next independent audit. This document is the
single current normative truth for the release candidate. It contains no historical
amendment corpus. Scope is LOCAL_CHAIN_ONLY: no real NAS/SMB mount, provider,
LaunchAgent, credential, or production operation may run from this worktree.

## Safety contract

Canonical local publication is the source of truth. Manifest-only publication mutates,
fsyncs, and reads back only the manifest/object projection; it never creates a binding,
pointer, source commit, guard, sidecar, or outbox. Dataset-plus-pointer publication
creates or byte-identically reuses one immutable binding, verifies its hash before
reading fields, then commits the pointer. A binding conflict, duplicate generation,
manifest drift, pointer proof failure, lock close failure, or journal failure cannot
roll back a durable ready pointer; reconciliation evidence is journaled when possible.

When configured, the coordinator reads one immutable SourceInstanceRecord bound to the
explicit dataset root and descriptor-stable canonical schema digest. SourceCommit carries
the exact source-instance ID and digest and never invents an all-zero identity. When
replication is not configured, SourceCommit is optional and the observation is the
typed SOURCE_NOT_CONFIGURED projection.

PointerIdentity is an exact tagged union: ABSENT is only a missing database or a full
canonical schema with no singleton and no extra payload; PRESENT includes the exact row,
device, inode, and schema digest; every unknown table/column, missing column, type,
constraint/index/schema drift, duplicate, unreadable, or malformed state is INVALID.
Factory and reader share one descriptor-stable schema contract.

Modern lineage requires retained successful terminal evidence, candidate/gate records,
and an exact SessionSelection relation (trade date, universe, provider, IDs, and all
hashes). Missing, forged, partial, mixed, unknown, or corrupt lineage is unavailable.
Legacy is explicit and only follows a read-only proof of an existing legacy manifest.
Ordered dates are not deduplicated; effective-day planning uses a proven local calendar
before any provider request.

Automation calls at most one bounded drain worker only when both replication gates are
true and an approved local destination descriptor/lock is available. CLI commands are
explicit and default to typed dry_run; lexical and descriptor-native checks reject
relative, root, home, unresolved, symlink-alias, mutable-root, and overlap paths.
Init requires CREATE_EMPTY_NAS_ARCHIVE_R2F4_3. Operation-day visibility uses the
Asia/Shanghai end-of-day cutoff for source publication and selected restore heads.
Status is read-only, sanitized, mode=status, and returns no paths/secrets.

## Integrity and threat limitations

Wire records use strict extra-forbidden schemas, canonical UTF-8 JSON, domain-separated
SHA-256 preimages, no-follow descriptor traversal, exclusive no-replace installation,
file/parent fsync, immutable event history, state-version CAS, bounded retries, and
exact byte/inode/fingerprint readback. Destination staging is never reader-visible;
unknown/orphan state is preserved for manual review/quarantine. Rollback is manual and
cannot replace a newer valid canonical generation. Canonical manifest/pointer schemas,
plain MarketStore behavior, and prior mirror compatibility remain unchanged. Public
responses expose only allowlisted state, reason codes, counts, hashes, bounded timestamps,
effects, and LOCAL_CHAIN_ONLY.

## Requirements and exact evidence

Exactly 42 FR, 16 NFR, 31 AC, and 40 EC IDs are authoritative below. Each ID occurs
once and maps to one real AST-discoverable test function; evidence anchors may be reused.

| ID | Effective latest semantics | Exact evidence anchor |
|---|---|---|
| FR-1 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| FR-1a | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| FR-2 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| FR-2a | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| FR-3 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| FR-3a | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| FR-3b | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| FR-3c | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| FR-3d | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| FR-3e | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| FR-3f | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| FR-3g | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| FR-3h | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| FR-4 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| FR-5 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| FR-5a | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| FR-6 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| FR-7 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| FR-8 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| FR-9 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| FR-10 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| FR-11 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| FR-12 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| FR-13 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| FR-14 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| FR-15 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| FR-16 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| FR-17 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| FR-18 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| FR-19 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| FR-20 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| FR-21 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| FR-22 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| FR-23 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| FR-24 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| FR-25 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| FR-26 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| FR-27 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| FR-3i | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| FR-3j | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| FR-28 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| FR-29 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| NFR-1 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| NFR-2 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| NFR-3 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| NFR-4 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| NFR-5 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| NFR-6 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| NFR-7 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| NFR-8 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| NFR-9 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| NFR-10 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| NFR-11 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| NFR-12 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| NFR-13 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| NFR-14 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| NFR-15 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| NFR-16 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| AC-1 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| AC-2 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| AC-3 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| AC-4 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| AC-5 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| AC-6 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| AC-7 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| AC-8 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| AC-9 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| AC-10 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| AC-11 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| AC-12 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| AC-13 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| AC-14 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| AC-15 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| AC-16 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| AC-17 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| AC-18 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| AC-19 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| AC-20 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| AC-21 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| AC-22 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| AC-23 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| AC-24 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| AC-25 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| AC-26 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| AC-27 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| AC-28 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| AC-29 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| AC-30 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| AC-31 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| EC-1 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| EC-2 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| EC-3 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| EC-4 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| EC-5 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| EC-6 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| EC-7 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| EC-8 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| EC-9 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| EC-10 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| EC-11 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| EC-12 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| EC-13 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| EC-14 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| EC-15 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| EC-16 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| EC-17 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| EC-25 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| EC-26 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| EC-27 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| EC-28 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| EC-29 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| EC-30 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| EC-31 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |
| EC-32 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_disabled_outbox_operation_is_zero_write |
| EC-33 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_sidecar_normative_ddl_identity_and_immutable_event_history |
| EC-34 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_source_checkpoint_recomputes_object_set_digest_and_rejects_mismatch |
| EC-35 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_status_missing_sidecar_is_unavailable_without_initialization |
| EC-36 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_verified_local_mirror_is_idempotent_and_manifest_readable |
| EC-37 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_retry_schedule_and_dead_letter_are_bounded |
| EC-38 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_only_does_not_create_binding_or_pointer_sidecar |
| EC-39 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_pointer_identity_rejects_extra_table_and_column |
| EC-40 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper |
| EC-18 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_manifest_corruption_is_not_treated_as_empty |
| EC-19 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_postcommit_invalid_pointer_journals_exact_context |
| EC-20 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_cli_rejects_lexical_paths_before_any_write |
| EC-21 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_market_replicate_defaults_to_typed_zero_write_dry_run |
| EC-22 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_replication_init_requires_exact_ack_and_dry_run_is_zero_write |
| EC-23 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_failed_readback_never_moves_manifest_or_published_pointer |
| EC-24 | Effective latest contract remains fail-closed, deterministic, local-first, and preserves canonical ready state on failure. | test_drain_factory_double_gate_short_circuits_before_destination |

## Release boundary

The implementation and evidence commits are separate: implementation_commit is the
code commit and evidence_base_commit is its doc/test-only successor. This candidate
does not claim independent SPEC or QUALITY GO. The final audit must inspect the exact
commit, all IDs and anchors, full/static/no-write results, and the runbook before any
real-NAS change window.
