# R2-F4.3 replication and restore — current implementation plan

Status: **RC / pending final independent gate**; Candidate/NO-GO pending
independent audit. This plan is subordinate to the current design document and
is not a production approval. Worktree operations are `LOCAL_CHAIN_ONLY`:
there is no real NAS/SMB, provider, LaunchAgent, credential, or production
execution.

## Implemented surface

The central factory injects the retained-evidence lineage reader and the
descriptor-stable source instance record into the dataset coordinator. The
coordinator owns the one manifest publication lock, verifies immutable
bindings before field use, and uses one finalize path for post-pointer proof,
lock close, and journal failures. Canonical ready state is never rolled back
because replication is degraded.

The CLI grammar is:

```text
stock-eva market-replicate [--destination ABSOLUTE_PATH] [--operation-day YYYY-MM-DD] [--execute] [--json]
stock-eva market-restore --source ABSOLUTE_PATH --destination ABSOLUTE_PATH [--operation-day YYYY-MM-DD] [--execute] [--json]
stock-eva market-replication-init --destination ABSOLUTE_PATH [--execute --acknowledge CREATE_EMPTY_NAS_ARCHIVE_R2F4_3] [--json]
```

Omitted `--execute` is a typed `dry_run` with zero writes and zero provider
requests. Lexical path validation rejects relative paths, `/`, home,
unresolved environment syntax, and local-storage overlap before operation
access. Execute uses the same factory/services as automation. Init never
adopts a non-empty root and requires the exact acknowledgement.

## Normative safety details

Wire records use strict schemas, canonical JSON, domain-separated SHA-256,
no-follow descriptor traversal, exclusive no-replace installation, file and
parent fsync, and exact readback. Source commits carry the exact immutable
source-instance IDs when configured; disabled replication returns
`SOURCE_NOT_CONFIGURED` without a fabricated zero identity. Sidecar/NAS work
is allowed only under both automation flags, a valid persisted destination
descriptor, and the writer lock. Restore writes only a new temporary root and
never canonical manifest, pointer, or outbox state.

Lineage callers must pass an explicit `LineageInput` or verified resolution.
Legacy input is explicit and only follows a read-only legacy-manifest proof;
modern input must match retained terminal evidence and exact
`SessionSelection` relation/hash preimages. Unknown, partial, mixed, corrupt,
or forged lineage fails before mutation/provider access.

## Current executable crosswalk

| ID | Exact test function |
|---|---|
| FR-MANIFEST | `test_manifest_only_does_not_create_binding_or_pointer_sidecar` |
| FR-SOURCE | `test_disabled_replication_still_seals_local_publication_binding` |
| FR-POINTER | `test_pointer_identity_distinguishes_valid_absent_from_invalid_partial` |
| FR-SCHEMA | `test_pointer_identity_rejects_extra_table_and_column` |
| FR-LINEAGE | `test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper` |
| FR-LINEAGE-UNAVAILABLE | `test_modern_lineage_requires_retained_success_evidence_reader` |
| FR-CALLSITE | `test_nas_writer_call_sites_are_explicit_and_no_global_lineage_shim_exists` |
| FR-DATES | `test_dataset_plan_rejects_duplicate_and_out_of_order_dates` |
| FR-MANIFEST-CORRUPTION | `test_manifest_corruption_is_not_treated_as_empty` |
| FR-EXPLICIT-LINEAGE | `test_omitted_lineage_fails_before_manifest_mutation` |
| FR-BINDING-REUSE | `test_binding_reuse_requires_exact_immutable_fields` |
| FR-BINDING-DUPLICATE | `test_reconcile_rejects_duplicate_generation_bindings` |
| FR-BINDING-HASH | `test_mutated_binding_hash_rejects_reconcile_without_pointer_change` |
| FR-JOURNAL | `test_postcommit_invalid_pointer_journals_exact_context` |
| FR-AUTOMATION | `test_drain_factory_double_gate_short_circuits_before_destination` |
| NFR-SOURCE-RECORD | `test_source_instance_record_is_immutable_and_fsynced` |
| NFR-SOURCE-PREIMAGE | `test_source_instance_identity_golden_preimage_binds_nonce_and_only_dataset_identity` |
| NFR-SOURCE-ROOT | `test_source_instance_read_binds_current_canonical_root` |
| NFR-DISABLED | `test_disabled_outbox_operation_is_zero_write` |
| NFR-STATUS | `test_public_status_exposes_fixed_local_chain_trust_scope` |
| NFR-STATUS-READ | `test_replication_status_missing_sidecar_is_unavailable_without_initialization` |
| NFR-POINTER-SAFETY | `test_failed_readback_never_moves_manifest_or_published_pointer` |

Every row is a concrete AST-discoverable test function; no grouped or
nonexistent historical anchors are normative.

## Verification gate

Run the focused RC tests, CLI dry-run probes, full pytest, backend/tests Ruff
checks, format check, compileall, diff check, and the strict crosswalk
validator. Record actual command output and the exact commit in the draft
acceptance evidence. Do not claim independent QUALITY GO here.
