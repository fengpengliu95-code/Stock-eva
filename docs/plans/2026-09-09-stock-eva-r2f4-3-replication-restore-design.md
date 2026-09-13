# R2-F4.3 replication and restore — current normative design

Status: **RC / pending final independent gate**. This is a candidate design,
not a production or QUALITY GO. No NAS/SMB mount, provider request,
LaunchAgent, credential, or production operation is permitted from this
worktree. The only accepted trust scope is `LOCAL_CHAIN_ONLY`.

## Contract

The local canonical publication is the sole source of truth. Manifest-only
publication mutates and fsyncs only the manifest and verifies its readback;
it never creates a pointer, binding, source commit, sidecar, outbox, or guard.
Dataset-plus-pointer publication creates one immutable binding even when
replication is disabled. An existing exact generation binding is verified and
reused; duplicate or conflicting bindings fail closed. All binding reads call
`SourcePublicationBinding.verify_hash()` before field use.

The source commit is optional when replication is not configured. It must
never contain zero identities. When configured, the coordinator reads one
immutable `SourceInstanceRecord` whose canonical root and descriptor-stable
schema digest match the local control database; the commit records its exact
`source_instance_id` and `source_instance_sha256` and verifies both identities.
Post-pointer proof, close, unlock, and journal failures preserve the ready
pointer and durably journal reconciliation evidence when possible.

`PointerIdentity` is an exact tagged union. ABSENT means a missing database or
an exact canonical schema with no singleton row and no extra payload fields;
PRESENT includes the exact row, device, inode, and schema digest; all schema,
constraint, index, duplicate, unreadable, missing, extra, or type drift is
INVALID. Its descriptor-stable schema digest is shared by factory and reader.

Modern lineage is admitted only from retained successful terminal evidence,
candidate/gate records, and an exact `SessionSelection`: date, universe,
provider, IDs, and all hashes must agree. Missing, forged, unknown, partial,
mixed, or corrupt lineage is unavailable. Legacy is explicit and only allowed
after a read-only proof of an existing legacy manifest. Ordered dates are not
deduplicated. Effective-day planning uses a proven local calendar before any
provider call.

Replication automation has two gates (`replication_enabled` and
`replication_drain_enabled`) plus an approved local destination descriptor and
writer lock. Disabled paths short-circuit before sidecar/NAS access. CLI
operations are explicit, default to zero-write `dry_run`, validate absolute
non-root/non-home/non-overlapping paths lexically and by descriptor, and
sanitize output. Init additionally requires the exact acknowledgement
`CREATE_EMPTY_NAS_ARCHIVE_R2F4_3`.

## Integrity and threat boundaries

All wire records use strict Pydantic schemas (`extra=forbid`), canonical JSON,
domain-separated SHA-256 preimages, no-follow descriptor traversal, exclusive
create/no-replace installation, fsync of files and parent directories, and
descriptor/inode/fingerprint readback. Destination state is local-only and
must not be confused with provider evidence. Unknown or orphan state is
quarantined or reported unavailable; rollback is manual and never replaces a
valid canonical pointer. Secrets, paths, credentials, and provider payloads
are absent from public status.

## Requirements and evidence crosswalk

The following exact anchors are the current executable evidence. The companion
implementation plan repeats this table; the validator rejects any anchor that
is not an actual test function.

| Requirement | Exact evidence anchor |
|---|---|
| manifest-only isolation | `test_manifest_only_does_not_create_binding_or_pointer_sidecar` |
| disabled binding and no zero source identity | `test_disabled_replication_still_seals_local_publication_binding` |
| pointer tagged union | `test_pointer_identity_distinguishes_valid_absent_from_invalid_partial` |
| pointer schema extras invalid | `test_pointer_identity_rejects_extra_table_and_column` |
| retained evidence resolver | `test_factory_resolver_publishes_exact_retained_selection_and_rejects_tamper` |
| explicit NAS writer callsites | `test_nas_writer_call_sites_are_explicit_and_no_global_lineage_shim_exists` |
| unavailable modern lineage | `test_modern_lineage_requires_retained_success_evidence_reader` |
| explicit writer lineage | `test_omitted_lineage_fails_before_manifest_mutation` |
| corrupt manifest | `test_manifest_corruption_is_not_treated_as_empty` |
| ordered dataset plan | `test_dataset_plan_rejects_duplicate_and_out_of_order_dates` |
| binding exact reuse | `test_binding_reuse_requires_exact_immutable_fields` |
| duplicate bindings | `test_reconcile_rejects_duplicate_generation_bindings` |
| binding hash verification | `test_mutated_binding_hash_rejects_reconcile_without_pointer_change` |
| post-pointer journal | `test_postcommit_invalid_pointer_journals_exact_context` |
| automation double gate | `test_drain_factory_double_gate_short_circuits_before_destination` |
| source record immutability | `test_source_instance_record_is_immutable_and_fsynced` |
| source identity preimage | `test_source_instance_identity_golden_preimage_binds_nonce_and_only_dataset_identity` |
| source root binding | `test_source_instance_read_binds_current_canonical_root` |
| disabled sidecar zero work | `test_disabled_outbox_operation_is_zero_write` |
| local chain trust scope | `test_public_status_exposes_fixed_local_chain_trust_scope` |
| explicit destination status | `test_replication_status_missing_sidecar_is_unavailable_without_initialization` |
| canonical manifest/pointer safety | `test_failed_readback_never_moves_manifest_or_published_pointer` |

## Release boundary

The final independent audit must inspect the exact commit, diff, validator
output, full test output, and disabled/no-real-NAS evidence. Candidate status
remains **Candidate/NO-GO pending audit** until that gate is separately
recorded. Unknown-orphan remediation and any real-NAS controlled operation
require a new explicit window and the runbook checklist.
