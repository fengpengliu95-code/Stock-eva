# Stock EVA R2-F4.3 Local Replication and Verified Restore — Implementation Plan

**Author:** Codex

**Date:** 2026-09-09 (Asia/Shanghai)

**Status:** In Review / NO-GO pending independent review of the R2-F4.3.5 final normative consolidation; implementation MUST NOT start before design approval

**Reviewers:** Stock EVA architecture, data reliability and operations reviewers

**Companion design:** `2026-09-09-stock-eva-r2f4-3-replication-restore-design.md`

**Base:** exact reviewed predecessor `6e66f1e531adbedd0e3787fccc724a92ab952834`

## Context

The existing R2-F reliability implementation has strict local publication and NAS-to-local
mirror primitives, but no production-wired local-to-NAS outbox. `NasMarketStore` publishes
validated Parquet objects and `manifest.json` under a manifest lock; `MarketDatasetMirror` reads
an existing archive and copies it to a local mirror. Their directions, trust assumptions and
failure contracts are different and must remain different.

The implementation must preserve the existing canonical Normalize → Quality Gate → Immutable
Parquet → SHA-256 → Manifest → Atomic Publish chain. The new code observes the committed local
manifest, writes only local control/journal state for its queue, and performs destination I/O in a
separate bounded operation. It must never make a NAS socket or TCC permission a prerequisite for
local `ready`.

The plan deliberately begins with offline RED tests and typed fakes. No real provider, SMB mount,
NAS path, LaunchAgent installation or production destination is touched in this worktree. A
future real-copy/restore window must be a separate reviewed operation with a cleaned-up permission
probe and a new evidence record.

The destination has one authority only: NAS `_replication/history/<replication_generation>/`
`replication-record.json` and `_replication/head.json`. Local SQLite is limited to queue state,
immutable attempt audit and a last-observed cache; no local row can select or promote a NAS
generation, and restore verifies the NAS archive without relying on that cache.

### Inherited reliability constraints reviewed

Implementation must retain the predecessor boundaries: R2-F0.1 transport and quality gates remain
fail-closed; R2-F1 control readers remain zero-write; R2-F2 evidence/candidate/gate hashes remain
immutable; R2-F3 shadow remains isolated from canonical publication; R2-F4.0 admission stays
default-off; R2-F4.1 calendar authority remains PIT/conflict-safe; and R2-F4.2 Universe sidecar,
API and CLI readers remain strict, sanitized and zero-write. This plan adds only downstream
replication/restore control and never widens canonical provider authority.

## Functional Requirements

- FR-1: Add a default-off, explicitly enabled replication setting; disabled execution MUST be zero-work.
- FR-1a: `replication_enabled` authorizes local checkpoint capture/enqueue only. Automated NAS draining additionally requires `STOCK_EVA_MARKET_REPLICATION_DRAIN_ENABLED=true`, an approved descriptor and the project writer lock; CLI `--execute` is an independent explicit authorization.
- FR-2: Build one deterministic intent from the committed local manifest/pointer, without provider access or data refetch.
- FR-2a: After manifest success and before a dataset-backed DuckDB pointer commit, write the immutable private `<local_dataset>/_replication/source-commits/<run_id>.json` publication-binding record, fsync its bytes and parent directories, and require exact pointer/binding identity; this additive artifact MUST NOT change canonical manifest or pointer schemas.
- FR-3: Keep the canonical transaction before enqueue; enqueue failure MUST preserve local ready and pointer bytes.
- FR-3a: Before every next canonical pointer commit, run a durability guard under the existing refresh/control lock; it MUST prove or durably establish the current singleton checkpoint, otherwise block only the next commit with `OUTBOX_DURABILITY_UNAVAILABLE` and preserve the current ready pointer.
- FR-3b: Route every dataset-backed `NasMarketStore`/dataset pointer writer seam (`save_refresh`, reconcile-control-pointer, CLI reconciliation, `full_history` and legacy `save_external_publication` callers) through `publish_dataset_and_pointer`; no direct pointer write may bypass binding, the manifest publication lock or the guard. The plain `MarketStore` canonical commit remains byte/behavior compatible and does not acquire this binding/guard or access NAS; its post-commit replication observation is `SOURCE_NOT_CONFIGURED` when no explicit dataset/manifest is present.
- FR-3c: The dataset-backed central wrapper acquires one non-reentrant opaque `ManifestPublicationLock` token before baseline manifest read and holds it through object/staging writes, manifest replace/readback, binding write/fsync, DuckDB pointer commit and post-commit exact proof. `backfill`, `upsert`, `refresh`, reconcile and every other manifest mutation use that same lock; `RefreshRunLock` is scheduling only, never the atomicity dependency. A final manifest fingerprint CAS immediately before pointer commit leaves the pointer unchanged on drift.
- FR-3d: Legacy/no-selection publications use fixed domain-separated canonical-null selection/lineage digests; missing required v2 manifest metadata returns `SOURCE_UNAVAILABLE` and leaves the pointer unchanged. Legal reconcile may reuse a binding only after the same lock-bound exact proof.
- FR-3e: Implement one `ManifestPublicationCoordinator` with exactly two mutually exclusive typed operations: `publish_manifest_only(...)` (manifest update/verification only, no binding/pointer) for every `NasMarketStore.upsert_bars` and `BackfillService` batch, and `publish_dataset_and_pointer(...)` (manifest → binding → pointer) for dataset publication. Both share only a token-checked locked primitive; manifest-only MUST NOT call the pointer operation.
- FR-3f: Lock acquisition/token/baseline/guard/final-CAS failures before pointer commit leave pointer bytes/hash/inode unchanged. After pointer commit, unlock/close/post-proof/control failures cannot roll back canonical ready; return degraded `CONTROL_STATE_UNAVAILABLE`, durably reconcile, and let the next guard handle it.
- FR-3g: `publish_manifest_only` returns a typed three-state pointer proof: `ABSENT` only when the DB is absent or its proven-valid schema has no singleton row; `PRESENT{row_sha256,device,inode,schema_digest}` for a complete singleton; and `INVALID{reason_code=CONTROL_STATE_UNAVAILABLE}` for missing tables, corrupt schema, read failure, duplicate/malformed singleton or incomplete state. `INVALID` fails closed before any manifest mutation, and `ABSENT` is never a forced string, empty hash or synthetic row.
- FR-3h: Before any manifest-only mutation, the coordinator reads the existing manifest lineage mode and invokes the discriminated-union `LineageResolver` for every `trade_date` in a `BackfillService` batch. An empty manifest may classify incoming input as legacy or modern; a non-empty manifest retains one complete mode/lineage. Any missing, unavailable or mismatched date resolution fails the entire batch with `SOURCE_UNAVAILABLE` before the first upsert; it never partially mutates or contaminates a modern manifest. Every existing `NasMarketStore.upsert_bars` caller passes an explicit legacy input or an exact/allowlisted modern input; plain `MarketStore` is unchanged.
- FR-4: Implement the normative SQLite outbox, immutable identity, state transitions, terminal retention and state-version CAS.
- FR-5: Persist or journal enqueue gaps and reconcile a pointer/outbox gap after restart without a provider request.
- FR-5a: Install journals with an `O_EXCL|O_NOFOLLOW` temporary, file/directory fsync, macOS `renameatx_np(RENAME_EXCL)` or portable hard-link-no-replace fallback; reuse an existing target only when byte-identical and never overwrite it.
- FR-6: Implement strict descriptor-bound source snapshots for sentinel, manifest and every object.
- FR-7: Implement explicit destination trust, mount/sentinel checks and local-root overlap protection.
- FR-8: Copy only into a destination staging namespace that a strict reader cannot publish.
- FR-9: Read back every copied object and compare size, schema, rows, hash, path and lineage.
- FR-10: Write a self-contained immutable NAS `replication-record.json` after all object verification, then advance the complete NAS `head.json` atomically under the writer lock; SQLite is never destination authority.
- FR-11: Bind destination lineage to direction, checkpoint/source identity and sequence, parent record, destination trust and complete object inventory.
- FR-12: Make exact-checkpoint replication idempotent and reject older/conflicting lineage without changing NAS record/head.
- FR-13: Expose local-to-NAS replication and NAS-to-temporary-root restore as separate directions; never reverse replicate.
- FR-14: Implement bounded deterministic retry and durable dead-letter behavior.
- FR-15: Implement nonblocking local locking, leases and state-version CAS for concurrent drains.
- FR-16: Implement restore into a nonexistent temporary root only; complete semantic verification in hidden same-parent staging, then perform one atomic directory rename.
- FR-17: Verify restored sentinel, manifest, objects, schema, rows, dates, source and representative read-only queries.
- FR-18: Add a zero-write sanitized replication status API.
- FR-19: Add `market-replicate` with dry-run as the default and execution behind `--execute`.
- FR-20: Add `market-restore` with dry-run as the default and temporary-root writes behind `--execute`.
- FR-21: Add a separately acknowledged destination initialization path that never adopts non-empty roots.
- FR-22: Integrate with an existing optional refresh slot without TCC or credential assumptions.
- FR-23: Reject unsafe paths, symlink/TOCTOU races, root/home paths, unresolved environment syntax and overlap.
- FR-24: Sanitize every API, CLI and structured log projection; never expose paths, secrets, payload or raw exceptions.
- FR-25: Project local readiness independently from destination availability and queue lag.
- FR-26: Retain sufficient sanitized evidence to replay decisions and verification without provider access.
- FR-27: Preserve the existing mirror, dataset reader and canonical publication compatibility contracts.

## Non-Functional Requirements

- NFR-1: Status and dry-run tests MUST issue zero provider/network requests and zero filesystem/DB writes.
- NFR-2: Strict source/destination validation MUST cover 100% of referenced objects before ready.
- NFR-3: A destination reader MUST observe either the previous or the complete next manifest, never a partial generation.
- NFR-4: Each drain invocation MUST process at most one claimed intent by default, six claims per intent and 15 minutes.
- NFR-5: Retry delays MUST be exactly 60 seconds, 5 minutes, 30 minutes, 2 hours and 12 hours.
- NFR-6: Every mutable outbox change MUST be one SQLite transaction guarded by state-version CAS.
- NFR-7: All hash preimages MUST use the specified newline-terminated canonical UTF-8 JSON encoding.
- NFR-8: Public projections MUST contain no path, URL, username, credential, token, payload or arbitrary exception.
- NFR-9: Every phase boundary MUST recheck descriptor fingerprints and use no-follow opens.
- NFR-10: NAS failure MUST not change local publication readiness or canonical pointer identity.
- NFR-11: Status MUST be p95 under 500 ms with 10,000 terminal rows and no Parquet hash scan.
- NFR-12: Failed restore MUST leave no final reader-visible root; only non-visible quarantine is allowed.
- NFR-13: Existing default runtime and LaunchAgents MUST remain replication-off absent explicit reviewed configuration.
- NFR-14: Existing canonical, mirror and compatibility tests MUST remain green and their bytes unchanged.
- NFR-15: All implementation evidence in this plan MUST be offline; real NAS work is a separate window.

## Acceptance Criteria

### AC-1: Disabled local-first behavior (FR-1, FR-22, FR-25, NFR-1, NFR-13)

Given replication is disabled and local data is ready, When the automation and status surfaces run, Then the first branch returns before constructing or opening sidecar/source/destination/lock dependencies, local readiness remains ready, and all inspected bytes/inodes remain unchanged.

### AC-2: Binding, pointer and enqueue boundary (FR-2, FR-2a, FR-3, FR-3b, FR-3c, FR-3d, FR-5, FR-25, NFR-6, NFR-10)

Given a dataset-backed publication has a successful manifest but binding write/readback fails, When any pointer writer seam attempts publication, Then the new pointer commit is blocked, the prior ready pointer is unchanged, no provider is called, and the failure is `OUTBOX_DURABILITY_UNAVAILABLE`; given a valid binding, the central seam commits the pointer only when all binding fields match exactly. The same non-reentrant manifest lock MUST cover baseline read, object/staging writes, manifest replace/readback, binding fsync, pointer commit and post-commit exact proof; a final fingerprint drift blocks the pointer. A legacy/no-selection publication uses canonical-null selection/lineage digests, while missing required v2 metadata returns `SOURCE_UNAVAILABLE`; a plain `MarketStore` keeps its existing canonical commit and only observes `SOURCE_NOT_CONFIGURED`. After that pointer commit, a synthetic outbox error preserves ready state and projects `OUTBOX_ENQUEUE_FAILED` without rollback. Required tests are `test_binding_write_failure_blocks_pointer_without_rollback`, `test_publication_is_atomic_under_one_manifest_publication_lock`, `test_manifest_fingerprint_drift_blocks_pointer_cas`, `test_publication_binding_is_fsynced_before_pointer_and_exactly_matches`, `test_manifest_changed_before_pointer_commit_is_rejected`, `test_all_dataset_pointer_writers_route_through_coordinator`, `test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured`, `test_legacy_no_selection_uses_canonical_null_digests`, `test_missing_v2_manifest_metadata_blocks_pointer`, and `test_enqueue_failure_preserves_local_pointer_ready`.

### AC-3: Binding and journal crash recovery (FR-3, FR-4, FR-5, FR-26, NFR-6, NFR-7)

Given a process stops before pointer commit during binding installation, or after pointer commit before SQLite enqueue, When the next explicit writer/reconciliation runs, Then the old pointer remains unchanged in the first case, the exact binding is reused without overwrite, and exactly one deterministic intent is persisted in the second case; a binding whose manifest bytes changed or whose pointer never matched remains inactive. Required tests are `test_pointer_unchanged_after_binding_crash_is_recoverable`, `test_publication_binding_target_existing_identical_is_reused`, `test_multiple_checkpoint_journals_crash_and_recover_exactly_once`, and `test_binding_active_only_when_pointer_exact_match`.

### AC-4: Strict outbox contract (FR-4, FR-14, FR-15, NFR-6)

Given an empty control root, When the outbox is initialized and two workers claim the same intent, Then the normative DDL accepts it, exactly one CAS claim wins, and immutable identity/dead-letter rows cannot be deleted or rewritten.

### AC-5: Complete source snapshot (FR-6, FR-9, FR-11, FR-26, NFR-2, NFR-9)

Given a source manifest or object has a missing, changed, wrong-size, wrong-schema, wrong-row-count or wrong-hash condition, When an intent is planned, Then the candidate is rejected without destination object/record/head writes and with a sanitized stage reason.

### AC-6: Destination trust and path safety (FR-7, FR-21, FR-23, FR-24, NFR-8, NFR-9)

Given an unsafe path, unapproved mount, missing trust descriptor, symlink or overlapping root, When a dry-run is requested, Then it returns a bounded error and performs no initialization, credential read or write.

### AC-7: Verified staged transfer (FR-8, FR-9, FR-10, FR-11, NFR-2, NFR-3, NFR-9)

Given a complete source checkpoint and trusted empty destination, When one offline fake drain executes, Then all objects pass readback, a self-contained NAS record is committed, and the complete NAS head is advanced last before the sidecar result.

### AC-8: Failure leaves NAS head/record unchanged (FR-8, FR-9, FR-10, FR-12, NFR-3, NFR-12)

Given copy, verification, record write or head CAS is interrupted, When the attempt stops, Then the previous NAS head/record remains byte-identical and partial data is not referenced.

### AC-9: Idempotence and no old overwrite (FR-11, FR-12, FR-13, FR-27, NFR-3, NFR-14)

Given a destination has exact, older, ahead, conflicting or reverse lineage, When the same source is drained, Then only exact checkpoint lineage is reused and no unsafe NAS record/head or source pointer is overwritten.

### AC-10: Retry and dead-letter (FR-14, FR-25, FR-26, NFR-4, NFR-5)

Given a retryable destination error, When the bounded drain is run repeatedly, Then the exact retry schedule is durable and the sixth failure becomes dead-letter without provider access.

### AC-11: Concurrency and stale lease (FR-4, FR-15, FR-26, NFR-6)

Given one worker crashes while another claims the expired lease, When both attempt state advancement, Then the stale worker cannot copy/advance the NAS head and only the winning state-version transition is accepted.

### AC-12: Restore dry-run (FR-13, FR-16, FR-17, FR-20, FR-23, FR-24, NFR-1, NFR-12)

Given a trusted archive and explicit temporary destination, When `market-restore` is run without `--execute`, Then it performs no writes/provider calls and emits only counts/hashes and a safe plan.

### AC-13: Verified temporary restore (FR-16, FR-17, FR-20, FR-24, NFR-2, NFR-3, NFR-9, NFR-12)

Given a complete archive, When restore executes, Then a newly created temporary root passes strict manifest/object/query verification before one atomic directory rename, and canonical bytes/inodes are unchanged.

### AC-14: Restore failure is invisible (FR-16, FR-17, FR-20, FR-23, NFR-9, NFR-12)

Given a corrupt or changing archive, When restore executes, Then no final root is published, no canonical state is changed and only the allowlisted reason is returned.

### AC-15: Read-only status (FR-18, FR-24, FR-25, NFR-1, NFR-8, NFR-11)

Given outbox state is missing, corrupt, locked, empty, retrying, dead-lettered or healthy, When the status endpoint is called, Then it does not initialize state or perform a network probe, exposes bounded counts/lag and persisted destination health, and reports `mode=status`, zero provider requests and all effects false.

### AC-16: Replication CLI plan (FR-18, FR-19, FR-23, FR-24, NFR-1, NFR-13)

Given a valid or invalid explicit destination, When `market-replicate` runs with and without `--execute`, Then omitted execution is always zero-write and invalid paths cannot create state.

### AC-17: Explicit destination initialization (FR-7, FR-21, FR-23, NFR-9, NFR-15)

Given a new empty child of an approved mount, When the separately acknowledged initializer runs, Then it creates only the trust descriptor and non-published directories; existing/non-empty/share-root paths are rejected.

### AC-18: Existing mirror compatibility (FR-13, FR-27, NFR-14)

Given an existing archive, When old NAS-to-local mirror tests and new local-to-NAS tests run together, Then the old direction, reused generation and reader behavior remain unchanged.

### AC-19: LaunchAgent/TCC boundary (FR-1, FR-22, FR-25, NFR-10, NFR-13, NFR-15)

Given a refresh LaunchAgent cannot access `/Volumes`, When local publication succeeds, Then enqueue/journal remains local, drain failure is degraded only, and no mount or credential attempt occurs.

### AC-20: Offline evidence and release boundary (FR-24, FR-26, FR-27, NFR-1, NFR-15)

Given focused/full tests and strict validators run in an isolated worktree, When review closes, Then every FR/AC/EC has a real test/static anchor, exact HEAD and commands are recorded, and no production/NAS GO is claimed.

## Edge Cases

- EC-1: Disabled feature or missing local root → safe disabled/source-not-configured result and zero work.
- EC-2: Relative, tilde, `$VAR`, `${VAR}` or command-substitution CLI path → lexical `PATH_INVALID` with no I/O.
- EC-3: Root/home/local mutable/control/staging/temp or unresolved target → reject before creation.
- EC-4: Symlink or inode change in any ancestor/object/manifest → fail closed as unsafe/path changed.
- EC-5: Missing or unexpected SMB/CIFS mount/TCC access → destination unavailable, no credential probe.
- EC-6: Missing/oversized/malformed/wrong-role trust metadata → trust failure without initialization.
- EC-7: Unsafe, duplicate, extra, missing or non-Parquet manifest entry → whole source candidate rejected.
- EC-8: Missing/empty/wrong hash/size/schema/row count/partition object → whole intent rejected.
- EC-9: Orphan/partial/duplicate/extra destination object → never reader-visible; quarantine or conflict.
- EC-10: NAS record lineage is ahead/conflicting/unknown → no overwrite and exact reason.
- EC-11: NAS head/record changes between locked baseline and CAS → CAS conflict with current NAS head preserved.
- EC-12: Copy/process crash before NAS record/head commit → old head remains and verified objects may be reused.
- EC-13: Readback mismatch → no NAS record/head publication, bounded failure.
- EC-14: Missing/corrupt/locked outbox on status → 503 state unavailable, no migration/write.
- EC-15: Enqueue failure after local pointer → preserve local ready and write deterministic journal when possible.
- EC-16: Outbox and journal both fail → preserve local ready and report durability gap for reconciliation.
- EC-17: Crash after journal before import → idempotent import before journal removal.
- EC-25: A binding temp/install, manifest mutation, or pointer transaction crashes before commit → only an inactive immutable binding or quarantined temp may remain; the previous pointer remains ready and no mismatched binding becomes an active SourceCommit.
- EC-26: The local control DB inode rotates during a legitimate migration → the current checkpoint records the new device/inode/schema, but the immutable dataset-root nonce/identity remains stable; any replacement that fails strict root, nonce, schema, singleton, manifest or pointer proof is `LOCAL_POINTER_MISMATCH`/`OUTBOX_DURABILITY_UNAVAILABLE` and is never accepted.
- EC-27: A concurrent `backfill`/`upsert`/refresh mutates the manifest during a dataset-backed publication → the shared non-reentrant manifest lock serializes the mutation; any final fingerprint drift fails the pointer CAS and leaves the prior pointer unchanged.
- EC-28: `RefreshRunLock` is absent, reordered or nested while the manifest publication lock is held → it cannot be the publication atomicity dependency; the opaque manifest-lock token remains the sole ownership proof and lock-order violation fails closed.
- EC-29: A legacy/no-selection candidate lacks required v2 manifest metadata → fixed canonical-null selection/lineage digests are used only when metadata is intentionally absent; a required-but-missing field returns `SOURCE_UNAVAILABLE` and leaves the pointer unchanged.
- EC-30: A plain `MarketStore` publishes without an explicit local dataset/manifest → its existing canonical commit is unchanged; no binding/guard/NAS work runs and the post-commit observation is `SOURCE_NOT_CONFIGURED`.
- EC-31: `NasMarketStore.upsert_bars` or a `BackfillService` batch invokes `publish_manifest_only(...)` with explicit per-date lineage input → it may update and verify only the manifest; it must never create a binding or call the pointer wrapper, and pointer bytes/hash/inode remain unchanged.
- EC-32: Manifest lock acquisition, token, precommit or final-CAS readiness fails before pointer commit → pointer bytes/hash/inode remain unchanged and no post-commit degraded observation is emitted.
- EC-33: Pointer commit succeeds but unlock/close/post-proof control fails → canonical ready remains visible; emit degraded `CONTROL_STATE_UNAVAILABLE`, durably record reconciliation evidence, and let the next pre-publication guard repair/prove control state.
- EC-34: A manifest-only publication starts with an empty control database → return `PointerIdentity.ABSENT`; compare the complete tagged union before/after and do not serialize a fake string identity.
- EC-35: A multi-date `BackfillService` plan has one omitted, partial or conflicting per-date lineage input → the resolver returns `UNAVAILABLE/SOURCE_UNAVAILABLE` before any date is upserted; existing manifest and pointer identities remain unchanged.
- EC-18: Concurrent claim/lease expiry → stale worker cannot publish or mark success.
- EC-19: Sixth retry or non-retryable trust/integrity error → retained dead-letter, no infinite retry.
- EC-20: Restore source changes or is invalid → no final root and canonical state unchanged.
- EC-21: Restore destination exists/non-empty/unsafe/overlapping → reject before writes.
- EC-22: Restore verification/query fails before rename → no destination visible; post-rename non-semantic anomaly isolates the destination, no enqueue/canonical change.
- EC-23: Legacy mirror sees new lineage fields → old manifest reader remains compatible; new reader is strict.
- EC-24: Reverse replication, source deletion, manual pointer edit or credentialed mount → direction error, zero writes.

## API Contracts

The implementation will add one read-only API and CLI serializers with this exact bounded shape.

```typescript
type ReplicationReason =
  | "NONE" | "DISABLED" | "SOURCE_NOT_CONFIGURED" | "SOURCE_UNAVAILABLE"
  | "LOCAL_POINTER_MISMATCH" | "REPLICATION_STATE_UNAVAILABLE" | "OUTBOX_ENQUEUE_FAILED"
  | "OUTBOX_JOURNALED" | "OUTBOX_DURABILITY_UNAVAILABLE" | "CONTROL_STATE_UNAVAILABLE"
  | "DESTINATION_UNAVAILABLE"
  | "DESTINATION_MOUNT_UNAVAILABLE" | "DESTINATION_TRUST_FAILED" | "DESTINATION_REBOUND"
  | "DESTINATION_AHEAD" | "DESTINATION_CONFLICT" | "CAS_CONFLICT" | "COPY_FAILED"
  | "VERIFY_FAILED" | "RETRY_WAIT" | "DEAD_LETTER" | "ALREADY_REPLICATED"
  | "PATH_INVALID" | "PATH_CHANGED" | "SYMLINK_UNSAFE" | "DIRECTION_NOT_ALLOWED"
  | "MOUNT_UNSUPPORTED";
interface Effects { writes: boolean; canonical_writes: boolean;
  destination_writes: boolean; outbox_writes: boolean; restore_writes: boolean; }
interface ReplicationStatusResponse {
  status: "disabled" | "ready" | "degraded" | "unavailable";
  reason_code: ReplicationReason;
  enabled: boolean;
  source_ready: boolean;
  destination_configured: boolean;
  outbox_schema_version: number | null;
  pending_count: number;
  copying_count: number;
  verifying_count: number;
  retry_wait_count: number;
  dead_letter_count: number;
  last_replicated_source_manifest_sha256: string | null;
  last_replicated_at: string | null;
  local_ready: boolean;
  destination_health: "unknown" | "healthy" | "unavailable" | "unsupported";
  destination_health_observed_at: string | null;
  queue_lag_seconds: number | null;
  lag_seconds: number | null;
  provider_requests: 0;
  mode: "status";
  execution_allowed: false;
  effects: Effects;
  paths_exposed: false;
  trust_scope: "LOCAL_CHAIN_ONLY";
}

GET /api/v1/storage/replication -> ReplicationStatusResponse
  200: disabled, ready or degraded
  503: unavailable, same sanitized shape

interface ReplicatePlanResponse {
  status: "dry_run" | "ready" | "degraded" | "unavailable";
  reason_code: ReplicationReason;
  source_manifest_canonical_sha256: string | null;
  source_manifest_bytes_sha256: string | null;
  source_object_set_sha256: string | null;
  publication_binding_sha256: string | null;
  source_instance_id: string | null;
  source_sequence: number | null;
  checkpoint_id: string | null;
  pointer_generation: string | null;
  object_count: number;
  row_count: number;
  byte_count: number;
  planned_intent: boolean;
  mode: "plan" | "execute";
  execution_allowed: boolean;
  effects: Effects;
  provider_requests: 0;
  paths_exposed: false;
}

interface RestorePlanResponse {
  status: "dry_run" | "ready" | "unavailable";
  reason_code: ReplicationReason;
  source_manifest_canonical_sha256: string | null;
  source_object_set_sha256: string | null;
  publication_binding_sha256: string | null;
  source_instance_id: string | null;
  source_sequence: number | null;
  checkpoint_id: string | null;
  object_count: number;
  row_count: number;
  byte_count: number;
  rename_allowed: boolean;
  mode: "plan" | "execute";
  execution_allowed: boolean;
  effects: Effects;
  provider_requests: 0;
  paths_exposed: false;
}
```

CLI exit codes are fixed: `0` for a safe plan or successful execution, `1` for unavailable,
retry/dead-letter or integrity failure, and `2` for lexical/configuration/path/direction error.
CLI outputs never echo input paths, credentials, URLs or arbitrary exceptions.

Implement `local_ready` from the existing canonical readiness reader. `destination_health` and its
timestamp are read only from the SQLite `replication_destination_cache`, populated after a
descriptor-bound NAS head read; this cache is not destination authority. Status performs no
network/mount probe and returns `unknown` when no persisted probe exists. `queue_lag_seconds` is
derived from committed sidecar timestamps and is `null` when strict control evidence is unavailable.
Lineage and restore always read NAS archive record/head directly.

## Data Models

### Implementation file map

| File | Planned change | Boundary |
|---|---|---|
| `backend/app/config.py` | Add default-off replication settings and fixed bounds | No credentials; absolute destination only |
| `backend/app/main.py` | Wire the explicit local source root, strict pointer reader, pre-publication durability guard, frozen commit seam and replication service | No NAS/source fallback; no canonical schema migration |
| `backend/app/storage/layout.py` | Add local `_replication/source-instance.json`, `_replication/source-commits/<run_id>.json`, outbox/journal and replication lock paths | Never creates destination during read/status; binding paths are private additive metadata |
| `backend/app/storage/models.py` | Add strict private/public projection models | No path/payload fields in public model |
| `backend/app/storage/replication.py` | New source snapshot, outbox, journal, transfer, restore and status services | Only module allowed to implement local-to-NAS direction |
| `backend/app/storage/dataset.py` | Own `ManifestPublicationCoordinator` and one non-reentrant `ManifestPublicationLock` token; expose `publish_manifest_only` for `upsert_bars`/backfill and `publish_dataset_and_pointer` for `save_refresh`/reconcile | Both use a token-checked locked primitive; manifest-only never writes binding/pointer; dataset+pointer spans baseline manifest read → objects/staging → manifest replace/readback → binding fsync → pointer commit → exact proof |
| `backend/app/market/store.py` | Preserve the plain `MarketStore` canonical commit byte/behavior exactly; emit only the typed post-commit `SOURCE_NOT_CONFIGURED` replication observation when no explicit dataset/manifest exists | No binding, guard, NAS access or nested manifest lock for plain MarketStore |
| `backend/app/market/refresh.py` | Call `publish_manifest_only` for bar upsert and `publish_dataset_and_pointer` for a dataset-backed refresh publication; never acquire `_ManifestLock` directly | `RefreshRunLock` is optional scheduling only and cannot provide publication atomicity |
| `backend/app/market/backfill.py::BackfillService.plan/execute` | Use `publish_manifest_only` for every date/group after all per-date lineage resolutions pass; never call the pointer operation | Missing/extra/duplicate date mapping or one resolver failure yields whole-batch `SOURCE_UNAVAILABLE` before the first upsert; pointer bytes/hash/inode remain unchanged |
| `backend/app/market/full_history.py`, `backend/app/cli.py`, `backend/app/main.py` | Call `publish_dataset_and_pointer` for full-history, reconciliation and startup recovery; pass no lock token and never nest/acquire the manifest lock | All calls use one coordinator-owned opaque token |
| `backend/app/storage/replication.py` | Implement `DestinationArchiveReader` on root/generation dirfds and the typed service seam | No NAS path reopen; only pure validation helpers may be reused |
| `backend/app/storage/mirror.py` | Keep NAS-to-local API; share only safe validation helpers if needed | Must not call replication in reverse |
| `backend/app/market/automation.py` | Invoke local enqueue after canonical commit, preserve result | No destination I/O before canonical ready |
| `backend/app/api/storage.py` | Add read-only `/replication` route | No initialization/migration/write |
| `backend/app/cli.py` | Add `market-replicate`, `market-restore`, status and explicit init parsing; route CLI reconciliation through central binding seam | Dry-run default; no direct `save_external_publication` |
| `scripts/stock_eva_launchagents_install.sh` and `launchd/*.plist.in` | Validate optional hook/default-off/TCC behavior | No new required NAS agent |
| `tests/test_dataset_replication.py` | RED/GREEN contract and attack tests | Offline typed fakes only |
| `tests/test_nas_dataset.py` | Preserve and extend compatibility checks | Existing mirror direction stays intact |
| `docs/nas-storage.md` | Document outbox/restore/runbook and TCC boundary | Real operations remain separately authorized |

### Canonical hash preimages

Implement exactly:

```python
canonical_json_bytes(value) = (
    json.dumps(value, ensure_ascii=False, sort_keys=True,
               separators=(",", ":"), allow_nan=False) + "\n"
).encode("utf-8")
domain_sha256(domain, value) = sha256(
    domain.encode("ascii") + b"\n" + canonical_json_bytes(value)
).hexdigest()
```

The source checkpoint, publication-binding, plan and intent domains are respectively
`stock-eva/r2f4.3/source-checkpoint/v1`, `stock-eva/r2f4.3/source-publication-binding/v1`,
`stock-eva/r2f4.3/replication-plan/v1` and
`stock-eva/r2f4.3/replication-intent/v1`. The plan includes every sorted object path, object
hash, size, row count, trade date and source. Intent identity excludes operation day and includes direction,
destination ID, source instance/sequence and complete plan hash. Raw manifest bytes hash and
canonical manifest hash are observed from the unchanged committed source. The closed manifest
projection is `{dataset,schema_version,generation,files}`; each file is exactly
`{path,sha256,trade_date,source,row_count,provider_id,universe_id,evidence_id,evidence_sha256,
candidate_id,candidate_manifest_sha256,gate_report_sha256,adapter_version,source_schema_version}`
with absent legacy lineage values kept absent in manifest bytes and encoded as JSON `null` only in
the closed hash projection, and `files` sorted by `(path,sha256)`.
Thus `manifest_canonical_sha256 = domain_sha256("stock-eva/r2f4.3/manifest-canonical/v1",
manifest_projection)`. The pointer DB schema projection is the sorted, self-excluded SQLite
`sqlite_master` tuple list `{type,name,tbl_name,sql}` across application tables, indexes and
triggers, hashed as `domain_sha256("stock-eva/r2f4.3/pointer-db-schema/v1", schema_projection)`.
`source_manifest_bytes_sha256` is the direct SHA-256 of exact bytes read through its bound
descriptor and has no JSON preimage.

`dataset_identity = domain_sha256("stock-eva/r2f4.3/dataset-identity/v1",
{canonical_root_path,canonical_schema_digest,source_instance_domain,source_instance_nonce})` and
`source_instance_id = domain_sha256("stock-eva/r2f4.3/source-instance/v3", {dataset_identity})`.
The immutable nonce is created once inside the local dataset replication namespace and is never
derived from a DuckDB inode. The publication binding hash is
`publication_binding_sha256 = domain_sha256("stock-eva/r2f4.3/source-publication-binding/v1",
{binding_schema,schema_version,run_id,trade_date,published_at,manifest_generation,
manifest_bytes_sha256,manifest_canonical_sha256,source_object_set_sha256,selection_sha256,
lineage_sha256})`. `selection_sha256` and `lineage_sha256` are canonical hashes of the publisher's
closed selection and lineage projections, not free-form labels.

### State, transaction and crash implementation matrix

| Concern | Concrete implementation rule | Evidence |
|---|---|---|
| Canonical boundary | `publish_manifest_only` owns only manifest update/verify; `publish_dataset_and_pointer` owns baseline → objects/staging → manifest replace/readback → binding fsync → pointer commit → exact proof; enqueue starts only after release/commit | AC-2, AC-21, AC-23, EC-31 |
| Writer lock order | Optional `RefreshRunLock` is acquired by the scheduler before calling the wrapper; the wrapper alone acquires the non-reentrant manifest lock and no caller acquires/nests it | FR-3c, EC-28 |
| Plain MarketStore | Existing canonical commit path is unchanged; no binding/guard/NAS work; post-commit observation is `SOURCE_NOT_CONFIGURED` without explicit dataset/manifest | FR-3b, AC-22, EC-30 |
| Legacy metadata | Intentional no-selection uses canonical-null selection/lineage domains; required v2 metadata absence returns `SOURCE_UNAVAILABLE` before pointer commit | FR-3d, EC-29 |
| Coordinator operation boundary | Exactly two typed mutually exclusive operations share only a token-checked primitive; manifest-only never calls the pointer wrapper | FR-3e, AC-23, EC-31 |
| Pointer phase errors | Only acquire/token/baseline/guard/final-CAS occur before pointer commit; any failure leaves pointer identity untouched. Post-pointer control errors are degraded durable reconciliation | FR-3f, AC-24, EC-32/33 |
| Enqueue gap | `<checkpoint_id>.json` journal fallback; startup imports all journals sorted by `(source_published_at,checkpoint_id)` in one transaction | AC-3, EC-16/17 |
| Claim | Private in-memory transaction, lease owner/until and `state_version` conditional update; serialized through the descriptor-native engine | AC-4, AC-11 |
| Copy | Stage under destination `_staging/<intent>`; no manifest references staging | AC-7/8 |
| Readback | Open no-follow and verify size/schema/rows/hash/fingerprint before each rename | AC-5/7 |
| Publish | Under the descriptor lock, install self-contained NAS record, atomically commit complete NAS head first, then append SQLite result/cache; fsync each boundary | AC-8/9 |
| Retry | Fixed schedule, retryable classes only, sixth claim dead-letters | AC-10 |
| Restore | Require nonexistent destination, stage in same parent, verify, fsync, then one atomic directory rename; no typed pointer/canonical control call | AC-12/13/14 |
| Status | `initialize=False`, O_NOFOLLOW/inode SELECT-only snapshot, last persisted destination probe | AC-15 |
| Legacy mirror | Do not import or invoke its `sync()` from local-to-NAS service | AC-18 |

### Required crash matrix

| Failure point | Required durable observation and recovery |
|---|---|
| Manifest lock acquisition or concurrent backfill/upsert | One opaque non-reentrant token serializes the full publication; a competing mutation waits/fails busy and cannot change the baseline; no pointer is committed from a stale fingerprint. |
| Final manifest fingerprint CAS before pointer | A changed generation/bytes/object-set/manifest fingerprint returns `LOCAL_POINTER_MISMATCH`; binding remains inactive and the prior pointer is unchanged. |
| `publish_manifest_only` update/verify | Manifest-only may publish manifest/object changes but never writes binding/pointer; pointer bytes/hash/inode are asserted unchanged. |
| Pre-pointer lock/token/baseline/guard/final-CAS failure | Pointer transaction is not attempted; prior pointer bytes/hash/inode remain unchanged and no degraded post-commit observation is emitted. |
| Post-pointer unlock/close/exact-proof failure | Pointer remains canonical ready; emit `CONTROL_STATE_UNAVAILABLE` degraded observation and durable reconciliation evidence; next guard re-proves/repairs control state, never rolls back pointer. |
| Manifest success → binding write/fsync | No new pointer is committed; the prior pointer remains ready; a failed or orphan binding is inactive and may be quarantined. |
| Binding durable → pointer commit | The exact binding is reused without overwrite; a changed manifest or pointer mismatch blocks commit and leaves the prior pointer unchanged. |
| Canonical pointer commit → checkpoint/journal | Local pointer remains ready; next startup strictly re-observes the same pointer and creates one checkpoint/intent, with no provider request or rollback. |
| Journal temp/install/fsync/import | Journal is retained until intent/event/head commit is durable; repeated import is idempotent; existing identical target is reused, conflicting target is never overwritten, and failed unlink is harmless. |
| Lock acquisition/probe | Return `MOUNT_UNSUPPORTED`; no staging/record/head write. |
| Hidden staging copy/verification | Only hidden partials exist; retry reuses verified objects or isolates garbage; NAS head unchanged. |
| Immutable record install → NAS head commit | Complete record may exist but is unreachable; replay either commits the head or isolates the record. |
| NAS head fsync → sidecar result/cache | NAS head is authoritative; next run reads it and idempotently reconciles SQLite without duplicate copy. |
| NAS head replacement/fsync | Strict reader sees prior or new complete head/record, never a partial record; stale worker cannot advance. |
| Restore before directory rename | No final destination exists; hidden staging is removed or isolated. |
| Restore directory rename | Rename is the sole visibility point; after it only non-semantic directory/inode/manifest readback is permitted. |

## Implementation Steps

### Step 0 — Freeze scope and create RED inventory

1. Confirm exact base with `git rev-parse HEAD` and verify a clean worktree.
2. Add `tests/test_dataset_replication.py` with the named FR/AC/EC tests below, using local
   temporary roots and fake clocks/mounts/copy failures only.
3. Add tests for the current mirror and LaunchAgent assets before touching implementation.
4. Run the focused RED command and record failure names; no `--execute`, provider, NAS or real
   mount is allowed.

### Step 1 — Configuration and layout

1. Add `replication_enabled=false`, `replication_drain_enabled=false`, destination root, outbox basename, journal directory,
   retry/lease/time budgets and a fixed `local_to_nas` direction to `Settings`.
2. Reject non-basename SQLite names and unsafe/relative destination values at configuration
   validation, while keeping status able to report unavailable without creating paths.
3. Add `StorageLayout.replication_sidecar_root` (with the legacy
   `replication_database` name returning that directory), `replication_journal_root` and lock properties;
   `ensure_local_runtime_dirs()` may create local control/journal parents only in writer mode.

### Step 2 — Strict source and destination proof

1. Implement descriptor-bound source checkpoint using the existing strict local dataset/pointer
   reader; observe (do not alter) the committed pointer row, current control DB device/inode/schema,
   canonical manifest hash, active publication binding, `source_object_set_sha256` and complete
   inventory. Create/read the immutable dataset-root `source-instance.json` with path/schema/domain/
   fsync/nonce/dataset identity; DB inode is checkpoint evidence only, so a strict same-root/schema/
   nonce migration may rotate it while an invalid replacement fails closed. Write/read the immutable
   source-commit binding after manifest success and before pointer commit; missing or conflicting
   identity/binding is `OUTBOX_DURABILITY_UNAVAILABLE` and cannot journal.
2. Route every dataset-backed pointer writer through `publish_dataset_and_pointer`: dataset
   `NasMarketStore.save_refresh`, reconcile-control-pointer, CLI reconciliation, `full_history`
   and the internal-only `save_external_publication` path. Plain `MarketStore` without
   dataset/manifest is `SOURCE_NOT_CONFIGURED`; its existing canonical commit is not routed
   through this coordinator and no direct dataset save bypass is permitted.
3. Implement `DestinationArchiveReader` with one trusted root dirfd, `openat`/
   `O_NOFOLLOW`/fstat reads for sentinel/head/record/manifest/object and only pure validators from
   existing NAS code; no `NasMarketStore` path reopen. Add no-follow path traversal, explicit mount
   inspector and trusted destination descriptor.
4. Define destination initialization as a separate acknowledged command that accepts only a new
   empty child; it must not adopt an old archive or overwrite existing files.
5. Add source/binding/destination fingerprint race tests before writing transfer code.

### Step 3 — Outbox DDL, journal and enqueue boundary

1. Implement the one normative sidecar DDL, writer-only initialization and SELECT-only strict reader.
2. Compute `publication_binding_sha256` and checkpoint_id from the complete canonical checkpoint
   without source_sequence; bind each sidecar intent/journal to the active binding plus
   `source_instance_id`/`source_instance_sha256`, and allocate sequences only in one sorted
   `(source_published_at,checkpoint_id)` journal-import transaction.
3. Implement the journal temp `O_EXCL|O_NOFOLLOW`/fsync and no-replace install (macOS
   `renameatx_np(RENAME_EXCL)` or hard-link fallback), exact-target reuse and conflict rejection.
4. Implement the pre-publication durability guard under the existing refresh/control lock, then
   the frozen post-commit `on_canonical_committed(SourceCommit)` seam, checkpoint journal fallback
   and multi-journal crash-safe reconciliation of the strict pointer.
5. Keep enqueue errors out of `RefreshResult.status`; expose an additive sanitized replication
   projection while preserving existing automation/legacy JSON contracts.
6. Add transaction crash tests for binding-before-pointer, pointer-before-enqueue, SQLite commit, journal import and
   double durability failure.

### Step 4 — Transfer, lineage, retry and CAS

1. Implement explicit `local_to_nas` service; do not call `MarketDatasetMirror.sync()`.
2. Acquire both the existing project writer lock (for source proof) and descriptor-bound exclusive
   destination lock, validate trust, stage each object, read back size/schema/rows/hash, build the
   self-contained replication-record.json, fsync, and install a new history directory with
   no-replace semantics before committing complete NAS head.json. Existing exact records are read
   and reused byte-for-byte; conflicts are never overwritten. Append sidecar result/cache only
   after head fsync.
3. Compare checkpoint/source-instance/sequence and parent_record_hash so an older source cannot
   replace a newer destination; conflicting same-partition hashes and unknown/reverse lineage fail closed.
4. Implement lease/state-version CAS, fixed retry schedule, terminal dead-letter and cleanup of
   non-visible partials without deleting published objects.
5. Add injected copy/readback/manifest/CAS/concurrency tests.

### Step 5 — Restore verifier

1. Implement a separate `nas_to_temporary_root` reader/writer boundary with no source mutation.
2. Require a nonexistent final destination or verify an exact existing checkpoint without writes;
   create hidden staging/files with `O_CREAT|O_EXCL|O_NOFOLLOW`, fsync each file/directory/parent,
   write standard sentinel/manifest only after every verification, and install once with a
   same-parent no-replace operation. Never rename over an existing destination.
3. Read only the NAS archive `head.json` and its self-contained `replication-record.json`, then
   verify hashes, schema, row counts, dates, source, manifest identity and representative read-only
   queries without SQLite sidecar or `reconcile_control_pointer()`.
4. Add corrupt-source, symlink-race, existing-destination and post-rename non-semantic readback tests.

### Step 6 — API, CLI and automation wiring

1. Add `GET /api/v1/storage/replication` with 200/503 bounded response and zero-write dependency.
2. Add `market-replicate`, `market-restore`, `market-replication-status` and separately
   acknowledged destination initialization. Default every operation to dry-run.
3. In `main.py` inject the explicit local source reader, pre-publication durability guard, frozen
   commit seam and service only for enabled writer surfaces; disabled status/API/automation
   short-circuits before sidecar/source/destination construction. Invoke the guard before each
   next canonical pointer commit and the typed observation only after commit while the same
   refresh/control lock is held. `replication_enabled` wires local enqueue only; optional refresh-slot
   NAS drain additionally requires the drain flag and approved descriptor, and records destination
   failure separately from local refresh outcome.
4. Keep all output free of paths, credentials, URLs, payload and raw exceptions.

### Step 7 — LaunchAgent and documentation

1. Extend the existing refresh slot only if explicit replication is enabled; do not add a required
   sixth agent or a credentialed mount helper.
2. Verify the TCC-unavailable branch leaves local ready and the queue/journal observable.
3. Update `docs/nas-storage.md` with dry-run/execute commands, status reasons, recovery and the
   separate real-operation window. Do not document a production success not actually executed.

### Step 8 — GREEN, release candidate and review

Run focused and full offline suites, static checks and strict validators. Record actual outputs and
the exact final commit from `git rev-parse HEAD`; use no invented commit or production claim.

```bash
.venv/bin/pytest -q tests/test_dataset_replication.py tests/test_nas_dataset.py \
  tests/test_storage_readiness.py tests/test_market_automation.py \
  tests/test_launchagent_assets.py \
  --basetemp=/tmp/stock-eva-r2f4-3-replication-focused
.venv/bin/pytest -q --basetemp=/tmp/stock-eva-r2f4-3-replication-full
.venv/bin/ruff check backend tests
.venv/bin/ruff format --check backend tests
.venv/bin/python -m compileall -q backend
git diff --check
uv run --offline python /Users/finlay/.codex/skills/claude-skills--engineering--spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-design.md --strict
uv run --offline python /Users/finlay/.codex/skills/claude-skills--engineering--spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-implementation.md --strict
git rev-parse HEAD
git status --porcelain=v1
```

The final implementation commit MUST include only the approved source, test, LaunchAgent and
documentation files. A real NAS copy or restore must not be included in this commit's evidence.

## Out of Scope

- OS-1: Real NAS/SMB copy, restore, mount setup, credential/TCC authorization or production execution.
- OS-2: Provider requests, failover, normalization, quality gates, factor semantics or canonical schema changes.
- OS-3: Replacing or altering `NasMarketStore`, `MarketStore`, `manifest.json`, `published_snapshots`, canonical DDL, canonical Parquet or `MarketDatasetMirror`.
- OS-4: Reverse replication, source deletion, automatic overwrite of newer destination or manual pointer edits.
- OS-5: Cloud storage, authenticated transport, credential storage, encryption provisioning or vendor daemon.
- OS-6: Private user/portfolio backup to NAS; existing local-only backup remains authoritative.
- OS-7: Automatic deletion of orphan destination objects; cleanup/retention requires separate policy.
- OS-8: Public object/universe listing, path diagnostics, payload exposure or restore API execution.
- OS-9: A required new LaunchAgent or any background process that assumes `/Volumes` access.
- OS-10: Cross-volume rename advertised as atomic or copy-plus-delete used as publication.

## Mandatory evidence crosswalk

Each ID maps to one exact test/static anchor. These names are created in Step 0 or already exist;
they must be real and passing before implementation review can close. No grouped range is evidence.

| ID | Exact implementation evidence anchor |
|---|---|
| FR-1 | `tests/test_dataset_replication.py::test_replication_disabled_is_zero_work` |
| FR-1a | `tests/test_dataset_replication.py::test_automation_drain_requires_flag_and_approved_descriptor` |
| FR-2 | `tests/test_dataset_replication.py::test_source_checkpoint_binds_committed_pointer_without_canonical_mutation` |
| FR-2a | `tests/test_dataset_replication.py::test_publication_binding_is_fsynced_before_pointer_and_exactly_matches` |
| FR-3 | `tests/test_dataset_replication.py::test_enqueue_failure_preserves_local_pointer_ready` |
| FR-3a | `tests/test_dataset_replication.py::test_prepublication_guard_blocks_next_pointer_when_current_checkpoint_not_durable` |
| FR-3b | `tests/test_dataset_replication.py::test_all_dataset_pointer_writers_route_through_coordinator` |
| FR-3c | `tests/test_dataset_replication.py::test_publication_is_atomic_under_one_manifest_publication_lock` |
| FR-3d | `tests/test_dataset_replication.py::test_legacy_no_selection_uses_canonical_null_digests` |
| FR-3e | `tests/test_dataset_replication.py::test_manifest_only_operation_never_calls_pointer_wrapper` |
| FR-3f | `tests/test_dataset_replication.py::test_post_pointer_unlock_failure_degrades_without_rollback` |
| FR-3g | `tests/test_dataset_replication.py::test_manifest_only_empty_database_uses_absent_pointer_identity` |
| FR-3h | `tests/test_dataset_replication.py::test_backfill_service_requires_explicit_lineage_input_per_trade_date`; `tests/test_market_backfill.py::test_cli_nas_lineage_parser_preserves_order_and_rejects_duplicate_before_dict` |
| FR-3i | `tests/test_dataset_replication.py::test_manifest_only_invalid_pointer_identity_fails_closed` |
| FR-3j | `tests/test_dataset_replication.py::test_lineage_resolver_returns_union_and_reuses_session_selection_hash_contract` |
| FR-4 | `tests/test_dataset_replication.py::test_replication_sidecar_normative_ddl_identity_and_immutable_event_history` |
| FR-5 | `tests/test_dataset_replication.py::test_checkpoint_journals_import_sorted_by_published_at_and_checkpoint_id` |
| FR-5a | `tests/test_dataset_replication.py::test_journal_install_is_no_replace_and_reuses_identical_target` |
| FR-6 | `tests/test_dataset_replication.py::test_source_checkpoint_rejects_missing_changed_or_corrupt_object` |
| FR-7 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| FR-8 | `tests/test_dataset_replication.py::test_copy_uses_non_visible_staging_namespace` |
| FR-9 | `tests/test_dataset_replication.py::test_destination_readback_checks_size_schema_rows_and_hash` |
| FR-10 | `tests/test_dataset_replication.py::test_nas_record_and_head_are_atomic_last_visibility` |
| FR-11 | `tests/test_dataset_replication.py::test_destination_lineage_rejects_unknown_ahead_conflicting_and_reverse` |
| FR-12 | `tests/test_dataset_replication.py::test_same_checkpoint_replication_is_idempotent` |
| FR-13 | `tests/test_dataset_replication.py::test_replication_direction_is_local_to_nas_only` |
| FR-14 | `tests/test_dataset_replication.py::test_retry_schedule_and_dead_letter_are_bounded` |
| FR-15 | `tests/test_dataset_replication.py::test_concurrent_claim_uses_lease_and_state_version_cas` |
| FR-16 | `tests/test_dataset_replication.py::test_restore_writes_only_new_temporary_root` |
| FR-17 | `tests/test_dataset_replication.py::test_restore_verifies_manifest_objects_and_representative_query` |
| FR-18 | `tests/test_dataset_replication.py::test_replication_status_is_read_only_sanitized_and_bounded` |
| FR-19 | `tests/test_dataset_replication.py::test_market_replicate_defaults_to_zero_write_plan` |
| FR-20 | `tests/test_dataset_replication.py::test_market_restore_defaults_to_zero_write_plan` |
| FR-21 | `tests/test_dataset_replication.py::test_destination_init_requires_new_empty_child_and_ack` |
| FR-22 | `tests/test_launchagent_assets.py::test_refresh_replication_hook_is_optional_and_tcc_safe` |
| FR-23 | `tests/test_dataset_replication.py::test_path_validation_rejects_root_home_env_symlink_and_overlap` |
| FR-24 | `tests/test_dataset_replication.py::test_public_replication_projection_has_no_path_or_secret` |
| FR-25 | `tests/test_dataset_replication.py::test_nas_unavailable_does_not_change_local_ready_or_pointer` |
| FR-26 | `tests/test_dataset_replication.py::test_replication_evidence_replays_without_provider` |
| FR-27 | `tests/test_nas_dataset.py::test_verified_local_mirror_is_idempotent_and_manifest_readable` |
| AC-1 | `tests/test_dataset_replication.py::test_replication_disabled_is_zero_work` |
| AC-2 | `tests/test_dataset_replication.py::test_publication_is_atomic_under_one_manifest_publication_lock` |
| AC-3 | `tests/test_dataset_replication.py::test_pointer_unchanged_after_binding_crash_is_recoverable` |
| AC-4 | `tests/test_dataset_replication.py::test_replication_sidecar_normative_ddl_identity_and_immutable_event_history` |
| AC-5 | `tests/test_dataset_replication.py::test_source_checkpoint_rejects_missing_changed_or_corrupt_object` |
| AC-6 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| AC-7 | `tests/test_dataset_replication.py::test_copy_uses_non_visible_staging_namespace` |
| AC-8 | `tests/test_dataset_replication.py::test_interrupted_copy_keeps_previous_nas_head_and_record` |
| AC-9 | `tests/test_dataset_replication.py::test_same_checkpoint_replication_is_idempotent` |
| AC-10 | `tests/test_dataset_replication.py::test_retry_schedule_and_dead_letter_are_bounded` |
| AC-11 | `tests/test_dataset_replication.py::test_concurrent_claim_uses_lease_and_state_version_cas` |
| AC-12 | `tests/test_dataset_replication.py::test_market_restore_defaults_to_zero_write_plan` |
| AC-13 | `tests/test_dataset_replication.py::test_restore_writes_only_new_temporary_root` |
| AC-14 | `tests/test_dataset_replication.py::test_restore_rename_failure_leaves_destination_absent_or_quarantined` |
| AC-15 | `tests/test_dataset_replication.py::test_replication_status_is_read_only_sanitized_and_bounded` |
| AC-16 | `tests/test_dataset_replication.py::test_market_replicate_defaults_to_zero_write_plan` |
| AC-17 | `tests/test_dataset_replication.py::test_destination_init_requires_new_empty_child_and_ack` |
| AC-18 | `tests/test_nas_dataset.py::test_verified_local_mirror_is_idempotent_and_manifest_readable` |
| AC-19 | `tests/test_launchagent_assets.py::test_refresh_replication_hook_is_optional_and_tcc_safe` |
| AC-20 | `tests/test_dataset_replication.py::test_replication_release_evidence_is_offline_and_exact_head_recorded` |
| AC-21 | `tests/test_dataset_replication.py::test_manifest_fingerprint_drift_blocks_pointer_cas` |
| AC-22 | `tests/test_dataset_replication.py::test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured`; `tests/test_market_backfill.py::test_cli_plain_market_store_backfill_legacy_behavior_is_unchanged` |
| AC-23 | `tests/test_dataset_replication.py::test_manifest_only_upsert_and_backfill_preserve_pointer_bytes_hash_and_inode` |
| AC-24 | `tests/test_dataset_replication.py::test_post_pointer_unlock_failure_degrades_without_rollback` |
| AC-25 | `tests/test_dataset_replication.py::test_manifest_only_empty_database_uses_absent_pointer_identity` |
| AC-26 | `tests/test_dataset_replication.py::test_backfill_service_multi_date_batch_is_atomic_on_lineage_failure`; `tests/test_market_backfill.py::test_cli_nas_lineage_gap_or_extra_is_zero_provider_zero_write` |
| AC-27 | `tests/test_dataset_replication.py::test_manifest_only_invalid_pointer_identity_fails_closed` |
| AC-28 | `tests/test_dataset_replication.py::test_modern_backfill_requires_exact_lineage_or_resolver_evidence`; `tests/test_market_backfill.py::test_cli_nas_valid_lineage_all_dates_admitted_before_first_provider_fetch` |
| EC-1 | `tests/test_dataset_replication.py::test_replication_disabled_is_zero_work` |
| EC-2 | `tests/test_dataset_replication.py::test_path_validation_rejects_root_home_env_symlink_and_overlap` |
| EC-3 | `tests/test_dataset_replication.py::test_path_validation_rejects_root_home_env_symlink_and_overlap` |
| EC-4 | `tests/test_dataset_replication.py::test_source_and_destination_descriptor_races_fail_closed` |
| EC-5 | `tests/test_dataset_replication.py::test_destination_mount_and_tcc_failure_is_sanitized` |
| EC-6 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| EC-7 | `tests/test_dataset_replication.py::test_manifest_extra_duplicate_and_unsafe_entries_fail_closed` |
| EC-8 | `tests/test_dataset_replication.py::test_source_checkpoint_rejects_missing_changed_or_corrupt_object` |
| EC-9 | `tests/test_dataset_replication.py::test_orphan_and_partial_objects_are_not_reader_visible` |
| EC-10 | `tests/test_dataset_replication.py::test_older_or_conflicting_destination_never_overwritten` |
| EC-11 | `tests/test_dataset_replication.py::test_nas_head_cas_race_preserves_current_head` |
| EC-12 | `tests/test_dataset_replication.py::test_interrupted_copy_keeps_previous_nas_head_and_record` |
| EC-13 | `tests/test_dataset_replication.py::test_destination_readback_checks_size_schema_rows_and_hash` |
| EC-14 | `tests/test_dataset_replication.py::test_status_missing_corrupt_or_locked_outbox_is_zero_write` |
| EC-15 | `tests/test_dataset_replication.py::test_enqueue_failure_preserves_local_pointer_ready` |
| EC-16 | `tests/test_dataset_replication.py::test_double_durability_failure_is_observable_and_reconciles` |
| EC-17 | `tests/test_dataset_replication.py::test_checkpoint_journal_import_is_sorted_idempotent_before_removal` |
| EC-18 | `tests/test_dataset_replication.py::test_concurrent_claim_uses_lease_and_state_version_cas` |
| EC-19 | `tests/test_dataset_replication.py::test_retry_schedule_and_dead_letter_are_bounded` |
| EC-20 | `tests/test_dataset_replication.py::test_restore_source_change_leaves_no_final_root` |
| EC-21 | `tests/test_dataset_replication.py::test_restore_rejects_existing_or_unsafe_destination` |
| EC-22 | `tests/test_dataset_replication.py::test_restore_rename_failure_leaves_destination_absent_or_quarantined` |
| EC-23 | `tests/test_nas_dataset.py::test_verified_local_mirror_is_idempotent_and_manifest_readable` |
| EC-24 | `tests/test_dataset_replication.py::test_reverse_replication_and_mount_attempts_are_zero_write` |
| EC-25 | `tests/test_dataset_replication.py::test_manifest_changed_before_pointer_commit_is_rejected` |
| EC-26 | `tests/test_dataset_replication.py::test_legal_db_inode_rotation_with_same_root_nonce_is_accepted` |
| EC-27 | `tests/test_dataset_replication.py::test_manifest_lock_blocks_concurrent_backfill_or_upsert_mutation` |
| EC-28 | `tests/test_dataset_replication.py::test_refresh_run_lock_is_not_publication_atomicity_dependency` |
| EC-29 | `tests/test_dataset_replication.py::test_missing_v2_manifest_metadata_blocks_pointer` |
| EC-30 | `tests/test_dataset_replication.py::test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured`; `tests/test_market_backfill.py::test_cli_plain_market_store_rejects_lineage_input` |
| EC-31 | `tests/test_dataset_replication.py::test_manifest_only_operation_never_calls_pointer_wrapper` |
| EC-32 | `tests/test_dataset_replication.py::test_pointer_precommit_lock_error_preserves_pointer_identity` |
| EC-33 | `tests/test_dataset_replication.py::test_pointer_postcommit_control_error_is_durable_and_next_guard_reconciles` |
| EC-34 | `tests/test_dataset_replication.py::test_manifest_only_present_pointer_identity_is_exact_before_after` |
| EC-35 | `tests/test_dataset_replication.py::test_backfill_service_multi_date_failure_preserves_manifest_and_pointer`; `tests/test_market_backfill.py::test_backfill_service_dataset_preflight_fails_before_audit_or_provider` |
| EC-36 | `tests/test_dataset_replication.py::test_lineage_resolver_rejects_labels_bars_and_unverified_inheritance` |
| EC-37 | `tests/test_dataset_replication.py::test_missing_lineage_reader_returns_source_unavailable_before_mutation` |

## R2-F4.3.2 implementation amendment — single-authority H1-H6/M1-M6 closure

This section is the implementation authority for the companion design amendment. It supersedes
any earlier weaker model, DDL, API serializer or step. It remains a plan only: this worktree must
not perform implementation, NAS access, mount setup, provider requests or production execution.

### H1 — source reader and main wiring

Implement `LocalCanonicalPointerReader` against the explicit
`settings.local_market_dataset_root` only. Never use `nas_market_dataset_root`, a generic root,
current directory or destination fallback. The reader opens the sentinel, `manifest.json` and
the local control pointer row with `O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, fstats before/after, and
returns `SOURCE_UNAVAILABLE`/`LOCAL_POINTER_MISMATCH` on any mutation; it never initializes or
migrates a database.

Do not alter `published_snapshots` or `manifest.json`. After the canonical pointer commit and
exact binding proof, the strict reader creates a replication-only checkpoint containing the
original singleton row values (`run_id`, `trade_date`, `published_at`), current pointer DB
device/inode/schema digest, manifest canonical hash, active binding hash and
`source_object_set_sha256`; the current device/inode/schema tuple is checkpoint tamper evidence,
not stable source identity. An independent immutable private record at
`<local_dataset>/_replication/source-instance.json` is the source-instance authority and is never
canonical. Its exact fields are
`source_instance_schema,schema_version,source_instance_id,canonical_root_path,
canonical_schema_digest,source_instance_domain,fsync_contract,source_instance_nonce,dataset_identity,
created_at,source_instance_sha256`; its path is private and never public. The nonce is created once
under the local dataset root and, with the bound root, canonical schema digest and fixed domain,
defines the immutable dataset identity; no DB inode participates. The file is created only by
explicit writer execution using `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode `0600`, canonical UTF-8
JSON, file+parent fsync and immutable readback. Its `source_instance_sha256` hashes all other
fields under `stock-eva/r2f4.3/source-instance-record/v2`; `dataset_identity` hashes
`{canonical_root_path,canonical_schema_digest,source_instance_domain,source_instance_nonce}` under
`stock-eva/r2f4.3/dataset-identity/v1`; `source_instance_id` hashes `{dataset_identity}` under
`stock-eva/r2f4.3/source-instance/v3`.

The file's canonical closed projection is:

```json
{"source_instance_schema":"stock-eva/r2f4.3/source-instance/v2","schema_version":2,"source_instance_id":"<64-hex>","canonical_root_path":"/private/local-canonical","canonical_schema_digest":"<64-hex>","source_instance_domain":"stock-eva/r2f4.3/local-canonical","fsync_contract":"file_and_parent_directory","source_instance_nonce":"<64-hex>","dataset_identity":"<64-hex>","created_at":"2026-09-09T08:00:00Z","source_instance_sha256":"<64-hex>"}
```

Values are type examples only; the actual file is sorted compact UTF-8 with one newline and no
unknown/duplicate fields. The canonical root path is private control data and never public.
The sidecar and every journal bind both `source_instance_id` and `source_instance_sha256`;
missing, conflicting, unreadable or non-fsynced source-instance state is
`OUTBOX_DURABILITY_UNAVAILABLE` and MUST NOT create a journal. A DB inode rotation is legal only
when the same root-bound immutable nonce is present and the new descriptor passes exact schema,
singleton, ready-run, manifest and pointer proof; the new device/inode is recorded in that
checkpoint. Any replacement that fails those checks is `LOCAL_POINTER_MISMATCH` and is rejected.
The checkpoint computes a complete `checkpoint_id` before sequence allocation;
its closed preimage includes pointer and the exact `published_snapshots ps JOIN refresh_runs rr ON
rr.run_id=ps.run_id` proof (`ps.singleton=1`, `rr.status='ready'`, equal run IDs,
`rr.requested_date=ps.trade_date`, `rr.completed_at=ps.published_at` in canonical UTC-Z),
manifest/object hashes and the sorted complete inventory, and never includes `source_sequence`.
It contains no payload rows.

Source proof runs under the existing project refresh/control writer lock: open the canonical DuckDB
with `O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, fstat path and fd, verify device/inode, copy bytes through
the fd to a private mode-0600 temporary immutable clone, fsync clone/parent, re-fstat source
size/mtime/device/inode, and fail closed on drift. Open DuckDB only `read_only=True` on the clone
and verify the exact `published_snapshots ps JOIN refresh_runs rr ON rr.run_id=ps.run_id` proof:
`ps.singleton=1`, `rr.status='ready'`, equal `run_id`, `rr.requested_date=ps.trade_date`, and
`rr.completed_at=ps.published_at` in canonical UTC-Z form. Manifest inventory/hash is checked
separately; no manifest inventory field is assumed in `refresh_runs`. Always clean or isolate the
private clone in `finally`; never expose its path.

After manifest success and before the pointer transaction, the canonical owner writes the immutable
publication binding at `<local_dataset>/_replication/source-commits/<run_id>.json`. Its closed
fields are
`binding_schema,schema_version,run_id,trade_date,published_at,manifest_generation,
manifest_bytes_sha256,manifest_canonical_sha256,source_object_set_sha256,selection_sha256,
lineage_sha256,binding_sha256`, with `published_at=result.completed_at`. `source_object_set_sha256`
is the sole source object-set field; `object_set_sha256` is not a second source field. The publisher creates it
with `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode `0600`, canonical JSON, fsyncs the file and every
parent directory, then reads it back through the bound descriptor. A write/fsync/readback failure
returns `OUTBOX_DURABILITY_UNAVAILABLE` before any pointer commit and leaves the prior ready pointer
untouched. The artifact is additive and never changes canonical manifest/pointer schema; the private
`_replication` namespace is excluded from manifest inventory and legacy dataset readers.

After pointer commit, `SourceCommit` is active only if a descriptor-bound read proves exact equality
of the binding's run/date/completion, generation, manifest byte/canonical hashes,
`source_object_set_sha256`, `selection_sha256` and `lineage_sha256` with the committed pointer and
manifest. Orphan, changed-manifest or pointer-unchanged bindings remain inert. The exact tests are
`test_publication_binding_is_fsynced_before_pointer_and_exactly_matches`,
`test_binding_write_failure_blocks_pointer_without_rollback`,
`test_manifest_changed_before_pointer_commit_is_rejected`,
`test_binding_active_only_when_pointer_exact_match`,
`test_pointer_unchanged_after_binding_crash_is_recoverable`, and
`test_publication_binding_target_existing_identical_is_reused`.

Implement one central `publish_dataset_and_pointer` operation under the wrapper-owned
`ManifestPublicationLock`. It owns binding creation/readback, `before_canonical_pointer_commit`,
the one canonical pointer commit, exact active-binding proof and the typed post-commit
observation. Route dataset-backed `NasMarketStore.save_refresh`,
`reconcile_control_pointer`, dataset `backfill`, `upsert`, refresh, CLI reconciliation,
`full_history` publication and every legacy `save_external_publication` caller through this seam.
The legacy method may be called only inside the dataset seam. Plain `MarketStore` canonical
publication remains unchanged: it performs no binding/guard/NAS work and its replication
observation is `SOURCE_NOT_CONFIGURED` without explicit dataset/manifest. Required routing tests
are `test_all_dataset_pointer_writers_route_through_coordinator`,
`test_publication_is_atomic_under_one_manifest_publication_lock`,
`test_manifest_fingerprint_drift_blocks_pointer_cas`, and
`test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured`.

When the sidecar is unavailable, write `<checkpoint_id>.json` containing the full checkpoint
projection and no sequence. Recovery opens every journal no-follow, validates hashes, sorts by
`(source_published_at,checkpoint_id)`, and in one private in-memory transaction allocates source
sequences and creates intent, genesis event and head rows. The complete image is then serialized
through the descriptor-native engine and installed only after baseline CAS, file fsync and parent
fsync; only then may each journal be archived/deleted. Crashes or unlink failures leave
re-importable files.
Checkpoint and `(direction,destination_id,checkpoint_id)` uniqueness make repeat import/restart
and retry reuse one sequence and one intent regardless of operation_day. The intent contains this
checkpoint and can copy only the current visible generation.

The journal JSON is closed and contains exactly
`journal_schema_version,checkpoint_id,source_instance_id,source_instance_sha256,source_published_at,checkpoint_projection,
publication_binding_sha256,
checkpoint_payload_sha256,created_at,journal_sha256`. The projection is the complete checkpoint
field set used by `checkpoint_id`; it has no source sequence, intent id, operation day, payload or
credentials. Canonical JSON, one newline and the journal domain hash over
`{journal_schema_version,checkpoint_id,source_instance_id,source_instance_sha256,publication_binding_sha256,checkpoint_payload_sha256,
source_published_at,created_at}` are mandatory. Unknown/duplicate fields, filename mismatch or any
digest mismatch blocks import. Create a unique temporary sibling with
`O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode `0600`, write and `fsync(fd)`, fsync the journal
directory, then install with macOS `renameatx_np(RENAME_EXCL)` or a portable hard-link-no-replace
fallback followed by temporary unlink and parent fsync. If the target already exists, open it
no-follow and reuse it only when bytes are exactly identical; a differing target returns
`OUTBOX_DURABILITY_UNAVAILABLE` and is never overwritten.

Before every subsequent canonical pointer commit, implement
`before_canonical_pointer_commit(NextCanonicalPointer) -> CanonicalDurabilityGuardResult` inside
the same refresh/control writer lock. The guard reads the current singleton and proves its
checkpoint is accepted in the sidecar or has a complete fsynced descriptor-bound journal. If not,
it reconstructs the current visible checkpoint and durably journals/imports it; provider, Parquet
rewrite and canonical writes are forbidden. Failure returns `OUTBOX_DURABILITY_UNAVAILABLE` and
blocks only the next pointer commit; it never rolls back or changes the current ready pointer.
With no current singleton, the first publication is allowed. Tests MUST cover first/no-current,
already accepted, journal-only import, missing/invalid sidecar, crash during guard, crash after
guard before the next commit, and crash after pointer commit before observation:
`test_prepublication_guard_blocks_next_pointer_when_current_checkpoint_not_durable`,
`test_prepublication_guard_imports_current_fsynced_journal_before_next_pointer`,
`test_prepublication_guard_first_publication_has_no_prior_singleton`,
`test_prepublication_guard_crash_after_pointer_commit_recovers_current`,
`test_post_commit_observation_is_after_pointer_and_cannot_replace_guard`, and
`test_guard_and_post_commit_share_refresh_control_lock`.

Implement the frozen typed seam
`on_canonical_committed(SourceCommit) -> ReplicationObservation`. The canonical publication owner
calls it only after dataset-backed manifest/control-pointer commit or plain `MarketStore` DuckDB
commit; the replication service owns checkpoint/journal/outbox durability, catches every exception,
emits bounded effects/reason and cannot alter `RefreshResult` or roll back canonical state. The
seam is not an existing generic publication callback. Plain `MarketStore` or no explicit local
dataset root returns `SOURCE_NOT_CONFIGURED` and never falls back to NAS. CLI `--execute` acquires
the same project writer lock before source proof or writes; an external writer that bypasses it is
out of scope.
Add the reader, durability guard and service as explicit dependencies in `backend/app/main.py`;
construction errors
are blocked/unavailable and never silently replaced by a NAS reader. Add the four H1 tests named in
the design amendment before implementation:
`tests/test_dataset_replication.py::test_local_pointer_reader_binds_row_hash_generation_manifest_and_object_set`,
`tests/test_dataset_replication.py::test_source_pointer_mismatch_is_fail_closed`,
`tests/test_dataset_replication.py::test_replication_copies_only_current_visible_generation`, and
`tests/test_dataset_replication.py::test_main_wires_explicit_local_pointer_reader_without_nas_fallback`.
Also add `test_duckdb_source_clone_is_descriptor_bound_and_drift_fails_closed`,
`test_duckdb_clone_requires_ready_refresh_run_and_pointer_manifest_inventory`,
`test_source_proof_uses_published_snapshot_refresh_run_join_and_separate_manifest_inventory`,
`test_source_proof_rejects_refresh_run_status_date_or_completed_at_mismatch`,
`test_checkpoint_journals_import_sorted_by_published_at_and_checkpoint_id`,
`test_multiple_checkpoint_journals_crash_and_recover_exactly_once`, and
`test_cli_execute_holds_existing_project_writer_lock`,
`test_source_instance_record_is_immutable_and_fsynced`,
`test_source_instance_record_missing_or_conflicting_blocks_journal`, and
`test_source_instance_identity_uses_dataset_root_nonce_not_db_inode`,
`test_legal_db_inode_rotation_with_same_root_nonce_is_accepted`, and
`test_illegal_db_replacement_fails_closed`.

Use these exact frozen/sealed seam models; no generic callback or open-ended dictionary is allowed:

```typescript
type SourcePublicationBinding = Readonly<{
  binding_schema: "stock-eva/r2f4.3/source-publication-binding/v1";
  schema_version: 1; run_id: string; trade_date: string; published_at: string;
  manifest_generation: string; manifest_bytes_sha256: string;
  manifest_canonical_sha256: string; source_object_set_sha256: string;
  selection_sha256: string; lineage_sha256: string; binding_sha256: string;
}>;
type SourceCommit = Readonly<{
  source_commit_schema: "stock-eva/r2f4.3/source-commit/v1";
  pointer_row_sha256: string; pointer_generation: string; source_run_id: string;
  source_trade_date: string; source_published_at: string;
  pointer_db_device: number; pointer_db_inode: number; pointer_db_schema_digest: string;
  manifest_canonical_sha256: string; source_manifest_bytes_sha256: string;
  source_object_set_sha256: string; object_inventory: ReadonlyArray<ObjectInventoryItem>;
  publication_binding_sha256: string; publication_binding_manifest_generation: string;
  publication_binding_selection_sha256: string; publication_binding_lineage_sha256: string;
  source_instance_id: string; source_instance_sha256: string;
  committed_at: string; source_commit_sha256: string;
}>;
type ReplicationObservation = Readonly<{
  observation_schema: "stock-eva/r2f4.3/replication-observation/v1";
  source_commit_sha256: string; checkpoint_id: string; source_instance_id: string;
  source_sequence: number | null; intent_id: string | null; enqueue_state: string;
  reason_code: ReplicationReason; effects: Effects; observed_at: string;
  observation_sha256: string;
}>;
```

Hash each value object over every field except its own hash using its named domain and the common
canonical JSON encoder. The canonical publisher seals `SourceCommit` after pointer commit; the
replication service owns and seals `ReplicationObservation`. The source publisher invokes the seam
after pointer commit and before releasing the refresh/control lock; the service can only read
source evidence and append downstream evidence, never mutate `RefreshResult` or canonical state.
Neither model carries a path, credential, provider payload or mutable handle. Add
`test_source_commit_and_replication_observation_are_frozen_sealed_and_hash_bound`,
`test_post_commit_observation_is_after_pointer_and_cannot_replace_guard`, and
`test_main_wires_guard_and_typed_commit_seam_inside_refresh_lock`.
The dataset-backed publisher invokes the observer only after its local manifest/control-pointer
commit; the plain `MarketStore` invokes it only after its committed DuckDB publication transaction.
Neither callback runs pre-commit or mutates canonical rows/files.

### H2 — sidecar implementation and strict read-only reader

Replace the earlier single mutable outbox table with the exact sidecar schema below: immutable
`replication_sidecar_meta`, immutable `replication_intents`, immutable append-only
`replication_attempt_events`, plus mutable CAS-only `replication_heads` and the optional
non-authoritative `replication_destination_cache`. NAS `_replication/history` records and
`_replication/head.json` are the sole destination authority. `schema_identity`,
`schema_version`, `ddl_sha256` and `schema_digest` must be checked on every writer and reader.
The meta `source_instance_id` and `source_instance_sha256` MUST match the immutable private
`source-instance.json` record and the descriptor-bound canonical source; missing or conflicting
identity is `OUTBOX_DURABILITY_UNAVAILABLE` and MUST prevent journal/intent creation.
Each event sequence starts at zero, has a previous-event hash, and is replayed globally: every
intent has one head, every event references an intent, sequences are contiguous, and event hashes
and head state agree. The optional destination cache is never authority. Orphan, gap, broken-chain,
duplicate or invalid destination evidence is unavailable/fail-closed.
The strict destination reader must resolve each head's `(destination_id,replication_generation)` to
open the NAS head, the referenced self-contained record, manifest and every object with
descriptor-bound no-follow descriptors. It recomputes all record/head preimages and matches
descriptor, parent, source sequence/checkpoint, manifest and object hashes. Missing/extra fields,
broken lineage, incomplete generations or a head hash that does not recompute from its closed
fields are unavailable and are never repaired by status; SQLite cache is never authority.

Implement `DestinationArchiveReader` as the only destination read contract:

```text
read_sentinel(root_dirfd) -> SentinelProjection
read_head(root_dirfd) -> DestinationHead
read_record(root_dirfd, replication_generation) -> ReplicationRecord
read_manifest(generation_dirfd) -> ManifestBytes
read_object(generation_dirfd, relative_path) -> VerifiedObjectBytes
```

Open the root with `O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW`; every child is opened with
`openat` and `O_NOFOLLOW`, fstat before/after, and checked against parent device/inode and the
approved descriptor. Absolute paths, `..`, symlinks, extra entries and inode drift fail closed.
Do not call `NasMarketStore` to reopen any path; reuse only pure parsing/schema/Parquet validators
after the bound fd has supplied bytes. Add
`tests/test_dataset_replication.py::test_destination_archive_reader_uses_root_dirfd_openat_no_path_reopen`,
`tests/test_dataset_replication.py::test_destination_archive_reader_rejects_symlink_or_inode_drift`, and
`tests/test_dataset_replication.py::test_destination_archive_reader_reuses_only_pure_manifest_and_parquet_validation`.

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA user_version = 1;

CREATE TABLE replication_sidecar_meta (
    sidecar_id INTEGER PRIMARY KEY NOT NULL CHECK (sidecar_id = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    schema_identity TEXT NOT NULL CHECK (schema_identity = 'stock-eva/r2f4.3/replication-sidecar/v1'),
    ddl_sha256 TEXT NOT NULL CHECK (length(ddl_sha256) = 64),
    schema_digest TEXT NOT NULL CHECK (length(schema_digest) = 64),
    source_instance_id TEXT NOT NULL CHECK (length(source_instance_id) = 64),
    source_instance_sha256 TEXT NOT NULL CHECK (length(source_instance_sha256) = 64),
    created_at TEXT NOT NULL,
    generation_number INTEGER NOT NULL CHECK (generation_number >= 0),
    previous_generation_sha256 TEXT NOT NULL CHECK (length(previous_generation_sha256) = 64),
    generation_payload_sha256 TEXT NOT NULL CHECK (length(generation_payload_sha256) = 64)
) STRICT;

CREATE TABLE replication_intents (
    intent_id TEXT PRIMARY KEY NOT NULL CHECK (length(intent_id) = 64),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    operation_day TEXT NOT NULL CHECK (operation_day GLOB '????-??-??'),
    direction TEXT NOT NULL CHECK (direction = 'local_to_nas'),
    destination_id TEXT NOT NULL CHECK (length(destination_id) = 32),
    pointer_row_sha256 TEXT NOT NULL CHECK (length(pointer_row_sha256) = 64),
    pointer_generation TEXT NOT NULL CHECK (length(pointer_generation) BETWEEN 1 AND 128),
    source_run_id TEXT NOT NULL CHECK (length(source_run_id) BETWEEN 1 AND 128),
    source_trade_date TEXT NOT NULL CHECK (source_trade_date GLOB '????-??-??'),
    source_published_at TEXT NOT NULL,
    pointer_db_device INTEGER NOT NULL CHECK (pointer_db_device > 0),
    pointer_db_inode INTEGER NOT NULL CHECK (pointer_db_inode > 0),
    pointer_db_schema_digest TEXT NOT NULL CHECK (length(pointer_db_schema_digest) = 64),
    manifest_canonical_sha256 TEXT NOT NULL CHECK (length(manifest_canonical_sha256) = 64),
    source_object_set_sha256 TEXT NOT NULL CHECK (length(source_object_set_sha256) = 64),
    publication_binding_sha256 TEXT NOT NULL CHECK (length(publication_binding_sha256) = 64),
    source_instance_id TEXT NOT NULL CHECK (length(source_instance_id) = 64),
    source_instance_sha256 TEXT NOT NULL CHECK (length(source_instance_sha256) = 64),
    source_sequence INTEGER NOT NULL CHECK (source_sequence >= 1),
    checkpoint_id TEXT NOT NULL CHECK (length(checkpoint_id) = 64),
    source_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(source_manifest_bytes_sha256) = 64),
    plan_sha256 TEXT NOT NULL CHECK (length(plan_sha256) = 64),
    intent_sha256 TEXT NOT NULL CHECK (length(intent_sha256) = 64),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    created_at TEXT NOT NULL,
    UNIQUE (direction, destination_id, checkpoint_id),
    UNIQUE (direction, destination_id, source_instance_id, source_sequence),
    UNIQUE (source_instance_id, source_sequence)
) STRICT;

CREATE TABLE replication_destination_cache (
    destination_id TEXT PRIMARY KEY NOT NULL CHECK (length(destination_id) = 32),
    descriptor_sha256 TEXT NOT NULL CHECK (length(descriptor_sha256) = 64),
    head_sha256 TEXT CHECK (head_sha256 IS NULL OR length(head_sha256) = 64),
    replication_generation TEXT CHECK (replication_generation IS NULL OR length(replication_generation) = 64),
    record_sha256 TEXT CHECK (record_sha256 IS NULL OR length(record_sha256) = 64),
    source_instance_id TEXT CHECK (source_instance_id IS NULL OR length(source_instance_id) = 64),
    source_sequence INTEGER CHECK (source_sequence IS NULL OR source_sequence >= 1),
    health_state TEXT NOT NULL CHECK (health_state IN ('unknown','healthy','unavailable','unsupported')),
    health_observed_at TEXT,
    cache_version INTEGER NOT NULL CHECK (cache_version >= 0),
    updated_at TEXT NOT NULL,
    CHECK ((health_state = 'unknown' AND health_observed_at IS NULL)
        OR (health_state <> 'unknown' AND health_observed_at IS NOT NULL))
) STRICT;

CREATE TABLE replication_attempt_events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK (length(event_id) = 64),
    intent_id TEXT NOT NULL REFERENCES replication_intents(intent_id),
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
    prev_event_sha256 TEXT NOT NULL CHECK (length(prev_event_sha256) = 64),
    event_type TEXT NOT NULL CHECK (event_type IN ('intent_created','claim','transition','attempt','terminal')),
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (to_state IN ('pending','copying','verifying','retry_wait','replicated','dead_letter')),
    attempt INTEGER NOT NULL CHECK (attempt >= 0 AND attempt <= 6),
    reason_code TEXT NOT NULL,
    state_version INTEGER NOT NULL CHECK (state_version >= 0),
    occurred_at TEXT NOT NULL,
    destination_replication_generation TEXT CHECK (destination_replication_generation IS NULL OR length(destination_replication_generation) = 64),
    destination_record_sha256 TEXT CHECK (destination_record_sha256 IS NULL OR length(destination_record_sha256) = 64),
    destination_head_sha256 TEXT CHECK (destination_head_sha256 IS NULL OR length(destination_head_sha256) = 64),
    event_sha256 TEXT NOT NULL CHECK (length(event_sha256) = 64),
    UNIQUE (intent_id, event_sequence)
) STRICT;

CREATE TABLE replication_heads (
    intent_id TEXT PRIMARY KEY NOT NULL REFERENCES replication_intents(intent_id),
    current_state TEXT NOT NULL CHECK (current_state IN ('pending','copying','verifying','retry_wait','replicated','dead_letter')),
    state_version INTEGER NOT NULL CHECK (state_version >= 0),
    last_event_sequence INTEGER NOT NULL CHECK (last_event_sequence >= 0),
    lease_owner TEXT,
    lease_until TEXT,
    next_attempt_at TEXT NOT NULL,
    last_reason_code TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((current_state IN ('copying','verifying') AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR current_state NOT IN ('copying','verifying'))
) STRICT;

CREATE INDEX replication_heads_due_idx ON replication_heads (current_state, next_attempt_at);
CREATE INDEX replication_attempt_events_intent_idx
    ON replication_attempt_events (intent_id, event_sequence);
CREATE INDEX replication_intents_checkpoint_idx
    ON replication_intents (source_instance_id, checkpoint_id);
CREATE INDEX replication_destination_cache_health_idx
    ON replication_destination_cache (health_state, updated_at);

CREATE TRIGGER replication_sidecar_meta_no_update
BEFORE UPDATE ON replication_sidecar_meta
WHEN OLD.generation_payload_sha256 <> '0000000000000000000000000000000000000000000000000000000000000000'
BEGIN
    SELECT RAISE(ABORT, 'replication schema identity is immutable');
END;
CREATE TRIGGER replication_sidecar_meta_no_delete
BEFORE DELETE ON replication_sidecar_meta BEGIN
    SELECT RAISE(ABORT, 'replication schema identity is immutable');
END;
CREATE TRIGGER replication_intents_no_update
BEFORE UPDATE ON replication_intents BEGIN
    SELECT RAISE(ABORT, 'replication intent is immutable');
END;
CREATE TRIGGER replication_intents_no_delete
BEFORE DELETE ON replication_intents BEGIN
    SELECT RAISE(ABORT, 'replication intent is immutable');
END;
CREATE TRIGGER replication_attempt_events_no_update
BEFORE UPDATE ON replication_attempt_events BEGIN
    SELECT RAISE(ABORT, 'replication event is immutable');
END;
CREATE TRIGGER replication_attempt_events_no_delete
BEFORE DELETE ON replication_attempt_events BEGIN
    SELECT RAISE(ABORT, 'replication event is immutable');
END;
CREATE TRIGGER replication_heads_no_delete
BEFORE DELETE ON replication_heads BEGIN
    SELECT RAISE(ABORT, 'replication head is a derived audit projection');
END;
CREATE TRIGGER replication_heads_monotonic_cas
BEFORE UPDATE ON replication_heads
WHEN NEW.state_version <> OLD.state_version + 1
BEGIN
    SELECT RAISE(ABORT, 'replication head requires state-version CAS');
END;
```

#### Batch1.3 descriptor-native SQLite amendment (normative)

This amendment supersedes every earlier Batch1 sentence that permits a sidecar pathname
`sqlite3.connect(path)`, a SQLite WAL/SHM sidecar, a temporary filesystem clone, or a read-only
SQLite URI as the status/writer engine. The normative DDL above is unchanged, but all sidecar
read and write operations MUST use the same descriptor-native snapshot engine. Under the
approved local sidecar lock, it opens the explicit absolute parent through a trusted dirfd and
`O_NOFOLLOW`, then opens the fixed basename through that dirfd. It reads the main database bytes
and records a full descriptor fingerprint `(st_dev, st_ino, st_size, st_mtime_ns, sha256)` before
and after the entire SQLite query/transaction window. Any existing `replication.sqlite3-wal` or
`replication.sqlite3-shm` is `CONTROL_STATE_UNAVAILABLE` and MUST cause zero writes; this batch
does not merge WAL frames.

The engine MUST deserialize the stable main bytes into `sqlite3.connect(':memory:')` only. The
writer runs DDL/transactions in that private memory database, serializes the complete image,
and installs it under the already-open parent dirfd using `O_EXCL|O_NOFOLLOW` temporary bytes,
file fsync, baseline fingerprint CAS, no-replace/atomic install and parent-directory fsync.
It MUST never reopen the checked pathname, create or read WAL/SHM, or write an attacker-selected
replacement. A status read is SELECT-only against the memory image; it MUST close the in-memory
connection in every success and failure path, and MUST compare main bytes/hash and full
fingerprint before returning. Temporary cleanup is independently guarded; every descriptor is
closed, and cleanup failure returns a typed durability error after best-effort cleanup.

WAL/FULL remain properties of the normative schema contract where applicable, but no runtime
sidecar connection may materialize WAL/SHM in this batch. Tests MUST assert no clone/temp/WAL/SHM
creation, `sqlite3.connect` receives only `':memory:'`, replacement/same-stat byte races fail
closed, all descriptors close, and sidecar bytes/inodes/mtimes remain unchanged for status and
failed writes. The exact anchors are
`test_status_rejects_existing_wal_shm_without_writes`,
`test_sidecar_sqlite_engine_uses_memory_only`,
`test_sqlite_deserialize_failure_closes_private_connection`,
`test_status_same_stat_byte_mutation_is_unavailable`, and
`test_sidecar_writer_cleanup_failure_is_typed_and_closes_descriptors`, and
`test_memory_writer_cleanup_failure_is_typed_and_closes_descriptors`.

#### Batch1.4 sidecar writer-lock amendment (normative)

Every sidecar writer operation MUST acquire the fixed lock file
`<sidecar-basename>.lock` in the sidecar's explicit parent directory through the already
trusted parent dirfd, with `O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW`, mode `0600`, a regular-file
check, and an OS advisory exclusive lock. The lock descriptor identity is captured in an opaque
writer token. `initialize`, `_connect_writer`, every future sidecar mutation and every sidecar
CAS/install helper MUST carry that token; a missing, released, changed or non-owned token is a
typed `ReplicationDurabilityError` and the helper MUST perform no mutation. The same lock is
held continuously from auxiliary-file and main baseline capture through the private in-memory
transaction, temporary write/fsync, baseline fingerprint CAS, no-replace/atomic installation,
final readback and parent-directory fsync. An existing empty database is still a CAS baseline;
it MUST NOT be silently overwritten by a second writer. Cooperating concurrent writers therefore
have one winner, while the later writer re-reads the new baseline and either performs an exact
idempotent initialization or returns a typed identity/CAS conflict. The lock serializes the
cooperating A-B-A case; full `(st_dev, st_ino, st_size, st_mtime_ns, sha256)` comparisons remain
mandatory for uncooperative replacement or byte mutation.

All held descriptors MUST use explicit offset-independent `pread` (or an equivalent seek-to-zero
proof) for every repeated read. `_connect_writer` MUST close its private memory connection for
every `ReplicationDurabilityError`, SQLite error or other failure after opening it. Installation
cleanup MUST use independent `finally` paths: close every fd even when temporary unlink fails,
return a typed cleanup error, and leave only an explicitly observable residue for later safe
cleanup. Status remains writer-lock-free and zero-write, but retains the complete before/after
fingerprint proof and rejects any WAL/SHM appearance. Required anchors are
`test_sidecar_writer_lock_serializes_descriptor_sessions`,
`test_sidecar_cas_helper_requires_writer_lock_token`,
`test_repeated_initialize_reads_held_descriptor_from_offset_zero`,
`test_existing_empty_sidecar_concurrent_initializers_have_one_winner`, and
`test_connect_writer_closes_memory_connection_on_durability_error`, in addition to the Batch1.3
anchors above.

#### Batch1.5 lock-authority and auxiliary-state amendment (normative)

The sidecar writer token MUST retain the trusted parent dirfd identity and the baseline lock-entry
identity `(st_dev, st_ino, st_nlink, mode)`. At baseline capture, immediately before installation,
immediately after the atomic install and before final readback, and after final parent-directory
fsync, the writer MUST use that dirfd plus `O_NOFOLLOW` to reopen the fixed lock basename and prove
that it still names the held descriptor with the same device, inode, link count and mode; the held
lock fd and parent fd identities MUST also still match. Any replacement, symlink, link-count/mode
change or ABA authority change is a typed fail-closed error. No sidecar main-file write may occur
after a pre-install authority failure. This verification protects the baseline CAS even if an
uncooperating process installs a new lock basename.

The writer session MUST retain the main absent/present payload and complete fingerprint plus
explicit `WAL=ABSENT` and `SHM=ABSENT` baseline states. It MUST re-prove those auxiliary states
before installation, after installation and before final readback, and after parent fsync. Any
pre-install appearance or change is `CONTROL_STATE_UNAVAILABLE` with zero main publication. An
appearance after atomic install is a degraded durable result: the installed bytes are preserved,
the operation fails closed for subsequent use, and no overwrite or rollback attempt is allowed.
Status remains zero-write and writer-lock-free, but performs the same complete main and auxiliary
before/after proof.

All cleanup is a best-effort accumulator. Temporary unlink, temporary-fd close, target-fd close,
each trusted directory-fd close, lock unlock and lock-fd close MUST each be attempted independently.
Without a primary operation error, any cleanup failure raises a sanitized typed
`ReplicationDurabilityError`; with a primary error, the primary is preserved and receives only a
sanitized cleanup note (never a path or raw OS exception). The implementation MUST NOT claim that
an OS-level close succeeded when it failed. Required anchors are
`test_sidecar_writer_lock_entry_replacement_is_fail_closed`,
`test_sidecar_auxiliary_appearance_before_install_is_zero_write`,
`test_sidecar_auxiliary_appearance_after_install_is_degraded_and_not_overwritten`,
`test_sidecar_writer_cleanup_reports_unlock_and_fd_failures_after_attempts`, and
`test_sidecar_concurrent_initializers_have_one_multiprocess_winner`.

#### Batch1.6 immutable sidecar generations amendment (normative; supersedes prior sidecar writer text)

The earlier mutable `replication.sqlite3` pathname is a retired, non-compatible prototype; this
Batch1 has no migration or compatibility write path for it. The sidecar authority is the explicit
absolute `<local_control_dir>/replication-sidecar/` directory, reached only through a trusted
descriptor chain. Its allowlisted namespace is exactly `.writer.lock`, `genesis.json`, the optional
fixed `.staging` directory, and zero-padded generation files `00000000000000000000.db` through
`99999999999999999999.db`; staging temporary names exist only during a locked write and must be
removed before the operation is successful unless they are a strictly proven committed hardlink
residue. Any unknown name, WAL/SHM-like name, gap, duplicate, symlink,
wrong type or unreadable entry makes the sidecar unavailable. The canonical market schema remains
unchanged; the sidecar DDL is extended only by the Batch1.7 generation-metadata amendment below.

`genesis.json` is an immutable canonical record whose closed fields are the sidecar schema/version,
source instance id and digest, lock device/inode/link-count/mode, creation timestamp and its
domain-separated digest. The first locked initializer creates the fixed lock file and genesis with
no-replace installation, then creates generation `00000000000000000000.db`; subsequent writers
must prove the current lock entry still matches genesis before any write. A replacement or ABA
lock inode can never become a new authority: normal writers fail closed with
`OUTBOX_DURABILITY_UNAVAILABLE`.

Every mutation runs under that anchored lock, scans and validates the complete contiguous
generation chain, and computes the deterministic next sequence. It serializes the private
`:memory:` SQLite image, writes a deterministic `O_EXCL|O_NOFOLLOW` temporary file through the
held `.staging` dirfd, fsyncs it, then installs only the fixed next basename using no-replace hard-link/rename-excl
semantics and fsyncs the root directory. `EEXIST` is a typed CAS conflict; no generation is ever
replaced, truncated or modified. The lock and baseline are re-proven before install, after install
before readback, and after final fsync. A namespace anomaly before install causes zero generation
publication; one appearing after install leaves the immutable generation visible but returns a
degraded durable failure and never overwrites it.

Status never creates, locks, migrates or writes the root. It scans the exact namespace, validates
genesis and the contiguous chain, reads the highest generation through `O_NOFOLLOW`, deserializes
it in memory and re-scans stable generation/lock/main fingerprints before returning. Generation
files are immutable evidence. All cleanup uses the Batch1.5 best-effort accumulator, including
temporary unlink and every fd/lock close; cleanup errors are typed and sanitized. Required anchors
are `test_sidecar_generation_namespace_rejects_mutable_or_unknown_entries`,
`test_sidecar_generation_gap_or_symlink_is_unavailable_without_writes`,
`test_sidecar_generation_status_is_zero_write_and_reads_highest_stable_generation`,
`test_sidecar_generation_lock_replacement_is_fail_closed_before_publish`,
`test_sidecar_generation_concurrent_initializers_have_one_multiprocess_winner`,
`test_sidecar_generation_auxiliary_appearance_is_degraded_after_install`, and
`test_sidecar_generation_repeat_initialize_is_idempotent`.

The event closed field set is exactly `(event_id,intent_id,event_sequence,prev_event_sha256,
event_type,from_state,to_state,attempt,reason_code,state_version,occurred_at,
destination_replication_generation,destination_record_sha256,destination_head_sha256,event_sha256)`;
exclude only `event_sha256` from its preimage. Derive `event_id` from intent, sequence, attempt,
state version and timestamp. Event zero is `from_state=NULL`, `to_state=pending` and the 64-zero
previous hash. Replay requires contiguous unique sequences, valid state transitions, and final
event state/version equal to the mutable head. Keep the event, intent and cache indexes in the DDL. The
10,000-terminal-row benchmark performs indexed SELECTs only and asserts p95 `<500 ms`, unchanged
sidecar bytes/inode and zero Parquet hash scan in
`tests/test_dataset_replication.py::test_status_10k_terminal_rows_under_500ms_without_parquet_scan`.
The DDL identity, immutable triggers and SQLite execution are covered by
`tests/test_dataset_replication.py::test_replication_sidecar_normative_ddl_identity_and_immutable_event_history`.

### H3 — destination descriptor and initialization

The source proof must run inside the existing project refresh/control writer lock. Open the
explicit local DuckDB with `O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, fstat path and fd, require device/inode
equality, copy bytes through the fd to a private mode-0600 immutable clone, fsync clone and parent,
and re-fstat source size/mtime/device/inode before opening the clone. Open DuckDB only
`read_only=True` on the clone and verify the exact published-snapshot/refresh-run join above and
complete manifest inventory separately. Drift/open/schema failures fail closed; clean the
clone in `finally` and never expose its path. The CLI `--execute` path acquires the same project
writer lock before this proof and any sidecar/destination write. Writers outside that lock are out
of scope.

Use the exact private filename `.stock-eva-replication-destination.json`. Canonical JSON is sorted,
compact UTF-8 with one final newline and contains exactly `descriptor_schema`, `schema_version`,
`dataset`, `role`, `direction`, `root_dev`, `root_ino`, `parent_dev`, `parent_ino`, `mount_point`,
`fs_type`, `mount_generation`, `mount_fingerprint`, `sentinel_sha256`, `single_writer_host_id`,
`created_at` and `descriptor_sha256`. The role is `nas_archive`, direction is `local_to_nas`;
descriptor hash is `domain_sha256('stock-eva/r2f4.3/destination-descriptor/v1', all other fields)`;
`destination_id` is its first 32 lower-case hex characters. The descriptor is private; serializers
omit its path and mount point. `mount_fingerprint` is exactly
`domain_sha256('stock-eva/r2f4.3/mount-fingerprint/v1',
{mount_point,fs_type,normalized_options,st_dev,volume_id})`; unavailable `volume_id` is explicit
JSON `null`, and `mount_generation` is a fresh initialization nonce.

Use descriptor-bound directory fds and `O_NOFOLLOW`; at every open fstat the bound fd and perform a
mount probe, then compare root/parent device+inode, mount point, filesystem type, mount
generation/fingerprint and sentinel hash before each execute boundary.
Changes are `DESTINATION_REBOUND`, not a retryable success. Implement the standalone command
`market-replication-init --destination /absolute/new-child --execute --acknowledge
CREATE_EMPTY_NAS_ARCHIVE_R2F4_3`; only that exact typed ack authorizes initialization. Plan mode,
including an absent parent, performs zero mkdir/descriptor/sentinel/sidecar/mount work. The exact
anchors are `tests/test_dataset_replication.py::test_destination_descriptor_canonical_json_and_hash`,
`tests/test_dataset_replication.py::test_destination_descriptor_binds_root_parent_mount_and_sentinel`,
`tests/test_dataset_replication.py::test_destination_remount_or_inode_change_fails_closed`, and
`tests/test_dataset_replication.py::test_destination_init_requires_exact_typed_ack_and_is_dry_run_by_default`.

### H4 — lineage and ordering

Do not alter `published_snapshots` or `manifest.json`. After the canonical pointer commit, the
strict reader creates the complete checkpoint bound to the immutable source-instance record,
device/inode/schema and source-instance digest described in H1. The sidecar assigns `source_sequence` only in deterministic journal-import
order; operation_day is queue metadata and is excluded from checkpoint, plan and intent identity.
The destination writer stores the sole authority in NAS: each
`_replication/history/<replication_generation>/` contains immutable objects and a self-contained
`replication-record.json`; `_replication/head.json` is a complete pointer to one record. SQLite
stores only intent/event state and a non-authoritative last-observed destination cache. NAS head is
committed atomically first; the sidecar result/cache is appended afterwards and startup reconciles
by reading NAS head/record, never by trusting stale cache. Restore reads archive record/manifest/
objects without SQLite. Implement the partial order: equal checkpoint hashes are idempotent; a
strictly newer source descendant may copy; destination-ahead, equal-sequence divergence and
incomparable chains return their exact allowlisted reasons; final head baseline drift returns
`CAS_CONFLICT`. Never sort UUID generations, timestamps, mtimes or retry order. The exact anchors are
`tests/test_dataset_replication.py::test_nas_record_lineage_source_sequence_parent_record_partial_order`
and `tests/test_dataset_replication.py::test_destination_ahead_divergent_tie_and_cas_are_fail_closed`.

Before staging a generation, call `DestinationArchiveReader` for any existing directory selected
by the deterministic checkpoint. An existing record for the same checkpoint MUST be verified
byte-for-byte, including descriptor, source-instance record, manifest, complete inventory and all
objects, then reused with the same generation, record hash and immutable `created_at`. A differing,
partial or corrupt record is `DESTINATION_CONFLICT`/`DESTINATION_TRUST_FAILED`; it is never
regenerated or overwritten. Only an absent generation may receive a new immutable `created_at`.
Add `test_existing_generation_record_is_verified_and_reused_byte_identically`,
`test_existing_generation_record_conflict_is_not_overwritten`, and
`test_created_at_is_immutable_across_retries`.

All destination writers, including initialization and offline restore fakes, must use
`single_writer_host_id`, open `_replication/.writer.lock` through a descriptor-bound `O_NOFOLLOW` lock fd and obtain an OS advisory
exclusive lock. Hold it across baseline read, hidden staging, every verification, immutable
history/manifest write, head CAS and fsync. Lock probe/acquisition failure maps to
`MOUNT_UNSUPPORTED`; external writers that do not honor this protocol are explicitly out of scope.
Add concurrent-writer, lock-loss and crash-at-each-phase tests.

Use this destination layout exactly: `_replication/history/<replication_generation>/` contains the
standard `.stock-eva-dataset.json`, `manifest.json`, immutable Parquet objects and exactly one
self-contained `replication-record.json`; `_replication/head.json` contains the complete closed
pointer schema. `DestinationArchiveReader` follows head, record, sentinel, manifest and objects
only through its root/generation directory fds and `openat` no-follow operations. It may reuse only
pure `NasMarketStore` parsing/schema/Parquet validators after descriptor reads; it MUST NOT reopen a
NAS path through `NasMarketStore`. Staging and quarantine are outside history/head namespaces.

The implementation must serialize the self-contained record with the exact closed field set
`record_schema,schema_version,replication_generation,destination_id,direction,source_instance_id,
source_instance_sha256,source_sequence,checkpoint_id,publication_binding_sha256,parent_record_hash,pointer_row_sha256,pointer_generation,source_run_id,
source_trade_date,source_published_at,pointer_db_device,pointer_db_inode,pointer_db_schema_digest,
source_manifest_canonical_sha256,source_manifest_bytes_sha256,source_object_set_sha256,
destination_manifest_bytes_sha256,destination_object_set_sha256,object_inventory,object_count,
row_count,byte_count,descriptor_sha256,plan_sha256,created_at,record_sha256`. It uses sorted compact
UTF-8 JSON plus one newline, embeds the complete sorted inventory, rejects unknown/duplicate
fields, and computes `record_sha256` over every field except itself under the record domain.
The NAS head field set is exactly
`head_schema,schema_version,destination_id,replication_generation,record_sha256,descriptor_sha256,
direction,source_instance_id,source_sequence,checkpoint_id,publication_binding_sha256,parent_record_hash,manifest_sha256,
object_set_sha256,head_version,updated_at,head_sha256`; `head_sha256` excludes only itself from the
head domain. Restore verifies archive record/manifest/objects independently of SQLite.

The writer stages objects and record, fsyncs files/directories, and installs the complete history
directory with a same-parent no-replace operation (`renameat2(RENAME_NOREPLACE)` or a verified
exclusive equivalent); it never replaces an existing generation. It then atomically replaces
`head.json` and fsyncs its parent. This NAS head commit is the sole visibility point. Only afterwards
does it append the sidecar result and cache. If the process crashes after NAS head fsync, the next
run reads NAS head/record and idempotently reconciles the sidecar; if it crashes before head fsync,
the previous head remains authoritative.
In the head projection, `manifest_sha256` MUST equal the referenced record's
`destination_manifest_bytes_sha256` and `object_set_sha256` MUST equal its
`destination_object_set_sha256`; these aliases are never independently sourced.

All implementation digests use this exact preimage (including the final newline):

```python
canonical_json_bytes(value) = (
    json.dumps(value, ensure_ascii=False, sort_keys=True,
               separators=(",", ":"), allow_nan=False) + "\n"
).encode("utf-8")
domain_sha256(domain, value) = sha256(
    domain.encode("ascii") + b"\n" + canonical_json_bytes(value)
).hexdigest()
```

The closed preimages are exactly:
`source_object_set_sha256 = domain_sha256("stock-eva/r2f4.3/object-set/v1", object_inventory)`;
`dataset_identity = domain_sha256("stock-eva/r2f4.3/dataset-identity/v1",
{canonical_root_path,canonical_schema_digest,source_instance_domain,source_instance_nonce})`;
`source_instance_id = domain_sha256("stock-eva/r2f4.3/source-instance/v3", {dataset_identity})`;
`publication_binding_sha256 = domain_sha256("stock-eva/r2f4.3/source-publication-binding/v1",
{binding_schema,schema_version,run_id,trade_date,published_at,manifest_generation,
manifest_bytes_sha256,manifest_canonical_sha256,source_object_set_sha256,selection_sha256,
lineage_sha256})`;
`checkpoint_id = domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1",
{source_instance_id,source_instance_sha256,pointer_row_sha256,pointer_generation,source_run_id,source_trade_date,
source_published_at,pointer_db_device,pointer_db_inode,pointer_db_schema_digest,
manifest_canonical_sha256,source_manifest_bytes_sha256,source_object_set_sha256,object_inventory,
publication_binding_sha256})`;
`checkpoint_payload_sha256 = domain_sha256("stock-eva/r2f4.3/source-checkpoint-payload/v1",
{checkpoint_id,source_instance_id,source_instance_sha256,pointer_row_sha256,pointer_generation,source_run_id,
source_trade_date,source_published_at,pointer_db_device,pointer_db_inode,
pointer_db_schema_digest,manifest_canonical_sha256,source_manifest_bytes_sha256,
source_object_set_sha256,object_inventory,publication_binding_sha256})`;
`plan_sha256 = domain_sha256("stock-eva/r2f4.3/replication-plan/v1",
{direction,destination_id,checkpoint_id,object_inventory})`; and
`intent_id = domain_sha256("stock-eva/r2f4.3/replication-intent/v1",
{direction,destination_id,source_instance_id,source_sequence,checkpoint_id,plan_sha256})`.
`intent_sha256` hashes the complete immutable intent row. The replication generation hash,
history hash, descriptor hash, event hash and journal hash each use their named domain and their
closed sorted field set; `prev_event_sha256` and `parent_record_hash` are 64 lower-case hex
characters, with genesis exactly `0000000000000000000000000000000000000000000000000000000000000000`.
No self hash field is included in its own preimage. Golden vectors and one-byte newline/ordering
mutations are mandatory tests.

### H5 — API/CLI effects

Add `mode`, `execution_allowed` and `effects` to every response. `effects` has exactly
`writes`, `canonical_writes`, `destination_writes`, `outbox_writes` and `restore_writes`; status and
plan are all false, while execute is populated from observed writer effects. A successful replicate
claims destination/outbox effects and never returns a plan-shaped all-false response. An idempotent
execute may report no physical write only with `mode=execute` and proof of no write. Keep
`provider_requests=0`, `paths_exposed=false`; the exact effects anchor is
`tests/test_dataset_replication.py::test_execute_response_reports_effects_without_false_dry_run_claim`.

The serializer contract is:

```typescript
type ReplicationMode = "status" | "plan" | "execute";
interface Effects { writes: boolean; canonical_writes: boolean;
  destination_writes: boolean; outbox_writes: boolean; restore_writes: boolean; }
interface OperationProjection { mode: ReplicationMode; execution_allowed: boolean;
  effects: Effects; provider_requests: 0; paths_exposed: false; }
```

`status` and `plan` serialize all five effect flags as false. Execute serializers receive an
observed-effects object from the writer and cannot reuse the dry-run serializer; every physical
write is reflected, and a no-write idempotent execute is marked `mode=execute` with proof of no
write. `ReplicationReason` is a closed union, not `string`; unknown failures map to
`REPLICATION_STATE_UNAVAILABLE`.

For a lexically valid plan with strict source and descriptor proofs, serialize exactly
`mode=plan`, `status=dry_run`, `reason_code=NONE`, `execution_allowed=false`,
`provider_requests=0`, and all five effects false. Plan mode allocates no intent, does not create a
destination and never converts a validation failure to success. Add
`tests/test_dataset_replication.py::test_market_replicate_plan_success_is_dry_run_reason_none_and_zero_write`.

### H6 — restore sequencing

Require that the final restore destination does not exist. Create hidden staging through the parent
directory fd with `openat(..., O_CREAT|O_EXCL|O_NOFOLLOW, 0700)` and create every staged file with
`O_CREAT|O_EXCL|O_NOFOLLOW`; fsync each file, staging directory and parent. Do not expose staging
to any standard reader; use `DestinationArchiveReader` for archive reads and pure schema/Parquet
validators for staged bytes. Before the only visibility point, perform every semantic gate: descriptor,
sentinel, exact schema, no extra/duplicate entries,
complete object size/schema/row/hash/date checks, counts and representative read-only queries.
Fsync staged files/directories and parent, then install with same-parent no-replace
`renameat2(RENAME_NOREPLACE)` or an equivalent exclusive link/install protocol that proves no
replacement. Never use rename-over-existing or `os.replace`. If the final name appears, return
`PATH_CHANGED`/`DESTINATION_CONFLICT` without overwrite. An existing exact checkpoint is verified
byte-for-byte by `DestinationArchiveReader` and may return idempotent success without a write; any
different destination is rejected. The no-replace install is the sole visibility point; there is no
typed restore pointer and no canonical control mutation. After rename perform only bounded
non-semantic directory/inode/manifest-byte readback. A rename failure removes or isolates staging;
a post-rename fingerprint/bytes anomaly isolates the destination and strict readers reject it. The exact anchors are
`tests/test_dataset_replication.py::test_restore_semantics_complete_before_atomic_rename`,
`tests/test_dataset_replication.py::test_restore_rename_failure_leaves_destination_absent_or_quarantined`,
`tests/test_dataset_replication.py::test_restore_post_rename_readback_is_nonsemantic`,
`tests/test_dataset_replication.py::test_restore_staging_uses_o_excl_nofollow_and_fsync`,
`tests/test_dataset_replication.py::test_restore_install_is_no_replace`,
`tests/test_dataset_replication.py::test_restore_existing_same_checkpoint_is_verified_without_write`, and
`tests/test_dataset_replication.py::test_restore_existing_destination_is_never_overwritten`.

Persist a sanitized immutable `RestoreReport` under private restore evidence (or hidden staging
before rename) with exactly `report_schema,schema_version,report_id,destination_id,
source_replication_generation,source_record_sha256,source_instance_id,source_sequence,checkpoint_id,
object_count,row_count,byte_count,verification_state,reason_code,started_at,completed_at,
report_sha256`. Hash all fields except `report_sha256` under
`stock-eva/r2f4.3/restore-report/v1`, fsync the report and parent before private atomic rename, and
never expose paths, credentials or payload. Restore remains independently auditable without local
SQLite sidecar authority.

### M1/M2/M3/M4 — implementation order and failure rules

Use the closed `ReplicationReason` union from the design amendment, including
`LOCAL_POINTER_MISMATCH`, `DESTINATION_REBOUND`, `DESTINATION_AHEAD` and
`MOUNT_UNSUPPORTED`; map unknown errors to `REPLICATION_STATE_UNAVAILABLE`. Implement
one surface-specific priority algorithm. Disabled status/API/automation short-circuits to
`DISABLED` before any sidecar/source/destination/lock/provider construction and performs zero
sidecar I/O. Enabled writers use lexical path/direction, sidecar proof, source checkpoint proof,
destination mount/trust, lineage/CAS, copy/readback, retry/dead-letter, then idempotent/ready.
Explicit CLI status returns `DISABLED` before sidecar I/O when the feature is disabled; only when
enabled may it perform a strict read-only sidecar proof after lexical validation. It never
initializes, migrates, repairs or locks.

Use one reason-to-status-to-transport table in every serializer; do not maintain an internal/public
second mapping:

| Reason | Status | HTTP | CLI exit |
|---|---|---:|---:|
| `NONE` | `ready` | 200 | 0 |
| `ALREADY_REPLICATED` | `ready` | 200 | 0 |
| `DISABLED` | `disabled` | 200 | 0 |
| `OUTBOX_ENQUEUE_FAILED` | `degraded` | 200 | 1 |
| `OUTBOX_JOURNALED` | `degraded` | 200 | 1 |
| `RETRY_WAIT` | `degraded` | 200 | 1 |
| `SOURCE_NOT_CONFIGURED` | `unavailable` | 503 | 1 |
| `SOURCE_UNAVAILABLE` | `unavailable` | 503 | 1 |
| `LOCAL_POINTER_MISMATCH` | `unavailable` | 503 | 1 |
| `REPLICATION_STATE_UNAVAILABLE` | `unavailable` | 503 | 1 |
| `OUTBOX_DURABILITY_UNAVAILABLE` | `unavailable` | 503 | 1 |
| `CONTROL_STATE_UNAVAILABLE` | `degraded` | 200 | 1 |
| `DESTINATION_UNAVAILABLE` | `unavailable` | 503 | 1 |
| `DESTINATION_MOUNT_UNAVAILABLE` | `unavailable` | 503 | 1 |
| `DESTINATION_TRUST_FAILED` | `unavailable` | 503 | 1 |
| `DESTINATION_REBOUND` | `unavailable` | 503 | 1 |
| `DESTINATION_AHEAD` | `unavailable` | 503 | 1 |
| `DESTINATION_CONFLICT` | `unavailable` | 503 | 1 |
| `CAS_CONFLICT` | `unavailable` | 503 | 1 |
| `COPY_FAILED` | `unavailable` | 503 | 1 |
| `VERIFY_FAILED` | `unavailable` | 503 | 1 |
| `DEAD_LETTER` | `unavailable` | 503 | 1 |
| `MOUNT_UNSUPPORTED` | `unavailable` | 503 | 1 |
| `PATH_INVALID` | `unavailable` | 422 | 2 |
| `PATH_CHANGED` | `unavailable` | 422 | 2 |
| `SYMLINK_UNSAFE` | `unavailable` | 422 | 2 |
| `DIRECTION_NOT_ALLOWED` | `unavailable` | 422 | 2 |

The `NONE -> ready` row applies to status and execute; the sole plan-mode success projection is
`mode=plan,status=dry_run,reason_code=NONE` with all effects false, and is covered by
`test_market_replicate_plan_success_is_dry_run_reason_none_and_zero_write`.

`MarketStore` without an explicit local dataset root maps to `SOURCE_NOT_CONFIGURED`; it is never
allowed to infer a source from NAS. The canonical callback catches replication exceptions and
returns this same projection without changing canonical result/status.

Hash implementation is closed: sorted lists use `(relative_path,object_sha256)` and no digest
field is included in its own preimage. The exact domains and fields are:

```text
pointer_row_sha256 = domain_sha256("stock-eva/r2f4.3/pointer-row/v1",
  {singleton,run_id,trade_date,published_at})
source_object_set_sha256 = domain_sha256("stock-eva/r2f4.3/object-set/v1",
  sort(object_inventory, key=(relative_path,object_sha256)))
descriptor_sha256 = domain_sha256("stock-eva/r2f4.3/destination-descriptor/v1",
  {descriptor_schema,schema_version,dataset,role,direction,root_dev,root_ino,parent_dev,
   parent_ino,mount_point,fs_type,mount_generation,mount_fingerprint,sentinel_sha256,
   single_writer_host_id,created_at})
replication_generation = domain_sha256("stock-eva/r2f4.3/replication-generation/v1",
  {destination_id,source_instance_id,source_sequence,checkpoint_id,publication_binding_sha256,source_object_set_sha256,
   parent_record_hash,plan_sha256})
checkpoint_id = domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1",
  {source_instance_id,source_instance_sha256,pointer_row_sha256,pointer_generation,source_run_id,source_trade_date,
   source_published_at,pointer_db_device,pointer_db_inode,pointer_db_schema_digest,
   manifest_canonical_sha256,source_manifest_bytes_sha256,source_object_set_sha256,object_inventory,
   publication_binding_sha256})
record_sha256 = domain_sha256("stock-eva/r2f4.3/replication-record/v1",
  {record_schema,schema_version,replication_generation,destination_id,direction,
   source_instance_id,source_instance_sha256,source_sequence,checkpoint_id,publication_binding_sha256,parent_record_hash,pointer_row_sha256,
   pointer_generation,source_run_id,source_trade_date,source_published_at,pointer_db_device,
   pointer_db_inode,pointer_db_schema_digest,source_manifest_canonical_sha256,
   source_manifest_bytes_sha256,source_object_set_sha256,destination_manifest_bytes_sha256,
   destination_object_set_sha256,object_inventory,object_count,row_count,byte_count,descriptor_sha256,
   plan_sha256,created_at})
head_sha256 = domain_sha256("stock-eva/r2f4.3/replication-head/v1",
  {head_schema,schema_version,destination_id,replication_generation,record_sha256,
   descriptor_sha256,direction,source_instance_id,source_instance_sha256,source_sequence,checkpoint_id,
   publication_binding_sha256,
   parent_record_hash,manifest_sha256,object_set_sha256,head_version,updated_at})
event_id = domain_sha256("stock-eva/r2f4.3/replication-event-id/v1",
  {intent_id,event_sequence,attempt,state_version,occurred_at})
event_sha256 = domain_sha256("stock-eva/r2f4.3/replication-event/v1",
  {event_id,intent_id,event_sequence,prev_event_sha256,event_type,from_state,to_state,
   attempt,reason_code,state_version,occurred_at,destination_replication_generation,
   destination_record_sha256,destination_head_sha256})
journal_sha256 = domain_sha256("stock-eva/r2f4.3/replication-journal/v1",
  {journal_schema_version,checkpoint_id,source_instance_id,source_instance_sha256,publication_binding_sha256,checkpoint_payload_sha256,
   source_published_at,created_at})
source_commit_sha256 = domain_sha256("stock-eva/r2f4.3/source-commit/v1",
  {source_commit_schema,pointer_row_sha256,pointer_generation,source_run_id,source_trade_date,
   source_published_at,pointer_db_device,pointer_db_inode,pointer_db_schema_digest,
   manifest_canonical_sha256,source_manifest_bytes_sha256,source_object_set_sha256,object_inventory,
   publication_binding_sha256,publication_binding_manifest_generation,
   publication_binding_selection_sha256,publication_binding_lineage_sha256,
   source_instance_id,source_instance_sha256,committed_at})
observation_sha256 = domain_sha256("stock-eva/r2f4.3/replication-observation/v1",
  {observation_schema,source_commit_sha256,checkpoint_id,source_instance_id,source_sequence,
   intent_id,enqueue_state,reason_code,effects,observed_at})
intent_sha256 = domain_sha256("stock-eva/r2f4.3/replication-intent-row/v1",
  {intent_id,schema_version,direction,destination_id,checkpoint_id,pointer_row_sha256,
   pointer_generation,source_run_id,source_trade_date,source_published_at,pointer_db_device,
   pointer_db_inode,pointer_db_schema_digest,manifest_canonical_sha256,source_object_set_sha256,
   source_instance_id,source_instance_sha256,source_sequence,source_manifest_bytes_sha256,publication_binding_sha256,
   plan_sha256,object_count,row_count,byte_count,created_at})
```

Genesis `prev_event_sha256` and `parent_record_hash` are exactly 64 zero hex characters; all other
digests are 64 lower-case hex. `checkpoint_id` excludes `source_sequence`. In one
Private in-memory transaction recovery sorts all valid journals by `(source_published_at,checkpoint_id)`,
allocates sequences and creates intent/event/head rows, then serializes through the descriptor-native
engine with baseline CAS and fsync. Unique constraints make restart-safe import and retries reuse one sequence and intent regardless of
operation_day. `operation_day` is scheduling metadata only and is excluded from `checkpoint_id`,
`plan_sha256`, `intent_id` and `intent_sha256`. Use the normalized JSON encoder above with explicit null/false/zero/empty values and
one final newline. Golden tests mutate one field, one sort order and the newline and require a
different digest. The canonical source digest name is always `source_object_set_sha256`; a bare
`object_set_sha256` is reserved only for the destination-head compatibility alias. Add
`test_source_object_set_sha256_golden_vectors_are_canonical` before any writer is enabled.

In `backend/app/cli.py`, dispatch replication/status/init/restore before
`ensure_local_runtime_dirs()` or any factory; lexical validation has no I/O, strict readers are
read-only, and only execute can create approved local sidecar parents. Explicitly inject
`local_market_dataset_root`; NAS-only settings return `SOURCE_NOT_CONFIGURED`. Journal writes use
`<checkpoint_id>.json`, mode `0600`, `O_NOFOLLOW`, complete newline-terminated bytes, a unique
`O_EXCL` temporary, fsync(fd), macOS `renameatx_np(RENAME_EXCL)` or portable hard-link-no-replace
install and parent fsync. Existing target bytes may be reused only when identical; otherwise the
journal is a durability failure. Import validates no-follow fd/inode and hash, applies all sorted
intent/event/head rows in the private in-memory image, serializes and commits the complete
snapshot with baseline CAS and fsync, then removes the journal; failed removal is safely
re-importable. The exact early-dispatch/source/journal anchors are
`tests/test_dataset_replication.py::test_market_replicate_dry_run_parent_absent_creates_nothing`,
`tests/test_dataset_replication.py::test_market_restore_dry_run_parent_absent_creates_nothing`,
`tests/test_dataset_replication.py::test_market_replication_status_missing_sidecar_does_not_initialize`,
`tests/test_dataset_replication.py::test_nas_only_configuration_never_becomes_replication_source`,
and `tests/test_dataset_replication.py::test_journal_no_follow_fsync_atomic_import_and_crash_recovery`,
`tests/test_dataset_replication.py::test_journal_temp_uses_o_excl_no_follow_and_fsync`,
`tests/test_dataset_replication.py::test_journal_install_is_no_replace_and_reuses_identical_target`, and
`tests/test_dataset_replication.py::test_journal_conflicting_target_fails_without_overwrite`.

Implement this exact CLI grammar:

```text
stock-eva market-replicate [--destination ABSOLUTE_PATH] [--operation-day YYYY-MM-DD] [--execute] [--json]
stock-eva market-restore --source ABSOLUTE_PATH --destination ABSOLUTE_PATH [--execute] [--json]
stock-eva market-replication-init --destination ABSOLUTE_PATH [--execute --acknowledge CREATE_EMPTY_NAS_ARCHIVE_R2F4_3] [--json]
stock-eva market-replication-status [--json]
```

Reject unknown/repeated flags, missing values, non-ISO dates and lexical path errors with exit `2`
before filesystem access. Precedence is explicit `--destination`, then
`STOCK_EVA_REPLICATION_DESTINATION_ROOT`, then default `None`; no alternate config source exists.
Source is always `settings.local_market_dataset_root`, never a CLI flag or NAS fallback.
`--execute` is the only writer gate; omitted execution is read-only plan. `--json` changes only
serialization. Status has no source/destination override and may only run the strict sidecar SELECT.

### M5/M6 — exact settings, steps and evidence gate

Implement exactly these names/defaults: `replication_enabled` /
`STOCK_EVA_REPLICATION_ENABLED=false`; `replication_drain_enabled` /
`STOCK_EVA_MARKET_REPLICATION_DRAIN_ENABLED=false`; `replication_destination_root` /
`STOCK_EVA_REPLICATION_DESTINATION_ROOT=None`; the legacy `replication_database_name` /
`STOCK_EVA_REPLICATION_DATABASE_NAME=replication.sqlite3` is parsing-only and never a sidecar
pathname; `replication_journal_root_name` /
`STOCK_EVA_REPLICATION_JOURNAL_ROOT_NAME=replication-journal`; `replication_max_attempts` /
`STOCK_EVA_REPLICATION_MAX_ATTEMPTS=6`; `replication_lease_seconds` /
`STOCK_EVA_REPLICATION_LEASE_SECONDS=900`; `replication_drain_timeout_seconds` /
`STOCK_EVA_REPLICATION_DRAIN_TIMEOUT_SECONDS=900`; and fixed
`replication_direction=local_to_nas`. Retry delays are fixed `(60,300,1800,7200,43200)` and not
environment-configurable. `replication_enabled` only permits local checkpoint enqueue; automated
NAS writes require the drain flag, approved descriptor and lock. CLI `--execute` is independent
explicit authorization. Reject root/home/share/unresolved-env/relative/overlap paths.

The automation wiring MUST enforce both flags: with `replication_enabled=true` and the drain flag
false it may capture and enqueue a local checkpoint but MUST not open or write NAS. A background
drain additionally requires a descriptor approved by the initialization contract and the existing
writer lock. The CLI execute path remains a separate explicit operator action and does not turn on
the automation drain flag. Required tests are
`test_replication_enabled_only_enqueues_when_drain_disabled`,
`test_automation_drain_requires_flag_and_approved_descriptor`, and
`test_cli_execute_is_independent_explicit_authorization`.

Before Step 1, add all H1-H6/M1-M6 RED tests. Step 1 implements settings/layout and early dispatch;
Step 2 implements pointer/source and descriptor proof; Step 3 implements the exact sidecar,
journal, digest and replay; Step 4 implements lineage/copy/CAS/retry; Step 5 implements hidden
restore; Step 6 wires `main.py`, API, CLI and existing automation; Step 7 updates docs/assets and
proves default-off/TCC behavior; Step 8 runs GREEN focused/full/static/strict gates. Every NFR-1
through NFR-15 must have the exact anchor below and pass with explicit bytes/inode, no-provider,
15-minute-budget and hash-golden assertions. No real NAS/SMB, provider, LaunchAgent or production
path is allowed.

| ID | Exact planned evidence anchor |
|---|---|
| NFR-1 | `tests/test_dataset_replication.py::test_dry_run_and_status_are_zero_write_bytes_and_inodes` |
| NFR-2 | `tests/test_dataset_replication.py::test_strict_snapshot_checks_every_manifest_object` |
| NFR-3 | `tests/test_dataset_replication.py::test_destination_reader_sees_previous_or_complete_manifest_only` |
| NFR-4 | `tests/test_dataset_replication.py::test_drain_budget_is_one_intent_six_attempts_and_fifteen_minutes` |
| NFR-5 | `tests/test_dataset_replication.py::test_retry_schedule_is_exact_and_bounded` |
| NFR-6 | `tests/test_dataset_replication.py::test_outbox_transition_is_transactional_and_state_versioned` |
| NFR-7 | `tests/test_dataset_replication.py::test_source_object_set_sha256_golden_vectors_are_canonical` |
| NFR-8 | `tests/test_dataset_replication.py::test_status_cli_api_have_no_path_or_secret` |
| NFR-9 | `tests/test_dataset_replication.py::test_destination_archive_reader_uses_root_dirfd_openat_no_path_reopen` |
| NFR-10 | `tests/test_dataset_replication.py::test_nas_failure_does_not_change_local_ready_pointer` |
| NFR-11 | `tests/test_dataset_replication.py::test_status_10k_terminal_rows_under_500ms_without_parquet_scan` |
| NFR-12 | `tests/test_dataset_replication.py::test_failed_restore_has_no_reader_visible_root` |
| NFR-13 | `tests/test_launchagent_assets.py::test_replication_defaults_off_and_no_provider_in_dry_run` |
| NFR-14 | `tests/test_nas_dataset.py::test_canonical_manifest_and_pointer_bytes_remain_unchanged` |
| NFR-15 | `tests/test_dataset_replication.py::test_spec_evidence_uses_no_real_nas_or_provider` |
| NFR-16 | `tests/test_dataset_replication.py::test_public_status_exposes_fixed_local_chain_trust_scope` |

The full FR/AC/EC crosswalk above and this NFR table MUST remain byte-for-byte identical to the
companion design crosswalk. Before implementation release, an automated checker must extract both
tables, assert identical IDs and anchors, assert every anchor node exists, and reject weak grouped
anchors. The final evidence records exact commands, actual HEAD and clean/diff output; no SPEC or
production GO is claimed from this document.

## Review and release boundary

The implementation plan is not a production approval. Before implementation, the design and this
plan require independent SPEC review. After implementation, the release candidate requires
independent QUALITY review of the exact commit, all test anchors, diff, static checks and no-write
evidence. Real NAS copy/restore remains blocked until a new explicit operation window is opened.

## R2-F4.3.3 sixth-round implementation amendment — manifest publication lock and plain-store boundary

This amendment is the implementation authority for the H1/M1/M3 closure and supersedes any
earlier weaker lock, callback or plain-store wording. It remains a plan only: do not implement,
access NAS, mount a share, call a provider or run a production operation from this worktree.

### H1 — one lock-owned dataset publication

Promote the existing `_ManifestLock` in `backend/app/storage/dataset.py` to the internal,
non-reentrant `ManifestPublicationLock`. Its acquisition returns an opaque,
unforgeable `ManifestPublicationLockToken`; callers cannot serialize, manufacture, pass or
reacquire it. The dataset-backed `publish_dataset_and_pointer(...)` wrapper acquires
exactly one token and keeps it until post-commit exact proof completes. Its critical section is
strictly:

```text
baseline descriptor-bound manifest read + fingerprint
  -> objects and private staging creation/readback
  -> manifest replacement + fsync + strict readback
  -> immutable source-binding write/readback + file/parent fsync
  -> final manifest fingerprint CAS
  -> DuckDB external publication pointer commit
  -> pointer/manifest/binding exact proof
```

Define
`manifest_fingerprint = domain_sha256("stock-eva/r2f4.3/manifest-fingerprint/v1", {
manifest_bytes_sha256,manifest_canonical_sha256,manifest_generation,source_object_set_sha256})`.
Immediately before the pointer transaction, re-read the manifest with descriptor-bound handles and
compare that complete fingerprint. Any generation, byte hash, canonical hash or object-set drift
returns `LOCAL_POINTER_MISMATCH`, leaves the prior pointer unchanged and leaves the new binding
inactive. Keep the token through pointer commit and exact proof; only then release it and begin
enqueue/journal/NAS work.

Route every dataset-backed `NasMarketStore` writer through this wrapper exactly once:
`save_refresh`, `reconcile_control_pointer`, `backfill`, `upsert`, refresh,
`full_history` and CLI reconciliation. Implement `_publish_bars_locked(token, ...)` as an
internal token-checked path that never acquires `_ManifestLock`; no public method may acquire the
lock and call another public method that acquires it. Every manifest mutation uses this lock, so
concurrent backfill/upsert/refresh/reconcile cannot mutate the baseline during object/staging,
manifest, binding or pointer phases. Startup, CLI and `full_history` call the wrapper and do not
acquire or nest the manifest lock.

`RefreshRunLock` is optional scheduling coordination only. If present, the one allowed order is
`RefreshRunLock -> ManifestPublicationLock`; the wrapper never acquires `RefreshRunLock`, and no
path acquires these locks in reverse order. Refresh-run locking is never the publication
atomicity dependency. Busy, token mismatch, order inversion or any pre-pointer readiness failure
is fail-closed with no pointer advance; a post-pointer unlock/close failure follows the degraded
control path below. Add RED tests before implementation and make them GREEN only after the
lock path is wired:

`test_publication_is_atomic_under_one_manifest_publication_lock`,
`test_manifest_lock_blocks_concurrent_backfill_or_upsert_mutation`,
`test_manifest_fingerprint_drift_blocks_pointer_cas`,
`test_refresh_run_lock_is_not_publication_atomicity_dependency`,
`test_pointer_unchanged_after_manifest_publication_crash_before_pointer`, and
`test_binding_reproved_after_manifest_publication_crash_after_pointer`.

### M1 — legacy null digests and required v2 metadata

Implement these exact canonical-null digests for intentional legacy/no-selection publications:

```text
canonical_null_selection_sha256 =
  domain_sha256("stock-eva/r2f4.3/selection-null/v1", None)
canonical_null_lineage_sha256 =
  domain_sha256("stock-eva/r2f4.3/lineage-null/v1", None)
```

The preimage is literal UTF-8 `null` plus one newline under each domain. Do not substitute a
missing field, `"null"`, empty object or empty string. A schema-v2 manifest must contain
`provider_id,universe_id,evidence_id,evidence_sha256,candidate_id,candidate_manifest_sha256,
gate_report_sha256,adapter_version,source_schema_version` on every object entry; required-but-
missing, malformed or conflicting metadata returns `SOURCE_UNAVAILABLE` before binding/pointer
commit. Only intentionally absent selection uses the two fixed null digests.
`reconcile_control_pointer` may reuse an existing binding only after the same lock-bound exact
proof. Required anchors are `test_legacy_no_selection_uses_canonical_null_digests` and
`test_missing_v2_manifest_metadata_blocks_pointer`.

### M3 — plain `MarketStore` compatibility and ownership

Do not route the plain `MarketStore` through the replication publication wrapper. Preserve its
existing canonical commit, locking, bytes and behavior exactly; it must not write a binding, run
the guard, acquire `ManifestPublicationLock`, infer a dataset root or access NAS. After its
successful canonical DuckDB commit, emit only the typed replication observation
`SOURCE_NOT_CONFIGURED` when no explicit dataset/manifest-backed `NasMarketStore` was used.
Replication errors cannot change or roll back plain-store canonical success. Only the dataset
wrapper methods listed above may create an active `SourceCommit`. Required compatibility anchor:
`test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured`.

### File ownership and transaction/crash matrix

`backend/app/storage/dataset.py` owns the lock/token, central wrapper and `_publish_bars_locked`;
`backend/app/market/refresh.py`, `backfill.py`, `full_history.py` and `backend/app/cli.py` call
the wrapper without acquiring `_ManifestLock`; `backend/app/market/store.py` retains the plain
canonical path. The implementation sequence is:

```text
baseline manifest -> objects/staging -> manifest replace/readback
  -> binding fsync -> final fingerprint CAS -> pointer commit -> exact proof
  -> release manifest lock -> journal/outbox observation
```

| Failure point | Required implementation result |
|---|---|
| Concurrent mutation/backfill while wrapper owns the lock | Competing mutation waits or returns busy; it cannot alter the baseline or publish a stale pointer. |
| Final manifest fingerprint drift | Return `LOCAL_POINTER_MISMATCH`; keep the previous pointer ready and the new binding inactive. |
| Binding/fsync failure | Return `OUTBOX_DURABILITY_UNAVAILABLE`; do not attempt pointer commit. |
| Crash before pointer commit | Prior pointer remains visible; staging/temp/binding is absent or quarantined and inactive. |
| Crash after pointer commit before exact proof | Reconciliation uses the same lock-bound manifest/binding proof; it never regenerates or overwrites the binding. |
| Plain `MarketStore` canonical or replication failure | Preserve existing canonical result/bytes; return `SOURCE_NOT_CONFIGURED` observation and no replication side effect. |

Add the lock, writer-routing, null-digest, missing-metadata, plain-store, CAS-drift and crash
anchors to the exact crosswalk above. Keep status `In Review / NO-GO`; this amendment is not
implementation or SPEC/QUALITY evidence.

## Final normative consolidation — R2-F4.3.5 coordinator, lineage and phase contracts

This is the single final normative specification. It supersedes every earlier conflicting
paragraph, model, file map, transaction row or test anchor in both R2-F4.3 documents. In
particular, the old `commit_published_snapshot_with_binding` name is retired and MUST NOT be
implemented; the only coordinator operations are `publish_manifest_only` and
`publish_dataset_and_pointer`. No standalone dataset-initialization publication writer exists.
`MarketStore` is never a dataset writer.

### 1. Exact typed operations

One `ManifestPublicationCoordinator` owns the non-reentrant `ManifestPublicationLock` and exposes
exactly these mutually exclusive typed operations:

```typescript
type PointerIdentity =
  | Readonly<{ kind: "ABSENT" }>
  | Readonly<{ kind: "PRESENT"; row_sha256: string; device: number;
               inode: number; schema_digest: string }>
  | Readonly<{ kind: "INVALID"; reason_code: "CONTROL_STATE_UNAVAILABLE" }>;
type ManifestOnlyInput = Readonly<{
  bars: ReadonlyArray<DailyBar>; source: string;
  lineage_input: LineageInput;
}>;
type ManifestOnlyResult = Readonly<{
  manifest_generation: string; manifest_bytes_sha256: string;
  manifest_canonical_sha256: string; source_object_set_sha256: string;
  pointer_before: PointerIdentity; pointer_after: PointerIdentity;
  pointer_unchanged: true;
}>;
type DatasetPointerInput = Readonly<{
  bars: ReadonlyArray<DailyBar>; source: string; result: RefreshResult;
  selection: PublishedSelection | null; lineage_input: LineageInput;
}>;
type DatasetPointerResult = Readonly<{
  manifest_generation: string; publication_binding: SourcePublicationBinding;
  source_commit: SourceCommit; observation: ReplicationObservation;
}>;
interface ManifestPublicationCoordinator {
  publish_manifest_only(input: ManifestOnlyInput): ManifestOnlyResult;
  publish_dataset_and_pointer(input: DatasetPointerInput): DatasetPointerResult;
}
```

Both operations acquire one coordinator-owned lock token and may share only a token-checked locked
primitive. `publish_manifest_only` performs baseline read → objects/staging → manifest
replace/readback and final manifest verification only. It MUST NOT create/read/write a source
binding, invoke a pointer writer, run the pre-publication guard, or emit a `SourceCommit`.
`upsert_bars` and each backfill batch use it. `publish_dataset_and_pointer` performs the full
manifest → binding/fsync → final fingerprint CAS → DuckDB pointer commit → post-commit exact proof
path. It is used by `save_refresh`, `reconcile_control_pointer`, `full_history`, CLI reconciliation
and startup reconciliation. The manifest-only operation MUST NOT call or delegate to the pointer
operation.

For manifest-only operations, `ABSENT` is permitted only when the control DB is absent, or when a
descriptor-bound read proves the schema valid and proves that the singleton pointer row is absent.
It is never represented by a forced string, empty hash or synthetic row. A present pointer is
`PRESENT` with exactly the four canonical-rebuild fields `row_sha256`, `device`, `inode` and
`schema_digest`. Missing tables, corrupt or unknown schema, read failure, duplicate/malformed
singleton state or any incomplete control state returns `INVALID` with
`reason_code=CONTROL_STATE_UNAVAILABLE`; it MUST fail closed before manifest-only mutation. The
coordinator captures the tagged union before and after and requires structural equality of every
field; an `INVALID` result never proceeds and is not converted to `ABSENT`.
Required anchors: `test_manifest_only_empty_database_uses_absent_pointer_identity`,
`test_manifest_only_present_pointer_identity_is_exact_before_after`, and
`test_manifest_only_upsert_and_backfill_preserve_pointer_bytes_hash_and_inode`.

### 2. Closed writer inventory and plain-store boundary

| Writer seam | Exact operation | Prohibited behavior |
|---|---|---|
| `backend/app/storage/dataset.py::NasMarketStore.save_refresh` | `publish_dataset_and_pointer` | no direct pointer save or second lock |
| `backend/app/storage/dataset.py::NasMarketStore.upsert_bars` | `publish_manifest_only` | no binding, guard, pointer or `SourceCommit` |
| `backend/app/storage/dataset.py::NasMarketStore.reconcile_control_pointer` | `publish_dataset_and_pointer` | no standalone initialization writer |
| `backend/app/market/backfill.py::BackfillService.plan/execute` | `publish_manifest_only` per date/group after all date resolutions pass | explicit lineage input per `trade_date`; any resolver failure is whole-batch `SOURCE_UNAVAILABLE` before the first upsert |
| `backend/app/market/full_history.py` | `publish_dataset_and_pointer` | no lock acquisition or direct pointer write |
| `backend/app/cli.py` reconciliation | `publish_dataset_and_pointer` | no lock acquisition or direct pointer write |
| `backend/app/main.py` startup reconciliation | `publish_dataset_and_pointer` | no lock acquisition; use the coordinator |

The plain `MarketStore` path is completely unchanged. It does not acquire
`ManifestPublicationLock`, write a binding, run a guard, alter the manifest for replication or
access NAS. After its existing canonical commit, the replication observation is
`SOURCE_NOT_CONFIGURED`; replication errors cannot alter or roll back canonical success. The
retired `commit_published_snapshot_with_binding` and any `MarketStore.save_refresh` dataset-writer
entry are removed from the implementation inventory.

### 3. One lineage algorithm with no manifest-level schema change

The manifest-level schema and bytes do not gain fields. Define the exact object lineage set
`L = {provider_id,universe_id,evidence_id,evidence_sha256,candidate_id,
candidate_manifest_sha256,gate_report_sha256,adapter_version,source_schema_version}`. Before a
manifest-only mutation, read and classify the existing manifest:

1. All `L` fields absent on every object and no unknown lineage field means `legacy`; use exactly
   `domain_sha256("stock-eva/r2f4.3/selection-null/v1", None)` and
   `domain_sha256("stock-eva/r2f4.3/lineage-null/v1", None)`. The preimage is literal canonical
   JSON `null` plus one newline under each domain.
2. All `L` fields present exactly once on every object means `modern`; require strict lower-case
   64-hex/type/identity checks and exact immutable evidence/selection/gate consistency.
3. Partial presence, unknown or duplicate fields, malformed values, mixed legacy/modern objects,
   or any conflict means `SOURCE_UNAVAILABLE` before any manifest mutation. No modern manifest may
   be polluted by a backfill without exact supplied/derived immutable lineage.

An empty manifest may classify the incoming input as legacy or modern. A non-empty manifest must
inherit its existing single complete mode and lineage; a modern caller must provide or derive the
same exact immutable lineage. Reconcile follows the same algorithm and may reuse a binding only
after exact lock-bound proof. Required anchors are
`test_legacy_no_selection_uses_canonical_null_digests`,
`test_modern_v2_lineage_is_strictly_validated`,
`test_backfill_service_requires_explicit_lineage_input_per_trade_date`,
`test_backfill_service_multi_date_batch_is_atomic_on_lineage_failure`,
`test_partial_unknown_or_conflicting_v2_lineage_is_source_unavailable`, and
`test_backfill_service_multi_date_failure_preserves_manifest_and_pointer`.

### 4. Pointer linearization and phase-specific failure

Before pointer commit, the only failure phases are lock acquire, token validation, baseline read,
pre-publication guard and final manifest-fingerprint CAS. Any failure leaves pointer bytes, row
hash and DB inode/device exactly unchanged; no binding is active. The DuckDB pointer commit is the
linearization point. After it succeeds, unlock/close/post-proof/control failures MUST NOT roll back
or hide canonical `ready`; they return a degraded `ReplicationObservation` with
`CONTROL_STATE_UNAVAILABLE`, durably record sanitized reconciliation evidence when possible, and
leave the pointer ready. The next pre-publication guard reopens and reconciles the descriptor-bound
control/binding state before permitting another pointer commit. There is no vague “release
readiness” phase. Required anchors are
`test_pointer_precommit_lock_error_preserves_pointer_identity`,
`test_post_pointer_unlock_failure_degrades_without_rollback`, and
`test_pointer_postcommit_control_error_is_durable_and_next_guard_reconciles`.

The final lock/crash matrix is:

| Phase | Required effect |
|---|---|
| manifest-only, including empty DB | Update/verify manifest only; `pointer_before == pointer_after` as tagged unions. |
| lock acquire/token/baseline/guard/final-CAS before pointer | Fail closed; previous pointer bytes/hash/inode unchanged. |
| pointer transaction failure | Pointer unchanged; no active binding. |
| pointer committed, unlock/close/post-proof/control failure | Canonical ready remains; degraded `CONTROL_STATE_UNAVAILABLE`; durable reconcile; no rollback. |
| next writer after post-pointer control error | Guard proves/reconciles current state before any next pointer commit. |

The exact crosswalk contains FR-3e through FR-3j, AC-23 through AC-28 and EC-31 through EC-37 in
both documents. Status remains `In Review / NO-GO`; this consolidation is a specification update,
not implementation or production evidence.

## Final normative read-state and lineage resolver correction — R2-F4.3.7

This section is normative and supersedes every older pointer-read, nullable-lineage or inferred-
lineage sentence in both R2-F4.3 documents. It is still a specification only: it authorizes no
implementation, NAS access, provider request or production operation.

### M1 — three-state pointer read and no undefined fingerprint

`PointerIdentity` is the complete private read result. Its `PRESENT` branch contains only fields
that can be reconstructed from the canonical control database and descriptor-bound filesystem
read; no additional database identity field or alias is defined:

```typescript
type PointerIdentity =
  | Readonly<{ kind: "ABSENT" }>
  | Readonly<{ kind: "PRESENT"; row_sha256: string; device: number;
               inode: number; schema_digest: string }>
  | Readonly<{ kind: "INVALID"; reason_code: "CONTROL_STATE_UNAVAILABLE" }>;
```

`row_sha256` is the existing domain-separated hash of the exact canonical singleton-row
projection. `device` and `inode` are the `fstat` identity of the opened control DB descriptor;
`schema_digest` is the existing strict control-schema digest. `SourceCheckpoint` continues to
use its explicit `pointer_row_sha256`, `pointer_db_device`, `pointer_db_inode` and
`pointer_db_schema_digest` fields, which are the corresponding four values; no fifth fingerprint
is introduced.

`ABSENT` is legal only in exactly two cases: the control DB does not exist, or a read-only,
descriptor-bound read proves the known schema valid and proves the singleton pointer row absent.
An existing DB with a missing table, unknown/corrupt schema, unreadable/locked read, duplicate or
malformed singleton, missing required state, or any other incomplete control state returns
`INVALID{reason_code: "CONTROL_STATE_UNAVAILABLE"}`. It MUST NOT be normalized to `ABSENT`.
`publish_manifest_only` reads the tagged state before mutation and again after mutation; any
`INVALID` state fails closed before the first manifest/object mutation. A `PRESENT` state must
match exactly on all four fields, and `ABSENT` must remain `ABSENT`; no sentinel string, empty
hash, synthetic row or partial equality is accepted. The invalid-state test is zero-provider and
asserts manifest, object, pointer bytes/inode and sidecar bytes remain unchanged.

### M2/M3 — private frozen publication lineage and resolver seam

The manifest has no new fields. Define a private, frozen `PublicationLineage` with exactly these
nine immutable fields (not labels inferred from bars):

```typescript
type PublicationLineage = Readonly<{
  provider_id: string;
  universe_id: string;
  evidence_id: string;
  evidence_sha256: string;
  candidate_id: string;
  candidate_manifest_sha256: string;
  gate_report_sha256: string;
  adapter_version: string;
  source_schema_version: string;
}>;
```

`SelectionRelation` is an alias of the existing frozen
`backend/app/market/candidates.py::SessionSelection`; it is not a new model or hash contract.
Its exact fields are `selection_id`, `trade_date`, `universe_id`, `selected_candidate_id`,
`selected_provider_id`, `reason`, `fallback_from`, `evidence_sha256`,
`candidate_manifest_sha256`, `gate_report_sha256`, `selected_at` and `selection_sha256`.
The existing selection preimage is `SessionSelection.model_dump(mode="json")` with only
`selection_sha256` removed, encoded as canonical JSON with sorted keys, UTF-8, no insignificant
whitespace and no NaN, then plain SHA-256; no new domain or alternate selection digest is
introduced. The relation additionally requires `selected_candidate_id`, `selected_provider_id`,
`universe_id`, the three evidence/gate hashes and `trade_date` to equal the resolved
`PublicationLineage` and requested date, with `reason="primary_ready"` and
`fallback_from` absent. The existing `publication_lineage_sha256` is exactly
`domain_sha256("stock-eva/r2f4.2/publication-lineage/v2", <the nine-field lineage object>)`;
the replication binding's `selection_sha256`/`lineage_sha256` fields are aliases of those two
existing values, not a second hash scheme. The relation is checked before any manifest mutation.

The only resolver seam is the strict local `LineageResolver` (planned in
`backend/app/storage/replication.py`), whose result is the explicit discriminated union below;
`null` is never returned or used to mean either legacy or unavailable:

```typescript
type LineageInput =
  | Readonly<{ mode: "legacy" }>
  | Readonly<{ mode: "modern"; exact: PublicationLineage }>
  | Readonly<{ mode: "modern"; candidate_id: string; evidence_id: string }>;

// These are additive fields/signature constraints on the existing Pydantic
// models in backend/app/market/backfill.py; they do not introduce a runner or
// replace BackfillPlan/BackfillRunRecord.
type BackfillBatchPlan = Readonly<{
  index: number;
  start_date: string; end_date: string;
  trading_dates: ReadonlyArray<string>;
  symbols: ReadonlyArray<string>;
  requested_points: number;
  estimated_provider_requests: number;
  lineage_by_trade_date: Readonly<Record<string, LineageInput>>;
}>;

interface BackfillService {
  plan(input: Readonly<{
    start_date: string; end_date: string; symbols: ReadonlyArray<string>;
    symbol_batch_size: number; date_batch_size: number; max_batches: number;
    trading_dates?: ReadonlyArray<string>;
    lineage_by_trade_date: Readonly<Record<string, LineageInput>>;
  }>): BackfillPlan;
  execute(plan: BackfillPlan, options: Readonly<{
    min_request_interval_seconds: number;
  }>): BackfillRunRecord;
}

type LineageResolveResult =
  | Readonly<{ kind: "LEGACY"; selection_sha256: string; lineage_sha256: string }>
  | Readonly<{ kind: "MODERN"; lineage: PublicationLineage; selection: SessionSelection }>
  | Readonly<{ kind: "UNAVAILABLE"; reason_code: "SOURCE_UNAVAILABLE" }>;

interface LineageResolver {
  resolve(input: LineageInput, trade_date: string,
          existing_mode: "empty" | "legacy" | "modern"): LineageResolveResult;
}
```

For `mode="modern"; exact`, the resolver validates every supplied digest, candidate, selection,
evidence and gate reference against local retained successful evidence. The ID form may load only
by allowlisted `candidate_id`/`evidence_id` from those same local readers and then performs the
same full validation. It MUST NOT call a provider, use labels, inspect bars to guess lineage, or
inherit any unverified field. Missing evidence, unavailable readers, digest mismatch, selection
relation mismatch or unknown IDs returns `SOURCE_UNAVAILABLE` before manifest mutation.

`LineageResolveResult` is a discriminated union; no `null` result has semantic meaning. `LEGACY`
contains the already-defined explicit legacy/no-selection digest pair and never fabricates a
`SessionSelection`; `MODERN` contains both the nine-field lineage and the exact F4.2
`SessionSelection`; `UNAVAILABLE` contains only `reason_code="SOURCE_UNAVAILABLE"`. Existing
non-empty legacy manifests accept only `mode="legacy"`; modern input is rejected. Existing
non-empty modern manifests accept only a fully validated exact same lineage (caller-supplied or
resolver-loaded); legacy, partial and conflicting input is rejected. An empty manifest may choose
legacy or modern, but modern still requires exact refs or the allowlisted resolver path. This
decision is made before object staging or manifest replacement.

`BackfillService.plan` MUST retain the existing `BackfillPlan`/`BackfillBatchPlan` shape and build
each batch's `trading_dates` in ascending order. The required `lineage_by_trade_date` input is
partitioned into each batch and MUST contain exactly one entry for every date in that batch (no
missing, extra or duplicate date key; a duplicate is invalid before execution). The existing
`BackfillService.execute(plan, *, min_request_interval_seconds)` remains the execution seam and
MUST run the resolver once for every date in each batch, in ascending `trade_date` order, retaining
the resulting `LineageResolveResult` values until all dates in that batch are validated. Only
after every date is `LEGACY` or `MODERN` may it fetch/validate provider bars and call
`NasMarketStore.upsert_bars` per date/group. Any `UNAVAILABLE` result records that batch as
`status="error", error_code="SOURCE_UNAVAILABLE"` in the existing `BackfillAuditStore`, performs
zero upserts/manifest/object/pointer mutation for that batch, and never validates one date and
then mutates another. A date's lineage input is never inferred from another date, labels or bars.

The former optional lineage argument is replaced by required
`lineage_input: LineageInput` on `ManifestOnlyInput`, `DatasetPointerInput`,
`NasMarketStore.upsert_bars(..., *, lineage_input)`, and each `BackfillBatchPlan` date mapping. A legacy
caller passes the explicit legacy branch; a modern caller passes exact refs or the two allowlisted
IDs. There is no default that silently selects legacy. `publish_manifest_only` resolves and
validates this input before its lock-held manifest mutation; `publish_dataset_and_pointer` uses
the same resolver before binding/pointer work. Plain `MarketStore` signatures and behavior remain
unchanged and never invoke this resolver.

The current production implementation path is `BackfillService.execute` in
`backend/app/market/backfill.py`; it is the only allowed caller that fans a multi-date plan into
`NasMarketStore.upsert_bars`. Any direct dataset-store caller must choose the explicit legacy
branch or the exact/allowlisted modern branch at the call site. `MarketStore.upsert_bars` and all
other plain-store APIs retain their existing signatures and behavior and are not adapted through
this service.

Concrete implementation seams are `backend/app/storage/dataset.py` for the coordinator and
descriptor-bound pointer reader, `backend/app/storage/replication.py` for the frozen lineage and
resolver, `backend/app/market/evidence.py` and `backend/app/market/candidates.py` for strict
retained-evidence readers, and `backend/app/market/backfill.py` for the explicit batch signature.
The required evidence anchors are
`test_manifest_only_invalid_pointer_identity_fails_closed`,
`test_lineage_resolver_returns_union_and_reuses_session_selection_hash_contract`,
`test_modern_backfill_requires_exact_lineage_or_resolver_evidence`,
`test_lineage_resolver_rejects_labels_bars_and_unverified_inheritance`, and
`test_missing_lineage_reader_returns_source_unavailable_before_mutation`.

## R2-F4.3.9 tenth-round normative amendment — CLI BackfillService store split

This is the final normative amendment for the ordinary `backfill` command. It supersedes every
earlier backfill/CLI sentence that implies a `Record` map is the input boundary, that lineage is
silently defaulted, that the plain `MarketStore` needs lineage, or that a dataset plan may call a
provider/audit writer before all date lineage has been admitted. It is still specification only:
no implementation, provider request, NAS access, database write or production enablement is
authorized by this section.

### 1. Local lineage-file grammar and ordered parser boundary

`backend/app/cli.py` adds an optional `backfill --lineage-input PATH` flag. `PATH` is a local,
regular, non-symlink file opened with a no-follow descriptor; URLs, credential references,
environment expansion, `HOME`/root paths and unresolved paths are rejected. The file is private
operator input: its path, JSON content, identifiers and raw parser exception are never printed or
logged. The exact JSON shape is an object with only these fields:

```json
{
  "schema": "stock-eva/r2f4.3/backfill-lineage/v1",
  "entries": [
    {"trade_date": "2026-09-07", "lineage_input": {"mode": "legacy"}},
    {"trade_date": "2026-09-08", "lineage_input": {"mode": "modern", "candidate_id": "<id>", "evidence_id": "<id>"}}
  ]
}
```

`entries` is deliberately an ordered JSON array, never a JSON object keyed by date. The parser
uses an ordered `(key, value)` object-pairs boundary and rejects duplicate keys in the top-level
object, each entry or each `lineage_input` object before converting any object to a `dict`. It
returns `tuple[(trade_date, LineageInput), ...]` in file order. It then requires strict ISO dates,
ascending order, exactly one entry per expected confirmed `trade_date`, no missing date/gap and no
extra date, before constructing `lineage_by_trade_date`. Unknown fields, malformed modes,
duplicate entries and missing/unreadable files return the sanitized CLI error projection
`status="error", error_code="BACKFILL_LINEAGE_INPUT_INVALID"` or
`BACKFILL_LINEAGE_INPUT_UNAVAILABLE`, exit code `2`, `provider_requests=0`, and every write/effect
flag false. No provider is constructed or called on this failure path.

The expected dates are obtained before provider construction from the strict local calendar
snapshot (`get_trading_calendar().snapshot().confirmed_open_sessions`). For `--effective-days`,
the same local snapshot selects the bounded final sessions; if the snapshot cannot prove the
range, the command returns `SOURCE_UNAVAILABLE` with zero provider requests and zero writes. The
file parser receives this expected ordered tuple, so date gap/extra detection is complete before
any BaoStock calendar query, `validate_readiness`, reconciliation, audit initialization or market
data request.

### 2. `main()` store split and exact NAS admission order

The ordinary `backfill` branch in `backend/app/cli.py` resolves settings/layout and identifies the
store type first, then dispatches before the generic NAS `validate_readiness()`/
`reconcile_control_pointer()` block and before constructing `BaoStockProvider`:

1. For a plain `MarketStore`, a supplied `--lineage-input` is rejected as
   `BACKFILL_LINEAGE_INPUT_NOT_SUPPORTED_FOR_PLAIN_STORE`, exit `2`, with
   `provider_requests=0` and zero writes. Without the option, the existing plain-store
   `BackfillService.plan(...)` and `BackfillService.execute(...)` path, provider calendar lookup,
   audit behavior, output shape and status mapping remain unchanged. It does not instantiate the
   lineage resolver or coordinator.
2. For a `NasMarketStore`, the command first derives the local expected dates and performs the
   ordered lineage-file parse above. If the flag is omitted, it performs a strict read-only
   manifest-mode read: only a proven existing `legacy` mode may be converted into an explicit
   `LineageInput{mode="legacy"}` pair for every expected date; an absent/empty or invalid mode
   requires the flag, and an existing `modern` mode also requires the flag because modern input
   must carry evidence references for every date. This is an explicit `MANIFEST_LEGACY` branch,
   never a silent default. No `Record` conversion occurs until all parser checks pass.
3. The admitted ordered pairs are passed to `BackfillService.plan_dataset(...)`; this method
   retains the existing `BackfillPlan`/`BackfillBatchPlan` models, partitions the pairs by the
   existing batch dates, and rejects any missing, extra, duplicate or cross-date mapping. It uses
   the local date tuple and makes no provider request. The plan is rejected before any audit,
   manifest, object or pointer write if its mapping is not exact.
4. `BackfillService.execute(...)` performs the strict `LineageResolver` preflight for every date
   in every dataset batch, ascending by `trade_date`, retaining all discriminated results before
   the first provider fetch. A provider object may be constructed after parser/plan admission,
   but until every result is `LEGACY` or `MODERN` it MUST NOT open a provider session, login or
   request data; initialize/record `BackfillAuditStore`, upsert bars, stage objects, mutate
   `manifest.json` or commit the DuckDB pointer. Any `UNAVAILABLE/SOURCE_UNAVAILABLE` result
   returns the existing sanitized run projection with `provider_requests=0`, zero effects and
   unchanged manifest/pointer/sidecar bytes. After all dates pass, the existing readiness/control
   checks may run, provider fetches may begin, and every `NasMarketStore.upsert_bars` invocation
   receives the resolved explicit input for its own `trade_date`; no date may inherit another
   date's lineage.

### 3. Real `BackfillService` API compatibility

`backend/app/market/backfill.py::BackfillService` remains the only backfill service; no alternate
runner class or generic callback is introduced. Its existing plain-store methods retain their
current contract, while the dataset path adds a separate method so a required lineage argument
cannot alter plain callers:

```typescript
interface BackfillService {
  // Existing plain MarketStore contract: unchanged signature and behavior.
  plan(input: ExistingPlainBackfillPlanInput): BackfillPlan;
  // Dataset-only overload; trading_dates are already proven by the local calendar.
  plan_dataset(input: Readonly<{
    start_date: string; end_date: string; symbols: ReadonlyArray<string>;
    symbol_batch_size: number; date_batch_size: number; max_batches: number;
    trading_dates: ReadonlyArray<string>;
    lineage_pairs: ReadonlyArray<Readonly<{
      trade_date: string; lineage_input: LineageInput;
    }>>;
  }>): BackfillPlan;
  // Existing return model and interval option remain unchanged.
  execute(plan: BackfillPlan, options: Readonly<{
    min_request_interval_seconds: number;
  }>): BackfillRunRecord;
}
```

`plan_dataset` is valid only when the service store is `NasMarketStore`; the plain `plan` is valid
only for `MarketStore` and must reject a dataset plan. Every `BackfillBatchPlan` created by
`plan_dataset` carries a `lineage_by_trade_date` mapping only after the ordered-pairs boundary has
proved uniqueness and exact date coverage. `execute` resolves all mappings before any side effect;
the provider bars are then split by their exact `trade_date` and the corresponding explicit
`lineage_input` is passed to `NasMarketStore.upsert_bars(..., *, lineage_input)`. A direct
dataset-store caller must likewise pass explicit `mode="legacy"` or the exact/allowlisted modern
input. `MarketStore` APIs and their legacy call sites remain untouched.

### 4. Required CLI and service evidence

The following are exact planned behavior tests, in addition to the existing R2-F4.3 resolver and
publication tests:

- `tests/test_market_backfill.py::test_cli_nas_lineage_parser_preserves_order_and_rejects_duplicate_before_dict`
- `tests/test_market_backfill.py::test_cli_nas_lineage_missing_duplicate_invalid_is_zero_provider_zero_write`
- `tests/test_market_backfill.py::test_cli_nas_lineage_gap_or_extra_is_zero_provider_zero_write`
- `tests/test_market_backfill.py::test_cli_nas_valid_lineage_all_dates_admitted_before_first_provider_fetch`
- `tests/test_market_backfill.py::test_cli_nas_existing_legacy_manifest_selects_explicit_legacy_branch`
- `tests/test_market_backfill.py::test_backfill_service_dataset_preflight_fails_before_audit_or_provider`
- `tests/test_market_backfill.py::test_cli_plain_market_store_backfill_legacy_behavior_is_unchanged`
- `tests/test_market_backfill.py::test_cli_plain_market_store_rejects_lineage_input`

These tests must assert `provider_requests=0`, provider-call count `0`, canonical manifest/object/
pointer and audit-sidecar bytes/inodes unchanged for NAS missing/duplicate/invalid/gap/extra and
resolver-failure cases; assert every expected date is admitted before the first provider fetch for
the valid-NAS case; assert the pre-existing plain-store CLI plan/execute behavior byte/behavior
compatible; and assert the plain-store option rejection occurs before provider construction.
The two crosswalks above are updated to reference these exact anchors. The R2-F4.3 status remains
`In Review / NO-GO`; this amendment does not claim implementation or production enablement.

#### Batch1.7 immutable-generation integrity and threat-model amendment (normative)

This amendment supersedes conflicting Batch1.6 sidecar-generation wording. Batch1.7 protects the
cooperative Stock EVA writers, process crashes, accidental corruption, and symlink/path trust. It
does not claim to defend against a same-UID uncooperative process that continuously changes the
namespace between atomic syscalls. Hostile rollback or deletion of the chain tail cannot be proven
without an external trust anchor; this batch has no external anchor. A locally complete chain may
therefore report `ready` only with public `trust_scope="LOCAL_CHAIN_ONLY"`; that value means
internally verified, not externally anchored or rollback-proof.

The only generation commit linearization point is installation of the fixed zero-padded basename
with no-replace hard-link (or an equivalent checked rename-excl). Lock, callback, baseline and
auxiliary namespace checks happen before that point. A normal cooperative precheck failure writes
no generation. If an uncooperative process inserts an entry after the last precheck but before the
link and the link succeeds, the generation is nevertheless committed by definition: it MUST be
retained, the operation returns a sanitized degraded
`ReplicationPostCommitConflict(reason_code="CONTROL_STATE_UNAVAILABLE")`, and later readers fail
closed on the namespace conflict. The implementation MUST NOT claim that this race had zero write.

Every generation `replication_sidecar_meta` row has the closed additions
`generation_number INTEGER`, `previous_generation_sha256 TEXT`, and
`generation_payload_sha256 TEXT`. `generation_number` equals the filename sequence; generation
zero uses `previous_generation_sha256=64-zero-hex`; every later generation stores the lower-case
SHA-256 of the complete previous generation bytes. `generation_payload_sha256` is
`domain_sha256("stock-eva/r2f4.3/replication-generation-payload/v1", preimage)` where `preimage`
is the closed object `{generation_number,previous_generation_sha256,tables}`. `tables` is ordered
as `(replication_sidecar_meta,replication_intents,replication_destination_cache,
replication_attempt_events,replication_heads)`; each item contains the exact `PRAGMA table_info`
column order and rows ordered by `rowid`; the `generation_payload_sha256` cell itself is excluded
from that one table projection, and SQLite values use canonical JSON (bytes are tagged lower-case
hex). No other field is excluded. This avoids circular hashing while binding all logical content.

Readers recompute this digest, require metadata sequence equal to the filename, require the exact
previous-byte digest, and validate every generation in the complete contiguous chain before
selecting the highest one. A changed middle generation, legal old snapshot substitution, or hash
field edit is unavailable. The DDL trigger permits only the private construction transition from
the all-zero payload digest to its computed digest before no-replace installation; installed
generation files and all non-placeholder metadata are immutable.

Genesis remains a separate immutable record. Initialization is deterministic and resumable: under
the cooperative lock, install `genesis.json` no-replace first, then install generation zero
no-replace. A crash between those points leaves a valid genesis with no generation; the next
initializer verifies the same source/lock identity and deterministically resumes generation zero.
It MUST never create a second genesis or replace either artifact. Lock device/inode/link-count is
only cooperative serialization and tamper evidence, not the security root; replacement discovered
by a post-linearization proof returns degraded `CONTROL_STATE_UNAVAILABLE` and never replaces an
immutable generation.

All sidecar cleanup uses one best-effort accumulator: every temporary unlink, fd close, directory
close, lock unlock and connection close is attempted independently. A cleanup-only failure raises
sanitized `ReplicationDurabilityError`; with a primary failure it adds only a bounded
`replication_cleanup=failed` note and never exposes a path or raw OS exception.

The exact behavior anchors are
`tests/test_dataset_replication.py::test_generation_metadata_binds_sequence_parent_and_content_hash`,
`tests/test_dataset_replication.py::test_generation_parent_chain_tamper_is_unavailable`,
`tests/test_dataset_replication.py::test_genesis_partial_initialization_resumes_deterministically`,
`tests/test_dataset_replication.py::test_same_next_generation_multiprocess_has_one_no_replace_winner`,
`tests/test_dataset_replication.py::test_postlinearization_namespace_conflict_preserves_generation_and_degrades`,
`tests/test_dataset_replication.py::test_sidecar_generation_auxiliary_appearance_after_install_is_degraded_and_immutable`,
and `tests/test_dataset_replication.py::test_sidecar_generation_lock_replacement_after_install_is_degraded_and_immutable`.
The amendment remains implementation-only for Batch1: no NAS transfer, restore, provider request,
or production enablement is authorized.

#### Batch1.8 status trust scope and cleanup finalization (normative)

`ReplicationStatusResponse` has the closed public field
`trust_scope: "LOCAL_CHAIN_ONLY"`. It is present in disabled, ready, degraded and unavailable
responses, is fixed by the serializer and is validated by the strict public model. No remote,
external-attested or destination trust value is admitted in this batch. The field is a bounded
capability statement and never contains a path, credential, payload or exception string.

Every descriptor-owning helper supplies one cleanup accumulator to `_close_descriptors`; no
sidecar path may call it without an accumulator or ignore its returned failures. The read,
optional-read, stat, fsync, generation, lock, and status paths capture a typed primary failure
before entering `finally`, then independently attempt every artifact, directory, memory
connection, lock and temporary-fd close. Cleanup cannot replace a primary error; it adds only the
sanitized note `replication_cleanup=failed`. With no primary error, any cleanup failure raises a
typed `ReplicationDurabilityError`. Required runtime anchors are
`tests/test_dataset_replication.py::test_public_status_exposes_fixed_local_chain_trust_scope`,
`tests/test_dataset_replication.py::test_descriptor_cleanup_failure_after_primary_read_error_is_recorded`,
`tests/test_dataset_replication.py::test_sidecar_entry_cleanup_failure_preserves_typed_primary`,
and `tests/test_dataset_replication.py::test_descriptor_cleanup_failure_without_primary_is_typed`.
The API/crosswalk/model change is additive to the local status projection only; canonical market
data, NAS transfer and provider behavior remain unchanged.

#### Batch2.1 outbox completion and descriptor-native staging amendment (normative)

The generic local `transition` operation MUST reject `to_state="replicated"` before opening or
mutating a sidecar generation. Caller-supplied destination generation, record or head digests do
not constitute destination authority and can never authorize completion. The dedicated
`complete_replication(VerifiedDestinationCommitProof)` seam remains distinct from generic local
transitions; Batch3 supplies a frozen closed proof projection, not a secrecy-based private token.
Only a configured `DestinationCommitVerifier` that freshly rereads and verifies the persisted
descriptor, head, record and every object may authorize completion of a leased intent. Existing
generic transition tests MUST still prove zero sidecar writes and preservation of the leased
`verifying` head.

`import_journal_files` MUST open the explicit absolute journal root once through its trusted
descriptor chain and retain that root dirfd through enumeration, every `O_NOFOLLOW` child read,
sidecar import, every `unlinkat`-equivalent journal removal and final directory fsync. It MUST
capture and re-prove the held root's `(device,inode,mode,nlink)` before and after each phase (the
expected link count is adjusted only for successful regular-file unlink). It MUST never reopen the
root pathname after the initial descriptor bind; pathname replacement therefore cannot redirect a
read or deletion to a new directory. Journal child bytes, regular type, mode `0600` and link count
are also checked, and all public errors remain sanitized.

The sidecar root allowlist includes the fixed descriptor-native directory `.staging` in addition to
`.writer.lock`, `genesis.json` and zero-padded generation basenames. Staging entries are limited to
`.<20-digit-generation>.db.tmp`, regular mode-`0600` files. A status reader performs no cleanup:
it may ignore a staging entry only when the matching generation is present and the staging file is
provably the same hardlink (identical bytes/hash, device, inode, mode and link count exactly `2`).
Any pre-link orphan, unknown name, symlink, wrong type/mode, mismatched bytes or unexpected link
count makes status unavailable without writes. A locked writer may remove only a validated regular
pre-link orphan before its next mutation; it never removes an unknown or unsafe entry.

Generation writes place the deterministic temporary file in `.staging` through held dirfds, then
link the fixed generation basename into the root with no-replace semantics. The generation is the
linearization point. If cleanup of a staged hardlink fails after a successful link, the operation
returns sanitized degraded `CONTROL_STATE_UNAVAILABLE`; future status remains readable only when
the strict hardlink-alias proof above succeeds. Crash/subprocess tests cover this case, and a
same-`source_published_at` journal batch is ordered by `(source_published_at,checkpoint_id)`.
The concrete anchors are
`tests/test_dataset_replication.py::test_generic_transition_cannot_complete_replication_without_dedicated_proof`,
`tests/test_dataset_replication.py::test_journal_import_uses_one_held_root_descriptor_across_path_replacement`,
`tests/test_dataset_replication.py::test_committed_generation_staging_hardlink_residue_is_ignored_read_only`,
`tests/test_dataset_replication.py::test_prelink_staging_orphan_blocks_status_then_writer_cleans_it`,
`tests/test_dataset_replication.py::test_link_then_process_crash_leaves_proven_committed_staging_alias`,
`tests/test_dataset_replication.py::test_staging_unlink_failure_is_degraded_but_proven_alias_remains_readable`,
and `tests/test_dataset_replication.py::test_checkpoint_journals_import_sorted_by_published_at_and_checkpoint_id`.

#### Batch3 destination archive amendment (offline implementation)

`backend/app/storage/replication.py` now contains the explicit-root
`DestinationDescriptor`, descriptor-native `DestinationArchiveReader`, and
`DestinationArchiveWriter` used by offline fake-destination tests. It stages the complete
inventory, manifest and sentinel, verifies size/hash readback, installs an immutable
`replication-record.json` generation with the normative closed fields, and atomically CAS
advances `_replication/head.json` under the descriptor-bound single-writer lock. History
generation installation uses same-parent no-replace primitives (`renameatx_np(RENAME_EXCL)`/
`renameat2` where available, otherwise exclusive hard-link installation for regular files); head
creation uses exclusive hard-link installation and an existing-head CAS uses an atomic native
exchange. There is no stat-then-rename or unconditional `os.replace` fallback; unsupported
filesystems return typed `MOUNT_UNSUPPORTED`. A legacy descriptor missing normalized-options or
volume-id is never adopted or silently rewritten; only an explicitly empty destination may be
initialized with a new descriptor. The reader recomputes record/head hashes, checks
parent lineage and rejects symlinks, extras, missing or changed objects. The persisted descriptor
includes normalized mount-options, volume-id and configured single-writer host identity; each
operation rereads and byte-compares it before mutation. `VerifiedDestinationCommitProof` is a
frozen closed projection and the dedicated sidecar completion seam invokes a configured
descriptor-native verifier for fresh descriptor/head/record/object readback; generic local
transitions still reject `replicated`.
The Batch3 attack/regression anchors are
`tests/test_replication_destination_batch3.py::test_writer_revalidates_persisted_descriptor_and_host_before_mutation`,
`tests/test_replication_destination_batch3.py::test_history_pollution_is_rejected_before_first_no_head_mutation`,
`tests/test_replication_destination_batch3.py::test_history_staging_orphan_is_rejected_before_mutation`,
`tests/test_replication_destination_batch3.py::test_orphan_generation_is_recovered_without_recopied_source`,
`tests/test_replication_destination_batch3.py::test_orphan_child_generation_is_recovered_without_recopied_source`,
`tests/test_replication_destination_batch3.py::test_completion_rejects_unconfigured_or_mutated_proof_before_sidecar_write`,
`tests/test_replication_destination_batch3.py::test_destination_head_install_never_uses_unconditional_replace`,
`tests/test_replication_destination_batch3.py::test_destination_no_replace_install_unsupported_is_typed_and_zero_head_write`,
and `tests/test_replication_destination_batch3.py::test_destination_writer_requires_explicit_configured_host_identity`.
This batch remains offline-only: no NAS/SMB, provider, restore, API/CLI/automation wiring or
production enablement is included.

#### Batch3.2 live-boundary, claim binding and effect semantics (normative)

This amendment supersedes any conflicting Batch3 wording above. Every destination root session
starts from two independent proofs: the canonical persisted descriptor bytes/hash read through the
held root descriptor, and a fresh live `SystemMountInspector` probe. The persisted descriptor is
only an expectation; it is not a substitute for the live probe. The probe canonicalizes the
mountpoint, lower-case filesystem type, de-duplicated lexicographically sorted options, volume id,
and held-root device identity, then compares all values to the persisted descriptor. Tests may
inject a local fake `MountInspector` with the same contract; no real NAS or mount operation is
permitted.

The same descriptor-native persisted/live comparison is performed immediately before and after
each destination write boundary: staging-directory creation, every staging object/manifest/
sentinel/record write and readback, generation-directory no-replace installation, history fsync,
head CAS installation, and post-CAS head/readback. A pre-boundary mismatch stops all later writes;
if an earlier staging/object/generation mutation occurred, the sanitized result sets
`destination_writes=true`, otherwise it is false. A mismatch after the head linearization point
does not remove or rewrite the head: the result is degraded `CONTROL_STATE_UNAVAILABLE` with
`destination_writes=true`, and the immutable generation remains the only recoverable artifact.

Sidecar-owned execution uses the frozen, closed `ReplicationClaimContext` model with exactly
`intent_id`, `worker_id`, `state_version`, `checkpoint_id`, `source_instance_id`, and
`source_instance_sha256`. `DestinationArchiveWriter.replicate(..., claim_context=...)` requires
this context whenever an intent is supplied, rejects non-model/extra fields, and compares the
context checkpoint and source identities to the complete `SourceCheckpoint` before opening the
destination write session. The returned `VerifiedDestinationCommitProof` carries the same intent,
worker, state-version, source-instance id and source-instance digest values; `DestinationCommitVerifier`
and sidecar completion reread the current destination and leased sidecar state, so callers cannot
supply arbitrary restart values.
Restart reconciliation derives its claim identity from the current sidecar claim/head and the
strict checkpoint projection; a stale worker or proof cannot advance the head.

`destination_writes` is a monotonic operation-effect bit, not a success bit: it becomes true as
soon as staging, an object/record, a generation directory, or the head is actually installed and
never returns to false. A CAS conflict after generation installation therefore reports
`CAS_CONFLICT` with `destination_writes=true`; failures before the first destination mutation
report false. The canonical market dataset, manifest, pointer, provider behavior, NAS transfer
enablement, and automatic failover remain unchanged and out of scope.

The exact Batch3.2 behavior anchors are
`tests/test_replication_destination_batch3.py::test_mount_identity_drift_before_staging_is_zero_write`,
`tests/test_replication_destination_batch3.py::test_live_mount_identity_fields_are_independently_fail_closed`,
`tests/test_replication_destination_batch3.py::test_mount_identity_drift_after_staging_is_degraded_with_effect`,
`tests/test_replication_destination_batch3.py::test_claim_context_binds_writer_proof_for_sidecar_completion`,
`tests/test_replication_destination_batch3.py::test_claim_context_checkpoint_mismatch_is_zero_write`, and
`tests/test_replication_destination_batch3.py::test_cas_conflict_after_generation_install_reports_destination_effect`.
The plan remains `In Review / NO-GO` until the complete release gates close; this amendment does
not authorize production or real-NAS execution.
