# Stock EVA R2-F4.3 Local Replication and Verified Restore

**Author:** Codex

**Date:** 2026-09-09 (Asia/Shanghai)

**Status:** In Review / NO-GO pending independent review of the R2-F4.3.5 final normative consolidation; implementation remains blocked

**Reviewers:** Stock EVA architecture, data reliability and operations reviewers

**Related specifications:** R2-F0.1 Provider Transport Stabilization, R2-F1 Continuity
Controller, R2-F2 Provider Evidence, R2-F3 Daily Bar Shadow, R2-F4.0 Capability-Gated Selection,
R2-F4.1 Promoted Calendar, R2-F4.2 Exact-session Universe

## Context

Task 17 of the R2-F reliability roadmap identifies a concrete durability gap: the local
canonical dataset can publish a valid manifest/pointer, but that success is not yet represented
by a durable outbound replication queue. The current `NasMarketStore` and
`MarketDatasetMirror` already validate immutable Parquet, row counts, schema and SHA-256, and
publish `manifest.json` last. The existing mirror is intentionally NAS-to-local and must not be
silently reused as a reverse copy path.

The current publication chain has two local commit surfaces. A dataset-backed publication first
renames validated immutable objects and atomically publishes the local manifest, then commits the
local DuckDB external publication pointer. A plain `MarketStore` commits its refresh audit and
published snapshot in one local DuckDB transaction. Neither surface owns a durable NAS intent.
Therefore an enqueue failure or process crash after a successful local pointer must not make a
valid local publication fail, disappear, or be retried by fetching new market data.

The existing NAS documentation records an operational constraint: macOS background processes
cannot assume access to `/Volumes/Stock` because of TCC. NAS availability must therefore be
observable and retryable without delaying local readiness. Any real SMB copy, destination
initialization, or restore is a later, separately authorized operation; this specification and
its tests use isolated local fixtures and fake mount/readback boundaries only.

R2-F4.3 adds a local durable outbox, an explicit local-to-NAS replication direction, strict
destination trust and lineage, verified staged transfer, read-only status, dry-run-first CLI
commands, and a temporary-root restore verifier. It does not change Normalize, Quality Gate,
immutable canonical object creation, manifest/pointer semantics, provider selection, or the
existing NAS-to-local mirror contract.

The single-authority rule is normative: NAS `_replication/history/<replication_generation>/`
`replication-record.json` plus `_replication/head.json` are the destination archive/history and
current-head authority. Local SQLite is only a queue, immutable attempt audit and last-observed
cache; it can never select, promote, restore or declare a NAS generation current.

### Inherited reliability constraints reviewed

The design inherits and does not weaken the predecessor contracts: R2-F0.1 transport failures
are typed and fail closed; R2-F1 continuity/control readers are read-only and never invent dates;
R2-F2 evidence, candidate, gate and manifest references are immutable and hash-bound; R2-F3
shadow work is isolated from canonical publication and remains zero-write unless explicitly
authorized; R2-F4.0 capability admission is default-off and cannot widen provider authority;
R2-F4.1 calendar authority is point-in-time and conflict-safe; and R2-F4.2 Universe sidecar/API/
CLI readers are strict, sanitized and zero-write. Replication is downstream of a committed
canonical pointer and cannot become a new source of market truth.

## Functional Requirements

- FR-1: Replication MUST be an explicitly enabled capability with `STOCK_EVA_REPLICATION_ENABLED=false` as the default; disabled mode MUST perform zero outbox, destination, restore and provider work.
- FR-1a: `replication_enabled` MUST only authorize local checkpoint capture/enqueue. Automated NAS draining additionally requires `STOCK_EVA_MARKET_REPLICATION_DRAIN_ENABLED=true`, a validated approved descriptor and the project writer lock; the setting defaults to false and the CLI `--execute` is an independent explicit operator authorization.
- FR-2: A successful local canonical publication MUST define a deterministic replication intent from the committed local manifest/pointer identity; replication MUST observe the committed pointer and MUST NOT fetch or republish market data.
- FR-2a: After manifest success and before a dataset-backed DuckDB pointer commit, the canonical owner MUST create the immutable private `<local_dataset>/_replication/source-commits/<run_id>.json` publication-binding record, fsync its bytes and parent directories, and require the pointer to match it exactly; this additive artifact MUST NOT change canonical manifest or pointer schemas.
- FR-3: The canonical publication transaction MUST commit before any outbox enqueue attempt; an enqueue failure MUST NOT roll back, hide, or change a ready local pointer.
- FR-3a: Before every next canonical pointer commit, a refresh-lock durability guard MUST prove or durably establish the current singleton checkpoint; failure MUST block only the next commit with `OUTBOX_DURABILITY_UNAVAILABLE` and MUST preserve the current ready pointer.
- FR-3b: Every dataset-backed `NasMarketStore`/dataset pointer writer seam (`save_refresh`, reconcile-control-pointer, CLI reconciliation, `full_history` and legacy `save_external_publication` callers) MUST route through `publish_dataset_and_pointer`; no direct pointer write may bypass the binding, manifest publication lock or pre-publication guard. The plain `MarketStore` canonical commit MUST remain byte/behavior compatible and MUST not acquire this binding/guard or access NAS; its post-commit replication observation is `SOURCE_NOT_CONFIGURED` when no explicit dataset/manifest is present.
- FR-3c: The dataset-backed central wrapper MUST acquire one non-reentrant opaque `ManifestPublicationLock` token before baseline manifest read and hold it through object/staging writes, manifest replace/readback, publication-binding write/fsync, DuckDB pointer commit and post-commit exact proof. `backfill`, `upsert`, `refresh`, reconcile and every other manifest mutation MUST use the same lock; `RefreshRunLock` is scheduling only and MUST NOT be the atomicity dependency. A final manifest fingerprint CAS check immediately before pointer commit MUST leave the pointer unchanged on drift.
- FR-3d: Legacy/no-selection publications MUST use fixed domain-separated canonical-null selection/lineage digests; missing required v2 manifest metadata MUST return `SOURCE_UNAVAILABLE` and leave the pointer unchanged. A legal reconcile may reuse a binding only after the same lock-bound exact proof.
- FR-3e: One `ManifestPublicationCoordinator` MUST expose exactly two mutually exclusive typed operations: `publish_manifest_only(...)` (manifest update/verification only, no binding/pointer) for every `NasMarketStore.upsert_bars` and `BackfillService` batch, and `publish_dataset_and_pointer(...)` (manifest → binding → pointer) for dataset publication. Both may share only a token-checked locked primitive; manifest-only MUST NOT call the pointer operation.
- FR-3f: Lock acquisition/token/baseline/guard/final-CAS failures before pointer commit MUST leave pointer bytes/hash/inode unchanged. After pointer commit, unlock/close/post-proof/control failures MUST NOT roll back canonical ready; they return degraded `CONTROL_STATE_UNAVAILABLE`, durably reconcile, and are handled by the next guard.
- FR-3g: `publish_manifest_only` MUST return a typed three-state pointer proof: `ABSENT` only when the DB is absent or its proven-valid schema has no singleton row; `PRESENT{row_sha256,device,inode,schema_digest}` for a complete singleton; and `INVALID{reason_code=CONTROL_STATE_UNAVAILABLE}` for missing tables, corrupt schema, read failure, duplicate/malformed singleton or incomplete state. `INVALID` MUST fail closed before any manifest mutation, and `ABSENT` MUST never be a forced string, empty hash or synthetic row.
- FR-3h: Before any manifest-only mutation, the coordinator MUST read the existing manifest lineage mode and invoke the discriminated-union `LineageResolver` for every `trade_date` in a `BackfillService` batch. An empty manifest may classify the incoming input as legacy or modern; a non-empty manifest MUST retain one complete mode/lineage. Any missing, unavailable or mismatched date resolution MUST fail the entire batch with `SOURCE_UNAVAILABLE` before the first upsert; it MUST never partially mutate or contaminate a modern manifest. Every existing `NasMarketStore.upsert_bars` caller MUST pass an explicit legacy input or an exact/allowlisted modern input; plain `MarketStore` is unchanged.
- FR-4: The outbox MUST be a local durable SQLite sidecar with the normalized schema, immutable intent identity, allowlisted states, state-version CAS and durable terminal audit defined in this document.
- FR-5: An enqueue failure MUST be observable as a sanitized `OUTBOX_ENQUEUE_FAILED` or `OUTBOX_JOURNALED` projection; a crash between pointer commit and enqueue MUST be recoverable by comparing the current strict source manifest with durable outbox/journal state.
- FR-5a: Journal installation MUST use an `O_EXCL|O_NOFOLLOW` temporary, file/directory fsync, macOS `renameatx_np(RENAME_EXCL)` or portable hard-link-no-replace fallback; an existing target may be reused only when byte-identical and MUST never be overwritten.
- FR-6: Source discovery MUST use one descriptor-bound strict snapshot of the local immutable sentinel, manifest and every referenced Parquet object, including size, schema, row count, object hash, inode/fingerprint and source manifest bytes hash.
- FR-7: A destination MUST be an explicitly supplied absolute path with a trusted replication descriptor, expected dataset sentinel, valid manifest role, approved SMB/CIFS mount when configured as NAS, and no unsafe overlap with local mutable roots.
- FR-8: All object transfer MUST use a destination staging namespace that is outside the published manifest and cannot be served as a dataset generation.
- FR-9: Before promotion, every destination object MUST be read back and match source size, canonical schema, row count, SHA-256, source lineage and expected relative path.
- FR-10: Destination publication MUST write all verified immutable objects and a self-contained NAS `replication-record.json` first, then advance the complete NAS `head.json` atomically under lock; no partial object/record or partially assembled generation may be visible through a strict reader.
- FR-11: Destination lineage MUST bind direction, source checkpoint (`checkpoint_id`/`source_instance_id`/`source_sequence`), source manifest/object-set hashes, replication generation, parent record hash, plan hash, destination trust identity and complete object inventory; an unknown, older, conflicting or reverse lineage MUST fail closed.
- FR-12: Replicating the same checkpoint to the same trusted destination MUST be idempotent and return `ALREADY_REPLICATED` without copying or changing NAS record/head; SQLite may only append the corresponding audit/cache observation.
- FR-13: The service MUST implement only `local_to_nas`; restore is a separately named `nas_to_temporary_root` operation and MUST NOT enqueue, overwrite local canonical data or reverse-replicate a destination into the source.
- FR-14: Transfer attempts MUST use bounded exponential retry and a durable `dead_letter` state after the configured maximum; retries MUST never increase market-provider requests or bypass a failed integrity gate.
- FR-15: Concurrent drains MUST use a local nonblocking lock plus outbox lease/state-version CAS; only the lease owner may advance an intent, and lease expiry MUST make a stale worker harmless.
- FR-16: Restore MUST validate a trusted destination manifest and copy only immutable referenced objects into a new explicitly supplied temporary root; all semantic verification MUST finish in hidden staging before the one atomic directory rename.
- FR-17: A verified restore MUST check manifest identity, sentinel, object hashes, exact canonical schema, row counts, trade-date/source partitions and representative read-only market queries; it MUST not create or update canonical control pointers.
- FR-18: `GET /api/v1/storage/replication` MUST be read-only, initialize nothing, perform zero provider/network requests, expose no path/credential/raw exception, and return bounded replication lag/state.
- FR-19: `market-replicate` MUST default to a zero-write plan and require `--execute` for outbox drain or destination writes; dry-run MUST not initialize destination trust or the outbox schema.
- FR-20: `market-restore` MUST default to a zero-write plan and require `--execute`; execution MAY write only a newly created temporary restore root and its verification evidence, never canonical data, pointer, manifest or outbox state.
- FR-21: A destination initialization, if needed, MUST be a separate explicit operation that accepts only a new empty child dataset and never adopts or overwrites an existing non-empty destination implicitly.
- FR-22: LaunchAgent integration MUST reuse an existing refresh slot or an explicitly configured operator command; it MUST not add a credentialed network transport, depend on TCC access, or make NAS failure alter local refresh success.
- FR-23: Path validation MUST reject relative paths, `~`, unresolved environment syntax, `/`, home roots, local mutable/control/staging/temporary roots, symlink ancestors, mount changes and source/destination overlap.
- FR-24: Public API, CLI and structured logs MUST expose only allowlisted reason codes, counts, hashes and bounded timestamps; they MUST NOT expose payload rows, absolute paths, SMB URLs, credentials, tokens or arbitrary exception text.
- FR-25: Replication status MUST distinguish local canonical readiness from destination availability, queue lag, retry wait and dead-letter state; NAS outage MUST leave a valid local pointer ready.
- FR-26: All state transitions, source/destination lineage, retry decisions, crash recovery and restore verification MUST be replayable from sanitized durable evidence without rereading a provider.
- FR-27: Existing `MarketDatasetMirror` NAS-to-local behavior, `NasMarketStore` readers, canonical manifest/pointer bytes and R2-F0..F4.2 compatibility contracts MUST remain unchanged unless an independently reviewed compatibility adapter is added.

## Non-Functional Requirements

- NFR-1: Every dry-run/status path MUST issue zero provider/network requests and zero DB, Parquet, manifest, pointer, destination or outbox writes; tests MUST assert filesystem bytes, inode/fingerprint and SQLite bytes are unchanged.
- NFR-2: A source or destination strict validation MUST be descriptor-bound and complete: 100% of manifest-referenced objects are checked before a ready result.
- NFR-3: A transfer MUST be single-generation atomic from the destination reader's perspective: the immutable NAS record directory is complete before the NAS head changes once, only after all objects pass readback.
- NFR-4: Enqueue, drain and restore operations MUST use bounded work: at most 1 source intent claim per invocation by default, at most 6 attempts per intent, and at most 15 minutes of one drain process.
- NFR-5: Retry delays MUST be deterministic and bounded to 60 seconds, 5 minutes, 30 minutes, 2 hours and 12 hours; no busy loop or shortened retry interval is permitted.
- NFR-6: Outbox state transitions MUST be SQLite-transactional with a state-version CAS; a process crash MUST leave either the prior committed state or a recoverable lease-expired state.
- NFR-7: Hash preimages MUST use canonical UTF-8 JSON, sorted keys, compact separators, explicit null/false/zero/empty values and a newline-terminated domain encoding defined below.
- NFR-8: No public status response may contain an absolute path, path segment, SMB URL, username, password, token, provider payload or arbitrary exception message; this is enforced by a privacy test.
- NFR-9: Destination and restore path checks MUST use `lstat`/`O_NOFOLLOW`-equivalent descriptor checks at every phase boundary and recheck source/destination fingerprints immediately before publication.
- NFR-10: A successful local publication MUST remain independent of destination reachability; a destination outage may set replication state to degraded/unavailable but MUST NOT change local `ready` or the local pointer identity.
- NFR-11: Status reads MUST complete within 500 ms p95 for a local outbox containing 10,000 terminal rows and MUST never scan or hash all Parquet objects.
- NFR-12: A verified restore MUST leave no published restore root on any failed gate; failed staging garbage MAY be retained only in a non-reader-visible quarantine namespace with a sanitized report.
- NFR-13: The default runtime and all existing LaunchAgent templates MUST remain replication-off unless an explicit reviewed configuration enables the feature; no new provider or credential is constructed by status/dry-run.
- NFR-14: The implementation MUST preserve existing canonical publication and mirror tests byte-for-byte where those tests fingerprint canonical objects, manifests, pointers or legacy JSON projections.
- NFR-15: The spec-stage acceptance run MUST use no real NAS, SMB mount, LaunchAgent, credential, provider request or production path; real copy/restore requires a separate change window.

## Acceptance Criteria

### AC-1: Disabled local-first behavior (FR-1, FR-22, FR-25, NFR-1, NFR-13)

Given replication is disabled and a valid local canonical publication exists, When automation and status are run, Then the disabled branch returns before constructing or opening the sidecar/source/destination/lock, local readiness and pointer identity remain ready, the provider request count is zero, the outbox/destination/restore hooks are not called, and no local tree or database bytes change.

### AC-2: Binding, pointer and enqueue boundary (FR-2, FR-2a, FR-3, FR-3b, FR-3c, FR-3d, FR-5, FR-25, NFR-6, NFR-10)

Given a dataset-backed publication has a successful manifest but binding write/readback fails, When any pointer writer seam attempts publication, Then the new pointer commit is blocked, the prior ready pointer is unchanged, no provider is called, and the failure is `OUTBOX_DURABILITY_UNAVAILABLE`; given a valid binding, the central seam commits the pointer only when all binding fields match exactly. The same non-reentrant manifest lock MUST cover baseline read, object/staging writes, manifest replace/readback, binding fsync, pointer commit and post-commit exact proof; a final fingerprint drift blocks the pointer. A legacy/no-selection publication uses canonical-null selection/lineage digests, while missing required v2 metadata returns `SOURCE_UNAVAILABLE`; a plain `MarketStore` keeps its existing canonical commit and only observes `SOURCE_NOT_CONFIGURED`. After that pointer commit, a synthetic outbox error preserves ready state and projects `OUTBOX_ENQUEUE_FAILED` without rollback. Required tests are `test_binding_write_failure_blocks_pointer_without_rollback`, `test_publication_is_atomic_under_one_manifest_publication_lock`, `test_manifest_fingerprint_drift_blocks_pointer_cas`, `test_publication_binding_is_fsynced_before_pointer_and_exactly_matches`, `test_manifest_changed_before_pointer_commit_is_rejected`, `test_all_dataset_pointer_writers_route_through_coordinator`, `test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured`, `test_legacy_no_selection_uses_canonical_null_digests`, `test_missing_v2_manifest_metadata_blocks_pointer`, and `test_enqueue_failure_preserves_local_pointer_ready`.

### AC-3: Binding and journal crash recovery (FR-3, FR-4, FR-5, FR-26, NFR-6, NFR-7)

Given a process stops before pointer commit during binding installation, or after pointer commit before SQLite enqueue, When the next explicit writer/reconciliation runs, Then the old pointer remains unchanged in the first case, the exact binding is reused without overwrite, and exactly one deterministic intent is persisted in the second case; a binding whose manifest bytes changed or whose pointer never matched remains inactive. Required tests are `test_pointer_unchanged_after_binding_crash_is_recoverable`, `test_publication_binding_target_existing_identical_is_reused`, `test_multiple_checkpoint_journals_crash_and_recover_exactly_once`, and `test_binding_active_only_when_pointer_exact_match`.

### AC-4: Strict outbox contract (FR-4, FR-14, FR-15, NFR-6)

Given a fresh empty local control root, When the execution service initializes the outbox and claims an intent twice concurrently, Then the normalized DDL is accepted, one claim wins by state-version CAS, the losing worker performs no copy, and terminal rows cannot be deleted or have immutable identity fields changed.

### AC-5: Complete source snapshot (FR-6, FR-9, FR-11, FR-26, NFR-2, NFR-9)

Given a manifest references an object with a missing file, changed inode, wrong size, wrong schema, wrong row count or wrong hash, When replication plans the intent, Then the complete source candidate is invalid, no destination object/record/head is written, and the sanitized reason identifies the failing validation stage.

### AC-6: Destination trust and path safety (FR-7, FR-21, FR-23, FR-24, NFR-8, NFR-9)

Given a destination is relative, contains unresolved environment syntax, is `/`, overlaps local mutable storage, has a symlink ancestor, has an unexpected mount, or lacks a trusted destination descriptor, When `market-replicate` is run without `--execute`, Then it returns a bounded plan/error without creating a directory, reading credentials, initializing a sidecar or exposing the supplied path.

### AC-7: Verified staged transfer (FR-8, FR-9, FR-10, FR-11, NFR-2, NFR-3, NFR-9)

Given a trusted empty destination and a complete local source checkpoint, When one intent executes against an offline fixture, Then each immutable object is copied to hidden staging, read back for size/schema/rows/hash, written with a self-contained NAS replication record, and only then is the NAS head atomically advanced.

### AC-8: Failure leaves NAS head/record unchanged (FR-8, FR-9, FR-10, FR-12, NFR-3, NFR-12)

Given an object copy, readback, record write or head CAS is interrupted, When the operation terminates, Then the prior NAS head/record remains readable and byte-identical, no partial namespace is referenced by it, and a strict reader cannot see an incomplete generation.

### AC-9: Idempotence and no old overwrite (FR-11, FR-12, FR-13, FR-27, NFR-3, NFR-14)

Given a destination already contains the exact checkpoint lineage, an older source lineage, a conflicting object hash or a reverse-direction marker, When replication is attempted, Then exact lineage is `ALREADY_REPLICATED`, destination-ahead/conflicting/reverse lineage is rejected with an allowlisted reason, and neither NAS record/head nor local canonical pointer changes.

### AC-10: Retry and dead-letter (FR-14, FR-25, FR-26, NFR-4, NFR-5)

Given a destination is temporarily unavailable or a bounded copy fails, When the drain is invoked repeatedly, Then the intent follows `pending → copying/verifying → retry_wait` with the exact bounded delay schedule, reaches `dead_letter` after six failed claims, and never issues a market-provider request.

### AC-11: Concurrency and stale lease (FR-4, FR-15, FR-26, NFR-6)

Given two workers share one outbox and one destination and one worker crashes while leased, When the second worker observes lease expiry, Then it can claim the same deterministic intent exactly once, a stale worker cannot advance state or publish a NAS head, and state-version conflicts are sanitized.

### AC-12: Restore dry-run (FR-13, FR-16, FR-17, FR-20, FR-23, FR-24, NFR-1, NFR-12)

Given a trusted source archive and an explicit absolute temporary destination, When `market-restore` runs without `--execute`, Then it validates the plan only, reports object/count/hash projections without paths, performs zero writes/provider calls, and does not create the temporary root or touch canonical state.

### AC-13: Verified temporary restore (FR-16, FR-17, FR-20, FR-24, NFR-2, NFR-3, NFR-9, NFR-12)

Given a complete trusted archive, When `market-restore --execute` writes a new temporary root, Then the final root contains a strict sentinel/manifest and verified immutable objects, representative read-only queries succeed, and canonical manifest/pointer/control DB bytes and inodes remain unchanged.

### AC-14: Restore failure is invisible (FR-16, FR-17, FR-20, FR-23, NFR-9, NFR-12)

Given a restore source has a corrupt object, schema mismatch, duplicate/extra entry, symlink race, changed source fingerprint or failed representative query, When execution runs, Then no final restore root is published, partial data is not reader-visible, canonical state is unchanged, and the result names only the failing reason code.

### AC-15: Read-only status (FR-18, FR-24, FR-25, NFR-1, NFR-8, NFR-11)

Given the outbox is missing, corrupt, locked, empty, retrying, dead-lettered or healthy, When `GET /api/v1/storage/replication` is called, Then it performs no initialization, migration or network probe, returns the exact bounded status projection with `mode=status`, `provider_requests=0` and all effects false, and never returns paths, credentials or raw exceptions.

### AC-16: Replication CLI plan (FR-18, FR-19, FR-23, FR-24, NFR-1, NFR-13)

Given a valid or invalid explicit destination, When `market-replicate` runs with and without `--execute`, Then omitted `--execute` is always zero-write, `--execute` is the only mode allowed to claim/copy, output is sanitized and deterministic, and an invalid destination cannot initialize outbox or destination state.

### AC-17: Explicit destination initialization (FR-7, FR-21, FR-23, NFR-9, NFR-15)

Given a new empty child directory on an approved mount, When the separately acknowledged initialization operation executes, Then it creates only the trusted destination descriptor/sentinel and required non-published staging/quarantine directories; an existing non-empty path, share root, symlink or mount mismatch is rejected without overwrite.

### AC-18: Existing mirror compatibility (FR-13, FR-27, NFR-14)

Given an existing NAS dataset and the current `MarketDatasetMirror`, When legacy NAS-to-local sync and new local-to-NAS planning are tested together, Then the legacy direction, reused-generation result, older-source guard and canonical reader behavior remain unchanged, and no new reverse path is invoked by the old command.

### AC-19: LaunchAgent/TCC boundary (FR-1, FR-22, FR-25, NFR-10, NFR-13, NFR-15)

Given the refresh LaunchAgent runs with no TCC access to the configured NAS, When a local refresh publishes successfully, Then it records/enqueues a local intent or journal, local ready remains successful, the bounded drain reports destination unavailable, no credentialed mount is attempted, and no sixth always-on agent is required.

### AC-20: Offline evidence and release boundary (FR-24, FR-26, FR-27, NFR-1, NFR-15)

Given all focused replication/restore fakes, existing NAS tests and static validators run in an isolated worktree, When the release candidate is reviewed, Then every FR/AC/EC anchor has a real test node or named static check, no real NAS/provider/production operation is claimed, the exact Git HEAD and command outputs are recorded, and the document remains `In Review` until independent SPEC and QUALITY review.

## Edge Cases

- EC-1: Replication is disabled or no local dataset root is configured → return `DISABLED`/`SOURCE_NOT_CONFIGURED`, zero provider and zero writes.
- EC-2: A CLI path is relative, contains `~`, `$VAR`, `${VAR}` or command-substitution syntax → return `PATH_INVALID` before filesystem creation.
- EC-3: A path resolves to `/`, the home directory, a local control/staging/temp root, or an unresolved environment target → reject without `stat`-then-create behavior.
- EC-4: Source or destination root, parent, manifest, sentinel, object or staging component is a symlink or changes inode during an operation → return `PATH_CHANGED`/`SYMLINK_UNSAFE` and fail closed.
- EC-5: Expected SMB/CIFS mount is absent, unexpected, or the path moves outside its mount point → return `DESTINATION_MOUNT_UNAVAILABLE` without probing credentials.
- EC-6: Sentinel, destination descriptor or manifest is missing, oversized, malformed, wrong role or wrong schema → return `DESTINATION_TRUST_FAILED` or `SOURCE_UNAVAILABLE` without initialization.
- EC-7: Source manifest has unsafe, duplicate, extra, missing, out-of-root or non-Parquet paths → reject the entire candidate; never filter the bad entry and continue.
- EC-8: A source object is missing, empty, wrong size, wrong SHA-256, wrong schema, wrong row count, wrong source/date partition or changed during read → reject the whole intent.
- EC-9: Destination contains a partial object, orphan final object, duplicate path, extra object or stale staging directory → published reader ignores it; replication either quarantines it or returns `DESTINATION_CONFLICT`.
- EC-10: Destination history has a greater source sequence, a different object hash for the same logical partition, or unknown lineage → never overwrite; return `DESTINATION_AHEAD`/`DESTINATION_CONFLICT`.
- EC-11: The NAS head/record baseline changes between locked read and head CAS → CAS fails, current NAS head is preserved, and the intent enters bounded retry or terminal conflict.
- EC-12: Copy fails after any number of objects or process exits before record/head commit → no partial generation is referenced; retry reuses only hash-verified immutable objects.
- EC-13: Readback size/schema/rows/hash or lineage verification fails → mark the attempt failed, never publish a destination record/head, and retain only sanitized diagnostics.
- EC-14: Outbox database is missing, corrupt, schema-mismatched or locked on status → return `REPLICATION_STATE_UNAVAILABLE`; status must not initialize, migrate or write it.
- EC-15: Outbox enqueue fails after canonical pointer commit → preserve local ready, atomically write the deterministic intent journal when possible, and expose `OUTBOX_ENQUEUE_FAILED` or `OUTBOX_JOURNALED`.
- EC-16: Both outbox and journal writes fail after pointer commit → preserve local ready, expose `OUTBOX_DURABILITY_UNAVAILABLE`, and let the next strict reconciliation derive the current intent without fetching data.
- EC-17: Process crashes after journal write before outbox import → import is idempotent; journal removal is attempted only after the outbox row is committed.
- EC-25: A binding temp/install, manifest mutation, or pointer transaction crashes before commit → only an inactive immutable binding or quarantined temp may remain; the previous pointer remains ready and no mismatched binding becomes an active SourceCommit.
- EC-26: The local control DB inode rotates during a legitimate migration → the current checkpoint records the new device/inode/schema, but the immutable dataset-root nonce/identity remains stable; any replacement that fails strict root, nonce, schema, singleton, manifest or pointer proof is `LOCAL_POINTER_MISMATCH`/`OUTBOX_DURABILITY_UNAVAILABLE` and is never accepted.
- EC-27: A concurrent `backfill`/`upsert`/refresh mutates the manifest during a dataset-backed publication → the shared non-reentrant manifest lock serializes the mutation; any final fingerprint drift fails the pointer CAS and leaves the prior pointer unchanged.
- EC-28: `RefreshRunLock` is absent, reordered or nested while the manifest publication lock is held → it cannot be used as the publication atomicity dependency; the opaque manifest-lock token remains the sole ownership proof and lock-order violation fails closed.
- EC-29: A legacy/no-selection candidate lacks required v2 manifest metadata → fixed canonical-null selection/lineage digests are used only when metadata is intentionally absent; a required-but-missing field returns `SOURCE_UNAVAILABLE` and leaves the pointer unchanged.
- EC-30: A plain `MarketStore` publishes without an explicit local dataset/manifest → its existing canonical commit is unchanged; no binding/guard/NAS work runs and the post-commit observation is `SOURCE_NOT_CONFIGURED`.
- EC-31: `NasMarketStore.upsert_bars` or a `BackfillService` batch invokes `publish_manifest_only(...)` with explicit per-date lineage input → it may update and verify only the manifest; it must never create a binding or call the pointer wrapper, and pointer bytes/hash/inode remain unchanged.
- EC-32: Manifest lock acquisition, token, precommit or final-CAS readiness fails before pointer commit → pointer bytes/hash/inode remain unchanged and no post-commit degraded observation is emitted.
- EC-33: Pointer commit succeeds but unlock/close/post-proof control fails → canonical ready remains visible; emit degraded `CONTROL_STATE_UNAVAILABLE`, durably record reconciliation evidence, and let the next pre-publication guard repair/prove control state.
- EC-34: A manifest-only publication starts with an empty control database → return `PointerIdentity.ABSENT`; compare the complete tagged union before/after and do not serialize a fake string identity.
- EC-35: A multi-date `BackfillService` plan has one omitted, partial or conflicting per-date lineage input → the resolver returns `UNAVAILABLE/SOURCE_UNAVAILABLE` before any date is upserted; existing manifest and pointer identities remain unchanged.
- EC-18: Two workers claim the same intent, or a worker lease expires → one state-version CAS wins; stale workers cannot copy, promote or mark success.
- EC-19: Retry attempt reaches the sixth failure or a non-retryable integrity/trust failure occurs → durable `dead_letter`/terminal reason; no automatic unbounded retry.
- EC-20: Restore source is invalid or changes during copy → no final temporary root, no canonical DB/pointer mutation, and bounded sanitized result.
- EC-21: Restore destination already exists, is non-empty, overlaps canonical roots, is symlinked or is not an explicitly temporary child → reject before writes.
- EC-22: Restore representative query fails before rename → no destination is visible; a post-rename non-semantic fingerprint anomaly isolates the destination, preserves canonical state, and does not call replication enqueue.
- EC-23: Existing NAS-to-local mirror sees a destination with new lineage metadata → legacy `DatasetManifest` read remains compatible, but the new replication reader requires the exact lineage contract.
- EC-24: A caller attempts `nas_to_local`, destination-to-source copy, reverse replication, source deletion, manual pointer edit or credentialed mount → return `DIRECTION_NOT_ALLOWED` and perform zero writes.

## API Contracts

The API is read-only. Execution is intentionally CLI-only and requires explicit `--execute`.
All paths below are logical routes, not filesystem paths.

```typescript
type ReplicationState =
  | "disabled" | "ready" | "degraded" | "unavailable";
type ReplicationReason =
  | "NONE" | "DISABLED" | "SOURCE_NOT_CONFIGURED"
  | "SOURCE_UNAVAILABLE" | "LOCAL_POINTER_MISMATCH" | "REPLICATION_STATE_UNAVAILABLE"
  | "OUTBOX_ENQUEUE_FAILED" | "OUTBOX_JOURNALED"
  | "OUTBOX_DURABILITY_UNAVAILABLE" | "CONTROL_STATE_UNAVAILABLE" | "DESTINATION_UNAVAILABLE"
  | "DESTINATION_MOUNT_UNAVAILABLE" | "DESTINATION_TRUST_FAILED"
  | "DESTINATION_REBOUND" | "DESTINATION_AHEAD" | "DESTINATION_CONFLICT" | "CAS_CONFLICT"
  | "COPY_FAILED" | "VERIFY_FAILED" | "RETRY_WAIT" | "DEAD_LETTER"
  | "ALREADY_REPLICATED" | "PATH_INVALID" | "PATH_CHANGED"
  | "SYMLINK_UNSAFE" | "DIRECTION_NOT_ALLOWED" | "MOUNT_UNSUPPORTED";

interface Effects {
  writes: boolean;
  canonical_writes: boolean;
  destination_writes: boolean;
  outbox_writes: boolean;
  restore_writes: boolean;
}

interface ReplicationStatusResponse {
  status: ReplicationState;
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
}

GET /api/v1/storage/replication -> ReplicationStatusResponse
  200: status is disabled, ready or degraded
  503: status is unavailable; body is the same bounded shape

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

The CLI exit codes are fixed: `0` for a safe plan or successful execution, `1` for unavailable,
retry/dead-letter or integrity failure, and `2` for lexical/configuration/path/direction error.
`market-replicate` and `market-restore` output the bounded interfaces above; neither command
prints its input path. A separate `market-replication-status` alias MAY call the same read-only
projection but MUST preserve the response fields and zero-write guarantees.

`local_ready` is read from the existing canonical readiness projection. `destination_health` and
`destination_health_observed_at` are read only from the SQLite `replication_destination_cache`,
which is populated after a descriptor-bound NAS head read by a writer; it is an audit cache, not a
destination authority. Status never opens a network socket, mounts a share, refreshes health or
turns an absent/corrupt cache into healthy. Queue lag is derived from the sidecar's committed
timestamps and is `null` when control evidence is not proven. The NAS archive/head is always the
source of truth for lineage and restore.

## Data Models

### Immutable identity and hash preimages

All digests use the following exact encoding:

```python
canonical_json_bytes(value) = (
    json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ) + "\n"
).encode("utf-8")

domain_sha256(domain, value) = sha256(
    domain.encode("ascii") + b"\n" + canonical_json_bytes(value)
).hexdigest()
```

`source_manifest_bytes_sha256` hashes the exact bytes opened through a no-follow descriptor.
`manifest_canonical_sha256` hashes the canonical JSON projection of the already committed
`manifest.json`; both values are retained so formatting changes cannot be mistaken for object
changes. `pointer_row_sha256` hashes the exact singleton committed-row projection, while
`pointer_db_schema_digest` hashes the read-only control schema identity. `object_inventory` is the
sorted list of `{relative_path, object_sha256, size_bytes, row_count, trade_date, source}` for
every manifest file; no inventory item is omitted, duplicated or self-referential.

The manifest projection is closed: `{dataset,schema_version,generation,files}`, where each file
entry is `{path,sha256,trade_date,source,row_count,provider_id,universe_id,evidence_id,
evidence_sha256,candidate_id,candidate_manifest_sha256,gate_report_sha256,adapter_version,
source_schema_version}`; absent legacy lineage values remain absent in manifest bytes and are
represented as explicit JSON `null` only in the closed hash projection.
`files` is sorted by `(path,sha256)` before hashing. Thus
`manifest_canonical_sha256 = domain_sha256("stock-eva/r2f4.3/manifest-canonical/v1",
manifest_projection)`. The control schema projection is the sorted, self-excluded SQLite
`sqlite_master` tuple list `{type,name,tbl_name,sql}` for all application tables, indexes and
triggers; `pointer_db_schema_digest = domain_sha256("stock-eva/r2f4.3/pointer-db-schema/v1",
schema_projection)`. Direct byte hashes (`source_manifest_bytes_sha256`) are SHA-256 of exact
descriptor-read bytes and have no JSON preimage.

The closed preimages are:

```text
source_object_set_sha256 = domain_sha256("stock-eva/r2f4.3/object-set/v1", object_inventory)
dataset_identity = domain_sha256("stock-eva/r2f4.3/dataset-identity/v1",
  {canonical_root_path,canonical_schema_digest,source_instance_domain,source_instance_nonce})
source_instance_id = domain_sha256("stock-eva/r2f4.3/source-instance/v3", {dataset_identity})
publication_binding_sha256 = domain_sha256("stock-eva/r2f4.3/source-publication-binding/v1",
  {binding_schema,schema_version,run_id,trade_date,published_at,manifest_generation,
   manifest_bytes_sha256,manifest_canonical_sha256,source_object_set_sha256,selection_sha256,
   lineage_sha256})
checkpoint_id = domain_sha256("stock-eva/r2f4.3/source-checkpoint/v1",
  {source_instance_id,source_instance_sha256,pointer_row_sha256,pointer_generation,source_run_id,source_trade_date,
   source_published_at,pointer_db_device,pointer_db_inode,pointer_db_schema_digest,
   manifest_canonical_sha256,source_manifest_bytes_sha256,source_object_set_sha256,object_inventory,
   publication_binding_sha256})
checkpoint_payload_sha256 = domain_sha256("stock-eva/r2f4.3/source-checkpoint-payload/v1",
  {checkpoint_id,source_instance_id,source_instance_sha256,pointer_row_sha256,pointer_generation,source_run_id,
   source_trade_date,source_published_at,pointer_db_device,pointer_db_inode,
   pointer_db_schema_digest,manifest_canonical_sha256,source_manifest_bytes_sha256,
   source_object_set_sha256,object_inventory,publication_binding_sha256})
plan_sha256 = domain_sha256("stock-eva/r2f4.3/replication-plan/v1",
  {direction,destination_id,checkpoint_id,object_inventory})
intent_id = domain_sha256("stock-eva/r2f4.3/replication-intent/v1",
  {direction,destination_id,source_instance_id,source_sequence,checkpoint_id,plan_sha256})
```

`checkpoint_id` is the complete canonical checkpoint digest and deliberately excludes
`source_sequence`; it is stable before SQLite allocation and is therefore the journal filename
and recovery identity. `source_sequence` is assigned by the sidecar only after deterministic
journal ordering, so the same checkpoint receives the same sequence across restart. The
replication-generation, record and head hashes below use separate domains; UUIDs never supply
ordering.

`destination_id` is the first 32 lower-case hex characters of the trusted descriptor hash. It is
not a path encoding and is the only destination identity exposed in private audit; public output
exposes no destination identifier unless it is an opaque 32-hex digest.

### Model tables

| Entity | Fields | Constraints and purpose |
|---|---|---|
| `SourceInstanceRecord` | source_instance_schema, schema_version, source_instance_id, canonical_root_path, canonical_schema_digest, source_instance_domain, fsync_contract, source_instance_nonce, dataset_identity, created_at, source_instance_sha256 | Private immutable `<local_dataset>/_replication/source-instance.json`; identity is dataset-root/schema/domain plus the immutable nonce, never DB inode; file/parent fsync and strict readback are required. |
| `SourcePublicationBinding` | binding_schema, schema_version, run_id, trade_date, published_at, manifest_generation, manifest_bytes_sha256, manifest_canonical_sha256, source_object_set_sha256, selection_sha256, lineage_sha256, binding_sha256 | Immutable private `<local_dataset>/_replication/source-commits/<run_id>.json`; created after manifest success and before pointer commit, then active only when the committed pointer exactly matches every field. |
| `SourceCheckpoint` | checkpoint_id, source_instance_id, source_instance_sha256, publication_binding_sha256, pointer DB device/inode/schema digest, singleton run/date/published_at, generation, manifest/source_object_set hashes, complete inventory | Created from the committed pointer and active binding before sequence allocation; checkpoint_id excludes source_sequence and records the current device/inode/schema tuple for tamper/migration proof. |
| `ReplicationIntent` | intent_id, operation_day, direction, destination_id, checkpoint_id, source sequence, plan/intent hashes, object/row/byte counts | Frozen after enqueue; deduplicated by destination/checkpoint and source sequence; operation_day is scheduling metadata only; no payload rows. |
| `ReplicationAttemptEvent` | intent_id, event sequence, prev/event hashes, transition fields, attempt, state version, reason, timestamp, optional NAS record/head hashes | Append-only immutable evidence; contiguous replay from genesis; max six claims; result hashes bind the post-head sidecar audit to NAS. |
| `ReplicationHead` | intent_id, state, state_version, lease owner/until, next attempt, last event sequence/reason | Mutable derived projection updated only by CAS in the same transaction as its event. |
| `DestinationTrust` | descriptor schema/role, destination_id prefix, sentinel hash, mount identity, single writer host, created_at | Explicit archive role; descriptor-bound no-follow reads; no public path/URL/credential. |
| `ReplicationRecord` | self-contained NAS `replication-record.json`: generation, source instance/sequence, checkpoint, parent record, descriptor, manifest/object hashes, complete inventory and counts | Immutable record inside `_replication/history/<replication_generation>/`; NAS archive is the destination authority. |
| `DestinationHead` | self-contained NAS `head.json`: destination/generation/record/checkpoint/source sequence/parent/manifest/object/descriptor hashes, version and head hash | Atomic NAS pointer to one immutable record; NAS head is the only current selector. SQLite stores only a non-authoritative observation cache. |
| `RestoreReport` | report hash, source record/checkpoint lineage, object/row/byte counts, verification result, reason, started/completed UTC | Immutable sanitized report under private restore evidence; never canonical or destination authority. |
| `ReplicationStatus` | state/reason, local_ready, destination health from last persisted probe, counts, queue lag | Public projection omits paths, credentials, SQL, payload and raw exceptions; status does no probe. |

### State, reason, HTTP and CLI contracts

The only forward state transitions are:

```text
pending -> copying -> verifying -> replicated
pending/copying/verifying -> retry_wait -> pending
pending/copying/verifying -> dead_letter
```

`replicated` and `dead_letter` are terminal for the immutable intent. A manual requeue, if later
approved, creates a new attempt record linked to the same intent through a reviewed operation and
does not delete or rewrite the terminal audit.

The following is the one and only reason-to-status-to-transport mapping; internal exceptions are
first reduced to one of these allowlisted reasons and no second public mapping exists.

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

### Transaction boundary

The implementation MUST preserve this sequence:

```text
strict provider/candidate gates
  -> canonical immutable object + local manifest succeeds
  -> immutable publication-binding record write/readback + file/parent fsync
  -> pre-publication durability guard under the existing refresh/control lock
  -> central publish_dataset_and_pointer pointer transaction commits
  -> exact binding match activates SourceCommit
  -> frozen SourceCommit observation creates replication-only source checkpoint
  -> local outbox enqueue transaction (or atomic journal fallback)
  -> optional bounded destination drain
  -> unrelated downstream shadow/universe work
```

The canonical transaction never depends on a NAS socket, destination lock, outbox SQLite commit or
restore. A local enqueue failure is an operational durability gap, not a canonical data failure.
The checkpoint is read after commit and is replication-owned; it cannot update canonical rows/files.
Startup reconciliation compares the current strict pointer identity with durable checkpoint,
intent and journal evidence. If a crash occurs before the checkpoint is durable, the next explicit
writer observes the same committed pointer and creates it; it never refetches or stitches rows.

### Crash matrix

| Crash/failure point | Local pointer | Outbox/destination visibility | Recovery |
|---|---|---|---|
| Before canonical commit | unchanged | no intent | refresh remains failed/partial under existing contract |
| After manifest success, before binding install | unchanged | only absent/quarantined binding temp | retry writes the same immutable binding; no pointer commit occurs |
| `publish_manifest_only` update/verify | unchanged | manifest/object changes only; no binding/pointer | upsert/backfill verifies manifest and proves pointer bytes/hash/inode unchanged |
| Pre-pointer lock/token/baseline/guard/final-CAS failure | unchanged | no active binding or pointer advance | return the typed precommit failure; prior pointer identity is unchanged |
| After pointer commit, unlock/close/exact-proof failure | ready | pointer is committed; reconciliation evidence is durable | return degraded `CONTROL_STATE_UNAVAILABLE`; next guard re-proves control state and never rolls back |
| After binding install/fsync, before pointer commit | unchanged | inactive exact binding only | reuse exact binding; changed manifest or mismatched pointer blocks commit |
| Manifest changes after binding, before pointer commit | unchanged | stale binding remains inert | central seam returns `LOCAL_POINTER_MISMATCH`; no overwrite or pointer advance |
| After canonical commit, before enqueue | ready | no row; journal or reconciliation gap | next execution derives current intent; no rollback |
| During outbox SQLite commit | ready | prior committed row or no row | journal/reconciliation imports once |
| Lock acquisition/probe fails | ready | no staging/record/head write | return `MOUNT_UNSUPPORTED`; no unproven retry |
| After `pending` claim | ready | no published destination change | lease expiry returns intent to claimable state |
| During object staging | ready | only non-visible partials | retry cleans/quarantines staging; manifest unchanged |
| After object staging, before immutable record commit | ready | only hidden partials; current NAS head unchanged | retry reuses only verified hidden objects or isolates them |
| After record directory commit, before NAS head commit | ready | complete unselected record; current NAS head unchanged | replay verifies record and either advances head or isolates record |
| After NAS head fsync, before sidecar result/cache | ready | new complete NAS head is authoritative; SQLite is stale only | next run reads NAS head/record and idempotently reconciles sidecar result/cache |
| During NAS head replacement/fsync | ready | previous or new complete head, never partial | strict head reader accepts one committed state; retry is idempotent |
| During restore directory rename | ready | no destination or one complete standard sentinel/manifest directory | `DestinationArchiveReader` and the pure output validators see only the complete final directory |

### Destination and restore safety

The destination archive is a separate trust domain. Replication may write only a trusted
`nas_archive` root explicitly selected by the operator. It MUST not write a source root, local
control root, user database root, staging root, temporary root, share root or any unresolved
environment path. A NAS replication record is publishable only when its lineage says
`direction=local_to_nas` and its source object set is complete; the separate head is the only
visible selector. Restore reads only the archive record, manifest and objects and writes a new temporary root using the existing
standard sentinel/`manifest.json` output validated by pure schema/Parquet readers, but never updates
`MarketStore`, `NasMarketStore.control`, `CURRENT`, `published_snapshots`, canonical Parquet or the
replication outbox.

## Out of Scope

- OS-1: Real SMB/NAS copy, destination initialization, mount setup, credentials, TCC approval or production restore execution; these require a separately authorized window.
- OS-2: Any provider request, canonical refresh retry, provider failover, symbol-level mixing, normalization, quality-gate or factor change.
- OS-3: Replacing or altering `manifest.json`, canonical Parquet, `published_snapshots`, canonical DDL, `NasMarketStore`, `MarketStore` or the existing NAS-to-local `MarketDatasetMirror` with another storage platform.
- OS-4: Reverse replication, NAS-to-local canonical promotion, automatic overwrite of a newer destination, source deletion or manual pointer/manifest editing.
- OS-5: Cloud object storage, authenticated network APIs, SMB credential storage, encrypted transport provisioning or vendor-specific replication daemons.
- OS-6: Private user/portfolio database backup to NAS; the existing local-only private backup boundary remains unchanged.
- OS-7: Automatic cleanup that deletes unreferenced destination objects; quarantine/retention policy is a separate operator decision.
- OS-8: Public full-universe/object listing, path diagnostics, payload exposure or a restore API endpoint.
- OS-9: New LaunchAgent count or a background agent that assumes `/Volumes` access; only an optional bounded hook in an existing slot may be planned.
- OS-10: Cross-volume rename presented as atomic; copy-plus-delete is not a publication primitive.

## Evidence and review boundary

This document is a contract, not implementation evidence. The companion implementation plan must
create the named tests before code, run them RED, implement the smallest conforming slice, run
them GREEN, and record the exact `git rev-parse HEAD` output. No test result in this document is a
production or real-NAS claim. The final status remains `In Review` until independent SPEC and
QUALITY reviewers approve the exact implementation commit.

### Individual evidence crosswalk

The planned anchors are exact test node names to be created in `tests/test_dataset_replication.py`
or existing compatibility files. Every row is intentionally one-to-one and must be replaced by a
real passing test/static check before an implementation release is considered.

| ID | Exact planned evidence anchor |
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

## R2-F4.3.2 normative amendment — single-authority release-candidate closure

This section is normative and supersedes any earlier sentence, model, DDL, API field or step in
this document that is less strict. It closes the H1-H6/M1-M6 findings. It is still a design
contract: the named tests are planned evidence, not evidence already obtained. No implementation,
NAS access, mount, provider request or production operation is authorized by this amendment.

### H1 — exact committed local source pointer

Replication MUST construct its source only from the explicit
`settings.local_market_dataset_root`. `nas_market_dataset_root`, a generic configured dataset
root, a current working directory, an environment fallback, or a destination path MUST NOT be a
source. The `LocalCanonicalPointerReader` is a read-only, descriptor-bound reader that observes the
already committed local sentinel, `manifest.json`, and local control pointer row; it MUST NOT alter
`published_snapshots`, `manifest.json`, canonical Parquet, or any canonical DDL. The reader uses
`O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, binds the control DB device/inode/schema and singleton row, records
file fingerprints before and after each read, rejects any change, and never initializes or migrates
a database.

The source checkpoint is replication-owned and is created only after a canonical pointer commit
and an active publication binding. It copies the exact original values from the strict pointer
row: `run_id`, `trade_date`, `published_at`, plus the current pointer DB `device`, `inode`,
`schema_digest`, `manifest_canonical_sha256` and `source_object_set_sha256`; the current
device/inode/schema tuple is checkpoint tamper evidence, not stable source identity. An independent immutable private record at
`<local_dataset>/_replication/source-instance.json` is the source-instance authority and is never
part of the canonical manifest or pointer schema. Its closed fields are
`source_instance_schema,schema_version,source_instance_id,canonical_root_path,
canonical_schema_digest,source_instance_domain,fsync_contract,source_instance_nonce,dataset_identity,
created_at,source_instance_sha256`. The nonce is created once under the local dataset root and,
together with the bound root, canonical schema digest and fixed domain
`stock-eva/r2f4.3/local-canonical`, defines the immutable `dataset_identity`; neither identity uses a
DuckDB device or inode. The file is created only by explicit writer execution with
`O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode `0600`, complete canonical JSON, file+parent fsync, and
immutable readback. `source_instance_sha256` hashes all fields except itself under
`stock-eva/r2f4.3/source-instance-record/v2`; `dataset_identity` is the domain hash of
`{canonical_root_path,canonical_schema_digest,source_instance_domain,source_instance_nonce}` under
`stock-eva/r2f4.3/dataset-identity/v1`; `source_instance_id` is the domain hash of
`{dataset_identity}` under `stock-eva/r2f4.3/source-instance/v3`.

The file's canonical closed projection is:

```json
{"source_instance_schema":"stock-eva/r2f4.3/source-instance/v2","schema_version":2,"source_instance_id":"<64-hex>","canonical_root_path":"/private/local-canonical","canonical_schema_digest":"<64-hex>","source_instance_domain":"stock-eva/r2f4.3/local-canonical","fsync_contract":"file_and_parent_directory","source_instance_nonce":"<64-hex>","dataset_identity":"<64-hex>","created_at":"2026-09-09T08:00:00Z","source_instance_sha256":"<64-hex>"}
```

Values are type examples only; the actual file is sorted compact UTF-8 with one newline and no
unknown/duplicate fields. The canonical root path is retained only in this private control file,
never in public API/CLI/log output.
The sidecar, every journal and every checkpoint MUST carry both `source_instance_id` and
`source_instance_sha256`; a missing, conflicting, unreadable or non-fsynced source-instance file
maps to `OUTBOX_DURABILITY_UNAVAILABLE` and MUST NOT create a journal. A source DB inode rotation
is legal only when the same root-bound immutable nonce is present and the new descriptor passes the
exact schema, singleton, ready-run, manifest and pointer proof; the new device/inode is recorded
in that checkpoint. Any replacement that fails those checks is `LOCAL_POINTER_MISMATCH` and is not
accepted. No new column, table, trigger or DDL migration is made to the canonical control schema.
The reader requires:

```text
pointer row is the committed singleton for the requested run/date
pointer_db_device/inode/schema == the descriptor-bound control snapshot
pointer_generation == the observed manifest.generation
manifest_canonical_sha256 == domain_sha256("stock-eva/r2f4.3/manifest-canonical/v1", manifest_projection)
source_object_set_sha256 == domain_sha256("stock-eva/r2f4.3/object-set/v1", sorted complete manifest object inventory)
published_snapshots.singleton = 1 and refresh_runs.status = 'ready'
published_snapshots.run_id = refresh_runs.run_id
published_snapshots.trade_date = refresh_runs.requested_date
published_snapshots.published_at = refresh_runs.completed_at
```

Source-instance acceptance is covered by
`test_source_instance_record_is_immutable_and_fsynced`,
`test_source_instance_record_missing_or_conflicting_blocks_journal`,
`test_source_instance_identity_uses_dataset_root_nonce_not_db_inode`,
`test_legal_db_inode_rotation_with_same_root_nonce_is_accepted`, and
`test_illegal_db_replacement_fails_closed`.

The exact proof query is `published_snapshots ps JOIN refresh_runs rr ON rr.run_id = ps.run_id`;
it MUST return one row with the equal run/date/completion values above, with all timestamps in
canonical UTC-Z form. Manifest inventory and its hashes are verified separately from this join;
no manifest inventory column is assumed in `refresh_runs`.

The canonical owner MUST write the additive immutable publication-binding artifact only after the
candidate manifest has passed its existing semantic/readback checks and before committing the
DuckDB external publication pointer. Its exact private path is
`<local_dataset>/_replication/source-commits/<run_id>.json`; its closed fields are
`binding_schema,schema_version,run_id,trade_date,published_at,manifest_generation,
manifest_bytes_sha256,manifest_canonical_sha256,source_object_set_sha256,selection_sha256,
lineage_sha256,binding_sha256`, with `published_at` copied exactly from `result.completed_at`.
`source_object_set_sha256` is the sole source object-set field; `object_set_sha256` is not a second
source field.
`selection_sha256` and `lineage_sha256` are the canonical hashes of the publisher's existing
closed selection and lineage projections. The file is created with
`O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode `0600`, canonical JSON, fsynced, read back through a
bound descriptor, then its file and every parent directory are fsynced. A binding write, fsync or
readback failure blocks this pointer commit and leaves the prior pointer untouched; it is never
silently regenerated or replaced. The `_replication` namespace is private additive metadata and is
excluded from `manifest.json`, the canonical object inventory and all legacy dataset readers.

After the pointer commit, the binding becomes active only when a descriptor-bound read proves exact
equality of `run_id`, `trade_date`, `published_at`, `manifest_generation`, manifest byte/canonical
hashes, `source_object_set_sha256`, `selection_sha256` and `lineage_sha256` against the committed
pointer and manifest. An orphan binding, a binding for a changed manifest, or a pointer that never
committed is inert evidence and cannot produce `SourceCommit`. The binding schema and pointer
schema remain unchanged. Required tests are
`test_publication_binding_is_fsynced_before_pointer_and_exactly_matches`,
`test_binding_write_failure_blocks_pointer_without_rollback`,
`test_manifest_changed_before_pointer_commit_is_rejected`,
`test_binding_active_only_when_pointer_exact_match`,
`test_pointer_unchanged_after_binding_crash_is_recoverable`, and
`test_publication_binding_target_existing_identical_is_reused`.

All dataset-backed pointer writer seams MUST call the central
`publish_dataset_and_pointer` operation under the wrapper-owned
`ManifestPublicationLock`. The seam owns binding creation/readback, the pre-publication guard,
the one canonical pointer commit, exact active-binding proof, and the post-commit typed
observation. The required callers are `NasMarketStore.save_refresh`,
`NasMarketStore.reconcile_control_pointer`, dataset `backfill`,
`upsert`, refresh, the CLI reconciliation path, `full_history` publication and the legacy
`save_external_publication` call path. `save_external_publication` may be called only internally by
this dataset seam; no direct caller may bypass the binding, lock or guard. Plain `MarketStore`
canonical publication is unchanged: it performs no binding/guard/NAS work and its replication
observation is `SOURCE_NOT_CONFIGURED` without an explicit dataset/manifest. Required routing
tests are `test_all_dataset_pointer_writers_route_through_coordinator`,
`test_publication_is_atomic_under_one_manifest_publication_lock`,
`test_manifest_fingerprint_drift_blocks_pointer_cas`, and
`test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured`.

Before every subsequent canonical pointer commit, the same refresh/control writer lock MUST run a
`before_canonical_pointer_commit(NextCanonicalPointer) -> CanonicalDurabilityGuardResult` guard.
The guard reads the current committed singleton and proves that its `checkpoint_id` is already
accepted in the sidecar, or that its `<checkpoint_id>.json` journal is complete, fsynced and
descriptor-bound. If neither proof exists, the guard reconstructs the current checkpoint from the
current visible pointer and durably journals/imports it before allowing the next pointer commit.
No provider, Parquet rewrite or canonical mutation is allowed in this guard. Failure to prove or
durably establish the current checkpoint returns `OUTBOX_DURABILITY_UNAVAILABLE` and blocks only
the next pointer commit; it MUST NOT roll back, hide or alter the current ready pointer. The
post-commit observation seam remains after a successful pointer commit and is not a substitute for
this pre-publication guard.

The first publication with no current singleton is explicitly allowed because there is no prior
ready state to protect. The required state/crash matrix is: first/no-current -> allow; current
already accepted -> allow; current only in a valid fsynced journal -> import then allow; current
missing/invalid sidecar and journal -> attempt journal/import, then block on failure; crash during
guard -> old pointer remains ready and the next guard retries; crash after guard/import but before
the next pointer -> the durable checkpoint is reused; crash after pointer commit before the
post-commit observation -> the current pointer is recoverable from its checkpoint/journal and the
next guard repairs or blocks before any further pointer. Required tests are
`test_prepublication_guard_blocks_next_pointer_when_current_checkpoint_not_durable`,
`test_prepublication_guard_imports_current_fsynced_journal_before_next_pointer`,
`test_prepublication_guard_first_publication_has_no_prior_singleton`,
`test_prepublication_guard_crash_after_pointer_commit_recovers_current`,
`test_post_commit_observation_is_after_pointer_and_cannot_replace_guard`, and
`test_guard_and_post_commit_share_refresh_control_lock`.

The publication seam uses three frozen, sealed value objects with closed field sets:

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

`binding_sha256` hashes every `SourcePublicationBinding` field except itself under
`stock-eva/r2f4.3/source-publication-binding/v1`; `source_commit_sha256` hashes every
`SourceCommit` field except itself under the named domain;
`observation_sha256` does the same for `ReplicationObservation`. The canonical publication owner
seals `SourceCommit` after pointer commit; the replication service owns and seals the observation.
The owner boundary is explicit: the source publisher invokes the seam after the pointer commit and
before releasing the refresh/control lock, while the service may only read source evidence and
append downstream evidence. Neither value object contains paths, credentials, provider payload or
mutable handles, and the seam cannot mutate a canonical result. The seam is not the legacy generic
publication callback. Required tests are
`test_source_commit_and_replication_observation_are_frozen_sealed_and_hash_bound`,
`test_post_commit_observation_is_after_pointer_and_cannot_replace_guard`, and
`test_main_wires_guard_and_typed_commit_seam_inside_refresh_lock`.

It also requires the sentinel role/schema, manifest role, object paths, object hashes, sizes,
schemas and row counts to be valid. A mismatch is `LOCAL_POINTER_MISMATCH` and invalidates the
whole candidate. Before sequence allocation, the resulting immutable checkpoint computes
`checkpoint_id` from the complete canonical projection (including pointer row, DB device/inode/
schema, run/date/published_at, manifest/object hashes and the sorted complete inventory); the
preimage never contains `source_sequence`. It contains no payload rows. A source proof MUST also
open the canonical DuckDB with the existing refresh/control writer lock held: open the DB with
`O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, fstat path and fd, verify device/inode, copy bytes through the fd
to a private mode-0600 temporary immutable clone, fsync clone and parent, re-fstat the source
size/mtime/device/inode, and reject any drift. DuckDB is opened only `read_only=True` on that
clone to verify the exact published-snapshot/refresh-run join above and manifest inventory
separately. The clone is deleted or isolated in private quarantine in `finally`; its path is never
public evidence.

If the sidecar is unavailable, a journal stores this complete checkpoint projection and
`checkpoint_id` with no sequence. Recovery enumerates every journal, validates each descriptor-bound
file, and imports them sorted by `(source_published_at, checkpoint_id)`. One private in-memory
transaction allocates the next source sequence and creates the intent, genesis event and mutable
head for every pending checkpoint in that deterministic order. The complete image is then
serialized through the Batch1.3 descriptor-native engine; only after baseline CAS, file fsync and
parent fsync may each journal be archived or deleted. A crash or unlink failure leaves it
re-importable and deduplication is by checkpoint.
The same checkpoint always reuses its sequence and intent, so restart, time and journal order do
not create a second request or intent. Historical files, an unreferenced object, or a later pointer
cannot be selected.

The journal JSON is closed and contains exactly
`journal_schema_version,checkpoint_id,source_instance_id,source_instance_sha256,source_published_at,checkpoint_projection,
publication_binding_sha256,checkpoint_payload_sha256,created_at,journal_sha256`. `checkpoint_projection` is the complete
checkpoint field set used by the `checkpoint_id` preimage; it contains no `source_sequence`,
`intent_id`, `operation_day`, provider payload or credentials. Canonical JSON, one newline and
`journal_sha256 = domain_sha256("stock-eva/r2f4.3/replication-journal/v1",
{journal_schema_version,checkpoint_id,source_instance_id,source_instance_sha256,publication_binding_sha256,checkpoint_payload_sha256,
source_published_at,created_at})` are mandatory. Unknown/duplicate fields, a filename mismatch or
any digest mismatch makes the journal invalid and prevents import.

The typed post-commit seam is `on_canonical_committed(SourceCommit) -> ReplicationObservation`.
The canonical publication owner owns the seam; the replication service owns checkpoint capture,
journal/outbox durability and sanitized observation. It catches every replication exception and
returns a bounded reason/effects projection, never changing the canonical `RefreshResult` or
rolling back a committed pointer. A plain `MarketStore` without an explicit local dataset root
returns `SOURCE_NOT_CONFIGURED`; it never falls back to NAS or a generic root. The seam runs only
after the existing dataset-backed manifest/pointer commit or plain `MarketStore` DuckDB commit,
while the existing project refresh/control writer lock is held. CLI `--execute` acquires the same
lock before source proof or any sidecar/destination operation; external writers that bypass it are
out of scope.

The implementation plan MUST add this reader to `backend/app/main.py`, the local storage
publication callback, and the automation dependency wiring, passing it to the replication service
after the canonical pointer commit. For the dataset-backed publisher, the callback is after the
local manifest and control-pointer commit; for the plain `MarketStore`, it is after the committed
DuckDB publication transaction. Neither callback runs before commit or changes canonical state.
Construction fails clearly when the explicit local root is absent; there is no NAS fallback and no
canonical migration. Required tests are
`test_local_pointer_reader_binds_row_hash_generation_manifest_and_object_set`,
`test_source_pointer_mismatch_is_fail_closed`,
`test_replication_copies_only_current_visible_generation`, and
`test_main_wires_explicit_local_pointer_reader_without_nas_fallback`,
`test_duckdb_source_clone_is_descriptor_bound_and_drift_fails_closed`,
`test_duckdb_clone_requires_ready_refresh_run_and_pointer_manifest_inventory`,
`test_source_proof_uses_published_snapshot_refresh_run_join_and_separate_manifest_inventory`,
`test_source_proof_rejects_refresh_run_status_date_or_completed_at_mismatch`,
`test_checkpoint_journals_import_sorted_by_published_at_and_checkpoint_id`,
`test_multiple_checkpoint_journals_crash_and_recover_exactly_once`, and
`test_cli_execute_holds_existing_project_writer_lock`.

### H2 — strict sidecar identity, immutable evidence and descriptor-bound status

The sidecar is a strict SQLite database, not one mutable audit row. Its first transaction must
insert exactly one `replication_sidecar_meta` row whose
`schema_identity='stock-eva/r2f4.3/replication-sidecar/v1'`, `schema_version=1`,
`ddl_sha256` is the SHA-256 of the normalized DDL below, and `schema_digest` is
`domain_sha256('stock-eva/r2f4.3/replication-schema/v1',
{schema_identity,schema_version,ddl_sha256})`. Any missing, extra, mismatched or duplicate meta
row makes the sidecar unavailable; readers never repair it.
The meta `source_instance_id` and `source_instance_sha256` MUST exactly match the immutable
private `source-instance.json` record and the bound canonical source descriptor. A missing or
conflicting record makes the sidecar `OUTBOX_DURABILITY_UNAVAILABLE`; no journal or intent may be
created until the identity proof is restored by explicit writer execution.

The sidecar has four intentionally different classes of state:

* `replication_intents` is the immutable intent/outbox. Its source pointer identity, object set,
  destination identity, plan hash and intent hash are frozen forever.
* `replication_attempt_events` is an append-only immutable transition/attempt log. Each intent has
  contiguous `event_sequence` beginning at zero; every event carries `prev_event_sha256` and an
  `event_sha256` over its exact fields. No event may be updated or deleted.
* NAS `_replication/history/<replication_generation>/replication-record.json` and NAS
  `_replication/head.json` are the sole destination authority. SQLite stores no destination
  history or head; its optional `replication_destination_cache` is only a last-observed audit
  cache and can never authorize, select or overwrite NAS data.
* `replication_heads` is the only mutable intent lease/state projection. A writer must update it
  with `WHERE intent_id=? AND state_version=?`, incrementing `state_version` exactly once, and
  append the matching event in the same SQLite transaction. A head without a reachable event, an
  orphan event, a gap or a broken hash chain, or a head
  whose last sequence is not the replayed state is unavailable, not an inferred status.

The strict destination reader is the descriptor-native `DestinationArchiveReader`; it opens the
trusted destination root once as `root_dirfd` with
`O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW`, then uses only `openat(root_dirfd, child,
O_RDONLY|O_CLOEXEC|O_NOFOLLOW)` and child directory fds for every sentinel, `head.json`, record,
manifest and object. Each fd is fstat before and after read, and parent device/inode plus the
descriptor fingerprint are checked at every boundary. No path is re-resolved after trust, and
`NasMarketStore` MUST NOT be called to reopen a path. Only its pure sentinel/schema/Parquet
validation functions may be reused after bytes have been read through the bound fds. The reader
recomputes complete head/record preimages and requires all checkpoint, manifest, object-set,
descriptor and parent hashes to agree. A missing/extra field, symlink, inode drift, mismatched
record, broken parent, invalid health cache or head that does not name a complete history
generation is unavailable; SQLite cache is never used as an authority and status never repairs it.

The closed reader contract is:

```text
DestinationArchiveReader.read_sentinel(root_dirfd) -> SentinelProjection
DestinationArchiveReader.read_head(root_dirfd) -> DestinationHead
DestinationArchiveReader.read_record(root_dirfd, replication_generation) -> ReplicationRecord
DestinationArchiveReader.read_manifest(generation_dirfd) -> ManifestBytes
DestinationArchiveReader.read_object(generation_dirfd, relative_path) -> VerifiedObjectBytes
```

`root_dirfd` and `generation_dirfd` are owned descriptor handles, not paths. Relative names are
validated against the fixed archive grammar before `openat`; absolute names, `..`, symlinks,
unexpected device/inode or duplicate/extra entries fail closed. Required tests are
`test_destination_archive_reader_uses_root_dirfd_openat_no_path_reopen`,
`test_destination_archive_reader_rejects_symlink_or_inode_drift`, and
`test_destination_archive_reader_reuses_only_pure_manifest_and_parquet_validation`.

The normative schema is the following exact SQL after removing trailing whitespace and normalizing
line endings. Readers must compare the normalized DDL and digests, not merely table names:

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
    created_at TEXT NOT NULL
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
BEFORE UPDATE ON replication_sidecar_meta BEGIN
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

The event closed field set is exactly `(event_id,intent_id,event_sequence,prev_event_sha256,
event_type,from_state,to_state,attempt,reason_code,state_version,occurred_at,
destination_replication_generation,destination_record_sha256,destination_head_sha256,event_sha256)`; the
event preimage excludes only `event_sha256`, and `event_id` is itself a domain hash of the intent,
sequence, attempt, state version and timestamp. The first event has sequence `0`,
`from_state=NULL`, `to_state='pending'`, and a 64-zero `prev_event_sha256`. Replay requires every
sequence `0..max` exactly once, every transition allowed by the state graph, and the final event
state/version to equal the mutable head. The event and history indexes above are mandatory. The
status benchmark inserts 10,000 terminal intents/events, performs only indexed SELECTs, and asserts
p95 `<500 ms`, unchanged DB bytes/inode and zero Parquet hashing in
`test_status_10k_terminal_rows_under_500ms_without_parquet_scan`.
The DDL identity, immutable triggers and SQLite execution are covered by
`test_replication_sidecar_normative_ddl_identity_and_immutable_event_history`.

### H3 — destination descriptor, mount identity and typed initialization

The source half of an execute is also descriptor-bound: while the existing canonical refresh/
control writer lock is held, open the explicit local DuckDB with
`O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, fstat the path and fd, and require device/inode equality. Copy
bytes through that fd to a private mode-0600 temporary immutable clone, fsync the clone and parent,
then fstat the source again and require unchanged size/mtime/device/inode. Open DuckDB only
`read_only=True` on the clone and verify the exact published-snapshot/refresh-run join above and
complete manifest inventory separately. Any open/read/copy/drift/schema failure is
fail-closed and the temporary clone is removed or isolated in `finally`; no clone path is exposed.
The CLI execute path acquires this same project writer lock before the proof and all writes.
External writers that bypass the project lock are out of scope.

The exact private descriptor filename is `.stock-eva-replication-destination.json`. Its canonical
JSON (sorted keys, compact separators, UTF-8, one terminating newline) has exactly this schema:

```json
{"descriptor_schema":"stock-eva/r2f4.3/destination/v1","schema_version":1,"dataset":"stock-eva-market","role":"nas_archive","direction":"local_to_nas","root_dev":0,"root_ino":0,"parent_dev":0,"parent_ino":0,"mount_point":"/private/approved/child","fs_type":"smbfs","mount_generation":"<64-hex>","mount_fingerprint":"<64-hex>","sentinel_sha256":"<64-hex>","single_writer_host_id":"<64-hex>","created_at":"2026-09-09T00:00:00Z","descriptor_sha256":"<64-hex>"}
```

The placeholder values above are type examples only. `root_dev/root_ino` and
`parent_dev/parent_ino` are captured from the destination and its parent; `mount_point`,
`fs_type`, `mount_generation` and `mount_fingerprint` come from the mount inspector;
`sentinel_sha256` binds the expected role/schema; `single_writer_host_id` identifies the sole
approved writer host; `role` MUST equal `nas_archive` and direction MUST equal `local_to_nas`.
`descriptor_sha256` is
`domain_sha256('stock-eva/r2f4.3/destination-descriptor/v1', all other descriptor fields)`, and
`destination_id` is the first 32 lower-case hex characters of that descriptor hash. The descriptor
file is private control data: public API/CLI/status never returns its path or mount_point.

The exact mount fingerprint preimage is
`domain_sha256('stock-eva/r2f4.3/mount-fingerprint/v1',
{mount_point,fs_type,normalized_options,st_dev,volume_id})`, where `mount_point` is normalized
without symlinks, `normalized_options` is a sorted list of canonical `key=value` options,
`st_dev` is the opened root device, and `volume_id` is the volume/filesystem identifier when the
platform exposes one, otherwise explicit JSON `null`. `mount_generation` is a fresh descriptor
initialization nonce, not a timestamp or ordering value.

Every open boundary reopens the descriptor with no-follow, fstats the bound fd and performs a mount
probe; it compares root/parent device and inode, mount point, filesystem type, mount
generation/fingerprint and sentinel digest. A remount,
inode replacement, device change or sentinel change is `DESTINATION_REBOUND` and cannot be
recovered by retrying the same operation. Descriptor reads are bound to the opened directory fd;
path re-resolution is not a trust proof.

Initialization is a separate command, never an implicit side effect:
`market-replication-init --destination /absolute/new-child --execute
--acknowledge CREATE_EMPTY_NAS_ARCHIVE_R2F4_3`. The exact typed acknowledgement is the literal
`CREATE_EMPTY_NAS_ARCHIVE_R2F4_3`; a boolean, environment variable, free text or a different
spelling is rejected. Without `--execute`, including when the parent is absent, it performs no
mkdir, descriptor, sentinel, sidecar or mount operation. Execute accepts only a new empty child
whose parent is the approved mount; it creates the descriptor, sentinel and non-published staging/
quarantine directories, then fsyncs them. Tests are
`test_destination_descriptor_canonical_json_and_hash`,
`test_destination_descriptor_binds_root_parent_mount_and_sentinel`,
`test_destination_remount_or_inode_change_fails_closed`, and
`test_destination_init_requires_exact_typed_ack_and_is_dry_run_by_default`.

### H4 — monotonic publication lineage and conflict ordering

The canonical `published_snapshots` row and `manifest.json` remain byte-for-byte and schema
unchanged. After a pointer commit, the strict source reader creates the complete replication-only
checkpoint described in H1. The sidecar assigns a monotonic `source_sequence` only during journal
import; operation_day is queue metadata and is not part of checkpoint, plan or intent identity. A
random canonical generation UUID remains an identifier only and MUST NOT be used for ordering.

The NAS archive is the sole destination authority. Each
`_replication/history/<replication_generation>/` directory contains immutable standard dataset
objects plus a self-contained `replication-record.json` carrying source instance/sequence,
checkpoint, parent record, descriptor, manifest/object hashes, complete object inventory and all
counts. `_replication/head.json` is a complete, independently recomputable pointer to exactly one
record; it contains no authority delegated to SQLite. The head is committed atomically only after
the record directory and objects are complete and fsynced. Local SQLite records only queue/event
state and a non-authoritative last-observed destination cache. The archive/head is read directly
for lineage and restore; a local cache can never make a missing NAS record ready.

Lineage is a partial order, not a timestamp guess:

| Comparison | Required result |
|---|---|
| Same source instance/sequence and same manifest/object-set/checkpoint hashes | `ALREADY_REPLICATED`, no NAS record/head write |
| Same source instance and source sequence strictly greater than destination, with a valid parent chain | copy may proceed after head CAS |
| Destination source sequence is greater and source is an ancestor | `DESTINATION_AHEAD`, never overwrite |
| Same sequence with any differing hash, broken parent, or unknown chain | `DESTINATION_CONFLICT` |
| Different source instances or incomparable chains | `DESTINATION_CONFLICT` |
| Final head baseline changed | `CAS_CONFLICT`, preserve the current head |

The `parent_record_hash` chain is verified to genesis (`64` zero hex characters) or a stored
trusted checkpoint; missing, cyclic or ambiguous ancestry is unavailable/conflict, never “latest
wins”. A UUID lexical comparison, canonical `published_at` tie-break, filesystem mtime or retry
order is not an ordering rule. Required test:
`test_nas_record_lineage_source_sequence_parent_record_partial_order` and
`test_destination_ahead_divergent_tie_and_cas_are_fail_closed`.

Before deriving a new destination generation, the writer MUST use
`DestinationArchiveReader` to inspect the candidate history directory. If a record for the same
`checkpoint_id` already exists, it MUST verify descriptor, source-instance record, manifest,
complete object inventory, every object byte/hash and the stored record bytes. An exact valid
record is reused byte-for-byte with its original generation, record hash and immutable `created_at`;
it is never regenerated or overwritten. A differing, incomplete or corrupt existing record maps to
`DESTINATION_CONFLICT`/`DESTINATION_TRUST_FAILED` and is never overwritten. Only an absent
generation may be staged with a new immutable `created_at`. Tests are
`test_existing_generation_record_is_verified_and_reused_byte_identically`,
`test_existing_generation_record_conflict_is_not_overwritten`, and
`test_created_at_is_immutable_across_retries`.

The NAS history record is self-contained and immutable. Every
`_replication/history/<replication_generation>/` directory MUST contain exactly one
`replication-record.json` with this closed field set, plus the existing standard sentinel,
`manifest.json` and referenced immutable Parquet objects:

```json
{"record_schema":"stock-eva/r2f4.3/replication-record/v1","schema_version":1,"replication_generation":"<64-hex>","destination_id":"<32-hex>","direction":"local_to_nas","source_instance_id":"<64-hex>","source_instance_sha256":"<64-hex>","source_sequence":1,"checkpoint_id":"<64-hex>","publication_binding_sha256":"<64-hex>","parent_record_hash":"<64-hex>","pointer_row_sha256":"<64-hex>","pointer_generation":"<opaque>","source_run_id":"<opaque>","source_trade_date":"2026-09-09","source_published_at":"2026-09-09T08:00:00Z","pointer_db_device":1,"pointer_db_inode":1,"pointer_db_schema_digest":"<64-hex>","source_manifest_canonical_sha256":"<64-hex>","source_manifest_bytes_sha256":"<64-hex>","source_object_set_sha256":"<64-hex>","destination_manifest_bytes_sha256":"<64-hex>","destination_object_set_sha256":"<64-hex>","object_inventory":[{"relative_path":"<safe-relative-path>","object_sha256":"<64-hex>","size_bytes":0,"row_count":0,"trade_date":"2026-09-09","source":"<opaque>"}],"object_count":0,"row_count":0,"byte_count":0,"descriptor_sha256":"<64-hex>","plan_sha256":"<64-hex>","created_at":"2026-09-09T08:00:00Z","record_sha256":"<64-hex>"}
```

The example is type-only; values are never placeholders in an actual record. The JSON is sorted,
compact UTF-8 with one newline, contains no unknown/duplicate fields, and `record_sha256` is the
domain hash of every field except itself. The object inventory is complete, sorted by
`(relative_path,object_sha256)`, and is the same canonical inventory used by source and destination
object-set hashes. Restore can therefore verify archive record, manifest and objects without
consulting SQLite, a source pointer or a provider.

The NAS head is also self-contained and atomically replaced. Its exact closed schema is:

```json
{"head_schema":"stock-eva/r2f4.3/replication-head/v1","schema_version":1,"destination_id":"<32-hex>","replication_generation":"<64-hex>","record_sha256":"<64-hex>","descriptor_sha256":"<64-hex>","direction":"local_to_nas","source_instance_id":"<64-hex>","source_instance_sha256":"<64-hex>","source_sequence":1,"checkpoint_id":"<64-hex>","publication_binding_sha256":"<64-hex>","parent_record_hash":"<64-hex>","manifest_sha256":"<64-hex>","object_set_sha256":"<64-hex>","head_version":1,"updated_at":"2026-09-09T08:00:00Z","head_sha256":"<64-hex>"}
```

`head_sha256` hashes every other head field under its named domain; the reader recomputes it and
follows only the named record. The writer stages objects and record in a hidden same-parent
directory, fsyncs them and the history parent, and installs the complete history directory with a
same-parent no-replace operation (`renameat2(RENAME_NOREPLACE)` or a verified exclusive
equivalent). It MUST never replace an existing generation. It then atomically replaces `head.json`
and fsyncs its parent. This NAS head commit is the only destination visibility point. Only after it
succeeds does the writer append the sidecar result and update the non-authoritative destination
cache. A crash after NAS head commit but before SQLite result is recovered by reading NAS head/record
and idempotently appending the already-observed result; a crash before NAS head leaves the previous
head unchanged. No local cache is allowed to select a record or make a missing NAS head ready.
In the head projection, `manifest_sha256` is exactly the referenced record's
`destination_manifest_bytes_sha256` and `object_set_sha256` is exactly its
`destination_object_set_sha256`; these aliases are not independently sourced values.

Every destination writer, including initialization, replication and restore fixtures, MUST use the
descriptor's `single_writer_host_id`, then open `_replication/.writer.lock` with a descriptor-bound
`O_NOFOLLOW` fd and an OS advisory exclusive lock. The lock covers baseline head/history read,
hidden staging, complete verification, immutable history/manifest write, head CAS and fsync. A
writer that cannot acquire or probe this lock is `MOUNT_UNSUPPORTED`, not a retry of an unproven
write. Writers outside this protocol are explicitly out of scope. Required tests cover concurrent
writers, lock loss and crash at every covered phase.

### H5 — truthful mode/effects projections

Every status/API/CLI plan and execute response has this exact additive shape:

```typescript
type ReplicationMode = "status" | "plan" | "execute";
interface Effects {
  writes: boolean;
  canonical_writes: boolean;
  destination_writes: boolean;
  outbox_writes: boolean;
  restore_writes: boolean;
}
interface OperationProjection {
  mode: ReplicationMode;
  execution_allowed: boolean;
  effects: Effects;
  provider_requests: 0;
  paths_exposed: false;
}
```

`status` and `plan` MUST return all effects false. An execute response MUST be built from the
writer's observed effects, not a dry-run constant: a successful replication with a claim and
destination publication returns `writes=true, destination_writes=true, outbox_writes=true` and
canonical/restore false; a failed execute reports any writes that actually occurred and its
terminal reason. An idempotent execute may have no physical destination write, but MUST say
`mode=execute`, `execution_allowed=true` and `writes=false` only when the writer proves no write
occurred. It MUST never label an execute as a plan or silently claim all-false effects. Required
test: `test_execute_response_reports_effects_without_false_dry_run_claim`.

For a lexically valid plan whose strict source/descriptor proofs pass, the controlled projection
is exactly `mode=plan`, `status=dry_run`, `reason_code=NONE`, `execution_allowed=false`,
`provider_requests=0`, and all five effect flags false. A plan never claims an execute, does not
allocate an intent or initialize a destination, and a validation failure retains its specific
allowlisted reason instead of being rewritten as success. Required test:
`test_market_replicate_plan_success_is_dry_run_reason_none_and_zero_write`.

### H6 — hidden restore staging and post-rename rule

Restore requires that the final destination path does not exist. It creates hidden staging through
the parent directory fd with `openat(..., O_CREAT|O_EXCL|O_NOFOLLOW, 0700)` and creates every
staged file with `O_CREAT|O_EXCL|O_NOFOLLOW`, then fsyncs each file, staging directory and parent.
It is named by an opaque restore id and excluded by the existing reader. Before the only visibility point it MUST validate the
source descriptor and fingerprints, exact standard sentinel/manifest schema and role, complete
object set (including no extra/duplicate entries), every object size/schema/row/hash/date, counts,
and representative read-only API queries. It fsyncs every staged file, the staging directory and
parent, then installs the hidden staging directory with a same-parent no-replace operation
(`renameat2(RENAME_NOREPLACE)`, or a platform-equivalent exclusive link/install protocol that
proves no replacement). It MUST NOT use rename-over-existing or `os.replace`. If the final name
appears, the operation returns `PATH_CHANGED`/`DESTINATION_CONFLICT` and never overwrites it. An
existing final destination with the exact checkpoint is read and verified byte-for-byte by
`DestinationArchiveReader` and may return idempotent success without modifying it; a differing
destination is always rejected. The no-replace install is the only visibility point; the existing
reader recognizes only the standard final sentinel and `manifest.json`.

After rename, only bounded non-semantic readback of the final directory/inode and manifest bytes is
allowed; there is no second schema/count/query gate that can retroactively fail the task. If the
rename fails, staging is removed or isolated in quarantine; if a post-rename readback reports an
unexpected inode/bytes result, the destination is isolated and the strict reader rejects it. There
is no typed restore pointer and no modification to canonical control state. Required tests are
`test_restore_semantics_complete_before_atomic_rename`,
`test_restore_rename_failure_leaves_destination_absent_or_quarantined`,
`test_restore_post_rename_readback_is_nonsemantic`,
`test_restore_staging_uses_o_excl_nofollow_and_fsync`,
`test_restore_install_is_no_replace`,
`test_restore_existing_same_checkpoint_is_verified_without_write`, and
`test_restore_existing_destination_is_never_overwritten`.

The crash rows are normative: crash during hidden staging leaves no final destination (or isolated
staging); crash immediately before rename leaves no visible destination; crash during the atomic
rename leaves either the old nonexistent state or one complete directory; crash after rename leaves
the complete standard sentinel/manifest visible; crash during post-rename readback does not rerun
semantic gates. Canonical local manifest, pointer and control DB bytes/inodes are unchanged in
every row.

Every execution writes a sanitized immutable `RestoreReport` only to private restore evidence (or
the hidden/new temporary root before rename), never to canonical or NAS authority. Its exact fields
are `report_schema`, `schema_version`, `report_id`, `destination_id`,
`source_replication_generation`, `source_record_sha256`, `source_instance_id`, `source_sequence`,
`checkpoint_id`, `object_count`, `row_count`, `byte_count`, `verification_state`, `reason_code`,
`started_at`, `completed_at`, and `report_sha256`; no path, credential or payload is allowed.
`report_sha256 = domain_sha256("stock-eva/r2f4.3/restore-report/v1", all fields except
report_sha256)`. The report is fsynced before its private atomic rename; a failed or interrupted
restore leaves only a non-reader-visible report/quarantine record and remains independently
auditable.

### M1/M2/M3/M4 — closed status, early CLI dispatch, source and journal rules

The public `ReplicationReason` is a closed union:
`NONE | DISABLED | SOURCE_NOT_CONFIGURED | SOURCE_UNAVAILABLE | LOCAL_POINTER_MISMATCH |
REPLICATION_STATE_UNAVAILABLE | OUTBOX_ENQUEUE_FAILED | OUTBOX_JOURNALED |
OUTBOX_DURABILITY_UNAVAILABLE | DESTINATION_UNAVAILABLE | DESTINATION_MOUNT_UNAVAILABLE |
DESTINATION_TRUST_FAILED | DESTINATION_REBOUND | DESTINATION_AHEAD | DESTINATION_CONFLICT |
CAS_CONFLICT | COPY_FAILED | VERIFY_FAILED | RETRY_WAIT | DEAD_LETTER | ALREADY_REPLICATED |
PATH_INVALID | PATH_CHANGED | SYMLINK_UNSAFE | DIRECTION_NOT_ALLOWED | MOUNT_UNSUPPORTED`.
Unknown internal failures map to `REPLICATION_STATE_UNAVAILABLE`; no free-form reason is public.

The unique reason priority is surface-specific and fixed. For disabled status/API/automation, the
first branch is `DISABLED` and returns before constructing a sidecar, source reader, destination
reader, lock or provider; it performs zero sidecar I/O. For an explicitly enabled writer, priority
is lexical path/direction → sidecar schema/control proof → source configuration/checkpoint proof →
destination mount/descriptor trust → lineage/CAS → copy/readback → retry/dead-letter →
idempotent/ready. For explicit CLI status, the disabled branch still returns `DISABLED` before
sidecar I/O; only when enabled does lexical validation precede an optional strictly read-only
sidecar proof. It may not initialize, migrate, repair or acquire a lock. A more specific earlier
class wins; status never overwrites a higher-priority reason with a later projection. This mapping
and priority is implemented and tested, not inferred from enum order.
The `NONE -> ready` row applies to status and execute; the sole plan-mode success projection is
`mode=plan,status=dry_run,reason_code=NONE` with all effects false, as defined in H5.

The hash contract is closed and exact. Every set/list is sorted by the stated tuple and no object
contains its own digest field in the preimage:

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
checkpoint_payload_sha256 = domain_sha256("stock-eva/r2f4.3/source-checkpoint-payload/v1",
  {checkpoint_id,source_instance_id,source_instance_sha256,pointer_row_sha256,pointer_generation,source_run_id,
   source_trade_date,source_published_at,pointer_db_device,pointer_db_inode,
   pointer_db_schema_digest,manifest_canonical_sha256,source_manifest_bytes_sha256,
   source_object_set_sha256,object_inventory,publication_binding_sha256})
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
   pointer_generation,source_run_id,source_trade_date,source_published_at,pointer_db_inode,
   pointer_db_device,pointer_db_schema_digest,manifest_canonical_sha256,source_object_set_sha256,
   source_instance_id,source_instance_sha256,source_sequence,source_manifest_bytes_sha256,publication_binding_sha256,
   plan_sha256,object_count,row_count,byte_count,created_at})
```

`prev_event_sha256` and `parent_record_hash` at genesis are exactly 64 zero hex characters;
all other digests are 64 lower-case hex. `checkpoint_id` is computed before SQLite allocation and
does not include `source_sequence`. Recovery sorts every valid journal by
`(source_published_at,checkpoint_id)` and, in one private in-memory transaction, allocates the
next sequence and creates intent/event/head rows. The complete image is serialized through the
descriptor-native engine with baseline CAS and fsync. The unique constraints make allocation and
journal import restart-safe: a repeated checkpoint or retry reuses its existing sequence and
intent, regardless of operation_day. `operation_day` is scheduling metadata only and is excluded
from `checkpoint_id`, `plan_sha256`, `intent_id` and `intent_sha256`. The normalized JSON encoder is the one defined above,
including explicit null/false/zero/empty values and one final newline. Golden tests mutate one
field, one sort order and the newline and prove a different digest. The canonical source digest
name is always `source_object_set_sha256`; a bare `object_set_sha256` is reserved only for the
destination-head compatibility alias. Required vector test:
`test_source_object_set_sha256_golden_vectors_are_canonical`.

New CLI commands dispatch before `ensure_local_runtime_dirs()`, DB initialization or any provider
factory: lexical validation is first, then strict read-only proofs. `market-replicate`,
`market-restore`, `market-replication-init` and `market-replication-status` with no `--execute`
must leave a missing parent byte/inode-identical. Execute alone may create explicitly allowed local
sidecar parents. Replication source is always `local_market_dataset_root`; NAS-only configuration
returns `SOURCE_NOT_CONFIGURED` and does not read NAS. Required tests include
`test_market_replicate_dry_run_parent_absent_creates_nothing`,
`test_market_restore_dry_run_parent_absent_creates_nothing`,
`test_market_replication_status_missing_sidecar_does_not_initialize`, and
`test_nas_only_configuration_never_becomes_replication_source`.

The journal filename is `<checkpoint_id>.json` under the configured local journal directory. Create
a unique temporary sibling with `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW` mode `0600`, write the complete
newline-terminated canonical bytes, `fsync(fd)`, fsync the journal directory, then install the
target with macOS `renameatx_np(RENAME_EXCL)` or a portable hard-link-no-replace fallback followed
by temporary-file unlink and parent fsync. If the target already exists, open it no-follow and
reuse it only when its bytes are exactly identical; any differing target is
`OUTBOX_DURABILITY_UNAVAILABLE` and is never overwritten. Import opens no-follow and fstats
before/after; it validates the checkpoint/binding hash, sorts all pending journals by
`(source_published_at,checkpoint_id)`, inserts intent/event/head rows in one private in-memory
transaction, serializes the complete image through the descriptor-native engine, fsyncs the DB and
parent, and only then unlinks the journal and fsyncs its parent. Unlink failure leaves a harmless
journal for idempotent re-import. Crash tests cover temp, install, existing-target and import
boundaries and require no provider request. The journal-specific anchors are
`test_journal_temp_uses_o_excl_no_follow_and_fsync`,
`test_journal_install_is_no_replace_and_reuses_identical_target`, and
`test_journal_conflicting_target_fails_without_overwrite`.

The command grammar and precedence are fixed:

```text
stock-eva market-replicate [--destination ABSOLUTE_PATH] [--operation-day YYYY-MM-DD] [--execute] [--json]
stock-eva market-restore --source ABSOLUTE_PATH --destination ABSOLUTE_PATH [--execute] [--json]
stock-eva market-replication-init --destination ABSOLUTE_PATH [--execute --acknowledge CREATE_EMPTY_NAS_ARCHIVE_R2F4_3] [--json]
stock-eva market-replication-status [--json]
```

Unknown or repeated flags, missing values, non-ISO dates and lexical path violations return exit
code `2` before any filesystem access. A destination flag wins over
`STOCK_EVA_REPLICATION_DESTINATION_ROOT`, which wins over the default `None`; no other config
source is consulted. The source is never a flag: it is always the explicit
`settings.local_market_dataset_root`, and a NAS-only configuration is `SOURCE_NOT_CONFIGURED`.
`--execute` is the only write gate; omitted `--execute` is a read-only plan. `--json` changes only
serialization, not effects or validation order. The status command has no destination/source
override and can only perform the strict sidecar SELECT described above.

### M5/M6 — exact configuration and mandatory evidence

The only configuration names and defaults are:

| Setting | Environment | Default / rule |
|---|---|---|
| `replication_enabled` | `STOCK_EVA_REPLICATION_ENABLED` | `false` |
| `replication_drain_enabled` | `STOCK_EVA_MARKET_REPLICATION_DRAIN_ENABLED` | `false`; automation NAS writes require this plus an approved descriptor and lock |
| `replication_destination_root` | `STOCK_EVA_REPLICATION_DESTINATION_ROOT` | `None`; explicit absolute approved child only |
| `replication_database_name` | `STOCK_EVA_REPLICATION_DATABASE_NAME` | `replication.sqlite3`; safe basename only |
| `replication_journal_root_name` | `STOCK_EVA_REPLICATION_JOURNAL_ROOT_NAME` | `replication-journal`; safe basename only |
| `replication_max_attempts` | `STOCK_EVA_REPLICATION_MAX_ATTEMPTS` | `6`; fixed upper bound |
| `replication_lease_seconds` | `STOCK_EVA_REPLICATION_LEASE_SECONDS` | `900`; positive bounded integer |
| `replication_drain_timeout_seconds` | `STOCK_EVA_REPLICATION_DRAIN_TIMEOUT_SECONDS` | `900`; maximum 15 minutes |
| `replication_direction` | `STOCK_EVA_REPLICATION_DIRECTION` | `local_to_nas`; any other value rejected |

Retry delays are not environment-configurable and remain exactly `(60,300,1800,7200,43200)`
seconds. No unresolved environment syntax, `HOME`, root, share root, credential or path fallback
is accepted. Descriptor schema, descriptor digest, mount identity and the typed acknowledgement
above are fixed contracts, not operator-provided strings.

`replication_enabled` permits only local checkpoint capture/enqueue. Automation may write NAS only
when `replication_drain_enabled` is true, the descriptor is approved and the project writer lock
is held. CLI `--execute` is an independent explicit authorization and does not enable background
draining. These rules are covered by
`test_replication_enabled_only_enqueues_when_drain_disabled`,
`test_automation_drain_requires_flag_and_approved_descriptor`, and
`test_cli_execute_is_independent_explicit_authorization`.

The following NFR rows are added to the existing FR/AC/EC crosswalk in both documents. They are
planned exact anchors and MUST be created and passed during implementation; a future test name is
not current evidence.

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

The implementation crosswalk MUST include these exact rows byte-for-byte. The focused gate also
requires explicit assertions for canonical object/pointer bytes and inodes, zero provider/network
requests, the 15-minute budget, and every hash golden vector. The implementation plan's steps,
models, DDL, API, reason mapping, transaction and crash matrix are amended by this section and
must reproduce it before any implementation is called SPEC GO.

## R2-F4.3.3 sixth-round normative amendment — manifest publication lock and plain-store boundary

This amendment is the sole authority for the H1/M1/M3 closure and supersedes any earlier
statement that makes the refresh lock, a callback, or a plain `MarketStore` the publication
atomicity boundary. It remains a specification only: no implementation, NAS access, mount,
provider request or production execution is authorized.

### H1 — one lock-owned dataset publication

The existing `_ManifestLock` is promoted to the internal `ManifestPublicationLock`. It is
non-reentrant and its acquisition returns an opaque, unforgeable
`ManifestPublicationLockToken`; the token cannot be serialized, supplied by a caller, or acquired
twice by the same publication. The dataset-backed central wrapper
`publish_dataset_and_pointer(...)` acquires exactly one token and owns it until the
post-commit exact proof completes. Its ordered critical section is:

```text
baseline descriptor-bound manifest read + fingerprint
  -> objects and private staging creation/readback
  -> manifest replacement + fsync + strict readback
  -> immutable source-binding write/readback + file/parent fsync
  -> final manifest fingerprint CAS
  -> DuckDB external publication pointer commit
  -> pointer/manifest/binding exact proof
```

The final fingerprint is
`manifest_fingerprint = domain_sha256("stock-eva/r2f4.3/manifest-fingerprint/v1", {
manifest_bytes_sha256,manifest_canonical_sha256,manifest_generation,source_object_set_sha256})`.
The wrapper re-reads it through descriptor-bound handles immediately before the pointer
transaction. Any generation, byte hash, canonical hash or object-set drift returns
`LOCAL_POINTER_MISMATCH`, leaves the previous pointer untouched and keeps the new binding
inactive. The lock remains held through the pointer transaction and the exact proof; enqueue,
journal import and NAS drain begin only after the local publication boundary has completed.

Every dataset-backed `NasMarketStore` writer uses this wrapper exactly once:
`save_refresh`, `reconcile_control_pointer`, `backfill`, `upsert`, refresh,
`full_history` and CLI reconciliation. Their internal `_publish_bars_locked(token, ...)` path
requires the wrapper-owned token and never acquires `_ManifestLock`; no public method may acquire
the lock and then call another public method that acquires it. All manifest mutation paths use the
same lock, so concurrent backfill/upsert/refresh/reconcile cannot mutate the baseline or manifest
between the read, binding and pointer phases. A startup, CLI or `full_history` caller passes no
lock token and does no lock acquisition itself.

`RefreshRunLock` is optional scheduling coordination only. When present, its fixed order is
`RefreshRunLock -> ManifestPublicationLock`; the wrapper never acquires `RefreshRunLock`, and no
path acquires the two in reverse order. Publication correctness never depends on the refresh lock.
Lock busy, token mismatch, lock-order inversion or any pre-pointer readiness failure is fail-closed
with no pointer advance; a post-pointer unlock/close failure follows the M3 degraded path below.
Required concurrency/crash anchors are
`test_publication_is_atomic_under_one_manifest_publication_lock`,
`test_manifest_lock_blocks_concurrent_backfill_or_upsert_mutation`,
`test_manifest_fingerprint_drift_blocks_pointer_cas`,
`test_refresh_run_lock_is_not_publication_atomicity_dependency`,
`test_pointer_unchanged_after_manifest_publication_crash_before_pointer`, and
`test_binding_reproved_after_manifest_publication_crash_after_pointer`.

### M1 — legacy null digests and required v2 metadata

The legacy/no-selection adapter has exactly these canonical null digests; omission, the string
`"null"`, an empty object and an empty string are different and invalid:

```text
canonical_null_selection_sha256 =
  domain_sha256("stock-eva/r2f4.3/selection-null/v1", None)
canonical_null_lineage_sha256 =
  domain_sha256("stock-eva/r2f4.3/lineage-null/v1", None)
```

The canonical JSON preimage for each is the literal UTF-8 `null` followed by one newline under
the stated domain. A legacy publication may reuse an existing immutable binding only when these
digests and every pointer/manifest field match under the publication lock. A schema-v2 manifest
must contain `provider_id,universe_id,evidence_id,evidence_sha256,candidate_id,
candidate_manifest_sha256,gate_report_sha256,adapter_version,source_schema_version` for every
object entry; required-but-missing, malformed or conflicting metadata returns `SOURCE_UNAVAILABLE`
before binding/pointer commit. This is distinct from an intentionally absent selection, which uses
the two fixed null digests. Required anchors are
`test_legacy_no_selection_uses_canonical_null_digests` and
`test_missing_v2_manifest_metadata_blocks_pointer`.

### M3 — plain `MarketStore` compatibility and ownership

The plain `MarketStore` path is not a dataset-backed replication writer. Its existing canonical
commit, locking, bytes and behavior remain unchanged. It does not write a source binding, run the
pre-publication durability guard, acquire `ManifestPublicationLock`, infer a dataset root, or
access NAS. After its successful canonical DuckDB commit, the typed replication observation is
`SOURCE_NOT_CONFIGURED` unless an explicit dataset/manifest-backed `NasMarketStore` wrapper was
used. Replication errors can therefore never alter plain-store canonical success. The only paths
that may produce an active `SourceCommit` are the dataset-backed wrapper methods listed above.
`test_plain_market_store_canonical_commit_is_unchanged_and_replication_is_not_configured` is the
required compatibility anchor.

### File ownership and transaction/crash matrix

`backend/app/storage/dataset.py` owns `ManifestPublicationLock`, its opaque token, the central
wrapper and `_publish_bars_locked`; `backend/app/market/refresh.py`, `backfill.py`,
`full_history.py` and `backend/app/cli.py` call the wrapper without acquiring the manifest lock;
`backend/app/market/store.py` retains the plain canonical path. The transaction boundary is:

```text
baseline manifest -> objects/staging -> manifest replace/readback
  -> binding fsync -> final fingerprint CAS -> pointer commit -> exact proof
  -> release manifest lock -> journal/outbox observation
```

| Failure point | Required result |
|---|---|
| Concurrent mutation/backfill while wrapper owns the lock | Mutation waits or returns busy; it cannot alter the baseline or publish a stale pointer. |
| Final manifest fingerprint drift | `LOCAL_POINTER_MISMATCH`; previous pointer remains ready; new manifest/binding is inactive. |
| Binding/fsync failure | `OUTBOX_DURABILITY_UNAVAILABLE`; pointer is not attempted and prior pointer bytes remain unchanged. |
| Crash before pointer commit | Old pointer remains visible; staging/temp/binding may be quarantined but never becomes active. |
| Crash after pointer commit before exact proof | Reconciliation reopens the same lock-bound manifest/binding and proves the committed pointer; it does not regenerate or overwrite the binding. |
| Plain `MarketStore` canonical commit or replication failure | Existing canonical result/bytes are preserved; observation is `SOURCE_NOT_CONFIGURED`; no replication side effect can roll it back. |

The design and implementation crosswalks include FR-3b/3c/3d, AC-21/AC-22 and EC-27 through
EC-30 with exact anchors. The status remains `In Review / NO-GO`; this amendment does not claim
implementation or SPEC/QUALITY approval.

## R2-F4.3.4 seventh-round normative amendment — coordinator operations and phase errors

This amendment supersedes earlier coordinator wording and closes H1/M2/M3/L. It is still a
specification only; implementation, NAS access, provider requests and production execution remain
blocked.

### H1 — two mutually exclusive typed coordinator operations

One `ManifestPublicationCoordinator` owns the single `ManifestPublicationLock` and exposes only
these two operations:

```typescript
type ManifestOnlyInput = Readonly<{
  bars: ReadonlyArray<DailyBar>;
  lineage_input: LineageInput;
  source: string;
}>;
type ManifestOnlyResult = Readonly<{
  manifest_generation: string;
  manifest_bytes_sha256: string;
  manifest_canonical_sha256: string;
  source_object_set_sha256: string;
  pointer_row_sha256_before: string;
  pointer_db_device_before: number;
  pointer_db_inode_before: number;
  pointer_db_schema_digest_before: string;
  pointer_unchanged: true;
}>;
type DatasetPointerInput = Readonly<{
  bars: ReadonlyArray<DailyBar>;
  result: RefreshResult;
  selection: PublishedSelection | null;
  lineage_input: LineageInput;
  source: string;
}>;
type DatasetPointerResult = Readonly<{
  manifest_generation: string;
  publication_binding: SourcePublicationBinding;
  source_commit: SourceCommit;
  observation: ReplicationObservation;
}>;

interface ManifestPublicationCoordinator {
  publish_manifest_only(input: ManifestOnlyInput): ManifestOnlyResult;
  publish_dataset_and_pointer(input: DatasetPointerInput): DatasetPointerResult;
}
```

`publish_manifest_only(...)` acquires one coordinator-owned token, runs baseline → objects/staging
→ manifest replace/readback → final manifest proof, and returns only manifest hashes plus the
pointer-unchanged proof. It MUST NOT create/read/write a publication binding, call a pointer
writer, run the pre-publication guard, or emit a `SourceCommit`. `upsert_bars` and every backfill
batch use this operation. `publish_dataset_and_pointer(...)` acquires the same token and runs the
complete manifest → binding → final fingerprint CAS → DuckDB pointer commit → exact proof path.
Manifest-only MUST NOT call or delegate to the pointer operation; the two operations are mutually
exclusive even though they share the token-checked locked primitive internally.

The real writer inventory is closed: `NasMarketStore.save_refresh` and
`NasMarketStore.reconcile_control_pointer`, dataset `upsert_bars`, `backfill` batches,
`full_history` publication, CLI reconciliation and startup reconciliation. `save_refresh`,
reconcile, full_history, CLI and startup call `publish_dataset_and_pointer`; `upsert_bars` and
backfill call `publish_manifest_only`. No caller acquires or passes a lock token. `MarketStore`
without an explicit dataset/manifest remains outside this inventory and keeps its existing
canonical commit. Required anchors are
`test_manifest_only_operation_never_calls_pointer_wrapper`,
`test_manifest_only_upsert_and_backfill_preserve_pointer_bytes_hash_and_inode`,
`test_all_dataset_pointer_writers_route_through_coordinator`, and
`test_publication_is_atomic_under_one_manifest_publication_lock`.

### M2 — one legacy/modern v2 lineage algorithm

There are no new manifest-level fields; the existing manifest bytes and object entry shape remain
unchanged. Let `L` be exactly the object lineage field set
`{provider_id,universe_id,evidence_id,evidence_sha256,candidate_id,
candidate_manifest_sha256,gate_report_sha256,adapter_version,source_schema_version}`. For every
object entry, classify only as follows:

1. If every field in `L` is absent and no unknown lineage field exists, select `legacy` and set
   `selection_sha256 = domain_sha256("stock-eva/r2f4.3/selection-null/v1", None)` and
   `lineage_sha256 = domain_sha256("stock-eva/r2f4.3/lineage-null/v1", None)`. The preimage is
   literal canonical JSON `null` plus one newline; do not use omission, `"null"`, `{}` or `""`.
2. If every field in `L` exists exactly once, select `modern` and strictly validate type, lower-case
   64-hex digest format, non-empty identities and all selection/gate/evidence consistency against
   the existing immutable evidence. Modern values are never silently replaced with nulls.
3. Any partial presence, unknown field, duplicate key, malformed value, conflicting object values
   or conflict with the selected candidate is `SOURCE_UNAVAILABLE`; no manifest binding or pointer
   commit is attempted. The same rule applies during reconcile.

The mode is per object but the candidate is publishable only when all entries resolve to one
consistent mode and source lineage; mixed legacy/modern entries are `SOURCE_UNAVAILABLE`. Required
anchors are `test_legacy_no_selection_uses_canonical_null_digests`,
`test_modern_v2_lineage_is_strictly_validated`,
`test_partial_unknown_or_conflicting_v2_lineage_is_source_unavailable`, and
`test_manifest_has_no_new_level_fields_for_lineage_compatibility`.

### M3 — pointer linearization and phase-specific lock errors

Before the pointer transaction, only lock acquire/token validation, baseline read, precommit guard
and final fingerprint CAS are precommit phases. Any failure in these phases MUST
leave pointer bytes, pointer row hash and pointer DB inode unchanged; no binding becomes active and
the typed result is the corresponding fail-closed precommit reason.

The DuckDB pointer commit is the linearization point. After it succeeds, an unlock, file close,
post-proof or observation-control error MUST NOT attempt rollback or hide the ready canonical
pointer. The wrapper emits a degraded `ReplicationObservation` with reason
`CONTROL_STATE_UNAVAILABLE`, durably writes sanitized reconciliation evidence when possible, and
returns canonical `ready`. The next `before_canonical_pointer_commit` guard MUST re-open the
descriptor-bound pointer/binding state and reconcile/prove it before allowing another pointer
commit. This is distinct from a precommit `OUTBOX_DURABILITY_UNAVAILABLE` failure.

| Phase | Failure result | Pointer effect |
|---|---|---|
| lock acquire/token/baseline/precommit/final-CAS | typed unavailable/mismatch | unchanged bytes/hash/inode |
| pointer transaction | canonical transaction failure | unchanged bytes/hash/inode |
| pointer committed, unlock/close/post-proof/observation control | `degraded/CONTROL_STATE_UNAVAILABLE` plus durable reconciliation | remains ready; no rollback |

Required anchors are `test_pointer_precommit_lock_error_preserves_pointer_identity`,
`test_post_pointer_unlock_failure_degrades_without_rollback`, and
`test_pointer_postcommit_control_error_is_durable_and_next_guard_reconciles`.

### L — closed implementation/file inventory and crash evidence

The implementation inventory must name these exact seams and no generic callback substitute:

| Writer seam | Coordinator operation | Pointer guarantee |
|---|---|---|
| `backend/app/storage/dataset.py::NasMarketStore.save_refresh` | `publish_dataset_and_pointer` | binding/pointer exact proof under one token |
| `backend/app/storage/dataset.py::NasMarketStore.upsert_bars` | `publish_manifest_only` | pointer bytes/hash/inode unchanged |
| `backend/app/storage/dataset.py::NasMarketStore.reconcile_control_pointer` | `publish_dataset_and_pointer` | existing binding may be reused only after exact proof |
| `backend/app/market/backfill.py::BackfillService.plan/execute` | `publish_manifest_only` per date/group after all date resolutions pass | explicit lineage input per `trade_date`; any resolver failure is whole-batch `SOURCE_UNAVAILABLE` before the first upsert |
| `backend/app/market/full_history.py` | `publish_dataset_and_pointer` | no direct lock or pointer save |
| `backend/app/cli.py` reconciliation | `publish_dataset_and_pointer` | no direct lock or pointer save |
| `backend/app/main.py` startup reconciliation | `publish_dataset_and_pointer` | no direct lock; next guard owns recovery |

The crash matrix is extended: a manifest-only crash leaves only manifest/staging state and the
prior pointer unchanged; a pointer precommit lock/token/CAS failure leaves pointer identity
unchanged; a post-pointer unlock/close failure records degraded control state and next-guard
reconciliation, never rollback. The crosswalk includes FR-3e/3f, AC-23/24 and EC-31/32/33 with
real named anchors. Status remains `In Review / NO-GO`.

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
