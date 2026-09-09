# Stock EVA R2-F4.3 Local Replication and Verified Restore

**Author:** Codex

**Date:** 2026-09-09 (Asia/Shanghai)

**Status:** In Review / NO-GO pending the R2-F4.3.1 amendment below; implementation remains blocked

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
- FR-2: A successful local canonical publication MUST define a deterministic replication intent from the committed local manifest/pointer identity; replication MUST observe the committed pointer and MUST NOT fetch or republish market data.
- FR-3: The canonical publication transaction MUST commit before any outbox enqueue attempt; an enqueue failure MUST NOT roll back, hide, or change a ready local pointer.
- FR-4: The outbox MUST be a local durable SQLite sidecar with the normalized schema, immutable intent identity, allowlisted states, state-version CAS and durable terminal audit defined in this document.
- FR-5: An enqueue failure MUST be observable as a sanitized `OUTBOX_ENQUEUE_FAILED` or `OUTBOX_JOURNALED` projection; a crash between pointer commit and enqueue MUST be recoverable by comparing the current strict source manifest with durable outbox/journal state.
- FR-6: Source discovery MUST use one descriptor-bound strict snapshot of the local immutable sentinel, manifest and every referenced Parquet object, including size, schema, row count, object hash, inode/fingerprint and source manifest bytes hash.
- FR-7: A destination MUST be an explicitly supplied absolute path with a trusted replication descriptor, expected dataset sentinel, valid manifest role, approved SMB/CIFS mount when configured as NAS, and no unsafe overlap with local mutable roots.
- FR-8: All object transfer MUST use a destination staging namespace that is outside the published manifest and cannot be served as a dataset generation.
- FR-9: Before promotion, every destination object MUST be read back and match source size, canonical schema, row count, SHA-256, source lineage and expected relative path.
- FR-10: Destination publication MUST write all verified immutable objects and an immutable replication history record first, then advance the separate destination head atomically under lock; no partial object/history or partially assembled generation may be visible through a strict reader.
- FR-11: Destination lineage MUST bind direction, source checkpoint (`source_instance_id`/`source_sequence`), source manifest/object-set hashes, replication generation, parent replication hash, plan hash, destination trust identity and complete object inventory; an unknown, older, conflicting or reverse lineage MUST fail closed.
- FR-12: Replicating the same source instance/sequence and checkpoint to the same trusted destination MUST be idempotent and return `ALREADY_REPLICATED` without copying or changing destination history/head.
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
- NFR-3: A transfer MUST be single-generation atomic from the destination reader's perspective: immutable history is complete before the destination head changes once, only after all objects pass readback.
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

### AC-2: Pointer-before-enqueue boundary (FR-2, FR-3, FR-5, FR-25, NFR-6, NFR-10)

Given a canonical callback commits a ready manifest/pointer and the outbox insert raises a synthetic storage error, When the refresh completes, Then the ready result and pointer are unchanged, the error is projected as `OUTBOX_ENQUEUE_FAILED`, no provider is called again, and the recovery reconciliation reports the missing intent without rolling back canonical state.

### AC-3: Crash recovery journal (FR-4, FR-5, FR-26, NFR-6, NFR-7)

Given a process stops after the pointer commit and before the SQLite outbox commit, When the next explicit drain reconciles the current strict manifest and intent journal, Then exactly one deterministic intent is persisted and claimed, duplicate imports are no-ops, and the prior canonical pointer remains byte-identical.

### AC-4: Strict outbox contract (FR-4, FR-14, FR-15, NFR-6)

Given a fresh empty local control root, When the execution service initializes the outbox and claims an intent twice concurrently, Then the normalized DDL is accepted, one claim wins by state-version CAS, the losing worker performs no copy, and terminal rows cannot be deleted or have immutable identity fields changed.

### AC-5: Complete source snapshot (FR-6, FR-9, FR-11, FR-26, NFR-2, NFR-9)

Given a manifest references an object with a missing file, changed inode, wrong size, wrong schema, wrong row count or wrong hash, When replication plans the intent, Then the complete source candidate is invalid, no destination object/history/head is written, and the sanitized reason identifies the failing validation stage.

### AC-6: Destination trust and path safety (FR-7, FR-21, FR-23, FR-24, NFR-8, NFR-9)

Given a destination is relative, contains unresolved environment syntax, is `/`, overlaps local mutable storage, has a symlink ancestor, has an unexpected mount, or lacks a trusted destination descriptor, When `market-replicate` is run without `--execute`, Then it returns a bounded plan/error without creating a directory, reading credentials, initializing a sidecar or exposing the supplied path.

### AC-7: Verified staged transfer (FR-8, FR-9, FR-10, FR-11, NFR-2, NFR-3, NFR-9)

Given a trusted empty destination and a complete local source checkpoint, When one intent executes against an offline fixture, Then each immutable object is copied to hidden staging, read back for size/schema/rows/hash, written into an immutable replication history generation, and only then is the destination head atomically advanced.

### AC-8: Failure leaves destination head/history unchanged (FR-8, FR-9, FR-10, FR-12, NFR-3, NFR-12)

Given an object copy, readback, history write or head CAS is interrupted, When the operation terminates, Then the prior destination head/history remains readable and byte-identical, no partial namespace is referenced by it, and a strict reader cannot see an incomplete generation.

### AC-9: Idempotence and no old overwrite (FR-11, FR-12, FR-13, FR-27, NFR-3, NFR-14)

Given a destination already contains the exact source instance/sequence lineage, an older source lineage, a conflicting object hash or a reverse-direction marker, When replication is attempted, Then exact lineage is `ALREADY_REPLICATED`, destination-ahead/conflicting/reverse lineage is rejected with an allowlisted reason, and neither destination history/head nor local canonical pointer changes.

### AC-10: Retry and dead-letter (FR-14, FR-25, FR-26, NFR-4, NFR-5)

Given a destination is temporarily unavailable or a bounded copy fails, When the drain is invoked repeatedly, Then the intent follows `pending → copying/verifying → retry_wait` with the exact bounded delay schedule, reaches `dead_letter` after six failed claims, and never issues a market-provider request.

### AC-11: Concurrency and stale lease (FR-4, FR-15, FR-26, NFR-6)

Given two workers share one outbox and one destination and one worker crashes while leased, When the second worker observes lease expiry, Then it can claim the same deterministic intent exactly once, a stale worker cannot advance state or publish a destination head, and state-version conflicts are sanitized.

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
- EC-11: A destination head/history baseline changes between locked read and head CAS → CAS fails, current destination head is preserved, and the intent enters bounded retry or terminal conflict.
- EC-12: Copy fails after any number of objects or process exits before history/head commit → no partial generation is referenced; retry reuses only hash-verified immutable objects.
- EC-13: Readback size/schema/rows/hash or lineage verification fails → mark the attempt failed, never publish destination history/head, and retain only sanitized diagnostics.
- EC-14: Outbox database is missing, corrupt, schema-mismatched or locked on status → return `REPLICATION_STATE_UNAVAILABLE`; status must not initialize, migrate or write it.
- EC-15: Outbox enqueue fails after canonical pointer commit → preserve local ready, atomically write the deterministic intent journal when possible, and expose `OUTBOX_ENQUEUE_FAILED` or `OUTBOX_JOURNALED`.
- EC-16: Both outbox and journal writes fail after pointer commit → preserve local ready, expose `OUTBOX_DURABILITY_UNAVAILABLE`, and let the next strict reconciliation derive the current intent without fetching data.
- EC-17: Process crashes after journal write before outbox import → import is idempotent; journal removal is attempted only after the outbox row is committed.
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
  | "OUTBOX_DURABILITY_UNAVAILABLE" | "DESTINATION_UNAVAILABLE"
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
  source_instance_id: string | null;
  source_sequence: number | null;
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
  source_instance_id: string | null;
  source_sequence: number | null;
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
`destination_health_observed_at` are read only from the mutable destination-head health projection
written by a locked writer probe; status never opens a network socket, mounts a share, refreshes
health or turns an absent probe into healthy. Queue lag is derived from the sidecar's committed
timestamps and is `null` when control evidence is not proven.

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
source_schema_version}` with absent legacy lineage values represented as explicit JSON `null`.
`files` is sorted by `(path,sha256)` before hashing. Thus
`manifest_canonical_sha256 = domain_sha256("stock-eva/r2f4.3/manifest-canonical/v1",
manifest_projection)`. The control schema projection is the sorted, self-excluded SQLite
`sqlite_master` tuple list `{type,name,tbl_name,sql}` for all application tables, indexes and
triggers; `pointer_db_schema_digest = domain_sha256("stock-eva/r2f4.3/pointer-db-schema/v1",
schema_projection)`. Direct byte hashes (`source_manifest_bytes_sha256`) are SHA-256 of exact
descriptor-read bytes and have no JSON preimage.

The closed preimages are:

```text
object_set_sha256 = domain_sha256("stock-eva/r2f4.3/object-set/v1", object_inventory)
source_instance_id = domain_sha256("stock-eva/r2f4.3/source-instance/v1",
  {pointer_db_schema_digest,pointer_db_inode})
source_snapshot_sha256 = domain_sha256("stock-eva/r2f4.3/source-snapshot/v1",
  {source_instance_id,source_sequence,pointer_row_sha256,pointer_generation,
   source_run_id,source_trade_date,source_published_at,pointer_db_inode,
   pointer_db_schema_digest,manifest_canonical_sha256,source_manifest_bytes_sha256,
   object_set_sha256,object_inventory})
plan_sha256 = domain_sha256("stock-eva/r2f4.3/replication-plan/v1",
  {direction,destination_id,source_snapshot_sha256,object_inventory})
intent_id = domain_sha256("stock-eva/r2f4.3/replication-intent/v1",
  {operation_day,direction,destination_id,source_instance_id,source_sequence,plan_sha256})
```

`source_sequence` is assigned by the sidecar after the pointer commit, so the same
`source_instance_id`/sequence is unique and stable across journal import. The replication
generation and history hashes below use separate domains; UUIDs never supply ordering.

`destination_id` is the first 32 lower-case hex characters of the trusted descriptor hash. It is
not a path encoding and is the only destination identity exposed in private audit; public output
exposes no destination identifier unless it is an opaque 32-hex digest.

### Model tables

| Entity | Fields | Constraints and purpose |
|---|---|---|
| `SourceCheckpoint` | source_instance_id, source_sequence, pointer DB inode/schema digest, singleton run/date/published_at, generation, manifest/object-set hashes, complete inventory | Created by the replication sidecar after the existing committed pointer; never changes canonical rows/files. |
| `ReplicationIntent` | intent_id, operation_day, direction, destination_id, source checkpoint fields, plan/intent hashes, object/row/byte counts | Frozen after enqueue; one unique source instance/sequence per destination; no payload rows. |
| `ReplicationAttemptEvent` | intent_id, event sequence, prev/event hashes, transition fields, attempt, state version, reason, timestamp | Append-only immutable evidence; contiguous replay from genesis; max six claims. |
| `ReplicationHead` | intent_id, state, state_version, lease owner/until, next attempt, last event sequence/reason | Mutable derived projection updated only by CAS in the same transaction as its event. |
| `DestinationTrust` | descriptor schema/role, destination_id prefix, sentinel hash, mount identity, single writer host, created_at | Explicit archive role; descriptor-bound no-follow reads; no public path/URL/credential. |
| `DestinationHistory` | replication_generation, source instance/sequence, parent replication hash, descriptor/plan digests, pointer/checkpoint and manifest/object hashes and counts | Immutable destination history and complete manifest/object set; never overwritten/deleted. |
| `DestinationHead` | destination_id, current replication_generation, head hash/version, last persisted health state/time, updated_at | Mutable pointer and persisted writer-probe projection; advanced under the destination lock and CAS; status reads it without probing. |
| `RestoreReport` | report hash, source lineage, object/row/byte counts, verification result, reason, started/completed UTC | Written only under hidden/new temporary root or local private audit; never canonical control authority. |
| `ReplicationStatus` | state/reason, local_ready, destination health from last persisted probe, counts, queue lag | Public projection omits paths, credentials, SQL, payload and raw exceptions; status does no probe. |

### State and reason contracts

The only forward state transitions are:

```text
pending -> copying -> verifying -> replicated
pending/copying/verifying -> retry_wait -> pending
pending/copying/verifying -> dead_letter
```

`replicated` and `dead_letter` are terminal for the immutable intent. A manual requeue, if later
approved, creates a new attempt record linked to the same intent through a reviewed operation and
does not delete or rewrite the terminal audit.

| Condition | Internal reason | Public/status projection |
|---|---|---|
| Feature disabled | `REPLICATION_DISABLED` | `DISABLED` |
| No local dataset | `LOCAL_SOURCE_NOT_CONFIGURED` | `SOURCE_NOT_CONFIGURED` |
| Strict source proof missing/corrupt | `LOCAL_SOURCE_UNAVAILABLE` | `SOURCE_UNAVAILABLE` |
| Committed pointer row/DB/manifest/object-set binding mismatch | `LOCAL_POINTER_MISMATCH` | `LOCAL_POINTER_MISMATCH` |
| Outbox read/schema/lock unavailable | `OUTBOX_UNAVAILABLE` | `REPLICATION_STATE_UNAVAILABLE` |
| Enqueue transaction failed | `OUTBOX_ENQUEUE_FAILED` | `OUTBOX_ENQUEUE_FAILED` |
| Journal fallback committed | `OUTBOX_JOURNALED` | `OUTBOX_JOURNALED` |
| Both durability paths failed | `OUTBOX_DURABILITY_UNAVAILABLE` | `OUTBOX_DURABILITY_UNAVAILABLE` |
| Mount/sentinel/descriptor invalid | `DESTINATION_TRUST_FAILED` | `DESTINATION_TRUST_FAILED` |
| Descriptor-bound mount/inode changed after initialization | `DESTINATION_REBOUND` | `DESTINATION_REBOUND` |
| Destination absent/unreachable | `DESTINATION_UNAVAILABLE` | `DESTINATION_UNAVAILABLE` |
| Destination source sequence is ahead | `DESTINATION_AHEAD` | `DESTINATION_AHEAD` |
| Same logical partition has another hash | `DESTINATION_CONFLICT` | `DESTINATION_CONFLICT` |
| Descriptor lock/mount capability unavailable | `MOUNT_UNSUPPORTED` | `MOUNT_UNSUPPORTED` |
| Final manifest baseline changed | `DESTINATION_CAS_CONFLICT` | `CAS_CONFLICT` |
| Copy/readback integrity failure | `COPY_INTEGRITY_FAILED`/`READBACK_VERIFY_FAILED` | `COPY_FAILED`/`VERIFY_FAILED` |
| Due time has not arrived | `RETRY_WAIT` | `RETRY_WAIT` |
| Max claims or terminal trust failure | `DEAD_LETTER` | `DEAD_LETTER` |
| Exact lineage already present | `ALREADY_REPLICATED` | `ALREADY_REPLICATED` |
| Path/symlink/direction violation | `PATH_INVALID`/`SYMLINK_UNSAFE`/`DIRECTION_NOT_ALLOWED` | matching allowlisted reason |

### Transaction boundary

The implementation MUST preserve this sequence:

```text
strict provider/candidate gates
  -> canonical immutable object + local manifest/pointer transaction commits
  -> strict local pointer observation creates replication-only source checkpoint
  -> canonical ready result becomes durable
  -> local outbox enqueue transaction (or atomic journal fallback)
  -> optional bounded destination drain
  -> existing post-publish/shadow/universe work
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
| After canonical commit, before enqueue | ready | no row; journal or reconciliation gap | next execution derives current intent; no rollback |
| During outbox SQLite commit | ready | prior committed row or no row | journal/reconciliation imports once |
| Lock acquisition/probe fails | ready | no staging/history/head write | return `MOUNT_UNSUPPORTED`; no unproven retry |
| After `pending` claim | ready | no published destination change | lease expiry returns intent to claimable state |
| During object staging | ready | only non-visible partials | retry cleans/quarantines staging; manifest unchanged |
| After object staging, before immutable history commit | ready | only hidden partials; current head unchanged | retry reuses only verified hidden objects or isolates them |
| After history commit, before destination head CAS | ready | immutable history exists but is not visible from head | replay/CAS either advances the head or leaves it unchanged |
| During destination head CAS/fsync | ready | prior or new complete history/head, never partial | strict head reader accepts one committed state; retry is idempotent |
| During restore directory rename | ready | no destination or one complete standard sentinel/manifest directory | strict `NasMarketStore` reader sees only the complete final directory |

### Destination and restore safety

The destination archive is a separate trust domain. Replication may write only a trusted
`nas_archive` root explicitly selected by the operator. It MUST not write a source root, local
control root, user database root, staging root, temporary root, share root or any unresolved
environment path. A destination history record is publishable only when its lineage says
`direction=local_to_nas` and its source object set is complete; the separate head is the only
visible selector. Restore reads this archive and writes a new temporary root using the existing
standard sentinel/`manifest.json` reader, but never updates `MarketStore`, `NasMarketStore.control`,
`CURRENT`, `published_snapshots`, canonical Parquet or the replication outbox.

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
| FR-2 | `tests/test_dataset_replication.py::test_source_checkpoint_binds_committed_pointer_without_canonical_mutation` |
| FR-3 | `tests/test_dataset_replication.py::test_enqueue_failure_preserves_local_pointer_ready` |
| FR-4 | `tests/test_dataset_replication.py::test_replication_sidecar_normative_ddl_identity_and_immutable_event_history` |
| FR-5 | `tests/test_dataset_replication.py::test_pointer_gap_recovers_from_journal_or_current_manifest` |
| FR-6 | `tests/test_dataset_replication.py::test_source_checkpoint_rejects_missing_changed_or_corrupt_object` |
| FR-7 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| FR-8 | `tests/test_dataset_replication.py::test_copy_uses_non_visible_staging_namespace` |
| FR-9 | `tests/test_dataset_replication.py::test_destination_readback_checks_size_schema_rows_and_hash` |
| FR-10 | `tests/test_dataset_replication.py::test_destination_history_and_head_are_atomic_last_visibility` |
| FR-11 | `tests/test_dataset_replication.py::test_destination_lineage_rejects_unknown_ahead_conflicting_and_reverse` |
| FR-12 | `tests/test_dataset_replication.py::test_same_source_sequence_replication_is_idempotent` |
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
| AC-2 | `tests/test_dataset_replication.py::test_enqueue_failure_preserves_local_pointer_ready` |
| AC-3 | `tests/test_dataset_replication.py::test_pointer_gap_recovers_from_journal_or_current_manifest` |
| AC-4 | `tests/test_dataset_replication.py::test_replication_sidecar_normative_ddl_identity_and_immutable_event_history` |
| AC-5 | `tests/test_dataset_replication.py::test_source_checkpoint_rejects_missing_changed_or_corrupt_object` |
| AC-6 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| AC-7 | `tests/test_dataset_replication.py::test_copy_uses_non_visible_staging_namespace` |
| AC-8 | `tests/test_dataset_replication.py::test_interrupted_copy_keeps_previous_destination_head_and_history` |
| AC-9 | `tests/test_dataset_replication.py::test_same_source_sequence_replication_is_idempotent` |
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
| EC-1 | `tests/test_dataset_replication.py::test_replication_disabled_is_zero_work` |
| EC-2 | `tests/test_dataset_replication.py::test_path_validation_rejects_root_home_env_symlink_and_overlap` |
| EC-3 | `tests/test_dataset_replication.py::test_path_validation_rejects_root_home_env_symlink_and_overlap` |
| EC-4 | `tests/test_dataset_replication.py::test_source_and_destination_descriptor_races_fail_closed` |
| EC-5 | `tests/test_dataset_replication.py::test_destination_mount_and_tcc_failure_is_sanitized` |
| EC-6 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| EC-7 | `tests/test_dataset_replication.py::test_manifest_extra_duplicate_and_unsafe_entries_fail_closed` |
| EC-8 | `tests/test_dataset_replication.py::test_source_snapshot_rejects_missing_changed_or_corrupt_object` |
| EC-9 | `tests/test_dataset_replication.py::test_orphan_and_partial_objects_are_not_reader_visible` |
| EC-10 | `tests/test_dataset_replication.py::test_older_or_conflicting_destination_never_overwritten` |
| EC-11 | `tests/test_dataset_replication.py::test_destination_head_cas_race_preserves_current_head` |
| EC-12 | `tests/test_dataset_replication.py::test_interrupted_copy_keeps_previous_destination_head_and_history` |
| EC-13 | `tests/test_dataset_replication.py::test_destination_readback_checks_size_schema_rows_and_hash` |
| EC-14 | `tests/test_dataset_replication.py::test_status_missing_corrupt_or_locked_outbox_is_zero_write` |
| EC-15 | `tests/test_dataset_replication.py::test_enqueue_failure_preserves_local_pointer_ready` |
| EC-16 | `tests/test_dataset_replication.py::test_double_durability_failure_is_observable_and_reconciles` |
| EC-17 | `tests/test_dataset_replication.py::test_journal_import_is_idempotent_before_removal` |
| EC-18 | `tests/test_dataset_replication.py::test_concurrent_claim_uses_lease_and_state_version_cas` |
| EC-19 | `tests/test_dataset_replication.py::test_retry_schedule_and_dead_letter_are_bounded` |
| EC-20 | `tests/test_dataset_replication.py::test_restore_source_change_leaves_no_final_root` |
| EC-21 | `tests/test_dataset_replication.py::test_restore_rejects_existing_or_unsafe_destination` |
| EC-22 | `tests/test_dataset_replication.py::test_restore_rename_failure_leaves_destination_absent_or_quarantined` |
| EC-23 | `tests/test_nas_dataset.py::test_verified_local_mirror_is_idempotent_and_manifest_readable` |
| EC-24 | `tests/test_dataset_replication.py::test_reverse_replication_and_mount_attempts_are_zero_write` |

## R2-F4.3.1 normative amendment — release-candidate closure

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

The source checkpoint is replication-owned and is created only after the canonical pointer commit.
It copies the exact original values from the strict pointer row: `run_id`, `trade_date`,
`published_at`, plus the observed `pointer_db_inode`, `pointer_db_schema_digest`,
`manifest_canonical_sha256` and `object_set_sha256`. It also records `source_instance_id` derived
from the bound local pointer/database identity. No new column, table, trigger or DDL migration is
made to the canonical control schema. The reader requires:

```text
pointer row is the committed singleton for the requested run/date
pointer_db_inode/schema == the descriptor-bound control snapshot
pointer_generation == the observed manifest.generation
manifest_canonical_sha256 == domain_sha256("stock-eva/r2f4.3/manifest-canonical/v1", manifest_projection)
object_set_sha256 == domain(sorted complete manifest object inventory)
```

It also requires the sentinel role/schema, manifest role, object paths, object hashes, sizes,
schemas and row counts to be valid. A mismatch is `LOCAL_POINTER_MISMATCH` and invalidates the
whole candidate. The resulting immutable `SourceCheckpoint` contains the bound pointer row values,
database inode/schema digest, `source_instance_id`, a sidecar-assigned monotonic `source_sequence`,
manifest canonical hash, object-set hash and complete inventory; it contains no payload rows. The
sidecar assigns `source_sequence` strictly monotonically and uniquely within `source_instance_id`
when a new committed pointer is observed, in the same transaction that persists its intent/head;
an exact previously observed checkpoint reuses its sequence rather than allocating another one.
It is not written to or inferred by canonical storage. If the sidecar is unavailable, the journal
stores the checkpoint preimage without a sequence; only the next sidecar import may allocate the
sequence and derive the intent, otherwise the result is `OUTBOX_DURABILITY_UNAVAILABLE`. The intent
preimage includes this checkpoint, so the transfer can copy only the generation visible
through the committed pointer at snapshot time. Historical files, an unreferenced object, or a
later pointer cannot be selected.

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
`test_main_wires_explicit_local_pointer_reader_without_nas_fallback`.

### H2 — strict sidecar identity, immutable evidence and descriptor-bound status

The sidecar is a strict SQLite database, not one mutable audit row. Its first transaction must
insert exactly one `replication_sidecar_meta` row whose
`schema_identity='stock-eva/r2f4.3/replication-sidecar/v1'`, `schema_version=1`,
`ddl_sha256` is the SHA-256 of the normalized DDL below, and `schema_digest` is
`domain_sha256('stock-eva/r2f4.3/replication-schema/v1',
{schema_identity,schema_version,ddl_sha256})`. Any missing, extra, mismatched or duplicate meta
row makes the sidecar unavailable; readers never repair it.

The sidecar has four intentionally different classes of state:

* `replication_intents` is the immutable intent/outbox. Its source pointer identity, object set,
  destination identity, plan hash and intent hash are frozen forever.
* `replication_attempt_events` is an append-only immutable transition/attempt log. Each intent has
  contiguous `event_sequence` beginning at zero; every event carries `prev_event_sha256` and an
  `event_sha256` over its exact fields. No event may be updated or deleted.
* `replication_destination_history` is immutable destination lineage plus the complete manifest
  and object-set evidence; `replication_destination_heads` is its mutable current-head and last
  persisted health projection. History is never updated or deleted.
* `replication_heads` is the only mutable intent lease/state projection. A writer must update it
  with `WHERE intent_id=? AND state_version=?`, incrementing `state_version` exactly once, and
  append the matching event in the same SQLite transaction. A head without a reachable event, an
  orphan event, a gap, a broken hash chain, an invalid destination history/head link, or a head
  whose last sequence is not the replayed state is unavailable, not an inferred status.

The strict destination reader additionally requires the head's
`(destination_id,replication_generation)` to resolve to exactly one matching history row, and
requires the history's descriptor, parent, source-sequence and checkpoint hashes to match the
descriptor-bound archive files. A mismatched cross-destination link, invalid health state/time
pair, head that does not name a complete history generation, or head whose `head_sha256` does not
recompute from its closed fields is unavailable; it is never repaired by status.

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
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
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
    pointer_db_inode INTEGER NOT NULL CHECK (pointer_db_inode > 0),
    pointer_db_schema_digest TEXT NOT NULL CHECK (length(pointer_db_schema_digest) = 64),
    manifest_canonical_sha256 TEXT NOT NULL CHECK (length(manifest_canonical_sha256) = 64),
    object_set_sha256 TEXT NOT NULL CHECK (length(object_set_sha256) = 64),
    source_instance_id TEXT NOT NULL CHECK (length(source_instance_id) = 64),
    source_sequence INTEGER NOT NULL CHECK (source_sequence >= 1),
    source_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(source_manifest_bytes_sha256) = 64),
    source_snapshot_sha256 TEXT NOT NULL CHECK (length(source_snapshot_sha256) = 64),
    plan_sha256 TEXT NOT NULL CHECK (length(plan_sha256) = 64),
    intent_sha256 TEXT NOT NULL CHECK (length(intent_sha256) = 64),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    created_at TEXT NOT NULL,
    UNIQUE (direction, destination_id, source_instance_id, source_sequence),
    UNIQUE (source_instance_id, source_sequence),
    UNIQUE (source_instance_id, pointer_row_sha256, pointer_generation,
            manifest_canonical_sha256, object_set_sha256)
) STRICT;

CREATE TABLE replication_destination_history (
    replication_generation TEXT PRIMARY KEY NOT NULL CHECK (length(replication_generation) = 64),
    destination_id TEXT NOT NULL CHECK (length(destination_id) = 32),
    source_instance_id TEXT NOT NULL CHECK (length(source_instance_id) = 64),
    source_sequence INTEGER NOT NULL CHECK (source_sequence >= 1),
    parent_replication_hash TEXT NOT NULL CHECK (length(parent_replication_hash) = 64),
    descriptor_sha256 TEXT NOT NULL CHECK (length(descriptor_sha256) = 64),
    plan_sha256 TEXT NOT NULL CHECK (length(plan_sha256) = 64),
    pointer_row_sha256 TEXT NOT NULL CHECK (length(pointer_row_sha256) = 64),
    pointer_generation TEXT NOT NULL CHECK (length(pointer_generation) BETWEEN 1 AND 128),
    source_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(source_manifest_bytes_sha256) = 64),
    source_snapshot_sha256 TEXT NOT NULL CHECK (length(source_snapshot_sha256) = 64),
    source_manifest_canonical_sha256 TEXT NOT NULL CHECK (length(source_manifest_canonical_sha256) = 64),
    source_object_set_sha256 TEXT NOT NULL CHECK (length(source_object_set_sha256) = 64),
    destination_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(destination_manifest_bytes_sha256) = 64),
    destination_object_set_sha256 TEXT NOT NULL CHECK (length(destination_object_set_sha256) = 64),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    history_sha256 TEXT NOT NULL CHECK (length(history_sha256) = 64),
    created_at TEXT NOT NULL,
    UNIQUE (destination_id, replication_generation),
    UNIQUE (destination_id, source_instance_id, source_sequence)
) STRICT;

CREATE TABLE replication_destination_heads (
    destination_id TEXT PRIMARY KEY NOT NULL CHECK (length(destination_id) = 32),
    replication_generation TEXT NOT NULL,
    head_sha256 TEXT NOT NULL CHECK (length(head_sha256) = 64),
    head_version INTEGER NOT NULL CHECK (head_version >= 0),
    health_state TEXT NOT NULL CHECK (health_state IN ('unknown','healthy','unavailable','unsupported')),
    health_observed_at TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (destination_id, replication_generation)
        REFERENCES replication_destination_history(destination_id, replication_generation),
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
CREATE INDEX replication_destination_history_order_idx
    ON replication_destination_history (destination_id, source_instance_id, source_sequence);

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
CREATE TRIGGER replication_destination_history_no_update
BEFORE UPDATE ON replication_destination_history BEGIN
    SELECT RAISE(ABORT, 'destination history is immutable');
END;
CREATE TRIGGER replication_destination_history_no_delete
BEFORE DELETE ON replication_destination_history BEGIN
    SELECT RAISE(ABORT, 'destination history is immutable');
END;
CREATE TRIGGER replication_destination_heads_monotonic_cas
BEFORE UPDATE ON replication_destination_heads
WHEN NEW.head_version <> OLD.head_version + 1
BEGIN
    SELECT RAISE(ABORT, 'destination head requires state-version CAS');
END;
CREATE TRIGGER replication_destination_heads_no_delete
BEFORE DELETE ON replication_destination_heads BEGIN
    SELECT RAISE(ABORT, 'destination head is a derived audit projection');
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

Writers MUST set WAL/FULL for every connection, commit the head and event together, fsync the
database and WAL where present, then fsync the sidecar parent directory. The read-only status
reader opens the sidecar with `O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, fstats before and after the
transaction, uses a SQLite read-only URI, verifies inode/device and does a complete global
reachability/replay check. It performs no `CREATE`, `PRAGMA` migration, checkpoint, journal
cleanup, lock repair or writer fallback. Tests MUST assert sidecar bytes and inode are unchanged
for missing, corrupt, locked and healthy status; see
`test_outbox_global_reachability_and_event_replay` and
`test_status_missing_corrupt_or_locked_outbox_is_zero_write`.

The event closed field set is exactly `(event_id,intent_id,event_sequence,prev_event_sha256,
event_type,from_state,to_state,attempt,reason_code,state_version,occurred_at,event_sha256)`; the
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
unchanged. After a pointer commit, the strict source reader creates a replication-only checkpoint
containing the original singleton row values, pointer DB inode/schema digest, manifest canonical
hash and object-set hash. The sidecar assigns a monotonic `source_sequence`, unique only within
the observed `source_instance_id`; this sequence is not inserted into canonical storage. A random
canonical generation UUID remains an identifier only and MUST NOT be used for ordering.

The destination stores an immutable replication history record for each transfer. Each record has a
fresh `replication_generation` identifier, the complete destination manifest/object inventory,
`parent_replication_hash`, `source_instance_id`, `source_sequence`, source checkpoint hashes and
descriptor/plan digests. A separate current head points to one history record and stores the last
persisted writer-probe health state/time; history records are never overwritten or deleted, and the
full parent chain can be reconstructed from the history. Health updates use the same
descriptor-bound lock and head CAS, while a read-only status request only reads this persisted
projection.
The private destination layout is fixed: `_replication/history/<replication_generation>/` contains
the standard `.stock-eva-dataset.json`, `manifest.json` and immutable Parquet objects;
`_replication/head.json` contains only the current history generation and its head hash. A strict
destination reader follows `head.json` by descriptor-bound fd, then applies the existing
`NasMarketStore` sentinel/manifest validation to that history directory. Staging and quarantine
names are outside both the history and head namespaces.

Lineage is a partial order, not a timestamp guess:

| Comparison | Required result |
|---|---|
| Same source instance/sequence and same manifest/object-set/checkpoint hashes | `ALREADY_REPLICATED`, no history/head write |
| Same source instance and source sequence strictly greater than destination, with a valid parent chain | copy may proceed after head CAS |
| Destination source sequence is greater and source is an ancestor | `DESTINATION_AHEAD`, never overwrite |
| Same sequence with any differing hash, broken parent, or unknown chain | `DESTINATION_CONFLICT` |
| Different source instances or incomparable chains | `DESTINATION_CONFLICT` |
| Final head baseline changed | `CAS_CONFLICT`, preserve the current head |

The `parent_replication_hash` chain is verified to genesis (`64` zero hex characters) or a stored
trusted checkpoint; missing, cyclic or ambiguous ancestry is unavailable/conflict, never “latest
wins”. A UUID lexical comparison, canonical `published_at` tie-break, filesystem mtime or retry
order is not an ordering rule. Required test:
`test_lineage_source_sequence_replication_generation_parent_hash_partial_order` and
`test_destination_ahead_divergent_tie_and_cas_are_fail_closed`.

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

### H6 — hidden restore staging and post-rename rule

Restore requires that the final destination path does not exist. It writes into hidden staging in
the same parent directory, named by an opaque restore id and excluded by the existing
`NasMarketStore` sentinel/manifest reader. Before the only visibility point it MUST validate the
source descriptor and fingerprints, exact standard sentinel/manifest schema and role, complete
object set (including no extra/duplicate entries), every object size/schema/row/hash/date, counts,
and representative read-only API queries. It fsyncs every staged file, the staging directory and
parent, then atomically renames the hidden staging directory to the previously nonexistent
destination on the same filesystem. That directory rename is the only visibility point; the
existing `NasMarketStore` reader recognizes only the standard final sentinel and `manifest.json`.

After rename, only bounded non-semantic readback of the final directory/inode and manifest bytes is
allowed; there is no second schema/count/query gate that can retroactively fail the task. If the
rename fails, staging is removed or isolated in quarantine; if a post-rename readback reports an
unexpected inode/bytes result, the destination is isolated and the strict reader rejects it. There
is no typed restore pointer and no modification to canonical control state. Required tests are
`test_restore_semantics_complete_before_atomic_rename`,
`test_restore_rename_failure_leaves_destination_absent_or_quarantined`, and
`test_restore_post_rename_readback_is_nonsemantic`.

The crash rows are normative: crash during hidden staging leaves no final destination (or isolated
staging); crash immediately before rename leaves no visible destination; crash during the atomic
rename leaves either the old nonexistent state or one complete directory; crash after rename leaves
the complete standard sentinel/manifest visible; crash during post-rename readback does not rerun
semantic gates. Canonical local manifest, pointer and control DB bytes/inodes are unchanged in
every row.

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

The hash contract is closed and exact. Every set/list is sorted by the stated tuple and no object
contains its own digest field in the preimage:

```text
pointer_row_sha256 = domain_sha256("stock-eva/r2f4.3/pointer-row/v1",
  {singleton,run_id,trade_date,published_at})
object_set_sha256 = domain_sha256("stock-eva/r2f4.3/object-set/v1",
  sort(object_inventory, key=(relative_path,object_sha256)))
descriptor_sha256 = domain_sha256("stock-eva/r2f4.3/destination-descriptor/v1",
  {descriptor_schema,schema_version,dataset,role,direction,root_dev,root_ino,parent_dev,
   parent_ino,mount_point,fs_type,mount_generation,mount_fingerprint,sentinel_sha256,
   single_writer_host_id,created_at})
replication_generation = domain_sha256("stock-eva/r2f4.3/replication-generation/v1",
  {destination_id,source_instance_id,source_sequence,source_object_set_sha256,
   parent_replication_hash,plan_sha256})
history_sha256 = domain_sha256("stock-eva/r2f4.3/replication-history/v1",
  {replication_generation,destination_id,source_instance_id,source_sequence,
   parent_replication_hash,descriptor_sha256,plan_sha256,
   pointer_row_sha256,pointer_generation,source_manifest_bytes_sha256,source_snapshot_sha256,
   source_manifest_canonical_sha256,source_object_set_sha256,
   destination_manifest_bytes_sha256,destination_object_set_sha256,object_count,row_count,byte_count})
head_sha256 = domain_sha256("stock-eva/r2f4.3/replication-head/v1",
  {destination_id,replication_generation,head_version,health_state,health_observed_at})
event_id = domain_sha256("stock-eva/r2f4.3/replication-event-id/v1",
  {intent_id,event_sequence,attempt,state_version,occurred_at})
event_sha256 = domain_sha256("stock-eva/r2f4.3/replication-event/v1",
  {event_id,intent_id,event_sequence,prev_event_sha256,event_type,from_state,to_state,
   attempt,reason_code,state_version,occurred_at})
journal_sha256 = domain_sha256("stock-eva/r2f4.3/replication-journal/v1",
  {journal_schema_version,intent_id,intent_sha256,source_instance_id,source_sequence,created_at})
intent_sha256 = domain_sha256("stock-eva/r2f4.3/replication-intent-row/v1",
  {intent_id,schema_version,operation_day,direction,destination_id,pointer_row_sha256,
   pointer_generation,source_run_id,source_trade_date,source_published_at,pointer_db_inode,
   pointer_db_schema_digest,manifest_canonical_sha256,object_set_sha256,source_instance_id,
   source_sequence,source_manifest_bytes_sha256,source_snapshot_sha256,
   plan_sha256,object_count,row_count,byte_count,created_at})
```

`prev_event_sha256` and `parent_replication_hash` at genesis are exactly 64 zero hex characters;
all other digests are 64 lower-case hex. A single sidecar transaction first reuses the sequence for
an exact `(source_instance_id,pointer_row_sha256,pointer_generation,manifest_canonical_sha256,
object_set_sha256)` checkpoint; only a genuinely new committed pointer allocates the greatest
existing sequence for that `source_instance_id` plus one. The unique constraints make allocation
and journal import restart-safe. The normalized JSON encoder is the one defined above, including
explicit null/false/zero/empty values and one final newline. Golden tests must mutate one field, one
sort order and the newline and prove a different digest.

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

The journal filename is `<intent_id>.json` under the configured local journal directory. Create
with `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW` mode `0600`, write the complete newline-terminated
canonical bytes, `fsync(fd)`, atomically rename a same-directory temporary file, and fsync the
journal directory and its parent. Import opens no-follow and fstats before/after; it validates the
intent hash, inserts intent/event/head in one WAL/FULL transaction, fsyncs the DB and parent, and
only then unlinks the journal and fsyncs its parent. Unlink failure leaves a harmless journal for
idempotent re-import. Crash tests cover each boundary and require no provider request.

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
| NFR-7 | `tests/test_dataset_replication.py::test_hash_preimages_are_golden_and_newline_terminated` |
| NFR-8 | `tests/test_dataset_replication.py::test_status_cli_api_have_no_path_or_secret` |
| NFR-9 | `tests/test_dataset_replication.py::test_descriptor_bound_reads_reject_symlink_and_toctou` |
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
