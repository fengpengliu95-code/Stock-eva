# Stock EVA R2-F4.3 Local Replication and Verified Restore

**Author:** Codex

**Date:** 2026-09-09 (Asia/Shanghai)

**Status:** In Review / implementation blocked until this specification is approved

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
- FR-10: Destination publication MUST publish all verified immutable objects first and replace the destination manifest/pointer atomically last; no partial object or partially assembled generation may be visible through a strict reader.
- FR-11: Destination lineage MUST bind direction, source dataset role, source manifest identity/bytes hash, source generation, plan hash, destination trust identity and object inventory; an unknown, older, conflicting or reverse lineage MUST fail closed.
- FR-12: Replicating the same source manifest to the same trusted destination MUST be idempotent and return `ALREADY_REPLICATED` or `REUSED` without copying or changing the destination pointer.
- FR-13: The service MUST implement only `local_to_nas`; restore is a separately named `nas_to_temporary_root` operation and MUST NOT enqueue, overwrite local canonical data or reverse-replicate a destination into the source.
- FR-14: Transfer attempts MUST use bounded exponential retry and a durable `dead_letter` state after the configured maximum; retries MUST never increase market-provider requests or bypass a failed integrity gate.
- FR-15: Concurrent drains MUST use a local nonblocking lock plus outbox lease/state-version CAS; only the lease owner may advance an intent, and lease expiry MUST make a stale worker harmless.
- FR-16: Restore MUST validate a trusted destination manifest and copy only immutable referenced objects into a new explicitly supplied temporary root; the restore root MUST be atomically finalized only after complete readback verification.
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
- NFR-3: A transfer MUST be single-generation atomic from the destination reader's perspective: the published manifest changes at most once and only after all objects pass readback.
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

Given replication is disabled and a valid local canonical publication exists, When automation and status are run, Then local readiness and pointer identity remain ready, the provider request count is zero, the outbox/destination/restore hooks are not called, and no local tree or database bytes change.

### AC-2: Pointer-before-enqueue boundary (FR-2, FR-3, FR-5, FR-25, NFR-6, NFR-10)

Given a canonical callback commits a ready manifest/pointer and the outbox insert raises a synthetic storage error, When the refresh completes, Then the ready result and pointer are unchanged, the error is projected as `OUTBOX_ENQUEUE_FAILED`, no provider is called again, and the recovery reconciliation reports the missing intent without rolling back canonical state.

### AC-3: Crash recovery journal (FR-4, FR-5, FR-26, NFR-6, NFR-7)

Given a process stops after the pointer commit and before the SQLite outbox commit, When the next explicit drain reconciles the current strict manifest and intent journal, Then exactly one deterministic intent is persisted and claimed, duplicate imports are no-ops, and the prior canonical pointer remains byte-identical.

### AC-4: Strict outbox contract (FR-4, FR-14, FR-15, NFR-6)

Given a fresh empty local control root, When the execution service initializes the outbox and claims an intent twice concurrently, Then the normalized DDL is accepted, one claim wins by state-version CAS, the losing worker performs no copy, and terminal rows cannot be deleted or have immutable identity fields changed.

### AC-5: Complete source snapshot (FR-6, FR-9, FR-11, FR-26, NFR-2, NFR-9)

Given a manifest references an object with a missing file, changed inode, wrong size, wrong schema, wrong row count or wrong hash, When replication plans the intent, Then the complete source candidate is invalid, no destination object or pointer is written, and the sanitized reason identifies the failing validation stage.

### AC-6: Destination trust and path safety (FR-7, FR-21, FR-23, FR-24, NFR-8, NFR-9)

Given a destination is relative, contains unresolved environment syntax, is `/`, overlaps local mutable storage, has a symlink ancestor, has an unexpected mount, or lacks a trusted destination descriptor, When `market-replicate` is run without `--execute`, Then it returns a bounded plan/error without creating a directory, reading credentials, initializing a sidecar or exposing the supplied path.

### AC-7: Verified staged transfer (FR-8, FR-9, FR-10, FR-11, NFR-2, NFR-3, NFR-9)

Given a trusted empty destination and a complete local source snapshot, When one intent executes against an offline fixture, Then each immutable object is copied to staging, read back for size/schema/rows/hash, renamed into the destination object namespace, and only then is the lineage-bearing manifest atomically published.

### AC-8: Failure leaves destination pointer unchanged (FR-8, FR-9, FR-10, FR-12, NFR-3, NFR-12)

Given an object copy, readback, manifest write or final rename is interrupted, When the operation terminates, Then the prior destination manifest remains readable and byte-identical, no partial namespace is referenced by it, and a strict reader cannot see an incomplete generation.

### AC-9: Idempotence and no old overwrite (FR-11, FR-12, FR-13, FR-27, NFR-3, NFR-14)

Given a destination already contains the exact source lineage, an older source lineage, a conflicting object hash or a reverse-direction marker, When replication is attempted, Then exact lineage is `ALREADY_REPLICATED`, older/conflicting/reverse lineage is rejected with an allowlisted reason, and neither destination nor local canonical pointer changes.

### AC-10: Retry and dead-letter (FR-14, FR-25, FR-26, NFR-4, NFR-5)

Given a destination is temporarily unavailable or a bounded copy fails, When the drain is invoked repeatedly, Then the intent follows `pending → copying/verifying → retry_wait` with the exact bounded delay schedule, reaches `dead_letter` after six failed claims, and never issues a market-provider request.

### AC-11: Concurrency and stale lease (FR-4, FR-15, FR-26, NFR-6)

Given two workers share one outbox and one destination and one worker crashes while leased, When the second worker observes lease expiry, Then it can claim the same deterministic intent exactly once, a stale worker cannot advance state or publish a pointer, and state-version conflicts are sanitized.

### AC-12: Restore dry-run (FR-13, FR-16, FR-17, FR-20, FR-23, FR-24, NFR-1, NFR-12)

Given a trusted source archive and an explicit absolute temporary destination, When `market-restore` runs without `--execute`, Then it validates the plan only, reports object/count/hash projections without paths, performs zero writes/provider calls, and does not create the temporary root or touch canonical state.

### AC-13: Verified temporary restore (FR-16, FR-17, FR-20, FR-24, NFR-2, NFR-3, NFR-9, NFR-12)

Given a complete trusted archive, When `market-restore --execute` writes a new temporary root, Then the final root contains a strict sentinel/manifest and verified immutable objects, representative read-only queries succeed, and canonical manifest/pointer/control DB bytes and inodes remain unchanged.

### AC-14: Restore failure is invisible (FR-16, FR-17, FR-20, FR-23, NFR-9, NFR-12)

Given a restore source has a corrupt object, schema mismatch, duplicate/extra entry, symlink race, changed source fingerprint or failed representative query, When execution runs, Then no final restore root is published, partial data is not reader-visible, canonical state is unchanged, and the result names only the failing reason code.

### AC-15: Read-only status (FR-18, FR-24, FR-25, NFR-1, NFR-8, NFR-11)

Given the outbox is missing, corrupt, locked, empty, retrying, dead-lettered or healthy, When `GET /api/v1/storage/replication` is called, Then it performs no initialization or migration, returns the exact bounded status projection and `provider_requests=0`/`writes=false`, and never returns paths, credentials or raw exceptions.

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
- EC-10: Destination manifest is newer, has a different object hash for the same logical partition, or has an unknown lineage → never overwrite; return `DESTINATION_NEWER`/`DESTINATION_CONFLICT`.
- EC-11: A destination manifest changes between baseline read and final replace → CAS fails, current destination pointer is preserved, and the intent enters bounded retry or terminal conflict.
- EC-12: Copy fails after any number of objects or process exits before manifest replace → no partial generation is referenced; retry reuses only hash-verified immutable objects.
- EC-13: Readback size/schema/rows/hash or lineage verification fails → mark the attempt failed, never publish the manifest, and retain only sanitized diagnostics.
- EC-14: Outbox database is missing, corrupt, schema-mismatched or locked on status → return `REPLICATION_STATE_UNAVAILABLE`; status must not initialize, migrate or write it.
- EC-15: Outbox enqueue fails after canonical pointer commit → preserve local ready, atomically write the deterministic intent journal when possible, and expose `OUTBOX_ENQUEUE_FAILED` or `OUTBOX_JOURNALED`.
- EC-16: Both outbox and journal writes fail after pointer commit → preserve local ready, expose `OUTBOX_DURABILITY_UNAVAILABLE`, and let the next strict reconciliation derive the current intent without fetching data.
- EC-17: Process crashes after journal write before outbox import → import is idempotent; journal removal is attempted only after the outbox row is committed.
- EC-18: Two workers claim the same intent, or a worker lease expires → one state-version CAS wins; stale workers cannot copy, promote or mark success.
- EC-19: Retry attempt reaches the sixth failure or a non-retryable integrity/trust failure occurs → durable `dead_letter`/terminal reason; no automatic unbounded retry.
- EC-20: Restore source is invalid or changes during copy → no final temporary root, no canonical DB/pointer mutation, and bounded sanitized result.
- EC-21: Restore destination already exists, is non-empty, overlaps canonical roots, is symlinked or is not an explicitly temporary child → reject before writes.
- EC-22: Restore readback or representative query fails after temporary rename → quarantine the temporary root, preserve canonical state, and do not call replication enqueue.
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
  | "SOURCE_UNAVAILABLE" | "REPLICATION_STATE_UNAVAILABLE"
  | "OUTBOX_ENQUEUE_FAILED" | "OUTBOX_JOURNALED"
  | "OUTBOX_DURABILITY_UNAVAILABLE" | "DESTINATION_UNAVAILABLE"
  | "DESTINATION_MOUNT_UNAVAILABLE" | "DESTINATION_TRUST_FAILED"
  | "DESTINATION_NEWER" | "DESTINATION_CONFLICT" | "CAS_CONFLICT"
  | "COPY_FAILED" | "VERIFY_FAILED" | "RETRY_WAIT" | "DEAD_LETTER"
  | "ALREADY_REPLICATED" | "PATH_INVALID" | "PATH_CHANGED"
  | "SYMLINK_UNSAFE" | "DIRECTION_NOT_ALLOWED";

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
  lag_seconds: number | null;
  provider_requests: 0;
  writes: false;
  paths_exposed: false;
}

GET /api/v1/storage/replication -> ReplicationStatusResponse
  200: status is disabled, ready or degraded
  503: status is unavailable; body is the same bounded shape

interface ReplicatePlanResponse {
  status: "dry_run" | "ready" | "degraded" | "unavailable";
  reason_code: ReplicationReason;
  source_manifest_sha256: string | null;
  source_manifest_identity: string | null;
  source_generation: string | null;
  object_count: number;
  row_count: number;
  byte_count: number;
  planned_intent: boolean;
  provider_requests: 0;
  writes: false;
  paths_exposed: false;
}

interface RestorePlanResponse {
  status: "dry_run" | "ready" | "unavailable";
  reason_code: ReplicationReason;
  source_manifest_identity: string | null;
  object_count: number;
  row_count: number;
  byte_count: number;
  finalization_allowed: boolean;
  provider_requests: 0;
  writes: false;
  paths_exposed: false;
}
```

The CLI exit codes are fixed: `0` for a safe plan or successful execution, `1` for unavailable,
retry/dead-letter or integrity failure, and `2` for lexical/configuration/path/direction error.
`market-replicate` and `market-restore` output the bounded interfaces above; neither command
prints its input path. A separate `market-replication-status` alias MAY call the same read-only
projection but MUST preserve the response fields and zero-write guarantees.

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
`source_manifest_identity` hashes the canonical JSON projection of the validated manifest. The
two values are both retained so formatting changes cannot be mistaken for object changes.

`source_snapshot_sha256` is
`domain_sha256("stock-eva/r2f4.3/source-snapshot/v1", {source_manifest_identity,
source_manifest_bytes_sha256, source_generation, object_inventory})`. `object_inventory` is the
sorted list of `{relative_path, object_sha256, size_bytes, row_count, trade_date, source}` for
every manifest file. `plan_sha256` is
`domain_sha256("stock-eva/r2f4.3/replication-plan/v1", {direction, destination_id,
source_snapshot_sha256, object_inventory})`. `intent_id` is
`domain_sha256("stock-eva/r2f4.3/replication-intent/v1", {operation_day,
source_manifest_identity, destination_id, direction, plan_sha256})`.

`destination_id` is a digest of the trusted descriptor's contract version, role, dataset/schema,
sentinel digest and approved mount identity. It is not a path encoding and is the only destination
identity exposed in private audit; public output exposes no destination identifier unless it is an
opaque 64-hex digest.

### Outbox DDL (normative v1)

The implementation MUST use this SQLite DDL after whitespace normalization. No reader or status
path may execute it. Timestamps are UTC ISO-8601 with `Z`; dates are `YYYY-MM-DD`.

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA user_version = 1;

CREATE TABLE replication_outbox (
    intent_id TEXT PRIMARY KEY NOT NULL CHECK (length(intent_id) = 64),
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    operation_day TEXT NOT NULL CHECK (operation_day GLOB '????-??-??'),
    direction TEXT NOT NULL CHECK (direction = 'local_to_nas'),
    destination_id TEXT NOT NULL CHECK (length(destination_id) = 64),
    source_generation TEXT NOT NULL CHECK (length(source_generation) BETWEEN 1 AND 128),
    source_manifest_identity TEXT NOT NULL CHECK (length(source_manifest_identity) = 64),
    source_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(source_manifest_bytes_sha256) = 64),
    source_snapshot_sha256 TEXT NOT NULL CHECK (length(source_snapshot_sha256) = 64),
    plan_sha256 TEXT NOT NULL CHECK (length(plan_sha256) = 64),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    state TEXT NOT NULL CHECK (state IN ('pending','copying','verifying','retry_wait','replicated','dead_letter')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0 AND attempt <= 6),
    state_version INTEGER NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    next_attempt_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_until TEXT,
    last_reason TEXT,
    last_failure_class TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    replicated_at TEXT,
    CHECK ((state IN ('copying','verifying') AND lease_owner IS NOT NULL AND lease_until IS NOT NULL)
        OR state NOT IN ('copying','verifying')),
    CHECK ((state = 'replicated' AND replicated_at IS NOT NULL)
        OR (state <> 'replicated' AND replicated_at IS NULL)),
    UNIQUE (direction, destination_id, source_manifest_identity)
) STRICT;

CREATE INDEX replication_outbox_due_idx
    ON replication_outbox (state, next_attempt_at, operation_day);

CREATE TRIGGER replication_outbox_identity_immutable
BEFORE UPDATE OF operation_day, direction, destination_id, source_generation,
    source_manifest_identity, source_manifest_bytes_sha256, source_snapshot_sha256,
    plan_sha256, object_count, row_count, byte_count ON replication_outbox
BEGIN
    SELECT RAISE(ABORT, 'replication intent identity is immutable');
END;

CREATE TRIGGER replication_outbox_no_delete
BEFORE DELETE ON replication_outbox
BEGIN
    SELECT RAISE(ABORT, 'replication audit rows are immutable');
END;
```

The journal fallback is an atomic, no-follow, newline-terminated JSON file under the local control
root. Its basename is the 64-hex `intent_id`; its content is the validated intent preimage plus
`journal_schema_version=1` and `intent_id`. A journal is imported before it is removed. If both
SQLite and journal writes fail, the strict current-manifest reconciler derives the current intent
on the next execution and emits `OUTBOX_DURABILITY_UNAVAILABLE`; it never rolls back canonical
publication.

### Model tables

| Entity | Fields | Constraints and purpose |
|---|---|---|
| `ReplicationIntent` | intent_id, operation_day, direction, destination_id, source generation/identities, plan hash, object/row/byte counts | Frozen after enqueue; one unique intent per destination and source manifest; no payload rows. |
| `ReplicationAttempt` | intent_id, attempt, state, state_version, lease owner/until, next attempt, sanitized reason, timestamps | CAS-updated operational state; max six claims; terminal states retained. |
| `DestinationTrust` | descriptor version, role, dataset/schema, destination_id, sentinel hash, mount class, created_at | Explicit archive role; no path, URL or credential; descriptor is no-follow read-only after initialization. |
| `DestinationLineage` | direction, source manifest identity/bytes hash, source generation, source snapshot/plan hashes, destination_id, object inventory hash | Stored in destination manifest metadata and compared before CAS publication. |
| `RestoreReport` | report hash, source lineage, object/row/byte counts, verification result, reason, started/completed UTC | Written only under a temporary restore root or local private audit; never used as canonical pointer authority. |
| `ReplicationStatus` | state, reason, enabled/configured flags, counts, last replicated identity/time, lag | Public projection omits paths, credentials, SQL, payload and raw exceptions. |

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
| Outbox read/schema/lock unavailable | `OUTBOX_UNAVAILABLE` | `REPLICATION_STATE_UNAVAILABLE` |
| Enqueue transaction failed | `OUTBOX_ENQUEUE_FAILED` | `OUTBOX_ENQUEUE_FAILED` |
| Journal fallback committed | `OUTBOX_JOURNALED` | `OUTBOX_JOURNALED` |
| Both durability paths failed | `OUTBOX_DURABILITY_UNAVAILABLE` | `OUTBOX_DURABILITY_UNAVAILABLE` |
| Mount/sentinel/descriptor invalid | `DESTINATION_TRUST_FAILED` | `DESTINATION_TRUST_FAILED` |
| Destination absent/unreachable | `DESTINATION_UNAVAILABLE` | `DESTINATION_UNAVAILABLE` |
| Source older than destination | `DESTINATION_NEWER` | `DESTINATION_NEWER` |
| Same logical partition has another hash | `DESTINATION_CONFLICT` | `DESTINATION_CONFLICT` |
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
  -> canonical ready result becomes durable
  -> local outbox enqueue transaction (or atomic journal fallback)
  -> optional bounded destination drain
  -> existing post-publish/shadow/universe work
```

The canonical transaction never depends on a NAS socket, destination lock, outbox SQLite commit or
restore. A local enqueue failure is an operational durability gap, not a canonical data failure.
The recovery worker compares the current local manifest identity and object inventory with durable
outbox rows and journal files. Because a later manifest contains the prior immutable objects, a
single current-manifest intent can recover a crash window without refetching or stitching rows.

### Crash matrix

| Crash/failure point | Local pointer | Outbox/destination visibility | Recovery |
|---|---|---|---|
| Before canonical commit | unchanged | no intent | refresh remains failed/partial under existing contract |
| After canonical commit, before enqueue | ready | no row; journal or reconciliation gap | next execution derives current intent; no rollback |
| During outbox SQLite commit | ready | prior committed row or no row | journal/reconciliation imports once |
| After `pending` claim | ready | no published destination change | lease expiry returns intent to claimable state |
| During object staging | ready | only non-visible partials | retry cleans/quarantines staging; manifest unchanged |
| After object rename, before manifest replace | ready | orphan immutable object, old manifest | strict reader sees old generation; retry reuses verified object |
| During manifest atomic replace | ready | old or new complete manifest | CAS/readback selects one complete identity; no partial JSON |
| After destination manifest commit, before `replicated` state | ready | new generation visible and fully verified | next run recognizes exact lineage and marks replicated idempotently |
| During restore finalization | ready | no final restore or one complete temp root | strict restore verifier keeps only verified temp root; canonical untouched |

### Destination and restore safety

The destination archive is a separate trust domain. Replication may write only a trusted
`nas_archive` root explicitly selected by the operator. It MUST not write a source root, local
control root, user database root, staging root, temporary root, share root or any unresolved
environment path. A destination manifest is publishable only when its lineage says
`direction=local_to_nas` and its source object set is complete. Restore reads this archive and
writes a new temporary root with a new local manifest, but never updates `MarketStore`,
`NasMarketStore.control`, `CURRENT`, `published_snapshots`, canonical Parquet or the outbox.

## Out of Scope

- OS-1: Real SMB/NAS copy, destination initialization, mount setup, credentials, TCC approval or production restore execution; these require a separately authorized window.
- OS-2: Any provider request, canonical refresh retry, provider failover, symbol-level mixing, normalization, quality-gate or factor change.
- OS-3: Replacing `manifest.json`, canonical Parquet, `NasMarketStore`, `MarketStore` or the existing NAS-to-local `MarketDatasetMirror` with another storage platform.
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
| FR-2 | `tests/test_dataset_replication.py::test_local_publication_intent_binds_committed_manifest` |
| FR-3 | `tests/test_dataset_replication.py::test_enqueue_failure_preserves_local_pointer_ready` |
| FR-4 | `tests/test_dataset_replication.py::test_outbox_normative_ddl_and_immutable_identity` |
| FR-5 | `tests/test_dataset_replication.py::test_pointer_gap_recovers_from_journal_or_current_manifest` |
| FR-6 | `tests/test_dataset_replication.py::test_source_snapshot_rejects_missing_changed_or_corrupt_object` |
| FR-7 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| FR-8 | `tests/test_dataset_replication.py::test_copy_uses_non_visible_staging_namespace` |
| FR-9 | `tests/test_dataset_replication.py::test_destination_readback_checks_size_schema_rows_and_hash` |
| FR-10 | `tests/test_dataset_replication.py::test_destination_manifest_is_atomic_last_pointer` |
| FR-11 | `tests/test_dataset_replication.py::test_destination_lineage_rejects_unknown_older_conflicting_and_reverse` |
| FR-12 | `tests/test_dataset_replication.py::test_same_generation_replication_is_idempotent` |
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
| AC-4 | `tests/test_dataset_replication.py::test_outbox_normative_ddl_and_immutable_identity` |
| AC-5 | `tests/test_dataset_replication.py::test_source_snapshot_rejects_missing_changed_or_corrupt_object` |
| AC-6 | `tests/test_dataset_replication.py::test_destination_trust_requires_safe_absolute_approved_root` |
| AC-7 | `tests/test_dataset_replication.py::test_copy_uses_non_visible_staging_namespace` |
| AC-8 | `tests/test_dataset_replication.py::test_interrupted_copy_keeps_previous_destination_manifest` |
| AC-9 | `tests/test_dataset_replication.py::test_same_generation_replication_is_idempotent` |
| AC-10 | `tests/test_dataset_replication.py::test_retry_schedule_and_dead_letter_are_bounded` |
| AC-11 | `tests/test_dataset_replication.py::test_concurrent_claim_uses_lease_and_state_version_cas` |
| AC-12 | `tests/test_dataset_replication.py::test_market_restore_defaults_to_zero_write_plan` |
| AC-13 | `tests/test_dataset_replication.py::test_restore_writes_only_new_temporary_root` |
| AC-14 | `tests/test_dataset_replication.py::test_restore_failure_leaves_no_final_root_or_canonical_change` |
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
| EC-11 | `tests/test_dataset_replication.py::test_destination_manifest_cas_race_preserves_current_pointer` |
| EC-12 | `tests/test_dataset_replication.py::test_interrupted_copy_keeps_previous_destination_manifest` |
| EC-13 | `tests/test_dataset_replication.py::test_destination_readback_checks_size_schema_rows_and_hash` |
| EC-14 | `tests/test_dataset_replication.py::test_status_missing_corrupt_or_locked_outbox_is_zero_write` |
| EC-15 | `tests/test_dataset_replication.py::test_enqueue_failure_preserves_local_pointer_ready` |
| EC-16 | `tests/test_dataset_replication.py::test_double_durability_failure_is_observable_and_reconciles` |
| EC-17 | `tests/test_dataset_replication.py::test_journal_import_is_idempotent_before_removal` |
| EC-18 | `tests/test_dataset_replication.py::test_concurrent_claim_uses_lease_and_state_version_cas` |
| EC-19 | `tests/test_dataset_replication.py::test_retry_schedule_and_dead_letter_are_bounded` |
| EC-20 | `tests/test_dataset_replication.py::test_restore_source_change_leaves_no_final_root` |
| EC-21 | `tests/test_dataset_replication.py::test_restore_rejects_existing_or_unsafe_destination` |
| EC-22 | `tests/test_dataset_replication.py::test_restore_failure_leaves_no_final_root_or_canonical_change` |
| EC-23 | `tests/test_nas_dataset.py::test_verified_local_mirror_is_idempotent_and_manifest_readable` |
| EC-24 | `tests/test_dataset_replication.py::test_reverse_replication_and_mount_attempts_are_zero_write` |
