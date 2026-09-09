# Stock EVA R2-F4.3 Local Replication and Verified Restore — Implementation Plan

**Author:** Codex

**Date:** 2026-09-09 (Asia/Shanghai)

**Status:** In Review / NO-GO pending the R2-F4.3.1 amendment; implementation MUST NOT start before design approval

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

### Inherited reliability constraints reviewed

Implementation must retain the predecessor boundaries: R2-F0.1 transport and quality gates remain
fail-closed; R2-F1 control readers remain zero-write; R2-F2 evidence/candidate/gate hashes remain
immutable; R2-F3 shadow remains isolated from canonical publication; R2-F4.0 admission stays
default-off; R2-F4.1 calendar authority remains PIT/conflict-safe; and R2-F4.2 Universe sidecar,
API and CLI readers remain strict, sanitized and zero-write. This plan adds only downstream
replication/restore control and never widens canonical provider authority.

## Functional Requirements

- FR-1: Add a default-off, explicitly enabled replication setting; disabled execution MUST be zero-work.
- FR-2: Build one deterministic intent from the committed local manifest/pointer, without provider access or data refetch.
- FR-3: Keep the canonical transaction before enqueue; enqueue failure MUST preserve local ready and pointer bytes.
- FR-4: Implement the normative SQLite outbox, immutable identity, state transitions, terminal retention and state-version CAS.
- FR-5: Persist or journal enqueue gaps and reconcile a pointer/outbox gap after restart without a provider request.
- FR-6: Implement strict descriptor-bound source snapshots for sentinel, manifest and every object.
- FR-7: Implement explicit destination trust, mount/sentinel checks and local-root overlap protection.
- FR-8: Copy only into a destination staging namespace that a strict reader cannot publish.
- FR-9: Read back every copied object and compare size, schema, rows, hash, path and lineage.
- FR-10: Publish the destination manifest/pointer atomically after all object verification.
- FR-11: Bind destination lineage to direction, source identity, destination trust and complete object inventory.
- FR-12: Make exact-source replication idempotent and reject older/conflicting lineage.
- FR-13: Expose local-to-NAS replication and NAS-to-temporary-root restore as separate directions; never reverse replicate.
- FR-14: Implement bounded deterministic retry and durable dead-letter behavior.
- FR-15: Implement nonblocking local locking, leases and state-version CAS for concurrent drains.
- FR-16: Implement restore into a newly created temporary root only, with atomic finalization.
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

Given replication is disabled and local data is ready, When the automation and status surfaces run, Then no replication dependency is constructed or called, local readiness remains ready, and all inspected bytes/inodes remain unchanged.

### AC-2: Pointer-before-enqueue boundary (FR-2, FR-3, FR-5, FR-25, NFR-6, NFR-10)

Given the canonical pointer has committed and an outbox insert fails, When the refresh returns, Then the ready result and pointer remain unchanged and a sanitized enqueue gap is observable.

### AC-3: Crash recovery journal (FR-4, FR-5, FR-26, NFR-6, NFR-7)

Given a crash occurs after pointer commit before outbox commit, When a later execution reconciles journals and the current manifest, Then one deterministic intent is imported and duplicate recovery is a no-op.

### AC-4: Strict outbox contract (FR-4, FR-14, FR-15, NFR-6)

Given an empty control root, When the outbox is initialized and two workers claim the same intent, Then the normative DDL accepts it, exactly one CAS claim wins, and immutable identity/dead-letter rows cannot be deleted or rewritten.

### AC-5: Complete source snapshot (FR-6, FR-9, FR-11, FR-26, NFR-2, NFR-9)

Given a source manifest or object has a missing, changed, wrong-size, wrong-schema, wrong-row-count or wrong-hash condition, When an intent is planned, Then the candidate is rejected without destination writes and with a sanitized stage reason.

### AC-6: Destination trust and path safety (FR-7, FR-21, FR-23, FR-24, NFR-8, NFR-9)

Given an unsafe path, unapproved mount, missing trust descriptor, symlink or overlapping root, When a dry-run is requested, Then it returns a bounded error and performs no initialization, credential read or write.

### AC-7: Verified staged transfer (FR-8, FR-9, FR-10, FR-11, NFR-2, NFR-3, NFR-9)

Given a complete source and trusted empty destination, When one offline fake drain executes, Then all objects pass readback and the complete lineage-bearing destination manifest is published last.

### AC-8: Failure leaves destination pointer unchanged (FR-8, FR-9, FR-10, FR-12, NFR-3, NFR-12)

Given copy, verification, rename or manifest publication is interrupted, When the attempt stops, Then the previous destination manifest remains byte-identical and partial data is not referenced.

### AC-9: Idempotence and no old overwrite (FR-11, FR-12, FR-13, FR-27, NFR-3, NFR-14)

Given a destination has exact, older, conflicting or reverse lineage, When the same source is drained, Then only exact lineage is reused and no unsafe destination or source pointer is overwritten.

### AC-10: Retry and dead-letter (FR-14, FR-25, FR-26, NFR-4, NFR-5)

Given a retryable destination error, When the bounded drain is run repeatedly, Then the exact retry schedule is durable and the sixth failure becomes dead-letter without provider access.

### AC-11: Concurrency and stale lease (FR-4, FR-15, FR-26, NFR-6)

Given one worker crashes while another claims the expired lease, When both attempt state advancement, Then the stale worker cannot copy/publish and only the winning state-version transition is accepted.

### AC-12: Restore dry-run (FR-13, FR-16, FR-17, FR-20, FR-23, FR-24, NFR-1, NFR-12)

Given a trusted archive and explicit temporary destination, When `market-restore` is run without `--execute`, Then it performs no writes/provider calls and emits only counts/hashes and a safe plan.

### AC-13: Verified temporary restore (FR-16, FR-17, FR-20, FR-24, NFR-2, NFR-3, NFR-9, NFR-12)

Given a complete archive, When restore executes, Then a newly finalized temporary root passes strict manifest/object/query verification and canonical bytes/inodes are unchanged.

### AC-14: Restore failure is invisible (FR-16, FR-17, FR-20, FR-23, NFR-9, NFR-12)

Given a corrupt or changing archive, When restore executes, Then no final root is published, no canonical state is changed and only the allowlisted reason is returned.

### AC-15: Read-only status (FR-18, FR-24, FR-25, NFR-1, NFR-8, NFR-11)

Given outbox state is missing, corrupt, locked, empty, retrying, dead-lettered or healthy, When the status endpoint is called, Then it does not initialize state, exposes bounded counts/lag, and reports zero provider requests and writes=false.

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
- EC-10: Newer/conflicting/unknown destination lineage → no overwrite and exact reason.
- EC-11: Manifest changes between baseline and replace → CAS conflict with current pointer preserved.
- EC-12: Copy/process crash before manifest → old manifest remains and verified objects may be reused.
- EC-13: Readback mismatch → no manifest publication, bounded failure.
- EC-14: Missing/corrupt/locked outbox on status → 503 state unavailable, no migration/write.
- EC-15: Enqueue failure after local pointer → preserve local ready and write deterministic journal when possible.
- EC-16: Outbox and journal both fail → preserve local ready and report durability gap for reconciliation.
- EC-17: Crash after journal before import → idempotent import before journal removal.
- EC-18: Concurrent claim/lease expiry → stale worker cannot publish or mark success.
- EC-19: Sixth retry or non-retryable trust/integrity error → retained dead-letter, no infinite retry.
- EC-20: Restore source changes or is invalid → no final root and canonical state unchanged.
- EC-21: Restore destination exists/non-empty/unsafe/overlapping → reject before writes.
- EC-22: Restore verification/query fails after staging → quarantine temp root, no enqueue/canonical change.
- EC-23: Legacy mirror sees new lineage fields → old manifest reader remains compatible; new reader is strict.
- EC-24: Reverse replication, source deletion, manual pointer edit or credentialed mount → direction error, zero writes.

## API Contracts

The implementation will add one read-only API and CLI serializers with this exact bounded shape.

```typescript
interface ReplicationStatusResponse {
  status: "disabled" | "ready" | "degraded" | "unavailable";
  reason_code: string;
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
  200: disabled, ready or degraded
  503: unavailable, same sanitized shape

interface ReplicatePlanResponse {
  status: "dry_run" | "ready" | "degraded" | "unavailable";
  reason_code: string;
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
  reason_code: string;
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

CLI exit codes are fixed: `0` for a safe plan or successful execution, `1` for unavailable,
retry/dead-letter or integrity failure, and `2` for lexical/configuration/path/direction error.
CLI outputs never echo input paths, credentials, URLs or arbitrary exceptions.

## Data Models

### Implementation file map

| File | Planned change | Boundary |
|---|---|---|
| `backend/app/config.py` | Add default-off replication settings and fixed bounds | No credentials; absolute destination only |
| `backend/app/storage/layout.py` | Add outbox/journal/replication lock paths | Never creates destination during read/status |
| `backend/app/storage/models.py` | Add strict private/public projection models | No path/payload fields in public model |
| `backend/app/storage/replication.py` | New source snapshot, outbox, journal, transfer, restore and status services | Only module allowed to implement local-to-NAS direction |
| `backend/app/storage/mirror.py` | Keep NAS-to-local API; share only safe validation helpers if needed | Must not call replication in reverse |
| `backend/app/market/automation.py` | Invoke local enqueue after canonical commit, preserve result | No destination I/O before canonical ready |
| `backend/app/api/storage.py` | Add read-only `/replication` route | No initialization/migration/write |
| `backend/app/cli.py` | Add `market-replicate`, `market-restore`, status and explicit init parsing | Dry-run default |
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

The source snapshot, plan and intent domains are respectively
`stock-eva/r2f4.3/source-snapshot/v1`, `stock-eva/r2f4.3/replication-plan/v1` and
`stock-eva/r2f4.3/replication-intent/v1`. The plan includes every sorted object path, object
hash, size, row count, trade date and source. Intent identity includes operation day, direction,
destination ID, source manifest identity and complete plan hash. Raw manifest bytes hash and
canonical manifest identity are both retained.

### Normative outbox DDL

The implementation MUST embed and execute the following only from an explicit writer lifecycle;
read/status paths MUST never execute it:

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

### State, transaction and crash implementation matrix

| Concern | Concrete implementation rule | Evidence |
|---|---|---|
| Canonical boundary | Call enqueue only after canonical manifest/control pointer commit; never await NAS | AC-2, EC-15 |
| Enqueue gap | Atomic journal fallback; startup reconciliation derives current manifest intent | AC-3, EC-16/17 |
| Claim | `BEGIN IMMEDIATE`, lease owner/until and `state_version` conditional update | AC-4, AC-11 |
| Copy | Stage under destination `_staging/<intent>`; no manifest references staging | AC-7/8 |
| Readback | Open no-follow and verify size/schema/rows/hash/fingerprint before each rename | AC-5/7 |
| Publish | Replace only lineage-bearing destination manifest after all objects; fsync file/parent | AC-8/9 |
| Retry | Fixed schedule, retryable classes only, sixth claim dead-letters | AC-10 |
| Restore | Stage beside new temp root, verify, atomically finalize; no canonical control call | AC-12/13/14 |
| Status | `initialize=False`, SELECT-only snapshot, sanitized projection | AC-15 |
| Legacy mirror | Do not import or invoke its `sync()` from local-to-NAS service | AC-18 |

## Implementation Steps

### Step 0 — Freeze scope and create RED inventory

1. Confirm exact base with `git rev-parse HEAD` and verify a clean worktree.
2. Add `tests/test_dataset_replication.py` with the named FR/AC/EC tests below, using local
   temporary roots and fake clocks/mounts/copy failures only.
3. Add tests for the current mirror and LaunchAgent assets before touching implementation.
4. Run the focused RED command and record failure names; no `--execute`, provider, NAS or real
   mount is allowed.

### Step 1 — Configuration and layout

1. Add `replication_enabled=false`, destination root, outbox basename, journal directory,
   retry/lease/time budgets and a fixed `local_to_nas` direction to `Settings`.
2. Reject non-basename SQLite names and unsafe/relative destination values at configuration
   validation, while keeping status able to report unavailable without creating paths.
3. Add `StorageLayout.replication_database`, `replication_journal_root` and lock properties;
   `ensure_local_runtime_dirs()` may create local control/journal parents only in writer mode.

### Step 2 — Strict source and destination proof

1. Implement descriptor-bound source snapshot using existing `NasMarketStore` validation concepts,
   but capture raw manifest bytes hash, canonical manifest identity and complete inventory.
2. Implement no-follow path traversal, explicit mount inspector and trusted destination descriptor.
3. Define destination initialization as a separate acknowledged command that accepts only a new
   empty child; it must not adopt an old archive or overwrite existing files.
4. Add source/destination fingerprint race tests before writing transfer code.

### Step 3 — Outbox DDL, journal and enqueue boundary

1. Implement the exact DDL, writer-only initialization and SELECT-only strict reader.
2. Implement deterministic source snapshot/plan/intent hashes and unique dedup.
3. Implement `enqueue_after_commit`, journal fallback and current-manifest reconciliation.
4. Keep enqueue errors out of `RefreshResult.status`; expose an additive sanitized replication
   projection while preserving existing automation/legacy JSON contracts.
5. Add transaction crash tests for pointer-before-enqueue, SQLite commit, journal import and
   double durability failure.

### Step 4 — Transfer, lineage, retry and CAS

1. Implement explicit `local_to_nas` service; do not call `MarketDatasetMirror.sync()`.
2. Validate destination trust, stage each immutable object, read back size/schema/rows/hash,
   fsync and rename final objects, then atomically publish the lineage-bearing manifest.
3. Compare object-set lineage so an older source cannot replace a newer destination; conflicting
   same-partition hashes and unknown/reverse lineage fail closed.
4. Implement lease/state-version CAS, fixed retry schedule, terminal dead-letter and cleanup of
   non-visible partials without deleting published objects.
5. Add injected copy/readback/manifest/CAS/concurrency tests.

### Step 5 — Restore verifier

1. Implement a separate `nas_to_temporary_root` reader/writer boundary with no source mutation.
2. Require explicit new temporary destination, stage objects, write temporary sentinel/manifest
   only after complete verification and atomically finalize the root.
3. Verify hashes, schema, row counts, dates, source, manifest identity and representative
   read-only summary/history queries without `reconcile_control_pointer()`.
4. Add corrupt-source, symlink-race, existing-destination and post-rename query-failure tests.

### Step 6 — API, CLI and automation wiring

1. Add `GET /api/v1/storage/replication` with 200/503 bounded response and zero-write dependency.
2. Add `market-replicate`, `market-restore`, `market-replication-status` and separately
   acknowledged destination initialization. Default every operation to dry-run.
3. Wire only local enqueue after canonical pointer commit. Optional refresh-slot drain is bounded,
   feature-flagged and records destination failure separately from local refresh outcome.
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
- OS-3: Replacing `NasMarketStore`, `MarketStore`, `manifest.json`, canonical Parquet or `MarketDatasetMirror`.
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

## R2-F4.3.1 implementation amendment — H1-H6/M1-M6 closure

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

Extend the committed local publication row atomically with `run_id`, `trade_date`, `published_at`,
`publication_sequence`, `pointer_generation`, `pointer_manifest_identity`,
`pointer_manifest_bytes_sha256`, `pointer_object_set_sha256` and `pointer_row_sha256`. The reader
must prove `pointer_generation == manifest.generation`, canonical manifest identity, exact manifest
bytes hash and complete object-set digest. It returns a `CanonicalPointerSnapshot` containing the
four pointer digests, visible generation, publication sequence, parent manifest hash and complete
inventory. The intent contains that snapshot and can copy only the current visible generation.
Add the reader and service as explicit dependencies in `backend/app/main.py`; construction errors
are blocked/unavailable and never silently replaced by a NAS reader. Add the four H1 tests named in
the design amendment before implementation:
`tests/test_dataset_replication.py::test_local_pointer_reader_binds_row_hash_generation_manifest_and_object_set`,
`tests/test_dataset_replication.py::test_source_pointer_mismatch_is_fail_closed`,
`tests/test_dataset_replication.py::test_replication_copies_only_current_visible_generation`, and
`tests/test_dataset_replication.py::test_main_wires_explicit_local_pointer_reader_without_nas_fallback`.

### H2 — sidecar implementation and strict read-only reader

Replace the earlier single mutable outbox table with the exact four-table sidecar below:
immutable `replication_sidecar_meta`, immutable `replication_intents`, immutable append-only
`replication_attempt_events`, and mutable CAS-only `replication_heads`. `schema_identity`,
`schema_version`, `ddl_sha256` and `schema_digest` must be checked on every writer and reader.
Each event sequence starts at zero, has a previous-event hash, and is replayed globally: every
intent has one head, every event references an intent, sequences are contiguous, event hashes and
head state agree. Orphan, gap, broken-chain or duplicate evidence is unavailable/fail-closed.

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
    destination_id TEXT NOT NULL CHECK (length(destination_id) = 64),
    pointer_row_sha256 TEXT NOT NULL CHECK (length(pointer_row_sha256) = 64),
    pointer_generation TEXT NOT NULL CHECK (length(pointer_generation) BETWEEN 1 AND 128),
    pointer_manifest_identity TEXT NOT NULL CHECK (length(pointer_manifest_identity) = 64),
    pointer_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(pointer_manifest_bytes_sha256) = 64),
    pointer_object_set_sha256 TEXT NOT NULL CHECK (length(pointer_object_set_sha256) = 64),
    publication_sequence INTEGER NOT NULL CHECK (publication_sequence >= 1),
    parent_manifest_sha256 TEXT CHECK (parent_manifest_sha256 IS NULL OR length(parent_manifest_sha256) = 64),
    source_generation TEXT NOT NULL CHECK (length(source_generation) BETWEEN 1 AND 128),
    source_manifest_identity TEXT NOT NULL CHECK (length(source_manifest_identity) = 64),
    source_manifest_bytes_sha256 TEXT NOT NULL CHECK (length(source_manifest_bytes_sha256) = 64),
    source_snapshot_sha256 TEXT NOT NULL CHECK (length(source_snapshot_sha256) = 64),
    plan_sha256 TEXT NOT NULL CHECK (length(plan_sha256) = 64),
    intent_sha256 TEXT NOT NULL CHECK (length(intent_sha256) = 64),
    object_count INTEGER NOT NULL CHECK (object_count >= 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
    created_at TEXT NOT NULL,
    UNIQUE (direction, destination_id, pointer_manifest_identity, plan_sha256)
) STRICT;

CREATE TABLE replication_attempt_events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK (length(event_id) = 64),
    intent_id TEXT NOT NULL REFERENCES replication_intents(intent_id),
    event_sequence INTEGER NOT NULL CHECK (event_sequence >= 0),
    prev_event_sha256 TEXT,
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
CREATE TRIGGER replication_heads_monotonic_cas
BEFORE UPDATE ON replication_heads
WHEN NEW.state_version <> OLD.state_version + 1
BEGIN
    SELECT RAISE(ABORT, 'replication head requires state-version CAS');
END;
```

Every connection sets WAL/FULL. A head update and event append are one transaction, followed by
database/WAL and parent-directory fsync. The status reader opens the DB read-only with
`O_RDONLY|O_CLOEXEC|O_NOFOLLOW`, verifies descriptor inode/device before and after SELECT and
performs no initialization, migration, checkpoint, repair or cleanup. It must preserve sidecar
bytes/inodes for all status cases. Implement and test global reachability/replay and the zero-write
status contract before any writer test: `tests/test_dataset_replication.py::test_outbox_global_reachability_and_event_replay`
and `tests/test_dataset_replication.py::test_status_missing_corrupt_or_locked_outbox_is_zero_write`.

### H3 — destination descriptor and initialization

Use the exact private filename `.stock-eva-replication-destination.json`. Canonical JSON is sorted,
compact UTF-8 with one final newline and contains exactly `descriptor_schema`, `schema_version`,
`dataset`, `role`, `direction`, `root_dev`, `root_ino`, `parent_dev`, `parent_ino`, `mount_point`,
`fs_type`, `mount_generation`, `mount_fingerprint`, `sentinel_sha256`, `created_at` and
`descriptor_sha256`. The role is `nas_archive`, direction is `local_to_nas`; descriptor hash is
`domain_sha256('stock-eva/r2f4.3/destination-descriptor/v1', all other fields)`. The descriptor is
private; serializers omit its path and mount point.

Use descriptor-bound directory fds and `O_NOFOLLOW`; compare root/parent device+inode, mount point,
filesystem type, mount generation/fingerprint and sentinel hash before each execute boundary.
Changes are `DESTINATION_REBOUND`, not a retryable success. Implement the standalone command
`market-replication-init --destination /absolute/new-child --execute --acknowledge
CREATE_EMPTY_NAS_ARCHIVE_R2F4_3`; only that exact typed ack authorizes initialization. Plan mode,
including an absent parent, performs zero mkdir/descriptor/sentinel/sidecar/mount work. The exact
anchors are `tests/test_dataset_replication.py::test_destination_descriptor_canonical_json_and_hash`,
`tests/test_dataset_replication.py::test_destination_descriptor_binds_root_parent_mount_and_sentinel`,
`tests/test_dataset_replication.py::test_destination_remount_or_inode_change_fails_closed`, and
`tests/test_dataset_replication.py::test_destination_init_requires_exact_typed_ack_and_is_dry_run_by_default`.

### H4 — lineage and ordering

Extend the local canonical pointer/manifest lineage with monotonic per-root
`publication_sequence` and `parent_manifest_sha256`, allocated and committed with the pointer.
Destination lineage stores sequence, parent hash, source pointer-row/manifest/object-set hashes,
descriptor identity and plan hash. Implement the partial order: equal sequence and equal hashes is
idempotent; a source descendant may copy; a destination ahead ancestor returns `DESTINATION_AHEAD`;
equal-sequence hash/parent disagreement or incomparable chains returns `DESTINATION_CONFLICT`;
final baseline drift returns `CAS_CONFLICT`. Never sort UUID generations, timestamps, mtimes or
retry order. The exact anchors are
`tests/test_dataset_replication.py::test_lineage_publication_sequence_parent_hash_partial_order`
and `tests/test_dataset_replication.py::test_destination_ahead_divergent_tie_and_cas_are_fail_closed`.

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

The source object-set digest is
`domain_sha256("stock-eva/r2f4.3/object-set/v1", sorted_object_inventory)`;
`source_snapshot_sha256` is
`domain_sha256("stock-eva/r2f4.3/source-snapshot/v1", {pointer_row_sha256,
pointer_generation,pointer_manifest_identity,pointer_manifest_bytes_sha256,
pointer_object_set_sha256,publication_sequence,parent_manifest_sha256,object_inventory})`;
`plan_sha256` is
`domain_sha256("stock-eva/r2f4.3/replication-plan/v1", {direction,destination_id,
source_snapshot_sha256,object_inventory})`; and `intent_id` is
`domain_sha256("stock-eva/r2f4.3/replication-intent/v1", {operation_day,direction,
destination_id,pointer_manifest_identity,plan_sha256})`. `intent_sha256` hashes the complete
immutable intent row. Descriptor and event hashes use the domains specified in the design
amendment. Golden vectors and one-byte newline/ordering mutations are mandatory tests.

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

### H6 — restore sequencing

Restore writes a hidden opaque staging root. Before rename, perform every semantic gate: descriptor,
sentinel, exact schema, no extra/duplicate entries, complete object size/schema/row/hash/date
checks, counts and representative read-only queries. Fsync and atomically rename to a new root,
then atomically finalize the typed restore pointer. Readers require that pointer and strict manifest;
an unreferenced root is not visible. After rename perform only bounded pointer/root/inode/hash
readback, never another semantic gate. Pointer-finalization failure leaves the root unpublished or
quarantined. The exact anchors are
`tests/test_dataset_replication.py::test_restore_semantics_complete_before_atomic_rename`,
`tests/test_dataset_replication.py::test_restore_pointer_failure_leaves_root_unpublished_or_quarantined`,
and `tests/test_dataset_replication.py::test_restore_post_rename_readback_is_nonsemantic`.

### M1/M2/M3/M4 — implementation order and failure rules

Use the closed `ReplicationReason` union from the design amendment, including
`LOCAL_POINTER_MISMATCH`, `DESTINATION_REBOUND`, `DESTINATION_AHEAD` and
`RESTORE_POINTER_UNPUBLISHED`; map unknown errors to `REPLICATION_STATE_UNAVAILABLE`. Implement
one priority algorithm: lexical path/direction, sidecar proof, disabled/source proof, destination
mount/trust, lineage/CAS, copy/readback, retry/dead-letter, then idempotent/ready.

In `backend/app/cli.py`, dispatch replication/status/init/restore before
`ensure_local_runtime_dirs()` or any factory; lexical validation has no I/O, strict readers are
read-only, and only execute can create approved local sidecar parents. Explicitly inject
`local_market_dataset_root`; NAS-only settings return `SOURCE_NOT_CONFIGURED`. Journal writes use
`<intent_id>.json`, mode `0600`, `O_NOFOLLOW`, complete newline-terminated bytes, fsync(fd),
same-directory atomic replace and parent fsync. Import validates no-follow fd/inode and hash,
commits intent/event/head WAL/FULL, fsyncs, then removes the journal; failed removal is safely
re-importable. The exact early-dispatch/source/journal anchors are
`tests/test_dataset_replication.py::test_market_replicate_dry_run_parent_absent_creates_nothing`,
`tests/test_dataset_replication.py::test_market_restore_dry_run_parent_absent_creates_nothing`,
`tests/test_dataset_replication.py::test_market_replication_status_missing_sidecar_does_not_initialize`,
`tests/test_dataset_replication.py::test_nas_only_configuration_never_becomes_replication_source`,
and `tests/test_dataset_replication.py::test_journal_no_follow_fsync_atomic_import_and_crash_recovery`.

### M5/M6 — exact settings, steps and evidence gate

Implement exactly these names/defaults: `replication_enabled` /
`STOCK_EVA_REPLICATION_ENABLED=false`; `replication_destination_root` /
`STOCK_EVA_REPLICATION_DESTINATION_ROOT=None`; `replication_database_name` /
`STOCK_EVA_REPLICATION_DATABASE_NAME=replication.sqlite3`; `replication_journal_root_name` /
`STOCK_EVA_REPLICATION_JOURNAL_ROOT_NAME=replication-journal`; `replication_max_attempts` /
`STOCK_EVA_REPLICATION_MAX_ATTEMPTS=6`; `replication_lease_seconds` /
`STOCK_EVA_REPLICATION_LEASE_SECONDS=900`; `replication_drain_timeout_seconds` /
`STOCK_EVA_REPLICATION_DRAIN_TIMEOUT_SECONDS=900`; and fixed
`replication_direction=local_to_nas`. Retry delays are fixed `(60,300,1800,7200,43200)` and not
environment-configurable. Reject root/home/share/unresolved-env/relative/overlap paths.

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
| NFR-7 | `tests/test_dataset_replication.py::test_hash_preimages_are_golden_and_newline_terminated` |
| NFR-8 | `tests/test_dataset_replication.py::test_status_cli_api_have_no_path_or_secret` |
| NFR-9 | `tests/test_dataset_replication.py::test_descriptor_bound_reads_reject_symlink_and_toctou` |
| NFR-10 | `tests/test_dataset_replication.py::test_nas_failure_does_not_change_local_ready_pointer` |
| NFR-11 | `tests/test_dataset_replication.py::test_status_is_bounded_without_parquet_hash_scan` |
| NFR-12 | `tests/test_dataset_replication.py::test_failed_restore_has_no_reader_visible_root` |
| NFR-13 | `tests/test_launchagent_assets.py::test_replication_defaults_off_and_no_provider_in_dry_run` |
| NFR-14 | `tests/test_nas_dataset.py::test_canonical_manifest_and_pointer_bytes_remain_unchanged` |
| NFR-15 | `tests/test_dataset_replication.py::test_spec_evidence_uses_no_real_nas_or_provider` |

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
