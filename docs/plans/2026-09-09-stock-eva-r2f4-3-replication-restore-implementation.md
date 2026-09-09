# Stock EVA R2-F4.3 Local Replication and Verified Restore — Implementation Plan

**Author:** Codex

**Date:** 2026-09-09 (Asia/Shanghai)

**Status:** In Review / implementation MUST NOT start before design approval

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

## Review and release boundary

The implementation plan is not a production approval. Before implementation, the design and this
plan require independent SPEC review. After implementation, the release candidate requires
independent QUALITY review of the exact commit, all test anchors, diff, static checks and no-write
evidence. Real NAS copy/restore remains blocked until a new explicit operation window is opened.
