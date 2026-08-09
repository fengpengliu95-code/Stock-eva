# Spec: Stock EVA R1-E Release 1 Historical Snapshot and Final Acceptance

**Author:** Codex root quality lead
**Date:** 2026-08-10
**Status:** Approved
**Reviewers:** User (scheme A approved 2026-08-10), independent R1-E reviewer before main integration
**Related specs:** `2026-07-29-stock-eva-strategy-review-roadmap.md`,
`2026-07-29-stock-eva-roadmap-execution-plan.md`,
`2026-08-09-stock-eva-linear-delivery-model.md`

## Context

Release 1 already has point-in-time classification, a deterministic market-regime engine,
sector/leader analysis and a market-to-stock workspace. The production market dataset contains
270 immutable trading-date partitions from 2025-07-01 through 2026-08-07, but the regime endpoint
still recomputes results on demand. The Roadmap requires one replayable market state for every
data-ready trading day and a final no-future-data acceptance over at least 20 real sessions.

The production classification database currently contains one promoted generation with
`source_snapshot_date=2026-08-07` and `observed_at=2026-08-09`. It therefore supports a truthful
current coverage audit (5,202/5,205 eligible securities mapped, 99.942%) but cannot prove that the
same classification was visible during the preceding 20 sessions. R1-E MUST preserve that
distinction: post-hoc replay can prove that no source/effective/price date exceeds the replayed
`as_of`, but it MUST NOT relabel later-observed classification as contemporaneous history.

R1-E also inherits the completed HTTP GET read-only boundary. Derived snapshots must have a
dedicated local control store and an explicit writer lifecycle. They must not reopen writes to the
market control DuckDB, immutable Parquet dataset, classification DuckDB or private user databases.
Release 2 fund-flow evidence, position guidance and broker behavior remain locked.

## Functional Requirements

- FR-1: The system MUST store derived market-regime snapshots in a dedicated local SQLite database
  under `local_control_dir`; it MUST NOT store them in the market control DuckDB, classification
  DuckDB, market dataset or a private user database.
- FR-2: The snapshot store MUST persist at most one immutable record for each
  `(as_of, formula_version)` pair.
- FR-3: Repeating a write with identical canonical content MUST be idempotent and MUST NOT change
  database bytes, row count, `recorded_at` or the existing snapshot identifier.
- FR-4: A write for an existing `(as_of, formula_version)` with different canonical content,
  result identity or publication lineage MUST fail closed as a conflict and MUST NOT overwrite the
  existing record.
- FR-5: Each snapshot MUST persist the regime payload, `result_id`, formula version, `as_of`,
  `data_as_of`, capture mode, UTC evidence cutoff, UTC recorded time, dataset generation, a
  SHA-256 hash of the verified dataset identity, a payload content hash and a deterministic
  snapshot identifier.
- FR-6: Persisted metadata and public responses MUST NOT contain filesystem paths, NAS mount names,
  credentials, exception text or traceback data.
- FR-7: A post-publication writer MUST capture the exact ready publication trading day after a
  successful market publication and MUST skip partial, error, stale-pointer or mismatched
  publication results.
- FR-8: Failure to capture a derived snapshot MUST NOT roll back or corrupt a successful immutable
  market publication; the failure MUST remain observable and the missing snapshot MUST fail the
  R1-E completeness audit.
- FR-9: A CLI backfill MUST default to a read-only plan, require explicit `--execute` confirmation,
  select dates only from the verified immutable dataset manifest and support a bounded inclusive
  date range.
- FR-10: An executing backfill MUST capture every selected ready trading date, label records
  `post_hoc_backfill`, and return inserted/idempotent/conflict/error counts plus the selected date
  range without exposing local paths.
- FR-11: Automatic after-close captures MUST be labelled `after_close`; a backfill MUST NOT claim
  contemporaneous availability solely because its `as_of` is historical.
- FR-12: Snapshot generation MUST evaluate only market rows with `trade_date <= as_of`; every
  persisted source-lineage latest date and evidence date MUST also be `<= as_of`.
- FR-13: `GET /api/v1/analysis/market-regime/snapshots/{as_of}` MUST read only an exact persisted
  snapshot, revalidate its payload and hashes, and MUST NOT recompute or create a missing record.
- FR-14: The existing `GET /api/v1/analysis/market-regime?as_of=...` response contract and on-demand
  behavior MUST remain backward compatible.
- FR-15: A missing exact snapshot MUST return a structured 404; an unreadable, corrupt or
  incompatible snapshot store MUST return a sanitized structured 503; a future Shanghai date MUST
  return 422 before storage access.
- FR-16: Constructing an HTTP dependency, starting the API, or issuing any GET MUST NOT create the
  snapshot database, parent directory, schema, WAL or journal files and MUST NOT change bytes,
  size, mtime, schema or row count of an existing snapshot database.
- FR-17: The Release 1 audit MUST inspect at least 20 real immutable trading sessions and verify
  exact persisted snapshot coverage, snapshot/recompute equality, no future price/lineage reads,
  current industry coverage ratio, the explicit unmapped-symbol list, and classification
  availability or unavailability for every replay date.
- FR-18: The Release 1 audit MUST distinguish `post_hoc_backfill` from `after_close` and MUST report
  contemporaneous classification visibility as unverified when no generation was observed by the
  replay date.
- FR-19: Release 1 acceptance MUST browser-check the installed market → sector → leader → stock →
  sector path with stable `as_of`, taxonomy and sector context and MUST keep missing fund-flow and
  narrow-market disclaimers visible.
- FR-20: Production installation and acceptance MUST prove that the market dataset, market control
  DB, classification DB, supplemental audit DB and private user DB fingerprints are unchanged by
  backfill, GET readback and browser validation. The new snapshot DB is the only expected derived
  data mutation.

## Non-Functional Requirements

- NFR-P1: An exact snapshot GET against a local database containing at least 270 records MUST have
  p95 service time below 500 ms over 20 serial warm requests on the deployment machine.
- NFR-P2: A 20-session audit replay MUST complete within 180 seconds after immutable dataset
  checksum validation has completed.
- NFR-R1: Snapshot writes MUST use a single SQLite transaction and a uniqueness constraint so a
  crash cannot expose a partially written record.
- NFR-R2: Readers MUST open SQLite with URI `mode=ro`, zero schema initialization and a bounded
  busy timeout; lock contention MUST fail closed rather than wait indefinitely.
- NFR-S1: API and CLI error payloads MUST use an allowlisted code and counters only; captured
  exceptions, SQL, filesystem paths and environment values MUST NOT be serialized.
- NFR-S2: Snapshot payloads MUST be validated with the public Pydantic model before write and after
  read, and the deterministic content hash MUST be verified on every exact read.
- NFR-C1: The existing market-regime API, R1-D workspace routes and stored market/classification
  schemas MUST remain backward compatible.
- NFR-T1: Every new behavior and error boundary MUST have observed RED then GREEN evidence; the
  integrated branch MUST pass the full backend suite, frontend suite/build, Ruff and diff checks.

## Acceptance Criteria

### AC-1: Immutable idempotent snapshot (FR-1, FR-2, FR-3, FR-4, FR-5, NFR-R1, NFR-S2)
Given a temporary verified market input and an empty derived snapshot store
When the same regime result and publication lineage are captured twice
Then exactly one `(as_of, formula_version)` row exists
And the second result is idempotent with unchanged bytes, metadata and identifier
And a divergent third capture fails with a conflict without changing the stored row.

### AC-2: Honest capture modes (FR-5, FR-10, FR-11, FR-18)
Given one historical date selected by explicit backfill and one newly published ready date
When both snapshots are persisted
Then the historical record is labelled `post_hoc_backfill`
And the publication record is labelled `after_close`
And neither label implies contemporaneous classification visibility.

### AC-3: Post-publication ownership (FR-7, FR-8)
Given ready, partial, mismatched-pointer and already-published automation outcomes
When the post-publication pipeline runs or replays after restart
Then only the exact ready publication captures one idempotent snapshot
And capture failure leaves market publication successful but makes the derived task observable.

### AC-4: Safe CLI planning and execution (FR-9, FR-10, NFR-S1)
Given a manifest containing multiple verified trading dates
When the CLI is called without `--execute`
Then it returns a read-only plan and creates no file or directory
When it is called with `--execute` and an inclusive range
Then it captures exactly the selected manifest dates and returns sanitized aggregate counters.

### AC-5: No-future snapshot payload (FR-12, NFR-S2)
Given a reader that returns market rows or evidence after the requested date
When a snapshot is generated
Then future inputs are excluded or the capture fails closed
And every persisted `data_as_of`, lineage date and evidence date is not later than `as_of`.

### AC-6: Exact read-only API (FR-6, FR-13, FR-15, FR-16, NFR-R2, NFR-S1)
Given a valid exact snapshot, a missing date, a corrupt payload, an incompatible schema and a
locked database
When each snapshot GET is issued
Then the valid date returns the persisted record
And missing returns sanitized 404
And corrupt, incompatible and locked states return sanitized 503
And no request changes database or filesystem fingerprints.

### AC-7: Future request rejected before storage (FR-15, FR-16)
Given an instrumented snapshot reader and the current Shanghai date
When a future `as_of` is requested
Then HTTP 422 is returned before the reader is called
And no storage path is created or changed.

### AC-8: Backward-compatible live regime endpoint (FR-14, NFR-C1)
Given the existing market-regime fixtures and R1-D client tests
When the new snapshot capability is installed
Then the existing endpoint returns the same response shape and deterministic IDs
And the workspace continues to render without changing its API request contract.

### AC-9: Twenty-session Release 1 audit (FR-17, FR-18, NFR-P2)
Given at least 20 ready dates in one checksum-verified immutable dataset and the production
classification database
When the R1-E audit runs over the latest 20 selected sessions
Then each date has one exact validated regime snapshot matching deterministic recomputation
And future price, lineage, membership and effective-window reads equal zero
And classification availability and contemporaneous visibility are reported per date.

### AC-10: Industry coverage and gaps (FR-17, FR-18)
Given the latest promoted production classification generation
When the R1-E audit reads the industry coverage
Then the ratio is at least 0.95
And eligible, mapped and unmapped counts reconcile
And every unmapped symbol is listed
And component-history or contemporaneous-history limitations remain explicit.

### AC-11: Installed browser decision path (FR-19, NFR-C1)
Given the installed production runtime and a real available sector
When the user navigates market → sector → leader → stock and returns
Then `as_of`, taxonomy and sector context remain stable
And backend order, reasons, contrary evidence, raw indicators and lineage are visible
And missing fund-flow, non-actionability and narrow-scope boundaries remain visible
And the browser console has no warning or error.

### AC-12: Production mutation boundary (FR-20, NFR-S1)
Given pre-install fingerprints for all protected production datasets and databases
When the official installer, snapshot backfill, representative GET sequence and browser acceptance
complete
Then every protected fingerprint is identical before and after
And only the dedicated snapshot database contains the expected new immutable records.

## Edge Cases

- EC-1: Snapshot database is missing → exact GET returns 404 without creating the file or parent.
- EC-2: Database exists but required table/schema is missing → exact GET returns
  `regime_snapshot_store_unavailable` 503 without migration.
- EC-3: Database is exclusively locked → reader fails within the bounded timeout with sanitized 503.
- EC-4: Stored payload is malformed, non-object JSON, has a future date or fails content hash →
  reader returns sanitized 503 and does not expose the payload.
- EC-5: Same key has different result or dataset lineage → writer raises immutable conflict and
  leaves the original record unchanged.
- EC-6: Manifest is missing, corrupt, ambiguous, changes during query or references a bad object →
  plan/capture fails closed before a snapshot row is written.
- EC-7: Requested CLI range is inverted, future, outside manifest dates or contains zero ready
  dates → validation error with no database initialization.
- EC-8: Market publication is partial/error or is not the current published pointer → no snapshot
  capture is attempted.
- EC-9: A historical classification generation was observed after replay `as_of` → it is reported
  unavailable for contemporaneous history and is never silently treated as visible then.
- EC-10: Industry coverage is below 95% or counts do not reconcile → Release 1 audit is not ready
  and lists the exact gaps.
- EC-11: Snapshot API receives a malformed or future calendar date → FastAPI returns 422 before
  storage access.
- EC-12: Re-running a complete backfill → all selected dates are idempotent, with zero inserted and
  zero mutation of existing rows.

## API Contracts

Existing endpoint remains unchanged:

```text
GET /api/v1/analysis/market-regime?as_of=YYYY-MM-DD
200 MarketRegimeResult
422 future_as_of
503 market_storage_unavailable
```

New exact persisted endpoint:

```text
GET /api/v1/analysis/market-regime/snapshots/{as_of}
200 MarketRegimeSnapshotResponse
404 {"detail":{"code":"regime_snapshot_not_found","as_of":"YYYY-MM-DD"}}
422 {"detail":{"code":"future_as_of",...}}
503 {"detail":{"code":"regime_snapshot_store_unavailable","storage_status":"unavailable"}}
```

```typescript
type CaptureMode = "after_close" | "post_hoc_backfill";

interface MarketRegimeSnapshotResponse {
  snapshot_id: string;                 // regime-snapshot- plus 24 lowercase hex chars
  as_of: string;                       // ISO date; key part 1
  formula_version: string;             // key part 2
  result_id: string;
  data_as_of: string | null;
  capture_mode: CaptureMode;
  evidence_cutoff_at: string;          // UTC timestamp
  recorded_at: string;                 // UTC timestamp
  dataset_generation: string;
  dataset_identity_hash: string;       // 64 lowercase hex chars; never a path
  content_hash: string;                // 64 lowercase hex chars
  result: MarketRegimeResult;
}
```

CLI contract:

```text
stock-eva market-regime-snapshots --start YYYY-MM-DD --end YYYY-MM-DD
  -> dry-run JSON, writes_snapshot_data=false

stock-eva market-regime-snapshots --start YYYY-MM-DD --end YYYY-MM-DD --execute
  -> aggregate JSON with selected/inserted/idempotent/conflict/error counts,
     writes_snapshot_data=true only when at least one row is inserted
```

## Data Models

### `market_regime_snapshots`

| Field | Type | Constraints |
| --- | --- | --- |
| snapshot_id | TEXT | Deterministic PK, `regime-snapshot-[0-9a-f]{24}` |
| as_of | TEXT | ISO date, NOT NULL, unique with `formula_version` |
| formula_version | TEXT | NOT NULL, unique with `as_of` |
| result_id | TEXT | NOT NULL, must equal validated payload result ID |
| data_as_of | TEXT nullable | ISO date, MUST be `<= as_of` |
| capture_mode | TEXT | `after_close` or `post_hoc_backfill` |
| evidence_cutoff_at | TEXT | UTC timestamp, immutable |
| recorded_at | TEXT | UTC timestamp, immutable |
| dataset_generation | TEXT | Non-empty verified manifest generation |
| dataset_identity_hash | TEXT | 64 lowercase hex, no path material |
| content_hash | TEXT | SHA-256 of canonical persisted fields and result payload |
| result_payload | TEXT | Canonical JSON object validated as `MarketRegimeResult` |

Unique index: `(as_of, formula_version)`. No update/delete API exists in R1-E.

### `ReleaseOneReplayReport`

| Field | Type | Constraints |
| --- | --- | --- |
| status | enum | `ready` or `not_ready` |
| session_count | integer | MUST be at least 20 for ready |
| start/end | ISO date | Inclusive manifest-selected bounds |
| snapshot_count | integer | MUST equal session count for ready |
| matching_result_count | integer | MUST equal session count for ready |
| future_price_reads | integer | MUST equal 0 |
| future_membership_reads | integer | MUST equal 0 |
| classification_available_sessions | integer | Reported, not fabricated |
| contemporaneous_classification_sessions | integer | Determined from `observed_at <= as_of` |
| coverage | object | eligible/mapped/unmapped/ratio and explicit symbol list |
| capture_modes | object | Counts for after-close and post-hoc snapshots |
| quality_issues | string array | Sorted allowlisted limitations |

## Out of Scope

- OS-1: Acquiring or fabricating 20 contemporaneous historical classification generations — the
  source history has not accumulated; R1-E reports the limitation instead.
- OS-2: Changing `market-regime-v1` thresholds, weights or confidence policy — calibration is a
  separate model-version change.
- OS-3: Full-A authoritative universe/price coverage, verified index component history or Beijing
  Stock Exchange inclusion — no trusted coverage artifact exists in Release 1.
- OS-4: Fund-flow L1/L2/L3 evidence, persistence/trend language, portfolio exposure or position
  advice — these remain Release 2.
- OS-5: A historical snapshot chart, date-picker redesign or new frontend framework — R1-D already
  closed the Release 1 decision path; R1-E only verifies it.
- OS-6: NAS writes for derived snapshots — the snapshot database stays local; immutable market
  Parquet remains the only NAS market artifact.
- OS-7: Broker connectivity, order placement, automatic trading or investment recommendations —
  prohibited by the product boundary.
