# Stock EVA R2-F4.2 Exact-session Universe Contract Implementation Plan

**Author:** Codex R2-F delivery quality lead

**Date:** 2026-09-08 (Asia/Shanghai)

**Status:** In Review / implementation not started

**Reviewers:** Independent SPEC reviewer and independent QUALITY reviewer (not yet assigned)

**Authority:** The companion design
[`2026-09-08-stock-eva-r2f4-2-exact-session-universe-design.md`](2026-09-08-stock-eva-r2f4-2-exact-session-universe-design.md),
Task 16 of the umbrella R2-F roadmap, and the reviewed R2-F4.0/R2-F4.1 contracts.

**Baseline:** R2-F4.1 delivery `2030ba7fc155dfc7a3615bbfe7570a787755b98b`.

**Delivery boundary:** This plan authorizes specification-first implementation in the isolated
R2-F4.2 worktree only. It authorizes no real provider request, credential read, production or
Application Support access, NAS access, deployment, LaunchAgent execution, branch merge or
canonical source switch. A passing offline implementation is not R2-F4.2 GO.

## Context

The current classification store has a writer-only publish path and read-only point-in-time
selection of promoted generations. The current market automation captures the union of positions
and all watchlists through allowlisted UserStore methods, but `build_canonical_raw_request` still
lets a BaoStock `all_stock` response define the expected daily set. This makes a session's expected
universe implicit and makes a source count look like a contract.

Task 16 needs a versioned exact-session contract for the declared product scope, dynamically
derived from promoted classification/security-master data, reviewed instrument evidence, the
promoted R2-F4.1 calendar and one writer-owned required-symbol snapshot. The implementation must
handle list/delist, ST, suspension, required indexes, unknown states and provider extras without
changing the existing canonical data chain.

The implementation therefore proceeds as an additive sidecar and staged compatibility migration.
The legacy BaoStock path remains the canonical path in `off` and `shadow` modes. `enforce` remains
an enum value for status/configuration compatibility only: every execution request returns
`BLOCKED_ENFORCE_NOT_ENABLED`, including tests, and there is no callable future-provider seam.
TickFlow and Tushare are excluded from this subversion.

## Functional Requirements

- FR-1: Add the contract module. Implementation MUST create `backend/app/market/universe.py`
  with strict immutable models, state derivation, count validation, canonical hashing, evidence
  binding and complete-candidate validation from the design.

- FR-2: Add a separate sidecar. Implementation MUST add a writer-owned
  `market_universe.sqlite3` sidecar/head with strict initialization, append-only rows, atomic
  promotion, restart validation and read-only descriptor-bound access. It MUST NOT replace or
  widen the classification DB authority.

- FR-3: Preserve PIT sources. Implementation MUST consume one pinned R2-F4.1 calendar snapshot,
  one promoted classification `read_snapshot(trade_date)` result and one explicit
  `exact_pit_cutoff`. It MUST reject future, degraded, unpromoted or
  `source_date_semantics=requested_unverified` inputs before promotion, mapping control/PIT
  failures to `CONTROL_STATE_UNAVAILABLE` or `PIT_VISIBILITY_INVALID`.

- FR-4: Capture required symbols once. Implementation MUST invoke the UserStore-owned
  `capture_required_symbol_snapshot_once` once, whose single SQLite transaction reads positions,
  watchlists and items, deduplicates deterministically, binds roles and hashes the snapshot.
  Public reads MUST NOT open the user database.

- FR-5: Build dynamic members. Implementation MUST materialize and count/hash three layers
  (`classification_evidence`, `effective_main_board`, `required_additions`), then
  derive valid required A-share additions and exactly `sh.000001`/`sz.399001` index members
  without fixed 3,195/5,205 gates.

- FR-6: Derive explicit states. Implementation MUST apply versioned listing/delisting and
  reviewed suspension semantics, retain ST state, distinguish `not_yet_listed` from the legacy
  API's `not_listed_yet`, and fail closed on unknown/contradictory critical data.

- FR-7: Enforce invariants. Implementation MUST enforce the count equation, explicit parenthesized
  unknown-zero and loaded-match gates, exact expected candidate set, extras rejection, duplicate
  rejection and whole-session provider purity before Normalize. `unknown=1` MUST reject even when
  `loaded=session_expected`.

- FR-8: Integrate without a switch. Implementation MUST provide `off`, `shadow` and `enforce`
  reporting values. `off` is the configured default; `shadow` compares only the legacy BaoStock
  session; `enforce` MUST immediately return `BLOCKED_ENFORCE_NOT_ENABLED` and MUST NOT invoke a
  provider or canonical path in this subversion. The production profile MUST reject any non-off
  mode as `BLOCKED_PRODUCTION_MODE_OFF`; shadow is isolated offline/staging only.

- FR-9: Schedule maintenance. Implementation MUST NOT add a Universe/classification scheduler
  lane or sixth LaunchAgent. It MAY add only one bounded post-success hook after the existing
  canonical refresh and shadow offer; if no successful canonical refresh occurs, no Universe work
  is attempted.

- FR-10: Add read-only status. Implementation MUST add the bounded
  `GET /api/v1/market/universe` and `market-universe` projections with zero provider requests,
  zero writes and no sidecar initialization/migration on every status path.

- FR-11: Keep predecessor surfaces frozen. Implementation MUST preserve R2-F2 golden hashes,
  R2-F3 sidecar/evidence qualification, R2-F4.0 failover readiness and R2-F4.1 calendar/runtime
  behavior. Any incompatible change MUST stop implementation and update the contract first.

- FR-12: Preserve failure evidence. Implementation MUST report allowlisted reasons and retain
  old sidecar/classification heads on failed maintenance, stale input, storage failure or race.
  It MUST NOT expose raw exceptions, paths, payloads, URLs, tokens or private symbol lists.

- FR-13: Reviewed evidence authority. Implementation MUST require a closed
  `mapping_id`/`mapping_version` semantic mapping with reviewed status and source/date/PIT rules.
  Current BaoStock classification evidence marked `requested_unverified` MUST result in
  `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`, never a qualified contract; a later reviewed artifact
  may unblock it without changing this release's frozen canonical path.

- FR-14: User snapshot transaction. Implementation MUST add a writer-only single-transaction
  `capture_required_symbol_snapshot_once(operation_id)` returning symbols privately plus
  `user_store_revision` and `snapshot_token`; a revision/token change before commit MUST abort
  with `USER_SNAPSHOT_CHANGED`. Public status MUST expose only count/hash.

- FR-15: Exact raw-batch gate. Implementation MUST validate the complete existing
  `ProviderRawBatch` and its `RawEndpointBatch` pages carrying date/provider/refresh/session/
  request/evidence/endpoint/page metadata before any Normalize call. It MUST reject duplicate,
  extra, missing, non-contiguous-page and session-drift conditions as a whole batch, with provider
  allowlist exactly `baostock`.

- FR-16: Normative sidecar. Implementation MUST enforce the v1 SQLite DDL, schema digest, store
  identity, inode, WAL/full-sync, exclusive lock, sequence/parent/head CAS and strict-reader
  rules in the design. This release MUST initialize v1 only through the writer and MUST perform
  no legacy sidecar migration.

- FR-17: Existing automation boundary. Implementation MUST preserve the real `_plan_continuity`
  repair-lock boundary and the existing refresh lease. Universe MUST be only a bounded lowest-
  priority hook after a successful canonical refresh and the existing shadow offer; R2-F4.2 MUST
  NOT add an independent lane, claim a shared all-lane lock/ledger, or repeat an equivalent
  provider request. If canonical refresh is not due or fails, no Universe offer runs.

## Non-Functional Requirements

- **NFR-1 — Offline-only:** The implementation and all acceptance tests MUST use local fixtures or
  fakes that fail on unexpected network access. No real provider, production, NAS, deployment or
  LaunchAgent operation is permitted.

- **NFR-2 — Read-only status:** Missing/corrupt/stale status reads MUST leave a fingerprinted
  temporary control, market, user, staging and data tree unchanged, including parent directories.

- **NFR-3 — Atomic persistence:** A kill or injected failure at each sidecar insert/commit/head
  boundary MUST yield the old valid head or an unavailable status, never a partial promoted
  contract.

- **NFR-4 — Deterministic output:** Identical frozen inputs MUST produce byte-identical members,
  counts, status and SHA-256 values independent of set or dictionary order.

- **NFR-5 — Bounded work:** One status call MUST use one bounded sidecar read and zero provider
  calls. The post-success Universe hook MUST perform at most one bounded classification/evidence
  read, one UserStore snapshot and one sidecar promotion attempt; it MUST inherit existing
  orchestration boundaries without widening `_plan_continuity` or refresh locking.

- **NFR-6 — Privacy:** Public output MUST contain no user symbols, watchlist names, positions,
  private DB paths, raw evidence or exception text; it MAY expose counts, safe IDs, hashes and
  allowlisted reason codes.

- **NFR-7 — Compatibility:** Existing public classification and market responses, legacy source
  literals, normalization, quality gates, manifests, pointers and default scheduler behavior
  MUST retain their current meaning when universe mode is `off`.

- **NFR-8 — Traceability:** Each FR and edge case MUST have an offline regression or static
  evidence entry in the final acceptance record. Focused/full pytest, Ruff, format, compile and
  diff checks MUST pass before review.

## Acceptance Criteria

### AC-1: Specification-first module and schema (FR-1, FR-2, NFR-3, NFR-4)

**Given** a clean baseline and the approved design, **When** implementation begins, **Then** the
new module and sidecar schema are added without modifying protected predecessor bytes, and an
interrupted transaction is read as old-head-or-unavailable after restart.

### AC-2: Calendar and classification PIT binding (FR-3, FR-11, NFR-1, NFR-7)

**Given** fake promoted calendar/classification generations with valid, future, degraded, unknown
and conflicting variants, **When** a contract is built, **Then** only the valid visible generation
with an explicit `exact_pit_cutoff` is accepted and every invalid or
`requested_unverified` variant makes no provider request and no canonical write.

### AC-3: Required-symbol snapshot (FR-4, FR-12, FR-14, NFR-6)

**Given** duplicate symbols across positions and all watchlists plus one valid out-of-base-board
symbol, **When** the writer captures the allowlisted sources, **Then** one deterministic private
snapshot and role map is produced; public status reveals only its count/hash, and a mid-capture
change blocks the contract.

### AC-4: Dynamic scope and indexes (FR-5, FR-7, FR-11, NFR-4)

**Given** changing main-board membership, excluded boards/types, and user data containing both
required indexes, **When** the builder runs, **Then** the three layer counts/hashes are derived
from unique members, the two indexes appear once each with explicit `trading` state and reviewed
source/date/identity evidence, and no fixed count is used; suspended or unknown index evidence
blocks the build.

### AC-5: State and vocabulary (FR-6, FR-7, FR-11, FR-13)

**Given** before-listing, listing-date, delisting-date, explicitly suspended, explicitly trading,
ST and contradictory records, **When** states are derived, **Then** the new contract vocabulary is
used exactly, ST remains an attribute, explicit suspension is honored, and critical ambiguity is
`unknown` and non-promotable while the legacy classification spelling remains unchanged.

### AC-6: Count and publication gate (FR-7, FR-12, FR-14, NFR-2)

**Given** one member in each state category and one critical unknown, **When** counts are
validated, **Then** the equation and `session_expected` hold, `unknown > 0` blocks promotion and
the old head remains unchanged; a fully resolved contract reports `unknown == 0`. **Given**
`unknown=1` and `loaded=session_expected`, **When** publication is evaluated, **Then** it is
rejected; **Given** `unknown=0` and `loaded != session_expected`, **Then** it is rejected.

### AC-7: Exact candidate set (FR-7, FR-8, FR-12)

**Given** a promoted contract, **When** a fake provider omits an expected row, returns an extra,
duplicates a symbol, returns a non-session row or mixes provider IDs, **Then** the complete
candidate is rejected before Normalize and the previous canonical pointer remains unchanged.

### AC-8: Sidecar tamper and restart (FR-2, FR-7, FR-12, NFR-3)

**Given** a valid sidecar, **When** a member, hash, head, inode, mode, schema or lock is changed,
**Then** the strict reader returns `unavailable` (it MUST NOT serve a historical cached projection)
and never repairs state from a GET or accepts a partial contract.

### AC-8a: Normative v1 sidecar storage (FR-16, NFR-3)

**Given** a v1 sidecar with the exact DDL, schema digest, store identity, inode, WAL/full-sync
settings and singleton head, **When** a writer commits rows and performs sequence/parent CAS,
**Then** the strict reader verifies the full chain read-only; **Given** a legacy or unknown schema,
**Then** writer-only initialization refuses migration and status is unavailable.

### AC-9: Read-only API and CLI (FR-10, FR-12, NFR-2, NFR-6)

**Given** missing, valid, stale and corrupt sidecar roots, **When** the API or CLI status is
  requested, **Then** output contains zero provider requests and false writes, has no path/payload/
  symbol/token/exception text, and the complete filesystem fingerprint is unchanged.

### AC-10: Maintenance boundary (FR-9, FR-12, FR-17, NFR-5)

**Given** the real `run_due_once` flow, **When** `_plan_continuity` reports repair/control work,
**Then** its existing repair lease/lock and return behavior are unchanged and no Universe offer
runs; **Given** a successful canonical refresh, **Then** existing post-publish and shadow hooks
complete first and only then may one bounded Universe offer run; **Given** canonical is not due or
fails, **Then** no independent Universe lane or repeated provider request is created.

### AC-11: Failed maintenance keeps authority (FR-2, FR-9, FR-12, NFR-3)

**Given** a previous valid classification/universe head and a fake provider, storage, clock or
  CAS failure, **When** maintenance executes, **Then** the prior heads and canonical data remain
  unchanged, the failure is sanitized, and repeated ticks do not create an unbounded request loop.

### AC-12: Legacy migration modes (FR-8, FR-11, NFR-7)

**Given** a legacy BaoStock refresh with a matching or divergent contract, **When** mode is `off`,
**Then** legacy behavior and output are unchanged; **When** mode is `shadow`, **Then** only a
read-only comparison of legacy BaoStock is recorded after canonical success; **When** mode is
`enforce`, **Then** every execution returns `BLOCKED_ENFORCE_NOT_ENABLED`, performs no provider
request and no source switch; **Given** a production profile with `shadow` or `enforce`, **Then**
configuration is rejected as `BLOCKED_PRODUCTION_MODE_OFF` and legacy `off` behavior remains.

### AC-13: Frozen predecessor regression (FR-11, NFR-7, NFR-8)

**Given** the R2-F2 golden files, R2-F3 qualification evidence, R2-F4.0 frozen digest vectors and
R2-F4.1 calendar/runtime fixtures, **When** all focused and full gates run, **Then** every protected
hash, reader, default and behavior remains compatible.

### AC-14: Complete offline evidence (FR-1, FR-10, FR-12, NFR-1, NFR-8)

**Given** network-failing fakes and private temporary roots, **When** the implementation suite,
strict spec validator, static checks and independent reviews run, **Then** no real provider,
production, NAS, deployment or LaunchAgent state is accessed and every result is traceable to an
FR/AC/EC.

### AC-15: Automation decision matrix with bounded attempt record (FR-17, NFR-5, NFR-7)

**Given** continuity planning, freshness, repair, shadow and Universe/classification work are
due, **When** the existing one-shot automation runs, **Then** `_plan_continuity` and its repair
lock boundary remain first, `_execute_due` uses only its existing refresh lease, and no claim is
made that all lanes share one lock; **Given** canonical refresh succeeds, **Then** existing
post-publish and shadow offer complete before at most one bounded Universe hook; **Given** no
canonical refresh is due or it fails, **Then** no independent Universe lane runs and no equivalent
provider request is repeated. The hook reacquires the existing refresh lock non-blocking, then the
sidecar lock, and durably records its dedup plan before any classification request; a duplicate
plan is not retried.

### AC-16: Pre-Normalize raw interface (FR-15, NFR-4)

**Given** an accumulated existing `ProviderRawBatch` containing `RawEndpointBatch` rows with
date/provider/refresh/session/request/evidence/endpoint/page metadata, **When** the
`validate_provider_raw_batch(batch, contract)` runs in the offline/future enforce seam, **Then**
duplicate, extra, missing, non-contiguous page, invalid `tradestatus`, illegal suspended
placeholder and session-drift cases reject the entire batch before Normalize;
`provider_id` other than `baostock` is rejected and no partial page is returned. Mode `off` does
not call it; mode `shadow` observes after canonical success and cannot block canonical.

## Edge Cases

- EC-1: Calendar is missing, unknown, conflicting, locked, swapped or hash-invalid → no
  provider call; status is unavailable with `CONTROL_STATE_UNAVAILABLE`,
  `PIT_VISIBILITY_INVALID`, `CALENDAR_UNAVAILABLE` or `CALENDAR_CONFLICT`.
- EC-2: Classification DB has no promoted PIT generation, only a future/degraded generation,
  or changed snapshot during build → no contract promotion.
- EC-3: Instrument evidence uses `requested_unverified` for a state that requires an observed
  date → state is unknown; publication is blocked.
- EC-4: Listing/delisting windows are absent, contradictory or malformed → unknown; no guessed
  state or inferred end date.
- EC-5: Suspension status is represented only by zero volume/blank activity → do not infer
  suspended; existing quality gate handles the row.
- EC-6: ST is missing or contradictory → `st_state=unknown`; never map it to `no`.
- EC-7: User symbol is malformed, unsupported or changes between reads → block the complete
  snapshot; do not drop it.
- EC-8: User symbol duplicates an index or appears in multiple lists → one member and stable
  roles, one count contribution.
- EC-9: Required index evidence is missing, future-dated, identity-mismatched, suspended or
  unknown → `REQUIRED_INDEX_NOT_TRADING`; the contract blocks and no index row is accepted.
- EC-10: Provider returns an extra, missing, duplicate, not-listed, delisted or suspended row
  contrary to the exact contract → reject the complete candidate.
- EC-11: A candidate declares any provider other than BaoStock → reject whole-session purity; no
  TickFlow/Tushare construction or symbol-level fallback is permitted.
- EC-12: Sidecar fails after one table insert, commit, head update or inode replacement → old
  head or unavailable after restart.
- EC-13: Public GET is invoked on a missing sidecar → no directory/schema initialization and
  no user-store access.
- EC-14: Maintenance competes with freshness, repair, shadow or another process → priority and
  existing lock defer it without duplicate provider work.
- EC-15: A changed classification generation has lower quality or older effective date → old
  promoted contract remains authoritative.
- EC-16: Configuration requests `enforce` or contains an unsafe mode → status is blocked and
  legacy canonical behavior remains unchanged.
- EC-17: A raw DAILY_ASTOCK row has missing/unknown `tradestatus`, an illegal suspended
  placeholder or a state mismatch → `UNIVERSE_STATE_MISMATCH`; reject the whole batch before
  Normalize and retain the later Quality Gate.
- EC-18: An attempt plan is absent, duplicated, terminal twice, or exceeds its request budget →
  `UNIVERSE_SCHEMA_MISMATCH`; do not infer a previous success or resend a request.

## API Contracts

### API/CLI implementation contract

The implementation MUST expose the design's status shape without member payloads:

```typescript
interface UniverseStatusResponse {
  status: "ready" | "unavailable" | "stale" | "blocked";
  trade_date: string;
  verified_head_trade_date: string | null;
  universe_id: "all-main-board-plus-required-symbols";
  schema_version: 1;
  scope: "all-main-board-plus-required-symbols";
  contract_id: string | null;
  contract_sha256: string | null;
  classification_generation_id: string | null;
  calendar_generation_id: string | null;
  calendar_sha256: string | null;
  counts: {
    total: number;
    trading: number;
    suspended: number;
    not_yet_listed: number;
    delisted: number;
    unknown: number;
    session_expected: number;
    loaded: number | null;
    critical_attribute_unknown: number;
  };
  layers: {
    classification_evidence_count: number;
    classification_evidence_sha256: string;
    effective_main_board_count: number;
    effective_main_board_sha256: string;
    required_additions_count: number;
    required_additions_sha256: string;
  };
  required_indexes: ["sh.000001", "sz.399001"];
  required_user_symbol_count: number;
  publication_eligible: boolean;
  reason_code: string | null;
  provider_requests: 0;
  writes: false;
}
```

`GET /api/v1/market/universe?trade_date=YYYY-MM-DD` and
`stock-eva market-universe --date YYYY-MM-DD` use this projection. HTTP `422` and CLI exit `2`
represent invalid date/configuration before sidecar access. A verified head whose date differs from
the requested date is HTTP `200`, `status=stale`, `reason_code=UNIVERSE_DATE_MISMATCH`, and
exposes only `verified_head_trade_date`; a valid empty store with no head is HTTP `200` unavailable
with `CONTROL_STATE_UNAVAILABLE`. Blocked/ready/stale are HTTP `200` (CLI exit `1`). If path,
inode, schema, WAL, lock or hashes cannot be proven, return HTTP `503` with the fixed body
`{"code":"universe_control_unavailable"}` (CLI exit `3`). Neither operation initializes or
migrates sidecar state.
`publication_eligible` means exactly `(unknown == 0 and critical_attribute_unknown == 0 and
required_indexes_are_trading and all_evidence_checks_pass and (loaded is null or
loaded == session_expected))`; candidate publication additionally requires
`loaded == session_expected` with no missing/extra/duplicate/session-drift condition.

### Internal writer/automation contract

```typescript
interface UniverseMaintenanceDecision {
  lane: "freshness" | "repair" | "shadow" | null;
  action: "run" | "defer" | "none";
  reason_code: string | null;
  provider_requests: number;
  writes_canonical: false;
}

interface UniversePostSuccessHook {
  offer(
    canonical_result: RefreshResult,
    refresh_id: string,
    trade_date: string,
    manifest_ref: string | null,
    evidence_ref: string | null,
    now: string,
  ): UniverseHookResult;
}

interface UniverseHookResult {
  status: "PROMOTED" | "BLOCKED" | "DEFER";
  reason_code: string | null;
  provider_requests: number;
  writes_canonical: false;
}

interface LegacyUniverseIntegration {
  mode: "off" | "shadow" | "enforce";
}

interface BuildUniverseRequest {
  schema_version: 1;
  universe_id: "all-main-board-plus-required-symbols";
  scope: "all-main-board-plus-required-symbols";
  trade_date: string;
  calendar_generation_id: string;
  calendar_sha256: string;
  classification_generation_id: string;
  exact_pit_cutoff: string;
  source_refs: SourceRefsV1;
  required_user_symbol_snapshot: RequiredUserSymbolSnapshot;
  instrument_evidence: InstrumentEvidence[];
  provider_id: "baostock";
  mode: "off" | "shadow" | "enforce";
}

interface UniverseBuildResult {
  status: "candidate" | "promoted" | "blocked" | "unavailable";
  contract: UniverseContract | null;
  reason_code: string | null;
  provider_requests: number;
  writes_sidecar: boolean;
  writes_canonical: false;
}

interface UniverseCandidateGate {
  schema_version: 1;
  universe_id: "all-main-board-plus-required-symbols";
  scope: "all-main-board-plus-required-symbols";
  trade_date: string;
  calendar_sha256: string;
  contract_sha256: string;
  provider_id: "baostock";
  refresh_id: string;
  provider_session_id: string;
  request_id: string;
  endpoint_id: string;
  pages: number;
  adapter_version: string;
  observed_symbols: string[];
  missing_symbols: string[];
  extra_symbols: string[];
  duplicate_symbols: string[];
  session_drift: string[];
  result: "pass" | "reject";
  reason_code: string | null;
}

```

The internal writer may write classification/universe control evidence under its lock. It MUST
never change a canonical market pointer merely by creating a contract. The candidate gate MUST
consume the existing `ProviderRawBatch` and its `RawEndpointBatch` endpoint/page rows; it MUST NOT
introduce a parallel raw-batch type. It MUST validate the complete batch before Normalize and
reject any date/provider/session/request/evidence/endpoint drift, duplicate page or
`(symbol, trade_date)` key, missing/extra symbol, non-contiguous page, or
`loaded != session_expected`. It MUST reject `unknown=1` even if the loaded count matches. No
provider other than BaoStock may enter this interface.

## Data Models

### Code-level models

Implementation MUST realize the companion design models `UniverseContractV1`, `UniverseMemberV1`,
`UniverseCountsV1`, `InstrumentEvidenceV1`, `RequiredUserSymbolSnapshotV1`,
`UniverseStatusSnapshotV1` and `UniverseCandidateGate`. All models are strict, frozen, extra-forbid,
finite, safe-ID/hash validated and use the exact state vocabularies in the design.

`SourceRefsV1` is a required frozen model (calendar/classification generation IDs and hashes,
required snapshot ID/hash, sorted instrument evidence IDs, provider, trade date, exact PIT cutoff
and source-date semantics). `UniverseContractV1` MUST persist and hash sorted-unique
`classification_evidence_ids`, `effective_main_board_ids` and `required_addition_ids`, with a
partition digest for each. Excluded classification evidence is durable and linked with an explicit
exclusion reason; the strict reader recomputes all three partitions from evidence links and member
roles rather than trusting only final members or counts.

The builder MUST retain three separately accounted layers: (1) every unique promoted PIT
`classification_evidence` identity, including excluded records; (2) the
`effective_main_board` subset using `list_date <= trade_date < delist_date` (or no delist
date); and (3) `required_additions`, the valid user A-share union plus the two required indexes.
Each layer has its own sorted identity list, count and SHA-256: the strict fields are
`classification_evidence_ids`, `effective_main_board_ids` and `required_addition_ids`, each with
its partition SHA-256 in the payload and explicit sidecar columns. Excluded classification
evidence is persisted with role/exclusion links and is rehashed by the reader. The final member hash is computed
from the union, with overlap contributing one member and unioned roles. `not_yet_listed` and
`delisted` required additions remain visible but never enter `session_expected`.

The implementation MUST add the reviewed semantic mapping registry described by the design. A
mapping has closed provider/source-schema values, `mapping_id`, `mapping_version`, date/PIT rules
and review status. An unmapped value or `requested_unverified` source is unknown and yields
`BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`; current BaoStock classification therefore cannot
qualify this contract.

Required-index tests MUST use only a versioned local `InstrumentEvidenceV1` fixture/input with
`authority_status=reviewed`, `artifact_origin=local_reviewed_fixture`, provider `baostock`, closed
`mapping_id`/`mapping_version`, exact `trade_date`, `index_role=required_index`, identity and
explicit `expected_trading_state=trading`. This fixture proves only derivation logic; it is not a
provider qualification claim. Production has no qualified artifact in this subversion and must
remain stably `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`.

`InstrumentEvidenceV1` for each required index MUST persist/validate
`evidence_id`, `evidence_sha256`, `provider_id=baostock`, `authority_status`,
`artifact_origin`, `mapping_id`, `mapping_version`, `symbol`, `security_id`,
`source_snapshot_date`, `evidence_trade_date`, `index_role=required_index`, and explicit
`expected_trading_state=trading`. Any missing, future, non-reviewed or mismatched field returns
`REQUIRED_INDEX_NOT_TRADING` or `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`.

The writer-owned `backend/app/user/store.py` MUST implement the single-transaction boundary; the
Universe module MUST NOT chain the existing public list methods or open additional connections:

```typescript
interface RequiredUserSymbolSnapshotRead {
  snapshot: RequiredUserSymbolSnapshot;
  user_store_revision: string;
  snapshot_token: string;
}

capture_required_symbol_snapshot_once(operation_id: string): RequiredUserSymbolSnapshotRead
```

Only the existing UserStore writer owner may call this API; public API/CLI has no writer
connection or initialization permission. It opens exactly one already-initialized UserStore SQLite connection, issues `BEGIN` read
transaction, reads positions/watchlists/items in one deterministic SQL snapshot, and computes the
snapshot token as the domain SHA-256 of sorted canonical rows. It verifies the writer revision or
SQLite `data_version` before and after on the same connection. Busy/locked, mutation/revision
mismatch, schema mismatch or hash mismatch MUST rollback and return `USER_SNAPSHOT_CHANGED` or
`USER_STORE_UNAVAILABLE`, with no retry or second observation; close is required on commit and
rollback. Capture MUST NOT initialize or migrate UserStore. The private sidecar stores only
normalized symbols, role bitset, revision/token digest, count, capture time and hash. Public API/
CLI stores only expose count/hash.

### Sidecar schema

| Table | Required identity and contents | Mutation rule |
|---|---|---|
| `universe_meta` | schema version, schema digest, deployment store identity, DB inode | immutable after writer-only bootstrap |
| `universe_contract` | complete canonical `payload_json`, explicit identity/sequence/parent/hash/source refs/counts, partition IDs/hashes, trade date and PIT cutoff | insert-only |
| `universe_member` | contract hash, unique symbol, member payload/hash | insert-only |
| `universe_instrument_evidence` | evidence ID/hash, BaoStock mapping/source/date projection | insert-only |
| `universe_required_symbol_snapshot` | snapshot ID/hash, revision/token digest, count, private role projection | insert-only |
| `contract_evidence` | bidirectional contract-to-evidence links | insert-only; no orphan links |
| `contract_required_snapshot` | one bidirectional contract-to-required-snapshot link and hash | insert-only; exactly one per contract |
| `universe_attempt` | immutable post-success dedup plan, refresh/date identity and request budget | insert-only; one per dedup key |
| `universe_attempt_result` | immutable terminal status/reason and actual request count | insert-only; zero or one per plan |
| `universe_head` | singleton 1, contract ID/hash, sequence, head hash | one writer-only transactional CAS update |

The exact DDL is the design's normative v1 DDL; implementation MUST checksum its canonical DDL as
`schema_digest`, set `PRAGMA user_version=1`, `journal_mode=WAL`, `synchronous=FULL` and
`foreign_keys=ON`, and use a bounded busy timeout. It MUST hold an exclusive sidecar lock plus the
existing refresh/control lock. Only `universe_head` updates, and only by CAS on sequence and
contract hash; all other tables are append-only with update/delete rejection. The strict reader
checks descriptor-bound path/inode, store identity, schema/table allowlist, schema digest, all row
hashes, sequence-parent chain and head hash in one read transaction without DDL/DML. v1 writer
initialization is the only initialization path; there is no legacy migration in this release.
Crash before commit leaves the old head, rows committed before a crash remain unreachable, and a
CAS conflict returns unavailable without retry or head mutation.

The implementation MUST use this normative v1 DDL (the canonical whitespace-normalized text is
the schema-digest preimage):

```sql
PRAGMA user_version = 1;
CREATE TABLE universe_meta (meta_key TEXT PRIMARY KEY CHECK
 (meta_key IN ('schema_version','schema_digest','store_id','db_inode')), meta_value TEXT NOT NULL);
CREATE TABLE universe_contract (contract_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE
 CHECK (sequence > 0), parent_contract_id TEXT REFERENCES universe_contract(contract_id),
 trade_date TEXT NOT NULL, universe_id TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK
 (schema_version = 1), scope TEXT NOT NULL, calendar_generation_id TEXT NOT NULL,
 calendar_sha256 TEXT NOT NULL, classification_generation_id TEXT NOT NULL,
 exact_pit_cutoff TEXT NOT NULL, provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
 source_date_semantics TEXT NOT NULL, required_symbol_snapshot_id TEXT NOT NULL,
 required_symbol_snapshot_sha256 TEXT NOT NULL, instrument_evidence_ids_json TEXT NOT NULL,
 counts_json TEXT NOT NULL, layer_counts_json TEXT NOT NULL, source_refs_json TEXT NOT NULL,
 classification_evidence_ids_json TEXT NOT NULL,
 classification_evidence_partition_sha256 TEXT NOT NULL,
 effective_main_board_ids_json TEXT NOT NULL,
 effective_main_board_partition_sha256 TEXT NOT NULL,
 required_addition_ids_json TEXT NOT NULL,
 required_addition_partition_sha256 TEXT NOT NULL,
 payload_json TEXT NOT NULL, contract_sha256 TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL);
CREATE TABLE universe_member (contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id),
 symbol TEXT NOT NULL, member_sha256 TEXT NOT NULL, member_json TEXT NOT NULL,
 PRIMARY KEY (contract_id, symbol));
CREATE TABLE universe_instrument_evidence (evidence_id TEXT PRIMARY KEY,
 evidence_sha256 TEXT NOT NULL UNIQUE, provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
 authority_status TEXT NOT NULL CHECK (authority_status IN ('reviewed','unqualified')),
 artifact_origin TEXT NOT NULL CHECK (artifact_origin IN ('local_reviewed_fixture','production_reviewed_artifact')),
 security_id TEXT NOT NULL, symbol TEXT NOT NULL,
 mapping_id TEXT NOT NULL, mapping_version TEXT NOT NULL, source_snapshot_date TEXT NOT NULL,
 evidence_trade_date TEXT NOT NULL, exclusion_reason TEXT NOT NULL, index_role TEXT NOT NULL CHECK
 (index_role IN ('required_index','not_applicable')), source_date_semantics TEXT NOT NULL,
 evidence_json TEXT NOT NULL);
CREATE TABLE universe_required_symbol_snapshot (snapshot_id TEXT PRIMARY KEY,
 snapshot_sha256 TEXT NOT NULL UNIQUE, user_store_revision TEXT NOT NULL,
 snapshot_token_digest TEXT NOT NULL, symbol_count INTEGER NOT NULL CHECK (symbol_count >= 0),
 snapshot_json TEXT NOT NULL);
CREATE TABLE contract_evidence (contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id),
 evidence_id TEXT NOT NULL REFERENCES universe_instrument_evidence(evidence_id),
 evidence_role TEXT NOT NULL CHECK (evidence_role IN
 ('classification_evidence','effective_main_board','required_addition','member')),
 security_id TEXT NOT NULL, symbol TEXT NOT NULL, exclusion_reason TEXT NOT NULL,
 PRIMARY KEY (contract_id, evidence_id, evidence_role));
CREATE TABLE contract_required_snapshot (contract_id TEXT PRIMARY KEY
 REFERENCES universe_contract(contract_id), snapshot_id TEXT NOT NULL
 REFERENCES universe_required_symbol_snapshot(snapshot_id), snapshot_sha256 TEXT NOT NULL);
CREATE TABLE universe_attempt (attempt_id TEXT PRIMARY KEY, dedup_key TEXT NOT NULL UNIQUE,
 hook_kind TEXT NOT NULL CHECK (hook_kind = 'universe_post_success'), refresh_id TEXT NOT NULL,
 trade_date TEXT NOT NULL, operation_day TEXT NOT NULL,
 request_budget INTEGER NOT NULL CHECK (request_budget = 1),
 classification_max_attempts INTEGER NOT NULL CHECK (classification_max_attempts = 1),
 created_at TEXT NOT NULL, planned_sha256 TEXT NOT NULL UNIQUE);
CREATE TABLE universe_attempt_result (attempt_id TEXT PRIMARY KEY REFERENCES universe_attempt(attempt_id),
 terminal_status TEXT NOT NULL CHECK (terminal_status IN ('blocked','deferred','succeeded','failed')),
 classification_request_count INTEGER NOT NULL CHECK (classification_request_count IN (0,1)),
 reason_code TEXT NOT NULL, finished_at TEXT NOT NULL, result_sha256 TEXT NOT NULL UNIQUE);
CREATE TABLE universe_head (singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
 sequence INTEGER NOT NULL, contract_id TEXT NOT NULL, contract_sha256 TEXT NOT NULL,
 head_sha256 TEXT NOT NULL, updated_at TEXT NOT NULL,
 FOREIGN KEY (contract_id) REFERENCES universe_contract(contract_id));
CREATE TRIGGER universe_contract_no_update BEFORE UPDATE ON universe_contract BEGIN
 SELECT RAISE(ABORT, 'immutable_contract'); END;
CREATE TRIGGER universe_contract_no_delete BEFORE DELETE ON universe_contract BEGIN
 SELECT RAISE(ABORT, 'immutable_contract'); END;
CREATE TRIGGER universe_member_no_update BEFORE UPDATE ON universe_member BEGIN
 SELECT RAISE(ABORT, 'immutable_member'); END;
CREATE TRIGGER universe_member_no_delete BEFORE DELETE ON universe_member BEGIN
 SELECT RAISE(ABORT, 'immutable_member'); END;
CREATE TRIGGER universe_evidence_no_update BEFORE UPDATE ON universe_instrument_evidence BEGIN
 SELECT RAISE(ABORT, 'immutable_evidence'); END;
CREATE TRIGGER universe_evidence_no_delete BEFORE DELETE ON universe_instrument_evidence BEGIN
 SELECT RAISE(ABORT, 'immutable_evidence'); END;
CREATE TRIGGER universe_snapshot_no_update BEFORE UPDATE ON universe_required_symbol_snapshot BEGIN
 SELECT RAISE(ABORT, 'immutable_snapshot'); END;
CREATE TRIGGER universe_snapshot_no_delete BEFORE DELETE ON universe_required_symbol_snapshot BEGIN
 SELECT RAISE(ABORT, 'immutable_snapshot'); END;
CREATE TRIGGER universe_meta_no_update BEFORE UPDATE ON universe_meta BEGIN
 SELECT RAISE(ABORT, 'immutable_meta'); END;
CREATE TRIGGER universe_meta_no_delete BEFORE DELETE ON universe_meta BEGIN
 SELECT RAISE(ABORT, 'immutable_meta'); END;
CREATE TRIGGER contract_evidence_no_update BEFORE UPDATE ON contract_evidence BEGIN
 SELECT RAISE(ABORT, 'immutable_contract_evidence'); END;
CREATE TRIGGER contract_evidence_no_delete BEFORE DELETE ON contract_evidence BEGIN
 SELECT RAISE(ABORT, 'immutable_contract_evidence'); END;
CREATE TRIGGER contract_snapshot_no_update BEFORE UPDATE ON contract_required_snapshot BEGIN
 SELECT RAISE(ABORT, 'immutable_contract_snapshot'); END;
CREATE TRIGGER contract_snapshot_no_delete BEFORE DELETE ON contract_required_snapshot BEGIN
 SELECT RAISE(ABORT, 'immutable_contract_snapshot'); END;
CREATE TRIGGER universe_attempt_no_update BEFORE UPDATE ON universe_attempt BEGIN
 SELECT RAISE(ABORT, 'immutable_attempt'); END;
CREATE TRIGGER universe_attempt_no_delete BEFORE DELETE ON universe_attempt BEGIN
 SELECT RAISE(ABORT, 'immutable_attempt'); END;
CREATE TRIGGER universe_attempt_result_no_update BEFORE UPDATE ON universe_attempt_result BEGIN
 SELECT RAISE(ABORT, 'immutable_attempt_result'); END;
CREATE TRIGGER universe_attempt_result_no_delete BEFORE DELETE ON universe_attempt_result BEGIN
 SELECT RAISE(ABORT, 'immutable_attempt_result'); END;
```

The writer MUST reject any pre-existing non-v1 database; no legacy migration is implemented.
An empty initialized store has no `universe_head` row; first promotion inserts sequence 1 with a
null parent. `payload_json` is the complete canonical UniverseContractV1 preimage, including
sorted members, required indexes, evidence IDs, snapshot hash, all counts/layer counts, source
refs, PIT fields and all three partition IDs/hashes. Its digest and all explicit identity/source/
count/partition columns must match the decoded payload. The strict reader reconstructs the
contract from payload, re-hashes every member row and every persisted excluded evidence row,
checks exact role-bearing bidirectional `contract_evidence` and `contract_required_snapshot` links,
and rejects every orphan. Explicit provider, source-date, required-snapshot and evidence-ID
columns must equal `SourceRefsV1` and their payload counterparts. It verifies
`head.sequence == contract.sequence`, `sequence=1` parent null, later sequence = parent+1, and
`head_sha256` over canonical `{"singleton_id":1,"sequence":N,"contract_id":"...",
"contract_sha256":"..."}`. CAS is conditional on the expected sequence/hash, with insert only
on empty head. DDL normalization is the ordered statement text from the DDL block (comments
removed, whitespace collapsed outside quoted literals, whitespace removed immediately inside/around
`(`, `)` and `,`, quoted content preserved); the exact digest
is `domain_sha256("stock-eva/r2f4.2/universe-schema/v1", normalized_ddl)` and is stored as
`schema_digest`.
The strict reader verifies path/inode, store identity, schema/table allowlist, WAL/full-sync mode,
all payload/member/link/evidence hashes and parent chain in one read-only transaction. It never
creates a directory, DDL, DML, WAL/SHM file or migration. A crash leaves old head or an unreferenced
candidate; corruption is `unavailable`, never a historical cached projection. It also validates
one immutable `universe_attempt` plan per dedup key, at most one terminal result, and actual
classification requests not exceeding one; missing or duplicate attempt state is unavailable.
An initialized empty store with no head row is also `unavailable`; no historical cached status is
retained.

### Configuration additions

Additive settings are required for the isolated sidecar and mode:

| Setting | Default | Constraint |
|---|---|---|
| `universe_contract_database_name` | `market_universe.sqlite3` | local `.sqlite3` basename, distinct from all control DBs |
| `market_universe_mode` | `off` | closed enum `off`, `shadow`, `enforce`; production profile accepts only `off` |
| `market_universe_maintenance_enabled` | `false` | strict boolean, no effect on legacy canonical path when false |
| `market_universe_maintenance_interval_seconds` | `86400` | positive bounded interval; no retry-loop setting |

Adding the database name to `Settings` and `StorageLayout` is an implementation necessity even
though the older Task 16 file list omitted those two files; it keeps path validation and private
control layout centralized.

## Implementation Sequence

### Step 0 — Freeze and RED inventory

1. Confirm the worktree is at the stated baseline and clean before implementation changes.
2. Read and record SHA-256 for R2-F2 golden objects, the protected R2-F3 files, R2-F4.0
   `failover.py`/vectors and R2-F4.1 calendar/runtime files.
3. Add no code until the companion design has passed independent SPEC review. Generate spec test
   extractor output only into an ignored scratch directory; do not commit `NotImplementedError`
   stubs.
4. Create `tests/test_market_universe.py` with RED cases for AC-1 through AC-16 and all ECs. Use
   network-failing fakes and temporary roots; align failover compatibility coverage with the
   umbrella target `tests/test_market_failover.py` (the baseline readiness file is retained only
   as a compatibility fixture name until implementation updates the target).

### Step 1 — Strict models and pure derivation

1. Implement strict models, safe symbol normalization and domain-separated canonical hashes in
   `backend/app/market/universe.py`.
2. Implement explicit board identity using the existing `derive_security_identity` rules; do not
   duplicate a weaker prefix classifier.
3. Implement listing, delisting, suspension, ST and unknown state derivation with a versioned
   source-semantic mapping.
4. Implement count equation, critical-attribute count, expected candidate set and complete
   candidate rejection as pure functions.
5. Re-run only the new model/derivation tests; no provider construction is allowed.

### Step 2 — Sidecar store and read-only reader

1. Add `universe_contract_database_name` to `backend/app/config.py` and its distinct-name checks;
   expose the path through `backend/app/storage/layout.py`.
2. Implement writer-only sidecar initialization and immutable schema in `universe.py`, copying
   the proven path, mode, inode, lock, hash and atomic-head discipline from R2-F4.1.
3. Implement one verified snapshot read and unavailable mapping for missing/corrupt/locked/tampered
   state. GET readers MUST avoid schema initialization.
4. Add RED/GREEN tests for interrupted commit, head CAS, duplicate rows, digest mismatch,
   sidecar replacement, empty-head behavior, bidirectional/orphan links, safe-mode checks and
   restart behavior.

### Step 3 — PIT builder and evidence binding

1. Add a writer-only builder that pins one calendar snapshot and one classification snapshot at the
   operation boundary.
2. Add the exact writer-only `capture_required_symbol_snapshot_once(operation_id)` transaction
   using only the allowlisted UserStore position/watchlist methods. Verify revision/token before
   commit, record roles/count/hash privately, and do not put names in public output.
3. Bind promoted classification generation, instrument evidence IDs, source-date semantics and
   R2-F4.1 calendar generation/hash. Reject future/degraded/requested-unverified critical evidence.
4. Build stock and index members, calculate states/counts and promote only with unknown-zero and
   all evidence checks.
5. Add tests for PIT boundaries, listing/delisting, ST/suspension, index evidence, dynamic counts,
   unsupported required symbols, classification generation drift, single-connection capture,
   busy/locked rollback and mutation-token abort.

### Step 4 — Candidate validation and staged legacy seam

1. Implement the pure `validate_provider_raw_batch(batch: ProviderRawBatch,
   contract: UniverseContractV1) -> UniverseCandidateGate`, aggregating existing endpoint batches,
   completions, lineage and pages; bind BaoStock, request trade date and refresh/session/request
   identity before Normalize.
2. Preserve existing `ProviderRequest`, `DailyBar`, `RefreshResult`, Normalize, quality-gate,
   evidence, manifest and pointer behavior in mode `off`; `off` MUST NOT invoke the validator.
3. In `shadow`, use a read-only observer after successful canonical/evidence publish; compare only
   legacy BaoStock counts/hash/reason and record drift without blocking or mutating canonical.
   Missing required ChiNext/STAR in the legacy request is drift only; do not change
   `build_canonical_raw_request`.
4. Make `enforce` execution an unconditional `BLOCKED_ENFORCE_NOT_ENABLED` result (zero provider
   requests). The callback is an offline/future enforce seam only; no production path may route
   through it and no secondary provider is accepted.
5. Add tests proving missing/extra/duplicate/non-session/mixed-provider batches, invalid
   `tradestatus`/suspended placeholders and session drift reject as a whole before Normalize, and
   that no symbol-level source stitching is possible.

### Step 5 — Automation maintenance priority

1. Document/test the existing priority decision in `backend/app/market/automation.py` as
   continuity/repair before freshness execution, then post-success shadow; Universe is not a new
   scheduler lane and is offered only by the bounded hook after that sequence.
2. Preserve the real boundaries: `_plan_continuity` and its repair lease run before the existing
   refresh lease; `_execute_due` owns only the existing refresh lease; post-publish and shadow hooks
   run after that lease. A continuity plan failure blocks the canonical path and no Universe hook.
3. Offer one read-only legacy BaoStock shadow comparison only after a canonical refresh succeeds;
   it is never a retry and never runs alongside Universe construction. Reuse existing refresh,
   shadow and evidence identities for deduplication; do not invent an all-lane request ledger.
4. Only after existing post-publish and shadow hooks return may the bounded Universe hook read
   promoted evidence, capture the UserStore snapshot once, and attempt one sidecar promotion.
   Current BaoStock `requested_unverified` evidence blocks before provider acquisition.
5. Implement typed `UniversePostSuccessHook.offer(canonical_result, refresh_id, trade_date,
   manifest_ref, evidence_ref, now)`. It must reacquire the existing `RefreshRunLock` non-blocking,
   then the sidecar lock; failure returns `DEFER/CONTROL_STATE_UNAVAILABLE`. Under the sidecar
   lock insert the immutable `universe_attempt` plan before any classification request. Use
   `dedup_key=sha256(canonical {hook_kind, refresh_id, trade_date, operation_day})`,
   `request_budget=1`, `classification_max_attempts=1`, and one immutable terminal result. A
   duplicate key is no-op/defer, not a resend; unqualified evidence records stable blocked with
   zero requests. Do not add a sixth LaunchAgent, independent scheduler lane or additional scheduler lock. Add concurrency,
   restart, deferred, priority, post-success ordering, failure and no-repeat tests covering
   `classification/sync.py`, `classification/store.py`, `backend/app/market/automation.py` and
   `tests/test_market_failover.py`.

### Step 6 — Read-only API and CLI

1. Add additive status model fields to `backend/app/market/models.py` and a read-only dependency in
   `backend/app/api/market.py`.
2. Add `GET /api/v1/market/universe`; validate date before store access, map failures to the
   allowlist and hide paths/errors/symbols.
3. Add `market-universe` to `backend/app/cli.py` with no `--execute` option and zero-write
   semantics. It reads no credentials and constructs no provider.
4. Add route/CLI filesystem fingerprint tests for missing, valid, stale and corrupt states.

### Step 7 — Compatibility documentation and migration rehearsal

1. Update `docs/classification.md`, `docs/market-data.md`, `docs/data-providers.md` and the
   R2-F4.2 acceptance draft with the sidecar, PIT, state vocabulary, count equation and mode
   boundaries.
2. Rehearse a synthetic staged sidecar population from fake promoted classification and
   instrument evidence. Confirm legacy canonical files and pointer remain unchanged.
3. Verify the migration is additive: no historical partition is relabeled, no old date is used as
   a new date, no existing provider is replaced, and no public GET creates control state.

### Step 8 — Verification and serial independent review

Run the RED/GREEN and repository gates in a private temporary-root configuration:

```bash
.venv/bin/pytest -q tests/test_market_universe.py \
  tests/test_point_in_time_classification.py \
  tests/test_market_automation.py \
  tests/test_market_failover.py \
  tests/test_market_data.py \
  --basetemp=/tmp/stock-eva-r2f4-2-focused
.venv/bin/pytest -q --basetemp=/tmp/stock-eva-r2f4-2-full
.venv/bin/ruff check backend tests
.venv/bin/ruff format --check backend tests
.venv/bin/python -m compileall -q backend
git diff --check
uv run --offline python \
  /Users/finlay/.codex/skills/claude-skills--engineering--spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-09-08-stock-eva-r2f4-2-exact-session-universe-design.md --strict
uv run --offline python \
  /Users/finlay/.codex/skills/claude-skills--engineering--spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-09-08-stock-eva-r2f4-2-exact-session-universe-implementation.md --strict
```

From `tests/fixtures/r2f2_golden`, rerun the existing golden SHA-256 verification. Compare
protected file hashes to the baseline. Inspect that every new test uses only temporary roots and
that network-failing fakes recorded zero unexpected calls.

Commit boundaries are:

1. design and implementation plan only;
2. RED tests and pure model/store implementation;
3. PIT builder and staged integration;
4. API/CLI/automation/docs;
5. final acceptance evidence only after the exact-head reviews.

At every boundary, an H/M finding blocks the next boundary. A fresh independent SPEC review must
inspect the exact candidate commit first, followed by a separate QUALITY review of the same exact
head. No self-review or green test count can replace either review.

## Closed reasons and FR/AC/EC traceability

The implementation MUST use only the following public/sanitized reason set:
`CONTROL_STATE_UNAVAILABLE`, `PIT_VISIBILITY_INVALID`, `CALENDAR_UNAVAILABLE`, `CALENDAR_CONFLICT`,
`CLASSIFICATION_UNAVAILABLE`, `USER_STORE_UNAVAILABLE`,
`BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`, `PIT_CUTOFF_VIOLATION`, `USER_SNAPSHOT_CHANGED`,
`REQUIRED_INDEX_NOT_TRADING`, `REQUIRED_SYMBOL_INVALID`, `UNIVERSE_UNKNOWN_NONZERO`, `UNIVERSE_COUNT_MISMATCH`,
`UNIVERSE_MISSING_SYMBOL`, `UNIVERSE_EXTRA_SYMBOL`, `UNIVERSE_DUPLICATE_SYMBOL`,
`UNIVERSE_SESSION_DRIFT`, `UNIVERSE_STATE_MISMATCH`, `UNIVERSE_DATE_MISMATCH`, `UNIVERSE_STORAGE_UNAVAILABLE`, `UNIVERSE_SCHEMA_MISMATCH`,
`UNIVERSE_HEAD_CAS_CONFLICT`, `BLOCKED_ENFORCE_NOT_ENABLED`, `BLOCKED_PRODUCTION_MODE_OFF`,
`LEGACY_SHADOW_DRIFT`, `NONE`.
Unmapped provider/exception text MUST map to a closed boundary reason and MUST NOT be surfaced.

The implementation MUST use this deterministic mapping (the design table is normative and must
remain identical):

| Internal condition | Sanitized reason | HTTP / CLI projection |
|---|---|---|
| invalid date before any read | `PIT_VISIBILITY_INVALID` | 422 / 2 |
| missing calendar control | `CALENDAR_UNAVAILABLE` | 200 blocked / 1 |
| conflicting calendar control | `CALENDAR_CONFLICT` | 200 blocked / 1 |
| calendar read is safe but unavailable | `CONTROL_STATE_UNAVAILABLE` | 200 blocked / 1 |
| sidecar/path storage cannot be proven | `UNIVERSE_STORAGE_UNAVAILABLE` | 503 fixed body / 3 |
| source date/observation beyond `exact_pit_cutoff` | `PIT_CUTOFF_VIOLATION` | 200 blocked / 1 |
| requested-unverified or unreviewed classification/instrument evidence | `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED` | 200 blocked / 1 |
| no promoted classification visible at PIT | `CLASSIFICATION_UNAVAILABLE` | 200 blocked / 1 |
| required index absent, identity/date mismatch, suspended or unknown | `REQUIRED_INDEX_NOT_TRADING` | 200 blocked / 1 |
| malformed/unsupported required user symbol | `REQUIRED_SYMBOL_INVALID` | 200 blocked / 1 |
| user snapshot mutation during capture | `USER_SNAPSHOT_CHANGED` | 200 blocked / 1 |
| user store busy/locked/unreadable | `USER_STORE_UNAVAILABLE` | 200 blocked / 1 |
| nonzero unknown member/state attribute | `UNIVERSE_UNKNOWN_NONZERO` | 200 blocked / 1 |
| loaded count mismatch | `UNIVERSE_COUNT_MISMATCH` | 200 blocked / 1 |
| missing expected row | `UNIVERSE_MISSING_SYMBOL` | 200 blocked / 1 |
| extra expected row | `UNIVERSE_EXTRA_SYMBOL` | 200 blocked / 1 |
| duplicate expected row | `UNIVERSE_DUPLICATE_SYMBOL` | 200 blocked / 1 |
| endpoint/page/session/request/date drift | `UNIVERSE_SESSION_DRIFT` | 200 blocked / 1 |
| DAILY_ASTOCK `tradestatus` or suspended placeholder disagrees with contract | `UNIVERSE_STATE_MISMATCH` | 200 blocked / 1 |
| valid head trade date differs from requested date | `UNIVERSE_DATE_MISMATCH` | 200 stale / 1 |
| sidecar path/inode/schema/hash/WAL/lock cannot be proven | `UNIVERSE_STORAGE_UNAVAILABLE` | 503 fixed body / 3 |
| sidecar payload, partition, link, parent, head or attempt validation fails | `UNIVERSE_SCHEMA_MISMATCH` | 503 fixed body / 3 |
| nonblocking refresh/sidecar lock unavailable | `CONTROL_STATE_UNAVAILABLE` | 200 blocked/defer / 1 |
| sidecar head CAS conflict | `UNIVERSE_HEAD_CAS_CONFLICT` | 200 blocked/defer / 1 |
| mode `enforce` in any execution | `BLOCKED_ENFORCE_NOT_ENABLED` | 200 blocked / 1 |
| non-off mode in production profile | `BLOCKED_PRODUCTION_MODE_OFF` | 200 blocked / 1 |
| BaoStock transport/protocol failure (`CONNECT_ERROR`, `SEND_ERROR`, `RECV_TIMEOUT`, `EOF`, `SHORT_HEADER`, `BAD_COMPRESSION`, `PROTOCOL_ERROR`, `PAGINATION_STALLED`, `RATE_LIMIT`, or unknown provider code) during acquisition | `CLASSIFICATION_UNAVAILABLE` | 200 blocked / 1 |
| read-only legacy shadow differs, including missing ChiNext/STAR from legacy request | `LEGACY_SHADOW_DRIFT` | 200 blocked/diagnostic / 1 |

Raw BaoStock `provider_code` is retained only in private evidence/diagnostics; semantic meaning is
never guessed or exposed. `USER_STORE_UNAVAILABLE` is used for writer-store read failures;
`CALENDAR_UNAVAILABLE`/`CALENDAR_CONFLICT` are precise internal variants of control failure.

| FRs | ACs | ECs | Offline test/static trace |
|---|---|---|---|
| FR-1..FR-3 | AC-1, AC-2 | EC-1..EC-4 | `tests/test_market_universe.py::test_contract_identity_and_pit`; calendar/classification protected SHA check |
| FR-4..FR-6, FR-13, FR-14 | AC-3..AC-5 | EC-2, EC-4, EC-6..EC-8, EC-11 | `test_three_layers_and_effective_window`, `test_reviewed_mapping_blocks_requested_unverified`, `test_user_snapshot_single_connection_busy_rollback` |
| FR-7, FR-14 | AC-4, AC-6 | EC-9 | `test_required_indexes_must_be_explicit_trading`, `test_unknown_one_loaded_match_rejects` |
| FR-8..FR-12, FR-15 | AC-5..AC-7, AC-16 | EC-5, EC-10, EC-11, EC-17 | `test_raw_batch_gate_rejects_missing_extra_duplicate_drift_before_normalize` |
| FR-16 | AC-8, AC-8a, AC-15 | EC-12, EC-13, EC-18 | `test_sidecar_schema_digest_inode_wal_cas_links_attempts_and_crash`; normative DDL/static digest |
| FR-9, FR-17 | AC-10, AC-15 | EC-14..EC-16 | `tests/test_market_automation.py::test_decision_matrix_and_post_success_order` |
| FR-10, FR-12 | AC-9, AC-11, AC-14 | EC-1, EC-13 | API/CLI zero-write filesystem fingerprint and public-schema static scan |

The evidence record MUST list every individual FR-1..FR-17 and EC-1..EC-18, not merely the ranges
shown above, with its exact test/static line and result. It MUST include both strict validator
commands (design and implementation), `git diff --check`, and the exact reviewed commit. Status
remains `In Review`; no implementation or specification check may label this plan SPEC GO.

## Rollback and Recovery

- The feature flag returns the running system to legacy `off` behavior without data migration.
- A failed or corrupt sidecar is quarantined by the existing strict control-store recovery policy;
  operators preserve the DB, evidence and audit artifacts for review and do not hand-edit SQLite,
  Parquet, manifests or pointers.
- A failed classification refresh preserves the prior promoted classification generation and
  Universe head. A failed shadow comparison changes no canonical state.
- If an implementation change would require changing a frozen predecessor contract, stop, record
  the exact incompatibility and produce a new breaking-contract review before editing.

## Out of Scope

- OS-1: Enabling `enforce` in production, changing R2-F4.0 effective failover, publishing a
  secondary provider or performing automatic whole-session failover.
- OS-2: Real BaoStock/TickFlow/Tushare/AKShare acquisition, credentials, terms/quota review,
  production calendar/classification maintenance, NAS, deployment or LaunchAgent execution.
- OS-3: Rewriting canonical `DailyBar`, R2-F2 evidence, R2-F3 qualification, R2-F4.0
  readiness, R2-F4.1 runtime calendar, Normalize, Quality Gate, Parquet, manifest or pointer.
- OS-4: Fixed 3,195/5,205 gates, symbol-level mixing, old-date carry-forward, manual file
  edits, hidden extras or a public full member list.
- OS-5: New taxonomies, CNINFO events, component-history qualification, intraday/minute/tick
  data, brokerage access, trading or investment recommendations.
- OS-6: Claiming R2-F4.2 GO, R2-F4 GO, R2-F5 soak completion, secondary qualification or
  Release 2 GO from offline implementation evidence.
