# Stock EVA R2-F4.2 Exact-session Universe Contract Design

**Author:** Codex R2-F delivery quality lead

**Date:** 2026-09-08 (Asia/Shanghai)

**Status:** Draft / In Review / implementation blocked until this contract is independently reviewed

**Reviewers:** Independent SPEC reviewer and independent QUALITY reviewer (not yet assigned)

**Related contracts:**

- `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md`, Task 16
- `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-implementation.md`, Task 16
- `docs/plans/2026-08-30-stock-eva-r2f4-0-capability-gated-selection-design.md`
- `docs/plans/2026-08-31-stock-eva-r2f4-1-promoted-calendar-design.md`
- `docs/plans/2026-08-31-stock-eva-r2f4-1-promoted-calendar-implementation.md`

**Baseline:** R2-F4.1 reviewed delivery `2030ba7fc155dfc7a3615bbfe7570a787755b98b`.

**Decision boundary:** This is a contract and implementation design only. No code, provider
request, production control/canonical write, NAS access, deployment, LaunchAgent invocation or
real-data claim is authorized by this document.

## Context

The current classification plane already stores promoted, point-in-time security-master,
index-component and sector generations. It selects a generation visible at an `as_of` date and
keeps listing windows, daily trade status, lineage and source-date semantics. The current market
refresh path, however, asks BaoStock `query_all_stock(day=...)` to define the daily expected set
inside the refresh itself. User positions and all watchlist items are collected in the writer
automation, while public GET paths do not read the private user database. These are useful inputs,
but they are not yet one immutable contract for a particular trading session.

Task 16 requires `all-main-board-plus-required-symbols`, required indexes, listing/delisting,
ST/suspension and unknown handling to be explicit. A count such as 3,195 canonical rows or about
5,205 classification-eligible rows is an observation for one source/scope at one time, not a
stable gate. A fixed count would reject valid listing changes, hide missing symbols, and confuse
the main-board market scope with the broader classification scope.

R2-F4.0 deliberately leaves exact-session universe, calendar authority and canonical failover
unqualified. R2-F4.1 now supplies a promoted calendar reader, but its generation is separate from
classification and must be pinned once per external operation. The new contract must bind both
authorities and the instrument evidence used to derive the session state without modifying the
existing `Normalize -> Quality Gate -> Immutable Parquet -> SHA-256 -> Manifest -> Atomic Publish`
chain.

The architectural decision is a separate, append-only `market_universe.sqlite3` sidecar and head.
It avoids widening the classification database's writer authority, lets a corrupt or missing
universe state degrade independently, and permits a staged comparison with the legacy BaoStock
path. A new contract can be built and reviewed while the legacy path remains the only canonical
publisher; no direct production switch is part of R2-F4.2.

## Functional Requirements

- FR-1: Versioned scope. The system MUST identify every contract as
  `universe_id=all-main-board-plus-required-symbols`, `schema_version=1`, a single
  `trade_date`, and a content hash. It MUST NOT use a fixed expected row count as the identity or
  publication gate.

- FR-2: Exact-session identity. A contract MUST represent one complete session and MUST bind
  `trade_date`, the promoted calendar generation/hash, the promoted classification generation,
  the required-symbol snapshot and every instrument-evidence identity. A range or a provider's
  unbounded universe MUST NOT substitute for the exact session.

- FR-3: Promoted calendar PIT gate. A contract builder MUST select one verified R2-F4.1
  calendar snapshot for the operation. The date MUST be a confirmed open session in that snapshot;
  weekend, holiday, unknown year, conflict, missing generation or calendar identity drift MUST
  make the contract unavailable or blocked.

- FR-4: Promoted classification PIT gate. The builder MUST select only a `promoted=true`
  classification generation visible at `trade_date`, with `source_snapshot_date <= trade_date`
  and `observed_at` no later than the declared PIT cutoff. It MUST preserve the selected
  `classification_generation_id` and MUST NOT silently select a future or merely inserted,
  degraded generation.

- FR-5: Main-board base scope. Base members MUST be derived from the selected security master
  using explicit identity rules: ordinary stock, SSE/SZSE, board `main`, and an effective listing
  window containing `trade_date`. ChiNext, STAR, BSE, B shares, funds, ETFs, bonds, indexes and
  other types MUST NOT enter the base set.

- FR-6: Required user symbols. The writer pipeline MUST capture the union of symbols from the
  existing allowlisted `UserStore.list_positions()` and every
  `list_watchlist_items(watchlist.id)` call exactly once per operation. It MUST normalize and
  deduplicate the union, persist only a bounded snapshot hash/count plus the contract's member
  roles, and MUST NOT read the private user database from a public GET. A valid A-share user symbol
  outside main board (for example, an explicitly required ChiNext or STAR stock) MUST remain an
  explicit member rather than being dropped.

- FR-7: Required indexes. The contract MUST include exactly the current required market index
  obligations `sh.000001` and `sz.399001` under `required_indexes`, with explicit instrument
  evidence and expected session state. User inclusion of either symbol MUST deduplicate against
  this obligation. No index may be inferred from a stock count.

- FR-8: Instrument evidence binding. Every member MUST bind reviewed instrument evidence that
  proves symbol, security type, exchange, board, listing window and source/date semantics. The
  evidence identity MUST be retained as an ID/hash; raw provider payloads, credentials and URLs
  MUST NOT be copied into public status or logs.

- FR-9: Listing and delisting states. For each member, the builder MUST derive
  `not_yet_listed` when `trade_date < list_date` and `delisted` when `trade_date >= delist_date`.
  Missing, contradictory, malformed or out-of-envelope listing windows MUST derive `unknown` and
  MUST block promotion. The new contract vocabulary `not_yet_listed` MUST NOT silently rename the
  existing classification API exclusion reason `not_listed_yet`; a mapping is required at the
  contract boundary.

- FR-10: Suspension state. A member whose reviewed source explicitly says suspended for the
  session MUST be `suspended`; a reviewed explicit trading state MUST be `trading`. The builder
  MUST NOT infer suspension from zero volume, blank activity, missing bars or an unverified source
  code. Missing or conflicting suspension semantics MUST be `unknown`.

- FR-11: ST state. The contract MUST retain `st_state` as `yes`, `no`, `not_applicable` or
  `unknown`. ST MUST NOT remove a valid main-board member from scope. `unknown` MUST remain visible
  and MUST NOT be coerced to `no`; a publishable candidate MUST have no unknown critical
  instrument attribute, including ST, for members expected in the session.

- FR-12: Closed expected-state vocabulary. Every member MUST have exactly one
  `expected_trading_state` in `trading`, `suspended`, `not_yet_listed`, `delisted` or `unknown`.
  Any unknown or unrecognized value MUST fail closed; it MUST NOT be treated as trading, absent,
  or a zero-row success.

- FR-13: Deterministic count equation. The contract MUST satisfy
  `total = trading + suspended + not_yet_listed + delisted + unknown` over all stock and index
  members, with `total` equal to the number of unique members. It MUST expose
  `critical_attribute_unknown_count` separately. `session_expected = trading + suspended` is the
  exact number of rows a candidate must contain. The equation MUST be calculated from the
  contract, never from a hard-coded market count.

- FR-14: Unknown-zero publication gate. A contract MAY be promoted only when
  `unknown == 0`, `critical_attribute_unknown_count == 0`, all required indexes are resolved,
  and all source/date/hash checks pass. A candidate MAY publish only against a promoted contract.
  A nonzero unknown state MUST preserve the previous canonical pointer.

- FR-15: Exact candidate set. For a candidate bound to a promoted contract, the expected row
  set MUST be exactly the unique symbols whose state is `trading` or `suspended`, including all
  required indexes. Rows for `not_yet_listed` or `delisted` members are not expected. A provider
  response missing any expected symbol, or returning a row for any symbol outside the contract,
  MUST invalidate the entire candidate.

- FR-16: Extras and duplicates are errors. Provider rows outside the contract are `universe
  extras`, not ignorable data. Duplicate symbol rows, duplicate session points, mismatched board or
  index identity, and an extra provider row MUST be rejected before normalization or publication.

- FR-17: Whole-session purity. A candidate MUST have one provider ID, one adapter/contract
  version and one exact-session contract for the complete set. Symbol-level mixing, fallback rows,
  averaging, carry-forward from an older date and silent provider substitution MUST NOT be allowed.

- FR-18: Immutable sidecar authority. The sidecar MUST use writer-only initialization and
  append-only contract/member/evidence rows, a singleton head, strict path/inode checks and
  atomic head publication. Existing promoted contracts MUST remain readable and immutable.
  Missing, corrupt, locked, path-substituted or hash-inconsistent sidecar state MUST be unavailable,
  not repaired by a GET.

- FR-19: Canonical identity. Contract and member hashes MUST use a documented canonical JSON
  encoding with sorted keys, no omitted null/false/zero/empty fields, a domain-separated SHA-256,
  and explicit digest-field exclusion. A changed date, generation, evidence, required-symbol
  snapshot, state or member set MUST change the contract identity.

- FR-20: Read-only status. `GET /api/v1/market/universe` and the `market-universe` CLI MUST
  return a bounded status for one requested date, perform zero provider requests, perform zero
  database/Parquet/manifest/pointer/user-store writes, and never initialize or migrate the sidecar.
  They MUST expose `provider_requests=0` and `writes=false`.

- FR-21: Sanitized diagnostics. Public status, CLI output and operational logs MUST expose only
  allowlisted reason codes, counts, safe IDs and hashes. They MUST NOT expose payload rows, user
  symbol lists, local paths, SQL, URLs, tokens, raw provider exceptions or stack traces.

- FR-22: Maintenance priority. Universe/classification maintenance MUST use the existing
  one-shot automation orchestration and no sixth LaunchAgent. Its deterministic priority MUST be
  below due freshness, due repair and due shadow work. At most one bounded maintenance operation
  may be selected per scheduler tick; a maintenance failure MUST not erase the previous head or
  cause an unbounded retry loop.

- FR-23: Classification maintenance trigger. The writer MAY schedule a new classification
  generation on its declared cadence or when reviewed instrument evidence reports a version
  change. It MUST publish through the existing classification store contract, preserve previous
  promoted generations, and MUST build/promote a new universe contract only after all PIT and
  unknown-zero checks pass.

- FR-24: Legacy compatibility modes. The legacy BaoStock `all-main-board` canonical path MUST
  remain operational and unchanged in R2-F4.2. A contract integration MUST provide explicit
  `off`, `shadow` and `enforce` modes, default to `off`, and never enable `enforce` in production
  or claim canonical authority in this subversion. `shadow` MAY compare exact sets and report
  drift, but MUST NOT change the canonical result.

- FR-25: No authority widening. R2-F4.2 MUST NOT add a secondary provider to canonical
  `ProviderId`, change R2-F4.0 failover readiness, enable TickFlow/Tushare failover, change R2-F3
  qualification, modify the promoted calendar semantics, or alter canonical normalization,
  quality gates, Parquet, manifests or pointers.

- FR-26: Safe migration path. The new sidecar MUST be populated from a staged writer-only
  flow and compared against the legacy BaoStock observed set before any future enforce decision.
  No migration may reinterpret an old canonical partition, use an old date as a new date, or
  switch production by merely finding a sidecar file.

- FR-27: Frozen predecessor contracts. R2-F2 golden evidence, R2-F3 Daily Bar shadow
  qualification, R2-F4.0 capability shield and R2-F4.1 promoted calendar/runtime contracts MUST
  remain byte/hash/behavior compatible at their protected surfaces. Any required breaking change
  MUST be a separately reviewed contract, not an incidental Task 16 edit.

## Non-Functional Requirements

- **NFR-1 — Offline delivery boundary:** All R2-F4.2 implementation, tests and acceptance
  evidence MUST use injected fakes or local fixtures. The delivery MUST contain zero real provider
  requests, credential reads, production state access, NAS access, deployment or LaunchAgent run.

- **NFR-2 — Fail-closed default:** Missing, stale, ambiguous, unknown, malformed, tampered or
  locked universe/classification/calendar state MUST result in `unavailable` or `blocked`; no
  weekday, count, status, factor, suspension or symbol set may be guessed.

- **NFR-3 — Read isolation:** The read-only API/CLI MUST leave a fingerprinted temporary control,
  market, user, staging and data tree byte-for-byte unchanged, including when the sidecar is
  missing or corrupt.

- **NFR-4 — Atomicity and restart safety:** A process interruption at every sidecar write boundary
  MUST leave either the prior valid head or a fully verifiable new head. A reader MUST never accept
  a partially inserted contract as promoted authority.

- **NFR-5 — PIT integrity:** No record with source snapshot, observation or effective semantics
  after the requested session may enter a contract. A contract must identify its PIT cutoff and
  source-date semantics.

- **NFR-6 — Bounded public surface:** Status responses MUST contain at most 32 reason codes and
  no member payloads or user symbol values. All IDs and hashes MUST match their safe-format
  validators; diagnostics MUST be allowlisted.

- **NFR-7 — Bounded work:** A status read MUST use one sidecar snapshot and no provider call. A
  maintenance tick MUST perform at most one classification acquisition and one contract promotion
  attempt, and MUST respect the existing refresh lock and provider request budgets.

- **NFR-8 — Determinism:** Rebuilding the same frozen inputs MUST produce byte-identical contract,
  member-count and hash outputs regardless of Python set/dictionary iteration order.

- **NFR-9 — Privacy:** Required user symbols MAY exist only in private writer-owned evidence and
  immutable local member rows. Public status/logging MUST expose count/hash/role aggregates only;
  no positions, watchlist names or user-store paths may leak.

- **NFR-10 — Compatibility:** Existing market/classification/failover/calendar APIs retain their
  response meanings and defaults. New fields are additive, and legacy BaoStock readers remain
  usable when no UniverseContract is available.

- **NFR-11 — Test completeness:** Every FR and EC MUST map to an automated offline regression or a
  documented static check. The focused suite, full repository suite, Ruff, format, compile and
  diff checks MUST pass before independent review.

- **NFR-12 — No false qualification:** A green status or a complete-looking count MUST NOT imply
  provider qualification, secondary failover readiness, production enablement or Release 2 GO.

## Acceptance Criteria

### AC-1: Dynamic scope and identity (FR-1, FR-2, FR-13, FR-19)

**Given** two frozen security snapshots with different valid member sets and no fixed expected
count, **When** contracts are built for the same session, **Then** each contract's total and hash
are derived from its unique members and the hashes differ when the member set differs.

### AC-2: Confirmed calendar PIT (FR-2, FR-3, NFR-2, NFR-5)

**Given** a confirmed promoted calendar and a requested open session, **When** the builder pins one
calendar snapshot, **Then** the contract binds its calendar generation/hash and accepts the date;
**When** the date is weekend, holiday, unknown-year, conflicting or future, **Then** no contract is
promoted and the result is `CALENDAR_UNAVAILABLE` or `CALENDAR_CONFLICT` with zero provider calls.

### AC-3: Promoted classification visibility (FR-4, FR-27, NFR-5)

**Given** a promoted generation visible at `trade_date`, a newer future generation and a degraded
candidate, **When** the builder reads PIT classification, **Then** it binds only the visible
promoted generation; a future/degraded-only store returns unavailable and never leaks its rows.

### AC-4: Main-board scope (FR-5, FR-27)

**Given** SSE/SZSE main-board stocks, ChiNext, STAR, BSE, B shares, funds, ETFs, bonds and index
records, **When** the base set is derived, **Then** only effective SSE/SZSE ordinary main-board
stocks enter the base set and every excluded record has a deterministic exclusion reason.

### AC-5: Listing and delisting (FR-9, FR-12, NFR-2)

**Given** securities before listing, on a listing date, on a delisting date and with contradictory
listing windows, **When** expected states are derived, **Then** they are respectively
`not_yet_listed`, an eligible state, `delisted` and `unknown`; the contradictory contract cannot
be promoted. The API's legacy `not_listed_yet` spelling remains unchanged.

### AC-6: Suspension and ST truthfulness (FR-8, FR-10, FR-11, FR-12)

**Given** explicit reviewed trading, suspension, zero-volume, blank-activity and ST evidence,
**When** the contract is built, **Then** only explicit suspension yields `suspended`, zero volume
does not imply suspension, ST is retained without changing scope, and absent/conflicting critical
attributes remain `unknown` rather than being guessed.

### AC-7: Required user symbols are preserved (FR-6, FR-9, FR-12, FR-21, NFR-9)

**Given** positions and all watchlists containing duplicate main-board symbols, a valid non-main-
board A-share, an unsupported BSE symbol and a symbol removed between reads, **When** the writer
captures one allowlisted snapshot, **Then** valid symbols are deduplicated and retained with roles,
unsupported/unstable symbols make the contract blocked, and public status exposes no symbol value.

### AC-8: Required indexes are explicit (FR-7, FR-8, FR-13, FR-15)

**Given** user data that also contains the two summary index symbols, **When** the contract is
built, **Then** `required_indexes` contains exactly `sh.000001` and `sz.399001` once each, their
instrument evidence is bound, and the candidate expected set contains each exactly once.

### AC-9: Count equation and unknown-zero gate (FR-12, FR-13, FR-14, NFR-2)

**Given** members in every expected-state category and one unknown member, **When** counts are
computed, **Then** `total = trading + suspended + not_yet_listed + delisted + unknown` and
`session_expected = trading + suspended`; **When** `unknown` or critical-attribute-unknown is
nonzero, **Then** promotion and publication are rejected and the prior pointer is preserved.

### AC-10: Evidence and generation binding (FR-2, FR-4, FR-8, FR-19)

**Given** an instrument evidence object whose source/date/hash does not match the selected
classification or requested session, **When** promotion is attempted, **Then** the entire contract
is rejected with a sanitized lineage reason; matching evidence produces a deterministic identity.

### AC-11: Exact candidate set (FR-14, FR-15, FR-16, FR-17)

**Given** a promoted contract with trading and suspended members plus not-listed/delisted members,
**When** a provider candidate omits one expected symbol, returns an extra, duplicates a symbol,
returns a row for a non-session member or supplies rows from two providers, **Then** the complete
candidate is invalidated before normalization/publication; no partial candidate is returned.

### AC-12: Whole-session purity (FR-17, FR-25, NFR-12)

**Given** a primary candidate missing one symbol and a secondary row for that symbol, **When** the
selection seam is evaluated, **Then** it rejects symbol-level stitching, preserves the pointer and
does not make any provider failover eligible.

### AC-13: Immutable sidecar and restart (FR-18, FR-19, FR-26, NFR-4)

**Given** a valid contract promotion, an interrupted write, a missing head, a changed member row,
or a replaced database inode, **When** a reader reopens the sidecar, **Then** it returns the prior
valid head or an unavailable result, never the partial/tampered contract; writer recovery does not
run from a public GET.

### AC-14: Read-only status/API/CLI (FR-20, FR-21, NFR-3, NFR-6)

**Given** missing, valid, stale and corrupt sidecar states, **When** `GET /api/v1/market/universe`
or `market-universe` is invoked, **Then** it returns a bounded status with `provider_requests=0`
and `writes=false`, performs no initialization/DDL/DML/user-store access, and omits paths,
payloads, tokens, URLs, exceptions and symbol lists.

### AC-15: Maintenance priority (FR-22, FR-23, NFR-7)

**Given** freshness, repair, shadow and universe maintenance are all due, **When** one automation
tick selects work, **Then** it selects the highest-priority due lane and does not run universe
maintenance; when no higher lane is due, **Then** at most one bounded universe/classification
operation runs under the existing lock.

### AC-16: Failed maintenance preserves prior authority (FR-18, FR-23, FR-26, NFR-4)

**Given** a previous promoted contract and a classification/instrument acquisition failure,
**When** maintenance executes, **Then** the previous head and contract remain byte-identical, the
failure is observable by an allowlisted reason, and no retry storm or canonical write occurs.

### AC-17: Legacy staged migration (FR-24, FR-26, FR-27, NFR-10)

**Given** a legacy BaoStock refresh and a matching or divergent exact contract, **When** mode is
`off`, **Then** existing behavior is unchanged; **When** mode is `shadow`, **Then** the comparison
is read-only and cannot alter publication; **When** mode is `enforce`, **Then** it is rejected by
the default production configuration and requires a separately reviewed enablement decision.

### AC-18: Frozen predecessor surfaces (FR-25, FR-27, NFR-10, NFR-12)

**Given** R2-F2 golden objects, R2-F3 evidence/qualification files, R2-F4.0 digest vectors and
R2-F4.1 calendar/runtime files at the baseline commit, **When** R2-F4.2 tests and static checks
run, **Then** all protected hashes, readers, defaults and behavior remain unchanged.

### AC-19: Offline boundary (NFR-1, NFR-12)

**Given** fake providers that fail loudly on unexpected access and private temporary roots, **When**
the focused and full suites run, **Then** all evidence is offline, no credential/environment
secret is read, and no production/NAS/LaunchAgent state is touched.

## Edge Cases

- EC-1: The requested date is a weekend, exchange holiday, unknown calendar year or calendar
  conflict → return `CALENDAR_UNAVAILABLE`/`CALENDAR_CONFLICT`; do not query a provider.
- EC-2: Calendar generation disappears, is locked, has a changed path/inode or fails hash
  readback → return unavailable and do not fall back to bundled/stale calendar authority.
- EC-3: No promoted classification generation is visible at the requested date → return
  `CLASSIFICATION_UNAVAILABLE`; do not use a future or degraded generation.
- EC-4: Classification source date or observation is after the PIT cutoff → reject the whole
  contract as `PIT_VISIBILITY_INVALID`.
- EC-5: Security master is empty, has duplicate natural identities, or references an unknown
  security from an index/membership record → reject the contract.
- EC-6: A listing date is missing, after a delisting date, or contains an invalid timezone/date
  envelope → derive `unknown` and block publication.
- EC-7: `daily_trade_status=0` appears without a reviewed source semantic saying it means
  suspended → do not infer; derive `unknown`.
- EC-8: A row has zero volume, blank amount, blank factor or no price but no explicit
  suspension evidence → do not turn the row into `suspended`; let the existing quality gate fail.
- EC-9: ST is absent or contradictory → retain `st_state=unknown`; do not relabel it `no` or
  remove the member.
- EC-10: A position/watchlist symbol is malformed, BSE, B share, fund, ETF, bond or index other
  than the two required indexes → block the contract and expose only `REQUIRED_SYMBOL_INVALID`.
- EC-11: UserStore changes during a capture → abort the snapshot and contract; never combine
  two reads into a synthetic set.
- EC-12: The same user symbol appears in positions and multiple watchlists → one member, stable
  sorted roles, one count contribution.
- EC-13: An index evidence object is absent, future-dated, or has an identity mismatch → the
  required-index obligation is unresolved and publication is blocked.
- EC-14: Provider returns a symbol outside the contract, including a valid but not-required
  ChiNext/STAR/BSE symbol → reject as extras; do not filter it away.
- EC-15: Provider omits a suspended row or returns a row for a not-yet-listed/delisted member
  → reject the entire candidate; do not treat the omission/row as legal zero coverage.
- EC-16: Duplicate symbol/session rows or conflicting rows from the same provider → reject
  before Normalize; no first-row-wins behavior.
- EC-17: Candidate contains BaoStock rows plus a TickFlow/Tushare row → reject whole-session
  purity; no symbol-level failover.
- EC-18: Sidecar transaction is interrupted after a member/evidence insert but before head
  commit → reader sees prior head or unavailable, never the staged partial contract.
- EC-19: Sidecar has a valid-looking row with a changed digest, extra table, unsafe mode,
  symlink or replaced inode → strict reader returns unavailable; GET does not repair it.
- EC-20: Classification maintenance fails after acquiring a candidate generation → preserve all
  prior promoted generations/head and record a sanitized terminal reason.
- EC-21: A universe maintenance tick races a freshness/repair/shadow task → existing lock and
  priority policy prevent a second operation; no provider request is duplicated.
- EC-22: The legacy path has no sidecar contract or has a divergent shadow comparison → `off`
  remains compatible, `shadow` reports drift, and `enforce` blocks rather than switching source.

## API Contracts

### `GET /api/v1/market/universe?trade_date=YYYY-MM-DD`

This endpoint is read-only and status-only. It does not accept `execute`, provider, symbol, or
source parameters. It reads an already-created strict sidecar snapshot and never opens the private
user store or constructs a provider.

```typescript
interface UniverseStatusResponse {
  status: "ready" | "unavailable" | "stale" | "blocked";
  trade_date: string;
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
  required_indexes: ["sh.000001", "sz.399001"];
  required_user_symbol_count: number;
  publication_eligible: boolean;
  reason_code: string | null;
  provider_requests: 0;
  writes: false;
}
```

HTTP behavior:

- `200` returns any bounded status above; `status=ready` is necessary but does not qualify a
  secondary provider or enable failover.
- `422` returns `{ "code": "invalid_trade_date" }` for malformed/future query input before any
  store access.
- `503` is reserved for a configured storage dependency that cannot be safely inspected; its
  body is `{ "code": "universe_control_unavailable" }` and contains no path or exception.

### `stock-eva market-universe --date YYYY-MM-DD`

The CLI prints the same canonical JSON projection and has no `--execute` option. It exits `0` for
`ready`, `1` for `unavailable`, `stale` or `blocked`, and `2` for invalid input/configuration. It
must not initialize a missing control directory.

### Internal writer contract

The following are implementation interfaces, not public HTTP authority:

```typescript
interface BuildUniverseRequest {
  trade_date: string;
  calendar_snapshot_id: string;
  classification_generation_id: string;
  required_user_symbol_snapshot: RequiredUserSymbolSnapshot;
  instrument_evidence: InstrumentEvidence[];
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
  contract_sha256: string;
  provider_id: "baostock" | "tickflow" | "tushare";
  adapter_version: string;
  observed_symbols: string[];
  missing_symbols: string[];
  extra_symbols: string[];
  duplicate_symbols: string[];
  result: "pass" | "reject";
}
```

`UniverseCandidateGate` is evaluated for the complete session before Normalize. A passing gate
does not itself create a canonical candidate; the existing evidence, quality, manifest and atomic
publish chain remains authoritative.

## Data Models

### UniverseContractV1

| Field | Type | Constraints |
|---|---|---|
| `schema_version` | integer literal | Exactly `1` |
| `universe_id` | safe ID | Exactly `all-main-board-plus-required-symbols` |
| `scope` | literal | Same as `universe_id`; no fixed count |
| `trade_date` | date | One confirmed open session |
| `classification_generation_id` | safe ID | Promoted PIT generation only |
| `calendar_generation_id` | safe ID | Promoted R2-F4.1 generation only |
| `calendar_sha256` | SHA-256 | Exact pinned calendar identity |
| `required_symbol_snapshot_sha256` | SHA-256 | Writer-owned deduplicated union identity |
| `instrument_evidence_ids` | sorted tuple of safe IDs | Non-empty; all source/date-bound |
| `required_indexes` | fixed tuple | Exactly `sh.000001`, `sz.399001` |
| `required_user_symbols` | sorted tuple of safe symbols | Private sidecar only; public status exposes count |
| `members` | sorted tuple of UniverseMemberV1 | Unique by symbol; includes indexes |
| `counts` | UniverseCountsV1 | Must satisfy the count equation |
| `critical_attribute_unknown_count` | nonnegative integer | Zero required for promotion |
| `publication_eligible` | boolean | True only when unknown and critical unknown are zero and all evidence verifies |
| `created_at` | UTC timestamp | Immutable, after all input observations |
| `contract_sha256` | SHA-256 | Domain-separated digest excluding itself only |

### UniverseMemberV1

| Field | Type | Constraints |
|---|---|---|
| `symbol` | safe symbol | `sh.######` or `sz.######`; unique in contract |
| `security_id` | safe ID | Binds classification/security-master identity |
| `member_kind` | `stock` or `index` | Index only for the two required indexes |
| `scope_roles` | sorted tuple | `main_board_base`, `required_user`, or `required_index` |
| `exchange` | `SSE` or `SZSE` | Derived, not caller-selected |
| `board` | `main`, `chinext`, `star`, or `index` | Versioned identity derivation |
| `list_date` | date or null | PIT source value; null is invalid for current stock state unless source contract permits it |
| `delist_date` | date or null | `>= list_date` when both exist; no inferred end date |
| `expected_trading_state` | closed enum | `trading`, `suspended`, `not_yet_listed`, `delisted`, `unknown` |
| `st_state` | closed enum | `yes`, `no`, `not_applicable`, `unknown` |
| `state_source` | safe ID | Explicit evidence/source semantic identity |
| `effective_from` / `effective_to` | date or null | Evaluated at `trade_date`; no future leakage |
| `instrument_evidence_id` | safe ID | Must exist and hash-validate |
| `member_sha256` | SHA-256 | Digest of the complete member projection excluding itself |

The contract vocabulary intentionally uses `not_yet_listed`; the existing classification service
continues to return `not_listed_yet` for backward compatibility. There is no implicit alias in
storage or hashes.

### UniverseCountsV1

| Field | Type | Constraints |
|---|---|---|
| `total` | integer >= 0 | Number of unique contract members |
| `trading` | integer >= 0 | Members expected to provide a session row |
| `suspended` | integer >= 0 | Members expected to provide a legal suspended placeholder |
| `not_yet_listed` | integer >= 0 | No session row expected |
| `delisted` | integer >= 0 | No session row expected |
| `unknown` | integer >= 0 | Must be zero for promotion/publication |
| `session_expected` | integer >= 0 | Exactly `trading + suspended` |
| `loaded` | integer or null | Set only after one complete provider candidate is checked |
| `critical_attribute_unknown` | integer >= 0 | Includes unknown ST/identity/state evidence |

The invariant is:

```text
total = trading + suspended + not_yet_listed + delisted + unknown
session_expected = trading + suspended
publishable = (unknown == 0
               and critical_attribute_unknown == 0
               and loaded is null or loaded == session_expected)
```

The parenthesized form is normative: a promoted contract requires the first two conditions; a
published candidate additionally requires `loaded == session_expected`, no missing/extra/duplicate
symbols, and every existing canonical gate.

### InstrumentEvidenceV1

| Field | Type | Constraints |
|---|---|---|
| `evidence_id` | safe ID | Immutable local evidence identity |
| `provider_id` | safe provider ID | Source identity; does not grant qualification |
| `adapter_version` | safe version | Reviewed adapter identity |
| `source_schema` | safe version | Exact instrument schema |
| `symbol` / `security_id` | safe IDs | Must match member |
| `security_type` / `exchange` / `board` | closed values | Must match security identity rules |
| `list_date` / `delist_date` | date or null | Source-observed or explicitly unavailable, never guessed |
| `listing_status` | safe source value | Preserved for audit; not reinterpreted without a versioned mapping |
| `daily_trade_status` | safe source value or null | Mapped to state only under a reviewed source semantic |
| `suspension_state` | `trading`, `suspended`, `not_supplied`, `unknown` | Explicit semantic required for current state |
| `st_state` | `yes`, `no`, `not_applicable`, `unknown` | Unknown is never coerced |
| `source_snapshot_date` | date | `<= trade_date` |
| `source_date_semantics` | `source_observed` or `requested_unverified` | `requested_unverified` cannot prove PIT authority |
| `observed_at` | UTC timestamp | At or before PIT cutoff |
| `lineage_hash` | SHA-256 | Binds source-shaped evidence without retaining payload publicly |
| `evidence_sha256` | SHA-256 | Complete immutable identity |

### RequiredUserSymbolSnapshotV1

| Field | Type | Constraints |
|---|---|---|
| `snapshot_id` | safe ID | One writer capture per operation |
| `symbols` | sorted tuple of safe symbols | Private; deduplicated union |
| `roles_by_symbol` | map of symbol to sorted roles | `position`, `watchlist`; watchlist names are excluded |
| `captured_at` | UTC timestamp | One operation instant |
| `source` | literal | `UserStore.allowlisted_positions_and_watchlists` |
| `snapshot_sha256` | SHA-256 | Complete projection digest |

### UniverseControlSidecarV1

The sidecar is a separate strict SQLite control store at a configured local-control basename,
default `market_universe.sqlite3`. Its writer-owned schema contains immutable
`universe_meta`, `universe_contract`, `universe_member`, `universe_instrument_evidence`,
`universe_required_symbol_snapshot` and singleton `universe_head` tables, plus no unallowlisted
tables. Update/delete triggers, safe mode/inode checks, object readback, parent/head CAS and
atomic commit follow the R2-F4.1 calendar-generation control-store pattern. GET readers use
descriptor-bound read-only access and never run DDL/DML/migration.

### UniverseStatusSnapshotV1

The public status is a projection of one verified sidecar snapshot. It contains status, date,
contract/classification/calendar IDs and hashes, the count equation, fixed index IDs,
required-user count, a publication boolean, one allowlisted reason, `provider_requests=0` and
`writes=false`. It contains no members, user symbols, raw evidence, payload, path, SQL or
exception text.

### Canonical hash contract

All contract/member/snapshot hashes use:

```python
canonical_json_bytes(value) = (
    json.dumps(value, ensure_ascii=False, sort_keys=True,
               separators=(",", ":"), allow_nan=False) + "\n"
).encode("utf-8")

domain_sha256(domain, value) = sha256(
    domain.encode("ascii") + b"\n" + canonical_json_bytes(value)
).hexdigest()
```

The exact domains are `stock-eva/r2f4.2/universe-member/v1`,
`stock-eva/r2f4.2/required-symbol-snapshot/v1` and
`stock-eva/r2f4.2/universe-contract/v1`. The digest field itself is the only excluded field;
null, false, zero, empty tuple and roles remain in the preimage.

## Out of Scope

- OS-1: Enabling secondary publication, automatic failover, or changing the R2-F4.0 effective
  failover flag. Those require later canonical-capability qualification and a separate reviewed
  subversion.
- OS-2: Rewriting `DailyBar`, `ProviderId`, R2-F2 evidence schema, R2-F3 TickFlow capability,
  canonical Parquet, manifests, SHA-256 pointers or existing Normalize/Quality Gate behavior.
- OS-3: Treating 3,195, 5,205, or any other observed count as a permanent gate.
- OS-4: Symbol-level multi-provider stitching, old-date carry-forward, manual Parquet/pointer
  edits, or filtering provider extras into an apparently complete candidate.
- OS-5: Real BaoStock/TickFlow/Tushare/AKShare requests, credential or quota validation,
  provider terms re-review, production repairs, NAS replication, deployment or LaunchAgent runs.
- OS-6: A public endpoint that exposes the full universe/member list or private position/
  watchlist symbols. A future separately authorized data endpoint would need its own privacy and
  PIT contract.
- OS-7: New CNINFO events, broad classification taxonomies, index component history
  qualification, intraday/minute/tick data, brokerage access or automatic trading.
- OS-8: Replacing the classification DB with the Universe sidecar or using the sidecar as a
  general-purpose security master. The sidecar records immutable evidence references and the exact
  session projection only.
- OS-9: Claiming `R2-F4.2 GO`, production readiness, secondary qualification, R2-F4 GO,
  R2-F5 soak completion or Release 2 GO from offline tests or a complete-looking contract.
