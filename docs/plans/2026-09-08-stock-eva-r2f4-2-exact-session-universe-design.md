# Stock EVA R2-F4.2 Exact-session Universe Contract Design

**Author:** Codex R2-F delivery quality lead

**Date:** 2026-09-08 (Asia/Shanghai)

**Status:** In Review / implementation blocked until this contract is independently reviewed

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
  strict source references, three identity partitions, the required-symbol snapshot and every
  instrument-evidence identity. A range or a provider's
  unbounded universe MUST NOT substitute for the exact session.

- FR-3: Promoted calendar PIT gate. A contract builder MUST select one verified R2-F4.1
  calendar snapshot for the operation. The date MUST be a confirmed open session in that snapshot;
  weekend, holiday, unknown year, conflict, missing generation or calendar identity drift MUST
  make the contract unavailable or blocked with `CONTROL_STATE_UNAVAILABLE` or
  `PIT_VISIBILITY_INVALID`.

- FR-4: Promoted classification PIT gate. The builder MUST select only a `promoted=true`
  classification generation visible at `trade_date`, with `source_snapshot_date <= trade_date`
  and `observed_at` no later than the declared PIT cutoff. It MUST preserve the selected
  `classification_generation_id` and MUST NOT silently select a future or merely inserted,
  degraded generation. A generation whose `source_date_semantics=requested_unverified` MUST NOT
  be used as authority, even when its requested date equals `trade_date`; the builder MUST return
  `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED` until a reviewed evidence mapping is available.

- FR-5: Main-board base scope. Base members MUST be derived from the selected security master
  using explicit identity rules: ordinary stock, SSE/SZSE, board `main`, and an effective listing
  window containing `trade_date`. ChiNext, STAR, BSE, B shares, funds, ETFs, bonds, indexes and
  other types MUST NOT enter the base set.

- FR-6: Required user symbols. The writer pipeline MUST invoke the UserStore-owned
  `capture_required_symbol_snapshot_once` exactly once per operation; that API performs the
  allowlisted positions/watchlists/items read in one SQLite transaction. It MUST normalize and
  deduplicate the union, persist only a bounded snapshot hash/count plus the contract's member
  roles, and MUST NOT read the private user database from a public GET. A valid A-share user symbol
  outside main board (for example, an explicitly required ChiNext or STAR stock) MUST remain an
  explicit member rather than being dropped.

- FR-7: Required indexes. The contract MUST include exactly the current required market index
  obligations `sh.000001` and `sz.399001` under `required_indexes`, with explicit reviewed
  instrument evidence, source snapshot date at or before `trade_date`, identity mapping version,
  and expected session state exactly `trading`. `suspended` or `unknown` for either required index
  MUST block the contract. User inclusion of either symbol MUST deduplicate against this obligation.
  No index may be inferred from a stock count.

- FR-8: Instrument evidence binding. Every member MUST bind reviewed instrument evidence that
  proves symbol, security type, exchange, board, listing window and source/date semantics. A
  closed, versioned semantic mapping (`mapping_id`, `mapping_version`, accepted source values,
  mapped state, and date/PIT rules) MUST be recorded as reviewed before it can establish authority.
  The evidence identity MUST be retained as an ID/hash; raw provider payloads, credentials and
  URLs MUST NOT be copied into public status or logs. The current BaoStock classification path is
  `requested_unverified` and therefore can only produce a blocked result, not a qualified result;
  this is a temporary qualification state, not a permanent claim that BaoStock is authoritative.

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
  `(unknown == 0 and critical_attribute_unknown_count == 0 and required_indexes_are_trading and
  all source/date/hash checks pass)`. A candidate MAY publish only against a promoted contract
  and only when `(loaded == session_expected and no_missing and no_extra and no_duplicate and
  no_session_drift)`. Any nonzero unknown, including `unknown == 1` with otherwise matching
  `loaded`, MUST reject promotion/publication and preserve the previous canonical pointer.

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
  one-shot automation orchestration and no sixth LaunchAgent. R2-F4.2 MUST NOT claim that every
  lane shares one lock: `_plan_continuity` runs before the existing refresh lease and the existing
  shadow offer runs after canonical completion. The Universe hook is the lowest-priority bounded
  post-success hook, after the existing shadow offer, and a maintenance failure MUST not erase the
  previous head or cause an unbounded retry loop.

- FR-23: Classification maintenance trigger. Existing classification/sync cadence and writer
  contract remain unchanged. R2-F4.2 MUST NOT add an independent Universe/classification lane;
  it MAY offer one bounded Universe build only after a successful canonical refresh and the
  existing shadow offer, using already-promoted evidence. It MUST preserve previous promoted
  generations and build/promote only after all PIT and unknown-zero checks pass.

- FR-24: Legacy compatibility modes. The legacy BaoStock `all-main-board` canonical path MUST
  remain operational and unchanged in R2-F4.2. A contract integration MUST provide explicit
  `off`, `shadow` and `enforce` reporting values, default to `off`. `shadow` MAY compare only the
  legacy BaoStock set and report drift, but MUST NOT change the canonical result. Any `enforce`
  execution request MUST return `BLOCKED_ENFORCE_NOT_ENABLED` immediately; there is no callable
  future-provider seam in this subversion and no production configuration may invoke it.
  The production profile MUST hard-fail any non-`off` Universe mode as
  `BLOCKED_PRODUCTION_MODE_OFF`, preserving the existing BaoStock canonical behavior; `shadow` is
  available only to explicitly isolated offline/staging verification.

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
instrument evidence is bound to a reviewed source/date/identity mapping, each index state is
explicitly `trading`, and the candidate expected set contains each exactly once; a suspended or
unknown index makes the contract blocked.

### AC-9: Count equation and unknown-zero gate (FR-12, FR-13, FR-14, NFR-2)

**Given** members in every expected-state category and one unknown member, **When** counts are
computed, **Then** `total = trading + suspended + not_yet_listed + delisted + unknown` and
`session_expected = trading + suspended`; **When** `unknown` or critical-attribute-unknown is
nonzero, **Then** promotion and publication are rejected and the prior pointer is preserved. **Given**
`unknown=1` and `loaded=session_expected`, **When** publication eligibility is evaluated, **Then**
it is rejected; **Given** `unknown=0` and `loaded != session_expected`, **Then** it is also rejected.

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
valid head only when it is still the atomically committed, fully verified on-disk head; any
tampered/corrupt state returns `unavailable` (never a historical cached projection); writer recovery
does not run from a public GET. Contract, evidence, required-snapshot, member, role-link and head
CAS rows are committed in one SQLite transaction; an interrupted write or failed CAS rolls back all
of them, leaving no unreachable rows or orphan sequence.

### AC-14: Read-only status/API/CLI (FR-20, FR-21, NFR-3, NFR-6)

**Given** missing, valid, stale and corrupt sidecar states, **When** `GET /api/v1/market/universe`
or `market-universe` is invoked, **Then** it returns a bounded status with `provider_requests=0`
and `writes=false`, performs no initialization/DDL/DML/user-store access, and omits paths,
payloads, tokens, URLs, exceptions and symbol lists.

### AC-15: Maintenance boundary (FR-22, FR-23, FR-27, NFR-7)

**Given** the real `run_due_once` flow, **When** `_plan_continuity` reports repair/control work,
**Then** its existing repair lease/lock and return behavior remain unchanged and no Universe hook
runs; **Given** a successful canonical refresh, **Then** the existing post-publish and shadow
offer complete first and only then may one bounded Universe offer run; **Given** no canonical
refresh is due or it fails, **Then** no independent Universe lane or repeated provider request is
created.

### AC-16: Failed maintenance preserves prior authority (FR-18, FR-23, FR-26, NFR-4)

**Given** a previous promoted contract and a classification/instrument acquisition failure,
**When** maintenance executes, **Then** the previous head and contract remain byte-identical, the
failure is observable by an allowlisted reason, and no retry storm or canonical write occurs.

### AC-17: Legacy staged migration (FR-24, FR-26, FR-27, NFR-10)

**Given** a legacy BaoStock refresh and a matching or divergent exact contract, **When** mode is
`off`, **Then** existing behavior is unchanged; **When** mode is `shadow`, **Then** the comparison
is read-only and compares only legacy BaoStock; **When** mode is `enforce`, **Then** execution is
immediately `BLOCKED_ENFORCE_NOT_ENABLED` (including non-production calls), with no provider
request and no source switch.

### AC-18: Frozen predecessor surfaces (FR-25, FR-27, NFR-10, NFR-12)

**Given** R2-F2 golden objects, R2-F3 evidence/qualification files, R2-F4.0 digest vectors and
R2-F4.1 calendar/runtime files at the baseline commit, **When** R2-F4.2 tests and static checks
run, **Then** all protected hashes, readers, defaults and behavior remain unchanged.

### AC-19: Offline boundary (NFR-1, NFR-12)

**Given** fake providers that fail loudly on unexpected access and private temporary roots, **When**
the focused and full suites run, **Then** all evidence is offline, no credential/environment
secret is read, and no production/NAS/LaunchAgent state is touched.

### AC-20: Provider raw batch gate (FR-15, FR-16, FR-17, NFR-4)

**Given** a complete existing `ProviderRawBatch` for BaoStock, **When** the pure
`validate_provider_raw_batch(batch, contract)` offline/future enforce seam evaluates all endpoint
pages, **Then** request/date/refresh/session/lineage, duplicate/extra/missing, page continuity and
`tradestatus`/suspended-placeholder violations reject the whole batch before Normalize. `off` does
not call the seam; `shadow` compares after canonical success and records drift without blocking.

## Edge Cases

- EC-1: The requested date is a weekend, exchange holiday, unknown calendar year or calendar
  conflict → return `CONTROL_STATE_UNAVAILABLE`, `PIT_VISIBILITY_INVALID`,
  `CALENDAR_UNAVAILABLE` or `CALENDAR_CONFLICT`; do not query a provider.
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
- EC-13: An index evidence object is absent, future-dated, identity-mismatched, suspended or
  unknown → `REQUIRED_INDEX_NOT_TRADING`; the required-index obligation blocks publication.
- EC-14: Provider returns a symbol outside the contract, including a valid but not-required
  ChiNext/STAR/BSE symbol → reject as extras; do not filter it away.
- EC-15: Provider omits a suspended row or returns a row for a not-yet-listed/delisted member
  → reject the entire candidate; do not treat the omission/row as legal zero coverage.
- EC-16: Duplicate symbol/session rows or conflicting rows from the same provider → reject
  before Normalize; no first-row-wins behavior.
- EC-17: Candidate contains BaoStock rows plus a TickFlow/Tushare row → reject whole-session
  purity; no symbol-level failover.
- EC-18: Sidecar transaction is interrupted after any contract/evidence/snapshot/member/link insert
  but before head commit → reader sees prior head or unavailable, never a staged partial contract.
- EC-19: Sidecar has a valid-looking row with a changed digest, extra table, unsafe mode,
  symlink or replaced inode → strict reader returns unavailable; GET does not repair it.
- EC-20: Classification maintenance fails after acquiring a candidate generation → preserve all
  prior promoted generations/head and record a sanitized terminal reason.
- EC-21: A universe maintenance tick races a freshness/repair/shadow task → existing lock and
  priority policy prevent a second operation; no provider request is duplicated.
- EC-22: The legacy path has no sidecar contract or has a divergent shadow comparison → `off`
  remains compatible, `shadow` reports drift, and `enforce` blocks rather than switching source.
- EC-23: The raw DAILY_ASTOCK row has missing/unknown `tradestatus`, an illegal suspended
  placeholder or a state mismatch → `UNIVERSE_STATE_MISMATCH`; reject the whole batch before
  Normalize and retain the later Quality Gate.
- EC-24: An attempt plan is absent, duplicated, terminal twice, or exceeds its request budget →
  `UNIVERSE_SCHEMA_MISMATCH`; do not infer a previous success or resend a request.

## API Contracts

### `GET /api/v1/market/universe?trade_date=YYYY-MM-DD`

This endpoint is read-only and status-only. It does not accept `execute`, provider, symbol, or
source parameters. It reads an already-created strict sidecar snapshot and never opens the private
user store or constructs a provider.

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

HTTP behavior:

- A strict read first verifies the current head. If a valid head exists but its `trade_date` is
  not the requested valid date, the response is HTTP `200`, `status=stale`, and
  `reason_code=UNIVERSE_DATE_MISMATCH`; it includes only the requested date and safe head date,
  never a guessed projection. `ready` is necessary but does not qualify a secondary provider or
  enable failover. A valid empty store with no head is HTTP `200`, `status=unavailable`, with
  `reason_code=CONTROL_STATE_UNAVAILABLE`.
- `422` returns `{ "code": "invalid_trade_date" }` for malformed/future query input before any
  store access.
- `503` is returned whenever sidecar path/inode/schema/hash/WAL/lock or storage state cannot be
  proven safely; its exact body is `{ "code": "universe_control_unavailable" }` and contains
  no path or exception. The same status maps to CLI exit `3`.

Status field matrix (also normative for the CLI):

| Status | Condition | Nullable fields and source |
|---|---|---|
| `ready` | verified head exists and `head.trade_date == requested trade_date` | contract IDs, source refs, counts and layers are populated from that head; `verified_head_trade_date` equals requested date |
| `stale` | verified head is valid but `head.trade_date != requested confirmed date` | all populated contract/source/count fields are explicitly the verified head's values, `trade_date` is the request, `verified_head_trade_date` is head date, reason is `UNIVERSE_DATE_MISMATCH`; no requested-date projection is implied |
| `blocked` | durable latest attempt for requested date is terminal blocked, with no verified head for that date | contract/source/count/layer fields are null; `verified_head_trade_date` is null; reason is the durable attempt reason |
| `unavailable` | safe store read has no head, or storage/state cannot be proven | all contract/source/count/layer fields and `verified_head_trade_date` are null; no cached prior head is used; unprovable storage is HTTP 503 |

The single public TypeScript shape uses nullable fields, with this matrix as its discriminated
runtime constraint; implementations MUST NOT populate a nullable field outside the row's stated
status/source rule.

### `stock-eva market-universe --date YYYY-MM-DD`

The CLI prints the same canonical JSON projection and has no `--execute` option. It exits `0` for
`ready`, `1` for safe `unavailable`, `stale` or `blocked`, `2` for invalid date, and `3` for
sidecar/storage-unverifiable (the HTTP-503 case). It must not initialize a missing control
directory.

### Internal writer contract

The following are implementation interfaces, not public HTTP authority:

```typescript
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

interface UniversePublicationContext {
  run_id: string;
  trade_date: string;
  manifest_ref: string;
  evidence_refs: string[];
  publication_lineage_sha256: string;
  status: "ready";
}

interface UniverseMaintenanceDecision {
  lane: "freshness" | "repair" | "shadow" | null;
  action: "run" | "defer" | "none";
  reason_code: string | null;
  provider_requests: number;
  writes_canonical: false;
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
  request_aggregates: Array<{request_id: string; trade_date: string; refresh_id: string;
    provider_session_id: string; pages: number; batch_digest: string}>;
  endpoint_aggregates: Array<{endpoint_id: string; request_id: string; pages: number;
    row_count: number; batch_digest: string}>;
  page_aggregates: Array<{endpoint_id: string; request_id: string; page: number;
    row_count: number; page_digest: string}>;
  batch_digest: string;
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

`UniverseCandidateGate` is evaluated for the complete session before Normalize. A passing gate
does not itself create a canonical candidate; the existing evidence, quality, manifest and atomic
publish chain remains authoritative.

### Pre-Normalize raw batch contract

The only input accepted by `UniverseCandidateGate` is the complete, accumulated existing provider
raw batch plus an independent `UniverseContractV1` argument. It is an internal transport envelope,
not a public response. The raw batch MUST NOT be required to contain invented contract/evidence
fields: existing `ProviderRawBatch` request, endpoint, page and transport lineage are validated
first, then the independent contract identity is checked against the aggregate. Pages are sorted
by contiguous page number before validation.

The gate MUST consume the existing `ProviderRawBatch` directly and MUST NOT introduce a parallel
raw-batch type. `ProviderRawBatch.request`, `provider_id`, `adapter_version`,
`endpoint_contract_version`, `request_plan_hash`, `endpoint_batches`, `request_completions`,
`transport_lineage`, `transport_observations` and `endpoint_summaries` remain the authoritative
fields. Each `RawEndpointBatch` supplies `endpoint`, `schema_variant`, `request_role`,
`instrument_role`, `plan_ordinal`, `lineage` (including refresh/session/request/page), `rows`,
`row_count`, `source_schema`, `date_semantics` and its typed row-order digest.

The pure function is exactly
`validate_provider_raw_batch(batch: ProviderRawBatch, contract: UniverseContractV1) -> UniverseCandidateGate`.
It first validates every existing `request_completion`, transport observation, request plan and
endpoint/page lineage using the existing model validators, then aggregates each logical request,
endpoint batch and page into deterministic tuples and a batch digest. Only after that does it bind
provider=`baostock`, `request.trade_date`, `refresh_id`, `provider_session_id`,
`request_id`, endpoint and the legacy request universe from existing model fields; no nonexistent
contract/evidence field is demanded from `ProviderRawBatch`. It checks contiguous pages,
duplicate/extra/missing symbols, exact session, `tradestatus`/legal
suspended placeholders and loaded count before returning a pass; no partial rows are returned.
The loaded candidate is the union of `DAILY_ASTOCK` stock rows and `INDEX_HISTORY` index rows;
`ALL_STOCK` contributes only an observed-universe cross-check, while factor endpoints contribute
only control/factor-completeness observations and never rows. The result contains request-,
endpoint- and page-aggregate tuples plus `batch_digest`, not a single endpoint/page scalar.
The existing `run_canonical_raw_refresh(validate_batch=...)` callback is only a future/offline
enforce seam. Mode `off` MUST NOT call it. Mode `shadow` MUST NOT pass it into canonical refresh
or block canonical: after successful canonical/evidence publish, a read-only observer compares
the legacy BaoStock request universe to the contract and records `LEGACY_SHADOW_DRIFT` (including
missing required ChiNext/STAR) by count/hash only. Mode `enforce` is always blocked and cannot
route production. `build_canonical_raw_request` is unchanged.

For DAILY_ASTOCK rows, the raw gate requires `tradestatus=1` for a `trading` member and the
existing legal OHLCV/placeholder shape for a `suspended` member with explicit `tradestatus=0`;
missing/unknown status, illegal placeholder or state mismatch rejects the complete batch with
`UNIVERSE_STATE_MISMATCH`. Required indexes must have explicit reviewed `trading` evidence; later
Quality Gate checks remain mandatory and are not replaced by this raw gate.

The legacy callback `validate_universe` in the current automation remains wired exactly as-is in
the canonical path; mode `off` is byte/behavior unchanged. The new pure gate is never supplied to
that callback. In `shadow`, after canonical `ready`, the observer obtains a read-only
`UniversePublicationContext` from `MarketStore.published_refresh` plus its
`publication_lineage` and CandidateStore evidence references. It accepts the observation only when
`run_id`, `trade_date`, manifest reference, evidence references and their lineage digest match the
published readback; mismatch records `UNIVERSE_SESSION_DRIFT` and cannot block or mutate canonical.

## Data Models

### Three-layer membership construction

The contract is the union of three separately counted and hashed layers; no layer may be
reconstructed from another layer's count.

| Layer | Definition | Count/hash rule |
|---|---|---|
| `classification_evidence` | Every unique promoted PIT security identity and its reviewed instrument-evidence reference, including records later excluded from product scope | `classification_evidence_count` and `classification_evidence_sha256` hash sorted `(security_id,symbol,evidence_id,exclusion_reason)` identities; duplicate natural identities or unknown identity classification block |
| `effective_main_board` | The subset whose closed identity is ordinary stock on SSE/SZSE, `board=main`, and `list_date <= trade_date` and (`delist_date is null` or `trade_date < delist_date`) | `effective_main_board_count` and `effective_main_board_sha256` hash sorted member projections, including state/evidence; no fixed count |
| `required_additions` | Valid A-share required-user symbols not already in `effective_main_board`, plus exactly `sh.000001`,`sz.399001` | `required_additions_count` and `required_additions_sha256` hash the sorted non-overlapping partition projection; required-index identities are retained |

The final member set is `effective_main_board ∪ required_additions`; these two partition identity
sets do not overlap. Source evidence may carry multiple roles, but those role overlaps are retained
only in `UniverseMemberV1.scope_roles` and do not change partition counts. A required-user symbol with an
unprovable identity, unsupported security type, or invalid symbol is not silently omitted: the
snapshot is blocked. Listing-window rules are exact: listing date is inclusive, delist date is
exclusive for an active base row, and `trade_date >= delist_date` derives `delisted` for a required
addition. Missing or contradictory windows produce `unknown`/blocked rather than an inferred date.

### Reviewed instrument semantic mapping

`InstrumentSemanticMappingV1` is a closed, immutable registry entry with `mapping_id`,
`mapping_version`, `provider_id`, `source_schema`, accepted raw field/value identifiers, mapped
`security_type`/board/listing/suspension/ST states, required source date semantics, and reviewer
status exactly `reviewed` or `unqualified`. Only `reviewed` entries can establish authority, and
the mapping version is included in evidence and contract hashes. A raw provider value not listed in
the mapping is `unknown`; the implementation MUST NOT infer its meaning. The current BaoStock
classification path (`query_all_stock` with `source_date_semantics=requested_unverified`) has no
reviewed authority mapping in this release and therefore always yields
`BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`; it may qualify only after a future reviewed evidence
artifact and separately reviewed contract, never merely because a request succeeded.

For the two required indexes, the builder accepts only a versioned local
`InstrumentEvidenceV1` artifact/input with `authority_status=reviewed`,
`artifact_origin=local_reviewed_fixture`, `provider_id=baostock`, a closed `mapping_id` and
`mapping_version`, exact `trade_date`, `index_role` equal to `required_index`, symbol identity,
and explicit `expected_trading_state=trading`. The artifact is an offline input to this release,
not a network discovery result. Production currently has no such qualified artifact, so production
status is stably `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`; this must not be represented as a
qualified index or silently made available by a fixture.

### SourceRefsV1 and identity partitions

`SourceRefsV1` is a closed, immutable model and is part of the contract payload and contract
hash. It contains exactly `calendar_generation_id`, `calendar_sha256`,
`classification_generation_id`, `classification_generation_sequence`,
`classification_source_snapshot_date`, `classification_snapshot_sha256`,
`required_symbol_snapshot_id`, `required_symbol_snapshot_sha256`, `instrument_evidence_ids`,
`provider_id=baostock`,
`trade_date`, `exact_pit_cutoff`, and `source_date_semantics`. IDs are safe-format values and
every list is sorted, unique, and non-empty where its obligation exists; no URL, token or raw
payload is allowed. If `GenerationSummary` has no source hash, the contract records only its ID,
sequence and source-date metadata and computes `classification_snapshot_sha256` from the selected
exact PIT snapshot; no fictitious generation hash is allowed. The reader compares this model to
the explicit columns and linked rows.

The contract persists three role-partitioned identity sets (a symbol may be represented in more
than one set, but each set itself is sorted and unique):

| Partition | Contents and hash |
|---|---|
| `classification_evidence_ids` | All promoted PIT classification evidence identities, including excluded records; sorted IDs and `classification_evidence_sha256` over canonical `(security_id,symbol,evidence_id,exclusion_reason)` projections |
| `effective_main_board_ids` | The effective ordinary SSE/SZSE main-board stock identities at `trade_date`; sorted security IDs and `effective_main_board_sha256` over complete member projections |
| `required_additions_ids` | Sorted identities for valid required-user additions not already in the effective main-board set, plus the two required indexes; `required_additions_partition_sha256` over the canonical partition projection |

Partition hashes use separate domains and canonical projections: `classification_evidence_ids`
hashes sorted `{security_id,symbol,evidence_id,exclusion_reason}` rows under
`stock-eva/r2f4.2/universe-partition/classification-evidence/v1`; `effective_main_board_ids`
hashes sorted complete security/member projections under
`stock-eva/r2f4.2/universe-partition/effective-main-board/v1`; and
`required_additions_ids` hashes sorted `{symbol,security_id,scope_roles,state,evidence_id}`
projections under `stock-eva/r2f4.2/universe-partition/required-additions/v1`. The latter is
formed after removing every identity already in `effective_main_board`; required-index identities
remain obligations and are included exactly once. Original overlapping roles remain in
`universe_member.scope_roles`, not in the partition identity sets. Each count equals its sorted
unique projection length.

Each partition has an explicit JSON column and SHA-256 column in `universe_contract`, and the
same IDs are present in `payload_json`. `universe_instrument_evidence` stores every classification
record, including excluded records with a closed `exclusion_reason`; link rows identify its
partition and member role. A strict reader recomputes each partition from persisted evidence/link
rows and `universe_member.scope_roles`, then compares count and hash. Missing excluded evidence,
an orphan link, an ID in the wrong partition, or a partition that can only be reconstructed from
the final member set is `UNIVERSE_SCHEMA_MISMATCH`.

### UniverseContractV1

| Field | Type | Constraints |
|---|---|---|
| `schema_version` | integer literal | Exactly `1` |
| `universe_id` | safe ID | Exactly `all-main-board-plus-required-symbols` |
| `scope` | literal | Same as `universe_id`; no fixed count |
| `trade_date` | date | One confirmed open session |
| `classification_generation_id` | safe ID | Promoted PIT generation only |
| `classification_generation_sequence` / `classification_source_snapshot_date` | integer/date | Explicit GenerationSummary metadata; no fictitious source hash |
| `classification_snapshot_sha256` | SHA-256 | Newly computed digest of the selected exact PIT classification snapshot |
| `calendar_generation_id` | safe ID | Promoted R2-F4.1 generation only |
| `calendar_sha256` | SHA-256 | Exact pinned calendar identity |
| `required_symbol_snapshot_sha256` | SHA-256 | Writer-owned deduplicated union identity |
| `exact_pit_cutoff` | UTC timestamp | Inclusive evidence visibility cutoff, bound into the contract hash |
| `source_refs` | SourceRefsV1 | Complete source identity object, included in payload/hash |
| `provider_id` / `source_date_semantics` | `baostock` / closed semantics | Explicit source reference columns, cross-checked with `source_refs` |
| `required_symbol_snapshot_id` / `instrument_evidence_ids_json` | safe ID/JSON IDs | Explicit source reference columns, cross-checked with links and payload |
| `classification_evidence_ids` / `classification_evidence_partition_sha256` | sorted IDs/SHA-256 | Layer 1 persisted identity partition, including excluded evidence |
| `effective_main_board_ids` / `effective_main_board_partition_sha256` | sorted IDs/SHA-256 | Layer 2 persisted identity partition |
| `required_additions_ids` / `required_additions_partition_sha256` | sorted IDs/SHA-256 | Layer 3 persisted identity partition |
| `classification_evidence_count` / `classification_evidence_sha256` | integer/SHA-256 | Layer 1 count/hash |
| `effective_main_board_count` / `effective_main_board_sha256` | integer/SHA-256 | Layer 2 count/hash (same digest as the partition) |
| `required_additions_count` / `required_additions_sha256` | integer/SHA-256 | Layer 3 count/hash (same digest as the partition) |
| `instrument_evidence_ids` | sorted tuple of safe IDs | Non-empty; all source/date-bound |
| `required_indexes` | fixed tuple | Exactly `sh.000001`, `sz.399001` |
| `required_user_symbols` | sorted tuple of safe symbols | Private sidecar only; public status exposes count |
| `members` | sorted tuple of UniverseMemberV1 | Unique by symbol; includes indexes |
| `counts` | UniverseCountsV1 | Must satisfy the count equation |
| `critical_attribute_unknown_count` | nonnegative integer | Zero required for promotion |
| `publication_eligible` | boolean | True only when `(unknown == 0 and critical_attribute_unknown_count == 0 and required_indexes_are_trading and all evidence verifies and (loaded is null or loaded == session_expected))`; a published candidate additionally requires `loaded == session_expected` |
| `created_at` | UTC timestamp | Immutable, after all input observations |
| `contract_sha256` | SHA-256 | Domain-separated digest excluding itself only |

### UniverseMemberV1

| Field | Type | Constraints |
|---|---|---|
| `symbol` | safe symbol | `sh.######` or `sz.######`; unique in contract |
| `security_id` | safe ID | Binds classification/security-master identity |
| `member_kind` | `stock` or `index` | Index only for the two required indexes |
| `scope_roles` | sorted tuple | `effective_main_board`, `required_user`, or `required_index` |
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

For `member_kind=index`, the only valid symbols are `sh.000001` and `sz.399001`; their evidence
MUST be provider-observed (not `requested_unverified`), have `source_snapshot_date <= trade_date`,
carry the reviewed identity/mapping version, and derive `expected_trading_state=trading`.

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
contract_publishable = (unknown == 0
                         and critical_attribute_unknown == 0
                         and required_indexes_are_trading
                         and all_source_date_hash_checks_pass)
publication_eligible = (contract_publishable
                        and (loaded is null or loaded == session_expected))
candidate_publishable = (contract_publishable
                         and loaded == session_expected
                         and no_missing and no_extra and no_duplicate
                         and no_session_drift)
```

The parentheses are normative. A promoted contract requires `contract_publishable`; an exact
published candidate requires `candidate_publishable`. Thus `unknown=1` with
`loaded=session_expected` is rejected, as is `unknown=0` with a loaded-count mismatch.

### InstrumentEvidenceV1

| Field | Type | Constraints |
|---|---|---|
| `evidence_id` | safe ID | Immutable local evidence identity |
| `provider_id` | safe provider ID | Source identity; does not grant qualification |
| `authority_status` | `reviewed` or `unqualified` | Only `reviewed` can establish authority |
| `artifact_origin` | literal | `local_reviewed_fixture` or `production_reviewed_artifact` |
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
| `evidence_trade_date` | date | Exact session date; required for index evidence |
| `index_role` | `required_index` or `not_applicable` | Required-index evidence must be `required_index` |
| `exclusion_reason` | closed reason or `none` | Required for every classification record, including excluded evidence |
| `expected_trading_state` | closed state | Required-index evidence must explicitly be `trading` |
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
| `source` | literal | `UserStore.capture_required_symbol_snapshot_once` |
| `snapshot_sha256` | SHA-256 | Complete projection digest |

The writer-owned `backend/app/user/store.py` MUST expose one transaction-bound API; the Universe
module MUST NOT call `list_positions`, `list_watchlists` and `list_watchlist_items` separately or
open/chain additional connections:

```typescript
interface RequiredUserSymbolSnapshotRead {
  snapshot: RequiredUserSymbolSnapshot;
  user_store_revision: string;
  snapshot_token: string;
}

capture_required_symbol_snapshot_once(operation_id: string): RequiredUserSymbolSnapshotRead
```

Only the existing UserStore writer owner may call this API; public API/CLI processes have no writer
connection or initialization permission. The implementation opens exactly one existing SQLite
connection, verifies the UserStore schema is already initialized (capture MUST NOT initialize or
migrate it), issues `BEGIN` read transaction,
reads positions, watchlists and items in one deterministic SQL snapshot, and computes
`snapshot_token = domain_sha256("stock-eva/r2f4.2/user-rows/v1", canonical_rows)` where
`canonical_rows` contains sorted table/column/value tuples. It reads a writer-maintained revision
or SQLite `data_version` before and after the transaction on the same connection. A busy/locked
database, mutation/revision mismatch, schema mismatch or row/hash mismatch MUST `ROLLBACK` and
return `USER_SNAPSHOT_CHANGED` or `USER_STORE_UNAVAILABLE`; it MUST never retry or merge a second
observation. Connection close is mandatory on both commit and rollback paths. The private sidecar
may store only normalized symbols, role bitset, revision, token digest, count, capture time and
snapshot hash. Public status may expose only count and hash (never symbol values, roles, names,
revision or token).

### UniverseControlSidecarV1

The sidecar is a separate strict SQLite control store at a configured local-control basename,
default `market_universe.sqlite3`. This release has writer-only v1 initialization and **no legacy
migration**: a missing sidecar may be initialized only by the writer; a non-empty unknown schema,
schema digest mismatch, or pre-existing legacy database is blocked, not migrated. The contract row
stores the complete canonical `payload_json`; explicit identity/sequence/parent/hash/source/count
columns and redundant member rows are cross-checked, while link tables provide exact evidence and
required-snapshot ownership. Durable attempt/dedup state is append-only: a planned row is written
before any classification request, and one immutable terminal result is later linked to it. The
following DDL is normative (whitespace-normalized and
domain-hashed as `schema_digest`):

```sql
PRAGMA user_version = 1;
CREATE TABLE universe_meta (
  meta_key TEXT PRIMARY KEY CHECK (meta_key IN ('schema_version','schema_digest','store_id','db_inode')),
  meta_value TEXT NOT NULL
);
CREATE TABLE universe_contract (
  contract_id TEXT PRIMARY KEY,
  sequence INTEGER NOT NULL UNIQUE CHECK (sequence > 0),
  parent_contract_id TEXT REFERENCES universe_contract(contract_id),
  trade_date TEXT NOT NULL, universe_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  scope TEXT NOT NULL, calendar_generation_id TEXT NOT NULL,
  calendar_sha256 TEXT NOT NULL, classification_generation_id TEXT NOT NULL,
  classification_generation_sequence INTEGER NOT NULL,
  classification_source_snapshot_date TEXT NOT NULL,
  classification_snapshot_sha256 TEXT NOT NULL,
  exact_pit_cutoff TEXT NOT NULL, provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
  source_date_semantics TEXT NOT NULL, required_symbol_snapshot_id TEXT NOT NULL,
  required_symbol_snapshot_sha256 TEXT NOT NULL, instrument_evidence_ids_json TEXT NOT NULL,
  counts_json TEXT NOT NULL, layer_counts_json TEXT NOT NULL, source_refs_json TEXT NOT NULL,
  classification_evidence_ids_json TEXT NOT NULL,
  classification_evidence_partition_sha256 TEXT NOT NULL,
  effective_main_board_ids_json TEXT NOT NULL,
  effective_main_board_partition_sha256 TEXT NOT NULL,
  required_additions_ids_json TEXT NOT NULL,
  required_additions_partition_sha256 TEXT NOT NULL,
  payload_json TEXT NOT NULL, contract_sha256 TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
);
CREATE TABLE universe_member (
  contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id),
  symbol TEXT NOT NULL, member_sha256 TEXT NOT NULL, member_json TEXT NOT NULL,
  PRIMARY KEY (contract_id, symbol)
);
CREATE TABLE universe_instrument_evidence (
  evidence_id TEXT PRIMARY KEY, evidence_sha256 TEXT NOT NULL UNIQUE,
  provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
  authority_status TEXT NOT NULL CHECK (authority_status IN ('reviewed','unqualified')),
  artifact_origin TEXT NOT NULL CHECK (artifact_origin IN ('local_reviewed_fixture','production_reviewed_artifact')),
  security_id TEXT NOT NULL, symbol TEXT NOT NULL,
  mapping_id TEXT NOT NULL, mapping_version TEXT NOT NULL,
  source_snapshot_date TEXT NOT NULL, evidence_trade_date TEXT NOT NULL,
  exclusion_reason TEXT NOT NULL,
  index_role TEXT NOT NULL CHECK (index_role IN ('required_index','not_applicable')),
  source_date_semantics TEXT NOT NULL,
  evidence_json TEXT NOT NULL
);
CREATE TABLE universe_required_symbol_snapshot (
  snapshot_id TEXT PRIMARY KEY, snapshot_sha256 TEXT NOT NULL UNIQUE,
  user_store_revision TEXT NOT NULL, snapshot_token_digest TEXT NOT NULL,
  symbol_count INTEGER NOT NULL CHECK (symbol_count >= 0), snapshot_json TEXT NOT NULL
);
CREATE TABLE contract_evidence (
  contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id),
  evidence_id TEXT NOT NULL REFERENCES universe_instrument_evidence(evidence_id),
  evidence_role TEXT NOT NULL CHECK (evidence_role IN
    ('classification_evidence','effective_main_board','required_addition','member')),
  security_id TEXT NOT NULL, symbol TEXT NOT NULL, exclusion_reason TEXT NOT NULL,
  PRIMARY KEY (contract_id, evidence_id, evidence_role)
);
CREATE TABLE contract_required_snapshot (
  contract_id TEXT PRIMARY KEY REFERENCES universe_contract(contract_id),
  snapshot_id TEXT NOT NULL REFERENCES universe_required_symbol_snapshot(snapshot_id),
  snapshot_sha256 TEXT NOT NULL
);
CREATE TABLE universe_attempt (
  attempt_id TEXT PRIMARY KEY, dedup_key TEXT NOT NULL UNIQUE,
  hook_kind TEXT NOT NULL CHECK (hook_kind = 'universe_post_success'),
  refresh_id TEXT NOT NULL, trade_date TEXT NOT NULL, operation_day TEXT NOT NULL,
  request_budget INTEGER NOT NULL CHECK (request_budget = 1),
  classification_max_attempts INTEGER NOT NULL CHECK (classification_max_attempts = 1),
  created_at TEXT NOT NULL, planned_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE universe_attempt_result (
  attempt_id TEXT PRIMARY KEY REFERENCES universe_attempt(attempt_id),
  terminal_status TEXT NOT NULL CHECK (terminal_status IN ('blocked','deferred','succeeded','failed')),
  classification_request_count INTEGER NOT NULL CHECK (classification_request_count IN (0,1)),
  reason_code TEXT NOT NULL, finished_at TEXT NOT NULL, result_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE universe_head (
  singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
  sequence INTEGER NOT NULL, contract_id TEXT NOT NULL,
  contract_sha256 TEXT NOT NULL, head_sha256 TEXT NOT NULL, updated_at TEXT NOT NULL,
  FOREIGN KEY (contract_id) REFERENCES universe_contract(contract_id)
);
CREATE TRIGGER universe_contract_no_update BEFORE UPDATE ON universe_contract BEGIN
  SELECT RAISE(ABORT, 'immutable_contract');
END;
CREATE TRIGGER universe_contract_no_delete BEFORE DELETE ON universe_contract BEGIN
  SELECT RAISE(ABORT, 'immutable_contract');
END;
CREATE TRIGGER universe_member_no_update BEFORE UPDATE ON universe_member BEGIN
  SELECT RAISE(ABORT, 'immutable_member');
END;
CREATE TRIGGER universe_member_no_delete BEFORE DELETE ON universe_member BEGIN
  SELECT RAISE(ABORT, 'immutable_member');
END;
CREATE TRIGGER universe_evidence_no_update BEFORE UPDATE ON universe_instrument_evidence BEGIN
  SELECT RAISE(ABORT, 'immutable_evidence');
END;
CREATE TRIGGER universe_evidence_no_delete BEFORE DELETE ON universe_instrument_evidence BEGIN
  SELECT RAISE(ABORT, 'immutable_evidence');
END;
CREATE TRIGGER universe_snapshot_no_update BEFORE UPDATE ON universe_required_symbol_snapshot BEGIN
  SELECT RAISE(ABORT, 'immutable_snapshot');
END;
CREATE TRIGGER universe_snapshot_no_delete BEFORE DELETE ON universe_required_symbol_snapshot BEGIN
  SELECT RAISE(ABORT, 'immutable_snapshot');
END;
CREATE TRIGGER universe_meta_no_update BEFORE UPDATE ON universe_meta BEGIN
  SELECT RAISE(ABORT, 'immutable_meta');
END;
CREATE TRIGGER universe_meta_no_delete BEFORE DELETE ON universe_meta BEGIN
  SELECT RAISE(ABORT, 'immutable_meta');
END;
CREATE TRIGGER contract_evidence_no_update BEFORE UPDATE ON contract_evidence BEGIN
  SELECT RAISE(ABORT, 'immutable_contract_evidence');
END;
CREATE TRIGGER contract_evidence_no_delete BEFORE DELETE ON contract_evidence BEGIN
  SELECT RAISE(ABORT, 'immutable_contract_evidence');
END;
CREATE TRIGGER contract_snapshot_no_update BEFORE UPDATE ON contract_required_snapshot BEGIN
  SELECT RAISE(ABORT, 'immutable_contract_snapshot');
END;
CREATE TRIGGER contract_snapshot_no_delete BEFORE DELETE ON contract_required_snapshot BEGIN
  SELECT RAISE(ABORT, 'immutable_contract_snapshot');
END;
CREATE TRIGGER universe_attempt_no_update BEFORE UPDATE ON universe_attempt BEGIN
  SELECT RAISE(ABORT, 'immutable_attempt');
END;
CREATE TRIGGER universe_attempt_no_delete BEFORE DELETE ON universe_attempt BEGIN
  SELECT RAISE(ABORT, 'immutable_attempt');
END;
CREATE TRIGGER universe_attempt_result_no_update BEFORE UPDATE ON universe_attempt_result BEGIN
  SELECT RAISE(ABORT, 'immutable_attempt_result');
END;
CREATE TRIGGER universe_attempt_result_no_delete BEFORE DELETE ON universe_attempt_result BEGIN
  SELECT RAISE(ABORT, 'immutable_attempt_result');
END;
```

`store_id` is a deployment-bound constant and `db_inode` is the writer-created inode captured in
`universe_meta`; both are checked on every open. The writer MUST hold the existing refresh/control
lock plus an exclusive sidecar lock file, set `PRAGMA journal_mode=WAL`, `synchronous=FULL`,
`foreign_keys=ON`, and use a bounded busy timeout. Contract/member/evidence/snapshot/link rows are
append-only; contract, evidence, snapshot, member, role-link and head CAS rows MUST be inserted or
updated in one SQLite transaction. A failed CAS rolls back that entire transaction; there are no
pre-committed unreachable rows or orphan sequence. An empty initialized store has **no** `universe_head`
row; the first promotion inserts sequence 1 with a null parent.

The canonical contract preimage is `payload_json` decoded as strict JSON and re-encoded by the
documented canonical JSON algorithm. `payload_json` MUST contain the complete UniverseContractV1,
including sorted `members`, required indexes, evidence IDs, required-symbol snapshot hash, all
counts/layer counts, source refs, PIT fields and all three partition ID/hash fields. Its
`contract_sha256` MUST equal the domain hash
of that preimage; explicit columns (`contract_id`, schema/scope/date, calendar/classification
source refs, PIT cutoff, required-snapshot hash, counts, layer counts and partition ID/hash
fields) MUST equal the corresponding payload fields. The reader reconstructs `UniverseContractV1`
from `payload_json`, sorts and re-hashes every
`universe_member` row, then verifies the member set/hash and all bidirectional links. The exact
role-set is: excluded classification evidence = `{classification_evidence}`; effective main-board
member = `{classification_evidence,effective_main_board,member}`; required non-index addition =
`{classification_evidence,required_addition,member}`; required index =
`{required_addition,member}`. For every link, `security_id`, `symbol` and `exclusion_reason` MUST
equal the referenced evidence payload; no one-link shortcut is permitted. Every payload evidence
ID has exactly its role-set and existing evidence row, every link belongs to the contract, exactly
one `contract_required_snapshot` link matches the payload snapshot/hash, and no
evidence/member/snapshot/link row is orphaned from a referenced contract. It also verifies that
each classification evidence row (including excluded rows) has a role-bearing link, every
attempt dedup key has exactly one immutable plan and at most one immutable terminal result, and
the recorded request count never exceeds the budget. Missing or duplicate attempt state is
`UNIVERSE_SCHEMA_MISMATCH`, never a cached prior success.

`head_preimage` is the canonical JSON object
`{"singleton_id":1,"sequence":N,"contract_id":"...","contract_sha256":"..."}`;
`head_sha256` is its domain-separated digest. The reader requires `head.sequence == contract.sequence`,
`head.contract_sha256 == contract.contract_sha256`, sequence 1 to have no parent, and every later
contract to have `sequence = parent.sequence + 1` with matching parent hash. CAS is
`UPDATE universe_head ... WHERE singleton_id=1 AND sequence=expected_sequence AND
contract_sha256=expected_hash`; insert is allowed only for an empty head and expected sequence 0,
and a zero-row CAS is a transaction rollback.

The ordered schema digest preimage is the newline-joined, in-DLL order of the normalized `PRAGMA`,
`CREATE TABLE`, `CREATE INDEX` (none), and `CREATE TRIGGER` statements. Normalization removes SQL
comments, trims each statement, collapses whitespace outside quoted literals, removes whitespace
immediately inside/around `(`, `)` and `,`, preserves quoted case/content, and uses no locale or
formatter. `schema_digest = domain_sha256("stock-eva/r2f4.2/
universe-schema/v1", normalized_ddl)`. The strict reader opens the descriptor-bound database
read-only, verifies path/inode, store identity, `user_version`, exact schema digest, allowed table
set, payload/member/link/evidence hashes, parent chain and head digest, and never runs DDL/DML or
creates a WAL/SHM file. A crash before commit leaves the prior head; a crash during CAS yields
either old or fully verified new head. CAS
conflict aborts and rolls back all contract/evidence/snapshot/member/link/head changes without
retrying or changing the head. Any corruption returns `unavailable`, never
a historical cached projection.

### UniverseStatusSnapshotV1

The public status is a projection of one verified sidecar snapshot. It contains status, date,
contract/classification/calendar IDs and hashes, the count equation, fixed index IDs,
required-user count, a publication boolean, one allowlisted reason, `provider_requests=0` and
`writes=false`. An initialized empty store with no head row is `unavailable`, not `ready`, and
there is no historical cached status. It contains no members, user symbols, raw evidence, payload, path,
SQL or exception text.

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

## Automation decision matrix

The matrix describes the real current one-shot flow and intentionally does not invent a shared
lock or request ledger. `run_due_once` computes the schedule, calls `_plan_continuity` before the
refresh lease, acquires the existing refresh lease only for `_execute_due`, then calls the existing
post-publish and shadow hooks after the lease is complete. R2-F4.2 adds no independent scheduler
lane: the only safe Universe offer is a bounded hook after a successful canonical result and after
the existing shadow offer.

| Real flow boundary | Existing action/lock boundary | Universe effect |
|---|---|---|
| `_plan_continuity` reports a control/repair block | Repair executor uses its own existing repair lease/lock before refresh lease; `run_due_once` returns without canonical execution | No Universe read/build/offer |
| `decision.action == run` | `_execute_due` runs inside the existing refresh lease; existing Normalize/Quality/Manifest/Atomic Publish remains unchanged | On successful canonical result only, call existing `_run_post_publish`, then existing `_offer_shadow`; only after those return may one bounded Universe offer run |
| canonical result is failure, skipped, or not ready | Existing result handling and scheduler state; no Universe lock acquisition or retry | No Universe offer; failure does not trigger a second provider request |
| `decision.action != run` | Existing wait/delay/post-publish path; no independent Universe lane is added | No Universe offer in R2-F4.2, even if a sidecar is due |

The post-success offer is keyed to the completed canonical outcome/refresh identity and is at most
once. It may read the already-pinned calendar/classification evidence and capture the UserStore
snapshot, but current BaoStock classification with `requested_unverified` evidence returns
blocked without acquisition. `shadow` means legacy BaoStock only. `enforce` is a reportable
configuration value whose execution immediately returns `BLOCKED_ENFORCE_NOT_ENABLED`; TickFlow
and Tushare are not candidates. This bounded choice preserves R2-F1, R2-F4.1 and existing
`_plan_continuity`/repair lock boundaries.

`UniversePostSuccessHook.offer(canonical_result, refresh_id, trade_date, manifest_ref,
evidence_ref, now)` is invoked only after canonical `ready`, post-publish completion, and the
existing shadow offer have returned. It first reacquires the existing `RefreshRunLock` in
non-blocking mode and then the exclusive sidecar lock; lock failure returns `DEFER` with
`CONTROL_STATE_UNAVAILABLE` and makes no provider request. Under the sidecar lock it inserts the
immutable `universe_attempt` plan before any classification request. The dedup lookup and plan
insert occur in one `BEGIN IMMEDIATE` transaction and commit before the request; its deterministic
`dedup_key` is the hash of `{hook_kind, refresh_id, trade_date, operation_day}`; budget and
`classification_max_attempts` are both exactly 1. The terminal immutable result records status,
reason and actual request count. A duplicate key is a no-op/defer, never a resend. If the current
classification evidence is unqualified, the hook records stable `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`
with zero requests. `operation_day` is the `Asia/Shanghai` calendar date captured at attempt start
and remains fixed across midnight. `planned_sha256` is the domain hash
`stock-eva/r2f4.2/universe-attempt-plan/v1` over the complete canonical plan preimage; result
hashing uses `stock-eva/r2f4.2/universe-attempt-result/v1` over status, request count, closed
reason and finish time. The reader recomputes both and treats absent, altered or duplicate result
events as unavailable. No independent lane or unbounded retry exists.

## Closed reason set and traceability matrix

The implementation MUST use only these reason codes in status, tests and sanitized logs:
`CONTROL_STATE_UNAVAILABLE`, `PIT_VISIBILITY_INVALID`, `CALENDAR_UNAVAILABLE`, `CALENDAR_CONFLICT`,
`CLASSIFICATION_UNAVAILABLE`, `USER_STORE_UNAVAILABLE`,
`BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`, `PIT_CUTOFF_VIOLATION`, `USER_SNAPSHOT_CHANGED`,
`REQUIRED_INDEX_NOT_TRADING`, `REQUIRED_SYMBOL_INVALID`, `UNIVERSE_UNKNOWN_NONZERO`, `UNIVERSE_COUNT_MISMATCH`,
`UNIVERSE_MISSING_SYMBOL`, `UNIVERSE_EXTRA_SYMBOL`, `UNIVERSE_DUPLICATE_SYMBOL`,
`UNIVERSE_SESSION_DRIFT`, `UNIVERSE_STATE_MISMATCH`, `UNIVERSE_DATE_MISMATCH`, `UNIVERSE_STORAGE_UNAVAILABLE`, `UNIVERSE_SCHEMA_MISMATCH`,
`UNIVERSE_HEAD_CAS_CONFLICT`, `BLOCKED_ENFORCE_NOT_ENABLED`, `BLOCKED_PRODUCTION_MODE_OFF`, `LEGACY_SHADOW_DRIFT`,
`NONE`. Unknown provider/exception text MUST map to `UNIVERSE_STORAGE_UNAVAILABLE` or
`CLASSIFICATION_UNAVAILABLE` according to the failed boundary, never to a guessed semantic.

The mapping is deterministic and closed:

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

The raw BaoStock `provider_code` is retained only in private evidence/diagnostics; its semantic
is never guessed and it is never exposed in HTTP/CLI. `USER_STORE_UNAVAILABLE` is used when the
writer-owned store cannot be read; `CALENDAR_UNAVAILABLE`/`CALENDAR_CONFLICT` remain precise
internal variants but map to the control row above.

| Requirement(s) | AC / EC | Required offline evidence or static check |
|---|---|---|
| FR-1..FR-4, FR-19, FR-27 | AC-1..AC-3, AC-10, AC-18; EC-1..EC-5 | `tests/test_market_universe.py::test_contract_identity_and_pit`; static protected-file SHA manifest |
| FR-5, FR-9, FR-12, FR-13 | AC-4..AC-5, AC-9; EC-4, EC-6 | `test_three_layer_scope_and_effective_window`, `test_listing_and_delisting_boundary` |
| FR-6, FR-21 | AC-7, AC-14; EC-7, EC-8, EC-11..EC-12 | `test_user_snapshot_single_connection_busy_rollback`; public projection schema/static privacy scan |
| FR-7, FR-8, FR-10, FR-11 | AC-6, AC-8; EC-7, EC-9, EC-13 | `test_required_indexes_must_be_explicit_trading`, `test_reviewed_mapping_required` |
| FR-14..FR-17 | AC-9, AC-11, AC-12; EC-10, EC-14..EC-17 | `test_unknown_one_and_loaded_match_reject`, `test_raw_batch_gate_rejects_extras_duplicates_drift` |
| FR-18, FR-19, FR-26 | AC-13, AC-16; EC-18..EC-20 | `test_sidecar_schema_digest_parent_chain_links_and_cas`; static DDL digest check |
| FR-20, FR-21 | AC-14; EC-1..EC-3, EC-13 | `tests/test_market_universe.py::test_status_zero_write`; filesystem fingerprint |
| FR-22, FR-23 | AC-15, AC-16; EC-14, EC-20, EC-21 | `tests/test_market_automation.py::test_post_success_universe_offer_order` |
| FR-24, FR-25 | AC-12, AC-17, AC-18; EC-11, EC-16, EC-22 | `tests/test_market_failover.py` and static provider allowlist (`baostock` only) |
| FR-15..FR-17 | AC-20; EC-23 | pure raw-batch gate fixtures for request/endpoint/page aggregation, state and whole-session rejection |
| FR-18, FR-19, FR-23, FR-26 | AC-13, AC-16; EC-24 | sidecar attempt/hash/transaction rollback and strict-reader tests |
| NFR-1..NFR-12 | AC-1..AC-20; EC-1..EC-24 | offline-only fakes, zero-write fingerprint, deterministic hash, privacy scan, protected SHA and validator outputs |

The final acceptance record MUST list each FR/AC/EC result, test or static trace, exact commit,
and the two strict validator outputs. This document remains `In Review` until independent SPEC
and QUALITY reviews of the exact implementation commit complete.

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
