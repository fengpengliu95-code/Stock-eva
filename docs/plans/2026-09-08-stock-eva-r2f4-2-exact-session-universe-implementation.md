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
The legacy BaoStock path remains the canonical path in `off` and `shadow` modes. Mode/profile
handling follows one matrix: production with any non-`off` mode returns `B_P`
(`BLOCKED_PRODUCTION_MODE_OFF`), while nonproduction/staging with `enforce` returns `B_E`
(`BLOCKED_ENFORCE_NOT_ENABLED`); both blocked cells perform zero Universe-provider requests and
leave the legacy canonical path unchanged. There is no callable future-provider seam. TickFlow and
Tushare are excluded from this subversion.

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
  `capture_required_symbol_snapshot_existing()` exactly once per operation; that API performs the
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
  one-shot automation orchestration and no sixth LaunchAgent or independent scheduler lane.
  `_offer_universe_maintenance` is the one lowest-priority hook after all higher-priority
  continuity/repair, refresh, post-publish and shadow work for that tick. R2-F4.2 MUST NOT claim
  that every flow shares one lock: `_plan_continuity` and repair retain their existing boundary,
  while the hook reacquires `RefreshRunLock` non-blocking. A maintenance failure MUST not erase the
  previous head or cause an unbounded retry loop.

- FR-23: Classification maintenance trigger. Existing classification/sync cadence and writer
  contract remain unchanged. After higher-priority work, `_offer_universe_maintenance` MUST run at
  most once per scheduler tick when no repair is active and no refresh is running, including a
  non-freshness/holiday/weekend tick. A fresh canonical success supplies the sealed context after
  post-publish and shadow; otherwise the hook uses only the last verified immutable context input
  persisted in the Universe sidecar and performs bounded classification cadence/source-version
  planning. It MUST preserve previous promoted generations and build/promote only after all PIT
  and unknown-zero checks pass.

- FR-24: Legacy compatibility modes. The legacy BaoStock `all-main-board` canonical path MUST
  remain operational and unchanged in R2-F4.2. A contract integration MUST provide explicit
  `off`, `shadow` and `enforce` reporting values, default to `off`. `shadow` MAY compare only the
  legacy BaoStock set and report drift, but MUST NOT change the canonical result. The sole
  production/nonproduction decision is the mode/profile matrix below: production with any
  non-`off` mode returns `B_P` (`BLOCKED_PRODUCTION_MODE_OFF`), while nonproduction/staging with
  `enforce` returns `B_E` (`BLOCKED_ENFORCE_NOT_ENABLED`). Blocked cells perform no
  Universe-provider request; the legacy canonical path remains unchanged. There is no callable
  provider seam in this subversion. `shadow` is available only to explicitly
  isolated offline/staging verification.

The mode/profile matrix is normative and evaluated before any Universe provider construction:

| Profile | `off` | `shadow` | `enforce` |
|---|---|---|---|
| production | legacy canonical unchanged; `NONE`, zero Universe requests | `B_P` (`BLOCKED_PRODUCTION_MODE_OFF`), zero Universe requests | `B_P` (`BLOCKED_PRODUCTION_MODE_OFF`), zero Universe requests |
| nonproduction/staging | legacy canonical unchanged; `NONE`, zero Universe requests | read-only legacy observer only; `LEGACY_SHADOW_DRIFT` when it differs, otherwise `NONE`; no canonical block | `B_E` (`BLOCKED_ENFORCE_NOT_ENABLED`), zero requests |

This matrix is distinct from a provider failure: a blocked cell is a deterministic configuration
result, never a retry or a request to authenticated or secondary services. `shadow` observes only
the already-built legacy BaoStock request and never changes the builder's default behavior.
The profile discriminator is exact: take the existing `Settings.environment`, require a string,
apply Unicode whitespace trim and Unicode `casefold()` (no aliases, substring tests or environment
variable enumeration), then map exactly `production` to `production`; exactly `development`,
`test` or `staging` to `nonproduction`; any other value is an invalid profile and returns
`CONTROL_STATE_UNAVAILABLE` before mode evaluation or provider construction. Thus only the first
matrix row can produce `B_P` (`BLOCKED_PRODUCTION_MODE_OFF`), and only the second row can produce
`B_E` (`BLOCKED_ENFORCE_NOT_ENABLED`); an invalid profile has neither production nor shadow authority.
For compact notation, `B_P` means `BLOCKED_PRODUCTION_MODE_OFF` and `B_E` means
`BLOCKED_ENFORCE_NOT_ENABLED`; production with any non-`off` mode is `B_P`, while nonproduction
with `enforce` is `B_E`. No other profile/mode pair may emit either alias.

- FR-25: No authority widening. R2-F4.2 MUST NOT add a secondary provider to canonical
  `ProviderId`, change R2-F4.0 failover readiness, enable TickFlow/Tushare failover, change R2-F3
  qualification, modify the promoted calendar semantics, or alter canonical normalization,
  quality gates, Parquet, manifests or pointers.

- FR-26: Safe migration path. The new sidecar MUST be populated from a staged writer-only
  flow and compared against the legacy BaoStock observed set; enforce remains permanently blocked in
  this subversion.
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
  attempt, and MUST respect the existing refresh lock and provider request budgets. When maintenance
  is disabled, it performs zero hook, sidecar, snapshot, observer and provider work.

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
**When** the date is weekend, holiday or unknown-year, **Then** no contract is promoted and the
result is `CALENDAR_UNAVAILABLE` with zero provider calls; **When** the date conflicts with a
calendar, **Then** the result is `CALENDAR_CONFLICT`; **When** it is future, **Then** the result is
the unique `PIT_VISIBILITY_INVALID` semantic-date rejection with zero provider calls.

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
does not run from a public GET. Source-state, contract, evidence, required-snapshot, member,
role-link and head CAS rows are committed in one SQLite transaction; an interrupted write or failed CAS rolls back the
entire promotion, so readers see only the old head or a fully verified new head.

### AC-14: Read-only status/API/CLI (FR-20, FR-21, NFR-3, NFR-6)

**Given** missing, valid, stale and corrupt sidecar states, **When** `GET /api/v1/market/universe`
or `market-universe` is invoked, **Then** it returns a bounded status with `provider_requests=0`
and `writes=false`, performs no initialization/DDL/DML/user-store access, and omits paths,
payloads, tokens, URLs, exceptions and symbol lists.

**Given** a readable sidecar with a latest source-state row, an exact head whose source digest
matches or differs, a durable blocking/indeterminate attempt, and an external classification or
calendar change that has not been persisted, **When** the API/CLI status is read, **Then** it uses
only the sidecar row: it returns `ready`, `stale(UNIVERSE_SOURCE_VERSION_CHANGED)`, or
`blocked` by the priority above, and the unpersisted external change is not discovered.

**Given** a syntactically invalid `trade_date`, **Then** pure format parsing returns HTTP 422 /
CLI exit 2 without I/O; **Given** a syntactically valid but future `trade_date` and an unprovable
sidecar path/inode/schema, **When** the API/CLI is invoked, **Then** sidecar proof runs first and
the result is the independent HTTP 503 / CLI exit 3 control envelope; **When** the sidecar is proven
readable, **Then** the future semantic date returns the unique HTTP 422 / CLI exit 2
`PIT_VISIBILITY_INVALID` before any head/source projection. Lexical-invalid and
sidecar-proof/future-semantic cases are separate tests; the ordering never depends on whether the
date is a weekend.

### AC-15: Maintenance boundary (FR-22, FR-23, FR-27, NFR-7)

**Given** the real `run_due_once` flow, **When** `_plan_continuity` reports repair/control work,
repair is active, refresh is running, or the refresh lock is busy, **Then** the existing boundary
remains unchanged and no Universe hook runs; **Given** a successful freshness refresh, **Then**
post-publish and shadow complete first and `_offer_universe_maintenance` runs once with the fresh
context; **Given** the decision is non-run and no repair/error is active, **Then** the existing
shadow boundary completes and the lowest-priority hook evaluates cadence using the persisted
sidecar context, including weekend/non-freshness ticks. No independent lane or repeated provider
request is created.

**Given** `store.save_refresh` has succeeded and context construction, validation or holder sealing
raises `RuntimeError`, **When** the callback exits, **Then** it records only a sealed-holder-
unavailable diagnostic, closes resources in `finally`, never raises, and returns the frozen
`ready_result` rather than an outer `failure_result`. A separate service regression covers
exactly-once consumer failure independently.
The service, not the hook, consumes the fresh sealed context exactly once after post-publish and
shadow and passes it as the hook's `context` argument.

**Given** `market_universe_maintenance_enabled=false`, **When** a scheduler tick completes,
**Then** no Universe hook, sidecar access, UserStore snapshot, observer or provider request occurs;
the disabled decision is `action=none`, `reason_code=NONE`, `provider_requests=0` and
`writes_canonical=false`, while legacy behavior is unchanged.

### AC-16: Failed maintenance preserves prior authority (FR-18, FR-23, FR-26, NFR-4)

**Given** a previous promoted contract and a classification/instrument acquisition failure,
**When** maintenance executes, **Then** the previous head and contract remain byte-identical, the
failure is observable by an allowlisted reason, and no retry storm or canonical write occurs.

### AC-17: Legacy staged migration (FR-24, FR-26, FR-27, NFR-10)

**Given** a legacy BaoStock refresh and a matching or divergent exact contract, **When** mode is
`off`, **Then** existing behavior is unchanged; **When** mode is `shadow`, **Then** the comparison
is read-only, compares only legacy BaoStock and returns private diagnostic drift without blocking;
**When** mode/profile is evaluated, **Then** production with any non-`off` mode is
`B_P` (`BLOCKED_PRODUCTION_MODE_OFF`) and nonproduction/staging `enforce` is
`B_E` (`BLOCKED_ENFORCE_NOT_ENABLED`), with no Universe-provider request and no source switch;
the legacy canonical path remains unchanged.

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
`validate_provider_raw_batch(batch, contract)` is an offline fixture-only pure function that evaluates all endpoint
pages, **Then** request/date/refresh/session/lineage, duplicate/extra/missing, page continuity and
`tradestatus`/suspended-placeholder violations reject the whole batch before Normalize. `off` does
not call the seam; `shadow` compares the callback-captured prevalidation snapshot at the existing
`_offer_shadow` boundary (including builder rejection), and after canonical success also records
drift without blocking.

## Edge Cases

- EC-1: A lexically invalid date is rejected by the pure format parser as
  `PIT_VISIBILITY_INVALID` (HTTP 422 / CLI 2) without sidecar I/O. A lexically valid future
  date is checked only after sidecar proof and then has the same unique `PIT_VISIBILITY_INVALID`
  semantic-date reason (HTTP 422 / CLI 2). A valid non-future weekend/holiday/unknown calendar
  year returns `CALENDAR_UNAVAILABLE`; a calendar conflict returns `CALENDAR_CONFLICT`; none
  queries a provider.
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
  → reject the entire candidate; do not treat the omission/row as legal zero coverage. A changed
  classification/source/mapping digest makes the old contract stale/non-publishable and due
  immediately, with `UNIVERSE_SOURCE_VERSION_CHANGED` until a new verified contract exists.
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
- EC-21: A universe maintenance tick races a freshness/repair/shadow task, or its configured
  `lock_path` is `None` → existing nonblocking lock policy returns `DEFER` with
  `CONTROL_STATE_UNAVAILABLE`; no provider request is duplicated. When maintenance is disabled,
  the tick returns `action=none`/`NONE` with zero hook, sidecar, snapshot and provider activity.
- EC-22: The legacy path has no sidecar contract or has a divergent shadow comparison → `off`
  remains compatible, `shadow` reports private diagnostic drift without blocking, and `enforce`
  blocks rather than switching source.
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
type UniverseReasonCode = "CONTROL_STATE_UNAVAILABLE" | "PIT_VISIBILITY_INVALID" | "CALENDAR_UNAVAILABLE" | "CALENDAR_CONFLICT" | "CLASSIFICATION_UNAVAILABLE" | "USER_STORE_UNAVAILABLE" | "BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED" | "PIT_CUTOFF_VIOLATION" | "USER_SNAPSHOT_CHANGED" | "REQUIRED_INDEX_NOT_TRADING" | "REQUIRED_SYMBOL_INVALID" | "UNIVERSE_UNKNOWN_NONZERO" | "UNIVERSE_COUNT_MISMATCH" | "UNIVERSE_MISSING_SYMBOL" | "UNIVERSE_EXTRA_SYMBOL" | "UNIVERSE_DUPLICATE_SYMBOL" | "UNIVERSE_SESSION_DRIFT" | "UNIVERSE_STATE_MISMATCH" | "UNIVERSE_SOURCE_VERSION_CHANGED" | "DATE_MISMATCH" | "UNIVERSE_STORAGE_UNAVAILABLE" | "UNIVERSE_SCHEMA_MISMATCH" | "UNIVERSE_HEAD_CAS_CONFLICT" | "BLOCKED_ENFORCE_NOT_ENABLED" | "BLOCKED_PRODUCTION_MODE_OFF" | "LEGACY_SHADOW_DRIFT" | "ATTEMPT_INDETERMINATE" | "UNIVERSE_IDENTITY_CONFLICT" | "NONE";
type BlockedReason = "PIT_VISIBILITY_INVALID" | "CALENDAR_UNAVAILABLE" | "CALENDAR_CONFLICT" | "CLASSIFICATION_UNAVAILABLE" | "USER_STORE_UNAVAILABLE" | "BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED" | "PIT_CUTOFF_VIOLATION" | "USER_SNAPSHOT_CHANGED" | "REQUIRED_INDEX_NOT_TRADING" | "REQUIRED_SYMBOL_INVALID" | "UNIVERSE_UNKNOWN_NONZERO" | "UNIVERSE_COUNT_MISMATCH" | "UNIVERSE_MISSING_SYMBOL" | "UNIVERSE_EXTRA_SYMBOL" | "UNIVERSE_DUPLICATE_SYMBOL" | "UNIVERSE_SESSION_DRIFT" | "UNIVERSE_STATE_MISMATCH" | "UNIVERSE_SOURCE_VERSION_CHANGED" | "BLOCKED_ENFORCE_NOT_ENABLED" | "BLOCKED_PRODUCTION_MODE_OFF" | "ATTEMPT_INDETERMINATE" | "UNIVERSE_IDENTITY_CONFLICT" | "CONTROL_STATE_UNAVAILABLE" | "UNIVERSE_HEAD_CAS_CONFLICT";
type ReadyReason = "NONE";
type StaleReason = "UNIVERSE_SOURCE_VERSION_CHANGED" | "DATE_MISMATCH";
interface UniverseControlError { code: "universe_control_unavailable"; }
interface CliUnavailableError { code: "universe_unavailable"; reason_code: "CONTROL_STATE_UNAVAILABLE"; }
interface UniverseStatusCounts { total: number; trading: number; suspended: number;
  not_yet_listed: number; delisted: number; unknown: number; session_expected: number;
  loaded: number | null; critical_attribute_unknown_count: number; }
interface UniverseStatusLayers { classification_evidence_count: number;
  classification_evidence_sha256: string; effective_main_board_count: number;
  effective_main_board_sha256: string; required_additions_count: number;
  required_additions_sha256: string; }
interface UniverseStatusBase { trade_date: string; universe_id: "all-main-board-plus-required-symbols";
  schema_version: 1; scope: "all-main-board-plus-required-symbols"; provider_requests: 0;
  writes: false; }
interface UniverseStatusReady extends UniverseStatusBase { status: "ready";
  verified_head_trade_date: string; contract_id: string; contract_sha256: string;
  source_state_id: string; source_state_sha256: string; source_version_digest: string;
  classification_generation_id: string; calendar_generation_id: string; calendar_sha256: string;
  counts: UniverseStatusCounts; layers: UniverseStatusLayers;
  required_indexes: ["sh.000001", "sz.399001"]; required_user_symbol_count: number;
  publication_eligible: true; reason_code: ReadyReason; }
interface UniverseStatusStale extends UniverseStatusBase { status: "stale";
  verified_head_trade_date: string; contract_id: string; contract_sha256: string;
  source_state_id: string; source_state_sha256: string; source_version_digest: string;
  classification_generation_id: string; calendar_generation_id: string; calendar_sha256: string;
  counts: UniverseStatusCounts; layers: UniverseStatusLayers;
  required_indexes: ["sh.000001", "sz.399001"]; required_user_symbol_count: number;
  publication_eligible: false; reason_code: StaleReason; }
interface UniverseStatusBlocked extends UniverseStatusBase { status: "blocked";
  verified_head_trade_date: null; contract_id: null; contract_sha256: null;
  source_state_id: null; source_state_sha256: null; source_version_digest: null;
  classification_generation_id: null; calendar_generation_id: null; calendar_sha256: null;
  counts: null; layers: null; required_indexes: null; required_user_symbol_count: null;
  publication_eligible: false; reason_code: BlockedReason; }
interface UniverseStatusUnavailable extends UniverseStatusBase { status: "unavailable";
  verified_head_trade_date: null; contract_id: null; contract_sha256: null;
  source_state_id: null; source_state_sha256: null; source_version_digest: null;
  classification_generation_id: null; calendar_generation_id: null; calendar_sha256: null;
  counts: null; layers: null; required_indexes: null; required_user_symbol_count: null;
  publication_eligible: false; reason_code: "CONTROL_STATE_UNAVAILABLE"; }
type UniverseStatusResponse = UniverseStatusReady | UniverseStatusStale | UniverseStatusBlocked | UniverseStatusUnavailable;
```

HTTP behavior:

Only lexical format validation occurs before store I/O; future-date semantic validation occurs
after strict sidecar proof.

The public GET has one and only one current-source input: a verified `universe_source_state` row
from the readable sidecar. The pure date parser runs before I/O; for lexically valid input, the
reader then verifies the sidecar and complete head chain. When a valid head exists for another date,
it uses that head's linked source-state row and does not require a source-state row for the requested
date. For an exact-date head, the reader validates the head-linked source-state and consults only
the sidecar's latest requested-date source-state and attempt rows. GET MUST NOT
read external classification, calendar, UserStore, provider or environment state, and therefore
does not claim real-time discovery. A writer-verified change is invisible to GET until it is
persisted as a new source-state row; this is intentional PIT snapshot semantics. A required head
source-state row that is missing or invalid is unprovable and uses the independent control-error
boundary rather than guessing a current source.

The date/status ordering and every public result/HTTP or CLI code are defined only by the single
matrix below. The independent storage error envelopes are not status-union values.

The four interfaces above are the only public shapes. `Ready` and `Stale` carry verified head
fields; `Blocked` and `Unavailable` carry null head fields. Implementations MUST NOT populate a
nullable field outside its interface. After pure lexical format parsing, the following single
matrix is the complete exact-head/no-head decision algorithm and the sole status decision authority
for HTTP and CLI; no prose rule outside this matrix may override it:

| Verified state | Attempt/source condition | Public result and safe fields |
|---|---|---|
| any sidecar state | date syntax invalid (pure format parse) | 422 `PIT_VISIBILITY_INVALID`; no sidecar I/O or source projection |
| lexically valid date | sidecar/head/source-state/attempt schema or storage cannot be proven | independent 503 `UniverseControlError`; no semantic date check |
| readable sidecar | date is future by the trusted calendar/clock | 422 `PIT_VISIBILITY_INVALID`; no provider request |
| exact-date head | no strictly newer source-state, or newer source-state has equal digest | `ready`, complete fields from head |
| exact-date head | newer source-state differs; no matching attempt | `stale/UNIVERSE_SOURCE_VERSION_CHANGED`, complete old-head fields |
| exact-date head | newer differs; matching `blocked` result | `blocked` with persisted `BlockedReason`, all head fields null |
| exact-date head | newer differs; matching `deferred` result | `blocked/CONTROL_STATE_UNAVAILABLE` or `blocked/UNIVERSE_HEAD_CAS_CONFLICT`, all head fields null |
| exact-date head | newer differs; matching `failed` result | `blocked` with persisted `BlockedReason`, all head fields null |
| exact-date head | newer differs; matching `ATTEMPT_INDETERMINATE` result or `RUNNING` plan without result | `blocked/ATTEMPT_INDETERMINATE`, all head fields null |
| exact-date head | matching `succeeded` result has the same source digest and complete head | `ready`, complete fields from head |
| exact-date head | newer differs; matching `succeeded` (complete) result but no exact new head | 503 `UNIVERSE_SCHEMA_MISMATCH`; success must promote head in the same transaction |
| valid head on another date | requested date differs; its linked source-state verifies | `stale/DATE_MISMATCH`, complete safe fields from that head; requested source-state not required |
| no head | requested-date `blocked`, `deferred`, `failed`, `ATTEMPT_INDETERMINATE`, or `RUNNING` plan | `blocked` with the deterministic attempt reason; all head fields null |
| no head | requested-date `succeeded` result without its exact promoted head | 503 `UNIVERSE_SCHEMA_MISMATCH`; a success without a head is not `unavailable` |
| no head | no requested-date attempt | `unavailable/CONTROL_STATE_UNAVAILABLE`, all head fields null |

### `stock-eva market-universe --date YYYY-MM-DD`

The CLI prints the same canonical JSON projection and has no `--execute` option. It exits `0` for
`ready`, `1` for safe `unavailable`, `stale` or `blocked`, `2` for invalid date, and `3` for
sidecar/storage-unverifiable (the HTTP-503 case). It must not initialize a missing control
directory.

### Internal writer contract

The following are implementation interfaces, not public HTTP authority:

```typescript
interface UniverseMaintenanceDecision {
  priority_order: number | null;
  action: "run" | "defer" | "none";
  reason_code: UniverseReasonCode | null;
  provider_requests: number;
  writes_canonical: false;
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

interface UniversePostSuccessHook {
  offer(
    trade_date: string,
    now: string,
    context: UniversePublicationContext | null,
  ): UniverseHookResult;
}

interface UniverseHookResult {
  status: "PROMOTED" | "BLOCKED" | "DEFER";
  reason_code: UniverseReasonCode | null;
  provider_requests: number;
  writes_canonical: false;
}

interface UniversePublicationContext {
  run_id: string;
  trade_date: string;
  manifest_ref: string;
  evidence_refs: string[]; // exactly evidence_sha256, candidate_manifest_sha256, gate_report_sha256
  publication_lineage_json: string;
  publication_lineage_sha256: string;
  status: "ready";
  created_at: string;
}

interface CanonicalRefreshExecution {
  result: RefreshResult;
  builder_outcome: "accepted" | "rejected"; // private, never serialized
  builder_diagnostic: LegacyBuilderDiagnostic | null; // counts/hashes only, private
  legacy_shadow_observation: LegacyShadowObservation | null; // counts/hashes only
}

class CanonicalRefreshCallable(Protocol) {
  _refresh_lock_held: boolean;
  __call__(**kwargs: object): CanonicalRefreshExecution;
  consume_universe_publication_context(
    run_id: string, trade_date: string,
  ): UniversePublicationContext | null;
}

`CanonicalRefreshCallable` is the direct callable returned by `canonical_refresh_callback`; its
additive `CanonicalRefreshExecution` wrapper carries the unchanged legacy `RefreshResult`, the
builder outcome, and bounded count/hash diagnostics. The callback calls the private inspection
exactly once immediately before the builder, passes that same immutable object to the builder, and
reduces the observed request universe to `LegacyShadowObservation` before returning; one builder
call therefore yields one wrapper and no provider retry. The service unwraps
`execution.result` for the existing `AutomationOutcome`/legacy flow, passes only the sanitized
observation to `_offer_shadow`, and releases raw request objects in `finally`; raw symbols never
enter
`AutomationOutcome`, logs, persistence or public output. The callback also owns the thread-safe
bounded publication-context holder keyed by `(run_id, trade_date)`, with at most one sealed entry
per refresh. After a successful canonical result, `MarketAutomationService` performs exactly-once consume using
`consume_universe_publication_context(run_id, trade_date)` after post-publish and the existing
shadow offer, then passes the consumed context to `UniversePostSuccessHook.offer`. The hook never
consumes the holder. `main.py` and the automation CLI use the same factory to inject one callable
and one hook; the read-only `market-universe` CLI uses the same settings/layout projection but
constructs neither writer nor provider. A missing/non-callable consume method skips only Universe
work and preserves the legacy result.
The call order is fixed: (1) callback calls `inspect_legacy_main_board_input` exactly once and
freezes its immutable `MainBoardInspection`; (2) it calls the additive builder seam once with
`inspection=...`; (3) on acceptance it freezes the builder's `ProviderRequest.session_symbols` as
the observed count/hash projection, while on rejection it computes the same projection from sorted
inspection main-board symbols plus the fixed index additions `("sh.000001", "sz.399001")`; (4) it assigns
`builder_outcome` and the counts/hash-only `builder_diagnostic` in that builder try/except and
returns `CanonicalRefreshExecution`; (5) the service unwraps `result` and passes the execution's
counts/hash-only `legacy_shadow_observation` to `_offer_shadow`; (6) the callback releases raw
request objects in `finally`; (7) only then does it continue the existing post-publish/Universe
offer ordering.
Fetch, quality, publish and shadow failures MUST NOT change the already-assigned
`builder_outcome`. Snapshot-capture, observer, handoff or release exceptions are isolated and
cannot replace the returned legacy result or trigger another provider request.

The internal legacy request seam is deliberately additive and optional:

```typescript
interface MainBoardInspection {
  trade_date: string;
  main_board_count: number;
  shanghai_count: number;
  shenzhen_count: number;
  total_expected_count: number;
  metadata_provider_requests: number;
  main_board_symbols: readonly string[]; // private call-stack data only
}
function inspect_legacy_main_board_input(
  adapter: BaoStockAdapter, trade_date: string,
) -> MainBoardInspection;
function build_canonical_raw_request(
  adapter: BaoStockAdapter, *, trade_date: string, refresh_id: string,
  required_symbols: Set<string>, inspection: MainBoardInspection | null = null,
) -> ProviderRequest;

interface LegacyPreflightSnapshot {
  trade_date: string;
  main_board_count: number;
  shanghai_count: number;
  shenzhen_count: number;
  total_expected_count: number;
  metadata_provider_requests: number;
  observed_symbol_count: number;
  observed_symbols_sha256: string;
}
interface LegacyBuilderDiagnostic {
  outcome: "accepted" | "rejected";
  observed_symbol_count: number;
  observed_symbols_sha256: string;
  failure_class: "NONE" | "INPUT_REJECTED" | "BUILDER_ERROR";
}
```

When `inspection` is supplied, `build_canonical_raw_request` MUST consume that exact object and
MUST NOT inspect inputs a second time. With the default `null`, all existing callers retain the
current inspection and legacy behavior byte-for-byte; this optional argument is not a provider
switch. The callback owns raw symbols only within its provider scope and reduces them before
returning. Builder acceptance and rejection both return the additive wrapper and the private
diagnostic may expose only counts/hashes. Raw `symbols` MUST NOT cross the wrapper's public
boundary, enter `AutomationOutcome`, logs or persistence, or be placed in
`LegacyPreflightSnapshot`/`LegacyShadowHandoff`; the callback computes the legacy observation
before its provider/evidence resources are released in `finally`.
The offline legacy regression is `tests/test_market_universe_staged.py::test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison`.
It proves the bounded observer emits only count/hash diagnostics and no raw symbols. The release
candidate boundary is additionally covered by
`tests/test_market_universe_staged.py::test_release_candidate_removes_runner_and_raw_symbol_public_fields`.

In the canonical callback implementation, immediately after `store.save_refresh` returns success
the code freezes `ready_result`. It then runs context construction and the holder seal in an
independent nested `try/except Exception`; `RuntimeError`, validation failures and seal/sink
failures are converted to a sealed-holder-unavailable marker and sanitized diagnostics, never
raised. A `finally` closes all context/evidence resources. The outer failure handler MUST return
the frozen `ready_result` whenever `save_refresh` succeeded; it MUST not replace that branch with
`failure_result`. There is no `MarketStore` reader or new MarketStore schema/lineage dependency.
A crash before a context is consumed leaves no new Universe attempt/head; a later successful
canonical tick may offer again. A consumed context is persisted as an immutable sidecar input row
for later maintenance when no fresh callback context exists.

The publication-lineage digest is new and is never assumed to exist in the current store. Its
domain is `stock-eva/r2f4.2/publication-lineage/v2`; its exact canonical preimage is the current
code-level `_R2F2_LINEAGE_FIELDS` object with this fixed key set: `provider_id`, `universe_id`,
`evidence_id`, `evidence_sha256`, `candidate_id`, `candidate_manifest_sha256`,
`gate_report_sha256`, `adapter_version`, and `source_schema_version` (the current code has nine
fields). The digest is `domain_sha256(domain, canonical_json(lineage_object))`.
`manifest_ref` is the safe `candidate_manifest_sha256`; `evidence_refs` is exactly the sorted
unique tuple of the three content hashes `evidence_sha256`, `candidate_manifest_sha256`, and
`gate_report_sha256`. Evidence and candidate IDs remain lineage metadata only and MUST NOT enter
`evidence_refs`.
CandidateStore immutable bundle/readers resolve those refs, reconstruct the exact lineage object
and recheck this new digest before accepting the context; no raw payload is included.

interface UniverseBuildResult {
  status: "candidate" | "promoted" | "blocked" | "unavailable";
  contract: UniverseContract | null;
  reason_code: UniverseReasonCode | null;
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
  endpoint_counts: Array<{endpoint_id: string; row_count: number}>;
  counts: {loaded: number | null; session_expected: number; missing: number; extra: number;
    duplicate: number};
  universe_raw_projection_sha256: string;
  adapter_version: string;
  result: "pass" | "reject";
  reason_code: UniverseReasonCode | null;
}
```

The internal writer may write classification/universe control evidence under its lock. It MUST
never change a canonical market pointer merely by creating a contract. The candidate gate MUST
consume the existing `ProviderRawBatch` and its `RawEndpointBatch` endpoint/page rows; it MUST NOT
introduce a parallel raw-batch type. It MUST validate the complete batch before Normalize and
reject any date/provider/session/request/endpoint/transport drift, duplicate page or
`(symbol, trade_date)` key, missing/extra symbol, non-contiguous page, or
`loaded != session_expected`. It MUST reject `unknown=1` even if the loaded count matches. No
provider other than BaoStock may enter this interface.

`validate_provider_raw_batch(batch, contract)` receives the complete existing batch and an
independent contract identity; it does not require contract/evidence fields absent from
`ProviderRawBatch`. It first validates existing request completions, transport observations,
request plan and endpoint/page lineage using existing model validators, then emits deterministic
endpoint semantics from the validated object and computes `universe_raw_projection_sha256`; only
afterward does it bind the independent contract date/provider/refresh identity.
The loaded candidate is `DAILY_ASTOCK` stock rows plus `INDEX_HISTORY` index rows; `ALL_STOCK` is
only an observed-universe cross-check and factor endpoints are control/factor-completeness only,
never loaded rows. The result is safe endpoint counts, loaded/session counts and the projection
hash; request IDs, provider session IDs, pages and payloads are not exposed.

The shadow-only required-symbol observer is a pure read-only function over the already-built legacy
request plan; it does not call BaoStock or rebuild the plan:

```typescript
interface LegacyShadowObservation {
  provider_id: "baostock";
  request_trade_date: string;
  contract_sha256: string | null;
  expected_symbol_count: number | null;
  observed_symbol_count: number;
  missing_required_symbol_count: number | null;
  extra_symbol_count: number | null;
  drift_sha256: string | null;
  reason_code: "LEGACY_SHADOW_DRIFT" | "NONE" | "NO_COMPARISON";
  control_reason: "CONTROL_STATE_UNAVAILABLE" | null;
}
function observe_legacy_request_universe(
  snapshot: LegacyPreflightSnapshot, builder_outcome: "accepted" | "rejected",
  builder_diagnostic: LegacyBuilderDiagnostic | null, contract: UniverseContractV1 | null,
): LegacyShadowObservation;
interface LegacyShadowHandoff {
  trade_date: string;
  builder_outcome: "accepted" | "rejected";
  diagnostic: LegacyShadowObservation;
}
```

The legacy builder's `ProviderRequest` is already sorted-unique and its preflight rejects symbols
outside the main-board scope. Therefore the observer does not claim to inspect duplicate provider
rows or an unreturned request. At the pre-validation boundary the callback calls
`inspect_legacy_main_board_input` once and passes the resulting immutable `MainBoardInspection`
to the builder; this is the only inspection input. An accepted builder records only the count/hash
projection of the built `ProviderRequest.session_symbols`; a rejected builder computes the same
projection from sorted inspection main-board symbols plus exactly `("sh.000001", "sz.399001")`.
Thus a required ChiNext/STAR addition can remain absent and report drift without exposing the set.
The snapshot contains only bounded counts/hashes, is owned by the callback call stack, and is
reduced to `LegacyShadowObservation` before the callback's `finally`; it is never logged, persisted
or placed in `LegacyShadowHandoff`. The canonical path and its failure result are never changed by
this capture.

With a contract, the observer derives the expected sorted unique set only inside its private
comparison frame, including required additions and the two required indexes. It compares the
expected count/hash with the snapshot's observed count/hash; it does not return set members or
attempt to reconstruct missing/extra values. A preflight rejection is retained in
`builder_outcome` but is not reinterpreted as a provider-row failure. Set hashes use
`domain_sha256("stock-eva/r2f4.2/legacy-shadow-symbol-set/v1", sorted_symbols)` and
`drift_sha256` uses only `provider_id`, `request_trade_date`, `contract_sha256`, expected
count/hash and observed count/hash. The returned record contains counts and digests only. Equal
count/hash pairs return `NONE`; any mismatch returns `LEGACY_SHADOW_DRIFT`, including required
ChiNext/STAR additions absent from the legacy input.
With no persisted contract, the typed result is `NO_COMPARISON` with
`control_reason=CONTROL_STATE_UNAVAILABLE` and nullable expected/missing/extra/digest fields.

The observer is invoked in nonproduction `shadow` either at preflight rejection or after a
successful canonical refresh and CandidateStore evidence readback. The internal call is
`_offer_shadow(outcome, execution)` and consumes only the execution's counts/hash-only
`LegacyShadowObservation`. Its typed handoff is
`LegacyShadowHandoff {trade_date, builder_outcome, diagnostic}` from the refresh orchestration to
the existing bounded shadow diagnostic channel, at most once per scheduler tick. It is not persisted
in the Universe sidecar, head, attempt or public status. A missing contract yields the typed
`NO_COMPARISON/CONTROL_STATE_UNAVAILABLE` result, never a blocked hook status. A diagnostic-channel
failure is sanitized and isolated; it cannot change the canonical result or `UniverseHookResult`.
The handoff consumes `builder_outcome`, `builder_diagnostic` and the sanitized observation, and
contains counts and hashes only; raw symbols exist only during the private comparison stack frame
and are never emitted to logs or storage.
The observer never filters, patches or changes the builder's default behavior, does not call
BaoStock, and is not a provider acquisition seam; the optional typed inspection argument is the
only additive seam.

`universe_raw_projection_sha256` is the domain hash
`stock-eva/r2f4.2/raw-batch-projection/v1` over the exact JSON result of
`batch.model_dump(mode="json", warnings="error")` after Pydantic revalidation of the complete
existing `ProviderRawBatch`; no field is excluded and no nested mapping is redefined. The hash is
internal only. Endpoint semantics are evaluated from object fields after validation, and the gate
returns only safe endpoint counts, loaded/session counts and this digest. A non-null
`failure_class` rejects the complete batch. No payload, request ID, provider session ID, page
identity or raw row is exposed by the gate.

The current legacy `validate_universe` callback remains wired exactly as-is; mode `off` is
unchanged and the new pure gate is not passed to it. In `shadow`, the callback captures the
prevalidation snapshot through the single inspection seam, passes it to the builder; after builder
rejection or canonical `ready`, `_offer_shadow` invokes the pure observer on the frozen preflight snapshot, then the service
consumes the sealed callback holder once and uses CandidateStore immutable bundle/readers to verify
`run_id`, `trade_date`, manifest/evidence references and lineage digest. Observer drift is a
private diagnostic only (never a `UniversePostSuccessHook` status or `BlockedReason`); a context
mismatch is `UNIVERSE_SESSION_DRIFT` for the hook and never changes the legacy result or mutates
the request.

## Data Models

### Code-level models

Implementation MUST realize the companion design models `UniverseContractV1`, `UniverseMemberV1`,
`UniverseCountsV1`, `InstrumentEvidenceV1`, `RequiredUserSymbolSnapshotV1`,
`UniverseStatusSnapshotV1` and `UniverseCandidateGate`. All models are strict, frozen, extra-forbid,
finite, safe-ID/hash validated and use the exact state vocabularies in the design.

`UniverseContractV1` has generated `contract_id`, `contract_sha256` and immutable `created_at`.
`payload_json` is the canonical `contract_preimage`, containing every semantic field including
`parent_contract_id`, sorted members, partitions, evidence and snapshot references, while
excluding only `contract_id`, `contract_sha256` and observational `created_at`. Compute
`contract_sha256 = domain_sha256("stock-eva/r2f4.2/universe-contract/v1", contract_preimage)` and
then derive `contract_id` as its first 32 lowercase hex characters. A reader recomputes both and
checks all DDL columns against the payload; generated values are never self-referential.

`SourceRefsV1` is a required frozen model (calendar generation ID/hash, classification generation
ID, its source/source-version/sequence/observed-at/source-date-semantics metadata, a newly computed exact-PIT classification snapshot digest,
required snapshot ID/hash, sorted instrument evidence IDs, semantic mapping hash, source-version digest,
provider, trade date, exact PIT cutoff
and source-date semantics). If the existing `GenerationSummary` lacks a source hash, the
implementation MUST NOT invent one. `UniverseContractV1` MUST persist and hash sorted-unique
`classification_evidence_ids`, `effective_main_board_ids` and `required_additions_ids`, with a
partition digest for each. Excluded classification evidence is durable and linked with an explicit
exclusion reason; the strict reader recomputes all three partitions from evidence links and member
roles rather than trusting only final members or counts.

`UniverseContractV1` also carries `source_state_id`, `source_state_sha256` and
`source_version_digest`; each is cross-checked against the latest persisted
`universe_source_state` row and the corresponding `SourceRefsV1` fields.

`UniverseSourceStateV1` is an immutable writer-produced sidecar row containing exactly
`source_state_id`, `source_state_sha256`, `trade_date`, `provider_id=baostock`,
`source_version_digest`, `source_refs_json` and trusted UTC `verified_at`. Its digest is
`domain_sha256("stock-eva/r2f4.2/universe-source-state/v1", {trade_date,provider_id,
source_version_digest,source_refs_json})`; `verified_at` is an observation/order column excluded
from identity; `source_state_id` is the first 32 lowercase
hex characters. The strict reader validates `source_refs_json` as `SourceRefsV1`, recomputes the
digest and selects the latest row for the requested date by `(verified_at,source_state_id)` from
this sidecar table only. Public GET never reads external classification, calendar, UserStore or
provider state; changes are invisible until a writer verifies and persists a new row, preserving
PIT snapshot semantics.

The source-state reader parses `source_refs_json` as the exact `SourceRefsV1` object and checks, field
by field, that its `provider_id`, `trade_date`, `source_version_digest`, `source_date_semantics`,
classification metadata, calendar metadata, required-snapshot identity and evidence/mapping
digests agree with the row and with the linked contract. `source_state_sha256` is recomputed from
the exact preimage above; `source_state_id` must equal its first 32 lowercase characters. Rows are
ordered by trusted UTC `verified_at`, with `source_state_id` as the deterministic tie-breaker.
An identical `(trade_date,source_version_digest,source_refs_json)` hash is idempotent: a later
revalidation with a different `verified_at` reuses the existing row and cannot make the head stale;
a different identity hash is a new immutable row and never an update. A result may reference a source state only after
this validation; a blocked/failed/indeterminate result may leave `source_state_id` null only when
construction was impossible, while a succeeded result must reference a verified row. This
nullable FK is the sole permitted optional source-state link.

The builder MUST retain three separately accounted layers: (1) every unique promoted PIT
`classification_evidence` identity, including excluded records; (2) the
`effective_main_board` subset using `list_date <= trade_date < delist_date` (or no delist
date); and (3) `required_additions`, the valid user A-share union after removing effective
main-board identities, plus each required index only when absent from that base; index obligations
remain explicit even when already represented.
Each layer has its own sorted identity list, count and SHA-256: the strict fields are
`classification_evidence_ids`, `effective_main_board_ids` and `required_additions_ids`, each with
its partition SHA-256 in the payload and explicit sidecar columns. Excluded classification
evidence is persisted with role/exclusion links and is rehashed by the reader. The final member hash is computed
from the union, with source-role overlap retained only in `UniverseMemberV1.scope_roles`.
The classification-evidence set is an evidence-domain projection and may refer to a symbol also in
a member set; the two member-domain partitions `effective_main_board_ids` and
`required_additions_ids` are disjoint by canonical symbol/security identity.
`symbol` is the sole member identity key and `security_id` maps one-to-one to it within a contract;
the DDL unique constraint and strict reader enforce this. Conflicting mappings are
`UNIVERSE_IDENTITY_CONFLICT` and block without merging or omission.
`not_yet_listed` and
`delisted` required additions remain visible but never enter `session_expected`.

`UniverseMemberV1` is hashed as a complete canonical projection with the digest field excluded:

| Field | Required projection value |
|---|---|
| `symbol` | canonical safe symbol; the sole member identity key |
| `security_id` | one-to-one security-master identity |
| `member_kind` | `stock` or `index`; index only for the two required indexes |
| `scope_roles` | sorted unique roles: `effective_main_board`, `required_user`, `required_index` |
| `exchange` / `board` | derived exchange and board identity, never caller-selected |
| `list_date` / `delist_date` | exact PIT listing window values, including nulls |
| `expected_trading_state` | `trading`, `suspended`, `not_yet_listed`, `delisted` or `unknown` |
| `st_state` | `yes`, `no`, `not_applicable` or `unknown` |
| `state_source` | safe source/semantic evidence identity |
| `effective_from` / `effective_to` | PIT effective window values, including nulls |
| `instrument_evidence_id` | reviewed evidence identity referenced by the member |
| `member_sha256` | `domain_sha256("stock-eva/r2f4.2/universe-member/v1", member_preimage)`; excluded from `member_preimage` |

The exact `member_preimage` is the JSON object of every row above except `member_sha256`, with
sorted `scope_roles`, explicit null values and canonical key ordering. The vector fixture MUST
include one effective main-board stock, one required user addition and the two required indexes,
and MUST assert that changing any state, role, evidence identity, listing boundary or null changes
the digest while reordering roles does not. The member digest is recomputed from `member_json` by
the strict reader; no abbreviated symbol-only digest is valid.

Use the exact design domains/projections: classification evidence is sorted
`{security_id,symbol,evidence_id,exclusion_reason}`; effective main-board is sorted complete
security/member projections; effective main-board and required additions both hash sorted complete
`UniverseMemberV1` projections (all fields except `member_sha256`) after applying their partition
filter, with required additions removing identities already in the effective main-board partition. Required
indexes are included at most once. Roles that overlap are retained only in `universe_member.scope_roles`,
and each partition count is the length of its sorted unique projection.

The implementation MUST add the reviewed semantic mapping registry described by the design. A
mapping has closed provider/source-schema values, versioned unique
`mapping_id=baostock-<source_schema>-<mapping_version>` (safe identifier alphabet),
`mapping_version` that MUST match both its payload and every evidence reference, date/PIT rules
and review status. An unmapped value or `requested_unverified` source is unknown and yields
`BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`; current BaoStock classification therefore cannot
qualify this contract.

Required-index tests MUST use only a versioned local `InstrumentEvidenceV1` fixture/input with
`authority_status=reviewed`, `artifact_origin=local_reviewed_fixture`, provider `baostock`, closed
`mapping_id`/`mapping_version`/`mapping_sha256` referencing the immutable semantic registry, exact `trade_date`, `index_role=required_index`, identity and
explicit `expected_trading_state=trading`. This fixture proves only derivation logic; it is not a
provider qualification claim. Production has no qualified artifact in this subversion and must
remain stably `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED`.

### Hash preimage registry

Every digest is domain-separated and recomputable from persisted or existing model fields.

| Domain | Canonical preimage |
|---|---|
| `stock-eva/r2f4.2/classification-snapshot/v1` | sorted exact PIT `(security_id,symbol,source_record_id,source_snapshot_date,listing_status,trade_status,st_state,exclusion_reason,source_date_semantics)` projection; never a GenerationSummary hash |
| `stock-eva/r2f4.2/instrument-evidence/v1` | complete reviewed `InstrumentEvidenceV1`, mapping hash and exact dates included |
| `stock-eva/r2f4.2/semantic-mapping/v1` | canonical semantic-mapping payload and identity/version/provider/schema |
| `stock-eva/r2f4.2/universe-contract/v1` | canonical `contract_preimage` in `payload_json`, including parent ID and all semantic fields, excluding only `contract_id`, `contract_sha256` and `created_at` |
| `stock-eva/r2f4.2/publication-lineage/v2` | exact current `_R2F2_LINEAGE_FIELDS` projection: `provider_id`, `universe_id`, `evidence_id`, `evidence_sha256`, `candidate_id`, `candidate_manifest_sha256`, `gate_report_sha256`, `adapter_version`, `source_schema_version`; no pre-existing digest is assumed |
| `stock-eva/r2f4.2/universe-publication-context/v1` | `{run_id,trade_date,manifest_ref,evidence_refs,publication_lineage_json,publication_lineage_sha256,status,created_at}` with sorted safe references; `context_sha256` hashes exactly this object and `context_id` is derived as the first 32 lowercase hex characters of `context_sha256`, both excluded from the preimage |
| `stock-eva/r2f4.2/universe-head/v1` | `{"singleton_id":1,"sequence":N,"contract_id":"...","contract_sha256":"..."}` |
| `stock-eva/r2f4.2/raw-batch-projection/v1` | exact `batch.model_dump(mode="json", warnings="error")` after Pydantic revalidation of complete `ProviderRawBatch`; no field is excluded or remapped |
| `stock-eva/r2f4.2/universe-source-version/v1` | exact object `{provider_id,adapter_version,endpoint_contract_version,classification:{source,source_version,sequence,observed_at,source_date_semantics},classification_snapshot_sha256,semantic_mapping_sha256,instrument_evidence:[sorted {evidence_id,evidence_sha256,mapping_id,mapping_sha256,source_version,source_date_semantics,trade_date}]}`; this is `source_version_digest` |
| `stock-eva/r2f4.2/universe-source-state/v1` | exact object `{trade_date,provider_id,source_version_digest,source_refs_json}` persisted in `universe_source_state`; `verified_at` is excluded from identity; `source_state_id=source_state_sha256[:32]` |
| `stock-eva/r2f4.2/universe-attempt-plan/v1` | complete immutable attempt key, source version, operation day, budget and hook identity |
| `stock-eva/r2f4.2/universe-attempt-result/v1` | terminal status, actual request count, closed reason and finish timestamp |
| `stock-eva/r2f4.2/user-rows/v1` | exactly `{"schema_version":1,"user_rows":[sorted exact table/column/value rows]}`; this is `snapshot_token` |
| `stock-eva/r2f4.2/required-symbol-snapshot/v1` | exactly `{"schema_version":1,"snapshot_token":snapshot_token,"symbols":[sorted {symbol,roles}]}`; this is `snapshot_sha256` |
| `stock-eva/r2f4.2/universe-member/v1` | complete `UniverseMemberV1` JSON projection with `member_sha256` excluded; this is the member vector digest |
| `stock-eva/r2f4.2/universe-partition/classification-evidence/v1` | sorted unique JSON rows `{security_id,symbol,evidence_id,exclusion_reason}` including excluded records |
| `stock-eva/r2f4.2/universe-partition/effective-main-board/v1` | sorted unique complete `UniverseMemberV1` JSON projections (excluding `member_sha256`) filtered to the effective main-board base |
| `stock-eva/r2f4.2/universe-partition/required-additions/v1` | sorted unique complete `UniverseMemberV1` JSON projections (excluding `member_sha256`) after base-set subtraction |

Plan/result preimages use strict UTC RFC-3339 timestamps with `Z`; `planned_sha256` excludes only
itself and includes `(trade_date,operation_day,canonical_run_id,source_version_digest,hook_kind,
refresh_id,request_budget,classification_max_attempts,created_at,attempt_status)`. `result_sha256`
excludes only itself and includes `(attempt_id,terminal_status,classification_request_count,
reason_code,source_state_id,finished_at)`, where `source_state_id` is null only before a source
state can be constructed. `created_at` and `finished_at` are UTC microsecond timestamps and are
not replaced by local time or wall-clock text. The required-user snapshot preimages are defined
only by the `user-rows/v1` and `required-symbol-snapshot/v1` registry rows above; every model and
reader MUST use those exact objects and MUST NOT introduce a second snapshot hash formula.
The field mapping is exact: `snapshot_token` and the persisted
`snapshot_token_digest` are the same 64-hex value
`domain_sha256("stock-eva/r2f4.2/user-rows/v1", {"schema_version":1,"user_rows":[sorted exact table/column/value rows]})`;
`required_symbol_snapshot_sha256`/`snapshot_sha256` is the projection digest
`domain_sha256("stock-eva/r2f4.2/required-symbol-snapshot/v1", {"schema_version":1,"snapshot_token":snapshot_token,"symbols":[sorted {symbol,roles}]})`.
There is no hash-of-hash or alternate field naming.

`attempt_id` is a generated safe identifier and `dedup_key` is the derived hash of the canonical
attempt key; both are excluded from the plan preimage except where `attempt_id` is explicitly part
of the result preimage. No other DDL field may be omitted, and no generated identifier may be
silently included. The canonical attempt object therefore has exactly the DDL plan fields plus
these explicitly derived identities and digest fields.

For a requested trade date, let `latest_terminal` be the newest immutable attempt-result row for
that date/source digest with terminal status `blocked`, `deferred`, `succeeded`, `failed` or
`ATTEMPT_INDETERMINATE`, ordered by `(finished_at DESC,attempt_id DESC)`; the safe immutable
`attempt_id` tie-breaker makes selection deterministic. Maintenance is due exactly when there is no
exact-date contract, or the current source/mapping/classification digest differs from that
contract, or the required-symbol snapshot differs from that contract, or
UTC `now >= latest_terminal.finished_at + configured_interval_seconds`. If trusted UTC `now <
finished_at`, or either timestamp is malformed/incomparable, the control state is unavailable.
This UTC rule survives restarts. An injected monotonic clock may guard elapsed time within one tick
only and MUST NOT be persisted or used as the due source. A same `(trade_date,operation_day)` row
and the one-fetch global ceiling always win: no version change can cause a second fetch during the
same Shanghai operation day. `operation_day` is captured once at attempt start and does not change
across midnight.

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
  snapshot_token: string;
}

capture_required_symbol_snapshot_existing(): RequiredUserSymbolSnapshotRead
```

Only the existing UserStore writer owner may call this API; public API/CLI has no writer
connection or initialization permission. It opens exactly one descriptor-bound, already-initialized
UserStore SQLite connection, verifies the allowlisted schema, issues `BEGIN IMMEDIATE` to obtain
writer exclusion, reads positions/watchlists/items in one deterministic SQL snapshot, and uses
exactly the `user-rows/v1` registry preimage for `snapshot_token` and the
`required-symbol-snapshot/v1` registry preimage for `snapshot_sha256`. `captured_at` is a UTC
RFC-3339 observation excluded from both identity hashes. `snapshot_id` is derived from
`snapshot_sha256`; the token proves source rows and the snapshot hash proves the symbol/role
projection, with no alternate formula. It commits to release the exclusion. Busy/locked,
unavailable, schema mismatch or row/hash mismatch MUST rollback and return
`USER_STORE_UNAVAILABLE`, with no retry or second observation; close is required on commit and
rollback. Capture MUST NOT initialize or migrate UserStore. The private sidecar stores only
normalized symbols, role bitset, token digest, count, capture time and hash; public API/CLI stores
only expose count/hash.

### Sidecar schema

| Table | Required identity and contents | Mutation rule |
|---|---|---|
| `universe_meta` | schema version, schema digest, deployment store identity, DB inode | immutable after writer-only bootstrap |
| `universe_source_state` | latest writer-verified source refs and `source_version_digest` per trade date, with immutable row digest | insert-only; strict reader selects latest verified row |
| `universe_contract` | complete canonical `payload_json`, explicit identity/sequence/parent/hash/source refs/counts, partition IDs/hashes, trade date and PIT cutoff | insert-only |
| `universe_member` | contract hash, unique symbol, member payload/hash | insert-only |
| `universe_instrument_evidence` | evidence ID/hash, BaoStock mapping/source/date projection | insert-only |
| `universe_required_symbol_snapshot` | snapshot ID/hash, canonical token digest, count, private role projection | insert-only |
| `universe_publication_context` | immutable prior successful canonical run/date, safe manifest/evidence refs, new lineage digest and context hash used as maintenance input (not a status cache) | insert-only; immutable input |
| `universe_semantic_mapping` | immutable BaoStock semantic mapping payload/hash and review status | insert-only |
| `contract_evidence` | bidirectional contract-to-evidence links | insert-only; every link is transactionally validated |
| `contract_required_snapshot` | one bidirectional contract-to-required-snapshot link and hash | insert-only; exactly one per contract |
| `universe_attempt` | immutable post-success dedup plan, refresh/date identity and request budget | insert-only; one per dedup key |
| `universe_attempt_result` | immutable terminal status/reason and actual request count | insert-only; zero or one per plan |
| `universe_head` | singleton 1, contract ID/hash, sequence, head hash | one writer-only transactional CAS update |

The exact DDL is the design's normative v1 DDL; implementation MUST checksum its canonical DDL as
`schema_digest`, set `PRAGMA user_version=1`, `journal_mode=WAL`, `synchronous=FULL` and
`foreign_keys=ON`, and use a bounded busy timeout. It MUST hold an exclusive sidecar lock plus the
existing refresh/control lock. Source-state, contract, evidence, snapshot, member, role-link and
head CAS rows are inserted/updated in one SQLite transaction; CAS failure rolls back the entire promotion. Only
`universe_head` updates, and only by CAS on sequence and contract hash; all other tables are
append-only with update/delete rejection. A CAS conflict is recorded as a durable terminal
`UNIVERSE_HEAD_CAS_CONFLICT` attempt when that result transaction is provable; a readable sidecar
then exposes blocked, never an old head as ready. Failure to prove that result transaction or any
schema/path/inode/hash corruption uses the independent control-error/HTTP-503 boundary.
The strict reader
checks descriptor-bound path/inode, store identity, schema/table allowlist, schema digest, all row
hashes, sequence-parent chain and head hash in one read transaction without DDL/DML. v1 writer
initialization is the only initialization path; there is no legacy migration in this release.
Crash before commit leaves the old head and rolls back the transaction. A CAS conflict rolls back
promotion; when its terminal result is durably committed, the readable sidecar maps it to blocked
`UNIVERSE_HEAD_CAS_CONFLICT` without retry or head mutation.

The implementation MUST use this normative v1 DDL (the canonical whitespace-normalized text is
the schema-digest preimage):

```sql
PRAGMA user_version = 1;
CREATE TABLE universe_meta (
    meta_key TEXT PRIMARY KEY CHECK
    (meta_key IN ('schema_version','schema_digest','store_id','db_inode')),
    meta_value TEXT NOT NULL
);
CREATE TABLE universe_source_state (
    source_state_id TEXT PRIMARY KEY, source_state_sha256 TEXT NOT NULL UNIQUE,
    trade_date TEXT NOT NULL, provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
    source_version_digest TEXT NOT NULL, source_refs_json TEXT NOT NULL, verified_at TEXT NOT NULL
);
CREATE TABLE universe_contract (
    contract_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE CHECK (sequence > 0),
    parent_contract_id TEXT REFERENCES universe_contract(contract_id), trade_date TEXT NOT NULL,
    universe_id TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    scope TEXT NOT NULL, calendar_generation_id TEXT NOT NULL, calendar_sha256 TEXT NOT NULL,
    classification_generation_id TEXT NOT NULL, classification_generation_sequence INTEGER NOT NULL,
    classification_source TEXT NOT NULL, classification_source_version TEXT NOT NULL,
    classification_source_snapshot_date TEXT NOT NULL, classification_observed_at TEXT NOT NULL,
    classification_snapshot_sha256 TEXT NOT NULL, exact_pit_cutoff TEXT NOT NULL,
    provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
    source_date_semantics TEXT NOT NULL CHECK
    (source_date_semantics IN ('source_observed','requested_unverified')),
    source_state_id TEXT NOT NULL REFERENCES universe_source_state(source_state_id),
    source_state_sha256 TEXT NOT NULL, source_version_digest TEXT NOT NULL,
    required_symbol_snapshot_id TEXT NOT NULL, required_symbol_snapshot_sha256 TEXT NOT NULL,
    instrument_evidence_ids_json TEXT NOT NULL, counts_json TEXT NOT NULL,
    layer_counts_json TEXT NOT NULL, source_refs_json TEXT NOT NULL,
    classification_evidence_ids_json TEXT NOT NULL,
    classification_evidence_partition_sha256 TEXT NOT NULL,
    effective_main_board_ids_json TEXT NOT NULL,
    effective_main_board_partition_sha256 TEXT NOT NULL, required_additions_ids_json TEXT NOT NULL,
    required_additions_partition_sha256 TEXT NOT NULL, payload_json TEXT NOT NULL,
    contract_sha256 TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
);
CREATE TABLE universe_member (
    contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id), symbol TEXT NOT NULL,
    security_id TEXT NOT NULL, member_sha256 TEXT NOT NULL, member_json TEXT NOT NULL,
    PRIMARY KEY (contract_id, symbol), UNIQUE (contract_id, security_id)
);
CREATE TABLE universe_semantic_mapping (
    mapping_id TEXT PRIMARY KEY, mapping_version TEXT NOT NULL,
    provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'), source_schema TEXT NOT NULL,
    payload_json TEXT NOT NULL, mapping_sha256 TEXT NOT NULL UNIQUE,
    authority_status TEXT NOT NULL CHECK (authority_status IN ('reviewed','unqualified'))
);
CREATE TABLE universe_instrument_evidence (
    evidence_id TEXT PRIMARY KEY, evidence_sha256 TEXT NOT NULL UNIQUE,
    provider_id TEXT NOT NULL CHECK (provider_id = 'baostock'),
    authority_status TEXT NOT NULL CHECK (authority_status IN ('reviewed','unqualified')),
    artifact_origin TEXT NOT NULL CHECK
    (artifact_origin IN ('local_reviewed_fixture','production_reviewed_artifact')),
    security_id TEXT NOT NULL, symbol TEXT NOT NULL, mapping_id TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    mapping_sha256 TEXT NOT NULL REFERENCES universe_semantic_mapping(mapping_sha256),
    source_snapshot_date TEXT NOT NULL, evidence_trade_date TEXT NOT NULL,
    exclusion_reason TEXT NOT NULL,
    index_role TEXT NOT NULL CHECK (index_role IN ('required_index','not_applicable')),
    source_date_semantics TEXT NOT NULL CHECK
    (source_date_semantics IN ('source_observed','requested_unverified')),
    evidence_json TEXT NOT NULL, adapter_version TEXT NOT NULL,
    source_schema TEXT NOT NULL, security_type TEXT NOT NULL CHECK
    (security_type IN ('stock','index','fund','etf','bond','other')),
    exchange TEXT, board TEXT, list_date TEXT, delist_date TEXT,
    listing_status TEXT NOT NULL, daily_trade_status TEXT,
    suspension_state TEXT NOT NULL CHECK
    (suspension_state IN ('trading','suspended','not_supplied','unknown')),
    st_state TEXT NOT NULL CHECK (st_state IN ('yes','no','not_applicable','unknown')),
    expected_trading_state TEXT CHECK
    (expected_trading_state IS NULL OR expected_trading_state IN
    ('trading','suspended','not_yet_listed','delisted','unknown')),
    observed_at TEXT, lineage_hash TEXT
);
CREATE TABLE universe_required_symbol_snapshot (
    snapshot_id TEXT PRIMARY KEY, snapshot_sha256 TEXT NOT NULL UNIQUE,
    snapshot_token_digest TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    symbol_count INTEGER NOT NULL CHECK (symbol_count >= 0), snapshot_json TEXT NOT NULL
);
CREATE TABLE universe_publication_context (
    context_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, trade_date TEXT NOT NULL,
    manifest_ref TEXT NOT NULL, evidence_refs_json TEXT NOT NULL,
    publication_lineage_json TEXT NOT NULL,
    publication_lineage_sha256 TEXT NOT NULL, context_sha256 TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status = 'ready'), created_at TEXT NOT NULL,
    UNIQUE (run_id, trade_date)
);
CREATE TABLE contract_evidence (
    contract_id TEXT NOT NULL REFERENCES universe_contract(contract_id),
    evidence_id TEXT NOT NULL REFERENCES universe_instrument_evidence(evidence_id),
    evidence_role TEXT NOT NULL CHECK
    (evidence_role IN ('classification_evidence','effective_main_board',
                       'required_additions','member')),
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
    hook_kind TEXT NOT NULL CHECK (hook_kind = 'universe_post_success'), refresh_id TEXT NOT NULL,
    canonical_run_id TEXT NOT NULL, source_version_digest TEXT NOT NULL, trade_date TEXT NOT NULL,
    operation_day TEXT NOT NULL,
    attempt_status TEXT NOT NULL CHECK (attempt_status = 'RUNNING'),
    request_budget INTEGER NOT NULL CHECK (request_budget = 1),
    classification_max_attempts INTEGER NOT NULL CHECK (classification_max_attempts = 1),
    created_at TEXT NOT NULL, planned_sha256 TEXT NOT NULL UNIQUE,
    UNIQUE (trade_date, operation_day)
);
CREATE TABLE universe_attempt_result (
    attempt_id TEXT PRIMARY KEY REFERENCES universe_attempt(attempt_id),
    terminal_status TEXT NOT NULL CHECK
    (terminal_status IN ('blocked','deferred','succeeded','failed','ATTEMPT_INDETERMINATE')),
    classification_request_count INTEGER NOT NULL CHECK (classification_request_count IN (0,1)),
    reason_code TEXT NOT NULL CHECK (reason_code IN
    ('CONTROL_STATE_UNAVAILABLE','PIT_VISIBILITY_INVALID','CALENDAR_UNAVAILABLE',
     'CALENDAR_CONFLICT','CLASSIFICATION_UNAVAILABLE','USER_STORE_UNAVAILABLE',
     'BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED','PIT_CUTOFF_VIOLATION','USER_SNAPSHOT_CHANGED',
     'REQUIRED_INDEX_NOT_TRADING','REQUIRED_SYMBOL_INVALID','UNIVERSE_UNKNOWN_NONZERO',
     'UNIVERSE_COUNT_MISMATCH','UNIVERSE_MISSING_SYMBOL','UNIVERSE_EXTRA_SYMBOL',
     'UNIVERSE_DUPLICATE_SYMBOL','UNIVERSE_SESSION_DRIFT','UNIVERSE_STATE_MISMATCH',
     'UNIVERSE_SOURCE_VERSION_CHANGED','DATE_MISMATCH','UNIVERSE_STORAGE_UNAVAILABLE',
     'UNIVERSE_SCHEMA_MISMATCH','UNIVERSE_HEAD_CAS_CONFLICT','BLOCKED_ENFORCE_NOT_ENABLED',
     'BLOCKED_PRODUCTION_MODE_OFF','LEGACY_SHADOW_DRIFT','ATTEMPT_INDETERMINATE',
     'UNIVERSE_IDENTITY_CONFLICT','NONE')),
    source_state_id TEXT REFERENCES universe_source_state(source_state_id),
    finished_at TEXT NOT NULL, result_sha256 TEXT NOT NULL UNIQUE
);
CREATE TABLE universe_head (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1), sequence INTEGER NOT NULL,
    contract_id TEXT NOT NULL, contract_sha256 TEXT NOT NULL, head_sha256 TEXT NOT NULL,
    updated_at TEXT NOT NULL, FOREIGN KEY (contract_id) REFERENCES universe_contract(contract_id)
);
CREATE TRIGGER universe_head_no_delete BEFORE DELETE ON universe_head
BEGIN SELECT RAISE(ABORT,'immutable_head'); END;
CREATE TRIGGER universe_head_update_guard BEFORE UPDATE ON universe_head
WHEN NEW.singleton_id != OLD.singleton_id OR NEW.sequence != OLD.sequence + 1
     OR NEW.contract_id = OLD.contract_id
BEGIN SELECT RAISE(ABORT,'invalid_head_transition'); END;
CREATE TRIGGER universe_meta_no_update BEFORE UPDATE ON universe_meta
BEGIN SELECT RAISE(ABORT,'immutable_meta'); END;
CREATE TRIGGER universe_meta_no_delete BEFORE DELETE ON universe_meta
BEGIN SELECT RAISE(ABORT,'immutable_meta'); END;
CREATE TRIGGER universe_source_state_no_update BEFORE UPDATE ON universe_source_state
BEGIN SELECT RAISE(ABORT,'immutable_source_state'); END;
CREATE TRIGGER universe_source_state_no_delete BEFORE DELETE ON universe_source_state
BEGIN SELECT RAISE(ABORT,'immutable_source_state'); END;
CREATE TRIGGER universe_contract_no_update BEFORE UPDATE ON universe_contract
BEGIN SELECT RAISE(ABORT,'immutable_contract'); END;
CREATE TRIGGER universe_contract_no_delete BEFORE DELETE ON universe_contract
BEGIN SELECT RAISE(ABORT,'immutable_contract'); END;
CREATE TRIGGER universe_member_no_update BEFORE UPDATE ON universe_member
BEGIN SELECT RAISE(ABORT,'immutable_member'); END;
CREATE TRIGGER universe_member_no_delete BEFORE DELETE ON universe_member
BEGIN SELECT RAISE(ABORT,'immutable_member'); END;
CREATE TRIGGER universe_mapping_no_update BEFORE UPDATE ON universe_semantic_mapping
BEGIN SELECT RAISE(ABORT,'immutable_mapping'); END;
CREATE TRIGGER universe_mapping_no_delete BEFORE DELETE ON universe_semantic_mapping
BEGIN SELECT RAISE(ABORT,'immutable_mapping'); END;
CREATE TRIGGER universe_evidence_no_update BEFORE UPDATE ON universe_instrument_evidence
BEGIN SELECT RAISE(ABORT,'immutable_evidence'); END;
CREATE TRIGGER universe_evidence_no_delete BEFORE DELETE ON universe_instrument_evidence
BEGIN SELECT RAISE(ABORT,'immutable_evidence'); END;
CREATE TRIGGER universe_snapshot_no_update BEFORE UPDATE ON universe_required_symbol_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_snapshot'); END;
CREATE TRIGGER universe_snapshot_no_delete BEFORE DELETE ON universe_required_symbol_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_snapshot'); END;
CREATE TRIGGER universe_publication_context_no_update BEFORE UPDATE ON universe_publication_context
BEGIN SELECT RAISE(ABORT,'immutable_publication_context'); END;
CREATE TRIGGER universe_publication_context_no_delete BEFORE DELETE ON universe_publication_context
BEGIN SELECT RAISE(ABORT,'immutable_publication_context'); END;
CREATE TRIGGER contract_evidence_no_update BEFORE UPDATE ON contract_evidence
BEGIN SELECT RAISE(ABORT,'immutable_contract_evidence'); END;
CREATE TRIGGER contract_evidence_no_delete BEFORE DELETE ON contract_evidence
BEGIN SELECT RAISE(ABORT,'immutable_contract_evidence'); END;
CREATE TRIGGER contract_snapshot_no_update BEFORE UPDATE ON contract_required_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_contract_snapshot'); END;
CREATE TRIGGER contract_snapshot_no_delete BEFORE DELETE ON contract_required_snapshot
BEGIN SELECT RAISE(ABORT,'immutable_contract_snapshot'); END;
CREATE TRIGGER universe_attempt_no_update BEFORE UPDATE ON universe_attempt
BEGIN SELECT RAISE(ABORT,'immutable_attempt'); END;
CREATE TRIGGER universe_attempt_no_delete BEFORE DELETE ON universe_attempt
BEGIN SELECT RAISE(ABORT,'immutable_attempt'); END;
CREATE TRIGGER universe_attempt_result_no_update BEFORE UPDATE ON universe_attempt_result
BEGIN SELECT RAISE(ABORT,'immutable_attempt_result'); END;
CREATE TRIGGER universe_attempt_result_no_delete BEFORE DELETE ON universe_attempt_result
BEGIN SELECT RAISE(ABORT,'immutable_attempt_result'); END;

```

The writer MUST reject any pre-existing non-v1 database; no legacy migration is implemented.
An empty initialized store has no `universe_head` row; first promotion inserts sequence 1 with a
null parent. `payload_json` is the complete canonical UniverseContractV1 preimage, including
sorted members, required indexes, evidence IDs, snapshot hash, all counts/layer counts, source
refs, PIT fields and all three partition IDs/hashes. Its digest and all explicit identity/source/
count/partition columns must match the decoded payload. The strict reader verifies each evidence
mapping ID/version/hash against the immutable semantic registry, then reconstructs the contract
from payload and re-hashes every member row and every persisted excluded evidence row,
checks exact role sets in bidirectional `contract_evidence`: excluded classification evidence is
`{classification_evidence}`; effective main-board member is
`{classification_evidence,effective_main_board,member}`; required non-index addition is
`{classification_evidence,required_additions,member}`; required index is
`{required_additions,member}`. For every role link, `security_id`, `symbol` and `exclusion_reason`
must equal the evidence payload. It also checks exact `contract_required_snapshot` links,
and rejects every invalid link. Explicit provider, source-date, required-snapshot and evidence-ID
columns must equal `SourceRefsV1` and their payload counterparts. It verifies
`head.sequence == contract.sequence`, `sequence=1` parent null, later sequence = parent+1, and
`head_sha256` over canonical `{"singleton_id":1,"sequence":N,"contract_id":"...",
"contract_sha256":"..."}`. CAS is conditional on the expected sequence/hash, with insert only
on empty head. All source-state/contract/evidence/snapshot/member/link/head changes share this transaction;
a failed CAS rolls back the complete transaction. DDL
normalization is the ordered statement text from the DDL block (comments
removed, whitespace collapsed outside quoted literals, whitespace removed immediately inside/around
`(`, `)` and `,`, quoted content preserved); the exact digest
is `domain_sha256("stock-eva/r2f4.2/universe-schema/v1", normalized_ddl)` and is stored as
`schema_digest`.

Before a non-run tick can use `universe_publication_context`, the strict reader recomputes
`context_sha256` with domain `stock-eva/r2f4.2/universe-publication-context/v1` over the exact
`{run_id,trade_date,manifest_ref,evidence_refs,publication_lineage_json,
publication_lineage_sha256,status,created_at}` object, checks sorted safe references and revalidates
the nine-field lineage projection through CandidateStore. This row is an immutable input record,
not a cached status/head; an invalid or unreachable row makes the store unavailable. The reader
may select only the newest valid row by `(created_at,context_id)` and never reconstruct missing
canonical data from it.

Reachability is strict: every contract, member, evidence, role-link and required-snapshot row
must be reachable from the current `universe_head` parent chain and verified in that chain. The
semantic-mapping registry, publication-context input, and attempt/attempt-result rows are explicit
allowed global sets, not head-chain rows; source-state rows are another explicit immutable global
set selected by requested trade date; every source-state payload/digest, mapping/context payload/hash and every attempt
key/plan/result reference is still validated bidirectionally, and any invalid global row makes the
store unavailable. A context is accepted only when its run/date, safe refs, lineage projection and
`context_sha256` rehash exactly; `context_id` must equal `context_sha256[:32]` (first 32 lowercase hex characters); no
unreachable candidate or orphan row is tolerated.
The strict reader verifies path/inode, store identity, schema/table allowlist, WAL/full-sync mode,
all payload/member/link/evidence hashes and parent chain in one read-only transaction. It never
creates a directory, DDL, DML, WAL/SHM file or migration. A crash leaves the old head or no new
state; corruption is `unavailable`, never a historical cached projection. It also validates
one immutable `universe_attempt` plan per dedup key, at most one terminal result, and actual
classification requests not exceeding one. A successful result MUST have a non-null
`source_state_id` whose row and digest verify; blocked/deferred/failed or
`ATTEMPT_INDETERMINATE` results may have null `source_state_id` only when no source state was
constructible and remain bound to the immutable plan's source digest. A valid `RUNNING` plan
without a result derives `blocked/ATTEMPT_INDETERMINATE`; only a missing/duplicate plan, altered
plan/result hash or illegal state is `UNIVERSE_SCHEMA_MISMATCH`/unavailable.

Result-state invariants are closed: `succeeded` requires `reason_code=NONE` and a verified non-null
`source_state_id`; `ATTEMPT_INDETERMINATE` requires the same reason; `blocked`, `deferred` and
`failed` require a non-`NONE` reason from the applicable mapping. A source-state FK on a non-success
result is permitted only when the state was successfully constructed for that attempt and its
provider/trade-date/source-version fields equal the immutable plan; otherwise it MUST be null.
An initialized empty store with no head row is also `unavailable`; no historical cached status is
retained.

### Configuration additions

Additive settings are required for the isolated sidecar and mode:

| Setting | Default | Constraint |
|---|---|---|
| `universe_contract_database_name` | `market_universe.sqlite3` | local `.sqlite3` basename, distinct from all control DBs |
| `market_universe_mode` | `off` | closed enum `off`, `shadow`, `enforce`; production profile accepts only `off` |
| `market_universe_maintenance_enabled` | `false` | strict boolean, no effect on legacy canonical path when false |
| `market_universe_maintenance_interval_seconds` | `86400` | integer in inclusive `[60,604800]`; trusted UTC `finished_at` is the persisted due source; no retry-loop setting |

The interval is read once at the start of each scheduler tick and invalid values return
`CONTROL_STATE_UNAVAILABLE` with zero provider requests. Due checks use trusted UTC
`now >= latest_terminal.finished_at + interval_seconds`, which survives restart; trusted UTC
`now < finished_at` or malformed/incomparable timestamp is `CONTROL_STATE_UNAVAILABLE`. The
injected monotonic clock is only a same-tick elapsed guard and never the persisted due source.
Tests inject deterministic UTC and monotonic clocks and cover interval-minus-one, exact interval,
restart and clock-before-finished boundaries.

Adding the database name to `Settings` and `StorageLayout` is an implementation necessity even
though the older Task 16 file list omitted those two files; it keeps path validation and private
control layout centralized.

`Settings` fields and wiring are normative: add `universe_contract_database_name` (default
`market_universe.sqlite3`), `market_universe_mode` (default `off`),
`market_universe_maintenance_enabled` (default `false`) and
`market_universe_maintenance_interval_seconds` (default `86400`, inclusive `[60,604800]`).
`StorageLayout` resolves the validated basename and rejects collisions with existing control DBs;
it does not accept a free-form path. `main.py` and the automation CLI construct one direct
`CanonicalRefreshCallable` and one `UniversePostSuccessHook` through the same factory and inject
both into `MarketAutomationService`; the read-only `market-universe` CLI uses the same
settings/layout projection and constructs no writer/provider. Add tests for defaults/invalid range,
basename collision, main/automation-CLI injection identity and zero-write status.
When `market_universe_maintenance_enabled=false`, the scheduler MUST make no maintenance
invocation: it MUST not call `_offer_universe_maintenance` or `UniversePostSuccessHook`, capture a
required-symbol snapshot, initialize/read/write the maintenance sidecar, construct a classification
provider, or issue an observer/provider request. Each tick returns a disabled decision with
`action=none`, `reason_code=NONE`, `provider_requests=0` and `writes_canonical=false`; legacy
canonical refresh and its existing post-publish/shadow behavior remain unchanged. This flag does
not change an explicitly requested read-only status projection, which remains subject to the
status contract above.
The profile/mode matrix is evaluated only after this enabled check; the disabled short-circuit
cannot emit `B_P` or `B_E`.

## Implementation Sequence

### Step 0 — Freeze and RED inventory

1. Confirm the worktree is at the stated baseline and clean before implementation changes.
2. Read and record SHA-256 for R2-F2 golden objects, the protected R2-F3 files, R2-F4.0
   `failover.py`/vectors and R2-F4.1 calendar/runtime files.
3. Add no code until the companion design has passed independent SPEC review. Generate spec test
   extractor output only into an ignored scratch directory; do not commit `NotImplementedError`
   stubs.
4. Create `tests/test_market_universe.py` with RED cases for AC-1 through AC-20 and all ECs. Use
   network-failing fakes and temporary roots; align failover compatibility coverage with the
   umbrella target `tests/test_market_failover_readiness.py` (the baseline readiness file is retained only
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
   sidecar replacement, empty-head behavior, bidirectional link validation, safe-mode checks and
   restart behavior. Include source-state/result lifecycle regressions for nullable blocked
   results, successful-result non-null enforcement, same-hash idempotence, verified-at tie
   ordering, source-ref column equality and rollback of source-state/result/contract/head as one
   transaction.

### Step 3 — PIT builder and evidence binding

1. Add a writer-only builder that pins one calendar snapshot and one classification snapshot at the
   operation boundary.
2. Add the exact writer-only `capture_required_symbol_snapshot_existing()` transaction using one
   existing connection, `BEGIN IMMEDIATE`, and only the allowlisted UserStore position/watchlist
   tables. Commit to release writer exclusion, record roles/count/hash privately, and do not put names in public output.
3. Bind promoted classification generation, instrument evidence IDs, source-date semantics and
   R2-F4.1 calendar generation/hash. Reject future/degraded/requested-unverified critical evidence.
4. Build stock and index members, calculate states/counts and promote only with unknown-zero and
   all evidence checks.
5. Add tests for PIT boundaries, listing/delisting, ST/suspension, index evidence, dynamic counts,
   unsupported required symbols, classification generation drift, single-connection capture and
   busy/locked rollback; no second read, data-version comparison or revision API is permitted.

### Step 4 — Candidate validation and staged legacy seam

1. Implement the pure `validate_provider_raw_batch(batch: ProviderRawBatch,
   contract: UniverseContractV1) -> UniverseCandidateGate`, aggregating existing endpoint batches,
   completions, lineage and pages; bind BaoStock, request trade date and refresh/session/request
   identity before Normalize.
2. Preserve existing `ProviderRequest`, `DailyBar`, `RefreshResult`, Normalize, quality-gate,
   evidence, manifest and pointer behavior in mode `off`; `off` MUST NOT invoke the validator.
3. In `shadow`, use a read-only observer after successful canonical/evidence publish; compare only
   legacy BaoStock counts/hash/reason and record drift without blocking or mutating canonical.
   Missing required ChiNext/STAR in the legacy request is drift only; use only the optional
   inspection input and preserve the builder's default `build_canonical_raw_request` behavior.
4. Add the private optional `MainBoardInspection` argument to `build_canonical_raw_request` and
   have the callback call `inspect_legacy_main_board_input` exactly once, passing the same object
   to the builder; on acceptance snapshot the built `ProviderRequest.session_symbols`, and on
   rejection snapshot inspection main-board symbols plus `("sh.000001", "sz.399001")`. Assign
   `builder_outcome` and sanitized counts/hash `builder_diagnostic` in the builder try/except;
   keep the omitted-argument path byte/behavior compatible and keep raw symbols stack-private.
5. Apply the mode/profile matrix before any provider construction: production non-`off` returns
   `B_P` and nonproduction/staging `enforce` returns `B_E`, both with zero Universe-provider
   requests while the legacy canonical path remains unchanged. The callback is not an enforce seam
   in this release; no production path may route through it and no secondary provider is accepted.
6. Add tests proving missing/extra/duplicate/non-session/mixed-provider batches, invalid
   `tradestatus`/suspended placeholders and session drift reject as a whole before Normalize, and
   that no symbol-level source stitching is possible.
7. Add the legacy inspection seam regressions: one callback inspection call whose object identity
   is consumed by the builder, omitted optional input preserving default compatibility, and
   builder rejection yielding only count/hash private diagnostics with raw-symbol privacy.

### Step 5 — Automation maintenance priority

1. Document/test the existing priority decision in `backend/app/market/automation.py` as
   continuity/repair before freshness execution, then post-success shadow; append exactly one
   lowest-priority `_offer_universe_maintenance` call to the same `run_due_once` tick. Universe is
   not a new scheduler lane.
2. Preserve the real boundaries: `_plan_continuity` and its repair lease run before the existing
   refresh lease; `_execute_due` owns only the existing refresh lease; post-publish and shadow hooks
   run after that lease. Repair execution, refresh-running/lock-busy state or an active error
   suppresses the maintenance call; a non-run/no-error tick may still evaluate cadence.
3. Offer one read-only legacy BaoStock shadow comparison from the callback-captured prevalidation
   snapshot at the existing `_offer_shadow` boundary, including builder rejection; after a
   canonical refresh succeeds it is still read-only, never a retry and never runs alongside
   Universe construction. Reuse existing refresh,
   shadow and evidence identities for deduplication; do not invent an all-lane request ledger.
   On non-run ticks the maintenance hook reads only the strictly verified immutable sidecar
   publication-context input, never a MarketStore reader or cached head.
4. Only after existing post-publish and shadow hooks return may the bounded hook consume a fresh
   callback context; absent that context it uses the persisted sidecar input for cadence planning.
   It reads promoted evidence, captures the UserStore snapshot once, and attempts one sidecar
   promotion. Current BaoStock `requested_unverified` evidence blocks before provider acquisition.
5. Implement typed `UniversePostSuccessHook.offer(trade_date: str, now: str,
   context: UniversePublicationContext | None)`. `_offer_universe_maintenance` invokes it once at
   the end of each eligible `run_due_once` tick. A fresh sealed context is passed only after
   post-publish and shadow; a non-run/no-error tick passes `context=None`, and the hook reads the
   latest strictly verified immutable sidecar publication-context input for the exact trade date,
   deferring when absent or mismatched. The service consumes the sealed callback exactly once after
   CandidateStore immutable readers verify publication lineage and evidence references, then passes
   the context to the concrete `UniverseMaintenanceService`; the service never exposes a generic
   callable fallback. `main.py` and the automation CLI construct the same strict service wiring; an
   enabled lane without that exact service returns explicit control-unavailable/blocking status.
   The read-only `market-universe` CLI only projects status.
   `RefreshResult` and MarketStore schema are unchanged. It must reacquire the existing
   `RefreshRunLock` non-blocking,
   then the sidecar lock. A missing or `None` configured `lock_path` returns
   `DEFER/CONTROL_STATE_UNAVAILABLE` before any path synthesis or provider construction; any other
   lock failure has the same result. Under the sidecar lock insert the immutable
   `universe_publication_context` input (when a fresh sealed context is supplied) and
   `universe_attempt` plan before any classification request. Context validation, dedup lookup and
   insert share one `BEGIN IMMEDIATE` transaction that commits before the request. Use
   `dedup_key=sha256(canonical {trade_date, operation_day, canonical_run_id, source_version_digest})`.
   For a fresh or persisted context, first require `context.run_id == refresh_id`, then set
   `canonical_run_id = refresh_id = context.run_id`; all three are sourced from the context, never
   from a nullable hook argument. A context whose run/date does not exactly match the requested trade
   date returns `DEFER/CONTROL_STATE_UNAVAILABLE`.
   `source_version_digest` is the domain hash
   `stock-eva/r2f4.2/universe-source-version/v1` over the exact registry object: provider/adapter/
   endpoint-contract versions; classification
   `{source,source_version,sequence,observed_at,source_date_semantics}`;
   `classification_snapshot_sha256`; `semantic_mapping_sha256`; and sorted instrument-evidence
   tuples `{evidence_id,evidence_sha256,mapping_id,mapping_sha256,source_version,source_date_semantics,trade_date}`. Due only when no row has that
   key and no exact contract exists, its classification snapshot/mapping/source-version digest
   changed, or UTC `now` is at least the latest terminal `finished_at` plus the configured
   `market_universe_maintenance_interval_seconds`. A global ceiling permits only one classification fetch per
   `(trade_date,operation_day)`, so a version change cannot bypass same-day budget; the same key is
   never retried that day. `request_budget=1`, `classification_max_attempts=1`, and one immutable
   terminal result. The lifecycle is strict: before any provider request, preflight computes the source digest from
   the existing mapping/instrument/classification contract and the writer transaction inserts the
   immutable attempt plan and occupies the `(trade_date,operation_day)` slot. After the six-call
   fetch, validated classification evidence constructs `UniverseSourceStateV1`. If construction
   fails before a source state exists, a second transaction preserves the attempt result with
   `source_state_id=NULL` and no head change. If source state construction succeeds but contract
   build fails, that state and the terminal result commit together without changing the head. Only
   a successful contract commits source state, result, complete contract/evidence/member/link rows
   and head CAS in one transaction. Duplicate identical source-state hash rows are idempotent;
   `verified_at` ties are ordered by `source_state_id`, and every source-state column must equal the
   validated `SourceRefsV1` provider/trade-date/digest fields.
   A duplicate key is no-op/defer, not a resend; unqualified evidence records stable blocked with
   zero requests. The dedicated classification provider factory is constructed with
   `max_attempts=1`. In `backend/app/classification/provider.py`, implement
   `make_baostock_classification_provider` for this hook; each due attempt calls it once and it
   constructs a fresh `BaoStockClassificationProvider`/`BaoStockProvider` with `client=None`, a
   new provider session, and no canonical-session reuse. The factory signature is
   `make_baostock_classification_provider(*, client=None, session_factory,
   max_attempts=1) -> BaoStockClassificationProvider`; it passes the fresh `session_factory` and
   literal `max_attempts=1` into the provider constructor and rejects an override above one. The
   classification constructor correspondingly accepts `session_factory` and `max_attempts` as
   keyword-only wiring while preserving legacy default construction. Pass `max_attempts=1` through
   to that
   session, so each of the six data endpoints is attempted at most once within the one fetch.
   One budget unit is one logical provider.fetch. Its fixed six logical calls are
   `query_all_stock`, `query_stock_basic`, `query_stock_industry`, `query_hs300_stocks`,
   `query_sz50_stocks` and `query_zz500_stocks`; login and logout are lifecycle transport audits,
   not data endpoints or fetch units. The logical acquisition budget is exactly one provider.fetch
   comprising those six calls. Actual endpoint/transport requests
   remain bounded and audited by the existing contract. `operation_day` is the `Asia/Shanghai` clock
   date captured at attempt start and
   remains fixed across midnight. Hash the complete plan with domain
   `stock-eva/r2f4.2/universe-attempt-plan/v1` and the terminal result with
   `stock-eva/r2f4.2/universe-attempt-result/v1`; the reader recomputes both and rejects altered,
   missing or duplicate events. A committed `RUNNING` plan with no result is
   `ATTEMPT_INDETERMINATE`, not retried on that Shanghai operation day; the next Shanghai day has
   a new key. Do not add a sixth LaunchAgent, independent scheduler lane or additional scheduler lock. Add concurrency,
   restart, deferred, priority, post-success ordering, failure and no-repeat tests covering
   `classification/sync.py`, `classification/store.py`, `backend/app/market/automation.py` and
   `tests/test_market_failover_readiness.py`.

### Step 6 — Read-only API and CLI

1. Add additive status model fields to `backend/app/market/models.py` and a read-only dependency in
   `backend/app/api/market.py`.
2. Add `GET /api/v1/market/universe`; validate date before store access, map failures to the
   allowlist and hide paths/errors/symbols.
3. Add `market-universe` to `backend/app/cli.py` with no `--execute` option and zero-write
   semantics. It reads no credentials and constructs no provider.
4. Add route/CLI filesystem fingerprint tests for missing, valid, stale and corrupt states, plus
   `tests/test_market_universe_status.py::test_source_version_and_terminal_attempt_status_matrix`
   and `tests/test_market_universe_status.py::test_equal_timestamp_newer_source_state_id_is_stale`
   cover exact-date head, equal-time source ordering, changed digest, and terminal matrix.
   `tests/test_market_universe_status.py::test_market_universe_cli_parser_and_invalid_date_are_read_only`
   covers pure lexical parsing, while
   `tests/test_market_universe_status.py::test_future_date_checks_sidecar_proof_before_semantic_date`
   covers the unique sidecar-proof-before-future-date ordering.

### Step 7 — Compatibility documentation and no-migration rehearsal

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
  tests/test_market_failover_readiness.py \
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
`UNIVERSE_SESSION_DRIFT`, `UNIVERSE_STATE_MISMATCH`, `DATE_MISMATCH`, `UNIVERSE_STORAGE_UNAVAILABLE`, `UNIVERSE_SCHEMA_MISMATCH`,
`UNIVERSE_SOURCE_VERSION_CHANGED`,
`UNIVERSE_HEAD_CAS_CONFLICT`, `BLOCKED_ENFORCE_NOT_ENABLED`, `BLOCKED_PRODUCTION_MODE_OFF`,
`LEGACY_SHADOW_DRIFT`, `ATTEMPT_INDETERMINATE`, `UNIVERSE_IDENTITY_CONFLICT`, `NONE`.
Unmapped provider/exception text MUST map to a closed boundary reason and MUST NOT be surfaced.

The implementation MUST use this deterministic mapping (the design table is normative and must
remain identical):

| Internal condition | Sanitized reason | HTTP / CLI projection |
|---|---|---|
| date syntax invalid (pure format parse, before any read), or valid future date after sidecar proof | `PIT_VISIBILITY_INVALID` | 422 / 2 |
| missing calendar control | `CALENDAR_UNAVAILABLE` | 200 blocked / 1 |
| conflicting calendar control | `CALENDAR_CONFLICT` | 200 blocked / 1 |
| calendar read is safe but unavailable | `CONTROL_STATE_UNAVAILABLE` | 200 blocked / 1 |
| sidecar/path storage cannot be proven | `UNIVERSE_STORAGE_UNAVAILABLE` | 503 fixed body / 3 |
| source date/observation beyond `exact_pit_cutoff` | `PIT_CUTOFF_VIOLATION` | 200 blocked / 1 |
| requested-unverified or unreviewed classification/instrument evidence | `BLOCKED_INSTRUMENT_EVIDENCE_UNQUALIFIED` | 200 blocked / 1 |
| no promoted classification visible at PIT | `CLASSIFICATION_UNAVAILABLE` | 200 blocked / 1 |
| required index absent, identity/date mismatch, suspended or unknown | `REQUIRED_INDEX_NOT_TRADING` | 200 blocked / 1 |
| malformed/unsupported required user symbol | `REQUIRED_SYMBOL_INVALID` | 200 blocked / 1 |
| symbol/security_id conflict or duplicate natural identity | `UNIVERSE_IDENTITY_CONFLICT` | 200 blocked / 1 |
| user snapshot mutation during capture | `USER_SNAPSHOT_CHANGED` | 200 blocked / 1 |
| user store busy/locked/unreadable | `USER_STORE_UNAVAILABLE` | 200 blocked / 1 |
| nonzero unknown member/state attribute | `UNIVERSE_UNKNOWN_NONZERO` | 200 blocked / 1 |
| loaded count mismatch | `UNIVERSE_COUNT_MISMATCH` | 200 blocked / 1 |
| missing expected row | `UNIVERSE_MISSING_SYMBOL` | 200 blocked / 1 |
| extra expected row | `UNIVERSE_EXTRA_SYMBOL` | 200 blocked / 1 |
| duplicate expected row | `UNIVERSE_DUPLICATE_SYMBOL` | 200 blocked / 1 |
| endpoint/page/session/request/date drift | `UNIVERSE_SESSION_DRIFT` | 200 blocked / 1 |
| DAILY_ASTOCK `tradestatus` or suspended placeholder disagrees with contract | `UNIVERSE_STATE_MISMATCH` | 200 blocked / 1 |
| strictly newer requested-date source-state digest differs from the head, with no corresponding failed/indeterminate attempt | `UNIVERSE_SOURCE_VERSION_CHANGED` | 200 stale / 1; a matching failed/indeterminate attempt instead maps to blocked |
| valid head trade date differs from requested date | `DATE_MISMATCH` | 200 stale / 1 |
| sidecar path/inode/schema/hash/WAL/lock cannot be proven | `UNIVERSE_STORAGE_UNAVAILABLE` | 503 fixed body / 3 |
| sidecar payload, partition, link, parent, head or attempt validation fails | `UNIVERSE_SCHEMA_MISMATCH` | 503 fixed body / 3 |
| nonblocking refresh/control lock unavailable while the sidecar is readable | `CONTROL_STATE_UNAVAILABLE` | 200 blocked/defer / 1 |
| sidecar head CAS conflict recorded as a terminal attempt | `UNIVERSE_HEAD_CAS_CONFLICT` | 200 blocked / 1 |
| committed RUNNING attempt without a result on the same operation day | `ATTEMPT_INDETERMINATE` | 200 blocked / 1 |
| mode `enforce` in nonproduction/staging execution | `B_E` (`BLOCKED_ENFORCE_NOT_ENABLED`) | 200 blocked / 1 |
| mode `shadow` or `enforce` in production profile | `B_P` (`BLOCKED_PRODUCTION_MODE_OFF`) | 200 blocked / 1 |
| BaoStock transport/protocol failure (`CONNECT_ERROR`, `SEND_ERROR`, `RECV_TIMEOUT`, `EOF`, `SHORT_HEADER`, `BAD_COMPRESSION`, `PROTOCOL_ERROR`, `PAGINATION_STALLED`, `RATE_LIMIT`, or unknown provider code) during acquisition | `CLASSIFICATION_UNAVAILABLE` | 200 blocked / 1 |
| read-only legacy shadow differs, including missing ChiNext/STAR from legacy request | `LEGACY_SHADOW_DRIFT` | private diagnostic only; never a Universe blocked status or HTTP/CLI error |

Raw BaoStock `provider_code` is retained only in private evidence/diagnostics; semantic meaning is
never guessed or exposed. `USER_STORE_UNAVAILABLE` is used for writer-store read failures;
`CALENDAR_UNAVAILABLE`/`CALENDAR_CONFLICT` are precise internal variants of control failure.

### Mandatory individual evidence crosswalk

The final evidence record MUST contain one row for every FR-1 through FR-27, AC-1 through AC-20,
and EC-1 through EC-24. Grouped IDs or ranges are not evidence. Each row must name its exact
offline test/static line and result, and the record must include both strict validator commands,
`git diff --check`, the exact reviewed commit, and `In Review` status; no implementation or
specification check may label this plan SPEC GO.

### Mandatory individual AC/EC evidence crosswalk

The implementation checklist is intentionally keyed one-for-one to the companion design; no
range notation is used for review evidence.

| Companion ID | Implementation evidence anchor |
|---|---|
| AC-1 | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| AC-2 | `tests/test_market_automation.py::test_calendar_fails_closed_outside_confirmed_year` |
| AC-3 | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| AC-4 | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| AC-5 | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| AC-6 | `tests/test_market_universe_review_red.py::test_required_index_evidence_requires_closed_identity_and_state` |
| AC-7 | `tests/test_market_automation.py::test_required_symbols_include_positions_and_all_watchlists` |
| AC-8 | `tests/test_market_universe_review_red.py::test_required_index_evidence_requires_closed_identity_and_state` |
| AC-9 | `tests/test_market_universe.py::test_unknown_one_loaded_match_rejects_contract_publication` |
| AC-10 | `tests/test_market_universe.py::test_builder_rejects_authority_bundle_from_wrong_classification_generation` |
| AC-11 | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| AC-12 | `tests/test_market_automation.py::test_automatic_due_refresh_uses_canonical_callback_and_never_legacy_fetch` |
| AC-13 | `tests/test_market_universe_staged.py::test_durable_attempt_claim_is_restart_safe_and_allows_first_empty_sidecar` |
| AC-14 | `tests/test_market_universe_status.py::test_api_and_cli_project_the_same_promoted_snapshot` |
| AC-15 | `tests/test_market_universe_staged.py::test_fresh_sealed_context_orders_canonical_postpublish_shadow_then_maintenance` |
| AC-16 | `tests/test_market_universe_staged.py::test_failed_acquisition_records_terminal_and_preserves_prior_head_and_canonical_artifacts` |
| AC-17 | `tests/test_market_universe_staged.py::test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison` |
| AC-18 | `tests/test_market_automation.py::test_automation_outcome_is_typed_and_legacy_dump_omits_only_new_null_field` |
| AC-19 | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| AC-20 | `tests/test_market_universe_review_red.py::test_raw_gate_rejects_cross_page_duplicate_identity_and_bad_shard_projection` |
| EC-1 | `tests/test_market_universe_status.py::test_market_universe_cli_parser_and_invalid_date_are_read_only` |
| EC-2 | `tests/test_market_universe_status.py::test_empty_or_missing_sidecar_is_control_error_without_initialization` |
| EC-3 | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| EC-4 | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| EC-5 | `tests/test_market_universe.py::test_builder_rejects_authority_bundle_from_wrong_classification_generation` |
| EC-6 | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| EC-7 | `tests/test_market_universe_review_red.py::test_required_index_evidence_requires_closed_identity_and_state` |
| EC-8 | `tests/test_market_universe_review_red.py::test_raw_gate_rejects_cross_page_duplicate_identity_and_bad_shard_projection` |
| EC-9 | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| EC-10 | `tests/test_market_universe.py::test_requested_unverified_source_is_never_publishable` |
| EC-11 | `tests/test_market_automation.py::test_automation_does_not_disguise_snapshot_failure_or_touch_store_or_provider` |
| EC-12 | `tests/test_market_universe.py::test_sidecar_reader_rejects_missing_bidirectional_role_link` |
| EC-13 | `tests/test_market_universe_review_red.py::test_required_index_evidence_requires_closed_identity_and_state` |
| EC-14 | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| EC-15 | `tests/test_market_universe_review_red.py::test_raw_gate_rejects_cross_page_duplicate_identity_and_bad_shard_projection` |
| EC-16 | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| EC-17 | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| EC-18 | `tests/test_market_universe.py::test_sidecar_cas_conflict_does_not_change_head` |
| EC-19 | `tests/test_market_universe_review_red.py::test_normative_ddl_matches_implementation_executes_and_strict_reader_accepts` |
| EC-20 | `tests/test_market_universe_staged.py::test_maintenance_service_promotes_one_reviewed_session_and_restart_is_noop` |
| EC-21 | `tests/test_market_universe_staged.py::test_disabled_maintenance_is_zero_hook_and_preserves_legacy_tick` |
| EC-22 | `tests/test_market_universe_staged.py::test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison` |
| EC-23 | `tests/test_market_universe_review_red.py::test_raw_gate_rejects_cross_page_duplicate_identity_and_bad_shard_projection` |
| EC-24 | `tests/test_market_universe_staged.py::test_attempt_dedup_identity_is_four_fields_but_plan_hash_is_complete` |

### Mandatory individual FR evidence crosswalk

Each companion design requirement has one implementation evidence row; grouped FR ranges are not
accepted as a substitute.

| FR (requirement name) | Exact implementation evidence anchor |
|---|---|
| FR-1 — Versioned scope | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| FR-2 — Exact-session identity | `tests/test_market_universe_review_red.py::test_normative_ddl_matches_implementation_executes_and_strict_reader_accepts` |
| FR-3 — Promoted calendar PIT gate | `tests/test_market_automation.py::test_calendar_fails_closed_outside_confirmed_year` |
| FR-4 — Promoted classification PIT gate | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| FR-5 — Main-board base scope | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| FR-6 — Required user symbols | `tests/test_market_automation.py::test_required_symbols_include_positions_and_all_watchlists` |
| FR-7 — Required indexes | `tests/test_market_universe_review_red.py::test_required_index_evidence_requires_closed_identity_and_state` |
| FR-8 — Instrument evidence binding | `tests/test_market_universe.py::test_builder_rejects_authority_bundle_from_wrong_classification_generation` |
| FR-9 — Listing and delisting states | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| FR-10 — Suspension state | `tests/test_market_universe_review_red.py::test_raw_gate_rejects_cross_page_duplicate_identity_and_bad_shard_projection` |
| FR-11 — ST state | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| FR-12 — Closed expected-state vocabulary | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| FR-13 — Deterministic count equation | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| FR-14 — Unknown-zero publication gate | `tests/test_market_universe.py::test_unknown_one_loaded_match_rejects_contract_publication` |
| FR-15 — Exact candidate set | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| FR-16 — Extras and duplicates are errors | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| FR-17 — Whole-session purity | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| FR-18 — Immutable sidecar authority | `tests/test_market_universe.py::test_sidecar_cas_conflict_does_not_change_head` |
| FR-19 — Canonical identity | `tests/test_market_universe_staged.py::test_attempt_plan_hash_preimage_uses_canonical_utc_z_timestamp` |
| FR-20 — Read-only status | `tests/test_market_universe_status.py::test_api_and_cli_project_the_same_promoted_snapshot` |
| FR-21 — Sanitized diagnostics | `tests/test_market_universe_status.py::test_market_universe_cli_parser_and_invalid_date_are_read_only` |
| FR-22 — Maintenance priority | `tests/test_market_universe_staged.py::test_fresh_sealed_context_orders_canonical_postpublish_shadow_then_maintenance` |
| FR-23 — Classification maintenance trigger | `tests/test_market_universe_staged.py::test_maintenance_service_promotes_one_reviewed_session_and_restart_is_noop` |
| FR-24 — Legacy compatibility modes | `tests/test_market_universe_staged.py::test_legacy_observer_is_count_hash_only_and_no_contract_is_typed_no_comparison` |
| FR-25 — No authority widening | `tests/test_market_universe_staged.py::test_release_candidate_removes_runner_and_raw_symbol_public_fields` |
| FR-26 — Safe migration path | `tests/test_market_universe_status.py::test_no_migration_rehearsal_preserves_canonical_files_and_pointer` |
| FR-27 — Frozen predecessor contracts | `tests/test_market_automation.py::test_automation_outcome_is_typed_and_legacy_dump_omits_only_new_null_field` |


### Final release-candidate attack-anchor registry

The design and implementation records share this exact behavior-level registry. Every anchor is
executed offline against typed fixtures; no `object()` or generic exception is used as a semantic
substitute. The registry covers the adversarial gates that must remain visible during review.

| Behavior | Exact test anchor |
|---|---|
| Main-board listing PIT window | `tests/test_market_universe_review_red.py::test_effective_main_board_requires_point_in_time_listing_window` |
| ST missing/conflicting/unknown vocabulary | `tests/test_market_universe_review_red.py::test_st_missing_conflicting_or_unknown_vocabulary_fails_closed` |
| Classification evidence PIT cutoff | `tests/test_market_universe_review_red.py::test_reviewed_evidence_requires_observed_at_at_or_before_cutoff` |
| Wrong classification generation bundle | `tests/test_market_universe.py::test_builder_rejects_authority_bundle_from_wrong_classification_generation` |
| Required index identity/state | `tests/test_market_universe_review_red.py::test_required_index_evidence_requires_closed_identity_and_state` |
| User snapshot atomic capture | `tests/test_market_universe_review_red.py::test_user_snapshot_capture_is_existing_descriptor_bound_and_atomic` |
| User B-share rejection | `tests/test_market_universe_review_red.py::test_user_capture_rejects_b_share_identity` |
| Sidecar orphan evidence rejection | `tests/test_market_universe_core_red.py::test_orphan_evidence_invalidates_global_sidecar` |
| Sidecar path replacement | `tests/test_market_universe_review_red.py::test_universe_sidecar_fails_closed_when_path_is_replaced_after_open` |
| Raw all-endpoint adapter boundary | `tests/test_market_provider_contract.py::test_baostock_adapter_uses_ordinary_incumbent_sdk_boundary_for_all_endpoints` |
| Raw exact plan completion/cardinality | `tests/test_market_provider_contract.py::test_provider_raw_batch_requires_exact_plan_batch_completion_cardinality` |
| Raw extra/role/date shard rejection | `tests/test_market_provider_contract.py::test_raw_batch_binds_exact_logical_shard_role_schema_symbol_and_dates` |
| Raw duplicate projection/lineage | `tests/test_market_provider_contract.py::test_provider_raw_batch_rejects_duplicate_projection_and_lineage` |
| Raw mixed session/date rejection | `tests/test_market_provider_contract.py::test_endpoint_schema_variants_reject_date_mismatch_and_mixed_sessions` |
| Raw suspended stock semantics | `tests/test_market_provider_contract.py::test_active_daily_rows_reject_null_activity_and_suspended_rows_reject_activity` |
| Raw suspended index semantics | `tests/test_market_provider_contract.py::test_index_history_rows_reject_suspended_index` |
| Raw complete frame and pagination terminal | `tests/test_market_provider_contract.py::test_success_attempt_requires_all_complete_frame_markers_and_one_pagination_terminal` |
| Raw page after terminal | `tests/test_market_provider_contract.py::test_page_after_pagination_terminal_is_rejected` |
| Fresh sealed maintenance order | `tests/test_market_universe_staged.py::test_fresh_sealed_context_orders_canonical_postpublish_shadow_then_maintenance` |
| Maintenance acquisition terminal preservation | `tests/test_market_universe_staged.py::test_failed_acquisition_records_terminal_and_preserves_prior_head_and_canonical_artifacts` |
| Maintenance classification terminal/no provider | `tests/test_market_universe_staged.py::test_maintenance_unqualified_classification_is_durable_and_never_calls_provider` |
| Maintenance restart dedup | `tests/test_market_universe_staged.py::test_attempt_dedup_identity_is_four_fields_but_plan_hash_is_complete` |
| Disabled zero-work lane | `tests/test_market_universe_staged.py::test_disabled_maintenance_is_zero_hook_and_preserves_legacy_tick` |
| Read-only status/API/CLI | `tests/test_market_universe_status.py::test_empty_or_missing_sidecar_is_control_error_without_initialization` |
| Sidecar bidirectional role-link integrity | `tests/test_market_universe.py::test_sidecar_reader_rejects_missing_bidirectional_role_link` |
| First empty sidecar claim | `tests/test_market_universe_staged.py::test_durable_attempt_claim_is_restart_safe_and_allows_first_empty_sidecar` |
| Requested-unverified publication block | `tests/test_market_universe.py::test_requested_unverified_source_is_never_publishable` |
| Status source/terminal precedence | `tests/test_market_universe_status.py::test_source_version_and_terminal_attempt_status_matrix` |
| Equal timestamp source identity | `tests/test_market_universe_status.py::test_equal_timestamp_newer_source_state_id_is_stale` |
| Sidecar proof precedes future-date rejection | `tests/test_market_universe_status.py::test_future_date_checks_sidecar_proof_before_semantic_date` |
| Canonical callback does not use legacy fetch | `tests/test_market_automation.py::test_automatic_due_refresh_uses_canonical_callback_and_never_legacy_fetch` |
| Snapshot failure zero-work | `tests/test_market_automation.py::test_automation_does_not_disguise_snapshot_failure_or_touch_store_or_provider` |
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
