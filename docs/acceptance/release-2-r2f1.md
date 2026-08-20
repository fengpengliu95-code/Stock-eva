# Release 2 — R2-F1 Continuity Controller

**Date:** 2026-08-21 (Asia/Shanghai)

**Verdict:** R2-F1 OFFLINE CODE GO

**Planning head:** 386dc78037d306b668e1003e2235c62f4eda54ea

**Exact reviewed code HEAD:** 336107be1149d829c0ea841dd2466982bff7d689

**Independent review:** High 0 / Medium 0 — R2-F1 OFFLINE CODE GO

**Documentation closure:** enclosing git commit; exact hash is recorded in the final handoff.

**Production verdict:** NO-GO. OFFLINE CODE GO / REAL PROVIDER AND PRODUCTION EXECUTION NOT
AUTHORIZED.

## Scope and decision boundary

R2-F1 adds a fail-closed continuity controller for the current BaoStock canonical scope. It
strictly inventories the current immutable manifest, computes confirmed-calendar gaps, persists
writer-owned repair jobs and attempts, gives freshness priority, gates repair claims on the
existing provider-health circuit, executes at most one whole-session repair through the existing
canonical publication chain, and exposes read-only API/CLI status.

The existing Normalize -> Quality Gate -> Immutable Parquet -> SHA-256 -> Manifest -> Atomic
Publish chain remains the authority. R2-F1 does not add raw provider evidence, a second provider,
symbol-level mixing, automatic failover, or a new LaunchAgent. R2-F2 has not started and is not
included in this acceptance.

## Verification record

All verification used fakes, injected clocks/calendars/health stores and temporary roots. No
provider host, NAS path, installed runtime or production control store was accessed.

| Gate | Result |
| --- | --- |
| R2-F1 focused fault matrix | PASS — 466 tests, exit 0 |
| Full repository pytest | PASS — 1,403 tests, exit 0 |
| ruff check backend tests | PASS — 144 targets, exit 0 |
| ruff format --check backend tests | PASS |
| git diff --check | PASS |
| Independent final code/spec review | PASS — High 0, Medium 0 |

Focused command:

    uv run --extra dev pytest -q tests/test_market_continuity.py tests/test_market_automation.py tests/test_market_get_read_only.py tests/test_market_reliability.py tests/test_nas_dataset.py tests/test_provider_health.py tests/test_api_baseline.py tests/test_launchagent_assets.py --basetemp=/tmp/stock-eva-r2f1-acceptance

Universal offline gate:

    uv run --extra dev pytest -q --basetemp=/tmp/stock-eva-r2f1-full
    uv run --extra dev ruff check backend tests
    uv run --extra dev ruff format --check backend tests
    git diff --check

One initial full-suite run reported the known order-sensitive observation at
tests/test_market_regime.py:910; the isolated test and the complete rerun passed. No test was
suppressed or changed to hide it. It remains a Low observation for future test isolation review.

## Delivered boundaries

### Strict inventory and fail-closed scanning

- NasMarketStore.verified_ready_session_inventory() derives sessions from one current manifest
  snapshot and verifies safe paths, object existence, SHA-256, canonical Parquet schema and row
  count. The singleton pointer and mutable DuckDB rows are not continuity evidence.
- The pure scanner computes exactly confirmed open sessions minus verified ready sessions, applies
  the configured lower boundary and latest expected upper boundary, rejects unknown/conflicting
  calendar state, and performs zero writes/provider calls.
- A valid empty manifest means all in-range confirmed sessions are missing; a malformed, oversized,
  unsafe or corrupt manifest/object makes the complete scan unavailable.

### Durable queue, CAS and recovery

- repair_jobs and repair_attempts are additive writer-owned DuckDB tables with deterministic IDs,
  allowlisted states, sanitized columns, UTC timestamps and no payload, URL, token, path or
  arbitrary exception fields.
- Explicit migration is protected by market-refresh.lock; read-only API/CLI readers use SELECT-only
  snapshots and never initialize or repair the schema.
- Enqueue is idempotent. Claim/finalize/reap use shared lock plus state-version, owner and lease
  identity CAS. Two-process contention yields one lease and one running attempt.
- Restart, stale lease, retry budget, exponential capped delay, dead-letter and strict manifest
  reconciliation are covered. A terminal job never reopens to pending; an externally published
  date can reconcile to published without a provider call.

### Scheduling, health and canonical repair

- A due freshness target always wins. When freshness is current or waiting for a later retry slot,
  at most one strictly older repair is eligible; the controller never loops through backlog.
- OPEN, HALF_OPEN, missing or unreadable provider health creates no repair claim, attempt or
  provider request. Only the existing scheduler owns HALF_OPEN probing; R2-F1 adds no probe logic.
- A repair uses symbols=None, the current required universe, run_kind=repair and a deterministic
  request key. Partial, duplicate, wrong-universe, row-count or quality failures remain
  whole-attempt failures. A successful older repair extends the cumulative manifest without
  regressing the latest pointer.
- Crash after immutable publication but before queue finalization is reconciled from strict
  manifest evidence and does not fetch or publish a second time. Rollback is
  STOCK_EVA_MARKET_REPAIR_ENABLED=false (or market_repair_enabled=False).

### Read-only API, CLI and calendar control

- GET /api/v1/market/status adds sanitized continuity fields while preserving existing fields.
  Missing or malformed queue/control state returns unavailable/CONTROL_STATE_UNAVAILABLE and does
  not initialize tables.
- market-continuity planning is network-free and write-free, including with a missing runtime
  root, invalid range, missing queue or missing calendar control. --execute is enqueue-only: it
  repeats strict evidence checks under lock, writes only the additive queue, and makes zero
  provider, Parquet, manifest and pointer writes.
- Calendar conflict, missing calendar control and unknown calendar years fail closed. Calendar
  state is read-only in status/plan paths; it is never guessed from weekdays.

## Requirement-to-evidence matrix

The following maps every requirement in the approved design to the focused tests and implementation
paths reviewed at the exact code HEAD.

### Functional requirements

| ID | Evidence |
| --- | --- |
| FR-1 | Bounded settings and unconfigured-start tests in test_market_continuity.py; scan rejects before any writer construction. |
| FR-2 | Lower-boundary scan tests exclude earlier sessions from inventory and enqueue. |
| FR-3 | Latest-session and requested-range validation tests reject future/expanded ranges. |
| FR-4 | Confirmed-calendar, unknown-year and conflict tests reject the whole scan. |
| FR-5 | test_nas_dataset.py strict inventory tests cover one manifest snapshot, safe path, hash, schema and row count. |
| FR-6 | Lightweight manifest_dates is explicitly tested as non-authoritative; pointer/available_dates are not used. |
| FR-7 | Non-contiguous inventory scan asserts exact oldest-first set difference. |
| FR-8 | Calendar, manifest and object corruption tests fingerprint tree/control state and prove zero writes. |
| FR-9 | Additive migration and legacy daily/backfill compatibility tests preserve prior rows and objects. |
| FR-10 | Deterministic enqueue ID and restart/idempotency tests prove no duplicates or backward transitions. |
| FR-11 | Transition, version, owner and lease-CAS tests reject stale or illegal mutations. |
| FR-12 | Two-process shared-lock claim test produces one lease and one running attempt. |
| FR-13 | Expired lease tests create one abandoned attempt, increment the counter once and enter future retry wait. |
| FR-14 | Retry finalization tests require sanitized failure evidence and strictly future exponential retry time. |
| FR-15 | Non-retryable/exhausted tests dead-letter without blocking another eligible date. |
| FR-16 | Automation priority matrix runs due freshness before any repair claim/provider call. |
| FR-17 | Current/waiting freshness tests allow at most one oldest historical repair. |
| FR-18 | Latest-session exclusion and older-repair tests enforce strict target ordering. |
| FR-19 | Lock contention and locked revalidation tests cover both lanes and no-write busy behavior. |
| FR-20 | Provider OPEN/HALF_OPEN/unavailable health tests create no repair lease, attempt or request. |
| FR-21 | Existing scheduler probe tests and static review show repair code never acquires/resolves HALF_OPEN. |
| FR-22 | Full-session repair tests assert symbols=None, current required symbols, repair run kind and deterministic key. |
| FR-23 | Reliability/publication tests prove unchanged normalization, quality, immutable object, hash, manifest and atomic pointer gates. |
| FR-24 | Crash-window and strict manifest reconciliation tests close a published job without a second fetch. |
| FR-25 | Additive run_kind=repair tests retain legacy refresh values and rows. |
| FR-26 | API additive-field, missing-table and malformed-queue tests return sanitized unavailable status without migration. |
| FR-27 | Read-only CLI fingerprint tests prove no path, database, schema, pointer or provider mutation. |
| FR-28 | Execute CLI tests prove strict locked rescan, queue-only writes and zero provider/canonical writes. |
| FR-29 | Schema and public-payload tests exclude payload, URL, token, absolute path and raw exception text. |
| FR-30 | Disable-switch tests preserve jobs/attempts and freshness while preventing claims/execution. |

### Non-functional requirements

| ID | Evidence |
| --- | --- |
| NFR-1 | All invalid calendar, inventory, queue and control-state tests compare before/after fingerprints. |
| NFR-2 | Real two-process claim contention plus CAS assertions. |
| NFR-3 | Decision/executor tests enforce one bounded repair per invocation and no backlog loop. |
| NFR-4 | Freshness-due matrix asserts zero repair claims and provider calls. |
| NFR-5 | Restart, committed-state, stale-lease and crash reconciliation tests. |
| NFR-6 | Naive datetime rejection, UTC persistence and Shanghai-local calendar tests. |
| NFR-7 | API/CLI read-only tests compare tree, bytes, schema, sizes and mtimes. |
| NFR-8 | Legacy refresh rows, disabled repair and optional dependency tests preserve prior behavior. |
| NFR-9 | DuckDB schema and serialized-output allowlist tests reject sensitive/raw fields. |
| NFR-10 | Static completion audit and reliability regressions show no timeout/retry/coverage/factor-gate or canonical-chain weakening. |
| NFR-11 | All commands used fakes/temp roots; no network, provider, installation, NAS, LaunchAgent or production operation occurred. |

### Acceptance criteria

| ID | Evidence |
| --- | --- |
| AC-1 | Pure non-contiguous gap scan returns the exact missing set and zero writes. |
| AC-2 | Configured lower and latest expected upper boundaries are enforced. |
| AC-3 | Unknown calendar is sanitized unavailable and zero-write. |
| AC-4 | Missing/hash/schema/row-count/unsafe/corrupt inventory is unavailable and zero-write. |
| AC-5 | Repeated scan/enqueue across restart creates exactly one deterministic job per gap. |
| AC-6 | Shared-lock/CAS contention yields one lease/attempt. |
| AC-7 | Expired lease recovery is audited and future-scheduled. |
| AC-8 | Due freshness cannot be starved by historical repair. |
| AC-9 | Current/waiting freshness permits only one oldest repair. |
| AC-10 | Non-CLOSED or unreadable health blocks claim and provider work. |
| AC-11 | Retryable failures wait; exhausted/non-retryable failures dead-letter; other dates proceed. |
| AC-12 | Post-publication crash is reconciled without second provider/publication call. |
| AC-13 | Only whole-session canonical publication is allowed; invalid partial candidates fail closed. |
| AC-14 | Additive migration preserves legacy rows and supports repair run kind. |
| AC-15 | API and CLI missing/corrupt control state report unavailable without writes. |
| AC-16 | Rollback switch disables repair while preserving evidence and freshness. |

### Edge cases

| ID | Evidence |
| --- | --- |
| EC-1 | Missing/future/invalid continuity start is rejected before reads/writes. |
| EC-2 | CLI start earlier than configured boundary is rejected with zero initialization. |
| EC-3 | CLI end beyond latest or inverted range is rejected with zero writes. |
| EC-4 | Any unknown calendar date rejects the complete range. |
| EC-5 | Calendar conflict state rejects even a weekday-compatible date. |
| EC-6 | Missing/unreadable/oversized/malformed manifest is unavailable before queue setup. |
| EC-7 | Duplicate partition/date, unsafe path or unexpected source rejects the inventory. |
| EC-8 | Missing/changed/hash/schema/row-count-invalid Parquet rejects all inventory. |
| EC-9 | Valid empty manifest makes all in-range confirmed open sessions missing. |
| EC-10 | Mutable local-only mode cannot claim immutable readiness. |
| EC-11 | Repeated enqueue across all job states is idempotent and never reopens terminal work. |
| EC-12 | Manifest appearance before claim reconciles without provider work. |
| EC-13 | Stale worker finalization is rejected by copied lease/version CAS. |
| EC-14 | Pre-claim process failure leaves no committed attempt mutation. |
| EC-15 | Post-claim process failure leaves an expiring lease and one audited abandonment. |
| EC-16 | Manifest-published/job-unfinalized crash is reconciled exactly once. |
| EC-17 | Missing queue/control schema is unavailable without read-path migration. |
| EC-18 | Malformed queue schema is sanitized unavailable and never auto-repaired by GET/plan. |
| EC-19 | Busy writer/lock returns already-running/no state transition. |
| EC-20 | Missing/corrupt health store preserves work and makes no repair request. |
| EC-21 | Health opening after decision is blocked by locked re-read. |
| EC-22 | Health opening during an attempt yields one bounded finalized failure; no second repair. |
| EC-23 | Dead-letter backlog does not block a newer pending date. |
| EC-24 | Later authorized publication can strictly reconcile dead-letter to published only. |
| EC-25 | Partial/error result remains an attempt failure despite transport success. |
| EC-26 | Older repair cannot regress latest pointer; cumulative manifest remains atomic. |
| EC-27 | Unknown internal status/reason serializes as unavailable with allowlisted code. |
| EC-28 | Offset/UTC boundary tests preserve deterministic UTC ordering and Shanghai session logic. |

## Bounded commit history

The following are the exact commits after the planning head that materially delivered or corrected
R2-F1. Full hashes are recoverable from the exact reviewed HEAD's immutable git history.

| Commit | Boundary |
| --- | --- |
| dbe9769 | Strict verified session inventory |
| 2d7f572 | Bind inventory validation to immutable objects |
| 3398ced | Descriptor-bound validation regression |
| a558ef8 | Additive repair queue, attempts and CAS foundation |
| 0e51c70 | Queue invariants |
| be85e68 | Secure migration and evidence lineage |
| f24a797 | Secure reconciliation and migration |
| e60e854 | Bind repair evidence and migration artifacts |
| 8c156f5 | Secure migration publication |
| 4970103 | Bind migration input and commit point |
| 02173c5 | Recover exchange-window conflicts |
| 83a30c8 | Bind migration commit to configured parent |
| a14fdfd | Pure confirmed-calendar gap scanner |
| 653007a | Revalidate gaps before enqueue |
| bc32360 | Write-free enqueue preflight |
| 96a6cf9 | Freshness priority and health gate |
| 785bf81 | Validate continuity decisions |
| d49bbf4 | Bounded whole-session repair executor |
| cd16d83 | Close repair crash gaps |
| 6476009 | Type repair automation outcomes |
| 6dc4fa4 | Sanitize repair execution evidence |
| 26ecb52 | Restrict repair public payloads |
| 95ea19d | Fail closed on invalid automation output |
| 02d28e1 | Add read-only continuity API/CLI status |
| 99e3964 | Preserve continuity read-only boundaries |
| d57bb05 | Validate locked continuity scans |
| 3d4e69e | Classify invalid continuity evidence |
| a49cfbe | Require readable continuity control state |
| b1bf796 | Satisfy repository format gate |
| 89482db | Gate repairs on calendar conflict state |
| 336107b | Fail closed on missing calendar control state; exact reviewed code HEAD |

No commit in this sequence contains R2-F2 raw evidence, a second-provider adapter, symbol-level
mixing or automatic failover.

## Limitations and threat boundary

- The macOS writer migration relies on UF_APPEND and renameatx_np; unsupported platforms and unsafe
  fallback paths fail closed. Cross-filesystem staging/link failures also fail closed.
- The design does not claim OS isolation against a same-UID actor that continuously modifies a
  private staging inode. Unknown or replaced staging residue is retained for manual audit rather
  than silently removed.
- After a successful preflight followed by locked revalidation failure, a canonical lock artifact
  may remain; broad runtime/control directories are not created or rewritten.
- The current scope is BaoStock/all-main-board only. Provider-neutral evidence, second-source
  qualification and failover remain future stages.
- The repository's one order-sensitive test_market_regime.py:910 observation remains Low even
  though isolated and complete reruns passed.

## Rollback and compatibility

Set STOCK_EVA_MARKET_REPAIR_ENABLED=false (or market_repair_enabled=False) to stop repair
claims/execution. Keep jobs, attempts, refresh audit and provider-health evidence. Freshness
scheduling remains available. The continuity tables and run_kind=repair are additive; legacy
daily/backfill rows and immutable publication bytes remain readable. No downgrade or manual
Parquet/manifest/pointer edit is part of this acceptance.

## Production and next-stage boundary

The production refresh and LaunchAgent remain intentionally unloaded/frozen from the prior
incident response. This acceptance did not restore them, install a runtime, access NAS, perform a
real provider request, run a canary, mutate a production database, publish Parquet/manifest/pointer
data, or send any external message.

R2-F1 stops here. R2-F2 Provider Evidence Framework may begin only after a new explicit user
approval. The second source remains subject to whole-session shadow qualification for at least 20
consecutive trading days, with automatic failover disabled until shadow, qualification and manual
failover validation are separately complete.

**Final boundary: OFFLINE CODE GO / REAL PROVIDER AND PRODUCTION EXECUTION NOT AUTHORIZED.**
