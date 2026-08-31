# Stock EVA R2-F4.1 Promoted Runtime Calendar Design

**Author:** Codex root, R2-F delivery lead

**Date:** 2026-08-31 (Asia/Shanghai)

**Status:** IN REVIEW / IMPLEMENTATION NOT STARTED / R2-F4 NO-GO

**Reviewers:** One independent reviewer at a time; human approval is required at the completed
subversion GO gate. The owner has delegated intermediate technical choices to the delivery lead.

**Base:** `f5eaa99079db876704ee0281def5fc58af7bcc3c` (human-approved R2-F4.0)

## Context

Task 15 of the R2-F implementation plan requires additive runtime calendar generations and
next-year maintenance. The current `get_trading_calendar()` permanently caches bundled 2025/2026
configs. Long-running automation and continuity services therefore cannot observe a newly approved
year without changing a release. BaoStock's existing `trading_dates()` returns only open dates;
closed rows, duplicate dates and full-range completeness are lost before reconciliation.

Official SSE/SZSE schedules remain authority. Machine observations are cross-checks, not authority.
The current repository has official notice metadata but no trusted automatic notice discovery or
parser. This slice adds bounded retrieval of **reviewed, exact official notice locations and body
hashes**, coupled to reviewed full-year schedule extraction. It does not guess future URLs or let a
search result/HTML parser manufacture authority. Once a source package is staged, acquisition,
cross-check and promotion are automatic in the existing calendar job. Missing source packages are
an explicit operational prerequisite, not a successful maintenance run.

This is an additive calendar-control contract. It does not alter Normalize, Quality Gate, canonical
Parquet/SHA-256/Manifest/Atomic Publish, R2-F2 evidence/candidate/selection, R2-F3 qualification or the
R2-F4.0 selection shield. Completing a calendar generation does not qualify TickFlow for failover.

## Architecture and alternatives

Chosen: a dedicated `calendar_generations.sqlite3` alongside `calendar_sync.sqlite3`, immutable
source/generation rows, a transactional calendar-only head, a strict read-only replay reader, and
a live calendar facade which freezes one snapshot per public operation. Existing bundled JSON is
never rewritten. The existing calendar-sync one-shot/loop invokes next-year maintenance only when
runtime mode is explicitly enabled; no extra LaunchAgent is installed.

Alternatives considered: editing release JSON would couple yearly operations to deployment and
lose runtime lineage; accepting a machine calendar alone would remove official authority; an
unreviewed general HTML crawler would conflate discovery/extraction with approval. None is selected.

Official source acquisition has an explicit operator boundary: an operator stages a source package
containing independently reviewed SSE and SZSE schedules, notice URLs/numbers, publication dates,
expected body hashes and extraction review identity. The worker retrieves those exact resources
once each and compares bytes to the reviewed hashes, then retrieves one exact full-year BaoStock
calendar. No new token, credential, subscription or full-service secondary capability is involved.

## Functional requirements

- FR-1: Runtime calendar mode MUST default to disabled. Disabled mode MUST preserve the bundled
  calendar and existing canonical/qualification behavior without accessing runtime control state.
- FR-2: The runtime store MUST live at `local_control_dir/calendar_generations.sqlite3` by default.
  Its configurable basename MUST be a safe `.sqlite3` basename distinct from other control stores.
  Existing calendar-sync tables MUST NOT be migrated or reused as generation authority.
- FR-3: A source package MUST contain exactly one SSE and one SZSE full-year schedule, in that
  order, for the same year. Each MUST bind its official URL, notice number/title, publication date,
  reviewed body SHA-256, sorted unique holiday-closure dates and extraction review ID/date.
- FR-4: The rule is `cn-a-share-weekends-closed-v1`: every Saturday/Sunday is closed; the reviewed
  weekday closure dates are closed; all other dates of an explicitly covered full year are open.
  Closure dates MUST belong to that year, be weekdays, and be unique/sorted. Unknown years,
  including their weekends, MUST remain unknown. No generic weekday fallback is permitted.
- FR-5: Every SHA identity MUST use canonical UTF-8 JSON (sorted keys, compact separators,
  ensure_ascii=false, allow_nan=false, trailing newline) and its versioned domain prefix plus
  newline. Public digest fields MUST be required lowercase 64-hex strings, not optional sentinels.
  All write/authority boundaries MUST revalidate serialized input, including model_copy/construct.
- FR-6: Source staging MUST validate metadata, publication/review dates against the trusted
  Shanghai clock and both schedule hashes before writes. Individually valid but disagreeing
  SSE/SZSE schedules MUST be persisted as quarantined candidates and cause zero network requests.
  Malformed source packages MUST fail before creating a database, lock or directory.
- FR-7: Staging the same source hash MUST be idempotent. Sources and generations MUST be immutable;
  a new notice/extraction requires a new source hash. Source staging MUST never promote a year.
- FR-8: Source retrieval MUST use HTTPS and exact allowlisted official origins (`www.sse.com.cn`,
  `www.szse.cn`, `investor.szse.cn`), default port only, no userinfo/query/fragment, no redirect,
  no environment credentials/proxy authentication, and no retry. URLs remain local authority
  metadata; public operational errors MUST NOT include URLs, body content or raw exceptions.
- FR-9: Each official body MUST be bounded at 1 MiB and match the staged SHA-256 exactly. Missing,
  changed, oversized or failed bodies MUST leave the candidate unpromoted. Verified official body
  bytes MUST be retained as immutable calendar-control evidence, never as canonical market RAW.
- FR-10: Add an isolated BaoStock `calendar_days(start,end)` capability without changing the return
  contract of `trading_dates()` or the canonical provider adapter. It MUST validate exact fields,
  exactly one row per requested civil date, chronological order, unique dates, flags exactly
  `0`/`1`, and inclusive complete coverage before returning `(date, bool)` pairs. A missing closed
  date is an incomplete response, even if all expected open dates were returned.
- FR-11: Live maintenance MUST use the existing pinned BaoStock transport, fixed endpoint
  `trade_dates`, max_attempts=1 and unchanged timeout. It MUST preserve transport identity/audit and
  persistent health accounting. OPEN/HALF_OPEN health MUST stop acquisition before official or
  machine requests; this worker MUST NOT close circuits, probe, retry or refresh market data.
- FR-12: Maintenance MUST stage the candidate before requests and reserve a durable slot keyed by
  `(target_year, Shanghai_date)` before acquisition. At most two official HTTP requests and one
  logical machine calendar request are permitted in a slot. A crash/rerun or changed candidate
  MUST NOT reuse the slot to issue another request. A later day may try only after health permits.
- FR-13: A complete successful machine observation MUST bind provider=`baostock`, contract version
  `r2f4.1-baostock-calendar-days-v1`, full-year range, observed time, all civil-date flags and their
  content hash. Both official calendars and machine flags MUST match for every date. Unavailable
  machine data MUST be explicit and MUST NOT qualify an official-only candidate.
- FR-14: Promotion MUST recheck source/evidence/observation hashes, full-year agreement, source
  review times, and the current parent generation under one writer transaction. It MUST reject a
  stale expected parent and preserve the previous head on any failure. The new object MUST be
  read back and hash-verified before the calendar-only head changes atomically.
- FR-15: A generation MUST record sequence, parent hash, bundled-base checksum, source hash,
  verified official evidence hashes, machine observation, promotion timestamp and own SHA-256.
  The runtime reader MUST replay the complete ordered chain and reject a missing parent, changed
  object, head rewind/mismatch or invalid semantic transition. A caller-supplied calendar config
  MUST NOT be sufficient promotion authority.
- FR-16: Applying a year may extend the greatest covered year by one or amend an already covered
  year. It MUST NOT skip a year. It MUST NOT change any open/closed status on or before the prior
  calendar's latest completed session at promotion time (18:10 Shanghai availability boundary).
  If the clock's current year is not yet covered, the protected horizon includes every known day
  before the current date. Future-only amendments retain all previous generation bytes.
- FR-17: Runtime readers MUST never create/migrate/repair anything. Enabled runtime with a missing,
  corrupt, locked, unsafe-path or unprovable store MUST yield unavailable and an empty calendar;
  it MUST NOT silently fall back to bundled or older promoted authority. A valid initialized store
  with no promoted rows may compose its verified bundled base. Disabled mode stays bundled-only.
- FR-18: Stores MUST reject symlinks, hardlinked control files, non-regular files, unsafe ownership
  or writable-by-other control state, and active SQLite journal/WAL sidecars. Readers MUST use
  bounded read-only connections, verify schema/immutable guards and leave bytes/tree/mtime intact.
  Only explicit staging execute may initialize the generation store; scheduled maintenance MUST
  NOT recreate a missing store or repair corruption.
- FR-19: Every public calendar operation MUST use one immutable runtime snapshot. The default
  calendar factory and long-lived automation/continuity/calendar-sync consumers MUST see a later
  promotion on their next operation without restart. A range MUST NOT mix generations mid-loop.
  Concrete injected test/qualification calendars MUST retain their existing behavior.
- FR-20: Calendar-sync MUST freeze authority for a plan/execute operation, reject an authority
  checksum change before provider access, and never report `ready` for mixed known/unknown ranges.
  Existing conflicts still quarantine; unknown portions produce `observed_only`.
- FR-21: Next-year policy MUST be based on Shanghai dates: before October 1, absent next-year
  authority is `not_due`; October 1 through December 14 is `pending`; from December 15 it is
  `action_required`. Missing current-year authority is always blocking. A future date/year MUST
  never become usable because of policy status alone; a verified bundled or promoted year is
  required. Existing bundled years remain valid baselines, newly added years require promotion.
- FR-22: The existing `calendar-sync --startup --execute` LaunchAgent and in-process calendar loop
  MUST invoke at most one due runtime maintenance slot, targeting a missing current year first,
  otherwise next year from October 1. A missing staged source means `SOURCE_PENDING`, zero
  requests, no head change. The legacy current/historical reconciliation remains a separate
  bounded operation. No sixth LaunchAgent or change to market refresh cadence is allowed.
- FR-23: Add zero-write `calendar-generation-status` and
  `GET /api/v1/market/calendar-generation`; they MUST return bounded generation/coverage/policy/
  conflict states and zero request/write counters, without market store or provider construction.
  Existing `/market/status` keeps its schema and uses the same calendar facade for expected dates.
- FR-24: Add `calendar-generation-stage --source PATH [--execute]` and
  `calendar-maintenance [--execute]`. Default planning MUST have zero writes/network requests.
  Source files MUST be bounded regular no-symlink JSON with duplicate-key rejection. Staging
  execute writes only calendar control; maintenance execute writes only calendar control/sync and
  existing provider-health audit. Neither may construct market/NAS stores, publish, or auto-shadow.
- FR-25: No ProviderId, canonical candidate/selection/manifest, frozen R2-F2/R2-F3 contracts or
  R2-F4.0 capability projection may change. Runtime calendar availability MUST NOT silently mark
  the TickFlow promoted-calendar capability QUALIFIED; that needs a later versioned authority.
- FR-26: Operational failures MUST use a closed safe vocabulary: `SOURCE_PENDING`,
  `SOURCE_INVALID`, `SOURCE_CONFLICT`, `OFFICIAL_UNAVAILABLE`, `OFFICIAL_HASH_MISMATCH`,
  `MACHINE_UNAVAILABLE`, `MACHINE_CONFLICT`, `SKIPPED_CIRCUIT_OPEN`, `ALREADY_ATTEMPTED`,
  `PARENT_CHANGED`, `HISTORY_CHANGE`, `CONTROL_STATE_UNAVAILABLE`, `NOT_DUE`, `PROMOTED`.
  Unexpected programming failures MUST not be disguised as successful or unavailable evidence.

## Non-functional requirements

- NFR-1: All development/acceptance requests MUST be injected offline fakes. No real Provider,
  official HTTP, credentials, production Application Support/canonical/control, NAS, deployment or
  LaunchAgent execution occurs in this slice's acceptance. Production enablement is not claimed.
- NFR-2: A source input is limited to 256 KiB JSON, each official object to 1 MiB, exactly two
  schedules and at most 366 machine days. The reader MUST bound generations/candidates (256/1024)
  and reject excess rather than truncate. SQLite acquisition timeout is zero; provider timeout and
  request count are never increased. HTTP uses existing 5s connect/write/pool and 30s read bounds.
- NFR-3: Staging/promotion crash injection before commit MUST preserve the prior calendar head and
  all canonical bytes. Concurrent promotion MUST have one winner; loser observes `PARENT_CHANGED`.
- NFR-4: Full repository pytest, focused calendar/automation/continuity/fund-flow/read-only/golden
  tests, ruff check/format, compileall, strict spec validator and diff check MUST pass. Existing
  canonical and R2-F2/R2-F3/R2-F4.0 protected file hashes MUST stay unchanged except the explicitly
  additive BaoStock calendar-days method and documented calendar consumers.

## Acceptance criteria

### AC-1: Immutable identity and strict source (FR-3, FR-4, FR-5, FR-6, FR-7)
Given valid dual-source fixtures and malformed/duplicate/out-of-year/construct-bypassed variants,
When staging is planned/executed, Then only fully valid identities are admitted; disagreement is
quarantined with zero network; identical staging is idempotent and never promotes.

### AC-2: Bounded official acquisition (FR-8, FR-9, FR-12)
Given fake HTTP success, redirect, error, changed hash and oversized/chunked bodies,
When a slot executes, Then each official URL is requested at most once, disallowed locations stop
pre-client, failures preserve head, and successful bytes are retained and hash-bound.

### AC-3: Exact machine coverage (FR-10, FR-11, FR-13)
Given fake BaoStock rows with complete leap/non-leap years, duplicate/missing closed days, invalid
flags, unsorted/short/out-of-range rows and transport failures, When calendar_days is called,
Then only exact civil-day coverage passes; no partial list is used and attempts stay one.

### AC-4: Promotion agreement (FR-13, FR-14, FR-15)
Given staged sources, verified official bytes and a full matching machine year,
When maintenance promotes, Then an immutable hash-verified generation and its head commit together;
one different date or unavailable machine result preserves the old head and records its reason.

### AC-5: Completed history and year continuity (FR-16)
Given bundled plus promoted authority and a trusted pre/post-18:10 clock,
When an amendment changes completed history or skips a year, Then promotion fails unchanged;
a future-only amendment or next consecutive year succeeds without rewriting prior objects.

### AC-6: Reader corruption and missing state (FR-15, FR-17, FR-18, NFR-2)
Given enabled runtime and missing/corrupt/locked/schema-changed/linked/wrong-mode/journaled files,
When read repeatedly, Then result is unavailable, no bundled fallback occurs and tree/bytes/mtime
are unchanged; a valid empty initialized store yields only bundled authority.

### AC-7: Atomicity and races (FR-14, FR-15, NFR-3)
Given fault injection after insert/before head/at commit and concurrent stale-parent requests,
When promotion fails, Then previous head and canonical fixture bytes persist; at most one writer
wins and no orphan generation is usable as authority.

### AC-8: Live consumer visibility (FR-19)
Given one long-lived default calendar facade held by automation and continuity,
When a synthetic next-year generation is promoted, Then the next decisions see it without restart;
each range is pinned to one snapshot even if promotion is injected mid-iteration.

### AC-9: Sync snapshot and mixed range (FR-20)
Given a plan made at generation A and a changed generation B, or a known/unknown range,
When sync executes, Then stale authority stops before provider calls and mixed coverage cannot be
ready or update last-known-good. Existing confirmed legacy behavior remains green.

### AC-10: Policy boundaries (FR-21)
Given clocks immediately before/at Oct 1, Dec 15, year rollover and data availability,
When status/planning runs, Then not_due/pending/action_required/current-year blocking are exact;
unknown-year weekdays and weekends never become open from policy alone.

### AC-11: One durable maintenance slot (FR-11, FR-12, FR-22)
Given repeated startup/daily/monthly calls, crash after reservation and OPEN/HALF_OPEN health,
When the existing job runs, Then requests stay within one slot budget and zero for a blocked
circuit; missing sources and interrupted slots never trigger a market refresh or another attempt.

### AC-12: Read-only API/CLI (FR-1, FR-23, FR-24)
Given disabled/valid/missing/corrupt private runtime and hard market/provider/path sentinels,
When status and default plan commands run, Then they do no writes, provider/market construction
or production/NAS access; response counters and exit statuses match the documented contract.

### AC-13: Source/maintenance integration (FR-22, FR-24, FR-26)
Given a private staged package and fake official/BaoStock acquisition,
When the existing calendar job and direct maintenance CLI execute, Then the candidate is recorded
before calls, results are safe/closed, promotion becomes visible, and a second same-day call is
skipped. Legacy five-agent assets remain unchanged.

### AC-14: Protected contracts (FR-25, NFR-1, NFR-4)
Given baseline golden bytes, qualification schemas and synthetic canonical trees,
When all positive/negative calendar flows run, Then hashes and canonical trees remain unchanged,
secondary remains blocked and full offline regression plus independent H0/M0 review passes.

## Edge cases

- EC-1: Leap year, Feb 29, year boundaries, empty/open-all-weekday schedules: exact full-year rule
  applies; metadata review is authority, no minimum trading-count guess (AC-1, AC-3, AC-10).
- EC-2: Future publication/review time, duplicate JSON keys, Unicode notice numbers, bad hash,
  blank review ID, non-finite numbers: reject safely before writes (AC-1).
- EC-3: Official redirect/auth/status failure, missing Content-Length, streaming overrun, bad
  encoding body: hash bytes, bound reads, no parser fallback or retry (AC-2).
- EC-4: Machine SDK returns only opens or stops one civil row early, including the last closed
  day: fail completeness even if open-date set matches (AC-3).
- EC-5: Store disappears/corrupts after a prior successful read, path replaced or lock contended:
  do not serve stale authority (AC-6, AC-8).
- EC-6: DB write/disk/transaction fault or stale head: rollback; no pointer compensation against
  canonical market data (AC-7).
- EC-7: Changed source package during a spent daily slot or interrupted worker: retain audit and
  skip until a later allowed slot (AC-11).
- EC-8: No official package exists at policy milestone: explicit pending/action_required, not an
  inferred calendar or a retry loop (AC-10, AC-11).
- EC-9: Two different future amendments or bundled release checksum change: stale parent/base
  mismatch fails closed; a reviewed migration is required, not implicit rebase (AC-5, AC-6).

## API contracts

`GET /api/v1/market/calendar-generation` accepts no client date or execute parameter. It is a
standalone read-only dependency path, not a market-store endpoint. HTTP 200 covers bounded domain
status; invalid settings/query return sanitized 422; unexpected programming errors remain 500.

```typescript
interface CalendarGenerationStatusV1 {
  schema_version: 1;
  runtime_enabled: boolean;
  status: "disabled" | "ready" | "unavailable";
  generation_sha256: string | null;
  covered_years: number[];
  covered_through: string | null; // maximum listed year end; not proof across unlisted gaps
  current_year_ready: boolean;
  next_year: number;
  next_year_status: "confirmed" | "not_due" | "pending" | "action_required";
  last_outcome: string | null; // FR-26 vocabulary only
  conflict_detected: boolean;
  provider_requests: 0;
  canonical_writes: false;
}
```

CLI status exits 0 for disabled/ready, 1 unavailable, 2 invalid config/input. Stage/maintenance
plans exit 0 when valid and include `network_requests=0`, `writes_calendar_state=false`,
`canonical_writes=false`. Stage execute exits 0 staged/idempotent, 1 quarantined/control failure,
2 invalid package. Maintenance execute exits 0 promoted/not_due/source_pending/already_attempted,
1 acquisition/conflict/circuit/control failures, 2 invalid configuration. Execute results include
safe outcome, source/generation hash or null, actual official/machine request counts and calendar
write flag. No body, URL, file path, SQL, token or original exception is returned.

## Data models

All new authority models are frozen, extra-forbid, and revalidated at public boundaries. Calendar
source metadata may contain reviewed official URLs; transport/status events may not.

| Entity | Fields and types | Constraints |
|---|---|---|
| OfficialCalendarScheduleV1 | exchange, year, coverage_start/end, title, notice_no, official_url, published_on, body_sha256, closed_dates, review_id, reviewed_on, schedule_sha256 | full year; strict sorted weekday closures; exact origin; reviewed hashes/dates |
| CalendarSourceBundleV1 | schema_version=1, rule_version, year, schedules[2], source_sha256 | exact SSE/SZSE order; all identities required |
| CalendarMachineObservationV1 | provider, contract_version, range_start/end, observed_at, days[{date,is_open}], observation_sha256 | 365/366 complete ordered dates; strict boolean |
| CalendarGenerationV1 | sequence, parent_sha256/null, bundled_sha256, source_sha256, official_body_hashes[2], machine, promoted_at, generation_sha256 | derived/validated; parent sequence + 1; complete authority chain |
| CalendarCandidate | source_sha256, canonical source bytes, staged_at, staging_sequence, admission/reason | immutable; unique hash; disagreement quarantined |
| CalendarOfficialObject | body_sha256, body_bytes | bounded, immutable, verified bytes |
| CalendarPromotion | sequence, generation_sha256, canonical generation bytes | append-only; schema guards; no replacement |
| CalendarHead | singleton=1, sequence, generation_sha256 | atomic compare-and-swap; must equal latest promotion |
| CalendarMaintenanceAttempt | target_year + slot_date PK, source_sha256, started_at, finished_at/null, outcome | reserve before calls; never reused; terminal outcome safe |

SQLite schema/version and its immutable triggers are frozen in this version. Readers validate the
schema and complete hash-linked authority graph, not merely the mutable head. A missing terminal
attempt remains spent; attempts are audit, never promotion authority. `CalendarSyncStore` continues
to keep machine transport/sync observations; a generation owns a verified immutable observation
copy rather than trusting a mutable success flag.

## Out of scope

- OS-1: Automatic search/discovery or unchecked parsing of unseen annual notices. There is no
  reviewed discovery API; a source locator/extraction package is an explicit operator input.
- OS-2: Live retrieval, production installation or enabling runtime mode in Application Support.
  Acceptance uses offline provider/HTTP fakes; no real future-year availability is claimed.
- OS-3: Canonical failover, symbol mixing, second-source adjustment/suspension/unit qualification,
  exact-session universe, replication/restore and later Task16+ scope.
- OS-4: Rewriting bundled JSON, existing canonical bar/evidence/schema/hash formats or the R2-F3
  descriptor-bound `VerifiedConfirmedCalendarReader`. The latter remains a separate qualification
  authority, not an alias to the live operational calendar.
- OS-5: Reworking historical backfill's provider calendar enumeration or price/analysis internals.
  Runtime calendar gates are integrated wherever current code already consumes official calendar;
  canonical provider fetch/quality contracts remain intact.

## Delivery gate

Only independent H0/M0 review of the exact final commit plus AC-1 through AC-14 evidence permits
`R2-F4.1 CALENDAR RUNTIME GO / PRODUCTION ENABLEMENT NOT CLAIMED / R2-F4 NO-GO`.
Pause at that gate for human review; do not start the universe subversion. Technical design
clarifications and review fixes follow the owner's delegated approval policy without new prompts.
