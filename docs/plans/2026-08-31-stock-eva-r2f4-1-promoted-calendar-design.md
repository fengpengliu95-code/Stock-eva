# Stock EVA R2-F4.1 Promoted Runtime Calendar Design

**Author:** Codex root, R2-F delivery lead

**Date:** 2026-08-31 (Asia/Shanghai)

**Status:** SPEC APPROVED / TASK 1 IN PROGRESS / R2-F4.1 NO-GO / R2-F4 NO-GO

**Specification review:** Independent SPEC GO at
`790c832684f9f540488310080715dad12a92f27a` (H=0, M=0). The implementation must use
an explicit closed Literal/enum for `last_outcome`, as required by FR-26.

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
- FR-5: Every structured SHA identity MUST use canonical UTF-8 JSON (sorted keys, compact separators,
  ensure_ascii=false, allow_nan=false, trailing newline) and its versioned domain prefix plus
  newline. Public digest fields MUST be required lowercase 64-hex strings, not optional sentinels.
  All write/authority boundaries MUST revalidate serialized input, including model_copy/construct.
  Raw official bodies are the explicit exception: `body_sha256=sha256(body_bytes).hexdigest()`.
  Body bytes mean HTTP payload after standard content decoding, before any text decoding,
  whitespace normalization or HTML parsing; the 1 MiB bound applies to these decoded bytes.
- FR-6: Source staging MUST validate metadata, publication/review dates against the trusted
  Shanghai clock and both schedule hashes before writes. Individually valid but disagreeing
  SSE/SZSE schedules MUST be persisted as quarantined candidates and cause zero network requests.
  Agreement means equality of the complete civil-day open/closed maps derived independently using
  FR-4. Metadata, notice body hashes and schedule hashes are independently verified and are NOT
  required to be equal between exchanges; different valid announcements may express the same map.
  Malformed source packages MUST fail before creating a database, lock or directory.
- FR-7: Store construction MUST be lazy and perform zero path access/initialization. Pure
  `plan_stage(source, now)` MUST finish validation before `stage_execute(source, now)` is allowed
  to initialize anything. Staging the same source hash MUST be idempotent. Sources and generations MUST be immutable;
  a new notice/extraction requires a new source hash. Source staging MUST never promote a year.
- FR-8: Source retrieval MUST use HTTPS and exact allowlisted official origins (`www.sse.com.cn`,
  `www.szse.cn`, `investor.szse.cn`), default port only, no userinfo/query/fragment, no redirect,
  no environment credentials/proxy authentication, and no retry. URLs remain local authority
  metadata; public operational errors MUST NOT include URLs, body content or raw exceptions.
- FR-9: Each official body MUST be nonempty, bounded at 1 MiB and match the staged SHA-256 exactly. Missing,
  changed, oversized or failed bodies MUST leave the candidate unpromoted. Verified official body
  bytes MUST be retained as immutable calendar-control evidence, never as canonical market RAW.
- FR-10: Add an isolated BaoStock `calendar_days(start,end)` capability without changing the return
  contract of `trading_dates()` or the canonical provider adapter. It MUST validate exact fields,
  exactly one row per requested civil date, chronological order, unique dates, flags exactly
  `0`/`1`, and inclusive complete coverage before returning `(date, bool)` pairs. A missing closed
  date is an incomplete response, even if all expected open dates were returned.
- FR-11: Live maintenance MUST use the existing pinned BaoStock transport, fixed endpoint
  `trade_dates`, max_attempts=1 and unchanged timeout. It MUST preserve transport identity/audit and
  persistent health accounting. The maintenance worker MUST construct a dedicated BaoStock
  instance with `max_attempts=1`, never mutate/reuse the canonical provider instance. Missing,
  corrupt or unreadable health state MUST yield `CONTROL_STATE_UNAVAILABLE` with zero requests;
  the worker MUST NOT initialize/repair it. Only a verified existing CLOSED health snapshot allows
  acquisition. OPEN/HALF_OPEN health MUST stop acquisition before official or
  machine requests; this worker MUST NOT close circuits, probe, retry or refresh market data.
- FR-12: Maintenance MUST stage the candidate before requests and reserve a durable slot keyed by
  `(target_year, Shanghai_date)` before acquisition. Reservation MUST persist the exact source hash
  and `expected_parent_sha256` observed inside its transaction. Promotion MUST use this recorded
  parent, not a later head; generation and attempt parent/source/slot identities MUST agree.
  At most two official HTTP requests and one
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
  year, but this version supports promotion targets only in the trusted clock's current or next
  Shanghai year (including the low-level promotion boundary). Historical years and years beyond
  next year are outside this contract. It MUST NOT skip a year. It MUST NOT change any open/closed status on or before the prior
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
- FR-19: `live.snapshot()` MUST return a concrete immutable calendar snapshot loaded once.
  Every continuity scan, automation decision/run, calendar-sync plan/execute, API market
  status/summary, portfolio/fund-flow operation and supplement ingestion operation MUST capture
  it at the external operation boundary and use that same object for all status/range/source
  calls. Individual live calendar methods also snapshot once before their internal date loops.
  The default
  calendar factory and long-lived automation/continuity/calendar-sync consumers MUST see a later
  promotion on their next operation without restart. A range MUST NOT mix generations mid-loop.
  Concrete injected test/qualification calendars MUST retain their existing behavior.
- FR-20: Calendar-sync MUST freeze authority for a plan/execute operation, reject an authority
  checksum change before provider access, and never report `ready` for mixed known/unknown ranges.
  Existing conflicts still quarantine; unknown portions produce `observed_only`. Stale plan
  execution MUST return `CalendarSyncResult(status='error',
  failure_code='CALENDAR_AUTHORITY_CHANGED')` before provider access, with no sync-state update;
  CLI exit is 1. This is not `PARENT_CHANGED` (which is generation promotion CAS failure).
  Add `failure_code: Literal['CALENDAR_AUTHORITY_CHANGED'] | None = None` to CalendarSyncResult;
  ordinary results serialize it as null. This additive top-level CLI result field is not added to
  MarketDataStatus. Revalidate the result before output; a non-null failure_code requires status
  error, and authority drift must never produce a persisted CalendarSyncRun.
- FR-21: Next-year policy MUST be based on Shanghai dates: before October 1, absent next-year
  authority is `not_due`; October 1 through December 14 is `pending`; from December 15 it is
  `action_required`. Missing current-year authority is always blocking. A future date/year MUST
  never become usable because of policy status alone; a verified bundled or promoted year is
  required. Existing bundled years remain valid baselines, newly added years require promotion.
- FR-22: The existing `calendar-sync --startup --execute` LaunchAgent and in-process calendar loop
  MUST invoke at most one due runtime maintenance slot. Select the latest staging_sequence for
  each year (including a quarantined latest candidate; never silently fall back to an older one).
  Priority is: an unpromoted current-year revision, then missing current-year authority, then
  next year from October 1 if missing or if its latest candidate differs from active source.
  Already-active source with no newer candidate is `NOT_DUE`. A missing staged source means `SOURCE_PENDING`, zero
  requests, no head change. The legacy current/historical reconciliation remains a separate
  bounded operation. No sixth LaunchAgent or change to market refresh cadence is allowed.
- FR-23: Add zero-write `calendar-generation-status` and
  `GET /api/v1/market/calendar-generation`; they MUST return bounded generation/coverage/policy/
  conflict states and zero request/write counters, without market store or provider construction.
  Existing `/market/status` keeps its schema and uses the same calendar facade for expected dates.
- FR-24: Add `calendar-generation-stage --source PATH [--execute]` and
  `calendar-maintenance [--year YYYY] [--execute]`. Explicit year may be only the current or next
  Shanghai year and may process a reviewed revision before October 1; it does not bypass health,
  parent, history, already-active-source or daily-slot gates. Default planning MUST have zero writes/network requests.
  Source files MUST be bounded regular no-symlink JSON with duplicate-key rejection. Staging
  execute writes only calendar control; maintenance execute writes only calendar control/sync and
  existing provider-health audit. Neither may construct market/NAS stores, publish, or auto-shadow.
- FR-25: No ProviderId, canonical candidate/selection/manifest, frozen R2-F2/R2-F3 contracts or
  R2-F4.0 capability projection may change. Runtime calendar availability MUST NOT silently mark
  the TickFlow promoted-calendar capability QUALIFIED; that needs a later versioned authority.
- FR-26: Public operational outcomes MUST use a closed safe vocabulary: `STAGED`, `ALREADY_STAGED`, `SOURCE_PENDING`,
  `SOURCE_INVALID`, `SOURCE_CONFLICT`, `OFFICIAL_UNAVAILABLE`, `OFFICIAL_HASH_MISMATCH`,
  `MACHINE_UNAVAILABLE`, `MACHINE_CONFLICT`, `SKIPPED_CIRCUIT_OPEN`, `ALREADY_ATTEMPTED`,
  `PARENT_CHANGED`, `HISTORY_CHANGE`, `CALENDAR_AUTHORITY_CHANGED`, `CONTROL_STATE_UNAVAILABLE`, `NOT_DUE`, `PROMOTED`.
  Candidate admission is exactly `awaiting_machine` with staging reason `STAGED`, or `quarantined`
  with reason `SOURCE_CONFLICT`. An idempotent staging call returns `ALREADY_STAGED` without
  changing the persisted reason; a previously quarantined candidate remains quarantined.
  `RUNNING` is internal attempt state only; status reports it as `ALREADY_ATTEMPTED`, never success.
  Unexpected programming failures MUST not be disguised as successful or unavailable evidence.
- FR-27: The new three CLI paths, standalone API and default live calendar factory MUST use an
  explicit `CalendarRuntimeSettings` plain-model projection, not `BaseSettings/get_settings()`.
  Read only the exact environment keys listed below (no environment enumeration, `.env` loading,
  token names, provider clients or StoragePreflight). Embedded callers MAY pass their already
  resolved Settings through this same field allowlist so the configured private root is retained.

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
When staging is planned/executed, Then only fully valid identities are admitted; raw-body versus
structured-domain vectors and self-digest exclusions match the fixed projection contract; disagreement is
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
When promotion fails, Then previous head and canonical fixture bytes persist; attempt source/parent
remain bound to reservation, at most one writer
wins and no orphan generation is usable as authority.

### AC-8: Live consumer visibility (FR-19)
Given one long-lived default calendar facade held by automation and continuity,
When a synthetic next-year generation is promoted, Then the next decisions see it without restart;
each external operation is pinned to one snapshot even if promotion is injected between status,
range and source calls as well as mid-iteration.

### AC-9: Sync snapshot and mixed range (FR-20)
Given a plan made at generation A and a changed generation B, or a known/unknown range,
When sync executes, Then stale authority reports CALENDAR_AUTHORITY_CHANGED with zero requests/state
writes and mixed coverage cannot be
ready or update last-known-good. Existing confirmed legacy behavior remains green.

### AC-10: Policy boundaries (FR-21)
Given clocks immediately before/at Oct 1, Dec 15, year rollover and data availability,
When status/planning runs, Then not_due/pending/action_required/current-year blocking are exact;
unknown-year weekdays and weekends never become open from policy alone.

### AC-11: One durable maintenance slot (FR-11, FR-12, FR-22)
Given repeated startup/daily/monthly calls, crash after reservation, missing/corrupt health and OPEN/HALF_OPEN health,
When the existing job runs, Then requests stay within one slot budget and zero for a blocked
circuit; missing sources and interrupted slots never trigger a market refresh or another attempt.

### AC-12: Read-only API/CLI (FR-1, FR-23, FR-24, FR-27)
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
`canonical_writes=false`. Stage execute exits 0 staged/idempotent, 1 quarantined/control failure
(including idempotent restaging of a quarantined candidate),
2 invalid package. Maintenance execute exits 0 promoted/not_due/source_pending/already_attempted,
1 acquisition/conflict/circuit/control failures, 2 invalid configuration. Execute results include
safe outcome, source/generation hash or null, actual official/machine request counts and calendar
write flag. No body, URL, file path, SQL, token or original exception is returned.
For idempotent staging, the payload outcome is ALREADY_STAGED in both cases; admission remains
awaiting_machine with exit 0, or quarantined with admission reason SOURCE_CONFLICT and exit 1.

## Data models

All new authority models are frozen, extra-forbid, and revalidated at public boundaries. Calendar
source metadata may contain reviewed official URLs; transport/status events may not.

| Entity | Fields and types | Constraints |
|---|---|---|
| OfficialCalendarScheduleV1 | exchange, year, coverage_start/end, title, notice_no, official_url, published_on, body_sha256, closed_dates, review_id, reviewed_on, schedule_sha256 | full year; strict sorted weekday closures; exact origin; reviewed hashes/dates |
| CalendarSourceBundleV1 | schema_version=1, rule_version, year, schedules[2], source_sha256 | exact SSE/SZSE order; all identities required |
| CalendarMachineObservationV1 | provider, contract_version, range_start/end, observed_at, days[{date,is_open}], observation_sha256 | 365/366 complete ordered dates; strict boolean |
| CalendarGenerationV1 | sequence, parent_sha256/null, bundled_sha256, source_sha256, attempt_target_year, attempt_slot_date, official_body_hashes[2], machine, promoted_at, generation_sha256 | derived/validated; parent sequence + 1; complete authority chain |
| CalendarCandidate | source_sha256, canonical source bytes, staged_at, staging_sequence, admission/reason | immutable; unique hash; disagreement quarantined |
| CalendarOfficialObject | body_sha256, body_bytes | bounded, immutable, verified bytes |
| CalendarPromotion | sequence, generation_sha256, canonical generation bytes | append-only; schema guards; no replacement |
| CalendarHead | singleton=1, sequence, generation_sha256 | atomic compare-and-swap; must equal latest promotion |
| CalendarMaintenanceAttempt | target_year + slot_date PK, source_sha256, expected_parent_sha256/null, started_at, finished_at/null, outcome, official_requests, machine_requests | reserve before calls; immutable identity/parent; one terminal update; never reused |
| CalendarSyncResult additive field | failure_code: Literal["CALENDAR_AUTHORITY_CHANGED"] or null, default null | ordinary results null; non-null only with status error; top-level CLI serialization; not persisted for drift |

SQLite schema/version and its immutable triggers are frozen in this version. Readers validate the
schema and complete hash-linked authority graph, not merely the mutable head. A missing terminal
attempt remains spent; attempts are audit, never promotion authority. `CalendarSyncStore` continues
to keep machine transport/sync observations; a generation owns a verified immutable observation
copy rather than trusting a mutable success flag.

### Exact hash projections

Every structured model is projected with `model_dump(mode='json')`: ISO date strings, UTC aware
datetimes serialized consistently as `...Z`, tuples as JSON arrays, all null/default/false/zero
fields retained. The only excluded field is the **top-level self digest** shown below; nested
digests remain included. Model/schema tests MUST freeze a complete valid fixture vector for each
projection, in addition to the canonical primitive vector. No caller may omit another field.

| Digest | Domain | Exact preimage projection |
|---|---|---|
| schedule_sha256 | stock-eva/r2f4.1/calendar-schedule/v1 | all OfficialCalendarScheduleV1 fields except schedule_sha256 |
| source_sha256 | stock-eva/r2f4.1/calendar-source/v1 | schema_version, rule_version, year, complete schedules including their digests |
| observation_sha256 | stock-eva/r2f4.1/calendar-machine/v1 | provider, contract_version, range_start, range_end, observed_at, days with exact date/is_open fields |
| generation_sha256 | stock-eva/r2f4.1/calendar-generation/v1 | all CalendarGenerationV1 fields except generation_sha256; machine includes observation_sha256 |
| bundled_sha256 | stock-eva/r2f4.1/calendar-bundled/v1 | {configs: [complete existing CalendarConfig JSON values ordered by year]} |
| body_sha256 | none | exact HTTP decoded payload bytes; standard sha256, no JSON/domain prefix |

Primitive fixed vectors: canonical JSON for `{zero:0, false:false, null:null, empty:[]}` is
`{"empty":[],"false":false,"null":null,"zero":0}\n` (the final `\n` denotes one byte 0x0a).
The raw-byte vector `b'abc'` hashes to
`ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad`.
The body hashes in a generation are in SSE/SZSE order and MUST equal the source package's expected
body hashes and stored object's raw-byte hashes. SQLite metadata's `schema_sha256` hashes the
exact UTF-8 DDL source below, not a structure that includes schema_sha256 itself.

### Non-credential runtime settings contract

`CalendarRuntimeSettings` is a plain immutable model. Its loader calls `os.environ.get` only for
the keys below, one by one; it does not enumerate environment, call get_settings, inspect .env,
or load credentials. Defaults match existing Settings where fields overlap. New default runtime
factory/maintenance/status paths share this one source. Runtime enablement for unattended use
must be placed in the service process environment; adding the two new keys to a general `.env`
alone is not claimed to enable this env-only lane. An embedded caller may explicitly project an
already-resolved settings object by these field names, without invoking another settings loader.

```text
STOCK_EVA_CALENDAR_RUNTIME_ENABLED                 false
STOCK_EVA_CALENDAR_GENERATION_DATABASE_NAME        calendar_generations.sqlite3
STOCK_EVA_CALENDAR_SYNC_DATABASE_NAME              calendar_sync.sqlite3
STOCK_EVA_LOCAL_CONTROL_DIR                       var/control
STOCK_EVA_LOCAL_LOCK_DIR                          var/locks
STOCK_EVA_PROVIDER_HEALTH_DATABASE_NAME           provider_health.sqlite3
STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS          30.0
STOCK_EVA_AUTO_REFRESH_MIN_REQUEST_INTERVAL_SECONDS 0.5
STOCK_EVA_PROVIDER_CIRCUIT_FAILURE_THRESHOLD       3
STOCK_EVA_PROVIDER_CIRCUIT_COOLDOWN_SECONDS        900.0
STOCK_EVA_PROVIDER_CIRCUIT_PROBE_LEASE_SECONDS     120.0
```

Invalid values produce sanitized 422 (API) or exit 2 (CLI) before path/client access. Relative
control paths are anchored once to the current working directory without resolving symlinks;
all subsequent reader/writer path checks use the anchored path. Existing path/transport minimums
and maximums remain unchanged. Database basenames cannot collide with one another.

### SQLite schema v1 and transition contract

The following DDL is the normative v1 schema. Foreign keys MUST be enabled on every writer
connection; readers MUST verify schema objects/guards and run foreign-key integrity checks
without migrations. `journal_mode=DELETE`, `synchronous=FULL`, and zero SQLite busy timeout apply.
Writers use one nonblocking process lock plus `BEGIN IMMEDIATE`; readers use shared nonblocking
locks and read-only URI connections. Metadata/head initialization occurs only after source
prevalidation during explicit stage execute. No scheduled worker initialization is allowed.

```sql
CREATE TABLE calendar_generation_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    schema_sha256 TEXT NOT NULL CHECK(length(schema_sha256) = 64),
    bundled_sha256 TEXT NOT NULL CHECK(length(bundled_sha256) = 64)
);
CREATE TABLE calendar_generation_candidate (
    staging_sequence INTEGER PRIMARY KEY CHECK(staging_sequence > 0),
    source_sha256 TEXT NOT NULL UNIQUE CHECK(length(source_sha256) = 64),
    payload_json TEXT NOT NULL CHECK(length(payload_json) <= 262144),
    staged_at TEXT NOT NULL,
    admission TEXT NOT NULL CHECK(admission IN ('awaiting_machine', 'quarantined')),
    reason TEXT NOT NULL CHECK(
        (admission = 'awaiting_machine' AND reason = 'STAGED') OR
        (admission = 'quarantined' AND reason = 'SOURCE_CONFLICT'))
);
CREATE TABLE calendar_official_object (
    body_sha256 TEXT PRIMARY KEY CHECK(length(body_sha256) = 64),
    body_bytes BLOB NOT NULL CHECK(length(body_bytes) BETWEEN 1 AND 1048576)
);
CREATE TABLE calendar_maintenance_attempt (
    target_year INTEGER NOT NULL CHECK(target_year BETWEEN 1900 AND 9998),
    slot_date TEXT NOT NULL,
    source_sha256 TEXT NOT NULL REFERENCES calendar_generation_candidate(source_sha256),
    expected_parent_sha256 TEXT REFERENCES calendar_generation_promotion(generation_sha256),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    outcome TEXT NOT NULL CHECK(outcome IN (
        'RUNNING', 'OFFICIAL_UNAVAILABLE', 'OFFICIAL_HASH_MISMATCH',
        'MACHINE_UNAVAILABLE', 'MACHINE_CONFLICT', 'SKIPPED_CIRCUIT_OPEN',
        'PARENT_CHANGED', 'HISTORY_CHANGE', 'CONTROL_STATE_UNAVAILABLE', 'PROMOTED')),
    official_requests INTEGER NOT NULL DEFAULT 0 CHECK(official_requests BETWEEN 0 AND 2),
    machine_requests INTEGER NOT NULL DEFAULT 0 CHECK(machine_requests BETWEEN 0 AND 1),
    PRIMARY KEY(target_year, slot_date),
    CHECK((outcome = 'RUNNING' AND finished_at IS NULL AND
           official_requests = 0 AND machine_requests = 0) OR
          (outcome != 'RUNNING' AND finished_at IS NOT NULL)),
    CHECK(outcome != 'PROMOTED' OR (official_requests = 2 AND machine_requests = 1))
);
CREATE TABLE calendar_generation_promotion (
    sequence INTEGER PRIMARY KEY CHECK(sequence > 0),
    generation_sha256 TEXT NOT NULL UNIQUE CHECK(length(generation_sha256) = 64),
    parent_sha256 TEXT REFERENCES calendar_generation_promotion(generation_sha256),
    source_sha256 TEXT NOT NULL REFERENCES calendar_generation_candidate(source_sha256),
    attempt_target_year INTEGER NOT NULL,
    attempt_slot_date TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK(length(payload_json) <= 262144),
    promoted_at TEXT NOT NULL,
    UNIQUE(sequence, generation_sha256),
    UNIQUE(attempt_target_year, attempt_slot_date),
    FOREIGN KEY(attempt_target_year, attempt_slot_date)
        REFERENCES calendar_maintenance_attempt(target_year, slot_date)
);
CREATE TABLE calendar_generation_head (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    generation_sha256 TEXT,
    CHECK((sequence = 0 AND generation_sha256 IS NULL) OR
          (sequence > 0 AND generation_sha256 IS NOT NULL)),
    FOREIGN KEY(sequence, generation_sha256)
        REFERENCES calendar_generation_promotion(sequence, generation_sha256)
);
CREATE TRIGGER calendar_meta_no_update BEFORE UPDATE ON calendar_generation_meta
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_meta_no_delete BEFORE DELETE ON calendar_generation_meta
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_candidate_no_update BEFORE UPDATE ON calendar_generation_candidate
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_candidate_no_delete BEFORE DELETE ON calendar_generation_candidate
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_object_no_update BEFORE UPDATE ON calendar_official_object
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_object_no_delete BEFORE DELETE ON calendar_official_object
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_promotion_no_update BEFORE UPDATE ON calendar_generation_promotion
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_promotion_no_delete BEFORE DELETE ON calendar_generation_promotion
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_head_no_delete BEFORE DELETE ON calendar_generation_head
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_attempt_no_delete BEFORE DELETE ON calendar_maintenance_attempt
BEGIN SELECT RAISE(ABORT, 'calendar_immutable'); END;
CREATE TRIGGER calendar_attempt_terminal_only BEFORE UPDATE ON calendar_maintenance_attempt
WHEN OLD.outcome != 'RUNNING' OR NEW.outcome = 'RUNNING'
  OR NEW.target_year != OLD.target_year OR NEW.slot_date != OLD.slot_date
  OR NEW.source_sha256 != OLD.source_sha256
  OR NEW.expected_parent_sha256 IS NOT OLD.expected_parent_sha256
  OR NEW.started_at != OLD.started_at
BEGIN SELECT RAISE(ABORT, 'calendar_attempt_immutable'); END;
```

Writers explicitly allocate `max(staging_sequence)+1` under transaction; no AUTOINCREMENT/internal
sequence table is needed. Initialization inserts metadata and head `(1,0,NULL)` atomically. Stored
JSON MUST be canonical, UTF-8 byte-bounded and strict-model valid, regardless of weaker SQL length
constraints. All text timestamps are normalized aware UTC; slot_date is the Shanghai date of
started_at. Application validation checks lowercase hex, exact ISO dates and strict types.

Reservation validates the latest candidate, current complete head and admissible target year,
then inserts one RUNNING row with the observed parent. `promote` takes a reserved slot identity,
not arbitrary parent authority; it re-reads that RUNNING row and verifies source/parent/timestamps.
It inserts objects/generation, replays and readback-verifies them, then updates head with
`WHERE singleton=1 AND sequence=:old_sequence AND generation_sha256 IS :reserved_parent`;
rowcount MUST be one. The same transaction sets attempt to PROMOTED with actual 2/1 counts.
Source publication/review must not be after reservation; machine observed_at must be between
reserved started_at and promoted_at; promotion timestamps must not move backwards along the chain.
Reader verification requires exact contiguous sequence starting at 1, matching parent links,
head equal to the highest sequence, and every generation's referenced attempt terminal PROMOTED
with matching source/parent/slot and counts. Terminal PROMOTED attempts without a generation are
also invalid. A failed promotion rolls back all these writes; a separate best-effort terminal
failure update may spend the already-reserved slot without granting any authority. A crash leaves
RUNNING permanently spent for that day. No slot reset/delete API is provided.

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
