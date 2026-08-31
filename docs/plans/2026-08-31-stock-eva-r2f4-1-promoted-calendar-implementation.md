# Stock EVA R2-F4.1 Promoted Runtime Calendar Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: use `superpowers:executing-plans`,
> `superpowers:test-driven-development` and `superpowers:verification-before-completion` task by
> task. Use one implementation/review subagent at a time; prefer gpt-5.6-luna. Technical decisions
> are delegated; pause only at the final subversion GO for human review.

**Goal:** Add immutable, verified operational calendar generations and bounded next-year
maintenance without changing canonical provider/publication authority.

**Architecture:** A separate calendar generation SQLite sidecar stores immutable source packages,
verified official objects, promotion lineage and a transactional head. A strict reader/live facade
composes bundled plus promoted years. Reviewed source locators drive bounded official retrieval
and complete BaoStock civil-day reconciliation in the existing calendar job.

**Tech Stack:** Python 3.12, SQLite, Pydantic 2, httpx, FastAPI, argparse, pytest, SHA-256.

**Design:** `docs/plans/2026-08-31-stock-eva-r2f4-1-promoted-calendar-design.md`

**Base:** `f5eaa99079db876704ee0281def5fc58af7bcc3c`

**Worktree:** `/Users/finlay/.codex/worktrees/r2f4-1/Stock- evaluation`

**Branch:** `codex/r2-f4-1-promoted-calendar`

**Initial evidence:** offline frozen uv sync succeeded; full baseline pytest completed with exit 0
(2202 tests), with only the existing Starlette TestClient/httpx deprecation warning.

**Task 0:** Independent SPEC GO at `790c832684f9f540488310080715dad12a92f27a`, H=0/M=0;
strict validator 100/100 with zero warnings. Implementation may begin. Final subversion GO is
not yet claimed.

**Task 1 progress:** In progress, not accepted. The specification extractor produced 14 private
scratch stubs (all expected failures); these are traceability aids, not implementation acceptance.
The latest test-first checkpoint has 36 focused cases: 30 passing and 6 expected contract failures.
Fault injection, descriptor-bound writer races and complete transition-graph verification remain
required before Task 1 can enter independent spec/quality review. An earlier full regression run
passed before the expanded RED cases; it is not the current acceptance gate.

## Execution boundaries

- No real HTTP/Provider, credential reads, Application Support, production control/canonical,
  `/Volumes/Stock`, deployment, LaunchAgent invocation, branch merge or remote push.
- Do not touch the dirty main worktree or previous reviewed worktrees.
- Preserve all bundled calendar JSON, R2-F2 golden files and canonical format/selection code,
  R2-F3 qualification models/readers/evidence and `market/failover.py` byte-for-byte.
- New `BaoStockProvider.calendar_days` is additive. Do not change `trading_dates`/fetch behavior,
  timeouts, retries, socket patch or canonical BaoStock adapter.
- Tests write only their private temporary roots. Fake network and strict provider constructors
  must make unexpected real access fail loudly. No source notice is assumed actually available.
- Every implementation task gets spec-compliance review followed by independent quality review.
  Do not start the next task with unresolved H/M findings.

## Task 0: Freeze and review the specification

1. Validate the design using the strict validator below.
2. Review FR/AC/model/CLI traceability and the approved umbrella Task15 scope.
3. Commit design and plan only; ask a fresh read-only subagent for SPEC GO/NO-GO.
4. Resolve findings in documents before code. Record approved specification commit/status.
5. Run the spec test extractor into a private scratch file and map AC-1 through AC-14 to the
   concrete tests below; do not commit generated `NotImplementedError` placeholders.

```bash
uv run --offline python \
  /Users/finlay/.codex/skills/claude-skills--engineering--spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-08-31-stock-eva-r2f4-1-promoted-calendar-design.md --strict
git diff --check
```

## Task 1: Immutable source, generation, and read-only authority store

**Files:**

- Create `backend/app/market/calendar_generation.py`
- Create `tests/test_market_calendar_generation.py`
- Do not modify existing calendar consumers yet.

### Step 1.1: Write RED identity and store tests

Use synthetic reviewed source URLs (only injected clients will ever fetch them), two distinct
body-byte fixtures, a fixed Shanghai clock, and complete weekday/closure maps. Start with:

```python
def test_stage_never_promotes_and_repeated_source_is_idempotent(tmp_path):
    store = CalendarGenerationStore(tmp_path / "calendar_generations.sqlite3")
    source = source_fixture(year=2027)
    first = store.stage_execute(source, now=NOW)
    before = tree_bytes(tmp_path)
    assert store.stage_execute(source, now=NOW).source_sha256 == first.source_sha256
    assert tree_bytes(tmp_path) == before
    assert store.read().generation_sha256 is None

def test_promoted_runtime_generation_extends_bundled_without_rewrite(tmp_path):
    before = bundled_bytes()
    store = staged_store(tmp_path)
    attempt = store.reserve_attempt(source_sha256=SOURCE_SHA, target_year=2027, now=NOW)
    result = store.promote(
        source_sha256=SOURCE_SHA, official_bodies=BODIES,
        machine=complete_machine_fixture(2027), attempt=attempt, now=NOW,
    )
    assert result.outcome == "PROMOTED"
    assert store.read().calendar.session_status(date(2027, 1, 4)) == "open"
    assert bundled_bytes() == before
```

Add parameterized tests for wrong/missing/null digests, extra keys, duplicate exchanges/dates,
future review/publication, out-of-year closure, weekend closure, unsorted data, Unicode notice
identity, model_copy/construct bypass, quarantined dual-source conflict, incompatible base,
head rewind/missing parent, incomplete machine days, old-session changes and skipped years.

### Step 1.2: Run RED

Run `.venv/bin/pytest tests/test_market_calendar_generation.py -q` and record the expected missing
implementation failure. Keep tests asserting actual persisted bytes/authority, not mock counters.

### Step 1.3: Implement the closed domain

Use explicit `build_*` factories for computed hashes; public models require their identities.
Store construction is path-access-free; pure `plan_stage` validates before `stage_execute` can
initialize. All structured preimages exclude only their own top-level digest and retain nested
digests/defaults/nulls exactly as the design's projection table defines. Official body hashes use
plain raw-byte SHA-256, not the structured helper. Add hardcoded complete-model vectors.
The common digest contract is exactly:

```python
def canonical_json_bytes(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")

def domain_sha256(domain, value):
    return hashlib.sha256(domain.encode("ascii") + b"\n" + canonical_json_bytes(value)).hexdigest()
```

Domains: `stock-eva/r2f4.1/calendar-source/v1`, `.../calendar-schedule/v1`,
`.../calendar-machine/v1`, `.../calendar-generation/v1`, `.../calendar-bundled/v1`.
No imports from `failover.py` or changes to its frozen policy hashes.

Implement the exact normative SQLite DDL from the design appendix for candidates, official
objects, promotions, head, parent-bound attempts and base/schema metadata. Preserve all specified
immutable/terminal-only guards. Validate
schema and hashes independently on read. Separate explicit initialization/staging from read-only
open; enabled missing state is unavailable, never an implicit initialization or older fallback.

Promotion ordering:

```text
revalidate source + body digests + machine -> BEGIN IMMEDIATE
read/verify active chain + reserved RUNNING attempt -> compare recorded parent -> protect history
insert verified official objects -> insert immutable generation -> readback hash
CAS head -> mark the same attempt PROMOTED -> COMMIT
```

Any exception rolls back. Bound paths with no-follow/regular-file/owner/permission checks and
nonblocking locking; use safe private file creation only in explicit stage execute. Test
database/journal substitution, symlink/hardlink, changed schema, lock contention and missing file.

### Step 1.4: Run GREEN and self-review

Run all Task1 tests, ruff/format/diff checks. Replay disk/transaction failure and stale-parent race
against private stores. Explicitly prove no canonical fixture byte changed. Commit Task1.

### Step 1.5: Independent reviews

First reviewer checks AC-1/4/5/6/7 contract coverage; only after SPEC PASS run quality/safety review.
Fix H/M via RED/GREEN and re-review the exact new commit before Task2.

## Task 2: Complete machine transport and bounded source maintenance

**Files:**

- Add one method to `backend/app/market/baostock.py`
- Create `backend/app/market/calendar_maintenance.py`
- Modify `backend/app/market/calendar_sync.py` only for mixed-range/stale-plan safety and reuse
- Create `tests/test_market_calendar_maintenance.py`
- Add scoped cases in `tests/test_baostock_provider.py` and `tests/test_calendar_sync.py`

### Step 2.1: Write RED acquisition, policy and slot tests

Test leap/non-leap complete rows and every missing/duplicate/invalid flag/order case, including a
missing **closed** final day with an unchanged open-date set. Assert existing `trading_dates()`
contract remains unchanged. Add fake HTTP redirect/disallowed-host/hash/size/network failure tests.

```python
def test_machine_missing_closed_day_is_not_success():
    provider = BaoStockProvider(client=fake_calendar_client(rows_without_closed_day()),
                                max_attempts=1)
    with pytest.raises(BaoStockError):
        provider.calendar_days(date(2027, 1, 1), date(2027, 12, 31))

def test_next_year_thresholds():
    assert policy_status("2026-09-30") == "not_due"
    assert policy_status("2026-10-01") == "pending"
    assert policy_status("2026-12-15") == "action_required"
```

Add durable slot/crash/restart/circuit tests asserting candidate and reservation exist before
client calls; two official requests plus one machine maximum; no second attempt after a failure,
process interruption, repeated startup or changed source in the same `(year,day)` slot.

### Step 2.2: Run RED

Run the three changed test modules; capture expected failures for calendar_days, maintenance
worker and mixed known/unknown sync. Do not loosen existing fixtures/gates to obtain GREEN.

### Step 2.3: Implement acquisition and maintenance

`calendar_days` uses the same existing refresh/login/_read/logout path and `trade_dates` endpoint,
then validates full civil-day coverage before returning dated booleans. Do not make the existing
canonical path depend on the new method.

Official fetcher: injectable httpx client, `trust_env=False`, TLS verification, no redirects,
max_attempts=1, fixed 5/30-second timeouts, stream-bound 1 MiB; count a request only once the client
is called. Accept only the two staged official source URLs. Preserve safe error class/counts.

Maintenance: no candidate or policy not due -> zero-request safe outcome. Before acquisition,
validate source/admission, current parent and existing health; missing/corrupt health is
CONTROL_STATE_UNAVAILABLE with zero requests and no initialization. Construct a dedicated
BaoStockProvider(max_attempts=1) only when executing the verified path; never mutate a canonical
instance. Reserve a slot binding source and parent. Fetch both official bodies,
then a complete machine observation; reconcile exact dates; promote under current-parent CAS.
Reuse calendar-sync observation/health accounting where possible without trusting its mutable
success flag as authority. Stage disagreement stays quarantined. Record explicit failure outcome;
do not auto-shadow, auto-refresh, reset a breaker or replace a last-good generation on failure.
Implement exact automatic candidate priority and explicit `--year` current/next-only override from
FR-22/24. Latest quarantined candidates cannot be skipped, current-year future revisions have a
reachable path, and neither a year override nor changed source bypasses the daily slot.

Fix legacy `CalendarSyncService` to pin a concrete calendar snapshot for each plan/execute and
reject plan checksum drift before requests. Classify a mixed known/unknown successful observation
as `observed_only`, not `ready`; retain conflict priority and old known-only semantics.
Drift returns status=error/failure_code=CALENDAR_AUTHORITY_CHANGED, writes no sync state and exits
CLI 1. Do not reuse generation PARENT_CHANGED for this distinct stale-plan condition.
CalendarSyncResult adds only `failure_code: Literal['CALENDAR_AUTHORITY_CHANGED'] | None = None`;
normal CLI result JSON includes failure_code=null. Enforce non-null only with status error.

### Step 2.4: GREEN, commit and serial reviews

Run focused Task1/Task2 + full existing `tests/test_baostock_provider.py` and
`tests/test_calendar_sync.py`; ruff/format/diff. Commit. Request spec review for AC-2/3/9/10/11,
then independent quality review; repair all H/M before Task3.

## Task 3: Live reader, CLI/API and existing calendar job integration

**Files:**

- Modify `backend/app/config.py`, `backend/app/storage/layout.py`, `.env.example`
- Modify `backend/app/market/calendar.py`, `backend/app/market/calendar_sync.py`
- Modify `backend/app/api/market.py`, `backend/app/cli.py`, `backend/app/main.py`
- Modify `backend/app/market/continuity.py`, `backend/app/market/automation.py`,
  `backend/app/market/supplement_ingestion.py`, `backend/app/fund_flow/service.py`,
  `backend/app/api/user.py`, `backend/app/api/fund_flow.py` only for operation-snapshot pinning
- Add `tests/test_market_calendar_runtime.py`
- Extend `tests/test_market_get_read_only.py`, `tests/test_market_automation.py`,
  `tests/test_calendar_sync.py`, `tests/test_launchagent_assets.py`
- Do not change frozen MarketDataStatus fields, canonical storage or R2-F3 snapshot readers.

### Step 3.1: RED live-snapshot and public-boundary tests

Construct one live calendar facade, hold it in SchedulePolicy/ContinuityInventory, then promote a
private synthetic year. Prove the next operation sees it without restart and that one range read
cannot mix snapshots. Promote/corrupt/delete between operations to prove no stale fallback.

Test default-disabled mode avoids runtime paths; enabled missing/locked/corrupt control is
unavailable with no initialization. Exercise status API/CLI through real strict readers under
hard `builtins.open`/OS/network/provider/market-store sentinels for production paths.

Test default stage/maintenance CLI is zero-write/zero-network, invalid package is pre-write,
execute-mode errors are sanitized with exact exit codes, and the existing calendar job automatically
performs a staged next-year slot without changing its five-agent assets or invoking market refresh.

### Step 3.2: Run RED

Run new runtime tests and scoped existing tests before production integration. Verify each failure
is the missing contract, not faulty setup or relaxed old assertions.

### Step 3.3: Implement live and public integration

Split bundled loading from runtime loading. Keep concrete `TradingCalendar` injection intact and
add `snapshot()` returning itself. The default factory returns a settings-bound live facade; its
public operations delegate to one fresh concrete snapshot. Do not cache runtime authority across
operations or follow a mutable pointer once per day inside a range.

New settings: `calendar_runtime_enabled=false` and the safe generation database basename, with
the design's exact 11-key plain-model `CalendarRuntimeSettings` environment allowlist. New CLI/API
and default live factory do not call get_settings or read .env. Document this env-only lane in
.env.example; a general .env entry alone is not runtime enablement. Use the same projection across
default consumers, maintenance and status; embedded callers may explicitly project already-resolved
settings. Avoid accidental global/default-root reads. Existing qualification readers stay concrete.

At each external continuity scan, automation decision/run, API market status/summary,
portfolio/fund-flow operation and supplement-ingestion operation, capture `calendar.snapshot()`
once and use that object for every status/range/source query. Tests inject promotion between two
calendar methods to prove the outer operation does not mix generations.

Add the three specified CLI commands before generic StoragePreflight, market-store or runtime-dir
initialization. Add standalone calendar-generation API/status models in the new calendar domain.
Existing `/market/status`, summary, portfolio and fund-flow dependencies use the same live
calendar for dates without changing their response contract. Startup/daily calendar worker invokes
one maintenance slot when enabled; no new service or LaunchAgent label.

### Step 3.4: GREEN, commit and serial reviews

Run Task1-3 tests plus all continuity/automation/calendar/fund-flow/read-only/launchagent tests.
Review enabled/disabled config and long-lived service injection paths line by line. Commit and
run spec review for AC-8/12/13 followed by quality review. All H/M must close before Task4.

## Task 4: Full verification, operational docs and final GO review

**Files:**

- Update `docs/calendar-maintenance.md`, `docs/market-data.md`, `docs/data-providers.md`
- Update umbrella R2-F roadmap and implementation status only
- Create `docs/acceptance/release-2-r2f4-1.md`
- Update this design/plan metadata to actual approved/implemented baseline

### Step 4.1: Acceptance traceability and operational runbook

Record exact source package schema/construction example, annual operator-reviewed locator and
extraction step, zero-write plan, explicit staging, runtime enablement prerequisite, maintenance
slot policy, safe pending/conflict/circuit responses, strict read-only status and recovery procedure.
Do not recommend deleting stores/rewinding heads to recover. Preserve copies for investigation;
restoration/rebase requires separately verified authority. Correct the old unknown-weekend wording.

Record AC-1..14 -> tests/commands, actual test counts/RED/GREEN, baseline/final commits, source and
canonical hashes, warnings/limitations. State that live next-year availability and production
runtime enablement were not exercised. Keep R2-F4.0 secondary/failover blocked.

### Step 4.2: Run repository gates

```bash
.venv/bin/pytest -q tests/test_market_calendar_generation.py \
  tests/test_market_calendar_maintenance.py tests/test_market_calendar_runtime.py \
  tests/test_calendar_sync.py tests/test_market_automation.py \
  tests/test_market_get_read_only.py tests/test_fund_flow_evidence.py \
  tests/test_r2f2_golden_compat.py tests/test_market_failover_readiness.py \
  --basetemp=/tmp/stock-eva-r2f4-1-focused
.venv/bin/pytest -q --basetemp=/tmp/stock-eva-r2f4-1-full
.venv/bin/ruff check backend tests
.venv/bin/ruff format --check backend tests
.venv/bin/python -m compileall -q backend
uv run --offline python \
  /Users/finlay/.codex/skills/claude-skills--engineering--spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-08-31-stock-eva-r2f4-1-promoted-calendar-design.md --strict
git diff --check
```

Run `shasum -a 256 -c sha256sums.txt` **from** `tests/fixtures/r2f2_golden`. Compare frozen bundled,
R2-F3 and `failover.py` hashes to the starting commit. Verify a complete synthetic maintenance run
preserves all synthetic canonical files while the calendar-only head advances. Verify five-agent
assets and canonical provider/quality/selection/storage files have no unintended changes.

### Step 4.3: Exact-HEAD independent final review

Commit completed docs and code. A fresh read-only reviewer must inspect the full baseline diff,
reproduce critical negatives and independently check AC coverage/frozen surfaces. Resolve H/M,
rerun gates proportionately, and re-review the exact final HEAD. Do not use an implementation
agent's self-review as final approval.

Only then report `R2-F4.1 CALENDAR RUNTIME GO / PRODUCTION ENABLEMENT NOT CLAIMED / R2-F4 NO-GO`,
exact commit, evidence, what improved and remaining risks. Keep this worktree/branch; do not merge
or start Task16/R2-F4.2 before human confirmation.
