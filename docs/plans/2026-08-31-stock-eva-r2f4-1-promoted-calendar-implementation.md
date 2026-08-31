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

**Task 1 gate:** Accepted at `197bba933ea74d5e860039dbfcaeea1f7a7921f4` after independent SPEC and
QUALITY re-review, both H=0/M=0/L=0. Root full pytest: 2332 passed, exit 0, only the existing
Starlette warning. Root focused replay: 130 repository cases plus 62 private probes (192 passed).
All backend/tests ruff/format, compileall, diff and nine R2-F2 golden digest checks passed. This is
Task 1 GO only; R2-F4.1 remains NO-GO until Tasks 2-4 and final exact-HEAD review are complete.

**Task 2a checkpoint:** Complete civil-day transport validation and per-operation calendar-sync
snapshot/authority drift protection are implemented. The developer reported initial scoped RED
as 18 failed/96 passed. Root independently observed 15 failures/1 legacy-compatibility pass before
implementation. Root review then corrected insufficiently isolated negative fixtures and added
full 365/366-day, audit identity, real SQLite last-good preservation and mid-operation snapshot
change coverage. Fresh root replay: 124 repository cases plus 20 private probes, 144 passed.
Full repository: 2360 passed, one existing Starlette TestClient/httpx warning, 129.87 seconds.
All backend/tests ruff and format (199 files), compileall, diff check, strict spec validator
(100/100, zero warnings), and nine R2-F2 golden checks passed. Task 1 module SHA-256 remains
`2e3c6ceeb9d543bda52a1e806e3c2f5319c4ec551b0d3af09e2e34cb89ce7611`.
No real acquisition occurred. This is a development checkpoint only; independent Task 2 SPEC
and QUALITY reviews follow completion of Task 2b, and no Task 2/version GO is claimed here.

**Task 2b control-interface checkpoint:** `read_control()` now exposes an immutable
`CalendarControlSnapshot` containing the original read result, staging-ordered candidates,
generation-ordered promoted sources and ordered attempts, from the same complete verified
connection. `read()` delegates without changing its result contract. Public `finish_attempt()`
revalidates failed outcomes, identities, timestamps and strict request counts, including actual
1/0 failures; it cannot promote, reset or overwrite a spent attempt. Root reproduced and fixed
two pre-try permission-error escapes in the read and failure-audit paths. Twenty private control
probes are retained as repository regressions alongside ordering, transaction and read-only
coverage. Root final combined replay: 159 calendar-generation cases, 124 BaoStock/calendar-sync
cases and 102 private probes, 385 passed. A legacy private FIFO probe was switched from fork to
spawn before the final combined replay to avoid fork-after-thread warnings; no production
transport behavior changed. All backend/tests ruff/format, compileall and diff checks passed.
The DDL digest and bundled projection digest remain unchanged. This checkpoint did not rerun
full pytest; the latest full result remains the Task 2a 2360-case run. Health preflight and
bounded acquisition/worker implementation, then complete Task 2 double review, remain pending.

**Task 2b existing-health checkpoint:** The new maintenance-only adapter now proves the exact
existing health schema/version and six bounded, state-valid circuits before any snapshot or audit
write. Reads are descriptor-bound, query-only and nonblocking; they preserve expired HALF_OPEN
leases and reject file/content changes, unsafe paths and sidecars without initialization. Existing
0644 health files remain supported. Audit writes use an existing-only, inode-bound owned alias,
preserve the original outcome semantics and reject malformed selected idempotency rows without
unbounded text reads. Initialize/reset/probe entrypoints are forbidden; `provider_health.py` is
byte-identical to the baseline.
Root observed five failing race/error probes during development: foreign-alias commit, foreign
alias deletion during creation, TypeError/ValueError masking, and in-place modification returning
a cached CLOSED result. All now pass and are retained in the repository suite. The final module
contains 59 retained cases; with 41 legacy provider-health tests and 29 private probes, 129 pass.
Root combined Task1/Task2 replay: 514 passed. Full repository: 2448 passed, one existing Starlette
warning, 130.47 seconds. All backend/tests ruff/format (201 files), compileall, diff check, strict
spec validation (100/100, zero warnings) and nine R2-F2 golden checks passed. No live acquisition
occurred. This is still a development checkpoint, not independent Task 2 or R2-F4.1 GO; bounded
official acquisition and the policy/slot worker remain next, followed by both independent reviews.

**Task 2b official-fetcher checkpoint:** `fetch_official_calendars` pins one bounded, strictly
validated source snapshot before constructing clients. It uses a fresh credentialless client per
exact reviewed URL, fixed timeouts, no redirects/retries/environment proxy, explicit gzip/deflate
negotiation, bounded decoded streaming and exact reviewed body hashes. Complete dual success is
the only result carrying internal body bytes; operational results never expose them. Actual
request counts distinguish client setup failure (0), first-request failure (1), and second-request
failure (2). Resource-close transport failures fail closed without masking a primary programming
failure. Source validation/conflict paths are client-free and zero-write.
Developer initial RED: 24 failures before implementation; root initial RED: 13 missing-interface
failures. Root then reproduced three contract failures: mutable caller source reused after
validation, ignored client-close failure, and unknown HTTP content encoding accepted as identity.
All are fixed and retained, including isolated streamed encoding/decoder and resource-close tests.
Fresh root combined replay: 565 passed, including 34 retained fetcher cases and 17 private fetcher
probes. All backend/tests ruff/format (203 files), compileall and diff checks pass. No frozen
health/generation/provider code changed and no real acquisition or persistence occurred. This
checkpoint did not repeat full pytest; latest full evidence is the preceding 2448-case health
checkpoint. Policy, durable slots and machine/promotion integration remain pending, then complete
Task 2 independent SPEC and QUALITY reviews; no Task 2 or version GO is claimed.

**Task 1 historical progress:** The specification extractor produced 14 private
scratch stubs (all expected failures); these are traceability aids, not implementation acceptance.
D1 completed with 45 focused cases passing (60 including 15 independent private-root probes).
It covers actual promotion write/commit faults, inserted-generation readback tampering and a
valid foreign-database substitution. This is a checkpoint, not Task 1 approval.

D2 extended the module to 61 cases against frozen implementation SHA-256
`24569d692056050ec0b8cb348d64ef5818a90c51cf86c719eac87f7cefd6b0b7`.
The independent test agent stopped at a usage limit; the root reviewed its unfinished tests and
corrected three false-positive traps (promotion-year versus history rejection, exact canonical
timestamp hashing, and truncated serializer warnings). Twelve contract failures were reproduced:
inner source/admission/review validation, reverse attempt linkage, SQL/JSON/base/parent identity,
attempt timestamps, promotion-year/no-skip replay, and warning leakage. These tests are not an
independent review verdict. Three additional root probes prove existing writers mutate an
unproven bundled base; they belong in the common verified-state fix.

D2b now shares one complete connection-level verifier between reader and writer paths. Root
replayed an unrelated-candidate corruption against all three writers and caught additional
extraction regressions: reverse PROMOTED linkage, monotonic promotion time, quarantined-source
promotion, and missing stage/reserve transactions. An orphan test was corrected to use canonical
SQL timestamps so it proves linkage rather than failing on timestamp syntax. These cases now
pass; root removed 239 unreachable lines of the old duplicated reader. The frozen checkpoint is
72 repository tests plus 21 private probes (93/93), with ruff/format/diff checks passing, at
implementation SHA-256 `8729af8302a4802d82c8cdd929bdfd985063eaeb54a17ad7776729449bfdd6ed`.

Nineteen private-root D3 probes were integrated by independent test-only work: 91 collected,
72 passing and 19 confirmed RED against the frozen D2b implementation. They cover zero-byte DB initialization, unsafe
control-directory permissions, post-connect reader path substitution, mid-open source symlink
substitution, non-normative schema index, complete snapshot metadata/methods, unbounded cursor
materialization, blank extraction review IDs, public official-body digest shape, and factories
hashing before UTC normalization, plus four failure-audit identity/control-proof bypasses.
D3a repaired public models, immutable snapshot behavior and failure-audit identity/graph checks.
Snapshot methods now inherit the existing TradingCalendar rules (including closed/unknown years),
and retain verified bundled/current-generation identities. Root's subsequent replay, after adding
the genuine SQLite EXCLUSIVE-lock test, collected 93 repository cases plus 21 private probes:
107 passed and 7 known failures, at implementation SHA-256
`dac5f3715c2e0d53a87d2199bac1e1a77f3df8d7d00dace0e2f6d1279df617d1`.
D3b closed those seven known failures. Root replayed 93 repository cases and 21 private probes:
114 passed, at implementation SHA-256
`1458c0babbdb8bed8818e4e0dea815207e17e9aebca11f5ba81737fd8c929bf2`;
ruff check/format and diff checks also passed. Readers now respect a genuine SQLite EXCLUSIVE
lock, and the existing descriptor/schema/cap probes pass. This is not Task 1 approval.

D3c test-only work completed: root replayed 113 collected, 96 passed and 17 confirmed RED against
the frozen D3b implementation. Tests cover failure-audit completion timestamps, serialized public
attempt validation, typed already-spent outcomes and same-map/different-generation identity.
Root additionally reproduced a promotion that used an unverified second bundled-base load,
partial initialization `(metadata=1, head=0)` after an injected failure, adoption of a concurrently
created empty file, and three malformed singleton states that incorrectly returned ready.
A real private FIFO source also blocked before descriptor validation; its test terminates only
its own bounded child process. These confirmed contract failures must close before Task 1 review.
The FIFO-lock and same-config/different-generation identity probes already pass. Root corrected
the latter test's initial mistaken assertion: changing review_id does not change legacy config
metadata, so the generation identity must distinguish otherwise equal config projections.
D3c implementation checkpoint completed. The repository module now has 116 passing cases,
including failed-attempt temporal replay and in-range nonzero RUNNING counts. Root replayed the
module plus all 49 private probes: 165 passed, with ruff check/format and diff checks passing.
The implementation SHA-256 is
`e775c5a47dc0fa8987297153676d283bbad1a1d060a060a87c95f7d1879ed57a`.
Task 1 is now entering frozen-candidate review, not accepted. No unverified path-race claim is
acceptance evidence; the independent specification and quality reviews remain required.

Task 1 candidate commit `a9e4145305803747f8c9a4407f06367c0fdcf7fc` passed root full pytest
(2318, exit 0), all backend/tests ruff and format (199 files), compileall, diff checks and nine
R2-F2 golden digests. The run reported the existing Starlette warning and a new FIFO-test fork
warning under the multi-threaded full suite. It is not an acceptance verdict.
Independent specification review R1 returned NO-GO (reported H=3/M=0, plus one test-reliability L):
the reader could adopt a valid foreign database swapped immediately before open, a FIFO swap
could block before descriptor checks, and SQLite BLOB/TEXT storage-type corruption escaped the
shared verifier as TypeError. Root reproduced all three as ten private RED cases, including all
read/stage/reserve/promote paths for both storage-type variants. The first repair passed those
ten cases, but root then reproduced a remaining post-validation/pre-open foreign swap: a second
lstat had re-adopted unverified identity. R1b now carries the original validated path proof into
both connection branches, uses nonblocking regular-file reader descriptors, explicitly checks
control FD types and validates SQLite column types before parsing. Test-only FIFO workers use
bounded spawn instead of fork. Root replayed 127 repository cases plus all 60 private probes:
187 passed, with full backend/tests ruff/format, compileall and diff checks passing, at SHA-256
`237e07b7688ee11a9a64139e64f4ceb2783bfcd9f1aa34e70be2d933b24fc7de`.
This repair is awaiting exact-commit specification re-review. Quality review and Task 2 have not
started; a green test run does not close the independent review by itself.

At repair commit `92ebf20194298089321d7a55f9e4ef3497a780c4`, independent SPEC R1 re-review passed
H=0/M=0/L=0 (11 R1 probes, 127 focused cases and 26 selected regressions). Root full pytest passed
2329 with only the existing Starlette warning; the new fork warning is gone.
Fresh QUALITY R1 review then returned NO-GO with H=1/M=1: the writer could commit a backwards
promotion timestamp which its own reader subsequently rejects, and a hardlink created during
source reading was not rejected by final descriptor checks. Root independently reproduced both
as two RED cases in a private pytest file. The repair adds a same-transaction comparison with the
verified head timestamp, permits equal timestamps, and checks final source descriptor/path
type, owner, mode, link count and content metadata against the initial proof. Root replayed 130
repository cases plus 62 private probes: 192 passed, with full backend/tests ruff/format,
compileall and diff checks passing, at implementation SHA-256
`2e3c6ceeb9d543bda52a1e806e3c2f5319c4ec551b0d3af09e2e34cb89ce7611`.
Exact-HEAD incremental specification and quality re-review remain required; Task 1 and Task 2
remain gated.

Protected regression checkpoint: existing calendar-sync, R2-F2 golden compatibility and R2-F4.0
failover-readiness tests passed (68), and all nine R2-F2 golden file digests match. Existing tracked
backend/test/script files remain unchanged; this does not replace the final full regression gate.
No Task 2 work or Task 1 acceptance is authorized by a partial green checkpoint. Even the green
full regression at the R1 candidate does not override its independently reproduced blockers.

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
- Create `backend/app/market/calendar_maintenance_health.py` for the maintenance-only verified
  existing-health adapter; keep `provider_health.py` unchanged.
- Extend `backend/app/market/calendar_generation.py` only with narrow verified-control snapshot
  and terminal-attempt interfaces required by maintenance; preserve its DDL, hashes and existing
  authority checks, and add corresponding `tests/test_market_calendar_generation.py` coverage.
- Modify `backend/app/market/calendar_sync.py` only for mixed-range/stale-plan safety and reuse
- Create `tests/test_market_calendar_maintenance.py`
- Create `tests/test_market_calendar_maintenance_health.py`
- Add scoped cases in `tests/test_baostock_provider.py` and `tests/test_calendar_sync.py`

**Serial implementation checkpoints (one subagent at a time):**

1. Task 2a: additive `calendar_days` plus calendar-sync snapshot/checksum/mixed-range safety,
   with RED/GREEN in the existing two test modules. No live facade or maintenance client yet.
2. Task 2b: bounded official acquisition, verified read-only health preflight, policy/slot worker
   and the narrow calendar-control interfaces needed by it, with RED/GREEN maintenance tests.
3. Freeze the complete Task 2 commit, run the combined regressions, then independent specification
   and quality review. A checkpoint is not Task 2 GO.

The control snapshot must reuse the existing complete reader proof, not open a parallel ad-hoc
SQL authority path. Actual failed-acquisition request counts may be 1/0 as well as 2/0 or 2/1;
the terminal-attempt interface must not fabricate two requests when only one client call occurred.
Calendar-sync authority checksums must include verified generation/bundled identities when present,
so equal legacy config projections with different reviewed generations still invalidate old plans.
Concrete legacy calendars retain their old config-only checksum and injection behavior.

Task 2b is implemented in two serial checkpoints: first the verified control snapshot/terminal
interfaces and existing-health adapter, then official acquisition and the policy/slot worker.
These remain one Task 2 review scope, not independent GO gates. The health adapter is separate
from the worker so its file/schema proof and audit-write restrictions can be tested directly.
Within the acquisition/worker checkpoint, implement and verify the bounded official fetcher
first, then integrate policy, slots and machine reconciliation. Keep one active developer;
these smaller handoffs do not replace the complete Task 2 specification and quality reviews.

The existing health store's `provider_health()` may reap expired leases, while its
`provider_health_snapshot()` still opens a read/write SQLite connection. Neither is the new
maintenance preflight. The adapter must use a bounded descriptor-bound read-only connection,
prove the existing schema/version and exactly six valid endpoint circuit rows, and preserve
OPEN/HALF_OPEN without probing or reaping. Validate SQLite storage types and state-dependent
timestamp/lease shapes before constructing a health model. Readable-by-other legacy health
files are not automatically unsafe; FR-18 forbids writable-by-other state, unsafe ownership,
links, non-regular files and active sidecars. Do not silently require a new health migration.
Audit writes reuse the existing observation/terminal-outcome logic only through an existing-only
validated handle; a missing/replaced/corrupt health database must never be recreated or adopted.
No preflight may call initialize, health repair, or any network operation.

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
Client construction must remain lazy until validated execution. Use a fresh credentialless
client for each official request, or an equivalently explicit cookie-free request path, so a
response cookie cannot be carried into the other request. Offline tests must inspect actual
request headers and client configuration, exercise standard decoded content hashing, and prove
that first-request failures report 1/0 while client-construction failures report 0/0. Internal
successful body bytes are never part of a public operational result or exception.
Negotiate only standard gzip/deflate encoding (identity is also accepted), and reject unknown or
malformed Content-Encoding tokens before accepting bytes. HTTPX's permissive unknown-encoding
identity fallback is not proof of a decoded official body. Keep its standard supported decoding
and apply the 1 MiB bound and reviewed digest to decoded bytes.

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
