# Release 1 R1-A acceptance

Date: 2026-07-29 (Asia/Shanghai)

R1-A synthetic contract: **VERIFIED**

Release 1 verdict: **PENDING — do not claim GO**

## Verified synthetic scope

- Classification coverage is independent from actionability. Suspended, missing-price and
  price-not-audited securities remain in the eligible SSE/SZSE main/ChiNext/STAR A-share
  denominator, while actionability reasons remain visible.
- Not-yet-listed, delisted, BSE, B-share and index fixtures are excluded with auditable reasons.
  BaoStock listing status and daily trade status remain separate source fields.
- Repeated publication with unchanged industry/index source `updateDate` reads one latest
  whole generation, returns its `generation_id`, and does not duplicate members.
- Future execute dates and source dates after the requested `as_of` fail closed.
  Request-only and source-observed date semantics remain distinct.
- Index component history capability is explicit. BaoStock HS300/SZ50/CSI500 remains
  `unverified`; no-source indexes remain `not_supplied`; no catalog entry is `verified`.
- The index master includes CSI1000 and ChiNext Index metadata without fabricated components.
- Existing synthetic contracts for no-future `observed_at`, source-supplied end dates only,
  BSE exclusion, versioned board derivation, idempotent immutable generations, conflict-safe
  atomic publication, unknown classifiers, empty/degraded states and dry-run no-write remain
  covered.
- Central publication rejects an empty security master and any record whose source,
  source version, observed timestamp or source snapshot date violates its enclosing snapshot.
  A failed candidate neither creates a generation nor advances the ready pointer.
- Every endpoint reads records, market scope and generation identity from one selected
  generation. Deterministic publish insertion tests cover securities, indexes, sectors,
  members and coverage, including security/membership co-generation.
- Provider required result sets and fields fail closed. Target SSE/SZSE main/ChiNext/STAR
  symbols require usable basic type/status metadata; required industry and component
  results cannot be empty.
- Degraded candidates are persisted for audit but never replace a trusted ready pointer.
  Zero-denominator and below-95% candidates remain non-ready.
- Classification GET is read-only: a missing database creates no DB, parent or temp
  directory; an existing database read executes no schema DDL/DML, leaves DB bytes
  unchanged and remains compatible with an open same-process writer.
- All HTTP classification endpoints reject a future Shanghai `as_of` with 422 before a
  store read. Service-level synthetic historical replay remains unrestricted.
- Sync publication reports `new_generation`; idempotent re-runs report both
  `new_generation=false` and `writes_classification_data=false` from the atomic publish
  outcome.
- Promotion is non-vacuous: audits must be non-empty, include the mandatory BaoStock
  industry taxonomy and all be ready. Empty-taxonomy generations remain auditable
  candidates and cannot create or replace ready.
- Read selection uses explicit persisted promoted history rather than a current-pointer
  sequence cutoff. Degraded candidates never enter records, scope or coverage responses.
  A later-published older ready generation remains available for historical `as_of`
  without regressing the current pointer's business date.
- Legacy `promoted` history migration is writer-only; GET readers do not run migration.
- Generation identity uses the `classification-v3` contract: canonical declared taxonomies
  plus stable record type/natural identity, lineage and content hash. Readiness inputs cannot
  collide, taxonomy order/duplicates are identity-neutral, and changed content with a reused
  lineage reaches the conflict guard instead of being treated as idempotent.
- Persisted generation reads preserve each row's schema version. Legacy v2 rows remain v2
  through idempotent publish, `generation_at` and atomic `read_snapshot`; Pydantic defaults
  cannot relabel stored history as v3.
- BaoStock login, query construction and pagination have real per-attempt wall-clock
  deadlines on the POSIX main-thread call stack. The deadline interrupt crosses BaoStock's
  broad exception handling, no operation worker is created, unsupported execution contexts
  fail before network access, and prior signal state is preserved. Initial-login timeout is
  converted to controlled CLI JSON with no database or temp-directory write.

## Verification

Each correction was observed RED before its production change:

```text
1. coverage/actionability split: 6 failed
   - eligibility incorrectly returned suspended/no_price/price_not_audited
   - API lacked separate source statuses and actionability
2. repeated source dates: 1 failed
   - returned sh.600000 twice and leaked removed sz.000001
3. date fail-closed/semantics: 5 failed across two RED runs
   - future execute/source dates were accepted, semantics were absent, and generation identity
     ignored semantics
4. component history capability: 2 failed
   - boolean capability and silent ready response remained
5. representative index master: 2 failed
   - catalog entries were absent and CSI1000 returned HTTP 404
6. publication snapshot integrity follow-up: 13 failed
   - direct store publication accepted empty security and envelope-inconsistent records
7. single-generation atomic reads: 7 failed
   - endpoint responses could mix generation A records with generation B scope/identity
8. upstream completeness and ready pointer: 14 failed across two RED runs
   - empty or incomplete required provider results were accepted, and degraded candidates
     advanced ready
9. GET read-only boundary: 3 failed across two RED runs
   - missing-DB GET created filesystem state and existing-DB GET executed schema DDL
   - a physical read-only DuckDB handle conflicted with an open same-process writer
10. future HTTP `as_of`: 5 failed
    - all five endpoint families reached the store instead of returning 422
11. actual write reporting: 1 failed
    - idempotent execute lacked `new_generation` and still reported a write
12. promoted-history review follow-up: 5 failed
    - empty audits vacuously advanced ready and could replace a trusted pointer
    - a degraded newer candidate leaked through a later older ready pointer
    - later publication of valid older history regressed the current business date
    - legacy writer initialization did not persist explicit promoted history
13. generation identity review follow-up: 3 failed
    - declared taxonomies were omitted from generation identity
    - declared taxonomy order and duplicates were not canonicalized
    - changed record content with a reused lineage was incorrectly treated as idempotent
14. persisted schema-version review follow-up: 3 failed
    - idempotent publish, generation_at and read_snapshot relabeled stored v2 rows as v3
15. BaoStock wall-clock deadline recovery: 4 failed
    - login, query and pagination ignored the configured end-to-end deadline
    - retry attempts had no tested derived total wall-clock bound
16. BaoStock deadline review follow-up: 4 failed across three RED runs
    - initial login timeout escaped classification error conversion and CLI JSON handling
    - two no-op-close request attempts left two live daemon operation workers
    - a non-main-thread call executed the client instead of failing closed before network
    - an expiring pre-existing timer whose handler returned leaked an internal interrupt
17. Classification session ownership follow-up: 1 failed
    - rejected worker-thread login logged out and discarded a session owned by another call
18. BaoStock active-session ownership follow-up: 2 failed
    - direct login and fetch retried against, then discarded, a pre-existing active session
```

2026-07-29 historical verification snapshot from the isolated worktree (before later observability follow-ups):

```text
uv run --extra dev pytest tests/test_point_in_time_classification.py -q
# 107 passed

uv run --extra dev pytest -q
# 444 passed

uv run --extra dev ruff check backend tests
# All checks passed!

git diff --check
# no output
```

## Live acceptance evidence

All live runs used the fixed `2026-07-29` `as_of` and isolated temporary storage.
They did not publish a trusted classification generation:

1. The initial wrapper remained blocked in one BaoStock socket `recv` for about
   15 minutes. The orchestrator terminated it with SIGTERM (`exit 143`); it
   emitted no aggregate result and wrote no classification database.
2. After the wall-clock deadline repair, a full TEMP sync with a 30-second
   operation deadline exited `1` after `71.830s`. It returned controlled
   `classification_sync_failed` JSON with `writes_classification_data=false`
   and created no classification database.
3. A bounded single-operation diagnostic later completed
   `query_all_stock(2026-07-29)` in `29.701s` and returned `7306` rows. This
   proves that the source can complete the request, but not that its latency or
   availability is stable.
4. A subsequent full TEMP sync with a 120-second operation deadline exited `1`
   after `372.854s`. It again returned controlled
   `classification_sync_failed` JSON with `writes_classification_data=false`
   and created no classification database.
5. The immediately following `max_attempts=1` staged diagnostic stopped at
   `query_all_stock(2026-07-29)` after its `120.010s` wall-clock deadline.
   Taken together with the earlier `29.701s` success, this is evidence of
   unstable source response rather than a verified stable slow-query bound.

### 2026-08-07 bounded diagnostic follow-up

This follow-up was diagnostic evidence only. It used `as_of=2026-08-07`, one
fresh explicit TEMP root, no classification store and no production/NAS/user
database access:

1. Login completed in roughly `0.4–4.3s`; the trading-calendar request completed
   in `0.173s` and confirmed that `2026-08-07` was a trading day.
2. `query_all_stock(2026-08-07)` exceeded both `10s` and `30s` single-attempt
   deadlines. The `30s` probe recorded only one request boundary. The preceding
   trading date, `2026-08-06`, also exceeded a `10s` deadline, ruling out a
   latest-date-only explanation.
3. `query_stock_basic` and `query_stock_industry` exceeded both `10s` and `30s`
   total request deadlines. Their request counters advanced during some probes,
   showing partial pagination progress but no complete result.
4. The independent index probes succeeded: HS300 returned 300 rows in `0.894s`,
   SZ50 returned 50 rows in `0.217s`, and CSI500 returned 500 rows in `1.606s`.
   Each returned the expected three-field schema and a `2026-08-03` snapshot date.
5. A real `BaoStockClassificationProvider.fetch` using a `10s` deadline and the
   unchanged two attempts reproduced two successful logins followed by
   `ClassificationProviderError -> deadline` in `21.929s`. It stopped at
   `security_universe`; parser validation and publication never ran.

The primary blocker remains unstable BaoStock full-universe/basic/industry
metadata service response. A larger deadline is not a verified remedy: the
earlier `120s` probes also failed. Parser/schema drift was not causal in the
observed full fetch because no universe result reached parsing; complete live
basic/industry schemas remain unverified. No TEMP publish canary, coverage audit,
ready generation, production acceptance or Release 1 GO is claimed by this
follow-up.

No live run touched the production classification path, NAS, `stock_eva.duckdb`,
or the user database. No threshold was lowered, no date was substituted, and
no production publication was attempted.

### 2026-08-09 observability and mainline follow-up

The cumulative R1-A observability repair was integrated into `main` through
`3de2480`. It adds sanitized stage/class diagnostics, counts only metadata query
operations that were actually invoked, removes raw exception cause/context from
the provider and sync boundaries, and closes both initial-login and
post-login socket-configuration cleanup gaps. An independent read-only review
approved the cumulative change after four review/fix rounds.

Mainline verification was serial and used no production, NAS or user data:

```text
uv run pytest -q --basetemp=/tmp/stock-eva-r1a-full-final
# 763 passed; exit 0

uv run ruff check backend tests
# All checks passed!

git diff --check
# no output
```

The first fresh TEMP canary exposed BaoStock 0.9.3 printing raw socket exception
text to stdout before the controlled JSON. The CLI boundary was repaired and
independently re-reviewed so provider construction and execution both discard
untrusted stdout/stderr without buffering it. The repeated real canary used
`as_of=2026-08-07`, a fresh explicit TEMP data root and a 30-second per-attempt
deadline. Its observed contract was:

```text
exit=1
stdout_lines=1
stderr_bytes=0
failure_stage=login
failure_class=transport
provider_request_count=0
writes_classification_data=false
configured_timeout_seconds=30.0
configured_max_attempts=2
elapsed_seconds=60.01
```

The one stdout line was valid JSON. The TEMP data root remained empty: no
classification database, DuckDB temp directory or other file was created, and
no classification-sync or pytest process remained afterward. This verifies the
failure/diagnostic boundary against the real third-party client, but it does not
verify a successful source read or publication. BaoStock failed both login
attempts before any metadata query invocation.

The installed local service remained healthy for its already deployed scope and
its local market dataset reported `ready`, but its OpenAPI contract did not yet
contain classification routes and no deployed `classification.duckdb` existed.
No production service was restarted or replaced during this acceptance run.

### 2026-08-09 successful real TEMP publication and browser follow-up

A later readiness probe confirmed TCP access and completed one bounded BaoStock
login in `0.666s`. A new explicit TEMP root then completed the full unchanged
classification sync with one machine-readable stdout line and no stderr:

```text
exit=0
status=ready
network_requests=6
writes_classification_data=true
new_generation=true
generation_id=classification-a5b8337d0da7e3a3594dfe95
source_snapshot_date=2026-08-07
source_date_semantics=requested_unverified
security_master_history=7336
index_component_history=850
sector_membership_history=5203
eligible_count=5205
mapped_count=5202
coverage_ratio=0.9994236311239193
quality_issues=[]
```

The three unmapped eligible symbols were `sh.603468`, `sz.001232` and
`sz.301707`. The promoted generation exceeded the 95% industry target without
lowering the threshold. HTTP readback against the same real TEMP database
returned the same generation for securities, 83 sectors, sector members and
coverage at `as_of=2026-08-09`. Requests at `2026-08-07` and `2026-08-06`
returned the truthful `no_classification_generation` empty state because the
generation had not yet been observed; `2026-08-10` was rejected with HTTP 422
before a store read. HS300 remained explicitly degraded with
`component_history_unverified`.

An isolated browser harness used the tracked mainline workspace with only its
API origin mechanically redirected to the temporary API. It read the existing
local canonical market dataset and the real TEMP classification generation.
The market-to-sector-to-leader-to-security path rendered:

- `range` / `risk_off`, low confidence and the narrow-scope disclaimer;
- 83 backend-ranked sectors and explicit missing/unavailable fund-flow evidence;
- a coal-sector deep link preserving `as_of`, taxonomy and sector identity;
- 25 backend leader candidates, all retaining unavailable limit-lock risk and
  non-actionable wording;
- `sh.600403` with the same decision context plus 260 effective sessions,
  qfq candles, volume, MA, MACD and RSI from backend values.

The browser console had no warnings or errors. No production service, user
database or NAS file was changed by this staging acceptance.

The official LaunchAgent installer passed its read-only preflight for main
`b2382ed` (`mutation=false`), but production installation was not attempted.
The current installed runtime is still `07f38d58` and lacks classification
routes. `DS220plus.local:445` was reachable, but the saved Finder connection did
not establish the required canonical `/Volumes/Stock` SMB mount; the NAS check
returned `not_mounted`. The installer therefore cannot execute its verified
mirror and atomic runtime switch yet, and the release process was not bypassed.

## Pending real acceptance gates

- One real TEMP promoted generation and the eligible-universe 95% industry target are now
  verified. Production installation and production readback remain pending on the NAS mount.
- The required 20-session historical replay and no-future read acceptance is not yet run on
  real published generations.
- Isolated real-data browser acceptance passed, but installed-runtime browser readback remains
  pending with the production deployment.
- Component history capability remains `unverified`, not `verified`.

These pending gates prevent a Release 1 GO claim.
