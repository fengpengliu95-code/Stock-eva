# Release 1 R1-A acceptance

Date: 2026-07-29 (Asia/Shanghai)

R1-A verdict: **GO — synthetic, production publication and browser path verified**

Release 1 verdict: **PENDING — R1-A GO is not Release 1 GO**

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

### 2026-08-09 real 20-session post-hoc replay

An isolated historical classification fetch for `as_of=2026-07-13` included
one controlled `security_basic/deadline` failure that reported no write. After
the bounded retry sequence, the TEMP database contained exactly one promoted
ready generation. A final 60-second run returned the same generation as an
idempotent no-write result:

```text
generation_id=classification-0b8fd0f28b15beab26e1e726
source_snapshot_date=2026-07-13
source_date_semantics=requested_unverified
observed_at=2026-08-09T12:02:26.975873+08:00
security_master_history=7305
index_component_history=850
sector_membership_history=5203
eligible_count=5202
mapped_count=5202
coverage_ratio=1.0
quality_issues=[]
```

The promoted generation was then joined read-only to the installed immutable
canonical price dataset for the 20 trading sessions from `2026-07-13` through
`2026-08-07`. The replay used the explicit audit cutoff
`known_at=2026-08-09T04:08:48.509336Z`. For every session it asserted that the
selected generation, every security and membership source date, every
membership effective window and every price row did not exceed the requested
`as_of`. It recalculated market regime, all sector rankings and the leading
sector's leader candidates for each date:

```text
status=verified_post_hoc_known_at
session_count=20
generation_count=1
minimum_sector_count=83
minimum_leader_count=3
future_membership_reads=0
future_price_reads=0
```

The strategic/tactical result changed from `bear/risk_off` to
`range/risk_off`, and the leading sector and candidate counts also changed,
showing that the matrix recalculated each session instead of repeating one
result. This proves the no-future-date behavior for the real post-hoc replay.
It does **not** prove that the classification was visible on those historical
dates: the only generation was actually observed on 2026-08-09 and the
production HTTP contract correctly hides it from earlier `as_of` requests.
Prospective historical visibility therefore remains distinct and unverified.

### 2026-08-09 production installation and upstream publication hold

After `/Volumes/Stock` was mounted, the canonical NAS check and its cleaned-up
write/read/delete probe both passed. The NAS dataset validated at 260
partitions and 829,494 rows; the installed local immutable mirror was newer at
270 partitions and 861,430 rows. The official mirror therefore returned
`destination_newer` with `copied_bytes=0` instead of replacing newer local
data with the older NAS generation.

The official installer built and atomically installed main
`af1527b788dc8aa48f31a06c60d35a4842529639`. Runtime metadata recorded the same
Git SHA and `built_at=2026-08-09T05:49:49Z`. All five LaunchAgents loaded, API
and workspace health returned ready, storage remained `local_dataset/ready`,
and the production OpenAPI contract exposed all five classification endpoint
families.

The first production classification publication did not pass. The installed
runtime returned exactly one sanitized JSON stdout line, zero stderr bytes and
no write:

```text
exit=1
failure_stage=security_basic
failure_class=deadline
elapsed_seconds=208.255
provider_request_count=4
configured_timeout_seconds=60.0
configured_max_attempts=2
writes_classification_data=false
```

The classification database remained absent. A 120-second, one-attempt
read-only canary against the same bulk `query_stock_basic()` operation also
ended in `_OperationDeadlineExceeded`; a single-symbol `sh.600000` basic query
succeeded in 0.952 seconds. A process-local 10,000-row page-size canary still
timed out at 46.624 seconds. These observations isolate the current hold to the
upstream bulk-basic operation rather than login, one-symbol metadata,
classification storage or pagination size. No experimental override was used
to publish production data and the completeness gate was not lowered.

The installed workspace completed its missing-classification browser state
without console errors. It displayed real 2026-08-07 market data and the
`range/risk_off` narrow-sample result, while sector rotation explicitly showed
`no_promoted_classification_generation`; it did not invent sectors or leaders.
Fund-flow and portfolio guidance remained unavailable/non-actionable.

A representative serial production GET pass covered health, storage,
market status/summary/supplemental/history, security analysis, market regime,
sector rotation, classification coverage, fund-flow evidence, portfolio
valuation and strategies. Every request returned HTTP 200. The market control
database and complete immutable dataset tree were identical before and after:

```text
control_sha256=31dbd6ff9db881bf9bf936156945efb79b898fee5016b4c9f0544ec4e671b84f
control_size_mtime=3158016:1786097560
dataset_tree_sha256=93416f3d956fb2738858db8123f84ce2d6f9a5a97a8d2d674821fb478d59adad
dataset_file_count=272
```

### 2026-08-09 production publication recovery and ready readback

A final retry from the installed runtime and production configuration recovered after the
earlier upstream hold. The unchanged completeness threshold and source contract produced one
sanitized stdout JSON line, no stderr and one promoted production generation:

```text
exit=0
status=ready
network_requests=6
writes_classification_data=true
new_generation=true
generation_id=classification-a5b8337d0da7e3a3594dfe95
source_snapshot_date=2026-08-07
source_date_semantics=requested_unverified
observed_at=2026-08-09T06:10:17.881305Z
security_master_history=7336
index_component_history=850
sector_membership_history=5203
eligible_count=5205
mapped_count=5202
coverage_ratio=0.9994236311239193
quality_issues=[]
```

Production HTTP readback at `as_of=2026-08-09` returned the same generation for 7,336
securities, 83 sectors, the leading sector's 31 members and 99.942% industry coverage. The
three unmapped eligible securities remained explicit. HS300 returned 300 components with
the truthful `component_history_unverified` degraded reason. A historical request at
`2026-08-07` returned `no_trusted_snapshot_at_as_of` because this generation was not observed
until 2026-08-09; `2026-08-10` was rejected with HTTP 422 before store access.

The production analysis path used the same classification generation. Sector rotation
returned 83 backend rankings with explicit low confidence where price coverage was sparse.
The leading M73 sector returned three candidates; `sh.600721` was qualified but remained
non-actionable because risk inputs and limit-lock evidence were unavailable. Market regime
remained the honest `range/risk_off` degraded narrow-scope result rather than a full-market or
fund-flow claim.

All five classification GET families plus sector rotation and sector leaders returned HTTP
200 in a final serial readback. The production classification database was byte- and
metadata-identical before and after the read-only pass:

```text
classification_sha256=45fd786ca80dd760e79b28432d3929de1b8da76111e265ea152ef5a8388f1941
classification_size_mtime=8663040:1786256009
```

The installed workspace then completed the production overview-to-sector-to-leader-to-stock
path. It preserved `as_of`, taxonomy and sector identity in the `sh.600721` deep link and
rendered 250 valid qfq sessions with K-line, volume, MA5/10/20/60/120/250, MACD and RSI14
from backend values. It retained the same degraded market, sector, leader-risk and missing
fund-flow context; the browser console contained no warnings or errors.

The installed code runtime remains the audited `af1527b788dc8aa48f31a06c60d35a4842529639`.
Later main commits in this acceptance sequence only update evidence documents and do not
change deployed code.

## R1-A decision and remaining Release 1 gates

R1-A is **GO**. Its implementation, independent review, full mainline tests, real
publication, production API readback, production GET immutability and browser decision path
all passed. The completed R1-A worktree may be removed while its branch and commits remain
available for audit.

This does not make Release 1 GO:

- the real 20-session post-hoc replay passed its no-future membership and price assertions,
  but contemporaneous historical visibility is not claimed because the generation was
  observed after the replayed sessions;
- component history capability remains explicitly `unverified`, not `verified`;
- R1-B, R1-C, R1-D and final R1-E evidence must still be reconciled and accepted in linear
  order before Release 1 can be declared GO.

R2 remains locked until the remaining Release 1 queue is complete.
