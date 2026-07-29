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
```

Fresh final verification from the isolated worktree:

```text
uv run --extra dev pytest tests/test_point_in_time_classification.py -q
# 104 passed

uv run --extra dev pytest -q
# 429 passed

uv run --extra dev ruff check backend tests
# All checks passed!

git diff --check
# no output
```

## Pending real acceptance gates

- No real BaoStock classification publication was executed in this recovery.
- The eligible-universe live industry mapping target of at least 95% is not yet measured.
- The required 20-session historical replay and no-future read acceptance is not yet run on
  real published generations.
- Browser acceptance for market to sector to leader to stock drill-down belongs to later
  Release 1 work and remains pending.
- The live provider probe previously timed out; component history capability therefore remains
  `unverified`, not `verified`.

These pending gates prevent a Release 1 GO claim.
