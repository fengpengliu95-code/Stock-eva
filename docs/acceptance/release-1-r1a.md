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
```

Fresh final verification from the isolated worktree:

```text
uv run --extra dev pytest tests/test_point_in_time_classification.py -q
# 50 passed

uv run --extra dev pytest -q
# 375 passed

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
