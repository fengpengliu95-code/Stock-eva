# Release 1 R1-C acceptance

Date: 2026-07-30 (Asia/Shanghai)

R1-C synthetic contract: **LOCALLY VERIFIED — INDEPENDENT REVIEW REQUIRED**

Release 1 verdict: **PENDING — do not claim GO**

## Verified local scope

- `sector-rotation-v1` reads one promoted point-in-time classification snapshot and one
  canonical market input bounded by the requested `as_of`.
- The response returns classification generation ID/schema/source date, one market
  `data_as_of` and content-hash lineage, plus explicit `narrow_main_board` actual scope.
- A below-95% initial classification generation remains unreadable. A later degraded
  classification candidate cannot replace or leak through the prior promoted generation.
- Historical replay selects the promoted generation visible at the requested date; a later
  promoted generation and future memberships are not visible.
- Unknown classification members are defensively excluded. Extra market symbols can
  contribute only to the actual observable main-board benchmark and cannot become a sector
  member or leader.
- Sector ranking exposes score, raw value, weight, weighted score, formula version, quality
  and missing inputs for 5/20/60-day relative strength, advancing breadth, MA20/60 breadth,
  turnover 5/20-day change, top-three turnover concentration, persistence and cross-sectional
  dispersion.
- Sector and leader rankings expose deterministic IDs, total score, confidence,
  supporting/contrary evidence, missing inputs and quality issues. Ties are deterministic by
  `sector_id` or `symbol`.
- Leader candidates combine current tradability, 5/20/60-day relative strength, 20-day
  trading activity, MA20/60 trend quality, 20-day sector contribution, persistence and
  dispersion consistency.
- Suspended, non-trading, bad-quality, invalid-price/activity and classification-price
  unverified securities are excluded with reasons.
- Limit-lock/one-price-board identification remains unavailable. Every candidate is
  `actionable_primary=false`, `actionability_status=risk_inputs_unavailable` and carries the
  missing limit-lock input; no candidate is presented as an actionable first choice.
- Turnover remains price-volume evidence only. R2 L1/L2/L3 fund-flow evidence is explicitly
  `missing`; no turnover value is labeled as fund inflow or institutional evidence.
- Missing classification or market history returns `empty`. Stale, partial, warm-up,
  narrow-scope and missing-evidence inputs remain `degraded`.
- Both GET endpoints reject a future Shanghai date with 422 before service/store read.
- Missing classification DB, market DB, dataset root or manifest remains read-only and
  creates no runtime state. A corrupt existing published manifest remains an explicit 503.
- A real temporary DuckDB classification store and canonical market store were read through
  both APIs without changing either database or creating reader temp state.
- No frontend, R2 fund-flow, portfolio, order, broker, deployment, network, NAS or user-data
  work was performed.

## RED → GREEN evidence

```text
uv run --extra dev pytest -o addopts='' -q tests/test_sector_rotation.py
# RED 1: 1 failed because the sector-rotation route returned 404 instead of future-date 422.

uv run --extra dev pytest -o addopts='' -q tests/test_sector_rotation.py
# First route GREEN: 1 passed.

uv run --extra dev pytest -o addopts='' -q tests/test_sector_rotation.py
# RED 2: 15 failed because the sector models/service, read-only dependency and leader route
# did not exist.

uv run --extra dev pytest -o addopts='' -q \
  tests/test_sector_rotation.py::test_leader_metrics_include_required_reasons_and_flow_is_missing
# Naming follow-up RED: 1 failed because the metric used tradeability instead of the
# contract term tradability.

uv run --extra dev pytest -o addopts='' -q tests/test_sector_rotation.py
# Final focused GREEN: 20 passed in 0.79s.
```

## Fresh verification

```text
uv run --extra dev pytest
# 507 passed in 57.43s

uv run --extra dev ruff check backend tests
# All checks passed!

git diff --check
# no output
```

## Pending real and Release 1 gates

- R1-A did not produce a live promoted classification generation, so production R1-C
  readback is expected to remain `empty`.
- No real market dataset, NAS, production database or user database was opened in this
  worker.
- The required at-least-20-session replay over real promoted classification generations and
  published canonical prices remains pending R1-E.
- Live industry mapping coverage, real price completeness across the intended Release 1
  universe and real leader continuity remain unverified.
- R1-D market → sector → leader → stock browser drill-down remains pending.
- Independent specification review and independent code-quality review of this commit are
  pending.

These gates prevent an R1-C review GO or Release 1 GO claim.
