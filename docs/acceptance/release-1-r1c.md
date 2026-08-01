# Release 1 R1-C acceptance

Date: 2026-08-01 (Asia/Shanghai)

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
- Sector ranking exposes score, raw value, weight, weighted score, formula version, quality,
  effective/target counts and missing inputs for 5/20/60-day relative strength, advancing
  breadth, MA20/60 breadth, turnover 5/20-day change, top-three turnover concentration,
  persistence, cross-sectional dispersion, qualified-leader count, leader diffusion and
  qualified-leader persistence.
- Each horizon requires at least two comparable members and 80% member coverage. Turnover
  uses one fixed comparable-member set over the whole window, preventing current-only IPOs
  from inflating the numerator.
- Sector and leader rankings expose deterministic IDs, total score, confidence,
  supporting/contrary evidence, missing inputs and quality issues. Ties are deterministic by
  `sector_id` or `symbol`.
- Leader candidates combine current tradability, 5/20/60-day relative strength, 20-day
  trading activity, MA20/60 trend quality, 20-day sector contribution, persistence and
  dispersion consistency.
- Suspended, non-trading, bad-quality, invalid-price/activity, out-of-scope and `as_of`
  ineligible securities are excluded with reasons. `price_available=null` is not itself an
  exclusion when a trustworthy current canonical bar proves price availability.
- Research-leader qualification is deterministic and versioned. Qualified-leader count,
  diffusion and persistence reflect only candidates passing relative-strength, activity,
  trend and sector-contribution gates; changing the qualification version changes candidate,
  sector-ranking and aggregate IDs.
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
- Local DuckDB analysis reads only explicit daily `published_daily_bars` partitions. The
  current pointer and every selected partition must link to a complete ready refresh audit;
  missing/tampered status, coverage, run or partition count fails closed with structured 503.
- An incomplete fake-ready object is rejected again at the store boundary before deleting a
  same-day partition or moving the pointer. Complete same-day reruns preserve prior history.
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

# Independent review repair RED: 13 failed across ready-publication completeness,
# publication audit trust, as-of eligibility, qualification-version IDs and tie ordering.

uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_data.py::test_ready_refresh_result_rejects_incomplete_publication_state \
  tests/test_sector_rotation.py::test_fake_ready_publication_cannot_replace_complete_same_day_partition \
  tests/test_sector_rotation.py::test_local_publication_requires_one_complete_explicit_date_partition \
  tests/test_sector_rotation.py::test_publication_audit_corruption_returns_structured_503 \
  tests/test_sector_rotation.py::test_as_of_ineligible_members_never_rank_or_become_leader_candidates \
  tests/test_sector_rotation.py::test_qualification_version_changes_candidate_sector_and_result_ids \
  tests/test_sector_rotation.py::test_sector_score_ties_break_only_by_sector_id_even_with_different_sizes
# Repair GREEN: 13 passed in 0.74s.
```

## Fresh verification

```text
uv run --extra dev pytest -o addopts='' -q
# 536 passed in 68.76s.

uv run --extra dev pytest -o addopts='' -q tests/test_sector_rotation.py --durations=5
# 45 passed in 9.69s; the 3,000-symbol x 65-session representative-scale
# leader-ranking case completed in 3.31s, below its 15s guardrail.

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
- Independent re-review of this repair commit remains pending; this worker does not declare
  the R1-C slice approved.

These gates prevent an R1-C review GO or Release 1 GO claim.
