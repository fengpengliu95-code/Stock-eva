# Release 1 R1-C acceptance

Date: 2026-08-09 (Asia/Shanghai)

R1-C implementation and installed-runtime acceptance: **GO**

Release 1 overall verdict: **not asserted by this slice**

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
- Members outside `narrow_main_board` are excluded before the price-coverage and confidence
  denominator. The response still retains the complete taxonomy `member_count` and the
  `sector.member_outside_narrow_main_board_scope` evidence, so scope loss is visible without
  falsely reporting an in-scope price gap.
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
- A classification member and its current canonical bar must agree on security type, exchange
  and board. Their exchange and board must also agree with the shared versioned
  `cn-symbol-prefix-v1` derivation. ChiNext/STAR symbols cannot be relabeled as main-board
  securities, and unknown stock prefixes fail closed with a specific reason. A mismatch cannot
  enter priced coverage, sector metrics, market/sector cache, leader candidates or the effective
  denominator of leader count/diffusion/persistence.
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
- The reader audits the complete unfiltered published partition inventory before applying the
  BaoStock business filter. Unknown/mixed sources are rejected, and the pointer must reference
  the unique run for the newest published date; a self-consistent rollback to an older run is
  rejected while historical `as_of` replay remains available.
- An incomplete fake-ready object is rejected again at the store boundary before deleting a
  same-day partition or moving the pointer. Complete same-day reruns preserve prior history.
- A complete audit cannot publish an active partial/bad-quality bar. The only permitted
  non-trading row is the explicit suspended placeholder contract; rejection occurs before any
  transaction mutates the prior pointer or partition. Publication automation applies the same
  bar-quality rule before declaring a run ready.
- Stable IDs directly include the complete sector and leader policy identity. Sector/leader
  empty results and no-candidate degraded leader results change when weights, formula versions
  or the research-leader qualification version changes.
- A real temporary DuckDB classification store and canonical market store were read through
  both APIs without changing either database or creating reader temp state.
- The installed runtime and existing overview UI were read back against the real local mirror.
  No market, classification, NAS or user-data content changed during installation or acceptance.

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

# Third independent-review repair RED: 6 failed and 1 passed. Active partial bars still
# published, a valid old pointer rollback and a fully source-tampered historical partition were
# accepted, and empty/no-candidate IDs reused the prior qualification/policy identity. The one
# passing case confirmed the existing suspended-placeholder fixture remained readable.

uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_automation.py::test_active_partial_bar_cannot_produce_ready_publication_or_move_pointer \
  tests/test_sector_rotation.py::test_ready_publication_rejects_active_partial_bar_and_preserves_partition \
  tests/test_sector_rotation.py::test_ready_publication_allows_explicit_suspended_placeholder \
  tests/test_sector_rotation.py::test_published_reader_rejects_pointer_rollback_to_valid_older_partition \
  tests/test_sector_rotation.py::test_published_reader_rejects_source_tampered_historical_partition \
  tests/test_sector_rotation.py::test_empty_result_ids_include_complete_policy_identity \
  tests/test_sector_rotation.py::test_no_candidate_degraded_leader_id_includes_qualification_version
# Repair GREEN: 7 passed in 0.74s. Two additional malformed-placeholder cases also pass.

# Final approval-review repair RED: 3 failed. A classification-main member whose current
# canonical bar reported a ChiNext board or a conflicting exchange still became the first
# leader candidate; a security-type mismatch produced only a generic missing-price reason.

uv run --extra dev pytest -o addopts='' -q \
  tests/test_sector_rotation.py::test_canonical_classification_scope_mismatch_is_excluded_everywhere
# Repair GREEN: 3 passed in 0.51s. Board, exchange and security-type mismatches now carry
# specific reasons and cannot affect candidate or sector-leadership effective counts.

# Prefix-derivation final repair RED: 6 failed and 1 passed. Classification and market rows that
# both mislabeled sz.300/301 or sh.688/689 as main-board, plus an unknown stock prefix, still
# entered the benchmark, cache, sector metrics and leader candidates. The public tradability
# metric formula version also retained the misspelling `tradeability`. The legal-main/index
# compatibility control passed.

uv run --extra dev pytest -o addopts='' -q \
  tests/test_sector_rotation.py::test_canonical_symbol_prefix_scope_mismatch_is_excluded_everywhere \
  tests/test_sector_rotation.py::test_supported_main_board_prefixes_and_index_history_remain_compatible \
  tests/test_sector_rotation.py::test_tradability_metric_formula_uses_contract_spelling
# Repair GREEN: 7 passed in 0.74s. A separate regression also proves that classification and
# market rows agreeing on the wrong exchange still fail against the symbol exchange prefix.

# Acceptance-closure RED: a sector containing 3 fully priced main-board members plus 28
# legal ChiNext/STAR members incorrectly returned sector.member_price_missing and reduced
# confidence by 3/31.

uv run --extra dev pytest -o addopts='' -q \
  tests/test_sector_rotation.py::test_out_of_scope_members_do_not_reduce_in_scope_price_coverage_or_confidence
# RED: 1 failed because sector.member_price_missing remained present.
# GREEN: 1 passed in 0.51s after using the in-scope member denominator.

uv run --extra dev pytest -o addopts='' -q tests/test_sector_rotation.py
# Developer GREEN: 65 passed in 10.65s; independent reviewer: 65 passed in 10.96s.
```

## Fresh verification

```text
uv run --extra dev pytest -o addopts='' -q tests/test_sector_rotation.py
# Integrated main: 65 passed in 11.05s.

uv run --extra dev pytest -q
# Reached 100% and exited 0. A separate collect-only run reported 769 tests.
# The 3,000-symbol x 65-session representative-scale leader case completed in
# 3.77s during development, below its 15s guardrail.

uv run --extra dev ruff check backend tests
# All checks passed!

git diff --check HEAD^ HEAD
# no output

uv run --extra dev ruff format --check \
  backend/app/sector/service.py tests/test_sector_rotation.py
# Production file formatted. The test file still has three formatter findings at
# historical lines introduced by 42e494e; none is in the R1-C closure hunk.
```

Independent review approved exact development commit
`91f7d16e3154a531ecc9411a67625e7380714806` with no blocker. Main integrated the
same tree as `1fe90a1621ec4d9b180dbc9d1409aa4605185f9d`.

## Installed-runtime acceptance

- The official LaunchAgent installer built and installed release `1fe90a1`. `RELEASE.json`
  reports that exact SHA; all five agents are loaded, API and workspace are ready, the API
  remains `ProcessType=Interactive`, and storage reports
  `local_dataset/ready/local` at generation
  `generation-2374166120134660baba408ed01b7870`.
- The NAS archive was older than the local mirror. The installer returned
  `destination_newer`, 270 files, 861,430 rows and `copied_bytes=0`; it did not replace or
  rewrite the local market generation.
- The real sector endpoint returned HTTP 200 in 6.45s with 83 rankings, classification
  generation `classification-a5b8337d0da7e3a3594dfe95`, mapping coverage 99.942%, and market
  data through 2026-08-07. The result truthfully remains `degraded` because source-date
  semantics are unverified, the universe is narrow main board, the requested date is later
  than the last trading session, and R2 fund-flow evidence is missing.
- Before installation, M73 reported 31 taxonomy members, 3 priced in-scope members,
  confidence 0.0726 and a false `sector.member_price_missing`. After installation it reports
  the same 31/3 membership and score, remains ranking-eligible, reports confidence 0.75,
  removes only the false price issue, and retains
  `sector.member_outside_narrow_main_board_scope`.
- The real leader endpoint returned HTTP 200 in 4.75s with 3 candidates and 28 explicit
  exclusions. The first research candidate is `sh.600721` 百花医药, but
  `actionable_primary=false`, limit-lock input is unavailable, and fund-flow evidence is
  explicitly `missing`; it is not presented as a buy/sell recommendation.
- Browser acceptance rendered the existing overview with 83 backend sectors, M73 `3 / 31`
  and 75% confidence, the real leader list, the non-actionable warning and Release 2 fund-flow
  absence. No browser warning or error was recorded.
- Pre/post SHA-256 checks were identical for the local immutable dataset tree
  (`49a48b8580b2e12897725f14e47884a5bb0b34dc759f638f510257aecb94931d`), NAS archive tree
  (`db94feda4088dbbbbd51dad90e26ca256852e51661a4499e6a01ebd312210309`), classification
  DuckDB, market control DuckDB and private user SQLite database.

## Remaining boundaries after R1-C GO

- The required at-least-20-session replay over real promoted classification generations and
  published canonical prices remains R1-E work.
- The standalone `#sectors` workspace, URL/state continuity and loading experience remain
  R1-D work. R1-C acceptance covers the existing overview integration, not that standalone
  page.
- Full-A-share coverage, L1/L2/L3 fund-flow evidence, real limit-lock/abnormal-liquidity
  inputs, position sizing and portfolio advice remain outside R1-C. Turnover must continue to
  be described only as price-volume evidence.

These boundaries do not block R1-C GO and must not be presented as already delivered.
