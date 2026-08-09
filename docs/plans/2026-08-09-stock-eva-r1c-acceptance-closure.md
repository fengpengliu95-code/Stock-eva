# Stock EVA R1-C Acceptance Closure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Status:** Approved by the user's instruction to continue R1-C, within the existing R1-C roadmap and sector-rotation contract.

**Goal:** Close the remaining R1-C correctness gap, independently review it, and verify the point-in-time sector-rotation and leader APIs against the installed local dataset before declaring R1-C GO.

**Architecture:** Keep the existing immutable classification and market snapshots, formula versions, stable identifiers, and narrow-main-board policy unchanged. Correct only the coverage boundary so members explicitly excluded from v1 cannot reduce in-scope price coverage or confidence. Preserve full taxonomy membership and exclusion evidence in the response.

**Tech Stack:** Python 3.12, FastAPI, Pydantic, DuckDB/Parquet, pytest, Ruff, macOS LaunchAgents.

---

## Scope and acceptance boundary

In scope:

- `backend/app/sector/service.py`: price-missing and confidence coverage use the same in-scope `member_symbols` denominator already used by metric and ranking eligibility calculations.
- `tests/test_sector_rotation.py`: a regression with three fully priced main-board members and 28 legal ChiNext/STAR members proves the corrected boundary.
- Independent code review, focused and full regression, installed-runtime API readback, read-only production-data integrity checks, and browser acceptance for the existing overview flow.
- Update R1-C contract and acceptance evidence only after the implementation and runtime checks are complete.

Out of scope:

- No sector scoring or leader scoring formula changes.
- No R1-B market-level leadership aggregation without a separately approved formula.
- No R1-D standalone sector page, deep-link, or loading-experience work.
- No R1-E historical replay and no Release 2 fund-flow, position, or advice features.
- No cache: a safe cache requires snapshot validation and lineage-bound keys, while current performance is within the existing guardrail.
- No production dataset, classification generation, NAS, holdings, or other user-data writes.

## Task 1: Add the failing boundary regression

**Files:**

- Modify: `tests/test_sector_rotation.py`

1. Build one sector containing three supported main-board securities with complete valid history plus 28 legal ChiNext/STAR classification members without in-scope prices.
2. Assert the result retains all 31 taxonomy members and reports three priced members.
3. Assert the sector remains ranking-eligible, does not report `sector.member_price_missing`, and reports confidence `0.75` rather than reducing it by `3 / 31`.
4. Assert the response still reports `sector.member_outside_narrow_main_board_scope` and remains degraded because narrow scope and fund-flow evidence are explicit limitations.
5. Run only the new test and record the expected RED failure before changing production code.

## Task 2: Correct the coverage denominator

**Files:**

- Modify: `backend/app/sector/service.py`

1. Define the in-scope target count once from `member_symbols` before price-quality and confidence decisions.
2. Compare `priced_symbols` against that target count for `sector.member_price_missing`.
3. Pass `priced_symbols / target_count` to `_confidence`, with a zero-safe fallback.
4. Keep `member_count=len(members)`, `priced_member_count`, the outside-scope issue, scoring, ranking thresholds, formula identities, and stable-ID inputs unchanged.
5. Re-run the new test for GREEN, then the complete sector-rotation focused suite.

## Task 3: Verify and commit the development slice

**Files:**

- Verify: `backend/app/sector/service.py`
- Verify: `tests/test_sector_rotation.py`
- Verify: `docs/plans/2026-08-09-stock-eva-r1c-acceptance-closure.md`

1. Confirm no conflicting pytest process is running before each test command.
2. Run Ruff check/format-check on changed Python files and `git diff --check`.
3. Review the exact diff for scope containment and commit one stable development slice.

## Task 4: Independent serial review

1. A separate reviewer inspects the committed diff and authoritative R1-C contracts without editing it.
2. The reviewer checks point-in-time isolation, strict snapshot trust, coverage semantics, stable IDs, leader qualification/actionability, fund-flow truthfulness, and regression quality.
3. Any blocking finding returns to Task 1 as a new RED test; otherwise record a review GO for the exact commit.

## Task 5: Integrate and run final repository verification

1. Preserve user-owned main-worktree changes and integrate only the reviewed R1-C commits.
2. Run the focused R1-C suite, the full backend suite, Ruff checks, formatting checks, and `git diff --check` from the integrated main commit.
3. Record actual counts and commit hashes; do not reuse stale acceptance counts.

## Task 6: Installed-runtime and product acceptance

1. Record dataset tree and classification/control-plane fingerprints before deployment.
2. Install/restart only the official Stock EVA runtime and verify its reported code SHA.
3. Read back the sector-rotation and leader endpoints for the production generation. Confirm the mixed-board sector uses full in-scope price coverage, omits the false price-missing issue, keeps outside-scope/fund-flow limitations, and does not present an actionable recommendation.
4. Recheck fingerprints to prove the acceptance run did not mutate production market or classification data.
5. Verify the existing overview browser path renders real sector and leader data without console errors. Treat the standalone `#sectors` page as R1-D, not R1-C.
6. Update `docs/sector-rotation.md` and `docs/acceptance/release-1-r1c.md` with current evidence, commit the acceptance record, clean the isolated worktree, declare R1-C GO or NO-GO, and pause before R1-D.
