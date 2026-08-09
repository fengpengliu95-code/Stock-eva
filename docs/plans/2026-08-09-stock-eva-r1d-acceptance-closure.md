# Stock EVA R1-D Acceptance Closure Implementation Plan

> **For Codex:** REQUIRED SUB-SKILLS: follow test-driven development, use the isolated worktree, and verify fresh evidence before completion claims.

**Status:** Approved by the user's instruction to confirm R1-C and continue R1-D.

**Goal:** Replace the stale standalone sector placeholder with a truthful, point-in-time market/sector/leader/stock frontend flow over the existing R1-B/R1-C contracts, then independently review and verify it in the installed runtime.

**Architecture:** Keep one request owner and the existing typed API parsers. Add a canonical sector hash alongside the existing overview hash, render a sector-specific ranked-list/detail workspace by reusing decision DTOs and evidence formatting, and carry an explicit return view through security context. Remove the legacy supplemental loader from the canonical sector page. No backend or score calculation changes.

**Tech Stack:** TypeScript 5.8, DOM APIs, Vite 7, Vitest/jsdom, Testing Library, KLineChart 10, Python/FastAPI installed runtime.

---

## Scope and acceptance boundary

In scope:

- `workspace/src/navigation.ts` and `workspace/src/state.ts`: canonical sector route and backward-compatible return context.
- `workspace/src/main.ts`: route ownership, rendering, selection, stale-request cancellation and security round trip for both overview and standalone sector views.
- `workspace/src/views/decision-flow.ts`: sector-specific ranked-list/detail renderer using backend facts only.
- `workspace/index.html`, `workspace/app.js`, `workspace/style.css`: replace the legacy placeholder, stop canonical supplemental loading, support query-bearing hashes and responsive presentation.
- Focused RED/GREEN tests, full frontend regression/build, independent review, main integration, full repository verification, installed-runtime and browser acceptance.
- Update R1-D/workspace documentation only after verified behavior exists.

Out of scope:

- Backend/API/formula/data changes, historical series, heatmaps, full component tables or fund-flow direction.
- R1-E 20-session replay/final Release 1 acceptance.
- Release 2/3 work, portfolio advice, order execution or broker integration.
- User learning documents, PDFs, protected market/classification/NAS/holdings data and the parked R2 worktree.

## Task 1: Add route and context RED tests

**Files:**

- Modify: `workspace/src/navigation.test.ts`
- Modify: `workspace/src/state.test.ts`
- Modify: `workspace/src/views/security-analysis.test.ts`

1. Prove `#sectors?as_of=...&taxonomy_id=...&sector_id=...` round-trips encoded ids.
2. Prove malformed sector state fails closed.
3. Prove a sector-origin cockpit back link retains the standalone sector route while a legacy context still returns to overview.
4. Run only these tests and record RED before production changes.

## Task 2: Add standalone renderer RED tests

**Files:**

- Modify: `workspace/src/views/decision-flow.test.ts`
- Modify: `workspace/src/assembled-workspace.test.ts`

1. Assert loading, ready/degraded, empty and error states for the standalone root.
2. Assert backend order, actual scope, selected-state semantics, raw metrics, supporting/contrary evidence, fund-flow absence and non-actionable leader copy.
3. Assert no legacy heatmap/component fiction or canonical supplemental UI remains in assembled HTML.
4. Record RED before adding the renderer/markup.

## Task 3: Implement canonical route and standalone renderer

**Files:**

- Modify: `workspace/src/navigation.ts`
- Modify: `workspace/src/state.ts`
- Modify: `workspace/src/views/decision-flow.ts`
- Modify: `workspace/index.html`
- Modify: `workspace/style.css`

1. Add typed sector hash parse/serialize functions without changing existing overview hashes.
2. Add an optional validated return view to decision context; legacy hashes default to overview.
3. Render a compact backend-ordered ranking list and selected-sector evidence/leader detail.
4. Use native controls, controlled text nodes and existing responsive breakpoints.
5. Remove unsupported chart/table placeholders instead of supplying fixtures.
6. Re-run focused renderer/navigation tests for GREEN.

## Task 4: Integrate request ownership and legacy shell

**Files:**

- Modify: `workspace/src/main.ts`
- Modify: `workspace/app.js`
- Modify: `workspace/src/decision-main.test.ts`
- Modify: `workspace/src/legacy-route-integration.test.ts`

1. Let the one decision manager load either overview or sector workspace with the same query and selected id.
2. Generate the active view's hash on selection and preserve focus.
3. Carry standalone return context into the cockpit; restore the matching route on back/forward/reload.
4. Abort pending analysis when leaving both decision views and reject stale completion.
5. Make the legacy shell recognize query-bearing hashes and remove `/market/supplemental` from canonical workspace startup.
6. Record focused RED/GREEN for historical deep links, rapid selection, route exit and round trip.

## Task 5: Development verification and commit

1. Confirm no conflicting Vitest process is running.
2. Run `npm test -- --run` and `npm run build` from `workspace/`.
3. Run `git diff --check` and inspect the exact diff for backend, data and future-release scope violations.
4. Commit a clean, stable development slice; do not declare R1-D GO.

## Task 6: Independent serial review

1. A separate reviewer inspects the exact committed diff read-only.
2. Review route validation, stale-request safety, backend ordering, point-in-time context, XSS-safe rendering, accessibility, truthful missing/degraded language, technical-cockpit regression and scope containment.
3. Any blocker returns to the first relevant RED task; otherwise issue APPROVED for the exact commit.

## Task 7: Main integration and full verification

1. Preserve user-owned main changes and cherry-pick only approved R1-D commits.
2. Run fresh full frontend tests/build and repository diff checks.
3. Run the full backend suite once from integrated main because the installed runtime packages frontend and backend together.
4. Record current counts and hashes; do not reuse the old 55-test or backend-count snapshots.

## Task 8: Installed-runtime and product acceptance

1. Record protected dataset/classification/control/user-data fingerprints before installation.
2. Install through the official runtime workflow and verify reported code SHA.
3. Browser-check bare/current and historical `#sectors` routes, selection, real leader data, cockpit round trip, back/forward, responsive layout and console/network diagnostics.
4. Prove no canonical supplemental request, fabricated visual, fund-flow inference or stale-response overwrite.
5. Recheck fingerprints, update `docs/workspace.md` and `docs/acceptance/release-1-r1d.md` with fresh evidence, commit the acceptance record, remove the clean worktree, declare R1-D GO or NO-GO, and pause for user confirmation before R1-E.
