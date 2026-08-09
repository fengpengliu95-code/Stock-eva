# Stock EVA R1-D Standalone Sector Workspace Specification

**Status:** Approved by the user's instruction to confirm R1-C and continue R1-D.

**Owner:** Stock EVA linear-delivery R1-D slice

**Date:** 2026-08-09

## Context

R1-A through R1-C publish point-in-time market-regime, sector-rotation and sector-leader evidence. The overview decision flow already renders those contracts, but the standalone `#sectors` navigation target still uses the legacy optional supplemental-market endpoint and says that classification and membership are not connected. That split makes a reviewed backend capability appear unavailable and breaks the intended market → sector → leader → stock workflow.

## Goal

Make the standalone sector workspace a truthful, reloadable frontend over the existing R1-B/R1-C analysis contracts. A user must be able to inspect the backend-ranked sectors, select one, review its reasons, raw metrics, limitations and leader candidates, open a candidate's technical cockpit, and return to the same sector context.

## Non-goals

- No backend, formula, classification, dataset or API changes.
- No R1-E 20-session replay or final Release 1 acceptance.
- No Release 2 fund-flow trends, position sizing, portfolio risk or investment recommendations.
- No sector heatmap, historical sector chart or full component-price table because the current reviewed APIs do not publish those datasets.
- No frontend score, rank, confidence, market-state or leader recalculation.
- No conversion of turnover/OHLCV into main-fund flow language.

## Functional requirements

### FR-1 Canonical sector route

- The route is `#sectors?as_of=<YYYY-MM-DD>&taxonomy_id=<id>[&sector_id=<id>]`.
- A bare `#sectors` resolves to the current Shanghai calendar date and the canonical BaoStock industry taxonomy, then replaces the hash with the canonical route.
- Malformed or incomplete query state fails closed to the bare-route default; it must not issue a request using unchecked date or taxonomy values.

### FR-2 Backend-owned ranking

- Read `GET /analysis/market-regime` and `GET /analysis/sector-rotation` for the same `as_of`; retain the backend ranking order exactly.
- Show sector name/id, backend rank, total score, confidence, quality, in-scope price coverage and ranking eligibility.
- Keep the actual covered universe, taxonomy, data date, lineage, formula version, missing inputs and quality issues visible.

### FR-3 Selected-sector evidence

- Select the requested sector when it exists; otherwise select the first backend ranking without claiming the requested sector was valid.
- Show the selected sector's supporting evidence, contrary evidence, raw metric values/formula versions, exclusion reasons and quality limitations.
- Fetch `GET /analysis/sectors/{sector_id}/leaders` only after a valid ranked sector is selected.
- Keep leader order and actionability fields exactly as returned. `actionable_primary=false`, missing limit-lock inputs and Release 2 fund-flow absence remain prominent.

### FR-4 Stock drill-down and return continuity

- A leader control opens the existing technical cockpit with the same `as_of`, `taxonomy_id`, `sector_id` and symbol.
- The security hash remains reloadable and records that the return target is the standalone sector workspace.
- Returning from the cockpit restores `#sectors` with the same selected sector and does not silently change the analysis date.
- Existing overview deep links and their return behavior remain backward compatible.

### FR-5 Request ownership

- Navigating to a newer sector/date aborts prior overview/leader requests.
- Leaving both decision views aborts their pending requests.
- A late response from an aborted route cannot update the DOM or hash.
- Reuse an already loaded market/sector/leader response when opening a cockpit from the same context; direct security deep links may fetch the three context slices in parallel.

### FR-6 Honest states

- Provide visible loading, empty, degraded and error states.
- HTTP 422 means the requested date is outside the readable boundary; HTTP 503 means local evidence storage is unavailable.
- An empty ranking means insufficient published evidence, not “no market hotspot.”
- Unsupported heatmap, historical series and component-detail visuals are removed rather than filled with fixtures or random graphics.
- Fund evidence remains explicitly `missing / unavailable` and attributed to Release 2.

### FR-7 Accessibility and layout

- Sector selection uses native buttons with accessible names and `aria-pressed`/`aria-current` state.
- Status and error messages use appropriate `role=status`/`role=alert` semantics.
- Keyboard selection preserves focus after rerender.
- The two-column sector workspace collapses to one column at the existing responsive breakpoint without body-level horizontal overflow.

## User stories

- As an after-close reviewer, I can open “板块与成交” and see the same published ranking used by the overview, rather than an unrelated “not connected” placeholder.
- As a reviewer of a sector, I can inspect why it ranked where it did and which evidence contradicts the result before looking at candidates.
- As a reviewer of a leader candidate, I can open its technical indicators and return to the same sector/date context.
- As a cautious user, I can distinguish missing evidence from a negative conclusion and can see that the v1 universe is narrower than all A shares.

## Acceptance criteria

1. `#sectors` renders backend sector rankings and no longer depends on `/market/supplemental` for the canonical workspace.
2. A historical sector deep link round-trips the date, taxonomy and selected sector through reload, selection, leader-to-stock navigation and back navigation.
3. Backend ordering is preserved; the frontend does not sort or recompute scores.
4. Loading, empty, HTTP 422, HTTP 503 and degraded/missing-evidence states have explicit non-misleading copy.
5. Switching or leaving routes aborts stale requests, and stale completions cannot overwrite the active view.
6. Actual scope, lineage, formula, raw metrics, reasons, contrary evidence and actionability limitations are visible.
7. No canonical sector UI calls an amount “main-fund flow” unless it comes from a separately identified upstream-reported field; R1-D itself publishes no fund-flow direction.
8. Existing overview, portfolio/watchlist entrypoints and technical-cockpit tests remain green.
9. Frontend typecheck, full Vitest suite and production Vite build pass.
10. Installed-runtime browser acceptance shows real sector/leader data, a working cockpit round trip, no console errors and unchanged protected-data fingerprints.

## Edge cases

- Requested sector id is absent from the current point-in-time ranking.
- Ranking is empty while market regime is present.
- One analysis endpoint returns 422 or 503.
- Leader response is empty or locally degraded.
- Metric values and coverage fields are legal `null` and must render as `—`.
- Sector ids require URL encoding.
- Rapid sector selection, browser back/forward and navigation away race with pending requests.
- A legacy security deep link lacks an explicit return target; it defaults to the existing overview behavior.

## API and data contracts

Inputs are the existing typed DTOs in `workspace/src/decision-api.ts`:

- `MarketRegimeResponse`
- `SectorRotationResponse` / `SectorRanking`
- `SectorLeadersResponse` / `LeaderCandidate`

The frontend validates each JSON response before rendering and treats contract violations as unavailable evidence. It preserves backend-provided `result_id`, `formula_version`, `as_of`, `data_as_of`, taxonomy, lineage, actual scope, status, quality, missing inputs and evidence arrays. No new persistent data model is introduced.

## Dependencies

- Reviewed R1-B market-regime endpoint.
- Reviewed R1-C sector-rotation and sector-leader endpoints.
- Existing R0 technical-cockpit route and chart implementation.
- Existing legacy shell navigation and Vite/TypeScript build.

## Security and privacy

- Requests remain localhost-only under `/api/v1`.
- No broker connection, order, external analytics, local/session storage or user-data mutation is added.
- UI errors expose only controlled summaries, never backend traces.
- Dynamic content uses DOM `textContent`; no API-derived HTML injection is permitted.

## Non-functional requirements

- One active decision request manager owns overview and leader requests.
- Avoid duplicate calls when context is already loaded.
- Preserve modular TypeScript and the tracked Vite build artifact.
- Keep changes surgical to the standalone sector workspace, shared routing/context and acceptance docs.

## Rollout and rollback

- Develop and review in the isolated `codex/r1-d-standalone-sector-closure` worktree.
- Integrate the reviewed commit to main, run full frontend and backend verification, then install through the existing official runtime installer.
- Verify installed code SHA and browser behavior before recording R1-D GO.
- Rollback is the prior installed runtime/main commit; no data migration or schema rollback is required.

## Resolved questions

- “Sector map” is implemented as a navigable evidence-ranked map/list because no reviewed spatial or historical-series contract exists.
- The optional supplemental AKShare endpoint is not the canonical R1-D source and is removed from this workspace flow.
- R1-D closes frontend continuity only; historical replay and final Release 1 browser gate remain R1-E.
