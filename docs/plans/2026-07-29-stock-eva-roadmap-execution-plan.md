# Stock EVA Roadmap Implementation Plan

> **For Codex workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` and
> `superpowers:test-driven-development` to implement one assigned work package at a time.

**Goal:** Deliver and verify every phase in the 2026-07-29 Stock EVA strategy-review
Roadmap without weakening the local, after-close, no-broker product boundary.

**Architecture:** Preserve the canonical BaoStock → DuckDB/Parquet → FastAPI pipeline and
SQLite private-user store. Add versioned derived-analysis services behind narrow API
contracts, then consume only those confirmed contracts from a modular web workspace.
Integrate one dependency-ordered work package at a time; each package receives independent
specification and code-quality review before it reaches `main`.

**Tech Stack:** Python 3.12, FastAPI, Pydantic, DuckDB/Parquet, SQLite, TA-Lib, pytest,
Ruff, TypeScript/Vite, KLineChart, browser-based acceptance.

---

## 1. Program controls

### Authoritative scope

- Product Roadmap:
  `docs/plans/2026-07-29-stock-eva-strategy-review-roadmap.md`
- This document controls implementation order and evidence, but cannot narrow the Product
  Roadmap.
- Market data remains local and deterministic. LLM components, when reached, are read-only
  consumers of redacted structured evidence.
- No work package may introduce real-time quotes, broker credentials, order APIs, automatic
  trading, or unqualified investment advice.

### Branch and worker protocol

1. Every implementation worker starts from the latest reviewed integration commit in an
   isolated Codex worktree.
2. A worker owns one bounded work package and commits only its own files.
3. New behavior follows RED → GREEN → REFACTOR. The worker reports the command and expected
   failure observed before production code.
4. The orchestrator inspects the commit and focused verification output.
5. A separate specification reviewer checks the diff against this plan and the Product
   Roadmap.
6. Only after specification approval does a separate code-quality reviewer inspect the
   same commit range.
7. Critical and important findings return to the implementation worker and are re-reviewed.
8. The orchestrator cherry-picks an approved commit, runs the full suite, and performs the
   relevant API/runtime/browser acceptance before starting a dependent package.

### Universal verification gate

Run after every integrated package:

```bash
uv run --extra dev pytest
uv run --extra dev ruff check backend tests
git diff --check
```

Packages that touch the workspace must also run the tracked frontend tests/build and a real
browser acceptance against the local API. Packages that change data publication must run the
real local-dataset acceptance without opening or modifying the production user database.

## 2. Dependency graph

```text
R0-A Security analysis API
  -> R0-B Stock cockpit web vertical slice
  -> R0-C Release 0 browser and performance acceptance

R1-A Point-in-time security/index/sector master
  -> R1-B Market regime engine
  -> R1-C Sector rotation and leader engine
  -> R1-D Market/sector workspace
  -> R1-E Release 1 historical and browser acceptance

R2-A Evidence-tier fund-flow ingestion
R2-B Portfolio daily ledger and risk engine
  -> R2-C EvidencePack and daily review
  -> R2-D Flow-enhanced sector/stock/portfolio workspace
  -> R2-E Release 2 continuity and browser acceptance

R3-A A-share replay and execution constraints
  -> R3-B Portfolio backtest and performance metrics
  -> R3-C Walk-forward/out-of-sample strategy lifecycle
  -> R3-D Strategy lab and decision-journal workspace
  -> R3-E Release 3 no-future-data and browser acceptance

All releases
  -> F-1 Requirement-by-requirement completion audit
  -> F-2 Production runtime deployment/readback
```

## 3. Release 0 — usable stock review

### R0-A: Security analysis API and indicator service

**Files:**

- Create: `backend/app/analysis/__init__.py`
- Create: `backend/app/analysis/models.py`
- Create: `backend/app/analysis/indicators.py`
- Create: `backend/app/analysis/service.py`
- Create: `backend/app/api/security.py`
- Modify: `backend/app/api/router.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `tests/test_security_analysis.py`
- Create or modify: API documentation under `docs/`

**Contract:**

- `GET /api/v1/securities/{symbol}/analysis?start=YYYY-MM-DD&end=YYYY-MM-DD`
- Indicators use the forward-adjusted series returned by `PriceSeriesService`.
- Suspended sessions remain excluded.
- Required first contract: OHLCV/amount, MA5/10/20/60/120/250, MACD(12,26,9), RSI14.
- Insufficient warm-up is JSON `null`, not zero or a frontend guess.
- Top-level metadata includes `symbol`, `status`, `as_of`, `source`,
  `price_adjustment`, `formula_version`, and `quality_issues`.
- Empty history is explicit. Missing adjustment factors continue to fail closed.
- Tests prove the requested `end` date prevents any future read.

**TDD sequence:**

1. Write response-model, warm-up, fixed-indicator and no-future-data tests.
2. Run focused tests and confirm failure because the analysis module/route does not exist.
3. Add the smallest TA-Lib adapter and service that satisfies those tests.
4. Add API validation/error tests, then the route.
5. Run focused tests, full backend verification, and commit.

**Exit evidence:**

- Fixed reference values for at least MA and one momentum indicator.
- Explicit tests for forward adjustment, suspension removal, warm-up, empty history, invalid
  date range, missing factor, and no future data.
- Full backend suite and Ruff pass.

### R0-B: Modular stock cockpit and K-line

**Files:**

- Create: `workspace/package.json`
- Create: `workspace/package-lock.json`
- Create: `workspace/tsconfig.json`
- Create: `workspace/vite.config.ts`
- Create: `workspace/src/api.ts`
- Create: `workspace/src/state.ts`
- Create: `workspace/src/charts/stock-cockpit.ts`
- Create: `workspace/src/views/security-analysis.ts`
- Create: `workspace/src/main.ts`
- Modify: `workspace/index.html`
- Modify: `workspace/style.css`
- Replace or retire only the superseded parts of: `workspace/app.js`
- Create: focused frontend tests under `workspace/src/`
- Modify: `tests/test_workspace_static.py`

**Contract:**

- Holdings and watchlist symbols are real links/buttons that open one security cockpit within
  two interactions.
- KLineChart renders forward-adjusted daily candles and real volume.
- MA overlays and MACD/RSI panes use API values; the browser never recomputes indicators.
- The visible header shows symbol, actual `as_of`, source, adjustment, formula version and
  quality status.
- Loading, empty, partial/error and stale states preserve truth and never inject demo series.
- Keyboard navigation, focus visibility, chart text alternatives and narrow-screen layout are
  tested.
- Vite emits assets that the existing static-server and LaunchAgent deployment can serve.

**TDD sequence:**

1. Write failing tests for API parsing, state transitions and symbol navigation.
2. Add the Vite/TypeScript shell without changing current market/portfolio behavior.
3. Add the security cockpit and KLineChart adapter using the reviewed R0-A schema.
4. Write failing static/accessibility integration assertions.
5. Add the smallest HTML/CSS integration and remove only superseded script paths.
6. Run frontend tests/build, backend workspace tests and commit.

**Exit evidence:**

- Built workspace loads with no console error.
- A real holding and a real watchlist item each reach the cockpit in at most two interactions.
- Candles, volume, MA, MACD and RSI match the R0-A response.
- Empty and API-error screenshots contain no fabricated points.

### R0-C: Release 0 acceptance

**No production implementation is allowed in this task.**

1. Run full Python and frontend verification from a clean integration checkout.
2. Run a cached 260-session analysis request repeatedly and record P95; target `<500 ms`.
3. Use the in-app browser at the real local workspace URL.
4. Validate desktop and narrow-screen layouts, keyboard navigation and focus states.
5. Compare at least five representative securities against raw API responses.
6. Record GO/NO-GO evidence in `docs/acceptance/release-0.md`.

Release 0 is GO only when every Product Roadmap Release 0 acceptance item has direct evidence.

## 4. Release 1 — market, sectors and leadership

### R1-A: Point-in-time security, index and sector master

**Files:**

- Create: `backend/app/classification/models.py`
- Create: `backend/app/classification/store.py`
- Create: `backend/app/classification/service.py`
- Create: `backend/app/api/classification.py`
- Create: `tests/test_point_in_time_classification.py`
- Modify controlled ingestion/publication and documentation files.

**Requirements:**

- Versioned `security_master_history`, `index_component_history` and
  `sector_membership_history` with effective dates and source lineage.
- Extend the truthful market scope beyond current沪深主板 before using “全 A 股”.
- Do not infer an end date not supplied by a source.
- Point-in-time queries must return membership valid at the requested `as_of_date`.
- Publish coverage and unmapped-security audit; target at least 95% for the eligible universe.

### R1-B: Versioned market-regime engine

**Files:**

- Create: `backend/app/regime/models.py`
- Create: `backend/app/regime/service.py`
- Create: `backend/app/regime/store.py`
- Create: `backend/app/api/analysis.py`
- Create: `tests/test_market_regime.py`

**Requirements:**

- Deterministic strategic state: `bull`, `range`, `bear`.
- Deterministic tactical state: `risk_on`, `neutral`, `risk_off`.
- Versioned component scores for trend, breadth, liquidity/available evidence, risk and
  leadership.
- Output confidence, missing inputs, supporting reasons and contrary evidence.
- Every threshold and weight is replayable; no hidden LLM classification.

### R1-C: Sector rotation and leader engine

**Files:**

- Create: `backend/app/sector/models.py`
- Create: `backend/app/sector/service.py`
- Create: `backend/app/api/sector.py`
- Create: `tests/test_sector_rotation.py`

**Requirements:**

- Sector `5d/20d/60d` relative strength, breadth, liquidity, persistence and dispersion.
- Leader scoring combines tradeability, relative strength, liquidity, trend quality and sector
  contribution.
- Suspended, limit-locked or abnormal-liquidity securities cannot silently become the top
  actionable candidate.
- Rankings are explainable and use only point-in-time membership.

### R1-D: Market and sector workspace

- Implement the decision order: market state → evidence → sector rotation → leader → stock.
- Provide market-regime reasons/contrary evidence, sector maps/rankings and leader drill-down.
- Keep the actual covered universe visible.
- Add loading/empty/degraded views for missing classification or regime snapshots.

### R1-E: Release 1 acceptance

- Replay at least 20 historical sessions and verify no future membership or price reads.
- Verify industry mapping coverage and all explicit gaps.
- Verify one regime snapshot per ready trading day.
- Browser-check market → sector → leader → stock drill-down.
- Record evidence in `docs/acceptance/release-1.md`.

## 5. Release 2 — capital evidence and position decisions

### R2-A: Evidence-tier fund-flow ingestion

- Preserve L1 upstream-reported order-size proxy, L2 public institutional traces and L3
  price-volume inference as separate records.
- Never add evidence tiers or incompatible provider formulas together.
- Each record carries source, date, semantics, formula/provider version and confidence.
- Implement bounded provider canaries before publication.
- Require at least 20 contiguous sessions before publishing persistence/trend language.
- Source outage retains the last trusted date and lowers confidence; it does not invent a new
  observation.

### R2-B: Portfolio daily ledger and risk engine

**Files:**

- Create: `backend/app/portfolio/ledger.py`
- Create: `backend/app/portfolio/risk.py`
- Create: `backend/app/api/portfolio_risk.py`
- Create: `tests/test_portfolio_risk.py`

**Requirements:**

- Local cash/NAV and daily snapshots.
- Position and sector concentration, ATR risk units, drawdown and risk-budget guardrails.
- Current exposure and target exposure band with deterministic reasons.
- Missing cash/cost/sector data produces an explicit degraded result.
- No broker or order-writing capability.

### R2-C: EvidencePack, daily review and decision journal

- Immutable daily evidence input references market, flow, sector, security and portfolio
  versions.
- Deterministic report tree and structured review schema.
- Decision journal records the evidence available at the time, user decision and later outcome.
- Any later LLM consumer receives only the structured pack and has no write path.

### R2-D: Integrated review workspace

- Show evidence tiers without merging semantics.
- Show market, sector, stock and portfolio constraints in one decision context.
- Show current versus target exposure as an advisory range with reasons, not an order.
- Add daily report navigation and missing-evidence warnings.

### R2-E: Release 2 acceptance

- Prove 20-session continuity for each published trend or explicitly mark it unavailable.
- Search UI/API schemas for forbidden turnover-as-inflow or certified-institution language.
- Recompute identical portfolio inputs and verify identical results.
- Browser-check a ready, partial and provider-outage day.
- Record evidence in `docs/acceptance/release-2.md`.

## 6. Release 3 — strategy validation

### R3-A: A-share replay and execution constraints

- Event-by-session replay constrained by `as_of_date`.
- T+1, suspension, board/risk-flag price limits, limit-lock tradeability, commissions and stamp
  duty.
- Point-in-time sector/index membership.
- Failing tests first for every execution constraint and future-read attempt.

### R3-B: Portfolio backtest and performance

- Signals, fills, holdings, cash, NAV, turnover and costs are replayable.
- Annualized return, maximum drawdown, Sharpe, win rate, turnover and cost sensitivity.
- Every result contains dataset, strategy and formula versions.

### R3-C: Out-of-sample strategy lifecycle

- Explicit research/train, validation and out-of-sample windows.
- Walk-forward support where appropriate.
- Strategy candidates require declared failure conditions and OOS evidence before observation
  status.
- No ML model enters a decision surface merely because in-sample metrics are positive.

### R3-D: Strategy lab and journal workspace

- Display assumptions, parameters, costs, coverage, in/out-of-sample split and failure
  conditions.
- Compare strategy observation with the decision journal.
- Keep all results labelled as rule candidates, not investment advice.

### R3-E: Release 3 acceptance

- Automated future-data adversarial tests.
- Point-in-time membership and transaction-cost verification.
- Known synthetic strategy with independently calculated expected fills/NAV.
- Browser-check replay, performance, OOS and failure-state surfaces.
- Record evidence in `docs/acceptance/release-3.md`.

## 7. Final completion audit and production readback

### F-1: Requirement audit

Create `docs/acceptance/roadmap-completion-audit.md` with one row for every explicit Roadmap
requirement:

- authoritative evidence;
- status: proved, contradicted, incomplete or missing;
- command, API response, file or screenshot that proves the status;
- remediation link for anything not proved.

Tests passing alone cannot prove product-level completion.

### F-2: Deployment and live readback

1. Merge only reviewed commits.
2. Build the tracked workspace assets.
3. Run full backend/frontend tests, Ruff and diff checks.
4. Run real local-dataset acceptance against the Application Support mirror.
5. Install/update the five LaunchAgents from the reviewed tree.
6. Poll bounded readiness for API and workspace.
7. Read back market status, security analysis, regime, sector, portfolio risk, review and
   strategy endpoints.
8. Complete browser acceptance on the deployed workspace.
9. Confirm the root worktree still preserves unrelated user files.

The Roadmap is complete only when F-1 contains no incomplete/missing requirement and F-2
proves the deployed local service, not merely the development checkout.
