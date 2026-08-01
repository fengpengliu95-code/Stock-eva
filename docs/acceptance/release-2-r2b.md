# Release 2 R2-B — Portfolio daily ledger and risk engine

Status: worker implementation complete; pending independent specification/code-quality
review and integration. This document is not a Release 2 GO decision.

## Product boundary

- Local, after-close research only.
- Snapshots are entered manually and remain in the private SQLite user database.
- There is no broker credential, account sync, order, trade execution, or automatic
  adjustment schema/API.
- Target exposure is an explainable research range. It is not a personalized investment
  conclusion, an order, or an instruction to trade.

## API contract

### Manual append-only ledger

- `POST /api/v1/portfolio/daily-snapshots`
  - Creates revision 1.
  - Repeating the same date and canonical content is idempotent.
  - Different content for an existing date returns `409` and requires `PUT`.
- `PUT /api/v1/portfolio/daily-snapshots/{as_of}`
  - Requires `expected_revision`.
  - A successful change appends a new revision and retains all older revisions.
  - Stale or concurrent revisions return `409`.
- `GET /api/v1/portfolio/daily-snapshots?as_of=YYYY-MM-DD[&revision=N]`
  - Selects only a snapshot at or before `as_of`; an explicit revision reads an immutable
    revision for the exact snapshot date.
  - A missing database/table returns `null` without creating a directory or database.

Example create body:

```json
{
  "as_of": "2026-07-29",
  "cash": 8000,
  "positions": [
    {"symbol": "sh.600001", "quantity": 10, "avg_cost": 90}
  ],
  "manual_nav": 9000
}
```

Every stored record returns `source=manual`, server `recorded_at`, `revision`, stable
`content_hash`, and stable `snapshot_id`. Decimal inputs must be finite and non-negative;
position quantities must be positive; symbols are normalized and duplicate symbols are
rejected.

### Deterministic portfolio risk

- `GET /api/v1/analysis/portfolio-risk?as_of=YYYY-MM-DD`
- Reads only:
  - the latest manual snapshot whose `as_of <= requested as_of`;
  - R1-C publication-aware canonical bars;
  - the promoted point-in-time classification generation visible at `as_of`;
  - the deterministic R1-B market-regime service through an injected local reader.
- It does not call another HTTP API and never falls back to `daily_bars` or
  `latest_refresh`.

The result contains:

- cash, manual/derived/selected NAV and valuation basis;
- gross/net exposure and maximum single-position weight;
- sector buckets with `unknown` shown separately;
- ATR14 per position and total one-ATR risk budget;
- daily NAV drawdown history;
- versioned single-position, sector, ATR-budget and drawdown guardrails;
- current exposure versus a deterministic market-state exposure band;
- selected snapshot, market, classification, regime and formula lineage in a stable result
  identity.

## Versioned research policy

`portfolio-risk-v1` uses guardrail policy `research-default-v1`:

| Guardrail | Threshold |
|---|---:|
| Maximum single-position weight | 20% |
| Maximum sector weight | 35% |
| Maximum total one-ATR risk / NAV | 6% |
| Maximum drawdown magnitude | 15% |

Base target exposure bands are keyed by the R1-B strategic/tactical state. A degraded or
low-confidence regime, or a scope that cannot support a full-A-share conclusion, widens the
band by 10 percentage points on each side (bounded by 0–100%), adds explicit reasons, and
keeps `personalization_allowed=false`. A missing/future regime makes the band unavailable.

## Fail-closed gates

- Missing cash, cost, NAV, published price, sector, ATR or regime appears in
  `missing_inputs`; values are not guessed.
- Suspended or bad-quality current bars cannot produce a price, weight or ATR value.
- Fewer than 15 usable observations cannot produce ATR14.
- Sector mapping below 95% keeps an `unknown` bucket but blocks the complete sector
  concentration claim and sector guardrail.
- Manual NAV that differs from derived NAV by more than CNY 0.01 emits a quality issue;
  a manual NAV that cannot be reconciled is also explicit.
- Stale portfolio or market dates are degraded.
- Future portfolio snapshots, raw/published bars, classification generations/memberships,
  regime inputs and NAV observations are excluded.
- Missing user storage is a structured empty result without writes. Corrupt SQLite,
  classification, or published-market storage maps to a stable structured `503`.

## TDD and synthetic acceptance evidence

Initial RED:

```text
UV_OFFLINE=1 uv run --extra dev pytest -q tests/test_portfolio_risk.py
ImportError: cannot import name 'portfolio_risk' from 'backend.app.api'
```

Focused GREEN after implementation and the publication/PIT integration case:

```text
UV_OFFLINE=1 uv run --extra dev pytest -q tests/test_portfolio_risk.py
.........................                                                [100%]
25 passed
```

The focused suite uses only `tmp_path` synthetic data and verifies:

- idempotency, revisions, optimistic conflicts, concurrent writers, retained history and
  point-in-time selection;
- finite/negative/duplicate input rejection and absence of broker/order tables;
- cash/cost/sector/ATR/price quality degradation;
- concentration, ATR budget, drawdown and guardrail pass/breach behavior;
- bull/range/bear target bands plus low-confidence/narrow-scope widening;
- deterministic result IDs and policy-version identity;
- no-future behavior for every dependency;
- real `ClassificationStore` historical generation selection and
  `PublishedSnapshotDuckDbMarketReader` isolation from a newer un-published raw bar;
- API `201/200/409/422/503` behavior and GET-without-write.

Full-suite, Ruff and diff-check evidence is recorded in the worker handoff after the final
fresh verification.

## Honest limitations

- This slice does not make R2 fund-flow evidence a portfolio-risk input; R2-A remains a
  separate evidence contract.
- The current R1-B output is intentionally narrow/degraded, so the real target band remains
  broad and non-personalized. It must not be described as a full-A-share allocation result.
- Sector completeness depends on a promoted >=95% point-in-time classification generation.
- The service values long-only manual quantities. Shorting, derivatives, leverage, broker
  margin and order simulation are out of scope.
- No production database, NAS dataset, deployment or real user portfolio was used in this
  worker package.
