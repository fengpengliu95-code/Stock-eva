# Release 2 R2-B — Portfolio daily ledger and risk engine

Status: worker implementation complete; pending independent specification/code-quality
review and integration. This document is not a Release 2 GO decision.

## Product boundary

- Local, after-close research only.
- Snapshots are entered manually and remain in the dedicated local
  `stock_eva_portfolio.sqlite3` database. Existing positions, watchlists, strategies and
  alerts remain in `stock_eva_user.sqlite3`; the portfolio API never changes that shared
  database's WAL mode or schema.
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
  - Stale or concurrent revisions return `409`. Only content equal to the current latest
    revision is an idempotent retry; content matching an older revision still returns `409`.
- `GET /api/v1/portfolio/daily-snapshots?as_of=YYYY-MM-DD[&revision=N][&known_at=...]`
  - Selects only a snapshot with effective date `as_of` or earlier and
    `recorded_at <= known_at`; an explicit revision uses the same knowledge cutoff.
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

- `GET /api/v1/analysis/portfolio-risk?as_of=YYYY-MM-DD[&known_at=...]`
- `as_of` is the effective market/snapshot date. `known_at` is an independent evidence
  knowledge boundary. When omitted it is deterministically the end of `as_of` in
  `Asia/Shanghai`; an explicit value must be timezone-aware and no later than request start,
  and is normalized to UTC. This permits an explicit later-known replay without leaking a
  later revision into the default historical view.
- Reads only:
  - the latest manual revision satisfying both effective-date and knowledge-time cutoffs;
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
- `evidence_cutoff_at` and deterministic `computed_at` (the same UTC logical watermark);
- public selected-snapshot, market-content, classification-generation, regime-result and
  formula/policy lineage, all bound into the stable result identity.

The market lineage exposes source/version, date range, row count, logical content ID and
SHA-256. Because the current local-dataset manifest has no historical `published_at`/run
history, it explicitly returns `publication_id=null`,
`publication_visibility_status=unverifiable` and degrades the result. A bar's `ingested_at`
is not presented as proof that a manifest had published it.

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
- Suspended, non-trading and bad-quality rows are removed before selecting the latest 15
  effective observations for ATR14. Current invalid price still blocks current valuation.
- ATR14 normalizes high/low/close to the latest valid adjustment factor before calculating
  14 true ranges. Missing/invalid factors fail closed; raw split gaps are never used.
- Fewer than 15 effective observations cannot produce ATR14.
- Sector mapping below 95% keeps an `unknown` bucket but blocks the complete sector
  concentration claim and sector guardrail.
- Manual NAV that differs from derived NAV by more than CNY 0.01 emits a quality issue;
  a manual NAV that cannot be reconciled is also explicit.
- Stale portfolio or market dates are degraded.
- Incomplete NAV history may expose an explicitly observed-only degraded drawdown metric,
  but the maximum-drawdown guardrail is `unavailable`, never `pass` or `breach`.
- Future portfolio snapshots, raw/published bars, classification generations/memberships,
  regime inputs and NAV observations are excluded.
- Missing user storage is a structured empty result without writes. Corrupt SQLite,
  classification, or published-market storage maps to a stable structured `503`.
- The dedicated ledger uses SQLite rollback-journal (`DELETE`) mode and permission `0600`.
  GET opens `mode=ro`/`query_only`; risk snapshot plus history are read in one transaction.
  Missing storage creates nothing, and a wrong WAL-mode/hot or corrupt file fails closed.

## Private backup lifecycle

The scheduled `backup-private-data` command publishes the shared user database and dedicated
portfolio ledger as one local atomic bundle. The bundle manifest binds both roles, UTC run
time, per-database SHA-256 and `PRAGMA integrity_check`; daily and weekly bundles publish only
after the whole candidate verifies. Failure in either existing database leaves prior complete
bundles unchanged. A never-created portfolio ledger is recorded as `not_initialized` and the
bundle is `partial`, not described as complete. Retention remains seven daily and four weekly
bundles, and network-volume targets remain forbidden. Backup roots/bundle directories are
`0700`; manifests and SQLite snapshots are `0600` and the verifier rejects weaker modes or
symlinked evidence.

This branch does not silently migrate any experimental portfolio table from the shared user
database. Such a database contains private user data and requires an explicit offline,
user-confirmed migration if one ever existed.

## TDD and synthetic acceptance evidence

Initial RED:

```text
UV_OFFLINE=1 uv run --extra dev pytest -q tests/test_portfolio_risk.py
ImportError: cannot import name 'portfolio_risk' from 'backend.app.api'
```

Focused GREEN after implementation and the publication/PIT integration case:

```text
UV_OFFLINE=1 uv run --extra dev pytest -q tests/test_portfolio_risk.py
.........................................                                [100%]
41 passed
```

The focused suite uses only `tmp_path` synthetic data and verifies:

- idempotency, revisions, optimistic conflicts, concurrent writers, retained history and
  point-in-time selection by `recorded_at` cutoff;
- physical GET no-write checks, separate DELETE-mode storage and a barrier-controlled
  concurrent writer/read-view test;
- finite/negative/duplicate input rejection and absence of broker/order tables;
- cash/cost/sector/ATR/price quality degradation;
- concentration, ATR budget, drawdown and guardrail pass/breach behavior;
- bull/range/bear target bands plus low-confidence/narrow-scope widening;
- deterministic result IDs and policy-version identity;
- equivalent UTC/`+08:00` cutoff replay, public lineage and deterministic computed time;
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
- Strict historical market-publication visibility remains unavailable until R2-C persists
  immutable manifest history with a trusted publication time/run identity. Current responses
  fail closed with `market.publication_visibility_unverifiable`.
- The service values long-only manual quantities. Shorting, derivatives, leverage, broker
  margin and order simulation are out of scope.
- No production database, NAS dataset, deployment or real user portfolio was used in this
  worker package.
