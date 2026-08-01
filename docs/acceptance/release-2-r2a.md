# Release 2 R2-A acceptance

Date: 2026-08-01 (Asia/Shanghai)

R2-A fund-flow evidence slice: **REPAIRED LOCALLY — INDEPENDENT RE-REVIEW REQUIRED**

Release 2 verdict: **PENDING — do not claim GO**

## Scope verified in this worker

- `GET /api/v1/analysis/fund-flow-evidence` is a deterministic, read-only analysis
  endpoint for the truthful `sh_sz_market` market universe and exact `sector` scopes.
  Market responses preserve the distinct upstream scope identity `沪深市场`; the API
  rejects the unsupported `all_a_share` label rather than widening the source claim.
- L1 is explicitly an upstream-reported order-size proxy. The API preserves
  `source`, `upstream`, the exact audited source page, explicit `trade_date`, aware
  `observed_at`, units, provider/formula version, object SHA-256 lineage and confidence.
  It never describes the records as exchange-certified institutional identity.
- L2 public institutional traces and L3 price-volume inference are separate
  `unavailable` records in this slice. They are not zero-filled, combined with L1 or
  presented as observations.
- L1 raw records use only existing `reported_*` supplemental fields. OHLCV and
  `amount` are not read or returned, and turnover is never renamed as net inflow.
- The 1/5/20-session aggregates use one source, upstream, endpoint, scope and
  provider contract. Mixed sources, upstreams, endpoints, scopes, duplicate dates and
  non-finite values fail closed rather than being added together.
- Every source `trade_date` is checked against the confirmed exchange calendar.
  Weekend/holiday points fail closed and unknown calendar years are explicitly
  `trading_calendar_unverifiable`; neither can enter raw evidence or lineage while a
  trend remains publishable.
- Explicit ingestion `through_date`, analysis `as_of` and manifest publication dates
  must be confirmed open sessions. Closed or unknown ingestion dates fail before any
  provider or audit write. Same-day ingestion additionally requires the calendar's
  after-close data-ready boundary; a pre-ready request performs no provider, audit or
  staging write, while completed historical sessions remain eligible. A
  weekend/holiday query may preserve the prior session as `last_trusted_date`, but it
  is degraded and cannot publish a weekend trend.
- The pinned provider contract maps market evidence only to the AKShare
  `stock_market_fund_flow` operation and its Eastmoney market source page, and sector
  evidence only to `stock_sector_fund_flow_hist` and its sector source page. Source,
  upstream, scope, endpoint and units are validated during ingestion, in the immutable
  manifest and again by the analysis service. Uniformly wrong endpoints fail closed.
- Observation timestamps must be timezone-aware and no later than publication. A
  later-observed backfill is not exposed as historically known evidence; historical
  publication knowledge remains `unverifiable` and cannot publish.
- A trend conclusion requires 20 contiguous exchange trading sessions, complete
  1/5/20 windows, zero trading-session staleness, a ready source snapshot, no future
  records, verified publication knowledge and an immutable object hash. Nineteen
  sessions or one missing middle session cannot publish a conclusion.
- Calendar continuity comes from the confirmed A-share trading calendar. Natural-day
  adjacency is not used as a substitute.
- `not_configured`, `empty`, `insufficient_history`, `ready`, `stale` and `error`
  are explicit API states. Every non-ready state has `can_publish_trend=false` and
  `conclusion=null`.
- Partial data and a failed later ingestion retain the last published raw evidence and
  `last_trusted_date`; they do not create a new observation or trend. A corrupt
  published manifest/object fails closed as `error`.
- Current supplemental manifests keep only one pointer per dataset kind. Therefore a
  later manifest cannot retroactively prove that object history was visible on an
  earlier `as_of`. Later-observed rows are withheld from historical raw evidence,
  `publication_knowledge=unverifiable`, and no trend can be published.
- `result_id` and semantic lineage IDs are stable for identical semantic inputs.
  Source and observation semantics, formula/provider version, object hash, scope,
  dates, metrics, missing inputs and confidence participate. The exact seconds of
  `computed_at` are deliberately excluded: moving publication by one second without
  changing visibility or any evidence semantics keeps the same ID. If publication
  knowledge or any gate changes, those changed response semantics still change the ID.
- One versioned policy object drives response `formula_version`, evidence semantics,
  units and L2/L3 availability. Formula, units, reasons and unavailable-level changes
  cannot retain an old identity.
- Manifest entries carry `source=akshare`, the exact supported provider contract,
  upstream, source scope, endpoint and unit map. Wrong types or values return a
  structured error instead of being stringified into trusted lineage.
- Missing or partial order-size components leave concentration null, disclose the
  valid component count and absolute denominator, add a structured missing input and
  lower confidence. The independently valid 20-session main-flow trend is not silently
  zero-filled or blocked solely by an unavailable concentration breakdown.
- Price divergence and member-level sector diffusion are explicitly unavailable in
  this slice. They remain null and are listed in `missing_inputs`; they are not
  inferred from L1 records.
- A GET against missing or corrupt synthetic roots created no database, directory,
  schema or cache. A GET against a ready synthetic publication left the complete file
  tree byte-for-byte unchanged.
- Fund-flow reads validate the requested dataset kind's manifest entry, immutable
  object hash, Parquet schema and source contract. An older unrelated classification
  or other fund-flow kind remains an informational
  `supplemental_dataset_dates_differ` issue and cannot demote a complete current
  market object; corruption of the requested object still returns `error`, and a
  sector query never falls back to a market object.
- No network, AKShare installation, NAS path, production database, user database,
  broker, order path, R1-C score or workspace UI was used or modified.

## RED to GREEN evidence

The interrupted worker left the initial implementation and 14 passing focused tests
uncommitted. This continuation preserved that state and added the following
test-first remediation:

```text
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py
# Resume boundary: 14 passed.

# Added publication-time, partial-snapshot and explicit component-unit tests.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py
# RED: 3 failed, 14 passed.
# GREEN: 17 passed.

# Added a counterexample proving a stored foreign upstream cannot be silently
# relabeled with model defaults.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py -k cannot_mask
# RED: 1 failed.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py
# GREEN: 18 passed.

# Added missing-lineage publication gating and explicit unavailable diffusion.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py \
  -k 'exactly_19 or without_object_lineage'
# RED: 2 failed.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py
# GREEN: 19 passed.

# Independent specification review then found four semantic counterexamples.
# The real provider scope first returned empty because the API queried all_a_share.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py \
  -k real_provider_market_scope
# RED: 1 failed (availability=empty).
# GREEN: 1 passed; sh_sz_market remains distinct from upstream identity 沪深市场.

# Closed weekend, statutory holiday and unknown-year dates were initially publishable.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py \
  -k 'closed_weekend_or_holiday or unknown_source_session'
# RED: 3 failed (all availability=ready).
# GREEN: 3 passed.

# Corrupt manifest source/provider semantics were initially accepted.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py \
  -k manifest_semantic_metadata_corruption
# RED: 3 failed (all source_status=ready).
# GREEN: 3 passed.

# A formula-version change altered the ID but not the reported version; an L2 reason
# change altered the response but not the ID.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py \
  -k formula_and_unavailable_semantics
# RED: 1 failed.
# GREEN: 1 passed with a single policy and semantic-response canonical ID.

# Second independent re-review reconstructed four additional counterexamples plus a
# stable-ID canonicalization defect before repair:
# - closed analysis/publication dates returned ready/can_publish=true;
# - a uniform cross-scope or empty endpoint was accepted;
# - naive/future observation timestamps were accepted and not returned in lineage;
# - missing concentration inputs retained the same confidence and no missing reason;
# - a one-second computed_at-only change changed result_id.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py
# RED: collection failed because the fixed provider-contract constants and new
# observation/metric schema did not exist. Direct baseline probes reproduced all five
# incorrect results above.

uv run --extra dev pytest -q tests/test_fund_flow_evidence.py
# GREEN: 50 passed.

uv run --extra dev pytest -q tests/test_fund_flow_evidence.py \
  -k 'manifest_semantic_metadata_corruption or uniform_wrong or observation_time \
      or concentration or result_id_ignores or publication_time or closed_analysis \
      or unknown_analysis or publication_date or ingestion_rejects'
# GREEN: 26 passed.

# Final independent review found two further counterexamples:
# - same-day ingestion at 10:00 Asia/Shanghai called the provider and published;
# - older unrelated classification/sector objects made a complete current market
#   object partial.
uv run --extra dev pytest -q tests/test_fund_flow_evidence.py \
  -k 'data_ready or unrelated_older or corrupt_target_market'
# RED: 2 failed, 3 passed.
# GREEN: 5 passed. The pre-ready rejection leaves provider/audit/manifest unchanged;
# the ready cutoff and completed historical sessions remain accepted; target-object
# corruption still fails closed and sector never falls back to market.

uv run --extra dev pytest -q tests/test_fund_flow_evidence.py
# GREEN: 55 passed.
```

## Verification

```text
uv run --extra dev pytest -q \
  tests/test_market_supplemental.py \
  tests/test_supplemental_ingestion.py \
  tests/test_fund_flow_evidence.py
# 70 passed

uv run --extra dev pytest
# 542 passed

uv run --extra dev ruff check backend tests
# All checks passed!

uv run --extra dev ruff format --check \
  backend/app/api/fund_flow.py \
  backend/app/fund_flow \
  backend/app/market/supplement_ingestion.py \
  tests/test_fund_flow_evidence.py
# 7 files already formatted

git diff --check
# no output
```

All fund-flow publications used by these tests were synthetic temporary Parquet,
manifest and SQLite artifacts. Test providers performed no network calls.

## Real canary and production status

No real AKShare/Eastmoney canary was executed in this worker. The optional dependency
was not installed, and no provider request was made. The production supplemental
publication was not read or changed. Consequently this acceptance provides **no real
market or sector fund-flow trend** and makes no claim about live source availability,
20-session continuity, date drift or provider formula stability.

A future real canary must remain explicit and bounded: one completed trading date,
market plus 3-5 named sectors, pinned provider version, schema/date checks, contiguous
session audit, immutable publication readback and manual semantic comparison. Until
that evidence exists, production trend availability must be shown as unavailable.

## Pending Release 2 gates

- Independent specification and code-quality re-review of this repaired R2-A slice.
- Bounded real market and 3-5-sector canary with at least 20 auditable contiguous
  sessions before any live trend language is published.
- A versioned historical publication-pointer contract if historical replay is to move
  from `unverifiable` to `verified`.
- Real L2 evidence sources and a separately labeled L3 price-volume model; neither may
  be merged with L1.
- R2-B portfolio ledger/risk, R2-C EvidencePack/journal, R2-D workspace integration and
  R2-E browser/continuity acceptance.
- Any later R1-C sector/leader integration must consume only `can_publish_trend=true`
  evidence and must version its own scoring formula.

These gates prevent an R2-A review GO or Release 2 GO claim.
