# Release 1 R1-B acceptance

Date: 2026-07-30 (Asia/Shanghai)

R1-B review remediation: **LOCALLY VERIFIED — RE-REVIEW REQUIRED**

Independent review: **SECOND CHANGES REQUIRED received; follow-up not yet re-reviewed**

Mainline fresh-suite isolation follow-up: **LOCALLY VERIFIED**

Release 1 verdict: **PENDING — do not claim GO**

## Scope verified in this worker

### 2026-08-09 R1-B performance/coverage closure follow-up

- The published-dataset path previously rebuilt the complete 130-session R1-B input on
  every API request. `MarketRegimeStore` now reuses the evaluated input only when the
  immutable manifest identity, `as_of` and UTC-normalized `known_at` are identical.
  The reader still parses and validates the current manifest and checks every referenced
  object exists before it may return a cache hit. A changed manifest generation/file
  identity causes a reread; malformed/unavailable metadata remains a 503 and cannot be
  masked by an earlier result. The cache is process-local, bounded (128 entries), does
  not write storage, and returns a deep copy.
- Focused regression evidence uses reader-call semantics rather than a machine-specific
  timing threshold: one unchanged immutable/PIT request produces one expensive reader
  call; changing either `known_at` or generation produces another; an availability
  preflight failure after a cache fill raises rather than returns the cached value.
- Cold reads now project only the 17 Parquet fields consumed by R1-B and construct the
  in-memory bar without repeat Pydantic validation. This is safe only on the published
  reader because the object has already passed the immutable manifest checksum/schema
  validation; the normal DuckDB reader remains fully model-validated. The projection
  test proves omitted fields (`open`, `volume`) are not read and every formula/PIT field
  is retained. It does not replace Root's installed-dataset cold-latency measurement.
- The R1-A ready classification generation is not evidence that R1-B has full-A price
  coverage. In the current code, R1-B receives only `DailyBar` rows and derives scope
  from their current-date symbol presence (`backend/app/regime/store.py:_scope`); it has
  no classification-store dependency or classification generation lineage. Separately,
  the R1-A provider persists `price_available=None` for its security records
  (`backend/app/classification/provider.py:_security_record`), and its `MarketScope`
  labels this `not_audited` rather than verified price coverage. Its coverage audit is
  also taxonomy-membership coverage, not a same-date join against the immutable price
  publication. Therefore the old wording that treated an R1-A artifact as entirely
  future is stale, but promoting R1-B v1 from the ready classification pointer would be
  an unsupported cross-store/full-A claim.
- A future, separately reviewed formula/model v2 may add a read-only PIT adapter that
  joins the selected classification generation (including observed/source dates and
  generation id) to the immutable price manifest and reports eligible, observed and
  missing-symbol counts. It must preserve the current narrow/degraded result unless that
  explicit join proves the price coverage; that v2 contract is not silently introduced
  by this R1-B performance closure.

- `market-regime-v1` deterministically returns strategic bull/range/bear and tactical
  risk_on/neutral/risk_off.
- Trend, breadth, liquidity, risk and leadership each carry their own formula version,
  score, weight, quality, missing inputs, evidence and lineage.
- Result identity includes the full input, formula version, weights and thresholds.
- Exact threshold boundaries are replayable and inclusive.
- Missing leadership and unavailable fund-flow evidence fail degraded without zero-filling
  or guessing.
- Expected and observed boards/index series, their missing sets, coverage basis and
  conclusion capability are explicit.
- Symbol presence never supplies a stock-universe denominator. R1-B exposes no
  authoritative audit variant, expected-universe count, or board/full-market coverage
  ratio. Every result remains narrow provisional, low confidence and unsuitable for a
  full A-share bull/bear conclusion—even when all four board labels and all six index
  symbols are observed.
- `ActualMarketScope` rejects caller-supplied full labels, true capability, authoritative
  basis and extra audit/count/ratio fields. A future authoritative integration requires an
  R1-A artifact and a model/formula version upgrade.
- The six-series index ratio reports only representative-index presence; it does not prove
  stock-universe coverage.
- Store reads are bounded by `as_of`; a defensive second boundary discards any future row.
- Risk requires separate warmups: 21 valid index sessions for 20-day volatility and 60
  valid sessions for 60-day drawdown. A shorter window is never labeled as 60-day.
- Provider `pct_change` is accepted only when `preclose` is finite and positive.
  Non-finite `pct_change` uses a finite positive-denominator close/preclose fallback when
  possible; otherwise the row is excluded and the component/result records a concrete
  quality issue.
- HTTP rejects a future Shanghai `as_of` with 422 before store read.
- A missing local market database, configured dataset root, or configured dataset manifest
  returns empty and creates no database, dataset path, temp state or schema.
- An existing corrupt/inconsistent published manifest remains an explicit storage error
  and is not swallowed as empty data.
- Canonical models can read and correctly label explicitly supplied main/ChiNext/STAR stock
  rows and representative index rows. Existing all-main-board ingestion remains unchanged.
- No BaoStock, NAS, production database or user data was accessed.

## RED → GREEN evidence

The focused test was written before the production implementation:

```text
uv run --extra dev pytest tests/test_market_regime.py -q
# RED: 16 failed
# Missing regime modules, market-regime API dependencies and canonical board/index contract.

uv run --extra dev pytest tests/test_market_regime.py -q
# GREEN after focused follow-ups: 19 passed

uv run --extra dev pytest \
  tests/test_market_regime.py::test_service_input_rejects_future_data_lineage -q
# Follow-up RED: 1 failed because a future data_as_of was accepted.
# Follow-up GREEN: 1 passed after input-level as_of validation.

uv run --extra dev pytest \
  tests/test_market_regime.py::test_narrow_price_scope_is_truthful_and_low_confidence -q
# Follow-up RED: 1 failed because missing boards/indexes lacked specific quality codes.
# Follow-up GREEN: covered by the final focused 19 passed.

# Independent-review counterexamples added before remediation:
uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# RED: 8 failed, 26 passed
# Failures: symbol-presence scope overclaim (21/130 sessions), 60-day drawdown warmup,
# four missing-root/manifest read-only cases, and NaN/inf API handling.

uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# Initial GREEN after remediation: 34 passed

uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_regime.py::test_regime_models_reject_non_finite_float_and_decimal \
  tests/test_market_regime.py::test_scope_model_rejects_inconsistent_full_a_claims
# Supplemental RED: 8 failed because non-finite evidence values and two contradictory
# full-A scope claims were still accepted.
# Supplemental GREEN: 8 passed after finite-model and scope invariants were enforced.

uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# Final focused GREEN: 36 passed

# Second-review counterexamples added before production changes:
uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_regime.py::test_zero_preclose_excludes_finite_provider_return_from_breadth_and_risk \
  tests/test_market_regime.py::test_r1b_scope_rejects_complete_caller_supplied_authoritative_claim
# RED: 2 failed. Risk accepted preclose=0 with finite pct_change as ready, and the
# caller-supplied complete authoritative scope passed validation.

uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_regime.py::test_zero_preclose_excludes_finite_provider_return_from_breadth_and_risk \
  tests/test_market_regime.py::test_r1b_scope_rejects_complete_caller_supplied_authoritative_claim \
  tests/test_market_regime.py::test_scope_model_rejects_r1b_full_a_promotion_fields \
  tests/test_market_regime.py::test_deterministic_states_cover_bull_range_bear_and_tactical_modes
# GREEN: 12 passed

uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# Prior focused GREEN: 43 passed

uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_regime.py::test_store_reuses_immutable_generation_input_and_reloads_on_pit_key_change \
  tests/test_market_regime.py::test_store_never_returns_cached_input_when_immutable_preflight_fails
# 2026-08-09 RED before cache implementation: first test failed (identity_calls=0).
# GREEN after implementation: 2 passed. The tests prove cache/PIT/generation/preflight
# call semantics without accessing an installed dataset.

uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# 2026-08-09 focused GREEN: 45 passed

uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_regime.py::test_published_reader_projects_only_regime_fields_from_validated_parquet
# 2026-08-09 RED: 1 failed because the reader requested the full DailyBar projection
# and attempted to decode the reduced fixture as a 24-column row.
# GREEN: 1 passed after the trusted immutable 17-field projection was introduced.

uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# 2026-08-09 final focused GREEN: 46 passed

# Mainline fresh-suite isolation reproduction:
uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_regime.py::test_api_missing_database_is_empty_and_creates_no_filesystem_state
# RED: 1 failed (503 instead of 200). A stale indirect get_settings override missed the
# FastAPI dependency identity and read only the temporary hostile corrupt manifest.

# After directly overriding get_market_regime_store with stores built from explicit Settings:
uv run --extra dev pytest -o addopts='' -q \
  tests/test_market_regime.py::test_api_missing_database_is_empty_and_creates_no_filesystem_state \
  tests/test_market_regime.py::test_api_existing_database_executes_select_only_and_preserves_bytes \
  tests/test_market_regime.py::test_api_non_finite_pct_change_falls_back_or_excludes_without_500 \
  tests/test_market_regime.py::test_api_missing_dataset_root_or_manifest_is_empty_without_writes \
  tests/test_market_regime.py::test_api_corrupt_existing_dataset_manifest_remains_explicit_failure
# GREEN: 8 passed

env \
  STOCK_EVA_LOCAL_MARKET_DATASET_ROOT=/tmp/stock-eva-r1b-hostile-env-c55429e \
  STOCK_EVA_NAS_MARKET_DATASET_ROOT=/tmp/stock-eva-r1b-hostile-nas-c55429e \
  uv run --extra dev pytest -o addopts='' -q <same targeted tests>
# Hostile-env GREEN: 8 passed
```

## Verification

```text
uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# 43 passed in 0.45s

uv run --extra dev pytest
# 487 passed in 55.19s

uv run --extra dev ruff check backend tests
# All checks passed!

git diff --check
# no output
```

## Real local narrow-scope replay acceptance

Run date: 2026-07-30 (Asia/Shanghai)

The worker read only the explicit public local market sources:

- published dataset:
  `/Users/finlay/Library/Application Support/Stock EVA/data/market-dataset`;
- control DB:
  `/Users/finlay/Library/Application Support/Stock EVA/data/market/stock_eva.duckdb`.

No NAS path or user database was opened. The latest 20 distinct manifest trading dates at
or before `2026-07-29` were replayed twice through `MarketRegimeStore` and
`MarketRegimeService`. No bar payload was emitted.

```text
trading dates: 2026-07-02 ... 2026-07-29
trading days: 20
replay rounds / store reads: 2 / 40
future rows: 0
data_as_of <= as_of: 20/20 in both rounds
full result equality: 20/20
result_id equality: 20/20
score equality: 20/20
strategic/tactical/status equality: 20/20
unique result IDs in one round: 20

strategic states: bear=14, range=6
tactical states: risk_off=17, neutral=3
status: degraded=20
quality status: degraded=20
score range: -61.6877 ... -0.4422

scope capability false / narrow_provisional / coverage unavailable: 20/20
observed universe range: 3191 ... 3197
observed boards on all dates: sse_main, szse_main
observed indexes on all dates: sh.000001, sz.399001
```

The aggregate missing-input set was:

```text
leadership.leader_diffusion
leadership.sector_persistence
liquidity.fund_flow_evidence
risk.representative_indexes
trend.index:sh.000300
trend.index:sh.000852
trend.index:sh.000905
trend.index:sz.399006
```

The aggregate quality-code set was:

```text
degraded_component_quality
market_scope_authoritative_coverage_unavailable
market_scope_missing_expected_boards
market_scope_missing_expected_index_series
missing_component_inputs
narrow_scope_not_full_a_share
```

The `2026-07-29` readback matched the known single-day result:

```text
data_as_of: 2026-07-29
result_id: regime-81b4cbb9645f95bee5885416
observed universe: 3191
strategic / tactical: bear / risk_off
status / score: degraded / -36.4379
confidence: low / 0.0
full-A capability: false
```

Every dataset file and the control DB were hashed before and after both rounds:

```text
control DB bytes: 3158016
control DB SHA-256 before/after:
  7841558f400338d5f70cc5fb915ef671deff09bb57c3cf33544a4c405f3c58c2

dataset files: 265/265 unchanged
dataset bytes: 39207949
dataset path+file-hash tree SHA-256 before/after:
  df0506ff04769071320e1c2dd322b39f8b5f84ceba9bc7ef7081d7d5b8637eaa
```

This is a deterministic replay of the observed main-board/two-index local dataset, not
full-A acceptance or a Release 1 GO. It does not include R1-C leadership implementation,
user data, or a user-judgement comparison.

## Pending real and Release 1 gates

- The 20-session local published-data replay above is verified, but no full-A coverage
  artifact exists.
- R1-A now has a ready classification generation, but it has not been joined to the
  immutable R1-B price publication under a shared PIT contract. It is therefore an
  eligible-universe lead for a versioned R1-B v2 audit, not proof of current price
  coverage or permission to remove the narrow-scope disclaimer.
- Root must measure cold and warm installed-dataset/API latency and browser behavior.
  This worker supplied only synthetic call-count regression evidence and did not open
  the installed dataset, production database or browser.
- Snapshot persistence for one result per ready trading day remains an R1-E gate; this
  acceptance evaluated results in memory and did not persist regime rows.
- No authoritative point-in-time expected-universe denominator or board coverage audit is
  available yet; therefore no R1-B runtime result can claim full-A capability. Future
  integration requires an R1-A artifact and model/formula version upgrade.
- Real ChiNext, STAR and representative-index price coverage remains unverified in this
  worker.
- Leadership remains missing until R1-C supplies point-in-time sector persistence and leader
  diffusion.
- Fund-flow evidence remains missing; price-volume turnover is not labeled as net flow.
- R1-A live classification publication/coverage gates remain pending per its acceptance
  record.
- R1-C, R1-D browser drill-down and independent follow-up review remain pending.

These gates prevent an R1-B review GO or Release 1 GO claim.
