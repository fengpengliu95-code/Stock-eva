# Release 1 R1-B acceptance

Date: 2026-07-30 (Asia/Shanghai)

R1-B review remediation: **LOCALLY VERIFIED — RE-REVIEW REQUIRED**

Independent review: **SECOND CHANGES REQUIRED received; follow-up not yet re-reviewed**

Mainline fresh-suite isolation follow-up: **LOCALLY VERIFIED**

Release 1 verdict: **PENDING — do not claim GO**

## Scope verified in this worker

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
# Focused GREEN: 43 passed

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

## Pending real and Release 1 gates

- No real market dataset was opened in this worker.
- The required at-least-20-session historical replay on real published data is pending R1-E.
- No live proof yet exists for one replayable result per ready trading day.
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
