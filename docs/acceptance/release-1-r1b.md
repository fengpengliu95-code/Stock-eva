# Release 1 R1-B acceptance

Date: 2026-07-29 (Asia/Shanghai)

R1-B review remediation: **LOCALLY VERIFIED — RE-REVIEW REQUIRED**

Independent review: **CHANGES REQUIRED received; follow-up not yet re-reviewed**

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
- Symbol presence never supplies a stock-universe denominator. Without an authoritative,
  point-in-time expected-universe audit, board/full-market coverage ratios stay null and
  every result remains narrow provisional, low confidence and unsuitable for a full
  A-share bull/bear conclusion—even when all four board labels and all six index symbols
  are observed.
- The six-series index ratio reports only representative-index presence; it does not prove
  stock-universe coverage.
- Store reads are bounded by `as_of`; a defensive second boundary discards any future row.
- Risk requires separate warmups: 21 valid index sessions for 20-day volatility and 60
  valid sessions for 60-day drawdown. A shorter window is never labeled as 60-day.
- Non-finite `pct_change` uses a finite close/preclose fallback when possible; otherwise
  the row is excluded and the component/result records a concrete quality issue.
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
```

## Verification

Fresh final verification is recorded before the atomic commit:

```text
```text
uv run --extra dev pytest -o addopts='' -q tests/test_market_regime.py
# 36 passed in 0.46s

uv run --extra dev pytest
# 480 passed in 58.44s

uv run --extra dev ruff check backend tests
# All checks passed!

git diff --check
# no output
```
```

## Pending real and Release 1 gates

- No real market dataset was opened in this worker.
- The required at-least-20-session historical replay on real published data is pending R1-E.
- No live proof yet exists for one replayable result per ready trading day.
- No authoritative point-in-time expected-universe denominator or board coverage audit is
  available yet; therefore no runtime result can claim full-A capability.
- Real ChiNext, STAR and representative-index price coverage remains unverified in this
  worker.
- Leadership remains missing until R1-C supplies point-in-time sector persistence and leader
  diffusion.
- Fund-flow evidence remains missing; price-volume turnover is not labeled as net flow.
- R1-A live classification publication/coverage gates remain pending per its acceptance
  record.
- R1-C, R1-D browser drill-down and independent follow-up review remain pending.

These gates prevent an R1-B review GO or Release 1 GO claim.
