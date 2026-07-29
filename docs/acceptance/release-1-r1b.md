# Release 1 R1-B acceptance

Date: 2026-07-29 (Asia/Shanghai)

R1-B synthetic contract: **VERIFIED**

Independent review: **PENDING**

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
- Expected and observed boards/index series, their missing sets, coverage ratios and
  conclusion capability are explicit.
- Main-board plus two-index history is labeled narrow provisional, low confidence and
  unsuitable for a full A-share bull/bear conclusion.
- Store reads are bounded by `as_of`; a defensive second boundary discards any future row.
- HTTP rejects a future Shanghai `as_of` with 422 before store read.
- A missing local market database returns empty and creates no database, directory,
  temp state or schema.
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
```

## Verification

Fresh final verification is recorded before the atomic commit:

```text
uv run --extra dev pytest tests/test_market_regime.py -q
# 19 passed

uv run --extra dev pytest
# 463 passed

uv run --extra dev ruff check backend tests
# All checks passed!

git diff --check
# no output
```

## Pending real and Release 1 gates

- No real market dataset was opened in this worker.
- The required at-least-20-session historical replay on real published data is pending R1-E.
- No live proof yet exists for one replayable result per ready trading day.
- Current published price history remains main-board plus two representative indexes; real
  ChiNext, STAR and broader representative-index price coverage is pending.
- Leadership remains missing until R1-C supplies point-in-time sector persistence and leader
  diffusion.
- Fund-flow evidence remains missing; price-volume turnover is not labeled as net flow.
- R1-A live classification publication/coverage gates remain pending per its acceptance
  record.
- R1-C, R1-D browser drill-down and independent specification/code-quality review remain
  pending.

These gates prevent an R1-B review GO or Release 1 GO claim.
