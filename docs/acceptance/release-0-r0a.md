# Release 0 / R0-A acceptance record

Date: 2026-07-29

Scope: backend security-analysis contract only. This record does not mark the
whole Release 0 as accepted; the stock cockpit and end-to-end browser acceptance
remain separate gates.

## Accepted contract

- `GET /api/v1/securities/{symbol}/analysis?start=YYYY-MM-DD&end=YYYY-MM-DD`
- Forward-adjusted (`qfq`) price series with adjustment/source/as-of metadata
- MA5/10/20/60/120/250
- MACD 12/26/9
- RSI14
- Versioned formula metadata
- Explicit `no_market_data` and `no_effective_trading_data` empty states
- Fail-closed quality validation for invalid prices, factors, volume, and amount
- Existing history API compatibility retained for raw-empty and all-suspended
  ranges

## Automated verification

Run from the main checkout after integrating the reviewed implementation:

```text
uv sync --extra dev
uv run --extra dev pytest
303 passed in 32.35s

uv run --extra dev ruff check backend tests
All checks passed!

uv lock --check
git diff --check
```

The implementation passed two code-quality review/fix cycles and a final
specification re-review before integration.

## Real-data verification

The API was started against the installed local BaoStock dataset while all
control and user-data paths were redirected to temporary directories.

Request:

```text
/api/v1/securities/sh.600000/analysis?start=2025-07-01&end=2026-07-28
```

Observed result:

```text
status: ready
source: baostock
price_adjustment: qfq
as_of_date: 2026-07-28
formula_version: ta-lib-0.7.0-r0-v1
quality_issues: []
series_count: 262
```

The first records correctly contain warm-up nulls. The last record contains
computed MA250, MACD, and RSI14 values.

Twenty-five consecutive real-data requests produced a nearest-rank P95 latency
of 0.077880 seconds, within the Release 0 target of 500 milliseconds.

## Remaining Release 0 gates

- Build and integrate the modular stock cockpit
- Validate holdings/watchlist entry in at most two interactions
- Validate K-line, volume, MA, MACD, and RSI rendering without frontend
  calculation
- Validate loading, empty, quality-error, narrow-screen, keyboard, and
  reduced-motion behavior
- Re-run the complete test suite from the integrated main branch
- Install the accepted build and perform browser acceptance against the live
  local service
