# Release 0 acceptance

Date: 2026-07-29 (Asia/Shanghai)

Verdict: **GO for deployment**

Release 0 now supports an end-to-end, after-close technical review from a
manual holding or watchlist item to a real-data stock cockpit. This verdict
does not claim the market-regime, sector, fund-flow, portfolio-risk, or
backtest capabilities assigned to later releases.

## Reviewed implementation

Integrated implementation:

```text
5023a14 feat(analysis): add versioned security indicators
22b611e fix(analysis): enforce market data quality gates
be07ead fix(analysis): tighten price and empty-series contracts
fa14baa feat(workspace): add security analysis cockpit
72c2912 fix(workspace): add API-backed indicator panes
afc8208 fix(workspace): guard cockpit navigation races
```

The backend package passed two specification/quality review-fix cycles. The
workspace package passed a specification review-fix cycle and two code-quality
review-fix cycles. The final independent code-quality review reported no
Critical, Important, or Minor findings.

## Fresh integrated verification

Run from the main checkout after integration:

```text
cd workspace
npm ci
npm test
# 8 files passed; 25 tests passed

npm run build
# Vite 7.3.6; security-cockpit.js 349.80 kB, gzip 76.09 kB

npm audit --omit=dev
# found 0 vulnerabilities

cd ..
uv run --extra dev pytest
# 307 passed in 33.46s

uv run --extra dev ruff check backend tests
# All checks passed

uv lock --check
git diff --check
```

The full development dependency tree reports one low-severity development-only
issue. It is not present in the production dependency audit and is stripped
from the installed runtime.

## Performance

The browser and API used the installed local market dataset generation
`generation-75bea1b431b04c1da4c9300a2f2f2c71`. The 260-session window was
2025-07-04 through 2026-07-29.

Thirty consecutive cached requests for `sh.600000` produced:

```text
minimum: 0.062414 s
median:  0.063390 s
P95:     0.070088 s
maximum: 0.079123 s
```

The nearest-rank P95 is below the Release 0 target of 500 milliseconds.

## Five-security API/browser comparison

Each raw API response was `ready`, contained 260 effective sessions, used
`baostock` / `qfq` / `ta-lib-0.7.0-r0-v1`, had no quality issues, and ended on
2026-07-29. The browser metadata, latest table row, and indicator cards matched
the API values at the displayed four-decimal precision.

| Symbol | Close | MA5 | MA20 | MA250 | MACD | Signal | Hist | RSI14 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `sh.600000` | 9.28 | 9.1220 | 8.8137 | 10.5425 | 0.1303 | 0.0754 | 0.0549 | 67.8394 |
| `sz.000001` | 11.28 | 11.1540 | 10.7635 | 10.9544 | 0.1681 | 0.1048 | 0.0633 | 68.4015 |
| `sh.600519` | 1321.00 | 1303.9840 | 1251.4485 | 1360.1658 | 23.7342 | 14.4581 | 9.2761 | 64.8722 |
| `sz.000858` | 75.17 | 74.4560 | 72.5254 | 100.8794 | -0.0210 | -0.6250 | 0.6039 | 54.4078 |
| `sh.601318` | 54.30 | 54.0460 | 51.3830 | 57.0858 | 0.8952 | 0.4210 | 0.4742 | 63.2952 |

## Browser acceptance

The acceptance browser loaded the integrated `workspace/index.html`,
`workspace/app.js`, and generated cockpit bundle from
`http://127.0.0.1:18080/workspace/`. A temporary API occupied port 8000 during
the bounded run, read the real local market dataset, and used a newly created
temporary user database. It did not open or modify the production user
database. The original API LaunchAgent was restored after the run and all five
LaunchAgents returned to ready state.

Observed behavior:

- The actual holding row contained an accessible
  `查看 sh.600000 个股技术分析` button. One click opened
  `#security/sh.600000?from=portfolio`.
- The actual watchlist contained an accessible
  `查看 sz.000001 个股技术分析` button. Keyboard Enter opened
  `#security/sz.000001?from=watchlists`; Space was also exercised.
- The focused watchlist button matched `:focus-visible` and showed a
  three-pixel `rgb(156, 197, 255)` outline with a three-pixel offset.
- Reloading a security hash restored the cockpit without a source-page click.
- The header showed the real symbol, `as_of` date, source, adjustment, formula
  version, status, and quality state.
- The ready chart created 18 canvases across candle/MA (229 px), volume
  (100 px), MACD (100 px), RSI14 (100 px), and the time axis (26 px).
- The accessible alternative contained the last 20 sessions and retained
  warm-up nulls as `—`, never zero.
- Real empty symbol `sh.999999` displayed `no_market_data`, `as_of=—`, and
  created zero chart elements, canvases, or alternative-table rows.
- The fail-closed HTTP 409 path was separately browser-verified with a
  deterministic synthetic quality error in
  `docs/acceptance/release-0-r0b-worker.md`; it created no chart or fabricated
  point.
- At 1280×720, body and document client/scroll widths were all 1265 px.
- At 390×844, body and document client/scroll widths were all 375 px, the
  chart was 309 px wide, metadata collapsed to one column, and the wide
  numerical table remained inside its own horizontal scroll container.
- Browser console warnings/errors remained empty after holding navigation,
  watchlist navigation, keyboard activation, hash reload, five-security
  comparison, empty-state rendering, and viewport changes.
- Reduced-motion CSS, a descriptive chart accessible name, and the numerical
  table alternative were present.

The temporary synthetic user fixture was moved to the macOS Trash after
acceptance and is recoverable until the Trash is emptied.

## Product acceptance

Release 0 gives the user enough truthful information to answer the scoped
technical questions:

- trend: forward-adjusted candles and MA5/10/20/60/120/250;
- momentum: API-backed MACD and RSI14 series plus latest values;
- volatility: daily candle range and price structure;
- price/volume confirmation: aligned candle and real-volume panes.

The visual hierarchy separates provenance, the primary chart, momentum panes,
and the numerical alternative. It is materially more useful than the baseline
empty chart frame while preserving the existing market, portfolio, strategy,
watchlist, and alert workflows.

Release 1 remains responsible for market regime, sector structure, and
leadership context. Release 2 remains responsible for fund-flow evidence and
portfolio risk. Release 3 remains responsible for replay, backtesting, and
strategy lifecycle.
