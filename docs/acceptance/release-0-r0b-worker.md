# Release 0 R0-B worker acceptance

- Date: 2026-07-29 (Asia/Shanghai)
- Branch baseline: `codex/r0-frontend` at `0271a0a`
- Code-review follow-up baseline: `b0e159b`
- Scope: R0-B synthetic worker acceptance only; this is not Release 0 completion.

## TDD evidence

The chart adapter test was changed before implementation:

```text
cd workspace
npm test -- --run src/charts/stock-cockpit.test.ts

FAIL ... registers four API-backed indicators ...
expected "spy" to be called 4 times, but got 2 times
```

After registering API-backed MACD and RSI14 templates and creating each
sub-indicator separately:

```text
PASS src/charts/stock-cockpit.test.ts (2 tests)
```

The accessible chart summary also failed before its implementation:

```text
npm test -- --run src/views/security-analysis.test.ts

FAIL ... expected "...MA5、10、20、60、120、250 图表"
to contain "MACD、RSI14"
```

The explicit keyboard delegation test failed before the minimal handler was
added:

```text
npm test -- --run src/navigation.test.ts

FAIL ... expected "spy" to be called 5 times, but got 3 times
```

The handler accepts only Enter/Space on a valid `data-security-symbol`
control, rejects repeats, calls `preventDefault()` before navigation, and the
test asserts the call count after every click/key action. This proves one
navigation per activation rather than a handler plus synthesized-click double
navigation.

Focused GREEN:

```text
npm test -- --run \
  src/assembled-workspace.test.ts \
  src/views/security-analysis.test.ts \
  src/charts/stock-cockpit.test.ts

Test Files  3 passed (3)
Tests       8 passed (8)
```

The assembled-page test parses the production `workspace/index.html`, executes
the production legacy `app.js` and the module source used to build the
production bundle, supplies synthetic fetch responses, and exercises the
actual legacy holding and watchlist controls.

## Browser harness

The browser run used a temporary Python server on
`http://127.0.0.1:18080/workspace/`. It served the checked-out production
`workspace/index.html`, `workspace/app.js`, `workspace/style.css`, and generated
`workspace/assets/security-cockpit.js`. The server injected one same-origin
script tag immediately before `app.js`; that script intercepted only requests
whose origin was `http://127.0.0.1:8000` and returned deterministic synthetic
responses.

The harness did not open a user database, did not call the live API, and did
not stop or modify the existing 8000/8080 services. The temporary harness file
was deleted after the browser run.

Representative command and URLs:

```text
python3 tmp/r0b_browser_harness.py

http://127.0.0.1:18080/workspace/?scenario=ready#portfolio
http://127.0.0.1:18080/workspace/?scenario=empty#security/sh.600000?from=portfolio
http://127.0.0.1:18080/workspace/?scenario=quality#security/sh.600000?from=portfolio
```

## Exact browser observations

- The legacy portfolio renderer produced one button named
  `查看 sh.600000 个股技术分析` with
  `data-security-symbol="sh.600000"` and source `portfolio`. Clicking that
  actual button changed the hash to
  `#security/sh.600000?from=portfolio`.
- Browser Back restored `#portfolio` and the real portfolio region, including
  the same holding row.
- The legacy watchlist renderer produced one button named
  `查看 sz.000001 个股技术分析` with source `watchlists`. Keyboard focus on
  that control had a visible `3px solid rgb(156, 197, 255)` outline and
  `3px` offset. `press("Enter")` changed the hash once to
  `#security/sz.000001?from=watchlists`.
- Reloading that hash restored the `sz.000001 技术驾驶舱` and 18 chart
  canvases without another source-view click.
- The request trace read `/api/v1/market/history/dates` before
  `/api/v1/securities/sh.600000/analysis?start=2024-01-02&end=2024-09-17`.
  The synthetic calendar contained exactly 260 dates, so the observed
  start/end were the first and last backend-supplied dates.
- Ready rendered four visually separate KLineChart panes: candle plus API MA
  (229 CSS px), volume (100 px), API MACD (100 px), and API RSI14 (100 px),
  followed by the 26 px time axis. The chart contained 18 canvases. The
  rendered indicator legends were `API MA`, `成交量`, `API MACD`, and
  `API RSI14`; MACD displayed MACD/Signal/Hist and RSI displayed RSI14.
- The accessible chart name included candle, volume, all six MA periods,
  MACD, and RSI14. The numerical alternative table was present with 20 body
  rows and columns through MACD, Signal, Hist, and RSI14.
- The `empty/no_market_data` scenario displayed `没有可用行情`, with zero
  `#security-chart` elements, zero canvases, and zero alternative-table rows.
- The HTTP 409 scenario displayed `数据质量门禁` and
  `synthetic missing adjust_factor`, with zero chart elements, zero canvases,
  and zero alternative-table rows.
- Desktop viewport: `innerWidth=1280`; body/document client and scroll widths
  were both 1265, so there was no body/document horizontal overflow.
- Mobile viewport: `390×844`; body/document client and scroll widths were both
  375, so there was no body/document horizontal overflow. The chart retained
  all 18 canvases and the alternative table stayed in its own
  `overflow-x: auto` container.
- The loaded CSSOM contained
  `@media (prefers-reduced-motion: reduce)` with automatic scroll behavior and
  `0.01ms` transition/animation durations.
- Console warnings/errors were `[]` after initial load, holding navigation,
  browser Back, watchlist navigation, keyboard activation, hash reload,
  ready/empty/409 scenario changes, and the 390×844 viewport change.

## Code-review follow-up

The malformed-route test was added before the parser change. Four cases
(`%`, truncated escape, non-hex escape, and truncated UTF-8) failed with
`URIError: URI malformed`. After the minimal `URIError` guard, all return
`null`; exceptions outside `decodeURIComponent` are not swallowed.

The deterministic request-ownership test supplies fetch promises that ignore
AbortSignal:

```text
open A -> open B -> resolve B -> resolve A

RED: expected rendered symbol sz.000001, received sh.600000
GREEN: hash, status, and rendered metadata remain sz.000001
```

The same test repeats the sequence with stale A rejecting after B. Neither the
old success nor the old failure can update the B state. It also verifies that
disposing the security module removes delegated controls and history
listeners, aborts the current request, clears chart state, and permits one
clean reinitialization.

For history navigation, the test changes from security A to security B and
then dispatches the browser's `popstate` plus `hashchange` pair. With the hash
identity check temporarily removed, the regression test failed:

```text
expected 12 fetch calls, received 14
```

Restoring the check produces one dates+analysis request pair and the test
passes. Direct hash changes are still observed through `hashchange`; only the
second event for the already-restored hash is ignored.

The Enter/Space test now directly asserts `defaultPrevented=true`, one
navigation per key action, and `repeat=true` neither prevents the event nor
navigates.

### Follow-up assembled browser race

A temporary server again served the actual production `workspace/index.html`,
legacy `app.js`, and rebuilt `assets/security-cockpit.js`. It exposed the real
legacy holding and watchlist controls together and intercepted only local API
requests. The synthetic analysis transport ignored abort and automatically
released A only after B completed. The observed trace was:

```text
requested:sh.600000
requested:sz.000001
resolved:sz.000001
auto-release-a
resolved:sh.600000
```

After the final line, the URL remained
`#security/sz.000001?from=watchlists`, the rendered metadata remained
`sz.000001`, status remained `sz.000001 无可绘制数据`, and browser
warning/error logs were `[]`. The 18080 harness was stopped and deleted; live
8000/8080 services and production user data were not touched.

## Final command verification

```text
cd workspace
npm ci
# added 113 packages; full dev tree reports 1 low-severity development issue

npm test
# 8 files passed; 25 tests passed

npm run build
# Vite 7.3.6; 7 modules; security-cockpit.js 349.80 kB

npm audit --omit=dev
# found 0 vulnerabilities

cd ..
uv run --extra dev pytest tests/test_workspace_static.py tests/test_launchagent_assets.py
# 30 passed

uv run --extra dev pytest
# 307 passed

uv run --extra dev ruff check backend tests
# All checks passed
```

## Limitations and integration handoff

This run intentionally used synthetic responses and proves R0-B browser
assembly and state behavior, not production market-data correctness. The
in-app browser's low-level keyboard API did not synthesize the native button
click in its initial unhandled form; the final run therefore exercised the
reviewed Enter/Space delegation described above, while Vitest also retains the
native `user-event` Enter/Space behavior test. R0-C integration must repeat the
browser acceptance against the installed runtime and real backend data before
Release 0 can be accepted.
