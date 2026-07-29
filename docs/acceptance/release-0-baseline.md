# Release 0 Browser Baseline

Date: 2026-07-29

This record captures the deployed workspace before Roadmap Release 0 implementation. It is
not a Release 0 acceptance result.

## Runtime baseline

- Five LaunchAgents reported `loaded`.
- API readiness: `http://127.0.0.1:8000/api/v1/health` ready.
- Workspace readiness: `http://127.0.0.1:8080/workspace/` ready.
- Storage mode: `local_dataset`, status `ready`, serving source `local`.
- Workspace readback showed expected and published session `2026-07-28`.
- Baseline backend verification: `256 passed in 31.39s`.
- Baseline lint verification: `uv run --extra dev ruff check backend tests` returned
  `All checks passed!`.

## Visible product state

### Existing value

- The overview shows actual market breadth and two index close snapshots.
- Data date, calendar status and publication status are visible.
- The workspace explicitly avoids presenting turnover as capital inflow.
- Loading and empty messages avoid demo values.

### Release 0 gaps

- No security-detail route or stock cockpit exists.
- No K-line, volume, moving-average, MACD or RSI chart is rendered.
- The portfolio detail panel is a static empty region.
- Portfolio rows do not provide an interactive security-analysis entry.
- Watchlist items do not provide an interactive security-analysis entry.
- The deployed private store contained no position or watchlist fixture suitable for acceptance;
  Release 0 navigation testing must use an automatically destroyed temporary SQLite store rather
  than modify production user data.
- Sector and portfolio-history frames remain explicit empty states.

## Responsive baseline

At a requested narrow viewport of `390 × 844`:

- document content did not create body-level horizontal overflow;
- the primary navigation became a horizontal strip and truncated labels within the viewport;
- the active page and local data state remained readable;
- no stock cockpit existed to evaluate chart responsiveness.

## Release 0 comparison path

The post-implementation browser acceptance must repeat:

1. Overview publication-state readback.
2. Holding → security cockpit in at most two interactions using temporary user data.
3. Watchlist → security cockpit in at most two interactions using temporary user data.
4. Real K-line, volume, MA, MACD and RSI values compared with the security-analysis API.
5. Loading, empty and API-error states with no fabricated series.
6. Desktop and `390 × 844` layouts.
7. Keyboard navigation and visible focus.
8. Console error inspection.
