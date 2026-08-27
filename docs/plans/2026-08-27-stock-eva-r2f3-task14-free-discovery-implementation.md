# Stock EVA R2-F3 Task14 Free Discovery Implementation Plan

**Status:** `OFFLINE IMPLEMENTATION APPROVED / REAL FREE CANARY NOT AUTHORIZED`
**Specification:** [Task14 Free discovery design](2026-08-27-stock-eva-r2f3-task14-free-discovery-design.md)

## Delivery boundary

This revision makes `FREE_DAILY_DISCOVERY` the default TickFlow discovery capability. It never
reads a credential, never enters the authenticated service, never persists evidence or control
state, and never starts shadow qualification. `AUTHENTICATED_DISCOVERY` remains an explicit,
separate legacy route and is the only route that may require `STOCK_EVA_TICKFLOW_TOKEN`.

The official pinned SDK is used only to initialize and validate the official `TickFlow.free()`
configuration without an SDK network call. All four Free HTTP requests use the existing bounded,
streaming transport because the SDK does not expose the receive-level byte and request bounds
required by the approved canary contract.

## RED sequence

Add a dedicated offline Task14 Free test module before production changes. RED must prove the
current code lacks the following contracts:

1. Free is credentialless even when `TICKFLOW_API_KEY` exists, initializes the pinned official
   Free factory silently with zero SDK retries, makes no SDK request and closes once.
2. The Free plan is exactly four ordered calls: exchanges, fixed five-symbol instruments,
   `CN_Equity_A`, and fixed five-symbol historical `1d` batch K-line.
3. The bounded HTTP path uses the exact Free origin, one attempt, four-request maximum, no auth
   headers and strict source parsing.
4. Success and every ordinal failure are zero-write and retain frozen capability states; Daily
   Bar and adjustment-factor qualification remain false and shadow never starts.
5. CLI defaults TickFlow to `free-daily`; only explicit `authenticated` may reach the existing
   credential-gated runner.

The expected RED command is:

```text
.venv/bin/pytest -q tests/test_market_provider_tickflow_free_task14.py
```

No RED or GREEN command may perform a real provider request.

## GREEN implementation map

1. `pyproject.toml` and `uv.lock`: pin exactly `tickflow==0.1.24`.
2. `providers/tickflow.py`: add the closed Free contract, fixed sample, strict source parsers,
   frozen capability report, official SDK Free initializer probe and zero-write runner. Preserve
   the authenticated adapter/runner as the explicit legacy capability.
3. `providers/http.py`: add a fixed `https://free-api.tickflow.org` client factory while reusing
   the existing bounded streaming transport and sanitized error vocabulary.
4. `cli.py`: add `--capability free-daily|authenticated`, default to Free, and keep plan/execute
   authorization boundaries explicit.
5. Tests: use only fakes/spies. Assert no token lookup, no auth headers, no retry/sleep, no payload
   or SDK notice in output, exact close ownership, exact request count and zero persistence.

## Verification gates

Run, in order:

1. Focused Free tests.
2. Existing Task14 TickFlow and provider compatibility tests.
3. Related provider registry, evidence, lifecycle and CLI tests.
4. Full offline `pytest`, LaunchAgent asset tests, `ruff check`, `ruff format --check`,
   `compileall`, strict spec validator and `git diff --check`.
5. Confirm R2-F2 golden fixture bytes/hashes are unchanged.
6. One independent read-only review of the exact implementation commit.

Only an offline review `GO` permits requesting a new, single-use Free discovery authorization.
That later canary remains `max_attempts=1`, zero-write, and cannot start shadow automatically.

## Explicit non-changes

Do not alter BaoStock normalization, Quality Gate, immutable Parquet, SHA-256, manifest, atomic
publish, canonical pointers, Tasks10-13 lifecycle, Tushare behavior, scheduler, LaunchAgents, NAS,
production DB, shadow qualification or failover.
