# Stock EVA R2-F3 Task14 Free Discovery Implementation Plan

**Status:** `CANARY #1 FAIL-CLOSED / V2 OFFLINE REMEDIATION IN VERIFICATION / NEW CANARY NOT AUTHORIZED`
**Specification:** [Task14 Free discovery design](2026-08-27-stock-eva-r2f3-task14-free-discovery-design.md)

## Delivery boundary

This revision makes `FREE_DAILY_DISCOVERY` the default TickFlow discovery capability. It never
reads a credential, never enters the authenticated service, never persists evidence or control
state, and never starts shadow qualification. `AUTHENTICATED_DISCOVERY` remains an explicit,
separate legacy route and is the only route that may require `STOCK_EVA_TICKFLOW_TOKEN`.

Free execute uses a closed runtime-settings projection that reads only eight named,
non-credential process variables; it does not construct general `BaseSettings`, enumerate the
environment or read `.env`. The existing provider record remains the authenticated descriptor.
A separately reviewed TermsEvidence `contract_version` embeds the exact Free capability descriptor
SHA-256 and is required before either the SDK or HTTP client is constructed.

The official pinned SDK is used only to initialize and validate the official `TickFlow.free()`
configuration without an SDK network call. All four Free HTTP requests use the existing bounded,
streaming transport because the SDK does not expose the receive-level byte and request bounds
required by the approved canary contract. The production SDK probe runs in an isolated child with
a closed environment, and the bounded HTTPX client disables environment proxy lookup.

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
6. An authenticated-only/stale registry review blocks Free before client creation, while a Free
   descriptor does not replace or masquerade as the authenticated request graph.

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

## Completed offline gate

Implementation commit `af652e0` was independently rejected for two Medium contract defects: it
used authenticated registry hashes as proof of the Free graph, and its CLI Free path constructed
general settings that could enumerate credential names. Remediation commit `099cb1c` added the
separately hashed Free descriptor, a closed eight-name non-credential settings projection, an
isolated SDK child and an HTTP client with environment proxy lookup disabled.

The exact remediation commit passed independent read-only re-review with H=0 and M=0. The focused
Free suite passed 104 tests, the full offline suite passed 2,017 tests, and the strict design
validator scored 100/100 with zero warnings. No real provider request occurred. The next permitted
step is to request authorization for the isolated Free descriptor preflight and one zero-write
Free discovery canary; this approval does not authorize shadow qualification.

## Canary #1 result and V2 remediation

The user authorized one Free descriptor preflight plus one zero-write Free canary. The descriptor
was attached to the isolated registry with zero provider requests. The `2026-08-10` canary then
completed all four requests and returned `unavailable / symbol_schema`; all four discoverable
capabilities remained `UNKNOWN`, both qualification flags remained false and shadow did not start.
Pre/post hashes proved that the registry after preflight, TermsEvidence objects and canonical
manifest were unchanged by the canary; evidence and shadow roots remained empty.

Offline triage found two Medium V1 contract defects:

1. the full `CN_Equity_A` member list was forced through the fixed-sample SH/SZ regex although the
   retained OpenAPI defines universe members only as strings;
2. parser failures did not carry a fixed endpoint identity, so the zero-write report could not
   distinguish universe from Daily schema failure.

V2 RED/GREEN requirements are:

1. accept additional bounded printable universe members only as opaque metadata while requiring
   the fixed five symbols to be present;
2. retain exact fixed-five instrument and Daily response sets, with all existing date/numeric
   gates unchanged;
3. normalize transport and parser errors to one of the four fixed logical endpoint identities;
4. retain no offending symbol, URL, raw payload or provider exception;
5. bump the Free adapter/source/descriptor and TermsEvidence contract version so V1 review fails
   before any future client construction.

V2 code commit `0da87a708176f3f92cbcc79244db2061ac76aef5`, followed by the report-enum
documentation correction `d32b63c45cd5fa67cc50515a5c5e316061d72b11`, passed final independent
read-only review with H=0, M=0 and decision `GO`. The reviewer confirmed the Free report model,
CLI and design all use only `discovered | unavailable` status and `discovery | unavailable`
outcome. The worktree was clean and the review made no network or provider request.

The first authorization is consumed. The offline gate is complete, but no V2 descriptor attachment
or real request is permitted without a new explicit single-use authorization.

## Explicit non-changes

Do not alter BaoStock normalization, Quality Gate, immutable Parquet, SHA-256, manifest, atomic
publish, canonical pointers, Tasks10-13 lifecycle, Tushare behavior, scheduler, LaunchAgents, NAS,
production DB, shadow qualification or failover.
