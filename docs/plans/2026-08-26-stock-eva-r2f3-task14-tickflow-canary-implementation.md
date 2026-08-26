# Stock EVA R2-F3 Task14 TickFlow Canary Implementation

**Status:** `RUNNER CODE GO / REAL CANARY AUTHORIZED-PENDING-EXECUTION` at
`81c68471f998952faa62662b17a7ab42d354614e`; no real request has occurred yet.
**Specification:** [Task14 design](2026-08-26-stock-eva-r2f3-task14-tickflow-canary-design.md)

## RED then GREEN record

The Task14 tests are intentionally written against the four-request contract before the runner
changes. RED must show the old three-endpoint placeholder, retrying HTTP policy, and missing
execute gates. GREEN is the smallest implementation that makes those tests pass without changing
Tasks10–13 lifecycle semantics.

### Third-remediation hardening

The compatibility parser remains read-only while execute is unambiguously Task14-only: it
requires 5–10 unique main-board symbols and exactly four requests. Official factor coverage is
checked from response map keys (empty per-symbol arrays mean `no_event`), CN_Index cannot be
empty, and compact kline arrays use strict numeric types, including non-null optional values and
non-negative int64 volume. Shared CLI sanitizers expose only fixed failure classes and pinned
endpoint identities. Streaming responses and owned clients are idempotently closed, iterator
timeouts retain the typed timeout class, and raw-evidence publication errors—including
collisions—become fixed `evidence_publish_error` failures with four requests and no readable
failed bundle.

## Implementation map

1. `providers/tickflow.py`: pinned static contract, exact four logical requests, UTC bounds,
   strict source-shaped parser, representative symbol validation, strict
   `CN_Equity_A`/`CN_Index` universe detail checks, allowlisted optional kline fields, and
   discovery-only report.
2. `providers/http.py`: fixed one-attempt/no-retry Task14 policy, exact host/TLS/no-redirect
   transport boundary, sanitized typed failures, response/client close ownership, and allowlisted
   observations.
3. `shadow_evidence.py`: reuse immutable bundle-first writer; raw bytes are retained only under
   the isolated shadow evidence root.
4. `config.py`, `storage/layout.py`, `providers/registry.py`: exact Task14 gates and isolated
   root validation; no canonical path widening.
5. `cli.py`: plan remains zero-write; execute requires authorization plus acknowledgement and
   uses only the fixed `https://api.tickflow.org` client factory after all gates. Tests inject
   fake transport/client factories; post-client failures still preserve sanitized
   `failure_class`, safe endpoint identity and actual `provider_requests`. No real socket is
   used by the offline verification.

## Explicit non-changes

Do not modify BaoStock, Tushare semantics, Task10–13 registry migrations, canonical evidence,
candidate/selection, shadow scheduler, LaunchAgents, NAS, production DB, or dependencies.

## Verification contract

Run focused Task14/provider tests, related shadow/evidence/registry tests, full offline pytest,
`tests/test_launchagent_assets.py`, the repository's strict design validator at
`/Users/finlay/.codex/skills/claude-skills--engineering/spec-driven-workflow/scripts/spec_validator.py`,
`ruff check`, `ruff format --check`, `compileall`
and `git diff --check`. Verify R2-F2 golden fixture bytes/hashes remain unchanged. No command may
access network, credentials, NAS, production DB or LaunchAgent.
