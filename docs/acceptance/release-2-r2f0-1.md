# Release 2 — R2-F0.1 Provider Transport Stabilization

**Date:** 2026-08-13 (Asia/Shanghai)

**Verdict:** OFFLINE CODE GO

**Exact reviewed code HEAD:** `b0b643fd78b1b0be27279cbd3380268577408c85`

**Production verdict:** NO-GO — installation, real canary and production execution are not
authorized

## Scope and decision boundary

R2-F0.1 makes the incumbent BaoStock transport path observable, bounded and fail-closed without
changing the canonical publication contract. The exact code HEAD above passes the offline code
gate. This verdict is not an install GO, real-provider GO, production GO, R2-F0 incident closure or
Release 2 GO.

This record is governed by the
[approved design](../plans/2026-08-12-stock-eva-r2f0-1-provider-transport-design.md),
[implementation plan](../plans/2026-08-12-stock-eva-r2f0-1-provider-transport-implementation.md),
[R2-F roadmap](../plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md) and the still-open
[R2-F0 production incident record](release-2-r2f0.md).

Task 7 was documentation-only. It made no network request and did not install, launch, stop or
modify a runtime. The test results below are the frozen pre-Task-7 evidence for the exact reviewed
code HEAD.

## Specification evidence

| Requirement | Result | Offline evidence |
| --- | --- | --- |
| Six market endpoints | PASS | The market vocabulary is exactly `trade_dates`, `all_stock`, `daily_astock`, `daily_factor`, `adjust_factor`, `index_history`. Classification uses a separate typed vocabulary and cannot enter the market-provider aggregate. |
| Request identity | PASS | Each external operation owns a stable `refresh_id`; each login/relogin owns a `provider_session_id`; each request/page owns a `request_id`, `attempt` and `page`. Login, request, retry and pagination tests verify scope stability and independence. |
| Pinned upstream | PASS | `baostock==0.9.3` is pinned. The project patch refuses use unless upstream `socketutil.py` SHA-256 is `248591168ad087fb9c91b64e8c909608082528ecbecf25541dcbce0fe9cfcd25`. `site-packages` is not edited. |
| Checked send/receive | PASS | The project patch uses `sendall()`, a bounded framed receive loop and strict header, body-length, compression and end-marker checks. Partial send, connect/send error, timeout, EOF, short header, missing marker, bad compression and malformed frame tests fail closed. |
| Socket receive metrics | PASS | Observations carry actual socket-loop `recv_calls`, `response_bytes` and `end_marker_seen`; pagination and later operation failures remain distinguishable from a prior successful frame. |
| Sanitized audit contract | PASS | Audit fields are limited to `refresh_id`, `provider_session_id`, `request_id`, `provider_id`, `endpoint`, `attempt`, `page`, `protocol_stage`, `elapsed_ms`, `recv_calls`, `response_bytes`, `end_marker_seen`, `provider_code`, `normalized_error`, `outcome`, `observed_at`. SQLite columns exclude payload, token, URL, raw message and exception fields. |
| Error taxonomy | PASS | Allowlisted normalized errors are `CONNECT_ERROR`, `SEND_ERROR`, `RECV_TIMEOUT`, `EOF`, `SHORT_HEADER`, `BAD_COMPRESSION`, `PROTOCOL_ERROR`, `PAGINATION_STALLED`, `RATE_LIMIT`, `UNKNOWN_PROVIDER_PROTOCOL_ERROR`. Provider status uses code-only mapping; unknown safe codes retain the code and never infer meaning from message text. |
| Operation terminal evidence | PASS | Every completed market logical call has one final `OPERATION` outcome. Internal retry recovery records only final success; exhausted login/request, typed failure and unclassified `Exception` record one sanitized failure. `BaseException` interrupt behavior and classification isolation remain intact. |
| Pagination and candidate integrity | PASS | Stalled/repeated/non-advancing pages, a full page without terminal continuation, page transport failure, duplicate symbols/points, wrong symbol, factor mismatch and universe/row-count divergence invalidate the complete candidate. Collected rows are not returned as a successful partial candidate. |
| Persistent circuit breaker | PASS | SQLite and in-memory stores share the endpoint contract. One terminal endpoint failure per refresh drives `CLOSED`, `OPEN` and `HALF_OPEN`; state, cooldown and leases survive restart. Provider health aggregates only the six market endpoints. |
| Global HALF_OPEN probe | PASS | Only one provider-wide probe lease may exist, including concurrent stores and different endpoints. Cooldown and lease expiry are reclaimable; only the owner can resolve. Ordinary late outcomes cannot bypass an OPEN/HALF_OPEN circuit. |
| Health-aware automatic paths | PASS | Automatic market refresh and automatic calendar maintenance gate provider access on persistent health. OPEN slots make zero full-refresh calls/writes and use existing slots. Cooldown performs one endpoint-specific, one-attempt, independent-session probe; success waits for the next scheduled slot. |
| Calendar evidence | PASS | Calendar success requires `provider_succeeded=true`, no observation error, one session, all attempts equal to one and exactly one successful OPERATION. Schema/date parsing after transport success trips the breaker and leaves calendar control bytes/state/runs unchanged. |
| Diagnostic canary | PASS | Default and partially confirmed CLI forms construct no provider. Explicit execution requires two confirmations, explicit date/stock/index symbols, `max_attempts=1` and six independent endpoint sessions. Observations stay in memory and no refresh is triggered. |
| Canary write boundary | PASS | Static no-write paths plus before/after fingerprints cover SQLite, Parquet, manifest, pointer, factor-cache and refresh-run sentinels. Results always state database/Parquet/manifest/pointer writes and refresh triggering as false. |
| Canonical chain unchanged | PASS | No timeout/retry increase, symbol-level stitching, Normalize/Quality Gate relaxation, immutable Parquet rewrite, manifest/SHA-256 change or pointer-promotion bypass was added. The chain remains `Normalize -> Quality Gate -> Immutable Parquet -> SHA-256 -> Manifest -> Atomic Publish`. |

Normal market attempts remain `max_attempts=2` with the existing 30-second deadline. Diagnostic
probes use one attempt. No new retry slot or automatic same-slot full refresh was added.

## Commit history

| Commit | Delivered boundary |
| --- | --- |
| `38cd048` | Typed transport contracts, six endpoints, scopes and BaoStock 0.9.3 pin. |
| `f1a4b68` | Version/SHA-checked `sendall()` and bounded framed receive patch. |
| `954a5eb` | Endpoint tagging and fail-closed pagination/candidate integrity. |
| `5307645` | Provider protocol review gaps, deadline observations and classification isolation. |
| `fd2011a` | Persistent sanitized audit and provider/endpoint circuit breaker. |
| `40fdd3d` | Health-aware scheduler and endpoint probe integration. |
| `577421c` | Explicit in-memory, write-free provider canary. |
| `23f8d1c` | Provider circuit, operation aggregation, calendar gate and symbol-role review closure. |
| `50df62c` | Complete login/request operation evidence and strict probe/canary validation. |
| `3db5d64` | Unclassified provider error fail-closed behavior and status-code preservation. |
| `2e289d1` | Calendar breaker outcome after post-transport parsing failure. |
| `b0b643f` | Strict calendar transport evidence and zero-write high-level failure boundary. |

Task 7 closes documentation around exact code HEAD `b0b643f…`; its documentation commit does not
change the code verdict subject.

## Verification record

The frozen commands are shown for reproducibility; Task 7 did not execute them or any provider
command.

```text
uv run pytest -q <focused transport/provider/health/automation/canary/calendar/reliability files>
uv run pytest -q
uv run ruff check <changed files>
uv run ruff format --check <changed files>
git diff --check
```

| Gate | Result |
| --- | --- |
| Associated offline regression selection | PASS — exactly 493 tests |
| Full repository | PASS — exactly 1,099 tests collected and passed |
| Ruff check and changed-file format check | PASS |
| `git diff --check` | PASS |
| Independent sixth-round code/spec review | PASS — High 0, Medium 0 |
| Sixth-round focused confirmation | PASS — 59 tests, twice independently |

### Transparent intermittent-test note

An earlier full-suite run reported one failure in
`tests/test_market_regime.py::test_store_never_returns_cached_input_when_immutable_preflight_fails`:
the fixture expected one reader call and observed zero. The test uses a process-local cache identity
derived from `id(self)`, so an identity reuse/cache collision was suspected, not proven. The test
passed immediately in isolation, the subsequent complete suites passed, and the independent
review's two 59-test runs both passed. No market-regime code or test was changed or suppressed.

## Known Low boundaries

- Manual `refresh` and backfill commands remain explicit operator overrides; they are not silently
  converted into automatic breaker-controlled workflows. Automatic scheduler and calendar paths
  are health-gated. Any operator override policy change requires a separate review.
- Canary filesystem fingerprints prove equality of the before/after snapshots together with a
  static no-write code path. They are not an operating-system-level proof that no transient write
  could ever occur between snapshots.

## Production and mutation boundary

No real provider request or real canary was performed for this acceptance. No production runtime,
control database, user database, factor cache, Parquet object, manifest, pointer or NAS path was
read or written by Task 7. No LaunchAgent or installed release was changed.

The production refresh LaunchAgent remains recorded as safely frozen/unloaded from the prior
incident response. This is a documentation statement only; Task 7 did not read back or change that
state. Restoring it, installing this candidate, running the real six-endpoint canary or attempting
a production repair each requires separate user approval, a fresh live pre-state and a separately
reviewed bounded procedure.

R2-F0 production incident closure remains NO-GO. The previous trusted runtime and canonical
pointer remain the authority until a separately approved production gate succeeds.

## Authorized next sequence

No next implementation task starts automatically from this acceptance.

1. After explicit user confirmation, implement **Gap Scanner + Health-aware Repair Queue** while
   preserving separate freshness/repair lanes and all publication gates.
2. Then introduce the **Multi-provider RAW Evidence Framework** so provider-shaped evidence is
   immutable and replayable before selection.
3. Evaluate a legally and operationally approved second source only in whole-session shadow mode.
   Qualification requires at least 20 consecutive trading sessions.
4. Never combine providers by symbol within a session candidate.
5. Keep automatic failover disabled through shadow and qualification. Manual failover design and
   execution require a later explicit approval before any automatic policy can be considered.

## Final decision

**OFFLINE CODE GO** applies only to exact code HEAD
`b0b643fd78b1b0be27279cbd3380268577408c85`.

**REAL PROVIDER CANARY NOT RUN. INSTALL NOT AUTHORIZED. PRODUCTION NO-GO. R2-F0 INCIDENT OPEN.**
