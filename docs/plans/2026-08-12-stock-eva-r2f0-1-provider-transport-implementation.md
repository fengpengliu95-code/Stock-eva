# Stock EVA R2-F0.1 Provider Transport Stabilization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a pinned, observable, fail-closed BaoStock transport with endpoint/provider circuit breaking and a write-free diagnostic canary, entirely under offline test.

**Architecture:** A project-owned patch wraps the pinned BaoStock 0.9.3 socket boundary and emits sanitized request observations through a refresh-scoped context. A separate SQLite control store persists transport audit and endpoint breaker state, while an in-memory implementation keeps diagnostic canaries write-free. The existing normalization and canonical publication path remains unchanged after a provider candidate is returned.

**Tech Stack:** Python 3.12, BaoStock 0.9.3 custom TCP protocol, contextvars, SQLite, DuckDB/Parquet existing publication stack, Pydantic, pytest, Ruff.

---

### Task 1: Freeze transport contracts and reproduce unsafe SDK behavior

**Files:**
- Create: `backend/app/market/provider_transport.py`
- Create: `tests/test_baostock_transport.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Steps:**

1. Add failing tests for allowlisted endpoints, identifiers, sanitized observations and normalized
   provider-code mapping. Assert unknown codes never use provider message text.
2. Add a fake socket proving upstream `send()` can report a partial send and proving the desired
   patch must use `sendall()`.
3. Run:
   `uv run --extra dev pytest -q tests/test_baostock_transport.py --basetemp=/tmp/stock-eva-r2f01-transport-red`
   and retain the expected RED output.
4. Implement only immutable enums/models, ID generation, context scope and error mapping.
5. Pin the dependency as `baostock==0.9.3`, refresh `uv.lock`, and rerun focused GREEN plus Ruff.
6. Commit: `feat(market): define provider transport contracts`.

### Task 2: Add the pinned checked-send and framed-receive patch

**Files:**
- Create: `backend/app/market/baostock_vendor.py`
- Modify: `backend/app/market/baostock.py`
- Modify: `tests/test_baostock_transport.py`
- Modify: `tests/test_baostock_provider.py`

**Steps:**

1. Add failing blocking-fake tests for `sendall`, connect/send error, recv timeout, EOF, short
   header, missing marker, bad compression and malformed protocol. Assert socket-layer counters and
   absence of payload/token/URL/exception text.
2. Add a failing test that a wrong BaoStock version or upstream `socketutil.py` SHA refuses patch
   installation before a provider call.
3. Run the focused file and confirm all new cases fail for missing patch behavior.
4. Implement the pinned patch, verifying BaoStock 0.9.3 and the upstream source SHA before replacing
   `SocketUtil.connect` and `send_msg`. Use `sendall()` and a bounded receive loop; emit exactly one
   sanitized terminal observation per protocol operation.
5. Install the patch only when constructing the real BaoStock client; fake test clients remain
   untouched.
6. Run focused GREEN, existing deadline/session tests and Ruff.
7. Commit: `fix(market): harden pinned baostock transport`.

### Task 3: Tag every endpoint and fail closed on pagination/protocol integrity

**Files:**
- Modify: `backend/app/market/baostock.py`
- Modify: `tests/test_baostock_provider.py`
- Modify: `tests/test_baostock_transport.py`

**Steps:**

1. Add failing tests that each provider call emits the exact endpoint tag and stable
   `refresh_id/provider_session_id/request_id`, including page and attempt.
2. Add failing tests for stalled page, repeated page, page-2 receive failure, exactly 2,000 rows
   followed by premature termination, duplicate symbols and all-stock/daily row-count divergence.
3. Confirm RED with focused pytest.
4. Change `_read` to require an endpoint, create request scopes before login, and advance a guarded
   pagination tracker. Label all six endpoint call sites.
5. Reject incomplete pagination and duplicate/universe inconsistencies before returning a batch.
   Do not change normalization or publication gate thresholds.
6. Run focused GREEN plus `tests/test_market_data.py tests/test_market_reliability.py` and Ruff.
7. Commit: `fix(market): fail closed on provider protocol gaps`.

### Task 4: Persist sanitized audit and implement two-level circuit state

**Files:**
- Create: `backend/app/market/provider_health.py`
- Modify: `backend/app/config.py`
- Modify: `backend/app/storage/models.py`
- Modify: `backend/app/storage/layout.py`
- Create: `tests/test_provider_health.py`

**Steps:**

1. Add failing tests for SQLite audit round-trip, forbidden-field absence, endpoint
   `CLOSED/OPEN/HALF_OPEN`, cooldown lease exclusivity, success-close, failure-reopen, restart
   persistence and provider aggregation.
2. Confirm RED without creating any repository-local control database.
3. Implement a narrow SQLite store under `local_control_dir`, an in-memory equivalent, and a
   deterministic breaker policy. Count one terminal endpoint failure per refresh, not internal
   attempts.
4. Add configuration for failure threshold and cooldown without changing socket timeout or retry
   defaults.
5. Run focused GREEN, migration/layout tests and Ruff.
6. Commit: `feat(market): add persistent provider circuit breaker`.

### Task 5: Make the scheduler health-aware without touching publication semantics

**Files:**
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/market/models.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/cli.py`
- Modify: `tests/test_market_automation.py`
- Modify: `tests/test_market_failures.py`
- Modify: `tests/test_launchagent_assets.py`

**Steps:**

1. Add failing tests that an OPEN provider records `SKIPPED_CIRCUIT_OPEN` and makes zero provider
   calls, and that cooldown runs only one endpoint HALF_OPEN probe.
2. Add failing tests that probe success closes the breaker but does not refresh in the same slot;
   failure reopens it; normal CLOSED behavior is byte/semantically unchanged.
3. Confirm RED with injected providers only.
4. Wire the health gate before `trading_dates`/publication. Persist only sanitized audit/control
   state and reuse existing scheduled retry slots; do not add or shorten slots.
5. Expose additive provider health in internal CLI/status output without raw errors.
6. Run focused GREEN, LaunchAgent asset tests and Ruff.
7. Commit: `feat(market): skip refresh while provider circuit is open`.

### Task 6: Add the explicit zero-write diagnostic canary

**Files:**
- Create: `backend/app/market/provider_canary.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_provider_canary.py`

**Steps:**

1. Add failing tests for zero-network default planning, two-flag execution confirmation,
   `max_attempts=1`, independent session per endpoint and success-without-refresh.
2. Fingerprint temporary control/data roots before and after an injected complete canary. Assert no
   SQLite, Parquet, manifest, pointer, factor-cache or refresh-run write.
3. Confirm RED using only fake clients.
4. Implement endpoint probes with an in-memory audit/breaker. `adjust_factor` requires an explicit
   stock symbol; `index_history` uses a required index. Never call `MarketStore.save_refresh`.
5. Run focused GREEN and CLI sanitization tests plus Ruff.
6. Commit: `feat(market): add write-free provider transport canary`.

### Task 7: Close offline code acceptance and document the production gate

**Files:**
- Modify: `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md`
- Modify: `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-implementation.md`
- Create: `docs/acceptance/release-2-r2f0-1.md`

**Steps:**

1. Run the focused transport, provider, health, automation, canary, reliability and immutable
   publication suites.
2. Run the complete test suite and Ruff/format/diff checks. Treat any unrelated baseline failure
   explicitly; do not suppress it.
3. Inspect the complete diff against the R2-F0 baseline and verify no timeout/retry increase,
   quality-gate relaxation, symbol stitching or canonical object mutation path was added.
4. Obtain an independent code/spec review and fix every High/Medium issue with new RED/GREEN proof.
5. Record exact commits, test commands, counts and the explicit boundary:
   `CODE GO / REAL PROVIDER CANARY AND PRODUCTION INSTALL NOT AUTHORIZED`.
6. Commit: `docs(acceptance): close R2-F0.1 offline code gate`.

## Stop conditions

- Any test unexpectedly reaches the public BaoStock host.
- The implementation requires a higher timeout, more than two normal attempts or new retry slots.
- A transport/protocol failure returns a candidate containing accepted partial rows.
- Canary execution creates any database, factor cache or publication artifact.
- A change is needed in Normalize, Quality Gate, immutable objects, manifest hashing or pointer
  promotion to make tests pass.
- Production installation, LaunchAgent mutation or real provider execution would be required for
  code acceptance.
