# Stock EVA R2-F1 Continuity Controller Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`,
> `superpowers:test-driven-development`, `superpowers:subagent-driven-development`, and
> `karpathy-guidelines` to implement this plan task-by-task.

**Goal:** Detect every confirmed missing canonical session, persist a restart-safe health-aware
repair obligation, and execute at most one full-session repair without starving current freshness
or weakening any canonical publication gate.

**Architecture:** Build a pure set-difference scanner from confirmed calendar sessions and one
strict verified current-manifest inventory. Persist deterministic repair jobs and append-only
attempt evidence in the writer-owned market DuckDB. Wrap the existing freshness scheduler with a
single decision/claim layer that reuses the existing provider circuit and cross-process refresh
lock; successful repairs still flow only through the existing full-session publication function.

**Tech Stack:** Python 3.12, Pydantic 2, DuckDB, immutable Parquet/JSON manifest, FastAPI, pytest,
Ruff, macOS `flock` through the existing `RefreshRunLock`.

**Authoritative specification:**
[R2-F1 Continuity Controller Design](2026-08-13-stock-eva-r2f1-continuity-controller-design.md)

**Delivery mode:** One subagent at a time, linear commits, RED before GREEN, focused review after
every task. Do not begin the next task while the current task has an unresolved High or Medium
finding.

**Authority boundary:** Offline fakes and temporary roots only. No network/provider request,
runtime installation, production/NAS mutation or LaunchAgent action is authorized by this plan.

---

## 0. Execution controls

### Worktree and baseline

Work only in:

```text
/Users/finlay/.codex/worktrees/r2f1/Stock- evaluation
```

Expected branch and planning baseline:

```text
branch: codex/r2-f1-continuity-controller
baseline: the reviewed R2-F0.1 documentation HEAD plus this approved planning commit
```

Before every task:

```bash
git branch --show-current
git status --short
git log -1 --oneline
```

Stop on an unexpected branch, dirty overlap or a baseline other than the latest reviewed task
commit. Never edit another worktree or production path.

### TDD loop per task

1. Add only the named tests.
2. Run the exact focused RED command and record why the new test fails.
3. Implement the minimum contract for that task.
4. Run focused GREEN, adjacent regressions, Ruff, format and `git diff --check`.
5. Inspect the diff for scope leakage and forbidden writes.
6. Commit only the named files.
7. Run a focused spec/code review. Fix every High/Medium with a new RED/GREEN commit before moving
   forward.

Use unique `--basetemp=/tmp/stock-eva-r2f1-...` roots. Tests MUST inject calendars, clocks,
providers, health stores and data roots; they MUST NOT resolve the public BaoStock host.

### Global stop conditions

- A test or command attempts a real provider/network request.
- A read-only path creates a directory/database/table or calls pointer reconciliation.
- A scan writes before calendar and strict manifest evidence are fully validated.
- A repair claim occurs when provider health is not exactly `CLOSED`.
- Repair code acquires/resolves a HALF_OPEN probe instead of using the existing scheduler path.
- More than one repair starts in a single invocation.
- A due freshness session loses its decision to a repair.
- A repair uses explicit symbols or mixes provider rows.
- Implementation requires changing Normalize, quality gates, SHA-256, manifest atomicity, pointer
  promotion, provider timeout/retry or circuit policy.
- Production/NAS/LaunchAgent/runtime access appears necessary for code acceptance.

---

## Task 1: Add the strict immutable ready-session inventory

**Requirements:** FR-5, FR-6, FR-8; NFR-1, NFR-9; AC-4; EC-6, EC-7, EC-8, EC-9,
EC-10.

**Files:**

- Modify: `backend/app/storage/models.py`
- Modify: `backend/app/storage/dataset.py`
- Modify: `tests/test_nas_dataset.py`
- Modify: `tests/test_storage_readiness.py`

### Step 1.1: Write the failing contract tests

Add tests with these observable names/behaviors:

```python
def test_verified_ready_inventory_enumerates_all_current_manifest_partitions(): ...
def test_verified_ready_inventory_rejects_missing_or_wrong_hash_object(): ...
def test_verified_ready_inventory_rejects_wrong_schema_and_row_count(): ...
def test_verified_ready_inventory_is_bound_to_one_manifest_snapshot(): ...
def test_lightweight_manifest_dates_is_not_the_continuity_inventory_contract(): ...
```

The first fixture must publish non-contiguous dates and prove the returned inventory contains both,
while `control.published_refresh()` still contains only the latest. The corruption cases must take
before/after dataset fingerprints and prove no bytes changed.

### Step 1.2: Run RED

```bash
uv run --extra dev pytest -q tests/test_nas_dataset.py tests/test_storage_readiness.py \
  -k 'verified_ready_inventory or continuity_inventory' \
  --basetemp=/tmp/stock-eva-r2f1-inventory-red
```

Expected: FAIL because `VerifiedReadySessionInventory` and
`verified_ready_session_inventory()` do not exist.

### Step 1.3: Implement the minimal strict API

Add a frozen Pydantic model equivalent to:

```python
class VerifiedReadySessionInventory(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["ready"] = "ready"
    source: Literal["baostock"] = "baostock"
    manifest_generation: str
    manifest_identity: str
    sessions: tuple[date, ...]
    verified_at: datetime
```

Add `NasMarketStore.verified_ready_session_inventory(source="baostock", *, now=None)`. It must:

1. call one `_read_snapshot(verify_checksums=True)` path;
2. derive sorted unique `(source, trade_date)` partitions from the same parsed manifest snapshot;
3. prove every referenced object is present, safe, hash-valid, schema-valid and row-count-valid;
4. return no filesystem path;
5. raise existing sanitized `DatasetError` on any invalidity; and
6. leave `manifest_dates()` unchanged as a lightweight historical-backfill helper.

If `_read_snapshot` lacks the partition metadata needed to avoid a second manifest read, extend
`PublishedReadSnapshot` with immutable partition keys. Do not read `manifest.json` twice and merge
two generations.

### Step 1.4: Run GREEN and adjacent publication tests

```bash
uv run --extra dev pytest -q tests/test_nas_dataset.py tests/test_storage_readiness.py \
  tests/test_full_market_history.py \
  --basetemp=/tmp/stock-eva-r2f1-inventory-green
uv run --extra dev ruff check backend/app/storage/models.py backend/app/storage/dataset.py \
  tests/test_nas_dataset.py tests/test_storage_readiness.py
uv run --extra dev ruff format --check backend/app/storage/models.py backend/app/storage/dataset.py \
  tests/test_nas_dataset.py tests/test_storage_readiness.py
git diff --check
```

### Step 1.5: Commit

```bash
git add backend/app/storage/models.py backend/app/storage/dataset.py \
  tests/test_nas_dataset.py tests/test_storage_readiness.py
git commit -m "feat(storage): expose verified session inventory"
```

### Task 1 review checkpoint

Review the commit against AC-4. Block progress if the implementation uses `published_refresh`,
`available_dates`, two independent manifest reads, cached success after an object mutation, or
exposes absolute paths.

---

## Task 2: Persist additive repair jobs, attempts and CAS leases

**Requirements:** FR-9, FR-10, FR-11, FR-12, FR-13, FR-14, FR-15, FR-25; NFR-2,
NFR-5, NFR-6, NFR-9; AC-5, AC-6, AC-7, AC-11, AC-14; EC-11, EC-12, EC-13,
EC-14, EC-15, EC-16, EC-17, EC-18, EC-19.
EC-28 is covered by the same injected UTC clock and timestamp-validation task.

**Files:**

- Modify: `backend/app/config.py`
- Modify: `backend/app/market/models.py`
- Modify: `backend/app/market/store.py`
- Modify: `backend/app/cli.py`
- Create: `backend/app/market/continuity.py`
- Create: `tests/test_market_continuity.py`
- Modify: `tests/test_market_automation.py`

### Step 2.1: Write failing schema/state tests

Create `tests/test_market_continuity.py` with focused tests:

```python
def test_continuity_settings_are_fail_closed_and_bounded(): ...
def test_writer_migration_is_additive_and_preserves_legacy_refresh_rows(): ...
def test_read_only_missing_tables_return_unavailable_without_initialization(): ...
def test_repair_schema_has_no_payload_url_token_path_or_exception_columns(): ...
def test_enqueue_is_deterministic_and_idempotent_across_restart(): ...
def test_two_processes_under_shared_refresh_lock_cannot_claim_the_same_job(): ...
def test_lease_finalize_requires_matching_owner_id_and_state_version(): ...
def test_expired_lease_becomes_one_abandoned_attempt_and_retry_wait(): ...
def test_terminal_job_cannot_return_to_pending(): ...
def test_dead_letter_does_not_block_a_newer_pending_job(): ...
def test_market_schema_migration_uses_the_shared_refresh_lock(): ...
```

Use a mutable injected UTC clock, two `MarketStore` objects and a real two-process contention test
pointing to the same temporary DuckDB/lock path. Include a legacy DB fixture with `daily` and
`backfill` refresh rows.

### Step 2.2: Run RED

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py \
  --basetemp=/tmp/stock-eva-r2f1-store-red
```

Expected: FAIL because settings, models, tables and CAS APIs are absent.

### Step 2.3: Add fail-closed settings and typed models

Add:

```python
market_continuity_start_date: date | None = None
market_repair_enabled: bool = False
market_repair_max_attempts: int = Field(default=4, ge=1, le=20)
market_repair_lease_seconds: int = Field(default=1800, ge=60, le=86400)
market_repair_retry_base_seconds: int = Field(default=3600, ge=900, le=86400)
```

Define frozen `RepairJob`, `RepairAttempt`, `RepairLease`, `RepairQueueSnapshot` and allowlisted
state/outcome types in `continuity.py`. All datetimes must reject naive values and normalize to UTC.
Identifiers must use the same bounded safe-id style as provider health.

Extend `RefreshResult.run_kind` to `Literal["daily", "backfill", "repair"]`. The DuckDB column is
already VARCHAR; do not rewrite existing rows.

### Step 2.4: Add explicitly migrated writer-owned tables and narrow transactions

Add `MarketStore.initialize_continuity_schema()` with the exact fields/constraints in the design.
Do **not** put continuity DDL into generic `_connect()`: existing scheduler-state or refresh writes
outside the continuity flow must not implicitly migrate the queue. Call the explicit migration only
while `market-refresh.lock` is held. Add small transaction methods, not one transaction spanning
provider work:

```python
enqueue_repair_jobs(missing_dates, *, universe_id, now) -> list[RepairJob]
repair_queue_snapshot() -> RepairQueueSnapshot
claim_repair_job(job_id, *, owner, expected_version, now, lease_seconds) -> RepairLease | None
finalize_repair_attempt(lease, *, outcome, refresh_result, now, retry_policy) -> RepairJob
reap_expired_repair_leases(*, now, retry_policy) -> list[RepairJob]
reconcile_published_repair_jobs(verified_dates, *, now) -> list[RepairJob]
```

Rules:

- deterministic id is `repair:{trade_date}:{universe_id}`;
- enqueue uses unique key and never changes an existing job;
- claim checks state/due/version, creates one running attempt and lease atomically;
- finalization checks lease id, owner and version;
- retry delay is `min(24h, base * 2 ** (attempt_number - 1))`;
- non-retryable/exhausted -> dead letter;
- strict verified-date reconciliation may move pending/retry-wait/leased/dead-letter to published,
  finalizing a currently leased attempt consistently if needed;
- read methods inspect table/schema availability and never call `_connect()` or migrate.

All continuity DuckDB writer methods are documented as requiring the external
`market-refresh.lock` and a completed explicit continuity migration. CAS is still mandatory and
tested. Update the hidden `market-schema-migrate` CLI so continuity-table migration also acquires
`RefreshRunLock`; a busy lock returns a sanitized no-write failure. Other automatic initialization
of continuity tables must likewise occur only inside that lock.

### Step 2.5: Run GREEN and compatibility tests

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py tests/test_market_data.py \
  tests/test_market_get_read_only.py tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r2f1-store-green
uv run --extra dev ruff check backend/app/config.py backend/app/market/models.py \
  backend/app/market/store.py backend/app/market/continuity.py backend/app/cli.py \
  tests/test_market_continuity.py tests/test_market_automation.py
uv run --extra dev ruff format --check backend/app/config.py backend/app/market/models.py \
  backend/app/market/store.py backend/app/market/continuity.py backend/app/cli.py \
  tests/test_market_continuity.py tests/test_market_automation.py
git diff --check
```

### Step 2.6: Commit

```bash
git add backend/app/config.py backend/app/market/models.py backend/app/market/store.py \
  backend/app/market/continuity.py backend/app/cli.py tests/test_market_continuity.py \
  tests/test_market_automation.py
git commit -m "feat(market): persist continuity repair queue"
```

### Task 2 review checkpoint

Review state transitions, transaction boundaries and schema. Block on a terminal-to-pending
transition, missing CAS predicate, raw error/message column, DuckDB transaction spanning provider
work, implicit read migration or non-UTC timestamp.

---

## Task 3: Implement the pure confirmed-calendar minus verified-manifest scanner

**Requirements:** FR-1, FR-2, FR-3, FR-4, FR-5, FR-6, FR-7, FR-8, FR-28; NFR-1,
NFR-6, NFR-11; AC-1, AC-2, AC-3, AC-4, AC-5; EC-1, EC-2, EC-3, EC-4, EC-5,
EC-6, EC-7, EC-8, EC-9, EC-10.

**Files:**

- Modify: `backend/app/market/continuity.py`
- Modify: `backend/app/market/calendar.py` only if a narrow range helper is required
- Modify: `tests/test_market_continuity.py`
- Modify: `tests/test_nas_dataset.py`

### Step 3.1: Write the failing scan and zero-write matrix

Add:

```python
def test_scan_is_confirmed_open_minus_strict_ready_inventory(): ...
def test_scan_never_crosses_configured_start_or_latest_expected_end(): ...
def test_requested_range_can_only_narrow_configured_range(): ...
def test_unknown_calendar_day_rejects_the_complete_scan_zero_write(): ...
def test_calendar_conflict_rejects_the_complete_scan_zero_write(): ...
def test_each_manifest_and_object_corruption_rejects_before_writer_construction(): ...
def test_local_mutable_store_cannot_claim_immutable_ready_inventory(): ...
def test_empty_valid_manifest_enumerates_all_confirmed_open_sessions_as_missing(): ...
```

For invalid cases monkeypatch the writer constructor to raise if touched and fingerprint the whole
temporary root before/after.

### Step 3.2: Run RED

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py tests/test_nas_dataset.py \
  -k 'scan or configured_start or unknown_calendar or corrupt or empty_valid_manifest' \
  --basetemp=/tmp/stock-eva-r2f1-scan-red
```

Expected: FAIL because `ContinuityInventory` and typed scan results are absent.

### Step 3.3: Implement the pure scanner

Add models equivalent to:

```python
class ContinuityScanResult(BaseModel):
    status: Literal["current", "gaps"]
    effective_start: date
    effective_end: date
    manifest_generation: str
    confirmed_open_sessions: tuple[date, ...]
    published_ready_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    writes_control_state: Literal[False] = False
    provider_requests: Literal[0] = 0
```

`ContinuityInventory.scan()` must:

1. validate configured/requested/latest boundaries without writer construction;
2. reject an external calendar conflict state;
3. walk every calendar date in range and reject the whole range on `unknown`;
4. consume only `VerifiedReadySessionInventory`;
5. return a deterministic oldest-first set difference; and
6. perform no network or persistence.

If a calendar helper is added, keep it pure and fail-closed; do not modify calendar acquisition or
source files.

### Step 3.4: Add explicit enqueue orchestration after successful scan

Add one writer-only method/service that accepts a completed scan and enqueues its missing dates
inside `RefreshRunLock`. It must re-run or revalidate the scan under the lock before writing. The
scanner itself remains pure. Do not construct a provider.

### Step 3.5: Run GREEN and checks

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py tests/test_nas_dataset.py \
  tests/test_calendar_sync.py \
  --basetemp=/tmp/stock-eva-r2f1-scan-green
uv run --extra dev ruff check backend/app/market/continuity.py backend/app/market/calendar.py \
  tests/test_market_continuity.py tests/test_nas_dataset.py
uv run --extra dev ruff format --check backend/app/market/continuity.py backend/app/market/calendar.py \
  tests/test_market_continuity.py tests/test_nas_dataset.py
git diff --check
```

### Step 3.6: Commit

```bash
git add backend/app/market/continuity.py backend/app/market/calendar.py \
  tests/test_market_continuity.py tests/test_nas_dataset.py
git commit -m "feat(market): scan confirmed missing sessions"
```

Omit `calendar.py` from the commit if no change was required.

### Task 3 review checkpoint

Review evidence ordering. Block if a writer, provider or schema initializer can be reached before
all calendar dates and immutable objects are verified, or if one bad date is silently skipped.

---

## Task 4: Add deterministic Freshness/Repair decisions and provider-health gating

**Requirements:** FR-16, FR-17, FR-18, FR-19, FR-20, FR-21, FR-30; NFR-2, NFR-3,
NFR-4, NFR-8, NFR-10; AC-8, AC-9, AC-10, AC-16; EC-19, EC-20, EC-21, EC-22,
EC-23.

**Files:**

- Modify: `backend/app/market/continuity.py`
- Modify: `backend/app/market/automation.py`
- Modify: `tests/test_market_continuity.py`
- Modify: `tests/test_market_automation.py`

### Step 4.1: Write the failing priority matrix

Parameterize this table:

| Freshness condition | Provider health | Queue | Expected |
| --- | --- | --- | --- |
| latest due | CLOSED | old pending | run freshness; no claim |
| latest current | CLOSED | three pending | run oldest one repair |
| latest retry-wait with later slot | CLOSED | pending | run oldest one repair |
| latest retry-wait but slot now due | CLOSED | pending | run freshness |
| current/waiting | OPEN | pending | existing probe/wait path; no claim |
| current/waiting | HALF_OPEN | pending | existing probe/wait path; no claim |
| current/waiting | unavailable | pending | blocked; no claim |
| current | CLOSED | no eligible job | none |
| repair disabled | CLOSED | pending | no repair; job retained |

Also test that a job equal to the latest expected session is excluded from repair and becomes
eligible only after the calendar advances.

### Step 4.2: Run RED

```bash
uv run --extra dev pytest -q tests/test_market_automation.py tests/test_market_continuity.py \
  -k 'continuity or repair_lane or freshness_priority or provider_health_blocks_repair' \
  --basetemp=/tmp/stock-eva-r2f1-policy-red
```

Expected: FAIL because the current scheduler has only the latest-session decision.

### Step 4.3: Implement a wrapper decision model

Add:

```python
Lane = Literal["freshness", "repair"]

class ContinuityDecision(BaseModel):
    action: Literal["run", "wait", "none"]
    lane: Lane | None
    target_session: date | None
    repair_job_id: str | None
    next_run_at: datetime | None
    reason_code: str | None
```

Keep `SchedulePolicy.decide()` as the authority for freshness slots. A narrow continuity policy
wraps its output and may choose one strictly older eligible repair only when freshness returns
current/success or future retry-wait. Do not duplicate or alter `RETRY_TIMES`.

### Step 4.4: Gate health before lease claim

Under the existing `RefreshRunLock`, re-read provider health. Only `CLOSED` proceeds to
`claim_repair_job`. Route `OPEN/HALF_OPEN` through the existing `_handle_open_circuit` behavior;
missing/unreadable health returns sanitized blocked state. Repair code must never call
`acquire_probe` or `resolve_probe`.

The continuity dependency must be optional/default-disabled so the legacy freshness path is
behaviorally identical when R2-F1 is disabled or not initialized.

### Step 4.5: Run GREEN and scheduler regressions

```bash
uv run --extra dev pytest -q tests/test_market_automation.py tests/test_market_continuity.py \
  tests/test_provider_health.py \
  --basetemp=/tmp/stock-eva-r2f1-policy-green
uv run --extra dev ruff check backend/app/market/automation.py backend/app/market/continuity.py \
  tests/test_market_automation.py tests/test_market_continuity.py
uv run --extra dev ruff format --check backend/app/market/automation.py \
  backend/app/market/continuity.py tests/test_market_automation.py tests/test_market_continuity.py
git diff --check
```

### Step 4.6: Commit

```bash
git add backend/app/market/automation.py backend/app/market/continuity.py \
  tests/test_market_automation.py tests/test_market_continuity.py
git commit -m "feat(market): prioritize freshness and gate repairs"
```

### Task 4 review checkpoint

Review every decision-table row. Block on repair-before-freshness, claim before health, an added
probe mechanism, more than one claim, or any changed freshness retry slot.

---

## Task 5: Execute one full-session repair and close crash windows

**Requirements:** FR-18, FR-19, FR-20, FR-21, FR-22, FR-23, FR-24, FR-25; NFR-2,
NFR-3, NFR-4, NFR-5, NFR-6, NFR-10; AC-11, AC-12, AC-13, AC-14; EC-12,
EC-13, EC-14, EC-15, EC-16, EC-22, EC-23, EC-24, EC-25, EC-26.

**Files:**

- Modify: `backend/app/market/continuity.py`
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/market/store.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/cli.py`
- Modify: `tests/test_market_continuity.py`
- Modify: `tests/test_market_automation.py`
- Modify: `tests/test_market_reliability.py`
- Modify: `tests/test_launchagent_assets.py`

### Step 5.1: Write failing execution/crash tests

Add fake-provider tests:

```python
def test_repair_fetches_symbols_none_and_uses_current_required_symbols(): ...
def test_repair_persists_run_kind_and_deterministic_request_key(): ...
def test_partial_or_invalid_repair_never_publishes_and_enters_retry_policy(): ...
def test_successful_old_repair_adds_manifest_partition_without_regressing_latest_pointer(): ...
def test_crash_after_manifest_before_job_finalize_recovers_without_second_fetch(): ...
def test_process_kill_before_claim_commit_leaves_no_attempt(): ...
def test_process_kill_after_claim_recovers_only_after_lease_expiry(): ...
def test_one_invocation_never_executes_second_repair(): ...
```

The crash-after-manifest fixture must count provider calls and publication calls, then construct a
new service/store instance and prove both counts remain zero during recovery.

### Step 5.2: Run RED

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py tests/test_market_automation.py \
  tests/test_market_reliability.py \
  -k 'repair and (publication or crash or full_session or run_kind or pointer)' \
  --basetemp=/tmp/stock-eva-r2f1-execution-red
```

Expected: FAIL because a repair lease is not wired to canonical publication/finalization.

### Step 5.3: Implement the one-attempt executor

Within the shared lock:

1. rebuild strict calendar/manifest inventory;
2. reconcile already-ready dates;
3. re-evaluate freshness and provider health;
4. claim exactly one eligible job;
5. call `run_publication_refresh` once with:

```python
run_publication_refresh(
    store,
    provider,
    trade_date=lease.target_session,
    required_symbols=required_symbols(),
    request_key=f"repair:baostock:{lease.target_session}:all-main-board",
    run_id=<attempt-bound safe id>,
    run_kind="repair",
    before_store=<existing transport audit finalizer>,
)
```

The provider's full-market method must receive `symbols=None` through the unchanged refresh
function. Do not add a repair-specific normalization or publication path.

After `ready`, strictly re-read inventory and require the target date before finalizing job/attempt
as published/succeeded. A missing post-publication manifest date is a sanitized storage failure,
not success. On `partial/error`, finalize exactly once using existing failure stage/class/retryable.

### Step 5.4: Wire automatic entry points with default-off execution

Construct the continuity controller in `main.py` and `auto-refresh-once --execute` only after
writer initialization and only when configured. Reuse:

- the same `NasMarketStore` and local `MarketStore` control;
- the same `SQLiteProviderHealthStore`;
- the same `RefreshRunLock` path;
- the same `BaoStockProvider`; and
- the same required-symbol callback/post-publication hook.

Do not create a new LaunchAgent or provider client. In local mutable mode, keep continuity
unavailable and preserve existing freshness behavior.

### Step 5.5: Run GREEN and canonical-chain regressions

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py tests/test_market_automation.py \
  tests/test_market_reliability.py tests/test_nas_dataset.py tests/test_provider_health.py \
  tests/test_launchagent_assets.py \
  --basetemp=/tmp/stock-eva-r2f1-execution-green
uv run --extra dev ruff check backend/app/market/continuity.py \
  backend/app/market/automation.py backend/app/market/store.py backend/app/main.py \
  backend/app/cli.py tests/test_market_continuity.py tests/test_market_automation.py \
  tests/test_market_reliability.py tests/test_launchagent_assets.py
uv run --extra dev ruff format --check backend/app/market/continuity.py \
  backend/app/market/automation.py backend/app/market/store.py backend/app/main.py \
  backend/app/cli.py tests/test_market_continuity.py tests/test_market_automation.py \
  tests/test_market_reliability.py tests/test_launchagent_assets.py
git diff --check
```

### Step 5.6: Commit

```bash
git add backend/app/market/continuity.py backend/app/market/automation.py \
  backend/app/market/store.py backend/app/main.py backend/app/cli.py \
  tests/test_market_continuity.py tests/test_market_automation.py \
  tests/test_market_reliability.py tests/test_launchagent_assets.py
git commit -m "feat(market): execute bounded session repairs"
```

### Task 5 review checkpoint

Perform an independent code/spec review of Tasks 1–5 at the exact HEAD. Require High 0 / Medium 0.
Specifically trace both crash windows, lock lifetime, CAS owner/version, provider-health ordering,
full-session arguments, post-publish verification and non-regressing latest pointer.

---

## Task 6: Expose write-free continuity status and CLI planning/enqueue

**Requirements:** FR-26, FR-27, FR-28, FR-29, FR-30; NFR-1, NFR-7, NFR-8,
NFR-9; AC-15, AC-16; EC-1, EC-2, EC-3, EC-4, EC-5, EC-6, EC-7, EC-8, EC-17,
EC-18, EC-27.

**Files:**

- Modify: `backend/app/market/models.py`
- Modify: `backend/app/market/continuity.py`
- Modify: `backend/app/market/store.py`
- Modify: `backend/app/storage/dataset.py`
- Modify: `backend/app/api/market.py`
- Modify: `backend/app/cli.py`
- Modify: `tests/test_market_get_read_only.py`
- Modify: `tests/test_market_continuity.py`
- Modify: `tests/test_api_baseline.py`
- Modify: `docs/market-data.md`

### Step 6.1: Write failing API/CLI zero-init tests

Add:

```python
def test_market_status_adds_sanitized_continuity_summary(): ...
def test_market_status_missing_continuity_tables_is_unavailable_and_zero_write(): ...
def test_market_status_corrupt_continuity_schema_is_sanitized_and_zero_write(): ...
def test_continuity_cli_plan_from_empty_runtime_creates_no_paths(): ...
def test_continuity_cli_invalid_range_creates_no_paths_even_with_execute(): ...
def test_continuity_cli_plan_never_reconciles_pointer_or_initializes_schema(): ...
def test_continuity_cli_execute_enqueues_only_after_strict_rescan_under_lock(): ...
def test_continuity_cli_execute_makes_zero_provider_and_canonical_writes(): ...
def test_continuity_outputs_contain_no_path_payload_url_token_or_exception(): ...
```

Fingerprint file trees, DuckDB bytes/schema, provider-health SQLite, Parquet, manifest and pointer
before/after. Monkeypatch provider constructors, `initialize_schema()` and
`reconcile_control_pointer()` to raise on plan/read paths.

### Step 6.2: Run RED

```bash
uv run --extra dev pytest -q tests/test_market_get_read_only.py \
  tests/test_market_continuity.py tests/test_api_baseline.py \
  -k 'continuity or market_status' \
  --basetemp=/tmp/stock-eva-r2f1-status-red
```

Expected: FAIL because API fields and CLI command do not exist.

### Step 6.3: Extend the API additively

Add these defaulted fields to `MarketDataStatus`:

```python
continuity_status: Literal["current", "gaps", "blocked", "unavailable"] = "unavailable"
continuity_start_date: date | None = None
missing_session_count: int = 0
oldest_missing_session: date | None = None
repair_execution_enabled: bool = False
repair_pending_count: int = 0
repair_retry_wait_count: int = 0
repair_active_count: int = 0
repair_dead_letter_count: int = 0
active_lane: Literal["freshness", "repair"] | None = None
continuity_reason_code: str | None = None
```

Build the summary from read-only strict inventory and SELECT-only queue readers. If continuity
tables are absent/malformed, return only the additive continuity fields as unavailable; do not
initialize or migrate. Preserve the existing 503 behavior for corruption of the base market
control schema. Read provider health only through `provider_health_snapshot()` so status never
reclaims an expired HALF_OPEN lease.

`NasMarketStore` may delegate continuity SELECT methods to its read-only control. `MarketStore`
local-only mode reports immutable inventory unavailable.

### Step 6.4: Add `market-continuity` CLI with early read-only branch

Add parser:

```text
market-continuity [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--execute]
```

Handle planning before the general `ensure_local_runtime_dirs()`, writer-store construction and
pointer reconciliation code. Validate configured/requested range and build strict inventory first.
Planning reports zero writes/network. Execute then acquires `market-refresh.lock`, repeats all
evidence checks, explicitly initializes/migrates the writer-owned schema, and enqueues only the
verified missing jobs. It does not claim or run a repair.

### Step 6.5: Document the operator contract

Update `docs/market-data.md` with:

- the explicit continuity start boundary;
- dry-run vs enqueue-only `--execute` behavior;
- automatic repair default-off switch;
- status field meanings;
- dead-letter and rollback behavior; and
- offline/production authorization boundary.

### Step 6.6: Run GREEN and read-only regressions

```bash
uv run --extra dev pytest -q tests/test_market_get_read_only.py \
  tests/test_market_continuity.py tests/test_api_baseline.py \
  --basetemp=/tmp/stock-eva-r2f1-status-green
uv run --extra dev ruff check backend/app/market/models.py backend/app/market/continuity.py \
  backend/app/market/store.py backend/app/storage/dataset.py backend/app/api/market.py \
  backend/app/cli.py tests/test_market_get_read_only.py tests/test_market_continuity.py \
  tests/test_api_baseline.py
uv run --extra dev ruff format --check backend/app/market/models.py \
  backend/app/market/continuity.py backend/app/market/store.py backend/app/storage/dataset.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_get_read_only.py \
  tests/test_market_continuity.py tests/test_api_baseline.py
git diff --check
```

### Step 6.7: Commit

```bash
git add backend/app/market/models.py backend/app/market/continuity.py \
  backend/app/market/store.py backend/app/storage/dataset.py backend/app/api/market.py \
  backend/app/cli.py tests/test_market_get_read_only.py tests/test_market_continuity.py \
  tests/test_api_baseline.py docs/market-data.md
git commit -m "feat(api): expose market continuity status"
```

### Task 6 review checkpoint

Review from a missing runtime root and from a legacy DB. Block on any directory/schema/pointer
mutation from GET/plan, any provider construction, raw exception/path leakage or behavior change to
existing status fields.

---

## Task 7: Close R2-F1 offline code acceptance

**Requirements:** All FR/NFR/AC/EC, with NFR-11 as the non-negotiable execution boundary.

**Files:**

- Create: `docs/acceptance/release-2-r2f1.md`
- Modify: `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md`
- Modify: `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-implementation.md`
- Modify: `docs/plans/2026-08-13-stock-eva-r2f1-continuity-controller-design.md`
- Modify: `docs/plans/2026-08-13-stock-eva-r2f1-continuity-controller-implementation.md`

### Step 7.1: Run the focused R2-F1 fault matrix

```bash
uv run --extra dev pytest -q \
  tests/test_market_continuity.py \
  tests/test_market_automation.py \
  tests/test_market_get_read_only.py \
  tests/test_market_reliability.py \
  tests/test_nas_dataset.py \
  tests/test_provider_health.py \
  tests/test_api_baseline.py \
  tests/test_launchagent_assets.py \
  --basetemp=/tmp/stock-eva-r2f1-acceptance
```

Record the exact count and HEAD. Do not summarize a partial selection as full-suite evidence.

### Step 7.2: Run the universal offline gate

```bash
uv run --extra dev pytest -q --basetemp=/tmp/stock-eva-r2f1-full
uv run --extra dev ruff check backend tests
uv run --extra dev ruff format --check backend tests
git diff --check
```

### Step 7.3: Perform the completion audit

For every FR, NFR, AC and EC in the design, identify the exact test/path/line proving it. In
particular re-check:

- current manifest entries, not singleton pointer, drive inventory;
- every object is strict-verified before queue writes;
- unknown/corrupt evidence is zero-write;
- jobs/attempts are additive, sanitized and restart-safe;
- claim/finalize/recovery use lock plus CAS;
- due freshness always wins;
- provider non-CLOSED/unavailable creates no attempt;
- only existing scheduler handles HALF_OPEN;
- one repair is whole-session and canonical-chain only;
- crash after manifest publish causes no second fetch;
- status/CLI reads initialize nothing; and
- rollback disables execution without deleting evidence.

Search the diff for forbidden changes:

```bash
git diff <r2f1-planning-head>...HEAD -- \
  backend/app/market/normalize.py backend/app/storage/dataset.py \
  backend/app/market/baostock.py backend/app/market/provider_health.py
rg -n "timeout|retry|coverage|adjust_factor|symbols=" \
  backend/app/market/continuity.py backend/app/market/automation.py
```

Review every hit in context; a search hit is not itself a defect or proof.

### Step 7.4: Obtain an independent final review

Review the exact code HEAD, spec and acceptance matrix. The final result must state:

```text
High: 0
Medium: 0
Verdict: R2-F1 OFFLINE CODE GO or NO-GO
```

Any High/Medium requires a new failing regression, minimal fix, focused/full re-run and separate
fix commit before re-review.

### Step 7.5: Write truthful acceptance evidence

`docs/acceptance/release-2-r2f1.md` must include:

- exact reviewed code and documentation commits;
- each bounded implementation/fix commit;
- RED/GREEN commands and actual counts;
- concurrency, restart, stale lease, dead-letter, freshness-priority, provider-health, crash-window,
  strict-manifest and zero-write evidence;
- independent review verdict;
- known Low limitations;
- rollback switch and schema compatibility;
- explicit statement that no real provider/install/production/NAS/LaunchAgent action occurred; and
- exact boundary:
  `OFFLINE CODE GO / REAL PROVIDER AND PRODUCTION EXECUTION NOT AUTHORIZED`.

Update the umbrella roadmap/plan only to mark the exact R2-F1 code gate and next confirmation gate.
Do not start or fold R2-F2 into this acceptance.

### Step 7.6: Commit documentation closure

```bash
git add docs/acceptance/release-2-r2f1.md \
  docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md \
  docs/plans/2026-08-12-stock-eva-r2f-data-reliability-implementation.md \
  docs/plans/2026-08-13-stock-eva-r2f1-continuity-controller-design.md \
  docs/plans/2026-08-13-stock-eva-r2f1-continuity-controller-implementation.md
git commit -m "docs(acceptance): close R2-F1 offline code gate"
```

### Task 7 stop/pause checkpoint

After R2-F1 passes independent quality review, stop the active goal and report what R2-F1 changed,
the exact tests/commits and every remaining production boundary. Wait for explicit user approval
before starting R2-F2 Provider Evidence Framework.

---

## Planned linear commit map

| Order | Commit | Boundary |
| --- | --- | --- |
| 1 | `feat(storage): expose verified session inventory` | Strict current-manifest/object authority |
| 2 | `feat(market): persist continuity repair queue` | Additive jobs/attempts, CAS and stale lease |
| 3 | `feat(market): scan confirmed missing sessions` | Pure fail-closed set-difference scanner |
| 4 | `feat(market): prioritize freshness and gate repairs` | Lane decision and provider health gate |
| 5 | `feat(market): execute bounded session repairs` | One full-session repair and crash reconciliation |
| 6 | `feat(api): expose market continuity status` | Read-only API/CLI and operator docs |
| 7 | `docs(acceptance): close R2-F1 offline code gate` | Exact evidence and pause gate |

Fix commits are allowed only when linked to a newly failing regression and must be recorded in the
acceptance file. No commit may contain R2-F2 raw evidence, multi-provider or failover work.

## Definition of R2-F1 offline complete

R2-F1 is complete only when all seven tasks are committed and reviewed, all design requirements
have direct passing evidence, full-suite/style gates pass, the exact code HEAD has High 0 / Medium
0, and the acceptance record states the offline-only boundary. Passing R2-F1 does not close the
R2-F0 production incident, install a runtime, authorize a provider call or begin R2-F2.
