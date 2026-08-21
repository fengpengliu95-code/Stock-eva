# Stock EVA R2-F Data Reliability Foundation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans and
> superpowers:test-driven-development to implement this plan task-by-task.

**Goal:** Make Stock EVA's after-close market data continuously obtainable, automatically
repairable, provider-auditable and capable of safe whole-session failover without weakening the
existing immutable publication gates.

**Architecture:** Keep the current Normalize -> Quality Gate -> Immutable Parquet -> SHA-256 ->
Manifest -> Atomic Publish chain. Add a continuity controller before ingestion, immutable
provider-shaped evidence and provider-specific normalization before the canonical gate, and a
whole-session selection record before pointer movement. Serve only the local canonical dataset;
replicate to NAS asynchronously and treat provider/NAS failures as observable control-plane state.

**Tech Stack:** Python 3.12, Pydantic 2, DuckDB, immutable Parquet/JSON manifests, FastAPI, pytest,
Ruff, macOS LaunchAgents, optional provider SDK/HTTP adapters selected only after qualification.

**Authoritative roadmap:**
`docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md`

**Planning status:** R2-F0.1 is OFFLINE CODE GO at exact code HEAD
`b0b643fd78b1b0be27279cbd3380268577408c85`; this document still authorizes no deployment,
provider purchase, credential creation, network canary or production repair. R2-F0 remains
production NO-GO. See
[R2-F0.1 acceptance](../acceptance/release-2-r2f0-1.md) and
[R2-F0 incident acceptance](../acceptance/release-2-r2f0.md).

**R2-F1 closure status:** OFFLINE CODE GO at exact reviewed code HEAD
336107be1149d829c0ea841dd2466982bff7d689; see
[R2-F1 acceptance](../acceptance/release-2-r2f1.md). The code gate is complete, but this does not
authorize installation, real provider access, production repair or R2-F2.

**Next confirmation gate:** Gap Scanner + Health-aware Repair Queue is the next code stage and may
start only after explicit user confirmation. Provider-neutral RAW Evidence Framework follows it;
then a second source must pass at least 20 consecutive trading sessions of whole-session shadow
qualification. Do not mix providers by symbol, and keep automatic failover disabled until shadow,
qualification and a separately approved manual failover stage are complete.

---

## Execution controls

### Worktree and branch

Start each R2-F version from the latest reviewed `main` in a dedicated worktree. Recommended
branches are:

```text
codex/r2-f0-incident-closure
codex/r2-f1-continuity-controller
codex/r2-f2-provider-evidence
codex/r2-f3-shadow-bakeoff
codex/r2-f4-controlled-failover
codex/r2-f5-production-soak
```

Do not stack the next version on an unreviewed commit. Preserve all user-owned files and unrelated
working-tree changes. Each version receives specification review, code-quality review and an
explicit GO before the next branch is created.

### TDD and evidence protocol

For every task:

1. Add the named failing tests first.
2. Run the focused command and record the observed RED reason.
3. Implement only the minimum contract required by that task.
4. Run the focused GREEN command and Ruff/diff checks.
5. Commit only the named files with the suggested bounded commit message.

The acceptance document for each version records the actual commands, counts and commit. Planned
test names and expected failures in this document are not evidence that they ran.

### Universal pre-integration gate

Run after all tasks in a version are complete:

```bash
uv run --extra dev pytest
uv run --extra dev ruff check backend tests
uv run --extra dev ruff format --check backend tests
git diff --check
```

If a version changes installed CLI/config/LaunchAgent behavior, also run the installer and runtime
checks only after explicit production authorization:

```bash
./scripts/stock_eva_launchagents_install.sh --install
./scripts/stock_eva_launchagents_status.sh
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/market/status
```

Do not read or mutate the production user database during data-plane acceptance. All pre-production
tests use temporary market, evidence, control and dataset roots.

### Migration rules

- Persistence changes are additive until the new reader has passed compatibility tests.
- Never rewrite an existing immutable Parquet object to add provider metadata.
- Missing provider fields in legacy rows/manifests resolve to `baostock` through a documented
  compatibility decoder; the stored legacy bytes remain unchanged.
- New DB columns are nullable or have deterministic legacy defaults before they become required for
  new writes.
- Every new automatic behavior has a default-off kill switch until its version GO.
- Rollback switches execution back to the previous reader/scheduler/provider policy; it does not
  delete new immutable evidence or audit rows.

---

## R2-F0 — Incident Closure

### Task 0: Freeze the incident fixture and mutation boundary

**Files:**

- Modify: `tests/test_market_reliability.py`
- Create: `docs/acceptance/release-2-r2f0.md`

**Step 1: Add the failing production-shaped fixture**

Add a provider fixture for one confirmed trading day containing:

```python
EXPECTED = {"sh.600984", "sh.603221", "sh.600000", "sh.000001", "sz.399001"}

# The first two are legal suspended placeholders:
# tradestatus=0, blank OHLC/activity, no adjustment factor.
# sh.600000 is active and has a factor.
# Both required indexes are valid.
```

Add these tests:

```python
def test_suspended_rows_without_factor_can_form_a_complete_publication(tmp_path): ...
def test_active_row_without_factor_still_blocks_publication(tmp_path): ...
def test_failed_incident_candidate_preserves_existing_pointer_and_objects(tmp_path): ...
```

The first test must expect ready/complete publication. The second must change only the active row's
factor to missing and expect `missing_adjust_factor`. The third fingerprints pointer/object bytes
before and after a failed run.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_reliability.py \
  -k 'suspended_rows_without_factor or active_row_without_factor or incident_candidate' \
  --basetemp=/tmp/stock-eva-r2f0-incident-red
```

Expected: the legal suspended case is partial with `invalid_suspended_placeholder`; negative pointer
tests remain green. Record the exact result in the acceptance draft.

**Step 3: Create the acceptance skeleton**

Document the observed installed baseline, exact scope, protected paths, planned commands and a
`NO-GO — implementation not run` verdict. Do not copy raw logs or filesystem secrets into the file.

**Step 4: Commit the RED fixture**

```bash
git add tests/test_market_reliability.py docs/acceptance/release-2-r2f0.md
git commit -m "test(market): reproduce suspended publication incident"
```

---

### Task 1: Correct suspension and adjustment-factor semantics

**Files:**

- Modify: `backend/app/market/normalize.py`
- Modify: `backend/app/market/store.py`
- Modify: `backend/app/market/automation.py`
- Modify: `tests/test_market_data.py`
- Modify: `tests/test_market_automation.py`
- Modify: `tests/test_market_reliability.py`

**Step 1: Add remaining negative tests**

Add focused cases proving:

```python
def test_active_blank_ohlcv_is_not_a_suspended_placeholder(): ...
def test_suspended_placeholder_with_nonzero_activity_is_rejected(): ...
def test_suspended_index_placeholder_is_rejected(): ...
def test_factor_is_required_for_every_non_suspended_stock(): ...
```

No test may special-case the two incident symbols.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_data.py tests/test_market_automation.py \
  tests/test_market_reliability.py -k 'suspended or factor or incident' \
  --basetemp=/tmp/stock-eva-r2f0-semantics-red
```

Expected: the incident fixture fails for the extra `missing_adjust_factor` issue while active and
malformed negative cases define the boundary.

**Step 3: Implement the minimum rule**

Change normalization so this rule is explicit:

```python
if suspended_placeholder:
    issues.append("suspended_placeholder")
elif security_type == "stock" and adjust_factor is None:
    issues.append("missing_adjust_factor")
```

Do not loosen price/activity validation. Keep the store's legal suspended shape check and require
the exact suspension issue for the normalized placeholder. Keep the publication gate's independent
factor check for non-suspended stock rows.

**Step 4: Run GREEN**

```bash
uv run --extra dev pytest -q tests/test_market_data.py tests/test_market_automation.py \
  tests/test_market_reliability.py -k 'suspended or factor or incident' \
  --basetemp=/tmp/stock-eva-r2f0-semantics-green
uv run --extra dev ruff check backend/app/market/normalize.py backend/app/market/store.py \
  backend/app/market/automation.py tests/test_market_data.py tests/test_market_automation.py \
  tests/test_market_reliability.py
git diff --check
```

Expected: the production-shaped legal suspension publishes; every active missing-factor or malformed
placeholder case fails closed.

**Step 5: Commit**

```bash
git add backend/app/market/normalize.py backend/app/market/store.py \
  backend/app/market/automation.py tests/test_market_data.py \
  tests/test_market_automation.py tests/test_market_reliability.py
git commit -m "fix(market): validate suspended rows without adjustment factors"
```

---

### Task 2: Add structured provider failures without breaking clients

**Files:**

- Create: `backend/app/market/failures.py`
- Modify: `backend/app/market/baostock.py`
- Modify: `backend/app/market/models.py`
- Modify: `backend/app/market/refresh.py`
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_market_failures.py`
- Modify: `tests/test_market_automation.py`

**Step 1: Write the failure-mapping tests**

Cover timeout, connect/DNS, provider status, schema, semantic, calendar, storage and unexpected
failure. Assert public models contain only:

```python
failure_stage: Literal["fetch", "normalize", "validate", "publish"] | None
failure_class: Literal[
    "transport_timeout", "transport_connect", "dns", "auth", "rate_limit",
    "provider_4xx", "provider_5xx", "schema", "semantic", "calendar",
    "universe", "coverage", "reconciliation", "storage", "internal",
] | None
retryable: bool | None
```

Assert injected tokens, URLs, exception messages, SQL and local paths do not appear in CLI JSON,
logs captured by tests or `RefreshResult.model_dump_json()`.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_failures.py tests/test_market_automation.py \
  -k 'failure or sanitized or retryable' \
  --basetemp=/tmp/stock-eva-r2f0-failures-red
```

Expected: `backend.app.market.failures` is absent and current failures collapse to
`provider_error`.

**Step 3: Implement typed internal failures**

Add immutable `MarketFailure` and `MarketFailureError` models. Map known BaoStock transport errors
at the adapter boundary; map schema/semantic errors where they are detected; catch unexpected
exceptions once at the orchestrator boundary as `internal`. Add optional fields to `RefreshResult`
so existing serialized consumers remain compatible.

During R2-F0, retain `provider_error` in legacy `quality_issues` for transport failures. The new
fields become the source of scheduler retry policy; do not remove the old token until R2-F2's API
compatibility review.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_failures.py tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r2f0-failures-green
uv run --extra dev ruff check backend/app/market/failures.py backend/app/market/baostock.py \
  backend/app/market/models.py backend/app/market/refresh.py backend/app/market/automation.py \
  backend/app/cli.py tests/test_market_failures.py tests/test_market_automation.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/market/failures.py backend/app/market/baostock.py \
  backend/app/market/models.py backend/app/market/refresh.py backend/app/market/automation.py \
  backend/app/cli.py tests/test_market_failures.py tests/test_market_automation.py
git commit -m "feat(market): classify provider and publication failures"
```

---

### Task 3: Close R2-F0 in synthetic and supervised production stages

**Files:**

- Modify: `docs/acceptance/release-2-r2f0.md`
- Modify if needed: `docs/market-data.md`

**Step 1: Run the isolated full publication acceptance**

Use a temporary settings file and dataset root. Exercise one ready incident fixture, one active
missing-factor fixture, one corrupt object and one interrupted publication. Verify manifest,
object SHA-256, pointer, representative history and read-only GET fingerprints.

```bash
uv run --extra dev pytest -q tests/test_market_data.py tests/test_market_automation.py \
  tests/test_market_reliability.py tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r2f0-acceptance
```

**Step 2: Pass the version integration gate**

Run the universal gate. Independent reviewers inspect the exact commit range. Update the acceptance
record to `CODE GO / PRODUCTION REPAIR PENDING`, not R2-F0 GO.

**Step 3: Read the installed pre-state**

After explicit production authorization, record:

```bash
./scripts/stock_eva_launchagents_status.sh
curl --fail --silent --show-error http://127.0.0.1:8000/api/v1/market/status
```

Fingerprint the current manifest/pointer and confirm the target remains missing. Do not stop APIs,
edit Parquet or change pointers manually.

**Step 4: Install and run exactly one supervised repair**

Use the reviewed installer, re-read release metadata, then execute the existing explicit guarded
command:

```bash
uv run python -m backend.app.cli refresh --date 2026-08-11 \
  --all-main-board --execute-all-main-board
```

The exact date is valid only for this incident. If production has advanced or already repaired it,
stop and recalculate from live calendar/manifest state instead of replaying this command blindly.

**Step 5: Verify and close**

Require ready result, 100% legal coverage, zero failed symbols, correct manifest hash, current
pointer at or after the repaired date, API readback and unchanged protected user-store fingerprints.
Update the acceptance file to `R2-F0 GO` only if all checks pass. Otherwise rollback the installed
runtime to the prior reviewed release; keep the previous canonical pointer and record NO-GO.

**Step 6: Commit the evidence**

```bash
git add docs/acceptance/release-2-r2f0.md docs/market-data.md
git commit -m "docs(acceptance): close R2-F0 incident repair"
```

---

## R2-F1 — Continuity Controller

### Task 4: Add restart-safe continuity inventory and repair jobs

**Files:**

- Modify: `backend/app/config.py`
- Modify: `backend/app/market/models.py`
- Modify: `backend/app/market/store.py`
- Create: `backend/app/market/continuity.py`
- Create: `tests/test_market_continuity.py`

**Step 1: Write failing inventory/store tests**

Add tests for:

```python
def test_inventory_is_confirmed_calendar_minus_ready_manifest_dates(tmp_path): ...
def test_inventory_never_scans_before_continuity_start_date(tmp_path): ...
def test_corrupt_manifest_and_unknown_calendar_schedule_no_job(tmp_path): ...
def test_same_missing_date_is_enqueued_idempotently_across_restart(tmp_path): ...
def test_completed_job_cannot_return_to_pending(tmp_path): ...
def test_dead_job_preserves_attempt_history_without_blocking_new_dates(tmp_path): ...
```

Use this state model:

```python
RepairJobState = Literal[
    "pending", "fetching", "validating", "retry_wait", "published", "dead_letter"
]
```

Do not reuse `RefreshState`; job state and overall market freshness are different concerns.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py \
  --basetemp=/tmp/stock-eva-r2f1-continuity-red
```

Expected: continuity module/config/table do not exist.

**Step 3: Implement the minimal inventory and writer-owned tables**

Add `market_continuity_start_date` to settings. Extend the existing writer-owned market DuckDB with
additive `repair_jobs` and `repair_attempts` tables. Use deterministic job key
`repair:{trade_date}:{universe_id}` and monotonic allowed transitions. Reader methods must return
empty when the DB/table is absent and must never initialize schema.

`ContinuityInventory.scan()` accepts an already validated calendar and immutable manifest snapshot;
it does no network work and no writes. `enqueue_missing()` is the explicit writer step.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py \
  --basetemp=/tmp/stock-eva-r2f1-continuity-green
uv run --extra dev ruff check backend/app/config.py backend/app/market/models.py \
  backend/app/market/store.py backend/app/market/continuity.py tests/test_market_continuity.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/config.py backend/app/market/models.py backend/app/market/store.py \
  backend/app/market/continuity.py tests/test_market_continuity.py
git commit -m "feat(market): persist missing-session repair jobs"
```

---

### Task 5: Split Freshness and Repair scheduling lanes

**Files:**

- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/market/continuity.py`
- Modify: `backend/app/cli.py`
- Modify: `tests/test_market_automation.py`
- Modify: `tests/test_market_continuity.py`

**Step 1: Write failing scheduling tests**

Cover the deterministic priority matrix:

| Condition | Expected decision |
|---|---|
| Latest expected session missing and due now | Run Freshness latest session |
| Latest is current; historical gap exists | Run oldest Repair job |
| Freshness waits for a later retry slot; repair budget available | Run one Repair job |
| Old repair repeatedly fails; a new latest session becomes due | Run Freshness, retain repair |
| No gap/current data | None |
| Calendar unknown/conflict | No fetch/write |
| Another process holds the refresh lock | Return already-running; no state transition |

Add process-kill/restart simulation: a stale `fetching` lease returns to `pending` only after its
lease expiry and increments an abandoned-attempt counter.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_automation.py \
  tests/test_market_continuity.py -k 'freshness or repair or lease or priority' \
  --basetemp=/tmp/stock-eva-r2f1-lanes-red
```

Expected: the current scheduler only evaluates the latest target and has no lane/job decision.

**Step 3: Implement one decision engine**

Add:

```python
Lane = Literal["freshness", "repair"]

class ContinuityDecision(BaseModel):
    action: Literal["run", "wait", "none"]
    lane: Lane | None
    target_session: date | None
    repair_job_id: str | None
    next_run_at: datetime | None
```

Keep existing retry slots for freshness. When freshness is current or waiting, allow at most one
repair attempt per one-shot invocation. Use the existing cross-process refresh lock for both lanes.
Pass `run_kind="repair"` through refresh audit after widening its literal. Do not loop through all
gaps in one LaunchAgent invocation.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_automation.py tests/test_market_continuity.py \
  --basetemp=/tmp/stock-eva-r2f1-lanes-green
uv run --extra dev ruff check backend/app/market/automation.py \
  backend/app/market/continuity.py backend/app/cli.py \
  tests/test_market_automation.py tests/test_market_continuity.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/market/automation.py backend/app/market/continuity.py \
  backend/app/cli.py tests/test_market_automation.py tests/test_market_continuity.py
git commit -m "feat(market): schedule freshness and repair lanes"
```

---

### Task 6: Expose continuity status and close R2-F1

**Files:**

- Modify: `backend/app/market/models.py`
- Modify: `backend/app/api/market.py`
- Modify: `backend/app/cli.py`
- Modify: `tests/test_market_get_read_only.py`
- Modify: `tests/test_market_continuity.py`
- Create: `docs/acceptance/release-2-r2f1.md`
- Modify: `docs/market-data.md`

**Step 1: Write failing read/status tests**

Extend `MarketDataStatus` additively with:

```python
continuity_status: Literal["current", "gaps", "blocked", "unavailable"]
continuity_start_date: date | None
missing_session_count: int
oldest_missing_session: date | None
repair_pending_count: int
repair_dead_letter_count: int
active_lane: Literal["freshness", "repair"] | None
```

Add tests proving a missing/non-migrated control DB returns an unavailable or empty read without
creating files/tables, corrupt state is sanitized, and no absolute path or job exception reaches the
API.

Define a CLI:

```text
market-continuity [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--execute]
```

Without `--execute`, it inventories and reports `writes_control_state=false`. Execute enqueues
only confirmed missing sessions and reports counts/IDs, not paths.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_get_read_only.py \
  tests/test_market_continuity.py -k 'continuity or missing_session or read_only' \
  --basetemp=/tmp/stock-eva-r2f1-status-red
```

**Step 3: Implement additive API/CLI contracts**

Construct read dependencies without schema initialization. Keep all existing status fields and
semantics. Validate CLI dates before constructing writers; reject future, inverted, before-start and
unknown-calendar ranges with zero write.

**Step 4: Run GREEN, fault matrix and full version gate**

```bash
uv run --extra dev pytest -q tests/test_market_continuity.py \
  tests/test_market_automation.py tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r2f1-status-green
uv run --extra dev ruff check backend/app/market/models.py backend/app/api/market.py \
  backend/app/cli.py tests/test_market_get_read_only.py tests/test_market_continuity.py
git diff --check
```

Then run the universal gate. In a temporary root inject two gaps, force the oldest to fail, advance
the latest session, restart the controller and prove the latest publishes while both repair records
remain truthful. Record exact evidence in `release-2-r2f1.md`.

**Step 5: Commit**

```bash
git add backend/app/market/models.py backend/app/api/market.py backend/app/cli.py \
  tests/test_market_get_read_only.py tests/test_market_continuity.py \
  docs/acceptance/release-2-r2f1.md docs/market-data.md
git commit -m "feat(api): expose market continuity and repair status"
```

**R2-F1 rollback:** disable repair execution while retaining inventory/status. Existing freshness
scheduling remains usable; do not delete repair tables or audit history.

---

## R2-F2 — Provider Evidence Framework

**Dedicated specification:** [R2-F2 Provider Evidence Framework Design](2026-08-21-stock-eva-r2f2-provider-evidence-design.md)<br>
**Dedicated implementation plan:** [R2-F2 Provider Evidence Framework Implementation](2026-08-21-stock-eva-r2f2-provider-evidence-implementation.md)<br>
**Specification status:** In Review — Tasks 7–9 remain gated until an independent review changes
the dedicated design to Approved. These documents add exact contracts, file whitelists and
evidence gates without changing R2-F1 or R2-F3 completion status.

### Task 7: Introduce provider-neutral contracts and a BaoStock compatibility adapter

**Files:**

- Create: `backend/app/market/providers/__init__.py`
- Create: `backend/app/market/providers/base.py`
- Create: `backend/app/market/providers/baostock.py`
- Modify: `backend/app/market/baostock.py`
- Modify: `backend/app/market/models.py`
- Create: `tests/test_market_provider_contract.py`
- Modify: `tests/test_market_data.py`

**Step 1: Write failing protocol/compatibility tests**

Define source-shaped and canonical-boundary models:

```python
class ProviderRequest(BaseModel):
    provider_id: str
    trade_date: date
    universe_id: str
    symbols: tuple[str, ...]

class ProviderRawBatch(BaseModel):
    request: ProviderRequest
    adapter_version: str
    endpoint_contract_version: str
    started_at: datetime
    completed_at: datetime
    source_schema: tuple[str, ...]
    units: dict[str, str]
    date_semantics: str
    rows: tuple[dict[str, object], ...]
    failed_symbols: tuple[str, ...] = ()

class DailyBarProvider(Protocol):
    provider_id: str
    def fetch_raw(self, request: ProviderRequest) -> ProviderRawBatch: ...
    def normalize(self, batch: ProviderRawBatch) -> ProviderBatch: ...
```

Test lowercase slug provider IDs, timezone-aware timestamps, exact requested symbols, finite typed
values, no secret/header fields and immutable tuples. Test that the compatibility adapter makes the
same provider calls and produces the same `DailyBar` models as the current direct BaoStock path.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_provider_contract.py tests/test_market_data.py \
  -k 'provider_contract or compatibility or fetch_raw' \
  --basetemp=/tmp/stock-eva-r2f2-provider-red
```

Expected: provider package and source-shaped contract are absent.

**Step 3: Add the narrow boundary without moving the incumbent**

Keep `backend/app/market/baostock.py` as the tested transport/parser. Add a compatibility adapter
that calls it exactly once and converts its captured source rows into `ProviderRawBatch`; during the
transition, existing `BaoStockProvider.fetch()` delegates through the adapter normalization path.

Provider ID validation must not be an open arbitrary string. Add a small `ProviderId` value model
and a registry; do not add a generic plugin loader, entry points or dynamic imports.

Do not alter publication, scheduler or API behavior in this task.

**Step 4: Run GREEN and current regression suite**

```bash
uv run --extra dev pytest -q tests/test_market_provider_contract.py tests/test_market_data.py \
  tests/test_market_reliability.py tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r2f2-provider-green
uv run --extra dev ruff check backend/app/market/providers backend/app/market/baostock.py \
  backend/app/market/models.py tests/test_market_provider_contract.py tests/test_market_data.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/market/providers backend/app/market/baostock.py \
  backend/app/market/models.py tests/test_market_provider_contract.py tests/test_market_data.py
git commit -m "refactor(market): add provider-neutral daily-bar contract"
```

---

### Task 8: Publish immutable raw evidence and replay it offline

**Files:**

- Modify: `backend/app/config.py`
- Modify: `backend/app/storage/layout.py`
- Create: `backend/app/market/evidence.py`
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_market_provider_evidence.py`
- Modify: `tests/test_market_get_read_only.py`

**Step 1: Write failing evidence-store tests**

Cover:

```python
def test_raw_evidence_is_content_addressed_and_idempotent(tmp_path): ...
def test_same_request_with_changed_bytes_creates_distinct_evidence_not_overwrite(tmp_path): ...
def test_manifest_rejects_hash_schema_provider_or_universe_mismatch(tmp_path): ...
def test_evidence_never_serializes_token_headers_cookie_or_local_root(tmp_path): ...
def test_offline_replay_performs_zero_network_calls_and_matches_online_candidate(tmp_path): ...
def test_evidence_reader_missing_root_is_write_free(tmp_path): ...
def test_interrupted_evidence_publish_exposes_no_partial_object(tmp_path): ...
```

Use source-shaped Parquet for row evidence and JSON for metadata. Canonical JSON is UTF-8, sorted
keys and compact separators. All stored paths are relative to the evidence root.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_provider_evidence.py \
  --basetemp=/tmp/stock-eva-r2f2-evidence-red
```

Expected: evidence store/replay CLI do not exist.

**Step 3: Implement evidence publication**

Add a dedicated local evidence root and staging directory in settings/layout. Reuse the existing
immutable publication primitives and hashing conventions; do not overload the canonical dataset
manifest. Required sequence:

```text
fetch raw in memory
-> validate/redact metadata
-> write .partial evidence object
-> read back schema/row count/hash
-> atomic object rename
-> atomic evidence manifest publish
-> normalize only from the published evidence reader
```

Add CLI:

```text
market-provider-replay --evidence-id ID [--compare-candidate-sha SHA]
```

It never accepts provider credentials and performs no network call or canonical publish.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_provider_evidence.py \
  tests/test_market_get_read_only.py -k 'evidence or representative_market_gets' \
  --basetemp=/tmp/stock-eva-r2f2-evidence-green
uv run --extra dev ruff check backend/app/market/evidence.py backend/app/config.py \
  backend/app/storage/layout.py backend/app/market/automation.py backend/app/cli.py \
  tests/test_market_provider_evidence.py tests/test_market_get_read_only.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/config.py backend/app/storage/layout.py backend/app/market/evidence.py \
  backend/app/market/automation.py backend/app/cli.py tests/test_market_provider_evidence.py \
  tests/test_market_get_read_only.py
git commit -m "feat(market): persist replayable provider evidence"
```

---

### Task 9: Version candidate and selection manifests and migrate source fields

**Files:**

- Modify: `backend/app/market/models.py`
- Modify: `backend/app/market/store.py`
- Modify: `backend/app/storage/dataset.py`
- Modify: `backend/app/storage/publication.py`
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/market/service.py`
- Modify: `backend/app/market/series.py`
- Modify: `backend/app/analysis/models.py`
- Modify: `backend/app/alert/models.py`
- Modify: `backend/app/user/models.py`
- Modify: `backend/app/api/market.py`
- Create: `tests/test_market_candidate_selection.py`
- Modify: `tests/test_market_data.py`
- Modify: `tests/test_market_get_read_only.py`

**Step 1: Write failing legacy/new compatibility tests**

Create real legacy fixtures from the current schemas and assert:

```python
def test_legacy_baostock_rows_and_manifests_decode_without_rewrite(tmp_path): ...
def test_new_candidate_references_exact_evidence_universe_and_gate_hashes(tmp_path): ...
def test_selection_references_one_complete_candidate_only(tmp_path): ...
def test_canonical_partition_rejects_more_than_one_provider(tmp_path): ...
def test_failed_selection_preserves_old_pointer_and_candidate_history(tmp_path): ...
def test_existing_market_analysis_alert_and_user_json_remain_compatible(tmp_path): ...
```

The new selection record contains:

```python
class SessionSelection(BaseModel):
    selection_id: str
    trade_date: date
    universe_id: str
    selected_candidate_id: str
    selected_provider_id: str
    reason: Literal["primary_ready", "qualified_fallback"]
    fallback_from: str | None
    reconciliation_id: str | None
    selected_at: datetime
```

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_candidate_selection.py \
  tests/test_market_data.py tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r2f2-selection-red
```

Expected: source literals and current manifest schema reject non-BaoStock identity; selection
contracts do not exist.

**Step 3: Implement additive versioned contracts**

- Replace public/internal BaoStock literals only where the value is truly provider identity; keep
  validated provider IDs and do not weaken unrelated literals.
- Decode missing legacy provider as `baostock` and legacy selection reason as `primary_ready` in
  memory. Do not rewrite objects.
- New publications require evidence ID/hash, candidate gate report hash, provider/adapter/schema
  versions and universe ID.
- Enforce one unique provider per canonical partition both before Parquet write and on strict read.
- Save rejected candidates and selection audits outside the canonical current pointer.
- Keep current API response fields; additive provider metadata may be added, but existing `source`
  values for legacy/current BaoStock remain `baostock`.

**Step 4: Run GREEN, migration and version gate**

```bash
uv run --extra dev pytest -q tests/test_market_candidate_selection.py \
  tests/test_market_data.py tests/test_market_get_read_only.py \
  tests/test_security_analysis.py tests/test_market_regime.py tests/test_portfolio_risk.py \
  --basetemp=/tmp/stock-eva-r2f2-selection-green
uv run --extra dev ruff check backend tests/test_market_candidate_selection.py
git diff --check
```

Then run the universal gate and one offline BaoStock evidence replay. Record byte fingerprints of
legacy objects before/after. Create `docs/acceptance/release-2-r2f2.md` with `R2-F2 GO` only if
old readers, new writers and GET no-write checks pass.

**Step 5: Commit**

```bash
git add backend/app/market/models.py backend/app/market/store.py \
  backend/app/storage/dataset.py backend/app/storage/publication.py \
  backend/app/market/automation.py backend/app/market/service.py \
  backend/app/market/series.py backend/app/analysis/models.py \
  backend/app/alert/models.py backend/app/user/models.py backend/app/api/market.py \
  tests/test_market_candidate_selection.py tests/test_market_data.py \
  tests/test_market_get_read_only.py docs/acceptance/release-2-r2f2.md
git commit -m "feat(market): publish provider-backed session selections"
```

**R2-F2 rollback:** set the orchestrator to BaoStock-only compatibility mode and read the last
legacy-compatible canonical pointer. Retain provider evidence/candidates as non-serving audit data.

---

## R2-F3 — Shadow Provider Bake-off

### Task 10: Add provider qualification and secret-safe configuration

**Files:**

- Create: `backend/app/market/providers/registry.py`
- Modify: `backend/app/config.py`
- Modify: `.env.example`
- Modify: `backend/app/cli.py`
- Create: `tests/test_market_provider_registry.py`
- Create: `docs/data-providers.md`

**Step 1: Write failing admission/config tests**

Add state-transition tests for:

```python
AdmissionState = Literal[
    "discovered", "canary", "shadow", "qualified", "failover_enabled", "quarantined"
]
```

Only these transitions are legal:

```text
discovered -> canary -> shadow -> qualified -> failover_enabled
canary/shadow/qualified/failover_enabled -> quarantined
quarantined -> canary only after a new reviewed adapter/contract/terms version
```

Add tests proving:

- Credentials are `SecretStr`/environment-only and absent from model dumps, repr, CLI output, logs,
  evidence and manifests.
- A provider cannot enter `shadow` until terms/use, fields, units, date semantics, quota and data
  retention decisions are recorded.
- A provider cannot enter `qualified` without 20 consecutive successful shadow sessions.
- Changing adapter, endpoint contract, terms hash or reconciliation policy invalidates the old
  qualification window.
- The registry reader does not create a missing DB/file.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_provider_registry.py \
  --basetemp=/tmp/stock-eva-r2f3-registry-red
```

**Step 3: Implement the bounded registry**

Store admission records in the writer-owned market control DB or one explicitly configured local
control DB; use the same choice consistently and document it. Required evidence fields:

```text
provider_id, adapter_version, endpoint_contract_version, terms_reviewed_at,
terms_evidence_hash, intended_use, retention_allowed, credential_mode,
quota_contract, required_fields, unit_contract, admission_state,
window_start, window_end, successful_sessions, quarantined_reason
```

Add only credential variable names to `.env.example`, never examples resembling real tokens. Add
CLI `market-provider-status` as a read-only command.

In `docs/data-providers.md`, record current roles and primary sources. Explicitly state that
TickFlow, Tushare, AKShare/EastMoney and mootdx are not canonical-qualified at document creation.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_provider_registry.py \
  --basetemp=/tmp/stock-eva-r2f3-registry-green
uv run --extra dev ruff check backend/app/market/providers/registry.py backend/app/config.py \
  backend/app/cli.py tests/test_market_provider_registry.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/market/providers/registry.py backend/app/config.py .env.example \
  backend/app/cli.py tests/test_market_provider_registry.py docs/data-providers.md
git commit -m "feat(market): govern provider qualification states"
```

---

### Task 11: Implement explicit-canary TickFlow and Tushare adapters

**Files:**

- Create: `backend/app/market/providers/http.py`
- Create: `backend/app/market/providers/tickflow.py`
- Create: `backend/app/market/providers/tushare.py`
- Modify: `backend/app/market/providers/__init__.py`
- Modify: `backend/app/config.py`
- Modify: `backend/app/cli.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `tests/test_market_provider_tickflow.py`
- Create: `tests/test_market_provider_tushare.py`
- Modify: `docs/data-providers.md`

**Step 1: Freeze external contracts before code**

For each provider, update `docs/data-providers.md` with the current official URLs, API/SDK version,
terms decision, endpoint fields, units, availability, authentication, quotas and required account
tier. Hash or otherwise version the reviewed contract text without storing credentials.

TickFlow must prove unadjusted daily bars, full declared universe, required indexes and either
source factors or enough corporate-action evidence to satisfy the canonical factor contract. Its
free daily endpoint is a canary option, not assumed to meet the full fallback contract.

Tushare must prove date-level `daily`, `adj_factor`, `suspend_d`, `trade_cal` and required index data
under an explicitly approved account/points tier. If the user has not approved account/cost/terms,
leave the adapter tests mocked and the registry state `discovered`; do not request or create an
account.

**Step 2: Write failing adapter tests with captured synthetic responses**

Test successful full-date responses plus timeout, 401/403, 429, 5xx, malformed JSON, missing field,
duplicate symbol/date, wrong date, non-finite values, unexpected unit, partial page/batch and schema
drift. Prove exact request count and that token values never enter exceptions or evidence.

Add provider-specific semantic tests:

```python
def test_tickflow_raw_contract_requests_unadjusted_daily_rows_and_indexes(): ...
def test_tickflow_factor_contract_is_explicit_not_inferred_from_adjusted_close(): ...
def test_tushare_daily_converts_lots_and_thousand_cny_only_in_normalizer(): ...
def test_tushare_suspension_and_factor_calls_share_exact_trade_date(): ...
def test_provider_client_obeys_retry_after_and_hard_request_budget(): ...
```

**Step 3: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py \
  --basetemp=/tmp/stock-eva-r2f3-adapters-red
```

Expected: adapters and the production HTTP dependency do not exist.

**Step 4: Implement minimal injected HTTP adapters**

Promote `httpx` to a direct bounded production dependency unless the provider's reviewed official
SDK is demonstrably required. Do not add both SDKs speculatively. Use one injected client wrapper
with connect/read/write/pool timeouts, a maximum-attempt policy, bounded `Retry-After`, request
counter and sanitized typed failures.

Adapters return source-shaped evidence only. Provider unit conversion, symbol normalization and
canonical semantics belong in `normalize()`, not in HTTP parsing. No adapter publishes canonical
data or changes provider admission state.

Add explicit CLI canary:

```text
market-provider-canary --provider tickflow|tushare --date YYYY-MM-DD
  [--symbols SYMBOL ...] [--execute]
```

Without `--execute`, it reports the planned endpoints/request bound and makes zero network calls or
writes. Executing a canary requires a registry state permitting canary and writes evidence only.

**Step 5: Run GREEN and offline checks**

```bash
uv run --extra dev pytest -q tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py tests/test_market_provider_contract.py \
  tests/test_market_provider_evidence.py \
  --basetemp=/tmp/stock-eva-r2f3-adapters-green
uv run --extra dev ruff check backend/app/market/providers backend/app/config.py \
  backend/app/cli.py tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py
git diff --check
```

**Step 6: Run bounded real canaries only after approval**

Start with required indexes and 5-10 representative stocks: active, suspended, ST, recent listing
and at least one corporate-action history. Compare request counts, response availability and units
to the frozen contract. Then run one full-universe date into an isolated evidence root. Do not move
the canonical pointer or mark the provider qualified.

If either real canary contradicts fields, units, terms or quota assumptions, quarantine that
contract version and stop its branch before shadow scheduling.

**Step 7: Commit**

```bash
git add backend/app/market/providers/http.py backend/app/market/providers/tickflow.py \
  backend/app/market/providers/tushare.py backend/app/market/providers/__init__.py \
  backend/app/config.py backend/app/cli.py pyproject.toml uv.lock \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  docs/data-providers.md
git commit -m "feat(market): add bounded secondary-provider canaries"
```

---

### Task 12: Normalize and reconcile provider candidates

**Files:**

- Create: `backend/app/market/reconciliation.py`
- Modify: `backend/app/market/providers/tickflow.py`
- Modify: `backend/app/market/providers/tushare.py`
- Modify: `backend/app/market/models.py`
- Create: `tests/test_market_reconciliation.py`
- Modify: `tests/test_market_provider_tickflow.py`
- Modify: `tests/test_market_provider_tushare.py`

**Step 1: Write failing normalization tests**

Prove all candidates use the canonical contract:

```text
symbol: sh.600000 / sz.000001
price: unadjusted CNY
volume: shares
amount: CNY
date: exchange trading date
factor: provider-specific raw value plus declared semantics
suspension: explicit state, not inferred solely from zero volume
```

Test Tushare lots -> shares and thousand CNY -> CNY exactly once. Test that TickFlow's declared
units are not multiplied unless its frozen contract requires it. Reject unknown unit/schema rather
than guessing.

**Step 2: Write failing reconciliation tests**

Create `ReconciliationPolicy(version="r2f-v1")` and compare complete provider candidates. Initial
policy, subject to versioned change only after observed calibration:

- Universe/symbol set: exact equality after expected-state exclusions.
- Suspension state and required indexes: exact equality.
- Unadjusted OHLC: within one legal price tick; a wider provider rounding contract must be explicit.
- Volume: at least 99.9% of comparable active symbols within 0.1% relative error after unit
  normalization.
- Amount: at least 99.9% within 0.5% relative error; document known auction/after-hours semantics.
- Adjusted return around corporate actions: within 5 basis points after provider-specific anchoring.
- Non-finite/impossible OHLC/negative activity: self-gate failure, never a tolerated difference.

Test factor anchors with different raw factor values that produce equivalent adjusted returns. Also
test equal factors with divergent adjusted returns fail.

**Step 3: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_reconciliation.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  --basetemp=/tmp/stock-eva-r2f3-reconcile-red
```

**Step 4: Implement a deterministic report**

Add:

```python
class ReconciliationReport(BaseModel):
    reconciliation_id: str
    policy_version: str
    trade_date: date
    universe_id: str
    left_candidate_id: str
    right_candidate_id: str
    status: Literal["ready", "material_mismatch", "unavailable"]
    compared_counts: dict[str, int]
    mismatch_counts: dict[str, int]
    sampled_mismatches: tuple[SanitizedMismatch, ...]
    report_hash: str
```

IDs/hashes derive from semantic inputs, not wall-clock computation time. Bound samples and never
include raw provider payloads or local paths. Store the complete machine report immutably and only a
bounded sanitized summary in control status.

A bad shadow candidate cannot demote a self-valid primary candidate. A material unexplained
mismatch quarantines the secondary contract version.

**Step 5: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_reconciliation.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  --basetemp=/tmp/stock-eva-r2f3-reconcile-green
uv run --extra dev ruff check backend/app/market/reconciliation.py \
  backend/app/market/providers backend/app/market/models.py \
  tests/test_market_reconciliation.py
git diff --check
```

**Step 6: Commit**

```bash
git add backend/app/market/reconciliation.py backend/app/market/providers/tickflow.py \
  backend/app/market/providers/tushare.py backend/app/market/models.py \
  tests/test_market_reconciliation.py tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py
git commit -m "feat(market): reconcile complete provider candidates"
```

---

### Task 13: Run shadow scheduling and produce the qualification report

**Files:**

- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/market/providers/registry.py`
- Modify: `backend/app/api/market.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_market_shadow.py`
- Modify: `tests/test_market_get_read_only.py`
- Create: `docs/acceptance/release-2-r2f3.md`
- Modify: `docs/data-providers.md`

**Step 1: Write failing shadow-isolation tests**

Prove:

```python
def test_shadow_runs_only_after_canonical_attempt_and_never_delays_pointer(tmp_path): ...
def test_shadow_failure_does_not_change_refresh_result_or_canonical_bytes(tmp_path): ...
def test_shadow_writes_evidence_candidate_and_reconciliation_but_no_selection(tmp_path): ...
def test_twenty_consecutive_sessions_are_required_not_twenty_successes_with_gaps(tmp_path): ...
def test_contract_or_policy_change_resets_qualification_window(tmp_path): ...
def test_material_mismatch_quarantines_secondary_not_primary(tmp_path): ...
```

Add status fields for each candidate: admission state, last attempt/success, consecutive sessions,
last failure class, last reconciliation status and quota/rate-limit observations. GET remains
write-free.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_shadow.py \
  tests/test_market_get_read_only.py -k 'shadow or provider_status' \
  --basetemp=/tmp/stock-eva-r2f3-shadow-red
```

**Step 3: Implement bounded shadow work**

Add CLI:

```text
market-provider-shadow --provider PROVIDER --start DATE --end DATE [--execute]
```

Default is a zero-network/zero-write plan. Automatic one-shot shadow work runs only after the
canonical freshness/repair decision is complete and within a configured request/time budget. It
may skip and report `budget_exhausted`; it must never extend the canonical availability deadline.

Registry promotion from `shadow` to `qualified` requires an explicit reviewed CLI action after the
machine 20-session gate passes. No automatic `failover_enabled` transition exists in R2-F3.

**Step 4: Run synthetic GREEN and version gate**

```bash
uv run --extra dev pytest -q tests/test_market_shadow.py \
  tests/test_market_reconciliation.py tests/test_market_provider_registry.py \
  tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r2f3-shadow-green
uv run --extra dev ruff check backend/app/market/automation.py \
  backend/app/market/providers/registry.py backend/app/api/market.py backend/app/cli.py \
  tests/test_market_shadow.py tests/test_market_get_read_only.py
git diff --check
```

Run the universal gate. This is `CODE GO / SHADOW WINDOW PENDING`, not R2-F3 GO.

**Step 5: Execute and observe the real 20-session window**

After approved credentials/terms and one full-date canary, install with canonical failover still
disabled. For every confirmed session record canonical timing, shadow timing, coverage, request
counts, rate-limit events, failure class and reconciliation. Include unsuccessful sessions.

The provider is R2-F3 qualified only if the exact same adapter/contract/policy completes 20
consecutive sessions and passes the roadmap's coverage/timing/reconciliation/terms gates.

**Step 6: Commit final evidence**

```bash
git add backend/app/market/automation.py backend/app/market/providers/registry.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_shadow.py \
  tests/test_market_get_read_only.py docs/acceptance/release-2-r2f3.md \
  docs/data-providers.md
git commit -m "docs(acceptance): qualify R2-F3 shadow provider"
```

**R2-F3 rollback:** disable shadow scheduling and revoke/remove runtime credentials from the local
environment. Retain already published non-secret evidence and reports according to the approved
retention decision. Canonical publication remains BaoStock-only.

---

## R2-F4 — Controlled Failover and Operations

### Task 14: Implement default-off whole-session failover

**Files:**

- Modify: `backend/app/config.py`
- Create: `backend/app/market/failover.py`
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/market/models.py`
- Modify: `backend/app/market/store.py`
- Modify: `backend/app/api/market.py`
- Modify: `.env.example`
- Create: `tests/test_market_failover.py`
- Modify: `tests/test_market_candidate_selection.py`
- Modify: `tests/test_market_automation.py`

**Step 1: Write the complete selection-policy matrix**

Add deterministic cases:

| Primary | Secondary | Admission/config | Expected |
|---|---|---|---|
| Ready | Any | Any | Publish primary |
| Transport/rate/provider unavailable | Ready | Qualified + enabled | Publish whole secondary |
| Coverage/semantic failure | Ready | Qualified + enabled | Publish whole secondary with validation-failure reason |
| Failed | Ready | Qualified but kill switch off | No publish; preserve pointer |
| Failed | Ready | Shadow/unqualified/quarantined | No publish; preserve pointer |
| Failed | Partial/error | Enabled | No publish; preserve pointer |
| Ready but material disagreement | Material mismatch | Any | Publish valid primary; quarantine secondary |
| Both unavailable | None | Any | Repair/retry state only |

For every fallback case assert:

```python
assert set(row.source for row in canonical_rows) == {selected_provider_id}
assert manifest.selected_provider_id == selected_provider_id
assert selection.fallback_from == primary_provider_id
assert selection.fallback_reason is not None
```

Add crash points before evidence, after evidence, after candidate, after selection-object staging and
before/after canonical pointer movement. Only the final pointer decides visibility; restart is
idempotent.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_failover.py \
  tests/test_market_candidate_selection.py tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r2f4-failover-red
```

Expected: no failover policy/config and all publication paths construct BaoStock directly.

**Step 3: Implement the smallest explicit policy**

Add settings with safe defaults:

```text
STOCK_EVA_MARKET_AUTO_FAILOVER_ENABLED=false
STOCK_EVA_MARKET_PROVIDER_PRIORITY=baostock
```

The priority parser validates registered provider IDs and rejects duplicates/unknown providers.
Secrets are configured separately and never included in the priority string.

`SessionSelectionPolicy.select()` receives already validated candidates plus registry snapshots. It
does not fetch, normalize, write or mutate admission state. `MarketPublicationOrchestrator` owns the
sequence:

```text
attempt primary evidence/candidate
-> if primary ready: select primary
-> else if policy permits: obtain exact-date/universe secondary evidence/candidate
-> select one candidate
-> persist selection object
-> invoke existing canonical atomic publication once
```

The secondary request has its own hard request/time budget. Same-day reconciliation may be
`unavailable` when the primary has no candidate; selection records that fact. Historical
qualification plus the secondary's self-gates are mandatory.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_failover.py \
  tests/test_market_candidate_selection.py tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r2f4-failover-green
uv run --extra dev ruff check backend/app/market/failover.py backend/app/market/automation.py \
  backend/app/market/models.py backend/app/market/store.py backend/app/api/market.py \
  backend/app/config.py tests/test_market_failover.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/config.py backend/app/market/failover.py \
  backend/app/market/automation.py backend/app/market/models.py backend/app/market/store.py \
  backend/app/api/market.py .env.example tests/test_market_failover.py \
  tests/test_market_candidate_selection.py tests/test_market_automation.py
git commit -m "feat(market): select qualified whole-session fallback"
```

---

### Task 15: Add versioned runtime calendar generations and next-year maintenance

**Files:**

- Modify: `backend/app/market/calendar.py`
- Modify: `backend/app/market/calendar_sync.py`
- Modify: `backend/app/market/models.py`
- Modify: `backend/app/api/market.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_market_calendar_generation.py`
- Modify: `tests/test_market_automation.py`
- Modify: `docs/market-data.md`

**Step 1: Write failing calendar-generation tests**

Cover:

```python
def test_promoted_runtime_generation_extends_bundled_calendar_without_rewrite(tmp_path): ...
def test_conflicting_sse_szse_or_machine_sources_quarantine_candidate(tmp_path): ...
def test_missing_next_year_is_informational_before_policy_date_and_actionable_after(tmp_path): ...
def test_unknown_year_never_falls_back_to_weekdays(tmp_path): ...
def test_calendar_reader_missing_store_is_write_free(tmp_path): ...
def test_calendar_generation_is_immutable_idempotent_and_hash_verified(tmp_path): ...
```

Use policy milestones, not a hard-coded assumption that official notices exist on a specific day:

```text
October 1: next-year pending status becomes visible
December 15: missing next-year calendar becomes actionable/blocks future-year readiness
Before first next-year session: a verified promoted generation is mandatory
```

The source metadata must preserve official SSE/SZSE notice URLs/numbers and machine-provider
contract versions. Tests use fixtures and make no network requests.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_calendar_generation.py \
  tests/test_market_automation.py -k 'calendar or unknown_year' \
  --basetemp=/tmp/stock-eva-r2f4-calendar-red
```

**Step 3: Implement additive generations**

Store immutable calendar generation metadata/objects in the existing calendar control area. The
reader composes bundled years with the latest promoted runtime generation. Promotion requires:

- exact date range/year and sorted unique dates;
- SSE/SZSE schedule reconciliation under the documented A-share calendar rule;
- no removal/change of an already completed confirmed session;
- machine-provider cross-check or an explicit unavailable result;
- object hash/readback and source metadata.

Extend the existing calendar LaunchAgent one-shot workflow; do not add a sixth LaunchAgent. A
network fetch writes a candidate first and promotes only after all checks.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_calendar_generation.py \
  tests/test_market_automation.py tests/test_fund_flow_evidence.py \
  -k 'calendar or continuity or trading_session' \
  --basetemp=/tmp/stock-eva-r2f4-calendar-green
uv run --extra dev ruff check backend/app/market/calendar.py \
  backend/app/market/calendar_sync.py backend/app/market/models.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_calendar_generation.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/market/calendar.py backend/app/market/calendar_sync.py \
  backend/app/market/models.py backend/app/api/market.py backend/app/cli.py \
  tests/test_market_calendar_generation.py tests/test_market_automation.py \
  docs/market-data.md
git commit -m "feat(calendar): promote versioned next-year sessions"
```

---

### Task 16: Build the versioned Universe Contract and schedule classification maintenance

**Files:**

- Create: `backend/app/market/universe.py`
- Modify: `backend/app/classification/sync.py`
- Modify: `backend/app/classification/store.py`
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/market/models.py`
- Modify: `backend/app/api/market.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_market_universe.py`
- Modify: `tests/test_point_in_time_classification.py`
- Modify: `tests/test_market_failover.py`

**Step 1: Write failing universe tests**

Define:

```python
ExpectedTradingState = Literal[
    "trading", "suspended", "not_yet_listed", "delisted", "unknown"
]

class UniverseContract(BaseModel):
    universe_id: str
    scope: Literal["all-main-board-plus-required-symbols"]
    trade_date: date
    classification_generation_id: str
    instrument_evidence_ids: tuple[str, ...]
    required_indexes: tuple[str, ...]
    required_user_symbols: tuple[str, ...]
    members: tuple[UniverseMember, ...]
    content_hash: str
```

Test Shanghai/Shenzhen main-board inclusion, ChiNext/STAR exclusion under the current product scope,
required indexes, user-held/watchlist additions, IPO/list/delist effective dates, ST flags,
suspension, unknown state and count reconciliation.

Required count equation:

```text
total = trading + suspended + not_yet_listed + delisted + unknown
publishable requires unknown == 0
loaded = trading + suspended for members expected in the session candidate
```

Test a primary provider outage can still use an already promoted exact-session contract; stale or
wrong-date contracts cannot be widened silently. Test that 3,195 canonical symbols and about 5,205
classification-eligible symbols are reported as different scopes, not a coverage defect by itself.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_universe.py \
  tests/test_point_in_time_classification.py tests/test_market_failover.py \
  -k 'universe or classification_maintenance or expected_state' \
  --basetemp=/tmp/stock-eva-r2f4-universe-red
```

**Step 3: Implement immutable exact-session contracts**

Build contracts from a promoted point-in-time classification/security-master generation, reviewed
provider instrument evidence, effective listing windows and existing required-symbol collection.
Do not query the private user DB from public GETs; the writer pipeline obtains only the existing
allowlisted symbol set.

Promote a contract only when all sources/date semantics reconcile and `unknown == 0`. The market
fetch/gate consumes the contract instead of asking each provider to define the canonical universe.
Provider rows outside the contract are recorded as extras and rejected from canonical data.

Add maintenance work to the existing one-shot orchestration with lowest priority after freshness,
repair and due shadow work. Run a classification/security-master plan on a declared cadence and
when instrument evidence detects a version change. Maintenance failure is observable and does not
rewrite the previous generation; an exact-session universe that cannot be established blocks that
session rather than guessing.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_market_universe.py \
  tests/test_point_in_time_classification.py tests/test_market_failover.py \
  --basetemp=/tmp/stock-eva-r2f4-universe-green
uv run --extra dev ruff check backend/app/market/universe.py \
  backend/app/classification/sync.py backend/app/classification/store.py \
  backend/app/market/automation.py backend/app/market/models.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_universe.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/market/universe.py backend/app/classification/sync.py \
  backend/app/classification/store.py backend/app/market/automation.py \
  backend/app/market/models.py backend/app/api/market.py backend/app/cli.py \
  tests/test_market_universe.py tests/test_point_in_time_classification.py \
  tests/test_market_failover.py
git commit -m "feat(market): publish exact-session universe contracts"
```

---

### Task 17: Add local-to-NAS replication outbox and verified restore

**Files:**

- Modify: `backend/app/config.py`
- Modify: `backend/app/storage/layout.py`
- Create: `backend/app/storage/replication.py`
- Modify: `backend/app/storage/mirror.py`
- Modify: `backend/app/market/automation.py`
- Modify: `backend/app/api/storage.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_dataset_replication.py`
- Modify: `tests/test_nas_dataset.py`
- Modify: `scripts/stock_eva_launchagents_install.sh`
- Modify: `docs/nas-storage.md`

**Step 1: Write failing outbox/replication tests**

Cover:

```python
def test_local_publication_enqueues_replication_after_pointer_commit(tmp_path): ...
def test_nas_unavailable_does_not_change_local_ready_result_or_pointer(tmp_path): ...
def test_replication_copies_immutable_objects_then_atomically_publishes_manifest(tmp_path): ...
def test_interrupted_copy_leaves_no_visible_nas_generation(tmp_path): ...
def test_same_generation_replication_is_idempotent(tmp_path): ...
def test_older_nas_manifest_cannot_overwrite_newer_local_or_reverse(tmp_path): ...
def test_restore_to_temporary_root_verifies_hash_schema_counts_and_api(tmp_path): ...
def test_replication_status_exposes_lag_not_paths_or_mount_credentials(tmp_path): ...
```

Outbox states:

```text
pending -> copying -> verifying -> replicated
pending/copying/verifying -> retry_wait -> dead_letter
```

The local publication transaction is complete before enqueue. A failed enqueue is separately
observable and must not roll back an already visible canonical pointer.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_dataset_replication.py tests/test_nas_dataset.py \
  --basetemp=/tmp/stock-eva-r2f4-replication-red
```

**Step 3: Implement direction-safe replication**

Keep the current NAS-to-local mirror CLI compatible. Add a separate replication service that:

1. validates source generation and all immutable object hashes;
2. copies missing objects to a destination staging root;
3. reads back size/schema/row count/hash;
4. atomically promotes the destination manifest/pointer;
5. marks the outbox replicated only after destination readback.

Add dry-run-first commands:

```text
market-replicate --destination /absolute/nas/dataset [--execute]
market-restore --source /absolute/nas/dataset --destination /absolute/temp/root [--execute]
```

Validate explicit paths and sentinel/mount type before writes. Never infer a destination from `~`,
`$HOME`, `/` or an unresolved environment variable. Never mount SMB or store credentials.

The LaunchAgent can drain the outbox only when the configured destination is already accessible
under the approved macOS permission model. Otherwise it reports backlog and the interactive CLI is
the supported path. Do not weaken TCC or add a credentialed network transport in this task.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_dataset_replication.py tests/test_nas_dataset.py \
  --basetemp=/tmp/stock-eva-r2f4-replication-green
uv run --extra dev ruff check backend/app/storage/replication.py \
  backend/app/storage/mirror.py backend/app/market/automation.py \
  backend/app/api/storage.py backend/app/cli.py tests/test_dataset_replication.py \
  tests/test_nas_dataset.py
git diff --check
```

**Step 5: Run an approved real copy and temporary restore drill**

Use the canonical NAS mount only after a cleaned-up read/write permission probe. Replicate one
generation, then restore to a new temporary directory created specifically for the drill. Run the
existing immutable dataset acceptance and representative API reads against that temporary root.
Remove only the validated temporary restore directory after recording hashes/results; preserve the
NAS generation and local canonical data.

**Step 6: Commit**

```bash
git add backend/app/config.py backend/app/storage/layout.py \
  backend/app/storage/replication.py backend/app/storage/mirror.py \
  backend/app/market/automation.py backend/app/api/storage.py backend/app/cli.py \
  tests/test_dataset_replication.py tests/test_nas_dataset.py \
  scripts/stock_eva_launchagents_install.sh docs/nas-storage.md
git commit -m "feat(storage): replicate and restore immutable market generations"
```

---

### Task 18: Add operator status, forced-failure drill and close R2-F4

**Files:**

- Modify: `backend/app/market/models.py`
- Modify: `backend/app/api/market.py`
- Modify: `backend/app/api/storage.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_market_operations.py`
- Modify: `tests/test_market_get_read_only.py`
- Create: `docs/runbooks/market-data-reliability.md`
- Create: `docs/acceptance/release-2-r2f4.md`

**Step 1: Write failing consolidated-status tests**

Expose additive, bounded sections:

```text
continuity: latest/oldest gap/count/active lane
provider: primary, enabled fallback, admission state, last failure/reconciliation
calendar: promoted version, covered through, next-year status/conflict
universe: universe_id, scope, total/state counts, unknown count
replication: last generation, pending count, lag sessions, last failure class
```

Test missing/corrupt/locked stores independently: one unavailable subsystem does not crash the
whole status endpoint, and GET creates/modifies nothing.

Add a test-only/injected drill harness; do not expose a public HTTP endpoint that can fail providers
or mutate production. CLI drill must require explicit temporary roots and `--execute`.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_market_operations.py \
  tests/test_market_get_read_only.py -k 'operations or reliability_status or drill' \
  --basetemp=/tmp/stock-eva-r2f4-operations-red
```

**Step 3: Implement status and runbook**

Write `docs/runbooks/market-data-reliability.md` with symptom -> status fields -> safe diagnostics ->
decision -> recovery -> verification for:

- primary outage;
- secondary outage/quarantine;
- missing date/repair dead letter;
- schema/reconciliation drift;
- calendar missing/conflict;
- universe unknown/count mismatch;
- local object/manifest corruption;
- NAS unavailable/replication lag;
- rollback to primary-only and prior installed release.

Commands default to plan/read mode and must identify all production-write commands visibly.

**Step 4: Run the R2-F4 gate**

Run focused GREEN, universal verification and an isolated forced-primary-failure drill. Then, after
explicit deployment approval and with the kill switch initially off, install/read back the runtime.
Enable failover only for the supervised drill, force a safe injected primary failure, verify one
whole-session secondary publication, then return configuration to the approved steady state.

If no secondary has R2-F3 GO, R2-F4 is automatically NO-GO; code completion is not enough.

**Step 5: Commit**

```bash
git add backend/app/market/models.py backend/app/api/market.py \
  backend/app/api/storage.py backend/app/cli.py tests/test_market_operations.py \
  tests/test_market_get_read_only.py docs/runbooks/market-data-reliability.md \
  docs/acceptance/release-2-r2f4.md
git commit -m "docs(acceptance): close controlled market failover"
```

**R2-F4 rollback:** set `STOCK_EVA_MARKET_AUTO_FAILOVER_ENABLED=false`, restore the previous
provider priority, reinstall the prior reviewed runtime if required and verify the last trusted
canonical pointer. Never delete the fallback session; immutable history records which provider was
selected.

---

## R2-F5 — Production Soak and Release 2 Re-entry

### Task 19: Implement the read-only R2-F acceptance harness

**Files:**

- Create: `backend/app/market/reliability_acceptance.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_r2f_acceptance.py`
- Modify: `tests/test_market_get_read_only.py`
- Modify: `docs/runbooks/market-data-reliability.md`

**Step 1: Write failing metric and mutation-boundary tests**

Define a validated report:

```python
class R2FAcceptanceReport(BaseModel):
    status: Literal["ready", "not_ready", "unavailable"]
    window_start: date | None
    window_end: date | None
    selected_sessions: tuple[date, ...]
    frozen_versions: FrozenReliabilityVersions
    continuity: MetricResult
    next_morning_availability: MetricResult
    same_evening_availability: MetricResult
    coverage: MetricResult
    source_purity: MetricResult
    provenance: MetricResult
    replay: MetricResult
    calendar: MetricResult
    universe: MetricResult
    replication: MetricResult
    restore: MetricResult
    quality_issues: tuple[str, ...]
```

Test exactly 19 vs 20 sessions, a missing middle session, late publication at 21:16, next-day
08:01, provider/adapter/policy change mid-window, missing selection/evidence hash, mixed-source
partition, unreconciled universe counts, unknown calendar, replication lag, missing restore drill
and corrupt immutable objects.

Fingerprint all input roots/DB files before and after success/error reports. The acceptance reader
must use captured immutable snapshots/read-only connections and must not initialize a missing
control DB.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_r2f_acceptance.py \
  tests/test_market_get_read_only.py -k 'r2f or reliability_acceptance' \
  --basetemp=/tmp/stock-eva-r2f5-acceptance-red
```

Expected: acceptance service and CLI are absent.

**Step 3: Implement deterministic read-only evaluation**

Add CLI:

```text
r2f-acceptance --start YYYY-MM-DD --end YYYY-MM-DD
  [--local-dataset-root ABSOLUTE_PATH]
  [--evidence-root ABSOLUTE_PATH]
```

It has no `--execute` because it never writes. Validate all roots before reads, select only confirmed
calendar sessions, require one frozen version set and calculate the exact roadmap targets:

```text
20/20 next-morning by 08:00 Asia/Shanghai
at least 18/20 same-evening by 21:15 Asia/Shanghai
zero missing sessions
100% legal universe coverage
zero mixed-source canonical partitions
complete evidence/candidate/selection/hash lineage
```

Replay validation reads a bounded deterministic sample and invokes only offline evidence replay.
Restore evidence is consumed as an immutable drill record; the acceptance GET/CLI does not initiate
a restore.

**Step 4: Run GREEN, full suite and performance check**

```bash
uv run --extra dev pytest -q tests/test_r2f_acceptance.py \
  tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r2f5-acceptance-green
uv run --extra dev ruff check backend/app/market/reliability_acceptance.py \
  backend/app/cli.py tests/test_r2f_acceptance.py tests/test_market_get_read_only.py
git diff --check
```

Then run the universal gate. Run the 20-session synthetic report repeatedly and record bounded
runtime; do not set a production target from an unrepresentative tiny fixture.

**Step 5: Commit**

```bash
git add backend/app/market/reliability_acceptance.py backend/app/cli.py \
  tests/test_r2f_acceptance.py tests/test_market_get_read_only.py \
  docs/runbooks/market-data-reliability.md
git commit -m "feat(acceptance): audit R2-F reliability window"
```

---

### Task 20: Freeze and observe the installed 20-session production window

**Files:**

- Create: `docs/acceptance/release-2-r2f.md`
- Modify: `docs/acceptance/release-2-r2f4.md`
- Modify if evidence changes: `docs/data-providers.md`
- Modify if operations change: `docs/runbooks/market-data-reliability.md`

**Step 1: Pass pre-soak GO/NO-GO**

Require R2-F0 through R2-F4 acceptance GO, one `qualified` secondary, reviewed terms/credentials,
full test suite, exact installed release readback and a green supervised failover drill. If any are
missing, do not start the official clock.

Record the frozen set:

```text
Git commit and installed RELEASE.json
canonical/evidence/calendar/universe schema versions
primary and secondary provider IDs
adapter and endpoint contract versions
reconciliation and selection policy versions
calendar generation and universe generation
auto-failover setting and provider priority
continuity start date and repair policy
```

**Step 2: Capture one immutable daily observation**

After every confirmed trading session, use read-only commands to record:

- target session and published time;
- canonical provider, universe/coverage and source purity;
- freshness/repair queue state;
- primary/secondary attempts, failure classes, quotas and reconciliation;
- evidence/candidate/selection hashes;
- calendar/universe versions;
- replication state and lag.

The acceptance document may summarize results in a table but references immutable IDs/hashes for
detail. Do not paste tokens, raw responses or absolute sensitive paths.

**Step 3: Handle changes and incidents honestly**

- A material provider adapter/endpoint, reconciliation policy, calendar semantics or universe
  contract change restarts the relevant 20-session window.
- A provider outage does not restart the window if the frozen fallback policy handles it and all
  canonical gates pass; it is positive failover evidence.
- A missing/late session stays in the matrix and makes the target fail. Repairing it later proves
  recovery but does not rewrite the original availability timestamp.
- A non-trading day is not counted.

**Step 4: Run bounded failure drills**

Within the window or immediately after it, run approved drills using injected/provider-safe
mechanisms:

1. Primary transport unavailable -> qualified secondary whole-session publish.
2. Secondary unavailable -> primary unaffected or no fallback when primary also unavailable.
3. Repair process interruption/restart -> exact-once recovery.
4. Corrupt candidate/hash -> pointer unchanged.
5. Calendar unknown/conflict -> no fetch/publish.
6. Universe unknown/count mismatch -> no publish.
7. NAS unavailable -> local ready plus visible backlog.
8. Verified NAS generation -> restore to temporary root and representative API readback.

Never intentionally corrupt the active canonical object, production control DB or real NAS
generation. Use injected failures or copied temporary fixtures.

**Step 5: Run the final report**

```bash
uv run python -m backend.app.cli r2f-acceptance \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --local-dataset-root "/absolute/reviewed/local/dataset" \
  --evidence-root "/absolute/reviewed/local/evidence"
```

Replace dates/paths from the frozen live configuration; do not copy this placeholder command
unchanged. Fingerprint protected stores before/after and require byte/metadata equality.

**Step 6: Record the verdict and commit**

`R2-F GO` requires every mandatory roadmap metric and no unresolved P0/P1 issue. Otherwise record
NO-GO with exact failed rows and continue the soak after a reviewed repair/new frozen window.

```bash
git add docs/acceptance/release-2-r2f.md docs/acceptance/release-2-r2f4.md \
  docs/data-providers.md docs/runbooks/market-data-reliability.md
git commit -m "docs(acceptance): record R2-F production soak"
```

---

### Task 21: Re-open Release 2 in dependency order

**Files:**

- Modify: `docs/plans/2026-07-29-stock-eva-roadmap-execution-plan.md`
- Modify only after independent reviews: `docs/acceptance/release-2-r2a.md`
- Modify only after independent reviews: `docs/acceptance/release-2-r2b.md`
- Create only at final Release 2 acceptance: `docs/acceptance/release-2.md`

**Step 1: Verify R2-F GO independently**

A read-only reviewer inspects the exact R2-F commit, installed release, 20-session report, immutable
IDs and runtime status. Do not use this implementation plan or a worker's completion statement as
acceptance evidence.

**Step 2: Re-run R2-A against real provider evidence**

Keep L1/L2/L3 semantics separate. Run real bounded canaries and prove at least 20 continuous
sessions for any trend language. Historical visibility and publication manifests must prove what
was knowable at each `as_of`; otherwise return degraded/unverifiable. Do not derive fund flow from
OHLCV/amount.

**Step 3: Re-review and accept R2-B**

Verify deterministic portfolio inputs, exact market/classification version references, missing-data
degradation, GET no-write and no broker/order behavior at the integrated commit.

**Step 4: Unblock R2-C, then R2-D, then R2-E**

Only when R2-F, R2-A and R2-B each have independent GO:

```text
R2-C EvidencePack/daily review
-> R2-D integrated workspace
-> R2-E Release 2 continuity/browser acceptance
```

Release 2 GO still requires `docs/acceptance/release-2.md`; R2-F GO alone is not Release 2 GO.

**Step 5: Commit the sequencing/evidence update**

Stage only files whose reviews actually completed. Do not pre-write GO verdicts.

---

## Task dependency map

```text
R2-F0: Task 0 -> 1 -> 2 -> 3
R2-F1: Task 4 -> 5 -> 6
R2-F2: Task 7 -> 8 -> 9
R2-F3: Task 10 -> 11 -> 12 -> 13 -> 20-session shadow observation
R2-F4: Task 14 -> 15 -> 16 -> 17 -> 18
R2-F5: Task 19 -> 20 -> 21
```

Task 15 (calendar) and Task 17 (replication) may be developed in separate isolated worktrees after
Task 14's shared contracts are reviewed, but they must be integrated serially and re-run the full
version gate. Task 16 owns the shared market/classification contract and must not be parallel-edited
with other tasks touching those files.

## Planned commit boundaries

| Version | Planned bounded commits | Required acceptance commit |
|---|---:|---:|
| R2-F0 | Incident RED, suspension fix, failure taxonomy | Supervised repair evidence |
| R2-F1 | Repair store, lane scheduler, status/CLI | Restart/gap matrix |
| R2-F2 | Provider contract, evidence/replay, selection migration | Legacy/new compatibility |
| R2-F3 | Registry, canary adapters, reconciliation, shadow scheduler | 20-session qualification |
| R2-F4 | Failover, calendar, universe, replication, operations | Forced failover/restore |
| R2-F5 | Acceptance harness | 20-session production soak |

Acceptance evidence should be committed separately from implementation when it depends on a later
installed runtime or observation window.

## Production mutation gates

| Action | Default | Additional authority/evidence required |
|---|---|---|
| Unit/integration tests in temporary roots | Allowed during implementation | Task branch and RED/GREEN protocol |
| Provider contract/terms web review | Read-only | Official source links and review timestamp |
| Real provider canary | Blocked | User approval of network/credentials/cost/terms and bounded request plan |
| Install LaunchAgents/runtime | Blocked | Reviewed version commit and explicit deployment approval |
| Repair a production date | Blocked | R2-F0 code GO, live pre-state, exact date/scope confirmation |
| Enable automatic fallback | Blocked/default off | R2-F3 GO, R2-F4 code review, supervised drill plan |
| Write to NAS | Blocked | Verified explicit mount/path, permission probe, dry-run diff |
| Delete temporary restore data | Blocked until verified | Exact temp root, recorded acceptance, recoverability decision |

## Deferred work after R2-F

- CNINFO point-in-time event layer for suspension/announcement/fundamental events.
- Broad AKShare supplemental enablement beyond already reviewed R2-A evidence contracts.
- Tertiary mootdx/TDX or AData canaries after one secondary has proven stable.
- Point-in-time financial statement ingestion.
- New R2-C/D product surfaces and all R3 strategy work until their upstream GO gates permit them.
- Any commercial redistribution or multi-user service model; provider terms must be reviewed again
  if the personal local-research boundary changes.

## Final implementation checklist

- [ ] R2-F0 closes the incident without symbol allowlists or relaxed active-stock factor gates.
- [x] R2-F1 repairs missing confirmed sessions after restart without starving freshness; offline
  code gate is GO at 336107be1149d829c0ea841dd2466982bff7d689.
- [ ] R2-F2 replays immutable raw evidence and preserves all legacy canonical objects/readers.
- [ ] R2-F3 qualifies at least one secondary over 20 consecutive trading sessions.
- [ ] R2-F4 demonstrates default-off, whole-session failover with zero mixed-source rows.
- [ ] Calendar covers the next required year and unknown/conflict remains fail-closed.
- [ ] Exact-session universe counts reconcile and unknown state is zero for publication.
- [ ] NAS outage cannot block local publication; replication lag is visible; restore is verified.
- [ ] Provider errors are structured and sanitized; credentials never enter evidence/status/logs.
- [ ] R2-F5 passes every mandatory SLO over one frozen 20-session production window.
- [ ] R2-A, R2-B and Release 2 retain independent GO/NO-GO decisions.
