# Stock EVA R1-E Release 1 Closure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans and
> superpowers:test-driven-development to implement this plan task-by-task.

**Goal:** Persist and expose immutable daily market-regime snapshots, automate future captures,
backfill the verified history, and produce a truthful Release 1 no-future completion audit.

**Architecture:** Add one dedicated local SQLite derived-snapshot store with explicit writer and
read-only reader lifecycles. Bind snapshot generation to one checksum-verified immutable market
publication, integrate exact-day capture into the existing after-close pipeline, and keep the
existing on-demand regime API unchanged. A separate read-only R1-E audit reconciles at least 20
persisted results with deterministic recomputation and point-in-time classification evidence.

**Tech Stack:** Python 3.12, Pydantic 2, SQLite, FastAPI, immutable Parquet/DuckDB readers, pytest,
Ruff, TypeScript/Vite and installed-browser acceptance.

**Authoritative spec:**
`docs/plans/2026-08-10-stock-eva-r1e-release-closure-design.md`

**Execution constraints:** Work only in
`/Users/finlay/.codex/worktrees/r1e-closure/Stock- evaluation` on
`codex/r1-e-release-closure`. Do not access production/NAS/user data, do not modify main, do not
run the full backend suite, and do not start R2. Use temporary directories for every test. One
implementer owns all tasks because the storage, CLI and pipeline changes are tightly coupled.

---

### Task 1: Immutable snapshot model and dedicated SQLite store

**Files:**
- Modify: `backend/app/config.py`
- Modify: `backend/app/storage/layout.py`
- Create: `backend/app/regime/snapshots.py`
- Create: `tests/test_regime_snapshots.py`

**Step 1: Write failing model/store tests**

Add tests that construct a real `MarketRegimeResult` from the existing fixtures and prove:

```python
def test_snapshot_store_inserts_once_and_exact_repeat_is_byte_stable(tmp_path): ...
def test_snapshot_store_rejects_divergent_same_date_without_mutation(tmp_path): ...
def test_snapshot_reader_missing_file_does_not_create_parent(tmp_path): ...
def test_snapshot_reader_rejects_bad_schema_payload_hash_future_lineage_and_lock(tmp_path): ...
def test_snapshot_metadata_never_contains_dataset_path(tmp_path): ...
```

Use a helper request containing `as_of`, capture mode, evidence cutoff, dataset generation,
`sha256(raw_dataset_identity)` and the validated result. Fingerprint the DB plus sidecar inventory
before an idempotent call and before/after each rejected call.

**Step 2: Run RED**

Run:

```bash
uv run --extra dev pytest -q tests/test_regime_snapshots.py \
  --basetemp=/tmp/stock-eva-r1e-snapshot-red
```

Expected: collection fails because `backend.app.regime.snapshots` and the configured snapshot path
do not exist. Record the exact failure.

**Step 3: Implement the minimal snapshot contract**

Add `regime_snapshot_database_name = "market_regime_snapshots.sqlite3"` and a
`StorageLayout.regime_snapshot_database` property. Validate the configured name as a local SQLite
basename without changing private-database validation semantics.

In `snapshots.py`, add:

```python
CaptureMode = Literal["after_close", "post_hoc_backfill"]

class MarketRegimeSnapshot(BaseModel):
    snapshot_id: str
    as_of: date
    formula_version: str
    result_id: str
    data_as_of: date | None
    capture_mode: CaptureMode
    evidence_cutoff_at: datetime
    recorded_at: datetime
    dataset_generation: str
    dataset_identity_hash: str
    content_hash: str
    result: MarketRegimeResult

class SnapshotCaptureRequest(BaseModel): ...
class RegimeSnapshotConflict(RuntimeError): ...
class RegimeSnapshotUnavailable(RuntimeError): ...
class RegimeSnapshotStore:
    def capture(self, request: SnapshotCaptureRequest) -> tuple[MarketRegimeSnapshot, bool]: ...
    def read_exact(self, as_of: date, formula_version: str) -> MarketRegimeSnapshot | None: ...
```

Canonicalize JSON with sorted keys and compact separators. Derive `snapshot_id` from the semantic
key/result/publication lineage, set `recorded_at` only on first insert, and include every persisted
field except `content_hash` in the content hash. Check an existing row through `mode=ro` before
opening a writer so an exact repeat creates no WAL/SHM or metadata mutation. Writer initialization
and insert use one explicit transaction and a UNIQUE `(as_of, formula_version)` constraint. Reader
connections use URI `mode=ro`, `timeout=0`, no PRAGMA migration and no parent creation.

**Step 4: Run GREEN and quality checks**

```bash
uv run --extra dev pytest -q tests/test_regime_snapshots.py \
  --basetemp=/tmp/stock-eva-r1e-snapshot-green
uv run --extra dev ruff check backend/app/regime/snapshots.py backend/app/config.py \
  backend/app/storage/layout.py tests/test_regime_snapshots.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/config.py backend/app/storage/layout.py \
  backend/app/regime/snapshots.py tests/test_regime_snapshots.py
git commit -m "feat(regime): persist immutable daily snapshots"
```

---

### Task 2: Bind capture and backfill to one verified publication

**Files:**
- Modify: `backend/app/storage/dataset.py`
- Modify: `backend/app/regime/store.py`
- Modify: `backend/app/regime/snapshots.py`
- Modify: `tests/test_regime_snapshots.py`
- Modify: `tests/test_market_regime.py`

**Step 1: Write failing publication-binding tests**

Add real temporary Parquet/manifest tests proving one captured publication exposes a validated
generation and ordered unique dates, strict checksums run once for a multi-date capture, every
result is evaluated against the same `PublishedReadSnapshot`, and a manifest/object change before
or during a query fails before insert.

Add a 20-session fixture containing an injected future row/evidence and assert the persisted
payload has zero future input dates and exactly 20 selected manifest dates.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_regime_snapshots.py tests/test_market_regime.py \
  -k 'publication or bound_snapshot or backfill or future' \
  --basetemp=/tmp/stock-eva-r1e-bound-red
```

Expected: failures show `PublishedReadSnapshot` lacks generation/date inventory and the regime
store cannot reuse an explicitly captured snapshot.

**Step 3: Implement the minimal bound reader/capture service**

Extend the internal `PublishedReadSnapshot` with validated `generation` and ordered
`trade_dates`, preserving compatibility for test constructors with safe defaults. Populate both
from the same manifest used for identity/path verification.

Add a `MarketRegimeStore` method that evaluates one `as_of` against a supplied immutable snapshot
without recapturing the manifest. It must reuse the existing filtering, cache and model validation,
not fork the formula.

Add `RegimeSnapshotCaptureService` methods:

```python
def plan(start: date, end: date) -> SnapshotBackfillPlan: ...
def capture_range(start: date, end: date, *, evidence_cutoff_at: datetime) \
        -> SnapshotBackfillOutcome: ...
def capture_after_close(result: RefreshResult) -> MarketRegimeSnapshot: ...
```

The service captures one strict publication snapshot, hashes its raw identity before persistence,
selects only manifest dates in the inclusive range, and uses `post_hoc_backfill` or `after_close`
honestly. Aggregate outcomes use allowlisted counters and dates only.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_regime_snapshots.py tests/test_market_regime.py \
  --basetemp=/tmp/stock-eva-r1e-bound-green
uv run --extra dev ruff check backend/app/storage/dataset.py backend/app/regime/store.py \
  backend/app/regime/snapshots.py tests/test_regime_snapshots.py tests/test_market_regime.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/storage/dataset.py backend/app/regime/store.py \
  backend/app/regime/snapshots.py tests/test_regime_snapshots.py tests/test_market_regime.py
git commit -m "feat(regime): bind snapshot backfill to publication"
```

---

### Task 3: Read-only exact API and explicit CLI

**Files:**
- Modify: `backend/app/api/analysis.py`
- Modify: `backend/app/cli.py`
- Modify: `tests/test_market_regime.py`
- Modify: `tests/test_market_get_read_only.py`
- Create: `tests/test_regime_snapshot_cli.py`

**Step 1: Write failing API/CLI tests**

Cover exact 200, missing 404, future 422 before reader call, corrupt/schema/locked 503, response
path/exception redaction and pre/post database/tree equality. Include the new route in the existing
representative GET immutability matrix.

CLI tests must prove default dry-run creates nothing, invalid/inverted/empty ranges fail before
writer initialization, `--execute` captures exact manifest dates, repeat execution is idempotent,
and output is one JSON line with empty stderr and no path, SQL or injected secret.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_regime_snapshot_cli.py \
  tests/test_market_regime.py tests/test_market_get_read_only.py \
  -k 'snapshot or market_gets_leave' \
  --basetemp=/tmp/stock-eva-r1e-api-red
```

Expected: new endpoint is 404/unregistered, CLI parser rejects the command, and the GET matrix
lacks the new reader.

**Step 3: Implement additive contracts**

Add a read-only dependency and exact path
`GET /analysis/market-regime/snapshots/{as_of}`. Reuse the Shanghai future-date gate. Return only
the validated public model; map missing to `regime_snapshot_not_found` 404 and all store/schema/
payload/lock errors to `regime_snapshot_store_unavailable` 503 without chaining unsafe details.

Add CLI `market-regime-snapshots --start --end [--execute]`. Parse and validate before constructing
the writer. Dry-run returns `writes_snapshot_data=false`. Execute uses the capture service and sets
`writes_snapshot_data=true` only when at least one row is newly inserted. Catch expected typed
failures and emit only allowlisted codes/counters.

Do not change the existing market-regime route or response.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_regime_snapshot_cli.py \
  tests/test_market_regime.py tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r1e-api-green
uv run --extra dev ruff check backend/app/api/analysis.py backend/app/cli.py \
  tests/test_regime_snapshot_cli.py tests/test_market_regime.py \
  tests/test_market_get_read_only.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/api/analysis.py backend/app/cli.py tests/test_regime_snapshot_cli.py \
  tests/test_market_regime.py tests/test_market_get_read_only.py
git commit -m "feat(api): expose persisted regime snapshots"
```

---

### Task 4: Capture future ready days in the after-close pipeline

**Files:**
- Modify: `backend/app/orchestration/after_close.py`
- Modify: `backend/app/orchestration/adapters.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/cli.py`
- Modify: `tests/test_after_close_pipeline.py`
- Modify: `tests/test_after_close_adapters.py`
- Modify: `tests/test_market_automation.py`

**Step 1: Write failing orchestration tests**

Add a `regime` work item before strategy/alert tasks. Prove exact ready publication capture,
restart idempotency, partial/mismatched skip, snapshot failure recorded as a sanitized task error,
private tasks still run, and a failed derived task does not change the ready publication result.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_after_close_pipeline.py \
  tests/test_after_close_adapters.py tests/test_market_automation.py \
  -k 'regime or post_publish or pipeline' \
  --basetemp=/tmp/stock-eva-r1e-pipeline-red
```

Expected: task kind/model and factory runner are missing.

**Step 3: Implement the minimum pipeline extension**

Extend `TaskKind` with `regime`, add an optional `RegimeSnapshotWorkRunner`, and claim one task
using formula-version/date idempotency before strategy and alert work. Persist only the task audit
reference/error in the existing orchestration database; snapshot payload remains in the dedicated
store. A typed snapshot failure marks the task error and permits remaining private tasks to run.

Wire the runner through `build_after_close_pipeline()` in API lifespan and CLI automation. The
factory must use `StorageLayout.regime_snapshot_database` and the same configured immutable market
reader; it must not create the snapshot database until an exact ready post-publication call.

**Step 4: Run GREEN and checks**

```bash
uv run --extra dev pytest -q tests/test_after_close_pipeline.py \
  tests/test_after_close_adapters.py tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r1e-pipeline-green
uv run --extra dev ruff check backend/app/orchestration backend/app/main.py backend/app/cli.py \
  tests/test_after_close_pipeline.py tests/test_after_close_adapters.py \
  tests/test_market_automation.py
git diff --check
```

**Step 5: Commit**

```bash
git add backend/app/orchestration backend/app/main.py backend/app/cli.py \
  tests/test_after_close_pipeline.py tests/test_after_close_adapters.py \
  tests/test_market_automation.py
git commit -m "feat(orchestration): capture ready regime snapshots"
```

---

### Task 5: Deterministic Release 1 replay and coverage audit

**Files:**
- Create: `backend/app/regime/acceptance.py`
- Modify: `backend/app/cli.py`
- Create: `tests/test_release_one_acceptance.py`
- Modify: `docs/market-regime.md`
- Create: `docs/acceptance/release-1.md`

**Step 1: Write failing audit tests**

Build one real temporary immutable dataset with at least 20 dates, a dedicated snapshot DB and
classification stores representing: contemporaneous history, later-observed post-hoc history,
future source/effective dates, coverage above and below 95%, unreconciled counts, unmapped symbols,
missing snapshots and mismatched deterministic results.

Assert `ready` only when all 20 snapshots exist, match recomputation, no future counts are zero and
latest coverage reconciles at or above 95%. Assert later-observed history remains counted as
post-hoc/unverified, not contemporaneous. Every failed gate returns `not_ready` plus sorted
allowlisted issues; it must not mutate any input.

**Step 2: Run RED**

```bash
uv run --extra dev pytest -q tests/test_release_one_acceptance.py \
  --basetemp=/tmp/stock-eva-r1e-acceptance-red
```

Expected: missing `backend.app.regime.acceptance` and CLI command.

**Step 3: Implement the audit and read-only CLI**

Add `ReleaseOneReplayReport` and a service that captures one verified dataset view, selects the
latest requested number of manifest dates (minimum 20), reads each exact snapshot, recomputes with
the same bound view, validates all result/lineage/evidence dates, and reads classification through
one explicit audit cutoff. Compute contemporaneous visibility separately with
`generation.observed_at <= end_of(as_of, Asia/Shanghai)`.

Audit current `baostock.industry_classification` coverage, require reconciled counts and ratio
`>= 0.95`, preserve the complete unmapped-symbol list and explicit component/contemporaneous
limitations. Add read-only CLI `release-one-audit --sessions 20`; it must never initialize a writer
and emits one sanitized JSON line.

Update `docs/market-regime.md` with snapshot semantics. Seed `docs/acceptance/release-1.md` with the
frozen command matrix and placeholders labelled `PENDING ROOT PRODUCTION EVIDENCE`; do not claim GO
or invent runtime results.

**Step 4: Run GREEN and focused closure checks**

```bash
uv run --extra dev pytest -q tests/test_regime_snapshots.py \
  tests/test_regime_snapshot_cli.py tests/test_release_one_acceptance.py \
  tests/test_market_regime.py tests/test_market_get_read_only.py \
  tests/test_after_close_pipeline.py tests/test_after_close_adapters.py \
  tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r1e-focused-final
uv run --extra dev ruff check backend tests
uv run --extra dev ruff format --check \
  backend/app/regime/snapshots.py backend/app/regime/acceptance.py \
  tests/test_regime_snapshots.py tests/test_regime_snapshot_cli.py \
  tests/test_release_one_acceptance.py
git diff --check
```

Do not run the full backend suite; root owns it after independent approval.

**Step 5: Commit and self-review**

```bash
git add backend/app/regime/acceptance.py backend/app/cli.py \
  tests/test_release_one_acceptance.py docs/market-regime.md \
  docs/acceptance/release-1.md
git commit -m "feat(acceptance): verify release one replay"
```

Review the complete range after the approved spec commit. Map every FR/NFR/AC to a test or root-only
production gate, remove any extra behavior, confirm all worktrees and production paths were not
touched, and report commit SHAs plus exact RED/GREEN commands. Stop without declaring R1-E GO.

---

### Task 6: Root-only review, integration and production gate

**Owner:** Root quality lead after the implementer stops.

1. Dispatch one read-only combined spec/code-quality reviewer for the full commit range. The same
   reviewer handles follow-up reviews; the implementer appends fixes. No parallel reviewer.
2. Cherry-pick approved commits to main in order while preserving user-owned dirty files.
3. Verify no concurrent test process, then run exactly one integrated backend full suite, Ruff,
   diff check, frontend full suite and build.
4. Fingerprint the immutable local/NAS dataset, market control DB, classification DB,
   supplemental audit DB and private user DB before installation/backfill.
5. Run official installer; verify `RELEASE.json`, LaunchAgents, API and workspace readiness.
6. Run snapshot dry-run, execute backfill over every manifest date, repeat it for idempotency, run
   `release-one-audit --sessions 20`, exact snapshot API readback and 20-request p95 measurement.
7. Browser-check market → sector → leader → stock → sector at 1440×900 and 390×844, including
   console, context, raw evidence, missing fund flow and narrow-scope copy.
8. Recompute protected fingerprints; only the new derived snapshot DB may change.
9. Replace pending acceptance placeholders with actual commands/results, commit the final evidence,
   remove the clean R1-E worktree and pause for user confirmation before unlocking R2.
