# Stock EVA R2-F4.0 Capability-Gated Selection Contract Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`,
> `superpowers:test-driven-development` and `superpowers:verification-before-completion` task by
> task.

**Goal:** Make it mechanically impossible to treat the narrow R2-F3 Daily Bar qualification as
canonical whole-session failover authority.

**Architecture:** Add one isolated, read-only `failover.py` domain that projects the strict R2-F3
Daily sidecar into a versioned capability matrix and deterministic preflight. Add allowlisted,
default-off configuration plus read-only API/CLI status. Do not modify canonical provider,
candidate, selection, storage or automation paths.

**Tech Stack:** Python 3.12, Pydantic 2, FastAPI, argparse, pytest, SHA-256 canonical JSON.

**Design authority:**
`docs/plans/2026-08-30-stock-eva-r2f4-0-capability-gated-selection-design.md`

**Starting commit:** `8095d54825ddf3e32de783a64d25e8c3fbe547a7`

**Branch:** `codex/r2-f4-0-capability-admission`

**Delivery boundary:** Offline code and read-only local status only. No provider/network request,
credential read, canonical write, installation, NAS operation or failover execution.

---

## Task 1: Add the immutable capability matrix and pure selection shield

**Files:**

- Create: `backend/app/market/failover.py`
- Create: `tests/test_market_failover_readiness.py`

### Step 1.1: Write RED model and policy tests

Add tests proving:

1. a verified `SHADOW_QUALIFIED` Daily status projects Daily Bar only;
2. factor remains `UNQUALIFIED`; units, suspension, universe, calendar and retention remain
   `UNKNOWN`;
3. canonical failover remains `UNQUALIFIED` even when configured auto-failover is true;
4. missing/unavailable Daily state never creates a qualified field;
5. primary ready always produces advisory `PRIMARY_ALLOWED/baostock`;
6. primary unavailable always produces `SECONDARY_BLOCKED/PRESERVE_POINTER`;
7. canonical capability, readiness and decision hashes are deterministic and reject tampering;
8. generic `admission_state=qualified` is not accepted as a capability input.

### Step 1.2: Run RED

```bash
.venv/bin/pytest -q tests/test_market_failover_readiness.py \
  --basetemp=/tmp/stock-eva-r2f4-0-task1-red
```

Expected: collection/import fails because `backend.app.market.failover` does not exist.

### Step 1.3: Implement the minimum pure domain

Implement closed enums/models, domain-separated canonical JSON hashing, Daily status projection,
readiness construction and advisory preflight. Keep v1 canonical eligibility a literal
`UNQUALIFIED`; no code path may construct a qualified secondary or a `PublishedSelection`.

### Step 1.4: Run GREEN and checks

```bash
.venv/bin/pytest -q tests/test_market_failover_readiness.py \
  --basetemp=/tmp/stock-eva-r2f4-0-task1-green
.venv/bin/ruff check backend/app/market/failover.py tests/test_market_failover_readiness.py
.venv/bin/ruff format --check backend/app/market/failover.py tests/test_market_failover_readiness.py
git diff --check
```

### Step 1.5: Commit and review

```bash
git add backend/app/market/failover.py tests/test_market_failover_readiness.py
git commit -m "feat(r2f4): add fail-closed capability readiness"
```

The independent reviewer must first confirm spec compliance and then code quality/security. High or
Medium findings return to the same implementer before Task 2 starts.

---

## Task 2: Add allowlisted configuration and read-only API/CLI wiring

**Files:**

- Modify: `backend/app/config.py`
- Modify: `backend/app/api/market.py`
- Modify: `backend/app/cli.py`
- Modify: `.env.example`
- Modify: `tests/test_market_failover_readiness.py`
- Modify: `tests/test_market_get_read_only.py`

### Step 2.1: Write RED config/status tests

Add tests proving:

- defaults are `false` and `baostock`;
- provider priority requires BaoStock first, unique known providers and no empty/token-like input;
- CLI has no `--execute`, reports zero provider requests/writes and exits 0 for safely blocked ready
  state, 1 for unavailable, 2 for invalid configuration;
- API and CLI read the strict Daily sidecar and external bundles without initialization;
- missing/corrupt/locked state is bounded unavailable;
- ready and unavailable GET/CLI calls preserve complete tree fingerprints, DB hashes and mtimes;
- configured true plus TickFlow priority still reports effective false and no eligible secondary;
- serialized output contains no credential, path, URL, payload, SQL or arbitrary exception text.

### Step 2.2: Run RED

```bash
.venv/bin/pytest -q tests/test_market_failover_readiness.py \
  tests/test_market_get_read_only.py -k 'failover_readiness' \
  --basetemp=/tmp/stock-eva-r2f4-0-task2-red
```

Expected: configuration fields, route and command are absent.

### Step 2.3: Implement minimum wiring

Add the two Settings fields and a credential-free allowlisted CLI settings projection. Reuse
`StorageLayout.validate_daily_bar_shadow_layout()` and
`DailyShadowRegistryReader(verify_external=True)`; do not add an initializer or writer. Expose the
domain readiness model directly as bounded API/CLI JSON.

### Step 2.4: Run GREEN and compatibility checks

```bash
.venv/bin/pytest -q tests/test_market_failover_readiness.py \
  tests/test_market_get_read_only.py \
  tests/test_market_daily_shadow_cli_api.py \
  tests/test_market_candidate_selection.py \
  --basetemp=/tmp/stock-eva-r2f4-0-task2-green
.venv/bin/ruff check backend/app/config.py backend/app/market/failover.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_failover_readiness.py \
  tests/test_market_get_read_only.py
.venv/bin/ruff format --check backend/app/config.py backend/app/market/failover.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_failover_readiness.py \
  tests/test_market_get_read_only.py
git diff --check
```

### Step 2.5: Commit and review

```bash
git add backend/app/config.py backend/app/api/market.py backend/app/cli.py .env.example \
  tests/test_market_failover_readiness.py tests/test_market_get_read_only.py
git commit -m "feat(r2f4): expose failover readiness shield"
```

Review must explicitly confirm no credential lookup, provider call, initializer, writer, canonical
selection or automation integration exists.

---

## Task 3: Freeze compatibility evidence and close R2-F4.0

**Files:**

- Create: `docs/acceptance/release-2-r2f4-0.md`
- Modify: `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md`
- Modify: `docs/plans/2026-08-12-stock-eva-r2f-data-reliability-implementation.md`
- Modify: `docs/data-providers.md`

### Step 3.1: Run the focused R2-F4.0 gate

```bash
.venv/bin/pytest -q tests/test_market_failover_readiness.py \
  tests/test_market_get_read_only.py \
  tests/test_market_candidate_selection.py \
  tests/test_market_daily_shadow_registry.py \
  tests/test_market_daily_shadow_cli_api.py \
  tests/test_r2f2_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f4-0-focused
```

### Step 3.2: Run repository and static gates

```bash
.venv/bin/pytest -q --basetemp=/tmp/stock-eva-r2f4-0-full
.venv/bin/ruff check backend tests
.venv/bin/ruff format --check backend tests
.venv/bin/python -m compileall -q backend
uv run --offline python \
  /Users/finlay/.codex/skills/claude-skills--engineering/spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-08-30-stock-eva-r2f4-0-capability-gated-selection-design.md --strict
git diff --check
```

### Step 3.3: Prove protected state did not change

Verify and record:

- every R2-F2 golden file and `sha256sums.txt` hash;
- the R2-F3 final qualification report SHA-256;
- strict R2-F3 Daily status remains `READY / SHADOW_QUALIFIED / 20/20 / CLOSED`, with publication
  and failover false;
- production canonical manifest SHA-256 and every declared Parquet object hash;
- no production provider-control or canonical file was created/modified by R2-F4.0 tests/status;
- changed-file diff does not include `providers/base.py`, `models.py`, `candidates.py`,
  `automation.py`, `store.py`, `storage/dataset.py` or any canonical data object.

### Step 3.4: Independent final review

The reviewer inspects the exact clean commit, design requirement by requirement, focused/full test
evidence and protected fingerprints. GO requires H=0/M=0 and explicit confirmation that Daily Bar
qualification remains failover-ineligible.

### Step 3.5: Record and commit acceptance

```bash
git add docs/acceptance/release-2-r2f4-0.md \
  docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md \
  docs/plans/2026-08-12-stock-eva-r2f-data-reliability-implementation.md \
  docs/data-providers.md
git commit -m "docs(acceptance): close R2-F4.0 selection shield"
```

The only permitted verdict is:

```text
R2-F4.0 SELECTION CONTRACT GO / SECONDARY BLOCKED / R2-F4 NO-GO
```

Pause for user confirmation before R2-F4.1.

---

## Explicit frozen surfaces

R2-F4.0 MUST produce no diff in:

- `backend/app/market/providers/base.py`
- `backend/app/market/models.py`
- `backend/app/market/candidates.py`
- `backend/app/market/automation.py`
- `backend/app/market/store.py`
- `backend/app/storage/dataset.py`
- R2-F2 golden fixtures and hashes
- R2-F3 Daily evidence/candidate/report artifacts
- production canonical Parquet, manifest and pointer

If implementing a test appears to require changing one of these surfaces, stop and revise the
R2-F4.0 design rather than widening the slice.

