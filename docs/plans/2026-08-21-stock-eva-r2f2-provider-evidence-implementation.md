# Stock EVA R2-F2 Provider Evidence Framework Implementation Plan

> **Spec-first gate:** This plan is executable only after
> [the R2-F2 design](2026-08-21-stock-eva-r2f2-provider-evidence-design.md) is independently
> reviewed and changed from **In Review** to **Approved**. No production code, test stubs or
> RED command may begin before that gate.

**Goal:** Capture one bounded, sanitized BaoStock source-shaped session, publish immutable evidence,
normalize only from that evidence, replay it offline, and record complete candidate/selection
lineage while preserving every legacy reader and canonical publication invariant.

**Baseline:** branch `codex/r2-f2-provider-evidence`, exact clean HEAD
`f6c18d64a8ac04bd24d3f49fb38a28523e6eb8de`.

**Delivery mode:** One subagent at a time, linear Task 7 → Task 8 → Task 9 commits. RED before
GREEN. Independent High/Medium review after each task. A High or Medium finding blocks the next
task until a new regression, minimal fix commit, focused/full re-run and fresh review are complete.

**Authority boundary:** Offline fakes, fixtures and temporary roots only. No real Provider/NAS/
network request, installation, production database mutation, pointer mutation, LaunchAgent action
or external communication is authorized by this plan. The existing production refresh remains
unloaded/frozen.

**Authoritative specification:**
[R2-F2 Provider Evidence Framework Design](2026-08-21-stock-eva-r2f2-provider-evidence-design.md)

## 0. Execution controls

### Worktree and baseline check

Before every task and review:

```bash
git branch --show-current
git status --short
git rev-parse HEAD
git log -1 --oneline
```

Stop on an unexpected branch, dirty overlap, a changed baseline, a live provider/NAS path or a
request to broaden the whitelist. Preserve unrelated changes; do not edit the main worktree.

### Global forbidden changes

The following are stop conditions, not implementation trade-offs:

- modifying `backend/app/market/provider_transport.py`, `baostock_vendor.py` or
  `provider_health.py` to change endpoint, socket, timeout, retry, status-code or circuit meaning;
- changing `backend/app/market/normalize.py`, existing quality gates, SHA-256, canonical Parquet
  schema or atomic pointer primitives to weaken a failure gate;
- storing a socket payload, token, credential, header, cookie, URL, absolute path or arbitrary
  exception in evidence, manifest, logs, status or CLI output;
- passing a live SDK object directly to normalization instead of an atomically published evidence
  reader;
- symbol-level source mixing, partial candidate publication, secondary-provider admission or
  automatic failover;
- rewriting old Parquet, old manifests, refresh rows or legacy API JSON to add provenance;
- any real network/provider/NAS/install/LaunchAgent/production action.

### TDD loop

For each task:

1. Add only the named tests and run the exact RED command. Record the actual failing test names and
   confirm the failure is the missing contract, not an environment or network failure.
2. Implement the minimum spec-backed contract in the exact file whitelist.
3. Run focused GREEN, adjacent regressions, Ruff, format and `git diff --check`.
4. Inspect the diff for source/payload leakage, path writes, transport changes and scope creep.
5. Commit only the whitelist files with the task's commit message.
6. Run a fresh independent static/code/spec review at the exact commit. No next task on an
   unresolved High/Medium finding.

### Shared test evidence rules

- Inject fake provider clients, transport observations, clocks and temporary evidence/dataset roots.
- Use unique bounded basetemp directories under `/tmp/stock-eva-r2f2-*`.
- Assert provider request count is zero on replay, GET and plan paths.
- Fingerprint file bytes, sizes, mtimes, database schema, canonical manifest and pointer before and
  after every negative/read-only test.
- Never call the public BaoStock host, credentials, NAS mount or installed runtime.

## Task 7 — Provider-neutral contracts and BaoStock compatibility adapter

**Requirements:** FR-1–FR-5, FR-19–FR-23, FR-27; NFR-1, NFR-6, NFR-9, NFR-11, NFR-14;
AC-1, AC-2, AC-3, AC-15; EC-1–EC-5, EC-10, EC-17, EC-18.

### Exact file whitelist

Create/modify only:

- Create `backend/app/market/providers/__init__.py`
- Create `backend/app/market/providers/base.py`
- Create `backend/app/market/providers/baostock.py`
- Modify `backend/app/market/baostock.py`
- Modify `backend/app/market/models.py`
- Create `tests/test_market_provider_contract.py`
- Modify `tests/test_market_data.py`

No other file is permitted in the Task 7 commit. In particular, do not touch transport, vendor
patch, provider health, normalizer, storage layout, canonical publication or API routes.

### 7.1 RED — contract and compatibility tests

Add tests for:

```python
def test_provider_id_is_static_baostock_allowlist(): ...
def test_provider_request_requires_exact_sorted_unique_symbols_and_aware_dates(): ...
def test_raw_batch_rejects_unknown_fields_secrets_headers_urls_and_raw_exception(): ...
def test_raw_batch_rejects_nonfinite_values_and_row_shape_mismatch(): ...
def test_baostock_adapter_preserves_endpoint_and_session_contract(): ...
def test_baostock_adapter_captures_source_rows_before_normalization(): ...
def test_baostock_compatibility_produces_existing_daily_bar_semantics(): ...
```

Run:

```bash
uv run --extra dev pytest -q tests/test_market_provider_contract.py tests/test_market_data.py \
  -k 'provider_contract or compatibility or fetch_raw or source_rows' \
  --basetemp=/tmp/stock-eva-r2f2-provider-red
```

Expected RED: provider package/contracts are absent. If a test reaches a real BaoStock host,
stop and fix the fixture before implementation.

### 7.2 GREEN — narrow static registry and adapter

Implement:

- a frozen `ProviderId`/registry with only `baostock`;
- typed `ProviderRequest`, `RawEndpointBatch` and `ProviderRawBatch` with bounded allowlisted
  source fields, UTC timestamps, exact date/universe/symbols and sanitized failure metadata;
- `DailyBarProvider` protocol with `fetch_raw()` and `normalize()` contracts;
- a BaoStock compatibility adapter that reuses the incumbent call/transport/parser boundary and
  exposes source-shaped rows before `normalize_baostock_rows()`;
- compatibility delegation so existing `BaoStockProvider.fetch()` callers keep the current
  canonical models and behavior during the transition.

The adapter MUST NOT change the six `ProviderEndpoint` values, request scopes, provider-session
scope, F0.1 failure mapping, max attempts, socket timeout, circuit state or pagination behavior.
Do not add a generic plugin mechanism or a second source. Do not change the normalizer.

Run:

```bash
uv run --extra dev pytest -q tests/test_market_provider_contract.py tests/test_market_data.py \
  tests/test_market_reliability.py tests/test_market_automation.py \
  --basetemp=/tmp/stock-eva-r2f2-provider-green
uv run --extra dev ruff check backend/app/market/providers backend/app/market/baostock.py \
  backend/app/market/models.py tests/test_market_provider_contract.py tests/test_market_data.py
uv run --extra dev ruff format --check backend/app/market/providers backend/app/market/baostock.py \
  backend/app/market/models.py tests/test_market_provider_contract.py tests/test_market_data.py
git diff --check
```

### 7.3 Task 7 commit and review gate

```bash
git add backend/app/market/providers backend/app/market/baostock.py \
  backend/app/market/models.py tests/test_market_provider_contract.py tests/test_market_data.py
git commit -m "refactor(market): add provider-neutral daily-bar contract"
```

Independent reviewer checks exact HEAD against FR-1–FR-5 and the whitelist, searches for dynamic
imports and provider literals, proves no transport/normalizer diff, and returns High/Medium/Low
counts. A High/Medium finding requires a new RED/GREEN fix commit before Task 8.

## Task 8 — Immutable evidence publication and offline replay

**Requirements:** FR-4–FR-11, FR-19, FR-23–FR-25; NFR-1–NFR-6, NFR-8–NFR-12;
AC-2, AC-4–AC-8, AC-12–AC-14; EC-2, EC-4–EC-9, EC-11, EC-13, EC-14, EC-19, EC-20.

### Exact file whitelist

Create/modify only:

- Modify `backend/app/config.py`
- Modify `backend/app/storage/layout.py`
- Create `backend/app/market/evidence.py`
- Modify `backend/app/market/automation.py`
- Modify `backend/app/cli.py`
- Create `tests/test_market_provider_evidence.py`
- Modify `tests/test_market_get_read_only.py`

Evidence-specific Pydantic models remain in `backend/app/market/evidence.py` unless a design review
first approves a whitelist amendment. Do not change `storage/dataset.py` in this task; reuse its
safe relative-path, descriptor/hash and atomic publication conventions through a narrow wrapper.

### 8.1 RED — evidence/replay and zero-write tests

Add tests for:

```python
def test_raw_evidence_is_content_addressed_and_idempotent(tmp_path): ...
def test_changed_bytes_create_distinct_evidence_without_overwrite(tmp_path): ...
def test_evidence_manifest_binds_hash_schema_provider_universe_and_row_count(tmp_path): ...
def test_evidence_rejects_secret_header_cookie_url_path_and_exception_fields(tmp_path): ...
def test_evidence_publish_is_atomic_at_each_crash_boundary(tmp_path): ...
def test_evidence_reader_rejects_symlink_toc_tou_oversize_and_object_substitution(tmp_path): ...
def test_evidence_reader_rejects_bad_parquet_decompression_and_manifest_swap(tmp_path): ...
def test_offline_replay_has_zero_network_and_zero_canonical_pointer_writes(tmp_path): ...
def test_replay_is_deterministic_and_matches_online_candidate(tmp_path): ...
def test_missing_evidence_root_is_write_free(tmp_path): ...
def test_get_and_plan_paths_do_not_initialize_evidence_storage(tmp_path): ...
```

Run:

```bash
uv run --extra dev pytest -q tests/test_market_provider_evidence.py \
  --basetemp=/tmp/stock-eva-r2f2-evidence-red
```

Expected RED: the evidence store, published reader and replay CLI do not exist. A failure that is
only a fixture/path problem must be corrected without broadening scope.

### 8.2 GREEN — bounded evidence store and replay

Implement only the following sequence:

```text
ProviderRawBatch in memory
  -> validate allowlisted schema/units/date and sanitize metadata
  -> write bounded source-shaped Parquet partial below evidence staging
  -> descriptor/readback schema, row count, size and SHA-256
  -> atomic object rename within evidence root
  -> canonical JSON evidence-manifest atomic publish
  -> open one hash/fingerprint-bound PublishedEvidence reader
  -> adapter.normalize(PublishedEvidence)
```

Required behavior:

- Add `evidence_root` and evidence staging layout with safe settings validation. Missing roots are
  unavailable on read paths and are not created implicitly.
- Use relative paths only and reject symlinks, traversal, absolute paths, object substitution,
  manifest swaps, oversize metadata/object and malformed/decompression-invalid Parquet.
- Never expose or persist transport payload, token, header, cookie, URL, local path or raw
  exception. Preserve only bounded provider code and allowlisted failure classes.
- Use deterministic JSON (`ensure_ascii=False`, sorted keys, compact separators), deterministic
  row ordering and content-addressed IDs. Identical bytes are idempotent; changed bytes never
  overwrite an existing object.
- Make `EvidenceReader` the only input accepted by adapter normalization in the canonical path.
- Add `market-provider-replay --evidence-id ID [--compare-candidate-sha SHA]`. It accepts no
  provider credentials or path and performs zero network, provider, canonical Parquet, canonical
  manifest or pointer writes.
- Keep GET and plan paths read-only and independent of evidence schema initialization.

The evidence Parquet is source-shaped, not a socket dump. Daily semantic validation remains the
existing normalizer/quality contract: unadjusted prices, `backAdjustFactor`, exact date, units,
duplicate/coverage and suspension rules.

Run:

```bash
uv run --extra dev pytest -q tests/test_market_provider_evidence.py \
  tests/test_market_get_read_only.py -k 'evidence or replay or representative_market_gets' \
  --basetemp=/tmp/stock-eva-r2f2-evidence-green
uv run --extra dev pytest -q tests/test_market_provider_evidence.py \
  tests/test_market_get_read_only.py tests/test_market_data.py \
  --basetemp=/tmp/stock-eva-r2f2-evidence-adjacent
uv run --extra dev ruff check backend/app/market/evidence.py backend/app/config.py \
  backend/app/storage/layout.py backend/app/market/automation.py backend/app/cli.py \
  tests/test_market_provider_evidence.py tests/test_market_get_read_only.py
uv run --extra dev ruff format --check backend/app/market/evidence.py backend/app/config.py \
  backend/app/storage/layout.py backend/app/market/automation.py backend/app/cli.py \
  tests/test_market_provider_evidence.py tests/test_market_get_read_only.py
git diff --check
```

### 8.3 Task 8 commit and review gate

```bash
git add backend/app/config.py backend/app/storage/layout.py backend/app/market/evidence.py \
  backend/app/market/automation.py backend/app/cli.py \
  tests/test_market_provider_evidence.py tests/test_market_get_read_only.py
git commit -m "feat(market): persist replayable provider evidence"
```

Independent reviewer checks every crash/TOCTOU/symlink/oversize/decompression/object-substitution/
manifest-swap/replay nondeterminism test, verifies zero canonical writes, checks no network imports
or credential arguments, and inspects the exact whitelist. High/Medium blocks Task 9.

## Task 9 — Candidate/selection manifests and source compatibility migration

**Requirements:** FR-12–FR-19, FR-23–FR-27; NFR-1, NFR-3, NFR-5, NFR-6, NFR-7, NFR-9,
NFR-10, NFR-12, NFR-14; AC-8–AC-15; EC-10–EC-18.

### Exact file whitelist

Create/modify only:

- Modify `backend/app/market/models.py`
- Modify `backend/app/market/store.py`
- Modify `backend/app/storage/dataset.py`
- Modify `backend/app/storage/publication.py`
- Modify `backend/app/market/automation.py`
- Modify `backend/app/market/service.py`
- Modify `backend/app/market/series.py`
- Modify `backend/app/analysis/models.py`
- Modify `backend/app/alert/models.py`
- Modify `backend/app/user/models.py`
- Modify `backend/app/api/market.py`
- Create `tests/test_market_candidate_selection.py`
- Modify `tests/test_market_data.py`
- Modify `tests/test_market_get_read_only.py`

No old Parquet/manifest fixture or deployed object may be rewritten. `docs/acceptance/release-2-r2f2.md`
is created only in the final acceptance closure commit after all code/review evidence exists; it is
not part of the Task 9 implementation commit unless the reviewed implementation plan explicitly
records it as the final bounded evidence commit.

### 9.1 RED — legacy/new compatibility and single-session selection tests

Add tests for:

```python
def test_legacy_baostock_rows_and_manifests_decode_without_rewrite(tmp_path): ...
def test_missing_legacy_source_decodes_as_baostock_in_memory(tmp_path): ...
def test_new_candidate_references_exact_evidence_universe_and_gate_hashes(tmp_path): ...
def test_every_candidate_gate_has_immutable_pass_or_fail_record(tmp_path): ...
def test_selection_references_one_complete_candidate_only(tmp_path): ...
def test_selection_rejects_mixed_provider_or_symbol_level_partition(tmp_path): ...
def test_qualified_fallback_is_reserved_and_unreachable_in_r2f2(tmp_path): ...
def test_new_canonical_manifest_requires_lineage_for_new_entries(tmp_path): ...
def test_lineage_mismatch_preserves_old_pointer_and_candidate_history(tmp_path): ...
def test_existing_market_analysis_alert_user_and_api_json_remain_compatible(tmp_path): ...
def test_get_reads_and_legacy_manifest_reads_are_write_free(tmp_path): ...
```

Run:

```bash
uv run --extra dev pytest -q tests/test_market_candidate_selection.py \
  tests/test_market_data.py tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r2f2-selection-red
```

Expected RED: current source literals and manifest readers have no candidate/selection lineage
contract. The expected failure must not be addressed by rewriting legacy fixtures.

### 9.2 GREEN — additive candidate and selection lineage

Implement:

- widen only genuine provider identity fields to the static `ProviderId` registry; keep public
  existing `source="baostock"` values and unrelated literals unchanged;
- decode missing legacy source as BaoStock in memory without touching bytes;
- add immutable gate reports and candidate manifests referencing exact published evidence,
  normalized candidate, universe, provider/adapter/schema versions and hashes;
- add a `SessionSelection` record with `primary_ready` active in R2-F2 and
  `qualified_fallback` reserved but rejected by current policy;
- require exactly one complete candidate/provider for each new canonical partition and reject
  mixed symbols, duplicate providers, incomplete coverage, wrong date/universe or missing lineage;
- preserve rejected candidate/gate/selection audit artifacts outside the serving pointer;
- add additive lineage fields to new manifest entries while accepting old schema/entries;
- invoke only the existing canonical publication chain after selection is complete;
- keep analysis, alert, user, market summary/history and GET response field compatibility. No new
  public failover endpoint or source-selection control is added.

Run focused GREEN and adjacent regressions:

```bash
uv run --extra dev pytest -q tests/test_market_candidate_selection.py \
  tests/test_market_data.py tests/test_market_get_read_only.py \
  tests/test_security_analysis.py tests/test_market_regime.py tests/test_portfolio_risk.py \
  --basetemp=/tmp/stock-eva-r2f2-selection-green
uv run --extra dev pytest -q tests/test_market_candidate_selection.py \
  tests/test_market_data.py tests/test_market_get_read_only.py \
  --basetemp=/tmp/stock-eva-r2f2-selection-adjacent
uv run --extra dev ruff check backend tests/test_market_candidate_selection.py
uv run --extra dev ruff format --check backend tests/test_market_candidate_selection.py
git diff --check
```

Before claiming Task 9 GREEN, run one offline BaoStock replay and compare pre/post byte/tree
fingerprints for a legacy manifest/object fixture. The replay must not write a canonical pointer.

### 9.3 Task 9 commit and review gate

```bash
git add backend/app/market/models.py backend/app/market/store.py \
  backend/app/storage/dataset.py backend/app/storage/publication.py \
  backend/app/market/automation.py backend/app/market/service.py \
  backend/app/market/series.py backend/app/analysis/models.py \
  backend/app/alert/models.py backend/app/user/models.py backend/app/api/market.py \
  tests/test_market_candidate_selection.py tests/test_market_data.py \
  tests/test_market_get_read_only.py
git commit -m "feat(market): publish provider-backed session selections"
```

Independent reviewer verifies each FR/NFR/AC/EC for Task 9, all legacy readers, one-provider
partition enforcement, exact lineage hashes, no fallback writes, no pointer mutation on failures,
and exact whitelist. High/Medium requires a new regression/fix/review cycle.

## Final R2-F2 acceptance closure

This closure starts only after Tasks 7–9 each have independent High/Medium-zero review. It is still
offline code acceptance; it is not a production or provider GO.

### Universal offline gate

Run from the exact reviewed code HEAD:

```bash
uv run --extra dev pytest -q --basetemp=/tmp/stock-eva-r2f2-full
uv run --extra dev ruff check backend tests
uv run --extra dev ruff format --check backend tests
git diff --check
```

Run the focused R2-F2 matrix:

```bash
uv run --extra dev pytest -q \
  tests/test_market_provider_contract.py \
  tests/test_market_provider_evidence.py \
  tests/test_market_candidate_selection.py \
  tests/test_market_data.py \
  tests/test_market_get_read_only.py \
  tests/test_baostock_transport.py \
  tests/test_baostock_provider.py \
  tests/test_market_reliability.py \
  --basetemp=/tmp/stock-eva-r2f2-acceptance
```

Record exact test counts, exit codes, command basetemps, exact code HEAD and each bounded commit.
Do not call a partial selection a full-suite gate.

### Manual traceability matrix

The validator is a structural aid, not acceptance evidence. The final review must manually trace:

| Requirement range | Implementation evidence | Test evidence |
| --- | --- | --- |
| FR-1–FR-5 | `providers/base.py`, `providers/baostock.py`, incumbent adapter boundary | provider contract RED/GREEN, exact endpoint/session assertions |
| FR-6–FR-11 | `market/evidence.py`, `config.py`, `storage/layout.py`, `cli.py` | hash/idempotence, crash, bounds, TOCTOU, replay zero-write/determinism |
| FR-12–FR-16 | candidate/selection models and automation | full-session, gate audit, mixed-source/fallback rejection |
| FR-17–FR-19, FR-24–FR-26 | models/store/dataset/publication/API readers | legacy fixtures, lineage mismatch, pointer/read-only fingerprints |
| FR-20–FR-23 | existing normalizer contract plus adapter failure mapping | date/unit/factor/suspension and sanitized failure tests |
| FR-27 | whitelist/diff and absence of second-source code | static review and no-network gate |
| NFR-1–NFR-6 | evidence/publish locking, hashes, canonical serializers | crash/concurrency/TOCTOU/determinism tests |
| NFR-7–NFR-12 | additive migration and read-only paths | full regression, legacy byte fingerprints, GET/plan/replay tests |
| NFR-13–NFR-14 | plan/review gates and static registry | commit/review records, dynamic-import scan |
| AC-1–AC-15 | design-to-code mapping | each AC has named focused regression and review evidence |
| EC-1–EC-20 | boundary guards and sanitized errors | each EC has a synthetic test or explicit static proof |

If `spec_validator.py --strict` cannot parse the Markdown because of Chinese/Markdown formatting,
record its actual exit code/output and complete this matrix manually. Do not weaken the spec to make
the validator green.

### Acceptance document and independent final review

Create `docs/acceptance/release-2-r2f2.md` only after the universal gate. It MUST state:

- exact reviewed code HEAD and all Task 7–9/fix commits;
- exact focused/full counts and commands;
- independent review verdict and High/Medium/Low findings;
- evidence object/manifest/candidate/selection lineage and zero-write/crash/concurrency results;
- legacy compatibility and byte-fingerprint results;
- explicit `R2-F2 OFFLINE CODE GO` only if every gate passes;
- explicit `REAL PROVIDER AND PRODUCTION EXECUTION NOT AUTHORIZED`;
- known Low limitations and threat boundary;
- rollback to BaoStock-only compatibility mode and last legacy pointer;
- R2-F3 out of scope and second source/failover still disabled.

Run a final independent review at the exact code/doc state. Any High/Medium finding reopens the
relevant task; do not edit acceptance language to hide an incomplete requirement.

Final documentation commit is separate from implementation if it depends on test counts or review
results:

```bash
git add docs/acceptance/release-2-r2f2.md \
  docs/plans/2026-08-21-stock-eva-r2f2-provider-evidence-design.md \
  docs/plans/2026-08-21-stock-eva-r2f2-provider-evidence-implementation.md
git commit -m "docs(acceptance): close R2-F2 offline delivery"
git diff --check
git status --short
```

After documentation review returns High 0 / Medium 0, stop and wait for the user's next explicit
approval. Do not begin R2-F3 in the same delivery window.

## Rollback and stop conditions

Rollback selects BaoStock-only compatibility mode and the last legacy-compatible pointer. It does
not delete evidence or candidate history, rewrite Parquet, edit manifests by hand or remove queue
state. Stop immediately if a test or implementation proposes increasing timeout/retry, lowering
coverage/factor gates, using stale data as current, mixing symbols/providers, or calling real
external services.
