# Stock EVA R2-F2 Provider Evidence Framework Implementation Plan

> **Spec-first gate:** This plan is executable only after
> [the R2-F2 design](2026-08-21-stock-eva-r2f2-provider-evidence-design.md) is independently
> reviewed and changed from **In Review** to **Approved**. The design is currently **In Review**
> after the third-round contract findings were reconciled; no production code, test stubs or
> RED command may begin before that gate.

**Goal:** Capture one bounded, sanitized BaoStock source-shaped session, publish immutable evidence,
normalize only from that evidence, replay it offline, and record complete candidate/selection
lineage while preserving every legacy reader and canonical publication invariant.

**Baseline:** branch `codex/r2-f2-provider-evidence`, exact clean HEAD
`70f7ed3c6739566d20d5ae7ba6895d65676a8033`.

**Delivery mode:** One subagent at a time, linear Task 7 → Task 8 → Task 9 commits. RED before
GREEN. Independent High/Medium review after each task. A High or Medium finding blocks the next
task until a new regression, minimal fix commit, focused/full re-run and fresh review are complete.

**Authority boundary:** Offline fakes, fixtures and temporary roots only. No real Provider/NAS/
network request, installation, production database mutation, pointer mutation, LaunchAgent action
or external communication is authorized by this plan. The existing production refresh remains
unloaded/frozen.

## Review closure preflight (must remain before RED)

The prior independent review was **NO-GO**. The third-round amendment additionally closes field
name drift, stale umbrella pseudo-code, the factor-cache schema mismatch, incomplete factor
resolution/cardinality, missing endpoint-constant validation, an over-claimed date gate and
lexical-only path safety. This plan MUST implement the dedicated design definitions verbatim and
MUST NOT narrow them to pass current tests. No ambiguity exceeds the 30% escalation threshold; any
newly discovered unresolved schema or security meaning stops the task and returns to specification
review.

### Feature manifest and dependency map

| Boundary | Authoritative files / artifacts | Dependency and allowed direction |
| --- | --- | --- |
| Provider transport/parser (read-only dependency) | `backend/app/market/baostock.py`, `provider_transport.py`, `baostock_vendor.py` | R2-F2 adapter consumes the six fixed endpoint calls, `ProviderEndpoint`, typed F0.1 `TransportObservation`; it MUST NOT change transport, retry, timeout, circuit or normalizer semantics. |
| Factor-cache snapshot API | Task 8 `backend/app/market/factor_cache.py` | Add only strict read-only `exact_snapshot_records()` plus stable row fingerprint over the current `factor_snapshots` columns; no DDL, migration or writer behavior change. |
| Provider contracts | Task 7 `backend/app/market/providers/{base,baostock}.py`, necessary `backend/app/market/baostock.py`/`market/models.py` compatibility | Safe types → endpoint/role variants → logical request/completion/raw discriminated rows → incumbent adapter; no dynamic provider/plugin import. |
| Evidence publication | Task 8 `backend/app/market/evidence.py`, `config.py`, `storage/layout.py` | Raw batch + transport projections + factor snapshot → bounded descriptor-bound object → ordered `EvidenceManifest`; uses existing local safe-path/hash/atomic conventions. |
| Replay/read-only boundary | Task 8 `cli.py`, `automation.py`, `tests/test_market_get_read_only.py` | Evidence reader → injected-clock normalizer/replay; zero network/provider/canonical pointer/database initialization on reads. |
| Candidate/selection lineage | Task 9 `backend/app/market/candidates.py` plus store/dataset/publication/service/series/API compatibility files | Published evidence → exact ten-gate aggregate → candidate manifest → session selection → existing canonical publication chain; no symbol-level mixing. |
| Existing serving chain | Normalize → Quality Gate → Immutable Parquet → SHA-256 → Manifest → Atomic Publish | R2-F2 adds only validated additive lineage hooks; legacy object bytes/readers remain authoritative. |

Task dependencies are one-way: provider contract depends on incumbent transport/parser; evidence
depends on provider contract; candidate/selection depends on evidence and R2-F1 lock/CAS/publication
boundaries. R2-F2 does not depend on or introduce any second provider, shadow schedule, failover,
NAS, LaunchAgent or production state.

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

**Requirements:** FR-1–FR-5, FR-19–FR-23, FR-27–FR-31; NFR-1, NFR-6, NFR-9, NFR-11, NFR-14;
AC-1–AC-3, AC-15–AC-17; EC-1–EC-5, EC-10, EC-17, EC-18, EC-21, EC-22.

### Exact file whitelist

Create/modify only:

- Create `backend/app/market/providers/__init__.py`
- Create `backend/app/market/providers/base.py`
- Create `backend/app/market/providers/baostock.py`
- Modify `backend/app/market/baostock.py`
- Modify `backend/app/market/models.py` (provider-source compatibility only)
- Create `tests/test_market_provider_contract.py`
- Modify `tests/test_market_data.py`

No other file is permitted in the Task 7 commit. In particular, do not touch transport, vendor
patch, provider health, normalizer, storage layout, canonical publication or API routes.

`base.py` owns provider-safe types, endpoint/role validators, logical requests/completions, raw
batches and transport projections. `providers/baostock.py` owns only the compatibility adapter.
The strict factor-cache snapshot API is owned by Task 8 in `backend/app/market/factor_cache.py`;
Task 7 may consume existing `FACTOR_FIELDS`/`exact_snapshots()` behavior but MUST NOT invent cache
provenance fields. `market/models.py` is limited to necessary additive provider-source compatibility.
No task may duplicate candidate/evidence models here.

### 7.1 RED — contract and compatibility tests

Add tests for:

```python
def test_provider_id_is_static_baostock_allowlist(): ...
def test_provider_request_requires_exact_sorted_unique_symbols_and_aware_dates(): ...
def test_raw_batch_rejects_unknown_fields_secrets_headers_urls_and_raw_exception(): ...
def test_raw_batch_rejects_nonfinite_values_and_row_shape_mismatch(): ...
def test_each_baostock_endpoint_has_exact_fields_variant_units_and_order(): ...
def test_endpoint_contract_constant_rejects_wrong_combination_and_model_copy(): ...
def test_endpoint_schema_variants_reject_date_mismatch_and_mixed_sessions(): ...
def test_validate_raw_date_binding_rejects_cross_day_before_normalize(): ...
def test_endpoint_role_validator_rejects_stock_index_role_mismatch(): ...
def test_transport_lineage_preserves_unknown_provider_code_and_maps_unknown_protocol(): ...
def test_transport_projection_preserves_all_allowlisted_fields_and_digest(): ...
def test_logical_request_plan_hash_excludes_observed_pages_and_completion_hash_binds_them(): ...
def test_request_completion_rejects_missing_duplicate_noncontiguous_pages_or_terminal_marker(): ...
def test_baostock_adapter_preserves_endpoint_and_session_contract(): ...
def test_baostock_adapter_captures_source_rows_before_normalization(): ...
def test_baostock_compatibility_produces_existing_daily_bar_semantics(): ...
def test_daily_schema_preserves_legal_suspended_empty_activity_and_factor(): ...
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

- the complete frozen `SafeProviderId`, `SafeSymbol`, `SafeVersion`, `ProviderEndpoint`,
  `InstrumentRole`, `RequestRole`, `ExpectedLogicalRequest`, `ExpectedLogicalRequestPlan`,
  `AttemptCompletion`, `RequestCompletion`, `ProviderRequest`, `TransportLineageRef`,
  `TransportObservationProjection`, discriminated `RawEndpointRow`/`RawEndpointBatch` and
  `ProviderRawBatch` contracts from the design. Evidence models belong to Task 8 and candidate/
  gate/selection models belong only to Task 9 `market/candidates.py`. Every model is `extra="forbid"`, immutable,
  finite-number checked, UTC-aware and bounded;
- a frozen `ProviderId`/registry with only `baostock`, rejecting empty/path-like/mixed-case/plugin
  identities;
- typed `ProviderRequest`, `RawEndpointBatch` and `ProviderRawBatch` with bounded allowlisted
  source fields, UTC timestamps, exact date/universe/symbols, separate logical request-plan and
  observed completion hashes, endpoint summaries and sanitized failure metadata;
- the authoritative `ENDPOINT_CONTRACTS` constant and exact endpoint/schema variants:
  `trade_dates.v1`, `all_stock.market.v1`,
  `daily_astock.v1`, `daily_factor.v1`, `adjust_factor.session.v1`,
  `index_history.session.v1` and `index_history.range.v1` for both stock/index roles. Freeze the exact field tuples,
  units, empty/null rules, request parameters, page/attempt lineage and deterministic row order
  from the design; no `dict[str, object]` or union-any escape hatch;
- `DailyBarProvider` protocol with `fetch_raw()` and `normalize()` contracts;
- a BaoStock compatibility adapter that reuses the incumbent call/transport/parser boundary and
  exposes source-shaped rows before `normalize_baostock_rows()`;
- sanitized `TransportObservationProjection` preserving every F0.1 allowlisted field and separate
  per-projection/ordered aggregate SHA-256 digests, binding every endpoint/page/attempt to
  `refresh_id/provider_session_id/request_id`;
- `InstrumentRole`/`RequestRole` cross-validation for stock/index `index_history` variants and
  `validate_raw_date_binding(request, batch)` before normalization; do not change `normalize.py`;
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

Independent reviewer checks exact HEAD against FR-1–FR-5 and FR-27–FR-31 and the whitelist, searches for dynamic
imports and provider literals, proves no transport/normalizer diff, and returns High/Medium/Low
counts. A High/Medium finding requires a new RED/GREEN fix commit before Task 8.

## Task 8 — Immutable evidence publication and offline replay

**Requirements:** FR-4–FR-11, FR-19, FR-23–FR-25, FR-30–FR-33; NFR-1–NFR-6, NFR-8–NFR-12,
NFR-15–NFR-17; AC-2, AC-4–AC-8, AC-12–AC-14, AC-17, AC-19; EC-2, EC-4–EC-9, EC-11, EC-13,
EC-14, EC-19–EC-22, EC-24–EC-26.

### Exact file whitelist

Create/modify only:

- Modify `backend/app/config.py`
- Modify `backend/app/storage/layout.py`
- Modify `backend/app/market/factor_cache.py` (strict read-only snapshot API only; no schema migration)
- Create `backend/app/market/evidence.py`
- Modify `backend/app/market/automation.py`
- Modify `backend/app/cli.py`
- Create `tests/test_market_provider_evidence.py`
- Modify `tests/test_adjustment_factor_cache.py`
- Modify `tests/test_market_get_read_only.py`

Evidence-specific Pydantic models, factor snapshot serialization and replay projection remain in
`backend/app/market/evidence.py`; the only factor-cache code allowed here is the strict read-only
`exact_snapshot_records(symbols, trade_date)` query plus its stable row-fingerprint helper in
`backend/app/market/factor_cache.py`. It MUST use the current `factor_snapshots` columns and MUST
not migrate/initialize schema. Do not change `storage/dataset.py` in this task; reuse its
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
def test_replay_injects_frozen_normalization_clock_and_excludes_invocation_time(tmp_path): ...
def test_missing_evidence_root_is_write_free(tmp_path): ...
def test_get_and_plan_paths_do_not_initialize_evidence_storage(tmp_path): ...
def test_manifest_requires_one_descriptor_per_request_shard_and_page(tmp_path): ...
def test_factor_snapshot_fingerprint_change_fails_closed_and_replay_uses_published_snapshot(tmp_path): ...
def test_factor_cache_snapshot_records_match_current_table_and_model_copy_is_read_only(tmp_path): ...
def test_safe_relative_path_rejects_semantic_traversal_and_storage_uses_dirfd_containment(tmp_path): ...
def test_safe_relative_path_model_copy_round_trip_rejects_escape(tmp_path): ...
def test_every_evidence_descriptor_binds_sanitized_transport_observation_digest(tmp_path): ...
def test_evidence_compare_create_allows_one_lineage_and_zero_canonical_writes_for_loser(tmp_path): ...
def test_evidence_root_ancestor_symlink_and_toc_tou_fail_closed(tmp_path): ...
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
- Add the exact relative settings/layout from the design: `objects/`, `manifests/`, `gates/`,
  `candidates/`, `selections/`, `staging/` and `orphan-audit/`, with fixed bounded size
  and row limits. Validate root/ancestor descriptors using no-follow identity checks; no caller
  supplied path component may escape the opened root.
- Use relative paths only and reject symlinks, traversal, absolute paths, object substitution,
  manifest swaps, oversize metadata/object and malformed/decompression-invalid Parquet.
- Never expose or persist transport payload, token, header, cookie, URL, local path or raw
  exception. Preserve only bounded provider code and allowlisted failure classes.
- Store one `EvidenceObjectDescriptor` per expected logical request/endpoint shard/page, in the
  exact ordered `ExpectedLogicalRequestPlan` and its observed `RequestCompletion`; each raw-page
  descriptor MUST carry object kind, plan ordinal, attempt, request ID, page, relative path,
  object/hash/schema hashes, row/byte counts, ordered fields/units, provider/universe,
  refresh/session, endpoint, schema/contract versions, pagination policy and transport observation
  digest. The manifest MUST reject missing,
  extra, duplicate, out-of-order, non-contiguous or swallowed multi-symbol/multi-index/multi-page
  objects, and MUST require a terminal/end marker without requiring a prefetch page total.
- Serialize one `factor_cache_snapshot` object containing only the selected keys returned by
  `AdjustmentFactorCache.exact_snapshot_records(symbols, trade_date)`. Each record MUST mirror the
  current `factor_snapshots` columns (`symbol`, `trade_date`, `fore_adjust_factor`,
  `back_adjust_factor`, `evidence_kind`, `evidence_effective_date`, `evidence_observed_on`,
  `source_row_hash`, `observed_at`) plus a stable row fingerprint; no invented `source`,
  `effective`, `observed`, `adjust_factor` or cache-version field and no schema migration is
  allowed. Capture and verify before/after fingerprints while the same `RefreshRunLock` is held.
  If the snapshot changes, is incomplete or cannot bind a mutually exclusive live/cache
  `FactorResolutionBinding`, fail closed. Online normalization and replay consume only this
  published snapshot; live mutable cache reads after capture are forbidden. The ordered binding
  hash `factor_resolution_sha256` MUST be copied into the evidence and candidate manifests.
- Use evidence compare-create/no-clobber CAS. Identical content is idempotent; a collision with
  different bytes fails closed. Preserve unreferenced staging under `orphan-audit` and never adopt
  it automatically.
- Use deterministic JSON (`ensure_ascii=False`, sorted keys, compact separators), deterministic
  row ordering and content-addressed IDs. Identical bytes are idempotent; changed bytes never
  overwrite an existing object.
- Run `validate_raw_date_binding(request, batch)` before any normalization and then apply the
  existing publication requested-date/session identity gate. A cross-day raw response produces no
  candidate. `normalize.py` remains unchanged and is not credited with requested-date validation.
- Generate `normalization_clock_utc` exactly once after fetch/transport completion and before
  evidence-manifest publish; inject it into online and replay normalization through
  `normalize_baostock_rows(..., ingested_at=normalization_clock_utc)`. Invocation time is never a
  `ReplayResult` field and never enters candidate/semantic/selection hashes.
- Make `EvidenceReader` the only input accepted by adapter normalization in the canonical path.
- Add `market-provider-replay --evidence-id ID [--compare-candidate-sha SHA]`. It accepts no
  provider credentials or path and performs zero network, provider, canonical Parquet, canonical
  manifest or pointer writes.
- Replay MUST report byte hash and semantic hash/match using the design's same-version byte GO and
  cross-version non-semantic-encoding exception; any unclassified difference is fail-closed.
- Readers and plan/replay paths MUST use SELECT/read-only descriptors and never initialize roots,
  schemas, locks or pointers. The full writer order is `RefreshRunLock` → evidence compare-create
  → evidence manifest → gate aggregate → candidate manifest → selection publish/readback → existing
  canonical manifest/pointer; a CAS loser performs zero canonical writes.
- Keep GET and plan paths read-only and independent of evidence schema initialization.

The only blocking lock is the existing R2-F1 `RefreshRunLock` at
`local_lock_dir/market-refresh.lock`. The precise order is: acquire that lock; capture and verify
the factor snapshot; evidence compare-create with O_EXCL/no-clobber and atomic rename; evidence
manifest; ten-outcome gate aggregate; candidate manifest; selection publish/readback; then the
existing canonical pointer chain. Evidence objects and manifests never acquire a second refresh
lock or a cross-root blocking lock. Replay takes no blocking lock (or a read-only snapshot) and
performs zero writes. A crash at any boundary leaves prior complete state or a complete new state;
staging residue is retained only for orphan audit.

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
uv run --extra dev ruff check backend/app/market/evidence.py backend/app/market/factor_cache.py backend/app/config.py \
  backend/app/storage/layout.py backend/app/market/automation.py backend/app/cli.py \
  tests/test_market_provider_evidence.py tests/test_adjustment_factor_cache.py tests/test_market_get_read_only.py
uv run --extra dev ruff format --check backend/app/market/evidence.py backend/app/market/factor_cache.py backend/app/config.py \
  backend/app/storage/layout.py backend/app/market/automation.py backend/app/cli.py \
  tests/test_market_provider_evidence.py tests/test_adjustment_factor_cache.py tests/test_market_get_read_only.py
git diff --check
```

### 8.3 Task 8 commit and review gate

```bash
git add backend/app/config.py backend/app/storage/layout.py backend/app/market/factor_cache.py \
  backend/app/market/evidence.py \
  backend/app/market/automation.py backend/app/cli.py \
  tests/test_market_provider_evidence.py tests/test_adjustment_factor_cache.py \
  tests/test_market_get_read_only.py
git commit -m "feat(market): persist replayable provider evidence"
```

Independent reviewer checks every crash/TOCTOU/symlink/oversize/decompression/object-substitution/
manifest-swap/replay nondeterminism test, verifies zero canonical writes, checks no network imports
or credential arguments, and inspects the exact whitelist. High/Medium blocks Task 9.

## Task 9 — Candidate/selection manifests and source compatibility migration

**Requirements:** FR-12–FR-19, FR-23–FR-29, FR-32; NFR-1, NFR-3, NFR-5, NFR-6, NFR-7, NFR-9,
NFR-10, NFR-12, NFR-14, NFR-17–NFR-18; AC-8–AC-16, AC-18, AC-20; EC-10–EC-18, EC-23,
EC-26.

### Exact file whitelist

Create/modify only:

- Create `backend/app/market/candidates.py`
- Modify `backend/app/market/store.py`
- Modify `backend/app/storage/dataset.py`
- Modify `backend/app/storage/publication.py`
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

`backend/app/market/candidates.py` is the single owner of `GateOutcome`,
`CandidateGateReport`, `CandidateManifest` and `SessionSelection`. `market/models.py` may carry
only additive legacy source compatibility from Task 7 and is not modified by Task 9; it must not
duplicate candidate, gate or selection definitions.

### 9.1 RED — legacy/new compatibility and single-session selection tests

Add tests for:

```python
def test_legacy_baostock_rows_and_manifests_decode_without_rewrite(tmp_path): ...
def test_missing_legacy_source_decodes_as_baostock_in_memory(tmp_path): ...
def test_new_candidate_references_exact_evidence_universe_and_gate_hashes(tmp_path): ...
def test_every_candidate_gate_has_immutable_pass_or_fail_record(tmp_path): ...
def test_gate_report_requires_exact_ordered_complete_gate_aggregate(tmp_path): ...
def test_selection_references_one_complete_candidate_only(tmp_path): ...
def test_selection_rejects_mixed_provider_or_symbol_level_partition(tmp_path): ...
def test_qualified_fallback_is_reserved_and_rejected_by_r2f2_writers(tmp_path): ...
def test_new_canonical_manifest_requires_lineage_for_new_entries(tmp_path): ...
def test_lineage_mismatch_preserves_old_pointer_and_candidate_history(tmp_path): ...
def test_candidate_rejects_any_lineage_provider_schema_universe_or_hash_mismatch(tmp_path): ...
def test_semantic_gate_rejects_active_missing_factor_nonzero_suspended_activity_and_suspended_index(tmp_path): ...
def test_selection_publish_order_never_moves_pointer_early(tmp_path): ...
def test_existing_market_analysis_alert_user_and_api_json_remain_compatible(tmp_path): ...
def test_get_reads_and_legacy_manifest_reads_are_write_free(tmp_path): ...
def test_factor_resolution_binds_published_snapshot_or_raw_endpoint(tmp_path): ...
def test_factor_resolution_requires_mutually_exclusive_live_cache_fields_and_hashes(tmp_path): ...
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
- add exactly one immutable `CandidateGateReport` aggregate per candidate, with the ordered ten-name
  `R2F2_GATE_ORDER` and exactly one `GateOutcome` for each gate. The names and current-code
  evidence are the single `R2F2_GATE_EVIDENCE` table: transport complete, schema, date, universe,
  coverage, semantic, factor, suspension, evidence hash and determinism. Missing, extra, duplicate,
  out-of-order or aggregate-hash-mismatched outcomes fail closed; the report uses only
  `gate_report_id`, `verdict` and `aggregate_sha256`; rejected candidates retain the
  sanitized complete report;
- treat the existing five-string `MarketDataStatus.publication_gates` as a legacy public
  description only; it is not copied into the ten-outcome R2-F2 aggregate and is not modified;
- add immutable candidate manifests referencing exact published evidence through one
  `evidence_sha256`, normalized candidate, universe, provider/adapter/schema versions, one gate
  aggregate path/hash, `factor_resolution_sha256` and exact row counts;
- add a `SessionSelection` record with `primary_ready` active in R2-F2. The schema may contain
  `qualified_fallback` for future compatibility, but every R2-F2 writer, orchestrator and validator
  MUST reject it, reject non-null `fallback_from`, and never load a second source/plugin;
- require exactly one complete candidate/provider for each new canonical partition and reject
  mixed symbols, duplicate providers, incomplete coverage, wrong date/universe or missing lineage;
- preserve rejected candidate/gate/selection audit artifacts outside the serving pointer;
- add additive lineage fields to new manifest entries while accepting old schema/entries;
- bind every factor-bearing normalized row to the published `factor_cache_snapshot` or an explicitly
  selected raw factor endpoint through `FactorResolutionBinding`; no live mutable cache read is
  allowed during replay;
- invoke only the existing canonical publication chain after selection is atomically published and
  readback/hash verified under the same `RefreshRunLock`; the canonical manifest/pointer MUST NOT
  reference a missing selection;
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
git add backend/app/market/store.py \
  backend/app/market/candidates.py \
  backend/app/storage/dataset.py backend/app/storage/publication.py \
  backend/app/market/service.py \
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

Run the specification validator against the **design only** (never against this implementation
plan and never as a substitute for tests or manual traceability):

```bash
python /Users/finlay/.codex/skills/claude-skills--engineering/spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-08-21-stock-eva-r2f2-provider-evidence-design.md --strict
```

Record the actual exit code, score, warnings and errors. The validator checks Markdown structure;
it is run only against the design and never against this implementation plan; it does not certify
code, implementation steps, endpoint semantics, security, tests, or GO status. The implementation
plan is validated only by the manual traceability and static/diff review below.
The manual FR/NFR/AC/EC traceability table above remains mandatory even when the design validator
is green.

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
| FR-12–FR-16 | `market/candidates.py` and automation | full-session, gate audit, mixed-source/fallback rejection |
| FR-17–FR-19, FR-24–FR-26 | models/store/dataset/publication/API readers plus evidence reader | legacy fixtures, lineage mismatch, pointer/read-only fingerprints |
| FR-20–FR-23 | `validate_raw_date_binding(request, batch)`, existing publication requested-date/session identity gate, adapter failure mapping and existing normalizer quality rules | pre-normalize cross-day rejection, unit/factor/suspension and sanitized failure tests; no `normalize.py` date-binding claim |
| FR-27 | whitelist/diff and absence of second-source code | static review and no-network gate |
| FR-28–FR-29 | `providers/base.py` frozen models and role cross-validator | immutable extra-forbid model and exact endpoint variant tests |
| FR-30–FR-31 | `providers/base.py` + `market/evidence.py` | logical-plan/completion and projection digest/cardinality tests |
| FR-32 | `market/candidates.py` | exact ordered ten-outcome aggregate test |
| FR-33 | `market/evidence.py` injected clock plus adapter call boundary | deterministic replay and frozen-clock tests; `normalize.py` remains unchanged |
| NFR-1–NFR-6 | evidence/publish locking, hashes, canonical serializers | crash/concurrency/TOCTOU/determinism tests |
| NFR-7–NFR-12 | additive migration and read-only paths | full regression, legacy byte fingerprints, GET/plan/replay tests |
| NFR-13–NFR-14 | plan/review gates and static registry | commit/review records, dynamic-import scan |
| NFR-15–NFR-17 | evidence layout/CAS, model↔persisted-field audit and manual traceability | descriptor-bound read, strict design validator and numbered matrix; repeat the design's field diff from actual models/SQL |
| NFR-18 | `market/candidates.py` selection policy | fallback rejection and static second-source scan |
| AC-1–AC-3 | Task 7 provider contract | named provider/adapter tests |
| AC-4–AC-7 | Task 8 evidence/replay | hash, crash, corruption, zero-write tests |
| AC-8–AC-11 | Task 9 candidate/selection | gate, lineage, canonical-chain and legacy tests |
| AC-12–AC-14 | Tasks 8–9 read-only/concurrency | fingerprints, CAS and TOCTOU tests |
| AC-15–AC-17 | Tasks 7–8 boundary/cardinality | role, exact variant and plan/completion tests |
| AC-18 | Task 9 gate aggregate | exact ten-outcome aggregate test |
| AC-19 | Task 8 clock/layout/CAS | frozen-clock, storage and replay tests |
| AC-20 | Task 9 validator/fallback boundary | static validator/manual traceability and fallback test |
| EC-1–EC-5 | Task 7 provider/transport contract | sanitized endpoint/failure tests |
| EC-6–EC-9, EC-19–EC-22, EC-24–EC-26 | Task 8 evidence/replay | bounds, corruption, parser, cardinality, digest, clock and storage tests |
| EC-10–EC-18, EC-23, EC-26 | Task 9 candidate/selection | lineage, semantic, aggregate, pointer and fallback tests |

If `spec_validator.py --strict` cannot parse the Markdown because of Chinese/Markdown formatting,
record its actual exit code/output and complete this matrix manually. Do not weaken the spec to make
the validator green.

### Mandatory edge-case and new-boundary traceability

The following rows are mandatory named tests/static proofs, not illustrative examples. Task numbers
refer to the exact RED/GREEN steps above; an absent test is a review blocker.

| Boundary | Task / RED-GREEN step | Required concrete evidence |
| --- | --- | --- |
| EC-3 exact date/session | Task 7.1 / 7.2 | `test_endpoint_schema_variants_reject_date_mismatch_and_mixed_sessions`; `daily_astock`/`index_history` rows outside requested date fail full batch. |
| EC-4 unknown provider code | Task 7.1 / 7.2 | `test_transport_lineage_preserves_unknown_provider_code_and_maps_unknown_protocol`; raw code is bounded, sanitized and mapped only to `UNKNOWN_PROVIDER_PROTOCOL_ERROR`. |
| EC-12 duplicate writer/request | Task 8.1 / 8.2 | `test_evidence_compare_create_allows_one_lineage_and_zero_canonical_writes_for_loser`; concurrent same request key has one complete winner. |
| EC-16 lineage mismatch | Task 9.1 / 9.2 | `test_candidate_rejects_any_lineage_provider_schema_universe_or_hash_mismatch`; old pointer and candidate history fingerprints remain unchanged. |
| EC-17 suspended placeholder | Task 7.1 / 7.2 and Task 9.1 / 9.2 | `test_daily_schema_preserves_legal_suspended_empty_activity_and_factor`; existing exact placeholder gate passes only for legal suspended rows. |
| EC-18 active/suspended/index semantics | Task 7.1 / 7.2 and Task 9.1 / 9.2 | `test_semantic_gate_rejects_active_missing_factor_nonzero_suspended_activity_and_suspended_index`. |
| Six endpoint IDs / nine stock-index schema-role variants | Task 7.1 / 7.2 | `test_each_baostock_endpoint_has_exact_fields_variant_units_and_order`, `test_endpoint_contract_constant_rejects_wrong_combination_and_model_copy`; no unknown fields/union-any. |
| Plan/attempt/object cardinality | Tasks 7.1–8.2 | `test_request_completion_rejects_missing_duplicate_noncontiguous_pages_or_terminal_marker`, `test_manifest_requires_one_descriptor_per_request_shard_and_page`; every attempt/request ID and page joins exactly one descriptor. |
| Transport observation digest | Task 7.1 / 7.2 and Task 8.1 / 8.2 | `test_every_evidence_descriptor_binds_sanitized_transport_observation_digest`; payload/message/URL/token assertions remain negative. |
| Gate cardinality | Task 9.1 / 9.2 | `test_gate_report_requires_exact_ordered_complete_gate_aggregate`; one canonical report/hash only. |
| Replay clock/hash | Task 8.1 / 8.2 | `test_replay_injects_frozen_normalization_clock_and_excludes_invocation_time`; same-version byte equality and explicit cross-version semantic exception. |
| Storage layout/CAS/no-write | Task 8.1 / 8.2 | `test_missing_evidence_root_is_write_free`, `test_evidence_root_ancestor_symlink_and_toc_tou_fail_closed`; selection ordering is owned by the Task 9 test below. |
| Factor snapshot provenance | Task 8.1 / 8.2 and Task 9.1 / 9.2 | `test_factor_cache_snapshot_records_match_current_table_and_model_copy_is_read_only`, `test_factor_resolution_binds_published_snapshot_or_raw_endpoint`; before/after fingerprint and live-cache exclusion. |
| SafeRelativePath semantics/storage | Task 8.1 / 8.2 | `test_safe_relative_path_rejects_semantic_traversal_and_storage_uses_dirfd_containment`, `test_safe_relative_path_model_copy_round_trip_rejects_escape`; construction and `model_copy` fail closed, dirfd/O_NOFOLLOW containment is required. |
| Selection/pointer order | Task 9.1 / 9.2 | `test_selection_publish_order_never_moves_pointer_early`; selection readback precedes existing pointer chain. |
| Fallback boundary | Task 9.1 / 9.2 | `test_qualified_fallback_is_reserved_and_rejected_by_r2f2_writers`; static scan proves no second source/plugin/failover. |

The reviewer MUST manually trace every FR-1–FR-33, NFR-1–NFR-18, AC-1–AC-20 and EC-1–EC-26 to a
Task step and one of these concrete tests or an explicit static/diff proof. The design-only
`spec_validator.py` is not allowed to claim implementation or traceability coverage.

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
