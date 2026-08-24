# Stock EVA R2-F3 Shadow Provider Bake-off Implementation Plan

**Author:** Codex delivery team — implementation-plan owner
**Date:** 2026-08-24 (Asia/Shanghai)
**Status:** **SPEC READY / IMPLEMENTATION NOT STARTED / R2-F3 CODE NO-GO** — documentation-only remediation; no Task 10–13 implementation or real provider window is claimed
**Exact planning base:** 428e890e35b7cedfd45efa810309032e1c7c10e6, clean in worktree codex/r2-f3-shadow-bakeoff
**Design authority:** [R2-F3 Shadow Provider Bake-off Design](2026-08-24-stock-eva-r2f3-shadow-bakeoff-design.md)
**Parent authority:** [R2-F roadmap](2026-08-12-stock-eva-r2f-data-reliability-roadmap.md), [R2-F implementation](2026-08-12-stock-eva-r2f-data-reliability-implementation.md), [R2-F2 design](2026-08-21-stock-eva-r2f2-provider-evidence-design.md), and [R2-F2 implementation](2026-08-21-stock-eva-r2f2-provider-evidence-implementation.md)

**Review remediation:** This revision closes independent-review H1/H2/H3/H4/H5/H7/H8 and M1/M2.
These dedicated Task 10–13 files supersede conflicting umbrella R2-F3 prose. `failover_enabled`
belongs only to R2-F4; R2-F3 has no such state or authority. **CODE GO / SHADOW WINDOW PENDING**
is a future post-implementation gate, not the current status.

## Execution Boundary

This plan is for offline code and synthetic fixtures. It MUST NOT create an account, obtain a
token/points, send a real provider/API request, touch NAS/production, install LaunchAgents, move a
canonical pointer, or change the canonical BaoStock provider. Real canary and the real 20-session
window are separate authority gates below.

The immutable canonical chain remains:

~~~text
BaoStock -> Normalize -> Quality Gate -> Immutable Parquet -> SHA-256
  -> Manifest -> Atomic Publish -> pointer
~~~

Secondary providers use separate ShadowProviderId, shadow contracts, provider_shadow_root,
and explicit provider_registry.sqlite3. They MUST NOT be passed to the current R2-F2
ProviderRawBatch, EvidenceManifest, CandidateManifest, SessionSelection, EvidenceStore,
CandidateStore, canonical writer or selection path. This is intentional: the
current static ProviderId is BaoStock-only, evidence models pin exact R2-F2 adapter/endpoint
constants, and SessionSelection rejects fallback. A future vocabulary-only migration is a
separate reviewed change; it MUST preserve all BaoStock bytes, hashes and readers.

## Provider contract freeze (as of 2026-08-24)

The implementation MUST include a source ledger in docs/data-providers.md using these URLs and
facts. They are discovery records, not runtime verification:

- TickFlow [docs](https://docs.tickflow.org/zh-Hans), [quickstart](https://docs.tickflow.org/zh-Hans/quickstart),
  [homepage](https://tickflow.org/), and [terms](https://tickflow.org/legal/terms/) show public/free
  daily and universe capabilities, but do not prove terms, suspension, units, factor schema,
  quota or retention. State remains discovered.
- Tushare [HTTP](https://tushare.pro/document/1?doc_id=40) documents http://api.tushare.pro;
  [daily](https://tushare.pro/document/1?doc_id=27) is unadjusted, omits suspended rows, uses
  lots for vol and thousand CNY for amount; [adj_factor](https://tushare.pro/document/2?doc_id=28),
  [suspend_d](https://tushare.pro/document/2?doc_id=214), [trade_cal](https://tushare.pro/document/2?doc_id=26),
  [index_daily](https://tushare.pro/document/1?doc_id=95), and [stock_basic](https://tushare.pro/document/1?doc_id=25)
  are documented interfaces. [Points](https://tushare.pro/document/1?doc_id=108) and account/token
  access commonly require 2000 points; [service terms](https://tushare.pro/document/1?doc_id=405)
  describe personal, non-transferable, non-commercial use; [user agreement](https://tushare.pro/document/1?doc_id=409)
  is also required review. HTTPS token transport is prohibited until an official source proves it.

No adapter may infer legal permission, factor equivalence, quota, retention, HTTPS support or
canonical readiness from a successful HTTP response.

## Common TDD and Review Rules

Every task follows: write named RED tests; run the bounded RED command and record the actual
failure; implement the minimum contract; run GREEN; run Ruff/format/diff; obtain an independent
High/Medium review; then make the bounded commit. A failing test must not be hidden by loosening
canonical quality gates or by adding a provider-specific symbol exception.

### Universal offline gate

~~~bash
uv run --extra dev pytest -q --basetemp=/tmp/stock-eva-r2f3-full
uv run --extra dev ruff check backend tests
uv run --extra dev ruff format --check backend tests
git diff --check
~~~

The design-only validator is run against the design, never this implementation plan:

~~~bash
uv run --offline python /Users/finlay/.codex/skills/claude-skills--engineering/spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-08-24-stock-eva-r2f3-shadow-bakeoff-design.md --strict
~~~

It validates Markdown structure and FR/AC/edge-case presence; it is not code, provider, security,
runtime, terms or GO evidence. Implementation-plan review is manual. git diff --check and a
Markdown link/heading check are required.

### Shared offline test fixtures

Use captured synthetic responses only. Include active, suspended/no-row, ST, recent listing,
corporate-action, required-index, malformed, duplicate, wrong-date, unknown-field, non-finite,
rate-limit, timeout, 401/403, 5xx, schema-drift and partial-page cases. HTTP is injected and
asserted to have zero calls in plan mode. Test roots are temporary and never /Volumes/Stock, an
installed Application Support tree, user DB, NAS, or the canonical evidence/candidate root.

### Mandatory golden R2-F2 compatibility fixture (M2)

Before each Task 10–13 RED run, capture immutable fingerprints for one real repository fixture
(read-only copy under pytest `tmp_path`) containing a legacy/new BaoStock manifest, provider
evidence manifest/object, `CandidateManifest`, `selection.json`/`SessionSelection`, canonical GET
response and their SHA-256/reader results. Each task MUST run named tests proving bytes, hashes,
selection ID/SHA, reader output and GET filesystem fingerprints remain identical. A failed golden
fixture blocks that task; it MUST NOT be fixed by rewriting the fixture or widening a canonical
model. This is regression evidence only and does not call a provider.

## Task 10 — Registry, admission and secret-safe configuration

### Scope and file whitelist

Create or modify only these files in the Task 10 commit:

- Create backend/app/market/providers/shadow_contracts.py for ShadowProviderId, admission
  states, static provider-contract descriptors and safe fields.
- Create backend/app/market/shadow_registry_schema.py containing the frozen migration IDs,
  DDL/PRAGMA constants, state-version CAS and lock-order contract.
- Create backend/app/market/providers/registry.py for the writer-owned registry repository,
  transitions, qualification-window reset and read-only status.
- Modify backend/app/config.py to add explicit local provider_registry.sqlite3,
  provider_shadow_root, bounded request/time/object limits, default-off flags and credential
  environment variable names only.
- Modify .env.example with names such as STOCK_EVA_TICKFLOW_TOKEN and
  STOCK_EVA_TUSHARE_TOKEN; values MUST be blank and MUST NOT resemble real tokens.
- Modify backend/app/storage/layout.py only for explicit, relative, no-follow shadow-root layout
  helpers; do not change canonical layout.
- Modify backend/app/cli.py for read-only market-provider-status and a zero-network plan parser.
- Modify backend/app/api/market.py additively for frozen Python response models,
  `UnavailableReason`, and explicit `response_model` (no write-capable status route).
- Create tests/test_market_provider_registry.py and any narrow storage/config test named in the
  RED command.
- Create tests/test_market_provider_registry_golden_compat.py with the mandatory R2-F2 manifest,
  evidence, candidate, selection, GET bytes/hash/reader fixture.
- Create/update docs/data-providers.md with the as-of ledger, role/disposition, terms status and
  non-secret configuration guidance.

No Task 10 change may modify normalize.py, canonical models, evidence.py, candidates.py,
automation.py publication, provider_transport.py, vendor code, canonical storage, NAS or tests
outside the whitelist. The static env map is exact: `tickflow` reads only
`STOCK_EVA_TICKFLOW_TOKEN`; `tushare` reads only `STOCK_EVA_TUSHARE_TOKEN`. Arbitrary env names,
CLI token values, missing approved TermsEvidence, or Tushare without official HTTPS proof MUST be
rejected before HTTP-client construction, with zero network and zero writes. Initial state for both
providers is `discovered`.

### RED

Add tests:

~~~python
def test_shadow_provider_ids_are_static_and_no_dynamic_import(): ...
def test_registry_transitions_are_closed_and_quarantine_resets_version(): ...
def test_missing_or_corrupt_registry_reader_is_unavailable_and_zero_write(tmp_path): ...
def test_registry_secret_env_name_never_serializes_value(): ...
def test_shadow_root_is_distinct_from_canonical_roots(tmp_path): ...
def test_qualification_requires_exactly_twenty_consecutive_sessions(): ...
def test_adapter_policy_terms_or_schema_change_resets_window(): ...
def test_terms_evidence_freezes_urls_bytes_hash_asof_reviewer_review_id_and_decisions(): ...
def test_exact_env_mapping_rejects_arbitrary_env_name_without_reading_client(): ...
def test_tushare_execute_rejects_before_http_client_when_https_or_terms_unapproved(): ...
def test_registry_reader_deserializes_fingerprinted_bytes_or_fails_closed(tmp_path): ...
def test_registry_golden_r2f2_manifest_evidence_candidate_selection_get_is_unchanged(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_provider_registry.py \
  tests/test_market_provider_registry_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-registry-red
~~~

Expected RED is absence of shadow types/repository and missing read-only isolation. Record the
actual failure, not an anticipated count.

### GREEN

Implement a small static registry, not a plugin loader. Freeze r2f3-registry-0001/0002, all DDL
CHECK/FOREIGN KEY/UNIQUE constraints, PRAGMA foreign_keys=ON, WAL, synchronous=FULL, bounded
busy_timeout, migration rollback and permission requirements before the first migration. The
writer uses provider_registry.sqlite3.lock -> SQLite transaction -> shadow bundle lock; each
transition is BEGIN IMMEDIATE plus expected state_version CAS. The read-only path opens the
regular 0600 DB by openat/O_NOFOLLOW, verifies ancestor/basename/fstat fingerprint before and
after bounded bytes, then Connection.deserialize() into :memory:. If deserialize is unavailable
or any TOCTOU/permission/schema check fails, return unavailable; never fall back to a pathname
reader. The registry schema MUST have a version,
provider ID, adapter/endpoint/schema/policy hashes, terms review/hash, intended use,
retention/credential mode, env-name (never value), quota/required-fields/unit contract, state,
window IDs/dates/count, and sanitized quarantine reason. Allowed transitions are:

~~~text
discovered -> canary -> shadow -> qualified
canary/shadow/qualified -> quarantined
quarantined -> canary only after a new reviewed version
~~~

failover_enabled is not a Task 10 state. SQLite initialization/migration is explicit and writer
owned in the independent local registry DB. provider_shadow_root is independent from
provider_evidence_root, canonical candidate/selection roots and dataset root. TermsEvidence is
immutable and stores only official URL allowlist, content-bytes SHA-256, contract version/as-of,
reviewer/review ID, approved intended use/retention/credential/quota decisions; unclosed terms keep
the provider discovered.

Run GREEN/checks:

~~~bash
uv run --extra dev pytest -q tests/test_market_provider_registry.py \
  tests/test_market_provider_registry_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-registry-green
uv run --extra dev ruff check backend/app/market/providers/shadow_contracts.py \
  backend/app/market/providers/registry.py backend/app/market/shadow_registry_schema.py \
  backend/app/config.py backend/app/storage/layout.py backend/app/api/market.py \
  backend/app/cli.py tests/test_market_provider_registry.py \
  tests/test_market_provider_registry_golden_compat.py
uv run --extra dev ruff format --check backend/app/market/providers/shadow_contracts.py \
  backend/app/market/providers/registry.py backend/app/market/shadow_registry_schema.py \
  backend/app/config.py backend/app/storage/layout.py backend/app/api/market.py \
  backend/app/cli.py tests/test_market_provider_registry.py \
  tests/test_market_provider_registry_golden_compat.py
git diff --check
~~~

Independent review MUST verify no secret value, network client, dynamic import, canonical-root
write or failover_enabled path entered the commit. Commit:

~~~bash
git add backend/app/market/providers/shadow_contracts.py \
  backend/app/market/shadow_registry_schema.py backend/app/market/providers/registry.py \
  backend/app/config.py backend/app/storage/layout.py backend/app/api/market.py \
  backend/app/cli.py .env.example tests/test_market_provider_registry.py \
  tests/test_market_provider_registry_golden_compat.py docs/data-providers.md
git commit -m "feat(market): add isolated shadow provider registry"
~~~

## Task 11 — Explicit-canary TickFlow and Tushare adapters

### Scope and file whitelist

- Create backend/app/market/providers/http.py for one injected bounded client.
- Create backend/app/market/providers/tickflow.py and tushare.py for source-shaped adapters.
- Create backend/app/market/shadow_evidence.py containing frozen ShadowAttempt,
  ShadowCompletion, ShadowEvidenceStore and ShadowEvidenceReader models/interfaces.
- Modify backend/app/market/providers/__init__.py only to export static shadow contracts.
- Modify backend/app/config.py, backend/app/cli.py, and pyproject.toml/uv.lock only if a
  reviewed bounded HTTP dependency is needed.
- Create tests/test_market_provider_tickflow.py, tests/test_market_provider_tushare.py, and
  tests/test_market_provider_contract.py.
- Create tests/test_market_shadow_evidence.py and tests/test_market_shadow_evidence_golden_compat.py
  for crash/cancel/orphan/final-attempt and the R2-F2 evidence/selection/GET fixture.
- Modify docs/data-providers.md only for frozen official contract records and status.

No adapter may import or call canonical EvidenceStore, SessionSelection, CandidateStore, dataset
writer or pointer code. It may reuse the R2-F2 successful-attempt-only algorithm concept
through shadow types: a ShadowRequestPlan predicts logical calls, each attempt retains only
sanitized counts/IDs, and only final successful source-shaped pages enter ShadowEvidenceManifest.
This avoids the current global BaoStock constants while preserving their safety semantics.

### RED

Freeze synthetic contract tests first:

~~~python
def test_tickflow_requests_unadjusted_daily_and_universe_without_inference(): ...
def test_tickflow_factor_or_corporate_action_requirement_is_explicit(): ...
def test_tushare_daily_units_are_raw_lots_and_thousand_cny(): ...
def test_tushare_daily_adj_factor_suspend_calendar_index_calls_share_session(): ...
def test_http_client_has_bounded_timeout_retry_after_and_request_budget(): ...
def test_failed_partial_attempt_is_discarded_not_shadow_evidence(tmp_path): ...
def test_http_or_token_never_enters_exception_or_evidence(tmp_path): ...
def test_canary_plan_makes_zero_network_and_zero_write(tmp_path): ...
def test_shadow_evidence_persists_only_final_successful_attempt(tmp_path): ...
def test_shadow_evidence_crash_cancel_and_orphan_are_unreadable(tmp_path): ...
def test_shadow_evidence_bundle_commit_marker_is_atomic_and_idempotent(tmp_path): ...
def test_shadow_evidence_golden_r2f2_manifest_evidence_selection_get_is_unchanged(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_provider_contract.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_shadow_evidence.py tests/test_market_shadow_evidence_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-adapters-red
~~~

Expected RED is absence of adapters and the production HTTP dependency. Record exact failures.

### GREEN

Use injected HTTP, not speculative SDKs. The wrapper MUST enforce connect/read/write/pool timeout,
maximum attempts, bounded Retry-After, response byte/row limits, request counter and sanitized
failure classes. It MUST reject Tushare token transport using HTTP/HTTPS configuration not proven
by the official ledger; until explicit evidence exists, real Tushare execution remains blocked.

Adapters return provider-shaped fields only. Unit conversion, symbol normalization, explicit
suspension state, factor anchoring and canonical comparison units are Task 12 responsibilities.
Adapters never publish evidence or change registry state. ShadowEvidenceStore owns the bounded
source-shaped write. It accepts only a complete ShadowCompletion: failed attempts retain sanitized
IDs/counts/outcome in memory/control audit, all failed partial rows/bytes are discarded before the
next attempt, and only final successful page refs are written. Publication is objects -> manifest ->
bundle.json -> fsync/rename COMMIT; cancel/crash before COMMIT leaves a bounded orphan audit that
ShadowEvidenceReader rejects. Reader uses dirfd/O_NOFOLLOW, pre/post fstat, hash/schema/row/page
checks and treats missing marker, orphan, changed object or non-final attempt as unavailable. The
CLI plan is:

~~~text
market-provider-canary --provider tickflow|tushare --date YYYY-MM-DD
  [--symbols SYMBOL ...] [--execute --external-authorization-id ID]
~~~

execute requires a registry canary state and an external authorization ID; the ID is a
non-secret audit reference. Before constructing the HTTP client, the factory MUST verify the
closed env map (`tickflow` -> `STOCK_EVA_TICKFLOW_TOKEN`, `tushare` ->
`STOCK_EVA_TUSHARE_TOKEN`), immutable TermsEvidence URL allowlist/content hash/version/as-of/
reviewer/review ID/approved intended-use/retention/credential/quota decisions, and official HTTPS
proof. Tushare with absent HTTPS proof or unapproved terms MUST reject at this point with zero
network and zero writes. It may write only a shadow evidence object/report. No command in this
offline task may execute real mode.

Run GREEN/checks:

~~~bash
uv run --extra dev pytest -q tests/test_market_provider_contract.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_shadow_evidence.py tests/test_market_shadow_evidence_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-adapters-green
uv run --extra dev ruff check backend/app/market/providers backend/app/market/shadow_evidence.py \
  backend/app/config.py backend/app/cli.py tests/test_market_provider_contract.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_shadow_evidence.py tests/test_market_shadow_evidence_golden_compat.py
uv run --extra dev ruff format --check backend/app/market/providers backend/app/market/shadow_evidence.py \
  backend/app/config.py backend/app/cli.py tests/test_market_provider_contract.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_shadow_evidence.py tests/test_market_shadow_evidence_golden_compat.py
git diff --check
~~~

Independent review MUST inspect that no provider request occurs, no credentials are created/read
outside env-only injection, no canonical write is reachable, and R2-F2 BaoStock fixtures still
round-trip byte/semantically. Commit:

~~~bash
git add backend/app/market/providers/http.py backend/app/market/providers/tickflow.py \
  backend/app/market/providers/tushare.py backend/app/market/providers/__init__.py \
  backend/app/market/shadow_evidence.py \
  backend/app/config.py backend/app/cli.py pyproject.toml uv.lock \
  tests/test_market_provider_contract.py tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py tests/test_market_shadow_evidence.py \
  tests/test_market_shadow_evidence_golden_compat.py docs/data-providers.md
git commit -m "feat(market): add explicit shadow provider canary adapters"
~~~

### Real-canary authority gate (not executed here)

Before any real execute, the user MUST explicitly approve provider/account/points/cost/terms,
intended use/retention, endpoint scheme, exact date, symbol/full-universe bound, timeout/retry
budget, env variable names and isolated roots. The operator MUST re-read official pages and record
authorization without storing credentials. Start with required indexes plus 5–10 representative
symbols, then one full-universe date. Any schema/unit/terms/quota contradiction quarantines the
version and stops the branch. The canonical pointer remains unchanged.

## Task 12 — Normalize and reconcile complete candidates

### Scope and file whitelist

- Create backend/app/market/shadow_normalize.py for provider-specific-to-canonical comparison
  units and explicit suspension/factor semantics.
- Create backend/app/market/shadow_reconciliation.py for deterministic r2f3-v1 reports.
- Create backend/app/market/shadow_candidates.py containing frozen ShadowQualityReport,
  ShadowCandidateStore and ShadowCandidateReader bundle/reader contracts.
- Create backend/app/market/canonical_comparison.py containing descriptor-bound read-only
  CanonicalComparisonSnapshot and CanonicalComparisonReader.capture(trade_date).
- Modify backend/app/market/providers/tickflow.py and tushare.py only to expose typed source
  evidence needed by the normalizer.
- Modify backend/app/market/models.py only for additive, shadow-namespaced models; do not alter
  DailyBar.source, RefreshResult.source, MarketSummary.source or canonical quality fields.
- Create tests/test_market_shadow_normalize.py and tests/test_market_reconciliation.py; update
  provider tests only for source-contract assertions.
- Create tests/test_market_shadow_candidates.py and tests/test_market_canonical_comparison.py,
  including golden R2-F2 manifest/evidence/candidate/selection/GET byte/hash/reader fixtures.

### RED

~~~python
def test_canonical_comparison_units_are_explicit(): ...
def test_tushare_lots_and_thousand_cny_convert_once(): ...
def test_tickflow_units_are_not_guessed(): ...
def test_no_row_suspension_requires_universe_state(): ...
def test_mixed_provider_symbols_are_rejected(): ...
def test_reconciliation_requires_same_date_universe_and_complete_candidates(): ...
def test_factor_anchor_equivalence_uses_adjusted_returns(): ...
def test_nonfinite_or_impossible_ohlc_is_self_gate_failure(): ...
def test_material_mismatch_quarantines_shadow_only(): ...
def test_shadow_quality_report_and_candidate_bundle_are_complete_or_unavailable(tmp_path): ...
def test_canonical_comparison_snapshot_binds_manifest_partition_and_r2f2_lineage(tmp_path): ...
def test_canonical_comparison_selection_candidate_evidence_gate_factor_normalized_hashes_are_bidirectional(tmp_path): ...
def test_canonical_comparison_drift_returns_unavailable_and_zero_write(tmp_path): ...
def test_shadow_candidate_golden_r2f2_manifest_evidence_selection_get_is_unchanged(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_shadow_normalize.py \
  tests/test_market_reconciliation.py tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py tests/test_market_shadow_candidates.py \
  tests/test_market_canonical_comparison.py \
  --basetemp=/tmp/stock-eva-r2f3-reconcile-red
~~~

### GREEN

Implement provider-specific parsers to a stable comparison model:

~~~text
symbol       sh.600000 / sz.000001
price        unadjusted CNY
volume       shares
amount       CNY
date         exchange trading date
suspension   explicit expected state (not zero-volume inference)
factor       raw value + provider anchor/semantics
~~~

Tushare vol is multiplied by 100 and amount by 1,000 exactly once; this is a declared unit
conversion, not a source-parser mutation. TickFlow is rejected when units are unproven. A
Tushare suspended/no-row symbol is covered only when suspend_d/universe evidence identifies its
expected state. Required indexes are exact. A candidate is complete only when its entire declared
universe is present or legally suspended/not-listed according to the versioned contract.

ShadowReconciliationReport uses deterministic semantic IDs/hashes and bounded sanitized samples:

~~~text
status = ready | material_mismatch | unavailable
policy = r2f3-v1
universe/index/suspension sets = exact
OHLC = <= one legal tick
active volume = >=99.9% within 0.1% relative error
amount = >=99.9% within 0.5% relative error
adjusted return around corporate actions = <=5 bp after source anchor
~~~

Any self-quality failure or material unexplained mismatch rejects/quarantines the secondary
contract. It never calls select_primary_candidate, writes SessionSelection, or changes
canonical bytes.

ShadowCandidateStore writes ShadowQualityReport, normalized object and ShadowCandidateManifest only
after the complete shadow evidence reader succeeds; ShadowCandidateReader requires one committed
bundle and revalidates every object/manifest hash before returning it. CanonicalComparisonReader is
strictly read-only: one capture takes the current immutable Dataset manifest generation/identity,
exact BaoStock trade-date partition path/SHA/row count and nine R2-F2 publication-lineage fields
(`provider_id`, `universe_id`, `evidence_id`, `evidence_sha256`, `candidate_id`,
`candidate_manifest_sha256`, `gate_report_sha256`, `adapter_version`, `source_schema_version`),
then descriptor-bound reads the existing candidate.json, gate.json, selection.json, normalized.json
and EvidenceReader. It reruns current model validators and bidirectionally checks selection ID/SHA,
candidate/evidence/gate/factor/normalized hashes, date/universe/provider and partition bar
semantics, then verifies the Dataset manifest identity again. Any missing/legacy lineage, object
change, hash/date/universe mismatch or generation drift returns the frozen unavailable reason and
performs zero writes/provider calls; no new canonical fields are introduced.

Run GREEN/checks:

~~~bash
uv run --extra dev pytest -q tests/test_market_shadow_normalize.py \
  tests/test_market_reconciliation.py tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py tests/test_market_shadow_candidates.py \
  tests/test_market_canonical_comparison.py \
  --basetemp=/tmp/stock-eva-r2f3-reconcile-green
uv run --extra dev ruff check backend/app/market/shadow_normalize.py \
  backend/app/market/shadow_reconciliation.py backend/app/market/shadow_candidates.py \
  backend/app/market/canonical_comparison.py backend/app/market/providers \
  backend/app/market/models.py tests/test_market_shadow_normalize.py \
  tests/test_market_reconciliation.py tests/test_market_shadow_candidates.py \
  tests/test_market_canonical_comparison.py
uv run --extra dev ruff format --check backend/app/market/shadow_normalize.py \
  backend/app/market/shadow_reconciliation.py backend/app/market/shadow_candidates.py \
  backend/app/market/canonical_comparison.py backend/app/market/providers \
  backend/app/market/models.py tests/test_market_shadow_normalize.py \
  tests/test_market_reconciliation.py tests/test_market_shadow_candidates.py \
  tests/test_market_canonical_comparison.py
git diff --check
~~~

Independent review MUST verify exact-one conversion, no amount/OHLCV fund-flow claim, complete
candidate requirement, no symbol mixing, no canonical selection call, and a BaoStock legacy
manifest/hash/GET fingerprint regression. Commit:

~~~bash
git add backend/app/market/shadow_normalize.py \
  backend/app/market/shadow_reconciliation.py backend/app/market/shadow_candidates.py \
  backend/app/market/canonical_comparison.py backend/app/market/providers/tickflow.py \
  backend/app/market/providers/tushare.py backend/app/market/models.py \
  tests/test_market_shadow_normalize.py tests/test_market_reconciliation.py \
  tests/test_market_shadow_candidates.py tests/test_market_canonical_comparison.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_provider_contract.py
git commit -m "feat(market): reconcile complete shadow candidates"
~~~

## Task 13 — Shadow scheduler and qualification report

### Scope and file whitelist

- Create backend/app/market/shadow_scheduler.py for post-canonical bounded work and session
  window evaluation.
- Create backend/app/market/shadow_jobs.py containing durable ShadowJobStore outbox, lease/CAS,
  recovery scanner and immutable ConfirmedSessionSnapshot/ShadowAttemptReport persistence.
- Modify backend/app/market/automation.py only to enqueue a bounded handoff after the canonical
  pointer/manifest commit and after RefreshRunLock is released; no shadow work or blocking enqueue
  may run inside the canonical lock.
- Modify backend/app/market/providers/registry.py for report/window transitions only.
- Modify backend/app/api/market.py additively for read-only shadow status; no write on GET.
- Modify backend/app/cli.py for plan/shadow/status/promote parser and explicit authorization gate.
- Create tests/test_market_shadow.py, update tests/test_market_get_read_only.py only for
  read-only shadow fingerprints, and create docs/acceptance/release-2-r2f3.md as a NO-GO/pending
  evidence skeleton.
- Create tests/test_market_shadow_jobs.py and tests/test_market_shadow_jobs_golden_compat.py for
  transaction ordering, crash/lease recovery, scanner recovery and the R2-F2 golden fixture.
- Modify docs/data-providers.md only for report schema/status.

### RED

~~~python
def test_shadow_runs_after_canonical_attempt_without_delaying_pointer(tmp_path): ...
def test_shadow_failure_does_not_change_refresh_result_or_canonical_bytes(tmp_path): ...
def test_shadow_writes_only_shadow_evidence_candidate_report(tmp_path): ...
def test_shadow_get_status_is_zero_write_on_missing_root(tmp_path): ...
def test_nineteen_sessions_or_gap_is_not_qualified(tmp_path): ...
def test_contract_policy_or_terms_change_resets_window(tmp_path): ...
def test_mismatch_quarantines_secondary_not_primary(tmp_path): ...
def test_report_contains_successes_and_failures_without_secrets(tmp_path): ...
def test_canonical_pointer_commit_precedes_lock_release_and_nonblocking_handoff(tmp_path): ...
def test_enqueue_or_worker_failure_cannot_change_or_delay_canonical_result(tmp_path): ...
def test_published_but_unenqueued_manifest_is_recovered_by_scanner(tmp_path): ...
def test_worker_lease_reclaims_after_crash_and_completion_is_idempotent(tmp_path): ...
def test_confirmed_session_snapshot_freezes_calendar_generation_and_next_sessions(tmp_path): ...
def test_each_attempt_report_records_success_failure_skip_unavailable_or_mismatch(tmp_path): ...
def test_gap_failure_or_version_drift_resets_window_in_one_transaction(tmp_path): ...
def test_shadow_scheduler_golden_r2f2_manifest_evidence_selection_get_is_unchanged(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_shadow.py \
  tests/test_market_get_read_only.py -k 'shadow or provider_status' \
  tests/test_market_shadow_jobs.py tests/test_market_shadow_jobs_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-shadow-red
~~~

### GREEN

The scheduler invokes a secondary only after canonical freshness/repair has made its decision,
within an independent deadline and request budget. It MUST be cancelable and MUST record
budget_exhausted rather than retrying or delaying canonical work. The canonical writer commits its
manifest and pointer under RefreshRunLock, determines the canonical outcome, releases that lock,
and only then performs a nonblocking bounded handoff to ShadowJobStore. Enqueue/worker failure
cannot change or delay the already-determined canonical result. A background scanner reads immutable
canonical manifests and recovers every published-but-unenqueued session idempotently. ShadowJobStore
uses its own lock/SQLite transaction, state_version CAS, bounded lease and cancellation fields;
crashed leases are reclaimable and duplicate completion is a no-op. No shadow work is attached to a
post_publish callback while RefreshRunLock is held.

Each confirmed session begins with immutable ConfirmedSessionSnapshot containing exact calendar
generation/hash, next confirmed sessions, universe hash and capture identity. Every attempt emits a
ShadowAttemptReport outcome success/failure/skip/unavailable/mismatch with timing, request/retry/
rate-limit counts, coverage, version vector and all relevant hashes. Attempt, session and
qualification-window counters/reset are committed in one SQLite transaction/CAS; any gap, failure,
unavailable/mismatch or adapter/endpoint/source-schema/normalizer/reconcile/terms/universe/
calendar/config drift resets the window while retaining every prior report. GET
/api/v1/market/provider-status and status CLI use the descriptor-bound deserialize reader and
no-initialize shadow root.

CLI:

~~~text
market-provider-shadow --provider PROVIDER --start DATE --end DATE
  [--execute --external-authorization-id ID]
market-provider-promote --provider PROVIDER --window-id ID --review-id ID [--execute]
~~~

The machine evaluator includes every confirmed session (success, failure, unavailable, mismatch,
budget exhaustion), exact calendar/universe IDs, canonical timing, shadow timing, request/retry/
rate-limit counts, coverage, gate/reconciliation status, evidence/candidate/report hashes and
failure class. Qualification requires the same adapter/endpoint/schema/normalizer/policy/terms
hashes, a continuous confirmed trading-session sequence of 20, full-universe coverage, timing and
reconciliation tolerances, and an explicit reviewed promotion. It does not enable fallback.

Run GREEN/checks:

~~~bash
uv run --extra dev pytest -q tests/test_market_shadow.py \
  tests/test_market_reconciliation.py tests/test_market_provider_registry.py \
  tests/test_market_get_read_only.py \
  tests/test_market_shadow_jobs.py tests/test_market_shadow_jobs_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-shadow-green
uv run --extra dev ruff check backend/app/market/shadow_scheduler.py backend/app/market/shadow_jobs.py \
  backend/app/market/automation.py backend/app/market/providers/registry.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_shadow.py \
  tests/test_market_get_read_only.py tests/test_market_shadow_jobs.py \
  tests/test_market_shadow_jobs_golden_compat.py
uv run --extra dev ruff format --check backend/app/market/shadow_scheduler.py backend/app/market/shadow_jobs.py \
  backend/app/market/automation.py backend/app/market/providers/registry.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_shadow.py \
  tests/test_market_get_read_only.py tests/test_market_shadow_jobs.py \
  tests/test_market_shadow_jobs_golden_compat.py
git diff --check
~~~

Run the universal offline gate and the design-only validator. Independent reviewer checks the
exact whitelist, no canonical call graph, no secrets, no network, reset logic and all-failure
reporting. Commit:

~~~bash
git add backend/app/market/shadow_scheduler.py backend/app/market/shadow_jobs.py \
  backend/app/market/automation.py \
  backend/app/market/providers/registry.py backend/app/api/market.py backend/app/cli.py \
  tests/test_market_shadow.py tests/test_market_shadow_jobs.py \
  tests/test_market_shadow_jobs_golden_compat.py tests/test_market_get_read_only.py \
  docs/acceptance/release-2-r2f3.md docs/data-providers.md
git commit -m "feat(market): schedule isolated provider shadow reports"
~~~

After Tasks 10–13 pass their focused/full checks and independent review, the implementation result
may become **CODE GO / SHADOW WINDOW PENDING**, never R2-F3 GO. This documentation revision remains
**SPEC READY / IMPLEMENTATION NOT STARTED / R2-F3 CODE NO-GO**.

## Real 20-session qualification (separate authorized stage)

This stage MUST NOT be run as part of Tasks 10–13 offline work. Before it starts, obtain explicit
external authorization naming provider, account/points/cost/terms, intended use/retention,
transport scheme, credential env names, isolated roots, request budget, date range and operator.
Re-read the official ledger pages as-of the authorization date; no token is placed in files or
logs. Start with the bounded representative canary, then one full-universe date. Keep canonical
failover disabled and verify pointer/manifest before and after.

For each confirmed trading session, record whether canonical ran, shadow ran/skipped/failed,
request/retry/rate-limit counts, timing, full universe/index/suspension coverage, self-quality,
reconciliation, adapter/contract/policy/universe/calendar/terms hashes and sanitized failure. A
session is successful only if the same frozen contract completes all gates; 20 successes with a
gap are not consecutive. Any provider/adapter/schema/terms/policy/universe/calendar/config
material change, missing session, failure, material mismatch or unverifiable state resets the
window. Promotion is an explicit reviewed action; no automatic failover_enabled transition.

Qualification report acceptance:

- 20 consecutive confirmed sessions, same adapter/contract/policy/terms hashes;
- zero missing canonical dates and no shadow-induced canonical delay;
- full legal versioned-universe and required-index coverage each session;
- reconciliation tolerances in the design and no unresolved material mismatch;
- all failures and unavailable sessions included, secrets absent;
- exact commit, installed status (only if separately authorized), roots, hashes, reports and
  independent review decision recorded.

Without every item, report **NO-GO / WINDOW RESET** and leave provider non-qualified.

## Rollback and acceptance evidence

Rollback disables shadow scheduling, marks the provider discovered/quarantined, removes or revokes
runtime credentials, and leaves BaoStock canonical/pointer/old evidence/candidate/selection and
user data untouched. Shadow evidence/report retention follows the approved terms; no destructive
cleanup is needed to roll back. A contract re-entry requires a new reviewed version and a new
20-session window.

docs/acceptance/release-2-r2f3.md MUST initially state NO-GO — real shadow window not run and
list exact code commits, RED/GREEN commands, full gate, design-validator result, files, offline
limitations, and independent review. It may be changed to R2-F3 GO only after the authorized
20-session report and independent review; code completion alone is insufficient.

## Traceability and independent review gate

| Requirement | Task/file evidence | Required tests/review |
|---|---|---|
| Registry/admission/secrets (H5/H7/M1) | Task10 shadow contracts, frozen migration schema, registry, config, layout, API response, env example | exact env/TermsEvidence rejection-before-client; DDL/PRAGMA/CAS/lock/deserialize/fingerprint; named zero-write reasons; `test_registry_golden_r2f2_manifest_evidence_candidate_selection_get_is_unchanged` |
| TickFlow/Tushare contracts | Task11 HTTP/adapters/docs | synthetic schema/unit/date/error/request-budget tests; initial `discovered`; Tushare no HTTPS/terms rejects before client |
| Successful-attempt-only (H2) | Task11 `shadow_evidence.py` Store/Reader | `test_shadow_evidence_persists_only_final_successful_attempt`, crash/cancel/orphan/atomic-marker and golden evidence tests |
| Normalization/quality | Task12 shadow normalizer/models | units, suspension/no-row, factors, finite/impossible values |
| Candidates/comparison (H3) | Task12 `shadow_candidates.py`, `canonical_comparison.py` | complete report/bundle, Dataset manifest/partition + R2-F2 lineage/hash drift unavailable, golden candidate/selection/GET tests |
| Reconciliation | Task12 reconciliation | exact set, tolerance, anchor equivalence, mismatch quarantine |
| Scheduler isolation (H4) | Task13 `shadow_jobs.py` outbox, scanner, scheduler/automation | pointer-before-release, enqueue/worker failure zero impact, scanner recovery, lease/CAS/idempotency and golden tests |
| Confirmed sessions/window (H8) | Task13 snapshots/reports/version vector/transaction | every outcome, gap/failure/drift reset in one CAS transaction, report retention |
| Status/read-only (M1) | Task10/13 API/CLI | frozen response_model, enum reasons, missing root/DB/descriptor fingerprints and zero-write GET |
| 20-session qualification | Task13 registry/report/acceptance | 19+gap reset, version reset, all failures, explicit promotion |
| R2-F2 compatibility (M2) | every task whitelist and named golden fixture | old BaoStock manifest/evidence/candidate/selection/GET byte/hash/reader fingerprints |
| Safety/out-of-scope | docs, static scan, authority record | no provider request/account/NAS/production; independent H/M review |

An independent reviewer MUST decide each of: High/Medium zero, exact files only, canonical path
untouched, shadow root/control isolation, successful-attempt-only plus crash/orphan audit, complete
candidate and descriptor-bound canonical comparison, exact env/TermsEvidence gate, registry DDL/
PRAGMA/lock/CAS/deserialize safety, pointer-before-release outbox ordering, lease/idempotence,
transactional session/window reset with all reports retained, every named golden R2-F2 fixture,
no mixed sources, no secret output, zero-network dry-run, no automatic failover, and clear current
status **SPEC READY / IMPLEMENTATION NOT STARTED / R2-F3 CODE NO-GO**. Any High/Medium finding
requires a new documentation or RED->GREEN fix/review commit. No reviewer may convert the offline
gate into R2-F3 GO without the real authorized window.
Any High/Medium finding requires a new RED->GREEN fix/review commit. No reviewer may convert the
offline gate into R2-F3 GO without the real authorized window.

## Definition of Done

Tasks 10–13 may be called code-complete only when the offline universal gate, strict design
validator, focused tests, Ruff/format/diff checks and independent review pass, all named files/
commits are recorded, and acceptance changes to **CODE GO / SHADOW WINDOW PENDING**. This revision
is still **SPEC READY / IMPLEMENTATION NOT STARTED / R2-F3 CODE NO-GO**. R2-F3 itself remains
**NO-GO** until the separate real canary authorization and same-contract 20-consecutive-session
qualification pass.
