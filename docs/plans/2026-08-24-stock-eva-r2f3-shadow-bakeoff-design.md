# Stock EVA R2-F3 Shadow Provider Bake-off Design

**Author:** Codex delivery team — specification owner
**Date:** 2026-08-24 (Asia/Shanghai)
**Status:** **CODE GO / SHADOW WINDOW PENDING** — specification and offline implementation may proceed; no real provider window has run
**Decision authority:** User-authorized offline specification only. Real canary, credentials, account/points, cost/terms acceptance and the 20-session window require a separate explicit external authorization.
**Baseline:** clean worktree at `428e890e35b7cedfd45efa810309032e1c7c10e6`; this document is a planning artifact and does not claim implementation, provider qualification or R2-F3 GO.
**Scope:** R2-F3 Tasks 10–13: provider registry, explicit-canary adapters, complete-session normalization/reconciliation, and isolated shadow scheduling/reporting.

## Context

R2-F3 evaluates a secondary source without weakening the after-close canonical chain:

```text
BaoStock primary -> Normalize -> Quality Gate -> Immutable Parquet -> SHA-256
  -> Manifest -> Atomic Publish -> current pointer -> local serving API
                         |
                         +-- independent shadow evidence/candidate/reconciliation report
```

The current R2-F2 contracts are deliberately BaoStock-only at this baseline:

- `backend/app/market/providers/base.py::ProviderId` admits only `baostock`, and
  `provider_registry()` rejects any other value.
- `ProviderRawBatch` and `EvidenceManifest` require the exact R2-F2 adapter and endpoint-contract
  versions and six-endpoint typed allowlist.
- `CandidateManifest` is a complete one-provider candidate, while `SessionSelection` currently
  accepts only `primary_ready`, rejects `qualified_fallback`, and is consumed by the canonical
  publication path.
- `backend/app/market/evidence.py` already supplies descriptor-bound, hash-checked, immutable
  evidence and `EvidenceStore` publication; `backend/app/market/candidates.py` supplies the
  ten-gate R2-F2 aggregate and canonical selection boundary.

R2-F3 therefore uses a separate `ShadowProviderId`, `ShadowEvidenceManifest`,
`ShadowCandidateManifest`, `ShadowReconciliationReport`, and `ShadowSessionReport`. This is the
safer compatibility choice: widening a vocabulary-only enum can be a later additive migration,
but passing a secondary batch through the current globally fixed R2-F2 models would either reject
it or tempt a canonical-writer change. No dynamic plugin import is introduced. A future migration
may add `tickflow`/`tushare` to a static provider vocabulary only after a model/persisted-field
compatibility review; that migration MUST leave all BaoStock v1 bytes, hashes, readers and
`primary_ready` selections unchanged.

### Official-facts contract ledger (as of 2026-08-24)

The following are discovery facts from the named official pages, not verified runtime capability.
The registry MUST preserve `discovered` until terms, schema, units, suspension/universe behavior,
quota, retention and a bounded canary are proven.

| Provider | Official source and fact frozen for planning | Contract consequence |
|---|---|---|
| TickFlow | [docs](https://docs.tickflow.org/zh-Hans), [quickstart](https://docs.tickflow.org/zh-Hans/quickstart), [homepage](https://tickflow.org/), [terms](https://tickflow.org/legal/terms/) expose a public/free daily API and a universe path, but the pages do not yet prove terms content, suspension semantics, units, factor schema, quota or retention. | `discovered`; daily/universe are canary hypotheses only. No canonical or qualified status may be inferred. |
| Tushare Pro | [HTTP document](https://tushare.pro/document/1?doc_id=40) documents `http://api.tushare.pro` only. [daily](https://tushare.pro/document/1?doc_id=27) is unadjusted and omits suspended rows; `vol` is lots and `amount` is thousand CNY. Official [adj_factor](https://tushare.pro/document/2?doc_id=28), [suspend_d](https://tushare.pro/document/2?doc_id=214), [trade_cal](https://tushare.pro/document/2?doc_id=26), [index_daily](https://tushare.pro/document/1?doc_id=95), and [stock_basic](https://tushare.pro/document/1?doc_id=25) endpoints exist. The [points page](https://tushare.pro/document/1?doc_id=108) and account/tiers normally require an account and commonly 2000 points; [service terms](https://tushare.pro/document/1?doc_id=405) describe personal, non-transferable, non-commercial use; [user agreement](https://tushare.pro/document/1?doc_id=409) is also in scope. | `discovered`; no account, token, points purchase or request is authorized. Because official HTTP documentation only shows HTTP, token transport over HTTPS is prohibited until an official page proves HTTPS. |

These facts MUST be re-reviewed before any real canary. A documentation URL is not proof of
availability, legality, data quality, or a stable schema. No provider is endorsed.

## Functional Requirements

- FR-1: The registry MUST store a static `ShadowProviderId` of `tickflow` or `tushare` and
  MUST reject arbitrary IDs, dynamic imports, path-like values and unknown adapters.
- FR-2: A registry record MUST include adapter, endpoint-contract, source-schema,
  reconciliation-policy and terms-evidence hashes, intended use, retention decision, credential
  mode, quota contract, required fields and units. Any material version change MUST reset the
  qualification window.
- FR-3: `discovered -> canary -> shadow -> qualified` MUST be the only forward shadow path;
  `canary/shadow/qualified -> quarantined` MUST be allowed; `quarantined -> canary` MUST require
  a new reviewed adapter/terms/contract version. R2-F3 MUST NOT create `failover_enabled`.
- FR-4: Credentials MUST be environment-only `SecretStr` values or an equivalent secret
  reference. They MUST NOT appear in models, repr, logs, evidence, manifests, reports, CLI JSON,
  exception text, URLs or persisted control data.
- FR-5: TickFlow and Tushare adapters MUST expose source-shaped daily/universe/index/factor/
  suspension/calendar evidence through injected bounded HTTP clients. Adapters MUST NOT normalize,
  publish canonical data, move a pointer, or change admission state.
- FR-6: Tushare adapter contracts MUST explicitly model unadjusted daily data, no-row suspension,
  lots, thousand-CNY amount, `adj_factor`, `suspend_d`, `trade_cal`, `index_daily`, and
  `stock_basic`; conversion belongs to the shadow normalizer exactly once.
- FR-7: TickFlow MUST remain `discovered` until unadjusted daily bars, universe/index coverage,
  factor/corporate-action evidence, suspension semantics, units, quota and retention are proven.
  Adjusted close alone MUST NOT satisfy the canonical factor contract.
- FR-8: A shadow attempt MUST capture one complete `(trade_date, universe_id)` candidate per
  provider. Symbol-level source mixing, partial candidate promotion and cross-session stitching
  MUST be rejected.
- FR-9: Shadow evidence MUST retain only final successful attempt pages/rows; failed partial
  payloads MUST be discarded and MUST NOT become evidence, candidate, report or quarantine object.
  Sanitized failure counters/IDs MAY remain in control audit.
- FR-10: Shadow normalization MUST produce canonical comparison units: lowercase `sh./sz.`
  symbols, unadjusted CNY prices, shares volume, CNY amount, exchange date, explicit suspension,
  and provider-declared factor semantics. Unknown units/schema/date semantics MUST fail closed.
- FR-11: A complete candidate MUST pass self-quality gates before reconciliation: exact legal
  universe/index set, requested date, finite/impossible-value checks, suspension rules, factor
  evidence, and 100% expected coverage after declared-state exclusions.
- FR-12: `ReconciliationPolicy(version="r2f3-v1")` MUST compare complete candidates only:
  exact eligible symbol/index/suspension sets, OHLC within one legal tick, active volume at least
  99.9% within 0.1% relative error, amount at least 99.9% within 0.5%, and adjusted returns near
  corporate actions within 5 basis points after provider-specific anchoring. Raw factor numbers
  need not match.
- FR-13: A material unexplained mismatch MUST quarantine the secondary contract version and
  MUST NOT demote, delay or mutate a self-validating BaoStock primary candidate.
- FR-14: Shadow scheduling MUST run only after the canonical freshness/repair decision and
  MUST have independent request/time budgets. Budget exhaustion or shadow failure MUST leave the
  canonical result, Parquet bytes, manifest, SHA and pointer unchanged.
- FR-15: Default CLI plan/dry-run MUST perform zero network calls and zero writes. `--execute`
  MUST be explicit and limited to evidence/shadow roots and the registry control DB; it MUST NOT
  write canonical evidence, candidate, selection, Parquet, pointer, user DB, NAS or production.
- FR-16: The shadow status/read API MUST be read-only and MUST report unavailable for missing,
  corrupt or uninitialized shadow registry/root without creating files, tables or migrations.
- FR-17: A provider MAY be promoted to `qualified` only by an explicit reviewed action after
  exactly 20 consecutive confirmed trading sessions using the same adapter, endpoint contract,
  schema, normalization, reconciliation policy and universe/calendar versions. Any gap, failure,
  material mismatch, or material version/config change MUST reset the window.
- FR-18: No R2-F3 GO may be declared without a completed real authorized shadow window. Offline
  code completion is exactly `CODE GO / SHADOW WINDOW PENDING`.

## Non-Functional Requirements

- **NFR-1 (canonical isolation):** BaoStock remains canonical primary; automatic failover is OFF and
  no shadow code may call `SessionSelection`, canonical `CandidateStore`, canonical evidence root,
  dataset writer or pointer publisher.
- **NFR-2 (security):** Secrets, cookies, authorization headers, raw provider payloads and arbitrary
  exceptions MUST be excluded from output. Credentials MUST be revoked/removed on rollback.
- **NFR-3 (determinism):** IDs/reports/hashes MUST derive from semantic inputs and versioned clocks,
  not wall-clock invocation time; one evidence identity MUST be idempotent.
- **NFR-4 (integrity):** Shadow files MUST use SHA-256, bounded sizes/rows, safe relative paths,
  dirfd + no-follow reads, atomic compare-create and immutable content-addressed objects.
- **NFR-5 (persistence isolation):** Registry state MUST use an explicitly initialized independent
  `provider_registry.sqlite3` local control DB; readers use SQLite URI read-only. Missing DB/table,
  corrupt schema or failed migration is `unavailable` with zero writes.
- **NFR-6 (concurrency):** One writer owns registry migrations and shadow publication. CAS/no-clobber
  object writes and an explicit writer lock MUST prevent duplicate session finalization.
- **NFR-7 (availability):** Shadow work MUST be bounded and cancelable; it MUST never extend the
  canonical availability deadline or block BaoStock publication.
- **NFR-8 (observability):** Reports MUST include all successes and failures, request counts,
  retry/rate-limit observations, coverage, timing, gate verdicts, reconciliation counts, version
  hashes and sanitized failure classes.
- **NFR-9 (compatibility):** Existing BaoStock evidence manifests, candidate/selection hashes,
  canonical Parquet, legacy readers, GET responses and five legacy publication-gate strings MUST
  remain byte/semantic compatible. No rewrite or backfill is implied.
- **NFR-10 (terms):** Terms and intended-use review is a hard admission gate; no retention,
  redistribution or commercial use is assumed from a public endpoint.
- **NFR-11 (offline boundary):** Tests use synthetic captured responses only; no provider/API
  request, account creation, token acquisition, points purchase, NAS access or production mutation
  occurs in the code/spec gate.
- **NFR-12 (reviewability):** Every task has RED then GREEN evidence, bounded files/commit, an
  independent review gate, and explicit authority status.

## Acceptance Criteria

The numbered criteria below collectively cover FR-1, FR-2, FR-3, FR-4, FR-5, FR-6, FR-7, FR-8,
FR-9, FR-10, FR-11, FR-12, FR-13, FR-14, FR-15, FR-16, FR-17 and FR-18.

### AC-1: Registry isolation and transitions (FR-1–FR-4, NFR-5, NFR-6)

Given a missing/corrupt `provider_registry.sqlite3` or an unapproved provider record
When a read-only status or transition is requested
Then status is `unavailable`/rejected, no DB/table/file is created, and no secret is serialized.

### AC-2: Dry-run boundary (FR-14, FR-15, FR-16, NFR-1, NFR-11)

Given a valid date/provider plan and temporary roots
When the CLI runs without `--execute`
Then it reports bounded endpoints/request count, performs zero network calls and zero filesystem,
SQLite, canonical-root, pointer, user-store or NAS writes.

### AC-3: Successful-attempt-only shadow evidence (FR-5, FR-8–FR-10, NFR-3–NFR-4)

Given synthetic pages where an early attempt partially returns rows and a later attempt succeeds
When the shadow adapter publishes evidence
Then only final successful pages are content-addressed and atomically published; partial rows,
failed payloads and raw exceptions are absent; replay gives the same semantic hash.

### AC-4: Provider-specific contract (FR-5–FR-7, NFR-10)

Given a TickFlow unknown-unit response or a Tushare response with wrong date, field, unit, HTTP
scheme, or no approved terms/credential record
When the adapter contract validates it
Then the candidate fails closed and remains `discovered`/`quarantined`; no canonical path runs.

### AC-5: Unit and suspension normalization (FR-6, FR-10–FR-11)

Given Tushare `vol=100` lots, `amount=20` thousand CNY and a suspension with no daily row
When normalization consumes the declared contract
Then it yields 10,000 shares and 20,000 CNY exactly once, represents the suspension through the
universe state, and rejects any inferred or double conversion.

### AC-6: Complete-candidate reconciliation (FR-8, FR-11–FR-13, NFR-1)

Given two complete same-date/full-universe candidates with a material mismatch
When `r2f3-v1` reconciliation runs
Then it emits a deterministic sanitized mismatch report, quarantines only the secondary contract,
and leaves BaoStock evidence/candidate/selection/pointer bytes unchanged.

### AC-7: Shadow scheduler isolation (FR-14, FR-15, FR-16, NFR-7)

Given a canonical run that is ready, partial, or error and a shadow adapter failure
When the scheduler executes its post-canonical shadow lane
Then canonical result and pointer behavior are unchanged; shadow evidence/report writes are
isolated; the scheduler records `failed`/`budget_exhausted` without retrying canonical work.

### AC-8: Qualification reset (FR-17–FR-18, NFR-8, NFR-12)

Given 19 successful sessions, then a missing session, provider failure, policy/hash change, or
non-consecutive date
When the qualification evaluator computes status
Then successful count resets to zero/window start and no qualified status or R2-F3 GO is emitted.

### AC-9: 20-session gate and authority (FR-17–FR-18, NFR-10, NFR-12)

Given the exact same adapter/contract/policy has 20 consecutive real, authorized, full-universe
sessions and all terms/timing/reconciliation gates pass
When an explicitly reviewed promotion is applied
Then registry status may become `qualified`, automatic failover remains disabled, and the acceptance
record names the exact window, versions, reports, commit and reviewer. Without this evidence the
status is `CODE GO / SHADOW WINDOW PENDING`.

### AC-10: Legacy BaoStock compatibility (NFR-1, NFR-9)

Given a legacy R2-F2 BaoStock manifest, evidence object, `primary_ready` selection and GET fixture
When R2-F3 registry/status/shadow code is installed or read
Then old hashes, bytes, model dumps, reader behavior and GET filesystem fingerprints are unchanged;
the fixture is not rewritten and no secondary appears in canonical status.

### AC-11: Secret and failure sanitization (FR-4, NFR-2)

Given a token, HTTP authorization header, provider URL query, SQL path or arbitrary exception
When a canary/shadow failure is serialized
Then only allowlisted class, bounded counts and safe IDs remain; a grep/fingerprint test finds none
of the injected values in logs, reports, evidence, manifests or CLI output.

## Edge Cases

- EC-1: Missing or corrupt registry DB/table returns `unavailable` and performs zero writes.
- EC-2: Unknown/provider-like/path-like ID, dynamic import or mixed-case ID is rejected.
- EC-3: Token missing, supplied on CLI, embedded in URL, or present in exception is sanitized and
  remains `discovered`; no network is attempted without explicit authorization.
- EC-4: Official HTTP documentation only proves `http://`; Tushare token transport over HTTPS is
  prohibited until an official source proves HTTPS.
- EC-5: TickFlow daily/free endpoint lacks proof of factors, suspension, quota, retention or
  terms; it remains discovery evidence, never qualification.
- EC-6: Tushare suspended symbol has no daily row; universe/suspension evidence must explain the
  expected state or the candidate fails coverage, not silently drop the symbol.
- EC-7: Duplicate, cross-date, non-finite, wrong-unit, unknown-field or partial rows reject the
  entire candidate and cannot be reconciled.
- EC-8: Different factor anchors with equivalent adjusted returns can pass; equal raw factors
  with divergent adjusted returns fail.
- EC-9: One provider has 3,193 rows and the other 2; no row-level stitching is permitted.
- EC-10: Primary unavailable and shadow candidate ready does not publish fallback in R2-F3;
  automatic failover is out of scope and OFF.
- EC-11: Shadow timeout/rate limit/budget exhaustion records a sanitized report and never delays
  canonical publication.
- EC-12: Evidence object/manifest replacement, symlink, traversal, size/row overflow or hash
  mismatch fails closed before normalization.
- EC-13: Two shadow writers race; one content identity wins, the other verifies or returns CAS
  conflict, with one report finalization.
- EC-14: A 20-success series with a calendar gap is not 20 consecutive trading sessions.
- EC-15: Adapter, schema, terms, universe, calendar or reconciliation policy change resets the
  window and cannot inherit qualification.
- EC-16: GET/status on an uninitialized shadow root does not initialize it, migrate it or mutate
  its mtime.

## API Contracts

These are offline/testable contracts; no endpoint is permission to call a real provider.

### CLI

```text
market-provider-status [--provider tickflow|tushare]
market-provider-canary --provider tickflow|tushare --date YYYY-MM-DD
  [--symbols SYMBOL ...] [--execute --external-authorization-id ID]
market-provider-shadow --provider tickflow|tushare --start DATE --end DATE
  [--execute --external-authorization-id ID]
market-provider-promote --provider tickflow|tushare --window-id ID
  [--review-id ID] [--execute]
```

Without `--execute`, all commands are zero-network/zero-write plans. Execution requires registry
state, env-only credentials, a bounded request plan and a non-secret external authorization ID.
The promote command is unavailable until real 20-session evidence is present; it never enables
failover.

### HTTP

`GET /api/v1/market/provider-status?provider_id={shadow_id}` MUST be read-only and return status
or an allowlisted `unavailable` reason. It MUST NOT initialize a DB, create a root, make provider
calls, mutate a pointer or expose credentials.

### TypeScript interface

```typescript
interface ShadowProviderStatus {
  provider_id: "tickflow" | "tushare";
  admission_state: "discovered" | "canary" | "shadow" | "qualified" | "quarantined";
  adapter_version: string;
  endpoint_contract_version: string;
  reconciliation_policy_version: string;
  consecutive_sessions: number;
  last_failure_class: string | null;
  last_reconciliation_status: "ready" | "material_mismatch" | "unavailable" | null;
  qualification_window_id: string | null;
  unavailable_reason: string | null;
}
```

### Transport and failure contract

Injected client settings MUST bound connect/read/write/pool timeouts, total attempts, retry-after,
response bytes and rows. Allowed classes are `transport_timeout`, `transport_connect`, `dns`,
`auth`, `rate_limit`, `provider_4xx`, `provider_5xx`, `schema`, `semantic`, `calendar`, `universe`,
`coverage`, `reconciliation`, `storage`, `internal`. Raw status bodies, URLs with query secrets,
SQL and local paths are never returned.

## Data Models

All shadow models are immutable, `extra="forbid"`, finite-number validated, safe-path validated,
and separate from R2-F2 canonical models.

```text
ShadowProviderId = tickflow | tushare
ShadowAdmissionState = discovered | canary | shadow | qualified | quarantined

ShadowProviderRecord:
  provider_id, adapter_version, endpoint_contract_version, source_schema_version,
  reconciliation_policy_version, terms_reviewed_at, terms_evidence_hash, intended_use,
  retention_allowed, credential_mode, credential_env_name, quota_contract, required_fields,
  unit_contract, admission_state, window_id, window_start, window_end,
  successful_sessions, quarantined_reason

ShadowEvidenceManifest:
  evidence_id, provider_id, adapter_version, endpoint_contract_version, trade_date, universe_id,
  request_plan_hash, final_attempt_ids, source_object_refs, object_count, row_count,
  request_count, retry_count, rate_limit_count, source_schema_hash, manifest_sha256

ShadowCandidateManifest:
  candidate_id, provider_id, trade_date, universe_id, evidence_id, evidence_sha256,
  normalized_object_ref, normalized_object_sha256, quality_report_ref, quality_report_sha256,
  source_schema_version, adapter_version, row_count, expected_symbol_count, status, manifest_sha256

ShadowReconciliationReport:
  reconciliation_id, policy_version, trade_date, universe_id, left_candidate_id,
  right_candidate_id, status, compared_counts, mismatch_counts, bounded_sanitized_samples,
  report_sha256

ShadowSessionReport:
  session_report_id, provider_id, trade_date, universe_id, canonical_attempt_at,
  shadow_started_at, shadow_completed_at, request_count, retry_count, coverage_ratio,
  quality_verdict, reconciliation_status, failure_class, evidence_sha256, candidate_sha256,
  report_sha256, window_id
```

### Storage and migration

| Artifact | Location | Owner | Allowed operation |
|---|---|---|---|
| Registry | explicit local `provider_registry.sqlite3` | single registry writer | additive schema migration under writer lock; read-only URI readers |
| Shadow evidence/objects/reports | explicit `provider_shadow_root` | shadow writer | bounded no-follow content-addressed atomic compare-create |
| R2-F2 provider evidence | existing `provider_evidence_root` | canonical writer | unchanged; secondary MUST NOT write |
| R2-F2 candidates/selections/canonical data | existing candidate/canonical roots | canonical writer | unchanged; secondary MUST NOT write |
| User DB/NAS | existing production paths | other subsystems | no R2-F3 operation |

`schema_version` is stored in the registry and each shadow manifest. The writer initializes/migrates
explicitly; readers open `file:...?...mode=ro`, validate schema, and return `unavailable` on
missing/corrupt state. Migrations are additive, versioned, and never rewrite immutable shadow
objects or R2-F2 files. `provider_shadow_root` has no symlinks, dirfd/no-follow traversal,
bounded object/manifest sizes, hash verification and atomic create/no-clobber semantics.

### Compatibility matrix

| Surface | BaoStock/R2-F2 | R2-F3 secondary | Migration rule |
|---|---|---|---|
| Provider ID | `ProviderId.BAOSTOCK` | `ShadowProviderId` | Keep distinct; a future static vocabulary addition is additive only |
| Raw batch/evidence | `ProviderRawBatch`/`EvidenceManifest`, exact v1 | shadow equivalents, per-provider contracts | Never pass secondary through canonical R2-F2 validators |
| Candidate | `CandidateManifest` | `ShadowCandidateManifest` | No shared candidate root or hash namespace |
| Selection | `SessionSelection(primary_ready)` | none | No fallback/selection write in R2-F3 |
| Canonical publication | BaoStock Normalize→Quality→Parquet→SHA→Manifest→pointer | none | BaoStock bytes/pointer remain unchanged |
| Status/GET | legacy fields and read-only behavior | additive shadow status, zero-write | Missing shadow state is unavailable, not initialized |

## Out of Scope

- OS-1: Intraday/tick trading, orders, broker credentials, automatic trading or investment advice.
- OS-2: Any real provider request, account creation, token/points acquisition, purchase, NAS access,
  installation, LaunchAgent change, production refresh or canonical pointer mutation in this gate.
- OS-3: HTTP/HTTPS transport inference; Tushare HTTPS remains prohibited until officially documented.
- OS-4: Dynamic provider plugins, symbol-level mixing, same-day fallback, automatic failover or
  `failover_enabled` state.
- OS-5: Replacing or weakening canonical BaoStock normalization, quality gates, immutable Parquet,
  SHA-256, manifest or atomic pointer publication.
- OS-6: Treating amount/OHLCV as fund flow, adjusted close as an unadjusted/factor substitute, or public
  availability as permission for redistribution/commercial use.
- OS-7: AKShare/EastMoney/mootdx qualification, new data suppliers, future calendar/universe systems,
  or R2-A/R2-B re-acceptance.

## Validator, Rollback, and Authority Gates

### Design validator

The strict structural validator MUST run only against this design:

```bash
uv run --offline python /Users/finlay/.codex/skills/claude-skills--engineering/spec-driven-workflow/scripts/spec_validator.py \
  --file docs/plans/2026-08-24-stock-eva-r2f3-shadow-bakeoff-design.md --strict
```

It validates Markdown structure and FR/AC/edge-case presence; it is not code, provider, security,
runtime, terms or GO evidence. Implementation-plan review is manual. `git diff --check` and a
Markdown link/heading check are required.

### Authority gates

1. **Specification gate:** independent review of both docs, exact clean base, no production files.
2. **Offline code gate:** Tasks 10–13 RED then GREEN with synthetic fixtures, full repository tests,
   Ruff/format/diff checks and independent review. Result is `CODE GO / SHADOW WINDOW PENDING`.
3. **External authorization gate:** user explicitly approves provider, account/points/cost/terms,
   legal use, exact date/window, request budget, credential env names, HTTPS transport (if any),
   evidence retention and isolated root. Without it, do not execute.
4. **Canary gate:** one bounded explicit canary into shadow evidence only; re-read terms/schema/
   units/quotas and quarantine on contradiction. No qualification or canonical mutation.
5. **Shadow-window gate:** same adapter/contract/policy and one approved universe/calendar complete
   20 consecutive real trading sessions with all failures reported. Any reset starts a new window.
6. **R2-F3 GO gate:** independent review of the full report and authorization evidence. Only then
   may `qualified` be recorded. This does not enable failover; no R2-F3 GO without real window.

### Rollback

Rollback is default-off and reversible: stop the shadow scheduler, leave BaoStock canonical and the
pointer untouched, mark the provider quarantined or discovered, revoke/remove local runtime
credentials, and retain only non-secret reports/evidence allowed by terms. Do not delete or rewrite
R2-F2 objects, canonical Parquet, manifests, selections or user data. If shadow root/control state
is corrupt, mark it unavailable and isolate it for offline forensic review; never repair it by
mutating the canonical root. Re-enabling requires a new reviewed contract/terms hash and a new
20-session window.

## Traceability

| Requirement | Design evidence | Planned verification |
|---|---|---|
| FR-1–FR-4, NFR-2, NFR-5 | shadow enum/registry/secret boundary | Task10 registry RED/GREEN, serialization and missing-DB tests |
| FR-5–FR-7, FR-9–FR-10 | provider contract ledger, adapter/evidence model | Task11 captured-response tests, no-network/no-secret tests |
| FR-8, FR-11–FR-13 | complete-candidate and policy gates | Task12 unit/suspension/factor/reconciliation tests |
| FR-14–FR-18, NFR-1, NFR-7–NFR-12 | scheduler, 20-session and authority gates | Task13 isolation/reset/report tests and independent review |
| NFR-3–NFR-6, storage table | hash/CAS/dirfd/root/SQLite contract | storage/concurrency/TOCTOU/read-only fingerprints |
| NFR-9 | compatibility matrix and AC-10 | old manifest/hash/selection/GET byte and semantic regression |
| EC-1–EC-16 | numbered edge-case matrix | focused failure fixtures; no external calls |
| API/Data models | CLI/HTTP/TypeScript and immutable model block | schema round-trip, sanitized output and CLI plan tests |
| Out-of-scope/rollback | explicit boundaries above | static diff, authorization checklist, rollback drill |

## Review Decision

This document is a specification only. At this baseline the decision is **APPROVED FOR OFFLINE
TASK 10–13 PLANNING**, **CODE GO / SHADOW WINDOW PENDING**, and **R2-F3 GO = NO-GO** until an
authorized real 20-session window is completed. No provider request, credential, account, NAS,
production or canonical mutation is authorized by this document.
