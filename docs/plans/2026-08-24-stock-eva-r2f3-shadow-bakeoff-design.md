# Stock EVA R2-F3 Shadow Provider Bake-off Design

**Author:** Codex delivery team — specification owner
**Date:** 2026-08-24 (Asia/Shanghai)
**Status:** **SPEC READY / IMPLEMENTATION NOT STARTED / R2-F3 CODE NO-GO** — this revision closes independent-review findings; no implementation or real provider window has run
**Decision authority:** User-authorized offline specification only. Real canary, credentials, account/points, cost/terms acceptance and the 20-session window require a separate explicit external authorization.
**Baseline:** clean worktree at `428e890e35b7cedfd45efa810309032e1c7c10e6`; this document is a planning artifact and does not claim implementation, provider qualification or R2-F3 GO.
**Scope:** R2-F3 Tasks 10–13: provider registry, explicit-canary adapters, complete-session normalization/reconciliation, and isolated shadow scheduling/reporting.

**Review remediation:** This revision is documentation-only remediation for independent review
H8/M2. The dedicated documents supersede the umbrella R2-F3 prose where they conflict. `CODE GO /
SHADOW WINDOW PENDING` is reachable only after Tasks 10–13 offline RED/GREEN, full checks and
independent review; the current status is **CODE NO-GO**. `failover_enabled` belongs to R2-F4 and
is not an R2-F3 registry state or authority gate.

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

### Independent review closure matrix (H8/M2 remediation)

| Finding | Closed specification contract | Required regression/transaction/file evidence |
|---|---|---|
| H1 status/authority | Current status is SPEC READY / IMPLEMENTATION NOT STARTED / R2-F3 CODE NO-GO; only clean Tasks 10–13 review may reach CODE GO/PENDING; failover_enabled is R2-F4-only | metadata/status assertions, dedicated-doc supersession, no Task10 execution |
| H2 evidence lifecycle | `shadow_evidence.py`: ShadowAttempt/Completion, Store/Reader, final-success-only, atomic COMMIT bundle, crash/cancel/orphan audit | final-attempt, crash/cancel/orphan, marker atomicity and bounded-reader tests |
| H3 comparison integrity | `shadow_candidates.py` quality/store/reader plus `canonical_comparison.py` descriptor-bound snapshot with exact Dataset/R2-F2/hash/date/universe bindings | every descriptor mutation returns unavailable; zero-write snapshot tests |
| H4 scheduling transaction | ShadowJobStore durable outbox; pointer/manifest commit then lock release then bounded handoff; scanner, independent lease/CAS/worker | ordering, handoff-failure, scanner recovery, crash lease and idempotence tests |
| H5 credentials/terms | exact env map, immutable TermsEvidence URL/bytes hash/version/as-of/reviewer/review ID/approved decisions; discovered until closed | reject before client construction, no-network/no-write tests and secret grep |
| H7 registry safety | frozen migration IDs/DDL/PRAGMA/permissions/lock order/CAS; descriptor-bound bytes + deserialize `:memory:` reader; fixed shadow root limits/atomic protocol | schema/migration/TOCTOU/fingerprint/reader capability tests |
| H8 session evidence | ConfirmedSessionSnapshot, all-outcome ShadowAttemptReport, version vector, same SQLite transaction/CAS reset retaining history | gap/failure/drift reset and all outcome report tests |
| M1 API contract | frozen Python response model, UnavailableReason enum, explicit response_model and named zero-write tests | missing/corrupt DB/root/descriptor GET fingerprint tests |
| M2 compatibility | every Task has named golden R2-F2 manifest/evidence/candidate/selection/GET bytes/hash/reader regression | Task10–13 focused commands and independent review fixture |

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
- FR-8: A shadow plan MUST expand into immutable `ShadowLogicalRequest` records. Each record has
  an ordinal, provider endpoint, role, exact trade date, symbol/index shard, schema/unit contract
  and request hash. A request may have retries, but a `ShadowRequestCompletion` is publishable
  only with one unique final success and contiguous final page identities; a failed attempt has
  zero durable page/object references and zero durable rows. A `ShadowCompletion` binds the exact
  ordinal set, request-plan hash and aggregate counts, and every ordinal MUST finalize before a
  candidate exists. Symbol-level source mixing, partial candidate promotion and cross-session
  stitching MUST be rejected.
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
- FR-19: The shadow evidence writer MUST expose `ShadowEvidenceStore` and `ShadowEvidenceReader`
  with `ShadowLogicalRequest/Plan`, `ShadowAttemptCompletion`, `ShadowRequestCompletion`,
  `ShadowAttempt` and `ShadowCompletion` models. It MUST use successful-attempt-only semantics:
  failed partial rows/bytes are discarded in memory, final bundles commit atomically, crash/cancel
  leaves only a sanitized orphan audit, and no orphan is readable as evidence. The staging bundle
  owns every object, manifest and `COMMIT`; no raw object is visible in a global root before the
  exclusive directory rename.
- FR-20: `ShadowCandidateStore`/`ShadowCandidateReader` MUST persist a `ShadowQualityReport` and
  complete candidate bundle in the shadow root; a failed or incomplete candidate MUST have no
  comparison snapshot or canonical side effect.
- FR-21: `canonical_comparison.py` MUST expose a descriptor-bound, read-only
  `CanonicalCandidateReader` and a closeable `PublishedCanonicalComparison` capability (with
  `CanonicalComparisonSnapshot` as its frozen value). It MUST bind the exact
  current canonical dataset manifest generation/identity, trade-date partition hash/row count,
  R2-F2 publication lineage, `CandidateStore` bundle `selection.json` selection ID/SHA-256,
  candidate/evidence/gate/factor/normalized hashes, trade date and universe. Any change or mismatch
  during comparison MUST return `unavailable`.
- FR-22: Shadow scheduling MUST use a durable `ShadowJobStore` outbox. The canonical pointer and
  manifest MUST commit before `RefreshRunLock` release; only after release may a nonblocking bounded
  handoff occur. A scanner MUST recover published-but-unenqueued sessions from immutable canonical
  manifests. Worker lease/cancel/crash/retry is independent and idempotent.
- FR-23: Each attempt MUST produce a `ShadowAttemptReport` with outcome `evidence_ready`, `success`,
  `failure`, `skip`, `unavailable` or `mismatch`; `evidence_ready` is nonterminal and cannot qualify.
  `ConfirmedSessionSnapshot` MUST freeze exact calendar
  generation/hash and next confirmed sessions. Attempt, session and qualification-window reset
  state MUST update in one SQLite transaction/CAS and retain every prior report.
- FR-24: `run_due_once` MUST determine its canonical outcome inside `RefreshRunLock`, release the
  lock, then make only a nonblocking bounded `shadow_handoff.offer(outcome)` call. Handoff/worker
  failure is a sanitized drop and cannot alter or delay the returned canonical outcome. A separate
  `ShadowScheduler.run_once` and bounded scanner process the durable outbox later.
- FR-25: `ConfirmedCalendarReader` MUST derive an exact confirmed-session snapshot from each
  involved year's canonical `CalendarConfig` dump, official-source metadata and closed dates;
  unknown years are unavailable. Attempt/session bundles publish and release their bundle lock
  before one registry exclusive CAS transaction inserts refs and updates/resets the qualification
  window; all history remains queryable.
- FR-26: A success is eligible for qualification only when one complete transaction proves a
  readable `ShadowEvidenceManifest`, `ShadowEvidenceAttemptRef` for every exact ordinal,
  `ShadowCandidateManifest`, successful attempt/session/job refs, and qualification-window hashes.
  Evidence SHA, candidate SHA, completion/page/count/row/hash and all composite identities MUST
  agree bidirectionally. Missing, unreadable or mismatched hashes are unavailable and cannot be
  repaired by nullable columns.
- FR-27: Failure, skip, unavailable and mismatch reports MUST retain a complete sanitized durable
  report but have empty page identities/refs/counts/rows and null evidence/candidate hashes; no such
  outcome is eligible for evidence, candidate or qualification.
- FR-28: Lifecycle states MUST be explicit: Task 11 may persist `evidence_ready` with evidence and
  no candidate while the job is `pending_normalization`; Task 12 may attach a validated candidate
  without completing the job; only Task 13 may create one immutable terminal attestation and a new
  terminal report version before CAS transitions to `completed`/`qualified`. Direct status bypasses
  MUST be rejected by SQL constraints and the terminal validator.

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
  `provider_registry.sqlite3` local control DB. Its writer uses `journal_mode=DELETE`,
  `synchronous=FULL`, `foreign_keys=ON` and an existing 0600 lock file; its reader takes a shared
  lock, copies bounded bytes through descriptor/fingerprint checks and deserializes to `:memory:`.
  Missing DB/table, journal/lock contention, corrupt schema or failed migration is `unavailable`
  with zero writes; pathname/URI readers are forbidden.
- **NFR-6 (concurrency):** Registry and shadow bundle locks are never held together. Network work
  takes no registry lock; bundle publish/release precedes registry CAS attach. CAS/no-clobber object
  writes and an explicit writer lock MUST prevent duplicate session finalization.
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
- **NFR-13 (transaction ordering):** No shadow work may run inside the canonical post-publication
  lock; canonical outcome is determined first, handoff failure cannot change/delay it, and durable
  outbox recovery is the source of truth.
- **NFR-14 (API compatibility):** Python response models MUST be frozen/extra-forbid, use an
  allowlisted `UnavailableReason` enum and explicit `response_model`; zero-write API tests MUST be
  named for missing DB/root, corrupt state, and every descriptor mismatch.
- **NFR-15 (migration safety):** Registry schema/migration IDs, DDL constraints, PRAGMA/version
  checks, lock path/order, CAS state version, ancestor/basename no-follow checks, fstat/fingerprint
  and permission/TOCTOU rules MUST be frozen before code.

## Acceptance Criteria

The numbered criteria below collectively cover FR-1, FR-2, FR-3, FR-4, FR-5, FR-6, FR-7, FR-8,
FR-9, FR-10, FR-11, FR-12, FR-13, FR-14, FR-15, FR-16, FR-17, FR-18, FR-19, FR-20, FR-21,
FR-22 and FR-23.

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

### AC-12: Shadow evidence crash and cancellation (FR-9, FR-19, NFR-3–NFR-4)

Given a retry with a partial first attempt, a process crash during bundle publication, cancellation,
or a completed bundle whose object commit is interrupted
When `ShadowEvidenceStore` and `ShadowEvidenceReader` recover
Then only final successful attempt pages are readable, failed/partial bytes are discarded, the
bundle has one atomic commit marker, orphan audit is sanitized and bounded, and the reader returns
`unavailable` for incomplete/orphan bundles without writing.

Given a plan with multiple endpoints and an early partial page, followed by retries that succeed
with different page identities
When the request and completion validators run
Then they require the exact ordinal set, one final success for every ordinal, contiguous terminal
pages and a bidirectional completion/page/object binding; duplicate, missing or out-of-order pages
remain unavailable and cannot publish.

Given a failure, skip, unavailable or mismatch outcome
When its attempt report is committed
Then a bounded sanitized durable report ref/hash is retained, while page/row/evidence/candidate
refs are empty/null; only a fully successful completion may create evidence or a candidate.

### AC-13: Candidate and canonical comparison binding (FR-20–FR-21, NFR-9, NFR-14)

Given a complete shadow candidate and a current canonical publication
When `CanonicalComparisonSnapshot` is opened and any dataset manifest generation/identity,
partition hash/row count, R2-F2 lineage, selection ID/SHA, candidate/evidence/gate/factor/normalized
hash, date or universe changes
Then the descriptor-bound read-only reader returns `unavailable`, performs zero writes/provider
calls, and never compares stale or mixed-source data.

### AC-14: Durable post-release handoff (FR-22, FR-24, NFR-7, NFR-13)

Given a canonical refresh that commits its manifest and pointer while holding `RefreshRunLock`
When the lock is released and shadow handoff/enqueue/worker fails, is cancelled, or the process dies
Then the canonical result and timing are already final and unchanged, the durable job remains
pending/reclaimable, a later scanner recovers every published-but-unenqueued manifest, and no
post-publication lock-held work delays canonical availability.

Given `run_due_once` returns either a run or a ready/non-run decision
When the handoff is offered
Then the offer occurs only after the lock scope, with zero wait and an idempotency key; busy or
worker failure is a safe log/drop and the original return value is unchanged.

### AC-15: Confirmed sessions and transactional reset (FR-17, FR-23, FR-25, NFR-5, NFR-8)

Given a confirmed calendar snapshot and a sequence containing success, failure, skip, unavailable,
mismatch, a date gap or a version-vector drift
When the attempt/session/window transaction commits
Then every `ShadowAttemptReport` is retained, the exact session vector is recorded, any gap/failure/
drift resets qualification atomically with the CAS state version, and no prior report is deleted.

Given an involved year absent from the canonical CalendarConfig/official metadata or a crash after
report-bundle rename but before registry CAS
When `ConfirmedCalendarReader` or the recovery scanner runs
Then it returns unavailable for the unknown year, or attaches/deduplicates the committed bundle
without changing the qualification window; a failed DB commit leaves the window unchanged.

### AC-17: Complete success graph and all-outcome exclusion (FR-26–FR-28, NFR-3–NFR-6)

Given a successful job with multiple logical requests and a final candidate
When the qualification transaction validates its evidence, attempt refs, candidate, session, job
and window
Then every exact ordinal has one final success with contiguous pages and matching counts/rows/hashes,
all composite identities and readable objects agree bidirectionally, and non-null evidence/candidate/
completion hashes are attached at every success boundary. Missing, unreadable or mismatched data
rolls back the complete transaction and returns unavailable.

Given each of `failure`, `skip`, `unavailable` and `mismatch`
When the durable attempt/session report is committed
Then its canonical sanitized JSON contains timing/coverage/retry/rate-limit/failure fields and a
report hash, but page identities/refs/counts/rows and evidence/candidate hashes are empty/null;
SQL/model validators reject any attempt to attach evidence, candidate or qualification.

Given Task 11 evidence-ready state, Task 12 candidate attachment, or a direct completed/qualified
write
When the lifecycle validator runs
Then evidence-ready remains nonterminal, candidate attachment remains pending normalization, and
only Task 13's immutable terminal attestation plus two expected-version CAS updates can complete or
qualify; direct bypasses are rejected and leave versions/history unchanged.

### AC-18: Failure-class report exclusion (FR-27)

Given a failure, skip, unavailable or mismatch report
When SQL and the terminal validator inspect it
Then the sanitized durable report remains queryable, but page identities/refs/counts/rows and
evidence/candidate hashes are empty/null and no qualification attestation can reference it.

### AC-16: Frozen API response and zero-write boundary (NFR-14, NFR-15)

Given missing/corrupt registry, shadow root, job store, canonical descriptor, or candidate bundle
When `GET /api/v1/market/provider-status` or a comparison/status plan runs
Then the frozen response model returns an allowlisted `UnavailableReason`, uses the declared
`response_model`, and named fingerprint tests prove zero file/DB/schema/mtime/network writes.

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
- EC-17: `ShadowEvidenceStore` sees a crash/cancel after object create but before bundle commit;
  reader rejects the orphan and only bounded orphan audit remains.
- EC-18: A failed retry has rows in memory but no final success; no shadow manifest, candidate or
  quality report is written.
- EC-19: Candidate comparison sees an R2-F2 selection bundle with changed `selection.json` bytes,
  selection ID or SHA; it returns `unavailable` and does not read another bundle.
- EC-20: Canonical pointer commit succeeds but enqueue handoff fails after lock release; scanner
  recovers the immutable manifest and enqueues idempotently.
- EC-21: Worker lease expires after process crash; a later worker reclaims it under CAS and a
  duplicate completion cannot create a second report.
- EC-22: Calendar generation changes or a confirmed-session gap appears; window resets in one
  transaction while prior reports remain queryable.
- EC-23: Tushare lacks official HTTPS proof or approved TermsEvidence at client construction;
  `execute` rejects before constructing the HTTP client and performs zero network/write operations.
- EC-24: A success attempt/session/job/window has a missing, unreadable or mismatched evidence or
  candidate object/hash; the terminal transaction rolls back and qualification remains unchanged.
- EC-25: Any failure, skip, unavailable or mismatch report carries page IDs, page refs, rows/counts
  or evidence/candidate hashes; the durable report is retained but SQL/model validation rejects it.
- EC-26: A crash before bundle rename, after bundle rename before DB, after an attach insert, or
  before either CAS leaves no partial qualification; recovery attaches only a committed deterministic
  bundle or reconciles the already committed row idempotently.

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

Credential mapping is a closed static map and is not configurable:

```text
tickflow -> STOCK_EVA_TICKFLOW_TOKEN
tushare  -> STOCK_EVA_TUSHARE_TOKEN
```

The token factory reads only the mapped environment variable after admission/TermsEvidence checks;
the token value is never put into `ShadowProviderRecord`, `TermsEvidence`, request models, URLs,
exceptions, logs or output. Any arbitrary env name, CLI token, missing approved TermsEvidence,
unapproved intended use/retention/quota, or Tushare HTTPS proof failure rejects before HTTP-client
construction and performs zero network/write operations. Initial TickFlow and Tushare states are
`discovered`.

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

### Frozen Python API response contract

```python
class UnavailableReason(StrEnum):
    REGISTRY_MISSING = "registry_missing"
    REGISTRY_SCHEMA_INVALID = "registry_schema_invalid"
    SHADOW_ROOT_UNAVAILABLE = "shadow_root_unavailable"
    JOB_STORE_UNAVAILABLE = "job_store_unavailable"
    CANONICAL_DESCRIPTOR_CHANGED = "canonical_descriptor_changed"
    CANDIDATE_DESCRIPTOR_CHANGED = "candidate_descriptor_changed"
    SELECTION_BINDING_INVALID = "selection_binding_invalid"
    EVIDENCE_INCOMPLETE = "evidence_incomplete"
    VERSION_VECTOR_DRIFT = "version_vector_drift"
    CALENDAR_SNAPSHOT_INVALID = "calendar_snapshot_invalid"

class ShadowProviderStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Literal["ready", "unavailable"]
    provider: ShadowProviderId | None
    state: ShadowAdmissionState | None
    unavailable_reason: UnavailableReason | None
    report_id: SafeIdentifier | None
```

The API route MUST declare `response_model=ShadowProviderStatusResponse`; all unavailable paths
must map to this model and never serialize an arbitrary exception. Named zero-write tests cover
each enum reason and missing/corrupt state.

## Data Models

All shadow models are immutable, `extra="forbid"`, finite-number validated, safe-path validated,
and separate from R2-F2 canonical models.

```text
ShadowProviderId = tickflow | tushare
ShadowAdmissionState = discovered | canary | shadow | qualified | quarantined

ShadowProviderRecord:
  provider_id, admission_state, adapter_hash, endpoint_contract_hash, source_schema_hash,
  normalizer_hash, reconciliation_policy_hash, terms_evidence_hash(nullable only when discovered),
  terms_review_id(nullable only when discovered), credential_env_name, intended_use,
  retention_decision, quota_contract, required_fields_json, unit_contract_json, state_version,
  quarantine_reason

TermsEvidence:
  terms_evidence_id, provider_id, official_url_allowlist_json, content_bytes_sha256,
  content_object_relpath,
  contract_version, as_of_date, reviewer, review_id, approved_intended_use,
  approved_retention, approved_credential_mode, approved_quota_decision, manifest_sha256

ConfirmedSessionSnapshot:
  snapshot_id, calendar_generation, calendar_sha256, confirmed_next_sessions,
  universe_id, universe_sha256, captured_at, snapshot_sha256

ShadowAttempt:
  attempt_id, job_id, provider_id, trade_date, universe_id, attempt_number,
  started_at, completed_at, outcome, request_count, retry_count, failure_class,
  final_page_refs, sanitized_orphan_audit_id

ShadowLogicalRequest:
  ordinal, request_id, provider_id, endpoint, role, trade_date, symbol_or_index_shard,
  schema_contract_hash, unit_contract_hash, request_hash

ShadowLogicalRequestPlan:
  plan_id, job_id, request_plan_hash, requests[ShadowLogicalRequest], exact_ordinal_set

ShadowAttemptCompletion:
  ordinal, attempt_id, request_id, endpoint, session_id, outcome, page_identities, page_count,
  row_count, terminal_marker, evidence_refs, evidence_sha256
  # failure|skip|unavailable|mismatch => evidence_refs/evidence_sha256/page rows are zero/null;
  # durable sanitized report ref/hash is still required

ShadowRequestCompletion:
  ordinal, request_id, attempt_ids, final_attempt_id, final_success, contiguous_page_refs,
  completion_sha256

ShadowCompletion:
  job_id, request_plan_hash, exact_ordinal_set, request_completions, aggregate_page_count,
  aggregate_row_count, evidence_id, completion_sha256, committed_at

ShadowEvidenceAttemptRef:
  evidence_id, provider_id, job_id, window_id, session_id, ordinal, attempt_id, endpoint,
  request_id, page_refs, page_count, row_count
  # one evidence has one final ref per ordinal; the composite identity is checked in both
  # directions against ShadowAttemptReport and ShadowEvidenceManifest

ShadowAttemptReport:
  report_id, attempt_id, job_id, provider_id, window_id, session_id, logical_request_ordinal,
  request_id, endpoint_class, outcome(evidence_ready|success|failure|skip|unavailable|mismatch), started_at,
  completed_at, coverage_expected, coverage_observed, request_count, retry_count,
  rate_limit_count, failure_class, page_identities, page_count, row_count, evidence_refs,
  evidence_id(nullable for failure classes), evidence_sha256(nullable unless success), candidate_sha256(nullable unless success),
  terminal_session_report_id(nullable unless terminal success), durable_report_ref,
  report_sha256 (required for every durable outcome; sanitized payload only)

QualificationWindow:
  provider_id, window_id, window_state(observing|qualified|reset), consecutive_sessions,
  version_vector_sha256, calendar_generation, calendar_sha256, last_session_report_id,
  qualification_evidence_sha256(nullable unless qualified),
  qualification_candidate_sha256(nullable unless qualified), terminal_attestation_id(nullable unless qualified),
  state_version

VersionVector:
  adapter_hash, endpoint_contract_hash, source_schema_hash, normalizer_hash,
  reconciliation_policy_hash, terms_evidence_hash, universe_hash, calendar_hash,
  config_hash

CanonicalComparisonSnapshot:
  snapshot_id, dataset_manifest_generation, dataset_manifest_sha256,
  trade_date_partition_sha256, trade_date_row_count, r2f2_publication_lineage,
  selection_id, selection_sha256, candidate_sha256, evidence_sha256, gate_sha256,
  factor_sha256, normalized_sha256, trade_date, universe_id, version_vector,
  snapshot_sha256

PublishedCanonicalComparison:
  capability_id, frozen CanonicalComparisonSnapshot, opened_roots, descriptor_fingerprints,
  closed, verify_before_reconcile, verify_after_reconcile, close()

ShadowJob:
  job_id, provider_id, window_id, version_vector_sha256, canonical_manifest_generation,
  canonical_manifest_sha256, trade_date, universe_id,
  state(pending|leased|pending_normalization|completed|failed|cancelled|unavailable), lease_owner,
  lease_expires_at, attempts, successful_evidence_sha256(nullable unless completed),
  successful_candidate_sha256(nullable unless completed), completion_sha256(nullable unless completed),
  terminal_attestation_id(nullable unless completed), state_version, job_sha256

ShadowEvidenceManifest:
  evidence_id, provider_id, adapter_version, endpoint_contract_version, trade_date, universe_id,
  request_plan_hash, completion_sha256, final_attempt_ids, source_object_refs, page_identities,
  object_count, row_count,
  request_count, retry_count, rate_limit_count, source_schema_hash, schema_version,
  bundle_relative_path, bundle_commit_sha256, orphan_audit_id, manifest_sha256
  # each completion/page/object reference is checked both directions; a page/object absent from
  # completion or a completion ref absent from this manifest is unavailable

ShadowQualityReport:
  quality_report_id, candidate_id, trade_date, universe_id, gate_version, ordered_gate_outcomes,
  expected_symbol_count, loaded_symbol_count, suspended_count, failed_symbols, verdict,
  input_evidence_sha256, report_sha256

ShadowCandidateManifest:
  candidate_id, provider_id, trade_date, universe_id, evidence_id, evidence_sha256,
  normalized_object_ref, normalized_object_sha256, quality_report_ref, quality_report_sha256,
  source_schema_version, adapter_version, row_count, expected_symbol_count, bundle_commit_sha256,
  status, manifest_sha256

ShadowReconciliationReport:
  reconciliation_id, policy_version, trade_date, universe_id, left_candidate_id,
  right_candidate_id, status, compared_counts, mismatch_counts, bounded_sanitized_samples,
  report_sha256

ShadowSessionReport:
  session_report_id, provider_id, trade_date, universe_id, canonical_attempt_at,
  shadow_started_at, shadow_completed_at, request_count, retry_count, coverage_ratio,
  quality_verdict, reconciliation_status, failure_class, successful_attempt_id,
  evidence_id, candidate_id, terminal_attestation_id, report_version,
  evidence_sha256, candidate_sha256, report_sha256, window_id
  # evidence_ready requires evidence only and no candidate/attestation; terminal success requires
  # both IDs/hashes and attestation; failure/skip/unavailable/mismatch has none

ShadowTerminalAttestation:
  attestation_id, provider_id, job_id, window_id, session_id, evidence_id, candidate_id,
  session_report_id, session_report_version, attempt_ordinal_closure_sha256, request_plan_sha256,
  completion_sha256, report_digest_sha256, evidence_sha256, candidate_sha256,
  terminal_outcome(success), immutable_version
  # immutable relation graph; exactly one per provider/job/window/session
```

Terminal success eligibility is a graph, not a job/window hash projection. Every terminal ordinal
report is composite-bound to the immutable session-report version through
`terminal_session_report_id`; the attestation is the sole relation that may make a job completed
or a window qualified. Denormalized job/window hashes are audit projections checked against that
attestation and are never sufficient for a reader or selector.

### Storage and migration

| Artifact | Location | Owner | Allowed operation |
|---|---|---|---|
| Registry | configured absolute local-control `provider_registry.sqlite3` plus sibling lock | single registry writer | additive migration under exclusive lock; descriptor-copy `:memory:` readers |
| Shadow evidence/objects/reports | explicit `provider_shadow_root` | shadow writer | bounded no-follow content-addressed atomic compare-create |
| R2-F2 provider evidence | existing `provider_evidence_root` | canonical writer | unchanged; secondary MUST NOT write |
| R2-F2 candidates/selections/canonical data | existing candidate/canonical roots | canonical writer | unchanged; secondary MUST NOT write |
| User DB/NAS | existing production paths | other subsystems | no R2-F3 operation |

`schema_version` is stored in the registry and each shadow manifest. The frozen registry migration
ledger is executable, not pseudocode: the canonical fenced SQL is in the implementation Task 10
section and is compiled against SQLite before implementation starts.

| Migration ID | DDL / invariant | Rollback |
|---|---|---|
| `r2f3-registry-0001` | Creates `schema_migration`, `terms_evidence`, `provider_record`, `qualification_window`, `shadow_job`, `shadow_attempt_report`, `session_report`, `shadow_evidence_ref`, `shadow_evidence_attempt_ref`, `shadow_candidate_ref` and immutable `shadow_terminal_attestation`, with exact composite FK/UNIQUE/CHECK definitions in Task 10 SQL. | Stop readers, retain immutable DB backup and shadow objects, restore previous registry DB copy; never delete/rewrite canonical data. |
| `r2f3-registry-0002` | Add-only review-object compatibility migration; existing providers remain `discovered` until `terms_evidence_hash` and review fields are non-null and validator-approved. | Reopen `0001` read-only; no destructive down-migration. |

Writer initialization MUST use PRAGMA `foreign_keys=ON`, `journal_mode=DELETE`, `synchronous=FULL`,
`busy_timeout` bounded, and a checked `schema_version`/migration ID. It MUST create the DB only
inside a safe absolute ancestor whose ancestors are regular directories, and the DB must be a
regular 0600 file and pre-created `provider_registry.sqlite3.lock` must also be 0600. The writer
opens that existing lock with `O_NOFOLLOW` and takes an exclusive flock for every transaction. It
MUST NOT acquire `RefreshRunLock` or run under a canonical publication lock. Every transition uses
`BEGIN IMMEDIATE`, expected `state_version`, one CAS UPDATE, and commit; conflict rolls back and
returns a sanitized retry/unavailable result. SQLite's DELETE journal recovers a crashed writer on
the next exclusive writer transaction; migration failure rolls back and leaves prior schema/state
readable.

The reader opens the already-existing lock with `O_NOFOLLOW` (never creates it), takes a shared
flock, then opens the DB basename through its parent descriptor with `O_NOFOLLOW`, requires a
regular 0600 file, copies bounded bytes, records before/after `fstat` plus device/inode/size/mtime
fingerprint, and calls `sqlite3.Connection.deserialize()` into `:memory:` for read-only schema and
status queries. If a journal is present, the lock cannot be acquired, deserialize is unavailable,
or any identity changes, the capability is `unavailable` with zero persistent writes. There is no
fallback to a pathname or URI SQLite reader.

`provider_shadow_root` is a configured absolute path, validated for safe regular ancestors and
bounded limits. Its fixed layout is `staging/<attempt-owned-nonce>/` and `bundles/<evidence_id>/`;
staging contains every payload object, manifest and `COMMIT`. Every file and the directory are
fsynced, then the writer exclusively atomically renames the complete directory into
`bundles/<evidence_id>`. Readers inspect bundles only and require `COMMIT`; no global raw-object
root exists. Pre-rename crash/cancel leaves staging residue; owner-marker+dirfd/inode recovery
removes payload and emits only sanitized audit JSON. Post-rename/pre-DB is a legal committed final
success; deterministic scanning may attach it by job/evidence identity or quarantine/delete under
approved retention, but it can never become a candidate without registry attach.

`ShadowEvidenceStore`/`ShadowEvidenceReader` are descriptor-bound. A `ShadowAttempt` may retain
only safe IDs, counts, outcome and failure class while rows live in memory. `ShadowCompletion`
names the exact request-plan ordinal set and its per-ordinal final successful attempts. Reader
validates object hashes, schema, row counts, page ordering, completion marker, manifest hash,
bidirectional page/object refs and final-attempt IDs before returning evidence.

SQL state checks are part of this contract: a success attempt, completed job, successful session or
qualified window cannot satisfy its CHECKs with nullable evidence/candidate/completion fields. The
application validator then opens each referenced bundle read-only, recomputes hashes and verifies
the same hash at attempt, session, job and window levels; missing/unreadable/mismatch is unavailable
and the transaction is rolled back. Failure-class rows are the only rows allowed to retain null
evidence/candidate fields.

`ShadowCandidateStore`/`ShadowCandidateReader` publish/read `ShadowQualityReport`, normalized object,
candidate manifest and one atomic candidate bundle in the same shadow-root safety protocol. A
`CanonicalCandidateReader` in `canonical_comparison.py` opens one dataset root plus candidate and
evidence roots, and allows only the four read-only files `candidate.json`, `gate.json`,
`selection.json` and `normalized.json`. It records root, bundle and every descriptor's identity,
fstat and hash before and after reading, uses `EvidenceReader`, re-runs existing public model
validators, and bidirectionally verifies the dataset manifest/generation, exact BaoStock partition
SHA/row count, nine R2-F2 lineage fields (`provider_id`, `universe_id`, `evidence_id`,
`evidence_sha256`, `candidate_id`, `candidate_manifest_sha256`, `gate_report_sha256`,
`adapter_version`, `source_schema_version`), every candidate/evidence/gate/factor/normalized hash,
date and universe. `PublishedCanonicalComparison.verify()` repeats the manifest identity check
before and after reconciliation; `close()` invalidates the capability. Missing lineage, any
TOCTOU/mixed-generation change or mismatch is `unavailable`; it never guesses or changes
canonical fields.

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
   Ruff/format/diff checks and independent review. Only a clean gate may change the status to
   `CODE GO / SHADOW WINDOW PENDING`; the current specification status is `R2-F3 CODE NO-GO`.
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
| FR-1–FR-4, FR-5–FR-7, H5, H7, NFR-2, NFR-5, NFR-15 | shadow enum/registry/TermsEvidence/secret boundary and frozen SQLite protocol | Task10 registry RED/GREEN, exact env mapping, terms rejection-before-client, migration/DDL/PRAGMA/CAS/reader safety tests |
| FR-9, FR-19, H2, NFR-3–NFR-4 | `shadow_evidence.py` successful-attempt-only store/reader and atomic bundle | Task11 final-attempt, crash/cancel/orphan, bounded bytes/hash/dirfd and golden evidence tests |
| FR-8, FR-11–FR-13, FR-20–FR-21, H3 | `shadow_candidates.py`, `canonical_comparison.py`, quality and descriptor binding | Task12 complete-candidate, golden candidate/selection, snapshot drift/unavailable and no-write tests |
| FR-14–FR-18, FR-22–FR-23, H4/H8, NFR-1, NFR-7–NFR-14 | durable job outbox, released-lock handoff, scanner, leases, reports, reset transaction | Task13 ordering/crash/lease/CAS/session-vector and golden GET/selection tests |
| FR-24–FR-27, H2/H8 | complete success graph, all-outcome sanitized reports, exact logical-request closure and qualification transaction | Task11/13 missing/unreadable/hash-mismatch, four outcome exclusion, multi-request/page, full transaction/crash/recovery probes |
| FR-28, H/M lifecycle | evidence_ready → candidate → terminal attestation → completed/qualified state machine | Task11 evidence-ready, Task12 candidate-pending, Task13 attestation, direct-bypass and stale/crash rollback probes |
| NFR-3–NFR-6, H7, storage table | hash/CAS/dirfd/root/SQLite contract | storage/concurrency/TOCTOU/read-only fingerprints |
| M2 | golden R2-F2 manifest/evidence/candidate/selection/GET byte/hash/reader fixtures | named regression in every Task 10–13 focused command and independent review |
| NFR-9 | compatibility matrix and AC-10 | old manifest/hash/selection/GET byte and semantic regression |
| EC-1–EC-23 | numbered edge-case matrix | focused failure fixtures; crash/orphan/descriptor drift/outbox/lease/reset/terms gates; no external calls |
| API/Data models | CLI/HTTP/TypeScript and immutable model block | schema round-trip, sanitized output and CLI plan tests |
| Out-of-scope/rollback | explicit boundaries above | static diff, authorization checklist, rollback drill |

## Review Decision

This document is a specification only. At this baseline the decision is **SPEC READY / IMPLEMENTATION
NOT STARTED / R2-F3 CODE NO-GO**. It may be used to review a future offline implementation, but
only a clean Tasks 10–13 RED/GREEN/full-check/independent-review gate may produce **CODE GO /
SHADOW WINDOW PENDING**; a real authorized 20-session window is additionally required for R2-F3
GO. No provider request, credential, account, NAS, production or canonical mutation is authorized
by this document.
