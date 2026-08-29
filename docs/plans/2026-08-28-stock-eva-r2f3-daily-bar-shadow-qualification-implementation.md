# Stock EVA R2-F3 TickFlow Free Daily Bar Shadow Qualification Implementation Plan

**Status:** `OFFLINE REMEDIATION GATE PASS / INDEPENDENT RE-REVIEW PENDING / DAILY BAR SHADOW NO-GO`

**Design:** [Daily Bar Shadow Qualification Design](2026-08-28-stock-eva-r2f3-daily-bar-shadow-qualification-design.md)

**Starting commit:** `d70fb02140e09ff3758ef224ad27abcf3e1e2e78`

**Implemented code through:** `2c306271b9affe17d94914c9d314ffc32903732c`

**Execution discipline:** one task at a time, witnessed RED before production edits, smallest
GREEN, focused verification, then commit. No real provider request before all offline tasks and an
independent exact-commit review pass.

## Task 7 offline evidence

- R2-F3 focused after review remediation: 111/111 passed.
- R2-F2/Task14/prior-shadow compatibility: 357/357 passed.
- Full repository: 2,134/2,134 passed; LaunchAgent assets: 26/26 passed.
- Full Ruff, format, compileall and diff checks passed; strict design validator: 100/100.
- All eight frozen R2-F2 fixture hashes matched before/after.
- Published canonical manifest remains
  `052799bddd785c8a4204e0bc3352b16edaa636934d819a156f247bb8393873a7`; all 271 referenced
  partitions match their descriptor SHA-256 values.
- No real provider request was made. The exact-commit independent review and real shadow window
  remain pending; see `docs/acceptance/release-2-r2f3-daily-bar-shadow.md`.

### Task 7 review-remediation delta

The first exact-commit review returned H=3/M=2/L=2. Its findings were implemented in
`2c306271b9affe17d94914c9d314ffc32903732c`:

- accept only the exact legal suspended-placeholder `partial` form and bind exclusions plus
  manifest/partition identity into each session's canonical-universe hash;
- separate stable universe/mapping policy from the naturally changing per-session active set, with
  a changing-universe 20-session E2E;
- revalidate current-epoch evidence and candidate bundles on CLI/API status reads;
- wire the production fixed-five HALF_OPEN probe and end its slot after resolution;
- validate every manifest descriptor and enforce an explicit failure-class allowlist.

The same reviewer then returned H=0/M=2/L=0 on `29e9906dc8334f8dd60e77de240f96575dd10ddb`.
The remaining identity findings are now remediated offline: the exact per-session active-symbol-set
hash is carried by the candidate and by the sidecar job/candidate/session/terminal graph, while a
new reviewed symbol-set binding component changes the descriptor and Terms contract version. The
canonical universe-policy hash now embeds the exact legal suspended `partial` issue allowlist.
Targeted RED/GREEN covers the descriptor/schema chain, policy allowlist, terminal mismatch and
candidate binding. A same-reviewer exact-commit re-review remains required.

The actual latest 20 local canonical dates now read 20/20 ready. No Provider request or canonical
write was made during that read-only proof. Same-reviewer exact-commit re-review remains required.

## Delivery result vocabulary

- `CODE NO-GO`: implementation or independent review is incomplete, or any High/Medium finding is
  open.
- `CODE GO / SHADOW WINDOW PENDING`: offline implementation and review pass, but the real
  20-session capability window is incomplete.
- `DAILY BAR SHADOW GO`: exact same-contract real whole-session OHLC evidence passes for 20
  consecutive confirmed dates and its final independent review has no High/Medium finding.
- `R2-F3 GO` is not implied by Daily Bar qualification while factor, suspension, units,
  publication/failover or other complete-provider requirements remain open.

## Task 1 — Lock the Daily models and real-shaped canonical reader

**Requirements:** FR-1, FR-4-FR-8, FR-19, FR-21; NFR-1-NFR-4; AC-1, AC-3; EC-1-EC-5.

**Create:**

- `backend/app/market/daily_shadow_models.py`
- `backend/app/market/daily_shadow_canonical.py`
- `tests/test_market_daily_shadow_canonical.py`
- `tests/fixtures/r2f3_daily_shadow/legacy_manifest.json`

**RED first:**

1. Build a real-shaped schema-v2 sentinel/legacy five-field manifest and exact Parquet fixture with
   stock and index rows. Assert the reader returns only ready active stocks and records indexes plus
   suspended/non-trading stocks in the exclusion hash.
2. Assert the reader accepts exact complete R2-F2 lineage but labels the legacy form
   `LEGACY_UNAVAILABLE`; reject partial lineage without synthesizing values.
3. Parameterize unsafe path, symlink ancestor/object, manifest/partition mutation, digest/count/
   schema/date/source mismatch, duplicate symbol and unknown exchange/security/quality state.
4. Reproduce the real compatibility bug: exchange values `sh`/`sz` and no manifest `universe_id`
   must be valid for this capability reader while the existing factor-aware reader remains unchanged.
5. Assert exact hashes are deterministic across order-independent source rows and that no path is
   created or modified.

Expected RED command:

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_canonical.py
```

The first run must fail because the new modules do not exist. Preserve the failure output in the
delivery notes before adding production code.

**GREEN:**

- Define frozen enums/models for profile, semantic states, lineage state, canonical row and
  `DailyCanonicalSnapshot` with canonical JSON/SHA validation.
- Implement descriptor-bound no-follow/bounded reads, exact manifest descriptor union, exact
  Parquet schema validation and pre/post fingerprints.
- Provide a closeable `PublishedDailyCanonicalProjection.open()/verify()/close()` capability.
- Keep all existing `canonical_comparison.py`, storage readers and canonical models untouched.

**Verify and commit:**

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_canonical.py
.venv/bin/ruff check backend/app/market/daily_shadow_models.py backend/app/market/daily_shadow_canonical.py tests/test_market_daily_shadow_canonical.py
.venv/bin/ruff format --check backend/app/market/daily_shadow_models.py backend/app/market/daily_shadow_canonical.py tests/test_market_daily_shadow_canonical.py
git diff --check
git add backend/app/market/daily_shadow_models.py backend/app/market/daily_shadow_canonical.py tests/test_market_daily_shadow_canonical.py tests/fixtures/r2f3_daily_shadow/legacy_manifest.json
git commit -m "feat(r2f3): add strict daily canonical projection"
```

## Task 2 — Build the credentialless full-session Free request contract

**Requirements:** FR-8-FR-18, FR-30; NFR-1, NFR-3-NFR-5; AC-2-AC-5, AC-12;
EC-4-EC-9.

**Create:**

- `backend/app/market/providers/tickflow_daily_shadow.py`
- `tests/test_market_tickflow_free_daily_shadow_provider.py`

**Modify:**

- `backend/app/market/providers/http.py` to retain only the received-byte count on success and
  failure; no failed payload bytes become evidence or public output.

- `backend/app/config.py` only if the closed Free runtime projection needs the exact sidecar DB
  basename; do not add credential lookup or general environment enumeration.

**RED first:**

1. Assert 3,193 canonical symbols become exactly 32 sorted shards, each <=100, with deterministic
   request/plan hashes, exact UTC day, `period=1d`, `adjust=none` and at most 40 requests.
2. Assert fixed-five Task14 plans cannot validate as full-session plans and no caller can inject a
   symbol, origin, endpoint, period, adjustment mode or shard size.
3. Use injected fake HTTP/SDK factories to prove exact Free origin, no auth/credential read,
   `trust_env=false`, no redirect, one attempt, sequential calls, zero hidden retry/sleep and exact
   close ownership.
4. Parameterize timeout/connect/429/5xx/redirect/host/oversize/malformed JSON and schema failures at
   every shard. Assert the next shard is not called and public errors contain only allowlisted
   endpoint, ordinal/count/bytes/timing and failure class.
5. Parameterize missing/extra/lowercase/duplicate provider symbols, wrong/multiple dates, length
   mismatch, optional-field drift, invalid/non-finite OHLC/activity and illegal OHLC ordering.
6. Assert volume/amount remain opaque source values and are never converted or described as known
   units. Assert factor/suspension state cannot be promoted.

Expected RED command:

```text
.venv/bin/pytest -q tests/test_market_tickflow_free_daily_shadow_provider.py
```

**GREEN:**

- Add a static capability descriptor with separate adapter/endpoint/schema/mapping/unit-state
  hashes and deterministic plan IDs.
- Reuse the reviewed Task14 symbol conversion and fixed SDK Free initializer without changing the
  Task14 adapter.
- Call `BoundedHttpClient` directly and sequentially; never use the SDK concurrent batch helper
  because it tolerates partial chunk failures.
- Return in-memory `ShadowAttempt`/`ShadowCompletion` inputs only when all ordinals succeed; failed
  paths return sanitized audit data and zero persistable rows.

**Verify and commit:**

```text
.venv/bin/pytest -q tests/test_market_tickflow_free_daily_shadow_provider.py tests/test_market_provider_tickflow_free_task14.py tests/test_market_provider_tickflow_task14.py
.venv/bin/ruff check backend/app/market/providers/tickflow_daily_shadow.py tests/test_market_tickflow_free_daily_shadow_provider.py
.venv/bin/ruff format --check backend/app/market/providers/tickflow_daily_shadow.py tests/test_market_tickflow_free_daily_shadow_provider.py
git diff --check
git add backend/app/market/providers/tickflow_daily_shadow.py backend/app/config.py tests/test_market_tickflow_free_daily_shadow_provider.py
git commit -m "feat(r2f3): add bounded free daily shadow adapter"
```

## Task 3 — Publish final-success evidence and Daily-only candidates

**Requirements:** FR-3, FR-15-FR-24; NFR-1-NFR-7; AC-3-AC-8; EC-6, EC-10,
EC-11, EC-15.

**Create:**

- `backend/app/market/daily_shadow_candidates.py`
- `tests/test_market_daily_shadow_candidates.py`

**Reuse without modifying:**

- `backend/app/market/shadow_evidence.py`

**RED first:**

1. Construct complete plans and attempt histories; assert only exactly one final success per
   ordinal can publish through `ShadowEvidenceStore` and every failed partial row is absent.
2. Assert evidence from the Task14 fixed-five plan, wrong profile/window/date/universe/mapping or
   mismatched canonical snapshot cannot build a Daily candidate.
3. Assert exact expected/observed set and one-row coverage, strict Decimal OHLC, semantic literals
   and all descriptor/evidence/completion/source/normalized hashes.
4. Assert candidate bundle publication is no-follow, private, bounded, atomic, idempotent on exact
   bytes and conflict-failing on the same identity with different bytes.
5. Compare exact rows, values at/below `0.01`, and one cell above `0.01`; require all symbol and
   price-cell comparisons to pass. Prove volume/amount/factor fields are not comparison inputs.
6. Mutate canonical manifest/partition before and after reconciliation; require unavailable and no
   candidate/terminal result.

Expected RED command:

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_candidates.py
```

**GREEN:**

- Define `DailyBarShadowCandidate`, `DailyBarQualityReport`,
  `DailyBarReconciliationPolicy/Report` and a content-addressed Daily candidate bundle store/reader.
- Validate the unchanged `ShadowEvidenceManifest` graph against the Daily plan and canonical
  snapshot before normalization.
- Normalize only OHLC and compare with Decimal absolute one-tick tolerance.
- Store source-shaped final-success pages in the existing evidence bundle; store candidate,
  quality and reconciliation JSON only in the Daily namespace.

**Verify and commit:**

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_candidates.py tests/test_market_shadow_evidence.py
.venv/bin/ruff check backend/app/market/daily_shadow_candidates.py tests/test_market_daily_shadow_candidates.py
.venv/bin/ruff format --check backend/app/market/daily_shadow_candidates.py tests/test_market_daily_shadow_candidates.py
git diff --check
git add backend/app/market/daily_shadow_candidates.py tests/test_market_daily_shadow_candidates.py
git commit -m "feat(r2f3): add daily candidate and OHLC reconciliation"
```

## Task 4 — Add the isolated sidecar registry, window and circuit

**Requirements:** FR-2, FR-3, FR-17, FR-18, FR-24-FR-29, FR-31; NFR-1,
NFR-3, NFR-5-NFR-7; AC-9-AC-12; EC-8-EC-13.

**Create:**

- `backend/app/market/daily_shadow_schema.py`
- `backend/app/market/daily_shadow_registry.py`
- `tests/test_market_daily_shadow_registry.py`

**Modify:**

- `backend/app/config.py`
- `backend/app/storage/layout.py`

**RED first:**

1. Freeze exact schema bytes/checksum/version for contract, epoch/window, job, attempt audit,
   evidence/candidate/session refs, terminal attestation and endpoint circuit tables.
2. Assert writer initialization requires exact reviewed contract/TermsEvidence and private
   non-overlapping paths; migration drift, unknown migration, wrong permissions and symlinks fail.
3. Assert reader missing/corrupt/locked/changed DB returns unavailable with zero create/migrate/WAL/
   journal bytes and uses shared-lock bounded copy plus query-only in-memory SQLite.
4. Assert append-only/CAS/hash/FK/trigger closure and crash rollback at every attachment/window
   transaction point. Raw SQLite writes that bypass the registered validator must fail.
5. Parameterize exact 20-date progression, failure/mismatch/unavailable/gap/duplicate/out-of-order/
   calendar/version/universe/terms changes and assert reset epochs retain all prior reports.
6. Assert session 20 changes only the Daily sidecar state and never invokes or mutates the existing
   `ShadowRegistry`, provider admission, canonical selection or failover state.
7. Test CLOSED/OPEN/HALF_OPEN state, frozen threshold/cooldown/probe lease, open-slot skip and
   fixed-five zero-write probe. A successful probe closes the circuit and ends the slot.

Expected RED command:

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_registry.py
```

**GREEN:**

- Implement schema v1 and deterministic bootstrap/reader. Keep it wholly separate from
  `shadow_registry_schema.py` and `providers/registry.py`.
- Implement contract install/read, job lease, sanitized failure terminalization, exact terminal
  graph attach, epoch reset and 20-session Daily-only transition in one transaction.
- Add `daily_bar_shadow_database_name` and derived layout/lock/bundle paths with strict basename,
  absolute-root and overlap validation.
- Implement one endpoint circuit with no full-run after HALF_OPEN success.

**Verify and commit:**

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_registry.py tests/test_market_provider_registry.py tests/test_market_shadow.py tests/test_market_shadow_jobs.py
.venv/bin/ruff check backend/app/market/daily_shadow_schema.py backend/app/market/daily_shadow_registry.py backend/app/config.py backend/app/storage/layout.py tests/test_market_daily_shadow_registry.py
.venv/bin/ruff format --check backend/app/market/daily_shadow_schema.py backend/app/market/daily_shadow_registry.py backend/app/config.py backend/app/storage/layout.py tests/test_market_daily_shadow_registry.py
git diff --check
git add backend/app/market/daily_shadow_schema.py backend/app/market/daily_shadow_registry.py backend/app/config.py backend/app/storage/layout.py tests/test_market_daily_shadow_registry.py
git commit -m "feat(r2f3): add isolated daily shadow qualification state"
```

## Task 5 — Assemble one-session worker with crash-safe ordering

**Requirements:** FR-9-FR-12, FR-15-FR-18, FR-20-FR-24, FR-27-FR-31;
NFR-1-NFR-7; AC-2-AC-12; EC-6-EC-16.

**Create:**

- `backend/app/market/daily_shadow_worker.py`
- `tests/test_market_daily_shadow_worker.py`
- `tests/test_market_daily_shadow_e2e.py`

**RED first:**

1. Offline fake end-to-end: canonical capability -> deterministic plan -> all final-success
   evidence -> candidate -> reconciliation -> terminal DB attach -> count one.
2. Inject every failure/crash boundary before/after network, evidence commit, candidate commit,
   canonical reverify and DB attach; assert no partial session counts and canonical tree unchanged.
3. Assert one invocation leases/attempts only one explicit date, no automatic next date, no retry,
   duplicate completed date causes zero provider calls and concurrent worker loses by lease/CAS.
4. Run a 20-session fake window with one reset scenario and one clean scenario; assert only the
   clean same-vector epoch becomes Daily qualified and provider/failover remain unchanged.
5. Assert OPEN/HALF_OPEN worker paths are skip/probe-only and sanitized.

Expected RED commands:

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_worker.py
.venv/bin/pytest -q tests/test_market_daily_shadow_e2e.py
```

**GREEN:**

- Implement an injected `DailyShadowWorker.run_one(trade_date)` orchestration with no lock during
  network, strict pre/post canonical verification and the required commit order.
- Reuse `ConfirmedCalendarReader` snapshots without changing the existing calendar contract.
- Ensure every exception is normalized into a sidecar outcome and never leaks source data.

**Verify and commit:**

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_worker.py tests/test_market_daily_shadow_e2e.py
.venv/bin/ruff check backend/app/market/daily_shadow_worker.py tests/test_market_daily_shadow_worker.py tests/test_market_daily_shadow_e2e.py
.venv/bin/ruff format --check backend/app/market/daily_shadow_worker.py tests/test_market_daily_shadow_worker.py tests/test_market_daily_shadow_e2e.py
git diff --check
git add backend/app/market/daily_shadow_worker.py tests/test_market_daily_shadow_worker.py tests/test_market_daily_shadow_e2e.py
git commit -m "feat(r2f3): assemble daily shadow session worker"
```

## Task 6 — Add zero-write plan/status CLI and API

**Requirements:** FR-26, FR-29-FR-34; NFR-2, NFR-5, NFR-8, NFR-10; AC-11-AC-13.

**Modify:**

- `backend/app/cli.py`
- `backend/app/api/market.py`
- `backend/app/config.py`
- `backend/app/storage/layout.py`
- `tests/test_market_get_read_only.py`
- `tests/test_market_provider_tickflow_free_task14.py`

**Create:**

- `tests/test_market_daily_shadow_cli_api.py`

**RED first:**

1. CLI plan reads the exact canonical projection, reports expected symbols/shards/hashes with
   `provider_requests=0` and `writes=false`; missing/corrupt state remains zero-write unavailable.
2. Execute rejects missing authorization/acknowledgement, unreviewed/stale descriptor, wrong
   profile/provider/date, disabled execution, unsafe paths and canonical unavailable before client.
3. Fake execute returns one sanitized object and writes only immutable shadow evidence/candidate/
   sidecar state; canonical/evidence-selection/provider registry spies remain untouched.
4. API missing/corrupt/locked sidecar returns unavailable without initialization. Ready response
   exposes capability states and literal false publication/failover flags, never rows/symbols/URLs.
5. Existing Task14 CLI defaults and existing `/market/provider-status` response bytes remain
   unchanged.

Expected RED command:

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_cli_api.py
```

**GREEN:**

- Add `market-provider-daily-shadow` parser and dispatch using the closed credential-free settings
  projection and exact provider/profile literals.
- Add the read-only Daily status endpoint with explicit response model and unavailable reasons.
- Keep the existing `market-provider-shadow`, canary and provider-status commands/routes unchanged.

**Verify and commit:**

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_cli_api.py tests/test_market_get_read_only.py tests/test_market_provider_tickflow_free_task14.py
.venv/bin/ruff check backend/app/cli.py backend/app/api/market.py backend/app/config.py backend/app/storage/layout.py tests/test_market_daily_shadow_cli_api.py
.venv/bin/ruff format --check backend/app/cli.py backend/app/api/market.py backend/app/config.py backend/app/storage/layout.py tests/test_market_daily_shadow_cli_api.py
git diff --check
git add backend/app/cli.py backend/app/api/market.py backend/app/config.py backend/app/storage/layout.py tests/test_market_daily_shadow_cli_api.py tests/test_market_get_read_only.py tests/test_market_provider_tickflow_free_task14.py
git commit -m "feat(r2f3): expose daily shadow plan and status"
```

## Task 7 — Offline code gate, acceptance record and independent review

**Requirements:** all FR/NFR/AC/EC; no real provider request.

**Create:**

- `docs/acceptance/release-2-r2f3-daily-bar-shadow.md`

**Modify:**

- this plan's status/evidence sections only after commands pass.

Run in order:

```text
.venv/bin/pytest -q tests/test_market_daily_shadow_canonical.py tests/test_market_tickflow_free_daily_shadow_provider.py tests/test_market_daily_shadow_candidates.py tests/test_market_daily_shadow_registry.py tests/test_market_daily_shadow_worker.py tests/test_market_daily_shadow_e2e.py tests/test_market_daily_shadow_cli_api.py
.venv/bin/pytest -q tests/test_market_provider_tickflow_free_task14.py tests/test_market_provider_tickflow_task14.py tests/test_market_provider_registry.py tests/test_market_shadow.py tests/test_market_shadow_evidence.py tests/test_market_shadow_jobs.py tests/test_market_shadow_candidates.py tests/test_market_reconciliation.py tests/test_market_shadow_e2e.py tests/test_market_get_read_only.py tests/test_r2f2_golden_compat.py
.venv/bin/pytest -q
.venv/bin/pytest -q tests/test_launchagent_assets.py
.venv/bin/ruff check backend tests
.venv/bin/ruff format --check backend tests
.venv/bin/python -m compileall -q backend
.venv/bin/python /Users/finlay/.codex/skills/claude-skills--engineering/spec-driven-workflow/scripts/spec_validator.py --file docs/plans/2026-08-28-stock-eva-r2f3-daily-bar-shadow-qualification-design.md --strict
git diff --check
git status --short
```

Also hash/read back the frozen R2-F2 golden fixtures and compare canonical manifest/partition
fingerprints before/after the offline gate.

Commit the exact code-gate evidence, then use the single allowed 5.6-luna subagent for an
independent read-only review of that exact clean commit. Any High or Medium issue is fixed with new
RED/GREEN evidence and the same reviewer is asked to re-review. Only H=0/M=0 may produce
`CODE GO / SHADOW WINDOW PENDING`.

## Task 8 — Install reviewed capability contract and run one real session

**Entry gate:** Task 7 exact-commit independent review GO. The user's standing authorization is
already sufficient; do not ask for another operational approval.

1. Create a capability-specific review JSON/TermsEvidence that binds the exact reviewed commit and
   all descriptor hashes. Record final-success-only retention, `raw_retention_contract=UNKNOWN`,
   unknown quota/units/suspension and factor unqualified.
2. Preflight and initialize only the Daily sidecar control DB and isolated shadow directories;
   fingerprint canonical manifest/partition, provider registry and all protected paths.
3. Run CLI plan and verify zero requests/writes plus exact canonical symbol/shard count.
4. Execute one explicit historical confirmed session with one attempt/shard and no follow-up.
5. Read back evidence/candidate/reconciliation/sidecar hashes, request count, semantic states,
   circuit state and protected pre/post fingerprints.
6. If provider transport/schema/coverage/reconciliation fails, retain only the allowed sanitized
   audit, diagnose before any later session and do not change timeouts/retries/tolerance.

The first real session does not automatically start the remaining window.

## Task 9 — Complete and independently review the 20-session window

**Entry gate:** Task 8 real session has a valid terminal Daily success and no unresolved defect.

Process one next confirmed historical session per bounded invocation in exact calendar order. Each
invocation repeats plan identity, canonical pre/post fingerprints and sidecar readback. Stop on
any failure/mismatch/unavailable/circuit-open state, diagnose it and preserve the reset epoch.

After session 20:

1. Export a sanitized immutable qualification report containing all 20 date/report/evidence/
   candidate/reconciliation hashes, exact common version vector, calendar snapshot, reset history,
   request/failure/rate-limit totals and semantic/eligibility states.
2. Prove `daily_bar_qualified=true`, factor/units/suspension unresolved as specified,
   `publication_eligible=false`, `failover_enabled=false`, provider admission unchanged and
   canonical bytes unchanged.
3. Commit only documentation/report references that contain no raw provider rows.
4. Ask the single 5.6-luna reviewer for an independent read-only audit of code, external local
   evidence and all 20 session graphs.
5. Fix any High/Medium issue or declare `DAILY BAR SHADOW GO`. Only then pause for the user's human
   version confirmation.

## Explicit non-changes and rollback

Do not modify the existing Tasks10-13 schema/hashes, Task14 Free discovery plan, authenticated
TickFlow, Tushare, BaoStock Normalize/Quality Gate/Parquet/SHA/manifest/pointer, `SessionSelection`
or failover. Do not install or alter a LaunchAgent in this subversion.

Rollback is capability-local: disable Daily execute/scheduling, preserve immutable audit bundles,
close/revoke any capability authorization record, and leave canonical/provider admission state
untouched. Deleting evidence or the sidecar DB is not part of rollback and requires a separate
explicit destructive action.
