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

### One fixed golden R2-F2 compatibility fixture (M2)

Task 10 creates exactly one immutable fixture tree at
`tests/fixtures/r2f2_golden/**` (sha256 manifest plus the required manifest/evidence/candidate/
selection JSON, Parquet partition and GET bytes) and exactly one test module
`tests/test_r2f2_golden_compat.py`. The fixture records fixed expected paths, bytes, SHA-256,
manifest generation, selection ID/SHA, reader result and GET fingerprint. It is checked in and
never generated by the serializer under test. Task 10 creates it; Tasks 11, 12 and 13 invoke the
same named test `test_r2f2_golden_compatibility_is_byte_hash_reader_and_get_stable`. Any failure
blocks that task and cannot be repaired by rewriting the fixture, widening a canonical model or
adding a second golden test. This is regression evidence only and does not call a provider.

## Task 10 — Registry, admission and secret-safe configuration

### Scope and file whitelist

Create or modify only these files in the Task 10 commit:

- Create backend/app/market/providers/shadow_contracts.py for ShadowProviderId, admission
  states, static provider-contract descriptors and safe fields.
- Create backend/app/market/shadow_registry_schema.py containing the frozen migration IDs,
  executable DDL/PRAGMA constants, state-version CAS and lock-order contract.
- Create backend/app/market/providers/registry.py for the writer-owned registry repository,
  transitions, qualification-window reset and read-only status.
- Modify backend/app/config.py to add explicit local provider_registry.sqlite3,
  provider_shadow_root, bounded request/time/object limits, default-off flags and credential
  environment variable names only.
- Modify .env.example with names such as STOCK_EVA_TICKFLOW_TOKEN and
  STOCK_EVA_TUSHARE_TOKEN; values MUST be blank and MUST NOT resemble real tokens.
- Modify backend/app/storage/layout.py only for configured absolute `provider_shadow_root` layout
  helpers (`staging/<attempt-owned-nonce>/`, `bundles/<evidence_id>/`) with no-follow descriptors;
  do not change canonical layout.
- Modify backend/app/cli.py for read-only market-provider-status and a zero-network plan parser.
- Modify backend/app/api/market.py additively for frozen Python response models,
  `UnavailableReason`, and explicit `response_model` (no write-capable status route).
- Create tests/test_market_provider_registry.py and any narrow storage/config test named in the
  RED command.
- Create tests/test_r2f2_golden_compat.py with the mandatory R2-F2 manifest,
  evidence, candidate, selection, GET bytes/hash/reader fixture.
- Create the fixed `tests/fixtures/r2f2_golden/**` tree with its checked-in `sha256sums.txt` and
  required JSON/Parquet/GET objects; no other golden fixture or golden test module is allowed.
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
def test_registry_ddl_compiles_pragmas_foreign_keys_checks_and_unique_constraints(tmp_path): ...
def test_registry_reader_existing_lock_shared_flock_journal_and_zero_write_fail_closed(tmp_path): ...
def test_registry_state_version_cas_and_lock_order_never_hold_bundle_and_registry_locks(): ...
def test_registry_cas_uses_begin_immediate_update_where_state_version_and_rowcount_one(): ...
def test_registry_stale_writer_rolls_back_without_state_or_history_loss(tmp_path): ...
def test_registry_composite_provider_window_job_session_cross_bind_is_rejected(tmp_path): ...
def test_terms_evidence_provider_hash_review_fk_and_model_sql_roundtrip(tmp_path): ...
def test_terms_evidence_canonical_bytes_manifest_hash_and_content_hash_are_frozen(tmp_path): ...
def test_terms_evidence_descriptor_mutation_symlink_size_or_hash_mismatch_is_unavailable(tmp_path): ...
def test_r2f2_golden_compatibility_is_byte_hash_reader_and_get_stable(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_provider_registry.py \
  tests/test_r2f2_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-registry-red
~~~

Expected RED is absence of shadow types/repository and missing read-only isolation. Record the
actual failure, not an anticipated count.

### GREEN

Implement a small static registry, not a plugin loader. Freeze r2f3-registry-0001/0002, all DDL
CHECK/FOREIGN KEY/UNIQUE constraints, PRAGMA foreign_keys=ON, `journal_mode=DELETE`,
`synchronous=FULL`, bounded busy_timeout, migration rollback and permission requirements before
the first migration. The
writer uses an exclusive provider_registry.sqlite3.lock only for the SQLite transaction; shadow
bundle publication is completed and its lock released before the registry CAS attach. Registry and
bundle locks are never held together; each transition is BEGIN IMMEDIATE plus expected state_version
CAS. The read-only path opens the
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
immutable and stores only bounded exact reviewed official content object bytes plus object/hash,
official URL allowlist, content-bytes SHA-256, contract version/as-of, reviewer/review ID, approved
intended use/retention/credential/quota decisions; provider_record stores only the manifest hash.
Unclosed terms keep the provider discovered.

### Frozen executable registry DDL and lock protocol

This is the complete `r2f3-registry-0001` SQL source. Task 10 MUST compile this exact block in a
temporary SQLite database, assert the PRAGMAs, run `PRAGMA foreign_key_check`, exercise every
CHECK/UNIQUE/FK and an expected `state_version` CAS, then close it. It is not a one-line schema
summary and the implementation MUST NOT silently substitute a different serializer/schema.

```sql
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = FULL;

CREATE TABLE schema_migration (
  migration_id TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL UNIQUE CHECK (schema_version > 0),
  applied_at TEXT NOT NULL
);

CREATE TABLE terms_evidence (
  terms_evidence_id TEXT PRIMARY KEY,
  provider_id TEXT NOT NULL CHECK (provider_id IN ('tickflow', 'tushare')),
  official_url_allowlist_json TEXT NOT NULL,
  content_object_relpath TEXT NOT NULL,
  content_bytes_sha256 TEXT NOT NULL CHECK (length(content_bytes_sha256) = 64),
  contract_version TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  reviewer TEXT NOT NULL,
  review_id TEXT NOT NULL,
  approved_intended_use TEXT NOT NULL,
  approved_retention TEXT NOT NULL,
  approved_credential_mode TEXT NOT NULL,
  approved_quota_decision TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
  UNIQUE (provider_id, manifest_sha256, review_id),
  UNIQUE (provider_id, manifest_sha256)
);

CREATE TABLE provider_record (
  provider_id TEXT PRIMARY KEY CHECK (provider_id IN ('tickflow', 'tushare')),
  admission_state TEXT NOT NULL CHECK
    (admission_state IN ('discovered', 'canary', 'shadow', 'qualified', 'quarantined')),
  adapter_hash TEXT NOT NULL CHECK (length(adapter_hash) = 64),
  endpoint_contract_hash TEXT NOT NULL CHECK (length(endpoint_contract_hash) = 64),
  source_schema_hash TEXT NOT NULL CHECK (length(source_schema_hash) = 64),
  normalizer_hash TEXT NOT NULL CHECK (length(normalizer_hash) = 64),
  reconciliation_policy_hash TEXT NOT NULL CHECK (length(reconciliation_policy_hash) = 64),
  terms_evidence_hash TEXT,
  terms_review_id TEXT,
  credential_env_name TEXT NOT NULL CHECK
    ((provider_id = 'tickflow' AND credential_env_name = 'STOCK_EVA_TICKFLOW_TOKEN') OR
     (provider_id = 'tushare' AND credential_env_name = 'STOCK_EVA_TUSHARE_TOKEN')),
  intended_use TEXT NOT NULL,
  retention_decision TEXT NOT NULL,
  quota_contract TEXT NOT NULL,
  required_fields_json TEXT NOT NULL,
  unit_contract_json TEXT NOT NULL,
  state_version INTEGER NOT NULL CHECK (state_version >= 0),
  quarantine_reason TEXT,
  CHECK (admission_state = 'discovered' OR
         (terms_evidence_hash IS NOT NULL AND terms_review_id IS NOT NULL)),
  FOREIGN KEY (provider_id, terms_evidence_hash, terms_review_id)
    REFERENCES terms_evidence(provider_id, manifest_sha256, review_id)
);

CREATE TABLE qualification_window (
  provider_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  window_start TEXT,
  window_end TEXT,
  consecutive_sessions INTEGER NOT NULL CHECK (consecutive_sessions >= 0),
  version_vector_sha256 TEXT NOT NULL CHECK (length(version_vector_sha256) = 64),
  calendar_generation TEXT NOT NULL,
  calendar_sha256 TEXT NOT NULL CHECK (length(calendar_sha256) = 64),
  window_state TEXT NOT NULL CHECK (window_state IN ('observing', 'qualified', 'reset')),
  last_session_report_id TEXT,
  qualification_evidence_sha256 TEXT CHECK
    (qualification_evidence_sha256 IS NULL OR length(qualification_evidence_sha256) = 64),
  qualification_candidate_sha256 TEXT CHECK
    (qualification_candidate_sha256 IS NULL OR length(qualification_candidate_sha256) = 64),
  state_version INTEGER NOT NULL CHECK (state_version >= 0),
  CHECK (window_state <> 'qualified' OR
    (last_session_report_id IS NOT NULL AND qualification_evidence_sha256 IS NOT NULL AND
     qualification_candidate_sha256 IS NOT NULL)),
  PRIMARY KEY (provider_id, window_id),
  UNIQUE (provider_id),
  FOREIGN KEY (provider_id) REFERENCES provider_record(provider_id)
);

CREATE TABLE shadow_job (
  job_id TEXT PRIMARY KEY,
  provider_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  trade_date TEXT NOT NULL,
  universe_id TEXT NOT NULL,
  canonical_manifest_generation TEXT NOT NULL,
  canonical_manifest_sha256 TEXT NOT NULL CHECK (length(canonical_manifest_sha256) = 64),
  version_vector_sha256 TEXT NOT NULL CHECK (length(version_vector_sha256) = 64),
  successful_evidence_sha256 TEXT CHECK
    (successful_evidence_sha256 IS NULL OR length(successful_evidence_sha256) = 64),
  successful_candidate_sha256 TEXT CHECK
    (successful_candidate_sha256 IS NULL OR length(successful_candidate_sha256) = 64),
  completion_sha256 TEXT CHECK (completion_sha256 IS NULL OR length(completion_sha256) = 64),
  run_status TEXT NOT NULL CHECK
    (run_status IN ('pending', 'leased', 'completed', 'failed', 'cancelled', 'unavailable')),
  lease_owner TEXT,
  lease_expires_at TEXT,
  attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
  state_version INTEGER NOT NULL CHECK (state_version >= 0),
  UNIQUE (provider_id, window_id, trade_date, universe_id),
  UNIQUE (job_id, provider_id, window_id),
  CHECK (run_status NOT IN ('completed') OR
    (successful_evidence_sha256 IS NOT NULL AND successful_candidate_sha256 IS NOT NULL AND
     completion_sha256 IS NOT NULL)),
  FOREIGN KEY (provider_id, window_id) REFERENCES qualification_window(provider_id, window_id)
);

CREATE TABLE shadow_attempt_report (
  attempt_id TEXT PRIMARY KEY,
  report_id TEXT NOT NULL UNIQUE,
  job_id TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  endpoint_class TEXT NOT NULL,
  logical_request_ordinal INTEGER NOT NULL CHECK (logical_request_ordinal >= 0),
  attempt_number INTEGER NOT NULL CHECK (attempt_number >= 0),
  version_vector_sha256 TEXT NOT NULL CHECK (length(version_vector_sha256) = 64),
  outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failure', 'skip', 'unavailable', 'mismatch')),
  started_at TEXT NOT NULL,
  completed_at TEXT NOT NULL,
  coverage_expected INTEGER NOT NULL CHECK (coverage_expected >= 0),
  coverage_observed INTEGER NOT NULL CHECK (coverage_observed >= 0),
  request_count INTEGER NOT NULL CHECK (request_count >= 0),
  retry_count INTEGER NOT NULL CHECK (retry_count >= 0),
  rate_limit_count INTEGER NOT NULL CHECK (rate_limit_count >= 0),
  failure_class TEXT NOT NULL,
  page_identities_json TEXT NOT NULL,
  page_count INTEGER NOT NULL CHECK (page_count >= 0),
  row_count INTEGER NOT NULL CHECK (row_count >= 0),
  terminal_marker INTEGER NOT NULL CHECK (terminal_marker IN (0, 1)),
  durable_report_ref TEXT NOT NULL,
  report_sha256 TEXT NOT NULL CHECK (length(report_sha256) = 64),
  evidence_refs_json TEXT NOT NULL,
  evidence_sha256 TEXT CHECK (evidence_sha256 IS NULL OR length(evidence_sha256) = 64),
  candidate_sha256 TEXT CHECK (candidate_sha256 IS NULL OR length(candidate_sha256) = 64),
  state_version INTEGER NOT NULL CHECK (state_version >= 0),
  CHECK (outcome = 'success' OR
         (page_identities_json = '[]' AND page_count = 0 AND row_count = 0 AND
          evidence_refs_json = '[]' AND evidence_sha256 IS NULL AND candidate_sha256 IS NULL AND
          terminal_marker = 0)),
  CHECK (outcome <> 'success' OR
         (terminal_marker = 1 AND page_identities_json <> '[]' AND evidence_refs_json <> '[]')),
  CHECK (outcome <> 'success' OR
    (evidence_sha256 IS NOT NULL AND candidate_sha256 IS NOT NULL)),
  FOREIGN KEY (job_id, provider_id, window_id) REFERENCES shadow_job(job_id, provider_id, window_id),
  FOREIGN KEY (provider_id, window_id) REFERENCES qualification_window(provider_id, window_id),
  UNIQUE (attempt_id),
  UNIQUE (attempt_id, provider_id, job_id, window_id, session_id),
  UNIQUE (attempt_id, provider_id, job_id, window_id, session_id, logical_request_ordinal),
  UNIQUE (job_id, provider_id, window_id, session_id, logical_request_ordinal, attempt_number)
);

CREATE TABLE session_report (
  session_report_id TEXT PRIMARY KEY,
  provider_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  successful_attempt_id TEXT,
  evidence_id TEXT,
  candidate_id TEXT,
  trade_date TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failure', 'skip', 'unavailable', 'mismatch')),
  calendar_generation TEXT NOT NULL,
  calendar_sha256 TEXT NOT NULL CHECK (length(calendar_sha256) = 64),
  universe_sha256 TEXT NOT NULL CHECK (length(universe_sha256) = 64),
  version_vector_sha256 TEXT NOT NULL CHECK (length(version_vector_sha256) = 64),
  evidence_sha256 TEXT CHECK (evidence_sha256 IS NULL OR length(evidence_sha256) = 64),
  candidate_sha256 TEXT CHECK (candidate_sha256 IS NULL OR length(candidate_sha256) = 64),
  report_ref TEXT NOT NULL,
  report_sha256 TEXT NOT NULL CHECK (length(report_sha256) = 64),
  state_version INTEGER NOT NULL CHECK (state_version >= 0),
  CHECK (outcome = 'success' OR (evidence_sha256 IS NULL AND candidate_sha256 IS NULL)),
  CHECK (outcome <> 'success' OR
    (successful_attempt_id IS NOT NULL AND evidence_id IS NOT NULL AND candidate_id IS NOT NULL AND
     evidence_sha256 IS NOT NULL AND candidate_sha256 IS NOT NULL)),
  UNIQUE (provider_id, window_id, trade_date),
  FOREIGN KEY (provider_id, window_id) REFERENCES qualification_window(provider_id, window_id),
  FOREIGN KEY (job_id, provider_id, window_id) REFERENCES shadow_job(job_id, provider_id, window_id),
  FOREIGN KEY (successful_attempt_id, provider_id, job_id, window_id, session_id)
    REFERENCES shadow_attempt_report(attempt_id, provider_id, job_id, window_id, session_id),
  FOREIGN KEY (evidence_id, provider_id, job_id, window_id, session_id)
    REFERENCES shadow_evidence_ref(evidence_id, provider_id, job_id, window_id, session_id),
  FOREIGN KEY (candidate_id, provider_id, job_id, window_id, session_id)
    REFERENCES shadow_candidate_ref(candidate_id, provider_id, job_id, window_id, session_id),
  UNIQUE (session_report_id, provider_id, job_id, window_id, session_id)
);

CREATE TABLE shadow_evidence_ref (
  evidence_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  completion_sha256 TEXT NOT NULL CHECK (length(completion_sha256) = 64),
  bundle_ref TEXT NOT NULL,
  bundle_sha256 TEXT NOT NULL CHECK (length(bundle_sha256) = 64),
  attached_session_report_id TEXT,
  UNIQUE (evidence_id, provider_id, job_id, window_id, session_id),
  FOREIGN KEY (job_id, provider_id, window_id) REFERENCES shadow_job(job_id, provider_id, window_id),
  FOREIGN KEY (provider_id, window_id) REFERENCES qualification_window(provider_id, window_id),
  FOREIGN KEY (attached_session_report_id, provider_id, job_id, window_id, session_id)
    REFERENCES session_report(session_report_id, provider_id, job_id, window_id, session_id)
);

CREATE TABLE shadow_evidence_attempt_ref (
  evidence_id TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  logical_request_ordinal INTEGER NOT NULL CHECK (logical_request_ordinal >= 0),
  attempt_id TEXT NOT NULL,
  endpoint TEXT NOT NULL,
  request_id TEXT NOT NULL,
  page_refs_json TEXT NOT NULL,
  page_count INTEGER NOT NULL CHECK (page_count >= 0),
  row_count INTEGER NOT NULL CHECK (row_count >= 0),
  PRIMARY KEY (evidence_id, logical_request_ordinal),
  UNIQUE (attempt_id, provider_id, job_id, window_id, session_id, logical_request_ordinal),
  FOREIGN KEY (evidence_id, provider_id, job_id, window_id, session_id)
    REFERENCES shadow_evidence_ref(evidence_id, provider_id, job_id, window_id, session_id),
  FOREIGN KEY (attempt_id, provider_id, job_id, window_id, session_id, logical_request_ordinal)
    REFERENCES shadow_attempt_report(attempt_id, provider_id, job_id, window_id, session_id, logical_request_ordinal)
);

CREATE TABLE shadow_candidate_ref (
  candidate_id TEXT PRIMARY KEY,
  evidence_id TEXT NOT NULL UNIQUE,
  job_id TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  window_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  candidate_ref TEXT NOT NULL,
  candidate_sha256 TEXT NOT NULL CHECK (length(candidate_sha256) = 64),
  quality_report_ref TEXT NOT NULL,
  quality_report_sha256 TEXT NOT NULL CHECK (length(quality_report_sha256) = 64),
  FOREIGN KEY (evidence_id, provider_id, job_id, window_id, session_id)
    REFERENCES shadow_evidence_ref(evidence_id, provider_id, job_id, window_id, session_id),
  FOREIGN KEY (job_id, provider_id, window_id) REFERENCES shadow_job(job_id, provider_id, window_id),
  UNIQUE (candidate_id, provider_id, job_id, window_id, session_id)
);
```

The lock file is initialized before any reader can run (`0600`, regular, safe ancestor). Writer
transactions take exclusive flock; readers take shared flock and never create a DB/lock/journal.
The global order is intentionally **not** registry-lock -> bundle-lock: network has no lock, bundle
directory publish and release happen first, then one exclusive registry CAS attaches refs. A writer
crash is recovered by SQLite's DELETE journal; a reader seeing a journal or unavailable lock fails
closed. Task 10 must also prove the registry lock and shadow bundle lock are never simultaneously
held.

The application validator additionally requires every composite identity and hash to agree in both
directions: `job/provider/window/session` in the job, attempt, evidence, attempt-ref, session and
candidate rows; `evidence_id` and completion/bundle SHA in the evidence refs; and candidate/evidence
quality hashes in the candidate ref. SQLite enforces the identity FKs/UNIQUEs; the frozen models
enforce the cross-object SHA and ordinal/page/ref equality before any candidate publication.

### Python/SQL round-trip contract (Task 10)

`TermsEvidence` and `ShadowProviderRecord` are frozen Pydantic models with `extra="forbid"`.
Their serializer and descriptor-bound reader projection use the exact SQL names below; no alias
may silently change nullability:

```text
TermsEvidence SQL projection:
  terms_evidence_id, provider_id, official_url_allowlist_json, content_object_relpath,
  content_bytes_sha256, contract_version, as_of_date, reviewer, review_id,
  approved_intended_use, approved_retention, approved_credential_mode,
  approved_quota_decision, manifest_sha256
ShadowProviderRecord SQL projection:
  provider_id, admission_state, adapter_hash, endpoint_contract_hash, source_schema_hash,
  normalizer_hash, reconciliation_policy_hash, terms_evidence_hash, terms_review_id,
  credential_env_name, intended_use, retention_decision, quota_contract,
  required_fields_json, unit_contract_json, state_version, quarantine_reason
```

`manifest_sha256` is the SHA-256 of canonical bytes covering the allowlisted URLs, exact reviewed
content bytes/hash, contract version, as-of date, reviewer, review ID and all approved
intended-use/retention/credential/quota decisions. The provider row stores only that manifest hash
and review ID; the token value is never a model or SQL column. Task 10 must insert a reviewed
TermsEvidence row, serialize/read it through the descriptor-copy projection, and assert byte-for-byte
field equality plus the composite `(provider_id, manifest_sha256, review_id)` FK. A discovered row
round-trips with both terms fields NULL; canary/shadow/qualified serialization rejects either NULL.

The canonical TermsEvidence serialization is frozen as follows. `official_url_allowlist_json` is a
JSON array of exact URL strings in lexical order; content bytes are the exact bounded object bytes,
with no newline normalization. The canonical field order is
`terms_evidence_id,provider_id,official_url_allowlist_json,content_object_relpath,
content_bytes_sha256,contract_version,as_of_date,reviewer,review_id,approved_intended_use,
approved_retention,approved_credential_mode,approved_quota_decision`. Encode the object with
UTF-8, `ensure_ascii=false`, `separators=(",", ":")`, `sort_keys=false`, followed by exactly one
LF. The manifest preimage is
`b"stock-eva/r2f3/terms-evidence/v1\\n" + canonical_json_bytes`; its lowercase hex SHA-256 is
`manifest_sha256`. `content_bytes_sha256` is SHA-256 over the exact content object bytes only.
Changing URL order/content bytes/version/as-of/reviewer/review/approval or any newline changes the
manifest; changing provider or review ID cannot reuse the old composite FK. The descriptor reader
opens the configured object through an ancestor dirfd with `O_NOFOLLOW`, records regular-file
identity/size before and after, enforces the bounded size, hashes the bytes, then projects the SQL
row. Any mutation, symlink, size overflow or hash mismatch is unavailable with zero writes.

The following CAS transaction shapes are frozen for implementation (the `?` values are bound,
never interpolated). Every writer starts with `BEGIN IMMEDIATE`; it commits only when
`cursor.rowcount == 1`, otherwise it executes `ROLLBACK` and returns stale/unavailable. Reports
are append-only; a reset updates only the window and job state.

```text
BEGIN IMMEDIATE;
UPDATE provider_record SET admission_state=?, terms_evidence_hash=?, terms_review_id=?,
  state_version=state_version+1
  WHERE provider_id=? AND state_version=?;
-- require rowcount == 1, else ROLLBACK; then COMMIT

BEGIN IMMEDIATE;
UPDATE qualification_window SET consecutive_sessions=?, window_start=?, window_end=?,
  version_vector_sha256=?, calendar_generation=?, calendar_sha256=?, state_version=state_version+1
  WHERE provider_id=? AND window_id=? AND state_version=?;
-- require rowcount == 1, else ROLLBACK; then COMMIT

BEGIN IMMEDIATE;
UPDATE shadow_job SET run_status=?, lease_owner=?, lease_expires_at=?,
  attempt_count=attempt_count+1, state_version=state_version+1
  WHERE job_id=? AND provider_id=? AND window_id=? AND state_version=?;
-- require rowcount == 1, else ROLLBACK; then COMMIT

BEGIN IMMEDIATE;
INSERT INTO shadow_attempt_report (...) VALUES (...);
INSERT INTO session_report (...) VALUES (...);
UPDATE qualification_window SET consecutive_sessions=?, state_version=state_version+1
  WHERE provider_id=? AND window_id=? AND state_version=?;
-- require the UPDATE rowcount == 1; any failure ROLLBACKs all inserts and leaves the window intact
COMMIT;
```

Task 10 must run two connections with the same starting version: exactly one CAS succeeds, the
stale writer gets rowcount zero and rolls back, and no partial report/window mutation remains.

Task 13 freezes the terminal transaction as one unit. A successful leased job uses the expected
`job_state_version` and `window_state_version`; all inserts and both CAS updates must succeed
before eligibility is true:

```text
BEGIN IMMEDIATE;
UPDATE shadow_job SET run_status='completed', successful_evidence_sha256=?,
  successful_candidate_sha256=?, completion_sha256=?, state_version=state_version+1
  WHERE job_id=? AND provider_id=? AND window_id=? AND run_status='leased'
    AND state_version=?;                         -- rowcount == 1
INSERT INTO shadow_attempt_report (...success report..., evidence_sha256=?, candidate_sha256=?);
INSERT INTO shadow_evidence_ref (...provider/job/window/session..., completion_sha256=?);
INSERT INTO shadow_evidence_attempt_ref (...one row for every exact ordinal...);
INSERT INTO shadow_candidate_ref (...provider/job/window/session/evidence...);
INSERT INTO session_report (...successful_attempt_id,evidence_id,candidate_id,
  evidence_sha256,candidate_sha256...);
UPDATE qualification_window SET window_state=?, consecutive_sessions=?,
  last_session_report_id=?, qualification_evidence_sha256=?, qualification_candidate_sha256=?,
  state_version=state_version+1
  WHERE provider_id=? AND window_id=? AND state_version=?; -- rowcount == 1
COMMIT;
```

The failure/skip/unavailable/mismatch terminal transaction uses the same expected versions and
`BEGIN IMMEDIATE`, inserts the complete sanitized report and session row with all evidence/candidate
fields null, updates `shadow_job.run_status` to `failed`/`unavailable`, resets the window and
commits; it contains no evidence/candidate insert. Any exception, missing object, unreadable bundle,
hash mismatch, FK/CHECK error or CAS rowcount other than one executes `ROLLBACK`. Crash points are
specified before bundle rename, after bundle rename before DB, after each insert, before each CAS,
and after DB commit. Recovery scans committed bundles by deterministic job/evidence identity,
attaches an orphan only through the same idempotent transaction, and reconciles a DB-committed row
against its bundle; a pre-commit crash leaves the prior job/window version unchanged. Lease recovery
may move expired `leased` to `pending` under its own CAS, but it cannot qualify a session.

Run GREEN/checks:

~~~bash
uv run --extra dev pytest -q tests/test_market_provider_registry.py \
  tests/test_r2f2_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-registry-green
uv run --extra dev ruff check backend/app/market/providers/shadow_contracts.py \
  backend/app/market/providers/registry.py backend/app/market/shadow_registry_schema.py \
  backend/app/config.py backend/app/storage/layout.py backend/app/api/market.py \
  backend/app/cli.py tests/test_market_provider_registry.py \
  tests/test_r2f2_golden_compat.py
uv run --extra dev ruff format --check backend/app/market/providers/shadow_contracts.py \
  backend/app/market/providers/registry.py backend/app/market/shadow_registry_schema.py \
  backend/app/config.py backend/app/storage/layout.py backend/app/api/market.py \
  backend/app/cli.py tests/test_market_provider_registry.py \
  tests/test_r2f2_golden_compat.py
git diff --check
~~~

Independent review MUST verify no secret value, network client, dynamic import, canonical-root
write or failover_enabled path entered the commit. Commit:

~~~bash
git add backend/app/market/providers/shadow_contracts.py \
  backend/app/market/shadow_registry_schema.py backend/app/market/providers/registry.py \
  backend/app/config.py backend/app/storage/layout.py backend/app/api/market.py \
  backend/app/cli.py .env.example tests/test_market_provider_registry.py \
  tests/test_r2f2_golden_compat.py docs/data-providers.md
git commit -m "feat(market): add isolated shadow provider registry"
~~~

## Task 11 — Explicit-canary TickFlow and Tushare adapters

### Scope and file whitelist

- Create backend/app/market/providers/http.py for one injected bounded client.
- Create backend/app/market/providers/tickflow.py and tushare.py for source-shaped adapters.
- Create backend/app/market/shadow_evidence.py containing frozen ShadowLogicalRequest/Plan,
  ShadowAttemptCompletion, ShadowRequestCompletion, ShadowCompletion, ShadowAttempt,
  ShadowEvidenceStore and ShadowEvidenceReader models/interfaces.
- Modify backend/app/market/providers/__init__.py only to export static shadow contracts.
- Modify backend/app/config.py, backend/app/cli.py, and pyproject.toml/uv.lock only if a
  reviewed bounded HTTP dependency is needed.
- Create tests/test_market_provider_tickflow.py, tests/test_market_provider_tushare.py, and
  tests/test_market_provider_contract.py.
- Create tests/test_market_shadow_evidence.py and tests/test_r2f2_golden_compat.py
  for crash/cancel/orphan/final-attempt and the R2-F2 evidence/selection/GET fixture.
- Modify docs/data-providers.md only for frozen official contract records and status.

No adapter may import or call canonical EvidenceStore, SessionSelection, CandidateStore, dataset
writer or pointer code. It may reuse the R2-F2 successful-attempt-only algorithm concept
through shadow types: a ShadowRequestPlan predicts logical calls, each attempt retains only
sanitized counts/IDs, and only final successful source-shaped pages enter ShadowEvidenceManifest.
This avoids the current global BaoStock constants while preserving their safety semantics.

`ShadowAttemptReport` is a frozen sanitized JSON object, not an arbitrary exception dump. Its
canonical field order is:

```text
report_id, attempt_id, job_id, provider_id, window_id, session_id, logical_request_ordinal,
request_id, endpoint_class, outcome, started_at, completed_at, coverage_expected,
coverage_observed, request_count, retry_count, rate_limit_count, failure_class,
page_identities, page_count, row_count, evidence_refs, evidence_sha256, candidate_sha256,
durable_report_ref, report_sha256
```

The object is UTF-8 JSON with `ensure_ascii=false`, compact separators, `sort_keys=false`, one LF,
and no secret/token/raw payload. `report_sha256` is SHA-256 of the domain-separated canonical
preimage `b"stock-eva/r2f3/shadow-attempt-report/v1\\n" + json_bytes` (the hash field itself is
excluded from the preimage). The SQL projection must round-trip every field and hash exactly.
The SQL `endpoint` column is a bounded endpoint identity (never a URL/token) and projects to the
JSON `endpoint_class`; it is also repeated in `shadow_evidence_attempt_ref` for ordinal binding.
For success, page/evidence/candidate fields are complete and non-null; for failure/skip/
unavailable/mismatch, page identities/refs/counts/rows and evidence/candidate hashes are exactly
empty/null while timing, coverage, retry/rate-limit counts and sanitized failure class remain.

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
def test_multi_endpoint_partial_and_different_retry_successes_publish_contiguous_pages(tmp_path): ...
def test_duplicate_missing_or_out_of_order_final_pages_are_unavailable(tmp_path): ...
def test_one_evidence_binds_multiple_final_attempt_refs_by_ordinal_endpoint_request_and_pages(tmp_path): ...
def test_failure_skip_unavailable_mismatch_persist_sanitized_report_without_evidence_or_candidate(tmp_path): ...
def test_failed_attempt_cannot_be_selected_as_final_completion(tmp_path): ...
def test_completion_rejects_endpoint_request_page_count_row_count_or_hash_mismatch(tmp_path): ...
def test_shadow_evidence_crash_cancel_and_orphan_are_unreadable(tmp_path): ...
def test_shadow_evidence_bundle_commit_marker_is_atomic_and_idempotent(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_provider_contract.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_shadow_evidence.py tests/test_r2f2_golden_compat.py \
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
source-shaped write. It accepts only a complete ShadowCompletion: every ordinal in the exact
request-plan set has one final successful ShadowRequestCompletion with contiguous final page refs;
failed attempts retain sanitized IDs/counts/outcome in memory/control audit and zero durable refs/
rows. Publication is `staging/<attempt-owned-nonce>/` (all payloads, manifest and COMMIT) followed
by fsync and exclusive atomic directory rename into `bundles/<evidence_id>/`; no raw object is
globally visible before rename. Cancel/crash before rename leaves staging residue only; owner-marker
recovery removes it and writes sanitized audit JSON. Post-rename/pre-DB is a legal committed final
bundle for deterministic attach/quarantine scanning. ShadowEvidenceReader reads bundles only,
requires COMMIT, and uses dirfd/O_NOFOLLOW, pre/post fstat, hash/schema/row/page checks; missing
marker, orphan, changed object or non-final attempt is unavailable. Every outcome also writes a
bounded sanitized `ShadowAttemptReport` ref/hash to the control DB; failure/skip/unavailable/
mismatch reports have zero page/row/evidence refs and are never eligible for candidate creation.
Manifest validation is
bidirectional: every completion ordinal/page identity must name exactly one object and every
manifest page/object must be named by the completion; otherwise the whole evidence is unavailable.
The
CLI plan is:

The executable validator/projection is frozen as this read-only algorithm (all queries are bound
parameters and run before any candidate write):

```python
plan = load_plan(job_id)  # exact ordinal set + request_plan_hash
refs = conn.execute("""
  SELECT r.logical_request_ordinal, r.attempt_id, r.endpoint, r.request_id,
         r.page_refs_json, r.page_count, r.row_count,
         a.outcome, a.page_identities_json, a.evidence_sha256, a.candidate_sha256
    FROM shadow_evidence_attempt_ref AS r
    JOIN shadow_attempt_report AS a
      ON (a.attempt_id, a.provider_id, a.job_id, a.window_id, a.session_id,
          a.logical_request_ordinal) =
         (r.attempt_id, r.provider_id, r.job_id, r.window_id, r.session_id,
          r.logical_request_ordinal)
   WHERE r.evidence_id=? AND r.provider_id=? AND r.job_id=? AND r.window_id=? AND r.session_id=?
   ORDER BY r.logical_request_ordinal
""", identity).fetchall()
assert {r.ordinal for r in refs} == plan.exact_ordinal_set
for r in refs:
    assert r.outcome == "success" and r.endpoint == plan[r.ordinal].endpoint
    assert r.request_id == plan[r.ordinal].request_id
    assert page_ids(r.page_refs_json) == contiguous_page_ids(r.page_count)
    assert page_ids(r.page_refs_json) == page_ids(r.page_identities_json)
    assert r.row_count == sum(page_rows(r.page_refs_json))
    assert sha256_objects(r.page_refs_json) == manifest_page_hash(evidence_id, r.ordinal)
assert bidirectional_manifest_pages(evidence_id, refs)
assert sha256_completion(plan, refs) == evidence_completion_sha(evidence_id)
```

The validator rejects a failed attempt selected as final, a missing/duplicate/out-of-order page,
an endpoint or request-id mismatch, a page count/row count/hash mismatch, an ordinal outside the
plan, a missing ordinal, or any manifest object not named by the completion. Named tests cover each
counterexample and prove zero candidate/evidence publication on failure.

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
  tests/test_market_shadow_evidence.py tests/test_r2f2_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-adapters-green
uv run --extra dev ruff check backend/app/market/providers backend/app/market/shadow_evidence.py \
  backend/app/config.py backend/app/cli.py tests/test_market_provider_contract.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_shadow_evidence.py tests/test_r2f2_golden_compat.py
uv run --extra dev ruff format --check backend/app/market/providers backend/app/market/shadow_evidence.py \
  backend/app/config.py backend/app/cli.py tests/test_market_provider_contract.py \
  tests/test_market_provider_tickflow.py tests/test_market_provider_tushare.py \
  tests/test_market_shadow_evidence.py tests/test_r2f2_golden_compat.py
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
  tests/test_r2f2_golden_compat.py docs/data-providers.md
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
  `CanonicalCandidateReader` and closeable `PublishedCanonicalComparison` capability whose frozen
  value is `CanonicalComparisonSnapshot`; do not assume or modify an existing reader.
- Modify backend/app/market/providers/tickflow.py and tushare.py only to expose typed source
  evidence needed by the normalizer.
- Modify backend/app/market/models.py only for additive, shadow-namespaced models; do not alter
  DailyBar.source, RefreshResult.source, MarketSummary.source or canonical quality fields.
- Create tests/test_market_shadow_normalize.py and tests/test_market_reconciliation.py; update
  provider tests only for source-contract assertions.
- Create tests/test_market_shadow_candidates.py and tests/test_market_canonical_comparison.py;
  these are read-only comparison tests. The fixed `tests/fixtures/r2f2_golden/**` and
  `tests/test_r2f2_golden_compat.py` are in the Task 12 whitelist for invocation, never rewrite.

Task 12's machine-checkable whitelist is exactly the following path set (fixture prefix is
read-only and the golden module may only be invoked):

```text
backend/app/market/shadow_normalize.py
backend/app/market/shadow_reconciliation.py
backend/app/market/shadow_candidates.py
backend/app/market/canonical_comparison.py
backend/app/market/providers/tickflow.py
backend/app/market/providers/tushare.py
backend/app/market/models.py
tests/test_market_shadow_normalize.py
tests/test_market_reconciliation.py
tests/test_market_shadow_candidates.py
tests/test_market_canonical_comparison.py
tests/test_r2f2_golden_compat.py                 # invoke named test only
tests/fixtures/r2f2_golden/**                    # read-only; no serializer writes
```

No other provider, canonical writer, fixture, selection or evidence path is in the Task 12
whitelist.

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
def test_canonical_candidate_reader_rejects_root_bundle_and_fd_toctou(tmp_path): ...
def test_published_canonical_comparison_verify_before_after_and_close_capability(tmp_path): ...
def test_canonical_comparison_rejects_mixed_generation_or_lineage(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_shadow_normalize.py \
  tests/test_market_reconciliation.py tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py tests/test_market_shadow_candidates.py \
  tests/test_market_canonical_comparison.py \
  --basetemp=/tmp/stock-eva-r2f3-reconcile-red
uv run --extra dev pytest -q \
  tests/test_r2f2_golden_compat.py::test_r2f2_golden_compatibility_is_byte_hash_reader_and_get_stable
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
bundle and revalidates every object/manifest hash before returning it. `CanonicalCandidateReader`
is strictly read-only: one open captures the current immutable Dataset manifest generation/identity,
exact BaoStock trade-date partition path/SHA/row count and nine R2-F2 publication-lineage fields
(`provider_id`, `universe_id`, `evidence_id`, `evidence_sha256`, `candidate_id`,
`candidate_manifest_sha256`, `gate_report_sha256`, `adapter_version`, `source_schema_version`),
then descriptor-bound reads the existing candidate.json, gate.json, selection.json, normalized.json
and EvidenceReader. It reruns current public model validators and bidirectionally checks selection ID/SHA,
candidate/evidence/gate/factor/normalized hashes, date/universe/provider and partition bar
semantics, then verifies the Dataset manifest identity again. `PublishedCanonicalComparison.verify()`
must run both before and after reconciliation and `close()` makes the capability unusable. Any
missing/legacy lineage, root/bundle/fd TOCTOU, object change, hash/date/universe mismatch or
generation drift returns the frozen unavailable reason and performs zero writes/provider calls; no
new canonical fields are introduced. The four-file whitelist is exactly candidate/gate/selection/
normalized JSON; no canonical writer changes are in scope.

Run GREEN/checks:

~~~bash
uv run --extra dev pytest -q tests/test_market_shadow_normalize.py \
  tests/test_market_reconciliation.py tests/test_market_provider_tickflow.py \
  tests/test_market_provider_tushare.py tests/test_market_shadow_candidates.py \
  tests/test_market_canonical_comparison.py \
  --basetemp=/tmp/stock-eva-r2f3-reconcile-green
uv run --extra dev pytest -q \
  tests/test_r2f2_golden_compat.py::test_r2f2_golden_compatibility_is_byte_hash_reader_and_get_stable
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
  tests/test_r2f2_golden_compat.py tests/fixtures/r2f2_golden/** \
  tests/test_market_provider_contract.py
git commit -m "feat(market): reconcile complete shadow candidates"
~~~

## Task 13 — Shadow scheduler and qualification report

### Scope and file whitelist

- Create backend/app/market/shadow_scheduler.py for post-canonical bounded work and session
  window evaluation.
- Create backend/app/market/shadow_jobs.py containing durable ShadowJobStore outbox, lease/CAS,
  recovery scanner and immutable ConfirmedSessionSnapshot/ShadowAttemptReport persistence.
- Create backend/app/market/shadow_calendar.py containing `ConfirmedCalendarReader` (or modify
  `calendar.py` only additively) to read exact involved-year CalendarConfig dumps, official-source
  metadata and closed dates and produce generation/hash/exact next confirmed sessions.
  This is a new shadow-specific reader/Protocol; it MUST NOT reuse or alias the existing
  `continuity.py` `ConfirmedCalendarReader`/Protocol name.
- Modify backend/app/market/automation.py only to enqueue a bounded handoff after the canonical
  pointer/manifest commit and after RefreshRunLock is released; no shadow work or blocking enqueue
  may run inside the canonical lock.
- Modify backend/app/market/providers/registry.py for report/window transitions only.
- Modify backend/app/api/market.py additively for read-only shadow status; no write on GET.
- Modify backend/app/cli.py for plan/shadow/status/promote parser and explicit authorization gate.
- Create tests/test_market_shadow.py, update tests/test_market_get_read_only.py only for
  read-only shadow fingerprints, and create docs/acceptance/release-2-r2f3.md as a NO-GO/pending
  evidence skeleton.
- Create tests/test_market_shadow_jobs.py and tests/test_r2f2_golden_compat.py for
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
def test_run_due_once_offers_handoff_only_after_lease_exit_and_returns_original_outcome(tmp_path): ...
def test_ready_and_non_run_decisions_offer_idempotently_without_canonical_lock(tmp_path): ...
def test_busy_handoff_is_dropped_without_changing_canonical_outcome(tmp_path): ...
def test_refresh_already_running_is_not_offered_or_retried_inside_lock(tmp_path): ...
def test_unexpected_handoff_exception_is_sanitized_and_never_changes_outcome(tmp_path): ...
def test_published_but_unenqueued_manifest_is_recovered_by_scanner(tmp_path): ...
def test_worker_lease_reclaims_after_crash_and_completion_is_idempotent(tmp_path): ...
def test_confirmed_session_snapshot_freezes_calendar_generation_and_next_sessions(tmp_path): ...
def test_each_attempt_report_records_success_failure_skip_unavailable_or_mismatch(tmp_path): ...
def test_gap_failure_or_version_drift_resets_window_in_one_transaction(tmp_path): ...
def test_confirmed_calendar_unknown_year_is_unavailable_and_universe_hash_is_durable(tmp_path): ...
def test_bundle_before_db_crash_scanner_attaches_or_dedupes_without_window_mutation(tmp_path): ...
def test_shadow_calendar_reader_is_distinct_from_continuity_protocol(tmp_path): ...
def test_success_terminal_transaction_attaches_all_refs_before_window_eligibility(tmp_path): ...
def test_failure_terminal_transaction_keeps_report_but_no_evidence_candidate(tmp_path): ...
def test_crash_before_after_bundle_db_and_orphan_recovery_preserve_versions(tmp_path): ...
def test_success_missing_unreadable_or_hash_mismatch_rolls_back_job_session_and_window(tmp_path): ...
~~~

Run RED:

~~~bash
uv run --extra dev pytest -q tests/test_market_shadow.py \
  tests/test_market_get_read_only.py -k 'shadow or provider_status' \
  tests/test_market_shadow_jobs.py tests/test_r2f2_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-shadow-red
~~~

### GREEN

The scheduler invokes a secondary only after canonical freshness/repair has made its decision,
within an independent deadline and request budget. It MUST be cancelable and MUST record
budget_exhausted rather than retrying or delaying canonical work. The exact automation refactor is:

```python
with refresh_run_lock:
    outcome = _execute_due()
# lock is released before any shadow operation
try:
    shadow_handoff.offer(outcome, wait_budget=0, nonblocking=True)
except Exception:  # busy/worker errors are sanitized and dropped
    safe_log("shadow_handoff_dropped", outcome_id=outcome.safe_id)
return outcome
```

The real implementation must use a narrow exception boundary and sanitized logging; it MUST NOT
call a worker or `post_publish` callback while `RefreshRunLock` is held. A `decision != run` ready
outcome is offered idempotently through the same post-lock handoff. Enqueue/worker failure cannot
change or delay the already-determined canonical result. `ShadowScheduler.run_once` is an
independent worker/CLI later, with its own lock, budget and cancellation. A bounded scanner scans
immutable canonical manifest/session records only within configured `shadow_start_date`, provider
and window, recovering every ready published-but-unenqueued identity idempotently. Frozen crash
points cover pointer commit, lock release, handoff offer, outbox commit, lease acquisition, bundle
rename, bundle-before-DB and DB CAS.

`ConfirmedCalendarReader` reads the exact involved-year `CalendarConfig` canonical dump, official
source metadata and closed dates; unknown year or missing proof is unavailable. It computes the
calendar generation/hash and exact next confirmed sessions. The universe hash comes from the exact
durable expected symbol/index set in `PublishedCanonicalComparison`, not a fresh provider response.
Each confirmed session begins with immutable ConfirmedSessionSnapshot containing exact calendar
generation/hash, next confirmed sessions, universe hash and capture identity. Every attempt emits a
ShadowAttemptReport outcome success/failure/skip/unavailable/mismatch with timing, request/retry/
rate-limit counts, coverage, version vector and all relevant hashes. The attempt/session report
bundle is durably published and its bundle lock released before one exclusive registry transaction/
CAS inserts attempt/session refs and updates or resets qualification_window; any gap, failure,
unavailable/mismatch or adapter/endpoint/source-schema/normalizer/reconcile/terms/universe/
calendar/config drift resets the window while retaining every prior report. A crash after bundle
publish but before DB commit is recovered by scanner attach/dedupe; if the DB commit fails the
qualification window is unchanged. GET
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
  tests/test_market_shadow_jobs.py tests/test_r2f2_golden_compat.py \
  --basetemp=/tmp/stock-eva-r2f3-shadow-green
uv run --extra dev ruff check backend/app/market/shadow_scheduler.py backend/app/market/shadow_jobs.py \
  backend/app/market/automation.py backend/app/market/providers/registry.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_shadow.py \
  tests/test_market_get_read_only.py tests/test_market_shadow_jobs.py \
  tests/test_r2f2_golden_compat.py
uv run --extra dev ruff format --check backend/app/market/shadow_scheduler.py backend/app/market/shadow_jobs.py \
  backend/app/market/automation.py backend/app/market/providers/registry.py \
  backend/app/api/market.py backend/app/cli.py tests/test_market_shadow.py \
  tests/test_market_get_read_only.py tests/test_market_shadow_jobs.py \
  tests/test_r2f2_golden_compat.py
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
  tests/test_r2f2_golden_compat.py tests/test_market_get_read_only.py \
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
| Registry/admission/secrets (H5/H7/M1) | Task10 shadow contracts, frozen migration schema, registry, config, layout, API response, env example | exact env/TermsEvidence rejection-before-client; DDL/PRAGMA/CAS/lock/deserialize/fingerprint; named zero-write reasons; `test_r2f2_golden_compatibility_is_byte_hash_reader_and_get_stable` |
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
