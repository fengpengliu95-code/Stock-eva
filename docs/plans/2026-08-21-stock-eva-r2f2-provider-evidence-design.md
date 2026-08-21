# Stock EVA R2-F2 Provider Evidence Framework Design

**Author:** Codex delivery team — specification owner
**Date:** 2026-08-21 (Asia/Shanghai)
**Status:** In Review
**Decision authority:** User approved starting R2-F2 on 2026-08-21; implementation still requires
  this specification to be independently reviewed and approved.
**Scope:** R2-F2 offline code and synthetic tests only; one BaoStock compatibility adapter.
**Baseline:** branch `codex/r2-f2-provider-evidence`, exact clean HEAD
`f6c18d64a8ac04bd24d3f49fb38a28523e6eb8de`
**Reviewers:** Independent code/spec reviewer pending; root quality review pending

**Related documents:**

- [R2-F roadmap](2026-08-12-stock-eva-r2f-data-reliability-roadmap.md)
- [R2-F umbrella implementation plan](2026-08-12-stock-eva-r2f-data-reliability-implementation.md)
- [R2-F1 design](2026-08-13-stock-eva-r2f1-continuity-controller-design.md)
- [R2-F1 implementation plan](2026-08-13-stock-eva-r2f1-continuity-controller-implementation.md)
- [R2-F1 acceptance](../acceptance/release-2-r2f1.md)
- [R2-F0.1 transport design](2026-08-12-stock-eva-r2f0-1-provider-transport-design.md)
- [R2-F0.1 transport implementation](2026-08-12-stock-eva-r2f0-1-provider-transport-implementation.md)

This document specifies Tasks 7–9 of the umbrella plan. It does not approve R2-F3, a second
provider, automatic failover, production refresh, installation, NAS access or LaunchAgent work.

## Context

The current market path is a direct BaoStock implementation. `backend/app/market/baostock.py`
performs endpoint-specific requests, validates the response shape, and calls the existing
`normalize_baostock_rows()` function. The canonical boundary remains
`Normalize -> Quality Gate -> Immutable Parquet -> SHA-256 -> Manifest -> Atomic Publish`.
`DailyBar`, `RefreshResult`, `MarketSummary`, price-series readers and the current dataset
manifest currently use `baostock` as a literal source identity. This is a compatibility fact, not
a license to rewrite old Parquet or old manifests.

R2-F0.1 already provides fixed BaoStock endpoint identifiers, refresh/session/request scopes,
socket-level receive metrics, checked-send transport patching, sanitized failure classification,
and endpoint/provider circuit state. R2-F1 adds strict immutable session inventory, a durable repair
queue, freshness-aware repair scheduling and read-only continuity status. R2-F2 must make the
provider-shaped input auditable and replayable before normalization while leaving those transport,
endpoint, circuit, scheduler and canonical publication semantics unchanged.

The direct path cannot prove which source bytes produced a candidate after a later investigation.
R2-F2 therefore captures a bounded, purpose-built source-shaped record once, validates and
publishes it atomically, and makes normalization read only from that published evidence. It also
records candidate gate decisions and one complete-session selection record. This creates the
lineage needed by the later shadow and failover stages without enabling them now.

## Design decisions and ambiguity audit

1. **Only BaoStock is admitted in R2-F2.** The provider registry is a small static allowlist whose
   only value is `baostock`. It has no plugin loader, entry points, dynamic import, arbitrary
   provider string admission or second-source adapter.
2. **The incumbent transport remains authoritative.** The compatibility adapter delegates to the
   current BaoStock transport/parser and preserves its endpoint names, session scopes, retry
   limits, timeout policy, provider-code mapping and circuit integration. F0.1 transport code is
   outside the R2-F2 file whitelist.
3. **Source-shaped evidence is bounded and intentional.** “Raw” means typed rows and request
   metadata required to reproduce the BaoStock normalization contract. It is not a socket dump,
   response body archive, log, exception, credential, URL, cookie or header store. The source-row
   schema is an endpoint allowlist and rejects unknown fields.
4. **Evidence is published before normalization.** The online sequence is
   `fetch source rows -> validate/redact -> write bounded Parquet partial -> read back schema,
   row count and hash -> atomic object rename -> atomic canonical JSON evidence manifest -> read
   the published evidence -> normalize -> candidate gates`. A live SDK result is never passed
   directly to normalization by the R2-F2 canonical path.
5. **Evidence and candidate artifacts are audit roots, not serving roots.** They use relative paths
   and content hashes. Canonical Parquet and the existing current pointer remain the only serving
   source. Evidence failure or candidate failure cannot move the pointer.
6. **Manifest compatibility uses additive metadata.** Existing dataset manifest format and legacy
   object bytes remain readable. New partition entries carry evidence/candidate lineage fields;
   legacy entries remain marked by absence of R2-F2 lineage rather than being rewritten. New strict
   writers require lineage for every new partition.
7. **`qualified_fallback` is reserved, not active.** The selection schema reserves
   `qualified_fallback` for a future R2-F4 policy, but R2-F2 can write only `primary_ready` and
   must reject a fallback candidate because no secondary is admitted. This preserves forward
   schema shape without exposing a value that old readers could encounter in current data.
8. **One candidate means one complete session.** Candidate and selection scope is exactly one
   `(trade_date, universe_id)` session. Symbol-level mixing, partial candidate promotion and
   implicit fallback remain prohibited.
9. **No guessed provider semantics.** BaoStock source/date/unit/factor/suspension semantics are
   explicit contracts tested against the existing normalizer. A field whose semantics cannot be
   established by the current adapter contract fails the candidate; it is not inferred from a
   provider message or sample.

### Ambiguities carried to review

The umbrella plan leaves the exact evidence Parquet layout, the additive manifest lineage fields,
the static registry shape, and whether `qualified_fallback` is emitted or delayed unspecified.
The decisions above resolve these at the narrowest compatible boundary. If an independent review
finds that an existing reader cannot ignore the additive fields, or that the current BaoStock
adapter cannot expose source rows without changing transport semantics, implementation must stop
and amend this specification before code. No provider procurement, terms assumption, unit guess or
automatic failover may be used to close that gap.

## Functional requirements

- FR-1: **Registry admission:** The provider registry MUST be static and allow only
  `ProviderId("baostock")` in R2-F2. Unknown, empty, mixed-case, path-like or plugin-resolved IDs
  MUST fail closed.
- FR-2: **Provider contract:** `ProviderRequest` MUST carry provider ID, exact trade date,
  universe ID and a sorted unique exact symbol set. `ProviderRawBatch` MUST carry adapter and
  endpoint-contract versions, timezone-aware timestamps, endpoint rows, source schema, units and
  date semantics.
- FR-3: **Compatibility adapter:** The BaoStock adapter MUST delegate to the incumbent
  transport/parser boundary without changing F0.1 endpoint identifiers, provider-code mapping,
  circuit state transitions, timeout, retry, pagination or session semantics.
- FR-4: **Source capture boundary:** The canonical online path MUST capture source-shaped rows
  before normalization, and normalization MUST accept only a validated reader of an atomically
  published evidence object.
- FR-5: **Source schema allowlist:** Evidence MUST contain only the declared BaoStock fields for
  `trade_dates`, `all_stock`, `daily_astock`, `daily_factor`, `adjust_factor` and `index_history`.
  Unknown columns, duplicate columns, row-length mismatches, non-finite numeric values or
  unsupported nested values MUST invalidate the complete evidence batch.
- FR-6: **Evidence object:** Source rows MUST be stored in a bounded Parquet object and its
  metadata in canonical UTF-8 JSON with sorted keys and compact separators. The evidence object
  MUST be content-addressed by SHA-256 and idempotent for identical bytes.
- FR-7: **Evidence manifest binding:** The evidence manifest MUST bind each relative object path to
  its content hash, schema identifier, row count, provider ID, adapter version, endpoint-contract
  version, trade date, universe ID, units, date semantics and request identity. Absolute paths,
  URL, token, cookie, header, raw payload and arbitrary exception fields MUST be impossible in the
  persisted model.
- FR-8: **Atomic evidence publish:** Partial objects MUST be written below the evidence staging
  root, read back and verified, then atomically renamed and followed by an atomically published
  canonical JSON manifest. Partial or swapped objects MUST never be treated as published.
- FR-9: **Evidence-only normalization:** The BaoStock normalizer MUST read the published,
  hash-verified source evidence through an evidence reader. A failed, missing, corrupt, oversized,
  decompression-invalid or manifest-mismatched object MUST fail before normalization and before
  any candidate or canonical publication.
- FR-10: **Offline replay:** `market-provider-replay` MUST read only an evidence ID from a local
  evidence root, perform zero network/provider calls, perform zero canonical Parquet/manifest/
  pointer writes, and produce a deterministic replay result.
- FR-11: **Replay equivalence:** Replay of an unchanged evidence object with the same adapter,
  normalizer and gate versions MUST produce the same canonical candidate bytes or an explicit
  semantic-equivalence report with identical date, universe, symbol set, factor and suspension
  semantics. Nondeterminism MUST fail closed.
- FR-12: **Candidate scope:** A candidate MUST cover one exact trade date, one exact universe,
  the complete required symbol/index set and one provider ID. A candidate with missing, duplicate,
  unexpected or mixed-provider symbols MUST be rejected as a whole.
- FR-13: **Gate evidence:** Every candidate gate evaluation MUST create an immutable, sanitized
  gate report, including rejected candidates. The report MUST record gate name/version, verdict,
  bounded metrics, failure taxonomy and referenced object hashes, never raw exception text.
- FR-14: **Candidate manifest:** A candidate manifest MUST bind candidate ID, evidence ID/hash,
  normalized candidate object hash/path, gate report hash, provider/adapter/schema versions,
  universe ID, trade date, row count and complete-session scope. It MUST be immutable and
  non-serving.
- FR-15: **Selection manifest:** A canonical publication MUST have exactly one immutable selection
  record for its complete session candidate. The selected candidate, provider, evidence hash,
  candidate manifest hash and gate report hash MUST all be present before pointer movement.
- FR-16: **No mixing/failover:** R2-F2 MUST NOT mix symbols across candidates/providers, select a
  secondary, invoke automatic failover or write `qualified_fallback`. The reserved value MUST be
  schema-compatible but unreachable under the R2-F2 registry and policy.
- FR-17: **Source identity migration:** Internal source fields MUST use the validated provider ID
  type while preserving public/current `baostock` values. Missing source fields in legacy objects
  MUST decode in memory as `baostock`; no legacy Parquet, manifest or JSON object may be rewritten.
- FR-18: **New publication lineage:** Every R2-F2-created canonical partition MUST carry evidence
  ID/hash, candidate manifest hash, gate report hash, provider ID, adapter version, source schema
  version and universe ID in additive manifest metadata. A strict R2-F2 reader MUST reject a
  mismatch among any of these values before serving or promotion.
- FR-19: **Canonical chain preservation:** Candidate selection MUST invoke the existing immutable
  Parquet, SHA-256, manifest and atomic pointer publication functions. R2-F2 MUST NOT hand-edit
  Parquet, manifest or pointer files.
- FR-20: **Exact source semantics:** BaoStock daily source evidence MUST preserve the exact
  requested ISO trade date and source symbols. Canonical daily rows remain unadjusted OHLCV/amount;
  volume is shares, amount is CNY, and turnover is the provider's percentage field. The adapter
  MUST reject a date mismatch or unsupported unit declaration.
- FR-21: **Factor semantics:** `adjustflag=3` daily prices remain unadjusted. For eligible stock
  rows, `backAdjustFactor` is a finite positive factor selected on or before the trade date;
  missing/invalid factors fail the candidate. Index rows have no stock factor requirement.
- FR-22: **Suspension semantics:** `tradestatus != "1"` means suspended/non-trading. A legal
  suspended stock may have missing factor and blank activity represented as zero, with exactly the
  existing suspended-placeholder quality issue. Active stock missing factors, non-zero suspended
  activity, suspended indexes or malformed placeholder rows fail closed.
- FR-23: **Failure taxonomy:** Evidence, candidate, replay and public status MUST use existing
  allowlisted failure classes plus transport normalized errors/provider code. Unknown provider
  status remains `UNKNOWN_PROVIDER_PROTOCOL_ERROR`; semantic meaning MUST NOT be guessed.
- FR-24: **Sanitization:** No evidence manifest, candidate manifest, gate report, replay result,
  log, CLI output or public model may contain token, secret, authorization header, cookie, URL,
  absolute/local path, raw payload or arbitrary exception text.
- FR-25: **Read-only public paths:** Existing GET endpoints and the replay/plan paths MUST remain
  zero-write. Missing/corrupt evidence or candidate data MUST report unavailable/error with an
  allowlisted reason and leave canonical pointer and historical objects unchanged.
- FR-26: **Legacy readers:** Existing legacy manifests, Parquet, refresh rows, analysis, alert,
  user and market API readers MUST decode without migration-time object rewrite. Existing response
  field names and `source="baostock"` behavior remain compatible.
- FR-27: **R2-F3 boundary:** Provider qualification, TickFlow/Tushare/AKShare adapters, shadow
  scheduling, 20-session observation, reconciliation against a second source and failover are
  explicitly deferred to R2-F3/F4.

## Non-functional requirements

- **NFR-1 (Offline):** All R2-F2 tests and review commands MUST use fakes, fixtures and temporary
  roots; zero DNS/socket/provider/NAS/LaunchAgent/install/production action is permitted.
- **NFR-2 (Atomicity):** A process crash at every evidence staging, object rename, evidence-manifest,
  candidate-manifest or selection-manifest boundary MUST expose either the prior complete state or
  a complete new state, never a partial published object.
- **NFR-3 (Integrity):** Every object read MUST bind an opened descriptor/stat fingerprint to
  SHA-256/schema/row-count validation; TOCTOU, symlink, path traversal, object substitution and
  manifest swap MUST fail closed.
- **NFR-4 (Bounds):** Evidence metadata MUST be <= 1 MiB, each source evidence object MUST be <=
  64 MiB, decompressed Parquet content MUST remain within the same bounded policy, and row counts
  MUST be non-negative and schema-consistent. Oversize or decompression failure is an error.
- **NFR-5 (Concurrency):** Shared market/evidence publication locks and CAS/version checks MUST
  allow exactly one logical evidence/candidate/selection publication for a request key; losers do
  zero canonical writes.
- **NFR-6 (Determinism):** Canonical JSON serialization, object hashes, candidate IDs, request keys,
  row ordering and replay output MUST be deterministic for identical inputs and contract versions.
- **NFR-7 (Compatibility):** No existing immutable object, legacy manifest, public response contract
  or API reader requires a rewrite. New fields are additive and old source values remain valid.
- **NFR-8 (Security):** Persisted schemas MUST have no payload/token/header/cookie/URL/path/raw
  exception field; all identifiers and provider codes are bounded allowlisted strings.
- **NFR-9 (Semantic correctness):** Date, units, adjustment-factor and suspension gates MUST be
  explicit and tested; no amount/OHLCV value may be labeled fund flow.
- **NFR-10 (Bounded work):** One online refresh request captures and publishes one complete session
  evidence/candidate/selection chain; it MUST not silently loop across sessions or symbol-slice
  fallbacks.
- **NFR-11 (No transport weakening):** R2-F2 MUST NOT increase timeouts, retries, retry frequency,
  circuit thresholds, coverage/factor gates or change endpoint/circuit meanings.
- **NFR-12 (Read-only audit):** GET, plan and replay fingerprints MUST show no changed file bytes,
  mtimes, database schema, canonical manifest or current pointer.
- **NFR-13 (Reviewability):** Each Task 7–9 commit MUST have focused RED/GREEN evidence, `ruff`,
  `git diff --check`, an independent High/Medium review and a bounded file whitelist.
- **NFR-14 (Provider admission):** R2-F2 may write only BaoStock identity and must not dynamically
  import or register an unreviewed provider.

## API contracts

These are internal Python contracts unless explicitly marked CLI. They are frozen for R2-F2 before
implementation.

```python
ProviderId = Literal["baostock"]

class ProviderRequest(BaseModel):
    provider_id: ProviderId
    trade_date: date
    universe_id: SafeIdentifier
    symbols: tuple[SafeSymbol, ...]

class ProviderRawBatch(BaseModel):
    provider_id: ProviderId
    request: ProviderRequest
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion
    started_at: datetime  # timezone-aware UTC
    completed_at: datetime  # timezone-aware UTC
    source_schema: tuple[str, ...]
    units: Mapping[str, str]
    date_semantics: Literal["explicit_trade_date"]
    endpoint_batches: tuple[RawEndpointBatch, ...]
    request_count: int
    failed_symbols: tuple[SafeSymbol, ...] = ()
    failure_class: FailureClass | NormalizedTransportError | None = None
```

`ProviderRawBatch` is an in-memory pre-publication contract. Its persisted equivalent is
`EvidenceManifest`; no raw batch model is serialized as an unrestricted object.

```python
class BaoStockDailyBarAdapter(Protocol):
    provider_id: Literal["baostock"]
    adapter_version: str
    endpoint_contract_version: str

    def fetch_raw(self, request: ProviderRequest) -> ProviderRawBatch: ...
    def normalize(self, evidence: PublishedEvidence) -> tuple[DailyBar, ...]: ...

class EvidenceStore(Protocol):
    def publish(self, batch: ProviderRawBatch) -> EvidenceManifest: ...
    def read(self, evidence_id: str) -> PublishedEvidence: ...
    def replay(self, evidence_id: str) -> ReplayResult: ...
```

CLI contract:

```text
market-provider-replay --evidence-id ID [--compare-candidate-sha SHA]
```

It returns JSON containing only `status`, `evidence_id`, `candidate_sha256`, `semantic_match`,
`failure_class` and bounded counts/dates. It accepts no credential, URL, path or provider argument.
Missing/corrupt evidence exits non-zero with an allowlisted `EVIDENCE_*` reason and zero writes.
No new HTTP write endpoint is introduced. Existing `GET /api/v1/market/status`, summary and
history contracts remain read-only and compatible; any additive lineage fields are optional and
must never be required by legacy clients.

```typescript
interface ProviderReplayResponse {
  status: "ready" | "unavailable" | "error";
  evidence_id: string;
  candidate_sha256: string | null;
  semantic_match: boolean | null;
  failure_class: string | null;
  row_count: number;
  trade_date: string | null;
}
```

## Data models

All persisted JSON uses canonical UTF-8, sorted keys and compact separators. All timestamps are
UTC-aware. `SafeIdentifier` is bounded ASCII `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`; paths are
relative, non-absolute, traversal-free and never symlinks.

### Provider registry

| Field | Type | Constraint |
| --- | --- | --- |
| provider_id | `Literal["baostock"]` | Static allowlist only |
| adapter_version | safe version | Pinned code contract |
| endpoint_contract_version | safe version | Fixed six-endpoint contract |
| supported_fields | tuple[str, ...] | Endpoint allowlist |
| date_semantics | literal | `explicit_trade_date` |
| volume_unit | literal | `shares` |
| amount_unit | literal | `CNY` |
| adjustment_semantics | literal | unadjusted OHLCV plus `backAdjustFactor` |
| admission_state | literal | R2-F2 only `qualified` for existing primary; no secondary |

### Evidence manifest

| Field | Type | Constraint |
| --- | --- | --- |
| evidence_id | safe identifier | Deterministic content/request ID |
| provider_id | ProviderId | `baostock` |
| adapter_version | safe version | Required |
| endpoint_contract_version | safe version | Required |
| trade_date / universe_id | date / safe identifier | Exact session scope |
| requested_at / completed_at / observed_at | UTC datetime | Required, ordered |
| request_count / retry_count | non-negative int | No payload |
| endpoint_batches | tuple[RawEndpointBatch, ...] | Six endpoint IDs only |
| source_schema / units / date_semantics | bounded metadata | Allowlisted fields/values |
| row_count / source_symbol_count | non-negative int | Readback exact |
| object_relative_path | relative path | Evidence root only |
| object_sha256 | 64 lowercase hex | Content binding |
| compression | literal | `parquet_internal` only |
| failure_class / provider_code | allowlisted/validated | No raw message |

### Candidate gate report

| Field | Type | Constraint |
| --- | --- | --- |
| gate_report_id | safe identifier | Immutable |
| candidate_id | safe identifier | One candidate |
| gate_version | safe version | Pinned policy |
| gate_name | allowlisted literal | schema/date/universe/coverage/semantic/factor/suspension/hash |
| verdict | `pass` or `fail` | Required |
| metrics | bounded scalar map | No payload/path/exception |
| failure_class | allowlisted optional | Sanitized |
| evidence_sha256 | hash | Exact input |
| created_at | UTC datetime | Required |

### Candidate manifest

| Field | Type | Constraint |
| --- | --- | --- |
| candidate_id | safe identifier | Full-session identity |
| trade_date / universe_id | date / safe identifier | Exact scope |
| provider_id | ProviderId | One provider only |
| evidence_id / evidence_sha256 | ID/hash | Exact evidence |
| normalized_object_relative_path / sha256 | relative path/hash | Non-serving candidate Parquet |
| gate_report_relative_path / sha256 | relative path/hash | Complete gate lineage |
| adapter_version / schema_version | safe version | Exact contract |
| row_count / required_symbol_count | non-negative int | Exact readback |
| status | `accepted` / `rejected` | Rejected candidates retained |

### Session selection

| Field | Type | Constraint |
| --- | --- | --- |
| selection_id | safe identifier | Immutable |
| trade_date / universe_id | date / safe identifier | Exact canonical partition |
| selected_candidate_id | safe identifier | Exactly one complete candidate |
| selected_provider_id | ProviderId | `baostock` in R2-F2 |
| reason | `primary_ready` / reserved `qualified_fallback` | R2-F2 writes only primary_ready |
| fallback_from | nullable ProviderId | Must be null in R2-F2 |
| evidence_sha256 / candidate_manifest_sha256 / gate_report_sha256 | hashes | Complete lineage |
| selected_at | UTC datetime | Required |

### Additive canonical manifest lineage

New file entries MAY carry these fields while retaining the current dataset manifest format and all
legacy fields: `provider_id`, `universe_id`, `evidence_id`, `evidence_sha256`, `candidate_id`,
`candidate_manifest_sha256`, `gate_report_sha256`, `adapter_version`, `provider_schema_version`.
Legacy entries without them decode as legacy BaoStock provenance in memory. A new writer MUST NOT
invent lineage for a legacy entry or rewrite its Parquet bytes.

## Exact BaoStock semantics

- `trade_dates` provides the explicit source calendar date; R2-F2 does not infer dates from arrival
  time or local weekdays.
- `all_stock` provides the source universe/status snapshot. Required symbols and indexes are
  checked against the declared `universe_id`; unknown or duplicate symbols fail.
- `daily_astock` supplies unadjusted daily OHLCV/amount with `adjustflag=3` and explicit date.
- `daily_factor`/`adjust_factor` supplies `backAdjustFactor` by symbol and effective date; a
  factor is selected only when its effective date is on or before the requested trade date.
- `index_history` supplies required index rows; index rows do not receive a stock adjustment
  factor and are not allowed to masquerade as stock rows.
- Source symbols retain the `sh.`/`sz.` six-digit form and canonical symbols are lowercase.
- A suspended stock is a legal placeholder only under the existing F0 semantics. This contract
  does not loosen active-row price, activity, factor, duplicate or coverage gates.

## Acceptance criteria

### AC-1: Static provider contract (FR-1–FR-3, NFR-11, NFR-14)

Given an exact BaoStock provider request
When the compatibility adapter is constructed
Then only the allowlisted ID is accepted, the incumbent call path is invoked once, and endpoint,
retry, timeout, provider-code and circuit observations remain unchanged.

### AC-2: Source rows precede normalization (FR-4, FR-9)

Given a fake BaoStock response with source-shaped rows
When the online candidate path runs
Then source rows are captured and atomically published before normalization, and the normalizer
reads through the published evidence reader rather than the live response object.

### AC-3: Schema and semantic allowlist (FR-5, FR-20, FR-21, FR-22, NFR-9)

Given an unknown field, row-shape mismatch, date mismatch, invalid unit, duplicate symbol, active
missing factor, invalid suspended placeholder or suspended index
When an evidence/candidate gate evaluates
Then the complete candidate fails closed with a sanitized reason and no canonical mutation.

### AC-4: Content-addressed evidence (FR-6–FR-8, NFR-2, NFR-3, NFR-4, NFR-6)

Given identical source rows and metadata
When evidence is published twice
Then the object/hash/manifest identity is idempotent; changed bytes create a distinct immutable
object; a crash or partial rename exposes no published partial object.

### AC-5: Evidence lineage binding (FR-7, FR-14, FR-18)

Given a manifest with a wrong hash, row count, schema, provider, universe, relative path, adapter
version or gate hash
When the evidence/candidate reader validates it
Then it rejects the complete candidate before normalization or pointer movement.

### AC-6: Offline replay (FR-10–FR-11, FR-25, NFR-1, NFR-12)

Given a valid local evidence object
When `market-provider-replay` runs
Then it performs zero network calls and zero canonical writes and produces deterministic byte or
semantic-equivalence output. A second run is identical.

### AC-7: Replay corruption and nondeterminism (FR-9, FR-11, FR-23–FR-25)

Given a corrupt Parquet/decompression stream, missing object, symlink, object substitution, manifest
swap or a deliberately nondeterministic row order
When replay runs
Then it returns a sanitized failure, writes nothing and leaves the prior canonical pointer and
objects unchanged.

### AC-8: Gate auditability (FR-12, FR-13, FR-14, NFR-8)

Given a candidate that passes or fails any gate
When the gate completes
Then an immutable gate report and candidate manifest record the verdict, bounded metrics and exact
input hashes without payload, secret, URL, path or raw exception.

### AC-9: Single-session selection (FR-12, FR-15–FR-16, NFR-5, NFR-10)

Given one complete BaoStock candidate and no admitted secondary
When selection runs
Then exactly one `primary_ready` selection is recorded for the session; no symbol-level mixing,
fallback or second provider call occurs.

### AC-10: Canonical publication lineage (FR-18–FR-19, FR-23, NFR-7)

Given a complete candidate and selection record
When the existing publication function runs
Then the new canonical manifest entry carries all required lineage hashes and the pointer moves
only through the existing atomic chain. Failed selection leaves the prior pointer unchanged.

### AC-11: Legacy compatibility (FR-17, FR-24, FR-26, NFR-7)

Given legacy BaoStock rows, manifests, refresh rows and API fixtures lacking R2-F2 metadata
When current readers load them
Then they decode as legacy BaoStock in memory, all bytes remain unchanged, and existing market,
analysis, alert and user response contracts remain valid.

### AC-12: Read-only status and plan (FR-25, NFR-12)

Given missing/corrupt evidence or candidate roots
When existing GET or plan/replay reads execute
Then they report unavailable/error with an allowlisted reason and filesystem, database schema,
manifest and pointer fingerprints are identical before and after.

### AC-13: Bounds and security (FR-7, FR-24, NFR-3, NFR-4, NFR-8)

Given an oversized metadata/object, path traversal, symlink, secret-like field, URL, token,
cookie, authorization header or raw exception
When serialization or readback runs
Then validation rejects it before publish and no forbidden value is emitted.

### AC-14: Crash/concurrency/TOCTOU (NFR-2, NFR-3, NFR-5)

Given two writers or a mutation during object/manifest read
When both attempt publication or one swaps a file
Then exactly one complete lineage can win, the other fails closed, and no partial or substituted
object reaches normalization or canonical publication.

### AC-15: R2-F3 boundary (FR-16, FR-27, NFR-1, NFR-14)

Given an attempted second provider, dynamic plugin, shadow schedule, 20-session qualification or
automatic failover
When the R2-F2 code path is exercised
Then it is unavailable/out of scope and no second-source or production request is made.

## Edge cases

- EC-1: Empty provider ID, invalid slug, dynamic import request or arbitrary registry value
  fails before transport.
- EC-2: Provider endpoint returns an unknown field, duplicate field, wrong row length or
  malformed type; the complete raw batch is rejected.
- EC-3: Provider returns a date outside the exact requested session or rows from two sessions;
  evidence is not published.
- EC-4: Provider status code is unknown; preserve bounded `provider_code` and map only to
  `UNKNOWN_PROVIDER_PROTOCOL_ERROR`.
- EC-5: Transport emits short header, EOF, bad compression, send error, receive timeout or
  pagination stall; no evidence/candidate is published and existing circuit semantics apply.
- EC-6: Evidence object exceeds 64 MiB, metadata exceeds 1 MiB or decompression expands beyond
  the bound; reject before normalization.
- EC-7: Evidence object is a symlink, path traversal, absolute path, directory or substituted
  inode; descriptor-bound validation rejects it.
- EC-8: Evidence manifest is atomically swapped during read; snapshot identity/fingerprint
  mismatch makes the read unavailable.
- EC-9: Evidence object is deleted, renamed, truncated or hash-mutated after manifest commit;
  replay fails and pointer is unchanged.
- EC-10: Candidate normalized object has duplicate/missing/unexpected symbols, wrong row count,
  mixed source, wrong date or wrong universe; whole candidate fails.
- EC-11: Gate report is missing, altered, oversize or references another evidence hash;
  selection is forbidden.
- EC-12: Two processes publish the same request key; one complete artifact lineage wins and the
  loser records no canonical selection.
- EC-13: Crash before object rename, after object rename, before evidence manifest, after
  evidence manifest, before candidate manifest or before selection; recovery sees only complete
  immutable states and never refetches solely because a complete evidence object exists.
- EC-14: Replay sees non-deterministic row order or timestamp generation; deterministic contract
  check fails rather than emitting a different candidate.
- EC-15: Legacy manifest lacks lineage fields or has source omitted; decode as legacy BaoStock
  without rewriting it.
- EC-16: New manifest lineage has mismatched provider, adapter/schema, universe, evidence,
  candidate or gate hash; strict R2-F2 read fails closed.
- EC-17: A suspended stock lacks factor and has blank activity; legal placeholder passes only
  with exact existing quality semantics.
- EC-18: An active stock lacks factor, a suspended row has non-zero activity, or a suspended
  index appears; candidate fails.
- EC-19: Replay CLI receives credentials, URL, path or provider arguments; parser rejects them
  and emits no sensitive value.
- EC-20: Missing evidence root/database/table on GET/plan/replay; return unavailable and do not
  initialize directories, databases, tables or pointers.

## Out of scope

- OS-1: TickFlow, Tushare, AKShare, EastMoney, mootdx/TDX, AData or any second-source adapter;
  deferred to R2-F3 and requires separate terms/credential/qualification review.
- OS-2: Provider shadow scheduling, 20 consecutive trading-day qualification and reconciliation
  against another source; R2-F3.
- OS-3: Automatic or manual production failover, `qualified_fallback` selection, symbol-level
  mixing or source switching; R2-F4 after qualification.
- OS-4: Changes to BaoStock transport endpoint identifiers, socket patch, retry/timeout policy,
  circuit thresholds/state semantics or provider health storage.
- OS-5: Changes to Normalize, existing quality gates, canonical Parquet schema, SHA-256
  algorithm, current pointer format or atomic publish primitives except additive lineage hooks.
- OS-6: Real Provider/NAS/network requests, installation, LaunchAgent loading, production
  refresh, production database mutation or external communication.
- OS-7: Intraday/tick data, broker integration, auto-trading, fund-flow inference or point-in-
  time financial statement/event ingestion.
- OS-8: Rewriting old manifests, Parquet, refresh rows, analysis/alert/user JSON or backfilling
  provenance that cannot be proven from existing bytes.

## Rollback and compatibility

The runtime switch for this version is an explicit BaoStock-only compatibility mode. If R2-F2 is
rolled back, the orchestrator MUST stop consuming new candidate/selection records, use the last
legacy-compatible canonical pointer, and continue serving legacy rows/readers. Evidence, rejected
candidates, gate reports and new non-serving audit objects are retained for investigation; no
manual Parquet, manifest or pointer edit is permitted. The R2-F1 repair switch remains
`STOCK_EVA_MARKET_REPAIR_ENABLED=false` independently.

Rollback is a code/runtime selection decision, not deletion or downgrade of immutable objects.
Before any later production action, the installed release and pointer must be independently read
back under a new authorization window.

## Threat model and trust boundary

The evidence root is local application-owned storage. The threat model covers accidental or
malicious path traversal, symlink substitution, object replacement, manifest swap, partial writes,
TOCTOU mutation, oversized/decompression-bomb content, malformed provider rows and leakage through
logs/status/CLI. Descriptor-bound no-follow reads, bounded metadata/object sizes, canonical JSON,
content hashes, row/schema checks and atomic rename are mandatory defenses.

The model does not claim protection against a same-UID process that can continuously mutate the
application's private staging inode outside the application lock, filesystem corruption beyond
hash detection, a compromised Python runtime, or a compromised host. Such conditions fail closed
when detected and require manual audit. Credentials are supplied only to the incumbent provider
transport at runtime and are never part of evidence or public state.

## Review gate

Status remains **In Review** until an independent reviewer checks every FR/NFR/AC/EC, verifies the
exact file whitelist and confirms that no implementation begins before approval. Any High or
Medium finding requires a spec amendment and a new review before RED. After approval, the linear
implementation plan below is authoritative for Tasks 7–9.
