# Stock EVA R2-F2 Provider Evidence Framework Design

**Author:** Codex delivery team — specification owner
**Date:** 2026-08-21 (Asia/Shanghai)
**Status:** In Review — six specification findings amended; independent re-review required
**Decision authority:** User approved starting R2-F2 on 2026-08-21; implementation still requires
  this specification to be independently reviewed and approved.
**Scope:** R2-F2 offline code and synthetic tests only; one BaoStock compatibility adapter.
**Baseline:** branch `codex/r2-f2-provider-evidence`, exact clean HEAD
`90680832a3b2d9c876b7917a88ad0213236250ab`
**Reviewers:** Independent code/spec review found six Medium specification gaps; this amendment
closes them for re-review. Status MUST NOT be changed to Approved by this fix.

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

## Specification review closure (authoritative amendment before RED)

The first independent review was correctly **NO-GO**. The six Medium findings were specification
gaps, not permission to narrow the product boundary. Their confirmed root causes and closures are:

| Finding | Confirmed root cause | Normative closure in this revision |
| --- | --- | --- |
| M1 — type contract incomplete | The earlier shorthand used `SafeIdentifier`, an undefined `RawEndpointBatch` and open mappings, so a writer could accept arbitrary provider/symbol/path/row shapes. | Freeze the complete frozen, `extra="forbid"`, UTC-aware model set in **Frozen type vocabulary** and **Frozen in-memory contracts** below. |
| M2 — endpoint schema under-specified | The earlier prose named six calls but did not freeze their actual call-site field order, variants, empty rules, units or ordering. | Freeze the exact fields observed/required by `baostock.py`, `DAILY_FIELDS`, `AdjustmentFactorCache.FACTOR_FIELDS` and existing fixtures in **Exact BaoStock endpoint contract**; no union/`Any` fallback. |
| M3 — manifest cardinality/plan ambiguous | One object and a free-form `endpoint_batches` tuple could swallow multiple symbols, pages or attempts and had no expected-plan hash. | Make `EvidenceManifest.objects` an ordered tuple of one descriptor per logical request/page/shard, bind an immutable expected request plan/hash and enforce exact coverage/cardinality. |
| M4 — transport lineage not cryptographically bound | IDs were named but no persisted object was required to bind them to the sanitized F0.1 observation. | Add `TransportLineageRef.observation_digest`, require one ref per endpoint/attempt/page and hash the canonical allowlisted observation projection only. |
| M5 — gate cardinality unclear | The previous `Candidate gate report` table described one gate row, while ACs required a complete report; missing/duplicate gates had no explicit fail-closed rule. | Freeze one canonical ordered `CandidateGateReport` aggregate containing the exact gate list, per-gate `GateOutcome` records and aggregate hash; `CandidateManifest` may reference exactly one report. |
| M6 — storage/replay/traceability boundary incomplete | Defaults, no-clobber/CAS order, replay clock/hash rules, qualified-fallback rejection and validator scope were distributed or implicit. | Freeze settings/layout, macOS-safe primitives, lock/CAS publication order, injected normalization clock, byte/semantic replay rules, explicit fallback rejection and manual traceability gates in this revision. |

The amendment is still documentation-only. No RED test, provider request, external operation or
production mutation may begin until the next independent review changes this document to
**Approved**.

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
- FR-28: **Frozen type closure:** The implementation MUST expose the complete `SafeProviderId`,
  `SafeSymbol`, `SafeVersion`, `ProviderEndpoint`, `ProviderRequest`, `TransportLineageRef`,
  discriminated raw-row/batch, evidence, gate, candidate, selection and replay models with
  immutable `extra="forbid"` validation. Any missing/extra/duplicate field MUST fail closed.
- FR-29: **Exact endpoint schema closure:** Each of the six endpoint IDs MUST use one of the
  explicitly named request/schema variants and exact ordered field tuples in this specification;
  an unobserved field, inferred unit, unknown null rule or unclassified same-endpoint variant MUST
  invalidate the complete batch.
- FR-30: **Request-plan cardinality:** Every logical request, symbol/index shard and SDK page MUST
  have exactly one ordered plan item, object descriptor and transport-lineage record. The expected
  plan and its hash MUST be verified before evidence, candidate or selection publication.
- FR-31: **Transport binding:** Every published object MUST bind refresh/session/request/endpoint/
  attempt/page to a SHA-256 digest of the sanitized F0.1 observation projection. Payload, URL,
  token, header, cookie and raw provider exception/message MUST remain impossible to persist.
- FR-32: **Gate aggregate:** Candidate publication MUST create exactly one ordered
  `CandidateGateReport` containing the complete `R2F2_GATE_ORDER`; missing, extra, duplicate or
  out-of-order outcomes MUST fail closed. A candidate manifest MUST point to one aggregate hash,
  not a loose set of gate rows.
- FR-33: **Clock/replay determinism:** The online and replay paths MUST inject one
  `normalization_clock_utc`, exclude replay-start time from candidate hashes and report both byte
  and semantic comparison according to the frozen equivalence rules.

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
- **NFR-15 (Path/layout):** Evidence settings, roots, ancestors, directory names and relative
  object paths MUST use the fixed bounded layout and descriptor-bound no-follow checks; missing
  read roots MUST remain zero-write.
- **NFR-16 (CAS/publication order):** Evidence compare-create, `RefreshRunLock`, selection,
  canonical manifest and pointer operations MUST follow the frozen order and no-clobber rules;
  a competing or stale writer MUST fail closed without canonical writes.
- **NFR-17 (Traceability):** Every EC-3/4/12/16/17/18 and every FR/NFR/AC MUST map to a named
  pytest or an explicit static/diff proof in the implementation plan. No validator score may be
  substituted for this manual traceability.
- **NFR-18 (Fallback prohibition):** `qualified_fallback` MAY exist only as a future-compatible
  enum value. R2-F2 writers, orchestrators and validators MUST reject it; no second provider,
  plugin, shadow or failover path may be present.
- **NFR-13 (Reviewability):** Each Task 7–9 commit MUST have focused RED/GREEN evidence, `ruff`,
  `git diff --check`, an independent High/Medium review and a bounded file whitelist.
- **NFR-14 (Provider admission):** R2-F2 may write only BaoStock identity and must not dynamically
  import or register an unreviewed provider.

## API contracts

These are internal Python contracts unless explicitly marked CLI. They are frozen for R2-F2 before
implementation. The complete model definitions are in **Frozen type vocabulary** and
**Frozen in-memory contracts** below; this section fixes the call boundary and the only public
replay response.

```python
class BaoStockDailyBarAdapter(Protocol):
    provider_id: Literal["baostock"]
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion

    def fetch_raw(self, request: ProviderRequest) -> ProviderRawBatch: ...
    def normalize(self, evidence: PublishedEvidence, *, clock: datetime) -> tuple[DailyBar, ...]: ...

class EvidenceStore(Protocol):
    def publish(self, batch: ProviderRawBatch) -> EvidenceManifest: ...
    def read(self, evidence_id: SafeIdentifier) -> PublishedEvidence: ...
    def replay(
        self, evidence_id: SafeIdentifier, *, compare_candidate_sha: SafeSha256 | None = None
    ) -> ReplayResult: ...
```

`fetch_raw` MUST return all six endpoint variants required by the exact request plan or a complete
sanitized failure; it MUST NOT return a partial batch marked successful. `normalize` MUST accept
only a descriptor/fingerprint/hash-verified `PublishedEvidence` reader and the already-frozen
`normalization_clock_utc`; a live SDK result is not a valid argument. `EvidenceStore` is a local
writer/reader boundary and never a network client.

CLI contract:

```text
market-provider-replay --evidence-id ID [--compare-candidate-sha SHA]
```

The parser accepts only the evidence ID and optional SHA-256 comparison. It rejects credentials,
URLs, local paths, provider names, arbitrary flags and unknown fields before any root access. It
returns exactly the sanitized `ReplayResult` projection (`status`, `evidence_id`, candidate/semantic
hashes and match booleans, normalization clock, bounded row/date fields and allowlisted failure
class). `replay_started_at` is not returned as candidate evidence and is never hashed. Missing or
corrupt evidence exits non-zero with an allowlisted `EVIDENCE_*`/`REPLAY_*` reason and zero writes.

No new HTTP write endpoint is introduced. Existing `GET /api/v1/market/status`, summary and
history contracts remain read-only and compatible; optional lineage fields are additive and never
required by legacy clients.

### Transport observation binding

R2-F0.1 already emits `TransportObservation` with the fixed fields
`refresh_id/provider_session_id/request_id/provider_id/endpoint/attempt/page/protocol_stage/
elapsed_ms/recv_calls/response_bytes/end_marker_seen/provider_code/normalized_error/outcome/
observed_at`. R2-F2 MUST retain those fields only through the sanitized model and compute
`observation_digest = SHA256(canonical_json(observation_without_digest))`. The canonical projection
contains no provider message, exception, URL, token, socket payload or credential. A successful
page descriptor MUST reference one successful terminal observation with matching endpoint,
attempt/page and IDs; a failed request is retained only as sanitized failure/lineage evidence and
cannot produce a published object. A digest mismatch, missing observation, duplicate lineage key or
observation from a different refresh/session/request invalidates the complete candidate and leaves
the canonical chain untouched.

```typescript
interface ProviderReplayResponse {
  status: "ready" | "unavailable" | "error";
  evidence_id: string;
  candidate_sha256: string | null;
  semantic_hash: string | null;
  byte_match: boolean | null;
  semantic_match: boolean | null;
  normalization_clock_utc: string | null;
  replay_started_at: string;
  failure_class: string | null;
  row_count: number;
  trade_date: string | null;
}
```

## Data models

All persisted JSON uses canonical UTF-8, sorted keys and compact separators. All timestamps are
UTC-aware. `SafeIdentifier` is bounded ASCII `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`; paths are
relative, non-absolute, traversal-free and never symlinks.

### Frozen type vocabulary

The following definitions are normative and supersede the earlier shorthand types. They are
Python/Pydantic contracts, not an invitation to use `dict[str, object]`, `Any`, an arbitrary enum
value or a dynamically loaded provider. Every model below MUST use
`ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)` (or the equivalent immutable
dataclass/enum enforcement), and every nested model MUST use the same policy.

```python
SafeProviderId = Annotated[
    str, StringConstraints(min_length=1, max_length=32,
                           pattern=r"^[a-z][a-z0-9-]{0,31}$")
]
SafeSymbol = Annotated[
    str, StringConstraints(min_length=9, max_length=9,
                           pattern=r"^(sh|sz)\.[0-9]{6}$")
]
SafeVersion = Annotated[
    str, StringConstraints(min_length=1, max_length=64,
                           pattern=r"^[A-Za-z0-9][A-Za-z0-9._+:-]{0,63}$")
]
SafeIdentifier = Annotated[
    str, StringConstraints(min_length=1, max_length=128,
                           pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
]
SafeSha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SafeRelativePath = Annotated[
    str, StringConstraints(min_length=1, max_length=512,
                           pattern=r"^[A-Za-z0-9._/-]+$")
]
BoundedToken = Annotated[
    str, StringConstraints(max_length=64, pattern=r"^[A-Za-z0-9._:-]*$")
]
BoundedText = Annotated[str, StringConstraints(max_length=256)]

class ProviderEndpoint(StrEnum):
    TRADE_DATES = "trade_dates"
    ALL_STOCK = "all_stock"
    DAILY_ASTOCK = "daily_astock"
    DAILY_FACTOR = "daily_factor"
    ADJUST_FACTOR = "adjust_factor"
    INDEX_HISTORY = "index_history"

class ProviderId(StrEnum):
    BAOSTOCK = "baostock"

class SelectionReason(StrEnum):
    PRIMARY_READY = "primary_ready"
    QUALIFIED_FALLBACK = "qualified_fallback"  # schema-reserved; writer-rejected in R2-F2

class GateName(StrEnum):
    TRANSPORT_COMPLETE = "transport_complete"
    SCHEMA = "schema"
    DATE = "date"
    UNIVERSE = "universe"
    COVERAGE = "coverage"
    SEMANTIC = "semantic"
    FACTOR = "factor"
    SUSPENSION = "suspension"
    EVIDENCE_HASH = "evidence_hash"
    DETERMINISM = "determinism"

SafeFailureClass = Literal[
    "transport_timeout", "transport_connect", "dns", "auth", "rate_limit",
    "provider_4xx", "provider_5xx", "schema", "semantic", "calendar", "universe",
    "coverage", "reconciliation", "storage", "internal",
    "CONNECT_ERROR", "SEND_ERROR", "RECV_TIMEOUT", "EOF", "SHORT_HEADER",
    "BAD_COMPRESSION", "PROTOCOL_ERROR", "PAGINATION_STALLED", "RATE_LIMIT",
    "UNKNOWN_PROVIDER_PROTOCOL_ERROR",
    "EVIDENCE_ROOT_UNAVAILABLE", "EVIDENCE_SCHEMA_MISMATCH", "EVIDENCE_HASH_MISMATCH",
    "EVIDENCE_OBJECT_OVERSIZE", "EVIDENCE_UNSAFE_PATH", "EVIDENCE_MANIFEST_INVALID",
    "REPLAY_NONDETERMINISTIC", "REPLAY_SEMANTIC_MISMATCH",
]

R2F2_GATE_ORDER: tuple[GateName, ...] = (
    GateName.TRANSPORT_COMPLETE, GateName.SCHEMA, GateName.DATE,
    GateName.UNIVERSE, GateName.COVERAGE, GateName.SEMANTIC,
    GateName.FACTOR, GateName.SUSPENSION, GateName.EVIDENCE_HASH,
    GateName.DETERMINISM,
)
```

`SafeProviderId` is the lexical safety type; the R2-F2 registry additionally requires the value
to be exactly `ProviderId.BAOSTOCK`. `SafeSymbol` is lowercase and must be sorted lexicographically
in every tuple. `SafeVersion` identifies a pinned code/schema contract, not a package URL. A
`SafeRelativePath` is resolved only beneath its already-opened root descriptor; lexical validation
alone is never treated as a TOCTOU defense. No persisted model may contain a URL, credential,
token, header, cookie, payload, absolute path or arbitrary exception/message field.

### Frozen in-memory contracts

The following is the complete contract surface. `Field(ge=0, le=...)` bounds are mandatory where
shown; omitted collection bounds are fixed at implementation time by the constants in the table
below and an over-bound value is rejected before any write.

```python
class ProviderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    provider_id: Literal["baostock"]
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier
    request_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    symbols: tuple[SafeSymbol, ...]  # sorted, unique, non-empty
    request_plan: "ExpectedRequestPlan"

class TransportLineageRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier
    request_id: SafeIdentifier
    endpoint: ProviderEndpoint
    attempt: int = Field(ge=1, le=20)
    page: int = Field(ge=1, le=16384)
    observation_digest: SafeSha256

class RawEndpointRowBase(BaseModel):
    """Discriminated union root; concrete variants are below, never an open mapping."""
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    endpoint: ProviderEndpoint
    schema_variant: SafeVersion

class TradeDatesRow(RawEndpointRowBase):
    endpoint: Literal[ProviderEndpoint.TRADE_DATES]
    schema_variant: Literal["trade_dates.v1"]
    calendar_date: date
    is_trading_day: BoundedToken

class AllStockRow(RawEndpointRowBase):
    endpoint: Literal[ProviderEndpoint.ALL_STOCK]
    schema_variant: Literal["all_stock.market.v1"]
    code: SafeSymbol
    tradeStatus: BoundedToken
    code_name: BoundedText

class DailyAStockRow(RawEndpointRowBase):
    endpoint: Literal[ProviderEndpoint.DAILY_ASTOCK]
    schema_variant: Literal["daily_astock.v1"]
    date: date
    code: SafeSymbol
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    preclose: Decimal
    volume: Decimal | None
    amount: Decimal | None
    adjustflag: Literal["3"]
    turn: Decimal | None
    tradestatus: BoundedToken
    pctChg: Decimal | None
    isST: BoundedToken

class FactorFields(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    code: SafeSymbol
    dividOperateDate: date
    foreAdjustFactor: Decimal
    backAdjustFactor: Decimal
    adjustFactor: Decimal

class DailyFactorRow(RawEndpointRowBase, FactorFields):
    endpoint: Literal[ProviderEndpoint.DAILY_FACTOR]
    schema_variant: Literal["daily_factor.v1"]

class AdjustFactorRow(RawEndpointRowBase, FactorFields):
    endpoint: Literal[ProviderEndpoint.ADJUST_FACTOR]
    schema_variant: Literal["adjust_factor.session.v1"]

class IndexHistoryFields(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    endpoint: Literal[ProviderEndpoint.INDEX_HISTORY]
    date: date
    code: SafeSymbol
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    preclose: Decimal
    volume: Decimal | None
    amount: Decimal | None
    adjustflag: Literal["3"]
    turn: Decimal | None
    tradestatus: BoundedToken
    pctChg: Decimal | None
    isST: BoundedToken

class IndexHistorySessionRow(RawEndpointRowBase, IndexHistoryFields):
    endpoint: Literal[ProviderEndpoint.INDEX_HISTORY]
    schema_variant: Literal["index_history.session.v1"]

class IndexHistoryRangeRow(RawEndpointRowBase, IndexHistoryFields):
    endpoint: Literal[ProviderEndpoint.INDEX_HISTORY]
    schema_variant: Literal["index_history.range.v1"]

RawEndpointRow = Annotated[
    TradeDatesRow | AllStockRow | DailyAStockRow | DailyFactorRow | AdjustFactorRow
    | IndexHistorySessionRow | IndexHistoryRangeRow,
    Field(discriminator="schema_variant"),
]

class RawEndpointBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    endpoint: ProviderEndpoint
    schema_variant: SafeVersion
    plan_ordinal: int = Field(ge=0, le=4096)
    shard_id: SafeIdentifier
    lineage: TransportLineageRef
    rows: tuple[RawEndpointRow, ...]
    row_count: int = Field(ge=0, le=10_000_000)
    provider_row_order_digest: SafeSha256

class ProviderRawBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    provider_id: Literal["baostock"]
    request: ProviderRequest
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion
    started_at: datetime
    completed_at: datetime
    normalization_clock_utc: datetime
    request_plan: "ExpectedRequestPlan"
    endpoint_batches: tuple[RawEndpointBatch, ...]
    transport_lineage: tuple[TransportLineageRef, ...]
    failure_class: SafeFailureClass | None = None

class EvidenceObjectDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    object_id: SafeIdentifier
    object_kind: Literal["raw_endpoint_page"]
    relative_path: SafeRelativePath
    sha256: SafeSha256
    schema_hash: SafeSha256
    row_count: int = Field(ge=0, le=10_000_000)
    byte_count: int = Field(ge=0, le=67_108_864)
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    provider_id: Literal["baostock"]
    universe_id: SafeIdentifier
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier
    request_id: SafeIdentifier
    endpoint: ProviderEndpoint
    shard_id: SafeIdentifier
    attempt: int = Field(ge=1, le=20)
    page: int = Field(ge=1, le=16_384)
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion
    schema_variant: SafeVersion
    normalization_clock_utc: datetime
    transport_observation_digest: SafeSha256

class EvidenceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    evidence_id: SafeIdentifier
    provider_id: Literal["baostock"]
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion
    trade_date: date
    universe_id: SafeIdentifier
    requested_at: datetime
    completed_at: datetime
    normalization_clock_utc: datetime
    expected_request_plan: "ExpectedRequestPlan"
    expected_request_plan_sha256: SafeSha256
    objects: tuple[EvidenceObjectDescriptor, ...]  # ordered; one request/page/shard per item
    transport_lineage: tuple[TransportLineageRef, ...]
    object_count: int = Field(ge=0, le=4096)
    row_count: int = Field(ge=0, le=10_000_000)
    manifest_sha256: SafeSha256

class PublishedEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    manifest: EvidenceManifest
    descriptors: tuple[EvidenceObjectDescriptor, ...]
    reader_identity: SafeSha256

class GateOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    gate_name: GateName
    gate_version: SafeVersion
    verdict: Literal["pass", "fail"]
    bounded_metrics: tuple[tuple[SafeIdentifier, int | float | bool | BoundedText | None], ...]
    failure_class: SafeFailureClass | None = None
    referenced_hashes: tuple[SafeSha256, ...]

class CandidateGateReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    report_id: SafeIdentifier
    candidate_id: SafeIdentifier
    gate_version: SafeVersion
    outcomes: tuple[GateOutcome, ...]  # exactly R2F2_GATE_ORDER, no missing/extra/duplicate
    aggregate_verdict: Literal["pass", "fail"]
    aggregate_sha256: SafeSha256
    created_at: datetime

class CandidateManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    candidate_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    provider_id: Literal["baostock"]
    evidence_id: SafeIdentifier
    evidence_manifest_sha256: SafeSha256
    normalized_object_relative_path: SafeRelativePath
    normalized_object_sha256: SafeSha256
    gate_report_relative_path: SafeRelativePath
    gate_report_sha256: SafeSha256
    adapter_version: SafeVersion
    source_schema_version: SafeVersion
    row_count: int = Field(ge=0, le=10_000_000)
    required_symbol_count: int = Field(ge=1, le=10_000_000)
    status: Literal["accepted", "rejected"]
    manifest_sha256: SafeSha256

class SessionSelection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    selection_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    selected_candidate_id: SafeIdentifier
    selected_provider_id: Literal["baostock"]
    reason: SelectionReason
    fallback_from: Literal["baostock"] | None
    evidence_manifest_sha256: SafeSha256
    candidate_manifest_sha256: SafeSha256
    gate_report_sha256: SafeSha256
    selected_at: datetime
    selection_sha256: SafeSha256

class ReplayResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    status: Literal["ready", "unavailable", "error"]
    evidence_id: SafeIdentifier
    candidate_sha256: SafeSha256 | None
    semantic_hash: SafeSha256 | None
    byte_match: bool | None
    semantic_match: bool | None
    normalization_clock_utc: datetime | None
    replay_started_at: datetime  # diagnostic only; excluded from every candidate/selection hash
    row_count: int = Field(ge=0, le=10_000_000)
    trade_date: date | None
    failure_class: SafeFailureClass | None
```

`SafeFailureClass` is an existing allowlist (`MarketFailureClass`, `NormalizedTransportError` and
the explicit `EVIDENCE_*`/`REPLAY_*` public categories); it is not a free-form string. All datetime
validators reject naive values and normalize to UTC. Tuple validators enforce sorted/unique
identifiers, exact counts and cross-object equality. The implementation MUST reject any model
that attempts to serialize a payload, exception, URL, path outside the declared relative field,
or a `qualified_fallback` selection.

`ExpectedRequestPlan` is also frozen and explicit:

```python
class ExpectedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    ordinal: int = Field(ge=0, le=4096)
    endpoint: ProviderEndpoint
    schema_variant: SafeVersion
    shard_id: SafeIdentifier
    symbols: tuple[SafeSymbol, ...]
    start_date: date
    end_date: date
    expected_pages: tuple[int, ...]  # non-empty, sorted, unique; actual pages must equal this

class ExpectedRequestPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    requests: tuple[ExpectedRequest, ...]  # exact six-endpoint plan for the session
    request_count: int = Field(ge=1, le=4096)
    plan_sha256: SafeSha256
```

The plan includes all symbol shards and explicit index requests. It is canonicalized before fetch;
`plan_sha256` is SHA-256 of the canonical JSON representation without `plan_sha256` itself. The
manifest's `expected_request_plan_sha256` MUST equal it, and every request ordinal, shard, page,
endpoint, schema variant and symbol set MUST have exactly one corresponding object/lineage record.
Multi-symbol, multi-index and multi-page responses therefore cannot be hidden in one object.

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
| expected_request_plan / hash | `ExpectedRequestPlan` / SHA-256 | Exact six-endpoint request/shard/page cardinality |
| objects | `tuple[EvidenceObjectDescriptor, ...]` | Ordered one-object-per-logical-request/page/shard; no object may swallow multiple pages/symbol shards |
| transport_lineage | `tuple[TransportLineageRef, ...]` | Every attempt/page bound to sanitized F0.1 observation digest |
| source_schema / units / date_semantics | bounded metadata | Allowlisted fields/values |
| row_count / object_count | non-negative int | Readback exact and equal to descriptor sums |
| object_relative_path / object_sha256 | relative path / 64 lowercase hex | Per-object content binding; path is evidence root only |
| compression | literal | `parquet_internal` only |
| failure_class / provider_code | allowlisted/validated | No raw message |

### Candidate gate report

The following aggregate is the only persisted gate contract. The row-oriented table is a field
summary; it MUST NOT be implemented as one file per loose gate or as a free-form list.

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

The aggregate MUST contain exactly the ordered names in `R2F2_GATE_ORDER` (11 outcomes), exactly
one outcome per name, and `aggregate_sha256` is the canonical JSON hash of the report without that
field. `aggregate_verdict` is `pass` only when every outcome is `pass`; a missing, extra, duplicate,
out-of-order or hash-mismatched outcome is a fail-closed report and cannot be selected.

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
| reason | `SelectionReason` | R2-F2 writer/orchestrator/validator accepts only `primary_ready`; `qualified_fallback` is schema-reserved and MUST be rejected |
| fallback_from | nullable `Literal["baostock"]` | Must be null in R2-F2; any non-null or fallback reason fails |
| evidence_sha256 / candidate_manifest_sha256 / gate_report_sha256 | hashes | Complete lineage |
| selected_at | UTC datetime | Required |

### Additive canonical manifest lineage

New file entries MAY carry these fields while retaining the current dataset manifest format and all
legacy fields: `provider_id`, `universe_id`, `evidence_id`, `evidence_sha256`, `candidate_id`,
`candidate_manifest_sha256`, `gate_report_sha256`, `adapter_version`, `provider_schema_version`.
Legacy entries without them decode as legacy BaoStock provenance in memory. A new writer MUST NOT
invent lineage for a legacy entry or rewrite its Parquet bytes.

## Exact BaoStock endpoint contract

This table is frozen from the current call sites and test fixtures; it is not inferred from a
provider web page or a provider error message. A response whose ordered `fields` tuple does not
match one of the named variants is a schema failure. There is no `union[Any]`, unknown-column
retention or silent field dropping. Raw strings are parsed only by the declared typed row model;
the original provider payload is not retained.

| Endpoint / variant | Current request call and exact fields (in order) | Typed row rules, units and empty/null rules | Request/page and canonical row ordering |
| --- | --- | --- | --- |
| `trade_dates.v1` | `query_trade_dates(start_date=ISO, end_date=ISO)`; `calendar_date`, `is_trading_day` | `calendar_date: date`; `is_trading_day: BoundedToken`. Only token `"1"` is a trading session; other tokens are not reinterpreted. No units. A range MAY be empty; the exact-date fetch requires exactly one row. | Provider pages are consumed in page number order; rows are unique and sorted by `calendar_date` ascending in evidence. A duplicate/date outside the requested range fails. |
| `all_stock.market.v1` | `query_all_stock(day=ISO)`; `code`, `tradeStatus`, `code_name` (the market fixtures expose this exact tuple) | `code: SafeSymbol`; `tradeStatus: BoundedToken` and `code_name: bounded UTF-8 string` (empty string is retained; JSON null is rejected). The market path uses `code` for main-board universe membership; it does not guess additional status semantics. No units. An empty snapshot or duplicate code fails. | One explicit date request; pages must advance under F0.1. Evidence rows sorted by `code` ascending; duplicate codes fail. |
| `daily_astock.v1` | `query_daily_history_k_AStock(date=ISO)`; exact `DAILY_FIELDS`: `date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST` | `date: date`; `code: SafeSymbol`; OHLC/preclose: finite numeric, non-empty; `volume`, `amount`: finite numeric when active, empty allowed only for a row classified as non-trading by the existing normalizer; `adjustflag: BoundedToken` and MUST be `"3"`; `turn`, `pctChg`: finite numeric or empty string→`None`; `tradestatus`, `isST`: bounded provider tokens, with active exactly `"1"` and no guessed meaning for other values. Units: volume `shares`, amount `CNY`, turnover `percent`, pctChg `percent`. | One exact trade date; pages must advance. Rows are unique by `(code,date)` and sorted by `(date,code)` in evidence. Date mismatch, duplicate symbol or non-finite value fails the full batch. |
| `daily_factor.v1` | `query_daily_adjust_factor(date=ISO)`; exact `AdjustmentFactorCache.FACTOR_FIELDS`: `code,dividOperateDate,foreAdjustFactor,backAdjustFactor,adjustFactor` | `code: SafeSymbol`; `dividOperateDate: date` and MUST be `<=` the requested session; three factors: finite positive numeric. Units: factors are dimensionless ratios. No null/empty factor values; an empty event response is valid only when the existing factor-cache contract can prove the snapshot another way. | A date event request may have pages; pages and rows are ordered by `(dividOperateDate,code)` in evidence. Duplicate `(code,dividOperateDate)` or future event fails. |
| `adjust_factor.session.v1` | `query_adjust_factor(stock_symbol,start_date="1990-01-01",end_date=ISO)`; same exact five factor fields | Same typed rules as `daily_factor.v1`; this is a distinct request variant because it is a per-stock history and may be empty for a legal suspended placeholder. It MUST NOT be used to invent an active stock factor. | Rows unique by `(code,dividOperateDate)` and sorted by `(code,dividOperateDate)` in evidence; every row's code equals the requested stock symbol. |
| `index_history.session.v1` | `query_history_k_data_plus(symbol, DAILY_FIELDS, start_date=ISO,end_date=ISO,frequency="d",adjustflag="3")`; exact 14 daily fields | Same typed daily rules as `daily_astock.v1`; the requested symbol role is discriminated: `index_symbol` MUST be one of existing `INDEX_SYMBOLS` and receives no stock factor; `stock_symbol` is not an index. Units are identical to daily bars. | One exact date/symbol; zero rows is missing and fails required coverage. One row is expected. |
| `index_history.range.v1` | `query_history_k_data_plus(symbol, DAILY_FIELDS, start_date=ISO,end_date=ISO,frequency="d",adjustflag="3")` in `fetch_range`; exact 14 daily fields | Same fields/types as `index_history.session.v1`, but the request range MAY contain multiple trading dates. Every row's code equals the requested symbol; no date outside range. | Rows unique and sorted by `(date,code)`; a repeated date or non-advancing page is `PAGINATION_STALLED`/schema failure. |

The two `index_history` variants and the two factor variants are explicit request/schema variants,
not a union of arbitrary rows. The provider's SDK iterator may expose pages in arrival order; the
adapter records page/attempt lineage and emits the deterministic evidence order shown above. A
provider row order that cannot be deterministically normalized is a complete candidate failure.

### Semantic boundary retained from the current normalizer

- `trade_dates` is the only calendar authority for the requested source date; arrival time,
  local weekday and inferred dates are forbidden.
- `all_stock` is the source universe snapshot. Main-board membership and required indexes are
  checked against the declared `universe_id`; unknown/duplicate symbols fail closed.
- `daily_astock` and `index_history` use `adjustflag="3"`; canonical OHLCV remains unadjusted.
- `daily_factor` and `adjust_factor` provide `backAdjustFactor` by symbol/effective date. The
  selected factor is the latest eligible effective date `<= trade_date`, and active stock rows
  without a finite positive factor fail.
- `tradestatus == "1"` is the current active classifier. A non-`"1"` row is not a license to
  infer a new suspension meaning: the existing placeholder and activity gates remain authoritative.
  A suspended stock may have empty activity/factor only under those existing gates. A suspended
  index, non-zero suspended activity, malformed placeholder, duplicate or coverage mismatch fails.
- Source symbols retain the `sh.`/`sz.` six-digit form and canonical symbols are lowercase.

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

### AC-16: Frozen models and endpoint variants (FR-28–FR-29, NFR-8–NFR-9)

Given a request or response using an unknown provider, symbol, field, schema variant, unit, null
rule or row shape
When the Task 7 contract/adapter validates it
Then the immutable discriminated model rejects the complete batch before evidence publication,
and each of the six endpoint IDs is accepted only through an explicitly frozen variant.

### AC-17: Request/object/transport cardinality (FR-30–FR-31, NFR-2–NFR-6, NFR-16)

Given multiple symbols, indexes, retries or SDK pages for one refresh
When Task 8 assembles its evidence manifest
Then every expected plan item has exactly one descriptor and bound sanitized observation lineage,
with no missing, extra, duplicate or swallowed object, and a competing writer performs zero
canonical writes.

### AC-18: Canonical gate aggregate (FR-32, NFR-6–NFR-8)

Given a candidate that passes or fails any semantic, coverage or integrity gate
When Task 9 persists the gate decision
Then exactly one ordered `CandidateGateReport` contains every `R2F2_GATE_ORDER` outcome and
aggregate hash; missing, duplicate, extra or mismatched outcomes make selection unavailable.

### AC-19: Replay clock and storage safety (FR-33, NFR-12, NFR-15–NFR-17)

Given a published evidence object and an online or replay normalization run
When the frozen clock, no-follow layout, CAS and replay comparison rules execute
Then online and replay use the same `normalization_clock_utc`, same-version output is byte-equal,
documented encoding-only changes require semantic equality, and missing/corrupt/read-only roots
cause zero writes.

### AC-20: Fallback and validator boundaries (NFR-18, FR-27)

Given an attempted `qualified_fallback`, second provider/plugin, shadow/failover control or an
implementation-plan validator invocation
When the R2-F2 writer/orchestrator/review gate runs
Then the fallback/second-source path is rejected or out of scope, the design-only validator is
reported only as a structural result, and manual implementation traceability remains required.

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
- EC-21: Expected request plan contains a missing, extra, duplicate, out-of-order or swallowed
  request/shard/page; manifest cardinality validation fails before evidence publication.
- EC-22: A descriptor has transport IDs/endpoint/attempt/page that do not match its sanitized
  observation digest; the complete evidence chain is unavailable.
- EC-23: Candidate gate outcomes omit, duplicate, reorder or add a gate; the aggregate report and
  candidate fail closed and no selection is published.
- EC-24: Online/replay normalization uses different clocks or replay-start time enters a hash;
  deterministic comparison fails closed.
- EC-25: Evidence root/ancestor is a symlink, wrong inode, non-directory or cross-root path, or a
  reader attempts implicit initialization; reject and preserve all pre-read fingerprints.
- EC-26: A compare-create/no-clobber collision finds different bytes at an existing content path;
  retain both audit contexts if possible, never overwrite and do not move the canonical pointer.

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

## Frozen settings, layout and publication protocol

The R2-F2 settings additions are deliberately relative and bounded. They are configuration fields,
not arbitrary paths supplied by a replay caller:

| Setting | Default / bound | Writer/read behavior |
| --- | --- | --- |
| `provider_evidence_root` | `Path("var/evidence")`; root must be an app-owned local path | Explicit writer setup may create it; GET/plan/replay MUST treat a missing root as unavailable and MUST NOT initialize it. |
| `provider_evidence_max_object_bytes` | exactly `64 * 1024 * 1024` | Reject before publish and recheck after descriptor readback. |
| `provider_evidence_max_manifest_bytes` | exactly `1 * 1024 * 1024` | Reject before atomic manifest publication and on descriptor-bound read. |
| `provider_evidence_max_rows` | `10_000_000` per object and aggregate | Reject before Parquet write/readback. |
| `provider_evidence_enabled` | `False` | R2-F2 writer/orchestrator path is opt-in and remains BaoStock-only; it does not enable production refresh. |
| `provider_evidence_contract_version` | `r2f2-evidence-v1` | SafeVersion; mismatch fails closed. |

The default relative layout is fixed as follows; every path is relative to the opened evidence root
descriptor and every directory/file is no-follow validated:

```text
var/evidence/
  objects/<sha256-prefix>/<object-id>.parquet
  manifests/<evidence-id>.json
  gates/<report-id>.json
  candidates/<candidate-id>.json
  selections/<selection-id>.json
  staging/<refresh-id>/<request-id>/<page>.partial
  locks/evidence-publication.lock
  locks/refresh-run.lock
  locks/selection-publication.lock
  orphan-audit/<refresh-id>/...
```

`objects`, `manifests`, `gates`, `candidates`, `selections`, `staging`, `locks` and `orphan-audit`
are fixed path components; no caller may substitute a directory name. Root and each ancestor are
opened with `O_DIRECTORY|O_NOFOLLOW`, identity-checked (`st_dev`, `st_ino`, mode) before and after
use, and rejected if a symlink, non-directory, path traversal, absolute path, cross-root path or
TOCTOU replacement is observed. Metadata/object descriptors are opened by relative descriptor,
size-bounded, SHA-256 checked and schema/row-count checked before their descriptor is passed to a
reader. The threat boundary is local application-owned storage; this does not claim protection
against a same-UID process that continuously mutates a private staging inode, host compromise or
filesystem corruption beyond detection.

Publication is a compare-and-create protocol, not an overwrite protocol:

1. Under the evidence publication lock, validate the complete `ExpectedRequestPlan` and acquire a
   `RefreshRunLock` for the full logical refresh. Write each bounded page object below staging,
   fsync it, read it back through a no-follow descriptor, and compare-create the content-addressed
   object. On macOS, an exclusive create/link or `renameatx_np` no-follow/no-clobber primitive is
   required; a platform without the required primitive fails closed. Existing identical bytes are
   idempotent; a hash/path collision with different bytes is an error and never overwrites.
2. After every expected object exists and every `TransportLineageRef`/observation digest matches,
   atomically publish the canonical evidence manifest. Any unreferenced staging artifact is kept
   under `orphan-audit` for review; it is never silently adopted.
3. Gate and candidate reports are then published with the same immutable compare-create rule. A
   `CandidateManifest` may reference only a complete evidence manifest and exactly one canonical
   `CandidateGateReport` aggregate whose ordered gate set is complete.
4. While the same `RefreshRunLock` remains held, publish the `SessionSelection` first. Only after
   selection readback/hash validation may the existing canonical Parquet/manifest/pointer chain
   run. The canonical manifest/pointer MUST NOT reference a missing selection; a selection may not
   reference missing candidate/evidence/gate objects.
5. Existing R2-F1 lock ownership and queue/CAS ordering remains authoritative. A competing writer
   loses the compare-and-create/CAS check and performs zero canonical writes. A stale lock-in
   revalidation failure may retain the canonical lock artifact but MUST NOT create broad directories
   or mutate the canonical pointer.

Readers never initialize roots, schemas, locks or directories. A missing root, missing object,
invalid ancestor, lock/CAS conflict or incomplete chain returns an allowlisted unavailable/error
   result with zero writes. This is a read-only boundary, not a best-effort repair path.

## Replay clock and deterministic equivalence

`normalization_clock_utc` is generated exactly once after the final fetch/transport observation is
complete and immediately before evidence manifest publication. Both online normalization and offline
replay receive this same injected UTC clock; neither calls `datetime.now()` independently while
constructing candidate bytes. `replay_started_at` is diagnostic-only and MUST NOT enter any
candidate, semantic or selection hash.

Canonical evidence and candidate serialization rules are frozen:

- rows are ordered by the endpoint/variant keys above, then by plan ordinal, shard, page and source
  row key; no hash-map iteration order is used;
- canonical JSON is UTF-8, `ensure_ascii=False`, sorted keys, compact separators, no NaN/Infinity,
  and the ordered tuple sequence is preserved;
- decimals/floats use the existing finite numeric parser and a canonical decimal text policy
  (no exponent where a fixed decimal is required); `null` is emitted only for the explicitly
  optional `turn`/`pctChg` values and no empty string is silently changed elsewhere;
- UTF-8 strings are normalized to valid UTF-8 and bounded; malformed bytes fail closed;
- Parquet schema, column order, row order and nullability are read back before hashing.

Replay computes both a byte hash and a semantic hash. `byte_match=true` is required when the same
adapter/normalizer/schema versions and canonical writer are used. `byte_match=false` MAY coexist
with `semantic_match=true` only when the documented writer version changed a non-semantic Parquet
encoding (compression codec/row-group/page metadata) while the semantic hash proves identical
trade date, universe, sorted symbol set, unadjusted OHLCV, factor, suspension and quality semantics.
R2-F2 GO requires byte equality for same-version replay and semantic equality plus an explicit
versioned encoding-difference reason for a cross-version replay. Any other difference is NO-GO.

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
