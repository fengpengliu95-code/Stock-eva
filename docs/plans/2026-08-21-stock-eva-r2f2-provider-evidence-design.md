# Stock EVA R2-F2 Provider Evidence Framework Design

**Author:** Codex delivery team — specification owner
**Date:** 2026-08-21 (Asia/Shanghai)
**Status:** In Review — architecture amendment required; implementation is blocked and this
document is not approved
**Decision authority:** User approved the R2-F2 direction and Option A on 2026-08-21; the prior
planning review is historical evidence only and does not approve this amended architecture.
**Scope:** R2-F2 offline code and synthetic tests only; one BaoStock compatibility adapter.
**Baseline:** branch `codex/r2-f2-provider-evidence`, exact clean code HEAD
`fea5678059f5b2955dbd1b3b8c570d94ad9c87e9`; this code HEAD is review evidence only and is not a
delivery or GO commit
**Reviewers:** The preceding independent reviews found contract gaps around attempt completion,
path/storage separation, field/type drift, CLI parser ordering and traceability. Three Task 7
implementation rounds were reviewed as NO-GO; the exact commits and reconstructed H5/M6 findings
are recorded in the architecture amendment below. None is delivery.

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

## Architecture amendment (authoritative revision before any RED)

This revision is documentation-only. It changes status to **In Review — architecture amendment
required** and deliberately does not self-approve. The exact code HEAD `fea5678` is a non-delivery
review baseline. The normative Option A successful-attempt-only rule remains unchanged.

### Task 7 review history and reconstructed H5/M6 findings

| Review round | Exact commit | Verdict | Reconstructed blocking evidence |
| --- | --- | --- | --- |
| Task 7 initial | `63903663c48149fa3529852eb889c482c9558447` | NO-GO | Provider-neutral contracts and adapter were introduced without a closed actual-session/page identity model; the implementation was not delivery-ready. |
| Task 7 repair 1 | `c977a40845fec69e99db3655cb0da1ec24700cee` | NO-GO | Retry/completion and transport projection closure improved, but caller-provided identity and completion/page join semantics remained unsafe for evidence publication. |
| Task 7 repair 2 | `fea5678059f5b2955dbd1b3b8c570d94ad9c87e9` | NO-GO | H5: `ProviderRequest` still carries caller-overridable `provider_session_id` and `request_id`; the adapter passes the session override into `provider_session_scope()`. M6: login/relogin observations and query/page request IDs are not modeled as disjoint actual lineages, so a terminal non-OPERATION observation can be mistaken for query completion and page identity is not closed by the required root/page tuple. |

The H5/M6 reconstruction is grounded in the exact code diff: `ProviderRequest` has
`provider_session_id`/`request_id`; `BaoStockProviderAdapter.fetch_raw()` passes the former into
incumbent login; `provider_session_scope()` accepts an optional override; and completion/page
selection is derived from the mixed observation stream. These are architecture blockers, not
permission to patch production code in this task.

### Identity contract amendment (normative)

1. `ProviderRequest` MUST contain only `provider_id`, `refresh_id`, `trade_date`, `universe_id`,
   complete sorted unique non-empty `session_symbols`, and one `ExpectedLogicalRequestPlan`.
   It MUST NOT contain a provider-session ID, root request ID, page request ID or any transport
   request identity. An envelope ID is not added; if a future envelope is needed it MUST use a
   distinct name and MUST be explicitly non-transport/non-lineage.
2. Every actual login/relogin MUST enter incumbent `provider_session_scope()` without a caller
   override; that scope generates one unique actual `provider_session_id`. Every logical query
   attempt MUST enter a real query-root `request_scope()` that generates one fresh root
   `request_id`; every pagination page MUST enter a fresh page `request_scope()` and capture its
   actual page `request_id`. Callers may not supply or override any of these IDs.
3. `AttemptCompletion` MUST carry the actual `provider_session_id` and query-root
   `root_request_id`. Login/relogin observations are sanitized transport audit/lineage only,
   never a query completion; they are associated with the current logical call by `plan_ordinal`.
   The terminal state of an attempt is determined only by the matching query-root observation
   whose `protocol_stage` is `OPERATION`. Page/complete/login observations cannot determine the
   attempt outcome.
4. Every captured final-success page MUST close over actual
   `(refresh_id, provider_session_id, root_request_id, page_request_id, endpoint, attempt, page)`
   (or an equivalent typed structure). One final-success page/shard produces exactly one
   `RawEndpointBatch` whose lineage is the actual page request and matching `COMPLETE` digest;
   the root `OPERATION` digest drives completion but never page object lineage. Failed-attempt pages retain only
   bounded counts/IDs/audit; all failed source rows and bytes are discarded.
5. `ProviderRawBatch`/`EvidenceManifest` joins MUST use the exact logical request plan, one
   completion per logical query, and contiguous final-success page batches/descriptors. Duplicate,
   missing, extra or out-of-order plans, completions, attempts, page IDs, batches or projections
   MUST fail closed. `RequestCompletion.row_count` MUST equal the sum of final page read-back rows;
   every final attempt's pages MUST use the same actual provider session. Actual session IDs are
   derived from observations/pages; no forged singular top-level session is accepted.
6. Every `raw_endpoint_page` `EvidenceObjectDescriptor` MUST retain actual
   `provider_session_id`, query-root `root_request_id` and actual `page_request_id`; the separate
   factor-snapshot descriptor retains a locally generated `capture_id`, with all provider/session/
   root/page/endpoint/role/shard/plan/attempt/transport identity fields null. `EvidenceManifest` has no singular session field and aggregates
   sanitized observations across all actual sessions. `attempt_count` counts query attempts only;
   login audit observations are not query attempts.
7. The six endpoint IDs and all nine stock/index variants remain exact. Source schema is validated
   before any zip/object publication, then the read-back source/object digest and schema digest are
   recomputed before the descriptor or manifest is accepted; adapter and endpoint-contract versions
   MUST equal the exact current constants, not merely match a safe-token grammar. The public export
   matrix MUST export every frozen provider contract required by the implementation and tests.
8. At the typed adapter boundary, only source `volume`, `amount`, `turn` and `pctChg` may become
   `None`, and only for a stock row with `tradestatus != "1"`; required date/code/OHLC/preclose/
   adjustflag/isST empties reject. Active blank activity, suspended index placeholders and malformed
   required fields reject; finite zero suspended activity may remain typed, while non-zero activity
   is rejected by the existing gate. Factor rows MUST use exact logical symbols and both factor
   variants sort by `(dividOperateDate, code)`; an empty daily-factor event is raw evidence only,
   while Task 8's factor-cache snapshot and later factor gate must prove resolution.
9. Task 8 owns `PublishedEvidence` and `EvidenceReader`. A Task 7 compatibility test MAY use a
   narrow test seam, but the canonical path MUST be replaced by Task 8's typed
   hash/schema/row/descriptor-bound reader before any evidence is normalized. Task 7 MUST NOT
   duplicate `PublishedEvidence` or claim canonical evidence normalization.

No production or test code is changed by this amendment. Existing outer `RefreshRunLock`, Option A,
no failed source bytes, no dynamic plugin/second provider, no transport/vendor/normalizer changes,
no network and the production-unchanged boundary remain normative.

### F0.1 session lifecycle and authoritative capture events (normative)

“Caller cannot override” is deliberately scoped: the adapter, `ProviderRequest` and every external
caller MUST NOT provide a session ID. It does not prohibit the incumbent transport from rebinding
an already-created live connection. `_login_scoped` MUST call `provider_session_scope()` with no
arguments for every login/relogin; that scope generates a fresh actual `provider_session_id`, which
is stored on the live connection. The scope may exit while the connection remains alive. A later
`_request_scope(saved_actual_id)` MAY call `provider_session_scope(saved_actual_id)` only to rebind
that exact ID already generated by the incumbent login scope; this is internal rebind, not caller
override, and MUST not change F0.1 transport semantics. `_discard_session` MUST clear the saved
connection/session identity. Every relogin repeats the no-argument scope and therefore generates a
different actual session ID; no long-lived session scope is required.

The adapter MUST use one authoritative capture registry/hook at scope entry. It MUST NOT invent a
second projection or add fields to `provider_transport`/the vendor client. `_login_scoped` registers
`lineage_kind="login_audit"`; the outer `_read` page 1 enters `request_scope()` as
`lineage_kind="query_root"`, and page > 1 enters a nested `request_scope()` as `lineage_kind="page"`.
Initial login has `plan_ordinal=0`; a relogin during `_read` inherits the active logical call's
`plan_ordinal`. The registry deterministically derives `plan_ordinal` and `lineage_kind`, includes
them in the projection canonical form and therefore in `observation_digest`; the adapter cannot
replace either value. Page 1's `page_request_id` MUST equal its query-root `root_request_id`; later
pages have their own actual page IDs.

The only terminal authority is the unique `ProtocolStage.OPERATION` observation from the matching
query-root request. A vendor `COMPLETE` on page 1 is transport evidence for that page, not the
completion terminal; page > 1 `COMPLETE` is likewise page transport evidence. `end_marker_seen` is
recorded on the relevant COMPLETE projection: page 1 may be the final page only when its marker is
seen, otherwise page N continues with fresh page scopes and only the final page may carry the marker.
No page may follow a marked page, and page 1/page N map one-to-one to the contiguous observed page
set without guessing a total. A successful final raw page/descriptor/`RawEndpointBatch` binds one
matching successful COMPLETE projection and its actual tuple/digest; it MUST NOT bind the OPERATION
digest. The root OPERATION projection remains in `TransportObservationAggregate` and drives
`AttemptCompletion`, but never binds a page object. Login/relogin observations enter only the
sanitized aggregate: they create no `TransportLineageRef`, object descriptor or `AttemptCompletion`.
Failed-attempt observations likewise remain aggregate-only, even after a partial page was received;
they create no page lineage/object and all failed source rows/bytes are discarded.

`TransportLineageRef` is therefore restricted to final-success page COMPLETE projections. Its
`observation_digest` MUST resolve to exactly one `protocol_stage=COMPLETE` projection with matching
`page`, while the completion's operation digest resolves separately to the matching root
`protocol_stage=OPERATION`. “Audit lineage” in this specification means sanitized aggregate
observations only and MUST NOT be read as an object/completion lineage.

### Factor snapshot capture identity and manifest counts (normative)

Inside the existing `RefreshRunLock`, immediately before the exact snapshot read, Task 8 MUST
generate one locally unique `capture_id`; it is not a provider/session/request ID and is not copied
from the last provider call. `FactorCacheSnapshotManifest.capture_id` and the matching descriptor's
`capture_id` MUST be identical in both directions. A `FACTOR_CACHE_SNAPSHOT` descriptor has
`capture_id` required and `provider_session_id`, `root_request_id`, `page_request_id`, `endpoint`,
`request_role`, `instrument_role`, `shard_id`, `plan_ordinal`, `attempt`, `page` and
`transport_observation_digest` all null. A `RAW_ENDPOINT_PAGE` descriptor has `capture_id` null
and all of those page/session/transport fields required. The factor descriptor MUST never borrow
or forge the last provider session/root/page identity; the existing factor SQL table is unchanged.

`EvidenceManifest.object_count` MUST equal `len(objects)`, including raw-page descriptors and the
optional factor snapshot descriptor. `EvidenceManifest.row_count` MUST equal the sum of
`row_count` over `RAW_ENDPOINT_PAGE` descriptors only. The factor snapshot row count is recorded
only in `FactorCacheSnapshotManifest.row_count` and its matching descriptor row count; it MUST NOT
enter the manifest source `row_count`. `EvidenceManifest.attempt_count` MUST equal
`sum(len(completion.attempts) for completion in request_completions)` and MUST exclude login audit,
observation and page counts.

## Specification review closure (superseded; retained for Option A rationale)

### User architecture decision: Option A — successful attempt only

The user selected **A: only persist the complete evidence from the final successful attempt**.
This is a normative boundary for every model, validator, writer, reader, hash and test below:

- `AttemptCompletion` MAY retain only bounded, sanitized failure-attempt counters, outcome and
  actual identity (`provider_session_id`, `root_request_id`, observed page numbers/page IDs and
  transport observation digest).
  It MUST NOT retain failed-attempt row values, payload bytes, page bytes, decompressed content or
  arbitrary provider text.
- A failed attempt's row count, payload and page bytes are discarded in memory before the next
  attempt and MUST never reach staging, an evidence object, a Parquet file, a manifest, a hash,
  a candidate or a pointer. R2-F2 has no quarantined failed-payload object; that forensic option
  is explicitly out of scope.
- `EvidenceObjectDescriptor` and source-shaped Parquet objects MUST reference only the one final
  successful attempt for each logical request. The descriptor is logically unique by
  `(plan_ordinal, root_request_id, page_request_id, page)` and its `attempt` MUST equal
  `RequestCompletion.successful_attempt`.
- `RequestCompletion` MUST be final-success bound before publication: `final_outcome=success`,
  exactly one `successful_attempt`/`successful_root_request_id`, that root request ID is the final attempt,
  all published descriptors belong to it, observed pages are contiguous from page 1, and a
  terminal/end marker is present. A final failure has null successful fields and cannot publish.
- Sanitized `TransportObservationProjection` records for failed attempts MAY be retained in the
  non-payload aggregate observations and included in the ordered aggregate digest; no
  `TransportLineageRef`, failed-attempt object descriptor or source row may be inferred from those
  records, even after a partial page was received.
- A retry sequence that eventually succeeds publishes only the final successful pages. A logical
  request that ultimately fails publishes no `EvidenceManifest`, does not call normalization,
  and cannot produce a candidate, selection or canonical pointer mutation.
- `RequestCompletion.row_count` is an aggregate only for a successful final attempt and is checked
  against the final-success descriptor read-back sum. For an ultimate failure it MUST be zero;
  failed-attempt source row counts are not a persisted field and cannot enter any manifest or hash.

The explicit publication sequence is therefore:

```text
attempt in memory -> validate complete attempt
  -> on failure: retain sanitized transport projection only, discard rows/page bytes
  -> on final success: retain only final successful pages
  -> object compare-create -> EvidenceManifest -> normalize -> gate/candidate/selection
  -> existing canonical publication chain
```

No implementation may broaden this sequence by persisting failed partial payloads.

The preceding independent review was correctly **NO-GO**. Its findings were specification gaps,
not permission to narrow the product boundary. The current review also required the successful-
attempt-only evidence boundary, field/type closure, accurate umbrella metadata, parser ordering and
separation of lexical path validation from descriptor-bound storage. Their confirmed root causes and
closures are:

| Finding | Confirmed root cause | Normative closure in this revision |
| --- | --- | --- |
| M1 — frozen batch summaries and roles incomplete | `source_schema`, `units` and `date_semantics` were only prose/table fields, while index-history stock/index variants had no role discriminator. | Carry these fields on every `RawEndpointBatch` and aggregate them through a frozen `EndpointContractSummary`; add `InstrumentRole`/`RequestRole` and an explicit endpoint-role validator. |
| M2 — logical request cardinality predicted pages | The former expected-total-pages field made a caller guess the provider's total page count before fetching. | Replace it with `ExpectedLogicalRequest` plus post-fetch `RequestCompletion`; hash logical requests separately from observed contiguous pages and require terminal/end-marker evidence without guessing totals. |
| M3 — transport projection incomplete | Only a digest was named; the complete allowlisted F0.1 observation fields and aggregate verification were not frozen. | Freeze `TransportObservationProjection` with the exact F0.1 field names, preserve bounded `provider_code`, and verify per-projection and ordered aggregate digests. |
| M4 — gate set contradicted itself | The model had ten names while the table said eleven and the implementation traceability described a different set. | Use one exact ten-gate `R2F2_GATE_ORDER`, map every gate to current Normalize/Quality/publication functions and require exactly ten ordered outcomes. |
| M5 — lock ownership was ambiguous | Evidence/selection layouts proposed additional blocking locks that could deadlock or reverse the R2-F1 lock order. | Use only the existing `RefreshRunLock` at `local_lock_dir/market-refresh.lock`; content-addressed O_EXCL/no-clobber writes are lock-free compare-create under that outer lock, and replay is read-only. |
| M6 — replay result leaked invocation time | `ReplayResult` included a start-time field even though deterministic public output was required. | Remove the field entirely; only the frozen evidence `normalization_clock_utc` enters hashes/results, while internal observed time is never a result, candidate field or semantic input. |
| M7 — factor provenance was not self-contained | The mutable `AdjustmentFactorCache` call chain was treated as if the live cache were replayable evidence and factor data was conflated with a provider endpoint. | Add `EvidenceObjectKind.factor_cache_snapshot`, capture only actually-read keys and safe provenance under `RefreshRunLock`, fingerprint before/after, and make online/replay normalization consume only the published snapshot. |
| M8 — requirement/file traceability was incomplete | FR-28–33, NFR-15–18, AC-16–20 and EC-21–26 were absent or attached to the wrong task; Task 9 also owned the same model file as Task 7. | Add complete numbered traceability and tests, move selection tests to Task 9, give Task 9 a single `market/candidates.py` model owner, and keep Task 7/8 whitelists disjoint. |
| M9 — contract vocabulary was not structurally reconciled | Review text and field tables were previously able to drift from the frozen models and gate count, so structural review could not prove closure. | Compare actual model/serializer field sets with the authoritative tables, verify the fixed ten-gate order and run strict design validation plus `git diff --check`; do not infer closure from prose searches. |
| M10 — model/table names drifted | The frozen models and field tables previously represented the same identity, verdict and hash concepts with different spellings. | Use one authoritative name in every model, table, hash rule, test and implementation reference: `gate_report_id`, `verdict`, `aggregate_sha256` and `evidence_sha256`. |
| M11 — umbrella plan retained stale pseudo-contracts | The umbrella Task 7–9 section duplicated old `dict` rows, open provider strings and obsolete selection fields. | Replace the section with a high-level roadmap summary and state that the dedicated design and implementation plan are the only contract authorities. |
| M12 — factor snapshot invented fields | The proposed snapshot row contained `source`, cache version and factor values not present in the current `factor_snapshots` table, and did not separate records from a published descriptor. | Define `FactorCacheSnapshotRecord(s)` from the current nine table columns only; add a strict read-only `exact_snapshot_records()` API under Task 8, then publish a separate descriptor-bound `FactorCacheSnapshotManifest` with before/after, object, schema and records hashes; no schema migration. |
| M13 — factor resolution/cardinality incomplete | Resolution did not discriminate live/cache provenance, cache identity was not descriptor-bound, and completion did not represent retries, request IDs or final outcome. | Add mutually exclusive live/cache bindings with `cache_object_id`/`cache_object_sha256`/`record_key`, descriptor-bound factor replay and `AttemptCompletion`/`RequestCompletion`; bind every page descriptor and resolution hash into the manifest/candidate. |
| M14 — endpoint/date/path validators were declarative only | Tables described variants, but no authoritative endpoint constant, pre-normalize date binding or semantic relative-path/storage boundary was frozen. | Add `ENDPOINT_CONTRACTS`, `validate_raw_date_binding(request,batch)`, and lexical plus dirfd/O_NOFOLLOW containment rules with model-copy regressions. |

The amendment is still documentation-only. No RED test, provider request, external operation or
production mutation may begin until an independent specification reviewer records a separate
approval decision for this amended contract.

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
   only admitted value is `ProviderId.BAOSTOCK`; a lexical `SafeProviderId` is converted to that
   enum before admission. It has no plugin loader, entry points, dynamic import, arbitrary provider
   string admission or second-source adapter.
2. **The incumbent transport remains authoritative.** The compatibility adapter delegates to the
   current BaoStock transport/parser and preserves its endpoint names, session scopes, retry
   limits, timeout policy, provider-code mapping and circuit integration. F0.1 transport code is
   outside the R2-F2 file whitelist.
3. **Source-shaped evidence is bounded and intentional.** “Raw” means typed rows and request
   metadata required to reproduce the BaoStock normalization contract. It is not a socket dump,
   response body archive, log, exception, credential, URL, cookie or header store. The source-row
   schema is an endpoint allowlist and rejects unknown fields.
4. **Evidence is published before normalization.** The online sequence is
   `fetch complete final-success source rows -> validate/redact -> write bounded Parquet partial ->
   read back schema, row count and hash -> atomic object rename -> atomic canonical JSON evidence
   manifest -> read the published evidence -> normalize -> candidate gates`. A failed attempt's
   partial rows/page bytes remain in memory only until sanitized transport audit is recorded, then
   are discarded; they never enter the partial-file step. A live SDK result is never passed directly
   to normalization by the R2-F2 canonical path.
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
- FR-2: **Provider contract:** `ProviderRequest` MUST carry only provider ID, refresh ID, exact
  trade date, universe ID, a sorted unique non-empty complete `session_symbols` set and one
  `ExpectedLogicalRequestPlan`; it MUST NOT carry provider-session or transport request IDs.
  `ProviderRawBatch` MUST carry adapter and
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
- FR-6: **Evidence object:** Source rows from only the final successful attempt MUST be stored in
  a bounded Parquet object and its metadata in canonical UTF-8 JSON with sorted keys and compact
  separators. Failed-attempt rows/page bytes are discarded in memory and MUST NOT create a
  staging file, object, manifest or hash. The evidence object MUST be content-addressed by
  SHA-256 and idempotent for identical bytes.
- FR-7: **Evidence manifest binding:** The evidence manifest MUST bind each final-success relative
  object path to its content hash, schema identifier, row count, provider ID, adapter version,
  endpoint-contract version, trade date, universe ID, units, date semantics and request identity.
  It MAY bind sanitized transport observations from failed attempts, but no failed-attempt object
  descriptor or source bytes may be referenced. Absolute paths, URL, token, cookie, header, raw
  payload and arbitrary exception fields MUST be impossible in the persisted model.
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
  rows, `backAdjustFactor` is a finite positive factor selected on or before the trade date from
  the published `factor_cache_snapshot` or an explicitly bound raw factor endpoint; missing,
  invalid or mutable-cache-only factors fail the candidate. Index rows have no stock factor
  requirement.
- FR-22: **Suspension semantics:** `tradestatus != "1"` means suspended/non-trading. Only source
  `volume`, `amount`, `turn` and `pctChg` blanks may become typed `None`, and only for a stock row;
  required date/code/OHLC/preclose/adjustflag/isST blanks reject. A legal suspended stock may
  retain finite zero activity, with exactly the existing suspended-placeholder quality issue; active
  blank activity, non-zero suspended activity, suspended indexes or malformed placeholder rows fail
  closed.
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
  `SafeSymbol`, `SafeVersion`, `SafeRelativePath`, `ProviderEndpoint`, `ProviderRequest`,
  `TransportLineageRef`, `FactorCacheSnapshotRecord(s)`, `FactorCacheSnapshotManifest`,
  `PublishedFactorCacheSnapshot`, discriminated raw-row/batch, evidence, gate, candidate, selection
  and replay models with
  immutable `extra="forbid"` validation. Any missing/extra/duplicate field MUST fail closed.
- FR-29: **Exact endpoint schema closure:** Each of the six endpoint IDs MUST use one of the
  explicitly named `ENDPOINT_CONTRACTS` request/schema/role variants and exact ordered field tuples
  in this specification;
  an unobserved field, inferred unit, unknown null rule or unclassified same-endpoint variant MUST
  invalidate the complete batch.
- FR-30: **Request-plan cardinality:** Every logical request and symbol/index shard MUST have one
  ordered `ExpectedLogicalRequest`; execution MUST produce exactly one `RequestCompletion` with
  actual provider session IDs, fresh query-root IDs per attempt, fresh page request IDs per page,
  ordered `AttemptCompletion` records, contiguous observed pages and
  one final outcome. Failed attempts may retain only sanitized counters/outcome/actual IDs in
  memory and transport audit. Each published source page MUST have exactly one descriptor carrying
  `plan_ordinal/attempt/root_request_id/page_request_id/page/object_kind`, and the descriptor MUST belong to the final
  successful attempt. `ProviderRawBatch` and `EvidenceManifest` MUST reject missing, extra,
  duplicate or out-of-order logical requests/completions/attempts/pages/batches/descriptors; every
  final batch lineage key plus digest MUST join exactly one sanitized observation projection, every
  final attempt's pages MUST share its actual provider session, and `row_count` MUST equal final
  page read-back rows; manifest `object_count` MUST equal `len(objects)`, manifest `row_count` MUST
  sum only raw-page descriptor rows, and factor snapshot rows MUST remain confined to their own
  descriptor/manifest row count. `attempt_count` MUST equal the sum of attempts in completions only.
  The final completion MUST be successful, terminal and bound to all published
  descriptors; an ultimate failure publishes no evidence manifest. The logical request-plan hash
  and completion hash MUST be verified before evidence, candidate or selection publication; no page
  total is guessed.
- FR-31: **Transport binding:** Every published final-success object MUST bind
  refresh/provider-session/query-root/page-request/endpoint/attempt/page to a SHA-256 digest of the
  sanitized F0.1 observation projection. Failed-attempt projections may be included only in the sanitized ordered
  transport aggregate and never yield source objects. Payload, URL, token, header, cookie and raw
  provider exception/message MUST remain impossible to persist.
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
  a complete new state, never a partial published object. Failed-attempt partial pages are discarded
  before any staging boundary and therefore cannot become orphan, published or quarantined evidence.
- **NFR-3 (Integrity):** Every object read MUST bind an opened descriptor/stat fingerprint to
  SHA-256/schema/row-count validation; TOCTOU, symlink, path traversal, object substitution and
  manifest swap MUST fail closed.
- **NFR-4 (Bounds):** Evidence metadata MUST be <= 1 MiB, each source evidence object MUST be <=
  64 MiB, decompressed Parquet content MUST remain within the same bounded policy, and row counts
  MUST be non-negative and schema-consistent. Oversize or decompression failure is an error.
- **NFR-5 (Concurrency):** The existing `RefreshRunLock` and content-addressed CAS/version checks
  MUST allow exactly one logical evidence/candidate/selection publication for a request key;
  losers do zero canonical writes. Evidence does not add a second blocking lock.
- **NFR-6 (Determinism):** Canonical JSON serialization, object hashes, candidate IDs, request keys,
  row ordering and replay output MUST be deterministic for identical inputs and contract versions.
  Transport identities are actual scope outputs; caller input cannot select a provider session,
  query-root request ID or page request ID.
- **NFR-7 (Compatibility):** No existing immutable object, legacy manifest, public response contract
  or API reader requires a rewrite. New fields are additive and old source values remain valid.
- **NFR-8 (Security):** Persisted schemas MUST have no payload/token/header/cookie/URL/path/raw
  exception field or failed-attempt page bytes; all identifiers and provider codes are bounded
  allowlisted strings. Failure-attempt transport audit is sanitized and non-payload only; login
  audit observations cannot become query completions or query attempt counts.
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
- **NFR-16 (CAS/publication order):** The existing `RefreshRunLock` at
  `local_lock_dir/market-refresh.lock` MUST be acquired before evidence compare-create, then gate,
  candidate, selection and existing canonical manifest/pointer operations follow the frozen order
  and no-clobber rules; a competing or stale writer MUST fail closed without canonical writes.
- **NFR-19 (Identity closure):** Each login/relogin MUST generate a unique incumbent provider
  session; each query attempt MUST generate one root request scope and each page one page request
  scope. All completion/page/descriptor joins MUST use actual IDs and matching `plan_ordinal`;
  caller overrides and singular forged session summaries are forbidden.
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
    provider_id: ProviderId
    adapter_version: SafeVersion
    endpoint_contract_version: SafeVersion

    def fetch_raw(self, request: ProviderRequest) -> ProviderRawBatch: ...
    def normalize(
        self, evidence: PublishedEvidence, *, normalization_clock_utc: datetime
    ) -> tuple[DailyBar, ...]: ...

class EvidenceStore(Protocol):
    def publish(self, batch: ProviderRawBatch) -> EvidenceManifest: ...
    def read(self, evidence_id: SafeIdentifier) -> PublishedEvidence: ...
    def replay(
        self, evidence_id: SafeIdentifier, *, compare_candidate_sha: SafeSha256 | None = None
    ) -> ReplayResult: ...

class FactorSnapshotReader(Protocol):
    def exact_snapshot_records(
        self, symbols: Sequence[SafeSymbol], trade_date: date
    ) -> tuple[FactorCacheSnapshotRecord, ...]: ...
```

`fetch_raw` MUST return all six endpoint variants required by the exact request plan or a complete
sanitized failure; it MUST NOT return a partial batch marked successful. `normalize` MUST accept
only a descriptor/fingerprint/hash-verified `PublishedEvidence` reader and the already-frozen
`normalization_clock_utc`; a live SDK result is not a valid argument. `EvidenceStore` is a local
writer/reader boundary and never a network client.

`exact_snapshot_records` MUST execute one explicit read-only `SELECT` over the existing
`factor_snapshots` columns, return only requested `(symbol, trade_date)` rows in deterministic
symbol order, compute `row_fingerprint` from the exact returned values, and never call
`_initialize`, `CREATE`, `ALTER`, `INSERT`, `UPDATE`, `DELETE` or `exact_snapshots` as a proxy.
The caller computes a selected-key fingerprint before and after online capture while holding the
existing refresh lock; any changed row, schema/column mismatch or missing requested key fails
closed. The method is not a cache writer and is not used by read-only GET/plan/replay paths.

CLI contract:

```text
market-provider-replay --evidence-id ID [--compare-candidate-sha SHA]
```

The parser accepts only the evidence ID and optional SHA-256 comparison. It rejects credentials,
URLs, local paths, provider names, arbitrary flags and unknown fields before any root access. It
returns exactly the sanitized `ReplayResult` projection (`status`, `evidence_id`, candidate/semantic
hashes and match booleans, the frozen normalization clock, bounded row/date fields and allowlisted
failure class). Invocation time is not part of this projection or any candidate evidence and is
never hashed. Missing or
corrupt evidence exits non-zero with an allowlisted `EVIDENCE_*`/`REPLAY_*` reason and zero writes.

No new HTTP write endpoint is introduced. Existing `GET /api/v1/market/status`, summary and
history contracts remain read-only and compatible; optional lineage fields are additive and never
required by legacy clients.

### Transport observation binding

R2-F0.1 already emits `TransportObservation`. R2-F2 freezes the exact persisted projection below;
there is no open mapping and no field may be added by an adapter. `provider_code` remains an
independently auditable bounded value even when `normalized_error` is
`UNKNOWN_PROVIDER_PROTOCOL_ERROR`.

`ProtocolStage`, `ProviderCode`, `NormalizedTransportError` and `TransportOutcome` below are the
existing typed definitions in `backend/app/market/provider_transport.py`; R2-F2 reuses their exact
names and enum values and does not redefine their transport semantics.

```python
class TransportObservationProjection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier  # actual session generated by incumbent login scope
    request_id: SafeIdentifier
    plan_ordinal: int = Field(ge=0, le=4096)
    lineage_kind: Literal["login_audit", "query_root", "page"]
    provider_id: ProviderId
    endpoint: ProviderEndpoint
    attempt: int = Field(ge=1, le=20)
    page: int = Field(ge=1, le=16_384)
    protocol_stage: ProtocolStage  # existing provider_transport.ProtocolStage
    elapsed_ms: int = Field(ge=0, le=86_400_000)
    recv_calls: int = Field(ge=0, le=10_000_000)
    response_bytes: int = Field(ge=0, le=67_108_864)
    end_marker_seen: bool
    provider_code: ProviderCode | None  # existing provider_transport.ProviderCode
    normalized_error: NormalizedTransportError | None  # existing F0.1 enum
    outcome: TransportOutcome  # existing F0.1 enum: success/error
    observed_at: datetime
    observation_digest: SafeSha256

class TransportObservationAggregate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    observations: tuple[TransportObservationProjection, ...]
    aggregate_digest: SafeSha256
```

`request_id` in this projection is an observed transport ID, never a caller-supplied field on
`ProviderRequest`. The authoritative scope-entry capture registry derives
`plan_ordinal`/`lineage_kind` (`login_audit`, `query_root`, or `page`) and includes both in the
projection digest; the adapter cannot supply an alternate projection. Initial login uses ordinal 0;
relogin inside a read uses the active logical ordinal. Page 1's page request ID equals its root ID.
`login_audit` observations are retained only as sanitized aggregate observations; they are not
`TransportLineageRef`, descriptors or completions. `query_root`'s unique `OPERATION` observation
can determine attempt terminal state; its digest never binds a page object. A page `COMPLETE`
observation is the only source for a final page `TransportLineageRef`, and `page` identifies a
pagination request.
`observation_digest` is SHA-256 of canonical JSON over the exact projection without that digest;
`aggregate_digest` is SHA-256 of the ordered tuple of projection canonical forms without the
aggregate field. No `message`, `payload`, `url`, `token`, header, cookie or raw exception is
accepted. Every `TransportLineageRef` and object descriptor binds one projection by matching
refresh/session/request/endpoint/attempt/page, `protocol_stage=COMPLETE` and digest. Missing,
duplicate, extra, or cross-refresh observations invalidate the complete evidence candidate. A failed
observation, including one after a partial page, may be retained only in the sanitized aggregate and
cannot produce a page lineage or published source object.

| Authoritative capture event | Scope entry and identity | Protocol/page meaning | Persisted consequence |
| --- | --- | --- | --- |
| Initial login | `_login_scoped` → `provider_session_scope()` with no argument; `plan_ordinal=0` | `lineage_kind=login_audit` | Sanitized aggregate observation only; no completion, page lineage or object |
| Relogin | `_login_scoped` → a fresh no-argument session scope; active logical `plan_ordinal` | `lineage_kind=login_audit` | Sanitized aggregate observation only; new actual session must differ from the prior one |
| Query root/page 1 | Outer `_read` → `request_scope()`; `page_request_id == root_request_id` | `lineage_kind=query_root`; unique `OPERATION` is attempt terminal authority; same-request `COMPLETE` is page-1 transport evidence | Root OPERATION digest drives `AttemptCompletion`; page-1 COMPLETE digest may bind a final page object |
| Pagination page N>1 | Nested `_read` page scope → fresh `request_scope()` | `lineage_kind=page`; `COMPLETE` is page transport evidence and `end_marker_seen` closes the observed page set | Only a final-success page COMPLETE digest may bind one `TransportLineageRef`/raw page object |
| Failed attempt or partial page | The same scopes as above, but non-success outcome | Aggregate observation only; partial rows/bytes are discarded | No page lineage, descriptor, object or manifest row |

The registry, not an adapter-local event shape, derives every row's `plan_ordinal` and
`lineage_kind`; both are hashed in the projection. `TransportLineageRef` is a final-success-page
projection of the table's COMPLETE rows, while `AttemptCompletion.operation_observation_digest`
is the separate root OPERATION projection digest.

```typescript
interface ProviderReplayResponse {
  status: "ready" | "unavailable" | "error";
  evidence_id: string;
  candidate_sha256: string | null;
  semantic_hash: string | null;
  byte_match: boolean | null;
  semantic_match: boolean | null;
  normalization_clock_utc: string | null;
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
R2F2_ADAPTER_VERSION: Literal["r2f2.v1"] = "r2f2.v1"
R2F2_ENDPOINT_CONTRACT_VERSION: Literal["r2f2-endpoints.v1"] = "r2f2-endpoints.v1"
# Lexical-only path value. It has no root/descriptor knowledge.
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

class InstrumentRole(StrEnum):
    STOCK = "stock"
    INDEX = "index"

class RequestRole(StrEnum):
    CALENDAR = "calendar"
    UNIVERSE = "universe"
    DAILY_STOCK = "daily_stock"
    DAILY_FACTOR = "daily_factor"
    ADJUST_FACTOR = "adjust_factor"
    INDEX_HISTORY = "index_history"

class EvidenceObjectKind(StrEnum):
    RAW_ENDPOINT_PAGE = "raw_endpoint_page"
    FACTOR_CACHE_SNAPSHOT = "factor_cache_snapshot"

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

R2F2_GATE_EVIDENCE: tuple[tuple[GateName, str, str], ...] = (
    (GateName.TRANSPORT_COMPLETE, "F0.1 TransportObservationProjection", "BaoStockProvider._read / provider transport observation collector"),
    (GateName.SCHEMA, "RawEndpointBatch", "providers.base exact discriminated endpoint/schema validator"),
    (GateName.DATE, "validate_raw_date_binding(request, batch) + publication identity", "R2-F2 adapter/evidence requested-date binding followed by existing publication requested_date/session identity gate; normalize.py is not changed and is not claimed to validate the requested date"),
    (GateName.UNIVERSE, "_publication_issues", "backend.app.market.automation._publication_issues expected/required symbols and indexes"),
    (GateName.COVERAGE, "run_publication_refresh", "backend.app.market.automation.run_publication_refresh coverage == 1"),
    (GateName.SEMANTIC, "normalize_baostock_rows + publication_bar_quality_issue", "existing normalized DailyBar and MarketStore quality contract"),
    (GateName.FACTOR, "_main_board_factor_snapshot + _publication_issues", "AdjustmentFactorCache resolution and missing_adjust_factor gate"),
    (GateName.SUSPENSION, "publication_bar_quality_issue", "MarketStore.publication_bar_quality_issue suspended placeholder/index rules"),
    (GateName.EVIDENCE_HASH, "EvidenceReader", "descriptor-bound schema, row-count and SHA-256 readback"),
    (GateName.DETERMINISM, "EvidenceStore.replay", "frozen normalization clock and byte/semantic replay comparison"),
)
```

`SafeProviderId` is the lexical safety type; the R2-F2 registry accepts it only after an allowlist
conversion to `ProviderId.BAOSTOCK`. All provider-bearing model and table fields use `ProviderId`;
there is no parallel provider literal field type. `SafeSymbol` is lowercase and must be sorted lexicographically
in every tuple. `SafeVersion` identifies a pinned code/schema contract, not a package URL.
`SafeRelativePath` is a pure lexical component validator. Its model validator MUST reject an empty
value, `.`, `..`, an absolute POSIX path, a leading separator, a backslash, NUL, a repeated or
trailing separator, and any component equal to `.` or `..`. It MUST NOT claim knowledge of an
evidence root, descriptor, inode or containment. Construction and `model_copy(update=...)`/round-trip
tests exercise these lexical rules.

Storage owns the separate descriptor-bound operation:

```python
def open_evidence_relative(root_dirfd: int, path: SafeRelativePath, flags: int) -> int: ...
```

`open_evidence_relative` MUST open the root and every ancestor with `dirfd` plus
`O_DIRECTORY|O_NOFOLLOW`, verify `(st_dev, st_ino, mode)` before and after use, open the final
object relative to those descriptors, apply `O_NOFOLLOW` to the final component and enforce
descriptor containment. A lexical pass is never TOCTOU protection; a caller-supplied resolved path
is never accepted. No persisted model may contain a URL, credential, token, header, cookie,
payload, absolute path or arbitrary exception/message field.

### Frozen in-memory contracts

The following is the complete contract surface. `Field(ge=0, le=...)` bounds are mandatory where
shown; omitted collection bounds are fixed at implementation time by the constants in the table
below and an over-bound value is rejected before any write.

```python
class ProviderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    provider_id: ProviderId
    refresh_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    session_symbols: tuple[SafeSymbol, ...]  # complete canonical target; sorted, unique, non-empty
    logical_request_plan: "ExpectedLogicalRequestPlan"

# `session_symbols` is the complete canonical target for this refresh (required stock universe
# plus required index symbols), not one SDK call's shard. It is sorted, unique and non-empty.

class TransportLineageRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    refresh_id: SafeIdentifier
    provider_session_id: SafeIdentifier  # actual session; never a ProviderRequest/caller field
    root_request_id: SafeIdentifier
    page_request_id: SafeIdentifier
    endpoint: ProviderEndpoint
    plan_ordinal: int = Field(ge=0, le=4096)
    attempt: int = Field(ge=1, le=20)
    page: int = Field(ge=1, le=16384)
    protocol_stage: Literal[ProtocolStage.COMPLETE]
    observation_digest: SafeSha256

class RawEndpointRowBase(BaseModel):
    """Discriminated union root; concrete variants are below, never an open mapping."""
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    endpoint: ProviderEndpoint
    schema_variant: SafeVersion
    request_role: RequestRole
    instrument_role: InstrumentRole | None

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

class PaginationPolicy(StrEnum):
    PROVIDER_TERMINAL = "provider_terminal"

class EndpointContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    endpoint: ProviderEndpoint
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    schema_variant: SafeVersion
    fields: tuple[SafeIdentifier, ...]
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    date_semantics: Literal["explicit_trade_date"]
    pagination_policy: PaginationPolicy

# This is the one authoritative endpoint/schema/role table. RawEndpointBatch,
# EndpointContractSummary and every object descriptor MUST equal the selected
# entry byte-for-byte for fields, units, date semantics and pagination policy.
ENDPOINT_CONTRACTS: tuple[EndpointContract, ...] = (
    EndpointContract(
        endpoint=ProviderEndpoint.TRADE_DATES, request_role=RequestRole.CALENDAR,
        instrument_role=None, schema_variant="trade_dates.v1",
        fields=("calendar_date", "is_trading_day"), units=(),
        date_semantics="explicit_trade_date", pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    EndpointContract(
        endpoint=ProviderEndpoint.ALL_STOCK, request_role=RequestRole.UNIVERSE,
        instrument_role=InstrumentRole.STOCK, schema_variant="all_stock.market.v1",
        fields=("code", "tradeStatus", "code_name"), units=(),
        date_semantics="explicit_trade_date", pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    EndpointContract(
        endpoint=ProviderEndpoint.DAILY_ASTOCK, request_role=RequestRole.DAILY_STOCK,
        instrument_role=InstrumentRole.STOCK, schema_variant="daily_astock.v1",
        fields=("date", "code", "open", "high", "low", "close", "preclose", "volume",
                "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST"),
        units=(("volume", "shares"), ("amount", "CNY"), ("turn", "percent"),
               ("pctChg", "percent")), date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    EndpointContract(
        endpoint=ProviderEndpoint.DAILY_FACTOR, request_role=RequestRole.DAILY_FACTOR,
        instrument_role=InstrumentRole.STOCK, schema_variant="daily_factor.v1",
        fields=("code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"),
        units=(("foreAdjustFactor", "dimensionless"), ("backAdjustFactor", "dimensionless"),
               ("adjustFactor", "dimensionless")), date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    EndpointContract(
        endpoint=ProviderEndpoint.ADJUST_FACTOR, request_role=RequestRole.ADJUST_FACTOR,
        instrument_role=InstrumentRole.STOCK, schema_variant="adjust_factor.session.v1",
        fields=("code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor", "adjustFactor"),
        units=(("foreAdjustFactor", "dimensionless"), ("backAdjustFactor", "dimensionless"),
               ("adjustFactor", "dimensionless")), date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    # INDEX_HISTORY has the same ordered source fields for both real call roles
    # (the incumbent provider fetches required indexes and explicit stock symbols).
    EndpointContract(
        endpoint=ProviderEndpoint.INDEX_HISTORY, request_role=RequestRole.INDEX_HISTORY,
        instrument_role=InstrumentRole.INDEX, schema_variant="index_history.session.v1",
        fields=("date", "code", "open", "high", "low", "close", "preclose", "volume",
                "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST"),
        units=(("volume", "shares"), ("amount", "CNY"), ("turn", "percent"),
               ("pctChg", "percent")), date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    EndpointContract(
        endpoint=ProviderEndpoint.INDEX_HISTORY, request_role=RequestRole.INDEX_HISTORY,
        instrument_role=InstrumentRole.INDEX, schema_variant="index_history.range.v1",
        fields=("date", "code", "open", "high", "low", "close", "preclose", "volume",
                "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST"),
        units=(("volume", "shares"), ("amount", "CNY"), ("turn", "percent"),
               ("pctChg", "percent")), date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    EndpointContract(
        endpoint=ProviderEndpoint.INDEX_HISTORY, request_role=RequestRole.INDEX_HISTORY,
        instrument_role=InstrumentRole.STOCK, schema_variant="index_history.session.v1",
        fields=("date", "code", "open", "high", "low", "close", "preclose", "volume",
                "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST"),
        units=(("volume", "shares"), ("amount", "CNY"), ("turn", "percent"),
               ("pctChg", "percent")), date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
    EndpointContract(
        endpoint=ProviderEndpoint.INDEX_HISTORY, request_role=RequestRole.INDEX_HISTORY,
        instrument_role=InstrumentRole.STOCK, schema_variant="index_history.range.v1",
        fields=("date", "code", "open", "high", "low", "close", "preclose", "volume",
                "amount", "adjustflag", "turn", "tradestatus", "pctChg", "isST"),
        units=(("volume", "shares"), ("amount", "CNY"), ("turn", "percent"),
               ("pctChg", "percent")), date_semantics="explicit_trade_date",
        pagination_policy=PaginationPolicy.PROVIDER_TERMINAL,
    ),
)

`ENDPOINT_CONTRACTS[(endpoint, instrument_role, schema_variant)]` is the sole authority for the
six endpoint IDs and their stock/index variants. `endpoint_contract_for(...)` MUST reject a missing
or duplicate key, and `RawEndpointBatch`, `EndpointContractSummary`, `EvidenceObjectDescriptor`
and their `model_copy()`/round-trip forms MUST be revalidated against the constant. A wrong
endpoint/role/schema combination, changed ordered field tuple, changed units, date semantics or
pagination policy is a complete batch failure before any evidence write.

class RawEndpointBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    endpoint: ProviderEndpoint
    schema_variant: SafeVersion
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    plan_ordinal: int = Field(ge=0, le=4096)
    shard_id: SafeIdentifier
    lineage: TransportLineageRef
    rows: tuple[RawEndpointRow, ...]
    row_count: int = Field(ge=0, le=10_000_000)
    source_schema: SafeVersion
    fields: tuple[SafeIdentifier, ...]
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    date_semantics: Literal["explicit_trade_date"]
    pagination_policy: PaginationPolicy
    provider_row_order_digest: SafeSha256

class EndpointContractSummary(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    endpoint: ProviderEndpoint
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    schema_variant: SafeVersion
    source_schema: SafeVersion
    fields: tuple[SafeIdentifier, ...]
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    date_semantics: Literal["explicit_trade_date"]
    pagination_policy: PaginationPolicy
    batch_count: int = Field(ge=1, le=4096)
    row_count: int = Field(ge=0, le=10_000_000)

class FactorCacheSnapshotRecord(BaseModel):
    """The exact current factor_snapshots table row; no inferred fields."""
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    symbol: SafeSymbol
    trade_date: date
    fore_adjust_factor: Decimal
    back_adjust_factor: Decimal | None
    evidence_kind: BoundedToken
    evidence_effective_date: date
    evidence_observed_on: date | None
    source_row_hash: SafeSha256
    observed_at: datetime
    row_fingerprint: SafeSha256

class LiveFactorResolution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    descriptor_id: SafeIdentifier
    object_sha256: SafeSha256
    endpoint: Literal[ProviderEndpoint.DAILY_FACTOR, ProviderEndpoint.ADJUST_FACTOR]
    row_key: SafeIdentifier
    schema_variant: SafeVersion

class CacheFactorResolution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    cache_object_id: SafeIdentifier
    cache_object_sha256: SafeSha256
    record_key: SafeIdentifier

class FactorResolutionBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    plan_ordinal: int = Field(ge=0, le=4096)
    symbol: SafeSymbol
    trade_date: date
    selected_kind: Literal["factor_cache_snapshot", "daily_factor", "adjust_factor"]
    selected_value_semantic_hash: SafeSha256
    live: LiveFactorResolution | None = None
    cache: CacheFactorResolution | None = None
    resolution_sha256: SafeSha256

    @model_validator(mode="after")
    def require_selected_resolution(self) -> "FactorResolutionBinding":
        if self.selected_kind == "factor_cache_snapshot":
            if self.cache is None or self.live is not None:
                raise ValueError("cache factor resolution requires cache only")
        elif self.live is None or self.cache is not None:
            raise ValueError("live factor resolution requires live only")
        return self

class FactorCacheSnapshotRecords(BaseModel):
    """In-memory read-only records; this is never a published object descriptor."""
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    cache_schema: tuple[SafeIdentifier, ...]
    rows: tuple[FactorCacheSnapshotRecord, ...]
    row_count: int = Field(ge=0, le=10_000_000)
    records_sha256: SafeSha256
    before_fingerprint: SafeSha256
    after_fingerprint: SafeSha256

class FactorCacheSnapshotManifest(BaseModel):
    """Published descriptor projection; it contains no mutable/live cache records."""
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    capture_id: SafeIdentifier
    object_id: SafeIdentifier
    relative_path: SafeRelativePath
    object_sha256: SafeSha256
    byte_count: int = Field(ge=0, le=67_108_864)
    row_count: int = Field(ge=0, le=10_000_000)
    schema_variant: SafeVersion
    schema_hash: SafeSha256
    records_sha256: SafeSha256
    before_fingerprint: SafeSha256
    after_fingerprint: SafeSha256

class PublishedFactorCacheSnapshot(BaseModel):
    """A descriptor-bound published factor snapshot; replay opens its object by dirfd."""
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    manifest: FactorCacheSnapshotManifest
    reader_identity: SafeSha256

class ProviderRawBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    provider_id: ProviderId
    request: ProviderRequest
    adapter_version: Literal["r2f2.v1"]
    endpoint_contract_version: Literal["r2f2-endpoints.v1"]
    started_at: datetime
    completed_at: datetime
    normalization_clock_utc: datetime
    logical_request_plan: "ExpectedLogicalRequestPlan"
    request_plan_hash: SafeSha256
    endpoint_batches: tuple[RawEndpointBatch, ...]
    request_completions: tuple["RequestCompletion", ...]
    completion_hash: SafeSha256
    transport_lineage: tuple[TransportLineageRef, ...]
    transport_observations: TransportObservationAggregate
    endpoint_summaries: tuple[EndpointContractSummary, ...]
    factor_cache_records: FactorCacheSnapshotRecords | None
    factor_resolution: tuple[FactorResolutionBinding, ...]
    factor_resolution_sha256: SafeSha256
    failure_class: SafeFailureClass | None = None

class EvidenceObjectDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    object_id: SafeIdentifier
    object_kind: EvidenceObjectKind
    relative_path: SafeRelativePath
    sha256: SafeSha256
    schema_hash: SafeSha256
    row_count: int = Field(ge=0, le=10_000_000)
    byte_count: int = Field(ge=0, le=67_108_864)
    provider_id: ProviderId
    universe_id: SafeIdentifier
    refresh_id: SafeIdentifier
    capture_id: SafeIdentifier | None = None
    provider_session_id: SafeIdentifier | None = None
    root_request_id: SafeIdentifier | None = None
    page_request_id: SafeIdentifier | None = None
    endpoint: ProviderEndpoint | None = None
    request_role: RequestRole | None = None
    instrument_role: InstrumentRole | None = None
    shard_id: SafeIdentifier | None = None
    plan_ordinal: int | None = Field(default=None, ge=0, le=4096)
    attempt: int | None = Field(default=None, ge=1, le=20)
    page: int | None = Field(default=None, ge=1, le=16_384)
    fields: tuple[SafeIdentifier, ...]
    adapter_version: Literal["r2f2.v1"]
    endpoint_contract_version: Literal["r2f2-endpoints.v1"]
    schema_variant: SafeVersion
    source_schema: SafeVersion
    units: tuple[tuple[SafeIdentifier, BoundedToken], ...]
    date_semantics: Literal["explicit_trade_date"]
    pagination_policy: PaginationPolicy
    normalization_clock_utc: datetime
    transport_observation_digest: SafeSha256 | None = None
    factor_snapshot_provenance_hash: SafeSha256 | None = None

class EvidenceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    evidence_id: SafeIdentifier
    provider_id: ProviderId
    adapter_version: Literal["r2f2.v1"]
    endpoint_contract_version: Literal["r2f2-endpoints.v1"]
    trade_date: date
    universe_id: SafeIdentifier
    requested_at: datetime
    completed_at: datetime
    normalization_clock_utc: datetime
    logical_request_plan: "ExpectedLogicalRequestPlan"
    request_plan_hash: SafeSha256
    request_completions: tuple["RequestCompletion", ...]
    completion_hash: SafeSha256
    objects: tuple[EvidenceObjectDescriptor, ...]  # ordered raw pages plus at most one factor snapshot
    transport_lineage: tuple[TransportLineageRef, ...]
    transport_observations: TransportObservationAggregate
    endpoint_summaries: tuple[EndpointContractSummary, ...]
    factor_cache_snapshot: PublishedFactorCacheSnapshot | None
    factor_resolution: tuple[FactorResolutionBinding, ...]
    factor_resolution_sha256: SafeSha256
    request_count: int = Field(ge=1, le=4096)
    attempt_count: int = Field(ge=1, le=20_000)
    failure_class: SafeFailureClass | None = None
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
    gate_report_id: SafeIdentifier
    candidate_id: SafeIdentifier
    gate_version: SafeVersion
    outcomes: tuple[GateOutcome, ...]  # exactly R2F2_GATE_ORDER, no missing/extra/duplicate
    verdict: Literal["pass", "fail"]
    aggregate_sha256: SafeSha256
    created_at: datetime

class CandidateManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    candidate_id: SafeIdentifier
    trade_date: date
    universe_id: SafeIdentifier
    provider_id: ProviderId
    evidence_id: SafeIdentifier
    evidence_sha256: SafeSha256
    normalized_object_relative_path: SafeRelativePath
    normalized_object_sha256: SafeSha256
    gate_report_relative_path: SafeRelativePath
    gate_report_sha256: SafeSha256
    factor_resolution_sha256: SafeSha256
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
    selected_provider_id: ProviderId
    reason: SelectionReason
    fallback_from: ProviderId | None
    evidence_sha256: SafeSha256
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
    row_count: int = Field(ge=0, le=10_000_000)
    trade_date: date | None
    failure_class: SafeFailureClass | None
```

`ProviderRawBatch.logical_request_plan` MUST equal `ProviderRequest.logical_request_plan`; its
`request_plan_hash` is recomputed from that plan. The batch has no caller-provided session or root
request field; the actual session set is derived from `AttemptCompletion`, page lineages and
sanitized observations during validation.

### Factor snapshot descriptor mapping (authoritative and bidirectional)

`FactorCacheSnapshotManifest` and the matching
`EvidenceObjectDescriptor(kind=factor_cache_snapshot)` are two projections of the same
published object. Their field mapping is closed and MUST be checked in both directions; a
serializer or reader MUST NOT introduce aliases or a second spelling:

| `EvidenceObjectDescriptor` | `FactorCacheSnapshotManifest` |
| --- | --- |
| `capture_id` | `capture_id` |
| `object_id` | `object_id` |
| `relative_path` | `relative_path` |
| `sha256` | `object_sha256` |
| `byte_count` | `byte_count` |
| `row_count` | `row_count` |
| `schema_variant` | `schema_variant` |
| `schema_hash` | `schema_hash` |
| `factor_snapshot_provenance_hash` | `records_sha256` |

Every left-hand value MUST equal its right-hand value and every right-hand value MUST equal its
left-hand value. In particular, `descriptor.sha256` is the manifest's `object_sha256`, while
`descriptor.schema_hash` is the manifest's `schema_hash`; neither may be omitted or substituted.
`FactorResolutionBinding.cache.cache_object_id` MUST equal this same descriptor's `object_id`,
and `cache_object_sha256` MUST equal this same descriptor's `sha256` and the manifest's
`object_sha256`. `record_key` is resolved only inside that descriptor-bound object. A cache
binding with a different descriptor, a live binding, an alias field or a live mutable cache read
fails closed.

`EvidenceObjectDescriptor` has a closed kind validator. A `raw_endpoint_page` requires
`capture_id=None`, non-null endpoint/request role/instrument role/shard/plan ordinal, attempt, page,
root/page request IDs, ordered `fields`/`units`, `pagination_policy` and
`transport_observation_digest`; its request ID and all cardinality fields must join one
final-success `AttemptCompletion`, and its digest MUST resolve to one matching `COMPLETE`
projection, never the root `OPERATION` projection. A model validator rejects a descriptor whose
attempt/outcome/root/page request IDs are not the `RequestCompletion.successful_attempt` binding.
A `factor_cache_snapshot` requires a non-null locally generated `capture_id`, null
`provider_session_id`, `root_request_id`, `page_request_id`, endpoint, request role, instrument
role, shard, plan ordinal, attempt, page and transport digest, plus a non-null
`factor_snapshot_provenance_hash`. Its descriptor fields MUST satisfy the exact bidirectional
mapping above: `capture_id` ↔ `capture_id`, `object_id` ↔ `object_id`, `relative_path` ↔ `relative_path`,
`sha256` ↔ `object_sha256`, `byte_count` ↔ `byte_count`, `row_count` ↔ `row_count`,
`schema_variant` ↔ `schema_variant`, `schema_hash` ↔ `schema_hash`, and
`factor_snapshot_provenance_hash` ↔ `records_sha256`. `EvidenceManifest.objects` MUST
contain this descriptor exactly once, and `EvidenceManifest.factor_cache_snapshot.manifest` MUST
be the same descriptor-bound identity/hash/size/row/schema/provenance projection. The cache
binding's `cache_object_id`/`cache_object_sha256` MUST resolve to this same descriptor and
manifest. Any mismatch in either direction fails closed. Thus a factor snapshot is an evidence
object kind, never a seventh provider endpoint, and cannot masquerade as a transport page.

`FactorCacheSnapshotRecords` is the in-memory read-only record set and is never replayed directly.
The writer serializes it to one immutable object, creates the `FactorCacheSnapshotManifest` and
the matching `EvidenceObjectDescriptor`, then exposes only `PublishedFactorCacheSnapshot` to the
reader. `FactorResolutionBinding.cache` MUST contain `cache_object_id`, `cache_object_sha256` and
`record_key`, with `live=None`; `cache_object_id` MUST equal the matching descriptor's `object_id`
and `cache_object_sha256` MUST equal its `sha256` and the manifest's `object_sha256`. The reader
opens that descriptor's relative path through the evidence root dirfd with `O_NOFOLLOW`, verifies
object hash/schema/byte count/row count and the records hash, and only then resolves `record_key`.
It MUST NOT query the live cache or accept an in-memory record set during replay.

The evidence writer MUST also run this aggregate validator before the first object compare-create:

```python
def validate_publishable_descriptors(
    completion: RequestCompletion,
    descriptors: tuple[EvidenceObjectDescriptor, ...],
) -> None:
    if completion.final_outcome is not TransportOutcome.SUCCESS:
        raise ValueError("ultimate failure cannot publish evidence")
    if any(
        descriptor.attempt != completion.successful_attempt
        or descriptor.root_request_id != completion.successful_root_request_id
        or descriptor.provider_session_id != completion.attempts[-1].provider_session_id
        or descriptor.page_request_id
        not in {request_id for _, request_id in completion.attempts[-1].page_request_ids}
        for descriptor in descriptors
        if descriptor.object_kind is EvidenceObjectKind.RAW_ENDPOINT_PAGE
    ):
        raise ValueError("only final successful attempt pages may be published")
```

The implementation MUST additionally compare the descriptor page set with the final attempt's
contiguous observed pages and require its terminal marker. This validator runs before staging,
manifest hashing or normalization; a failed attempt cannot be redirected into `orphan-audit`.

`SafeFailureClass` is an existing allowlist (`MarketFailureClass`, `NormalizedTransportError` and
the explicit `EVIDENCE_*`/`REPLAY_*` public categories); it is not a free-form string. All datetime
validators reject naive values and normalize to UTC. Tuple validators enforce sorted/unique
identifiers, exact counts and cross-object equality. The implementation MUST reject any model
that attempts to serialize a payload, exception, URL, path outside the declared relative field,
or a `qualified_fallback` selection.

### Frozen model file ownership

The implementation keeps model ownership disjoint between tasks:

- Task 7 `backend/app/market/providers/base.py` owns provider-safe types, endpoint/role contracts,
  logical requests, completions, raw endpoint batches and transport projections; the existing
  `backend/app/market/models.py` is touched only for necessary additive provider-source
  compatibility.
- Task 8 `backend/app/market/evidence.py` owns `EvidenceObjectDescriptor`, `EvidenceManifest`,
  `PublishedEvidence`, factor snapshot serialization and `ReplayResult`; settings/layout/CLI and
  automation only consume these models.
- Task 9 `backend/app/market/candidates.py` is the single owner of `GateOutcome`,
  `CandidateGateReport`, `CandidateManifest` and `SessionSelection`. `market/models.py` is not a
  second candidate-model owner.

The frozen contract listing is normative regardless of module placement; a task may not duplicate
or redefine a model in another module.

### Public export matrix (authoritative)

`backend.app.market.providers.base` and `backend.app.market.providers` MUST export the complete
Task 7 surface below; an import that is available only by module internals is incomplete. The
matrix is a review/test contract, not a license to export Task 8/9 models from Task 7.

| Export module | Required exports |
| --- | --- |
| `providers.base` | `SafeProviderId`, `SafeSymbol`, `SafeVersion`, `SafeIdentifier`, `SafeSha256`, `SafeRelativePath`, `ProviderId`, `ProviderEndpoint`, `InstrumentRole`, `RequestRole`, `EvidenceObjectKind`, `PaginationPolicy`, `ENDPOINT_CONTRACTS`, `EndpointContract`, `ExpectedLogicalRequest`, `ExpectedLogicalRequestPlan`, `AttemptCompletion`, `RequestCompletion`, `ProviderRequest`, `TransportLineageRef`, `TransportObservationProjection`, `TransportObservationAggregate`, `RawEndpointRow`, all nine concrete endpoint-row variants, `RawEndpointBatch`, `EndpointContractSummary`, `ProviderRawBatch`, `validate_raw_date_binding`, `provider_registry`, and the exact version constants. |
| `providers` package | Every provider-neutral type in the preceding row plus `BaoStockProviderAdapter`/`BaoStockDailyBarAdapter`; no `PublishedEvidence`, `EvidenceManifest`, `CandidateManifest` or `SessionSelection`. |

`__all__` and direct imports MUST agree with this matrix. The exact version constants are compared
by value in tests; a generic `SafeVersion` acceptance is insufficient.

### Model ↔ persisted-field audit (current closure)

The following manual diff is the review authority for persisted field names. The implementation
review MUST repeat this comparison from the actual models and SQL/JSON serializers; a validator
score cannot substitute for it.

| Model | Persisted projection / source | Field-diff result |
| --- | --- | --- |
| `RawEndpointBatch` / `EndpointContractSummary` | `ENDPOINT_CONTRACTS` | `endpoint`, role, variant, ordered `fields`, `source_schema`, exact `units`, `date_semantics` and `pagination_policy` all present; no hidden summary field. |
| `TransportObservationProjection` / `TransportLineageRef` | Sanitized transport aggregate / final page lineage | Registry-derived `plan_ordinal`/`lineage_kind` and digest are exact; lineage refs are final-success `protocol_stage=COMPLETE` only, while `AttemptCompletion.operation_observation_digest` binds the matching root `OPERATION`. |
| `ProviderRequest` / `ProviderRawBatch` | Logical request envelope / actual transport lineage | Request contains no provider-session/request identity; actual session IDs are derived from page/observation joins, and each completion/page uses root/page IDs. Adapter and endpoint-contract versions equal exact current constants. |
| `FactorCacheSnapshotRecord` / `FactorCacheSnapshotRecords` | Existing `factor_snapshots` SQL table / in-memory projection | Exact nine current columns plus derived `row_fingerprint`, `records_sha256` and before/after fingerprints; no invented `source`, `effective`, `observed`, `adjust_factor` or cache-version column. This record set is not a published descriptor. |
| `FactorCacheSnapshotManifest` / `PublishedFactorCacheSnapshot` | Published factor object manifest / descriptor-bound reader | Exactly `capture_id`, `object_id`, `relative_path`, `object_sha256`, `byte_count`, `row_count`, `schema_variant`, `schema_hash`, `records_sha256`, before/after fingerprints plus reader identity; no embedded mutable rows. |
| `EvidenceObjectDescriptor` | Evidence JSON descriptor | One `units` field only; raw page carries `capture_id=None` plus `plan_ordinal/attempt/root_request_id/page_request_id/page/object_kind`, while cache snapshot carries a locally generated `capture_id` and null provider/session/page transport identity. The exact bidirectional mapping includes `capture_id↔capture_id`, `object_id↔object_id`, `relative_path↔relative_path`, `sha256↔object_sha256`, `byte_count↔byte_count`, `row_count↔row_count`, `schema_variant↔schema_variant`, `schema_hash↔schema_hash`, `factor_snapshot_provenance_hash↔records_sha256`; cache binding identity/hash MUST match this same descriptor. |
| `EvidenceManifest` | Evidence manifest JSON | `factor_resolution_sha256` is the single ordered binding hash and is identical in model, serializer and table; there is no singular provider-session field, and observations aggregate across actual sessions. `attempt_count` excludes login audit. |
| `CandidateGateReport` | Gate report JSON | `gate_report_id`, `verdict` and `aggregate_sha256` are identical in model, serializer and table. |
| `CandidateManifest` / `SessionSelection` | Candidate/selection JSON | Candidate fields are exactly `candidate_id`, `trade_date`, `universe_id`, `provider_id`, `evidence_id`, `evidence_sha256`, `normalized_object_relative_path`, `normalized_object_sha256`, `gate_report_relative_path`, `gate_report_sha256`, `factor_resolution_sha256`, `adapter_version`, `source_schema_version`, `row_count`, `required_symbol_count`, `status`, `manifest_sha256`; selection fields are exactly the model fields below. |
| `ReplayResult` | CLI/public projection | Frozen normalization clock only; no invocation/start-time field. |

The field audit is structural, not a self-matching search. Reviewers MUST extract the field names
from the actual Pydantic models and their SQL/JSON serializers, compare them with the tables above,
and record any difference as a finding. The current authoritative names are `gate_report_id`,
`verdict`, `aggregate_sha256`, `evidence_sha256`, `normalized_object_sha256`,
`gate_report_sha256`, `source_schema_version`, `factor_resolution_sha256` and
`normalization_clock_utc`; no alias or duplicated spelling may be introduced. The review MUST NOT
claim closure from a prose token search; it must compare structured model/serializer fields with the
tables and report concrete differences.

The logical request plan and post-fetch completion are separate frozen contracts:

```python
class ExpectedLogicalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    plan_ordinal: int = Field(ge=0, le=4096)
    endpoint: ProviderEndpoint
    request_role: RequestRole
    instrument_role: InstrumentRole | None
    schema_variant: SafeVersion
    shard_id: SafeIdentifier
    symbols: tuple[SafeSymbol, ...]
    start_date: date
    end_date: date
    pagination_policy: PaginationPolicy

    @model_validator(mode="after")
    def validate_call_symbols(self) -> "ExpectedLogicalRequest":
        if self.endpoint in {ProviderEndpoint.TRADE_DATES, ProviderEndpoint.ALL_STOCK}:
            if self.symbols:
                raise ValueError("calendar and universe calls require empty per-call symbols")
        elif not self.symbols:
            raise ValueError("non-calendar/non-universe calls require per-call symbols")
        return self

class ExpectedLogicalRequestPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    requests: tuple[ExpectedLogicalRequest, ...]
    request_count: int = Field(ge=1, le=4096)
    request_plan_hash: SafeSha256

class AttemptCompletion(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    plan_ordinal: int = Field(ge=0, le=4096)
    provider_session_id: SafeIdentifier  # actual incumbent scope; never caller supplied
    attempt: int = Field(ge=1, le=20)
    root_request_id: SafeIdentifier  # actual query-root request_scope ID
    operation_observation_digest: SafeSha256  # matching query-root ProtocolStage.OPERATION
    observed_pages: tuple[int, ...]  # empty only when no page was received; otherwise contiguous
    page_request_ids: tuple[tuple[int, SafeIdentifier], ...]  # actual page request_scope IDs
    observed_page_count: int = Field(ge=0, le=16_384)
    terminal: bool
    outcome: TransportOutcome

    @model_validator(mode="after")
    def validate_observed_pages(self) -> "AttemptCompletion":
        if self.observed_page_count != len(self.observed_pages):
            raise ValueError("observed page count mismatch")
        if self.observed_pages and self.observed_pages != tuple(
            range(1, self.observed_page_count + 1)
        ):
            raise ValueError("observed pages must be contiguous")
        if tuple(page for page, _ in self.page_request_ids) != self.observed_pages:
            raise ValueError("page request IDs must close over observed pages")
        if len({request_id for _, request_id in self.page_request_ids}) != len(self.page_request_ids):
            raise ValueError("page request IDs must be unique")
        if self.outcome == TransportOutcome.SUCCESS and not self.terminal:
            raise ValueError("successful attempt must be terminal")
        return self

class RequestCompletion(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    plan_ordinal: int = Field(ge=0, le=4096)
    attempts: tuple[AttemptCompletion, ...]  # ordered by attempt; IDs are actual, unique and joined
    successful_attempt: int | None = Field(default=None, ge=1, le=20)
    successful_root_request_id: SafeIdentifier | None = None
    final_outcome: TransportOutcome
    row_count: int = Field(default=0, ge=0, le=10_000_000)

    @model_validator(mode="after")
    def validate_final_attempt(self) -> "RequestCompletion":
        if not self.attempts:
            raise ValueError("completion requires an attempt")
        if tuple(attempt.attempt for attempt in self.attempts) != tuple(
            range(1, len(self.attempts) + 1)
        ):
            raise ValueError("attempts must be strictly ordered")
        if len({attempt.root_request_id for attempt in self.attempts}) != len(self.attempts):
            raise ValueError("attempt request IDs must be unique")
        if self.final_outcome != self.attempts[-1].outcome:
            raise ValueError("final outcome must match final attempt")
        success_count = sum(
            attempt.outcome is TransportOutcome.SUCCESS for attempt in self.attempts
        )
        if self.final_outcome == TransportOutcome.SUCCESS:
            if success_count != 1:
                raise ValueError("a completion has exactly one final successful attempt")
            if self.successful_attempt is None or self.successful_root_request_id is None:
                raise ValueError("successful completion requires final request binding")
            final = self.attempts[-1]
            if final.attempt != self.successful_attempt:
                raise ValueError("successful attempt must be final")
            if final.root_request_id != self.successful_root_request_id or final.outcome != TransportOutcome.SUCCESS:
                raise ValueError("successful request binding mismatch")
        else:
            if success_count != 0:
                raise ValueError("a failed completion cannot contain a successful attempt")
            if self.successful_attempt is not None or self.successful_root_request_id is not None:
                raise ValueError("failed completion cannot bind a successful attempt")
            if self.row_count != 0:
                raise ValueError("failed completion row count must be zero")
        return self
```

The plan includes all symbol shards and explicit index requests, but never predicts the provider's
page count. `request_plan_hash` is SHA-256 of the canonical logical-request tuple without its hash
field. After execution, each logical query emits exactly one `RequestCompletion`; every attempt has
the actual provider session ID and a fresh query-root `root_request_id`, and every observed page has
a fresh actual `page_request_id`. Attempt numbers are strictly ordered, and non-empty observed pages
are unique, start at page 1 and are contiguous. Login/relogin observations are assigned the active
`plan_ordinal` as sanitized aggregate observations and are excluded from completions and `attempt_count`.
Exactly one successful attempt/root request ID is allowed
when `final_outcome=success`; it MUST be the final attempt and MUST be terminal. A successful
attempt is terminal: no later attempt is legal, and two successful attempts are never legal. All
earlier attempts in a successful completion are non-success transport outcomes. A failed final
attempt has `final_outcome` equal to its final non-success outcome, zero `row_count`, null
successful fields and cannot publish evidence. A transport failure before page 1 is represented by
an empty page tuple; no page is invented. `AttemptCompletion` deliberately has no source
`row_count`, row values or page bytes: failed attempts may retain only bounded F0.1 page/recv
 counters, outcome, actual root/page IDs and sanitized observation digest. Only the final
successful attempt's pages receive `EvidenceObjectDescriptor` and `TransportLineageRef` records. For a successful
completion, `RequestCompletion.row_count` MUST equal the sum of the final successful descriptors'
read-back row counts; failed-attempt counters are excluded from that sum, manifest `row_count` and
all object/hash inputs. The completion validator MUST bind every descriptor to the successful
root/page request IDs, actual provider session, contiguous pages and terminal marker before any
object compare-create.
`completion_hash` is SHA-256 of the ordered completion/attempt/page-descriptor tuple without its
hash field and is independent of the logical request-plan hash. Missing, duplicate,
non-contiguous, out-of-order, swallowed pages, duplicate root/page IDs, success-after-success,
success-before-later-attempt or inconsistent final success/failure cardinality fail closed. No
total-page guess is required.

| Identity model | Frozen fields | Cardinality and lineage rule |
| --- | --- | --- |
| `AttemptCompletion` | `plan_ordinal`, actual `provider_session_id`, `attempt`, actual `root_request_id`, matching `operation_observation_digest`, contiguous `observed_pages`, `(page, page_request_id)` pairs, `observed_page_count`, `terminal`, `outcome` | One row per logical query attempt. The root ID and operation digest come from query-root scope; page IDs are generated by page scopes; login/relogin audit is excluded. |
| `RequestCompletion` | `plan_ordinal`, ordered attempts, `successful_attempt`, `successful_root_request_id`, `final_outcome`, `row_count` | Exactly one row per `ExpectedLogicalRequest`. Terminal outcome is decided only by the matching root `ProtocolStage.OPERATION`; success binds the final attempt/session/page set, while failure has zero rows and no successful root. |
| `RawEndpointBatch` | endpoint/role/schema, plan/shard, actual page lineage, typed rows, exact source schema/fields/units/date/pagination, row/order digest | Exactly one final-success page/shard. Its page lineage digest joins exactly one sanitized projection; failed-attempt source rows are absent. |

### Provider registry

| Field | Type | Constraint |
| --- | --- | --- |
| provider_id | `ProviderId` | Static allowlist; only `ProviderId.BAOSTOCK` is admitted |
| adapter_version | safe version | Pinned code contract |
| endpoint_contract_version | safe version | Fixed six-endpoint contract |
| supported_fields | tuple[str, ...] | Endpoint allowlist |
| date_semantics | literal | `explicit_trade_date` |
| volume_unit | literal | `shares` |
| amount_unit | literal | `CNY` |
| adjustment_semantics | literal | unadjusted OHLCV plus `backAdjustFactor` |
| admission_state | literal | R2-F2 only `qualified` for existing primary; no secondary |

### Evidence manifest

The table below is an exact projection of the frozen `EvidenceManifest` model above; there are no
additional summary fields hidden in prose. Per-endpoint `source_schema`, `units` and
`date_semantics` are carried by `RawEndpointBatch` and aggregated into
`endpoint_summaries`; the manifest is verifiable by summing descriptor and completion counts.

| Field | Type | Constraint / verification |
| --- | --- | --- |
| evidence_id / provider_id | safe identifier / `ProviderId` | Deterministic ID and static provider |
| adapter_version / endpoint_contract_version | safe version | Exact pinned contracts |
| trade_date / universe_id | date / safe identifier | Exact session scope |
| requested_at / completed_at / normalization_clock_utc | UTC datetime | ordered; frozen clock used by normalizer/replay |
| logical_request_plan / request_plan_hash | `ExpectedLogicalRequestPlan` / SHA-256 | Logical requests only; hash recomputed before publish |
| request_completions / completion_hash | ordered `RequestCompletion` / SHA-256 | Observed contiguous pages and terminal markers; no expected total |
| request_count / attempt_count | bounded non-negative integers | Request count equals plan count; `attempt_count == sum(len(completion.attempts) for completion in request_completions)` and excludes login audit, observation and page counts. `object_count == len(objects)` including an optional factor descriptor; manifest source `row_count` sums raw-page descriptors only. Actual provider-session IDs are derived from descriptors/observations across the aggregate; no singular session field is admitted. |
| objects | `tuple[EvidenceObjectDescriptor, ...]` | Exactly one descriptor per final-success logical request/page/shard; failed attempts have none |
| transport_lineage / transport_observations | refs + `TransportObservationAggregate` | Every published final-success page is bound to an exact allowlisted projection; failed attempts may appear only as sanitized aggregate observations |
| endpoint_summaries | `tuple[EndpointContractSummary, ...]` | Exact source schema/units/date semantics, counts equal descriptor sums |
| factor_cache_snapshot / factor_resolution | `PublishedFactorCacheSnapshot` + ordered bindings | Descriptor-bound object identity/size/schema/row/records hashes; actual cache keys only |
| factor_resolution_sha256 | SHA-256 | Hash of ordered live/cache resolution bindings; enters candidate lineage |
| object_count / row_count | bounded integers | `object_count == len(objects)` including an optional factor descriptor; `row_count` sums only `RAW_ENDPOINT_PAGE` descriptor rows; factor snapshot rows are counted only in its own descriptor/manifest; failed-attempt counters never contribute |
| failure_class | allowlisted optional class | No raw provider message/exception |
| manifest_sha256 | SHA-256 | Canonical manifest bytes excluding its own hash |

### Candidate gate report

The following aggregate is the only persisted gate contract. The row-oriented table is a field
summary; it MUST NOT be implemented as one file per loose gate or as a free-form list.

| Field | Type | Constraint |
| --- | --- | --- |
| gate_report_id | safe identifier | Immutable |
| candidate_id | safe identifier | One candidate |
| gate_version | safe version | Pinned policy |
| outcomes | ordered tuple of `GateOutcome` | Exactly the ten names in `R2F2_GATE_ORDER`; no missing/extra/duplicate/reordered outcome |
| verdict / aggregate_sha256 | literal / hash | All ten outcomes agree; hash excludes itself |
| created_at | UTC datetime | Required |

The aggregate MUST contain exactly the ten ordered names in `R2F2_GATE_ORDER`, exactly one outcome
per name, and `aggregate_sha256` is the canonical JSON hash of the report without that field.
`verdict` is `pass` only when every outcome is `pass`; a missing, extra, duplicate,
out-of-order or hash-mismatched outcome is a fail-closed report and cannot be selected. The exact
ten names and their current-code evidence are `R2F2_GATE_EVIDENCE`; this is the only gate list.
The existing five-string `MarketDataStatus.publication_gates` response field remains a legacy
public description for compatibility; it is not a persisted R2-F2 candidate gate list, must not be
copied into `CandidateGateReport`, and is not extended or reinterpreted by this version.

### Candidate manifest

| Field | Type | Constraint |
| --- | --- | --- |
| candidate_id | safe identifier | Full-session identity |
| trade_date / universe_id | date / safe identifier | Exact scope |
| provider_id | ProviderId | One provider only |
| evidence_id / evidence_sha256 | ID/hash | Exact evidence manifest |
| normalized_object_relative_path | SafeRelativePath | Non-serving candidate Parquet path |
| normalized_object_sha256 | SafeSha256 | Hash of the normalized candidate object |
| gate_report_relative_path | SafeRelativePath | Complete gate report path |
| gate_report_sha256 | SafeSha256 | Hash of the aggregate gate report |
| factor_resolution_sha256 | hash | Exact ordered live/cache factor-resolution object hash |
| adapter_version / source_schema_version | safe version | Exact adapter and source schema contracts |
| row_count / required_symbol_count | non-negative int | Exact readback |
| status | `accepted` / `rejected` | Rejected candidates retained |
| manifest_sha256 | SafeSha256 | Canonical candidate manifest bytes excluding its own hash |

### Session selection

| Field | Type | Constraint |
| --- | --- | --- |
| selection_id | safe identifier | Immutable |
| trade_date / universe_id | date / safe identifier | Exact canonical partition |
| selected_candidate_id | safe identifier | Exactly one complete candidate |
| selected_provider_id | ProviderId | `ProviderId.BAOSTOCK` in R2-F2 |
| reason | `SelectionReason` | R2-F2 writer/orchestrator/validator accepts only `primary_ready`; `qualified_fallback` is schema-reserved and MUST be rejected |
| fallback_from | nullable `ProviderId` | Must be null in R2-F2; any non-null or fallback reason fails |
| evidence_sha256 / candidate_manifest_sha256 / gate_report_sha256 | hashes | Complete lineage |
| selected_at | UTC datetime | Required |
| selection_sha256 | SafeSha256 | Canonical selection bytes excluding its own hash |

### Additive canonical manifest lineage

New file entries MAY carry these fields while retaining the current dataset manifest format and all
legacy fields: `provider_id`, `universe_id`, `evidence_id`, `evidence_sha256`, `candidate_id`,
`candidate_manifest_sha256`, `gate_report_sha256`, `adapter_version`, `source_schema_version`.
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
| `daily_astock.v1` | `query_daily_history_k_AStock(date=ISO)`; exact `DAILY_FIELDS`: `date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST` | `date`, `code`, `open`, `high`, `low`, `close`, `preclose`, `adjustflag` and `isST` are required and empty values reject. Only `volume`, `amount`, `turn` and `pctChg` may map source `""` to `None`, and only when this is a stock row with `tradestatus != "1"`; active blank activity rejects. A suspended row may retain finite zero activity at this typed boundary, but the existing quality gate rejects non-zero suspended activity. `adjustflag: BoundedToken` MUST be `"3"`; active tokens remain exact and no index nullable placeholder is legal. Units: volume `shares`, amount `CNY`, turnover `percent`, pctChg `percent`. | One exact trade date; pages must advance. Rows are unique by `(code,date)` and sorted by `(date,code)` in evidence. Date mismatch, duplicate symbol, non-finite value, suspended index or non-zero suspended activity fails the full batch. |
| `daily_factor.v1` | `query_daily_adjust_factor(date=ISO)`; exact `AdjustmentFactorCache.FACTOR_FIELDS`: `code,dividOperateDate,foreAdjustFactor,backAdjustFactor,adjustFactor` | `code: SafeSymbol` and MUST equal the logical request symbol vocabulary; `dividOperateDate` is the provider event/effective date, not an invented `date` key, and MUST be `<=` the requested session. The request date selects the provider event feed through that date; it does not require every event's effective date to equal the request date. Three factors are finite positive numeric; no null/empty factor values. An empty event response is raw evidence only; Task 8's factor-cache snapshot and later factor gate must prove resolution. | A date event request may have pages; rows are ordered by `(dividOperateDate,code)` in evidence. Duplicate `(dividOperateDate,code)` or future event fails. |
| `adjust_factor.session.v1` | `query_adjust_factor(stock_symbol,start_date="1990-01-01",end_date=ISO)`; same exact five factor fields | Same typed rules as `daily_factor.v1`; this is a distinct request variant because it is a per-stock history through the requested end date and may be empty for a legal suspended placeholder. It MUST NOT be used to invent an active stock factor; `dividOperateDate` remains the provider event/effective date and need not equal the request end date. | Rows unique by `(dividOperateDate,code)` and sorted by `(dividOperateDate,code)` in evidence; every row's code equals the requested stock symbol. |
| `index_history.session.v1` | `query_history_k_data_plus(symbol, DAILY_FIELDS, start_date=ISO,end_date=ISO,frequency="d",adjustflag="3")`; exact 14 daily fields | Same typed daily rules as `daily_astock.v1`; the requested symbol role is discriminated: `index_symbol` MUST be one of existing `INDEX_SYMBOLS` and receives no stock factor; `stock_symbol` is not an index. Units are identical to daily bars. | One exact date/symbol; zero rows is missing and fails required coverage. One row is expected. |
| `index_history.range.v1` | `query_history_k_data_plus(symbol, DAILY_FIELDS, start_date=ISO,end_date=ISO,frequency="d",adjustflag="3")` in `fetch_range`; exact 14 daily fields | Same fields/types as `index_history.session.v1`, but the request range MAY contain multiple trading dates. Every row's code equals the requested symbol; no date outside range. | Rows unique and sorted by `(date,code)`; a repeated date or non-advancing page is `PAGINATION_STALLED`/schema failure. |

The two `index_history` schema variants are each admitted for both `InstrumentRole.INDEX` and
`InstrumentRole.STOCK`, because the current provider calls fetch the two required index symbols and
explicit stock symbols through the same SDK method. The two factor variants are explicit
request/schema variants, not a union of arbitrary rows. These nine entries (one calendar, one
universe, one daily stock, one daily factor, one adjust-factor, and four stock/index history
variants) are exactly the `ENDPOINT_CONTRACTS` table; no seventh endpoint is introduced. The
provider's SDK iterator may expose pages in arrival order; the adapter records page/attempt
lineage and emits the deterministic evidence order shown above. A provider row order that cannot
be deterministically normalized is a complete candidate failure.

### Endpoint role cross-validator

`RequestRole` and `InstrumentRole` are validated as a closed cross-product before any object is
written. The only permitted combinations are:

| Endpoint | Request role | Instrument role | Required symbol rule |
| --- | --- | --- | --- |
| `trade_dates` | `calendar` | `None` | `ExpectedLogicalRequest.symbols == ()`; exact requested date/range |
| `all_stock` | `universe` | `stock` | `ExpectedLogicalRequest.symbols == ()`; universe rows are stock symbols |
| `daily_astock` | `daily_stock` | `stock` | non-empty per-call symbols are a shard of `ProviderRequest.session_symbols` |
| `daily_factor` | `daily_factor` | `stock` | non-empty per-call symbols are declared stock factor keys |
| `adjust_factor` | `adjust_factor` | `stock` | non-empty per-call symbols; exactly one requested stock symbol per shard |
| `index_history` | `index_history` | `index` or `stock` | non-empty per-call symbols; `index` requires `INDEX_SYMBOLS`; `stock` is an explicit compatibility variant only |

`index_history.session.v1` and `index_history.range.v1` therefore carry an explicit role rather
than inferring index status from a symbol string. A stock-role index-history request is validated
against the stock request plan and cannot satisfy required index coverage; an index-role request
cannot consume stock factors. Any endpoint/role/schema-variant mismatch, mixed role in one batch,
or index symbol outside the current `INDEX_SYMBOLS` set invalidates the complete batch.

### Semantic boundary retained from the current normalizer

- `trade_dates` is the only calendar authority for the requested source date; arrival time,
  local weekday and inferred dates are forbidden.
- `all_stock` is the source universe snapshot. Main-board membership and required indexes are
  checked against the declared `universe_id`; unknown/duplicate symbols fail closed.
- `daily_astock` and `index_history` use `adjustflag="3"`; canonical OHLCV remains unadjusted.
- `daily_factor` and `adjust_factor` provide `backAdjustFactor` by exact logical symbol and provider
  event/effective `dividOperateDate`; the selected factor is the latest eligible event date `<=
  trade_date`. The provider query date/end date controls the event feed through that date; it does
  not fabricate a `date` key or require `dividOperateDate == trade_date`. Active stock rows without
  a finite positive factor fail. An empty `daily_factor` event is only raw evidence; Task 8's typed
  factor-cache snapshot and later factor gate must prove the selected value.
- `tradestatus == "1"` is the current active classifier. A non-`"1"` row is not a license to
  infer a new suspension meaning: the existing placeholder and activity gates remain authoritative.
  Only source `volume`, `amount`, `turn` and `pctChg` may translate `""` to `None`, and only for a
  stock row with `tradestatus != "1"`; required date/code/OHLC/preclose/adjustflag/isST empties
  reject. A suspended stock may retain finite zero activity at the typed boundary, while the
  existing gate rejects non-zero suspended activity. A suspended index, active blank activity,
  malformed placeholder, duplicate or coverage mismatch fails.
- Source symbols retain the `sh.`/`sz.` six-digit form and canonical symbols are lowercase.

The DATE gate is deliberately split at the evidence boundary. The adapter/evidence module MUST
implement `validate_raw_date_binding(request, batch)` before any call to
`normalize_baostock_rows`: it compares every raw date-bearing row and request range to the exact
`ProviderRequest.trade_date`/logical plan and rejects a cross-day batch with zero candidate
publication. The existing publication identity gate then compares the normalized session identity
to `run_publication_refresh(..., trade_date=...)`/`RefreshResult.requested_date` before the current
canonical chain runs. `normalize_baostock_rows` parses each row's own date and enforces
`adjustflag="3"`, but it is not claimed to validate the requested-date binding; `normalize.py` is
outside the R2-F2 whitelist and MUST NOT be modified for this gate.

### Factor-cache evidence boundary

The current factor call chain is authoritative and is frozen as follows:
`BaoStockProvider._main_board_factor_snapshot()` calls
`AdjustmentFactorCache.stream_through()`, `record_daily_events()`,
`materialize_from_stream()`, `exact_snapshots()`, optional `record_bootstrap()`, and finally
`normalized_rows()`; the cache's `FACTOR_FIELDS` remains the exact five-field provider contract.
R2-F2 adds no seventh provider endpoint. It captures a separate immutable
`EvidenceObjectKind.factor_cache_snapshot` using a new strict read-only
`AdjustmentFactorCache.exact_snapshot_records(symbols, trade_date)` method. The method returns
only rows actually selected from the existing `factor_snapshots` table, with exactly these current
columns: `symbol`, `trade_date`, `fore_adjust_factor`, `back_adjust_factor`, `evidence_kind`,
`evidence_effective_date`, `evidence_observed_on`, `source_row_hash` and `observed_at`; it adds only
a deterministic `row_fingerprint` derived from those returned values. It does not add or infer
`source`, `effective_date`, `observed_date`, `adjust_factor`, cache-version or provenance columns,
and it performs no schema migration. It MUST NOT copy unrelated cache rows, database paths, SQL,
tokens or mutable cache diagnostics.

The capture runs inside the same outer `RefreshRunLock` as online evidence publication. Immediately
before the exact snapshot read, Task 8 generates one locally unique `capture_id`; it is not a
provider/session/request ID and cannot be borrowed from the last provider call. The same ID is
stored in `FactorCacheSnapshotManifest.capture_id` and the matching descriptor, with exact
bidirectional validation. The adapter obtains a stable read-only `FactorCacheSnapshotRecords` set
and a before/after fingerprint
of the selected keys; any missing key, cache mutation, schema mismatch or fingerprint change fails
closed. The writer serializes those records to one immutable object and publishes a
`FactorCacheSnapshotManifest` plus matching `EvidenceObjectDescriptor`; the
`PublishedFactorCacheSnapshot` is the only factor input to online normalization and replay,
neither path reads the live mutable cache after capture. If the live `daily_factor` or
`adjust_factor` endpoint
also supplies a factor row, its raw endpoint object remains in `RawEndpointBatch` and the
`FactorResolutionBinding` records whether the selected provenance came from the cache snapshot or
that endpoint row. A binding mismatch, missing selected object or unproven source is a complete
candidate failure. Task 8 owns the read-only cache method and its schema/immutability tests,
serializes the records, manifest and binding in `market/evidence.py`, and adds no factor-cache
writer or live-cache replay path. The ordered binding hash `factor_resolution_sha256` is present in
both the evidence manifest and candidate manifest. Replay must open the published descriptor by
dirfd, verify its object hash/schema/size/row/records hashes, and reject any manifest/descriptor
or binding mismatch in either direction.

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

The date mismatch MUST be rejected by `validate_raw_date_binding(request, batch)` before
`normalize_baostock_rows` is called; the existing publication requested-date/session identity gate
must reject any normalized identity mismatch. This criterion does not authorize a `normalize.py`
change or claim that the normalizer performs the request-date check.

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
each of the six endpoint IDs is accepted only through an `ENDPOINT_CONTRACTS` entry, calendar and
universe logical requests require empty per-call symbols while other roles require exact non-empty
shards, and a `model_copy` or round-trip that changes role, fields, units, date semantics,
pagination policy or symbol vocabulary also fails closed. The Task 8 factor snapshot manifest and
descriptor must likewise reject any model-copy/round-trip identity, hash, size, row or schema
mismatch in either direction.

### AC-17: Request/object/transport cardinality (FR-30–FR-31, NFR-2–NFR-6, NFR-16)

Given multiple symbols, indexes, retries or SDK pages for one refresh
When Task 8 assembles its evidence manifest
Then every logical plan item has exactly one completion and every observed page has exactly one
descriptor and bound sanitized observation lineage only when it belongs to the final successful
attempt; login/relogin observations are audit-only and excluded from completions and attempt_count;
failed attempts retain only bounded page counters/outcomes/actual IDs and never create evidence
files or source row counts. Every attempt has an actual provider-session ID and query-root request
ID, every page has an actual page request ID, observed pages are contiguous, and exactly one final
successful attempt or final failure is recorded, with no missing,
extra, duplicate, out-of-order or swallowed page. A retry that succeeds publishes only final-attempt
pages and `RequestCompletion.row_count` equals the final descriptor read-back row sum. An ultimate
failure has `row_count=0`, publishes no evidence manifest, does not normalize, and cannot create a
candidate, selection or pointer mutation. A competing writer performs zero canonical writes. The
plan does not require a pre-fetched total page count.

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
cause zero writes. `SafeRelativePath` construction and `model_copy` reject lexical component
violations, while `open_evidence_relative(root_dirfd, path, flags)` independently enforces
dirfd/O_NOFOLLOW descriptor containment and TOCTOU identity checks. A factor snapshot replay
accepts only a descriptor-bound object whose ID/path/hash/bytes/rows/schema/records hashes agree
with both the published factor manifest and the evidence descriptor; live-cache reads are zero.

### AC-20: Fallback and validator boundaries (NFR-18, FR-27)

Given an attempted `qualified_fallback`, second provider/plugin, shadow/failover control or an
implementation-plan validator invocation
When the R2-F2 writer/orchestrator/review gate runs
Then the fallback/second-source path is rejected or out of scope, the design-only validator is
reported only as a structural result, and manual implementation traceability remains required.

### AC-21: Actual provider/query/page identity (FR-2, FR-30–FR-31, NFR-19)

Given a request plan containing login, relogin, retry and pagination observations
When the incumbent scopes and Task 7/8 validators assemble the lineage
Then `ProviderRequest` has no provider-session or transport request identity, each login/relogin
gets a unique actual provider session without caller override, each query attempt has one actual
query-root ID, each page has one actual page-request ID, page 1 reuses the root ID, and an internal
request rebind may use only the already-generated saved login ID. Login audit does not create a
completion, and only the matching `ProtocolStage.OPERATION` query-root observation determines the
attempt terminal outcome. Each final page descriptor joins exactly one `COMPLETE` observation,
never the root OPERATION digest; failed partial pages create no lineage/object. The optional factor
descriptor has a locally unique `capture_id` and null provider/session/page transport fields.
Every final page descriptor joins its exact
`(refresh_id, provider_session_id, root_request_id, page_request_id, endpoint, attempt, page)`;
duplicate/missing/extra/out-of-order IDs fail closed. `object_count == len(objects)`, manifest
`row_count` sums raw-page descriptors only, and `attempt_count` is the sum of completion attempts,
excluding login audit/observation/page counts.

## Edge cases

- EC-1: Empty provider ID, invalid slug, dynamic import request or arbitrary registry value
  fails before transport.
- EC-2: Provider endpoint returns an unknown field, duplicate field, wrong row length or
  malformed type; the complete raw batch is rejected.
- EC-3: Provider returns a date outside the exact requested session or rows from two sessions;
  `validate_raw_date_binding(request, batch)` rejects the complete batch before normalization and
  evidence is not published.
- EC-4: Provider status code is unknown; preserve bounded `provider_code` and map only to
  `UNKNOWN_PROVIDER_PROTOCOL_ERROR`.
- EC-5: Transport emits short header, EOF, bad compression, send error, receive timeout or
  pagination stall; no evidence/candidate is published and existing circuit semantics apply.
- EC-6: Evidence object exceeds 64 MiB, metadata exceeds 1 MiB or decompression expands beyond
  the bound; reject before normalization.
- EC-7: Evidence object is a symlink, empty/repeated/trailing component, `.`, `..`, backslash,
  NUL, absolute/traversing path, directory or substituted inode; the pure lexical
  `SafeRelativePath` validator rejects component violations and descriptor-bound
  `open_evidence_relative`/dirfd/O_NOFOLLOW containment rejects filesystem violations.
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
- EC-19: Replay CLI receives credentials, token, header, cookie, URL, provider, local path or
  unknown arguments; the parser rejects them before constructing the evidence layout or reader,
  emits no sensitive value, performs zero provider/network calls, and leaves root tree/bytes/mtime
  unchanged.
- EC-20: Missing evidence root/database/table on GET/plan/replay; return unavailable and do not
  initialize directories, databases, tables or pointers.
- EC-21: Expected request plan contains a missing, extra, duplicate, out-of-order or swallowed
  request/shard/page, duplicate attempt request ID, or inconsistent final success/failure;
  `AttemptCompletion`/`RequestCompletion` validation fails before evidence publication. Failed
  partial rows/page bytes are discarded in memory, no evidence file is created, a later successful
  retry publishes only its final pages, and an ultimate failure creates no manifest/candidate/
  selection/pointer.
- EC-22: A descriptor has transport IDs/endpoint/attempt/page that do not match its sanitized
  observation digest, or a descriptor belongs to a failed/non-final attempt; the complete evidence
  chain is unavailable and no failed-payload quarantine object is permitted.
- EC-23: Candidate gate outcomes omit, duplicate, reorder or add a gate; the aggregate report and
  candidate fail closed and no selection is published.
- EC-24: Online/replay normalization uses different frozen clocks or invocation time enters a
  result/hash; deterministic comparison fails closed.
- EC-25: Evidence root/ancestor is a symlink, wrong inode, non-directory or cross-root path, or a
  reader attempts implicit initialization; reject and preserve all pre-read fingerprints.
- EC-26: A compare-create/no-clobber collision finds different bytes at an existing content path;
  retain both audit contexts if possible, never overwrite and do not move the canonical pointer.
- EC-27: A caller supplies `provider_session_id`, root request ID or page request ID on
  `ProviderRequest`, or an external caller attempts an override; reject before transport. An
  internal `_request_scope(saved_actual_id)` rebind is valid only when the ID was generated by the
  current incumbent login scope; a relogin must use no-argument scope and produce a different ID.
- EC-28: Login/relogin `COMPLETE`/error observations are mistaken for query completion, or a scope
  event has adapter-invented `plan_ordinal`/`lineage_kind`; derive the fields in the capture
  registry, keep login observations aggregate-only, and fail the logical completion/manifest closed
  if the unique root OPERATION/page COMPLETE mapping is absent.
- EC-29: A final-success page uses a different provider session, duplicate page request ID,
  non-contiguous page set, OPERATION digest instead of COMPLETE digest, or mismatched
  endpoint/role/schema/version; reject the complete batch before object compare-create. A failed
  attempt with partial rows creates no lineage, descriptor or object.
- EC-30: A factor snapshot descriptor lacks its locally unique `capture_id`, borrows provider
  session/page identity, or changes `object_count`, raw-page-only manifest `row_count` or completion-
  attempt `attempt_count`; reject the manifest. A blank active/required field, suspended index,
  non-zero suspended activity, factor row whose code is outside the logical symbols, unsorted
  `(dividOperateDate,code)` factor rows, invented factor `date` key, unordered calendar or
  duplicate/out-of-range calendar row is presented; the typed adapter rejects the batch.

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
- OS-9: Persisting failed-attempt partial pages as quarantined forensic objects; R2-F2 retains only
  sanitized transport observations and counters, and failed payload/row/page bytes are discarded.

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
  orphan-audit/<refresh-id>/...
```

`objects`, `manifests`, `gates`, `candidates`, `selections`, `staging` and `orphan-audit`
are fixed path components; no caller may substitute a directory name. Root and each ancestor are
opened with `O_DIRECTORY|O_NOFOLLOW`, identity-checked (`st_dev`, `st_ino`, mode) before and after
use, and rejected if a symlink, non-directory, path traversal, absolute path, cross-root path or
TOCTOU replacement is observed. Metadata/object descriptors are opened by relative descriptor,
size-bounded, SHA-256 checked and schema/row-count checked before their descriptor is passed to a
reader. The threat boundary is local application-owned storage; this does not claim protection
against a same-UID process that continuously mutates a private staging inode, host compromise or
filesystem corruption beyond detection.

Publication is a compare-and-create protocol, not an overwrite protocol. There is exactly one
blocking refresh lock: the existing R2-F1 `RefreshRunLock` at
`local_lock_dir/market-refresh.lock` (`layout.market_refresh_lock`). Evidence publication adds
no blocking lock and does not acquire any cross-root lock:

1. Acquire `RefreshRunLock` first and keep it for the complete online sequence. Validate the
   complete `ExpectedLogicalRequestPlan`, capture the factor snapshot and its before/after
   fingerprint, and write each bounded page object below staging,
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
4. While the same existing `RefreshRunLock` remains held, publish the `SessionSelection` first. Only after
   selection readback/hash validation may the existing canonical Parquet/manifest/pointer chain
   run. The canonical manifest/pointer MUST NOT reference a missing selection; a selection may not
   reference missing candidate/evidence/gate objects.
5. Existing R2-F1 lock ownership and queue/CAS ordering remains authoritative. A competing writer
   loses the compare-and-create/CAS check and performs zero canonical writes. A stale lock-in
   revalidation failure may retain the canonical lock artifact but MUST NOT create broad directories
   or mutate the canonical pointer.

Readers never initialize roots, schemas, locks or directories. Replay uses no blocking lock (or a
read-only snapshot descriptor if concurrent mutation is detected). A missing root, missing object,
invalid ancestor, lock/CAS conflict or incomplete chain returns an allowlisted unavailable/error
   result with zero writes. This is a read-only boundary, not a best-effort repair path.

## Replay clock and deterministic equivalence

`normalization_clock_utc` is generated exactly once after the final fetch/transport observation is
complete and immediately before evidence manifest publication. Both online normalization and offline
replay receive this same injected UTC clock; neither calls `datetime.now()` independently while
constructing candidate bytes. The existing `normalize_baostock_rows(..., ingested_at=...)` entry
point receives this value through the adapter's `normalize(..., normalization_clock_utc=...)`
compatibility shim. Invocation time may be used only in an internal non-persisted diagnostic log;
it MUST NOT enter `ReplayResult`, candidate/selection models, semantic hashes or aggregate digests.

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

## Prior independent review evidence (non-authoritative after this amendment)

The final independent review at exact clean HEAD `474b5126b0b8f96dd641d4061e67c1526ea1e0d0`
checked every FR-1–FR-33, NFR-1–NFR-18, AC-1–AC-20 and EC-1–EC-26; the eight-group
`FactorCacheSnapshotManifest`/`EvidenceObjectDescriptor` mapping in both directions; cache
`FactorResolutionBinding` identity/SHA equality; the exact Task 7–9 whitelist and test ownership;
the six endpoint IDs and nine endpoint-role variants; the ten-gate order; Option A failed-attempt
discard/ultimate-failure behavior; transport lineage; single-lock/CAS ordering; deterministic
replay; path, CLI, legacy, GET, fallback and offline boundaries. The strict design validator
returned 100/100 with zero errors, warnings or info; `git diff --check` passed; the worktree was
clean. Findings: High 0, Medium 0, Low 0. No RED test, provider request, external operation or
production mutation occurred. This historical review does not approve the identity amendment or
authorize real Provider/NAS/install/LaunchAgent/production execution.

## Review gate

Status is **In Review — architecture amendment required**. The three Task 7 rounds are NO-GO and
`fea5678059f5b2955dbd1b3b8c570d94ad9c87e9` is non-delivery. An independent specification review
MUST issue a specification GO for this identity amendment before RED; because this is a breaking
contract amendment, the user MUST then explicitly approve that reviewed GO before any new
fix/replace commit or RED test. The new fix/replace commit must pass the named tests and review gate.
No self-approval, provider request, external operation or production change is authorized. The
amended linear implementation plan below remains blocked until both gates.
