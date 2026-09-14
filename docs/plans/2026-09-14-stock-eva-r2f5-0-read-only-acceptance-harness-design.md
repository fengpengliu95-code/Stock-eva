# Stock EVA R2-F5.0 Read-only Acceptance Harness Design

**Author:** Codex R2-F delivery lead

**Date:** 2026-09-14 (Asia/Shanghai)

**Status:** SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO

**Base commit:** `5393f499dbc8b84398658816f7a555dd3e547d47` (clean worktree)

**X7 revision base:** `97283a5ce6d67d925d84a9d4b759a8ebe98b3af9` (clean X6)

**Reviewers:** Independent SPEC reviewer and independent QUALITY reviewer (not yet assigned)

**Scope:** Task 19 only. A local, read-only acceptance evaluator for a proposed R2-F5 window.
Task 20's installed 20-session production observation and Task 21's Release 2 re-entry remain
separate blocking work.

## Context

The R2-F roadmap requires every mandatory reliability SLO to pass over one frozen window of
exactly 20 confirmed consecutive Asia/Shanghai trading sessions. R2-F4.0 through R2-F4.3 provide
read-only status, evidence, exact-session universe, calendar, replication and restore primitives,
but there is no single evaluator that joins those already-persisted identities without mutating
them. Task 19 therefore defines a read-only acceptance harness as a gate and evidence producer;
it is not the production soak itself.

The harness MUST evaluate only an explicitly supplied or configured local dataset/evidence root,
already-created control stores and immutable drill records. It MUST capture one in-memory,
descriptor-bound snapshot per operation, fingerprint every input root/database before and after
the evaluation, and fail closed when any source cannot be proven. It MUST never initialize a
missing database, repair a queue, restore a generation, call a provider, read credentials, or
write a canonical pointer.

This design is additive. Existing `MarketDataStatus`, universe status, calendar-generation
status, provider status, evidence/replay, candidate/selection, replication and restore contracts
remain authoritative and backward compatible. The harness may consume their strict readers and
sanitized projections; it MUST NOT reinterpret OHLCV/amount as fund-flow evidence, and MUST NOT
claim that synthetic fixtures or an offline report are installed-runtime or production evidence.

## Safety contract

- R2-F5.0 MUST be local-chain/read-only. `--execute` MUST NOT exist.
- A report with `status=ready` means only that the supplied captured snapshot satisfies the
  R2-F5 metric contract. It MUST NOT mean R2-F5 GO, production GO, Release 2 GO, R2-A GO or
  R2-B GO.
- The official Task 20 clock MUST NOT be started, advanced, repaired, or simulated by this slice.
- A missing, corrupt, locked, replaced, symlinked, path-invalid or version-drifting source MUST
  produce a bounded `unavailable` or `not_ready` report and zero writes.
- Public/API/CLI diagnostics MUST expose only the closed reason vocabulary, counts, timestamps,
  hashes and bounded identifiers. Provider payloads, tokens, SQL, exception text and absolute
  sensitive paths MUST NOT be returned.

## Functional Requirements

- FR-1: The evaluator MUST read only an explicitly allowlisted local dataset root, evidence root,
  and existing control stores; it MUST NOT create, initialize, migrate, repair or delete any input.
- FR-2: Every supplied root MUST be absolute, non-root, lexically normalized, and pass `lstat`/
  `open(..., O_NOFOLLOW)` descriptor-identity probes before directory enumeration or content reads;
  it MUST be disjoint from configured control, staging, temporary, user, NAS and home roots.
  Symlink aliases, unresolved variables, mutable-root descendants and overlap MUST fail closed.
- FR-3: Before reading and after producing either a success or error report, the evaluator MUST
  fingerprint every input root and database by the exact descriptor-bound tree/logical-snapshot
  algorithms below. Directory entries use full streaming content SHA-256; SQLite uses its one
  read-only transaction algorithm, never a live database/WAL file hash. Descriptor metadata detects
  replacement; content hashing detects content drift. Any input over a declared limit MUST yield
  `INPUT_LIMIT_EXCEEDED`/`unavailable`, never a bounded sample claim; any change MUST invalidate
  the report and preserve the prior state.
- FR-4: The evaluator MUST capture one immutable in-memory snapshot from strict existing readers
  before metric calculation. A missing control database MUST remain missing; the evaluator MUST
  never create an empty replacement.
- FR-5: Session selection MUST use only confirmed calendar sessions visible at the trusted
  Asia/Shanghai clock, in sorted order, and MUST require exactly 20 distinct consecutive sessions
  in the requested inclusive date range. A 19-session, 21-session, duplicate, future, unknown or
  non-consecutive range MUST NOT be `ready`.
- FR-6: A missing middle session, an unconfirmed calendar date, or a duplicate/non-advancing date
  MUST be reported explicitly; later repair MUST NOT rewrite the original availability observation.
- FR-7: The evaluator MUST freeze one version set across the selected sessions and require exact
  equality for Git commit/release identity, canonical/evidence schema, provider ID, adapter and
  endpoint contract, reconciliation and selection policy, calendar generation/hash, universe
  generation/hash, replication schema and restore-drill schema. Any material drift MUST fail the
  window and identify the drift class without exposing private paths.
- FR-8: Availability MUST use timezone-aware publication timestamps and inclusive cutoffs in
  `Asia/Shanghai`: same-evening is `<= 21:15` on the session date; next-morning is `<= 08:00` on
  the following civil date. A timestamp at 21:16 or 08:01 MUST fail the corresponding target.
- FR-9: For every selected session the evaluator MUST calculate continuity, next-morning,
  same-evening, legal-universe coverage, canonical source purity and immutable integrity metrics.
  The mandatory targets are zero missing dates, 20/20 next-morning, at least 18/20 same-evening,
  100% legal-universe coverage and zero mixed-source canonical partitions.
- FR-10: Each ready session MUST prove raw evidence, candidate and gate report, canonical
  manifest/object hash, and exact `SessionSelection` lineage; missing, partial, mismatched or
  forged lineage MUST fail closed.
- FR-11: Replay MUST consume only an existing bounded raw evidence object and the offline replay
  primitive. It MUST not perform provider/network I/O, normalize new provider data, or mutate a
  candidate, manifest, pointer or evidence object.
- FR-12: Calendar and universe metrics MUST consume the strict promoted-calendar and exact-session
  universe snapshots. Unknown/conflicting calendar state or nonzero universe unknown/count-mismatch
  state MUST make the report not ready; the evaluator MUST NOT guess weekdays or symbols.
- FR-13: Replication and restore metrics MUST consume immutable outbox/status and completed
  restore-drill records only. The evaluator MUST not drain an outbox, copy to NAS, restore a
  generation, mount SMB, or call a destination writer.
- FR-14: The report MUST use exactly `ready`, `not_ready` and `unavailable`; `ready` is legal only
  when all mandatory rows pass, `not_ready` when inputs are provable but one or more rows fail,
  and `unavailable` when a required input or proof cannot be safely read.
- FR-15: The CLI MUST expose `r2f-acceptance` with inclusive `--start`/`--end` dates and absolute
  `--local-dataset-root`/`--evidence-root` overrides, MUST emit one sanitized JSON report, and
  MUST use exit 0 for ready/not-ready, 1 for unavailable control/input state, and 2 for invalid
  arguments or path configuration.
- FR-16: The API MUST expose one additive read-only status endpoint using configured roots and the
  same report model, cutoff clock, reason vocabulary and zero-write guarantee as the CLI. Existing
  API response models MUST NOT change.
- FR-17: The evaluator MUST report `provider_requests=0`, `writes=false`, `restore_started=false`
  and `production_window_started=false` in every result, including errors.
- FR-18: The evaluator MUST record no Task 20 elapsed-session claim. A report MAY state that a
  supplied window is synthetically/evidentially complete, but MUST label installed production
  observation as pending unless separately authorized and evidenced.
- FR-19: The report MUST expose one `MetricResult` for each of the 17 roadmap Section 10 table
  dimensions, plus one independently testable `replication` child metric sourced by the
  Local/NAS isolation row (18 results total). Replication MUST NOT be represented as an 18th
  roadmap row. Each result MUST carry an observed value, target, status, reason, AC reference and
  unique planned test anchor; a missing dimension MUST make the report unavailable.
- FR-20: Secondary qualification/admission MUST be read through a strict immutable projection of
  the existing `provider_record` and `qualification_window` fields (`provider_id`,
  `admission_state`, `adapter_hash`, `endpoint_contract_hash`, `source_schema_hash`,
  `normalizer_hash`, `reconciliation_policy_hash`, terms evidence/review, `window_id`,
  `consecutive_sessions`, `version_vector_sha256`, calendar generation/hash, window state,
  qualification evidence/candidate hashes and terminal attestation). Task19 MUST NOT invent any
  admission, capability or qualification digest. If these fields do not prove failover admission,
  `qualification_proof_status=unavailable` and failover
  MUST be `unavailable`/`not_ready`; source purity alone MUST never substitute.
- FR-21: Forced-failover acceptance MUST require a separate immutable whole-session drill record
  proving primary-unavailable, qualified-secondary admission, one provider for every row, zero
  mixed rows, selection/pointer/manifest agreement and readback. BaoStock-only or no drill proof
  MUST fail closed.
- FR-22: Replication/restore evidence MUST be read through strict completed-record snapshot readers
  that reuse only the existing replication record/checkpoint/head fields and the existing
  `VerifiedArchiveSnapshot`, `RestoreReport` and terminal audit fields. `LOCAL_CHAIN_ONLY` MUST
  never satisfy a remote/NAS or Task 20 acceptance target. Remote verification, restore schema,
  row-count and API readback are available only from a Task20 writer-owned immutable drill
  envelope, if present; Task19 readers MUST NOT call `create=True`, writer, reconcile, drain,
  mount or restore paths.
- FR-23: Offline replay MAY run a frozen adapter/normalizer implementation on immutable bytes, but
  MUST inject an explicit offline-only adapter/normalizer identity and MUST reject unknown or
  mismatched identity. It MUST make zero network/provider requests and MUST NOT construct a default
  login/query-capable adapter.
- FR-24: `FrozenReliabilityVersions` MUST contain non-null ready-time identities for Git commit,
  installed RELEASE, dataset generation, primary/secondary provider IDs, the existing registry
  qualification window identity, adapter/endpoint/schema hashes, selection/reconciliation policy,
  config digest, auto-failover setting, kill switch, priority, continuity start/repair policy,
  calendar/universe generations, and replication/restore policy/evidence versions. Unsupported
  qualification/admission or remote-proof identities MUST be represented by typed absence and
  make the relevant metric unavailable; they MUST NOT be guessed. All 20 observations MUST equal
  this vector.
- FR-25: Raw captured calendar observations MUST retain source order and duplicates for validation;
  duplicate or out-of-order input MUST be unavailable. Only after validation MAY the evaluator
  derive sorted unique confirmed sessions. `SnapshotIdentity` MUST bind requested range, Shanghai
  clock instant/time-zone contract, all input fingerprints and the frozen version vector. Each
  fixed control-database role (`replication_sidecar`, `daily_shadow`, `shadow_registry`,
  `calendar_generation` or `universe`) MUST use its structured `sqlite_catalogs` entry. The canonical
  catalog version, allowed tables, columns/types, primary-key and ordering tuples, and system-table
  allowlist MUST be included in `config_digest` and the input-fingerprint preimage; missing, extra,
  type/key/order or `user_version` drift MUST be `unavailable`, never dynamically accepted.
- FR-26: The report MUST contain exactly 20 ordered per-session observations or immutable
  observation references with digests when a candidate window is evaluated. Session observations
  MUST contain only per-session recomputable date, availability, coverage, canonical integrity,
  source purity, provenance, calendar, universe, replication and read-boundary facts. Window-level
  recovery, forced failover, replay sample, adjustment equivalence, forced-error matrix, NAS
  outage isolation and restore drill MUST be carried by a typed `WindowEvidenceBundle`; the report
  MUST NOT imply that those drills occurred once per session.
- FR-27: Volatile elapsed/counter diagnostics MUST live in a non-semantic envelope. The semantic
  report payload and digest MUST exclude elapsed time, read duration and other volatile fields;
  identical snapshot identity and bytes MUST yield byte-identical semantic JSON.
- FR-28: All public enums, reason codes, identifiers, SHA-256 values, quality issues, counters and
  cardinalities MUST use closed vocabularies, bounded nonnegative types and exact regex/length
  validation. Ready reports MUST have exactly 20 observations and no null frozen-version field.

## Non-Functional Requirements

- NFR-1: Existing readers and public models MUST remain backward compatible; no existing table,
  manifest, pointer, partition, selection, evidence or status schema may be rewritten or widened.
- NFR-2: Report ordering, JSON serialization, reason precedence and metric arithmetic MUST be
  deterministic for identical captured bytes, clock and arguments.
- NFR-3: The evaluator MUST use read-only SQLite connections and descriptor-bound no-follow reads;
  it MUST close descriptors/connections on every success and failure path.
- NFR-4: Error payloads MUST be sanitized to an allowlisted reason code and bounded safe detail;
  raw provider responses, credentials, URLs, SQL, environment values and exception messages MUST
  NOT cross the API/CLI boundary.
- NFR-5: Path validation MUST reject `/`, the home directory, configured mutable roots, relative
  paths, symlink aliases, environment-variable syntax, `..` escape and source/destination overlap
  using `lstat`/no-follow descriptor probes before filesystem enumeration or content reads.
- NFR-6: Every read and replay MUST enforce bounded object bytes, row counts, session count (20)
  and evidence sample count (at most 3), and MUST return `unavailable` rather than scan without a
  bound.
- NFR-7: The harness MUST expose measured `elapsed_ms`, `read_operations` and `replay_sample_count`.
  The reference fixture MUST contain 20 sessions and 100,000 rows and complete within 10,000 ms;
  this is a real maximum fixture, not a micro-sample. Limits MUST be explicit: at most 100,000
  tree entries, 512 MiB total regular-file bytes per input, 1,000,000 SQLite rows per database,
  32 input roots/descriptors, 20 sessions and 3 replay samples. Exceeding any limit returns
  `INPUT_LIMIT_EXCEEDED`/`unavailable`; the benchmark includes full hashing and logical SQLite
  snapshot work in its 10,000 ms budget, not a sampled claim.
- NFR-8: A snapshot identity MUST bind the requested range, trusted Shanghai clock observation,
  input fingerprints and frozen version set. A report MUST be rejected if any bound field changes.
- NFR-9: Tests MUST cover RED, GREEN, focused acceptance, full regression, static checks,
  crosswalk validation and protected R2-F2/R2-F4 compatibility. No test result may be represented
  as production evidence.
- NFR-10: The implementation plan MUST preserve a separate production-mutation gate requiring
  explicit human authorization, installed-release readback, provider/terms approval and Task 20;
  no local code or synthetic fixture can satisfy that gate.
- NFR-11: The SLO inventory MUST preserve the roadmap thresholds exactly: zero missing dates,
  20/20 next-morning by 08:00, at least 18/20 same-evening by 21:15, 100% legal coverage and
  zero mixed-source rows. No threshold may be invented or relaxed by the harness.
- NFR-12: Replication lag, remote verification and restore duration MUST be passable only when a
  reviewed R2-F4 policy/evidence record supplies numeric thresholds. Missing thresholds or a
  `LOCAL_CHAIN_ONLY` trust scope MUST be `not_ready`/`unavailable`, never guessed.
- NFR-13: Snapshot concurrency MUST be fail-closed: a root/database descriptor or content change
  before, during or after any read invalidates the whole report; no retry may silently mix snapshots.
- NFR-14: The semantic report MUST use canonical UTF-8 JSON with sorted keys, compact separators,
  `ensure_ascii=false`, `allow_nan=false` and a domain-separated SHA-256; volatile diagnostics MUST
  be outside that digest.
- NFR-15: The acceptance API/CLI MUST be additive only. Existing response models/routes and persisted
  schemas MUST remain unchanged; all new acceptance fields MUST be namespaced to this report.

## Acceptance Criteria

### AC-1: Exact session window and status mapping (FR-4, FR-5, FR-6, FR-14)

Given a captured calendar with confirmed sessions, when the requested range contains exactly 20
distinct consecutive sessions, then the report selects those dates in order and can be `ready`.
Given 19, 21, duplicate, future or missing-middle sessions, when evaluation runs, then the report
is `not_ready` or `unavailable` with an allowlisted reason and never silently pads the window.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-2: Shanghai cutoff arithmetic (FR-8, FR-9)

Given timezone-aware publication times, when a session is published at 21:15 and next morning at
08:00 Asia/Shanghai, then both boundary observations count. When they occur at 21:16 or 08:01,
then the corresponding metric fails and the report cannot be ready.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-3: Frozen version set and drift (FR-7, NFR-8)

Given 20 session observations with one frozen version vector, when any provider, adapter, endpoint,
policy, calendar, universe, replication, restore or schema identity changes, then the report is
`not_ready` with `VERSION_DRIFT` and the exact source state remains untouched.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-4: Coverage and source purity (FR-9, FR-12)

Given legal-universe counts and canonical partition source identities, when every session has 100%
coverage and zero mixed-source rows, then those metrics pass. When any unknown/count mismatch or
mixed provider partition exists, then the report is not ready and names only the sanitized reason.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-5: Evidence lineage and replay (FR-10, FR-11)

Given retained final-success evidence, candidate/gate, manifest/object and selection hashes, when
the bounded offline sample replays semantically identically, then provenance and replay pass.
Given a missing hash, wrong binding, corrupt object or replay mismatch, then no publication state is
changed and the relevant metric is unavailable/not ready.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-6: Replication and restore evidence (FR-13, FR-17)

Given immutable replication status and a completed verified restore drill, when lag and restore
proof satisfy the declared evidence contract, then the metrics pass. Given lag, missing drill,
locked/corrupt sidecar or unverified generation, then the report is not ready/unavailable and does
not start a drain or restore.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-7: Read-only snapshot and fingerprints (FR-1, FR-3, FR-4, NFR-3)

Given an existing private fixture, when CLI/API evaluation succeeds or fails, then every input
fingerprint is byte/metadata identical before and after, no missing control DB is initialized, and
the captured snapshot is closed after use.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-8: Path validation and redaction (FR-2, NFR-4, NFR-5)

Given relative, root, home, mutable-root, symlink, unresolved-variable or overlapping paths, when
the CLI/API is invoked, then it rejects before enumeration with exit 2/HTTP 422 and no path is
echoed. Given corrupt or permission-denied input, then the response contains only a closed reason.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-9: API/CLI parity (FR-15, FR-16, FR-17)

Given identical configured roots, dates, frozen clock and fixture, when CLI and API are evaluated,
then their report status, selected sessions, metric results, reason order, `provider_requests` and
`writes` agree; the API performs no provider or store initialization.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-10: Mandatory metric gate (FR-9, FR-14, NFR-2)

Given all mandatory metrics pass for exactly 20 sessions, when the report is built, then status is
`ready`. Given any mandatory metric fails, then status is `not_ready`; given a required source is
unprovable, then status is `unavailable`. No optional observation can override this mapping.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-11: Bounded evaluation (NFR-6, NFR-7)

Given a 20-session fixture within the declared row/object/sample bounds, when evaluation runs, then
it records elapsed/read/replay counts and completes within 10,000 ms on the reference gate. Given
an over-bound object or row set, then it stops with unavailable rather than unboundedly reading.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-12: Protected compatibility (NFR-1, NFR-9)

Given the R2-F2 golden fixture and R2-F4 readers, when the crosswalk/focused checks run, then their
bytes and public response shapes remain compatible and no protected production module is widened.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-13: Production boundary (FR-18, NFR-10)

Given any offline ready report, when a reviewer inspects metadata, then it states
`production_window_started=false`, Task 20 pending and R2-F5.0 NO-GO. No report can authorize a
provider call, installation, LaunchAgent, NAS action or Release 2 re-entry.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-14: Deterministic and sanitized errors (FR-14, FR-17, NFR-2, NFR-4)

Given identical bytes and clock, when evaluation is repeated across success, missing, corrupt and
locked fixtures, then JSON is deterministic, reason precedence is stable, diagnostics are bounded,
and all results include zero provider requests/writes.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-15: Complete SLO metric inventory (FR-9, FR-19, NFR-11)

Given a candidate window, when the report is serialized, then all named roadmap dimensions are
present as separate `MetricResult` fields with exact targets and anchors. Missing continuity,
availability, canonical integrity, recovery, failover, adjustment, error handling, local/NAS,
replication, restore or read-boundary evidence cannot be hidden behind another metric.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-16: Secondary qualification and failover (FR-20, FR-21)

Given only BaoStock, no qualified secondary, or no immutable whole-session forced-failover drill,
when evaluation runs, then failover is `not_ready`/`unavailable` and top-level ready is impossible.
Given a qualified admission and complete drill record, then failover passes only when selection,
manifest, pointer and all rows agree with zero mixed-source rows.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-17: Remote trust and restore snapshot (FR-22, NFR-12)

Given a completed replication/restore record with destination generation, head proof, remote
verification, trust scope and reviewed numeric thresholds, when the strict snapshot reader runs,
then it can pass. Given `LOCAL_CHAIN_ONLY`, missing threshold, locked sidecar or writer/reconcile
only evidence, then the metric is unavailable/not ready and no destination operation starts.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-18: Offline replay identity (FR-11, FR-23)

Given immutable bytes and an exact frozen offline adapter/normalizer identity, when replay runs,
then it performs deterministic normalization and zero provider/network requests. Given unknown,
mismatched or default login-capable identity, then replay is unavailable before adapter construction.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-19: Full version vector (FR-7, FR-24)

Given 20 observations, when the frozen vector is captured, then every required identity is non-null
and equal across all observations. Missing RELEASE, config, qualification/admission, primary/
secondary, kill-switch/priority, policy or replication/restore evidence prevents ready.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-20: Raw sequence and snapshot identity (FR-3, FR-25, NFR-8, NFR-13)

Given raw calendar observations, when evaluation validates them, then it preserves source order and
rejects duplicates/out-of-order data before deriving sorted unique confirmed sessions. The report
returns a digest-bound SnapshotIdentity and observation refs; any concurrent input change invalidates
the whole report.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-21: Semantic determinism (FR-26, FR-27, NFR-14)

Given identical captured bytes, clock and arguments, when evaluation repeats, then exactly 20
per-session observations/refs and byte-identical semantic JSON/digest are returned. Changing only
elapsed/counter diagnostics MUST NOT change the semantic digest.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-22: Closed bounded report types (FR-28, NFR-3)

Given invalid enum, reason, hash, ID, negative counter, oversized quality issue or null ready
version, when report validation runs, then it rejects the report with a sanitized unavailable result.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

### AC-23: Additive compatibility (NFR-1, NFR-15)

Given existing market, universe, evidence, calendar, replication and restore API responses, when
the new acceptance endpoint/CLI is evaluated, then predecessor payloads/schemas remain unchanged;
the new fields exist only in the acceptance report.

Planned test anchors are machine-readable in the requirement matrix and X7 catalog only.

## Edge Cases

- EC-1: Exactly 19 confirmed sessions are available in the requested range; return `not_ready` with
  `SESSION_COUNT_NOT_20`, never infer a missing session.
- EC-2: Exactly 20 raw dates contain a duplicate or are out of order; return `unavailable` with
  `SESSION_SEQUENCE_INVALID`, preserving the observed order and no deduplication.
- EC-3: The requested end date is in the future or after the trusted Shanghai clock; return
  `unavailable`/`PIT_VISIBILITY_INVALID` before accepting session metrics.
- EC-4: A calendar year is unknown, conflicting or has no promoted generation; return unavailable
  with `CALENDAR_UNAVAILABLE` or `CALENDAR_CONFLICT`, never weekend-guess.
- EC-5: Universe status has unknown members, count mismatch, missing required index or changed
  generation; return `not_ready` with the corresponding sanitized universe reason.
- EC-6: A publication at exactly 21:15/08:00 has a non-Shanghai offset; normalize only a
  timezone-aware instant and apply the Shanghai civil cutoff, otherwise return unavailable.
- EC-7: A publication at 21:16 or 08:01 is present and later repaired; keep the original metric
  failure and do not rewrite its timestamp.
- EC-8: Provider, adapter, endpoint, policy, calendar, universe, replication or restore version
  changes mid-window; return `not_ready/VERSION_DRIFT` and identify the version class only.
- EC-9: Candidate, selection, gate, manifest, evidence or object hash is missing, corrupt, partial,
  wrong-date, wrong-provider or mixed; return unavailable and preserve all source bytes.
- EC-10: Replay sample is missing, oversized, malformed or semantically different; return
  `REPLAY_UNAVAILABLE` without creating or rewriting a candidate.
- EC-11: Replication sidecar is missing, corrupt, locked, ahead, lagged or has an orphan; return
  unavailable/degraded evidence and never import, drain or quarantine from the reader.
- EC-12: Restore drill is absent, not terminal, hash-invalid, source-changing or destination-unsafe;
  return `RESTORE_UNAVAILABLE` and never create a temporary restore destination.
- EC-13: Any input root/database is replaced, symlinked, permission-denied or changes fingerprint
  during read; return unavailable and close all descriptors.
- EC-14: Path is relative, `/`, home, a configured mutable root, unresolved-variable syntax or an
  ancestor/descendant overlap; reject after only allowed `lstat`/`O_NOFOLLOW` descriptor probes and
  before directory enumeration/content reads; do not create it.
- EC-15: SQLite is absent or locked; return unavailable without initialization, migration, retry
  loop or lock takeover.
- EC-16: An exception contains a path, token, SQL, URL, provider response or raw text; expose only
  its allowlisted reason and bounded safe identifier.
- EC-17: Object/row/sample bound is exceeded or evaluation exceeds 10,000 ms; stop at the bound and
  return unavailable with measured counters.
- EC-18: API/CLI receives malformed date, missing required argument or disallowed override; return
  HTTP 422/CLI 2 with no filesystem or provider access.
- EC-19: Roadmap SLO field is absent from a report or has no explicit threshold; return unavailable,
  never infer pass from another metric.
- EC-20: Only BaoStock or an unqualified/unstaged secondary exists; failover remains not_ready/
  unavailable and source purity cannot make it pass.
- EC-21: Forced-failover record lacks whole-session selection/pointer/manifest/readback proof or has
  mixed rows; failover returns unavailable and no pointer is changed.
- EC-22: Replication/restore record is LOCAL_CHAIN_ONLY, lacks remote head proof/verification or
  lacks a reviewed numeric lag/duration threshold; return not_ready/unavailable.
- EC-23: Replay identity is unknown, mismatched, login-capable or provider-default; reject before
  constructing it and perform zero network/provider calls.
- EC-24: Frozen vector omits RELEASE, dataset, secondary/admission, config, kill-switch/priority,
  policy, calendar/universe or replication/restore identity; ready validation rejects it.
- EC-25: Raw calendar list is duplicated, out of order or changed while sorting; preserve raw bytes,
  return unavailable and never evaluate a derived session list.
- EC-26: Semantic digest input contains elapsed/counter volatility or omits SnapshotIdentity,
  observation refs or one of 20 observations; reject the report as non-deterministic/incomplete.

## API Contracts

### `GET /api/v1/market/reliability-acceptance`

The endpoint is additive and read-only. It uses configured roots from `Settings`; arbitrary public
path overrides are not accepted. Dates are exact `YYYY-MM-DD` strings and the trusted clock is
injected only in tests.

```typescript
interface ReliabilityAcceptanceQuery {
  start: string; // YYYY-MM-DD, inclusive
  end: string;   // YYYY-MM-DD, inclusive
}

type MetricValue =
  | { kind: "count"; value: number }
  | { kind: "ratio"; value: number }
  | { kind: "duration_seconds"; value: number }
  | { kind: "bool"; value: boolean }
  | { kind: "hash"; value: string };

type MetricResult =
  | { status: "pass"; observed: MetricValue; target: MetricValue;
      reason_code: null; acceptance_ref: "AC-15"; planned_test_anchor: string }
  | { status: "fail"; observed: MetricValue; target: MetricValue;
      reason_code: FailureReasonCode; acceptance_ref: "AC-15"; planned_test_anchor: string }
  | { status: "unavailable"; observed: null; target: null;
      reason_code: UnavailableReasonCode; acceptance_ref: "AC-15"; planned_test_anchor: string };

type FailureReasonCode = Extract<AcceptanceReasonCode,
  "CONTINUITY_FAILED" | "AVAILABILITY_CUTOFF_FAILED" | "COVERAGE_FAILED" |
  "SOURCE_PURITY_FAILED" | "CANONICAL_INTEGRITY_FAILED" | "RECOVERY_FAILED" |
  "ERROR_HANDLING_FAILED" | "LOCAL_NAS_ISOLATION_FAILED" | "REPLICATION_LAG" |
  "REPLAY_SEMANTIC_MISMATCH" | "READ_BOUNDARY_FAILED" | "CALENDAR_CONFLICT" |
  "UNIVERSE_UNKNOWN_NONZERO" | "UNIVERSE_COUNT_MISMATCH" | "VERSION_DRIFT" |
  "LINEAGE_INVALID">;
type UnavailableReasonCode = Exclude<AcceptanceReasonCode, FailureReasonCode>;

// The discriminant is normative: pass has no reason and complete values; fail has a closed
// failure reason and complete values; unavailable has a closed unavailable reason and null values.
// Overall status precedence is invalid arguments/path (HTTP 422/CLI 2), then unavailable if any required
// proof is unavailable, then not_ready if any required metric fails, and ready only when every
// required metric passes with an exact 20-session snapshot.

// This is the only reason-code source of truth. Every table, default and public field below MUST
// use one of these values; the crosswalk validator parses this literal and rejects duplicates.
const ACCEPTANCE_REASON_CODES = [
  "INVALID_ARGUMENTS", "PATH_INVALID", "SNAPSHOT_CHANGED", "CONTROL_STATE_UNAVAILABLE",
  "PIT_VISIBILITY_INVALID", "CALENDAR_UNAVAILABLE", "CALENDAR_CONFLICT",
  "SESSION_SEQUENCE_INVALID", "SESSION_COUNT_NOT_20", "VERSION_DRIFT", "LINEAGE_UNAVAILABLE",
  "LINEAGE_INVALID",
  "CANONICAL_INTEGRITY_FAILED", "UNIVERSE_UNKNOWN_NONZERO", "UNIVERSE_COUNT_MISMATCH",
  "CONTINUITY_FAILED", "AVAILABILITY_CUTOFF_FAILED", "COVERAGE_FAILED", "SOURCE_PURITY_FAILED",
  "RECOVERY_FAILED", "FAILOVER_UNAVAILABLE", "REPLAY_UNAVAILABLE", "ADJUSTMENT_UNAVAILABLE",
  "ERROR_HANDLING_FAILED", "LOCAL_NAS_ISOLATION_FAILED", "REPLICATION_UNAVAILABLE",
  "REPLICATION_LAG", "REMOTE_PROOF_MISSING", "RESTORE_UNAVAILABLE",
  "REPLAY_SEMANTIC_MISMATCH", "READ_BOUNDARY_FAILED", "NONE", "DISABLED", "SOURCE_NOT_CONFIGURED",
  "SOURCE_UNAVAILABLE", "LOCAL_POINTER_MISMATCH", "REPLICATION_STATE_UNAVAILABLE",
  "DESTINATION_UNAVAILABLE", "DESTINATION_TRUST_FAILED", "COPY_FAILED", "VERIFY_FAILED",
  "RETRY_WAIT", "DEAD_LETTER", "INPUT_LIMIT_EXCEEDED",
] as const;
type ValuesOf<T> = T extends readonly (infer Value)[] ? Value : never;
type AcceptanceReasonCode = ValuesOf<typeof ACCEPTANCE_REASON_CODES>;

type ProductionCreatorKind = "task20_writer";

interface ImmutableObservationEnvelopeV1<T> {
  artifact_id: string;
  artifact_ref: string;
  schema_version: "r2f5-observation-envelope-v1";
  creator_kind: ProductionCreatorKind;
  creator_version: string;
  created_at: string; // RFC3339 UTC, `Z`
  payload: T;
  canonicalization_version: "project-canonical-json-v1";
  payload_sha256: string;
  envelope_sha256: string;
}

type TestEnvelope<T> = Omit<ImmutableObservationEnvelopeV1<T>, "schema_version" | "creator_kind"> & {
  schema_version: "r2f5-test-envelope-v1";
  creator_kind: "test_fixture";
};

// JCS-compatible project JSON is UTF-8, sorted keys, compact separators, ensure_ascii=false,
// allow_nan=false, with one trailing newline. payload_sha256 hashes canonical(payload). The
// envelope_sha256 preimage is the envelope fields except payload_sha256 and envelope_sha256,
// with payload replaced by payload_sha256; artifact IDs, creator/version, timestamps and schema
// remain in the preimage. Task20 writers create these bytes; Task19 only opens immutable refs,
// recomputes both hashes and rejects missing creator/version, unknown schema or any mismatch.

The envelope is the sole identity/hash wrapper for every window-level artifact. The outer bundle
and each non-null recovery, failover, replay, adjustment, error, NAS-isolation and restore record
MUST be an `ImmutableObservationEnvelopeV1`; payloads MUST NOT repeat an ambiguous artifact hash
or preimage field. `task20_writer` is the only production creator; `test_fixture` is synthetic-only
and cannot establish production acceptance. Existing R2-F4 reader projections are not new envelope
artifacts; `r2f5_reader` and `r2f4_writer` MUST NOT appear as envelope creators, and a reader MUST
never create an envelope. `TestEnvelope` is a test-only endpoint/type and MUST be rejected by a
production reader. Missing creator/version, unsupported schema or either hash mismatch is
`unavailable`, never a reconstructed or current-state proof.

Public validation is closed and immutable: identifiers use
`[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`, SHA-256 values use `[0-9a-f]{64}`, bounded strings are at
most 128 UTF-8 bytes, counters/counts and durations are integers in `[0, 2^31-1]`, ratios are
numbers in `[0,1]`, and hash values are the typed 64-lowercase-hex `MetricValue` variant.
`MetricValue.kind` MUST match the metric's declared value type; arrays are readonly tuples with
their stated exact/max cardinality and all JSON/API schemas MUST reject unknown fields. The API
and CLI expose only these validated projections and sanitized reason codes.

There are no open-ended date/time strings: session/range dates MUST match `YYYY-MM-DD` and pass
actual calendar validation; timestamps MUST be RFC3339 with seconds (optional micros), timezone
aware, and normalized to UTC `Z` in canonical bytes. `as_of_utc` is converted to the literal
`Asia/Shanghai` zone for civil-date selection and the 21:15/08:00 cutoffs. The structured contract
maps every one of the 18 metric results to an exact `MetricValue.kind`, observed/target nullability
and range; an unavailable result has both values null, while pass/fail values are complete.
Replication is the sole duration metric and uses `duration_seconds` end-to-end: observed
`lag_seconds` and target frozen threshold are bounded nonnegative canonical JSON numbers (no NaN,
Infinity or exponent spelling), never milliseconds.

interface SnapshotFingerprint {
  descriptor_role: "dataset" | "evidence" | "calendar" | "universe" | "replication" | "restore" | "control";
  descriptor_id: string;
  descriptor_state: "present" | "absent";
  device: number | null;
  inode: number | null;
  size_bytes: number | null;
  mtime_ns: number | null;
  ctime_ns: number | null;
  fingerprint_kind: "descriptor_metadata" | "content_sha256";
  hash_scope: "full_streaming_bytes" | "none";
  sha256: string | null;
}

type QualityIssueCode = AcceptanceReasonCode;

interface FrozenReliabilityVersions {
  git_commit: string;
  installed_release: string;
  installed_release_sha256: string;
  dataset_generation: string;
  canonical_schema: string;
  evidence_schema: string;
  primary_provider_id: string;
  secondary_provider_id: string;
  qualification_window_id: string;
  qualification_proof_status: "available" | "unavailable";
  adapter_hash: string;
  endpoint_contract_hash: string;
  source_schema_hash: string;
  normalizer_hash: string;
  reconciliation_policy_version: string;
  selection_policy_version: string;
  config_digest: string;
  auto_failover_enabled: boolean;
  failover_kill_switch: boolean;
  provider_priority: readonly string[]; // 1..8, safe IDs only
  continuity_start_date: string;
  repair_policy_version: string;
  calendar_generation: string;
  calendar_sha256: string;
  universe_generation: string;
  universe_sha256: string;
  replication_policy_version: string;
  replication_evidence_version: string;
  replication_trust_scope: "LOCAL_CHAIN_ONLY" | "REMOTE_VERIFIED";
  destination_generation: string;
  destination_head_sha256: string;
  remote_proof_artifact_ref: string | null;
  restore_policy_version: string;
  restore_evidence_version: string;
}

// All fields are required and non-null for status=ready; nullable proof fields are legal only for
// not_ready/unavailable and are then accompanied by their typed reason. Provider IDs use the
// existing closed registry IDs; no new ProviderId is introduced.

interface R2FAcceptanceReport {
  status: "ready" | "not_ready" | "unavailable";
  window_start: string | null;
  window_end: string | null;
  selected_sessions: readonly string[]; // exactly 20 for a candidate window
  frozen_versions: FrozenReliabilityVersions | null;
  continuity: MetricResult;
  next_morning_availability: MetricResult;
  same_evening_availability: MetricResult;
  coverage: MetricResult;
  canonical_integrity: MetricResult;
  source_purity: MetricResult;
  recovery: MetricResult;
  failover: MetricResult;
  provenance: MetricResult;
  replay: MetricResult;
  adjustment: MetricResult;
  calendar: MetricResult;
  universe: MetricResult;
  error_handling: MetricResult;
  local_nas_isolation: MetricResult;
  replication: MetricResult;
  restore: MetricResult;
  read_boundary: MetricResult;
  quality_issues: readonly QualityIssueCode[]; // max 64
  snapshot_identity: SnapshotIdentity | null;
  session_observations: readonly SessionObservation[]; // exactly 20 for a candidate window
  observation_refs: readonly string[]; // exactly 20 for a candidate window
  window_evidence_bundle: WindowEvidenceBundle | null;
  window_evidence_refs: readonly string[]; // exactly 1 when a bundle is captured
  pre_capture_failure: PreCaptureFailurePayloadV1 | null;
  semantic_report_sha256: string | null;
  diagnostic_envelope: DiagnosticEnvelope;
  provider_requests: 0;
  writes: false;
  restore_started: false;
  production_window_started: false;
}

interface SnapshotIdentity {
  requested_start: string;
  requested_end: string;
  as_of_utc: string;
  as_of_timezone: "Asia/Shanghai";
  input_fingerprints: readonly SnapshotFingerprint[]; // one per configured role, max 32
  frozen_versions: FrozenReliabilityVersions;
  input_fingerprint_sha256: string;
  frozen_version_vector_sha256: string;
  snapshot_sha256: string;
}

interface DiagnosticEnvelope {
  elapsed_ms: number | null;
  read_operations: number;
  replay_sample_count: number;
}

interface PreCaptureFailurePayloadV1 {
  schema_version: "r2f5-pre-capture-failure-v1";
  reason_code: UnavailableReasonCode;
  requested_start: string;
  requested_end: string;
  as_of_utc: string;
  descriptor_states: readonly ("dataset_absent" | "evidence_absent" | "calendar_absent" |
    "universe_absent" | "replication_absent" | "restore_absent" | "control_absent" | "error")[];
  semantic_report_sha256: string;
}

interface CalendarRawFacts {
  source_sequence: readonly string[]; // max 64, source order and duplicates retained
  generation: string;
  confirmed: boolean;
  unknown_state: boolean;
  conflict_state: boolean;
  raw_facts_sha256: string;
}

interface ReadBoundaryRawFacts {
  requested_as_of: string;
  max_visible_session: string | null;
  future_rows_seen: boolean;
  future_rows_count: number;
  query_count: number;
  write_count: number;
  probe_schema_digest: string;
}

interface SessionEvidenceBinding {
  evidence_id: string;
  evidence_sha256: string;
  candidate_id: string;
  candidate_sha256: string;
  gate_report_id: string;
  gate_report_sha256: string;
  manifest_id: string;
  manifest_sha256: string;
  object_id: string;
  object_sha256: string;
  selection_id: string;
  selection_sha256: string;
  binding_sha256: string;
}

interface PointerReconciliation {
  pointer_id: string;
  pointer_sha256: string;
  manifest_sha256: string;
  object_sha256: string;
  pointer_manifest_object_match: boolean;
  descriptor_sha256: string;
}

interface ReplicationObservation {
  immutable: true;
  state: "disabled" | "ready" | "degraded" | "unavailable";
  checkpoint_id: string;
  source_commit_sha256: string;
  intent_id: string | null;
  enqueue_state: "pending" | "copying" | "verifying" | "retry_wait" | "replicated" | "dead_letter";
  reason_code: "NONE" | "DISABLED" | "SOURCE_NOT_CONFIGURED" | "SOURCE_UNAVAILABLE" |
    "LOCAL_POINTER_MISMATCH" | "REPLICATION_STATE_UNAVAILABLE" | "DESTINATION_UNAVAILABLE" |
    "DESTINATION_TRUST_FAILED" | "COPY_FAILED" | "VERIFY_FAILED" | "RETRY_WAIT" | "DEAD_LETTER";
  observed_at: string;
  lag_seconds: number | null; // bounded [0, 2^31-1]
  trust_scope: "LOCAL_CHAIN_ONLY" | "REMOTE_VERIFIED";
  destination_generation: string | null;
  destination_record_sha256: string | null;
  destination_head_sha256: string | null;
  observation_sha256: string;
}

interface RecoveryObservation {
  immutable: true;
  event_id: string;
  attempt_id: string;
  before_generation: string;
  after_generation: string;
  queue_identity: string;
  restart_boundary: string;
  exactly_once_publication_id: string;
  after_manifest_sha256: string;
  after_pointer_sha256: string;
  after_selection_sha256: string;
  publication_count: 1;
  duplicate_proof_sha256: string;
  observed_at: string;
  observation_sha256: string;
}

interface ErrorHandlingObservation {
  immutable: true;
  events: [{
    event_id: string;
    forced_error_class: "timeout";
    sanitized_reason: AcceptanceReasonCode;
    normalized_result: "not_ready" | "unavailable";
    attempt_id: string;
    expected_class: "timeout";
    observed_class: "timeout";
    evidence_sha256: string;
    observed_at: string;
  }, { event_id: string; forced_error_class: "auth"; sanitized_reason: AcceptanceReasonCode;
    normalized_result: "not_ready" | "unavailable"; attempt_id: string; expected_class: "auth";
    observed_class: "auth"; evidence_sha256: string; observed_at: string }, { event_id: string; forced_error_class: "rate";
    sanitized_reason: AcceptanceReasonCode; normalized_result: "not_ready" | "unavailable";
    attempt_id: string; expected_class: "rate"; observed_class: "rate"; evidence_sha256: string;
    observed_at: string },
  { event_id: string; forced_error_class: "schema"; sanitized_reason: AcceptanceReasonCode;
    normalized_result: "not_ready" | "unavailable"; attempt_id: string; expected_class: "schema";
    observed_class: "schema"; evidence_sha256: string; observed_at: string }, { event_id: string; forced_error_class: "coverage";
    sanitized_reason: AcceptanceReasonCode; normalized_result: "not_ready" | "unavailable";
    attempt_id: string; expected_class: "coverage"; observed_class: "coverage"; evidence_sha256: string;
    observed_at: string },
  { event_id: string; forced_error_class: "storage"; sanitized_reason: AcceptanceReasonCode;
    normalized_result: "not_ready" | "unavailable"; attempt_id: string; expected_class: "storage";
    observed_class: "storage"; evidence_sha256: string; observed_at: string }];
  observation_sha256: string;
}

interface LocalNasIsolationObservation {
  immutable: true;
  event_id: string;
  local_publication_ready: true;
  local_publication_id: string;
  local_pointer_sha256: string;
  outage_start: string;
  outage_end: string;
  backlog_before_ids: readonly string[]; // max 100000
  backlog_after_ids: readonly string[]; // max 100000
  backlog_before_count: number;
  backlog_after_count: number;
  lag_seconds: number;
  lag_threshold_seconds: number;
  retryable: boolean;
  retry_state: "queued" | "retrying" | "completed" | "failed";
  retry_transition: "queued" | "retrying" | "completed" | "failed";
  nas_failure_did_not_block_local: true;
  attempt_id: string;
  observed_at: string;
  observation_sha256: string;
}

interface SessionObservation {
  session: string;
  ordinal: number;
  frozen_versions_sha256: string;
  same_evening_published_at: string | null;
  next_morning_published_at: string | null;
  required_count: number;
  loaded_count: number;
  suspension_count: number;
  not_listed_count: number;
  delisted_count: number;
  unknown_count: number;
  canonical_provider_ids: readonly string[]; // 1..8; exactly 1 for a pure partition
  evidence: SessionEvidenceBinding;
  pointer_reconciliation: PointerReconciliation;
  replication_observation: ReplicationObservation;
  calendar_raw_facts: CalendarRawFacts;
  read_boundary_raw_facts: ReadBoundaryRawFacts;
  schema_policy_versions: readonly string[]; // fixed schema/policy version vector
  schema_policy_digest: string;
  cutoff_results: { same_evening: MetricResult; next_morning: MetricResult };
  coverage: MetricResult;
  canonical_integrity: MetricResult;
  source_purity: MetricResult;
  provenance: MetricResult;
  calendar: MetricResult;
  universe: MetricResult;
  replication: MetricResult;
  read_boundary: MetricResult;
  observation_sha256: string;
}

type WindowEvidenceBundle = ImmutableObservationEnvelopeV1<WindowEvidenceBundlePayload>;

interface WindowEvidenceBundlePayload {
  recovery_observation: ImmutableObservationEnvelopeV1<RecoveryObservation> | null;
  failover_observation: ImmutableObservationEnvelopeV1<WholeSessionFailoverDrill> | null;
  replay_sample: ImmutableObservationEnvelopeV1<ReplaySampleEvidence> | null;
  adjustment_equivalence: ImmutableObservationEnvelopeV1<AdjustmentEquivalenceEvidence> | null;
  error_handling_observation: ImmutableObservationEnvelopeV1<ErrorHandlingObservation> | null;
  local_nas_isolation_observation: ImmutableObservationEnvelopeV1<LocalNasIsolationObservation> | null;
  restore_observation: ImmutableObservationEnvelopeV1<RestoreDrillEvidence> | null;
  observation_count: 1;
}

interface WholeSessionFailoverDrill {
  source_schema: "task20-writer-owned";
  primary_unavailable: true;
  qualified_secondary_provider_id: string;
  qualification_proof_status: "available" | "unavailable";
  session: string;
  selected_provider_id: string;
  selection_sha256: string;
  manifest_sha256: string;
  pointer_sha256: string;
  readback_sha256: string;
  mixed_source_rows: 0;
}

interface ReplaySampleEvidence {
  sample_object_sha256: string; candidate_sha256: string; semantic_equal: true;
  offline_context: OfflineReplayContext;
}

interface AdjustmentEquivalenceEvidence {
  compared_sessions: readonly string[]; tolerance_policy_version: string; equivalence_passed: true;
}

interface RestoreDrillEvidence {
  source_schema: "task20-writer-owned";
  sentinel_sha256: string; destination_id: string; destination_generation: string;
  destination_head_sha256: string; record_sha256: string; manifest_sha256: string;
  checkpoint_id: string; restore_report_id: string; restore_report_sha256: string;
  schema_version: string; row_count: number; api_readback_sha256: string;
  verification_state: "verified";
}

interface CompletedReplicationRestoreSnapshotV1 {
  trust_scope: "LOCAL_CHAIN_ONLY" | "REMOTE_VERIFIED";
  destination_generation: string;
  destination_head_sha256: string;
  destination_record_sha256: string;
  checkpoint_id: string;
  replication_policy_version: string;
  restore_policy_version: string;
  replication_observation_sha256: string;
  restore_report_sha256: string | null;
  policy_thresholds: FrozenR2F4PolicyThresholds | null;
}

interface FrozenR2F4PolicyThresholds {
  source: "reviewed-r2f4-policy-evidence";
  policy_version: string;
  replication_lag_seconds: number;
  restore_duration_seconds: number;
}

interface OfflineReplayContext {
  adapter_id: string;
  adapter_version: string;
  normalizer_id: string;
  normalizer_version: string;
  implementation_sha256: string;
  network_allowed: false;
  provider_requests: 0;
}
```

HTTP behavior MUST be: `200` for a valid report including `ready`/`not_ready`; `503` for
`unavailable` required control/input state; `422` for invalid dates/configuration. Error bodies use
the same bounded projection and never contain paths or raw exceptions.

Before a snapshot can be captured, a required missing/corrupt/locked descriptor MUST produce a
`PreCaptureFailurePayloadV1` with `semantic_report_sha256` equal to the canonical hash of that
payload (excluding the hash field itself from its preimage). The report then has
`semantic_report_sha256` equal to that payload hash, `snapshot_identity=null`, and no fabricated
observations. This makes pre-capture failures deterministic and testable without nullable digest
ambiguity.
The canonical payload hash excludes only its hash field; requested range, Shanghai as-of and every
typed descriptor state remain in the preimage.

### `r2f-acceptance` CLI

```text
r2f-acceptance --start YYYY-MM-DD --end YYYY-MM-DD \
  [--local-dataset-root ABSOLUTE_PATH] \
  [--evidence-root ABSOLUTE_PATH]
```

The CLI has no execute/write mode. It prints exactly one JSON report to stdout, diagnostics only to
stderr, and never prints tokens, provider bodies, SQL, URLs or absolute paths. Exit `0` means a
valid ready/not-ready report, exit `1` means unavailable control/input state, and exit `2` means
invalid arguments or path configuration.

### Internal reader contract

```typescript
interface AcceptanceReader {
  evaluate(input: AcceptanceInput): R2FAcceptanceReport;
}

interface AcceptanceInput {
  start: string;
  end: string;
  local_dataset_root: string;
  evidence_root: string;
  control_store_roots: string[];
  now: string; // timezone-aware UTC instant; cutoffs are Asia/Shanghai
}
```

`evaluate` MUST be pure with respect to input stores. It MAY construct in-memory Pydantic models,
but MUST NOT call writer constructors, provider adapters, restore services in execute mode, or
database initialization/migration paths.

```typescript
interface SecondaryQualificationReader {
  read_immutable_projection(descriptor: ReadonlyEvidenceDescriptor): SecondaryQualificationProjection;
}

interface CompletedReplicationRestoreReader {
  read_completed_snapshot(descriptor: ReadonlyEvidenceDescriptor): CompletedReplicationRestoreSnapshotV1;
}

interface ReadonlyEvidenceDescriptor {
  descriptor_id: string;
  object_sha256: string;
  descriptor_sha256: string;
  immutable: true;
  completed: true;
  source_generation: string;
}

interface OfflineReplayRegistry {
  resolve(context: OfflineReplayContext): OfflineReplayCallable;
}

type OfflineReplayCallable = (immutable_bytes: Readonly<Uint8Array>) => Readonly<Uint8Array>;

interface OfflineReplayResolver {
  resolve_exact(context: OfflineReplayContext): OfflineReplayCallable;
}
```

`ErrorHandlingObservation.events` is an exact six-element tuple (exactly six records) in this order and with unique
`event_id`s: `timeout`, `auth`, `rate`, `schema`, `coverage`, `storage`. Each element records the
forced class, expected/observed class, sanitized normalized result, attempt ID and evidence hash;
any duplicate, omission, extra class or mismatch is unavailable. `RecoveryObservation` is one
window-level historical record: it binds before gap/queue restart generation to after manifest,
pointer and selection hashes, `publication_count=1` and a duplicate-proof hash. It cannot be
replaced by current queue status. `LocalNasIsolationObservation` is likewise one window record and
must bind local-ready/pointer identity, outage interval, backlog IDs/counts and retry transition;
`nas_failure_did_not_block_local=true` is required for a pass.

`read_status` and `read_window` are insufficient for qualification/admission or whole-session
failover acceptance because they expose current capability state, not an immutable 20-session
qualification/admission/drill projection. The planned readers above MUST assemble their projections
only from existing immutable evidence and audit objects, with all IDs/hashes bound to the descriptor;
they MUST NOT create, initialize, reconcile, or write. The completed replication/restore reader is
descriptor-native and MUST NOT reuse `RestoreAuditStore` `create=True`, reconcile, or writer paths.

`OfflineReplayRegistry`/`OfflineReplayResolver` is a strict injected seam. Resolution is by the
complete frozen provider/adapter/normalizer identity and implementation digest; unknown or mismatched
identity is unavailable. The callable receives immutable bytes only. A provider-request sentinel,
network/socket fake and `network_allowed=false` MUST prove zero login/query/socket calls. The existing
`EvidenceReader` default path MUST NOT be called directly because its default construction path can
resolve a provider-capable implementation; implementation MUST inject this offline-only resolver.

## Data Models

| Entity | Field | Type | Constraints |
| --- | --- | --- | --- |
| `ImmutableObservationEnvelopeV1<T>` | identity/hash/creator/payload | closed immutable generic envelope | exact 10 fields; project-canonical-json-v1; creator/version/timestamp and both SHA-256 values required |
| `FrozenReliabilityVersions` | `git_commit` | safe hex string | exact reviewed commit; required |
| `FrozenReliabilityVersions` | provider/adapter/policy fields | safe identifier | exact equality across all 20 sessions |
| `FrozenReliabilityVersions` | calendar/universe/replication/restore fields | safe ID + SHA-256 + closed trust scope | current F4 `LOCAL_CHAIN_ONLY` is insufficient for remote claims; absent Task20 proof is unavailable |
| `MetricResult` | status | enum | `pass`, `fail`, `unavailable` only |
| `MetricResult` | observed/target | bounded nonnegative scalar/closed literal | no arbitrary provider text; exact threshold required |
| `MetricResult` | reason/traceability | `AcceptanceReasonCode`, `AC-15`, planned anchor | reason is closed; every mandatory row carries its AC reference and unique planned pytest anchor |
| `SnapshotFingerprint` | root/database identity | closed role + present/absent descriptor identity; nullable identity fields only for typed absence | every configured input role is represented, including missing descriptors; changes invalidate report |
| `CapturedSnapshot` | selected sessions | tuple of dates | exactly 20, sorted, unique and confirmed |
| `CapturedSnapshot` | input fingerprints | tuple | one per every input root/control DB |
| `CapturedSnapshot` | versions | `FrozenReliabilityVersions` | one frozen vector for operation |
| `SessionObservation` | session/cutoff fields | session + same-evening/next-morning timezone-aware timestamps | timestamps are immutable observations, not recomputed after repair |
| `SessionObservation` | coverage counts | required/loaded/suspension/not-listed/delisted/unknown bounded integers | loaded equals required and unknown is zero for pass |
| `SessionObservation` | source set | bounded `string[]` | canonical provider/source set cardinality is one for purity pass |
| `SessionObservation` | evidence binding | `SessionEvidenceBinding` | evidence/candidate/gate/manifest/object/selection IDs and hashes bind transitively |
| `SessionObservation` | pointer reconciliation | `PointerReconciliation` | pointer, manifest and object hashes must agree; descriptor hash is required |
| `SessionObservation` | per-session fields | immutable typed observation records | date, cutoffs, counts, provider/source, lineage, pointer/manifest/object and read-boundary facts only |
| `SessionObservation` | replication | `ReplicationObservation` | state, record/digest/time, lag and trust/destination proofs are explicit |
| `SessionObservation` | frozen/calendar/read boundary | digest + `CalendarRawFacts` + `ReadBoundaryRawFacts` | digest equals report vector; raw facts, future-row counts and probe counts are reducer inputs |
| `WindowEvidenceBundle` | window-level drills | immutable typed bundle | recovery, whole-session failover, replay sample, adjustment, six-class errors, NAS isolation and restore are one bundle, not per-session claims |
| `R2FAcceptanceReport` | status | enum | `ready`, `not_ready`, `unavailable` |
| `R2FAcceptanceReport` | selected sessions | tuple of date values | exact 20 only for a candidate window |
| `R2FAcceptanceReport` | metric fields | `MetricResult` | all mandatory rows always present |
| `R2FAcceptanceReport` | quality issues | tuple of `QualityIssueCode` | stable order, max 64, no raw text |
| `R2FAcceptanceReport` | mutation markers | literals | provider requests `0`, writes `false`, restore/production flags `false` |
| `R2FAcceptanceReport` | session observations/refs | tuple/list of 20 | ordered by validated Shanghai session; exactly 20 when candidate window is evaluated |
| `R2FAcceptanceReport` | semantic identity | `SnapshotIdentity` + SHA-256 | required; excludes volatile diagnostic envelope |
| `R2FAcceptanceReport` | diagnostics | `DiagnosticEnvelope` | nonnegative bounded counters; not included in semantic digest |
| `SnapshotIdentity` | requested range/as-of/fingerprints | dates, UTC instant, tz literal, SHA-256 | all required and bound to one captured snapshot |
| `SecondaryQualificationProjection` | existing registry projection | exact `provider_record`/`qualification_window` fields | no invented admission/capability/qualification hashes; proof absence is unavailable |
| `WholeSessionFailoverDrill` | Task20 writer-owned artifact | IDs/SHA-256/booleans/count | current F4 has no whole-session proof; missing artifact is unavailable |
| `CompletedReplicationRestoreSnapshotV1` | existing replication/archive projection | checkpoint/record/head + `VerifiedArchiveSnapshot` fields and reviewed policy thresholds | no unsupported remote hash; Task20 drill envelope owns remote/readback fields |
| `OfflineReplayContext` | adapter/normalizer identity | safe IDs/SHA-256 + literal false/zero | unknown or network-capable identity rejected |
| `RecoveryObservation` | restart/publication proof | immutable IDs, generations, timestamp and SHA-256 | current queue state alone cannot satisfy historical recovery |
| `ErrorHandlingObservation` | forced-error mapping | closed class/reason/result plus event/attempt/record identity | each forced class maps to a sanitized result |
| `LocalNasIsolationObservation` | outage/retry proof | interval, local publication, lag, transition and record identity | local publication and retry history are immutable; current status alone is insufficient |
| `WindowEvidenceBundle` | bundle cardinality | exactly one immutable bundle per evaluation | all window-level records are present or typed unavailable; no per-session duplication claim |
| `PreCaptureFailurePayloadV1` | pre-capture failure | typed sanitized payload | canonical payload hash is the semantic report hash; no path or fabricated observation |

Every `sha256`/`digest` field in these models is listed exactly once in the X7
`digest_contracts` block. A contract names its canonicalization version, root object, exact
included paths, excluded self/digest/envelope fields, ordering, null/absence encoding and
domain-separation prefix; “complete object” is not an admissible implementation description.

```typescript
interface CapturedSnapshot {
  snapshot_identity: SnapshotIdentity;
  raw_calendar_observations: string[]; // source order retained; max 64
  confirmed_sessions: string[]; // exactly 20, sorted unique, confirmed, inclusive
  input_descriptors: SnapshotFingerprint[]; // every configured role, including typed absence
  frozen_versions: FrozenReliabilityVersions;
  session_observations: SessionObservation[]; // exactly 20, ordinal 1..20
  window_evidence_bundle: WindowEvidenceBundle | null; // exactly one ref when present
  captured_at_utc: string;
}
```

All identifiers MUST match `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`, all SHA-256 fields MUST match
`[0-9a-f]{64}`, all counters/durations MUST be integers in `[0, 2^31-1]`, and all serialized
collections MUST have explicit maximum lengths. `MetricResult.reason_code` and quality issues are
closed enums; unknown values, extra fields, negative values and ready-time nulls are validation
errors. A ready report MUST have `len(selected_sessions)=len(session_observations)=20` and
`frozen_versions != null`; an unavailable report MAY have null identity only when path/control
proof fails before a snapshot can be captured, and MUST state that reason.

### Status and reason vocabulary

The `ACCEPTANCE_REASON_CODES` literal above is the single machine-readable source of truth. The
implementation MUST derive `AcceptanceReasonCode` and `QualityIssueCode` from it, and MUST NOT
declare a second union, default list or ad-hoc reason. It includes `CONTINUITY_FAILED`,
`READ_BOUNDARY_FAILED`, `REMOTE_PROOF_MISSING` and `REPLAY_SEMANTIC_MISMATCH` so every metric,
quality issue and sanitized default has one closed code. Unknown values and duplicate literals are
validator errors; arbitrary exception text MUST NOT cross the API/CLI boundary.

Reason precedence MUST be deterministic and exact, in this order: (1) `INVALID_ARGUMENTS`,
`PATH_INVALID`; (2) `SNAPSHOT_CHANGED`, `CONTROL_STATE_UNAVAILABLE`; (3)
`PIT_VISIBILITY_INVALID`; (4) `CALENDAR_UNAVAILABLE`, `CALENDAR_CONFLICT`; (5)
`SESSION_SEQUENCE_INVALID`, `SESSION_COUNT_NOT_20`; (6) `VERSION_DRIFT`; (7)
`LINEAGE_UNAVAILABLE`, `CANONICAL_INTEGRITY_FAILED`; (8) `UNIVERSE_UNKNOWN_NONZERO`,
`UNIVERSE_COUNT_MISMATCH`; (9) per-session fields in this exact order: `CONTINUITY_FAILED`,
`AVAILABILITY_CUTOFF_FAILED`, `COVERAGE_FAILED`, `SOURCE_PURITY_FAILED`, `RECOVERY_FAILED`,
`FAILOVER_UNAVAILABLE`, `LINEAGE_UNAVAILABLE`, `REPLAY_UNAVAILABLE`,
`REPLAY_SEMANTIC_MISMATCH`, `ADJUSTMENT_UNAVAILABLE`, `ERROR_HANDLING_FAILED`,
`LOCAL_NAS_ISOLATION_FAILED`, `REPLICATION_UNAVAILABLE`, `REPLICATION_LAG`,
`REMOTE_PROOF_MISSING`, `RESTORE_UNAVAILABLE`, `READ_BOUNDARY_FAILED`; (10) `INPUT_LIMIT_EXCEEDED`.
The report retains all applicable metric failures in metric order even when top-level status is
unavailable.

<!-- R2F5_X7_CONTRACTS_JSON -->
```json
{
  "contract_version": "r2f5-x7",
  "roadmap_dimensions": ["continuity", "next_morning_availability", "same_evening_availability", "coverage", "canonical_integrity", "source_purity", "recovery", "failover", "provenance", "replay", "adjustment", "calendar", "universe", "error_handling", "local_nas_isolation", "restore", "read_boundary"],
  "metric_fields": ["continuity", "next_morning_availability", "same_evening_availability", "coverage", "canonical_integrity", "source_purity", "recovery", "failover", "provenance", "replay", "adjustment", "calendar", "universe", "error_handling", "local_nas_isolation", "replication", "restore", "read_boundary"],
  "reason_partitions": {
    "failure": ["CONTINUITY_FAILED", "AVAILABILITY_CUTOFF_FAILED", "COVERAGE_FAILED", "SOURCE_PURITY_FAILED", "CANONICAL_INTEGRITY_FAILED", "RECOVERY_FAILED", "ERROR_HANDLING_FAILED", "LOCAL_NAS_ISOLATION_FAILED", "REPLICATION_LAG", "REPLAY_SEMANTIC_MISMATCH", "READ_BOUNDARY_FAILED", "CALENDAR_CONFLICT", "UNIVERSE_UNKNOWN_NONZERO", "UNIVERSE_COUNT_MISMATCH", "VERSION_DRIFT", "LINEAGE_INVALID"],
    "unavailable": ["INVALID_ARGUMENTS", "PATH_INVALID", "SNAPSHOT_CHANGED", "CONTROL_STATE_UNAVAILABLE", "PIT_VISIBILITY_INVALID", "CALENDAR_UNAVAILABLE", "SESSION_SEQUENCE_INVALID", "SESSION_COUNT_NOT_20", "LINEAGE_UNAVAILABLE", "FAILOVER_UNAVAILABLE", "REPLAY_UNAVAILABLE", "ADJUSTMENT_UNAVAILABLE", "REPLICATION_UNAVAILABLE", "REMOTE_PROOF_MISSING", "RESTORE_UNAVAILABLE", "NONE", "DISABLED", "SOURCE_NOT_CONFIGURED", "SOURCE_UNAVAILABLE", "LOCAL_POINTER_MISMATCH", "REPLICATION_STATE_UNAVAILABLE", "DESTINATION_UNAVAILABLE", "DESTINATION_TRUST_FAILED", "COPY_FAILED", "VERIFY_FAILED", "RETRY_WAIT", "DEAD_LETTER", "INPUT_LIMIT_EXCEEDED"]
  },
  "status_reason_matrix": {
    "pass": [null],
    "fail": ["failure"],
    "unavailable": ["unavailable"]
  },
  "metric_reducers": {
    "continuity": ["session.session", "session.ordinal"],
    "next_morning_availability": ["session.next_morning_published_at"],
    "same_evening_availability": ["session.same_evening_published_at"],
    "coverage": ["session.required_count", "session.loaded_count", "session.unknown_count"],
    "canonical_integrity": ["session.pointer_reconciliation", "session.evidence.manifest_sha256", "session.evidence.object_sha256"],
    "source_purity": ["session.canonical_provider_ids"],
    "recovery": ["window.recovery_observation.payload"],
    "failover": ["window.failover_observation.payload"],
    "provenance": ["session.evidence"],
    "replay": ["window.replay_sample.payload"],
    "adjustment": ["window.adjustment_equivalence.payload"],
    "calendar": ["session.calendar_raw_facts"],
    "universe": ["session.required_count", "session.loaded_count", "session.suspension_count", "session.not_listed_count", "session.delisted_count", "session.unknown_count"],
    "error_handling": ["window.error_handling_observation.payload"],
    "local_nas_isolation": ["window.local_nas_isolation_observation.payload"],
    "replication": ["session.replication_observation", "frozen_versions.replication_policy_version"],
    "restore": ["window.restore_observation.payload"],
    "read_boundary": ["session.read_boundary_raw_facts"]
  },
  "cardinality": {"sessions": 20, "window_bundle": 1, "error_classes": 6, "input_roles_max": 32, "tree_entries_max": 100000, "input_bytes_max": 536870912},
  "artifact_envelope_fields": ["artifact_id", "artifact_ref", "schema_version", "creator_kind", "creator_version", "created_at", "payload", "canonicalization_version", "payload_sha256", "envelope_sha256"],
  "date_time_formats": {
    "session_date": "YYYY-MM-DD / ^\\d{4}-\\d{2}-\\d{2}$ plus calendar-valid date",
    "rfc3339_utc": "RFC3339 with seconds or micros, normalized to UTC Z",
    "as_of": "UTC instant converted to Asia/Shanghai for civil-day cutoffs"
  },
  "metric_value_kinds": {
    "continuity": {"observed": "count", "target": "count", "unavailable_null": true, "range": "[0,20]"},
    "next_morning_availability": {"observed": "ratio", "target": "ratio", "unavailable_null": true, "range": "[0,1]"},
    "same_evening_availability": {"observed": "ratio", "target": "ratio", "unavailable_null": true, "range": "[0,1]"},
    "coverage": {"observed": "ratio", "target": "ratio", "unavailable_null": true, "range": "[0,1]"},
    "canonical_integrity": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "source_purity": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "recovery": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "failover": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "provenance": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "replay": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "adjustment": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "calendar": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "universe": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "error_handling": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "local_nas_isolation": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "replication": {"observed": "duration_seconds", "target": "duration_seconds", "unavailable_null": true, "range": "[0,2147483647]"},
    "restore": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"},
    "read_boundary": {"observed": "bool", "target": "bool", "unavailable_null": true, "range": "boolean"}
  },
  "creator_allowlist": {
    "production_creator_kind": "task20_writer",
    "test_envelope": {"schema_version": "r2f5-test-envelope-v1", "creator_kind": "test_fixture", "production_reader_accepts": false},
    "production_envelope_payloads": {"WindowEvidenceBundlePayload": ["task20_writer"], "RecoveryObservation": ["task20_writer"], "WholeSessionFailoverDrill": ["task20_writer"], "ReplaySampleEvidence": ["task20_writer"], "AdjustmentEquivalenceEvidence": ["task20_writer"], "ErrorHandlingObservation": ["task20_writer"], "LocalNasIsolationObservation": ["task20_writer"], "RestoreDrillEvidence": ["task20_writer"]},
    "reader_projections": {"SecondaryQualificationProjection": [], "CompletedReplicationRestoreSnapshotV1": []},
    "synthetic_envelope_only": ["test_fixture"],
    "reader_never_creates": ["r2f5_reader", "r2f4_writer"]
  },
  "limits": {"max_entries": 100000, "max_input_bytes": 536870912, "max_db_rows": 1000000, "max_input_roots": 32, "max_sessions": 20, "max_replay_samples": 3, "max_elapsed_ms": 10000},
  "model_schema_ast": {
    "ImmutableObservationEnvelopeV1": {"object_fields": ["artifact_id", "artifact_ref", "schema_version", "creator_kind", "creator_version", "created_at", "payload", "canonicalization_version", "payload_sha256", "envelope_sha256"], "tuple_item_schemas": {}},
    "TestEnvelope": {"object_fields": ["artifact_id", "artifact_ref", "schema_version", "creator_kind", "creator_version", "created_at", "payload", "canonicalization_version", "payload_sha256", "envelope_sha256"], "tuple_item_schemas": {}},
    "PreCaptureFailurePayloadV1": {"object_fields": ["schema_version", "reason_code", "requested_start", "requested_end", "as_of_utc", "descriptor_states", "semantic_report_sha256"], "tuple_item_schemas": {}},
    "CalendarRawFacts": {"object_fields": ["source_sequence", "generation", "confirmed", "unknown_state", "conflict_state", "raw_facts_sha256"], "tuple_item_schemas": {}},
    "ReadBoundaryRawFacts": {"object_fields": ["requested_as_of", "max_visible_session", "future_rows_seen", "future_rows_count", "query_count", "write_count", "probe_schema_digest"], "tuple_item_schemas": {}},
    "SessionEvidenceBinding": {"object_fields": ["evidence_id", "evidence_sha256", "candidate_id", "candidate_sha256", "gate_report_id", "gate_report_sha256", "manifest_id", "manifest_sha256", "object_id", "object_sha256", "selection_id", "selection_sha256", "binding_sha256"], "tuple_item_schemas": {}},
    "PointerReconciliation": {"object_fields": ["pointer_id", "pointer_sha256", "manifest_sha256", "object_sha256", "pointer_manifest_object_match", "descriptor_sha256"], "tuple_item_schemas": {}},
    "ReplicationObservation": {"object_fields": ["immutable", "state", "checkpoint_id", "source_commit_sha256", "intent_id", "enqueue_state", "reason_code", "observed_at", "lag_seconds", "trust_scope", "destination_generation", "destination_record_sha256", "destination_head_sha256", "observation_sha256"], "tuple_item_schemas": {}},
    "RecoveryObservation": {"object_fields": ["immutable", "event_id", "attempt_id", "before_generation", "after_generation", "queue_identity", "restart_boundary", "exactly_once_publication_id", "after_manifest_sha256", "after_pointer_sha256", "after_selection_sha256", "publication_count", "duplicate_proof_sha256", "observed_at", "observation_sha256"], "tuple_item_schemas": {}},
    "ErrorHandlingObservation": {"object_fields": ["immutable", "events", "observation_sha256"], "tuple_item_schemas": {"events": ["event_id", "forced_error_class", "sanitized_reason", "normalized_result", "attempt_id", "expected_class", "observed_class", "evidence_sha256", "observed_at"]}},
    "LocalNasIsolationObservation": {"object_fields": ["immutable", "event_id", "local_publication_ready", "local_publication_id", "local_pointer_sha256", "outage_start", "outage_end", "backlog_before_ids", "backlog_after_ids", "backlog_before_count", "backlog_after_count", "lag_seconds", "lag_threshold_seconds", "retryable", "retry_state", "retry_transition", "nas_failure_did_not_block_local", "attempt_id", "observed_at", "observation_sha256"], "tuple_item_schemas": {}},
    "SessionObservation": {"object_fields": ["session", "ordinal", "frozen_versions_sha256", "same_evening_published_at", "next_morning_published_at", "required_count", "loaded_count", "suspension_count", "not_listed_count", "delisted_count", "unknown_count", "canonical_provider_ids", "evidence", "pointer_reconciliation", "replication_observation", "calendar_raw_facts", "read_boundary_raw_facts", "schema_policy_versions", "schema_policy_digest", "cutoff_results", "coverage", "canonical_integrity", "source_purity", "provenance", "calendar", "universe", "replication", "read_boundary", "observation_sha256"], "tuple_item_schemas": {}},
    "SnapshotIdentity": {"object_fields": ["requested_start", "requested_end", "as_of_utc", "as_of_timezone", "input_fingerprints", "frozen_versions", "input_fingerprint_sha256", "frozen_version_vector_sha256", "snapshot_sha256"], "tuple_item_schemas": {}},
    "R2FAcceptanceReport": {"object_fields": ["status", "window_start", "window_end", "selected_sessions", "frozen_versions", "continuity", "next_morning_availability", "same_evening_availability", "coverage", "canonical_integrity", "source_purity", "recovery", "failover", "provenance", "replay", "adjustment", "calendar", "universe", "error_handling", "local_nas_isolation", "replication", "restore", "read_boundary", "quality_issues", "snapshot_identity", "session_observations", "observation_refs", "window_evidence_bundle", "window_evidence_refs", "pre_capture_failure", "semantic_report_sha256", "provider_requests", "writes", "restore_started", "production_window_started"], "tuple_item_schemas": {}},
    "SnapshotFingerprint": {"object_fields": ["descriptor_role", "descriptor_id", "descriptor_state", "device", "inode", "size_bytes", "mtime_ns", "ctime_ns", "fingerprint_kind", "hash_scope", "sha256"], "tuple_item_schemas": {}},
    "ReadonlyEvidenceDescriptor": {"object_fields": ["descriptor_id", "object_sha256", "descriptor_sha256", "immutable", "completed", "source_generation"], "tuple_item_schemas": {}},
    "CompletedReplicationRestoreSnapshotV1": {"object_fields": ["trust_scope", "destination_generation", "destination_head_sha256", "destination_record_sha256", "checkpoint_id", "replication_policy_version", "restore_policy_version", "replication_observation_sha256", "restore_report_sha256", "policy_thresholds"], "tuple_item_schemas": {}},
    "WholeSessionFailoverDrill": {"object_fields": ["source_schema", "primary_unavailable", "qualified_secondary_provider_id", "qualification_proof_status", "session", "selected_provider_id", "selection_sha256", "manifest_sha256", "pointer_sha256", "readback_sha256", "mixed_source_rows"], "tuple_item_schemas": {}},
    "ReplaySampleEvidence": {"object_fields": ["sample_object_sha256", "candidate_sha256", "semantic_equal", "offline_context"], "tuple_item_schemas": {}},
    "RestoreDrillEvidence": {"object_fields": ["source_schema", "sentinel_sha256", "destination_id", "destination_generation", "destination_head_sha256", "record_sha256", "manifest_sha256", "checkpoint_id", "restore_report_id", "restore_report_sha256", "schema_version", "row_count", "api_readback_sha256", "verification_state"], "tuple_item_schemas": {}},
    "OfflineReplayContext": {"object_fields": ["adapter_id", "adapter_version", "normalizer_id", "normalizer_version", "implementation_sha256", "network_allowed", "provider_requests"], "tuple_item_schemas": {}},
    "EvidenceObject": {"object_fields": ["evidence_id", "immutable_evidence_bytes"], "tuple_item_schemas": {}},
    "ObjectEvidence": {"object_fields": ["object_id", "immutable_object_bytes"], "tuple_item_schemas": {}},
    "CandidateObject": {"object_fields": ["candidate_id", "immutable_candidate_bytes"], "tuple_item_schemas": {}},
    "GateReport": {"object_fields": ["gate_report_id", "immutable_gate_report_bytes"], "tuple_item_schemas": {}},
    "Manifest": {"object_fields": ["manifest_id", "immutable_manifest_bytes"], "tuple_item_schemas": {}},
    "SessionSelection": {"object_fields": ["selection_id", "immutable_selection_bytes"], "tuple_item_schemas": {}},
    "PointerRecord": {"object_fields": ["pointer_id", "immutable_pointer_bytes"], "tuple_item_schemas": {}},
    "InputDescriptor": {"object_fields": ["descriptor_id", "descriptor_metadata"], "tuple_item_schemas": {}},
    "ReplicationRecord": {"object_fields": ["source_commit_id", "source_commit_bytes", "destination_record_id", "destination_record_bytes", "destination_generation", "immutable_destination_record_bytes"], "tuple_item_schemas": {}},
    "DestinationHead": {"object_fields": ["destination_generation", "destination_head_bytes", "immutable_destination_head_bytes"], "tuple_item_schemas": {}},
    "RecoveryProof": {"object_fields": ["event_id", "attempt_id", "before_generation", "after_generation", "queue_identity", "restart_boundary", "exactly_once_publication_id", "after_manifest_sha256", "after_pointer_sha256", "after_selection_sha256", "publication_count"], "tuple_item_schemas": {}},
    "ForcedErrorEvidence": {"object_fields": ["event_id", "forced_error_class", "immutable_evidence_bytes"], "tuple_item_schemas": {}},
    "LocalPublicationPointer": {"object_fields": ["local_publication_id", "immutable_pointer_bytes"], "tuple_item_schemas": {}},
    "ReplaySample": {"object_fields": ["sample_object_id", "immutable_sample_bytes"], "tuple_item_schemas": {}},
    "RestoreSentinel": {"object_fields": ["sentinel_id", "immutable_sentinel_bytes"], "tuple_item_schemas": {}},
    "RestoreRecord": {"object_fields": ["record_id", "immutable_record_bytes"], "tuple_item_schemas": {}},
    "RestoreReport": {"object_fields": ["restore_report_id", "immutable_restore_report_bytes"], "tuple_item_schemas": {}},
    "RestoreApiReadback": {"object_fields": ["readback_id", "immutable_readback_bytes"], "tuple_item_schemas": {}},
    "FingerprintSubject": {"object_fields": ["descriptor_role", "descriptor_id", "descriptor_state", "device", "inode", "size_bytes", "mtime_ns", "ctime_ns", "fingerprint_kind", "hash_scope", "captured_content_bytes"], "tuple_item_schemas": {}},
    "AcceptanceConfig": {"object_fields": ["dataset_root_descriptor", "evidence_root_descriptor", "control_store_descriptor", "clock_policy", "cutoff_policy", "limits", "replay_policy", "replication_policy", "restore_policy", "redaction_policy"], "tuple_item_schemas": {}},
    "SchemaPolicyVector": {"object_fields": ["schema_policy_versions"], "tuple_item_schemas": {}},
    "InstalledRelease": {"object_fields": ["release_identity", "immutable_release_bytes"], "tuple_item_schemas": {}},
    "CalendarGeneration": {"object_fields": ["calendar_generation", "immutable_calendar_bytes"], "tuple_item_schemas": {}},
    "UniverseGeneration": {"object_fields": ["universe_generation", "immutable_universe_bytes"], "tuple_item_schemas": {}},
    "OfflineImplementation": {"object_fields": ["adapter_id", "adapter_version", "normalizer_id", "normalizer_version", "immutable_implementation_bytes"], "tuple_item_schemas": {}},
    "FrozenReliabilityVersions": {"object_fields": ["git_commit", "installed_release", "installed_release_sha256", "dataset_generation", "canonical_schema", "evidence_schema", "primary_provider_id", "secondary_provider_id", "qualification_window_id", "qualification_proof_status", "adapter_hash", "endpoint_contract_hash", "source_schema_hash", "normalizer_hash", "reconciliation_policy_version", "selection_policy_version", "config_digest", "auto_failover_enabled", "failover_kill_switch", "provider_priority", "continuity_start_date", "repair_policy_version", "calendar_generation", "calendar_sha256", "universe_generation", "universe_sha256", "replication_policy_version", "replication_evidence_version", "replication_trust_scope", "destination_generation", "destination_head_sha256", "remote_proof_artifact_ref", "restore_policy_version", "restore_evidence_version"], "tuple_item_schemas": {}},
    "FailoverReadback": {"object_fields": ["readback_id", "immutable_readback_bytes"], "tuple_item_schemas": {}},
    "ReadonlyEvidenceObject": {"object_fields": ["descriptor_id", "immutable_object_bytes"], "tuple_item_schemas": {}},
    "R2FAcceptanceReport semantic payload": {"object_fields": ["status", "window_start", "window_end", "selected_sessions", "frozen_versions", "continuity", "next_morning_availability", "same_evening_availability", "coverage", "canonical_integrity", "source_purity", "recovery", "failover", "provenance", "replay", "adjustment", "calendar", "universe", "error_handling", "local_nas_isolation", "replication", "restore", "read_boundary", "quality_issues", "snapshot_identity", "session_observations", "observation_refs", "window_evidence_bundle", "window_evidence_refs", "pre_capture_failure", "provider_requests", "writes", "restore_started", "production_window_started"], "tuple_item_schemas": {}},
    "SnapshotFingerprint[]": {"object_fields": ["input_fingerprints"], "tuple_item_schemas": {}}
  },
  "digest_fields_by_model": {"CalendarRawFacts": ["raw_facts_sha256"], "CompletedReplicationRestoreSnapshotV1": ["replication_observation_sha256", "restore_report_sha256", "destination_record_sha256", "destination_head_sha256"], "ErrorHandlingObservation": ["observation_sha256"], "ErrorHandlingObservation.events": ["evidence_sha256"], "FrozenReliabilityVersions": ["config_digest", "installed_release_sha256", "calendar_sha256", "universe_sha256", "destination_head_sha256"], "ImmutableObservationEnvelopeV1": ["payload_sha256", "envelope_sha256"], "LocalNasIsolationObservation": ["observation_sha256", "local_pointer_sha256"], "OfflineReplayContext": ["implementation_sha256"], "PointerReconciliation": ["pointer_sha256", "manifest_sha256", "object_sha256", "descriptor_sha256"], "PreCaptureFailurePayloadV1": ["semantic_report_sha256"], "R2FAcceptanceReport": ["semantic_report_sha256"], "ReadBoundaryRawFacts": ["probe_schema_digest"], "ReadonlyEvidenceDescriptor": ["object_sha256", "descriptor_sha256"], "RecoveryObservation": ["duplicate_proof_sha256", "observation_sha256", "after_manifest_sha256", "after_pointer_sha256", "after_selection_sha256"], "ReplaySampleEvidence": ["sample_object_sha256", "candidate_sha256"], "ReplicationObservation": ["source_commit_sha256", "destination_record_sha256", "destination_head_sha256", "observation_sha256"], "RestoreDrillEvidence": ["sentinel_sha256", "destination_head_sha256", "record_sha256", "manifest_sha256", "restore_report_sha256", "api_readback_sha256"], "SessionEvidenceBinding": ["evidence_sha256", "candidate_sha256", "gate_report_sha256", "manifest_sha256", "object_sha256", "selection_sha256", "binding_sha256"], "SessionObservation": ["frozen_versions_sha256", "schema_policy_digest", "observation_sha256"], "SnapshotFingerprint": ["sha256"], "SnapshotIdentity": ["input_fingerprint_sha256", "frozen_version_vector_sha256", "snapshot_sha256"], "WholeSessionFailoverDrill": ["selection_sha256", "manifest_sha256", "pointer_sha256", "readback_sha256"]},
  "sqlite_catalogs": {
    "replication_sidecar": {"role": "replication", "user_version": 1, "allowed_tables": ["replication_sidecar_meta", "replication_intents", "replication_destination_cache", "replication_attempt_events", "replication_heads"], "system_tables": ["sqlite_sequence"], "tables": {"replication_sidecar_meta": {"columns": ["sidecar_id:INTEGER", "schema_version:INTEGER", "schema_identity:TEXT", "ddl_sha256:TEXT", "schema_digest:TEXT", "generation_number:INTEGER"], "primary_key": ["sidecar_id"], "order_by": ["sidecar_id"]}, "replication_intents": {"columns": ["intent_id:TEXT", "schema_version:INTEGER", "operation_day:TEXT", "direction:TEXT", "destination_id:TEXT", "checkpoint_id:TEXT", "row_count:INTEGER", "byte_count:INTEGER", "created_at:TEXT"], "primary_key": ["intent_id"], "order_by": ["intent_id"]}, "replication_destination_cache": {"columns": ["destination_id:TEXT", "descriptor_sha256:TEXT", "head_sha256:TEXT", "replication_generation:TEXT", "record_sha256:TEXT", "health_state:TEXT", "updated_at:TEXT"], "primary_key": ["destination_id"], "order_by": ["destination_id"]}, "replication_attempt_events": {"columns": ["event_id:TEXT", "intent_id:TEXT", "event_sequence:INTEGER", "to_state:TEXT", "event_sha256:TEXT", "occurred_at:TEXT"], "primary_key": ["event_id"], "order_by": ["intent_id", "event_sequence"]}, "replication_heads": {"columns": ["intent_id:TEXT", "current_state:TEXT", "state_version:INTEGER", "last_event_sequence:INTEGER", "updated_at:TEXT"], "primary_key": ["intent_id"], "order_by": ["intent_id"]}}, "catalog_digest_source": "existing-r2f4.3-replication-sidecar-ddl"},
    "daily_shadow": {"role": "qualification", "user_version": 1, "allowed_tables": ["schema_migration", "daily_shadow_terms_evidence", "daily_shadow_contract", "daily_shadow_epoch", "daily_shadow_window", "daily_shadow_job", "daily_shadow_attempt_audit", "daily_shadow_evidence_ref", "daily_shadow_candidate_ref", "daily_shadow_session_report", "daily_shadow_terminal_attestation", "daily_shadow_circuit", "daily_shadow_circuit_event"], "system_tables": ["sqlite_sequence"], "tables": {"schema_migration": {"columns": ["migration_id:TEXT", "schema_version:INTEGER", "checksum:TEXT", "applied_at:TEXT"], "primary_key": ["migration_id"], "order_by": ["migration_id"]}, "daily_shadow_terms_evidence": {"columns": ["terms_evidence_sha256:TEXT", "terms_evidence_id:TEXT", "review_id:TEXT", "contract_version:TEXT", "installed_at:TEXT"], "primary_key": ["terms_evidence_sha256"], "order_by": ["terms_evidence_sha256"]}, "daily_shadow_contract": {"columns": ["descriptor_sha256:TEXT", "provider:TEXT", "profile:TEXT", "terms_evidence_sha256:TEXT"], "primary_key": ["descriptor_sha256"], "order_by": ["descriptor_sha256"]}, "daily_shadow_epoch": {"columns": ["epoch_id:TEXT", "epoch_ordinal:INTEGER", "epoch_state:TEXT", "calendar_generation:TEXT", "calendar_sha256:TEXT"], "primary_key": ["epoch_id"], "order_by": ["epoch_ordinal"]}, "daily_shadow_window": {"columns": ["provider:TEXT", "profile:TEXT", "epoch_id:TEXT", "window_state:TEXT", "consecutive_sessions:INTEGER"], "primary_key": ["provider", "profile"], "order_by": ["provider", "profile"]}, "daily_shadow_job": {"columns": ["job_id:TEXT", "epoch_id:TEXT", "session_id:TEXT", "trade_date:TEXT", "run_status:TEXT"], "primary_key": ["job_id"], "order_by": ["epoch_id", "trade_date", "job_id"]}, "daily_shadow_attempt_audit": {"columns": ["audit_id:TEXT", "job_id:TEXT", "session_id:TEXT", "ordinal:INTEGER", "audit_sha256:TEXT"], "primary_key": ["audit_id"], "order_by": ["job_id", "ordinal"]}, "daily_shadow_evidence_ref": {"columns": ["evidence_id:TEXT", "session_id:TEXT", "evidence_sha256:TEXT", "bundle_sha256:TEXT"], "primary_key": ["evidence_id"], "order_by": ["session_id", "evidence_id"]}, "daily_shadow_candidate_ref": {"columns": ["candidate_id:TEXT", "evidence_id:TEXT", "session_id:TEXT", "candidate_sha256:TEXT"], "primary_key": ["candidate_id"], "order_by": ["session_id", "candidate_id"]}, "daily_shadow_session_report": {"columns": ["session_report_id:TEXT", "session_id:TEXT", "trade_date:TEXT", "outcome:TEXT", "report_sha256:TEXT"], "primary_key": ["session_report_id"], "order_by": ["trade_date", "session_report_id"]}, "daily_shadow_terminal_attestation": {"columns": ["attestation_id:TEXT", "session_report_id:TEXT", "attestation_sha256:TEXT", "immutable_version:INTEGER"], "primary_key": ["attestation_id"], "order_by": ["attestation_id"]}, "daily_shadow_circuit": {"columns": ["circuit_id:TEXT", "state:TEXT"], "primary_key": ["circuit_id"], "order_by": ["circuit_id"]}, "daily_shadow_circuit_event": {"columns": ["event_id:TEXT", "circuit_id:TEXT", "event_sha256:TEXT", "occurred_at:TEXT"], "primary_key": ["event_id"], "order_by": ["circuit_id", "occurred_at", "event_id"]}}, "catalog_digest_source": "existing-r2f4-daily-shadow-ddl"},
    "shadow_registry": {"role": "qualification", "user_version": 2, "allowed_tables": ["schema_migration", "terms_evidence", "provider_record", "qualification_window", "shadow_job", "shadow_evidence_ref", "shadow_candidate_ref", "session_report", "shadow_attempt_report", "shadow_evidence_attempt_ref", "shadow_terminal_attestation", "review_object", "qualification_session", "quarantine_snapshot"], "system_tables": ["sqlite_sequence"], "tables": {"schema_migration": {"columns": ["migration_id:TEXT", "schema_version:INTEGER", "applied_at:TEXT", "checksum:TEXT"], "primary_key": ["migration_id"], "order_by": ["migration_id"]}, "terms_evidence": {"columns": ["terms_evidence_id:TEXT", "provider_id:TEXT", "manifest_sha256:TEXT", "review_id:TEXT"], "primary_key": ["terms_evidence_id"], "order_by": ["terms_evidence_id"]}, "provider_record": {"columns": ["provider_id:TEXT", "admission_state:TEXT", "adapter_hash:TEXT", "endpoint_contract_hash:TEXT", "source_schema_hash:TEXT", "normalizer_hash:TEXT", "reconciliation_policy_hash:TEXT", "terms_evidence_hash:TEXT", "terms_review_id:TEXT", "state_version:INTEGER"], "primary_key": ["provider_id"], "order_by": ["provider_id"]}, "qualification_window": {"columns": ["provider_id:TEXT", "window_id:TEXT", "window_start:TEXT", "window_end:TEXT", "consecutive_sessions:INTEGER", "version_vector_sha256:TEXT", "calendar_generation:TEXT", "calendar_sha256:TEXT", "window_state:TEXT", "last_session_report_id:TEXT", "qualification_evidence_sha256:TEXT", "qualification_candidate_sha256:TEXT", "terminal_attestation_id:TEXT", "state_version:INTEGER"], "primary_key": ["provider_id", "window_id"], "order_by": ["provider_id", "window_id"]}, "shadow_job": {"columns": ["job_id:TEXT", "provider_id:TEXT", "window_id:TEXT", "trade_date:TEXT", "universe_id:TEXT", "canonical_manifest_generation:TEXT", "canonical_manifest_sha256:TEXT", "version_vector_sha256:TEXT", "run_status:TEXT", "state_version:INTEGER"], "primary_key": ["job_id"], "order_by": ["provider_id", "window_id", "trade_date", "job_id"]}, "shadow_evidence_ref": {"columns": ["evidence_id:TEXT", "job_id:TEXT", "provider_id:TEXT", "window_id:TEXT", "session_id:TEXT", "evidence_sha256:TEXT", "bundle_ref:TEXT", "bundle_sha256:TEXT"], "primary_key": ["evidence_id"], "order_by": ["provider_id", "window_id", "session_id", "evidence_id"]}, "shadow_candidate_ref": {"columns": ["candidate_id:TEXT", "evidence_id:TEXT", "job_id:TEXT", "provider_id:TEXT", "window_id:TEXT", "session_id:TEXT", "candidate_ref:TEXT", "candidate_sha256:TEXT"], "primary_key": ["candidate_id"], "order_by": ["provider_id", "window_id", "session_id", "candidate_id"]}, "session_report": {"columns": ["session_report_id:TEXT", "provider_id:TEXT", "job_id:TEXT", "window_id:TEXT", "session_id:TEXT", "trade_date:TEXT", "outcome:TEXT", "calendar_generation:TEXT", "calendar_sha256:TEXT", "universe_sha256:TEXT", "version_vector_sha256:TEXT", "report_ref:TEXT", "report_sha256:TEXT"], "primary_key": ["session_report_id"], "order_by": ["provider_id", "window_id", "trade_date", "session_report_id"]}, "shadow_attempt_report": {"columns": ["attempt_id:TEXT", "report_id:TEXT", "job_id:TEXT", "provider_id:TEXT", "window_id:TEXT", "session_id:TEXT", "version_vector_sha256:TEXT", "outcome:TEXT", "started_at:TEXT", "completed_at:TEXT", "coverage_expected:INTEGER", "coverage_observed:INTEGER", "report_sha256:TEXT"], "primary_key": ["attempt_id"], "order_by": ["provider_id", "window_id", "session_id", "attempt_id"]}, "shadow_evidence_attempt_ref": {"columns": ["evidence_id:TEXT", "provider_id:TEXT", "job_id:TEXT", "window_id:TEXT", "session_id:TEXT", "logical_request_ordinal:INTEGER", "attempt_id:TEXT"], "primary_key": ["evidence_id", "logical_request_ordinal"], "order_by": ["evidence_id", "logical_request_ordinal"]}, "shadow_terminal_attestation": {"columns": ["attestation_id:TEXT", "provider_id:TEXT", "job_id:TEXT", "window_id:TEXT", "session_id:TEXT", "evidence_id:TEXT", "candidate_id:TEXT", "session_report_id:TEXT", "report_digest_sha256:TEXT", "evidence_sha256:TEXT", "candidate_sha256:TEXT", "immutable_version:INTEGER"], "primary_key": ["attestation_id"], "order_by": ["provider_id", "window_id", "session_id", "attestation_id"]}, "review_object": {"columns": ["review_object_id:TEXT", "provider_id:TEXT", "terms_evidence_hash:TEXT", "adapter_hash:TEXT", "version_vector_sha256:TEXT", "reviewed_at:TEXT"], "primary_key": ["review_object_id"], "order_by": ["review_object_id"]}, "qualification_session": {"columns": ["provider_id:TEXT", "window_id:TEXT", "trade_date:TEXT", "session_report_id:TEXT", "terminal_attestation_id:TEXT", "calendar_generation:TEXT", "calendar_sha256:TEXT"], "primary_key": ["provider_id", "window_id", "trade_date"], "order_by": ["provider_id", "window_id", "trade_date"]}, "quarantine_snapshot": {"columns": ["provider_id:TEXT", "adapter_hash:TEXT", "terms_evidence_hash:TEXT", "version_vector_sha256:TEXT"], "primary_key": ["provider_id"], "order_by": ["provider_id"]}}, "catalog_digest_source": "existing-r2f4-shadow-registry-schema-version-2"},
    "calendar_generation": {"role": "calendar", "user_version": 1, "allowed_tables": ["calendar_generation_meta", "calendar_generation_candidate", "calendar_official_object", "calendar_maintenance_attempt", "calendar_generation_promotion", "calendar_generation_head"], "system_tables": ["sqlite_sequence"], "tables": {"calendar_generation_meta": {"columns": ["singleton:INTEGER", "schema_version:INTEGER", "schema_sha256:TEXT", "bundled_sha256:TEXT"], "primary_key": ["singleton"], "order_by": ["singleton"]}, "calendar_generation_candidate": {"columns": ["staging_sequence:INTEGER", "source_sha256:TEXT", "payload_json:BLOB", "admission:TEXT"], "primary_key": ["staging_sequence"], "order_by": ["staging_sequence"]}, "calendar_official_object": {"columns": ["body_sha256:TEXT", "body_bytes:BLOB"], "primary_key": ["body_sha256"], "order_by": ["body_sha256"]}, "calendar_maintenance_attempt": {"columns": ["target_year:INTEGER", "slot_date:TEXT", "source_sha256:TEXT", "outcome:TEXT"], "primary_key": ["target_year", "slot_date"], "order_by": ["target_year", "slot_date"]}, "calendar_generation_promotion": {"columns": ["sequence:INTEGER", "generation_sha256:TEXT", "source_sha256:TEXT", "promoted_at:TEXT"], "primary_key": ["sequence"], "order_by": ["sequence"]}, "calendar_generation_head": {"columns": ["singleton:INTEGER", "sequence:INTEGER", "generation_sha256:TEXT"], "primary_key": ["singleton"], "order_by": ["singleton"]}}, "catalog_digest_source": "existing-r2f4-calendar-generation-ddl"},
    "universe": {"role": "universe", "user_version": 1, "allowed_tables": ["universe_meta", "universe_source_state", "universe_contract", "universe_member", "universe_semantic_mapping", "universe_instrument_evidence", "universe_required_symbol_snapshot", "universe_publication_context", "contract_evidence", "contract_required_snapshot", "universe_attempt", "universe_attempt_result", "universe_head"], "system_tables": ["sqlite_sequence"], "tables": {"universe_meta": {"columns": ["meta_key:TEXT", "meta_value:TEXT"], "primary_key": ["meta_key"], "order_by": ["meta_key"]}, "universe_source_state": {"columns": ["source_state_id:TEXT", "source_state_sha256:TEXT", "trade_date:TEXT"], "primary_key": ["source_state_id"], "order_by": ["source_state_id"]}, "universe_contract": {"columns": ["contract_id:TEXT", "sequence:INTEGER", "trade_date:TEXT", "contract_sha256:TEXT"], "primary_key": ["contract_id"], "order_by": ["sequence", "contract_id"]}, "universe_member": {"columns": ["contract_id:TEXT", "symbol:TEXT", "security_id:TEXT", "member_sha256:TEXT"], "primary_key": ["contract_id", "symbol"], "order_by": ["contract_id", "symbol"]}, "universe_semantic_mapping": {"columns": ["mapping_id:TEXT", "mapping_version:TEXT", "mapping_sha256:TEXT"], "primary_key": ["mapping_id"], "order_by": ["mapping_id"]}, "universe_instrument_evidence": {"columns": ["evidence_id:TEXT", "evidence_sha256:TEXT", "symbol:TEXT", "mapping_id:TEXT"], "primary_key": ["evidence_id"], "order_by": ["evidence_id"]}, "universe_required_symbol_snapshot": {"columns": ["snapshot_id:TEXT", "snapshot_sha256:TEXT", "symbol_count:INTEGER"], "primary_key": ["snapshot_id"], "order_by": ["snapshot_id"]}, "universe_publication_context": {"columns": ["context_id:TEXT", "run_id:TEXT", "trade_date:TEXT", "context_sha256:TEXT"], "primary_key": ["context_id"], "order_by": ["trade_date", "context_id"]}, "contract_evidence": {"columns": ["contract_id:TEXT", "evidence_id:TEXT", "evidence_role:TEXT"], "primary_key": ["contract_id", "evidence_id", "evidence_role"], "order_by": ["contract_id", "evidence_id", "evidence_role"]}, "contract_required_snapshot": {"columns": ["contract_id:TEXT", "snapshot_id:TEXT", "snapshot_sha256:TEXT"], "primary_key": ["contract_id"], "order_by": ["contract_id"]}, "universe_attempt": {"columns": ["attempt_id:TEXT", "dedup_key:TEXT", "trade_date:TEXT", "planned_sha256:TEXT"], "primary_key": ["attempt_id"], "order_by": ["trade_date", "attempt_id"]}, "universe_attempt_result": {"columns": ["attempt_id:TEXT", "terminal_status:TEXT", "result_sha256:TEXT"], "primary_key": ["attempt_id"], "order_by": ["attempt_id"]}, "universe_head": {"columns": ["singleton_id:INTEGER", "sequence:INTEGER", "contract_id:TEXT", "head_sha256:TEXT"], "primary_key": ["singleton_id"], "order_by": ["singleton_id"]}}, "catalog_digest_source": "existing-r2f4-universe-ddl"}
  },
  "digest_contracts": [
    {"field": "ImmutableObservationEnvelopeV1.payload_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ImmutableObservationEnvelopeV1", "included_field_paths": ["payload"], "excluded_fields": ["payload_sha256", "envelope_sha256"], "ordering": "sorted object keys; source arrays retain declared order", "null_encoding": "JSON null; absent fields forbidden", "domain_separation_prefix": "r2f5/envelope-payload-v1\\0"},
    {"field": "ImmutableObservationEnvelopeV1.envelope_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ImmutableObservationEnvelopeV1", "included_field_paths": ["artifact_id", "artifact_ref", "schema_version", "creator_kind", "creator_version", "created_at", "canonicalization_version", "payload_sha256"], "excluded_fields": ["payload", "payload_sha256", "envelope_sha256"], "ordering": "sorted object keys", "null_encoding": "JSON null; absent fields forbidden", "domain_separation_prefix": "r2f5/envelope-v1\\0"},
    {"field": "PreCaptureFailurePayloadV1.semantic_report_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "PreCaptureFailurePayloadV1", "included_field_paths": ["schema_version", "reason_code", "requested_start", "requested_end", "as_of_utc", "descriptor_states"], "excluded_fields": ["semantic_report_sha256"], "ordering": "descriptor_states source order; object keys sorted", "null_encoding": "JSON null; absent fields forbidden", "domain_separation_prefix": "r2f5/pre-capture-v1\\0"},
    {"field": "CalendarRawFacts.raw_facts_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "CalendarRawFacts", "included_field_paths": ["source_sequence", "generation", "confirmed", "unknown_state", "conflict_state"], "excluded_fields": ["raw_facts_sha256"], "ordering": "source_sequence raw source order; object keys sorted", "null_encoding": "JSON null; absent fields forbidden", "domain_separation_prefix": "r2f5/calendar-raw-v1\\0"},
    {"field": "ReadBoundaryRawFacts.probe_schema_digest", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReadBoundaryRawFacts", "included_field_paths": ["requested_as_of", "max_visible_session", "future_rows_seen", "future_rows_count", "query_count", "write_count"], "excluded_fields": ["probe_schema_digest"], "ordering": "object keys sorted", "null_encoding": "null is JSON null for max_visible_session", "domain_separation_prefix": "r2f5/read-boundary-v1\\0"},
    {"field": "SessionEvidenceBinding.evidence_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "EvidenceObject", "included_field_paths": ["evidence_id", "immutable_evidence_bytes"], "excluded_fields": ["evidence_sha256", "envelope_sha256"], "ordering": "immutable bytes; no filesystem order", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/evidence-object-v1\\0"},
    {"field": "SessionEvidenceBinding.candidate_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "CandidateObject", "included_field_paths": ["candidate_id", "immutable_candidate_bytes"], "excluded_fields": ["candidate_sha256", "envelope_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/candidate-object-v1\\0"},
    {"field": "SessionEvidenceBinding.gate_report_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "GateReport", "included_field_paths": ["gate_report_id", "immutable_gate_report_bytes"], "excluded_fields": ["gate_report_sha256", "envelope_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/gate-report-v1\\0"},
    {"field": "SessionEvidenceBinding.manifest_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "Manifest", "included_field_paths": ["manifest_id", "immutable_manifest_bytes"], "excluded_fields": ["manifest_sha256", "envelope_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/manifest-v1\\0"},
    {"field": "SessionEvidenceBinding.object_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ObjectEvidence", "included_field_paths": ["object_id", "immutable_object_bytes"], "excluded_fields": ["object_sha256", "envelope_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/object-v1\\0"},
    {"field": "SessionEvidenceBinding.selection_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SessionSelection", "included_field_paths": ["selection_id", "immutable_selection_bytes"], "excluded_fields": ["selection_sha256", "envelope_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/selection-v1\\0"},
    {"field": "SessionEvidenceBinding.binding_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SessionEvidenceBinding", "included_field_paths": ["evidence_id", "evidence_sha256", "candidate_id", "candidate_sha256", "gate_report_id", "gate_report_sha256", "manifest_id", "manifest_sha256", "object_id", "object_sha256", "selection_id", "selection_sha256"], "excluded_fields": ["binding_sha256"], "ordering": "fixed field path order above", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/lineage-binding-v1\\0"},
    {"field": "PointerReconciliation.pointer_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "PointerRecord", "included_field_paths": ["pointer_id", "immutable_pointer_bytes"], "excluded_fields": ["pointer_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/pointer-v1\\0"},
    {"field": "PointerReconciliation.manifest_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "Manifest", "included_field_paths": ["manifest_id", "immutable_manifest_bytes"], "excluded_fields": ["manifest_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/pointer-manifest-v1\\0"},
    {"field": "PointerReconciliation.object_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ObjectEvidence", "included_field_paths": ["object_id", "immutable_object_bytes"], "excluded_fields": ["object_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/pointer-object-v1\\0"},
    {"field": "PointerReconciliation.descriptor_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "InputDescriptor", "included_field_paths": ["descriptor_id", "descriptor_metadata"], "excluded_fields": ["descriptor_sha256"], "ordering": "object keys sorted", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/descriptor-v1\\0"},
    {"field": "ReplicationObservation.source_commit_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReplicationRecord", "included_field_paths": ["source_commit_id", "source_commit_bytes"], "excluded_fields": ["source_commit_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/source-commit-v1\\0"},
    {"field": "ReplicationObservation.destination_record_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReplicationRecord", "included_field_paths": ["destination_record_id", "destination_record_bytes"], "excluded_fields": ["destination_record_sha256"], "ordering": "immutable bytes", "null_encoding": "null only when destination_generation is null", "domain_separation_prefix": "r2f5/destination-record-v1\\0"},
    {"field": "ReplicationObservation.destination_head_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "DestinationHead", "included_field_paths": ["destination_generation", "destination_head_bytes"], "excluded_fields": ["destination_head_sha256"], "ordering": "immutable bytes", "null_encoding": "null only for LOCAL_CHAIN_ONLY", "domain_separation_prefix": "r2f5/destination-head-v1\\0"},
    {"field": "ReplicationObservation.observation_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReplicationObservation", "included_field_paths": ["immutable", "state", "checkpoint_id", "source_commit_sha256", "intent_id", "enqueue_state", "reason_code", "observed_at", "lag_seconds", "trust_scope", "destination_generation", "destination_record_sha256", "destination_head_sha256"], "excluded_fields": ["observation_sha256"], "ordering": "object keys sorted", "null_encoding": "null literal for optional IDs/hashes", "domain_separation_prefix": "r2f5/replication-observation-v1\\0"},
    {"field": "RecoveryObservation.duplicate_proof_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "RecoveryProof", "included_field_paths": ["event_id", "attempt_id", "before_generation", "after_generation", "queue_identity", "restart_boundary", "exactly_once_publication_id", "after_manifest_sha256", "after_pointer_sha256", "after_selection_sha256", "publication_count"], "excluded_fields": ["duplicate_proof_sha256", "raw_bytes"], "ordering": "fixed field path order above", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/recovery-v1\\0"},
    {"field": "RecoveryObservation.observation_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "RecoveryObservation", "included_field_paths": ["immutable", "event_id", "attempt_id", "before_generation", "after_generation", "queue_identity", "restart_boundary", "exactly_once_publication_id", "after_manifest_sha256", "after_pointer_sha256", "after_selection_sha256", "publication_count", "duplicate_proof_sha256", "observed_at"], "excluded_fields": ["observation_sha256"], "ordering": "object keys sorted", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/recovery-observation-v1\\0"},
    {"field": "ErrorHandlingObservation.observation_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ErrorHandlingObservation", "included_field_paths": ["immutable", "events[].event_id", "events[].forced_error_class", "events[].sanitized_reason", "events[].normalized_result", "events[].attempt_id", "events[].expected_class", "events[].observed_class", "events[].evidence_sha256", "events[].observed_at"], "excluded_fields": ["observation_sha256"], "ordering": "six events fixed timeout/auth/rate/schema/coverage/storage order", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/error-handling-v1\\0"},
    {"field": "LocalNasIsolationObservation.observation_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "LocalNasIsolationObservation", "included_field_paths": ["immutable", "event_id", "local_publication_ready", "local_publication_id", "local_pointer_sha256", "outage_start", "outage_end", "backlog_before_ids", "backlog_after_ids", "backlog_before_count", "backlog_after_count", "lag_seconds", "lag_threshold_seconds", "retryable", "retry_state", "retry_transition", "nas_failure_did_not_block_local", "attempt_id", "observed_at"], "excluded_fields": ["observation_sha256"], "ordering": "backlog IDs source order; object keys sorted", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/local-nas-v1\\0"},
    {"field": "SessionObservation.frozen_versions_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "FrozenReliabilityVersions", "included_field_paths": ["git_commit", "installed_release", "installed_release_sha256", "dataset_generation", "canonical_schema", "evidence_schema", "primary_provider_id", "secondary_provider_id", "qualification_window_id", "qualification_proof_status", "adapter_hash", "endpoint_contract_hash", "source_schema_hash", "normalizer_hash", "reconciliation_policy_version", "selection_policy_version", "config_digest", "auto_failover_enabled", "failover_kill_switch", "provider_priority", "continuity_start_date", "repair_policy_version", "calendar_generation", "calendar_sha256", "universe_generation", "universe_sha256", "replication_policy_version", "replication_evidence_version", "replication_trust_scope", "destination_generation", "destination_head_sha256", "remote_proof_artifact_ref", "restore_policy_version", "restore_evidence_version"], "excluded_fields": ["frozen_versions_sha256", "semantic_report_sha256"], "ordering": "sorted object keys; provider_priority declared order", "null_encoding": "typed absence JSON null", "domain_separation_prefix": "r2f5/frozen-vector-v1\\0"},
    {"field": "SessionObservation.schema_policy_digest", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SessionObservation", "included_field_paths": ["schema_policy_versions"], "excluded_fields": ["schema_policy_digest"], "ordering": "schema_policy_versions declared order; object keys sorted", "null_encoding": "absence forbidden for ready", "domain_separation_prefix": "r2f5/schema-policy-v1\\0"},
    {"field": "SessionObservation.observation_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SessionObservation", "included_field_paths": ["session", "ordinal", "frozen_versions_sha256", "same_evening_published_at", "next_morning_published_at", "required_count", "loaded_count", "suspension_count", "not_listed_count", "delisted_count", "unknown_count", "canonical_provider_ids", "evidence", "pointer_reconciliation", "replication_observation", "calendar_raw_facts", "read_boundary_raw_facts", "schema_policy_versions", "schema_policy_digest", "cutoff_results", "coverage", "canonical_integrity", "source_purity", "provenance", "calendar", "universe", "replication", "read_boundary"], "excluded_fields": ["observation_sha256", "diagnostic_envelope", "semantic_report_sha256"], "ordering": "object keys sorted; arrays retain declared source/order", "null_encoding": "JSON null for declared optional timestamps only", "domain_separation_prefix": "r2f5/session-observation-v1\\0"},
    {"field": "WholeSessionFailoverDrill.selection_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SessionSelection", "included_field_paths": ["selection_id", "immutable_selection_bytes"], "excluded_fields": ["selection_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/failover-selection-v1\\0"},
    {"field": "WholeSessionFailoverDrill.manifest_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "Manifest", "included_field_paths": ["manifest_id", "immutable_manifest_bytes"], "excluded_fields": ["manifest_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/failover-manifest-v1\\0"},
    {"field": "WholeSessionFailoverDrill.pointer_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "PointerRecord", "included_field_paths": ["pointer_id", "immutable_pointer_bytes"], "excluded_fields": ["pointer_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/failover-pointer-v1\\0"},
    {"field": "WholeSessionFailoverDrill.readback_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "FailoverReadback", "included_field_paths": ["readback_id", "immutable_readback_bytes"], "excluded_fields": ["readback_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/failover-readback-v1\\0"},
    {"field": "ReplaySampleEvidence.sample_object_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReplaySample", "included_field_paths": ["sample_object_id", "immutable_sample_bytes"], "excluded_fields": ["sample_object_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/replay-sample-v1\\0"},
    {"field": "ReplaySampleEvidence.candidate_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "CandidateObject", "included_field_paths": ["candidate_id", "immutable_candidate_bytes"], "excluded_fields": ["candidate_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/replay-candidate-v1\\0"},
    {"field": "RestoreDrillEvidence.sentinel_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "RestoreSentinel", "included_field_paths": ["sentinel_id", "immutable_sentinel_bytes"], "excluded_fields": ["sentinel_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/restore-sentinel-v1\\0"},
    {"field": "RestoreDrillEvidence.destination_head_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "DestinationHead", "included_field_paths": ["destination_generation", "destination_head_bytes"], "excluded_fields": ["destination_head_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/restore-head-v1\\0"},
    {"field": "RestoreDrillEvidence.record_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "RestoreRecord", "included_field_paths": ["record_id", "immutable_record_bytes"], "excluded_fields": ["record_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/restore-record-v1\\0"},
    {"field": "RestoreDrillEvidence.manifest_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "Manifest", "included_field_paths": ["manifest_id", "immutable_manifest_bytes"], "excluded_fields": ["manifest_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/restore-manifest-v1\\0"},
    {"field": "RestoreDrillEvidence.restore_report_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "RestoreReport", "included_field_paths": ["restore_report_id", "immutable_restore_report_bytes"], "excluded_fields": ["restore_report_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/restore-report-v1\\0"},
    {"field": "RestoreDrillEvidence.api_readback_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "RestoreApiReadback", "included_field_paths": ["readback_id", "immutable_readback_bytes"], "excluded_fields": ["api_readback_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/restore-readback-v1\\0"},
    {"field": "SnapshotFingerprint.sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "FingerprintSubject", "included_field_paths": ["descriptor_role", "descriptor_id", "descriptor_state", "device", "inode", "size_bytes", "mtime_ns", "ctime_ns", "fingerprint_kind", "hash_scope", "captured_content_bytes"], "excluded_fields": ["sha256"], "ordering": "object keys sorted; path entries tree order", "null_encoding": "typed null for absent descriptor metadata", "domain_separation_prefix": "r2f5/fingerprint-v1\\0"},
    {"field": "FrozenReliabilityVersions.config_digest", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "AcceptanceConfig", "included_field_paths": ["dataset_root_descriptor", "evidence_root_descriptor", "control_store_descriptor", "clock_policy", "cutoff_policy", "limits", "replay_policy", "replication_policy", "restore_policy", "redaction_policy"], "excluded_fields": ["config_digest"], "ordering": "sorted object keys", "null_encoding": "absence forbidden for ready", "domain_separation_prefix": "r2f5/config-v1\\0"},
    {"field": "RecoveryObservation.after_manifest_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "Manifest", "included_field_paths": ["manifest_id", "immutable_manifest_bytes"], "excluded_fields": ["after_manifest_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/recovery-manifest-v1\\0"},
    {"field": "RecoveryObservation.after_pointer_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "PointerRecord", "included_field_paths": ["pointer_id", "immutable_pointer_bytes"], "excluded_fields": ["after_pointer_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/recovery-pointer-v1\\0"},
    {"field": "RecoveryObservation.after_selection_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SessionSelection", "included_field_paths": ["selection_id", "immutable_selection_bytes"], "excluded_fields": ["after_selection_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/recovery-selection-v1\\0"},
    {"field": "ErrorHandlingObservation.events.evidence_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ForcedErrorEvidence", "included_field_paths": ["event_id", "forced_error_class", "immutable_evidence_bytes"], "excluded_fields": ["evidence_sha256"], "ordering": "six fixed class order", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/forced-error-evidence-v1\\0"},
    {"field": "LocalNasIsolationObservation.local_pointer_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "LocalPublicationPointer", "included_field_paths": ["local_publication_id", "immutable_pointer_bytes"], "excluded_fields": ["local_pointer_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/local-pointer-v1\\0"},
    {"field": "CompletedReplicationRestoreSnapshotV1.replication_observation_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReplicationObservation", "included_field_paths": ["immutable", "state", "checkpoint_id", "source_commit_sha256", "destination_record_sha256", "destination_head_sha256", "observed_at"], "excluded_fields": ["replication_observation_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/completed-replication-v1\\0"},
    {"field": "CompletedReplicationRestoreSnapshotV1.restore_report_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "RestoreReport", "included_field_paths": ["restore_report_id", "immutable_restore_report_bytes"], "excluded_fields": ["restore_report_sha256"], "ordering": "immutable bytes", "null_encoding": "null only when no restore record", "domain_separation_prefix": "r2f5/completed-restore-v1\\0"},
    {"field": "CompletedReplicationRestoreSnapshotV1.destination_record_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReplicationRecord", "included_field_paths": ["destination_generation", "immutable_destination_record_bytes"], "excluded_fields": ["destination_record_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/completed-destination-record-v1\\0"},
    {"field": "CompletedReplicationRestoreSnapshotV1.destination_head_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "DestinationHead", "included_field_paths": ["destination_generation", "immutable_destination_head_bytes"], "excluded_fields": ["destination_head_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/completed-destination-head-v1\\0"},
    {"field": "ReadonlyEvidenceDescriptor.object_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReadonlyEvidenceObject", "included_field_paths": ["descriptor_id", "immutable_object_bytes"], "excluded_fields": ["object_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/readonly-object-v1\\0"},
    {"field": "ReadonlyEvidenceDescriptor.descriptor_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "ReadonlyEvidenceDescriptor", "included_field_paths": ["descriptor_id", "source_generation", "completed", "immutable"], "excluded_fields": ["descriptor_sha256"], "ordering": "object keys sorted", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/readonly-descriptor-v1\\0"},
    {"field": "SnapshotIdentity.input_fingerprint_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SnapshotIdentity", "included_field_paths": ["input_fingerprints"], "excluded_fields": ["input_fingerprint_sha256", "snapshot_sha256", "semantic_report_sha256"], "ordering": "descriptor_role then descriptor_id byte order", "null_encoding": "typed absence descriptor object, never omitted", "domain_separation_prefix": "r2f5/input-fingerprints-v1\\0"},
    {"field": "SnapshotIdentity.frozen_version_vector_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SnapshotIdentity", "included_field_paths": ["frozen_versions"], "excluded_fields": ["frozen_version_vector_sha256", "semantic_report_sha256"], "ordering": "sorted object keys; provider_priority declared order", "null_encoding": "typed JSON null for unavailable proof", "domain_separation_prefix": "r2f5/frozen-version-vector-v1\\0"},
    {"field": "SnapshotIdentity.snapshot_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "SnapshotIdentity", "included_field_paths": ["requested_start", "requested_end", "as_of_utc", "as_of_timezone", "input_fingerprints", "frozen_versions", "input_fingerprint_sha256", "frozen_version_vector_sha256"], "excluded_fields": ["snapshot_sha256", "semantic_report_sha256", "diagnostic_envelope"], "ordering": "object keys sorted; input descriptors role/id order", "null_encoding": "typed absence descriptor retained", "domain_separation_prefix": "r2f5/snapshot-identity-v1\\0"},
    {"field": "R2FAcceptanceReport.semantic_report_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "R2FAcceptanceReport", "included_field_paths": ["status", "window_start", "window_end", "selected_sessions", "frozen_versions", "continuity", "next_morning_availability", "same_evening_availability", "coverage", "canonical_integrity", "source_purity", "recovery", "failover", "provenance", "replay", "adjustment", "calendar", "universe", "error_handling", "local_nas_isolation", "replication", "restore", "read_boundary", "quality_issues", "snapshot_identity", "session_observations", "observation_refs", "window_evidence_bundle", "window_evidence_refs", "pre_capture_failure", "provider_requests", "writes", "restore_started", "production_window_started"], "excluded_fields": ["semantic_report_sha256", "pre_capture_failure.semantic_report_sha256", "diagnostic_envelope", "elapsed_ms", "read_operations", "replay_sample_count"], "ordering": "object keys sorted; sessions ordinal order; quality issues stable order", "null_encoding": "JSON null for declared unavailable fields", "domain_separation_prefix": "r2f5/semantic-report-v1\\0"},
    {"field": "FrozenReliabilityVersions.installed_release_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "InstalledRelease", "included_field_paths": ["release_identity", "immutable_release_bytes"], "excluded_fields": ["installed_release_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden for ready", "domain_separation_prefix": "r2f5/installed-release-v1\\0"},
    {"field": "FrozenReliabilityVersions.calendar_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "CalendarGeneration", "included_field_paths": ["calendar_generation", "immutable_calendar_bytes"], "excluded_fields": ["calendar_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden for ready", "domain_separation_prefix": "r2f5/calendar-generation-v1\\0"},
    {"field": "FrozenReliabilityVersions.universe_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "UniverseGeneration", "included_field_paths": ["universe_generation", "immutable_universe_bytes"], "excluded_fields": ["universe_sha256"], "ordering": "immutable bytes", "null_encoding": "absent forbidden for ready", "domain_separation_prefix": "r2f5/universe-generation-v1\\0"},
    {"field": "FrozenReliabilityVersions.destination_head_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "DestinationHead", "included_field_paths": ["destination_generation", "destination_head_bytes"], "excluded_fields": ["destination_head_sha256"], "ordering": "immutable bytes", "null_encoding": "typed null only for LOCAL_CHAIN_ONLY", "domain_separation_prefix": "r2f5/frozen-destination-head-v1\\0"},
    {"field": "OfflineReplayContext.implementation_sha256", "canonicalization_version": "project-canonical-json-v1", "root_object_type": "OfflineImplementation", "included_field_paths": ["adapter_id", "adapter_version", "normalizer_id", "normalizer_version", "immutable_implementation_bytes"], "excluded_fields": ["implementation_sha256"], "ordering": "fixed field path order above", "null_encoding": "absent forbidden", "domain_separation_prefix": "r2f5/offline-implementation-v1\\0"}
  ]
}
```

`R2F5_X7_CONTRACTS_JSON` canonical block digest (sorted-key compact UTF-8 JSON, SHA-256,
excluding Markdown fences) is `7861268d852ba79de023da0fbd3c30da0bb6fc1e8048653a6dc6b5f34e38035c`.

The validator parses this block and cross-checks its roadmap tuple, metric set, reason partitions,
status matrix, reducer field references, envelope fields, creator allowlists, date/time formats,
metric value kinds, limits and cardinalities against the TypeScript contracts, implementation plan
and matrix. Every digest-typed model field MUST occur exactly once in `digest_contracts`; its own
field MUST be excluded from its included paths. This is structural drift detection only; semantic
hash proof and human review of the reducers remain mandatory. `model_schema_ast` is the closed
object/tuple schema, and `digest_fields_by_model` is its explicit digest-field index; both are
machine-readable and are cross-checked against all 61 digest contracts.

## Static compatibility inventory

The implementation MUST reuse and read, without widening authority, the existing strict
`EvidenceReader` bytes/manifest primitives only (never its provider-capable default replay path),
the injected offline replay resolver, `CandidateStore`/`SessionSelection`, `UniverseSidecarStore` and
`build_universe_status`, promoted calendar generation reader/runtime status,
`ReplicationStatusService`, completed `RestoreService` drill records, and the existing dataset
manifest/pointer readers. It MUST preserve `read()` compatibility and MUST NOT add a provider,
LaunchAgent, user-store read, canonical writer, NAS transport or new persistence schema.

### Deterministic input tree fingerprint

The implementation MUST open each directory root once with `open(root, O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`
for this deterministic tree hash
and retain that descriptor as the root identity; it MUST enumerate only through descriptor-relative
operations. It MUST `fstat` the root before and after enumeration. Every relative path MUST be
POSIX-normalized (Unicode NFC, `/` separators, no empty, `.` or `..` components), encoded as UTF-8,
and sorted by UTF-8 byte order. Normalization collisions, path escape, symlink, FIFO, socket, device,
or other special entries are unavailable. A regular file with `st_nlink > 1` is hardlink ambiguity
and is unavailable.

The tree digest preimage MUST be a domain-separated prefix `r2f5/tree-v1\0` followed by a sequence
of length-prefixed canonical records. Each record contains, in this order, the UTF-8 relative-path
length and bytes, entry type, `st_dev`, `st_ino`, mode, size, `mtime_ns`, and a regular-file content
streaming SHA-256 (or an explicit typed absence marker for directories). Intermediate components
MUST be opened one at a time with `openat(parent_fd, component, O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)`
and fstat'ed; the leaf MUST use its parent fd and basename with `openat(parent_fd, basename,
O_NOFOLLOW|O_CLOEXEC)`. `openat(root_fd, relative_with_slash)` is forbidden. The reader MUST
`fstat` before reading, stream
the complete content, `fstat` after reading, and return `SNAPSHOT_CHANGED` if device/inode, size or
mtime changes. Directory identity is checked before and after the entire walk. `mtime_ns` is included
to detect metadata replacement/rollback even when content is unchanged; this detects concurrent
replacement but does not claim protection from a malicious filesystem root. Ancestor descriptor
identities are verification-only probes and are not in semantic tree records; root/entry identity,
path and mtime are in those records. Python/macOS MUST use `dir_fd`/`openat` APIs, not Linux-only
`openat2`.

The maximum is 100,000 entries and 512 MiB total regular-file bytes per input; exceeding either
bound returns `INPUT_LIMIT_EXCEEDED`/`unavailable`. Before/after tree digests MUST match. The
10-second/100,000-row fixture benchmark includes this complete tree hash; production inputs over
the limits cannot be ready. SQLite MUST NOT hash full-file bytes directly; its logical snapshot
algorithm below owns the database, WAL and SHM state.

### Unique SQLite logical snapshot fingerprint

SQLite has one algorithm, not a choice of file or page hashing. The evaluator MUST open an existing
database with a read-only URI `mode=ro&immutable=false`, set `PRAGMA query_only=ON`, issue `BEGIN`
and retain that transaction until capture ends. It MUST read `sqlite_schema`, `PRAGMA page_count`,
`PRAGMA user_version`, and every catalog-listed table using its fixed deterministic `ORDER BY`
clause and
typed length-prefixed encoding (including explicit null/type markers). The logical digest preimage
is `r2f5/sqlite-logical-v1\0` plus schema, page-count, user-version and ordered row records. It
MUST NOT hash the live database, `-wal` or `-shm` bytes directly. A busy/locked database is
`unavailable`; a missing database is not opened or created. The transaction MUST remain open through
the captured snapshot, then roll back/close without writes or journal creation.
The Python implementation MUST use `sqlite3.connect(uri, uri=True)` with those URI flags and MUST
not call `backup`, `VACUUM`, migration, initialization or any writer API; `query_only=ON` plus the
read transaction is the zero-write guarantee.

The evaluator MUST record descriptor identity before and after the transaction and reject changes
to device/inode/size/mtime or logical version fields as `SNAPSHOT_CHANGED`. WAL changes are allowed
while this consistent read transaction remains stable; `-wal`/`-shm` are not independently enumerated
or hashed. If a directory tree also contains the database, the tree walker MUST exclude that database
and its `-wal`/`-shm` siblings by descriptor-bound database ownership; the SQLite logical fingerprint
owns them, preventing double hashing or conflicting digests. The actual `sqlite_master` name/type
set MUST equal the catalog's `allowed_tables` plus its `system_tables`; every catalog table MUST
match its declared columns/types, primary-key tuple, order tuple and `user_version`. Missing,
extra, type, key, order or version drift is `unavailable`. The SQLite limit is 1,000,000 rows per
database and 512 MiB encoded logical bytes; exceeding either returns `INPUT_LIMIT_EXCEEDED`.

## R2-F5.0 metric contract and evidence sources

The following table is normative. Every row is a separate `MetricResult` in the report. The first
17 rows are the roadmap Section 10 table dimensions, parsed and compared verbatim by the validator;
`replication` is an independently testable child of `local_nas_isolation`, not a roadmap row. The
`session fields + reducer` column is the complete independent recomputation recipe from the 20
session observations and, where stated, the one window bundle. A missing policy value is not a pass.

The roadmap Section 10 source row `Local/NAS isolation` explicitly mandates observable replication
lag. This design preserves that source row and defines `replication` as a child metric. The
validator compares a fixed expected tuple to the actual roadmap table and never derives a new
roadmap dimension from prose.

| MetricResult field | Exact target/threshold | Session fields + reducer / window evidence | Failure reason | Acceptance ref | Planned anchor |
| --- | --- | --- | --- | --- | --- |
| `continuity` | zero missing canonical dates in 20 | `session`; count missing after sorted unique validation | `CONTINUITY_FAILED` | AC-15 | `test_r2f5_slo_continuity` |
| `next_morning_availability` | 20/20 by 08:00 Shanghai next civil day | `next_morning_published_at`; count <= cutoff | `AVAILABILITY_CUTOFF_FAILED` | AC-15 | `test_r2f5_slo_next_morning_availability` |
| `same_evening_availability` | >=18/20 by 21:15 Shanghai session day | `same_evening_published_at`; count <= cutoff | `AVAILABILITY_CUTOFF_FAILED` | AC-15 | `test_r2f5_slo_same_evening_availability` |
| `coverage` | 100% legal universe every session | `required_count`, `loaded_count`, status; all loaded/required=1 | `COVERAGE_FAILED` | AC-15 | `test_r2f5_slo_coverage` |
| `canonical_integrity` | pointer, manifest, object hash and row/date identity reconcile each session | `pointer_reconciliation`; all match=true | `CANONICAL_INTEGRITY_FAILED` | AC-15 | `test_r2f5_slo_canonical_integrity` |
| `source_purity` | zero mixed-provider canonical partitions | `canonical_provider_ids`; max cardinality=1 | `SOURCE_PURITY_FAILED` | AC-15 | `test_r2f5_slo_source_purity` |
| `recovery` | queued across restart and publishes exactly once | `window.recovery_observation`; one publication_count=1 | `RECOVERY_FAILED` | AC-15 | `test_r2f5_slo_recovery` |
| `failover` | qualified secondary whole-session publication, zero mixed rows | `window.failover_observation`; immutable drill pass | `FAILOVER_UNAVAILABLE` | AC-15 | `test_r2f5_slo_failover` |
| `provenance` | every session traces raw/provider/adapter/schema/time/hash/gate/selection | `evidence`; all seven bindings verify | `LINEAGE_UNAVAILABLE` | AC-15 | `test_r2f5_slo_provenance` |
| `replay` | sampled raw evidence replays semantically identically offline | `window.replay_sample`; semantic_equal=true | `REPLAY_UNAVAILABLE` | AC-15 | `test_r2f5_slo_replay` |
| `adjustment` | declared cross-provider return tolerance passes | `window.adjustment_equivalence`; policy and equivalence pass | `ADJUSTMENT_UNAVAILABLE` | AC-15 | `test_r2f5_slo_adjustment` |
| `calendar` | warning/acquisition observable; unknown/conflict fails closed | session calendar statuses; all confirmed | `CALENDAR_UNAVAILABLE` | AC-15 | `test_r2f5_slo_calendar` |
| `universe` | counts reconcile; unknown zero | session required/loaded/suspension/not-listed/delisted/unknown; all reconcile | `UNIVERSE_COUNT_MISMATCH` | AC-15 | `test_r2f5_slo_universe` |
| `error_handling` | six forced classes map to sanitized categories | `window.error_handling_observation.events`; exact six classes once | `ERROR_HANDLING_FAILED` | AC-15 | `test_r2f5_slo_error_handling` |
| `local_nas_isolation` | NAS outage leaves local ready and retryable backlog | window envelope payload; reducer requires local-ready, outage publication, nonnegative lag/threshold, retryable=true, exact backlog IDs/counts and queued-to-retrying transition | `LOCAL_NAS_ISOLATION_FAILED` | AC-15 | `test_r2f5_slo_local_nas_isolation` |
| `replication` *(child of local/NAS)* | frozen lag policy and remote proof pass | session `replication_observation` + completed record; reducer max nonnegative lag <= frozen threshold and remote proof | `REPLICATION_LAG` | AC-15 | `test_r2f5_slo_replication` |
| `restore` | verified generation restore/readback satisfies frozen policy | `window.restore_observation`; one completed drill | `RESTORE_UNAVAILABLE` | AC-15 | `test_r2f5_slo_restore` |
| `read_boundary` | GET calls make no filesystem mutation | session read-boundary results; all pass | `READ_BOUNDARY_FAILED` | AC-15 | `test_r2f5_slo_read_boundary` |

`adjustment`, `replication` and `restore` have no guessed numeric tolerance in this slice. The
reader MUST require a reviewed R2-F4 policy/evidence record carrying the exact value and version.
R2-F4.3's current `LOCAL_CHAIN_ONLY` evidence is valid offline-chain evidence but cannot pass the
remote/NAS acceptance dimension or establish Task 20 production readiness.

## Secondary admission and failover evidence contract

R2-F5.0 MUST add no `ProviderId`, canonical bar field, candidate schema or selection enum. It may
define read-only domain models over retained records:

```typescript
interface SecondaryQualificationProjection {
  provider_id: "tickflow" | "tushare";
  admission_state: "discovered" | "canary" | "shadow" | "qualified" | "quarantined";
  adapter_hash: string; endpoint_contract_hash: string; source_schema_hash: string;
  normalizer_hash: string; reconciliation_policy_hash: string;
  terms_evidence_hash: string | null; terms_review_id: string | null;
  window_id: string; window_start: string | null; window_end: string | null;
  consecutive_sessions: number; version_vector_sha256: string;
  calendar_generation: string; calendar_sha256: string;
  window_state: "observing" | "qualified" | "reset";
  last_session_report_id: string | null;
  qualification_evidence_sha256: string | null;
  qualification_candidate_sha256: string | null;
  terminal_attestation_id: string | null;
  qualification_proof_status: "available" | "unavailable";
}
```

Every field above is a direct projection of `provider_record` or `qualification_window` in the
existing shadow registry; no new admission/capability/qualification digest is accepted. The strict
reader MUST require immutable canonical bytes and exact descriptor binding. Existing
`read_status`/`read_window` cannot prove whole-session failover. A missing secondary or any
non-qualified state sets `qualification_proof_status=unavailable` and failover is unavailable.

If Task20 adds a writer-owned `WholeSessionFailoverDrill` envelope, its creator, schema version,
canonical JSON hash preimage, immutable object reference, selected provider/selection, pointer,
manifest, readback and mixed-row count MUST be documented by Task20. Task19 only reads that
artifact; it MUST NOT synthesize a digest from current rows. BaoStock-only, no qualified secondary,
and no whole-session forced-failover proof are expected negative evidence.

### Existing-field to artifact-source mapping

| Projection | Directly reusable R2-F4 fields/readers | Task20-only fields (not present today) | Task19 disposition when absent |
| --- | --- | --- | --- |
| `SecondaryQualificationProjection` | `provider_record.provider_id`, `admission_state`, adapter/endpoint/source-schema/normalizer/reconciliation hashes, terms evidence/review; `qualification_window.window_id`, dates, `consecutive_sessions`, version/calendar hashes, window state, last report, qualification evidence/candidate hashes, terminal attestation | none; a whole-session failover envelope may add its own artifact ref/hash, but not to this projection | `qualification_proof_status=unavailable` |
| replication completed record | `ReplicationObservation` source commit/checkpoint/intent/enqueue state/reason/effects/time; `ReplicationRecord` destination generation/record/head; `VerifiedDestinationCommitProof`; `VerifiedArchiveSnapshot` sentinel/head/record/manifest/checkpoint | remote verification attestation and remote trust scope beyond `LOCAL_CHAIN_ONLY` | replication unavailable/not ready for remote target |
| restore completed record | `RestoreReport` report/source generation/record/manifest/object/binding/checkpoint/counts/state/start-event hash/timestamps/effects; `RestoreAuditEvent`/terminal event actual fields | immutable drill envelope for schema, row-count and representative API readback, plus its creator/version/hash preimage | restore unavailable |

Task20-owned artifact fields are a schema boundary, not an assertion that Task20 exists or that its
soak has started. Task19 MUST accept only an immutable descriptor-native record whose artifact hash
is verified against the declared canonical preimage and creator/version; it MUST NOT hash current
status rows to manufacture a historical proof.

## Replication and restore completed-record snapshot contract

The acceptance reader consumes a `CompletedReplicationRestoreSnapshotV1` assembled from existing
read-only replication readers and `VerifiedArchiveSnapshot` (`sentinel`, `sentinel_bytes`, `head`,
`record`, `manifest_bytes`, `checkpoint`). It may also read actual `RestoreReport`,
`RestoreAuditEvent` and terminal-event fields: report ID/hash, source generation/record/checkpoint,
manifest/object/binding identities, object/row/byte counts, verification state, start-event hash,
timestamps and effects. It MUST NOT claim an unsupported remote-verification hash field because no
such F4 field exists. Current `LOCAL_CHAIN_ONLY` is offline-chain evidence only.

Remote verification, restore schema/row-count/API readback and numeric lag/duration thresholds are
available only from explicit Task20 writer-owned immutable drill envelopes. Their schema version,
creator, canonical hash preimage and artifact hash MUST be supplied; Task19 only reads completed
records. Thresholds absent from frozen R2-F4 policy/evidence are unavailable, not guessed. The
reader MUST use no `create=True`, writer, reconcile, drain, mount or restore execution path; an
absent/locked/corrupt sidecar is unavailable, not an empty success.

## Offline replay contract

Replay takes `OfflineReplayContext { adapter_id, adapter_version, normalizer_id, normalizer_version,
implementation_sha256, network_allowed: false, provider_requests: 0 }`. The implementation may
invoke the frozen adapter/normalizer on immutable raw bytes for deterministic normalization, but
MUST reject unknown/mismatched identity before construction. It MUST never call login/query,
construct a default network-capable adapter, resolve credentials, open a provider socket or write
replayed output. Deterministic normalization is allowed; provider/network side effects are not.

## Snapshot identity, sequence and concurrency

`SnapshotIdentity` is computed from requested start/end, an `as_of` UTC instant, the literal
`Asia/Shanghai` cutoff contract, all input descriptor fingerprints (including typed absence for a
missing root/control DB), and the complete frozen version vector. Raw calendar observations retain
source order and duplicate entries; the reader checks strict order before deriving sorted unique
confirmed sessions. A duplicate/out-of-order raw sequence is `unavailable`, not a sortable warning.
Every report returns the identity digest, the 20 ordered observation refs and each observation
digest, plus one window-bundle ref when a bundle exists. `SessionEvidenceBinding.binding_sha256`
MUST hash the canonical JSON of its seven ID/hash pairs; `PointerReconciliation` MUST prove the
pointer, manifest and object hash equality; `SessionObservation.observation_sha256` MUST hash the
canonical JSON of the complete per-session observation, including all MetricResult fields and record
hashes. `observation_refs[i]` MUST equal the digest of the observation with `ordinal=i+1`, and both
collections MUST have cardinality 20 for a candidate window. A bundle is exactly one immutable
window object and its digest is bound in `window_evidence_refs`; window drills are not copied into
each session. Missing/corrupt/locked artifacts are typed unavailable. If any descriptor, file
bytes, database page, version, or bound clock changes before/during/after evaluation, the whole
report is invalidated; the reader does not retry into a mixed snapshot.

Each `SessionObservation.frozen_versions_sha256` MUST equal
`SnapshotIdentity.frozen_version_vector_sha256`, computed over the complete frozen vector (not a
subset). Calendar reducers consume `calendar_raw_facts` and read-boundary reducers consume
`read_boundary_raw_facts`; a reducer MUST NOT use an already aggregated `MetricResult` as its input.
The report is independently recomputable from raw session facts plus the one window bundle envelope.

The semantic report payload consists only of SnapshotIdentity, frozen versions, selected sessions,
per-session observations, the immutable window bundle/reference, metric results, quality issue codes
and mutation markers. It is encoded
as canonical UTF-8 JSON (sorted keys, compact separators, `ensure_ascii=false`, `allow_nan=false`)
with a domain-separated SHA-256. `elapsed_ms`, read counts and replay counts are a separate
diagnostic envelope and MUST NOT change the semantic payload bytes or digest.

## Production mutation gates

The following actions are explicitly forbidden in R2-F5.0 and require a later approved window:

| Action | R2-F5.0 | Required later authority |
| --- | --- | --- |
| Provider/network/credential access | forbidden | separate provider and terms approval |
| Canonical/manifest/pointer/control mutation | forbidden | reviewed implementation plus writer gate |
| LaunchAgent install/load/start/stop | forbidden | installed-release approval and readback |
| NAS mount/copy/outbox drain | forbidden | approved descriptor, lock and F4.3 operator window |
| Restore operation | forbidden | bounded temporary-root drill authorization |
| Task 20 official soak clock | not started | R2-F0..F4 GO, installed readback, qualified secondary and human approval |
| R2-A/R2-B re-entry | forbidden | independent R2-F GO and separate A/B acceptance |

## Out of Scope

- OS-1: OS package, app, LaunchAgent, credential, provider, network, NAS or production installation.
- OS-2: Task 20's elapsed 20-session production soak, dashboard, chaos execution and final R2-F record.
- OS-3: Task 21 Release 2 re-entry, R2-A real canary, R2-B review, fund-flow evidence or investment advice.
- OS-4: New provider adapters, provider qualification, automatic failover, canonical selection or
  symbol-level mixing.
- OS-5: Calendar acquisition/promotion, universe maintenance, repair queue execution, outbox draining,
  restore initiation or any writer-owned migration.
- OS-6: New canonical, evidence, candidate, selection, calendar, universe, replication or restore schema.
- OS-7: Deriving source purity, coverage or cash-flow labels from amount, volume, OHLCV or proxies.
- OS-8: A production SLA inferred from a tiny synthetic fixture; NFR-7 is only a bounded harness gate.
- OS-9: Treating `status=ready` as release approval or as proof that Task 20 occurred.

## Required review decision

This candidate is complete only when an independent SPEC review confirms every FR/NFR/AC/EC and
the crosswalk validator passes. Until a separate implementation, focused/full/static verification,
installed readback and human gate occur, the authoritative state remains:

`SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO`.
