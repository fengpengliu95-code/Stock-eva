# Stock EVA R2-F5.0 Read-only Acceptance Harness Design

**Author:** Codex R2-F delivery lead

**Date:** 2026-09-14 (Asia/Shanghai)

**Status:** SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO

**Base commit:** `5393f499dbc8b84398658816f7a555dd3e547d47` (clean worktree)

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
  fingerprint every input root and database by descriptor identity, size, mtime/ctime and SHA-256
  of bounded metadata/content; any change MUST invalidate the report and preserve the prior state.
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
- FR-19: The report MUST expose one `MetricResult` for each of the 17 roadmap mandatory dimensions:
  continuity, next-morning availability, same-evening availability, coverage, canonical integrity,
  source purity, recovery, failover, provenance, replay, adjustment, calendar, universe, error
  handling, local/NAS isolation, replication and restore. Each result MUST carry an observed value,
  target, status and reason; a missing dimension MUST make the report unavailable.
- FR-20: Secondary qualification/admission MUST be read through a strict immutable evidence reader
  requiring provider, adapter, terms, version vector, qualification window, capability and
  admission hashes. Missing or non-qualified secondary evidence MUST make failover `not_ready` or
  `unavailable`; source purity alone MUST never substitute for qualification.
- FR-21: Forced-failover acceptance MUST require a separate immutable whole-session drill record
  proving primary-unavailable, qualified-secondary admission, one provider for every row, zero
  mixed rows, selection/pointer/manifest agreement and readback. BaoStock-only or no drill proof
  MUST fail closed.
- FR-22: Replication/restore evidence MUST be read through strict completed-record snapshot readers
  that require `trust_scope`, destination generation, destination-head proof and remote verification.
  `LOCAL_CHAIN_ONLY` MUST never satisfy a remote/NAS or Task 20 acceptance target, and readers MUST
  NOT call `create=True`, writer, reconcile, drain, mount or restore paths.
- FR-23: Offline replay MAY run a frozen adapter/normalizer implementation on immutable bytes, but
  MUST inject an explicit offline-only adapter/normalizer identity and MUST reject unknown or
  mismatched identity. It MUST make zero network/provider requests and MUST NOT construct a default
  login/query-capable adapter.
- FR-24: `FrozenReliabilityVersions` MUST contain non-null ready-time identities for Git commit,
  installed RELEASE, dataset generation, primary and secondary providers, qualification/admission,
  adapter, endpoint/schema, selection/reconciliation policy, config digest, auto-failover setting,
  kill switch, priority, continuity start/repair policy, calendar/universe generations, and
  replication/restore policy/evidence versions, trust scope, destination generation/head proof and
  remote verification. All 20 observations MUST equal this vector.
- FR-25: Raw captured calendar observations MUST retain source order and duplicates for validation;
  duplicate or out-of-order input MUST be unavailable. Only after validation MAY the evaluator
  derive sorted unique confirmed sessions. `SnapshotIdentity` MUST bind requested range, Shanghai
  clock instant/time-zone contract, all input fingerprints and the frozen version vector.
- FR-26: The report MUST contain exactly 20 ordered per-session observations or immutable
  observation references with digests when a candidate window is evaluated. Each observation MUST
  independently cover cutoff results, coverage, pointer/manifest/hash, source, lineage,
  replication and restore evidence.
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
  The reference synthetic gate MUST complete within 10,000 ms for 20 sessions and at most 100,000
  rows; exceeding the bound is a harness failure, not a production SLO pass.
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
- NFR-15: The acceptance API/CLI is additive only. Existing response models/routes and persisted
  schemas remain unchanged; all new acceptance fields are namespaced to this report.

## Acceptance Criteria

### AC-1: Exact session window and status mapping (FR-4, FR-5, FR-6, FR-14)

Given a captured calendar with confirmed sessions, when the requested range contains exactly 20
distinct consecutive sessions, then the report selects those dates in order and can be `ready`.
Given 19, 21, duplicate, future or missing-middle sessions, when evaluation runs, then the report
is `not_ready` or `unavailable` with an allowlisted reason and never silently pads the window.

Planned test anchors: `test_r2f5_exactly_20_confirmed_sessions_only`,
`test_r2f5_rejects_19_21_duplicate_future_and_missing_middle`.

### AC-2: Shanghai cutoff arithmetic (FR-8, FR-9)

Given timezone-aware publication times, when a session is published at 21:15 and next morning at
08:00 Asia/Shanghai, then both boundary observations count. When they occur at 21:16 or 08:01,
then the corresponding metric fails and the report cannot be ready.

Planned test anchors: `test_r2f5_cutoffs_are_inclusive_in_shanghai`,
`test_r2f5_late_evening_and_next_morning_fail`.

### AC-3: Frozen version set and drift (FR-7, NFR-8)

Given 20 session observations with one frozen version vector, when any provider, adapter, endpoint,
policy, calendar, universe, replication, restore or schema identity changes, then the report is
`not_ready` with `VERSION_DRIFT` and the exact source state remains untouched.

Planned test anchors: `test_r2f5_version_vector_is_frozen_across_window`,
`test_r2f5_version_drift_is_not_coerced_to_ready`.

### AC-4: Coverage and source purity (FR-9, FR-12)

Given legal-universe counts and canonical partition source identities, when every session has 100%
coverage and zero mixed-source rows, then those metrics pass. When any unknown/count mismatch or
mixed provider partition exists, then the report is not ready and names only the sanitized reason.

Planned test anchors: `test_r2f5_coverage_requires_exact_legal_universe`,
`test_r2f5_mixed_source_partition_fails_closed`.

### AC-5: Evidence lineage and replay (FR-10, FR-11)

Given retained final-success evidence, candidate/gate, manifest/object and selection hashes, when
the bounded offline sample replays semantically identically, then provenance and replay pass.
Given a missing hash, wrong binding, corrupt object or replay mismatch, then no publication state is
changed and the relevant metric is unavailable/not ready.

Planned test anchors: `test_r2f5_lineage_requires_evidence_candidate_gate_selection`,
`test_r2f5_replay_is_bounded_and_offline`, `test_r2f5_corrupt_lineage_fails_closed`.

### AC-6: Replication and restore evidence (FR-13, FR-17)

Given immutable replication status and a completed verified restore drill, when lag and restore
proof satisfy the declared evidence contract, then the metrics pass. Given lag, missing drill,
locked/corrupt sidecar or unverified generation, then the report is not ready/unavailable and does
not start a drain or restore.

Planned test anchors: `test_r2f5_replication_lag_is_visible`,
`test_r2f5_restore_requires_completed_immutable_drill`, `test_r2f5_acceptance_never_drains_or_restores`.

### AC-7: Read-only snapshot and fingerprints (FR-1, FR-3, FR-4, NFR-3)

Given an existing private fixture, when CLI/API evaluation succeeds or fails, then every input
fingerprint is byte/metadata identical before and after, no missing control DB is initialized, and
the captured snapshot is closed after use.

Planned test anchors: `test_r2f5_success_preserves_all_input_fingerprints`,
`test_r2f5_error_preserves_all_input_fingerprints`, `test_r2f5_missing_control_db_is_not_initialized`.

### AC-8: Path validation and redaction (FR-2, NFR-4, NFR-5)

Given relative, root, home, mutable-root, symlink, unresolved-variable or overlapping paths, when
the CLI/API is invoked, then it rejects before enumeration with exit 2/HTTP 422 and no path is
echoed. Given corrupt or permission-denied input, then the response contains only a closed reason.

Planned test anchors: `test_r2f5_path_validation_rejects_mutable_aliases`,
`test_r2f5_path_and_exception_redaction_is_stable`.

### AC-9: API/CLI parity (FR-15, FR-16, FR-17)

Given identical configured roots, dates, frozen clock and fixture, when CLI and API are evaluated,
then their report status, selected sessions, metric results, reason order, `provider_requests` and
`writes` agree; the API performs no provider or store initialization.

Planned test anchors: `test_r2f5_cli_and_api_have_identical_report_contract`,
`test_r2f5_api_is_read_only_and_sanitized`.

### AC-10: Mandatory metric gate (FR-9, FR-14, NFR-2)

Given all mandatory metrics pass for exactly 20 sessions, when the report is built, then status is
`ready`. Given any mandatory metric fails, then status is `not_ready`; given a required source is
unprovable, then status is `unavailable`. No optional observation can override this mapping.

Planned test anchors: `test_r2f5_ready_requires_every_mandatory_metric`,
`test_r2f5_not_ready_and_unavailable_are_distinct`.

### AC-11: Bounded evaluation (NFR-6, NFR-7)

Given a 20-session fixture within the declared row/object/sample bounds, when evaluation runs, then
it records elapsed/read/replay counts and completes within 10,000 ms on the reference gate. Given
an over-bound object or row set, then it stops with unavailable rather than unboundedly reading.

Planned test anchors: `test_r2f5_bounded_replay_and_row_limits`,
`test_r2f5_reference_fixture_reports_elapsed_within_bound`.

### AC-12: Protected compatibility (NFR-1, NFR-9)

Given the R2-F2 golden fixture and R2-F4 readers, when the crosswalk/focused checks run, then their
bytes and public response shapes remain compatible and no protected production module is widened.

Planned test anchors: `test_r2f5_protected_golden_objects_are_unchanged`,
`test_r2f5_existing_readers_remain_compatible`.

### AC-13: Production boundary (FR-18, NFR-10)

Given any offline ready report, when a reviewer inspects metadata, then it states
`production_window_started=false`, Task 20 pending and R2-F5.0 NO-GO. No report can authorize a
provider call, installation, LaunchAgent, NAS action or Release 2 re-entry.

Planned test anchors: `test_r2f5_offline_ready_never_claims_task20_or_release2`.

### AC-14: Deterministic and sanitized errors (FR-14, FR-17, NFR-2, NFR-4)

Given identical bytes and clock, when evaluation is repeated across success, missing, corrupt and
locked fixtures, then JSON is deterministic, reason precedence is stable, diagnostics are bounded,
and all results include zero provider requests/writes.

Planned test anchors: `test_r2f5_report_digest_and_reason_order_are_deterministic`,
`test_r2f5_missing_corrupt_locked_states_fail_closed`.

### AC-15: Complete SLO metric inventory (FR-9, FR-19, NFR-11)

Given a candidate window, when the report is serialized, then all 17 named roadmap dimensions are
present as separate `MetricResult` fields with exact targets and anchors. Missing continuity,
availability, canonical integrity, recovery, failover, adjustment, error handling, local/NAS,
replication or restore evidence cannot be hidden behind another metric.

Planned test anchors: `test_r2f5_report_has_every_roadmap_slo_metric`,
`test_r2f5_each_slo_threshold_is_explicit`.

### AC-16: Secondary qualification and failover (FR-20, FR-21)

Given only BaoStock, no qualified secondary, or no immutable whole-session forced-failover drill,
when evaluation runs, then failover is `not_ready`/`unavailable` and top-level ready is impossible.
Given a qualified admission and complete drill record, then failover passes only when selection,
manifest, pointer and all rows agree with zero mixed-source rows.

Planned test anchors: `test_r2f5_baostock_only_cannot_pass_failover`,
`test_r2f5_failover_requires_qualified_whole_session_drill`.

### AC-17: Remote trust and restore snapshot (FR-22, NFR-12)

Given a completed replication/restore record with destination generation, head proof, remote
verification, trust scope and reviewed numeric thresholds, when the strict snapshot reader runs,
then it can pass. Given `LOCAL_CHAIN_ONLY`, missing threshold, locked sidecar or writer/reconcile
only evidence, then the metric is unavailable/not ready and no destination operation starts.

Planned test anchors: `test_r2f5_local_chain_only_cannot_satisfy_remote_acceptance`,
`test_r2f5_replication_restore_reader_is_strictly_read_only`.

### AC-18: Offline replay identity (FR-11, FR-23)

Given immutable bytes and an exact frozen offline adapter/normalizer identity, when replay runs,
then it performs deterministic normalization and zero provider/network requests. Given unknown,
mismatched or default login-capable identity, then replay is unavailable before adapter construction.

Planned test anchors: `test_r2f5_replay_uses_injected_offline_identity`,
`test_r2f5_replay_rejects_unknown_identity_without_provider`.

### AC-19: Full version vector (FR-7, FR-24)

Given 20 observations, when the frozen vector is captured, then every required identity is non-null
and equal across all observations. Missing RELEASE, config, qualification/admission, primary/
secondary, kill-switch/priority, policy or replication/restore evidence prevents ready.

Planned test anchors: `test_r2f5_frozen_versions_are_complete_and_nonnull_when_ready`,
`test_r2f5_version_vector_drift_is_unavailable`.

### AC-20: Raw sequence and snapshot identity (FR-3, FR-25, NFR-8, NFR-13)

Given raw calendar observations, when evaluation validates them, then it preserves source order and
rejects duplicates/out-of-order data before deriving sorted unique confirmed sessions. The report
returns a digest-bound SnapshotIdentity and observation refs; any concurrent input change invalidates
the whole report.

Planned test anchors: `test_r2f5_raw_calendar_order_and_duplicates_fail_closed`,
`test_r2f5_snapshot_identity_binds_all_inputs_and_clock`.

### AC-21: Semantic determinism (FR-26, FR-27, NFR-14)

Given identical captured bytes, clock and arguments, when evaluation repeats, then exactly 20
per-session observations/refs and byte-identical semantic JSON/digest are returned. Changing only
elapsed/counter diagnostics MUST NOT change the semantic digest.

Planned test anchors: `test_r2f5_semantic_digest_excludes_volatile_envelope`,
`test_r2f5_per_session_observation_cardinality_and_order`.

### AC-22: Closed bounded report types (FR-28, NFR-3)

Given invalid enum, reason, hash, ID, negative counter, oversized quality issue or null ready
version, when report validation runs, then it rejects the report with a sanitized unavailable result.

Planned test anchors: `test_r2f5_report_types_are_closed_and_bounded`,
`test_r2f5_ready_requires_nonnull_versions_and_exact_20_observations`.

### AC-23: Additive compatibility (NFR-1, NFR-15)

Given existing market, universe, evidence, calendar, replication and restore API responses, when
the new acceptance endpoint/CLI is evaluated, then predecessor payloads/schemas remain unchanged;
the new fields exist only in the acceptance report.

Planned test anchors: `test_r2f5_acceptance_surface_is_additive`,
`test_r2f5_predecessor_payloads_are_byte_compatible`.

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
  ancestor/descendant overlap; reject before `stat`/enumeration and do not create it.
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

interface MetricResult {
  status: "pass" | "fail" | "unavailable";
  observed: number | string | boolean | null;
  target: number | string | boolean | null;
  reason_code: AcceptanceReasonCode | null;
}

type AcceptanceReasonCode =
  | "SESSION_COUNT_NOT_20" | "SESSION_SEQUENCE_INVALID" | "PIT_VISIBILITY_INVALID"
  | "CALENDAR_UNAVAILABLE" | "CALENDAR_CONFLICT" | "UNIVERSE_UNKNOWN_NONZERO"
  | "UNIVERSE_COUNT_MISMATCH" | "VERSION_DRIFT" | "AVAILABILITY_CUTOFF_FAILED"
  | "COVERAGE_FAILED" | "CANONICAL_INTEGRITY_FAILED" | "SOURCE_PURITY_FAILED"
  | "RECOVERY_FAILED" | "FAILOVER_UNAVAILABLE" | "LINEAGE_UNAVAILABLE"
  | "REPLAY_UNAVAILABLE" | "ADJUSTMENT_UNAVAILABLE" | "ERROR_HANDLING_FAILED"
  | "LOCAL_NAS_ISOLATION_FAILED" | "REPLICATION_UNAVAILABLE" | "REPLICATION_LAG"
  | "RESTORE_UNAVAILABLE" | "PATH_INVALID" | "SNAPSHOT_CHANGED"
  | "CONTROL_STATE_UNAVAILABLE" | "BOUNDS_EXCEEDED" | "INVALID_ARGUMENTS";

interface SnapshotFingerprint {
  descriptor_role: "dataset" | "evidence" | "calendar" | "universe" | "replication" | "restore";
  device: number;
  inode: number;
  size_bytes: number;
  mtime_ns: number;
  ctime_ns: number;
  sha256: string;
}

type QualityIssueCode = AcceptanceReasonCode | "REPLAY_SEMANTIC_MISMATCH" | "REMOTE_PROOF_MISSING";

interface FrozenReliabilityVersions {
  git_commit: string;
  installed_release: string;
  installed_release_sha256: string;
  dataset_generation: string;
  canonical_schema: string;
  evidence_schema: string;
  primary_provider_id: string;
  secondary_provider_id: string;
  qualification_id: string;
  admission_id: string;
  adapter_version: string;
  endpoint_contract_version: string;
  schema_version: string;
  reconciliation_policy_version: string;
  selection_policy_version: string;
  config_digest: string;
  auto_failover_enabled: boolean;
  failover_kill_switch: boolean;
  provider_priority: string[];
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
  remote_verification_sha256: string;
  restore_policy_version: string;
  restore_evidence_version: string;
}

interface R2FAcceptanceReport {
  status: "ready" | "not_ready" | "unavailable";
  window_start: string | null;
  window_end: string | null;
  selected_sessions: string[];
  frozen_versions: FrozenReliabilityVersions | null;
  continuity: MetricResult;
  next_morning_availability: MetricResult;
  same_evening_availability: MetricResult;
  coverage: MetricResult;
  source_purity: MetricResult;
  canonical_integrity: MetricResult;
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
  quality_issues: QualityIssueCode[];
  snapshot_identity: SnapshotIdentity | null;
  session_observations: SessionObservation[];
  observation_refs: string[];
  semantic_report_sha256: string;
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
  input_fingerprints: SnapshotFingerprint[];
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

interface SessionObservation {
  trade_date: string;
  ordinal: number;
  cutoff_results: { same_evening: MetricResult; next_morning: MetricResult };
  coverage: MetricResult;
  canonical_integrity: MetricResult;
  recovery: MetricResult;
  source_provider: string;
  source_purity: MetricResult;
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
  lineage_sha256: string;
  replication_trust_scope: "LOCAL_CHAIN_ONLY" | "REMOTE_VERIFIED";
  observation_sha256: string;
}

interface CompletedReplicationRestoreSnapshotV1 {
  trust_scope: "LOCAL_CHAIN_ONLY" | "REMOTE_VERIFIED";
  destination_generation: string;
  destination_head_sha256: string;
  remote_verification_sha256: string;
  replication_policy_version: string;
  restore_policy_version: string;
  lag_threshold_seconds: number;
  restore_duration_threshold_seconds: number;
  replication_record_sha256: string;
  restore_record_sha256: string;
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
  now: string; // timezone-aware UTC instant; cutoffs are Asia/Shanghai
}
```

`evaluate` MUST be pure with respect to input stores. It MAY construct in-memory Pydantic models,
but MUST NOT call writer constructors, provider adapters, restore services in execute mode, or
database initialization/migration paths.

## Data Models

| Entity | Field | Type | Constraints |
| --- | --- | --- | --- |
| `FrozenReliabilityVersions` | `git_commit` | safe hex string | exact reviewed commit; required |
| `FrozenReliabilityVersions` | provider/adapter/policy fields | safe identifier | exact equality across all 20 sessions |
| `FrozenReliabilityVersions` | calendar/universe/replication/restore fields | safe ID + SHA-256 + closed trust scope | destination generation/head and remote verification are required for remote claims; no fabricated nulls |
| `MetricResult` | status | enum | `pass`, `fail`, `unavailable` only |
| `MetricResult` | observed/target | bounded nonnegative scalar/closed literal | no arbitrary provider text; exact threshold required |
| `SnapshotFingerprint` | root/database identity | device/inode/size/timestamps/hash | descriptor-bound; changes invalidate report |
| `CapturedSnapshot` | selected sessions | tuple of dates | exactly 20, sorted, unique and confirmed |
| `CapturedSnapshot` | input fingerprints | tuple | one per every input root/control DB |
| `CapturedSnapshot` | versions | `FrozenReliabilityVersions` | one frozen vector for operation |
| `SessionObservation` | trade date/provider/source purity | date/safe ID/enum | date and provider must match selection/manifest |
| `SessionObservation` | publication timestamps | timezone-aware datetime | Shanghai cutoff comparison only |
| `SessionObservation` | coverage counts | bounded integers | loaded equals required for 100% pass |
| `SessionObservation` | lineage hashes | SHA-256 tuple | evidence/candidate/gate/manifest/object/selection all required |
| `R2FAcceptanceReport` | status | enum | `ready`, `not_ready`, `unavailable` |
| `R2FAcceptanceReport` | selected sessions | tuple[date] | exact 20 only for a candidate window |
| `R2FAcceptanceReport` | metric fields | `MetricResult` | all mandatory rows always present |
| `R2FAcceptanceReport` | quality issues | tuple of `QualityIssueCode` | stable order, max 64, no raw text |
| `R2FAcceptanceReport` | mutation markers | literals | provider requests `0`, writes `false`, restore/production flags `false` |
| `R2FAcceptanceReport` | session observations/refs | tuple/list of 20 | ordered by validated Shanghai session; exactly 20 when candidate window is evaluated |
| `R2FAcceptanceReport` | semantic identity | `SnapshotIdentity` + SHA-256 | required; excludes volatile diagnostic envelope |
| `R2FAcceptanceReport` | diagnostics | `DiagnosticEnvelope` | nonnegative bounded counters; not included in semantic digest |
| `SnapshotIdentity` | requested range/as-of/fingerprints | dates, UTC instant, tz literal, SHA-256 | all required and bound to one captured snapshot |
| `SecondaryQualificationEvidenceV1` | qualification/admission identities | safe IDs/SHA-256 | exact 20 sessions; strict immutable evidence reader |
| `WholeSessionFailoverDrillV1` | drill and selection proof | safe IDs/SHA-256/booleans/count | remote proof and zero mixed rows required for pass |
| `CompletedReplicationRestoreSnapshotV1` | trust/destination/threshold fields | closed scope, IDs/SHA-256, nonnegative seconds | reviewed R2-F4 policy values required; LOCAL_CHAIN_ONLY cannot pass remote target |
| `OfflineReplayContext` | adapter/normalizer identity | safe IDs/SHA-256 + literal false/zero | unknown or network-capable identity rejected |

All identifiers MUST match `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`, all SHA-256 fields MUST match
`[0-9a-f]{64}`, all counters/durations MUST be integers in `[0, 2^31-1]`, and all serialized
collections MUST have explicit maximum lengths. `MetricResult.reason_code` and quality issues are
closed enums; unknown values, extra fields, negative values and ready-time nulls are validation
errors. A ready report MUST have `len(selected_sessions)=len(session_observations)=20` and
`frozen_versions != null`; an unavailable report MAY have null identity only when path/control
proof fails before a snapshot can be captured, and MUST state that reason.

### Status and reason vocabulary

The implementation MUST use the following public reason codes and MUST NOT expose arbitrary
exception text: `SESSION_COUNT_NOT_20`, `SESSION_SEQUENCE_INVALID`, `PIT_VISIBILITY_INVALID`,
`CALENDAR_UNAVAILABLE`, `CALENDAR_CONFLICT`, `UNIVERSE_UNKNOWN_NONZERO`,
`UNIVERSE_COUNT_MISMATCH`, `VERSION_DRIFT`, `AVAILABILITY_CUTOFF_FAILED`,
`COVERAGE_FAILED`, `CANONICAL_INTEGRITY_FAILED`, `SOURCE_PURITY_FAILED`,
`RECOVERY_FAILED`, `FAILOVER_UNAVAILABLE`, `LINEAGE_UNAVAILABLE`, `REPLAY_UNAVAILABLE`,
`ADJUSTMENT_UNAVAILABLE`, `ERROR_HANDLING_FAILED`, `LOCAL_NAS_ISOLATION_FAILED`,
`REPLICATION_UNAVAILABLE`, `REPLICATION_LAG`, `RESTORE_UNAVAILABLE`, `PATH_INVALID`,
`SNAPSHOT_CHANGED`, `CONTROL_STATE_UNAVAILABLE`, `SESSION_COUNT_NOT_20`,
`SESSION_SEQUENCE_INVALID`, `BOUNDS_EXCEEDED`, `INVALID_ARGUMENTS`.

Reason precedence MUST be deterministic and exact, in this order: (1) `INVALID_ARGUMENTS`,
`PATH_INVALID`; (2) `SNAPSHOT_CHANGED`, `CONTROL_STATE_UNAVAILABLE`; (3)
`PIT_VISIBILITY_INVALID`; (4) `CALENDAR_UNAVAILABLE`, `CALENDAR_CONFLICT`; (5)
`SESSION_SEQUENCE_INVALID`, `SESSION_COUNT_NOT_20`; (6) `VERSION_DRIFT`; (7)
`LINEAGE_UNAVAILABLE`, `CANONICAL_INTEGRITY_FAILED`; (8) `UNIVERSE_UNKNOWN_NONZERO`,
`UNIVERSE_COUNT_MISMATCH`; (9) per-session fields in this order: cutoff, `COVERAGE_FAILED`,
`SOURCE_PURITY_FAILED`, `RECOVERY_FAILED`, `FAILOVER_UNAVAILABLE`, `LINEAGE_UNAVAILABLE`,
`REPLAY_UNAVAILABLE`, `ADJUSTMENT_UNAVAILABLE`, `ERROR_HANDLING_FAILED`,
  `LOCAL_NAS_ISOLATION_FAILED`, `REPLICATION_UNAVAILABLE`/`REPLICATION_LAG`,
  `RESTORE_UNAVAILABLE`; (10) `BOUNDS_EXCEEDED`.
The report retains all applicable metric failures in metric order even when top-level status is
unavailable.

## Static compatibility inventory

The implementation MUST reuse and read, without widening authority, the existing
`EvidenceReader`/offline replay, `CandidateStore`/`SessionSelection`, `UniverseSidecarStore` and
`build_universe_status`, promoted calendar generation reader/runtime status,
`ReplicationStatusService`, completed `RestoreService` drill records, and the existing dataset
manifest/pointer readers. It MUST preserve `read()` compatibility and MUST NOT add a provider,
LaunchAgent, user-store read, canonical writer, NAS transport or new persistence schema.

## R2-F5.0 metric contract and evidence sources

The following table is normative. Every row is a separate `MetricResult` in the report and in every
per-session observation where the dimension is session-scoped. The threshold is the roadmap target
unless marked `R2-F4 policy`; a missing policy value is not a pass.

| MetricResult field | Exact target/threshold | Required immutable evidence | Failure reason/test anchor |
| --- | --- | --- | --- |
| `continuity` | zero missing canonical dates in 20 selected sessions | raw calendar sequence + verified canonical session inventory | `CONTINUITY_FAILED` / `test_r2f5_continuity_metric` |
| `next_morning_availability` | 20/20 by 08:00 Shanghai next civil day | immutable publication timestamps | `AVAILABILITY_CUTOFF_FAILED` / `test_r2f5_next_morning_metric` |
| `same_evening_availability` | >=18/20 by 21:15 Shanghai session day | immutable publication timestamps | `AVAILABILITY_CUTOFF_FAILED` / `test_r2f5_same_evening_metric` |
| `coverage` | 100% legal universe every session | exact universe snapshot + loaded count | `COVERAGE_FAILED` / `test_r2f5_coverage_metric` |
| `canonical_integrity` | pointer, manifest, object hash and row/date identity reconcile each session | read-only dataset/pointer/manifest proof | `CANONICAL_INTEGRITY_FAILED` / `test_r2f5_canonical_pointer_manifest_hash_metric` |
| `source_purity` | zero mixed-provider canonical partitions | per-partition provider identity | `SOURCE_PURITY_FAILED` / `test_r2f5_source_purity_metric` |
| `recovery` | injected missing date queues across restart and publishes exactly once | immutable repair/recovery observation | `RECOVERY_FAILED` / `test_r2f5_recovery_metric` |
| `failover` | forced primary failure publishes one qualified secondary whole session, zero mixed rows | secondary admission + immutable drill/readback | `FAILOVER_UNAVAILABLE` / `test_r2f5_failover_metric` |
| `provenance` | every session traces raw evidence/provider/adapter/schema/time/hash/gate/selection | evidence/candidate/gate/selection lineage | `LINEAGE_UNAVAILABLE` / `test_r2f5_provenance_metric` |
| `replay` | sampled raw evidence replays semantically identically offline | bounded immutable sample + offline identity | `REPLAY_UNAVAILABLE` / `test_r2f5_replay_metric` |
| `adjustment` | declared cross-provider adjusted return tolerance passes; raw factor equality not required | reviewed R2-F4 reconciliation policy + factors | `ADJUSTMENT_UNAVAILABLE` / `test_r2f5_adjustment_metric` |
| `calendar` | next-year warning/acquisition observable; unknown/conflict fails closed | promoted calendar generation/status evidence | `CALENDAR_UNAVAILABLE` / `test_r2f5_calendar_metric` |
| `universe` | required/loaded/suspended/not-listed/delisted/unknown reconcile; unknown zero | exact-session universe snapshot | `UNIVERSE_COUNT_MISMATCH` / `test_r2f5_universe_metric` |
| `error_handling` | timeout/auth/rate/schema/coverage/storage map to sanitized categories | immutable typed failure observations | `ERROR_HANDLING_FAILED` / `test_r2f5_error_handling_metric` |
| `local_nas_isolation` | NAS outage leaves local ready and visible retryable backlog | local pointer + replication status/trust scope | `LOCAL_NAS_ISOLATION_FAILED` / `test_r2f5_local_nas_isolation_metric` |
| `replication` | lag and destination verification satisfy frozen R2-F4 numeric policy | completed replication record with remote proof | `REPLICATION_LAG` / `test_r2f5_replication_metric` |
| `restore` | verified generation restores/readbacks within frozen R2-F4 duration policy | completed immutable restore drill record | `RESTORE_UNAVAILABLE` / `test_r2f5_restore_metric` |

`adjustment`, `replication` and `restore` have no guessed numeric tolerance in this slice. The
reader MUST require a reviewed R2-F4 policy/evidence record carrying the exact value and version.
R2-F4.3's current `LOCAL_CHAIN_ONLY` evidence is valid offline-chain evidence but cannot pass the
remote/NAS acceptance dimension or establish Task 20 production readiness.

## Secondary admission and failover evidence contract

R2-F5.0 MUST add no `ProviderId`, canonical bar field, candidate schema or selection enum. It may
define read-only domain models over retained records:

```typescript
interface SecondaryQualificationEvidenceV1 {
  provider_id: string;
  adapter_version: string;
  endpoint_contract_version: string;
  qualification_id: string;
  qualification_sessions: number; // exactly 20
  qualification_sha256: string;
  terms_evidence_sha256: string;
  admission_id: string;
  admission_sha256: string;
  status: "qualified" | "unqualified" | "unavailable";
}

interface WholeSessionFailoverDrillV1 {
  drill_id: string;
  trust_scope: "LOCAL_CHAIN_ONLY" | "REMOTE_VERIFIED";
  primary_unavailable: boolean;
  secondary_qualified: boolean;
  session: string;
  selected_provider: string;
  selected_candidate_sha256: string;
  manifest_sha256: string;
  pointer_sha256: string;
  mixed_source_rows: number;
  readback_verified: boolean;
  drill_sha256: string;
}
```

The strict reader MUST require immutable bytes, exact hashes, `qualification_sessions=20`,
`secondary_qualified=true`, `REMOTE_VERIFIED` for a remote acceptance claim, and all pointer/
manifest/selection identities to agree. Current absence, BaoStock-only, unqualified, or
`LOCAL_CHAIN_ONLY` evidence MUST return `not_ready`/`unavailable` according to proof availability.

## Replication and restore completed-record snapshot contract

The acceptance reader consumes a `CompletedReplicationRestoreSnapshotV1` assembled from existing
read-only status/record readers. It MUST contain `trust_scope`, `destination_generation`,
`destination_head_sha256`, `remote_verification_sha256`, `replication_policy_version`,
`restore_policy_version`, numeric `lag_threshold_seconds` and `restore_duration_threshold_seconds`
from reviewed R2-F4 evidence, and immutable record hashes. It MUST reject a missing threshold,
missing remote proof or `LOCAL_CHAIN_ONLY` when evaluating the remote target. It MUST use no
`create=True`, writer, reconcile, drain, mount or restore execution path; an absent/locked/corrupt
sidecar is unavailable, not an empty success.

## Offline replay contract

Replay takes `OfflineReplayContext { adapter_id, adapter_version, normalizer_id, normalizer_version,
implementation_sha256, network_allowed: false, provider_requests: 0 }`. The implementation may
invoke the frozen adapter/normalizer on immutable raw bytes for deterministic normalization, but
MUST reject unknown/mismatched identity before construction. It MUST never call login/query,
construct a default network-capable adapter, resolve credentials, open a provider socket or write
replayed output. Deterministic normalization is allowed; provider/network side effects are not.

## Snapshot identity, sequence and concurrency

`SnapshotIdentity` is computed from requested start/end, an `as_of` UTC instant, the literal
`Asia/Shanghai` cutoff contract, all input descriptor fingerprints, and the complete frozen version
vector. Raw calendar observations retain source order and duplicate entries; the reader checks
strict monotonic order before deriving sorted unique confirmed sessions. A duplicate/out-of-order
raw sequence is `unavailable`, not a sortable warning. Every report returns the identity digest,
the 20 ordered observation refs and each observation digest. If any descriptor, file bytes,
database page, version, or bound clock changes before/during/after evaluation, the whole report is
invalidated; the reader does not retry into a mixed snapshot.

The semantic report payload consists only of SnapshotIdentity, frozen versions, selected sessions,
per-session observations, metric results, quality issue codes and mutation markers. It is encoded
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
