# Stock EVA R2-F5.0 Read-only Acceptance Harness Implementation Plan

> **Planning state:** SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO. This plan is
> not implementation evidence and does not start Task 20.

**Author:** Codex R2-F delivery lead

**Date:** 2026-09-14 (Asia/Shanghai)

**Status:** SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO

**Reviewers:** Independent SPEC reviewer and independent QUALITY reviewer (not yet assigned)

**Goal:** Implement Task 19 as a bounded, read-only evaluator for a proposed exact 20-session
R2-F5 window, consuming existing strict readers and immutable drill evidence only.

**Design authority:** [R2-F5.0 design](2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md)

**Base commit:** `5393f499dbc8b84398658816f7a555dd3e547d47` (must remain the starting identity)

**X4 revision base:** `4879bc7dddc87fd51613028a84ea834b67ac28d8` (clean X3)

**Worktree:** `/Users/finlay/.codex/worktrees/r2f5-0/Stock- evaluation`

**Delivery boundary:** Local code/spec and synthetic fixtures only. No provider, network,
credential, NAS, LaunchAgent, installation, restore execution, canonical mutation or production
control access.

## Context

Task 19 is an independent blocking subversion between the delivered R2-F4 contracts and Task 20's
official installed observation. The implementation must make the roadmap gate mechanically
testable without turning a report into a production claim. Existing F4 readers already enforce
descriptor binding, hash/manifest lineage, exact calendar/universe state, replication direction
and restore proof; this plan composes them behind one read-only boundary.

No task below authorizes a production soak. Any discovery of a missing contract MUST update the
design first and stop implementation until the reviewed specification is amended.

## Functional Requirements

- FR-1: The implementation MUST create only the acceptance reader/report domain and read-only CLI/
  API wiring described by the design; it MUST NOT add a writer or provider path.
- FR-2: It MUST validate paths and capture/fingerprint one immutable snapshot before evaluation;
  each input uses descriptor identity plus full streaming SHA-256 and a 512 MiB maximum.
- FR-3: It MUST select exactly 20 confirmed consecutive sessions and preserve missing-middle,
  19-versus-20 and future/PIT distinctions.
- FR-4: It MUST freeze and compare the complete version vector, apply inclusive Shanghai cutoffs,
  and compute every mandatory roadmap metric.
- FR-5: It MUST consume evidence/candidate/selection/replay/calendar/universe/replication/restore
  records without mutation and fail closed on missing, corrupt, locked or changed inputs.
- FR-6: It MUST expose the exact report, API, CLI exits, mutation markers and redaction contract.
- FR-7: It MUST stop at bounded object/row/sample/time limits and record measured counters.
- FR-8: It MUST leave protected predecessor readers, schemas, fixtures and public models compatible.
- FR-9: It MUST expose separate MetricResult fields for every roadmap Section 10 dimension, including
  canonical integrity, recovery, failover, adjustment, error handling, local/NAS isolation,
  replication, restore and read boundary.
- FR-10: It MUST require immutable secondary qualification/admission and whole-session failover
  drill evidence; BaoStock-only or source-purity-only input cannot pass failover.
- FR-11: It MUST read replication/restore completed records with trust scope, destination generation,
  head proof and frozen policy thresholds; LOCAL_CHAIN_ONLY cannot pass remote. Unsupported remote
  verification fields are absent unless supplied by a Task20 writer-owned drill envelope.
- FR-12: It MUST run replay only with injected frozen offline adapter/normalizer identity and zero
  provider/network construction, login or query.
- FR-13: It MUST require a complete non-null ready-time version vector and 20-session equality.
- FR-14: It MUST preserve raw calendar order/duplicates, validate before sorted unique derivation,
  and bind SnapshotIdentity to range, Shanghai clock, fingerprints and versions.
- FR-15: It MUST return exactly 20 ordered per-session observations or immutable references/digests;
  recovery, failover, replay, adjustment, error, NAS-isolation and restore records belong to one
  window evidence bundle, not to every session observation.
- FR-16: It MUST exclude elapsed/counters from semantic report JSON/digest and use canonical JSON.
- FR-17: It MUST enforce closed enums, reason codes, hashes, IDs, nonnegative bounds and quality issues.
- FR-18: It MUST apply exact reason precedence and fail closed on concurrent snapshot change.
- FR-19: It MUST expose 17 roadmap MetricResult fields plus replication as a Local/NAS child metric,
  each with an independent session/window reducer and explicit threshold.
- FR-20: It MUST project only existing provider_record/qualification_window fields and require an
  immutable whole-session failover envelope; missing proof is unavailable and purity cannot substitute.
- FR-21: It MUST read replication/restore snapshots with existing checkpoint/record/head/archive/audit
  fields and frozen thresholds; Task20 owns remote/schema/readback drill evidence.
- FR-22: It MUST run replay only with an injected frozen offline adapter/normalizer identity.
- FR-23: It MUST require a complete non-null frozen version vector and 20-session equality.
- FR-24: It MUST preserve raw calendar order/duplicates and bind SnapshotIdentity to all inputs.
- FR-25: It MUST return exactly 20 ordered per-session observations or digest-bound references.
- FR-26: It MUST exclude volatile diagnostics from semantic JSON and its digest.
- FR-27: It MUST enforce closed enums, reasons, hashes, IDs, counters and quality issues.
- FR-28: It MUST enforce exact reason precedence and fail closed on concurrent snapshot changes.

## Non-Functional Requirements

- NFR-1: Changes MUST be surgical and limited to the acceptance domain, CLI/API wiring, tests,
  runbook/spec evidence and crosswalk validator.
- NFR-2: No implementation test MUST open a real provider, read credentials, access NAS, load a
  LaunchAgent or touch Application Support production state.
- NFR-3: All failure paths MUST be sanitized, deterministic, bounded and zero-write.
- NFR-4: The reference 20-session/100,000-row synthetic evaluation MUST complete within 10,000 ms
  or produce a bounded failure; this is not a production SLO.
- NFR-5: Every requirement/criterion/edge case MUST have a planned unique test anchor in the
  matrix; planned anchors are not claimed as existing or passing until implementation.
- NFR-6: Every roadmap Section 10 threshold MUST be explicit and sourced from the roadmap or reviewed R2-F4 evidence.
- NFR-7: LOCAL_CHAIN_ONLY MUST be labeled offline-only and cannot satisfy remote/NAS/Task 20 acceptance.
- NFR-8: Readers MUST NOT call create=True, writer, reconcile, drain, mount or restore paths.
- NFR-9: Ready MUST require complete versions, exact 20 observations and valid SnapshotIdentity.
- NFR-10: Public enums/IDs/hashes/reasons/counters MUST be extra-forbidden, bounded and deterministic.
- NFR-11: Every roadmap Section 10 threshold MUST be explicit and sourced from the roadmap or reviewed R2-F4 evidence; the fixed source tuple has 17 rows and replication is a child metric.
- NFR-12: LOCAL_CHAIN_ONLY MUST be labeled offline-only and cannot satisfy remote/NAS/Task 20 acceptance.
- NFR-13: Readers MUST NOT call create=True, writer, reconcile, drain, mount or restore paths.
- NFR-14: Ready MUST require complete versions, exact 20 observations and valid SnapshotIdentity.
- NFR-15: Public enums/IDs/hashes/reasons/counters MUST remain bounded and deterministic, and the
  acceptance API/CLI MUST remain additive without changing predecessor models or schemas.
## Acceptance Criteria

### AC-1: Specification gate (FR-1, NFR-1, NFR-5)

Given the design and plan at the base commit, when the strict design validator and crosswalk test
run, then all mandatory sections, RFC 2119 requirements, unique IDs, AC parent references and
planned anchors are validated, with no implementation or production result claimed.

Planned test anchors are machine-readable in the requirement matrix and X4 catalog only.

### AC-2: RED boundary (FR-1, NFR-2)

Given the base tree before implementation, when the prescribed R2-F5 focused RED command runs,
then the absent acceptance service/API/CLI is demonstrated without provider/network/production I/O.

Planned test anchors are machine-readable in the requirement matrix and X4 catalog only.

### AC-3: GREEN report (FR-2, FR-3, FR-4, FR-5, FR-6, FR-7)

Given synthetic read-only fixtures, when the implementation evaluates them, then the report matches
the design model, status vocabulary, 20-session rule, cutoffs, metrics, lineage and markers.

Planned test anchors are machine-readable in the requirement matrix and X4 catalog only.

### AC-4: Full/static compatibility (FR-8, NFR-1, NFR-2)

Given the implemented slice, when focused, full, Ruff, compileall, diff and protected golden
checks run, then no predecessor compatibility surface is mutated and all gates are recorded by the
future acceptance owner. This plan records no result now.

Planned test anchors are machine-readable in the requirement matrix and X4 catalog only.

Traceability: the planned X4 acceptance criteria AC-5 through AC-23 below cover FR-9, FR-10,
FR-11, FR-12, FR-13, FR-14, FR-15, FR-16, FR-17, FR-18, FR-19, FR-20, FR-21, FR-22, FR-23,
FR-24, FR-25, FR-26, FR-27 and FR-28.

## Edge Cases

- EC-1: The implementation must preserve `unavailable` for missing/corrupt/locked control stores.
- EC-2: It must return unavailable for duplicate/out-of-order raw sessions and reject 19, 21 and
  missing-middle sessions without padding.
- EC-3: It must reject version drift and cutoff boundary failures deterministically.
- EC-4: It must reject mixed-source, coverage, universe and lineage inconsistencies.
- EC-5: It must reject unsafe/replaced/overlapping paths before enumeration.
- EC-6: It must reject over-bound replay/object/row/time inputs without unbounded retries.
- EC-7: It must preserve all fingerprints after success and error and never initialize stores.
- EC-8: It must keep Task 20, production, NAS, restore execution and Release 2 claims false.

## API Contracts

The implementation MUST follow the design's additive `GET /api/v1/market/reliability-acceptance`
contract and `r2f-acceptance --start --end [--local-dataset-root] [--evidence-root]` contract.
No existing endpoint/model is changed. API `200` covers valid ready/not-ready reports, `503`
covers unavailable input/control state, `422` covers invalid arguments; CLI exits are 0/1/2 for
ready-or-not-ready/unavailable/invalid respectively.

```typescript
interface R2FAcceptanceReader {
  evaluate(input: AcceptanceInput): R2FAcceptanceReport;
}
```

The report contract includes every roadmap Section 10 `MetricResult` field, plus replication as a
Local/NAS child metric and `read_boundary`,
`SnapshotIdentity`, a complete `FrozenReliabilityVersions`, exactly 20 ordered session
observations/references and a separate
volatile diagnostic envelope. Secondary qualification/admission, whole-session failover drill and
completed replication/restore readers are strict read-only adapters over existing immutable
records; they do not widen `ProviderId` or persisted schemas.

The implementation MUST add only read-only projection/descriptor readers where an existing status
reader is insufficient: `read_status`/`read_window` cannot establish historical secondary
qualification, admission or whole-session forced-failover proof. Qualification/admission and
completed replication/restore projections MUST be assembled from immutable evidence/audit objects,
with descriptor, generation, ID and hash bindings. The restore reader MUST NOT invoke
`RestoreAuditStore` `create=True`, reconcile or writer paths.

Replay MUST use an injected `OfflineReplayRegistry`/`OfflineReplayResolver` typed seam that resolves
the exact frozen provider/adapter/normalizer IDs, versions and implementation digest to a registered
pure callable over immutable bytes. Unknown or mismatched identities fail before construction; no
default provider-capable adapter, credential lookup, login/query, network/socket call or output
mutation is permitted. A provider-request sentinel and network/socket fake are required test seams;
the existing `EvidenceReader` default path is not a valid direct dependency for this contract.

## Data Models

The implementation MUST use the design's `MetricResult`, `FrozenReliabilityVersions`,
`SnapshotFingerprint`, `CapturedSnapshot`, `SessionObservation` and `R2FAcceptanceReport` models.
No persistence migration or new writer-owned database is permitted.

| Model | Required fields | Constraints |
| --- | --- | --- |
| `MetricResult` | status, observed, target, reason_code | closed status vocabulary; bounded scalar fields |
| `FrozenReliabilityVersions` | provider/adapter/policy/schema/calendar/universe/replication/restore IDs | exact equality across selected sessions |
| `CapturedSnapshot` | selected sessions, fingerprints, versions | exactly 20 confirmed dates; in-memory/read-only |
| `R2FAcceptanceReport` | status, metrics, markers, counters | no provider requests/writes; no production claim |
| `SnapshotIdentity` | requested range, Shanghai as-of, fingerprints, version digest | all evaluation reads bind to one identity |
| `SecondaryQualificationProjection` | existing provider_record/qualification_window fields | missing/unqualified is not_ready/unavailable; no invented IDs/hashes |
| `CompletedReplicationRestoreSnapshotV1` | existing checkpoint/record/head/archive and restore audit fields plus `FrozenR2F4PolicyThresholds` | LOCAL_CHAIN_ONLY or absent reviewed thresholds cannot pass remote; no writer/reconcile paths |
| `OfflineReplayContext` | frozen adapter/normalizer IDs and implementation hash | network false, provider requests zero; registry resolver rejects unknown/default identities |
| `SessionObservation` | session/cutoffs/counts/source set/evidence hashes/pointer reconciliation/replication/read-boundary facts | exactly ordered 20 observations; no window-drill claim |
| `WindowEvidenceBundle` | recovery/failover/replay/adjustment/error/NAS/restore records | exactly one immutable window bundle; current state alone is insufficient |
| `RecoveryObservation` | event/attempt/generation/queue/restart/exactly-once publication IDs and hashes | current queue status alone is insufficient |
| `ErrorHandlingObservation` | exactly six forced classes, expected/observed mapping, sanitized evidence hashes | each class exactly once; closed forced-error mapping only |
| `LocalNasIsolationObservation` | outage interval, local publication/pointer, backlog IDs/counts, lag, retry transition | local publication remains ready despite NAS outage |

## Planned pytest anchor catalog (X4)

This catalog is normative planning metadata. Each requirement ID appears once and maps to exactly
one stable future pytest node/case token; the crosswalk validator compares this table with the
matrix rather than treating non-empty prose as evidence. No catalog entry exists or passes yet.

| Requirement ID | Planned pytest node/case token |
| --- | --- |
| FR-1 | test_r2f5_req_fr_01 |
| FR-2 | test_r2f5_req_fr_02 |
| FR-3 | test_r2f5_req_fr_03 |
| FR-4 | test_r2f5_req_fr_04 |
| FR-5 | test_r2f5_req_fr_05 |
| FR-6 | test_r2f5_req_fr_06 |
| FR-7 | test_r2f5_req_fr_07 |
| FR-8 | test_r2f5_req_fr_08 |
| FR-9 | test_r2f5_req_fr_09 |
| FR-10 | test_r2f5_req_fr_10 |
| FR-11 | test_r2f5_req_fr_11 |
| FR-12 | test_r2f5_req_fr_12 |
| FR-13 | test_r2f5_req_fr_13 |
| FR-14 | test_r2f5_req_fr_14 |
| FR-15 | test_r2f5_req_fr_15 |
| FR-16 | test_r2f5_req_fr_16 |
| FR-17 | test_r2f5_req_fr_17 |
| FR-18 | test_r2f5_req_fr_18 |
| NFR-1 | test_r2f5_req_nfr_01 |
| NFR-2 | test_r2f5_req_nfr_02 |
| NFR-3 | test_r2f5_req_nfr_03 |
| NFR-4 | test_r2f5_req_nfr_04 |
| NFR-5 | test_r2f5_req_nfr_05 |
| NFR-6 | test_r2f5_req_nfr_06 |
| NFR-7 | test_r2f5_req_nfr_07 |
| NFR-8 | test_r2f5_req_nfr_08 |
| NFR-9 | test_r2f5_req_nfr_09 |
| NFR-10 | test_r2f5_req_nfr_10 |
| AC-1 | test_r2f5_req_ac_01 |
| AC-2 | test_r2f5_req_ac_02 |
| AC-3 | test_r2f5_req_ac_03 |
| AC-4 | test_r2f5_req_ac_04 |
| AC-5 | test_r2f5_req_ac_05 |
| AC-6 | test_r2f5_req_ac_06 |
| AC-7 | test_r2f5_req_ac_07 |
| AC-8 | test_r2f5_req_ac_08 |
| AC-9 | test_r2f5_req_ac_09 |
| AC-10 | test_r2f5_req_ac_10 |
| AC-11 | test_r2f5_req_ac_11 |
| AC-12 | test_r2f5_req_ac_12 |
| AC-13 | test_r2f5_req_ac_13 |
| AC-14 | test_r2f5_req_ac_14 |
| EC-1 | test_r2f5_req_ec_01 |
| EC-2 | test_r2f5_req_ec_02 |
| EC-3 | test_r2f5_req_ec_03 |
| EC-4 | test_r2f5_req_ec_04 |
| EC-5 | test_r2f5_req_ec_05 |
| EC-6 | test_r2f5_req_ec_06 |
| EC-7 | test_r2f5_req_ec_07 |
| EC-8 | test_r2f5_req_ec_08 |
| EC-9 | test_r2f5_req_ec_09 |
| EC-10 | test_r2f5_req_ec_10 |
| EC-11 | test_r2f5_req_ec_11 |
| EC-12 | test_r2f5_req_ec_12 |
| EC-13 | test_r2f5_req_ec_13 |
| EC-14 | test_r2f5_req_ec_14 |
| EC-15 | test_r2f5_req_ec_15 |
| EC-16 | test_r2f5_req_ec_16 |
| EC-17 | test_r2f5_req_ec_17 |
| EC-18 | test_r2f5_req_ec_18 |
| FR-19 | test_r2f5_req_fr_19 |
| FR-20 | test_r2f5_req_fr_20 |
| FR-21 | test_r2f5_req_fr_21 |
| FR-22 | test_r2f5_req_fr_22 |
| FR-23 | test_r2f5_req_fr_23 |
| FR-24 | test_r2f5_req_fr_24 |
| FR-25 | test_r2f5_req_fr_25 |
| FR-26 | test_r2f5_req_fr_26 |
| FR-27 | test_r2f5_req_fr_27 |
| FR-28 | test_r2f5_req_fr_28 |
| NFR-11 | test_r2f5_req_nfr_11 |
| NFR-12 | test_r2f5_req_nfr_12 |
| NFR-13 | test_r2f5_req_nfr_13 |
| NFR-14 | test_r2f5_req_nfr_14 |
| NFR-15 | test_r2f5_req_nfr_15 |
| AC-15 | test_r2f5_req_ac_15 |
| AC-16 | test_r2f5_req_ac_16 |
| AC-17 | test_r2f5_req_ac_17 |
| AC-18 | test_r2f5_req_ac_18 |
| AC-19 | test_r2f5_req_ac_19 |
| AC-20 | test_r2f5_req_ac_20 |
| AC-21 | test_r2f5_req_ac_21 |
| AC-22 | test_r2f5_req_ac_22 |
| AC-23 | test_r2f5_req_ac_23 |
| EC-19 | test_r2f5_req_ec_19 |
| EC-20 | test_r2f5_req_ec_20 |
| EC-21 | test_r2f5_req_ec_21 |
| EC-22 | test_r2f5_req_ec_22 |
| EC-23 | test_r2f5_req_ec_23 |
| EC-24 | test_r2f5_req_ec_24 |
| EC-25 | test_r2f5_req_ec_25 |
| EC-26 | test_r2f5_req_ec_26 |

## Planned implementation tasks

## X4 normative requirement mirror

The design is the full semantic authority. This mirror keeps every X4 ID explicit in the plan so
the crosswalk validator can detect omissions or duplicate definitions; it is not implementation
evidence.

### X4 acceptance criteria

### AC-5: Complete SLO metric inventory (FR-9, NFR-6)

Given a report, when it is serialized, then all roadmap SLO MetricResult fields and exact thresholds are present.

### AC-6: Secondary qualification/failover (FR-10, NFR-7)

Given BaoStock-only or absent drill evidence, when evaluated, then failover is not_ready/unavailable.

### AC-7: Replication/restore trust (FR-11, NFR-6, NFR-7, NFR-8)

Given LOCAL_CHAIN_ONLY or missing numeric policy, when evaluated, then remote metrics cannot pass and no writer path runs.

### AC-8: Offline replay identity (FR-12, NFR-8)

Given a frozen offline identity, when replay runs, then normalization is deterministic and provider requests are zero.

### AC-9: Complete versions (FR-13, NFR-9)

Given 20 observations, when versions are frozen, then all required identities are non-null and equal.

### AC-10: Sequence and SnapshotIdentity (FR-14, FR-15, FR-18, NFR-9)

Given raw calendar observations, when duplicate/order or fingerprint drift occurs, then evaluation is unavailable.

### AC-11: Semantic determinism (FR-16, NFR-10)

Given identical snapshot bytes, when diagnostics vary, then semantic JSON/digest remains byte-identical.

### AC-12: Closed bounded types (FR-17, NFR-9)

Given an invalid enum/hash/ID/negative bound, when validation runs, then the report is rejected safely.

### AC-13: Additive compatibility (NFR-1, NFR-10)

Given predecessor API/schema fixtures, when the acceptance surface is evaluated, then their bytes and contracts remain unchanged.

### AC-14: Deterministic/sanitized errors (FR-6, FR-28, NFR-3, NFR-15)

Given repeated identical snapshots or invalid input, when evaluation runs, then reason order and redacted output are stable.

### AC-15: Complete SLO inventory (FR-9, NFR-11)

Given a report, when serialized, then every roadmap Section 10 dimension has a separate MetricResult
and threshold, including read boundary.

### AC-16: Secondary/failover gate (FR-10, FR-20, NFR-12)

Given no qualified secondary or no whole-session drill, when evaluated, then failover cannot pass.

### AC-17: Remote replication/restore proof (FR-11, FR-21, NFR-12, NFR-13)

Given LOCAL_CHAIN_ONLY or missing frozen policy values, when evaluated, then remote metrics are unavailable/not_ready and no writer path runs.

### AC-18: Offline replay proof (FR-12, FR-22, NFR-13)

Given a frozen offline identity, when replay runs, then it performs zero provider/network calls; unknown identity is rejected first.

### AC-19: Complete frozen versions (FR-13, FR-23, NFR-14)

Given 20 observations, when versions are frozen, then every required identity is non-null and equal.

### AC-20: Raw sequence and SnapshotIdentity (FR-14, FR-24, FR-28, NFR-13)

Given raw calendar order or a concurrent fingerprint change, when evaluated, then duplicate/out-of-order or changed snapshots are unavailable.

### AC-21: Per-session cardinality (FR-15, FR-25, NFR-14)

Given a candidate window, when serialized, then exactly 20 ordered observations/refs independently cover the required fields.

### AC-22: Semantic digest isolation (FR-16, FR-26, NFR-10)

Given only elapsed/counter changes, when report envelopes are compared, then semantic JSON/digest is byte-identical.

### AC-23: Closed/bounded types (FR-17, FR-27, NFR-15)

Given invalid enum/hash/ID/negative/oversized values, when validation runs, then the report is rejected safely.

### X4 edge cases

- EC-9: Missing SLO field or threshold is unavailable and never inferred from another metric.
- EC-10: BaoStock-only/unqualified secondary cannot pass failover.
- EC-11: Incomplete forced-failover proof cannot pass or mutate a pointer.
- EC-12: LOCAL_CHAIN_ONLY or missing lag/duration policy cannot pass remote acceptance.
- EC-13: Unknown/default network-capable replay identity is rejected before construction.
- EC-14: Missing any required frozen version rejects ready.
- EC-15: Duplicate/out-of-order raw calendar is unavailable before sorting.
- EC-16: Volatile diagnostics in semantic payload are rejected or excluded from digest.
- EC-17: Any concurrent descriptor/content change invalidates the whole snapshot.
- EC-18: Invalid enum/reason/hash/ID/quality issue/counter is bounded and sanitized.
- EC-19: A missing SLO field or threshold is unavailable and never inferred.
- EC-20: BaoStock-only/unqualified secondary cannot pass failover.
- EC-21: Incomplete forced-failover proof cannot pass or mutate a pointer.
- EC-22: LOCAL_CHAIN_ONLY or missing lag/duration policy cannot pass remote acceptance.
- EC-23: Unknown/default network-capable replay identity is rejected before construction.
- EC-24: Missing frozen RELEASE/dataset/admission/config/policy/calendar/universe/replication/restore identity rejects ready.
- EC-25: Duplicate/out-of-order raw calendar is unavailable before sorting.
- EC-26: Volatile diagnostics or missing identity/observation makes the semantic report incomplete.

### Task 0 — Review and validator (SPEC-FIRST; current task)

1. Read the roadmap R2-F5/SLO and umbrella Task 19–21, R2-F0..F4 contracts/acceptance, and the
   existing readers/status/CLI/API/evidence/calendar/universe/replication/restore models.
2. Add the design, this plan, the requirement-evidence matrix and the spec crosswalk validator.
3. Run the strict validator and crosswalk only. Do not create a service, test fixture, database,
   production report or Task 20 observation.

### Task 1 — RED tests (future implementation boundary)

Files planned:

- Create `backend/app/market/reliability_acceptance.py`.
- Modify `backend/app/cli.py` and `backend/app/api/market.py` only for additive read-only wiring.
- Create `tests/test_r2f5_acceptance.py` and modify `tests/test_market_get_read_only.py`.
- Modify `docs/runbooks/market-data-reliability.md` only if that runbook exists or is explicitly
  approved as a new documentation path.

Write failing tests for exact 20/19/21/middle gap, raw duplicate/order preservation, cutoffs, the
complete frozen vector and drift, SnapshotIdentity/concurrency, lineage, coverage/source purity,
canonical pointer/manifest/hash, recovery, secondary qualification/admission and whole-session
failover, calendar/universe, error handling, local/NAS isolation, replication/restore evidence,
missing/corrupt/locked inputs, path/redaction, fingerprints, API/CLI parity, semantic digest
isolation, bounded types and zero provider/write markers. Path tests MUST permit only lstat/open
no-follow descriptor probes before enumeration/content reads.

Run RED, with no network or production roots:

```bash
uv run --offline --extra dev pytest -q tests/test_r2f5_acceptance.py \
  tests/test_market_get_read_only.py -k 'r2f5 or reliability_acceptance' \
  --basetemp=/tmp/stock-eva-r2f5-acceptance-red
```

Expected RED is an absent acceptance service/CLI/API contract. A provider or production access is
a test failure, not a tolerated RED condition.

### Task 2 — Read-only snapshot and report domain

Implement the minimum Pydantic models and pure evaluator from the design. Reuse strict readers;
open existing SQLite stores read-only; capture descriptors and fingerprints; enforce every bound,
hash and reason precedence. Add strict immutable qualification/admission, failover-drill and
replication/restore completed-record snapshot readers. Replay may invoke only an injected offline
adapter/normalizer on immutable bytes; reject unknown identity before construction. Do not add
schema migration, persistence, writer, reconcile, drain, mount or restore-execution seams.

### Task 3 — Metrics, replay and status wiring

Implement only deterministic metric projections, offline bounded replay, completed replication /
restore evidence consumption, additive API response and CLI JSON/exit behavior. Prove API/CLI
parity. The API and CLI MUST remain zero-provider, zero-write and no-restore.

### Task 4 — GREEN, focused, full and static verification

```bash
uv run --offline --extra dev pytest -q tests/test_r2f5_acceptance.py \
  tests/test_market_get_read_only.py -k 'r2f5 or reliability_acceptance' \
  --basetemp=/tmp/stock-eva-r2f5-acceptance-green
uv run --offline --extra dev pytest -q tests/test_r2f5_acceptance.py \
  tests/test_market_get_read_only.py --basetemp=/tmp/stock-eva-r2f5-acceptance-focused
uv run --offline --extra dev pytest -q --basetemp=/tmp/stock-eva-r2f5-acceptance-full
uv run --offline --extra dev ruff check backend/app/market/reliability_acceptance.py \
  backend/app/api/market.py backend/app/cli.py tests/test_r2f5_acceptance.py \
  tests/test_market_get_read_only.py tests/test_r2f5_0_spec_crosswalk.py
uv run --offline --extra dev ruff format --check backend tests
uv run --offline --extra dev python -m compileall -q backend tests
git diff --check
```

Run the strict design validator and crosswalk validator again. Verify the R2-F2 golden hashes and
protected R2-F4 surfaces against the base commit. Record actual counts and exit codes only after
they run; never prefill PASS or GO.

### Task 5 — Independent review and later handoff

An independent SPEC reviewer and independent QUALITY reviewer MUST inspect one exact clean
implementation HEAD. H/M findings block delivery. Only after implementation review may a separate
human authorize Task 20's pre-soak gates. Task 20 must freeze the installed release and observe
20 real confirmed sessions; this plan cannot substitute synthetic results for that evidence.

## Production mutation gates

The evaluator is always read-only. Provider/network/credential access, canonical/control mutation,
LaunchAgent operations, NAS drain, restore execution, installation and Task 20 clock start require
separate explicit authority and are outside this plan. A future `R2-F5 GO` record may be created
only by Task 20 after its installed-runtime evidence and independent review.

The CLI MUST reject `--execute`; there is no execution mode in R2-F5.0.

## Out of Scope

- OS-1: Any feature implementation in this SPEC-FIRST commit.
- OS-2: Task 20 production soak, dashboard, chaos drills or final R2-F acceptance record.
- OS-3: Task 21 R2-A/R2-B canaries and Release 2 re-entry.
- OS-4: Provider adapters, failover selection, repair/calendar/universe writers, replication drain,
  restore execution, LaunchAgent or deployment changes.
- OS-5: Schema migrations, breaking public response changes, fund-flow inference and production SLA claims.

## Planned requirement evidence

The authoritative requirement/evidence crosswalk is
[r2f5-0-requirement-evidence-matrix.md](../acceptance/r2f5-0-requirement-evidence-matrix.md).
Every anchor in that matrix is explicitly marked `PLANNED`, and no anchor is evidence of a pass at
this SPEC CANDIDATE stage.

## Required review decision

Until Task 0's validator/crosswalk and a later independent SPEC review are complete, the state is
`SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO`.
