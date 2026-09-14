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
- FR-2: It MUST validate paths and capture/fingerprint one immutable snapshot before evaluation.
- FR-3: It MUST select exactly 20 confirmed consecutive sessions and preserve missing-middle,
  19-versus-20 and future/PIT distinctions.
- FR-4: It MUST freeze and compare the complete version vector, apply inclusive Shanghai cutoffs,
  and compute every mandatory roadmap metric.
- FR-5: It MUST consume evidence/candidate/selection/replay/calendar/universe/replication/restore
  records without mutation and fail closed on missing, corrupt, locked or changed inputs.
- FR-6: It MUST expose the exact report, API, CLI exits, mutation markers and redaction contract.
- FR-7: It MUST stop at bounded object/row/sample/time limits and record measured counters.
- FR-8: It MUST leave protected predecessor readers, schemas, fixtures and public models compatible.

## Non-Functional Requirements

- NFR-1: Changes MUST be surgical and limited to the acceptance domain, CLI/API wiring, tests,
  runbook/spec evidence and crosswalk validator.
- NFR-2: No implementation test may open a real provider, read credentials, access NAS, load a
  LaunchAgent or touch Application Support production state.
- NFR-3: All failure paths MUST be sanitized, deterministic, bounded and zero-write.
- NFR-4: The reference 20-session/100,000-row synthetic evaluation MUST complete within 10,000 ms
  or produce a bounded failure; this is not a production SLO.
- NFR-5: Every requirement/criterion/edge case MUST have a planned unique test anchor in the
  matrix; planned anchors are not claimed as existing or passing until implementation.

## Acceptance Criteria

### AC-1: Specification gate (FR-1, NFR-1, NFR-5)

Given the design and plan at the base commit, when the strict design validator and crosswalk test
run, then all mandatory sections, RFC 2119 requirements, unique IDs, AC parent references and
planned anchors are validated, with no implementation or production result claimed.

Planned test anchors: `test_r2f5_spec_has_mandatory_sections`, `test_r2f5_crosswalk_is_exact`.

### AC-2: RED boundary (FR-1, NFR-2)

Given the base tree before implementation, when the prescribed R2-F5 focused RED command runs,
then the absent acceptance service/API/CLI is demonstrated without provider/network/production I/O.

Planned test anchors: `test_r2f5_red_service_absent`.

### AC-3: GREEN report (FR-2, FR-3, FR-4, FR-5, FR-6, FR-7)

Given synthetic read-only fixtures, when the implementation evaluates them, then the report matches
the design model, status vocabulary, 20-session rule, cutoffs, metrics, lineage and markers.

Planned test anchors: `test_r2f5_acceptance_report_green_contract`.

### AC-4: Full/static compatibility (FR-8, NFR-1, NFR-2)

Given the implemented slice, when focused, full, Ruff, compileall, diff and protected golden
checks run, then no predecessor compatibility surface is mutated and all gates are recorded by the
future acceptance owner. This plan records no result now.

Planned test anchors: `test_r2f5_protected_golden_objects_are_unchanged`.

## Edge Cases

- EC-1: The implementation must preserve `unavailable` for missing/corrupt/locked control stores.
- EC-2: It must reject 19, 21, duplicate and missing-middle sessions without padding.
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

## Planned implementation tasks

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

Write failing tests for exact 20/19/21/middle gap, cutoffs, frozen versions/drift, lineage,
coverage/source purity, calendar/universe, replication/restore evidence, missing/corrupt/locked
inputs, path/redaction, fingerprints, API/CLI parity and zero provider/write markers.

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
hash and reason precedence. Do not add schema migration, persistence or writer seams.

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
