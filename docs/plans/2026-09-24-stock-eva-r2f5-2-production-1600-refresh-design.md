# R2-F5.2 Production 16:00 Refresh Design

## Goal

Restore production daily-bar refresh and make the current Shanghai trading
session eligible at 16:00 Asia/Shanghai on every confirmed open day.

## Root cause addressed

The LaunchAgent can run at 16:00, but the runtime calendar currently keeps the
current session ineligible until 18:10.  In addition, the automation service
performs a second BaoStock `trade_dates` request after the authoritative local
calendar has already selected the target.  A zero-byte EOF from that independent
endpoint prevents all daily-bar endpoints from being attempted.

## Contract

- At 15:59 on an open day, the latest expected session remains the previous
  confirmed session. At 16:00, it becomes the current confirmed session.
- The immutable/promoted local calendar remains the sole scheduling authority.
  Unknown or unavailable calendar state remains fail-closed.
- Daily refresh does not call BaoStock `trade_dates` as a duplicate preflight.
  Calendar synchronization and its provider observations remain separate.
- Daily bar, factor, adjustment factor, universe, and index requests retain the
  existing transport evidence, endpoint circuit breakers, quality gates,
  immutable Parquet, SHA-256, manifest, and atomic publication contracts.
- A failed or incomplete market-data endpoint still invalidates the entire
  candidate. No partial or symbol-level mixed publication is introduced.
- A protocol-success response with an empty current-session universe is a
  fail-closed `validate/universe` failure. It is retryable at the existing
  bounded recovery slots, rather than being mislabeled as an internal error.
- FR-1: Nested transport observation consumers MUST fan out each observation
  to both the outer health/audit collector and the inner RAW evidence
  collector exactly once. An inner collector MUST NOT hide socket evidence
  from circuit-breaker accounting.
- FR-2: If a failed candidate is provisionally classified as `internal`, but
  its terminal socket observations contain a normalized transport error, the
  scheduler MUST use the observed transport failure class and retryability.
  It MUST NOT reinterpret a successful transport observation as an error.
- NFR-1: Observation fan-out MUST preserve fail-closed audit behavior: an
  exception from either sink aborts the operation and no canonical pointer is
  advanced.
- Later bounded LaunchAgent slots remain available for circuit-aware recovery
  when data are not yet available at 16:00.

## Acceptance

1. Boundary tests prove 15:59 selects the previous session and 16:00 selects the
   current session.
2. A daily refresh succeeds without invoking provider `trade_dates` when the
   local calendar confirms the target.
3. Existing provider failure, circuit breaker, canonical quality, and atomic
   publication tests remain green.
4. The installed LaunchAgent triggers at 16:00 and the installed runtime commit
   matches the reviewed commit.
5. A production run either publishes the current session through all gates or
   reports the exact failing market-data endpoint and protocol stage without
   changing canonical state.
6. Given nested outer and inner sinks, one emitted observation is visible once
   in each sink, and each context is restored when its scope exits. (FR-1)
7. Given an internal candidate failure plus a terminal `RECV_TIMEOUT`, the
   persisted scheduler state is retryable `transport_timeout`; given no
   terminal transport error, the internal classification is preserved. (FR-2)
8. Given either sink fails, the operation raises and canonical publication
   remains unchanged. (NFR-1)

## API contracts

No public HTTP or CLI payload shape changes. Existing `RefreshResult` and
`SchedulerState` fields carry the corrected failure class and retry schedule.

## Data models

No schema change. Existing immutable `TransportObservation`, `RefreshResult`,
and `SchedulerState` models are reused.

## Edge cases

- EC-1: Re-entering the same sink object MUST NOT duplicate observations.
- EC-2: Nested sinks MUST restore the previous sink on inner-scope exit.
- EC-3: Provider status/rate-limit evidence maps to its existing typed class;
  unknown protocol errors remain retryable transport failures without guessed
  provider semantics.

## Out of scope

- Increasing socket or wall-clock timeouts.
- Adding attempts or shortening retry intervals.
- Publishing partial data or enabling unqualified provider failover.
