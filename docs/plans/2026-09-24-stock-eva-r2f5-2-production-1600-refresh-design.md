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
- FR-3: BaoStock `query_daily_history_k_AStock` may return the provider's exact
  18-field daily wire schema by inserting `peTTM,pbMRQ,psTTM,pcfNcfTTM` before
  `isST`. The adapter MUST accept only that exact known superset (or the legacy
  exact 14-field schema), project it deterministically to the existing
  `daily_astock.v1` 14-field contract before typed validation/evidence, and
  discard no contract field. Any reordered, missing, or otherwise extended
  schema remains fail-closed.
- FR-4: The configured transport deadline applies independently to the initial
  BaoStock request and to each provider pagination transition. It MUST NOT be a
  cumulative deadline across already completed pages. This does not increase
  the socket/receive timeout: a blocking initial response or any single page
  transition still fails within the same configured bound, discards the
  session, and invalidates the full candidate.
- FR-5: Within one response frame, each non-empty socket `recv` is transport
  progress and MUST renew the same configured wall-clock deadline. The bound is
  therefore an inactivity deadline, not a total-response-size deadline. An
  empty recv, a recv with no bytes before the unchanged deadline, an excessive
  response size/call count, or a missing end marker still fails closed with the
  existing socket-level evidence.
- FR-6: BaoStock `query_daily_adjust_factor` may expose the exact five-field
  wire schema ending in the provider typo `adjustFacto`. The adapter MUST map
  only that exact alias to the existing `adjustFactor` contract field before
  typed validation/evidence. Missing fields, reordered fields, or any other
  alias remain fail-closed; the canonical factor model and gates do not change.
- FR-7: Because `query_daily_adjust_factor` returns all-market events, the RAW
  adapter MUST retain only rows whose `code` is in the immutable logical
  request's stock-symbol set and deterministically order retained rows by
  `(dividOperateDate, code)` before typed validation/evidence. Out-of-scope
  board rows are not candidate data. In-scope duplicates, invalid dates,
  malformed values, and missing required factor resolution remain fail-closed.
- FR-8: Durable candidate validation MUST treat same-day factor event rows as
  an ordered subset of the planned stock universe, including the valid empty
  subset. Full factor coverage remains mandatory through the immutable factor
  resolution snapshot, whose symbols MUST still equal the complete planned
  stock universe exactly.
- FR-9: Before immutable evidence capture, the canonical adapter MUST prove an
  exact full-universe factor snapshot for the target session. If the cache is
  stale, it MUST advance the existing contiguous BaoStock daily-event stream
  through every intervening trading session and materialize the target
  snapshot; a partial stream or incomplete snapshot fails closed. An already
  complete exact snapshot MUST not cause extra provider requests.
- FR-10: The BaoStock `all_stock` response is an all-market transport result.
  The RAW adapter MUST retain only known Shanghai/Shenzhen main-board prefixes
  with `tradeStatus=1`, preserving provider order, before typed evidence and
  universe equality validation. Unknown symbols, duplicate retained symbols,
  or malformed status fields remain fail-closed.
  A filtered transport page may be empty and MUST retain its page lineage; the
  complete logical request, not every individual page, owns the non-empty
  universe requirement. All retained pages are combined for exact plan
  equality validation.
- FR-11: Canonical refresh failures MUST emit a fixed, payload-free diagnostic
  phase and normalized error kind. The diagnostic MUST distinguish RAW fetch,
  RAW validation, factor evidence capture, normalization, gate evaluation,
  candidate publication, and canonical publication without logging exception
  messages, payloads, paths, URLs, tokens, or provider response bodies.
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
9. An exact BaoStock 18-field daily response is projected to the unchanged
   14-field `daily_astock.v1` rows, including `isST`; arbitrary supersets and
   reordered schemas still fail before evidence or canonical publication.
10. A deterministic multi-page response whose individual page transitions are
    each below the configured deadline may exceed that duration in aggregate
    and still complete; a blocking transition fails within one unchanged
    deadline and retains its page-level `RECV_TIMEOUT` observation.
11. A response that takes longer than the configured deadline in aggregate but
    delivers non-empty chunks within every deadline window completes; a socket
    that stops making byte progress still times out within the unchanged bound.
12. The exact observed `daily_factor` wire alias `adjustFacto` produces the
    unchanged typed `adjustFactor` field, while unknown factor schemas fail
    before evidence or canonical publication.
13. A daily-factor response containing both requested main-board symbols and
    other-board symbols publishes evidence only for the requested set; it does
    not weaken the full main-board factor-resolution gate.
14. A stale but contiguous factor cache is advanced through the target session
    before evidence capture, while an already complete target snapshot remains
    a zero-request fast path.
15. Mixed-board `all_stock` fixtures retain only active main-board rows before
    evidence; inactive and other-board rows cannot enter the canonical universe.
16. A phase-specific failure logs only its fixed phase and normalized error
    kind; a secret-bearing exception message is absent from logs and results.

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
- EC-4: Wire-schema projection is endpoint-specific to `daily_astock`; index
  history and every other endpoint retain exact existing schema equality.
- EC-5: Resetting the deadline between pages MUST preserve a pre-existing
  earlier process timer, duplicate/repeated-page detection, end-marker checks,
  and the derived bound of at most one deadline per attempted network step.

## Out of scope

- Increasing socket or wall-clock timeouts.
- Adding attempts or shortening retry intervals.
- Publishing partial data or enabling unqualified provider failover.
