# Release 2 / R2-F3 Free Daily Bar Shadow acceptance

## Current decision

`OFFLINE REMEDIATION GATE PASS / INDEPENDENT RE-REVIEW PENDING / DAILY BAR SHADOW NO-GO`

The implementation through code commit
`2c306271b9affe17d94914c9d314ffc32903732c` passes the prescribed offline gate.
The first independent review of `303f40ad6178d6f48ee5b37676c3ce4644d0971f` returned
`NO-GO` with H=3, M=2 and L=2. All seven findings have been remediated with witnessed RED/GREEN
evidence, but the same reviewer has not yet reviewed the new exact commit containing this record.
This document therefore does not claim code GO, a real
provider session, a 20-session qualification window, provider admission, publication eligibility,
automatic failover, adjustment-factor qualification, known units or known suspension semantics.

No real TickFlow or other provider request was made by Tasks 1-7. The next operational gate remains
closed until one independent reviewer returns H=0/M=0 for the exact clean commit containing this
record.

## Delivered capability

The new sidecar is limited to `TICKFLOW_FREE_DAILY_BAR_OHLC_V1`:

- a descriptor-bound, no-follow, read-only canonical projection over published BaoStock Daily
  partitions, including the exact reviewed suspended-placeholder exclusion form;
- one deterministic whole-session TickFlow Free plan, sorted into at most 40 sequential shards of
  at most 100 symbols, `period=1d`, `adjust=none`, one attempt and no hidden retry;
- final-success-only immutable source evidence and a separate Daily OHLC candidate;
- exact whole-session symbol equality and four-cell OHLC reconciliation with absolute tolerance
  `0.01`;
- an isolated append-only SQLite sidecar with lease/CAS terminal graphs, 20-date epochs and an
  endpoint circuit breaker;
- a one-date worker that never holds the sidecar lock during provider I/O and never starts the next
  date automatically;
- a zero-network/zero-write default CLI plan and a read-only status API;
- an execute path gated by an exact sidecar descriptor, explicit date, safe authorization ID,
  provider-request acknowledgement, execute flags, canonical readiness and a no-follow confirmed
  calendar root.

The second and later explicit invocations reuse the exact original 20-date calendar binding. A
failure/reset starts no implicit replacement window. A HALF_OPEN probe ends its scheduler slot even
when successful. The window binds a stable universe-policy/version vector while every date keeps
its own complete canonical universe/exclusion/mapping/manifest/partition identity. Provider
admission remains untouched and symbol-level mixing is absent.

## Exact implementation history

```text
c8e9968 add strict Daily canonical projection
ee2bc32 add bounded credentialless Free Daily adapter
81b2fb0 add final-success evidence and Daily OHLC candidate/reconciliation
8170970 add isolated Daily qualification sidecar
6a0dcb2 make terminal leases idempotent
421c42b assemble one-session worker and offline E2E
3b0c5fb expose zero-write plan, controlled execute and read-only status
2c30627 remediate exact-commit review and real-canonical blockers
```

The initial REDs were witnessed before each production slice. They included missing Daily modules,
missing worker/CLI/API surfaces and explicit failure/circuit/crash expectations. The Task 5 RED
also exposed that an unexpected fetcher exception did not advance the endpoint circuit; GREEN now
normalizes it to `provider_unavailable` and opens the circuit at the frozen third failure.

## First independent review and remediation

The first reviewer correctly rejected the code gate. The findings and closed evidence are:

| Finding | Remediation evidence |
| --- | --- |
| H1: legal suspended `partial` placeholders invalidated most canonical dates | RED reproduced `PARTITION_SEMANTICS_INVALID`; the reader now accepts only the exact reviewed suspended-placeholder issue set and hashes it into exclusions |
| H2: daily active mapping in the version vector made a 20-date window unreachable | read-only live analysis found 15 distinct mapping hashes in the latest 20 dates; the version vector now binds stable universe/mapping policy while each session retains its exact mapping |
| H3: universe hash omitted exclusion/manifest/partition identity | canonical universe v2 binds active symbols, exclusion SHA, manifest generation/SHA and partition path/SHA/count; model and adapter revalidate it |
| M1: status did not verify referenced immutable bundles | API/CLI readers now re-open and verify every current-epoch evidence/candidate graph, bounded to 20; missing either `COMMIT` is `UNAVAILABLE` with zero writes |
| M2: production CLI omitted the HALF_OPEN probe runner | the execute path now wires one credentialless bounded fixed-five request, resolves the circuit and ends the slot without evidence/candidate publication |
| L1: unrelated malformed manifest entries could be ignored | every manifest entry now has exact descriptor validation and trade dates are unique |
| L2: sidecar failure classes were regex-only | fetch, worker, audit, session and circuit boundaries now enforce one explicit failure-class allowlist |

The first targeted RED run failed the suspended-placeholder, manifest and changing-universe E2E
cases 3/3. The second targeted RED run failed external bundle verification, production HALF_OPEN
recovery and failure-class allowlisting 3/3. Their post-fix target set passed 23/23; the full R2-F3
focused set then passed 108/108.

A read-only projection of the actual latest 20 local canonical dates (`2026-07-14` through
`2026-08-10`) now returns ready for 20/20 dates, with 3,188-3,193 eligible active stocks per date.
This check made no canonical write and did not contact TickFlow.

## Offline gate evidence

All commands ran in the isolated R2-F3 worktree and used local fakes/fixtures. Provider I/O was not
invoked.

| Gate | Result |
| --- | --- |
| R2-F3 focused canonical/provider/candidate/registry/worker/E2E/CLI/API | `108/108`, exit 0 |
| R2-F2, Task14 and prior shadow compatibility | `357/357`, exit 0 |
| Full repository | `2,131/2,131`, exit 0 |
| LaunchAgent assets | `26/26`, exit 0 |
| `ruff check backend tests` | all checks passed |
| `ruff format --check backend tests` | 195 files already formatted |
| `python -m compileall -q backend` | exit 0 |
| strict design validator | 100/100, Grade A, 0 errors, 0 warnings |
| `git diff --check` | exit 0 |

Two non-blocking test-environment warnings remain visible: Starlette deprecates the current
`httpx` TestClient integration, and pytest occasionally cannot clean an old macOS-protected
migration garbage directory. Neither warning changed a test result or a tracked/runtime object.

## Frozen compatibility fingerprints

All eight R2-F2 frozen objects matched before and after the gate:

```text
GET.json                 1ecffe3f1572aa19520025cf885051fd8036d7eddabee0375b3f132c2d009452
candidate.json           8d58e85b3edd94f0e7d2af2fe372634b63612cebe8a981774116e4a264eee8e1
evidence.json            80c262de15a8027b6259993f938687b56aad0cda78b38d582387e2da83a18d66
manifest.json            f3c4cf48aa680190c9bd652584360e888418bb7cb4eba0248377d4fc24d7ef1f
evidence manifest        6fc19b6fd02db6da947d43e36762c64e5874099cc37ca662832d878d4c42cf7c
reader_models.json       f53e9824bafb294249f215d46162f266b134113e93e2c490df899a25f9c770e6
selection.json           92e0d74dfeba21b676262b6f639bec37b2fd09858fb2b984d81754a81bc08db1
sha256sums.txt           3f3f412cef86b4e6a41e2e1296a7be1a03a8d8cc2992a79a45333d0b8a9af02f
```

The local published canonical dataset was read back without writes. Its manifest SHA-256 remains
the previously recorded Task14 pre-gate value
`052799bddd785c8a4204e0bc3352b16edaa636934d819a156f247bb8393873a7`.
All 271 referenced Parquet objects match their immutable descriptor SHA-256 values; their ordered
date/path/content identity aggregate is
`3b8f962e80e82714e42820c37f32a65c68c87d98d8e96a5b0ff97743e61cb9fd`.

## Fail-closed and crash evidence

Offline tests prove the following boundaries:

- missing/corrupt/locked sidecar, unsafe layout, missing calendar, missing acknowledgement,
  disabled execute and unavailable canonical all stop before a provider call;
- missing or corrupt evidence/candidate bundles referenced by the current epoch make read-only
  status unavailable without initialization or repair;
- timeouts, rate limits, HTTP/schema/date/symbol/coverage failures discard all source rows and stop
  later shards;
- provider exceptions, three endpoint failures, OPEN skip, competing HALF_OPEN workers, probe
  success and probe failure are sanitized and bounded; the production CLI probe performs one
  fixed-five request and cannot continue into a full session in that slot;
- duplicate completed dates and concurrent lease losers make zero provider calls;
- canonical changes before evidence and after candidate publication never increment the window;
- evidence/candidate publication crashes may leave only immutable orphan objects; four DB attach
  crash points roll back every terminal reference/count;
- one reset epoch followed by one clean same-vector 20-session epoch qualifies only the latter;
- canonical bytes, existing provider registry, publication selection and failover state remain
  unchanged.

## Semantic and publication boundary

Even a future `SHADOW_QUALIFIED` Daily window means historical unadjusted-requested OHLC parity
only. The status contract remains:

```text
units_state=UNKNOWN
suspension_semantics_state=UNKNOWN
factor_evidence_state=UNQUALIFIED
adjustment_factor_qualified=false
publication_eligible=false
failover_enabled=false
observation_mode=HISTORICAL_SHADOW
```

No amount/volume value is interpreted as a known unit, no factor is manufactured, no old provider
observation is relabeled to a new date, and no provider row is silently mixed at symbol level.

## Remaining gates

- [ ] Same-reviewer exact-commit independent read-only re-review returns H=0/M=0.
- [ ] Capability-specific reviewed TermsEvidence and calendar root are installed in an isolated
  local runtime.
- [ ] One bounded real historical Daily session completes and is read back without canonical or
  provider-registry mutation.
- [ ] Twenty same-vector consecutive confirmed sessions complete with zero reset in the qualifying
  epoch.
- [ ] The immutable 20-session qualification report passes a final independent review.
- [ ] Only then may the decision become `DAILY BAR SHADOW GO`; R2-F3 still does not imply full
  provider/factor/failover readiness beyond its stated scope.
