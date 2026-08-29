# Release 2 / R2-F3 Free Daily Bar Shadow acceptance

## Current decision

`ACCELERATED WINDOW RATE-LIMITED / IMMEDIATE-CIRCUIT REMEDIATION PENDING RE-REVIEW / DAILY BAR SHADOW NO-GO`

The same independent reviewer returned H=0/M=0/L=0 for exact clean code commit
`a6716367a99fb7f35c4aff7f0e59753b9d38783b`. The first isolated runtime then exposed and preserved
an Asia/Shanghai date-semantics mismatch. After offline RED/GREEN remediation, full gates and a
second same-reviewer H=0/M=0/L=0 result, a new rotated runtime completed 2026-07-14 through
2026-07-16 as exact whole-session SUCCESS results. All returned OHLC cells matched canonical and
the circuit remained CLOSED through those sessions.

During accelerated continuation, 2026-07-17 returned a proven HTTP 429 at shard ordinal 28 after
ordinals 0-27 succeeded. The session failed closed: no evidence/candidate was attached, the epoch
reset, canonical and provider-admission bytes remained unchanged, and no retry/follow-up occurred.
The observation establishes 124 successful requests before this run's first 429, but does not prove
an upstream threshold or time window; the official Free quota remains `UNKNOWN`. Local triage found
that `rate_limited` used the generic three-failure circuit threshold, leaving the circuit CLOSED
after the first 429. New offline RED/GREEN changes only that policy: a proven 429 opens immediately
with a truthful failure count of one, ordinary failures retain threshold three, and cooldown still
permits only a fixed-five HALF_OPEN probe. The circuit policy/descriptor/Terms identity rotates;
timeouts, retries, reconciliation tolerance, canonical data, publication and failover do not.

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
0746623 close the per-session canonical symbol-set identity chain
a671636 bind the leased job hash in the terminal SQL trigger
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

The same reviewer then returned H=0/M=2/L=0 on exact clean commit
`29e9906dc8334f8dd60e77de240f96575dd10ddb`. Both Medium identity gaps are remediated pending an
exact-commit re-review: `canonical_symbol_set_sha256` now closes candidate -> sidecar job ->
candidate ref -> session report -> terminal attestation, and the descriptor/Terms version binds
that per-session closure contract; the stable universe-policy hash now explicitly includes the
reviewed suspended `partial` issue allowlist. Targeted RED/GREEN also proves a conflicting terminal
symbol-set hash cannot attach or advance the window. No provider request was made.

The next exact-commit pass found one final Medium SQL-boundary gap: the terminal insert trigger
joined the session and candidate hashes but not the leased job hash. A raw-SQL RED proved that a
job-only hash mutation passed that trigger; GREEN adds the exact job identity join and
`job.canonical_symbol_set_sha256 = NEW.canonical_symbol_set_sha256`. The focused trigger/registry
set passed, and the same reviewer returned H=0/M=0/L=0 on exact clean commit
`a6716367a99fb7f35c4aff7f0e59753b9d38783b` before any real request.

## First controlled real session and remediation

The isolated runtime is
`~/Library/Application Support/Stock EVA/r2f3-daily-shadow-20260829`; it does not overlap the
canonical dataset, production control databases or the Task14 discovery runtime. The initial plan
reported 3,190 expected symbols, 32 shards, `provider_requests=0`, `writes=false`; the sidecar DB,
lock and empty shadow tree were byte-identical before/after plan.

The one authorized execution then reported:

```text
trade_date=2026-07-14
provider_requests=32
expected_symbols=3190
observed_symbols=3190
outcome=MISMATCH
failure_class=reconciliation_mismatch
candidate_id=null
consecutive_sessions=0
window_state=RESET
circuit_state=CLOSED
```

The 32 final successful source pages remain immutable evidence under the user-selected
final-success-only policy; there are no failed-attempt payloads and no candidate bundle. Strict
readback reports one RESET epoch and one MISMATCH session. The canonical manifest, the referenced
2026-07-14 partition and the existing Task14 provider registry remained byte-identical. No retry or
next-date invocation occurred.

Offline RED reproduced the flaw at all three boundaries: request start was eight hours late, local
midnight for the requested date was rejected, and local midnight for the next date was accepted by
both provider parsing and evidence assembly. The four targeted tests failed 4/4 before production
changes and pass 4/4 after the minimal timezone fix; the provider/candidate focused files pass
35/35. That remediation later passed the full gates and same-reviewer re-review described below.

## Second controlled runtime and rate-limit triage

The Asia/Shanghai remediation was committed as
`1a6b2968243d616ee684b83ea376aa32b393df84`, passed 116 focused, 357 compatibility and 2,139 full
tests, and received same-reviewer H=0/M=0/L=0. A fresh runtime at
`~/Library/Application Support/Stock EVA/r2f3-daily-shadow-20260829-shanghai-v2` bound new
TermsEvidence and descriptor hashes. Its 2026-07-14 zero-write plan reported 3,190 symbols and 32
shards with an unchanged sidecar/lock/shadow tree. The real session then returned 3,190/3,190 rows,
12,760/12,760 reconciled price cells, `SUCCESS`, count 1 and circuit CLOSED. The next two explicit
dates also completed with exact quality/reconciliation PASS, raising the epoch to 3/20.

The accelerated fourth date, 2026-07-17, performed 29 one-attempt requests: 28 successful
100-symbol shards followed by one 178-byte 429 response. The terminal report is `FAILURE /
rate_limited`, candidate/evidence IDs are null, the epoch is RESET and all prior successful bundles
remain immutable. No threshold or window is inferred from this observation. Offline tests now prove
the first 429 opens the endpoint immediately, the next full slot is skipped with zero provider
requests, and only the existing post-cooldown fixed-five HALF_OPEN probe can recover. A separate
caller-owned historical runner limit of one full-session start per 60 seconds prevents this
qualification harness from generating the same accelerated burst; it is not provider semantics.

## Offline gate evidence

All commands ran in the isolated R2-F3 worktree and used local fakes/fixtures. Provider I/O was not
invoked.

| Gate | Result |
| --- | --- |
| R2-F3 focused canonical/provider/candidate/registry/worker/E2E/CLI/API | `118/118`, exit 0 |
| R2-F2, Task14 and prior shadow compatibility | `357/357`, exit 0 |
| Full repository | `2,141/2,141`, exit 0 |
| LaunchAgent assets | `26/26`, exit 0 |
| `ruff check backend tests` | all checks passed |
| `ruff format --check backend tests` | 195 files already formatted |
| `python -m compileall -q backend` | exit 0 |
| strict design validator | 100/100, Grade A, 0 errors, 0 warnings |
| `git diff --check` | exit 0 |

Two non-blocking test-environment warnings remain visible: Starlette deprecates the current
`httpx` TestClient integration, and pytest occasionally cannot clean an old macOS-protected
migration garbage directory. Neither warning changed a test result or a tracked/runtime object.

The two immediate-rate-limit REDs failed 2/2 against the prior contract and pass 2/2 after GREEN.
The rotated circuit policy hash is
`b3d4cdfa56b5db6bbc719514b35da4d430af75edf3de2c56c41fb393e8708495`; the resulting descriptor
base is `00d86cb0720f3381c1b41c8bc28f951e6e9feab6e94a829e66e3ba45d538e10f`. Existing failed
sidecars cannot satisfy this new Terms contract and remain preserved as immutable diagnostics.

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
- provider exceptions, three ordinary endpoint failures, first-429 immediate OPEN, OPEN skip,
  competing HALF_OPEN workers, probe success and probe failure are sanitized and bounded; the
  production CLI probe performs one fixed-five request and cannot continue into a full session in
  that slot;
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

- [ ] Same-reviewer exact-commit independent read-only re-review returns H=0/M=0 for the
  immediate-rate-limit circuit remediation and rotated contract hashes.
- [ ] New capability-specific reviewed TermsEvidence is installed in a third fresh isolated
  runtime for the rotated descriptor; both failed epochs and TermsEvidence objects remain preserved
  and are not reused.
- [ ] One bounded real historical Daily session completes and is read back without canonical or
  provider-registry mutation.
- [ ] Twenty same-vector consecutive confirmed sessions complete with zero reset in the qualifying
  epoch.
- [ ] The immutable 20-session qualification report passes a final independent review.
- [ ] Only then may the decision become `DAILY BAR SHADOW GO`; R2-F3 still does not imply full
  provider/factor/failover readiness beyond its stated scope.
