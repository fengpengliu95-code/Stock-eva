# Release 2 / R2-F4.1 Promoted Runtime Calendar acceptance

## Decision candidate

**R2-F4.1 CALENDAR RUNTIME GO / PRODUCTION ENABLEMENT NOT CLAIMED / R2-F4 NO-GO**

Implementation baseline: `5a07dd3df704532055718c3adb74a195f08d4824`. This record becomes
final only after the documentation commit passes full repository gates and serial independent SPEC
and QUALITY review at one exact clean HEAD. R2-F4.2 must not start before human confirmation.

## Delivered contract

- An independent calendar-generation SQLite sidecar stores immutable reviewed SSE/SZSE source
  packages, verified official bodies, exact BaoStock civil-day observations, attempts, generation
  lineage and one transactional calendar-only head. Bundled JSON is never rewritten.
- Runtime is default-disabled. Enabled readers load a fresh complete snapshot per external operation
  and fail closed to unavailable/empty on missing, corrupt, unsafe, locked or unprovable state; they
  never fall back to bundled or stale promoted authority.
- Maintenance stages before acquisition, spends one durable `(year, Shanghai date)` slot, requires
  an existing CLOSED health snapshot, uses at most two official requests plus one `trade_dates`
  request with `max_attempts=1`, and promotes only after full agreement and parent/history CAS.
- CLI/API/job integration is read-only by default and does not add a LaunchAgent. Calendar promotion
  writes no market Parquet, SHA-256 manifest or pointer and grants no secondary-provider authority.
- Legacy `calendar_sync.sqlite3` migration is writer-only and preserves backup/guard/completion
  evidence. Persistent identity binds migration ID plus original guard device/inode; incomplete or
  substituted evidence is rejected across restart.

## Acceptance traceability

| Criterion | Authoritative repository evidence |
| --- | --- |
| AC-1 strict source/identity | `tests/test_market_calendar_generation.py`: schema, digest, duplicate-key, review-time, conflict and idempotency cases |
| AC-2 bounded official acquisition | `tests/test_market_calendar_maintenance.py`: exact origin, encoding, body/hash/size, request-count and close-failure cases |
| AC-3 exact machine coverage | `tests/test_baostock_provider.py` and maintenance tests: 365/366 coverage, ordering, flags, endpoint/audit and transport failures |
| AC-4 promotion agreement | generation/maintenance tests: evidence readback, official-machine reconciliation, parent CAS and terminal audit |
| AC-5 history/year continuity | generation tests: current/next-year bounds, no skip, completed-session protection and future amendments |
| AC-6 corrupt/missing reader | generation/runtime tests: missing, schema/row/hash/link/sidecar/path changes and bounded readers |
| AC-7 atomicity/races | generation, maintenance and calendar-sync tests: pre-commit faults, concurrent winner, inode substitution and migration recovery |
| AC-8 live visibility | `tests/test_market_calendar_runtime.py` plus automation/continuity/fund-flow/supplement tests: one operation snapshot and next-operation refresh |
| AC-9 sync snapshot | `tests/test_calendar_sync.py`: authority drift before request, mixed known/unknown range and zero persisted run |
| AC-10 policy | maintenance/runtime tests: Shanghai Oct 1/Dec 15, missing current year and explicit-year bounds |
| AC-11 durable slot | maintenance tests: crash/rerun/changed source, health gates and exact `2/1` maximum requests |
| AC-12 read-only API/CLI | runtime and market read-only tests: zero initialization/network/provider/market-store access and safe exit contracts |
| AC-13 integration | runtime/automation/LaunchAgent tests: latest candidate priority, one due slot and unchanged five-agent assets |
| AC-14 protected contracts | full suite, R2-F2 golden hashes, baseline diff and canonical-file review |

## Verified implementation gates

Task 1 passed serial independent SPEC and QUALITY review at
`197bba933ea74d5e860039dbfcaeea1f7a7921f4`, H0/M0/L0. Task 2 passed at
`17bfeb13d20081d77f399d3a8b528f0d880fb486`, H0/M0/L0. Task 3 passed at
`5a07dd3df704532055718c3adb74a195f08d4824`: SPEC H0/M0/L0 and QUALITY
H0/M0/L1. The L1 is resolved by documenting that CLI `writes_calendar_state=false` means no
business sync run/state was added; an execute-time legacy control-store migration may still change
that control file's inode/schema/bytes.

The latest Task 3 evidence is 897/897 SPEC-scoped, 774/774 QUALITY-scoped and 2641/2641 full
repository tests. The root Task 4 replay passed 646/646 planned focused tests and 2641/2641 full
repository tests. Ruff check, 206-file format check, compileall and diff checks passed. The strict
design validator scored 100/100 with zero errors/warnings/info, and all nine tracked R2-F2 golden
objects matched `sha256sums.txt`. Baseline comparison found no diff in the protected canonical
models/contracts/evidence/candidate/selection/store/dataset/failover files or five LaunchAgent
assets. The exact reviewed documentation HEAD is recorded by the final independent reviews and Git
history.

## Operational boundaries and limitations

All development and acceptance acquisition used injected offline fakes and private temporary roots.
No real SSE/SZSE body, BaoStock request, credential, production Application Support control/canonical
state, NAS, deployment, LaunchAgent load or real calendar head was accessed or changed. No live
next-year availability or elapsed production operation was proven.

BaoStock remains the only canonical provider and has no project-controlled SLA. TickFlow remains
qualified only for the isolated historical Daily Bar shadow; its factor, units, suspension,
exact-session universe, promoted-calendar and raw-retention requirements remain unknown or
unqualified. Secondary publication and automatic failover remain false. R2-F4.2 exact-session
Universe Contract, replication, canonical capability qualification, failover drill and consolidated
R2-F4 operations are still pending.

Recovery must preserve the control store, backup, guard and completed evidence for investigation.
Operators must not delete evidence, edit SQLite, rewind heads, lower gates or substitute machine
observations for official authority. Any production enablement/restoration requires a separate
controlled window and verified source/health/control pre-state.
