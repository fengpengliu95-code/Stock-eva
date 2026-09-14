# R2-F5.0 requirement-evidence matrix (SPEC APPROVED - AMENDMENT CANDIDATE)

This is the normative planning crosswalk for Task 19. It is intentionally an evidence plan, not
an implementation or acceptance result. Every row is unique, every acceptance criterion names
its FR/NFR parent(s), and every anchor is `PLANNED`. No anchor below is claimed to exist, execute,
pass, or establish Task 20 production soak.

X8 revision base: `a7d3be1c6b9f760c659470fffcf6299bcd8ddf73`; predecessor planning base:
`5393f499dbc8b84398658816f7a555dd3e547d47`.

SQLite amendment X4 base: `1ec1d35bc00b5a64b9c0daf9fc8ac4927a884ba6` (clean committed X3 base;
X4 candidate not yet audited).

Approval metadata: independent audit reviewed clean X8 `964fcda98a90d4d79a0957ca8156618b87789877`;
SPEC GO, H0, M0, L1. L1 is the catalog-source validator's `startswith` checks, which MUST be
tightened to exact comparisons during implementation. This metadata-only approval commit is not
itself audited and does not claim RED, GREEN, implementation, Task 20 or production GO. Matrix
rows remain `PLANNED` evidence metadata, not execution results.

Amendment state: `SPEC APPROVED - AMENDMENT CANDIDATE / IMPLEMENTATION PAUSED / R2-F5.0 NO-GO`.
The SQLite zero-write amendment records observed macOS WAL/SHM behavior and is not yet approved;
its validator and implementation changes remain pending review.

The design is the requirement text authority. The matrix is the sole ID/parent/anchor crosswalk.
Every FR/NFR/AC/EC row has one globally unique planned anchor; the implementation plan catalog maps
each anchor one-to-one to a future pytest node/case. The SQLite X3 table maps each concrete RED
scenario one-to-one to the plan and structured contract. The validator checks IDs, parents,
anchors, model unions, exact member tuple schema, digest preimages and capture policy rather than
accepting token presence. Roadmap dimensions below are parsed from Section 10 at validation time.

| ID | Unique testable requirement summary | Parent FR/NFR refs | Planned test anchors | Stage/status |
| --- | --- | --- | --- | --- |
| FR-1 | Read existing allowlisted roots/control stores without create, initialize, migrate, repair, delete, provider, or canonical writer calls | — | `PLANNED::test_r2f5_req_fr_01` | SPEC CANDIDATE; PLANNED |
| FR-2 | Reject unsafe paths; use root-fd traversal and fixed same-parent-dirfd/two-round SQLite copy with rollback-journal absence sentinel | — | `PLANNED::test_r2f5_req_fr_02` | SPEC CANDIDATE; PLANNED |
| FR-3 | Fingerprint every input exactly; SQLite binds ordered members, rollback-journal proof and logical/catalog digests; any in-capture change invalidates | — | `PLANNED::test_r2f5_req_fr_03` | SPEC CANDIDATE; PLANNED |
| FR-4 | Capture one strict immutable in-memory snapshot and never initialize a missing control DB | — | `PLANNED::test_r2f5_req_fr_04` | SPEC CANDIDATE; PLANNED |
| FR-5 | Select exactly 20 distinct confirmed consecutive sessions visible to the Shanghai trusted clock | — | `PLANNED::test_r2f5_req_fr_05` | SPEC CANDIDATE; PLANNED |
| FR-6 | Report missing-middle, unknown, duplicate, non-advancing and later-repaired availability explicitly | — | `PLANNED::test_r2f5_req_fr_06` | SPEC CANDIDATE; PLANNED |
| FR-7 | Freeze exact provider/adapter/endpoint/policy/schema/calendar/universe/replication/restore versions | — | `PLANNED::test_r2f5_req_fr_07` | SPEC CANDIDATE; PLANNED |
| FR-8 | Apply inclusive same-evening 21:15 and next-morning 08:00 Asia/Shanghai cutoffs | — | `PLANNED::test_r2f5_req_fr_08` | SPEC CANDIDATE; PLANNED |
| FR-9 | Compute continuity, availability, coverage, source purity and integrity against every mandatory target | — | `PLANNED::test_r2f5_req_fr_09` | SPEC CANDIDATE; PLANNED |
| FR-10 | Require final-success raw evidence, candidate/gate, manifest/object and SessionSelection lineage | — | `PLANNED::test_r2f5_req_fr_10` | SPEC CANDIDATE; PLANNED |
| FR-11 | Replay at most three existing raw objects offline without provider/network/candidate mutation | — | `PLANNED::test_r2f5_req_fr_11` | SPEC CANDIDATE; PLANNED |
| FR-12 | Require strict calendar/universe states; unknown/conflict/count mismatch never guesses or publishes | — | `PLANNED::test_r2f5_req_fr_12` | SPEC CANDIDATE; PLANNED |
| FR-13 | Consume replication/restore drill evidence without draining, copying, mounting or restoring | — | `PLANNED::test_r2f5_req_fr_13` | SPEC CANDIDATE; PLANNED |
| FR-14 | Use only ready/not_ready/unavailable with ready only when all mandatory rows pass | — | `PLANNED::test_r2f5_req_fr_14` | SPEC CANDIDATE; PLANNED |
| FR-15 | CLI emits one sanitized JSON report with exits 0 valid, 1 unavailable, 2 invalid and verifies immutable window envelopes | — | `PLANNED::test_r2f5_req_fr_15` | SPEC CANDIDATE; PLANNED |
| FR-16 | Add one read-only API with configured roots and unchanged existing response models | — | `PLANNED::test_r2f5_req_fr_16` | SPEC CANDIDATE; PLANNED |
| FR-17 | Every result includes zero-write markers and closed typed MetricValue/reason/hash/cardinality validation | — | `PLANNED::test_r2f5_req_fr_17` | SPEC CANDIDATE; PLANNED |
| FR-18 | Never claim Task 20 elapsed production soak or R2-F5/Release 2 GO from an offline report; pre-capture failures use typed canonical payloads | — | `PLANNED::test_r2f5_req_fr_18` | SPEC CANDIDATE; PLANNED |
| NFR-1 | Preserve predecessor readers, public models, tables, manifests, pointers, partitions and schemas | — | `PLANNED::test_r2f5_req_nfr_01` | SPEC CANDIDATE; PLANNED |
| NFR-2 | Never open inputs through SQLite; use same-parent-dirfd no-follow member probes/copies and descriptor-safe cleanup | — | `PLANNED::test_r2f5_req_nfr_02` | SPEC CANDIDATE; PLANNED |
| NFR-3 | Keep failures deterministic, bounded, sanitized and zero-input-write; initial journal is SQLite-invalid and later journal change is SNAPSHOT_CHANGED | — | `PLANNED::test_r2f5_req_nfr_03` | SPEC CANDIDATE; PLANNED |
| NFR-4 | Complete the maximum fixture including two full SQLite member fingerprint/copy rounds, or bounded-fail within 10000 ms | — | `PLANNED::test_r2f5_req_nfr_04` | SPEC CANDIDATE; PLANNED |
| NFR-5 | Give every FR/NFR/AC/EC a unique planned test anchor without claiming it passed | — | `PLANNED::test_r2f5_req_nfr_05` | SPEC CANDIDATE; PLANNED |
| NFR-6 | Keep public errors to allowlisted reason/count/hash/time fields with no path/token/SQL/raw text | — | `PLANNED::test_r2f5_req_nfr_06` | SPEC CANDIDATE; PLANNED |
| NFR-7 | Enforce bounds plus secure outside-input temp root 0700/current uid/no-follow, files 0600/O_EXCL, temp-only fsync and max_attempts=1 | — | `PLANNED::test_r2f5_req_nfr_07` | SPEC CANDIDATE; PLANNED |
| NFR-8 | Bind snapshot identity to range, Shanghai clock, fingerprints and frozen versions | — | `PLANNED::test_r2f5_req_nfr_08` | SPEC CANDIDATE; PLANNED |
| NFR-9 | Run RED, GREEN, focused/full/static and protected compatibility checks before implementation GO | — | `PLANNED::test_r2f5_req_nfr_09` | SPEC CANDIDATE; PLANNED |
| NFR-10 | Require separate human/installed-release/terms/Task20 authority for production mutation and soak; production envelopes are Task20-owned | — | `PLANNED::test_r2f5_req_nfr_10` | SPEC CANDIDATE; PLANNED |
| AC-1 | Exactly 20 confirmed consecutive sessions can be ready; 19/21/duplicate/future/middle-gap cannot | FR-4, FR-5, FR-6, FR-14 | `PLANNED::test_r2f5_req_ac_01` | SPEC CANDIDATE; PLANNED |
| AC-2 | Inclusive Shanghai cutoff boundaries pass while 21:16 and 08:01 fail their metric | FR-8, FR-9 | `PLANNED::test_r2f5_req_ac_02` | SPEC CANDIDATE; PLANNED |
| AC-3 | Any material frozen-version drift produces not_ready/VERSION_DRIFT | FR-7, NFR-8 | `PLANNED::test_r2f5_req_ac_03` | SPEC CANDIDATE; PLANNED |
| AC-4 | 100% legal coverage and zero mixed-source rows pass; unknown/count mismatch or mixed source fails | FR-9, FR-12 | `PLANNED::test_r2f5_req_ac_04` | SPEC CANDIDATE; PLANNED |
| AC-5 | Complete lineage plus bounded offline semantic replay passes; missing/corrupt binding remains unavailable | FR-10, FR-11 | `PLANNED::test_r2f5_req_ac_05` | SPEC CANDIDATE; PLANNED |
| AC-6 | Replication lag/missing restore drill is visible and cannot initiate drain or restore | FR-13, FR-17 | `PLANNED::test_r2f5_req_ac_06` | SPEC CANDIDATE; PLANNED |
| AC-7 | Two complete SQLite copies/fingerprints are exact; typed DELETE/WAL tuple and six-checkpoint journal absence proof, secure temp, cleanup and unchanged input all hold | FR-1, FR-3, FR-4, NFR-2 | `PLANNED::test_r2f5_req_ac_07` | SPEC CANDIDATE; PLANNED |
| AC-8 | Invalid paths/config and raw diagnostic inputs fail before enumeration with redacted 2/422 output | FR-2, NFR-6 | `PLANNED::test_r2f5_req_ac_08` | SPEC CANDIDATE; PLANNED |
| AC-9 | API and CLI agree on report/markers/reason order and both perform zero initialization/provider calls | FR-15, FR-16, FR-17 | `PLANNED::test_r2f5_req_ac_09` | SPEC CANDIDATE; PLANNED |
| AC-10 | All mandatory pass gives ready; any provable failure gives not_ready; unprovable input gives unavailable | FR-9, FR-14, NFR-3 | `PLANNED::test_r2f5_req_ac_10` | SPEC CANDIDATE; PLANNED |
| AC-11 | In-bound reference fixture records counters and <=10000 ms; over-bound input bounded-fails | NFR-4, NFR-7 | `PLANNED::test_r2f5_req_ac_11` | SPEC CANDIDATE; PLANNED |
| AC-12 | Crosswalk, focused/full/static and protected golden checks preserve predecessor compatibility | NFR-1, NFR-9 | `PLANNED::test_r2f5_req_ac_12` | SPEC CANDIDATE; PLANNED |
| AC-13 | Offline ready metadata still states production_window_started=false, Task20 pending and R2-F5.0 NO-GO | FR-18, NFR-10 | `PLANNED::test_r2f5_req_ac_13` | SPEC CANDIDATE; PLANNED |
| AC-14 | Repeated identical captured bytes/clock give deterministic JSON, reason order and zero-write markers | FR-14, FR-17, NFR-3 | `PLANNED::test_r2f5_req_ac_14` | SPEC CANDIDATE; PLANNED |
| EC-1 | 19 confirmed sessions returns SESSION_COUNT_NOT_20 without inference or padding | FR-5, FR-14 | `PLANNED::test_r2f5_req_ec_01` | SPEC CANDIDATE; PLANNED |
| EC-2 | Duplicate/out-of-order raw dates return unavailable/SESSION_SEQUENCE_INVALID before sorted derivation | FR-5, FR-6 | `PLANNED::test_r2f5_req_ec_02` | SPEC CANDIDATE; PLANNED |
| EC-3 | Future date rejects with PIT_VISIBILITY_INVALID after strict source proof | FR-5, FR-6 | `PLANNED::test_r2f5_req_ec_03` | SPEC CANDIDATE; PLANNED |
| EC-4 | Unknown/conflicting calendar returns CALENDAR_UNAVAILABLE/CALENDAR_CONFLICT without guesses | FR-5, FR-12 | `PLANNED::test_r2f5_req_ec_04` | SPEC CANDIDATE; PLANNED |
| EC-5 | Universe unknown/count/index/generation mismatch prevents ready | FR-9, FR-12 | `PLANNED::test_r2f5_req_ec_05` | SPEC CANDIDATE; PLANNED |
| EC-6 | Non-Shanghai or naive timestamp cannot bypass timezone-aware cutoff | FR-8, NFR-2 | `PLANNED::test_r2f5_req_ec_06` | SPEC CANDIDATE; PLANNED |
| EC-7 | Later repair does not rewrite an original late/missing availability timestamp | FR-6, FR-9 | `PLANNED::test_r2f5_req_ec_07` | SPEC CANDIDATE; PLANNED |
| EC-8 | Any provider/adapter/endpoint/policy/calendar/universe/replication/restore drift is not_ready | FR-7, NFR-8 | `PLANNED::test_r2f5_req_ec_08` | SPEC CANDIDATE; PLANNED |
| EC-9 | Missing/corrupt/partial/wrong-date/provider/mixed lineage is unavailable | FR-10, NFR-3 | `PLANNED::test_r2f5_req_ec_09` | SPEC CANDIDATE; PLANNED |
| EC-10 | Missing/oversized/malformed/semantic-mismatch replay is bounded REPLAY_UNAVAILABLE | FR-11, NFR-7 | `PLANNED::test_r2f5_req_ec_10` | SPEC CANDIDATE; PLANNED |
| EC-11 | Missing/corrupt/locked/ahead/lagged/orphan replication evidence is read-only unavailable/degraded | FR-13, NFR-3 | `PLANNED::test_r2f5_req_ec_11` | SPEC CANDIDATE; PLANNED |
| EC-12 | Missing/nonterminal/hash-invalid/changing/unsafe restore drill is unavailable without destination creation | FR-13, NFR-3 | `PLANNED::test_r2f5_req_ec_12` | SPEC CANDIDATE; PLANNED |
| EC-13 | Replaced/unlink-recreated/writer-overlapped input or rollback-journal create/remove returns SNAPSHOT_CHANGED even if final trio matches; writer-before/new and writer-after/old are stable | FR-2, FR-3, NFR-2 | `PLANNED::test_r2f5_req_ec_13` | SPEC CANDIDATE; PLANNED |
| EC-14 | Relative/root/home/mutable/variable/overlap path rejects before stat/enumeration | FR-2, NFR-5 | `PLANNED::test_r2f5_req_ec_14` | SPEC CANDIDATE; PLANNED |
| EC-15 | Absent DB, illegal sidecar vector, initially present rollback journal, invalid temp security or failed cleanup returns its typed unavailable reason without retry/write | FR-1, FR-4, NFR-2 | `PLANNED::test_r2f5_req_ec_15` | SPEC CANDIDATE; PLANNED |
| EC-16 | Exception path/token/SQL/URL/provider text is reduced to allowlisted reason/detail | FR-14, NFR-6 | `PLANNED::test_r2f5_req_ec_16` | SPEC CANDIDATE; PLANNED |
| EC-17 | Object/row/sample/time bound stops safely and reports counters | FR-11, NFR-4, NFR-7 | `PLANNED::test_r2f5_req_ec_17` | SPEC CANDIDATE; PLANNED |
| EC-18 | Malformed dates/missing args/disallowed overrides return 422/2 without filesystem/provider I/O | FR-15, FR-16, NFR-5 | `PLANNED::test_r2f5_req_ec_18` | SPEC CANDIDATE; PLANNED |
| FR-19 | Expose 17 roadmap MetricResult fields plus replication as a Local/NAS child metric; each has an independent reducer and threshold | FR-9 | `PLANNED::test_r2f5_req_fr_19` | SPEC CANDIDATE; PLANNED |
| FR-20 | Project only existing provider_record/qualification_window fields; missing proof makes failover unavailable and purity cannot substitute | FR-10 | `PLANNED::test_r2f5_req_fr_20` | SPEC CANDIDATE; PLANNED |
| FR-21 | Read replication/restore snapshots with existing checkpoint/record/head/archive/audit fields; Task20 owns remote/readback envelope | FR-13 | `PLANNED::test_r2f5_req_fr_21` | SPEC CANDIDATE; PLANNED |
| FR-22 | Replay only with injected frozen offline adapter/normalizer identity and zero provider/network construction | FR-11 | `PLANNED::test_r2f5_req_fr_22` | SPEC CANDIDATE; PLANNED |
| FR-23 | Require complete non-null ready-time version vector and equality across all 20 observations | FR-7 | `PLANNED::test_r2f5_req_fr_23` | SPEC CANDIDATE; PLANNED |
| FR-24 | Preserve raw calendar order/duplicates and bind SnapshotIdentity to range, Shanghai clock, fingerprints and versions | FR-3, FR-5 | `PLANNED::test_r2f5_req_fr_24` | SPEC CANDIDATE; PLANNED |
| FR-25 | Return exactly 20 ordered per-session observations or digest-bound references; window drills live in one typed bundle | FR-9 | `PLANNED::test_r2f5_req_fr_25` | SPEC CANDIDATE; PLANNED |
| FR-26 | Exclude elapsed/read/replay counters from semantic canonical JSON and digest | FR-16 | `PLANNED::test_r2f5_req_fr_26` | SPEC CANDIDATE; PLANNED |
| FR-27 | Enforce closed enums/reason/date-time/MetricValue kinds, safe IDs/hashes, digest contracts, nonnegative bounds and readonly cardinalities | FR-14 | `PLANNED::test_r2f5_req_fr_27` | SPEC CANDIDATE; PLANNED |
| FR-28 | Enforce exact reason precedence and invalidate the whole report on concurrent snapshot change | FR-3, FR-6 | `PLANNED::test_r2f5_req_fr_28` | SPEC CANDIDATE; PLANNED |
| NFR-11 | Preserve roadmap continuity, availability, coverage and purity thresholds without invention or relaxation | FR-9 | `PLANNED::test_r2f5_req_nfr_11` | SPEC CANDIDATE; PLANNED |
| NFR-12 | Require reviewed R2-F4 numeric lag/duration thresholds; missing values or LOCAL_CHAIN_ONLY cannot pass remote | FR-13 | `PLANNED::test_r2f5_req_nfr_12` | SPEC CANDIDATE; PLANNED |
| NFR-13 | Fail closed on entry/presence/inode/hash or journal/parent-directory drift, detect unlink/recreate and journal create/remove, use two equal rounds and never retry/mix | FR-3, FR-4 | `PLANNED::test_r2f5_req_nfr_13` | SPEC CANDIDATE; PLANNED |
| NFR-14 | Encode semantic report and every digest field with one exact canonical contract; exclude envelope/volatile diagnostics as declared | FR-16 | `PLANNED::test_r2f5_req_nfr_14` | SPEC CANDIDATE; PLANNED |
| NFR-15 | Keep API/CLI additive and predecessor response/schema contracts unchanged | FR-15, FR-16 | `PLANNED::test_r2f5_req_nfr_15` | SPEC CANDIDATE; PLANNED |
| AC-15 | All 17 roadmap Section 10 table dimensions plus replication child appear as independent MetricResult fields with reducer/threshold/reason/anchor | FR-9, FR-19, NFR-11 | `PLANNED::test_r2f5_req_ac_15` | SPEC CANDIDATE; PLANNED |
| AC-16 | BaoStock-only/no secondary/no whole-session drill cannot pass failover; qualified proof requires whole-session purity | FR-20, FR-21 | `PLANNED::test_r2f5_req_ac_16` | SPEC CANDIDATE; PLANNED |
| AC-17 | LOCAL_CHAIN_ONLY/missing threshold or remote proof is unavailable/not_ready and starts no writer operation | FR-21, NFR-12, NFR-13 | `PLANNED::test_r2f5_req_ac_17` | SPEC CANDIDATE; PLANNED |
| AC-18 | Frozen offline replay identity permits deterministic normalization with zero provider/network; unknown identity rejects first | FR-22, NFR-13 | `PLANNED::test_r2f5_req_ac_18` | SPEC CANDIDATE; PLANNED |
| AC-19 | Complete required frozen vector is non-null and equal across 20 observations | FR-23, NFR-14 | `PLANNED::test_r2f5_req_ac_19` | SPEC CANDIDATE; PLANNED |
| AC-20 | Raw duplicate/out-of-order calendar fails before sorting and SnapshotIdentity binds every input/clock/version | FR-24, FR-28, NFR-13 | `PLANNED::test_r2f5_req_ac_20` | SPEC CANDIDATE; PLANNED |
| AC-21 | Candidate report contains exactly 20 ordered per-session observations/refs with independent metric evidence | FR-25, NFR-14 | `PLANNED::test_r2f5_req_ac_21` | SPEC CANDIDATE; PLANNED |
| AC-22 | Changing only elapsed/read/replay diagnostics leaves semantic JSON bytes and digest unchanged | FR-26, NFR-14 | `PLANNED::test_r2f5_req_ac_22` | SPEC CANDIDATE; PLANNED |
| AC-23 | Invalid enum/reason/hash/ID/negative/oversized fields reject safely; additive predecessor contracts remain unchanged | FR-27, NFR-15 | `PLANNED::test_r2f5_req_ac_23` | SPEC CANDIDATE; PLANNED |
| EC-19 | Missing SLO field or threshold returns unavailable and cannot infer pass from another dimension | FR-19, NFR-11 | `PLANNED::test_r2f5_req_ec_19` | SPEC CANDIDATE; PLANNED |
| EC-20 | BaoStock-only/unqualified secondary returns failover not_ready/unavailable regardless of purity | FR-20 | `PLANNED::test_r2f5_req_ec_20` | SPEC CANDIDATE; PLANNED |
| EC-21 | Incomplete forced-failover proof cannot pass or mutate a pointer | FR-21 | `PLANNED::test_r2f5_req_ec_21` | SPEC CANDIDATE; PLANNED |
| EC-22 | LOCAL_CHAIN_ONLY or missing reviewed lag/duration policy cannot pass remote acceptance | FR-21, NFR-12 | `PLANNED::test_r2f5_req_ec_22` | SPEC CANDIDATE; PLANNED |
| EC-23 | Unknown/default network-capable replay identity is rejected before construction | FR-22 | `PLANNED::test_r2f5_req_ec_23` | SPEC CANDIDATE; PLANNED |
| EC-24 | Missing RELEASE/dataset/admission/config/policy/calendar/universe/replication/restore identity rejects ready | FR-23, NFR-14 | `PLANNED::test_r2f5_req_ec_24` | SPEC CANDIDATE; PLANNED |
| EC-25 | Duplicate/out-of-order raw calendar remains unmodified and unavailable before sorted derivation | FR-24, FR-28 | `PLANNED::test_r2f5_req_ec_25` | SPEC CANDIDATE; PLANNED |
| EC-26 | Volatile diagnostics or absent identity/observation makes semantic report incomplete/non-deterministic | FR-25, FR-26, NFR-14 | `PLANNED::test_r2f5_req_ec_26` | SPEC CANDIDATE; PLANNED |

## SQLite zero-write planned RED crosswalk (X3)

Each row is a concrete future test and is not execution evidence. `Contract evidence` names the
machine structure the validator resolves and checks, not a prose token.

| Scenario key | Exact planned behavior | Contract evidence | Planned anchor | Stage/status |
| --- | --- | --- | --- | --- |
| delete_mode_absence | DB present/WAL+SHM typed absent and temp journal_mode=delete is the sole DELETE vector | `sqlite_member_tuple_schema`; `sqlite_capture.presence_mode_rules[0]` | `PLANNED::test_r2f5_sqlite_delete_mode_absent_sidecars` | RED PLANNED |
| wal_trio | DB/WAL/SHM all present, temp journal_mode=wal, exact trio naming and WAL application | `sqlite_capture.presence_mode_rules[1]`; `sqlite_capture.temp_storage` | `PLANNED::test_r2f5_sqlite_wal_trio_applied_from_temp` | RED PLANNED |
| wal_without_shm_invalid | WAL present and SHM absent is typed SQLite-invalid/unavailable | `sqlite_capture.invalid_presence_reason` | `PLANNED::test_r2f5_sqlite_wal_without_shm_is_invalid` | RED PLANNED |
| writer_before | Writer completed before first probe yields the new stable snapshot | `sqlite_capture.writer_timing.completed_before_capture` | `PLANNED::test_r2f5_sqlite_writer_before_yields_new_snapshot` | RED PLANNED |
| writer_during | Writer overlapping either full round yields SNAPSHOT_CHANGED | `sqlite_capture.writer_timing.overlaps_either_round` | `PLANNED::test_r2f5_sqlite_writer_during_is_snapshot_changed` | RED PLANNED |
| writer_after | Writer begun after stable boundary leaves captured old snapshot valid | `sqlite_capture.writer_timing.begins_after_stable_capture` | `PLANNED::test_r2f5_sqlite_writer_after_keeps_old_snapshot_valid` | RED PLANNED |
| unlink_recreate | Current entry/new fd mismatch with old fd detects unlink/recreate | `sqlite_capture.unlink_recreate_detection`; `sqlite_capture.entry_probe_protocol` | `PLANNED::test_r2f5_sqlite_unlink_recreate_is_detected` | RED PLANNED |
| temp_outside | Temp root is approved system temp outside every input root/alias | `sqlite_capture.temp_storage.approved_root` | `PLANNED::test_r2f5_sqlite_temp_is_outside_all_input_roots` | RED PLANNED |
| temp_modes | Root 0700/current uid/no-follow; files 0600/O_EXCL/no-follow | `sqlite_capture.temp_storage.root_mode`; `sqlite_capture.temp_storage.root_uid`; `sqlite_capture.temp_storage.root_open_flags`; `sqlite_capture.temp_storage.file_mode`; `sqlite_capture.temp_storage.file_open_flags` | `PLANNED::test_r2f5_sqlite_temp_owner_modes_and_no_follow` | RED PLANNED |
| cleanup_success | Finally cleanup proves all temp entries/root absent with temp-only fsync | `sqlite_capture.cleanup.success_requirement`; `sqlite_capture.temp_storage.fsync_scope` | `PLANNED::test_r2f5_sqlite_temp_cleanup_succeeds` | RED PLANNED |
| cleanup_failure | Cleanup failure is TEMP_CLEANUP_FAILED and leaks no path | `sqlite_capture.cleanup.failure_reason`; `sqlite_capture.cleanup.public_path_leak` | `PLANNED::test_r2f5_sqlite_temp_cleanup_failure_is_typed` | RED PLANNED |
| input_unchanged | Success and every failure leave DB/WAL/SHM bytes/metadata/presence unchanged | `sqlite_capture.source_write_policy`; `sqlite_capture.direct_input_sqlite_open`; `sqlite_capture.max_attempts` | `PLANNED::test_r2f5_sqlite_input_members_remain_unchanged` | RED PLANNED |
| initial_rollback_journal_present | Journal present at initial probe is SQLITE_SNAPSHOT_INVALID/unavailable and control unavailable; it is never copied/opened/parsed | `sqlite_capture.rollback_journal_sentinel.initial_present`; `sqlite_capture.rollback_journal_sentinel.policy` | `PLANNED::test_r2f5_sqlite_initial_rollback_journal_present_is_invalid` | RED PLANNED |
| rollback_journal_appears_disappears | Journal create/remove during capture is SNAPSHOT_CHANGED even when final DB/WAL/SHM equals the initial tuple | `sqlite_capture.rollback_journal_sentinel.appears_or_disappears_after_initial`; `sqlite_capture.rollback_journal_sentinel.between_checkpoint_detection` | `PLANNED::test_r2f5_sqlite_rollback_journal_appears_then_disappears_is_changed` | RED PLANNED |
| stable_rollback_journal_absent_delete | Stable DELETE capture proves journal absent at all six ordered checkpoints and binds the proof digest | `sqlite_capture.rollback_journal_sentinel.required_state`; `sqlite_capture.rollback_journal_sentinel.checkpoints`; `sqlite_capture.rollback_journal_sentinel.proof_model` | `PLANNED::test_r2f5_sqlite_stable_absent_rollback_journal_delete_is_accepted` | RED PLANNED |

## Mandatory SLO crosswalk

Each roadmap SLO table row has an independent report field, threshold source and planned anchor.
The roadmap has 17 rows. `replication` is an independently testable child of the Local/NAS row,
not an 18th roadmap dimension; FR-19/NFR-11 own that child inventory.

| Metric field | Exact target | Threshold source | Failure reason | Acceptance ref | Planned anchor |
| --- | --- | --- | --- | --- | --- |
| `continuity` | zero missing canonical dates in 20 sessions | roadmap Section 10 | `CONTINUITY_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_continuity` |
| `next_morning_availability` | 20/20 by 08:00 Shanghai next day | roadmap Section 10 | `AVAILABILITY_CUTOFF_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_next_morning_availability` |
| `same_evening_availability` | at least 18/20 by 21:15 Shanghai | roadmap Section 10 | `AVAILABILITY_CUTOFF_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_same_evening_availability` |
| `coverage` | 100% legal universe each session | roadmap Section 10 | `COVERAGE_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_coverage` |
| `canonical_integrity` | pointer/manifest/object hashes reconcile every session | roadmap Section 10 | `CANONICAL_INTEGRITY_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_canonical_integrity` |
| `source_purity` | zero mixed-provider canonical partitions | roadmap Section 10 | `SOURCE_PURITY_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_source_purity` |
| `recovery` | restart queue and exactly-once later publication | roadmap Section 10 | `RECOVERY_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_recovery` |
| `failover` | qualified secondary whole-session drill, zero mixed rows | roadmap Section 10 + R2-F4 evidence | `FAILOVER_UNAVAILABLE` | AC-15 | `PLANNED::test_r2f5_slo_failover` |
| `provenance` | complete raw/provider/version/hash/gate/selection lineage | roadmap Section 10 | `LINEAGE_UNAVAILABLE` | AC-15 | `PLANNED::test_r2f5_slo_provenance` |
| `replay` | bounded sample semantically identical offline | roadmap Section 10 | `REPLAY_UNAVAILABLE` | AC-15 | `PLANNED::test_r2f5_slo_replay` |
| `adjustment` | declared adjusted-return tolerance | reviewed R2-F4 policy; missing is unavailable | `ADJUSTMENT_UNAVAILABLE` | AC-15 | `PLANNED::test_r2f5_slo_adjustment` |
| `calendar` | warning/acquisition observable; unknown/conflict fail closed | roadmap Section 10 | `CALENDAR_UNAVAILABLE` | AC-15 | `PLANNED::test_r2f5_slo_calendar` |
| `universe` | required/loaded/suspension/listing counts reconcile; unknown zero | roadmap Section 10 | `UNIVERSE_COUNT_MISMATCH` | AC-15 | `PLANNED::test_r2f5_slo_universe` |
| `error_handling` | timeout/auth/rate/schema/coverage/storage sanitized categories | roadmap Section 10 | `ERROR_HANDLING_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_error_handling` |
| `local_nas_isolation` | NAS outage leaves local ready and retryable backlog | roadmap Section 10 + R2-F4.3 | `LOCAL_NAS_ISOLATION_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_local_nas_isolation` |
| `replication` *(child of local/NAS)* | lag/remote proof satisfy frozen R2-F4 numeric policy | reviewed R2-F4 policy; missing is unavailable | `REPLICATION_LAG` | AC-15 | `PLANNED::test_r2f5_slo_replication` |
| `restore` | restore/readback satisfies frozen R2-F4 duration policy | reviewed R2-F4 policy; missing is unavailable | `RESTORE_UNAVAILABLE` | AC-15 | `PLANNED::test_r2f5_slo_restore` |
| `read_boundary` | representative GET calls make no filesystem mutation | roadmap Section 10 | `READ_BOUNDARY_FAILED` | AC-15 | `PLANNED::test_r2f5_slo_read_boundary` |

## Crosswalk interpretation

`PLANNED` means a future RED/GREEN or static test anchor is named for implementation planning. It is not a test result. The only current verification claims are that the table and design are syntactically/crosswalk valid at base commit; Task 20 and production soak remain unstarted.
