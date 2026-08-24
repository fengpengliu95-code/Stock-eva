# R2-F2 offline acceptance record

This is the offline code-acceptance record for the reviewed R2-F2 tree. It is
not provider, production, deployment, or release authorization. The record is
deliberately evidence-first: the universal suite passed, but the exact focused
matrix required by the approved plan did not pass in its prescribed order, so
the required `R2-F2 OFFLINE CODE GO` line is not emitted.

## Reviewed identity and authority

- Exact code under test before this document: `04f9bece240a5d6f90af8b71bb432a96c35889ca` (`fix(market): validate provider universe rows`).
- Approved identity authority: `52039a07d678d4fec19632deeb39bca8b6c159a7`.
- Independent identity review authority: `69ffb9885eb775acab1734c46ce28b54535bcbf6`.
- Design validator authority is the design document only; its score is not implementation evidence.
- Architecture is Option A: one descriptor-bound root transaction and CAS publication. A1 content identity is used for evidence/manifest identity; no alternate publication architecture is enabled.
- Task 7, Task 8, and Task 9 were independently recorded as High/Medium-zero review GO at the reviewed code baseline. That review status is separate from the exact closure command gate below; the focused-order failure remains visible and blocks a closure GO.

## Commands and exact results

All commands were offline and used temporary local roots. No provider, DNS,
socket, NAS, LaunchAgent, installation, or production operation was performed.

| Gate | Command / base temp | Result |
| --- | --- | --- |
| Universal | `uv run --offline --extra dev pytest -q --basetemp=/tmp/stock-eva-r2f2-full-final` | exit `0`; `1677 passed`, `0 failed` |
| Ruff | `uv run --offline --extra dev ruff check backend tests` | exit `0`; `All checks passed!` |
| Format | `uv run --offline --extra dev ruff format --check backend tests` | exit `0`; `152 files already formatted` |
| Diff | `git diff --check` | exit `0` |
| Design-only validator | `uv run --offline python /Users/finlay/.codex/skills/claude-skills--engineering/spec-driven-workflow/scripts/spec_validator.py --file docs/plans/2026-08-21-stock-eva-r2f2-provider-evidence-design.md --strict` | exit `0`; score `100/100`, Grade A, errors `0`, warnings `0`, info `0` |
| Focused matrix, prescribed order | `uv run --offline --extra dev pytest -q tests/test_market_provider_contract.py tests/test_market_provider_evidence.py tests/test_market_candidate_selection.py tests/test_market_data.py tests/test_market_get_read_only.py tests/test_baostock_transport.py tests/test_baostock_provider.py tests/test_market_reliability.py --basetemp=/tmp/stock-eva-r2f2-acceptance` | exit `1`; `470 passed, 1 failed` of `471` |
| Focused failing test alone | `uv run --offline --extra dev pytest -q tests/test_baostock_transport.py::test_upstream_send_msg_accepts_partial_send_while_patch_must_use_sendall --basetemp=/tmp/stock-eva-r2f2-acceptance-single` | exit `0`; `1 passed` |
| Focused matrix, transport/provider first | same eight files, with `test_baostock_transport.py` and `test_baostock_provider.py` first, `--basetemp=/tmp/stock-eva-r2f2-acceptance-reordered` | exit `0`; `471 passed` |

The prescribed focused failure is
`tests/test_baostock_transport.py::test_upstream_send_msg_accepts_partial_send_while_patch_must_use_sendall`:
it expected `connection.send_calls == 1` and observed `0`. The isolated and
reordered passes demonstrate order-sensitive global test state, but do not
convert the prescribed focused command into a pass. The initial validator
attempt used the mistyped non-existent filename
`docs/plans/2026-08-21-stock-eva-provider-evidence-design.md` and exited `2`;
the exact corrected design-only command and result are recorded above.

## Bounded implementation history

`git log` verified the following authority-to-HEAD sequence. No production file
or fixture was changed for this acceptance document; the only new file is this
document.

```text
69ffb98 docs(plan): approve R2-F2 identity contract
4371633 fix(market): close provider evidence identity contract
870b017 fix(market): enforce provider evidence joins
3e030e9 fix(market): close provider lineage cardinality
7cfe706 refactor(market): centralize provider page lineage validation
6e2332a fix(market): reject unbound successful page observations
a78a7d6 refactor(market): bind every query page observation to attempts
f7eece5 fix(market): separate login and query capture namespaces
c06b97c refactor(market): close operation observation cardinality
f3ee002 refactor(market): bind capture identities to unique owners
81fa4d2 feat(market): persist replayable provider evidence
c283ce9 fix(market): close evidence publication gaps
caa9b0a fix(market): bind evidence replay transaction
509bfe8 docs(market): authorize strict evidence adapter gate
1428d46 fix(market): close evidence transaction review gaps
0017e86 docs(market): authorize canonical evidence routing
38e32f7 fix(market): route canonical refresh through evidence
c656b99 fix(market): close evidence lifecycle and lineage
b10a83f docs(market): bind evidence lineage to content identity
d283b5d fix(market): bind production evidence scope and identity
590955a fix(market): bind evidence names and factor plan
12cdec3 docs(market): externalize factor resolution evidence
0140a37 docs(market): authorize resolution object kind
c34af93 feat(market): externalize factor resolution evidence
05bf97c fix(market): verify resolution sidecar identity
f31cb4b feat(market): publish provider-backed session selections
09851e4 fix(market): close candidate selection publication review gaps
79fa5c3 fix(market): require lineage on canonical save
6436e4d fix(market): close canonical selection transaction gaps
b7516e6 fix(market): harden candidate audit replay
c6f65f0 fix(market): enforce approved candidate plan shape
04a8df0 fix(market): enforce evidence-derived candidate universe
5bfafb0 fix(market): move compatibility coverage to read-only routes
04f9bec fix(market): validate provider universe rows
```

The historical review record includes the original Task 7/8/9 NO-GO rounds
and their subsequent bounded repair cycles. Those findings were not hidden:
identity/cardinality, evidence transaction/replay, factor sidecar, production
scope, selection lineage, and candidate-plan issues are represented by the
repair commits above. The remaining closure observation is the focused test
order coupling described above, not a silently waived gate.

## Evidence, candidate, and selection proof

The reviewed implementation binds the chain as follows, with concrete proofs
listed rather than relying on the validator score:

1. `ProviderRequest` and `ProviderRawBatch` are frozen typed graphs. The static
   registry admits BaoStock only; the adapter boundary is source-shaped raw
   rows, and normalization accepts only a verified `PublishedEvidence` reader
   with the manifest clock.
2. Successful-attempt pages, sanitized transport observations, logical plan,
   completion, factor snapshot, and factor-resolution sidecar are validated in
   memory before a root/object/staging write. Option A holds the root fd and
   inode through compare-create, manifest, readback, and rollback; relative
   opens are no-follow. Failure/crash tests cover zero canonical evidence and
   no unreferenced object partials, while a pre-existing CAS winner is kept.
3. A reader binds root identity, manifest identity, evidence ID, descriptor
   identity, SHA/schema/row/byte counts, transport lineage, and factor
   descriptor both directions. Replay uses the published factor snapshot and
   resolution sidecar, never the live cache; it injects the frozen clock and
   computes byte and semantic hashes independently.
4. Task 9 adds the immutable candidate manifest, the exact ordered ten-gate
   aggregate, and a single BaoStock `SessionSelection`. Candidate lineage
   carries evidence/factor hashes, provider/adapter/schema versions, universe,
   date, row counts, gate aggregate and readback identity. Selection readback
   precedes canonical manifest/pointer publication under the same
   `RefreshRunLock`; failures preserve the old pointer and candidate audit.
5. `qualified_fallback` is a future-compatible enum value only. R2-F2 writers
   reject it, reject `fallback_from`, reject mixed providers/symbol partitions,
   and do not invoke a second source or failover route.

The named Task 9 set is present and collected in
`tests/test_market_candidate_selection.py`: legacy decode/no rewrite, missing
source in-memory BaoStock decode, exact evidence universe, immutable gate
records, ordered gate aggregate, one complete candidate, mixed-provider
rejection, fallback rejection, new-manifest lineage, old-pointer preservation,
lineage mismatch, semantic gates, selection ordering, public compatibility,
read-only legacy paths, published factor resolution, and mutually exclusive
factor sources. The universal suite ran all of these. The focused matrix
failure is unrelated to these selection assertions but blocks the prescribed
closure command.

The exact 17 mandatory names from the implementation plan are present (not
replaced by a parameterized surrogate):

```text
test_legacy_baostock_rows_and_manifests_decode_without_rewrite
test_missing_legacy_source_decodes_as_baostock_in_memory
test_new_candidate_references_exact_evidence_universe_and_gate_hashes
test_every_candidate_gate_has_immutable_pass_or_fail_record
test_gate_report_requires_exact_ordered_complete_gate_aggregate
test_selection_references_one_complete_candidate_only
test_selection_rejects_mixed_provider_or_symbol_level_partition
test_qualified_fallback_is_reserved_and_rejected_by_r2f2_writers
test_new_canonical_manifest_requires_lineage_for_new_entries
test_lineage_mismatch_preserves_old_pointer_and_candidate_history
test_candidate_rejects_any_lineage_provider_schema_universe_or_hash_mismatch
test_semantic_gate_rejects_active_missing_factor_nonzero_suspended_activity_and_suspended_index
test_selection_publish_order_never_moves_pointer_early
test_existing_market_analysis_alert_user_and_api_json_remain_compatible
test_get_reads_and_legacy_manifest_reads_are_write_free
test_factor_resolution_binds_published_snapshot_or_raw_endpoint
test_factor_resolution_requires_mutually_exclusive_live_cache_fields_and_hashes
```

## Static boundary and whitelist evidence

`git diff --check` and the Ruff commands above passed. The final acceptance
change is limited to `docs/acceptance/release-2-r2f2.md`; the code HEAD is
unchanged. Static review of the canonical path found only
`ProviderId.BAOSTOCK` admission and the candidate provider check. The sole
`provider.fetch(...)` call in `backend/app/market/automation.py` is in the
legacy branch used when `canonical_refresh is None`; the explicit
`run_canonical_raw_refresh` branch requires `BaoStockProviderAdapter`, calls
`fetch_raw`, publishes/readbacks evidence, and then normalizes under the lock.
The candidate/store path has no network client. Existing supplemental AKShare
symbols in the unrelated public supplemental route and the literal future
`qualified_fallback` enum are visible static findings, not R2-F2 canonical
provider/failover paths; the R2-F2 tests reject fallback and mixed providers.

The static scans used were:

```bash
rg -n 'ProviderId\.(BAOSTOCK|TUSHARE)|ProviderId\("(tushare|akshare|tickflow)"\)|tushare|akshare|tickflow|qualified_fallback|failover' \
  backend/app/market/candidates.py backend/app/market/automation.py \
  backend/app/market/providers/base.py backend/app/storage backend/app/api/market.py \
  tests/test_market_candidate_selection.py tests/test_market_automation.py
rg -n 'provider\.fetch\(|fetch_raw\(|requests\.|httpx\.|socket|urlopen|/Volumes/Stock|nas' \
  backend/app/market/candidates.py backend/app/market/automation.py \
  backend/app/storage/dataset.py
```

No network/NAS/production command was run. The scans are boundary evidence,
not a claim that unrelated supplemental code has been deleted.

## Manual traceability: FR-1–FR-33

Each item is manually mapped to current code and named proof. The universal
suite passed these tests; where the exact focused gate is relevant, its status
is the status in the command table, not an inferred pass.

| ID | Current code / proof |
| --- | --- |
| FR-1 | Static `ProviderId` registry in `backend/app/market/providers/base.py`; provider contract tests. |
| FR-2 | Frozen `ProviderRequest` identity fields and actual scope IDs; provider identity/cardinality tests. |
| FR-3 | `BaoStockProviderAdapter` compatibility seam; real adapter contract tests. |
| FR-4 | `market/evidence.py` raw capture boundary; raw-before-normalize and replay tests. |
| FR-5 | Endpoint/schema-role allowlist; provider contract and schema mutation tests. |
| FR-6 | Content-addressed successful-attempt evidence objects; evidence atomic/crash tests. |
| FR-7 | Manifest-to-descriptor/page/transport/factor binding; lineage and descriptor tests. |
| FR-8 | Staging/CAS/manifest transaction; crash, zero-write, compare-create, and TOCTOU tests. |
| FR-9 | EvidenceReader-bound adapter normalization; real adapter and evidence-only tests. |
| FR-10 | Replay CLI/parser and local reader; parser-before-reader and zero-network tests. |
| FR-11 | Frozen-clock deterministic replay; deterministic replay/hash tests. |
| FR-12 | Candidate exact date/universe scope; candidate universe and provider-row tests. |
| FR-13 | Immutable ten-gate report; gate cardinality and immutable outcome tests. |
| FR-14 | Candidate manifest evidence/factor/lineage hashes; candidate lineage tests. |
| FR-15 | One immutable `SessionSelection`; one-complete-candidate and pointer-order tests. |
| FR-16 | BaoStock-only no-mixing/no-failover policy; mixed-provider and fallback tests/static scan. |
| FR-17 | Legacy source migration in memory only; legacy byte/read-only tests. |
| FR-18 | New canonical entries require evidence/candidate/selection lineage; lineage-required tests. |
| FR-19 | Existing immutable canonical chain after selection readback; selection pointer-order test. |
| FR-20 | Exact daily source semantics; endpoint, date, units, and semantic gate tests. |
| FR-21 | Unadjusted `adjustflag=3` plus published factor resolution; factor semantic tests. |
| FR-22 | Suspended/non-trading placeholder semantics; suspension semantic tests. |
| FR-23 | Existing sanitized failure taxonomy; provider/evidence/replay failure tests. |
| FR-24 | Allowlisted identifiers and sanitized persisted errors; static field audit and negative tests. |
| FR-25 | Read-only GET/plan/replay paths; fingerprint and zero-write tests. |
| FR-26 | Legacy manifests/Parquet/analysis/alert/user/API compatibility; legacy compatibility tests. |
| FR-27 | R2-F3 boundary and BaoStock-only admission; static second-source/failover scan. |
| FR-28 | Frozen provider/version vocabulary and extra-forbid models; model round-trip tests. |
| FR-29 | Six endpoint IDs and exact variants/units/order; endpoint contract tests. |
| FR-30 | Logical plan/request/shard/page cardinality; request completion and descriptor-count tests. |
| FR-31 | Sanitized transport observation and actual session/root/page digest binding; transport lineage tests. |
| FR-32 | Exact ordered ten-gate aggregate; `test_gate_report_requires_exact_ordered_complete_gate_aggregate`. |
| FR-33 | Injected normalization clock and separate byte/semantic replay hashes; replay clock/hash tests. |

## Manual traceability: NFR-1–NFR-19

| ID | Current code / proof |
| --- | --- |
| NFR-1 | All commands in this record use `--offline`, fakes/fixtures, and `/tmp` roots; no external execution. |
| NFR-2 | Evidence/candidate/selection transaction crash boundaries; atomicity and rollback tests. |
| NFR-3 | Held fd/inode, no-follow descriptors, fingerprints and hashes; TOCTOU/symlink tests. |
| NFR-4 | Fixed metadata/object bounds and row/schema validation; bounds and oversize tests. |
| NFR-5 | `RefreshRunLock` plus CAS winner/loser behavior; concurrent compare-create tests. |
| NFR-6 | Canonical JSON, deterministic IDs/order/hashes, actual scope identity; replay determinism tests. |
| NFR-7 | Additive models and immutable legacy fixtures; byte fingerprint/read-only compatibility tests. |
| NFR-8 | No payload/token/header/cookie/URL/path/raw exception persisted; sanitization and parser tests. |
| NFR-9 | Date/unit/factor/suspension semantic gates; source and semantic tests. |
| NFR-10 | One complete session chain, no symbol-slice fallback; production route and bounded plan tests. |
| NFR-11 | Incumbent transport retry/timeout/circuit semantics retained; provider reliability/transport tests. |
| NFR-12 | GET/plan/replay read-only fingerprints; legacy and replay zero-write tests. |
| NFR-13 | Bounded commits, review rounds, focused/full evidence, Ruff and diff checks; commit log/static record. |
| NFR-14 | Static BaoStock-only admission and no dynamic provider import; registry/static scan. |
| NFR-15 | Fixed relative layout, root fd, no-follow containment; storage/symlink tests. |
| NFR-16 | Lock-before-CAS and frozen publication order; concurrency, pointer-old-preserved, and CAS tests. |
| NFR-17 | This explicit item-by-item matrix plus named edge-case tests/static proofs; validator is not substituted. |
| NFR-18 | `qualified_fallback` rejected by R2-F2 writers; fallback test and second-source/failover scan. |
| NFR-19 | Actual login/relogin/session/root/page IDs and plan ordinals; identity/cardinality tests. |

## Manual traceability: AC-1–AC-21

| ID | Current code / proof |
| --- | --- |
| AC-1 | Static provider contract and BaoStock registry; provider contract tests. |
| AC-2 | Raw rows precede normalize; real adapter/evidence tests. |
| AC-3 | Schema/semantic allowlist; endpoint and semantic tests. |
| AC-4 | Content-addressed evidence; hash/atomic publish tests. |
| AC-5 | Evidence lineage; descriptor/transport/factor binding tests. |
| AC-6 | Offline replay; local reader and frozen-clock tests. |
| AC-7 | Corruption/nondeterminism rejection; replay mutation tests. |
| AC-8 | Immutable gate audit; exact gate aggregate tests. |
| AC-9 | Single-session selection; candidate single-provider tests. |
| AC-10 | Canonical publication lineage; selection readback/pointer-order tests. |
| AC-11 | Legacy compatibility; missing-source and byte-fingerprint tests. |
| AC-12 | Read-only status/plan/replay; GET/replay fingerprints. |
| AC-13 | Bounds/security; safe paths, limits, sanitization tests. |
| AC-14 | Crash/concurrency/TOCTOU; evidence and candidate transaction tests. |
| AC-15 | R2-F3/second source boundary; static admission and fallback tests. |
| AC-16 | Frozen models and endpoint variants; model-copy and schema tests. |
| AC-17 | Request/object/transport cardinality; completion and descriptor tests. |
| AC-18 | Ordered ten-gate aggregate; gate test. |
| AC-19 | Replay clock/storage safety; clock, CAS, no-follow tests. |
| AC-20 | Fallback and validator boundary; fallback/static and strict validator evidence. |
| AC-21 | Actual provider/query/page identity; scope and lineage tests. |

## Manual traceability: EC-1–EC-30

| ID | Current code / proof |
| --- | --- |
| EC-1 | Empty/invalid/dynamic provider IDs rejected by static registry tests. |
| EC-2 | Unknown/duplicate fields, row shape, and schema variants rejected by contract tests. |
| EC-3 | Exact requested date/session enforced by provider date-binding tests. |
| EC-4 | Bounded unknown provider code maps to sanitized unknown-protocol failure. |
| EC-5 | Transport framing/EOF/send/timeout failures use existing taxonomy; transport tests. |
| EC-6 | Object/metadata/decompression bounds; bounds tests. |
| EC-7 | Symlink, empty/repeated/traversal components rejected; safe-path/storage tests. |
| EC-8 | Manifest swap/rename/replacement fails closed under held root identity; read transaction tests. |
| EC-9 | Object delete/rename/truncate/hash mutation fails closed; reader integrity tests. |
| EC-10 | Candidate duplicate/missing/unexpected symbols/rows rejected; universe tests. |
| EC-11 | Missing/altered/oversize/wrong-hash gate report rejected; gate tests. |
| EC-12 | Same request CAS has one winner and zero loser canonical writes. |
| EC-13 | Crash before/after rename/manifest leaves complete prior state or zero owned residue. |
| EC-14 | Replay row order/timestamp is deterministic and invocation time excluded. |
| EC-15 | Legacy missing lineage/source decodes in memory without rewrite. |
| EC-16 | Provider/schema/universe/evidence lineage mismatch preserves old pointer/history. |
| EC-17 | Legal suspended blank/factor placeholder is preserved and accepted only in that case. |
| EC-18 | Active missing factor, suspended activity, and suspended index semantic failures reject. |
| EC-19 | Replay CLI rejects credential/token/header/cookie/URL/provider/path/unknown args pre-reader. |
| EC-20 | Missing roots/database/table return unavailable and remain write-free. |
| EC-21 | Missing/extra/duplicate/out-of-order plan requests fail closed. |
| EC-22 | Descriptor transport IDs/endpoint/attempt/page mismatch fails closed. |
| EC-23 | Gate outcomes missing/duplicate/reordered/extra or aggregate mismatch fail closed. |
| EC-24 | Online/replay frozen clocks and semantic/byte hash rules reject nondeterminism. |
| EC-25 | Root/ancestor symlink, inode, directory, and cross-root replacement fail closed. |
| EC-26 | CAS collision with different bytes cannot overwrite the existing winner. |
| EC-27 | Caller cannot supply provider session/root/page request identity overrides. |
| EC-28 | Login/relogin observations cannot become query completion or attempt count. |
| EC-29 | Final pages require one actual provider session and unique page identity. |
| EC-30 | Factor snapshot descriptor requires local capture identity and null provider/page identity. |

## Compatibility, threat boundary, and stop condition

Legacy manifest/object fixtures were read-only checked; missing legacy source is
decoded as BaoStock in memory, with no fixture rewrite. GET, analysis, alert,
user, history, and public JSON compatibility tests passed in the universal run.
The exact legacy tree/object byte fingerprint and read-only checks are covered by
`test_legacy_baostock_rows_and_manifests_decode_without_rewrite`,
`test_get_reads_and_legacy_manifest_reads_are_write_free`, and the candidate
legacy tests. A rollback means disabling the new evidence-backed selection
writer, returning to BaoStock-only compatibility mode, and retaining the last
known legacy canonical pointer; it does not delete evidence, rewrite fixtures,
or activate a second source.

Known Low limitation: the prescribed focused matrix has order-sensitive global
transport-test state. Its failing test passes alone and in the transport-first
ordering, but the approved exact ordering exits 1. This is recorded as a
closure blocker, not hidden by the reordered result. Existing supplemental
AKShare-related code is outside the R2-F2 canonical route and is not a second
provider/failover authorization. The threat boundary remains local typed
evidence, descriptor-bound storage, and offline fixtures; real credentials,
provider transport, network/NAS, LaunchAgent and production execution are not
tested or authorized.

`REAL PROVIDER AND PRODUCTION EXECUTION NOT AUTHORIZED`.

`R2-F3`, provider qualification, shadow/second source, failover, and any
external operation remain disabled and out of scope. Because the prescribed
focused gate exited 1, `R2-F2 OFFLINE CODE GO` is intentionally not asserted.
This document records the evidence and the blocking condition; it is not a
final production verdict and does not start R2-F3.
