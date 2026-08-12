# Release 2 — R2-F0 Incident Closure

**Date:** 2026-08-12
**Status:** CODE GO / CONTROLLED REPAIR FAILED / RUNTIME ROLLED BACK / PRODUCTION NO-GO

## Scope and evidence boundary

This acceptance record closes the R2-F0 code gate for the 2026-08-11
all-main-board publication incident. The regression fixture contains two
ordinary suspended stocks without adjustment factors, one active stock with a
valid factor, and the two required summary indexes. Production code remains
symbol-agnostic; no symbol allowlist or coverage-gate weakening was added.

R2-F2 has not yet retained provider raw evidence. The fixture therefore
reproduces only the observed normalized failure shape: parseable zero OHLC
values, blank suspended activity, and absent suspension factors. It is not a
byte-for-byte copy of a live provider payload, and this record makes no raw
payload claim.

The code gate remains GO. The one authorized production repair was attempted
exactly once and failed closed during provider fetch with a typed
`transport_timeout`. No candidate dataset was published. The installed runtime
was then rolled back to the prior reviewed release. This is not an R2-F0
production GO.

## Code acceptance gates

| Gate | Result | Evidence |
| --- | --- | --- |
| Incident regression starts RED | PASS | Task 0 produced `F..`; the intended failure carried `invalid_publication_bar_quality:sh.600984:invalid_suspended_placeholder`, while both negative-boundary tests passed. |
| Suspension semantics | PASS | A suspended stock may omit its adjustment factor; an active stock may not. A legal suspended row must be a stock with zero volume and amount, partial quality, and exactly the suspended-placeholder issue. |
| Invalid placeholders fail closed | PASS | A suspended index and a suspended row with non-zero activity are rejected. Active-stock factor enforcement remains intact. |
| Publication immutability on failure | PASS | Temporary-store fingerprints confirmed that a failed candidate preserved the existing pointer, raw manifest bytes, and all manifest-referenced immutable objects. |
| Structured failure contract | PASS | Public results expose `failure_stage`, `failure_class`, and `retryable`; the legacy `provider_error` quality issue remains compatible. CLI output, logs, and public model serialization are sanitized. |
| Persistence and retry behavior | PASS | `refresh_runs` adds three columns through a compatibility migration with round-trip coverage; scheduler retries are driven by the structured retryability field. |
| Main-board factor bootstrap failures | PASS | Transport and semantic failures are classified without collapsing the publication gates. |
| Install-time schema compatibility | PASS | The first candidate install exposed a legacy 14-column `refresh_runs` database to the new 17-column read path and `/market/status` returned 503. The runtime was rolled back before repair. Commit `b2a860a` adds a fail-closed, pre-handoff writer migration; independent review returned `INSTALL GO`, and the second install migrated only the three nullable failure fields while retaining 284 historical runs. |
| Controlled production repair | FAIL SAFE | The only authorized 2026-08-11 guarded refresh produced run `07d5d268c5f3-be43af03f909`: `status=error`, `failure_stage=fetch`, `failure_class=transport_timeout`, `retryable=true`, and 0/0 fetched. It was not retried. The pointer and immutable dataset remained unchanged. |
| Production rollback | PASS | The same reviewed installer restored exact release `75c64e4364fadbeab8a1e4a964bfa208e91a7ff6`; five LaunchAgents, API, workspace and storage returned ready. Candidate release `b2a860a` and the failed-run evidence were retained. |

## Implemented rules

The normalization and publication contract now distinguishes a legal
non-trading suspension from an active row that lacks a factor:

- A suspended stock without an adjustment factor is a legal candidate only
  when it is marked non-trading/suspended, has zero volume and amount, carries
  partial quality, and has exactly the suspended-placeholder issue.
- An active stock without an adjustment factor remains a hard publication
  failure.
- A suspended index, non-zero suspended activity, an unexpected issue set, or
  any otherwise invalid placeholder remains a hard failure.
- Coverage and reconciliation gates remain unchanged, and there is no
  production-symbol special case.

Provider and publication failures now have a stable public classification:
`failure_stage`, `failure_class`, and `retryable`. Transport failures retain
the legacy `provider_error` quality issue during the compatibility period.
Scheduler retry decisions use the structured retryability value. Public CLI,
log, and model surfaces are sanitized, and the `refresh_runs` compatibility
migration persists and restores the three new fields. Main-board factor
bootstrap distinguishes transport failures from semantic failures.

## Code history

| Commit | Purpose |
| --- | --- |
| `5b170fc` | Reproduce the suspended-publication incident and freeze RED/immutability evidence. |
| `2bd4e50` | Correct suspended-row normalization and publication validation. |
| `1db5403` | Add structured provider/publication failure classification and persistence. |
| `377e157` | Align the sector-rotation suspended fixture with the stricter publication contract. |
| `b2a860a` | Migrate the market control schema before runtime handoff and fail closed on migration errors. |

## Fresh verification

All code-stage verification below completed on the reviewed R2-F0 branch:

| Check | Result |
| --- | --- |
| Isolated Task 3 four-file pytest gate | PASS — 92 tests, 100%, exit 0 |
| Full repository pytest | PASS — 881 tests collected, 100%, exit 0 |
| `ruff check backend tests` | PASS |
| `git diff --check 14529c8 HEAD` | PASS |
| Changed production and Task 2 file format check | PASS |
| Install migration and failure tests | PASS — 55 tests in the combined market-failure and LaunchAgent gate; independent focused review passed 10 tests |
| Install script syntax | PASS — `bash -n` |

The repository-wide `ruff format --check backend tests` is not universally
green: it reports 22 historical unformatted files and 113 formatted files.
The recorded starting baseline contained 23 historical unformatted files, so
this is not treated as a new R2-F0 formatting regression. In particular,
`tests/test_sector_rotation.py` was already historical-unformatted; its
two-line fixture adjustment passes diff and lint checks.

## Controlled production execution

The production pre-state confirmed `latest_expected_session=2026-08-11`,
`published_as_of=2026-08-10`, 271 manifest entries, and no 2026-08-11
partition. The latest pre-existing attempt had fetched 3,195/3,195 symbols and
was blocked only by the two legal suspended-placeholder rows reproduced by the
regression fixture. The published pointer referenced ready run
`cbd8d7c8d23c-b9a34e19a586` for 2026-08-10.

The first installation of the code-gated runtime found an unhandled deployment
compatibility condition: the production `refresh_runs` table still had 14
columns, while the new read path selected 17. `/api/v1/market/status` failed
closed with `market_control_read_failed`. No refresh ran. The prior runtime was
restored, after which the endpoint returned 200. A local RED-to-GREEN repair
added a candidate-release migration before runtime handoff, with rollback,
idempotency, legacy-row and sanitized-failure coverage. Independent review
authorized only the exact corrected release `b2a860a` for reinstallation.

The corrected installer reported `destination_newer` and `copied_bytes=0`,
then migrated only `failure_stage`, `failure_class` and `retryable`. Historical
rows retained NULL for these fields, the run count stayed 284, the API returned
200, and all install-after checks passed before the data repair began.

The existing guarded CLI was then executed once from the installed runtime and
production config root:

```text
python -m backend.app.cli refresh --date 2026-08-11 \
  --all-main-board --execute-all-main-board
```

It ended with a provider fetch timeout and persisted exactly one additional
sanitized audit row, `07d5d268c5f3-be43af03f909`. No second attempt was made,
and no manual Parquet, manifest, pointer or DuckDB publication edit was made.

## Post-failure protection and rollback evidence

After the failed repair and again after runtime rollback:

- `refresh_runs` contained exactly 285 rows and retained the one new typed
  failure row; the schema remained at 17 columns.
- The published pointer remained run `cbd8d7c8d23c-b9a34e19a586`, date
  2026-08-10.
- The manifest remained SHA-256
  `052799bddd785c8a4204e0bc3352b16edaa636934d819a156f247bb8393873a7`,
  generation `generation-6c48729bea29429d86891b102f0c8e52`, with 271
  entries, latest date 2026-08-10 and no 2026-08-11 partition. Every referenced
  object matched its manifest SHA-256.
- The user database remained SHA-256
  `8adba0364d399a56270d3a051db5db8d04fd0243cdecb32bdb01a81384e57e32`.
  The source and installed config hashes also remained unchanged.
- The installer again reported `destination_newer`, `copied_bytes=0`; no NAS
  data was copied over the local dataset.
- Runtime `current` and `RELEASE.json` returned to exact release
  `75c64e4364fadbeab8a1e4a964bfa208e91a7ff6`. Candidate release `b2a860a`
  remained available for audit. All five LaunchAgents were loaded, API,
  workspace and storage were ready, and `/api/v1/market/status` returned 200
  with expected 2026-08-11, published 2026-08-10 and delayed state.

## Continuation gate

The first production attempt authorized for this incident was consumed. A
second window was authorized later on 2026-08-12, but its mandatory read-only
provider health gate failed before candidate installation or any real repair.
That window therefore consumed no guarded refresh attempt and created no
additional repair run. Do not retry automatically or manually, and do not edit
the manifest, objects, pointer or control database to simulate success. A later
attempt requires new user authorization, a fresh live pre-state, confirmation
that the target remains missing, and a separately reviewed provider-availability
window. R2-F1 must not start while this R2-F0 production gate is NO-GO.

### Second authorized window: stopped at provider health gate

At 18:26-18:36 Asia/Shanghai, live state had advanced independently of the
first repair attempt:

- the expected latest session was 2026-08-12 while the pointer still referenced
  2026-08-10;
- the scheduled 18:10 run had added one 2026-08-12 provider-error row, bringing
  `refresh_runs` to 286, and had scheduled an 18:40 retry;
- the manifest was still SHA-256
  `052799bddd785c8a4204e0bc3352b16edaa636934d819a156f247bb8393873a7`,
  generation `generation-6c48729bea29429d86891b102f0c8e52`, with 271
  entries and neither 2026-08-11 nor 2026-08-12 published.

A no-write 2026-08-12 universe inspection succeeded once with 3,195 expected
symbols and five metadata requests. The immediately following 2026-08-11
inspection failed with `universe_inspection_failed` after the SDK reported a
response-receive error. Both probes left the control database and manifest byte
hashes unchanged.

Independent review allowed only a reversible freeze of the refresh LaunchAgent
to prevent the 18:40 scheduled retry from racing the controlled window. Before
the freeze, there was no active refresh process. Only the refresh agent was
unloaded; API, web, calendar and backup remained loaded, API/workspace/market
returned 200, `refresh_runs` stayed at 286, and protected hashes did not change.

Local read-only evidence showed complete 2026-08-11 factor snapshots for 3,193
stocks with no missing back factor. Nevertheless, the first required full-chain
canary for that exact date failed after 63.463 seconds with typed
`fetch/transport_timeout/retryable=true`. It covered the universe plus a normal
stock, both incident suspensions, both required indexes and factor endpoints.
No production refresh ran, no candidate release was installed, and the run
count, control database, manifest and factor-cache hashes remained unchanged.

The provider health policy required three consistent successes over at least
five minutes, so the first-round failure closed the window. The refresh
LaunchAgent was restored from its unchanged plist; it was loaded again with the
run count still exactly 286. The production runtime remained the prior release
`75c64e4364fadbeab8a1e4a964bfa208e91a7ff6` throughout this second window.

## Rollback and NO-GO boundary

Production acceptance requires 100% required-symbol coverage, zero recorded
failures, matching object hashes and manifest references, correct live pointer
and API readback, and unchanged protected fingerprints. The authorized attempt
did not satisfy these conditions, so R2-F0 remains production NO-GO. The prior
published pointer and installed release were preserved, the failed-run evidence
was retained, and the candidate was not promoted.
