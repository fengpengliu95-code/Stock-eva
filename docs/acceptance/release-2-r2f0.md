# Release 2 — R2-F0 Incident Closure

**Date:** 2026-08-12
**Status:** CODE GO / PRODUCTION REPAIR PENDING

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

This status is a code-stage decision only. The installed runtime and live
dataset have not been changed or verified, so this is not an R2-F0 production
GO.

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
| Controlled production repair | PENDING | Installed-state readback, runtime installation, live repair, and post-repair verification were deliberately not performed in the code phase. |

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

## Fresh verification

All code-stage verification below completed on the reviewed R2-F0 branch:

| Check | Result |
| --- | --- |
| Isolated Task 3 four-file pytest gate | PASS — 92 tests, 100%, exit 0 |
| Full repository pytest | PASS — 881 tests collected, 100%, exit 0 |
| `ruff check backend tests` | PASS |
| `git diff --check 14529c8 HEAD` | PASS |
| Changed production and Task 2 file format check | PASS |

The repository-wide `ruff format --check backend tests` is not universally
green: it reports 22 historical unformatted files and 113 formatted files.
The recorded starting baseline contained 23 historical unformatted files, so
this is not treated as a new R2-F0 formatting regression. In particular,
`tests/test_sector_rotation.py` was already historical-unformatted; its
two-line fixture adjustment passes diff and lint checks.

## Production work not executed

The code phase did not perform any of the following:

- read the installed pre-state;
- install or replace the runtime;
- run the supervised 2026-08-11 production repair;
- verify the live manifest, pointer, API response, or user-store state after a
  repair.

Production data repair is therefore unknown and pending. This record does not
claim production GO.

## Production continuation gate

Production work may begin only after explicit user authorization. The operator
must first read the live state and confirm that the target remains missing and
has not been repaired by another run. If the target is already repaired or the
state has otherwise changed, stop and recalculate the repair plan.

If the preconditions still hold, use the reviewed installer and the existing
guarded CLI exactly once. Do not hand-edit Parquet objects, manifests, or the
published pointer.

## Rollback and NO-GO boundary

Production acceptance requires 100% required-symbol coverage, zero recorded
failures, matching object hashes and manifest references, correct live pointer
and API readback, and unchanged protected fingerprints. If any condition is
not satisfied, the result remains NO-GO: preserve the prior published pointer
and installed release, retain the failure evidence, and do not promote the
candidate.
