# Release 2 — R2-F0 Incident Closure

**Date:** 2026-08-12
**Status:** NO-GO — implementation not run

## Scope

This acceptance record freezes the production-shaped regression for the
2026-08-11 all-main-board publication incident. The captured fixture contains
two ordinary suspended stocks without adjustment factors, one active stock
with a valid factor, and the two required summary indexes. It does not
special-case a production symbol in application code.

R2-F2 has not yet retained provider raw evidence. Accordingly, this fixture
reproduces the observed normalized failure shape: parseable zero OHLC values,
blank suspended activity, and absent suspension factors. It does not claim to
be a byte-for-byte copy of the live provider payload. The two observed symbols
are fixture identities only; the production rule must remain symbol-agnostic.

The R2-F0 implementation gate is limited to allowing a legal suspended
placeholder without an adjustment factor to participate in a complete
publication, while an active stock without an adjustment factor remains a
hard publication failure.

## Protected paths and forbidden boundaries

- Protect the currently published pointer, manifest, and immutable Parquet
  object bytes when a candidate fails validation.
- Do not change provider credentials, live-provider configuration, production
  datasets, NAS data, LaunchAgents, or scheduler installation during this
  task.
- Do not weaken the active-stock adjustment-factor gate or publish a partial
  candidate.

## Planned verification

```bash
uv run --extra dev pytest -q tests/test_market_reliability.py -k \
  'suspended_rows_without_factor or active_row_without_factor or incident_candidate' \
  --basetemp=/tmp/stock-eva-r2f0-incident-red
uv run --extra dev ruff check tests/test_market_reliability.py
uv run --extra dev ruff format --check tests/test_market_reliability.py
git diff --check
```

## Actual RED evidence

The regression command was run before any production-code change. Result:
`F..` (one intended failure, two negative-boundary tests passing).

- `test_suspended_rows_without_factor_can_form_a_complete_publication` failed
  as intended: the candidate carried
  `invalid_publication_bar_quality:sh.600984:invalid_suspended_placeholder`
  (and the analogous suspended-row rejection), so its expected complete
  publication could not be reached.
- `test_active_row_without_factor_still_blocks_publication` passed: an active
  stock missing an adjustment factor still produces `missing_adjust_factor`.
- `test_failed_incident_candidate_preserves_existing_pointer_and_objects`
  passed: a deliberately failed candidate leaves the existing pointer,
  manifest bytes, and every manifest-referenced immutable object byte-for-byte
  unchanged in a temporary dataset.

This is a red-phase test record only. No R2-F0 implementation, production
canary, migration, or operational rollout has been executed.
