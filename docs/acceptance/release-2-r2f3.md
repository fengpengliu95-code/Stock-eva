# Release 2 / R2-F3 acceptance

## Current decision: CODE NO-GO

The offline Task 13 implementation is isolated and reviewable, but the real
authorized shadow window has not run. No provider request, token, account,
NAS access, production refresh, canonical pointer mutation, or automatic
failover is authorized by this record.

The status may become **CODE GO / SHADOW WINDOW PENDING** only after the
offline RED→GREEN gate, full offline suite, strict design validator, Ruff and
independent review pass. It may become **R2-F3 GO** only after an explicitly
authorized, same-contract 20-consecutive-confirmed-session report and review.

## Offline evidence

- Task 13 RED command: `uv run --extra dev pytest -q tests/test_market_shadow.py tests/test_market_get_read_only.py -k 'shadow or provider_status' tests/test_market_shadow_jobs.py tests/test_r2f2_golden_compat.py --basetemp=/tmp/stock-eva-r2f3-shadow-red`
- Fixtures are synthetic and offline-only; provider transport and credentials remain disabled.
- Canonical BaoStock roots, manifests, evidence, candidates, selections and pointer are read-only to this task.
- The acceptance state intentionally does not claim a 20-session window or production readiness.

## Review checklist

- [ ] Exact Task 13 whitelist only
- [ ] No secrets, network, NAS, production or LaunchAgent operations
- [ ] Durable outbox ordering, leases, recovery and idempotence reviewed
- [ ] Terminal UDF/authorizer/append-only and two-CAS transaction reviewed
- [ ] Calendar generation/closed dates/unknown-year behavior reviewed
- [ ] R2-F2 golden fixture unchanged
- [ ] Full offline gate and independent review complete
