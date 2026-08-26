# Release 2 / R2-F3 acceptance

## Current decision: CODE GO / SHADOW WINDOW PENDING

The R2-F3 offline implementation at
`9cb46516ac383250d6f23ddfb6d6fe1c8ffb841b` passed the complete offline code
gate and independent review with no High, Medium or Low blockers. The real
authorized shadow window has not run. No provider request, token, account,
NAS access, production refresh, canonical pointer mutation, or automatic
failover is authorized by this record.

The status may become **R2-F3 GO** only after an explicitly authorized,
same-contract 20-consecutive-confirmed-session report and independent review.

## Offline evidence

- Task 13 RED command: `uv run --extra dev pytest -q tests/test_market_shadow.py tests/test_market_get_read_only.py -k 'shadow or provider_status' tests/test_market_shadow_jobs.py tests/test_r2f2_golden_compat.py --basetemp=/tmp/stock-eva-r2f3-shadow-red`
- Focused offline review: 247 passed.
- Related offline review: 224 passed.
- Full offline suite: 1,898 passed, plus 26 LaunchAgent asset tests, for 1,924 total.
- Strict design validator: 100/100.
- Ruff check, Ruff format check, compileall and range `git diff --check`: passed.
- Independent final decision: H=0, M=0, L=0; **CODE GO / SHADOW WINDOW PENDING**.
- Fixtures are synthetic and offline-only; provider transport and credentials remain disabled.
- Canonical BaoStock roots, manifests, evidence, candidates, selections and pointer are read-only to this task.
- The acceptance state intentionally does not claim a 20-session window or production readiness.

## Review checklist

- [x] Exact Task 10–13 whitelist only
- [x] No secrets, network, NAS, production or LaunchAgent operations
- [x] Durable outbox ordering, leases, recovery and idempotence reviewed
- [x] Terminal UDF/authorizer/append-only and two-CAS transaction reviewed
- [x] Calendar generation/closed dates/unknown-year behavior reviewed
- [x] R2-F2 golden fixture unchanged
- [x] Full offline gate and independent review complete
- [ ] Explicit external authorization recorded
- [ ] Bounded real-provider canary completed
- [ ] Same-contract 20-consecutive-confirmed-session shadow window completed
- [ ] Final independent delivery review complete
