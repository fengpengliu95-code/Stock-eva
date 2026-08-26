# Release 2 / R2-F3 Task14 acceptance

## Current decision: RUNNER CODE NO-GO / SHADOW WINDOW PENDING

Task14 defines an offline-only bounded TickFlow discovery canary. This record does not claim a
real provider call, provider approval, qualification, a 20-session window, or production
readiness. Independent review must change the code decision before any separately authorized
shadow window.

## Required evidence

- four exact logical requests and fixed hashes, with symbols/date/unit gates;
- official `ex_factor` provider-symbol coverage (including empty no-event arrays) and
  universe-detail schema enforcement, plus strict compact-kline numeric bounds;
- default-off and pre-client authorization/root/registry gates;
- one-attempt, four-request, timeout/size/TLS/redirect/status/schema fail-closed behavior;
- post-client CLI failures retaining sanitized `failure_class`, safe endpoint and actual
  `provider_requests`, while pre-client blocks remain zero-request;
- all evidence publication failures, including bundle collisions, are exposed only as the fixed
  `evidence_publish_error` with four completed requests and never expose a readable failed bundle;
- every streaming response and owned client closes exactly once, including iterator timeout and
  malformed/oversize/redirect paths; execute always requires 5–10 unique main-board symbols;
- fake-transport call order, parameter/header secrecy and no real socket;
- immutable isolated raw evidence bytes and retained sanitized failure audit;
- no canonical/candidate/pointer/shadow-job/failover/Tushare effect;
- focused, related, full offline, `tests/test_launchagent_assets.py`, the repository design
  validator, Ruff, compileall and diff checks;
- unchanged R2-F2 golden fixture bytes/hashes.

## Explicit remaining gate

No external authorization is present in this task. Therefore no real request is made and the
acceptance remains **RUNNER CODE NO-GO / SHADOW WINDOW PENDING** until an independent reviewer
approves the implementation and a separate user-authorized window is executed.
