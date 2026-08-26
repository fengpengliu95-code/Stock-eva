# Release 2 / R2-F3 Task14 acceptance

## Current decision: RUNNER CODE GO / REAL CANARY BLOCKED-CREDENTIAL

Task14 defines a bounded TickFlow discovery canary. Exact code HEAD
`81c68471f998952faa62662b17a7ab42d354614e` passed independent review with H/M/L all zero. This
record does not claim that a real provider call has occurred, that TickFlow is qualified, that a
20-session window has completed, or that production failover is ready.

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

The user authorized one minimal real TickFlow discovery canary on 2026-08-26. The authorization
is limited to five representative symbols, one confirmed trade date, four requests,
`max_attempts=1`, the pinned HTTPS origin, and isolated raw evidence only. It does not authorize
Tushare, purchases, NAS or canonical writes, a 20-session shadow window, publication, or
failover.

The controlled preflight on 2026-08-26/27 completed every pre-client gate except the credential:

- the private isolated root is
  `/Users/finlay/Library/Application Support/Stock EVA/r2f3-task14-canary-20260826`;
- control, evidence, and shadow roots are distinct, private, non-symlink directories;
- the registry is at admission state `CANARY`, state version 2;
- the immutable TermsEvidence manifest is
  `50ab53ae7f5d0e4a9f4372b901cb710a8e83fec1fd637c81fb1fd897624c4707` and its content object is
  `cfe37a166d6c5d71fcc9bfdc2ddd32524a945eacc44929417bf9b31c9c3f6817`;
- the captured official OpenAPI and terms objects have SHA-256
  `de80d0dc60df8a3d0bff491b9b29e6d615bc5e367b651005bdb773fb4e8d74e1` and
  `61c93be79963763027d615cbe3e67a4dde0d3b4ce251186977828b8fcae8f977` respectively;
- the plan reports exactly four requests and zero writes; the isolated shadow root contains zero
  files;
- the pre-execution canonical manifest snapshot is
  `052799bddd785c8a4204e0bc3352b16edaa636934d819a156f247bb8393873a7` (64,351 bytes);
- `STOCK_EVA_TICKFLOW_TOKEN` is absent from both the executing process environment and the current
  user launchd environment.

Consequently no provider endpoint was called, no authorization was consumed, and no raw provider
evidence or canonical data was written. Execution remains fail-closed until the fixed environment
credential is installed locally; the credential must never be pasted into this document, a CLI
argument, chat, logs, or source control.

## Independent code evidence

- independent fourth review at the exact HEAD: H=0, M=0, L=0;
- main-auditor full offline pytest: 1994 collected, 100% passed, exit 0;
- focused Task14 suite: 71 passed; LaunchAgent assets: 26 passed;
- strict design validator: 100/100 with zero errors or warnings;
- changed-file Ruff check and format, compileall, range `git diff --check`: passed;
- R2-F2 golden files unchanged; no real network, credential, NAS, production, canonical, or
  LaunchAgent action occurred during development or review.
