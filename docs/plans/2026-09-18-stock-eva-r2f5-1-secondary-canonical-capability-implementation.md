# Stock EVA R2-F5.1 Secondary Canonical Capability Closure Implementation Plan

**Status:** STARTED / OFFLINE ONLY / R2-F5.1 NO-GO

**Design:** `2026-09-18-stock-eva-r2f5-1-secondary-canonical-capability-design.md`

## Task 1 — Freeze the evidence and readiness models — COMPLETE

Create `backend/app/market/secondary_capability.py` and
`tests/test_market_secondary_capability.py`. Begin with RED tests for strict hashes, capability
independence, TickFlow Daily-only blocking, Tushare unit admission, and zero-effect markers. Implement
only immutable v1 models and a pure evaluator. No store, CLI, API or provider import is allowed.

Gate: focused tests, Ruff, format and diff checks; independent design/quality review.

Implemented at `6ceb8cc6432d94dd4511edabe0ea97b26ff326cf`; focused compatibility and static
checks passed. Version-level independent review remains pending.

## Task 2 — Add a descriptor-safe reviewed-evidence reader — COMPLETE

Add an offline reader for an explicitly supplied reviewed capability bundle. Require regular files,
no symlinks, bounded bytes, strict JSON, canonical hash and a closed capability vocabulary. Missing,
changed or conflicting evidence returns `CONTROL_STATE_UNAVAILABLE`; it never initializes a store.

Gate: replacement, truncation, oversized, duplicate, unknown-capability and changed-during-read RED/
GREEN tests. Fingerprint input before/after every test.

The reader is absolute-path-only, rejects symlinked file or parent components, binds pathname and
open-descriptor identities before/after a bounded read, rejects duplicate JSON members and validates
the immutable bundle hash. Every failure returns `CONTROL_STATE_UNAVAILABLE`, zero requests and zero
writes. Production evidence has not been created or read.

## Task 3 — Encode the Tushare candidate contract without enabling it — COMPLETE

Freeze official daily, adjustment-factor and suspension semantics as reviewed evidence references.
The evaluator may qualify activity units and daily/suspension endpoint semantics only when their
exact evidence is present. HTTPS data transport, private raw retention, intended account entitlement,
quota, exact-session universe and factor anchor/direction remain blocking until separately proven.

Gate: a partial Tushare bundle remains ineligible and constructs no client. TickFlow remains unchanged.

The offline builder admits only the reviewed daily-bar fields, activity units and endpoint-level
suspension behavior. Adjustment-factor anchor/direction/restatement is explicitly `UNQUALIFIED`;
the remaining unproven controls stay `UNKNOWN`. State-specific reason codes preserve that distinction,
while canonical failover and effective auto-failover remain disabled. No provider module is imported,
no client is constructed and no production evidence is written.

## Task 4 — Build the full-session qualification harness offline — COMPLETE

Define the future 20-session version vector and one-session candidate requirements. Use injected fake
providers only. Test complete universe, required indexes, suspension placeholders, factor continuity,
unit conversion exactly once, source purity, evidence lineage and max_attempts=1. Do not persist a
production window.

Gate: blocking/timeout/partial/duplicate/missing-factor/missing-suspension/bad-universe/bad-index and
mixed-source scenarios all fail closed.

The offline harness invokes an injected provider exactly once, converts declared lots and
thousand-CNY units exactly once, and hashes the complete session candidate. Incomplete responses,
identity/universe/index divergence, duplicate or mixed-source bars, suspension gaps and factor gaps
are rejected without writes. The window evaluator requires exactly 20 caller-supplied expected
sessions, one frozen version vector, qualified per-session outcomes and immutable
evidence/candidate/gate/selection lineage. Passing the offline window still leaves canonical and
automatic failover disabled.

## Task 5 — Independent review and installed read-only projection — IMPLEMENTED / INSTALL PENDING

After Tasks 1–4 are independently GO, add a read-only readiness projection. It may report eligibility
to start qualification but must keep canonical failover and effective auto-failover false. Install only
after full-suite verification. Refresh LaunchAgent remains disabled during acceptance.

Local review found and closed admission gaps before installation: OHLC is now part of the complete
candidate and its hash; non-finite prices, activity values and factors are rejected; request symbol,
index and previous-factor sets are exact; observations require UTC; and descriptor-close failure is
fail-closed. The qualification harness now requires a matching eligible capability-readiness hash
before invoking the injected provider. A blocked or mismatched readiness returns zero attempts and
zero provider requests. The path-based readiness projection is descriptor-safe, read-only and keeps
canonical/automatic failover disabled. Production installation remains pending the final review gate.

## Deferred authorization gates

Real authenticated provider access begins only after HTTPS transport, retention terms, intended
account capability and quota are reviewed. A successful canary still cannot publish. Canonical
failover requires a subsequent 20-session whole-session qualification and manual failover drill.
