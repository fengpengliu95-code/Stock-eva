# Stock EVA R2-F5.1 Secondary Canonical Capability Closure Implementation Plan

**Status:** STARTED / OFFLINE ONLY / R2-F5.1 NO-GO

**Design:** `2026-09-18-stock-eva-r2f5-1-secondary-canonical-capability-design.md`

## Task 1 — Freeze the evidence and readiness models

Create `backend/app/market/secondary_capability.py` and
`tests/test_market_secondary_capability.py`. Begin with RED tests for strict hashes, capability
independence, TickFlow Daily-only blocking, Tushare unit admission, and zero-effect markers. Implement
only immutable v1 models and a pure evaluator. No store, CLI, API or provider import is allowed.

Gate: focused tests, Ruff, format and diff checks; independent design/quality review.

## Task 2 — Add a descriptor-safe reviewed-evidence reader

Add an offline reader for an explicitly supplied reviewed capability bundle. Require regular files,
no symlinks, bounded bytes, strict JSON, canonical hash and a closed capability vocabulary. Missing,
changed or conflicting evidence returns `CONTROL_STATE_UNAVAILABLE`; it never initializes a store.

Gate: replacement, truncation, oversized, duplicate, unknown-capability and changed-during-read RED/
GREEN tests. Fingerprint input before/after every test.

## Task 3 — Encode the Tushare candidate contract without enabling it

Freeze official daily, adjustment-factor and suspension semantics as reviewed evidence references.
The evaluator may qualify activity units and daily/suspension endpoint semantics only when their
exact evidence is present. HTTPS data transport, private raw retention, intended account entitlement,
quota, exact-session universe and factor anchor/direction remain blocking until separately proven.

Gate: a partial Tushare bundle remains ineligible and constructs no client. TickFlow remains unchanged.

## Task 4 — Build the full-session qualification harness offline

Define the future 20-session version vector and one-session candidate requirements. Use injected fake
providers only. Test complete universe, required indexes, suspension placeholders, factor continuity,
unit conversion exactly once, source purity, evidence lineage and max_attempts=1. Do not persist a
production window.

Gate: blocking/timeout/partial/duplicate/missing-factor/missing-suspension/bad-universe/bad-index and
mixed-source scenarios all fail closed.

## Task 5 — Independent review and installed read-only projection

After Tasks 1–4 are independently GO, add a read-only readiness projection. It may report eligibility
to start qualification but must keep canonical failover and effective auto-failover false. Install only
after full-suite verification. Refresh LaunchAgent remains disabled during acceptance.

## Deferred authorization gates

Real authenticated provider access begins only after HTTPS transport, retention terms, intended
account capability and quota are reviewed. A successful canary still cannot publish. Canonical
failover requires a subsequent 20-session whole-session qualification and manual failover drill.

