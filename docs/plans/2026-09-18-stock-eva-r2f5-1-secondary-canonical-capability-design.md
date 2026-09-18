# Stock EVA R2-F5.1 Secondary Canonical Capability Closure Design

**Author:** Codex R2-F delivery lead

**Date:** 2026-09-18 (Asia/Shanghai)

**Status:** APPROVED FOR OFFLINE IMPLEMENTATION / PRODUCTION ENABLEMENT FORBIDDEN

**Base commit:** `01c2dbbb9b095193fab87bf95a0f748ae20b27f7`

**Approval basis:** The owner authorized recommended technical plans to proceed without an
intermediate approval gate. Version GO remains owner-reviewed.

**Scope:** Establish a machine-verifiable, fail-closed canonical-capability authority for a second
provider. Tushare is the first complete-session candidate because its official endpoint documents
declare unadjusted daily bars, activity units, adjustment factors and suspension records. TickFlow
Free remains Daily-OHLC shadow-only. This version performs no provider request and cannot publish.

## Context

R2-F5 cannot start its official 20-session production clock until a secondary provider is qualified
for whole-session canonical publication and a manual failover drill is green. BaoStock is currently
serving the latest canonical session, but sustained transport timeouts prevent reliable historical
repair. Repeating BaoStock calls is not a stability strategy.

TickFlow Free has completed a 20-session historical Daily Bar shadow window, but its public contract
does not define A-share activity units, suspension/missing-row semantics, factor direction and
anchoring, an exact-session legal universe, stable quota, or raw-response retention rights. Those
unknowns prohibit canonical normalization and failover. Tushare's current official daily contract
does declare unadjusted prices, volume in lots, amount in thousand CNY, no rows during suspension,
and separate adjustment-factor and suspension endpoints. It is therefore the next candidate, not an
approved provider.

This slice creates the authority and qualification boundary only. Missing HTTPS transport proof,
account entitlement, reviewed retention permission, credential, quota or live evidence remains a
typed blocker. No blocker may be waived by configuration, fixture data or operator assertion.

## Functional Requirements

- FR-1: The system MUST represent each canonical capability independently: daily bars, activity
  units, adjustment factor, suspension semantics, exact-session universe, required indexes,
  calendar binding, quota/account entitlement, transport security, raw retention, and full-session
  qualification.
- FR-2: Each capability MUST be `QUALIFIED`, `UNQUALIFIED`, or `UNKNOWN` and MUST cite an
  immutable reviewed-evidence identity when `QUALIFIED`.
- FR-3: The authority MUST accept only provider IDs `tickflow` and `tushare`; BaoStock remains
  primary and is not promoted through this contract.
- FR-4: TickFlow Free MUST remain non-canonical. Its current Daily Bar qualification MUST NOT
  imply any other capability.
- FR-5: Tushare units MAY become qualified only from reviewed official evidence declaring
  `vol=lots` and `amount=thousand_cny`; conversion remains exactly once at normalization.
- FR-6: Tushare daily/suspension semantics MAY become qualified only when reviewed evidence binds
  the daily no-row rule to a complete `suspend_d` result for the exact session. A missing daily row
  alone MUST NOT mean suspended.
- FR-7: Adjustment factors MAY become qualified only after official semantics and an offline
  anchor/direction/restatement test contract are both present. Merely naming `adj_factor` is
  insufficient.
- FR-8: Exact-session universe MUST be derived from reviewed listing status plus exact-session
  suspension evidence and the project's main-board policy. A current symbol list MUST NOT be used
  for historical qualification.
- FR-9: Transport security MUST remain unqualified until the actual SDK/API data path is proven
  HTTPS with certificate validation and without credential-in-URL behavior.
- FR-10: Raw retention MUST remain unknown until provider terms explicitly permit the project's
  private immutable evidence retention, local backup and replay period.
- FR-11: Quota/account entitlement MUST remain unknown until the intended account proves every
  required endpoint, request budget and bounded 429 behavior. Public headline limits are not account
  evidence.
- FR-12: A provider is eligible to begin full-session qualification only when FR-5 through FR-11
  and required-index/calendar bindings are all qualified.
- FR-13: Canonical failover remains `UNQUALIFIED` until 20 consecutive full-session shadow
  sessions, immutable evidence/candidate/gate/selection lineage, and a separate manual failover
  drill all pass under one frozen version vector.
- FR-14: All readers and projections MUST be read-only and return zero provider requests and zero
  canonical writes.
- FR-15: Unknown, malformed, missing, duplicated, conflicting, expired or hash-mismatched
  evidence MUST fail closed with a bounded reason code.
- FR-16: Existing R2-F3 Daily shadow, R2-F4.0 preflight and BaoStock canonical contracts MUST
  remain backward compatible. Automatic failover MUST remain effectively false.

## Non-Functional Requirements

- NFR-1: Public models use strict, frozen, extra-forbid validation and domain-separated SHA-256.
- NFR-2: No token, payload, URL query, raw exception, SQL or absolute private path is persisted or
  returned.
- NFR-3: Offline evaluation performs no network access, database initialization, manifest,
  Parquet, pointer, queue or LaunchAgent mutation.
- NFR-4: Every decision is deterministic for identical evidence bytes and trusted clock.
- NFR-5: Tests MUST cover every qualified-to-unknown downgrade and prove no cross-capability
  inference.

## Acceptance Criteria

### AC-1: Complete authority is deterministic (FR-1, FR-2, FR-15)

Given a complete correctly hashed authority record, evaluation
  returns the exact capability matrix; any missing/hash-invalid member returns unavailable.
### AC-2: TickFlow Daily remains non-canonical (FR-4, FR-16)

Given the existing TickFlow Daily shadow-qualified status, canonical
  readiness remains blocked and effective auto-failover remains false.
### AC-3: Tushare units require reviewed evidence (FR-5)

Given reviewed Tushare daily-unit evidence, lots and thousand-CNY are admitted;
  unknown or caller-declared units are rejected.
### AC-4: Missing rows never imply suspension (FR-6, FR-8)

Given a missing daily symbol without exact-session suspension/listing proof,
  the complete-session candidate is rejected.
### AC-5: Factor names are insufficient (FR-7)

Given a factor endpoint name but no reviewed anchor/direction/restatement contract,
  adjustment capability remains unqualified.
### AC-6: Operational evidence blocks client construction (FR-9, FR-10, FR-11)

Missing HTTPS, retention or account-quota evidence independently
  blocks qualification before client construction.
### AC-7: Qualification and failover remain separate (FR-12, FR-13)

All semantic capabilities without a frozen 20-session window and manual
  drill remain insufficient for canonical failover.
### AC-8: Evaluation has zero effects (FR-14, NFR-3)

Success and every error path report provider_requests=0, writes=false and
  leave all protected fingerprints unchanged.
### AC-9: Errors are bounded (FR-15, NFR-2)

Invalid evidence emits only allowlisted reason codes and bounded hashes.

### AC-10: Existing contracts remain compatible (FR-16)

Existing R2-F3/R2-F4.0 golden tests remain byte-compatible.

## Edge Cases

- EC-1: Official documents disagree or change hash: affected capability becomes `UNKNOWN`.
- EC-2: Daily says no suspended rows but suspension endpoint is unavailable: full session fails.
- EC-3: Factor exists only for the requested date with no previous anchor: factor fails.
- EC-4: Listing/delisting date is absent or later corrected: exact-session universe fails.
- EC-5: HTTPS website exists but SDK data transport is unproven: transport remains unqualified.
- EC-6: Account has some endpoints but not `suspend_d` or `adj_factor`: entitlement fails.
- EC-7: Quota is exhausted or 429 semantics are unbounded: qualification observation fails; no
  retry loop starts.
- EC-8: Terms allow personal use but do not address raw retention/backup: retention remains
  unknown.
- EC-9: A 20-session Daily-only window is supplied as full-session proof: rejected.
- EC-10: Provider rows from two sources appear in one candidate: candidate is invalid.

## API Contracts

```typescript
type CapabilityState = "QUALIFIED" | "UNQUALIFIED" | "UNKNOWN";

interface ReviewedCapabilityEvidenceV1 {
  provider_id: "tickflow" | "tushare";
  capability: string;
  review_id: string;
  observed_at: string;
  official_document_sha256: string;
  contract_sha256: string;
  state: CapabilityState;
  evidence_sha256: string;
}

interface SecondaryCanonicalReadinessV1 {
  status: "ready" | "blocked" | "unavailable";
  provider_id: "tickflow" | "tushare";
  capability_profile: "CANONICAL_SESSION_FAILOVER_V1";
  capabilities: Record<string, CapabilityState>;
  blocked_reasons: string[];
  eligible_for_full_session_qualification: boolean;
  canonical_failover_state: "UNQUALIFIED";
  effective_auto_failover_enabled: false;
  provider_requests: 0;
  writes: false;
  readiness_sha256: string;
}
```

This version adds an offline library contract first. CLI/API exposure is deferred until the model
and strict reader pass independent review.

HTTP API: N/A — this slice intentionally adds no HTTP route.

## Data Models

| Entity | Required fields | Constraints |
| --- | --- | --- |
| `ReviewedCapabilityEvidenceV1` | provider, capability, review ID, observation time, document and contract hashes, state | immutable; official evidence only; domain hash |
| `SecondaryCanonicalReadinessV1` | provider, profile, all capability states, reasons, qualification eligibility, zero-effect markers | fail closed; canonical state literal unqualified |
| `CanonicalQualificationVersionVectorV1` | adapter, endpoint, schema, normalizer, reconciliation, calendar, universe, terms and quota hashes | every field required before a window starts |

No production database schema is added in this slice. Later persistence requires a separate reviewed
writer contract.

## Out of Scope

- OS-1: Provider credentials, registration, purchase or authenticated requests.
- OS-2: Any real TickFlow/Tushare canary or shadow request.
- OS-3: Canonical normalization, candidate publication, selection, pointer mutation or failover.
- OS-4: Symbol-level provider mixing or using TickFlow OHLC with BaoStock/Tushare factors.
- OS-5: Relaxing coverage, factor, suspension, universe, calendar or source-purity gates.
- OS-6: Starting or crediting the R2-F5 Task 20 production soak.
