# Stock EVA R2-F4.0 Capability-Gated Selection Contract Design

**Author:** Codex root architecture lead

**Date:** 2026-08-30 (Asia/Shanghai)

**Status:** SPEC DRAFT / DIRECT LEGACY TASK 14 NO-GO / IMPLEMENTATION NOT STARTED

**Scope:** One blocking, offline-only selection shield between the narrow R2-F3 Daily Bar
qualification and any future R2-F4 canonical whole-session failover.

## Context

R2-F3 proved one narrow capability: credentialless TickFlow Free historical A-share 1d OHLC can
produce a complete whole-session shadow candidate and match the existing BaoStock canonical OHLC
projection for 20 consecutive confirmed sessions. That qualification deliberately excludes
activity units, adjustment factors, suspension semantics and raw-retention semantics. Its runtime
contract fixes `publication_enabled=false` and `failover_enabled=false`.

The umbrella R2-F4 Task 14 predates that narrower qualification contract. It assumes a secondary
provider already has a complete canonical candidate carrying Daily bars, activity units, factor,
suspension and exact-session universe evidence. Current code cannot make that claim:

- canonical `ProviderId`, `DailyBar`, candidate and manifest validation remain BaoStock-only;
- `SessionSelection` explicitly rejects `qualified_fallback`;
- the R2-F3 Daily candidate contains OHLC only and cannot satisfy the R2-F2 factor/suspension gates;
- generic shadow `admission_state=qualified` has no capability dimension and therefore is not
  canonical publication authority.

Direct implementation of the old Task 14 would either weaken an existing gate or silently treat an
`UNKNOWN`/`UNQUALIFIED` semantic as proved. R2-F4.0 adds a small, isolated readiness contract that
makes that impossible. It does not implement secondary publication.

## Functional requirements

- FR-1: Explicit qualification tracks

The public model MUST distinguish `DAILY_BAR_SHADOW_QUALIFIED` from
`CANONICAL_SESSION_FAILOVER_QUALIFIED`. The former MUST NOT imply or transition automatically to
the latter.

- FR-2: Complete canonical capability matrix

A secondary canonical capability snapshot MUST expose Daily Bar, activity units, adjustment
factor, suspension semantics, exact-session universe, promoted calendar, raw-retention contract
and full-session qualification states.

- FR-3: Closed vocabulary

Every capability state is exactly `QUALIFIED`, `UNQUALIFIED` or `UNKNOWN`. Absence, corruption or
an unrecognized value is not coerced to `QUALIFIED`.

- FR-4: Current TickFlow projection

A verified R2-F3 Daily sidecar may set only Daily Bar to `QUALIFIED`. It MUST project factor as
`UNQUALIFIED` and units, suspension, universe, calendar and raw-retention semantics as `UNKNOWN`
unless a future, separately versioned authority proves them.

- FR-5: Derived canonical eligibility

`canonical_session_failover_state=QUALIFIED` is legal only when every required capability is
qualified and the snapshot is bound to reviewed provider, adapter, policy, calendar, universe,
terms and qualification evidence hashes. R2-F4.0 schema v1 intentionally supports only
`UNQUALIFIED`; a later breaking schema is required to admit a canonical secondary.

- FR-6: Deterministic policy

A pure readiness/preflight policy consumes only already-read configuration and immutable status
snapshots. It MUST NOT fetch, normalize, write, mutate an admission registry or move a pointer.

- FR-7: Primary dominance

When the primary is ready, the preflight decision is always `PRIMARY_ALLOWED`, regardless of
secondary state or configuration.

- FR-8: Fail closed on primary failure

When the primary is unavailable, R2-F4.0 always returns
`SECONDARY_BLOCKED / PRESERVE_POINTER`. Daily-only qualification, generic provider admission or an
enabled configuration flag cannot select a secondary.

- FR-9: Default-off configuration

Add `STOCK_EVA_MARKET_AUTO_FAILOVER_ENABLED=false` and
`STOCK_EVA_MARKET_PROVIDER_PRIORITY=baostock`. Priority is ordered, begins with BaoStock, contains
only `baostock`, `tickflow`, `tushare`, has no duplicates and contains no credential material.

- FR-10: Configured versus effective

Status MUST expose the configured flag separately from `effective_auto_failover_enabled`, which is
fixed false in R2-F4.0.

- FR-11: Stable blocked reasons

Status uses exactly this ordered allowlist:

```text
CONTROL_STATE_UNAVAILABLE
NO_SECONDARY_CONFIGURED
DAILY_BAR_UNQUALIFIED
DAILY_BAR_ONLY
ACTIVITY_UNITS_UNKNOWN
FACTOR_UNQUALIFIED
SUSPENSION_UNKNOWN
EXACT_SESSION_UNIVERSE_UNKNOWN
PROMOTED_CALENDAR_UNKNOWN
RAW_RETENTION_UNKNOWN
FULL_SESSION_QUALIFICATION_UNQUALIFIED
SELECTION_EXECUTION_NOT_IMPLEMENTED
```

Reasons MUST appear only in the order above, without duplicates. An unavailable source maps to
`CONTROL_STATE_UNAVAILABLE`; a primary-only priority maps to `NO_SECONDARY_CONFIGURED`; a verified
but not-yet-qualified Daily lifecycle maps to `DAILY_BAR_UNQUALIFIED`; the qualified Daily-only
profile maps to every still-missing semantic reason; and every R2-F4.0 result ends with
`SELECTION_EXECUTION_NOT_IMPLEMENTED`. Public output never exposes paths, SQL, payload rows or
exception text.

The complete mapping is frozen:

| Input | Exact ordered blocked reasons |
|---|---|
| priority is only `baostock` | `NO_SECONDARY_CONFIGURED`, `SELECTION_EXECUTION_NOT_IMPLEMENTED` |
| selected priority provider has no verified capability source | `CONTROL_STATE_UNAVAILABLE`, `SELECTION_EXECUTION_NOT_IMPLEMENTED` |
| TickFlow Daily is pending/observing/reset | `DAILY_BAR_UNQUALIFIED`, all seven missing semantic/full-session reasons, `SELECTION_EXECUTION_NOT_IMPLEMENTED` |
| TickFlow Daily is shadow-qualified | `DAILY_BAR_ONLY`, all seven missing semantic/full-session reasons, `SELECTION_EXECUTION_NOT_IMPLEMENTED` |

“Seven missing semantic/full-session reasons” means, in exact order:
`ACTIVITY_UNITS_UNKNOWN`, `FACTOR_UNQUALIFIED`, `SUSPENSION_UNKNOWN`,
`EXACT_SESSION_UNIVERSE_UNKNOWN`, `PROMOTED_CALENDAR_UNKNOWN`, `RAW_RETENTION_UNKNOWN`, followed by
`FULL_SESSION_QUALIFICATION_UNQUALIFIED`. If Tushare precedes TickFlow in priority, R2-F4.0 does not
skip the unprovable Tushare capability to try a later provider.

- FR-12: Read-only API/CLI

Add `GET /api/v1/market/failover-readiness` and `market-failover-readiness`. Both are read-only,
perform zero provider requests and report `canonical_writes=false`.

- FR-13: Missing, corrupt or locked state

The API/CLI must return a bounded `unavailable` result without initializing, migrating, repairing
or modifying any sidecar.

- FR-14: Hash binding

Capability and readiness snapshots use domain-separated canonical JSON SHA-256. Equivalent input
is byte/digest deterministic; changed state or lineage changes the digest.

- FR-15: No authority widening

R2-F4.0 MUST NOT add TickFlow/Tushare to canonical `ProviderId`, relax `SessionSelection`, add a
secondary branch to automation, or modify canonical publication.

## Non-functional requirements

- **NFR-1 — Zero provider/network access:** All R2-F4.0 tests and acceptance are offline.
- **NFR-2 — Zero canonical mutation:** No code in this slice may write DB, evidence, Parquet,
  manifest, selection or pointer state.
- **NFR-3 — Descriptor-bound read:** The Daily sidecar is read only through the existing strict
  `DailyShadowRegistryReader(verify_external=True)` and its immutable evidence/candidate graph.
- **NFR-4 — No secret enumeration:** The CLI reads only an allowlisted, non-credential runtime
  projection. It does not inspect or echo provider token variables.
- **NFR-5 — Bounded public output:** No payload rows, raw SQL/errors, token, URL, local path or
  arbitrary exception string enters API/CLI output.
- **NFR-6 — Backward compatibility:** Existing R2-F2 golden bytes/hashes/readers, R2-F3 Daily
  descriptor/evidence/candidate/report hashes, canonical Parquet/manifest and BaoStock behavior are
  unchanged.
- **NFR-7 — GET no-write:** Missing and ready API reads leave the complete inspected filesystem tree
  and relevant DB bytes unchanged.
- **NFR-8 — Minimal surface:** Production changes are limited to one new isolated domain module,
  allowlisted config, read-only API/CLI wiring and documentation/tests.

## Data models

### Capability snapshot v1

| Field | Type | Constraints |
|---|---|---|
| `provider` | literal | `tickflow` |
| `source_profile` | literal | `TICKFLOW_FREE_DAILY_BAR_OHLC_V1` |
| `source_status` | enum | `READY` or `UNAVAILABLE` |
| capability state fields | enum | `QUALIFIED`, `UNQUALIFIED` or `UNKNOWN` |
| source descriptor/vector | SHA-256 or null | Present only for verified ready source state |
| component identity hashes | SHA-256 or null | Adapter, policy, terms, calendar, universe policy and qualification evidence |
| qualification sessions | integer | `0..20` |
| canonical failover state | literal | `UNQUALIFIED` in schema v1 |
| `snapshot_sha256` | SHA-256 | Domain-separated canonical projection |

```text
SecondaryCapabilitySnapshotV1
  provider = tickflow
  source_profile = TICKFLOW_FREE_DAILY_BAR_OHLC_V1
  source_status = READY | UNAVAILABLE
  daily_bar_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  activity_units_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  adjustment_factor_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  suspension_semantics_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  exact_session_universe_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  promoted_calendar_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  raw_retention_contract_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  full_session_qualification_state = QUALIFIED | UNQUALIFIED | UNKNOWN
  source_descriptor_sha256 = sha256 | null
  source_version_vector_sha256 = sha256 | null
  source_adapter_sha256 = sha256 | null
  source_policy_sha256 = sha256 | null
  source_terms_evidence_sha256 = sha256 | null
  source_calendar_generation = safe-id | null
  source_calendar_sha256 = sha256 | null
  source_universe_policy_sha256 = sha256 | null
  source_exact_universe_sha256 = sha256 | null
  source_qualification_evidence_sha256 = sha256 | null
  source_qualification_candidate_sha256 = sha256 | null
  source_qualification_sessions = 0..20
  canonical_session_failover_state = UNQUALIFIED
  snapshot_sha256 = sha256(canonical domain-separated projection)
```

`canonical_session_failover_state` is intentionally a literal `UNQUALIFIED` in v1. A future
canonical-capability qualification must introduce a new schema/profile and direct evidence rather
than mutating the meaning of v1.

Cross-field validation is fail closed:

- `source_status=UNAVAILABLE` requires every lineage hash/generation null, sessions zero and every
  capability `UNKNOWN` except the literal canonical state `UNQUALIFIED`;
- a ready `PENDING`, `OBSERVING` or `RESET` lifecycle requires Daily Bar `UNQUALIFIED`, full-session
  qualification `UNQUALIFIED` and canonical failover `UNQUALIFIED`;
- Daily Bar `QUALIFIED` requires ready `SHADOW_QUALIFIED`, exactly 20 sessions and the descriptor,
  vector, adapter, policy, terms, calendar and universe-policy identities;
- `full_session_qualification_state` is a literal `UNQUALIFIED` in v1;
- exact-universe and qualification evidence/candidate hashes remain null in the current Daily
  projection because the strict Daily status does not grant canonical authority; a future v2 must
  require every component non-null before it can represent canonical qualification;
- illegal enum values, contradictory counts/states and missing required ready lineage are rejected,
  never normalized.

### Readiness snapshot

| Field | Type | Constraints |
|---|---|---|
| `status` | enum | `ready` or `unavailable` |
| `policy_version` | literal | `r2f4.0-selection-shield-v1` |
| configured flag | boolean | Observed configuration only |
| effective flag | literal | Always false in R2-F4.0 |
| provider priority | ordered tuple | BaoStock first, known, unique |
| eligible secondary | null | No secondary admission in schema v1 |
| blocked reasons | ordered tuple | Closed vocabulary |
| request/write counters | literals | Zero and false |
| `readiness_sha256` | SHA-256 | Domain-separated canonical projection |

```text
FailoverReadinessV1
  status = ready | unavailable
  policy_version = r2f4.0-selection-shield-v1
  primary_provider = baostock
  configured_auto_failover_enabled = bool
  effective_auto_failover_enabled = false
  provider_priority = ordered provider IDs
  eligible_secondary = null
  secondary = SecondaryCapabilitySnapshotV1 | null
  blocked_reasons = ordered closed vocabulary
  provider_requests = 0
  canonical_writes = false
  readiness_sha256 = sha256(canonical domain-separated projection)
```

### Preflight decision

| Field | Type | Constraints |
|---|---|---|
| `schema_version` | literal | `1` |
| policy version/hash | literal/SHA-256 | Exact R2-F4.0 shield identity |
| `primary_state` | enum | `READY` or `UNAVAILABLE` |
| `readiness_sha256` | SHA-256 | Exact validated readiness input |
| `action` | enum | `PRIMARY_ALLOWED` or `SECONDARY_BLOCKED` |
| `selected_provider` | literal or null | `baostock` only for primary-ready |
| `pointer_action` | enum | `PRIMARY_PUBLICATION_MAY_PROCEED` or `PRESERVE_POINTER` |
| `reason` | enum | `PRIMARY_READY` or `SECONDARY_NOT_CANONICALLY_QUALIFIED` |
| request/write fields | literals | Zero and false |
| `decision_sha256` | SHA-256 | Domain-separated canonical projection without the digest field |

```text
primary READY -> PRIMARY_ALLOWED / baostock / PRIMARY_PUBLICATION_MAY_PROCEED / PRIMARY_READY
primary UNAVAILABLE -> SECONDARY_BLOCKED / null / PRESERVE_POINTER /
                       SECONDARY_NOT_CANONICALLY_QUALIFIED
```

The decision is advisory and confers no `PublishedSelection` or canonical write capability. Its
canonical preimage is every field above except `decision_sha256`, encoded with the same sorted,
compact, UTF-8 canonical JSON plus the domain `stock-eva/r2f4.0/preflight-decision/v1`.

## API contracts

### `GET /api/v1/market/failover-readiness`

Returns `FailoverReadinessV1`. HTTP success does not mean failover eligibility; callers must inspect
`status`, `effective_auto_failover_enabled`, `eligible_secondary` and `blocked_reasons`. The route
does not initialize the Daily sidecar and never calls a provider.

### `stock-eva market-failover-readiness`

Prints the same canonical JSON projection and exits:

- `0` for a successfully proven readiness snapshot, including a safely blocked secondary;
- `1` when the control state cannot be proven;
- `2` for invalid allowlisted configuration.

There is no `--execute` option.

## Acceptance criteria

### AC-1: Narrow qualification remains narrow (FR-1, FR-2, FR-3, FR-4, FR-5)

**Given** a verified R2-F3 `SHADOW_QUALIFIED` status with factor `UNQUALIFIED` and other required
semantics `UNKNOWN`, **When** the capability snapshot is built, **Then** Daily Bar is qualified and
canonical failover remains unqualified.

### AC-2: Generic admission is insufficient (FR-1, FR-5)

**Given** a generic registry record with `admission_state=qualified`, **When** it is offered without
a canonical capability snapshot, **Then** the capability gate rejects it.

### AC-3: Configuration cannot create authority (FR-9, FR-10)

**Given** configured auto-failover true and TickFlow in priority, **When** readiness is evaluated,
**Then** effective auto-failover is false and no eligible secondary exists.

### AC-4: Primary dominance is deterministic (FR-6, FR-7)

**Given** any secondary/configuration state and a ready primary, **When** preflight runs, **Then** it
returns `PRIMARY_ALLOWED` and performs no secondary call.

### AC-5: Primary failure preserves the pointer (FR-6, FR-8)

**Given** an unavailable primary and the current Daily-only secondary, **When** preflight runs,
**Then** it returns `SECONDARY_BLOCKED / PRESERVE_POINTER` and changes no canonical state.

### AC-6: Invalid control state is unavailable (FR-3, FR-13)

**Given** missing, corrupt, short, symlinked, wrong-permission, journaled or locked Daily control
state, **When** status is read, **Then** the result is bounded unavailable with zero writes.

### AC-7: Priority is fail closed (FR-9)

**Given** an unknown, duplicate or misordered provider priority, **When** configuration is parsed,
**Then** validation fails before any state read.

### AC-8: Public output is sanitized and read only (FR-11, FR-12)

**Given** any ready or unavailable status, **When** API/CLI output is serialized, **Then** it contains
no credential, path, URL, payload, SQL or exception text and reports zero requests/writes.

### AC-9: Identity is deterministic (FR-14)

**Given** an identical snapshot input, **When** its identity is rebuilt, **Then** the digest is
identical; when any identity field is tampered, model validation fails.

### AC-10: Compatibility is preserved (FR-15)

**Given** the frozen R2-F2/R2-F3 and canonical fixtures, **When** the compatibility and full suites
run, **Then** all existing hashes/readers and fallback-rejection behavior remain green.

### AC-11: Canonical implementation is unchanged (FR-15)

**Given** the reviewed starting commit, **When** the R2-F4.0 diff is inspected, **Then** canonical
provider, candidate, selection, automation, store and dataset implementation have no diff.

### AC-12: Independent gate (FR-1, FR-2, FR-3, FR-4, FR-5, FR-6, FR-7, FR-8, FR-9, FR-10, FR-11, FR-12, FR-13, FR-14, FR-15)

**Given** the exact clean delivery commit and all required evidence, **When** independent review is
performed, **Then** High and Medium findings are zero before the slice may be called
`SELECTION CONTRACT GO / SECONDARY BLOCKED`.

## Edge cases

- EC-1: Unprovable Daily sidecar

The sidecar is absent, corrupt, locked, has a SQLite journal, wrong owner/mode or external bundle
mismatch.

- EC-2: Every Daily lifecycle state

Daily state is `PENDING`, `OBSERVING`, `RESET`, `SHADOW_QUALIFIED` or unavailable.

- EC-3: Invalid provider priority

Configuration contains whitespace, an empty segment, duplicate providers, unknown provider,
secondary before BaoStock or token-like text.

- EC-4: Enabled but ineligible

Auto-failover is configured true while priority is primary-only or Daily-only.

- EC-5: Generic provider qualification

The generic provider registry says `qualified` while the Daily capability remains narrow.

- EC-6: Identity tamper

A caller tampers with a capability state, source hash, blocked reason, priority or readiness hash.

- EC-7: Repeated read-only status

Repeated API/CLI reads run while the sidecar is unchanged; bytes, mtimes and directory inventory
remain unchanged.

## Out of scope

- OS-1: Provider execution

Secondary provider requests, credentials or authenticated TickFlow/Tushare capability work.

- OS-2: Semantic qualification

Adjustment-factor, activity-unit, suspension, raw-retention, calendar or universe qualification.

- OS-3: Canonical secondary types

New canonical provider IDs, secondary candidates, `qualified_fallback` selections or publication.

- OS-4: Canonical chain changes

Changes to Normalize, Quality Gate, immutable Parquet, SHA-256, manifest or atomic pointer logic.

- OS-5: Later R2-F4 operations

Runtime calendar generations, exact-session Universe Contract, replication outbox/restore,
operator drill or automatic/manual failover.

- OS-6: Production operations

Production installation, LaunchAgent changes, NAS access or production control writes.

## Linear follow-on

After R2-F4.0 GO, execute one subversion at a time:

1. R2-F4.1 promoted runtime calendar generations and next-year fail-closed maintenance.
2. R2-F4.2 exact-session Universe Contract with `unknown == 0` publication rule.
3. R2-F4.3 local-to-NAS replication outbox and verified temporary-root restore.
4. A separately versioned secondary canonical-capability qualification covering Daily, units,
   factor, suspension, exact universe/calendar and retention. The current Daily profile cannot be
   promoted in place.
5. Controlled manual whole-session failover, then default-off automatic policy and supervised
   forced-primary-failure drill.
6. Consolidated operations status/runbook and R2-F4 final acceptance.
