# Stock EVA R2-F3 Task14 Free Daily Discovery Design

**Author:** Codex delivery team  
**Date:** 2026-08-27 (Asia/Shanghai)  
**Status:** **APPROVED FOR OFFLINE IMPLEMENTATION / REAL FREE CANARY NOT AUTHORIZED**  
**Reviewer:** User directive dated 2026-08-27; independent code review remains required  
**Supersedes:** The authenticated execution/default-mode portions of
`2026-08-26-stock-eva-r2f3-task14-tickflow-canary-design.md`; the prior authenticated capability
remains available only when explicitly requested.

## Context

The prior Task14 discovery path used the authenticated `https://api.tickflow.org` contract and
therefore stopped when `STOCK_EVA_TICKFLOW_TOKEN` was absent. That is the wrong default for the
current R2-F3 objective: TickFlow's official Python SDK and quickstart document a no-registration
free client, `TickFlow.free()`, at `https://free-api.tickflow.org` for historical daily K-lines,
instrument, exchange and universe metadata. Free service does not provide real-time quotes or
minute K-lines.

The official `tickflow==0.1.24` factory defaults to three retries and prints a free-tier notice.
Stock EVA requires one total attempt, deterministic machine-readable CLI output and the existing
bounded HTTPS transport. The adapter therefore uses the official factory only as the pinned Free
capability initializer, with retries disabled and the exact free origin supplied explicitly; all
network I/O remains inside Stock EVA's injected, byte-bounded transport. This avoids trusting SDK
defaults, dynamic base-URL environment variables, cache writes or hidden retries.

Free Daily discovery is intentionally narrower than provider qualification. It can prove that one
fixed A-share historical `1d` response and public metadata are reachable and structurally valid.
It cannot prove adjustment-factor semantics, suspension semantics, volume/amount units,
rate-limit/quota, raw-retention rights, complete-session coverage or long-term availability.

## Functional Requirements

- FR-1: The adapter MUST expose exactly two capability modes:
  `FREE_DAILY_DISCOVERY` and `AUTHENTICATED_DISCOVERY`. Free mode MUST be the CLI/default execution
  mode when authenticated capability is not explicitly requested. Selecting Free mode MUST NOT
  read `STOCK_EVA_TICKFLOW_TOKEN`, `TICKFLOW_API_KEY` or any credential value.
- FR-2: `AUTHENTICATED_DISCOVERY` MUST remain explicit. Only that mode MAY read the fixed
  `STOCK_EVA_TICKFLOW_TOKEN` environment variable and MUST fail before client construction when the
  credential is absent. Free mode MUST NOT add `x-api-key`, `Authorization`, cookies or any other
  credential header.
- FR-3: The production Free initializer MUST call the pinned official `TickFlow.free()` factory
  from `tickflow==0.1.24` with exact base URL `https://free-api.tickflow.org`, `max_retries=0`, fixed
  timeouts and a no-write cache path. It MUST reject a non-Free base URL or a non-empty SDK API key,
  suppress the SDK's human notice from CLI output, close the SDK client exactly once and perform no
  provider request through the SDK client. The production probe MUST run in an isolated child with
  a closed credential-free environment; the actual Free HTTP transport MUST use `trust_env=false`.
- FR-3a: Free execution MUST read a separately reviewed, persisted Free-capability descriptor.
  Its TermsEvidence `contract_version` MUST bind the Free adapter, endpoint, source-schema, unit,
  SDK-version and pinned-wheel hashes. The authenticated provider record hashes MUST NOT be used
  as proof of the Free request graph. A missing or mismatched Free review MUST fail before SDK or
  HTTP client construction with zero requests and writes.
- FR-4: A Free plan MUST contain exactly four logical requests, once each and in this order:
  `connectivity` (`GET /v1/exchanges`), `instrument_metadata`
  (`POST /v1/instruments` for the exact fixed sample `600000.SH`, `600519.SH`, `601318.SH`,
  `000001.SZ`, `000002.SZ`), `universe_metadata`
  (`GET /v1/universes/CN_Equity_A`) and `historical_daily_1d`
  (`GET /v1/klines/batch` for the same exact five symbols, `period=1d`, exact requested UTC-day
  bounds and `adjust=none`). No caller-supplied symbol, path, origin, period or adjustment mode may
  widen this plan.
- FR-5: The Free parser MUST require a non-empty exchange list containing `SH` and `SZ`, exactly
  one instrument record for each fixed sample symbol, an exact `CN_Equity_A` universe detail with
  internally consistent symbol count and membership of all five fixed samples, and exactly one
  valid unadjusted daily row per fixed symbol for the requested date. Wrong dates, duplicates,
  unknown fields, partial compact arrays, non-finite or impossible OHLC values, negative activity
  and missing endpoints MUST fail closed.
- FR-6: Free transport MUST use exact TLS origin `https://free-api.tickflow.org`, no redirects,
  5-second connect/write/pool bounds, a 30-second read bound, at most 8 MiB per response, at most
  four requests and `max_attempts=1`. Timeout, connection, 4xx, 429, 5xx, redirect, host mismatch,
  oversize, malformed JSON and schema drift MUST return only allowlisted sanitized metadata.
- FR-7: Free discovery execute MUST be zero-write on every success and failure path: no registry
  initialization or mutation, database write, raw evidence bundle, Parquet, manifest, candidate,
  selection, pointer, cache, temporary payload, scheduler job or LaunchAgent state. Existing
  registry/TermsEvidence MAY be read but MUST NOT be created or updated.
- FR-8: The report MUST record `provider_mode=FREE_DAILY_DISCOVERY` and a frozen capability map.
  Connectivity, instrument metadata, universe metadata and historical daily `1d` are
  `DISCOVERED` only after all four responses validate. Real-time quote and minute K-line are always
  `FORBIDDEN`; adjustment factor is `UNQUALIFIED`; suspension semantics, units,
  rate-limit/quota and raw-retention contract are `UNKNOWN`. These states MUST NOT be inferred from
  example values or successful connectivity.
- FR-9: Free success MUST remain `outcome=discovery`, `complete_candidate=false`,
  `writes=false` and `starts_shadow=false`. It MUST NOT call normalization, reconciliation,
  provider transition, Task13 scheduler, qualification or failover code.
- FR-10: CLI plan mode MUST make zero provider requests and writes. Execute MUST still require a
  safe `external_authorization_id` plus explicit provider-request acknowledgement. Offline
  implementation and tests MUST NOT consume the future real-canary authorization.
- FR-11: Daily Bar Shadow qualification MUST be represented separately from adjustment-factor
  qualification. Free Daily discovery MAY advance neither. Unknown or unqualified factor evidence
  MUST NOT invalidate the structural Free Daily report, but MUST keep any complete-session or
  canonical candidate gate closed.

## Non-Functional Requirements

- NFR-1: All Free-mode control flow MUST be deterministic and testable using injected fake SDK and
  HTTP factories. Tests MUST prove no real socket, DNS, credential, cache or filesystem writer is
  reached.
- NFR-2: The direct SDK dependency MUST be pinned exactly to `tickflow==0.1.24`; its version,
  factory signature, Free origin, retry mapping and close behavior MUST have offline contract
  tests. A version change requires a new contract hash and review.
- NFR-3: Free-mode public output MUST be valid single-object JSON and contain no SDK notice, URL
  query, raw response, header, token, arbitrary exception text or provider payload.
- NFR-3a: Free CLI settings MUST use a closed allowlist of non-credential environment names and
  MUST NOT instantiate a general settings source that enumerates the process environment or `.env`.
- NFR-4: Existing BaoStock canonical, R2-F2 golden fixtures, Tasks10-13 lifecycle, Tushare block,
  authenticated capability and automatic-failover default-off guarantees MUST remain unchanged.
- NFR-5: Every response and every owned SDK/HTTP client MUST close exactly once on success,
  parse failure, timeout, status error and cancellation.

## Acceptance Criteria

### AC-1: Free selection never reads credentials (FR-1, FR-2, NFR-1)

Given no authenticated capability request and a getenv spy that fails on either credential name,
when Free planning and execution run with injected fakes, then mode is `FREE_DAILY_DISCOVERY`, the
spy is never called, no credential header is present and the four Free requests complete once.

### AC-2: Explicit authenticated selection retains the token gate (FR-1, FR-2)

Given `AUTHENTICATED_DISCOVERY` is explicitly selected and the fixed Stock EVA token is absent,
when execute is called, then it fails before SDK/HTTP client construction with zero requests and
zero writes. Free mode remains available without changing authenticated policy.

### AC-3: Official Free initializer is pinned and silent (FR-3, NFR-2, NFR-3)

Given a fake official SDK factory and captured stdout/stderr, when the production Free factory is
exercised offline, then it invokes `TickFlow.free()` with the exact Free origin,
`max_retries=0`, fixed timeout and no-write cache, validates empty API key and exact base URL,
emits no notice and closes once without an SDK network call.

### AC-3a: Free descriptor and settings are capability-bound (FR-3a, NFR-3a)

Given an authenticated-only registry descriptor, stale Free contract version, environment
enumeration spy or either credential name, when Free execute is selected, then it fails before
client construction or proves that only the closed non-credential settings allowlist was read.
Only TermsEvidence bound to the exact Free descriptor hash permits the four requests.

### AC-4: Exact Free request graph (FR-4, FR-6)

Given valid fake responses for the requested historical date, when the Free canary executes, then
exactly four calls occur in the specified order with exact methods, paths and parameters, one
attempt each, no sleep and no auth header. Any fifth, minute, quote, factor, dynamic-symbol or
different-origin request is impossible.

### AC-5: Strict Free parser (FR-5, FR-8)

Given a missing SH/SZ exchange, wrong instrument, inconsistent universe, missing fixed symbol,
wrong daily date, duplicate row, partial array, unexpected field or invalid number, when parsing,
then a typed sanitized discovery failure is returned and every capability remains non-qualified.

### AC-6: Zero-write success and failure (FR-7, FR-9, NFR-4)

Given filesystem, registry, evidence, candidate, scheduler and canonical writer spies, when Free
execute succeeds or fails at each of the four endpoints, then no writer is called, no file or DB
byte changes, and the report always has `writes=false`, `writes_evidence=false`,
`writes_canonical=false`, `starts_shadow=false`.

### AC-7: Frozen capability states (FR-8, FR-11)

Given four valid Free responses, when the report is produced, then only connectivity, instrument,
universe and historical daily `1d` are `DISCOVERED`; quote/minute are `FORBIDDEN`, adjustment
factor is `UNQUALIFIED`, and suspension, units, quota and raw retention are `UNKNOWN`.

### AC-8: Transport failures are one-attempt and sanitized (FR-6, NFR-5)

Given timeout, redirect, wrong host, malformed JSON, 401/403/404/429/5xx or oversize response at
any ordinal, when Free execute runs, then actual request count is 1-4, no retry/sleep occurs, all
opened resources close exactly once, and CLI output contains only allowlisted failure and endpoint
identity.

### AC-9: No automatic lifecycle transition (FR-9, FR-11)

Given a fully valid Free response set and registry state `CANARY`, when execute completes, then the
registry bytes/state version are unchanged, no shadow job/window exists, no candidate is emitted
and Daily Bar plus adjustment-factor qualification remain false.

### AC-10: CLI authorization boundary (FR-10, NFR-3)

Given Free plan mode or execute without acknowledgement/authorization, when the CLI runs, then it
reports zero requests/writes and constructs no client. Given explicit future authorization and
valid fakes, execute returns one sanitized JSON object without SDK notice or payload fields.

## Edge Cases and Error Scenarios

- EC-1: Either SDK or HTTP factory raises before its first request -> typed unavailable, zero
  request, zero write, any created client closed once.
- EC-2: `TICKFLOW_FREE_BASE_URL`, `TICKFLOW_BASE_URL`, `TICKFLOW_API_KEY` or caller input attempts to
  override the Free contract -> ignored or rejected before network; never serialized.
- EC-3: Free endpoint responds with auth challenge, redirect, 429 or capability-not-available ->
  fail closed once; do not fall back to authenticated origin and do not read a token.
- EC-4: Instrument/universe daily metadata is empty, duplicated, cross-market or omits any fixed
  sample symbol -> structural failure; do not substitute another symbol.
- EC-5: Daily data contains more than the requested exact-date row, adjusted/unknown fields, empty
  arrays, mismatched array lengths or values outside the UTC day -> structural failure.
- EC-6: SDK initialization prints, opens a cache, exposes a non-empty key, uses an unexpected
  version or cannot close -> pre-request failure.
- EC-7: Any success or failure path attempts evidence/canonical/control persistence -> test fails
  and the implementation is NO-GO.

## API Contracts

The Free request surface is closed and ordered: `GET /v1/exchanges`,
`POST /v1/instruments`, `GET /v1/universes/CN_Equity_A`, then
`GET /v1/klines/batch`. No other method/path pair is permitted.

```typescript
type TickFlowCapabilityMode = "FREE_DAILY_DISCOVERY" | "AUTHENTICATED_DISCOVERY";
type CapabilityState = "DISCOVERED" | "UNQUALIFIED" | "UNKNOWN" | "FORBIDDEN";

interface TickFlowFreePlan {
  provider: "tickflow";
  providerMode: "FREE_DAILY_DISCOVERY";
  tradeDate: string;
  fixedSymbols: readonly [
    "600000.SH",
    "600519.SH",
    "601318.SH",
    "000001.SZ",
    "000002.SZ"
  ];
  requestCount: 4;
  endpoints: [
    "connectivity",
    "instrument_metadata",
    "universe_metadata",
    "historical_daily_1d"
  ];
}

interface TickFlowCapabilityRecord {
  connectivity: CapabilityState;
  instrumentMetadata: CapabilityState;
  universeMetadata: CapabilityState;
  historicalDaily1d: CapabilityState;
  realtimeQuote: CapabilityState;
  minuteKline: CapabilityState;
  adjustmentFactor: CapabilityState;
  suspensionSemantics: CapabilityState;
  units: CapabilityState;
  rateLimitQuota: CapabilityState;
  rawRetentionContract: CapabilityState;
}

interface TickFlowFreeCanaryReport {
  status: "discovered" | "blocked" | "error";
  outcome: "discovery" | "unavailable" | "failure";
  provider: "tickflow";
  providerMode: "FREE_DAILY_DISCOVERY";
  tradeDate: string;
  fixedSymbols: readonly [
    "600000.SH",
    "600519.SH",
    "601318.SH",
    "000001.SZ",
    "000002.SZ"
  ];
  requestCount: number;
  capabilities: TickFlowCapabilityRecord;
  completeCandidate: false;
  dailyBarQualified: false;
  adjustmentFactorQualified: false;
  writes: false;
  writesEvidence: false;
  writesCanonical: false;
  startsShadow: false;
}
```

Production CLI contract:

```text
market-provider-canary --provider tickflow --capability free-daily \
  --date YYYY-MM-DD [--execute --external-authorization-id ID \
  --acknowledge-provider-requests]
```

`--capability authenticated` is the only route to the existing token-gated capability.

## Data Models

| Entity | Field | Type | Constraints |
| --- | --- | --- | --- |
| Free plan | `provider_mode` | enum | exactly `FREE_DAILY_DISCOVERY` |
| Free plan | `trade_date` | date | explicit confirmed historical date |
| Free plan | `fixed_symbols` | literal tuple | exactly `600000.SH`, `600519.SH`, `601318.SH`, `000001.SZ`, `000002.SZ` |
| Free plan | `requests` | tuple | four fixed ordered logical requests |
| Free descriptor | `contract_version` | literal | embeds exact Free descriptor SHA-256 |
| Capability record | each capability | enum | fixed state vocabulary; no free text |
| Free report | `request_count` | integer | 0-4 actual requests |
| Free report | qualification flags | literal false | cannot be changed by discovery |
| Free report | write flags | literal false | success and failure |
| Free report | failure metadata | optional allowlisted token | no provider text, URL or payload |

N/A - This Task14 revision adds no database table, migration, persisted evidence model or pointer.

## Out of Scope

- OS-1: No real Free or authenticated provider request is authorized by implementation work. A
  separate one-time Free discovery authorization is required after offline GREEN and review.
- OS-2: No real-time quote, minute K-line, WebSocket, depth, financial, factor or authenticated
  endpoint call is part of Free discovery.
- OS-3: No unit conversion, suspension inference, factor normalization, full-universe daily fetch,
  Daily Bar Shadow qualification, adjustment-factor qualification or 20-session window.
- OS-4: No raw evidence retention, database/Parquet/manifest/pointer write, candidate, scheduler,
  provider-state transition, publication, failover, NAS, production DB or LaunchAgent change.
- OS-5: No Tushare, BaoStock, AKShare or other-provider behavior change and no symbol-level mixing.
- OS-6: No provider quota, retention, availability or legal-use claim may be inferred from a
  successful Free canary or SDK documentation example.
