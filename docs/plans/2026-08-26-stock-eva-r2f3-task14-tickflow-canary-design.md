# Stock EVA R2-F3 Task14 TickFlow Canary Design

**Author:** Codex delivery team
**Date:** 2026-08-26 (Asia/Shanghai)
**Status:** **DRAFT / RUNNER CODE NO-GO** — this is the contract for independent review; no
provider request is authorized or performed by this change.
**Scope:** Task14 only: a default-off, explicitly authorized, bounded TickFlow discovery canary
that writes isolated raw evidence. Tasks10–13 remain independent and unchanged.

## Context and decision boundary

The Task14 runner exists to test the pinned source contract without publishing data or starting a
shadow job. BaoStock remains the only canonical provider. A successful canary is still
`discovered`/`canary`, never `qualified`, and unknown units, retention, quota and suspension
semantics are discovery gaps rather than evidence of suitability.

The pinned official facts as of 2026-08-26 are OpenAPI
`https://docs.tickflow.org/zh-Hans/api-reference/openapi.json`, server
`https://api.tickflow.org`, `x-api-key`, GET `/v1/klines/batch` with `symbols`, `period=1d`,
`start_time`, `end_time`, `adjust=none`; GET `/v1/klines/ex-factors` with `symbols`, `start_time`,
`end_time`; and GET `/v1/universes/{id}` for `CN_Equity_A` and `CN_Index` with no query
parameters. Provider symbols are `600000.SH`/`000001.SZ` and are explicitly mapped to canonical
`sh.600000`/`sz.000001` only at the parser boundary. The response shapes are strict: batch `data`
is a provider-symbol map of compact column arrays, factor `data` is a provider-symbol map of
`[{timestamp, factor}]`, and universe/index `data` is `{symbols: [...]}`. Terms state that keys may
not be shared, malicious large-scale scraping is forbidden, and lawful personal/commercial use
is described subject to no unauthorized resale. Quota, retention, volume/amount units and
suspension semantics remain unconfirmed. These pages are evidence references, not approval.

## Functional requirements

- FR-1: The runner MUST be constructed only after exact true settings
  `provider_shadow_enabled` and `provider_shadow_execute_enabled` pass, isolated absolute shadow,
  evidence and control roots are validated as non-overlapping canonical/NAS roots, and registry
  state is `CANARY`. Any failed gate MUST create no directory, DB byte, client or socket.
- FR-2: The registry-bound TermsEvidence MUST include the allowlisted official URLs and
  OpenAPI/terms content hashes, as-of date, reviewer, review ID, internal-research/no-resale
  intended-use decision, retention decision, quota decision and environment-only credential mode.
  `external_authorization_id` MUST match the existing safe identifier grammar.
- FR-3: TickFlow MUST expose exactly four logical requests in this order:
  `daily_batch`, `ex_factors`, `universe(CN_Equity_A)`, `indexes(CN_Index)`. Every request MUST
  bind fixed endpoint/request/schema/unit hashes, exact trade date and UTC millisecond bounds.
  Execute symbols MUST contain 5–10 unique representative main-board `sh./sz.` symbols.
- FR-4: Daily requests MUST use `adjust=none`; source rows MUST remain source-shaped. The
  parser MUST strictly reject malformed OpenAPI response shapes, unknown factor/universe/index
  schemas, wrong dates, duplicates, missing endpoint responses and unbounded/unknown units.
  Unknown units MUST produce a typed discovery report and MUST NOT be a complete candidate.
- FR-5: Transport MUST allow at most four requests and one attempt each, use 5-second
  connect/write/pool bounds and 30-second read bound, cap every response at 8 MiB, disable
  redirects, verify TLS and use only the exact pinned HTTPS host. There is no retry or sleep.
  401/403/429/5xx, timeout, redirect, oversize and schema failures MUST be typed and sanitized.
- FR-6: Only allowlisted socket/http metadata and exact raw response bytes may be written
  to Task11's isolated immutable `ShadowEvidenceStore`; publish bundle-first and retain failure
  attempts/audit per the final-success policy. No candidate, pointer, canonical data, shadow job,
  failover or production mutation may occur.
- FR-7: CLI plan remains zero-write/zero-network. Execute requires
  `--external-authorization-id` and an explicit acknowledgement flag. The CLI MUST report
  blocked/unavailable without constructing a client when gates fail. Tushare remains pre-client
  blocked. Tests use an injected fake transport only.

## Non-functional requirements

- NFR-1: No arbitrary base URL or credential environment name is accepted; only the exact
  static contract and `STOCK_EVA_TICKFLOW_TOKEN` are permitted, and the token is never logged,
  serialized or included in exception text.
- NFR-2: Writes are confined to isolated absolute roots and use existing immutable,
  content-addressed evidence protocol. Canonical/NAS roots and R2-F2 bytes remain untouched.
- NFR-3: A canary success remains a discovery result and cannot invoke Task13 scheduler,
  normalizer, candidate writer or qualification transition.
- NFR-4: The fixed request, response, object and attempt bounds are deterministic and
  independently testable offline.

## API Contracts

```text
TickFlowCanaryRunner.execute(trade_date, symbols, *, external_authorization_id,
                             acknowledge_provider_requests) -> TickFlowCanaryReport
TickFlowCanaryReport.status = "discovered" | "blocked" | "error"
TickFlowCanaryReport.outcome = "discovery" | "unavailable" | "failure"
```

GET /v1/klines/batch; GET /v1/klines/ex-factors; GET /v1/universes/{id}

interface TickFlowCanaryRunner {
  execute(tradeDate: string, symbols: string[], authorizationId: string): TickFlowCanaryReport;
}

The report contains only provider ID, trade date, four logical endpoint names, request count,
typed sanitized failure, `units_status`, and evidence/write booleans. It never contains token,
authorization header, URL query, raw payload or arbitrary exception text.

## Acceptance Criteria

### AC-1: Pre-client gates (FR-1, NFR-2)

Given either setting false, an overlapping root, or a
  non-CANARY registry, when execute is called, then no client, network, directory or DB byte is
  created and the report is blocked.
### AC-2: Terms and authorization (FR-2, NFR-1)

Given absent/mismatched TermsEvidence or authorization ID,
  when execute is called, then it fails before credential lookup and the secret is absent from
  all output.
### AC-3: Four requests (FR-3)

Given five valid main-board symbols and fake responses, when execute is
  called, then exactly four calls occur once and in the specified order with exact paths,
  parameters and `x-api-key` only.
### AC-4: Strict source parser (FR-4)

Given malformed, wrong-date, duplicate, partial or unitless payloads,
  when parsing, then a typed discovery failure is returned and no complete candidate is emitted.
  Canonical symbols and provider symbols remain separate evidence fields; column lengths,
  finite/nonnegative numeric values, duplicate timestamps, exact UTC-day bounds, required keys,
  extra keys and empty/partial objects are all rejected.
### AC-5: Bounded transport (FR-5)

Given timeout, redirect, 401/403/429/5xx or >8 MiB responses, when the
  transport runs, then one bounded attempt is made, no retry/sleep occurs, and only a sanitized
  typed failure is reported.
### AC-6: Isolated evidence (FR-6, NFR-2)

Given four valid fake raw responses, when evidence is enabled,
  then immutable evidence bytes are readable from the isolated root only; no canonical,
  candidate, pointer or shadow job changes.
### AC-7: CLI boundary (FR-7)

Given plan mode or missing acknowledgement/authorization, when the CLI
  runs, then provider requests and writes are zero. Tushare is blocked before client creation.

## Edge Cases

### EC-1: Invalid symbols

- EC-1: Empty symbols or <5/>10/non-main-board/duplicate symbols.

### EC-2: Invalid transport and payload

- EC-2: Wrong UTC date bounds, redirect or non-TLS endpoint, missing endpoint/partial response,
malformed JSON/shape/schema drift, duplicate rows, unknown units, timeout/status/oversize,
evidence collision or interrupted publish.

### EC-3: Registry and evidence failures

- EC-3: Missing terms, state drift, interrupted bundle publication and root overlap fail closed without
creating a client or a partial readable bundle.

## Data Models

| Entity | Required fields |
| --- | --- |
| Logical request | ordinal, ID, endpoint, exact params, request/schema/unit hashes |
| Canary report | status, outcome, trade date, request count, typed failure, units status, writes |
| Raw evidence | allowlisted metadata, exact response bytes, content hash, immutable bundle |

## Out of Scope

- OS-1: No real call in this development task; no provider qualification, 20-session shadow window,
normalization, units conversion, candidate/selection/pointer/canonical write, failover, Tushare
execution, dynamic endpoint discovery, arbitrary URL/env configuration, NAS, production DB,
LaunchAgent or terms approval.
- OS-2: No automatic failover, scheduler handoff, promotion, quota/retention approval or
production credential management is part of Task14.
