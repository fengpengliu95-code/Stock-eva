# Data provider ledger

This is a discovery and capability ledger for the R2-F3/R2-F4.0 shadow lanes (as of
2026-08-30). It is not runtime provider access, provider endorsement, permission to
request data, or canonical failover authority. BaoStock remains the only canonical
provider. TickFlow has only the narrow Daily Bar shadow qualification recorded below;
the R2-F4.0 effective failover setting remains false.

| Provider | Official records | Disposition |
| --- | --- | --- |
| SSE / SZSE calendar notices | Exact reviewed official notice URLs and body SHA-256 are staged per year; allowed hosts are `www.sse.com.cn`, `www.szse.cn` and `investor.szse.cn` | R2-F4.1 calendar authority only. Exactly one reviewed SSE and one reviewed SZSE complete-year schedule must agree. Retrieval is bounded to the staged locations and cannot grant market-data or secondary-provider capability. |
| TickFlow | [OpenAPI](https://docs.tickflow.org/zh-Hans/api-reference/openapi.json), server `https://api.tickflow.org`, [API overview](https://docs.tickflow.org/zh-Hans/api-reference/introduction), [FAQ](https://docs.tickflow.org/zh-Hans/faq), [SDK practices](https://docs.tickflow.org/zh-Hans/sdk/python-best-practices), [terms](https://tickflow.org/legal/terms-of-service.md) | `DAILY_BAR_SHADOW_QUALIFIED` only for credentialless Free historical unadjusted 1d OHLC, bound to the reviewed R2-F3 sidecar. It is not canonical capability or failover qualification: `adjustment_factor=UNQUALIFIED`; `activity_units=UNKNOWN`; `suspension=UNKNOWN`; exact-session universe, promoted calendar and raw-retention contract remain `UNKNOWN`; `canonical_session_failover=UNQUALIFIED`; publication and failover remain `false`. The pinned four-request discovery contract remains documented for future separately authorized work, but no R2-F4.0 request or promotion is implied. |
| Tushare Pro | [HTTP](https://tushare.pro/document/1?doc_id=40), [daily](https://tushare.pro/document/1?doc_id=27), [adj_factor](https://tushare.pro/document/2?doc_id=28), [suspend_d](https://tushare.pro/document/2?doc_id=214), [trade_cal](https://tushare.pro/document/2?doc_id=26), [index_daily](https://tushare.pro/document/1?doc_id=95), [stock_basic](https://tushare.pro/document/1?doc_id=25), [points](https://tushare.pro/document/1?doc_id=108), [service terms](https://tushare.pro/document/1?doc_id=405), [user agreement](https://tushare.pro/document/1?doc_id=409) | `discovered`; official transport record documents HTTP only, so HTTPS token use is prohibited until separately proven. |

The closed credential map is:

```text
tickflow -> STOCK_EVA_TICKFLOW_TOKEN
tushare -> STOCK_EVA_TUSHARE_TOKEN
```

Values are environment-only and are never serialized, logged, put in URLs, or
stored in registry control data. TermsEvidence must be reviewed before a provider
leaves `discovered`; missing terms, intended-use, retention, quota, schema, units,
or transport proof fail closed before client construction. Task10 performs no
provider request, credential acquisition, NAS access, production mutation, or
canonical publication.

## R2-F4.1 calendar observation capability

BaoStock remains the canonical market provider and gains one isolated calendar-maintenance method:
`calendar_days(start, end)`. It uses the pinned transport endpoint `trade_dates`, one dedicated
session and `max_attempts=1`, and accepts only exact chronological, unique, inclusive civil-day
coverage with flags `0/1`. It does not change `trading_dates()` or canonical ingestion. Runtime
maintenance may call it only after an existing CLOSED health snapshot and after both official
objects have verified. Missing, partial, duplicate or invalid machine rows fail closed and cannot
promote.

This does not mark TickFlow's `promoted_calendar_state` qualified. TickFlow exact-session universe,
calendar authority, factors, suspension, units and raw-retention remain `UNKNOWN`/`UNQUALIFIED`;
publication and automatic failover remain false.

## TickFlow canonical-capability blockers

The reviewed R2-F3 result is **GO only for the narrow Daily Bar shadow** and remains **NO-GO for a
complete canonical-capability candidate or failover window**. It does not revoke the separately
bounded raw discovery canary, but a successful canary cannot close these gaps by itself:

- `CompactKlineData` declares `volume` as `integer/int64` and `amount` as `number/double`, but the
  public OpenAPI only labels them as volume and turnover amount. It does not declare shares versus
  lots or CNY versus thousand-CNY. Examples and A-share convention are not unit evidence.
- `ExFactorEntry` declares `timestamp` and `ex_factor`, but not whether the value is an event or
  cumulative factor, its forward/backward direction, anchor, or historical restatement behavior.
- The reviewed universe and K-line schemas contain no authoritative suspension/trading-state
  field. A missing daily row or zero activity cannot be reclassified as suspended, not listed, or
  active-zero-trade without provider-authored semantics.
- The API overview documents per-key limits and HTTP 429, but no reviewed fixed RPM, concurrency,
  daily/monthly allowance, endpoint weight, batch-symbol ceiling, or stable `Retry-After` contract
  is available for the intended account. SDK retry guidance is not authority to weaken the
  project-wide `max_attempts=1` canary boundary.
- The reviewed terms support lawful private research and prohibit credential sharing and
  unauthorized resale, but do not explicitly settle raw-response retention duration, internal
  sharing, backup/copying, termination deletion, or upstream-data redistribution rights.
- Public availability and marketing availability statements are not an enforceable data
  completeness, repair, maintenance-window, or continuity SLA.

Before a separately versioned canonical-capability profile may start full-session qualification,
provider-authored evidence must close the unit, factor, suspension/missing-row,
quota/account-tier, and private raw-retention contracts. The evidence URL/body hash, observation
date, review ID and contract hashes must be attached to that authority; any later contradiction or
material contract change resets its qualification window or quarantines the provider. Until then
`units_status=unknown`, `complete_candidate=false`, and no canonical normalization/reconciliation,
symbol-level mixing or failover is permitted. The already reviewed R2-F3 Daily OHLC-only
normalization/reconciliation remains valid inside its isolated non-publishing profile and grants no
additional semantic authority.

## Task11 shadow adapter contract

Task11 adds source-shaped, injected adapters only. The following four-request Task14 policy is a
**legacy parked contract** retained for audit history; R2-F4.0 does not execute or authorize it.
That parked policy uses
5-second connect/write/pool and 30-second read timeouts, exactly one attempt, four total
requests, no retry/sleep, exact HTTPS host/TLS and no redirects, plus bounded response bytes/rows. Transport failures are
typed and sanitized; raw payloads, URLs, headers and credentials are not reports.

TickFlow's offline plan requests `daily_batch`, `ex_factors`, `universe(CN_Equity_A)` and
`indexes(CN_Index)` in that order. Daily rows remain explicitly unadjusted, while
factor/corporate-action units, suspension semantics and quota/retention remain explicit
discovery gaps. Execute-mode failures retain only sanitized `failure_class`, safe endpoint
identity and actual post-client `provider_requests`; pre-client blocks remain zero-request.
The official factor map must contain exactly the requested provider-symbol keys; an empty
per-symbol array is a valid `no_event` discovery result, while missing/extra keys fail closed.
Compact kline optional arrays are non-null finite numbers, volume is non-negative int64, and
all execute calls require 5–10 unique main-board symbols.
Tushare's
offline plan requests `daily`, `adj_factor`, `suspend_d`, `trade_cal` and
`index_daily` for one exact trade date. Tushare `vol` remains source `lots` and
`amount` remains source `thousand_cny`; conversion is not performed by the
adapter. Tushare execute remains blocked without reviewed terms, official HTTPS
proof and a non-secret external authorization ID.

`market-provider-canary` is network-free by default and reports planned endpoint
identities and request bounds. Task11 evidence is isolated under the shadow root,
retains only final successful pages, and ends at `evidence_ready` /
`pending_normalization`; it never writes canonical data, candidates, selections,
Parquet or pointers. No real provider canary was run for this offline change.

## Task13 shadow report contract

The isolated outbox retains sanitized `ShadowAttemptReport` outcomes only:
`evidence_ready`, `success`, `failure`, `skip`, `unavailable` or `mismatch`.
Failure-class reports retain timing, coverage, retry/rate-limit counts and a
content hash, while page/row/evidence/candidate fields are empty or null.
`evidence_ready` is nonterminal; only a new terminal report version plus one
immutable attestation and the expected job/window CAS updates can complete a
session. Every report is append-only. Automatic failover remains disabled.

## R2-F4.2 provider and Universe boundary

R2-F4.2 的 exact-session Universe status projection 不会构造任何 provider，也不会把
BaoStock、TickFlow 或 Tushare 的未持久化结果当作当前 Universe。source/version 变化只有在
writer-owned sidecar 写入新的 `universe_source_state` 后才可见；查询层不能自行发现或修复
provider 变化。Universe candidate 仍只能以完整会话发布，禁止 symbol-level 混源；secondary
provider qualification、automatic failover 和生产 enablement 均保持关闭。
