# Data provider ledger

This is a discovery ledger for the R2-F3 shadow lane (as of 2026-08-27). It is not
runtime verification, provider endorsement, permission to request data, or a
qualification record. BaoStock remains the only canonical provider.

| Provider | Official records | Disposition |
| --- | --- | --- |
| TickFlow | [OpenAPI](https://docs.tickflow.org/zh-Hans/api-reference/openapi.json), server `https://api.tickflow.org`, [API overview](https://docs.tickflow.org/zh-Hans/api-reference/introduction), [FAQ](https://docs.tickflow.org/zh-Hans/faq), [SDK practices](https://docs.tickflow.org/zh-Hans/sdk/python-best-practices), [terms](https://tickflow.org/legal/terms-of-service.md) | qualification remains `discovered`; one isolated registry is at `CANARY` for the separately authorized discovery probe only. The pinned four-request contract is GET `/v1/klines/batch` (`symbols`, `period=1d`, `start_time`, `end_time`, `adjust=none`), GET `/v1/klines/ex-factors` (`symbols`, `start_time`, `end_time`), and two fixed IDs through official GET `/v1/universes/{id}`: `CN_Equity_A` and `CN_Index`, with no query parameters. `x-api-key` is the documented header. External codes use `600000.SH`/`000001.SZ`; canonical `sh.600000`/`sz.000001` is retained separately. Batch `data` is a provider-symbol to compact-column-array map, factors are provider-symbol to `[{timestamp, ex_factor}]`, and universe/index are strict detail objects with `id`, `name`, `region`, `category`, `symbol_count`, `symbols`, and optional `description`. Terms mention non-sharing, no malicious large-scale scraping and lawful personal/commercial use subject to no unauthorized resale. Units, factor direction/anchor, suspension/missing-row semantics, numerical quota and raw-response retention remain unverified. |
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

## TickFlow qualification blockers

The 2026-08-27 official-contract review is **NO-GO for a complete one-day shadow candidate and
NO-GO for the 20-session qualification window**. It does not revoke the separately bounded raw
discovery canary, but a successful canary cannot close these gaps by itself:

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

Before the registry may enter `SHADOW`, provider-authored and versioned evidence must close the
unit, factor, suspension/missing-row, quota/account-tier, and private raw-retention contracts. The
evidence URL/body hash, observation date, review ID and contract hashes must be attached to the
registry; any later contradiction or material contract change resets the qualification window or
quarantines the provider. Until then `units_status=unknown`, `complete_candidate=false`, no
normalization/reconciliation is allowed, and no symbol-level mixing or failover is permitted.

## Task11 shadow adapter contract

Task11 adds source-shaped, injected adapters only. Task14's bounded TickFlow policy uses
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
