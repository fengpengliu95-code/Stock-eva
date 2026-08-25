# Data provider ledger

This is a discovery ledger for the R2-F3 shadow lane (as of 2026-08-24). It is not
runtime verification, provider endorsement, permission to request data, or a
qualification record. BaoStock remains the only canonical provider.

| Provider | Official records | Disposition |
| --- | --- | --- |
| TickFlow | [docs](https://docs.tickflow.org/zh-Hans), [quickstart](https://docs.tickflow.org/zh-Hans/quickstart), [homepage](https://tickflow.org/), [terms](https://tickflow.org/legal/terms/) | `discovered`; daily/universe pages do not prove terms, suspension, units, factors, quota, or retention. |
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

## Task11 shadow adapter contract

Task11 adds source-shaped, injected adapters only. The bounded HTTP policy uses
connect/read/write/pool timeouts, at most five attempts, bounded `Retry-After`,
bounded response bytes/rows, and a hard request counter. Transport failures are
typed and sanitized; raw payloads, URLs, headers and credentials are not reports.

TickFlow's offline plan requests `daily`, `universe` and `indexes`. Daily rows
remain explicitly unadjusted, while factor/corporate-action evidence is an
explicit `unavailable` gap until the official contract proves it. Tushare's
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
