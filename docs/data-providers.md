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
