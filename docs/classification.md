# R1-A 时点分类主数据

R1-A 提供版本化的证券主数据、指数成分和行业成员快照。它是 Release 1 后续市场状态、
板块和龙头计算的只读输入，不提供实时行情、券商连接或交易写入。

## Coverage 与 actionability

Classification coverage 的分母是请求 `as_of` 当日已经上市且尚未退市的：

- 上交所和深交所证券；
- 主板、创业板和科创板；
- 普通 A 股股票。

北交所、B 股、指数及其他非股票证券、尚未上市和已经退市证券不进入分母。临时停牌、
`price_available=false` 和 `price_available=null` 仍在 coverage 分母内；它们分别
通过独立的 `actionability.reasons` 暴露 `suspended`、`no_price` 和
`price_not_audited`。Coverage API 同时返回这些原因的计数，不能用可交易性缩小行业
映射分母。

证券记录分别保留 BaoStock 原始字段：

- `listing_status` 来自 `query_stock_basic.status`，用于审计上市状态；
- `daily_trade_status` 来自 `query_all_stock.tradeStatus`，用于审计请求日交易状态；
- `is_tradable` 只由日交易状态派生，不与上市状态合并。

## 日期与代际

每次发布创建不可变 generation。中央 publish 门禁要求 security master 非空，并核对
所有 security/index/sector 记录的 `source`、`source_version`、`observed_at` 和
`source_snapshot_date` 都不越过 enclosing snapshot。相同输入重复发布返回同一
generation；同一 source snapshot 的冲突内容拒绝发布。

Candidate 可以持久化，但只有全部 coverage audit 都达到 ready（分母非零且映射率至少
95%）时，才在同一事务最后移动唯一 ready pointer。降级 candidate 不替换上一可信代；
没有旧 ready 时保持无 ready。所有 classification endpoint 通过一次原子 snapshot read
选择 generation，并从该 generation 读取 records、scope 和 ID，因此并发发布及相同
source `updateDate` 的连续发布不会混代、重复或泄漏上一代成员。

`observed_at` 晚于 `as_of` 的记录不可见。来源没有提供结束日时 `effective_to` 保持
`null`，系统不推断结束日。

`source_date_semantics` 明确区分日期证据：

- `source_observed`：日期来自响应中的 `updateDate`；
- `requested_unverified`：日期只是请求参数，wrapper 没有返回可确认的响应日。

BaoStock `query_all_stock(day=...)` 产生的证券 snapshot 使用
`requested_unverified`。行业和三个已接入指数成分查询使用响应 `updateDate`，并在
任何 source 日期晚于 requested `as_of` 时 fail closed。`classification-sync
--execute` 也会在发起请求前拒绝未来 `as_of`。

Provider 同时要求 `query_all_stock`、`query_stock_basic`、行业及配置的三个成分查询
均非空且包含必需字段。所有外观属于上交所/深交所主板、创业板或科创板的代码必须有
可用的 basic type/status 元数据；空结果和字段缺失不能降格为 `other` 后发布。

## 指数 master

| ID | Symbol | Component source | History capability |
|---|---|---|---|
| `sse_composite` | `sh.000001` | none | `not_supplied` |
| `szse_component` | `sz.399001` | none | `not_supplied` |
| `hs300` | `sh.000300` | BaoStock | `unverified` |
| `sz50` | `sh.000016` | BaoStock | `unverified` |
| `csi500` | `sh.000905` | BaoStock | `unverified` |
| `csi1000` | `sh.000852` | none | `not_supplied` |
| `chinext_index` | `sz.399006` | none | `not_supplied` |

接受 date 参数不等于历史能力已经验证。当前 BaoStock 三个组件接口都保持
`unverified`，有数据的 API 响应为 `degraded` 并包含
`component_history_unverified`。无组件来源的指数只提供 metadata/价格序列目标，
组件响应为 `not_available` 和 `component_history_not_supplied_by_source`；系统不
伪造成分股。

## API 与运行

主要只读端点：

```text
GET /api/v1/classification/securities?as_of=YYYY-MM-DD
GET /api/v1/classification/indexes/{index_id}/components?as_of=YYYY-MM-DD
GET /api/v1/classification/taxonomies/{taxonomy_id}/sectors?as_of=YYYY-MM-DD
GET /api/v1/classification/taxonomies/{taxonomy_id}/sectors/{sector_id}/members?as_of=YYYY-MM-DD
GET /api/v1/classification/coverage?as_of=YYYY-MM-DD&taxonomy_id={taxonomy_id}
```

HTTP endpoint 按 `Asia/Shanghai` 日期拒绝未来 `as_of`（422），并在访问 store 前
完成校验。GET reader 只执行 SELECT；为兼容 DuckDB 同进程 writer，它使用相同连接
配置但不初始化 temp/schema，也不执行 DDL/DML。classification DB 缺失时返回真实
empty/not-available，不创建 DB、父目录或临时目录。Writer 初始化和 migration
只属于 publish/sync execute 路径。

同步默认 dry-run，不发网络请求也不写 classification 数据：

```bash
uv run python -m backend.app.cli classification-sync --as-of YYYY-MM-DD
```

审阅计划后才可显式执行：

```bash
uv run python -m backend.app.cli classification-sync \
  --as-of YYYY-MM-DD \
  --execute
```

Sync 结果的 `new_generation` 明确表示是否插入新 generation；
`writes_classification_data` 与同一原子 publish outcome 一致。幂等重跑两者均为
`false`，不使用易竞态的 generation count 前后比较。

本次 R1-A recovery 只验证 synthetic contract。真实 BaoStock publication、live
coverage、20-session history 和 browser acceptance 的状态见
[`docs/acceptance/release-1-r1a.md`](acceptance/release-1-r1a.md)。
