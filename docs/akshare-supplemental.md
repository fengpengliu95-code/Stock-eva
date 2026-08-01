# AKShare 板块与资金流可行性审查

审查日期：2026-07-24。代码契约固定到 AKShare `1.18.78`
（Git tag `release-v1.18.78`）；本轮没有调用任何真实数据接口。

## 结论

AKShare 只作为 BaoStock 之外的可选补充层。BaoStock 继续是 canonical OHLCV 的唯一
主源，AKShare 数据不得覆盖或拼接任何开高低收量字段，也不得从 OHLCV 推断“主力”
或其他资金方向。

| 能力 | 结论 | 日期语义 | 原因 |
| --- | --- | --- | --- |
| `stock_industry_clf_hist_sw` | 契约可行，默认不启用 | `start_date`、`update_time` | 申万源文件提供分类变动日期；不推断结束日 |
| `stock_market_fund_flow` | 契约可行，默认不启用 | 明确交易日 | 东财历史接口；只保留上游报告资金字段，丢弃其指数价格 |
| `stock_sector_fund_flow_hist` | 契约可行，默认不启用 | 明确交易日 | 单行业历史接口，必须由受控行业集合调用 |
| `stock_sector_fund_flow_rank` | 拒绝 | AKShare 输出无可靠日期 | “今日/5日/10日”不能保存为历史事实 |
| `stock_board_industry_name_em` / `cons_em` | 拒绝作为历史分类 | 仅抓取观察时点 | 当前板块与成分没有生效日期 |
| 同花顺“即时/多日排行” | 拒绝 | 无明确数据日 | 页面排行不等于可回放日终事实 |

一手依据：

- [AKShare 1.18.78 资金流源码](https://github.com/akfamily/akshare/blob/release-v1.18.78/akshare/stock/stock_fund_em.py)
- [AKShare 1.18.78 申万分类源码](https://github.com/akfamily/akshare/blob/release-v1.18.78/akshare/stock/stock_industry_sw.py)
- [AKShare 1.18.78 东财板块源码](https://github.com/akfamily/akshare/blob/release-v1.18.78/akshare/stock/stock_board_industry_em.py)
- [AKShare 官方股票接口文档](https://akshare.akfamily.xyz/data/stock/stock.html)
- [AKShare 官方变更记录](https://akshare.akfamily.xyz/changelog.html)

## 许可证与数据边界

[AKShare 代码采用 MIT](https://github.com/akfamily/akshare/blob/release-v1.18.78/LICENSE)，
但 MIT 只覆盖适配器代码，不自动授予东方财富、申万宏源或同花顺数据的再分发许可。
AKShare 官方 README 也声明数据仅供学术研究、仅供参考并提示数据风险。因此 Stock
EVA 首版仅设计为用户本地、私人研究；不重新发布原始数据集，不把接口稳定性或数据
定义包装成交易所认证事实。商业化或公开分发前必须重新核对每个上游的使用条款。

## 已交付契约与摄取链路

`AKShareSupplementalProvider` 是无副作用的可选归一化器：

- 依赖通过 `uv sync --extra supplemental` 单独安装，默认运行环境不加载 AKShare；
- 构造 provider 和所有 GET 请求均不联网；
- 每条记录保留 `source=akshare`、实际上游、源页面、观察时间与明确源日期；
- 市场与板块资金流分别固定到 `stock_market_fund_flow` 和
  `stock_sector_fund_flow_hist` 的 source/upstream/scope/endpoint/unit 合同；摄取、
  manifest 读取和分析服务三层都拒绝空 endpoint、跨 scope endpoint 或单位漂移；
- `through_date` 是硬上界，未来日期不会进入结果；
- `through_date` 还必须是不晚于上海当日的已确认开市日；周末、法定节假日和未知年份
  在调用 provider 或写审计前失败；
- 缺少明确日期或必需的上游主力净流入字段时整批失败，不用抓取时间代替；
- 输出字段使用 `reported_*`，明确表示上游报告口径；
- fund-flow 模型没有 OHLCV 字段，东财响应中的指数收盘价被主动丢弃。

`GET /api/v1/market/supplemental` 永远只读已发布清单：

- 默认：`status=not_configured`、`akshare_supplemental_disabled`；
- 启用但未发布：`status=empty`、`supplemental_not_published`；
- 已发布：返回最后通过校验的市场资金、行业资金与行业分类记录；
- 清单、文件、hash 或 schema 损坏：`status=error`，不回退到半套数据；
- GET 不构造 provider、不抓取、不落库、不生成排名。

独立摄取链路使用 `stock-eva-supplemental` 哨兵、manifest 和不可变 ZSTD
Parquet；不会写 BaoStock canonical bar 表。每行统一保留
`source/upstream/endpoint/observed_at/trade_date`。分类记录的 `trade_date`
明确等于源文件的 `effective_from`，同时保留 `source_updated_on`，它不是推断的交易所
开市日。资金数值只使用 `reported_*` 字段；文件 schema 明确排除
OHLCV/amount。

每次运行写本机 SQLite 审计，request key 由固定 provider 契约、截至日期、数据集和
行业列表组成；同一请求成功后重复执行直接返回原运行。新结果先在本机 staging 写入，
校验日期上界、主键唯一性、来源、schema、记录数和 SHA-256；随后复制到数据集
`_staging`，从目标盘 readback，再同共享 rename 为不可变对象；最后一次性替换
manifest。资金流 manifest 同时固化 upstream、source scope、endpoint 和 units。
观察时间必须带时区且不得晚于摄取时钟或 manifest 发布时间。任一步失败只记录安全
错误码，旧 manifest 不移动。

## 显式初始化与 canary

默认本地根为 `var/supplemental`。使用 NAS 时推荐单独配置：

```bash
export STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market
export STOCK_EVA_SUPPLEMENTAL_DATA_DIR=/Volumes/Stock/stock-eva-supplemental
```

先显式初始化空哨兵和 manifest：

```bash
uv run python -m backend.app.market.supplement_cli --initialize
```

NAS 模式会先验证主市场数据集可用，并验证 supplemental 父目录确实位于
`smbfs/cifs` 挂载中。缺挂载或缺父目录时不会 `mkdir`，避免在
`/Volumes/Stock` 掉线后生成同名本地目录。普通 execute 也只接受已经初始化且哨兵
匹配的根。

不带 `--execute` 的计划完全不联网：

```bash
uv run python -m backend.app.market.supplement_cli \
  --through-date 2026-07-24 --market-flow --sector 银行
```

真正的小范围 canary 必须同时给出 `--canary --execute`：

```bash
uv sync --extra supplemental
uv run python -m backend.app.market.supplement_cli \
  --through-date 2026-07-24 \
  --market-flow --classification --sector 银行 \
  --canary --execute
```

真实执行会访问第三方接口；应先固定一个已完成交易日和少量行业，并人工核对记录数、
日期、字段及上游页面。当前代码不自动安装 AKShare，也不随日终 BaoStock 作业自动
运行。首次 canary 验收后，才可另行决定是否纳入收盘后调度。

## 失效与审计边界

AKShare 是对第三方网页/API 的适配层，字段、域名、反爬策略和接口可用性会变化。官方
变更记录中已有多次板块资金流修复，社区也报告过东财断连或限流。因此未来启用前仍需：

1. 固定 AKShare 版本并保存字段 canary；
2. 仅在官方交易日收盘后运行显式摄取；
3. 上游观察的发布对象 hash、记录数、最小/最大日期和失败原因写入清单与运行审计；
4. 先 staging，校验日期/重复/空值/范围，再移动 published pointer；
5. partial/error 保留旧完整快照；
6. 分类快照需要持续版本化后才能做历史回放，不能用今日分类回填过去；
7. 不接入无日期排名、实时接口或个股 OHLCV。

下一步若获批准，只应先安装固定的 optional dependency，对少量行业和一个明确交易日
做受控网络 canary。历史排名、实时接口和个股 OHLCV 仍不在允许范围内。
