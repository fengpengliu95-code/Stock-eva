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

## 已交付契约

`AKShareSupplementalProvider` 是无副作用的可选归一化器：

- 依赖通过 `uv sync --extra supplemental` 单独安装，默认运行环境不加载 AKShare；
- 构造 provider 和所有 GET 请求均不联网；
- 每条记录保留 `source=akshare`、实际上游、源页面、观察时间与明确源日期；
- `through_date` 是硬上界，未来日期不会进入结果；
- 缺少明确日期或必需的上游主力净流入字段时整批失败，不用抓取时间代替；
- 输出字段使用 `reported_*`，明确表示上游报告口径；
- fund-flow 模型没有 OHLCV 字段，东财响应中的指数收盘价被主动丢弃。

`GET /api/v1/market/supplemental` 当前只返回能力与空态：

- 默认：`status=not_configured`、`akshare_supplemental_disabled`；
- 即使设置实验开关：`status=empty`、`supplemental_ingestion_not_implemented`；
- GET 不抓取、不落库、不生成排名，不伪装为已经接入。

## 失效与审计边界

AKShare 是对第三方网页/API 的适配层，字段、域名、反爬策略和接口可用性会变化。官方
变更记录中已有多次板块资金流修复，社区也报告过东财断连或限流。因此未来启用前仍需：

1. 固定 AKShare 版本并保存字段 canary；
2. 仅在官方交易日收盘后运行显式摄取；
3. 原始响应 hash、记录数、最小/最大日期和失败原因写入运行审计；
4. 先 staging，校验日期/重复/空值/范围，再移动 published pointer；
5. partial/error 保留旧完整快照；
6. 分类快照需要持续版本化后才能做历史回放，不能用今日分类回填过去；
7. 不接入无日期排名、实时接口或个股 OHLCV。

下一步若获批准，只应先做少量行业和一个明确交易日的受控网络 canary；本轮不执行。
