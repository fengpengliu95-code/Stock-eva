# R1-B 市场状态 v1

R1-B 提供可按 `as_of` 重放的确定性市场状态，不使用 LLM，也不生成投资建议：

```text
GET /api/v1/analysis/market-regime?as_of=YYYY-MM-DD
```

HTTP 按 `Asia/Shanghai` 日期拒绝未来 `as_of`（422）。service 本身不依赖今日日期，
因此测试和历史验收可以重放任意历史日期，但 store 的查询条件始终是
`trade_date <= as_of`。GET 使用独立 SELECT-only reader；本地 DuckDB 或数据集路径
缺失时返回真实 `empty`，不创建数据库、父目录、临时目录或 schema。

## 输出契约

结果同时包含：

- 战略状态：`bull / range / bear`；
- 战术状态：`risk_on / neutral / risk_off`；
- trend、breadth、liquidity、risk、leadership 五个分项的原始得分、权重、
  加权得分、分项公式版本和质量；
- 总分、置信度、缺失输入、支持证据、反例和质量问题；
- 请求 `as_of`、实际行情 `data_as_of` 和内容 hash 血缘；
- 公式版本、全部权重和阈值；
- expected/observed/missing boards 与代表指数、分项及总 coverage ratio；
- 结果是否具备“全 A 股”结论能力和范围免责声明。

`result_id` 是输入、公式版本、权重和阈值的规范 JSON SHA-256 摘要。同一输入重复
执行得到相同 ID、得分、状态和证据。

## v1 公式

公式总版本为 `market-regime-v1`。分项分数都落在 `[-100, 100]`，正值表示更支持
风险偏好，负值表示更支持风险规避。

| 分项 | 公式版本 | v1 输入 |
| --- | --- | --- |
| trend | `trend-price-ma-v1` | 代表指数收盘价相对 MA20/60/120，以及 MA20 的 5 日斜率 |
| breadth | `breadth-price-v1` | 上涨比例、站上 MA20/60 比例、60 日新高/新低差 |
| liquidity | `liquidity-price-volume-v1` | 当日合格股票成交额相对前 5 日均值 |
| risk | `risk-price-v1` | 代表指数 20 日波动、60 日回撤、横截面涨跌离散度 |
| leadership | `leadership-sector-v1` | R1-C 之前没有合格板块持续性/龙头扩散输入，必须 missing |

趋势的四个条件分别映射为 `-100 / 0 / 100` 后取均值。宽度的比例类输入使用
`clamp((ratio - 0.5) × 200)`；新高/新低差使用
`clamp((new_high_ratio - new_low_ratio) × 500)`。流动性使用
`clamp((turnover_5d_ratio - 1) × 100)`。

风险分数越高表示风险越低。指数日波动在 1% 以下为 `100`、3% 以上为 `-100`；
60 日回撤在 5% 以下为 `100`、20% 以上为 `-100`；横截面离散度在 1.5% 以下为
`100`、4% 以上为 `-100`，区间内线性插值。

总分对可用分项按以下权重重新归一化；缺失分项不猜为零：

```text
trend=0.30
breadth=0.25
liquidity=0.15
risk=0.20
leadership=0.10
```

战略阈值为 `bull >= 25`、`bear <= -25`，其余为 `range`。战术阈值为
`risk_on >= 10`、`risk_off <= -10`，其余为 `neutral`。边界包含等号。

置信度取以下三者的最小值：

1. 有得分分项的权重之和；
2. 分项质量加权和（ready=`1`、degraded=`0.75`、missing=`0`）；
3. 实际 market scope coverage ratio。

`>=0.9` 为 high，`>=0.5` 为 medium，其余为 low。任何缺失分项、降级分项或
narrow scope 都使结果状态成为 `degraded`；全部分项缺失时为 `empty`。

## 市场范围真实性

“全 A 股”能力的 v1 期望价格范围是：

- boards：上交所主板、深交所主板、创业板、科创板；
- indexes：上证综指、深证成指、沪深 300、中证 500、中证 1000、创业板指。

北交所仍是 roadmap 的单独决策项，不被静默纳入或排除“全 A 股”文案。

当前既有 canonical 历史实际只发布沪深主板，以及上证综指和深证成指。因此用当前
生产形态读取时，接口必须返回 `narrow_provisional`、low confidence、
`narrow_scope_not_full_a_share`，并明确“不能支持全 A 股牛熊结论”。R1-B 仅把
canonical 模型和显式代码归类补通到创业板、科创板及代表指数；没有扩大
`all-main-board` 抓取，没有访问或写入真实 BaoStock/NAS/用户数据，也没有用合成数据
填充生产。

成交额趋势只是 price-volume evidence，不是主力、北向或机构资金净流入。在合格
资金证据进入后续 release 前，`liquidity.fund_flow_evidence` 保持 missing，流动性
分项保持 degraded。

## 回放边界

store 每次只读取截止 `as_of` 的最近 130 个已存在交易日。股票均线使用
`raw_close(d) × backFactor(d) / backFactor(data_as_of)`；停牌、非 ready 行和缺失
复权因子不进入相关宽度计算。任何 reader 异常返回的未来行都会被二次丢弃，并产生
`future_market_rows_discarded`。

R1-B 只生成按请求即时确定性结果，不持久化新的 regime 数据库。Release 1 的
“每个 ready 交易日一份快照”及至少 20 个真实历史交易日验收由 R1-E 在受控真实
数据上完成。
