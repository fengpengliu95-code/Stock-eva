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
- expected/observed/missing boards 与代表指数、coverage basis、可用 coverage ratio；
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
3. 权威 universe audit 给出的实际 market scope coverage ratio。

`>=0.9` 为 high，`>=0.5` 为 medium，其余为 low。任何缺失分项、降级分项或
narrow scope 都使结果状态成为 `degraded`；全部分项缺失时为 `empty`。
没有权威时点 expected-universe denominator 时，`board_coverage_ratio` 和
`coverage_ratio` 均为 `null`，第三项按 `0` 处理并记录
`market_scope_coverage_not_audited`，不能从符号存在性推导高置信度。

## 市场范围真实性

“全 A 股”能力的 v1 期望价格范围是：

- boards：上交所主板、深交所主板、创业板、科创板；
- indexes：上证综指、深证成指、沪深 300、中证 500、中证 1000、创业板指。

北交所仍是 roadmap 的单独决策项，不被静默纳入或排除“全 A 股”文案。

R1-B 的 reader 只能从当日行列出 `observed_boards`、`observed_index_series` 和
`observed_universe_count`。即使四个 board 都各出现少量股票且六个代表指数都存在，
符号存在性也不等于全市场覆盖审计：`coverage_basis` 必须保持
`symbol_presence_only`，`expected_universe_count`、`board_coverage_ratio` 和
`coverage_ratio` 必须为 `null`，结果必须是 `narrow_provisional`、low confidence、
`can_support_full_a_share_conclusion=false`。只有未来显式传入权威、可按时点重放的
expected-universe denominator 并证明完整覆盖，才允许
`coverage_basis=authoritative_universe_audit` 和全 A 结论能力。六个代表指数的
`index_coverage_ratio` 只表示明确列出的六条指数序列存在比例，不证明股票 universe
完整。

R1-B 没有扩大 `all-main-board` 抓取，没有访问或写入真实 BaoStock/NAS/用户数据，
也没有用合成数据填充生产。

成交额趋势只是 price-volume evidence，不是主力、北向或机构资金净流入。在合格
资金证据进入后续 release 前，`liquidity.fund_flow_evidence` 保持 missing，流动性
分项保持 degraded。

## 回放边界

store 每次只读取截止 `as_of` 的最近 130 个已存在交易日。股票均线使用
`raw_close(d) × backFactor(d) / backFactor(data_as_of)`；停牌、非 ready 行和缺失
复权因子不进入相关宽度计算。任何 reader 异常返回的未来行都会被二次丢弃，并产生
`future_market_rows_discarded`。

风险预热分别判断：20 日波动率至少需要 21 个有效指数 session，60 日回撤至少需要
60 个有效指数 session。未满 60 日时不得缩短窗口冒充 `index_drawdown_60d`，而是
记录 `risk.index_drawdown_60d_warmup` 并降级。横截面收益率仅接受有限
`pct_change`；NaN/inf 时，如果 `close`、`preclose` 有限且 `preclose != 0`，使用
确定性的收盘价回退并记录 `pct_change_fallback_used`，否则排除该行并记录
`invalid_return_input_excluded`。所有对外数值必须有限。

配置了 local/NAS published dataset root 但 root 或 `manifest.json` 不存在时，
read-only reader 返回空输入且不创建任何路径。已经发布的 manifest 若存在但损坏或
与对象不一致，则保留显式 storage failure；不能把损坏数据集吞成“无数据”。

R1-B 只生成按请求即时确定性结果，不持久化新的 regime 数据库。Release 1 的
“每个 ready 交易日一份快照”及至少 20 个真实历史交易日验收由 R1-E 在受控真实
数据上完成。
