# R1-C 板块轮动与龙头 v1

R1-C 是盘后、只读、可按 `as_of` 重放的确定性分析。它只消费：

1. R1-A 已经 `promoted`、行业映射率不低于 95% 的一个 classification
   generation；
2. 截止 `as_of`、由可信 publication audit 和显式日期分区共同确认的一个 canonical
   market `data_as_of` 及最多 80 个已存在 session。

每个请求只选择一次 classification snapshot 和一次 market input。分类 generation
ID、行情内容 hash、实际 `data_as_of` 和真实覆盖范围都随结果返回；degraded
classification candidate、未来成员和未来价格不能进入结果。

## API

```text
GET /api/v1/analysis/sector-rotation?as_of=YYYY-MM-DD&taxonomy_id={taxonomy_id}
GET /api/v1/analysis/sectors/{sector_id}/leaders?as_of=YYYY-MM-DD&taxonomy_id={taxonomy_id}
```

HTTP 按 `Asia/Shanghai` 日期在读取 store 前拒绝未来 `as_of`（422）。classification
数据库、本地 market 数据库、published dataset root 或 manifest 缺失时 GET 返回真实
`empty`，且不创建数据库、目录、schema 或临时文件。已经存在但缺少可信发布状态、物理
损坏、语义损坏或与对象不一致的 published store 保持显式 503。

本地 DuckDB 发布采用一日一分区。`published_snapshots` pointer 和每个被读取的
`published_daily_bars.publication_run_id` 都必须关联到 `refresh_runs` 中完整的
`ready` audit：requested/succeeded 数相等、coverage 为 1、failed/quality issues
为空，并与该分区日期、来源和唯一 symbol 数一致。partial/unpublished 同日重跑不能覆盖
已发布分区；合法同日完整重跑只替换该日，历史分区保留。外部 Parquet 数据集继续由不可变
manifest 和对象 checksum 承担同一信任边界。

当前 R1-A 尚无 live promoted classification generation，因此生产 readback 预期为
`empty`。这不是用合成结果填充生产的理由。

## 真实范围

R1-C v1 强制 `narrow_main_board`：

- 只对 classification 中有实际 canonical 价格的沪深主板证券计算；
- 创业板、科创板及未知板块不被静默纳入；
- 额外行情证券可以进入“实际可观测主板基准”，但不能成为分类成员或龙头；
- `as_of` 时尚未上市、已经退市、非股票或不受支持市场/板块的 classification member
  在分组、coverage denominator、指标和龙头候选前统一排除；
- `can_support_full_a_share_conclusion=false`；
- `can_support_all_industry_conclusion=false`。

返回结果会分别列出 classification eligible 数量、实际可观测主板证券数量、已有价格的
分类证券数量和被范围排除的 classification boards。

## 板块公式

总版本为 `sector-rotation-v1`。分项分数范围为 `[-100, 100]`，缺失项为 JSON
`null`，不会补零；总分只对可用项按权重重新归一化。

| 分项 | 权重 | 原始值与公式版本 |
| --- | ---: | --- |
| 5 日相对强度 | 0.08 | 板块成员等权 5 日收益减实际可观测主板等权 5 日收益；`sector-equal-weight-minus-observed-main-benchmark-5d-v1` |
| 20 日相对强度 | 0.12 | 同上，20 日；`...-20d-v1` |
| 60 日相对强度 | 0.12 | 同上，60 日；`...-60d-v1` |
| 上涨扩散度 | 0.08 | 当日有效成员上涨比例；`sector-advancing-member-ratio-v1` |
| MA20 breadth | 0.08 | 前复权收盘站上 MA20 的有效成员比例；`sector-qfq-close-above-ma20-ratio-v1` |
| MA60 breadth | 0.08 | 前复权收盘站上 MA60 的有效成员比例；`sector-qfq-close-above-ma60-ratio-v1` |
| 成交额 5 日变化 | 0.04 | 同一组可比成员当日总成交额 / 之前 5 个共同有效 session 的均值 - 1；`sector-current-turnover-vs-prior-5d-mean-v1` |
| 成交额 20 日变化 | 0.04 | 同上，20 日；`...-20d-mean-v1` |
| 成交额集中度 | 0.04 | 当日成交额前三成员占比；`sector-top3-turnover-share-cohesion-v1` |
| 持续性 | 0.08 | 板块日收益相对实际可观测主板基准连续同号 session，最多 20 日，落后为负；`sector-consecutive-relative-return-sign-max20-v1` |
| 横截面离散度 | 0.04 | 当日成员收益总体标准差；`sector-current-return-population-dispersion-v1` |
| 合格龙头数量 | 0.05 | 同时通过研究龙头资格门禁的候选数量；`qualified-research-leader-count-v1+research-leader-qualification-v1` |
| 龙头扩散度 | 0.10 | 合格龙头数量 / 当前可比研究候选数；`qualified-research-leader-diffusion-v1+research-leader-qualification-v1` |
| 龙头持续天数 | 0.05 | 合格龙头相对板块连续领先/落后 session 数的均值；`qualified-leader-mean-persistence-max20-v1+research-leader-qualification-v1` |

5/20/60 日收益分别至少需要 6/21/61 个有效 session。MA20/60 分别需要
20/60 个有效 session；不足时返回对应 warm-up missing input。每个分项同时返回
`effective_count / target_count / coverage_ratio`；少于 2 个可比成员或低于 80% 时该
分项为 missing，板块不进入有效排名。停牌、非 ready、带质量问题、缺失/非法复权因子
或非法价格/成交额的 bar 不进入计算。成交额变化始终使用跨整个窗口相同的可比成员，IPO
或短历史证券不会只在当日放大分子。

成交额及量价只属于 `price-volume evidence`，不表示主力、北向或机构净流入。
R2 的 L1/L2/L3 资金证据尚未接入，因此：

```json
{
  "status": "missing",
  "evidence_tier": null,
  "reason": "R2 fund-flow evidence is not integrated; turnover is price-volume evidence only"
}
```

## 龙头公式与可操作性边界

总版本为 `leader-ranking-v1`。候选先通过 `as_of` classification eligibility 与当日
canonical bar 的可交易性门禁；停牌、非交易、坏质量、非法价格/成交额及范围外板块会
进入 `exclusions`，不会参与排名。classification 的 `price_available` 是采集审计
元数据，不替代实际 canonical bar；真实 BaoStock 常见的 `price_available=null` 配合
当日可信 ready canonical bar 可以成为研究候选。

候选分项包括：

- `tradeability`；
- 相对实际可观测主板基准的 5/20/60 日相对强度；
- 当日成交额相对之前 20 日均值的活跃度；
- 前复权收盘相对 MA20/60 的趋势质量；
- 个股 20 日收益对板块等权收益的贡献；
- 相对板块日收益连续领先/落后的 session 数；
- 当日收益偏离板块平均收益的离散度一致性。

每项都返回原始值、分数、权重、加权分、公式版本、质量和 missing input，并汇总
supporting/contrary evidence。

研究龙头资格版本为 `research-leader-qualification-v1`，必须同时满足：20 日相对强度为
正、当日成交额不低于之前 20 日均值、趋势质量为正、20 日板块贡献为正；可交易性已由
候选入口门禁保证。每个候选显式返回 `leader_qualified`、资格/不资格原因和资格版本。
资格版本及结果进入 candidate、sector ranking 和总结果稳定 ID；改变资格版本不会沿用
旧 ID。

canonical daily bar 当前不足以可靠、按板块和风险警示状态识别一字板或涨跌停锁死。
R1-C 不猜测该输入：所有候选均返回
`limit_lock_status=unavailable`、`actionable_primary=false` 和
`actionability_status=risk_inputs_unavailable`。排名只表示研究候选次序，不是可操作
首选或交易指令。

## 状态与确定性

- 无 promoted classification 或无截止 `as_of` 的行情：`empty`；
- 有部分价格、暖机不足、数据陈旧、资金缺失、limit-lock 缺失或窄范围：
  `degraded`；
- R1-C v1 因资金与范围边界不会借由量价证据升级成完整全市场 `ready` 结论。

`result_id`、`ranking_id` 和 `candidate_id` 均由规范化输入、classification
generation、market lineage、公式版本、资格版本和权重生成。分数相同的板块按 `sector_id`
排序，分数相同的候选按 `symbol` 排序。
