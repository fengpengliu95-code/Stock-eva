# 阶段 1 行情数据契约

## 范围

阶段 1 只处理上交所、深交所 A 股主板的公开免费收盘后日线。BaoStock 是唯一
OHLCV 主源；AKShare 尚未接入，未来只能作为有来源标记的受控补充，不能与
BaoStock 的同一根 K 线拼接、平均或静默回填。

市场摘要目前包含：

- 上证综指 `sh.000001`、深证成指 `sz.399001`；
- 可交易主板股票的上涨、下跌、平盘和停牌数量；
- 可交易且成功加载股票的成交额合计及覆盖数量。

成交额是交易所日成交概览，不代表主力、北向或机构净流入。免费主源不能稳定
给出这些资金流字段，因此阶段 1 明确不输出相应结论。

历史回填、每日幂等审计和全市场 dry-run 的操作边界见
[第一版真实数据就绪](real-data-readiness.md)。历史范围请求按证券一次读取一段
日期，不按 60 个交易日逐日发起请求；证券与日期批次仍受硬上限保护。

## Canonical 日线

主键是 `trade_date + symbol + source`，天然按交易日可查询。重复刷新先替换
同一主键，再在同一 DuckDB 事务内登记刷新结果，因此不会累积重复行。

关键字段：

| 类别 | 字段 |
| --- | --- |
| 行情 | `open/high/low/close/preclose/volume/amount` |
| 复权 | `price_adjustment="none"`、`adjust_factor` |
| 交易状态 | `is_trading`、`is_suspended`、`is_st` |
| 血缘 | `source`、`source_record_id`、`ingested_at` |
| 质量 | `quality_status`、`quality_issues` |

`adjust_factor` 对应 BaoStock 的前复权因子。前复权读取严格使用
`前复权价格 = 未复权价格 × adjust_factor`，成交量和成交额不缩放。指数不适用时为 `null`；股票因子
缺失时也为 `null`，并把刷新降级为 `partial`，不会用 `1.0` 猜测。停牌日即使
BaoStock 返回前收盘价和零成交，也会标为停牌占位，不进入可交易宽度和成交额。
面向指标的前复权序列读取会直接排除停牌行；任何股票行缺少因子会返回数据质量错误，
不会生成部分复权序列。

## 摘要状态

| 状态 | 语义 |
| --- | --- |
| `empty` | 本地没有刷新记录；不代表市场为零 |
| `ready` | 请求切片全部加载且关键质量字段完整 |
| `partial` | 个别证券或复权因子失败；返回成功子集及失败列表 |
| `stale` | 最新数据早于调用方传入的 `expected_date` |
| `error` | 最近一次刷新整体失败；摘要不复用旧行生成市场结论 |

所有状态都包含 `as_of`、`source`、`completeness` 和 `freshness`。覆盖率按
`成功加载的期望证券数 / 期望证券总数` 计算，必须达到 100% 才是完整。完整刷新
通过当日证券列表建立期望主板集合；显式小切片则以用户给出的去重代码为期望集合。

新鲜度只与调用方明确提供的期望交易日比较。未提供 `expected_date` 时，系统不以
工作日算法猜测中国节假日，而是降级为 `partial/freshness_not_evaluated`；早于期望
交易日时为 `stale`。调用方必须先判断状态，再展示宽度或成交额。

## 刷新边界

CLI 只在显式调用时访问网络：

```bash
uv run python -m backend.app.cli refresh \
  --date YYYY-MM-DD \
  --symbols sh.600000 sh.000001

uv run python -m backend.app.cli refresh \
  --date YYYY-MM-DD \
  --all-main-board
```

BaoStock 官方说明当日日线约 17:30 后、复权因子约 18:00 后更新。建议 18:10
后运行，并由调度方传入确认已结束的交易日。当前 CLI 不自行推断“今天”、
不持续轮询，也不自动重试到另一个数据源。

Provider 在任何行情请求前调用 BaoStock 交易日历；非交易日不会继续请求日线。
显式刷新最多允许 20 个证券，每个请求最多尝试 2 次，真实客户端请求间隔至少
0.2 秒。全市场刷新不逐股循环：使用 BaoStock 0.9.3 的批量日线、批量因子和证券
列表接口，只单独请求两个摘要指数。任一失败都会显式降级，不会无界重试。

## 历史查询与导出

- `GET /api/v1/market/history/dates` 返回本地已有交易日。
- `GET /api/v1/market/history/{symbol}` 以单一 `source=baostock` 读取未复权或
  前复权序列，不跨来源填洞。
- `python -m backend.app.cli export --date YYYY-MM-DD` 使用 DuckDB 原生 `COPY`
  输出 `var/market/exports/date=YYYY-MM-DD/bars.parquet`。

Parquet 是单日、可重建的交换分区；DuckDB 仍是本地查询真值。当前规模不引入额外
数据湖目录、清单服务或分区协调器，避免为阶段 2 的手动持仓功能增加无关复杂性。

## 已知限制

- BaoStock 是免费公共服务，没有项目可控制的可用性承诺。
- 本地交易日历只在刷新时向 BaoStock 验证，没有离线节假日日历；离线 API 调用必须
  传入 `expected_date` 才能严格判断新鲜度。
- Parquet 目前只做显式单日导出，不自动维护全量镜像。
- 尚未实现 AKShare 补充字段和跨源对账。
- 尚未计算技术指标；后续如计算，只能用前复权序列。
