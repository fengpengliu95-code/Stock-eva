# 行情数据契约与运行状态

## 范围

核心行情只处理上交所、深交所 A 股主板的公开免费收盘后日线。BaoStock 是唯一
OHLCV 主源。AKShare 补充数据使用独立数据集、独立 manifest 和独立来源字段，
不能与 BaoStock 的同一根 K 线拼接、平均或静默回填。AKShare 数据集已初始化；
最近一次真实 canary 因上游不可用没有发布记录，因此其 published manifest 仍为空。

Canonical 的 board/index 字段契约可以正确读取显式提供的创业板、科创板和代表性
指数代码，但现有 `all-main-board` provider 路径没有因此扩大。未有真实发布覆盖前，
任何派生市场状态必须按实际 observed boards/index series 降级，不能把字段兼容性
称为真实全市场覆盖。

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

逻辑主键是 `trade_date + symbol + source`，天然按交易日可查询。正式 NAS 数据集
按交易日发布不可变 Parquet 分区，只有 manifest 引用的文件才是可见行情真值；同一
交易日修订会生成新 hash 文件并原子切换 manifest，不覆盖已发布文件。DuckDB 保存
刷新审计、调度状态和 published pointer 等本机控制状态；本地开发模式仍可使用
DuckDB canonical 表验证相同主键的幂等覆盖。

关键字段：

| 类别 | 字段 |
| --- | --- |
| 行情 | `open/high/low/close/preclose/volume/amount` |
| 复权 | `price_adjustment="none"`、`adjust_factor` |
| 交易状态 | `is_trading`、`is_suspended`、`is_st` |
| 血缘 | `source`、`source_record_id`、`ingested_at` |
| 质量 | `quality_status`、`quality_issues` |

`adjust_factor` 保存 BaoStock 的累计后复权因子，不保存会被未来公司行动重标的
`foreAdjustFactor`。以请求截止日 `T` 读取前复权序列时严格使用
`前复权价格(d,T) = 未复权价格(d) × backFactor(d) / backFactor(T)`，成交量和
成交额不缩放。指数不适用时为 `null`；股票因子缺失时也为 `null`，并把刷新降级为
`partial`。停牌日即使
BaoStock 返回前收盘价和零成交，也会标为停牌占位，不进入可交易宽度和成交额。
面向指标的前复权序列读取会直接排除停牌行；任何股票行缺少因子会返回数据质量错误，
不会生成部分复权序列。

### 全市场累计后复权因子引导

BaoStock 的 `query_daily_adjust_factor(date=T)` 是 T 日公司行动事件流，不是
全市场 T 日因子快照。第一次全市场刷新会对没有可信基线的正常交易股票逐一调用
`query_adjust_factor(..., end_date=T)`，并把每个成功结果的 `backAdjustFactor`
立即写入本机 `var/control/baostock_back_factor_cache.sqlite3`。成功且字段完整的
空历史响应是“截至 T 没有公司行动”的可审计证据，累计后复权因子为恒等值 `1`；
网络错误、字段错误或非有限值不能生成该证据，仍保持 `partial`。因此首次运行可以中断并安全续跑：
再次执行时只查询仍缺失的股票，不会从头开始，也不会把空结果替换成 `1`。

基线建立后，每个交易日只读取日事件流。未发生公司行动的股票沿用已验证的上一
交易日因子；发生事件的股票采用 BaoStock 当日明确报告的因子；漏跑多个交易日时
先按 BaoStock 交易日历逐日补齐事件流，再生成目标日快照。快照按目标交易日隔离，
较新日期的缓存不会用于较旧日期，避免未来数据进入历史复盘。

如果正向事件流已经从较新日期开始，而系统需要回填更早历史，最早回填日仍会按证券
各做一次 `query_adjust_factor(..., end_date=最早回填日)` 建立 point-in-time
基线。后续历史日只各调用一次 `query_daily_adjust_factor(date=历史日)`，然后从
任意更早的可信基线递推；不会再次逐证券 bootstrap。每日事件响应即使为空也会记录
“该日已完整观察”的证据，没有这条证据就禁止跨日沿用。快照同时保存因子的
`evidence_effective_date` 与 `evidence_observed_on`；事件必须满足
`effective_date <= 目标日` 且 `observed_on <= 目标日`。因此 2026 年才观察到的因子
值不能直接回填 2025 年，禁止用后来信息替代当时可得证据。BaoStock
`foreAdjustFactor` 会被后续事件重新归一化，不能仅靠 `observed_on` 修复，因此
canonical 和策略计算完全不使用该值。

因子缓存属于可重建的本机控制数据，不写入 NAS，也不进入 Git。需要从零重新引导
时，应先停止刷新进程，再删除该 SQLite 文件；下一次全市场刷新会重新逐证券获取。
在 0.5 秒请求间隔下，约 3,200 只股票仅限流等待就约 27 分钟，首次引导应预留
约一小时；完成后日常增量仍维持少量批量请求。

## 摘要状态

| 状态 | 语义 |
| --- | --- |
| `empty` | 本地没有刷新记录；不代表市场为零 |
| `ready` | 请求切片全部加载且关键质量字段完整 |
| `partial` | 个别证券或复权因子失败；返回成功子集及失败列表 |
| `stale` | 已发布数据早于后端交易日历计算的 `latest_expected_session` |
| `error` | 最近一次刷新整体失败；摘要不复用旧行生成市场结论 |

所有状态都包含 `as_of`、`source`、`completeness` 和 `freshness`。覆盖率按
`成功加载的期望证券数 / 期望证券总数` 计算，必须达到 100% 才是完整。完整刷新
通过当日证券列表建立期望主板集合；显式小切片则以用户给出的去重代码为期望集合。

新鲜度只与后端交易日历计算的期望交易日比较。调用方不能覆盖该日期。年度配置以
上交所、深交所年度休市公告为权威来源，BaoStock `query_trade_dates` 只做刷新前
机器校验。没有已确认年度配置时，工作日状态为 `unknown`，系统 fail closed 并返回
`calendar_unavailable/freshness_not_evaluated`，绝不按周一至周五猜测开市。
调用方必须先判断状态，再展示宽度或成交额。

## 刷新边界

CLI 只在显式调用时访问网络；GET、页面打开和状态查询永不抓取：

```bash
uv run python -m backend.app.cli refresh \
  --date YYYY-MM-DD \
  --symbols sh.600000 sh.000001

uv run python -m backend.app.cli refresh \
  --date YYYY-MM-DD \
  --all-main-board
```

BaoStock 官方说明当日日线约 17:30 后、复权因子约 18:00 后更新。后台自动化在
应用运行且 `STOCK_EVA_AUTO_REFRESH_ENABLED=true` 时采用固定、有限的本地计划：
18:10 首轮，18:40、19:20、20:10、21:00 退避重试，次日 07:15 最终校正。
应用关闭或电脑休眠时没有本地 SLA；启动和唤醒后的轮询会立即执行已错过的 due
任务。不会改用实时、分钟或其他 OHLCV 源。

每次 BaoStock 操作（含证券池分页）都应用统一最小间隔，默认 0.5 秒；单个操作最多
2 次尝试。真实客户端从 socket 建立连接、登录响应到后续读写都使用默认 30 秒超时，可通过
`STOCK_EVA_BAOSTOCK_SOCKET_TIMEOUT_SECONDS` 或 CLI 的
`--socket-timeout-seconds` 在 1–120 秒内调整。超时或普通网络异常会直接关闭损坏
socket、重新登录后再做有限重试；仅 BaoStock 明确错误、超时和系统网络错误会触发
恢复，代码或字段契约错误会立即失败。最终失败的证券保持缺失并继续处理下一只，已落盘
的因子引导进度不会回滚。进程中断和退出信号不会被重试逻辑吞掉。全市场 CLI 和
后台调度共享 `var/locks/market-refresh.lock` 非阻塞文件锁，
第二个进程只得到 `refresh_already_running`，不会并发访问数据源或 DuckDB。
进程日志只记录事件名、交易日、运行 ID、状态、覆盖数量和安全错误码，不记录持仓、
自选内容或 provider 原始响应；完整运行审计继续保存在 DuckDB。

Provider 在任何行情请求前调用 BaoStock 交易日历；非交易日不会继续请求日线。
显式刷新最多允许 20 个证券，每个请求最多尝试 2 次，真实客户端请求间隔至少
0.2 秒。全市场日线和证券列表使用 BaoStock 0.9.3 批量接口，只单独请求两个摘要
指数；只有首次因子基线或新增证券缺少基线时才逐股补齐。任一失败都会显式降级，
不会无界重试。

自动刷新采用 `staging -> validate -> published pointer`。抓取批次先作为进程内
staging 验证；partial/error 只写运行审计，不写 canonical 行。只有以下门槛全部
满足才原子写入 canonical 并发布：

- 期望主板证券覆盖率为 100%，无失败证券；
- 上证综指和深证成指均存在；
- 所有本地持仓和全部自选列表证券均有当日合格数据；
- 非停牌股票累计后复权因子完整，质量状态无 error；
- 策略只读取已经发布且因子完备的前复权序列。

partial/error 运行不会覆盖上一完整快照。`GET /api/v1/market/status` 返回
`market_phase`、`calendar_status`、`latest_expected_session`、
`published_as_of`、`refresh_state`、最近成功和下次重试时间，并声明能力为日终、
非实时。

## R2-F1 连续性扫描与修复队列边界

连续性只在配置 `STOCK_EVA_MARKET_CONTINUITY_START_DATE` 后生效。扫描的权威输入是
已确认的交易所日历与当前 manifest 引用的、经过路径/存在性/SHA-256/Parquet schema/
行数复核的不可变分区；本地可变 DuckDB 的 `daily_bars`、历史 manifest 目录和
`published_snapshots` 不能替代该证据。未配置起始日、日历未知或冲突、manifest/对象
校验失败时，整个扫描为 `unavailable`，不产生队列、provider、Parquet、manifest 或
pointer 写入。

新增 `GET /api/v1/market/status` 字段是兼容性 additive contract：
`continuity_status`（`current/gaps/blocked/unavailable`）、边界与缺口日期、待处理/
重试等待/租约中/dead-letter 计数、`active_lane`、`repair_execution_enabled` 和
allowlist `continuity_reason_code`。缺少或损坏连续性表只使这些字段降级，不会隐式
初始化或迁移数据库；基础行情控制 schema 损坏仍按既有 503 合同返回。状态读取只用
provider health 的 snapshot API，不会回收过期 HALF_OPEN 探测租约。

操作员可先做完全离线的计划读取：

```bash
uv run python -m backend.app.cli market-continuity \
  [--start YYYY-MM-DD] [--end YYYY-MM-DD]
```

该命令在 runtime 目录、写入型 store、pointer reconciliation 和 provider 构造之前
完成范围与严格 manifest 预检；输出缺口、`provider_requests=0` 以及所有写入标志为
false。`--execute` 只在同一锁内再次完整扫描并显式迁移本机连续性控制 schema、幂等写入
`repair_jobs`，仍是 enqueue-only：不 claim、不 lease、不创建 attempt、不请求 provider，
也不直接改 Parquet、manifest 或 pointer。计划或校验失败（包括日期倒置、越过配置边界、
未来日期和未知日历）即使带 `--execute` 也不创建 runtime 路径。

自动修复开关 `STOCK_EVA_MARKET_REPAIR_ENABLED` 默认关闭。关闭时连续性证据和状态仍可
读取，已记录的 job/attempt 不删除、不回退，正常 freshness lane 不受影响。队列的
`dead_letter` 不自动重开；只有严格 manifest reconciliation 能将已经发布的日期收敛为
`published`。真正的 provider repair、生产/NAS/LaunchAgent 操作不属于 R2-F1 离线交付，
必须另行授权和验收。

## 历史查询与导出

- `GET /api/v1/market/history/dates` 返回本地已有交易日。
- `GET /api/v1/market/history/{symbol}` 以单一 `source=baostock` 读取未复权或
  前复权序列，不跨来源填洞。
- `python -m backend.app.cli export --date YYYY-MM-DD` 从已发布数据生成只读交换导出，
  输出到本机 `var/staging/exports/date=YYYY-MM-DD/bars.parquet`；它不会修改 NAS
  manifest。正式 NAS 分区由发布流程自动维护。

单证券历史读取由 DuckDB 一次参数化查询完成，将 `symbol`、`source` 和日期区间
全部下推到 SQL；不会先加载每个交易日的全市场行再由 Python 过滤。API、策略回放和
股票详情都复用该查询。停牌排除、缺失复权因子报错、canonical 质量字段及前复权
计算仍由既有服务契约控制。

多证券策略/预警历史读取按最多 200 只证券为一批，使用 `symbol = ANY(?)`、
日期区间和 `source` 的参数化查询。超过单批上限时策略引擎分批执行，不退化为逐证券
打开 DuckDB。`present_points` 同样以精确交易日集合和证券集合在数据库内判断覆盖，
不会载入区间内的其他证券或日期。所有范围读取仍以请求截止日为上界，不引入未来数据。

canonical 日线写入使用单个事务内的一条集合化 `INSERT OR REPLACE`，通过列数组
`unnest` 交给 DuckDB 批量落表；相同 `(trade_date, symbol, source)` 仍幂等覆盖。
刷新结果与 published pointer 继续在同一事务提交，partial/error 不会发布。

2026-07-24 本机确定性基准使用 200 只证券、156 个日期（总计 31,200 行）：读取其中
20 只证券的 3,120 行由 20 次连接/查询降为 1 次，中位耗时由 0.1514 秒降为
0.0283 秒；稀疏覆盖判定的数据库返回行由 31,200 降为 4；2,000 行 canonical
幂等写入由 4,000 条语句降为 1 条，耗时由 0.5639 秒降为 0.0492 秒。时间仅用于
本机回归比较，连接、查询、语句和返回行计数是测试中的稳定约束。

正式运行时，NAS 上由 published manifest 引用的不可变 Parquet 分区是市场数据
真值；本机 DuckDB 是可重建的控制、审计和调度状态，不保存 NAS 历史行情的权威副本。
读取必须遵循 manifest，不能用目录 glob 把 partial、quarantine 或旧修订混入结果。
当前按交易日发布一个分区，后续只有真实扫描性能证明需要时才做离线月度合并。

2026-07-26 前已完成一个交易日的 BaoStock 全主板真实抓取、完整性门槛校验和 NAS
发布。覆盖约 3,190 只主板股票与 2 个摘要指数，复权因子也通过发布门槛。当前正在
执行约 260 个有效交易日的全主板历史回填；最终完成日期数、行数和容量以主任务结束
后读取 NAS manifest 的结果为准，文档不预填估算值。

策略指标已由严格 JSON AST 执行器实现，包括 SMA、EMA、RSI、`volume_ratio_5d`
以及上穿/下穿。指标只读取截止信号日的前复权有效交易日，不读取未来数据；停牌、
缺因子或质量异常的证券会被排除而不是生成部分可信信号。

## 已知限制

- BaoStock 是免费公共服务，没有项目可控制的可用性承诺。
- 年度日历配置需要随交易所下一年度休市公告更新；缺年配置会 fail closed。
- BaoStock 是机器校验和行情源，不替代交易所公告的权威性。
- NAS 历史回填仍在运行；完整的约 260 个交易日数据量、耗时和最终校验结果必须由
  manifest 与验收命令读回，不能从单日样本外推为已完成事实。
- AKShare 板块分类与资金流已具备独立数据集、离线契约和空态 API，但真实 canary
  上游不可用，published manifest 仍为空；可接受/拒绝能力、许可证和日期边界见
  [AKShare 补充数据审查](akshare-supplemental.md)。
- 免费数据源没有 SLA；补充源不可用不会阻断 BaoStock OHLCV 核心链路，也不能用
  OHLCV 推断资金净流入。
- 当前不提供实时/分钟行情、券商连接、自动交易或收益保证。
