# 第一版真实数据就绪与本地验收

## 安全原则

- 普通历史回填只接受显式证券集合，最多 20 只；
- 默认是 dry-run，只有 `--execute` 才抓取历史行情；
- 证券与交易日分别分块，计划超过 `--max-batches` 时在网络请求前拒绝；
- BaoStock 请求有最小间隔和有限重试，不进行长时间不可控循环；
- 全市场每日刷新默认只输出网络零写入计划，必须增加
  `--execute-all-main-board` 才执行；
- `--inspect-universe` 只查询交易日与证券池元数据，30 秒硬超时，不请求 OHLCV；
- 所有 provider 操作与分页统一限流，正常最小间隔 0.5 秒、最多 2 次尝试；
- 后台与 CLI 共享非阻塞跨进程锁，禁止两个全市场刷新重叠；
- NAS 全主板历史回填默认只生成计划，必须增加
  `--execute-all-main-board-history` 才按交易日逐分区发布；
- 历史回填分区发布到已验证的 NAS 归档；macOS 后台从 manifest/hash 校验后原子建立
  的 `Application Support` 本机镜像读取和发布日增量；
- 运行记录、控制状态和用户状态只写本机可变目录，均被 `.gitignore` 排除。本机镜像
  不可用时 fail closed，不会静默回退 NAS 或创建伪数据集。

## 历史窗口

`minimum_effective_days` 根据白名单策略 AST 估算最小有效交易日：

- SMA/EMA：周期本身；
- RSI：周期加 1；
- 收盘量比：6 个有效日，即当日加前 5 日；
- 上穿/下穿：最长操作数窗口再加 1 个前序有效日；
- `and/or/not`：递归取实际所需最长窗口。

例如 MA5 上穿 MA20 至少需要 21 个有效交易日。实际第一版验收采用 60 个有效日，
为指标热身和受控回放留出余量。

## 受控回填

先生成精确交易日历计划：

```bash
uv run python -m backend.app.cli backfill \
  --symbols sh.600000 sz.000001 \
  --end 2026-07-23 \
  --effective-days 60 \
  --symbol-batch-size 2 \
  --date-batch-size 60 \
  --max-batches 1
```

dry-run 会进行一次 BaoStock 交易日历查询，但不会抓取证券行情或写回填记录。输出
包含交易日、证券、批次数、数据点数量和预计请求上限。确认后增加 `--execute`。

相同参数生成确定性 request key 和 run ID。执行时只跳过状态为 ready 的批次；
error 或 partial 批次会在下次同参数执行时重试。每个批次保存请求点数、已加载点数、
安全错误码和完成时间，最终报告整体覆盖率。

可用 `--strategy-id` 与 `--strategy-version` 绑定已有策略版本。CLI 会比较计划的
有效交易日数量与 AST 的最小窗口，不足时在行情请求前拒绝。

## NAS 全主板历史回填

该入口只适用于已经初始化且通过 readiness 校验的 NAS 数据集。先生成计划：

```bash
STOCK_EVA_NAS_MARKET_DATASET_ROOT=/Volumes/Stock/stock-eva-market \
  uv run python -m backend.app.cli full-market-backfill \
  --start 2024-01-01 \
  --end 2026-07-23
```

默认 dry-run 只请求一次 BaoStock 交易日历，不请求证券池、日线、复权因子或指数行情，
不写行情分区或 NAS manifest；CLI 仍可能初始化本机 runtime 目录与控制库 schema。
输出包含确定性 request/run ID、有效交易日、已发布/待发布数量及 provider 请求次数
下界。`--max-sessions` 默认 3,000，是交易日数量的网络前硬上限；扩大范围必须显式
提高该值。

真实执行还必须增加 `--execute-all-main-board-history`。服务按交易日从旧到新执行，
每个交易日独立使用全主板证券池、两个摘要指数、质量和复权因子硬门槛。只有 ready
日期才通过同共享原子 rename 和 manifest 切换发布；partial/error 只留本机审计，
不会产生可见 NAS 分区。每轮开始前重新读取 manifest，已经发布的不可变日期会跳过，
因此中断后以相同参数重启不会重复抓取或覆盖完成分区。

整个执行持有现有全局刷新锁，不得与每日刷新并行。第一次最旧日期可能触发全量复权
因子 bootstrap，耗时显著高于后续日增量；计划中的请求数仅为不含该 bootstrap 的
下界，不是完成时间承诺。历史修复旧日期不会把当前 published pointer 倒退到过去。
以约 3,190 只股票、260 个历史交易日为例，严格 point-in-time 因子路径约为最早日
3,190 次逐股 baseline 请求，加后续每个历史日一次 daily event 请求；不会退化为
260 × 3,190 次逐日逐股请求。
若进程在 NAS manifest 发布后、本机 pointer 写入前中断，续跑会先校验最新不可变
Parquet 的 hash/schema/行数，再从 manifest 重建本机 ready 审计和 pointer。

本轮历史回填已经完成。2026-07-28 对 NAS published manifest 和引用对象的真实读回
结果为：

- 260 个有效交易日；
- 829,494 行 canonical 日线；
- 最新交易日 2026-07-24；
- NAS 占用约 38 MB；
- manifest、SHA-256、Parquet schema 和逐文件行数校验通过。

NAS 现作为这一批已验证历史的归档源；以上数字不含本机用户 SQLite、刷新审计、
策略、预警或备份。

## 每日收盘刷新

小范围每日刷新：

```bash
uv run python -m backend.app.cli refresh \
  --date 2026-07-23 \
  --symbols sh.600000 sz.000001
```

每日 request key 由来源、交易日和排序去重后的目标集合组成。相同请求会复用同一个
run ID，并以 DuckDB 主键覆盖相同 canonical 行，不产生重复运行记录或 K 线。
运行结果明确返回 ready、partial 或 error、请求/成功数量、失败证券和覆盖率。

查看最近审计：

```bash
uv run python -m backend.app.cli refresh-runs
```

全市场安全计划：

```bash
uv run python -m backend.app.cli refresh \
  --date 2026-07-23 \
  --all-main-board \
  --inspect-universe
```

不加 `--inspect-universe` 时 dry-run 完全无网络；增加后只读取 BaoStock 交易日和
主板证券池，不请求日线、因子或指数行情，也不写 DuckDB/SQLite。2026-07-24 对
2026-07-23 的元数据预检结果：

- 主板股票 3,191 只（上海 1,698、深圳 1,493），加 2 个摘要指数，共 3,193 个目标；
- 证券池分页后实际元数据请求 5 次，耗时 23.373 秒；
- 推算完整执行正常约 10 次 provider 请求，2 次有限尝试的理论上限 20 次；
- 按元数据速度外推的执行时间下限约 46.7 秒；它只是上线前的容量下界，不是免费
  数据源 SLA。

随后已在人工监督下完成一个交易日的全主板真实抓取、质量校验和 NAS 原子发布；
证券池、两个摘要指数、OHLCV、复权因子与 100% 可解释覆盖门槛均通过。以下命令
现保留为诊断和人工恢复入口，不再是“首次尚未执行”的步骤：

```bash
uv run python -m backend.app.cli refresh \
  --date 2026-07-23 \
  --all-main-board \
  --execute-all-main-board
```

执行前确认没有其他刷新或历史回填进程；执行中可中断进程，未通过完整门槛的批次
不会移动 published pointer。结束后仍需检查 `/api/v1/market/status`、
`refresh-runs`、覆盖率、两指数和复权因子门槛。

260 日历史窗口已经满足 MA5/MA20、量比等第一版策略的真实扫描数据门槛。每日刷新
继续使用同一完整性规则；在 LaunchAgent 生产配置中，发布目标是本机 immutable
dataset 镜像，而不是由后台进程直接写 NAS。

## 自动运行与运维

### 应用持续运行

设置 `STOCK_EVA_AUTO_REFRESH_ENABLED=true` 后，FastAPI 启动即检查 catch-up，进程
运行期间按 18:10、有限退避至 21:00、次日 07:15 的后端计划执行。电脑休眠或应用
退出期间没有本地执行保证；唤醒或重启后的下一次轮询会补跑。

### 应用不持续运行

一次性入口默认只输出后端计划、零网络：

```bash
uv run python -m backend.app.cli auto-refresh-once
```

系统自动化获授权后可在既定时点调用：

```bash
uv run python -m backend.app.cli auto-refresh-once --execute
```

该入口不接受交易日，复用后端日历、发布指针、重试状态、幂等门槛和刷新锁。

项目已实现 5 个 macOS LaunchAgent 模板及安装、状态和卸载脚本，覆盖 API、Web、
日终刷新、交易日历同步和私有 SQLite 备份。真实验证发现 macOS TCC 拒绝 launchd
后台进程访问 `/Volumes/Stock`，因此安装器会：

1. 从 Git tracked 文件构建 `Application Support` 下的隔离生产运行时；
2. 交互式校验 NAS manifest/hash/schema/行数；
3. 在本机 staging 复制并复核，逐个发布不可变分区，最后原子切换 manifest；
4. 让 5 个 LaunchAgent 只引用本机运行时、配置、数据和镜像路径。

镜像和运行时已经正式安装并完成读回：5 个 LaunchAgent 已加载，API/Web 正常，
storage readiness 为 `local_dataset/ready/local`，日历、日终刷新和私有备份最近
执行均成功。操作边界见
[macOS LaunchAgents](macos-launchagents.md)。

### 日志与故障处理

- 进程 stderr 输出 `market_refresh_decision`、`market_publication_started`、
  `market_publication_finished`、`market_publication_failed` 和锁冲突事件；
- 日志仅含运行 ID、交易日、状态、覆盖数量和安全错误码，不含 provider 原始内容或
  用户证券集合；
- DuckDB `refresh_runs`、调度状态和 published pointer 是可读回的审计真值；
- unexpected loop failure 会记录安全错误并在下一轮继续，不会让后台任务永久退出；
- `partial/error` 只保留运行审计，不写 canonical、不覆盖最后完整发布。

## 2026-07-24 本地真实小样本验收

验收数据位于临时目录，不进入项目或版本控制：

- BaoStock 交易日历解析出截至 2026-07-23 的连续 60 个有效交易日；
- 2 只主板证券共请求 120 个数据点，加载 120 个，覆盖率 100%；
- 同参数恢复运行保持相同 run ID，已完成历史批次没有再次抓取；
- 60 条前复权有效序列可读，日期严格止于指定交易日；
- 市场摘要在显式 expected date 下为 ready，来源为 BaoStock；
- 本地手动持仓估值覆盖 1/1；
- SMA5 与 SMA20 条件可对 2 只证券完成真实历史计算；
- 自选预警完成收盘后评估并保留来源、交易日和质量信息；
- 单日显式刷新覆盖 2/2，并生成 `run_kind=daily` 的幂等审计记录。

这些结果证明小样本本地日终链路可运行，不代表策略有效性或投资结论。

## 全阶段真实验收

交互式验收 CLI 可直接检查 NAS 归档或本机发布镜像的 manifest、不可变 Parquet、
历史窗口、持仓估值、全市场策略扫描和预警幂等性：

```bash
uv run python -m backend.app.acceptance \
  --local-dataset-root \
  "$HOME/Library/Application Support/Stock EVA/data/market-dataset"
```

验收过程只读指定的已验证行情镜像，在临时 SQLite 中建立合成持仓、自选、策略和
预警，结束后删除临时用户库；不会读取或修改真实持仓。LaunchAgent 的最终验收则应
确认 `mode=local_dataset`、`serving_source=local`。策略输出统一标为“规则候选”，
不构成投资建议。完整口径见 [端到端真实验收](acceptance-e2e.md)。

## 免费数据源限制与生产化缺口

- BaoStock 免费服务没有可用性 SLA，登录、日历、历史或复权接口可能临时失败；
- 当日日线通常在约 17:30 后、复权因子约 18:00 后可用，建议 18:10 后显式刷新；
- 停牌行保留为占位并从指标排除；缺复权因子或质量异常会降级；
- 退市、长期停牌和证券代码生命周期仍需更完整的证券主数据验证；
- 当前已有本地后台调度和安全日志，但没有外部失败通知、跨设备同步或集中监控；
- 260 个交易日、829,494 行、约 38 MB 的 NAS 历史归档已经完成并校验；
- 本机发布镜像已通过 2026-07-27 全市场日终刷新增量到 261 个交易日、832,687 行；
- LaunchAgent 隔离运行时和本机镜像已正式安装，5 个 agent 与端口/readiness 已读回；
- 2026-07-27 日终刷新 3,193/3,193、覆盖率 100%，重复调度为幂等 no-op；
- NAS 归档和本机镜像都可从免费源重建，不做复杂多副本；本机私有 SQLite 使用一致性
  快照备份。免费源不可用时保留最后完整发布，不混用不完整行情。
