# Stock EVA 工程架构

## 五阶段当前边界

阶段 1–5 的代码链路已经建立：BaoStock/NAS 行情、手动持仓与自选、策略 DSL 与
全市场扫描、工作台与幂等预警，以及收盘后编排、私有库备份和 LaunchAgent 资产。
系统只生成可解释的规则候选，不生成投资建议。当前运行重点是完成 260 个交易日的
NAS 历史回填；阶段 3/4 的真实 E2E 和 LaunchAgent 正式加载在回填完成后验收。

## 目录边界

```text
dashboard/            现有 HTML/CSS/JS 学习仪表盘
workspace/            独立复盘工作台，只通过版本化 API 读写
backend/app/          FastAPI 窄后端
  api/                版本化 HTTP 路由
  market/             Provider、规范化、控制状态、刷新与摘要
  regime/             R1-B 确定性市场状态、只读输入适配和版本化公式
tests/                后端契约测试
var/market/           本地开发行情或控制 DuckDB，内容不进入 Git
var/user/             本地 SQLite 用户数据，内容不进入 Git
var/control/          本机控制状态
var/staging/          本机发布 staging 与显式导出
var/locks/            本机跨进程锁
var/tmp/              本机 DuckDB spill 与临时文件
docs/                 学习资料与工程架构说明
```

## HTTP 契约

- `GET /api/v1/health`
  - 用于进程健康检查。
  - 返回固定服务标识和应用版本。
- `GET /api/v1/market/summary`
  - 返回 `empty`、`ready`、`partial`、`stale` 或 `error`。
  - 期望交易日由后端日历确定；拒绝调用方传入 freshness 参数。
  - 响应包含数据日期、来源、覆盖完整性、涨跌宽度、成交额概览和指数快照。
  - 空数据或失败响应中的零计数只是覆盖状态，不是市场结论。
- `GET /api/v1/market/status`
  - 纯本地只读，不触发行情网络请求。
  - 返回市场阶段、日历确认状态、期望交易日、发布指针、刷新/重试状态和能力边界。
- `GET /api/v1/analysis/market-regime?as_of=YYYY-MM-DD`
  - 只读重放战略/战术状态、五个版本化分项、置信度、证据和实际覆盖范围。
  - 当前主板加两条指数历史只能返回 narrow provisional，不能称为全 A 股结论。
  - 未来上海日期返回 422；缺失 DB/路径不创建任何文件或 schema。
- `/api/docs`
  - FastAPI 生成的开发期接口文档。

本地开发允许 `localhost:3000`、`localhost:8080` 和 `127.0.0.1:8080`
访问 API。部署时必须通过环境变量收窄允许来源。

## 组件边界

| 能力 | 计划组件 | 集成边界 |
| --- | --- | --- |
| 收盘后主板日线 | BaoStock | 已实现主数据提供器，不直接暴露给前端 |
| 市场与资金补充 | AKShare | 独立数据集已初始化；真实 canary 上游不可用时保持空 |
| 技术指标 | 内置确定性引擎 | SMA/EMA/RSI/量比，仅读取截止目标日的数据 |
| K 线图 | KLineChart | 前端展示层，不承担指标真值计算 |
| 绩效分析 | QuantStats | 接收本地收益序列 |
| 行情存储 | NAS manifest/Parquet + DuckDB | NAS 分区为行情真值；DuckDB 保存本机控制状态 |
| 用户状态 | SQLite | 保存手动持仓、自选和预警配置 |

数据提供器、指标计算、策略规则和用户数据必须保持解耦。任何外部数据源都不得
直接写入用户数据库；所有行情进入系统前必须经过字段规范化、交易日期检查和来源标记。

阶段 1 的 canonical 日线保存 BaoStock `adjustflag=3` 的未复权 OHLCV，并另存
累计后复权因子。停牌记录保留为占位行，但必须是 `is_suspended=true` 且带
`suspended_placeholder` 质量标记。摘要按单一来源切片计算，不拼接、平均或回填
其他来源的 OHLCV。后续指标只允许从未复权数据和因子确定性构造前复权序列。

阶段 1 的生产形态使用 NAS 上的不可变单日 Parquet 分区和原子 manifest 作为行情
真值。数据先在本机 staging 生成并校验，再上传 partial、从 NAS 读回核对 hash/schema/
行数、同共享 rename，最后切换 manifest。本机 DuckDB 只保存刷新审计、调度状态和
published pointer；本地开发模式仍可直接使用 DuckDB。请求截止日为 `T` 时，
前复权读取使用 `raw(d) × backFactor(d) / backFactor(T)`，避免 BaoStock
`foreAdjustFactor` 被未来公司行动重标造成数据泄漏；停牌和缺失因子都不能进入指标输入。

阶段 2 使用独立 SQLite 保存用户主动录入的持仓与自选；行情库与用户库之间没有外键
或复制。估值服务在请求时只读已发布市场数据，并以覆盖状态返回结果，不把行情写入
用户库。持仓采用版本字段乐观锁，SQLite 与其备份始终属于本地敏感数据。

阶段 3 的策略定义、不可变版本和运行结果也写入本地用户 SQLite；执行器只读
published manifest 指定的 `as_of_date` 及以前单一来源数据。JSON AST 验证、指标
计算、运行持久化和 API 保持分层，任何层都不接受源代码字符串或动态执行。

R1-B 市场状态不新增持久化数据库。SELECT-only reader 按请求 `as_of` 读取最近
130 个 canonical 交易日，构造 trend/breadth/liquidity/risk；R1-C 之前 leadership
保持 missing。公式、权重、阈值、范围 coverage 和输入内容 hash 均随响应返回，
同一输入可重复得到相同结果。详见 [R1-B 市场状态](market-regime.md)。

阶段 4 前半使用无构建的原生 HTML/CSS/JS 接入版本化 API。根入口将复盘工作台与
学习知识库分开；工作台不保存浏览器端副本、不内嵌示例行情，并原样呈现后端的
`empty`、`partial`、`stale`、`error` 状态。A 股红涨绿跌只承担视觉编码，文字标签
和数值符号始终同时提供。

阶段 4 后半的预警规则引用本地自选列表和不可变策略版本，不复制行情或策略 AST。
事件、幂等键及每次状态转换写入同一份本地用户 SQLite；评估继续只读已发布市场数据。
`eligible -> triggered` 只能由系统对明确交易日的合格收盘数据执行，用户操作仅能
确认、抑制或恢复评估资格。

第一版真实数据收口在本机控制 DuckDB 中保存回填运行与批次审计。普通小范围回填按
显式证券集合执行；全主板历史回填按交易日逐分区发布，使用确定性 request/run ID 和
NAS manifest 作为断点。已完成日期不会再次请求或覆盖，失败/部分日期不进入可见
manifest。BaoStock 全市场单日已真实 ready，260 个交易日历史回填正在执行。

本地自动化由官方年度休市公告配置决定 `latest_expected_session`，再用 BaoStock
交易日接口做机器校验。调度状态持久化到 DuckDB，进程启动与唤醒轮询会补跑错过的
任务。行情先写 canonical staging 并验证全覆盖、指数、本地持仓/自选证券、质量和
复权因子门槛；只有完整运行才能原子移动 published pointer，partial/error 仅留审计。
前端只读状态接口，GET 与页面生命周期不具备刷新副作用。

后台循环、显式全市场 CLI 和可选的一次性系统调度入口共享跨进程刷新锁。Provider
在每个真实请求和分页请求前统一节流；有限重试由 Provider 控制，时间退避由调度状态
控制。`auto-refresh-once` 使用后端时钟和持久化状态，不接受交易日参数，因此系统
自动化也不能把日期决定权重新交给脚本或前端。

单证券历史详情使用一次 DuckDB 参数化范围查询，过滤条件为
`symbol + source + trade_date BETWEEN start/end`。多证券策略与预警按最多 200 只
证券分批，每批使用一次参数化范围查询；覆盖判定也由 DuckDB 对精确交易日和证券集合
过滤，不在 Python 中物化无关行。canonical 写入在既有发布事务内使用单条集合化
`INSERT OR REPLACE ... SELECT unnest(...)`，保留主键幂等与原子 published pointer。
NAS 行情读取使用 manifest 明确列出的 Parquet，不扫描 `_staging` 或 quarantine。
只读 preflight 验证绝对路径、SMB 挂载、哨兵和 manifest；挂载不可用时，行情依赖
返回明确 503，而用户 SQLite API 继续工作。manifest 发布与本机 pointer 之间若发生
崩溃，续跑会校验最新 Parquet 后重建本机 ready 审计。详见
[NAS 市场数据集准备](nas-storage.md)。

AKShare 板块分类与报告资金流使用完全独立的哨兵、manifest、Parquet 和审计库，不得
覆盖 BaoStock OHLCV。数据集已经初始化；真实 canary 因上游当前不可用而安全失败，
所以已发布补充清单仍为空。

## 安全边界

- `.env`、数据库、Parquet、DuckDB、生成文件和临时目录不会进入 Git。
- `.env.example` 只能包含无秘密的示例值。
- 真实持仓和自选属于用户数据，不得放入示例、测试夹具或日志。
- API 空响应必须显式标记为空，禁止用虚构行情填充。
- 市场功能只提供研究和记录能力，不在 UI 或 API 中生成买卖建议。

## 全阶段工程验证

```bash
uv sync --extra dev
uv run --extra dev pytest
uv run --extra dev ruff check backend tests
```

本地开发启动：

```bash
uv run uvicorn backend.app.main:app --reload
```
