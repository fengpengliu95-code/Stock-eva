# Stock EVA 工程架构

## 阶段 0-1 目标

阶段 0 只建立可验证的工程边界，让现有静态学习仪表盘与未来 API 渐进共存。
阶段 1 在该边界内增加 BaoStock 收盘后日线摄取、DuckDB 行情存储和市场摘要。
当前版本不运行策略、不保存真实持仓，也不生成投资建议。

## 目录边界

```text
dashboard/            现有 HTML/CSS/JS 学习仪表盘，阶段 0 保持原样
workspace/            独立复盘工作台，只通过版本化 API 读写
backend/app/          FastAPI 窄后端
  api/                版本化 HTTP 路由
  market/             Provider、规范化、DuckDB、刷新与摘要
tests/                后端契约测试
var/market/           未来 DuckDB/Parquet 行情数据，内容不进入 Git
var/user/             本地 SQLite 用户数据，内容不进入 Git
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
- `/api/docs`
  - FastAPI 生成的开发期接口文档。

本地开发允许 `localhost:3000`、`localhost:8080` 和 `127.0.0.1:8080`
访问 API。部署时必须通过环境变量收窄允许来源。

## 后续组件边界

以下组件已选定，但阶段 0 尚未安装或调用：

| 能力 | 计划组件 | 集成边界 |
| --- | --- | --- |
| 收盘后主板日线 | BaoStock | 已实现主数据提供器，不直接暴露给前端 |
| 市场与资金补充 | AKShare | 后续受控补充；阶段 1 不参与 OHLCV |
| 技术指标 | `ta` | 接收规范化 OHLCV，不访问网络 |
| K 线图 | KLineChart | 前端展示层，不承担指标真值计算 |
| 绩效分析 | QuantStats | 接收本地收益序列 |
| 行情存储 | DuckDB/Parquet | DuckDB 为本地真值；已支持显式单日 Parquet 导出 |
| 用户状态 | SQLite | 保存手动持仓、自选和预警配置 |

数据提供器、指标计算、策略规则和用户数据必须保持解耦。任何外部数据源都不得
直接写入用户数据库；所有行情进入系统前必须经过字段规范化、交易日期检查和来源标记。

阶段 1 的 canonical 日线保存 BaoStock `adjustflag=3` 的未复权 OHLCV，并另存
前复权因子。停牌记录保留为占位行，但必须是 `is_suspended=true` 且带
`suspended_placeholder` 质量标记。摘要按单一来源切片计算，不拼接、平均或回填
其他来源的 OHLCV。后续指标只允许从未复权数据和因子确定性构造前复权序列。

阶段 1 收口已增加单日 Parquet 显式导出，但 DuckDB 继续作为本地真值；没有引入
自动分区编排。前复权读取由 canonical 未复权价格乘 BaoStock 前复权因子生成，
停牌和缺失因子都不能进入指标输入。

阶段 2 使用独立 SQLite 保存用户主动录入的持仓与自选；行情库与用户库之间没有外键
或复制。估值服务在请求时只读 DuckDB 收盘数据，并以覆盖状态返回结果，不把行情写入
用户库。持仓采用版本字段乐观锁，SQLite 与其备份始终属于本地敏感数据。

阶段 3 的策略定义、不可变版本和运行结果也写入本地用户 SQLite；执行器只读 DuckDB
中 `as_of_date` 及以前的单一来源数据。JSON AST 验证、指标计算、运行持久化和 API
保持分层，任何层都不接受源代码字符串或动态执行。

阶段 4 前半使用无构建的原生 HTML/CSS/JS 接入版本化 API。根入口将复盘工作台与
学习知识库分开；工作台不保存浏览器端副本、不内嵌示例行情，并原样呈现后端的
`empty`、`partial`、`stale`、`error` 状态。A 股红涨绿跌只承担视觉编码，文字标签
和数值符号始终同时提供。

阶段 4 后半的预警规则引用本地自选列表和不可变策略版本，不复制行情或策略 AST。
事件、幂等键及每次状态转换写入同一份本地用户 SQLite；评估继续只读 DuckDB。
`eligible -> triggered` 只能由系统对明确交易日的合格收盘数据执行，用户操作仅能
确认、抑制或恢复评估资格。

第一版真实数据收口在 DuckDB 中增加回填运行与批次审计。回填计划由 BaoStock
交易日历、显式证券集合、有效日批次和证券批次确定；确定性 request key 使相同计划
可以恢复。已完成批次不会再次请求，失败或部分批次可在相同 run ID 下重试。每个
交易日另有聚合覆盖记录，供市场摘要和每日审计读取。

本地自动化由官方年度休市公告配置决定 `latest_expected_session`，再用 BaoStock
交易日接口做机器校验。调度状态持久化到 DuckDB，进程启动与唤醒轮询会补跑错过的
任务。行情先写 canonical staging 并验证全覆盖、指数、本地持仓/自选证券、质量和
复权因子门槛；只有完整运行才能原子移动 published pointer，partial/error 仅留审计。
前端只读状态接口，GET 与页面生命周期不具备刷新副作用。

## 安全边界

- `.env`、数据库、Parquet、DuckDB、生成文件和临时目录不会进入 Git。
- `.env.example` 只能包含无秘密的示例值。
- 真实持仓和自选属于用户数据，不得放入示例、测试夹具或日志。
- API 空响应必须显式标记为空，禁止用虚构行情填充。
- 市场功能只提供研究和记录能力，不在 UI 或 API 中生成买卖建议。

## 阶段 0-1 验证

```bash
uv sync --extra dev
uv run --extra dev pytest
uv run --extra dev ruff check backend tests
```

本地开发启动：

```bash
uv run uvicorn backend.app.main:app --reload
```
