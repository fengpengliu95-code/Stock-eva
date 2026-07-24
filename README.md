# Stock EVA

Stock EVA 正在从 A 股量化学习仪表盘渐进演进为轻量的收盘后研究工作台。
第一版面向上交所、深交所 A 股主板，规划市场复盘、可组合策略选股、
手动持仓、自选和收盘后预警。

阶段 4 已在本地持仓与行情底座上增加独立的收盘复盘工作台，以及基于自选和不可变
策略版本的收盘后预警状态机；严格 JSON 策略 DSL 和确定性信号回放均通过现有 API
接入。现有 `dashboard/` 学习页面保持独立。当前版本不连接券商，不包含实时行情、
外部通知、自动交易、全市场实时扫描或投资建议。

## 工程快速开始

要求 Python 3.12 和 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync --extra dev
uv run uvicorn backend.app.main:app --reload
```

另开一个终端，从项目根目录提供静态页面：

```bash
python3 -m http.server 8080
```

打开 `http://127.0.0.1:8080/` 可分别进入 Stock EVA 复盘工作台与既有学习知识库。
工作台只读取 `http://127.0.0.1:8000/api/v1`；行情为空、部分、过期或失败时会保留
对应状态，不使用演示数字补位。持仓、自选和策略状态只写入本地 SQLite。

可用端点：

- `GET http://127.0.0.1:8000/api/v1/health`
- `GET http://127.0.0.1:8000/api/v1/market/status`
- `GET http://127.0.0.1:8000/api/v1/market/summary`
- `GET http://127.0.0.1:8000/api/v1/portfolio/positions`
- `GET http://127.0.0.1:8000/api/v1/portfolio/valuation`
- `GET http://127.0.0.1:8000/api/v1/watchlists`
- `POST http://127.0.0.1:8000/api/v1/strategies/validate`
- `GET http://127.0.0.1:8000/api/v1/strategies`
- `GET http://127.0.0.1:8000/api/v1/alerts/rules`
- `GET http://127.0.0.1:8000/api/v1/alerts/events`
- `http://127.0.0.1:8000/api/docs`

显式刷新一个已结束的交易日：

```bash
# 先用小范围验证网络和数据源
uv run python -m backend.app.cli refresh \
  --date 2026-07-23 \
  --symbols sh.600000 sz.000001 sh.000001 sz.399001

# 再按需刷新沪深主板和两个市场摘要指数
uv run python -m backend.app.cli refresh \
  --date 2026-07-23 \
  --all-main-board

# 上一条命令默认只输出全市场 dry-run；确认小样本成功后才可显式执行
uv run python -m backend.app.cli refresh \
  --date 2026-07-23 \
  --all-main-board \
  --execute-all-main-board

# 将本地已有的一个交易日导出为独立 Parquet 分区
uv run python -m backend.app.cli export --date 2026-07-23
```

受控回填最近 60 个有效交易日，默认先 dry-run：

```bash
uv run python -m backend.app.cli backfill \
  --symbols sh.600000 sz.000001 \
  --end 2026-07-23 \
  --effective-days 60 \
  --symbol-batch-size 2 \
  --date-batch-size 60 \
  --max-batches 1

# 检查计划后，以完全相同参数显式执行；重复执行会跳过已完成批次
uv run python -m backend.app.cli backfill \
  --symbols sh.600000 sz.000001 \
  --end 2026-07-23 \
  --effective-days 60 \
  --symbol-batch-size 2 \
  --date-batch-size 60 \
  --max-batches 1 \
  --execute

# 查看每日刷新及回填生成的覆盖审计
uv run python -m backend.app.cli refresh-runs
```

交易日和期望完成日由后端统一确定，前端与 API 调用方不能传入
`expected_date`。内置年度配置以交易所休市公告为权威来源，运行刷新前再用
BaoStock `query_trade_dates` 机器校验；缺少已确认年度配置的工作日会 fail closed。
`GET /api/v1/market/status` 只读返回市场阶段、日历状态、期望交易日、已发布日期、
刷新状态和日终非实时能力声明。

本地自动刷新是显式 opt-in。审阅配置和数据规模后，在 `.env` 设置：

```text
STOCK_EVA_AUTO_REFRESH_ENABLED=true
```

应用运行时会在 18:10 首次尝试，有限重试至 21:00，并在次日 07:15 校正；进程关闭
或电脑休眠期间不会运行，重新启动或唤醒后会立即补跑到当前应有状态。GET 请求和打开
页面永不触发抓取。只有全覆盖、摘要指数、本地持仓/自选证券、质量和非停牌股票复权
因子全部通过，才移动 published pointer；partial/error 保留上一完整快照。

历史日期和前复权序列端点：

- `GET /api/v1/market/history/dates`
- `GET /api/v1/market/history/sh.600000?start=2026-07-01&end=2026-07-23&adjustment=qfq`

运行验证：

```bash
uv run --extra dev pytest
uv run --extra dev ruff check backend tests
```

环境变量示例见 `.env.example`，工程边界与后续组件计划见
[工程架构](docs/architecture.md)，行情契约与降级语义见
[阶段 1 行情说明](docs/market-data.md)，本地持仓、自选、估值和备份边界见
[阶段 2 用户数据说明](docs/user-data.md)，策略 AST、指标和无未来数据语义见
[阶段 3 策略 DSL](docs/strategy-dsl.md)，工作台交互与状态边界见
[阶段 4 工作台说明](docs/workspace.md)，收盘后预警状态机见
[阶段 4 预警说明](docs/alerts.md)，真实数据回填、每日运行和验收边界见
[第一版真实数据就绪](docs/real-data-readiness.md)。

---

## 📈 既有量化操盘手学习知识库

<p align="center">
  <strong>一套系统化的量化交易学习知识库</strong><br/>
  <em>融合金融理论 · 技术分析 · Python量化 · 实战交易</em>
</p>

---

> [!TIP]
> 🎯 **目标读者**：零基础或初级投资者，希望通过系统学习，在 12 个月内建立完整的量化交易能力体系。

## 🌟 项目简介

本知识库是一个**面向中国 A 股市场**的量化交易学习资源合集，旨在帮助学习者从零开始，循序渐进地掌握：

- 📊 金融市场的底层逻辑与运行规律
- 📑 基本面分析与财务报表解读
- 📉 技术分析与图表形态识别
- 🐍 Python 编程与数据分析
- 🤖 量化策略开发、回测与优化
- 💰 实盘交易的风控与资金管理

我们相信，**优秀的交易者不是天生的，而是系统训练出来的**。本知识库将为你提供一条清晰、可执行的学习路线。

---

## 🗂️ 知识体系架构

本知识库共分为 **6 大模块**，覆盖从入门到实战的完整知识链条：

| 模块 | 名称 | 核心内容 | 预估学时 |
|:---:|:---|:---|:---:|
| 📘 模块一 | **市场基础与交易制度** | A 股交易规则、K 线基础、证券账户开通、交易软件使用 | ~30h |
| 📗 模块二 | **基本面分析** | 财务三表解读、估值方法（PE/PB/DCF）、行业分析框架 | ~40h |
| 📙 模块三 | **技术分析** | 经典图表形态、技术指标（MACD/KDJ/RSI/布林带）、量价关系 | ~50h |
| 📕 模块四 | **Python 量化编程** | Python 基础、Pandas/NumPy、数据获取（Tushare/AKShare）、可视化 | ~80h |
| 📓 模块五 | **量化策略开发与回测** | 经典策略（均值回归/动量/多因子）、Backtrader 回测框架、策略评估 | ~100h |
| 📔 模块六 | **实盘交易与风险管理** | 仓位管理、止损止盈、情绪控制、交易日志、持续迭代 | 持续 |

> [!NOTE]
> 预估学时基于每天 1-2 小时的学习节奏，实际进度因人而异。重要的是**保持节奏，持续积累**。

---

## 🛤️ 学习路线总览

```mermaid
graph TD
    Start["🚀 开始学习"] --> Phase1

    subgraph Phase1["🧱 筑基阶段（第 1-2 个月）"]
        direction LR
        M1["📘 市场基础<br/>交易制度"]
        M2["📗 基本面分析<br/>财报入门"]
        M1 --> M2
    end

    Phase1 --> Phase2

    subgraph Phase2["📈 进阶阶段（第 3-4 个月）"]
        direction LR
        M3["📙 技术分析<br/>图表与指标"]
        SIM["🎮 模拟交易<br/>实践验证"]
        M3 --> SIM
    end

    Phase2 --> Phase3

    subgraph Phase3["🤖 量化入门（第 5-8 个月）"]
        direction LR
        M4["📕 Python 编程<br/>数据分析"]
        M5["📓 策略开发<br/>回测优化"]
        M4 --> M5
    end

    Phase3 --> Phase4

    subgraph Phase4["💰 实战阶段（第 9-12 个月+）"]
        direction LR
        M6["📔 小资金实盘<br/>风控管理"]
        ITER["🔄 策略迭代<br/>持续学习"]
        M6 --> ITER
    end

    Phase4 --> Goal["🏆 稳定盈利体系"]

    style Start fill:#667eea,stroke:#764ba2,color:#fff
    style Goal fill:#f093fb,stroke:#f5576c,color:#fff
    style Phase1 fill:#e8f5e9,stroke:#4caf50
    style Phase2 fill:#fff3e0,stroke:#ff9800
    style Phase3 fill:#e3f2fd,stroke:#2196f3
    style Phase4 fill:#fce4ec,stroke:#e91e63
```

---

## 🧭 推荐学习路径

我们建议按照以下顺序进行学习，每个阶段都有明确的目标和里程碑：

### 阶段一：🧱 筑基（第 1-2 个月）

```
模块一（市场基础） ──▶ 模块二（基本面分析）
```

- ✅ 理解 A 股市场运行机制
- ✅ 能独立阅读上市公司财务报表
- ✅ 开通证券账户，熟悉交易软件

### 阶段二：📈 进阶（第 3-4 个月）

```
模块三（技术分析） ──▶ 开始模拟交易
```

- ✅ 掌握主流技术指标的原理与用法
- ✅ 在模拟账户中实践交易策略
- ✅ 建立个人的交易笔记习惯

### 阶段三：🤖 量化入门（第 5-8 个月）

```
模块四（Python 编程） ──▶ 模块五（策略开发与回测）
```

- ✅ 使用 Python 获取并分析股票数据
- ✅ 开发并回测至少 3 个量化策略
- ✅ 理解策略评估指标（夏普比率、最大回撤等）

### 阶段四：💰 实战（第 9-12 个月+）

```
模块六（实盘交易与风险管理） ──▶ 持续迭代
```

- ✅ 小资金实盘运行策略
- ✅ 建立完整的风控体系
- ✅ 持续优化策略，追求稳定盈利

> 📖 **详细路线图**：请参阅 [学习路线图](docs/00-学习路线图.md)

---

## 📂 目录结构

```
📁 Stock-evaluation/
├── 📄 README.md                          ← 你在这里
├── 📁 dashboard/
│   └── 📄 index.html                     ← 交互式学习仪表盘
├── 📁 docs/
│   ├── 📄 00-学习路线图.md                ← 详细学习路线与阶段规划
│   ├── 📁 01-股票市场基础/                ← 模块一：市场基础
│   ├── 📁 02-基本面分析/                  ← 模块二：基本面分析
│   ├── 📁 03-技术分析/                    ← 模块三：技术分析
│   ├── 📁 04-量化交易入门/                ← 模块四：量化交易入门
│   ├── 📁 05-风险管理/                    ← 模块五：风险管理
│   └── 📁 06-交易心理学/                  ← 模块六：交易心理学
└── 📁 resources/
    ├── 📁 data/                           ← 示例数据集
    ├── 📁 scripts/                        ← 实用脚本
    └── 📁 templates/                      ← 交易日志模板等
```

---

## 🖥️ 交互式学习仪表盘

我们提供了一个 **交互式学习仪表盘**，帮助你可视化学习进度、跟踪知识掌握情况。

🔗 **[点击打开仪表盘 →](dashboard/index.html)**

仪表盘功能包括：
- 📊 学习进度追踪
- 🗓️ 每日学习计划
- 📋 知识点检查清单
- 📈 学习时长统计

---

## 📚 如何使用本知识库

### 👶 如果你是零基础小白

1. 从 [学习路线图](docs/00-学习路线图.md) 开始，了解整体学习规划
2. 按照**筑基阶段**的内容，从模块一开始逐步学习
3. 每完成一个小节，在仪表盘中标记完成状态
4. 遇到不懂的概念，善用知识库内的交叉引用链接

### 🧑‍💻 如果你有一定编程基础

1. 快速浏览模块一、二的核心概念
2. 重点学习模块三（技术分析）
3. 直接进入模块四、五，开始量化编程和策略开发
4. 同步进行模拟交易，验证策略有效性

### 📊 如果你已有交易经验

1. 跳过基础模块，直接进入模块四（Python 量化）
2. 重点关注模块五的策略回测与优化
3. 结合模块六的风控体系，优化现有交易系统
4. 利用知识库查漏补缺，完善知识体系

---

## ⏱️ 时间投入参考

| 阶段 | 周期 | 每周建议学时 | 累计学时 | 关键产出 |
|:---:|:---:|:---:|:---:|:---|
| 🧱 筑基 | 第 1-2 月 | 8-10h | ~70h | 能看懂财报、理解市场 |
| 📈 进阶 | 第 3-4 月 | 8-10h | ~140h | 掌握技术分析、开始模拟盘 |
| 🤖 量化 | 第 5-8 月 | 10-15h | ~340h | 开发并回测量化策略 |
| 💰 实战 | 第 9-12 月+ | 10-15h | ~500h+ | 小资金实盘、策略迭代 |

> [!IMPORTANT]
> **质量 > 速度**。宁可每天学一小时但坚持一年，也不要三天打鱼两天晒网。交易是一场马拉松，不是短跑冲刺。

---

## 🌐 技术栈与工具

本知识库涉及的主要工具和技术：

| 类别 | 工具/技术 | 用途 |
|:---|:---|:---|
| 编程语言 | Python 3.10+ | 策略开发、数据分析 |
| 数据获取 | Tushare / AKShare | A 股历史数据获取 |
| 数据分析 | Pandas / NumPy | 数据处理与统计分析 |
| 可视化 | Matplotlib / Plotly | 图表绘制 |
| 回测框架 | Backtrader / Zipline | 策略回测 |
| 交易软件 | 同花顺 / 通达信 | 行情查看与交易 |
| 文档工具 | Markdown / Jupyter | 学习笔记与实验 |

---

## 🤝 贡献指南

欢迎对本知识库提出改进建议！你可以通过以下方式参与：

- 📝 **纠错与补充**：发现内容错误或有补充建议
- 💡 **策略分享**：分享你的量化策略思路
- 🔧 **工具推荐**：推荐好用的量化工具或数据源
- 📖 **学习心得**：分享你的学习经验与感悟

---

## ⚠️ 风险声明

> [!CAUTION]
> **投资有风险，入市需谨慎。**
>
> 1. 本知识库内容仅供**学习和研究**目的，不构成任何投资建议。
> 2. 股票投资存在**本金损失**的风险，请务必在充分了解风险的前提下做出投资决策。
> 3. 历史回测结果**不代表未来收益**，任何策略都无法保证盈利。
> 4. 建议使用**闲余资金**进行投资，切勿借贷炒股。
> 5. 本知识库作者**不对任何投资损失承担责任**。
>
> 请始终保持理性，做好风险管理，对自己的投资决策负责。

---

<p align="center">
  <strong>📈 愿你在量化交易的道路上，少走弯路，稳步前行。</strong><br/>
  <em>Stay Rational · Stay Disciplined · Stay Curious</em>
</p>

---

<sub>最后更新：2026 年 7 月 | 持续维护中 🚀</sub>
</p>
