# Stock EVA R2-F 数据可靠性版本说明

本文是基于 GitHub `main` 开始新会话、重新安装生产运行时或检查行情链路时的统一入口。
代码版本、已安装版本和数据版本是三个独立状态，任何会话都必须分别读回，禁止只凭文档
或分支名称推断生产已升级或行情已补齐。

## 版本定位

- 发布分支：GitHub `main`。
- 本地工程要求：Python 3.12、`uv`，前端构建还需要 `npm`。
- 产品边界：A 股主板收盘后研究；不提供实时行情、券商连接、自动交易或投资建议。
- Canonical 行情：未复权 OHLCV 加独立 `adjust_factor`；停牌证券保留受控占位行。
- 生产发布：Immutable Parquet、逐对象 SHA-256、Manifest、DuckDB pointer 和原子发布。
- 主要行情提供商：BaoStock。TickFlow Free 仅用于独立 shadow/discovery，未经资格验证前
  不自动接管 Canonical，也不进行 symbol-level 混源。

克隆或更新代码：

```bash
git clone git@github.com:fengpengliu95-code/Stock-eva.git
cd Stock-eva
git checkout main
git pull --ff-only origin main
git rev-parse HEAD
git rev-parse origin/main
```

两条 SHA 必须相同。跨对话时应把该 SHA 连同生产 `RELEASE.json` SHA 一起提供。

## 本版本更新

R2-F 在既有 Normalize、Quality Gate、Immutable Parquet、SHA-256、Manifest 和 Atomic
Publish 基础上增加了以下可靠性能力：

1. BaoStock 传输诊断：固定 endpoint、`refresh_id/provider_session_id/request_id`、socket
   层字节数、recv 次数、结束标志、协议阶段和统一错误分类。
2. Fail-closed 响应校验：短 header、EOF、坏压缩、分页停滞、重复页、缺失结束标志、
   重复 symbol、覆盖不足或异常提前结束均不能生成成功候选。
3. Endpoint 与 Provider 两级 Circuit Breaker，支持 `CLOSED/OPEN/HALF_OPEN` 和轻量探测。
4. Gap Scanner、持久化 Repair Queue、Freshness/Repair 双 lane 和进程重启后的 lease 恢复。
5. Provider RAW Evidence、候选/选择/lineage 合同、TickFlow Free shadow 和资格门禁。
6. 官方交易日历 generation、精确 session universe、只读 R2-F acceptance 与本地复制边界。
7. macOS 隔离 runtime 与 5 个 LaunchAgent；行情任务在 16:00 及晚间恢复窗口运行，并在
   次晨 07:15 校正。
8. 本次修复：历史日 `all_stock` 的 `tradeStatus=0` 停牌证券继续属于当日行情 expected
   universe，与 `daily_astock tradestatus=0` 占位行一致；未知状态仍 fail closed。元数据
   inspection 保持“仅活动证券”语义，不受此次修复影响。

## 工程运行与验证

```bash
uv sync --extra dev --frozen
uv run pytest -q
uv run ruff check backend tests
uv run ruff format --check backend tests
git diff --check
```

启动 API：

```bash
uv run uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

另一个终端启动静态工作台：

```bash
uv run python -m backend.app.static_server \
  --host 127.0.0.1 --port 8080 --directory .
```

常用只读接口：

```bash
curl -fsS http://127.0.0.1:8000/api/v1/health
curl -fsS http://127.0.0.1:8000/api/v1/storage/readiness
curl -fsS http://127.0.0.1:8000/api/v1/market/status
curl -fsS http://127.0.0.1:8000/api/v1/market/summary
```

## 生产安装与自动更新

安装器只从当前 Git 提交的 tracked 文件构建不可编辑 runtime。若本机行情镜像已经存在
且 NAS 未挂载，可显式复用本机数据；安装器仍会重新校验 manifest、每个 Parquet 的
SHA-256、schema 和行数。

```bash
scripts/stock_eva_launchagents_install.sh --check

scripts/stock_eva_launchagents_install.sh \
  --install --reuse-local-dataset

scripts/stock_eva_launchagents_status.sh
```

安装后必须读回精确版本：

```bash
python3 - <<'PY'
import json
from pathlib import Path

release = Path.home() / "Library/Application Support/Stock EVA/runtime/current/RELEASE.json"
print(json.loads(release.read_text())["git_sha"])
PY
```

行情 LaunchAgent 为 `com.finlay.stock-eva.refresh`，执行：

```text
python -m backend.app.cli auto-refresh-once --execute
```

调度时点为上海时间 `16:00、18:10、18:40、19:20、20:10、21:00、次晨 07:15`。
它是 one-shot 任务：空闲时 `launchctl` 显示无 PID 属正常现象；是否健康要同时检查 loaded、
最近退出码、日志、`/api/v1/market/status` 和 Canonical 日期。成功候选仍须通过完整 universe、
指数、用户证券、行情语义和非停牌股票复权因子门禁。

## 连续性与历史补洞

只读规划不会发 Provider 请求，也不会写控制表：

```bash
uv run python -m backend.app.cli market-continuity \
  --start 2025-07-01 --end YYYY-MM-DD
```

显式 `--execute` 只持久化经过验证的 repair jobs，不直接运行 Provider repair：

```bash
uv run python -m backend.app.cli market-continuity \
  --start 2025-07-01 --end YYYY-MM-DD --execute
```

真正的 repair 由同一个 `auto-refresh-once --execute` 在 Freshness lane 不 due、Provider
健康且锁可用时每次领取至多一个 session。不要通过循环人工重试碰运气；应检查 endpoint
和 protocol evidence，修复确定性阻断后再恢复调度。不得手工修改 Parquet、Manifest 或
pointer，也不得降低 coverage/factor/quality gate。

只读 R2-F 验收：

```bash
uv run python -m backend.app.cli r2f-acceptance \
  --start YYYY-MM-DD --end YYYY-MM-DD \
  --local-dataset-root \
  "$HOME/Library/Application Support/Stock EVA/data/market-dataset"
```

## 三层状态读回

每个新会话开始时按顺序记录：

1. GitHub：`git rev-parse origin/main`。
2. 已安装 runtime：`runtime/current/RELEASE.json` 的 `git_sha`。
3. 数据：`/api/v1/market/status` 的 `latest_expected_session`、`published_as_of`、
   `refresh_state`、continuity 和 repair counts。
4. Manifest：文件数、generation、manifest SHA-256、最新/最早交易日。
5. 调度：5 个 LaunchAgent loaded 状态、refresh 最近退出码和日志末尾。
6. Provider：本次 `refresh_id` 的 endpoint outcome、protocol stage、response bytes、
   end marker 和 circuit 状态。

GitHub `main` 更新不等于生产 runtime 自动升级；runtime 升级也不等于历史缺口自动清零。
只有三层证据全部匹配，才能声明对应范围 GO。

## 新对话启动模板

可把下面文字直接交给新的 Codex 会话：

```text
请基于 Stock EVA GitHub origin/main 的最新提交工作。先只读确认：
1. origin/main 与本地 HEAD SHA；
2. ~/Library/Application Support/Stock EVA/runtime/current/RELEASE.json 的 git_sha；
3. /api/v1/market/status；
4. canonical manifest 的 generation、SHA-256、文件数和日期范围；
5. repair queue 各状态数量；
6. com.finlay.stock-eva.refresh 的 loaded/last exit 状态；
7. 最近 refresh_id 的 endpoint/protocol evidence。
不得把 GitHub、安装版本或数据日期互相代替，不得手工改 Parquet/manifest/pointer，
不得降低质量门或为了碰运气重试 Provider。先报告差异，再继续任务。
```

更完整的安装边界见 [macOS LaunchAgents](../macos-launchagents.md)，数据源与质量门见
[行情说明](../market-data.md)，R2-F 总体路线见
[数据可靠性路线图](../plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md)。
