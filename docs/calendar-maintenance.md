# 交易日历持续维护

Stock EVA 对交易日历采取 **官方公告为权威、BaoStock 为机器观察** 的
fail-closed 模型。前端不提交日期，也不能用周一至周五规则推断开市。

## 权威配置

仓库内的 `backend/app/market/calendars/cn_a_share_*.json` 是版本化基线。R2-F4.1
可以在独立的 `calendar_generations.sqlite3` 中追加经核验的年度 generation；它不会
改写这些 JSON。启用 runtime 后，reader 每次外部操作只装载一个完整 snapshot，组合
基线与最新 promoted chain。
目前确认：

- 2025：上交所 `上证公告〔2024〕38号`，深交所
  `深证会〔2024〕413号`。
- 2026：上交所 `上证公告〔2025〕45号`，深交所
  `深证会〔2025〕481号`。

每份配置保存交易所、公告标题、公告号、发布日期和官方 URL。没有基线或 promoted
generation 的年份返回 `unknown`；未知年份的所有日期（包括周末）均保持 unknown，
不得用工作日或周末规则补全未覆盖年份。

## 年度 source package

source package 是不超过 256 KiB、禁止重复 JSON key 的 schema v1 文件。它必须按
`SSE, SZSE` 顺序各含一份同年度完整日历，并记录经人工复核的公告 URL、公告号/标题、
发布日期、原始 HTTP body SHA-256、工作日休市日期、提取复核人和复核时间。两个
schedule 独立计算 `schedule_sha256`，外层再计算 `source_sha256`；摘要必须使用项目的
domain-separated canonical JSON helper 生成，不能手工猜写。结构示意如下（摘要为
占位符，不可直接执行）：

```json
{
  "schema_version": 1,
  "rule_version": "cn-a-share-weekends-closed-v1",
  "year": 2027,
  "schedules": [
    {
      "exchange": "SSE", "year": 2027,
      "coverage_start": "2027-01-01", "coverage_end": "2027-12-31",
      "title": "reviewed title", "notice_no": "reviewed notice number",
      "official_url": "https://www.sse.com.cn/reviewed/exact/path",
      "published_on": "2026-12-20", "body_sha256": "<64 lowercase hex>",
      "closed_dates": ["2027-01-01"], "review_id": "operator-review-id",
      "reviewed_on": "2026-12-21T09:00:00+08:00",
      "schedule_sha256": "<computed 64 lowercase hex>"
    },
    {
      "exchange": "SZSE", "year": 2027,
      "coverage_start": "2027-01-01", "coverage_end": "2027-12-31",
      "title": "reviewed title", "notice_no": "reviewed notice number",
      "official_url": "https://investor.szse.cn/reviewed/exact/path",
      "published_on": "2026-12-20", "body_sha256": "<64 lowercase hex>",
      "closed_dates": ["2027-01-01"], "review_id": "operator-review-id",
      "reviewed_on": "2026-12-21T09:00:00+08:00",
      "schedule_sha256": "<computed 64 lowercase hex>"
    }
  ],
  "source_sha256": "<computed 64 lowercase hex>"
}
```

操作员应分别复核交易所公告位置和内容，保存未经文本归一化的 HTTP 解码后 body 摘要，
人工提取工作日休市日期并独立复核，再使用项目模型/helper 构造并校验摘要。搜索结果、
猜测 URL、机器日历或单一交易所公告都不能成为 authority。

```bash
.venv/bin/python -m backend.app.cli calendar-generation-stage --source /absolute/reviewed.json
.venv/bin/python -m backend.app.cli calendar-generation-stage --source /absolute/reviewed.json --execute
.venv/bin/python -m backend.app.cli calendar-generation-status
.venv/bin/python -m backend.app.cli calendar-maintenance
.venv/bin/python -m backend.app.cli calendar-maintenance --execute
```

不带 `--execute` 的 stage/maintenance 是零写、零网络计划。stage execute 只写 calendar
control，不请求 provider 或 promotion。maintenance execute 仅在已有安全 generation
store、已有 CLOSED health snapshot、未消费的 `(year, Shanghai date)` slot 和合格 source
同时成立时，最多请求两份固定官方 body 与一次 BaoStock `trade_dates`；`max_attempts=1`，
不会 retry、探测或关闭 circuit。成功也只推进 calendar head，不写行情 Parquet、manifest、
pointer，不触发 shadow 或 secondary failover。

## 启用、策略和状态

`STOCK_EVA_CALENDAR_RUNTIME_ENABLED=false` 是默认值。生产启用前必须先建立并核验 control
store、stage reviewed source、完成 dry-run 与受控 maintenance；仅把变量改为 true 不会
创造权威。下一年在 10 月 1 日前为 `not_due`，10 月 1 日至 12 月 14 日为 `pending`，
12 月 15 日起为 `action_required`；缺当前年始终 blocking。策略状态本身不会使未来日期可用。

安全结果包括 `SOURCE_PENDING`、`SOURCE_CONFLICT`、`SKIPPED_CIRCUIT_OPEN`、
`ALREADY_ATTEMPTED`、`PARENT_CHANGED`、`HISTORY_CHANGE` 和
`CONTROL_STATE_UNAVAILABLE`。它们不得用旧 authority 或机器观察冒充 promotion。
只读 CLI 与 `GET /api/v1/market/calendar-generation` 不初始化、不迁移、不修复 store。

## 机器观察与隔离

`calendar-sync` 调用 BaoStock `query_trade_dates`，逐日保存 provider
观察，并与已配置年度逐日对账：

- 一致：记录为 `ready`，可更新 last-known-good。
- 范围完全属于未知年份：只记 `observed_only`，不升级为权威日历。
- 任一官方/provider 不一致：整次运行 `quarantined`，保存冲突日期，
  不替换 last-known-good。

本地控制库默认为 `var/control/calendar_sync.sqlite3`，保存公告配置 SHA-256、
抓取时间、请求范围、来源元数据、逐日观察、冲突和下一次计划时间。它不包含
行情或用户持仓。

## 调度

- 每月 1 日 04:05（Asia/Shanghai）：过去 24 个月至当天的完整机器对账。
- 应用/任务启动：当天一次轻量检查。
- 每日 16:30：当天一次轻量检查。

CLI 默认只做零网络、零写入计划：

```bash
uv run python -m backend.app.cli calendar-sync
```

显式执行：

```bash
uv run python -m backend.app.cli calendar-sync --startup --execute
uv run python -m backend.app.cli calendar-sync --mode full --execute
```

可以用 `--start YYYY-MM-DD --end YYYY-MM-DD` 限定受控验收范围；两个参数必须
同时给出。网络错误只返回安全错误码，不删除或覆盖 last-known-good。

旧版 `calendar_sync.sqlite3` 只允许 writer 在 execute 路径做一次受控结构迁移。迁移保留
原 inode backup、in-progress guard 与完成 evidence，并把 migration ID 和原 guard
device/inode 绑定到不可变 identity。缺失、替换或不匹配的完成 evidence 会使后续 reader
fail closed。CLI 字段 `writes_calendar_state=false` 仅表示没有新增业务 sync run/state；
它不承诺 execute-time legacy control DB 的 inode、schema 或字节完全不变。

## 故障恢复

不要删除 control store、guard、`.removed` evidence、backup 或直接编辑 SQLite 来“恢复”，
也不要回退 head。先停用 runtime，保留整个 control/lock 目录的只读副本和权限/mtime，
记录安全 outcome；用只读 status 判断是 source、health、slot、parent、history 还是 migration
evidence 问题。恢复必须从已核验 authority 在独立临时根重放并完成复核，再经单独受控窗口
替换；本版本不自动 rebase、repair 或清理证据。

`GET /api/v1/market/status` 只读暴露最近尝试、最近成功、冲突时间和下一次
计划；它不触发同步，也不接受客户端日期。

## 官方来源

- 上交所 2025：
  <https://big5.sse.com.cn/site/cht/www.sse.com.cn/disclosure/announcement/general/c/c_20241223_10767108.shtml>
- 深交所 2025：
  <https://investor.szse.cn/disclosure/notice/general/t20241223_611283.html>
- 上交所 2026：
  <https://www.sse.com.cn/disclosure/announcement/general/c/c_20251222_10802507.shtml>
- 深交所 2026：
  <https://investor.szse.cn/disclosure/notice/general/t20251222_618087.html>
