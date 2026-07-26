# 交易日历持续维护

Stock EVA 对交易日历采取 **官方公告为权威、BaoStock 为机器观察** 的
fail-closed 模型。前端不提交日期，也不能用周一至周五规则推断开市。

## 权威配置

仓库内的 `backend/app/market/calendars/cn_a_share_*.json` 是版本化权威配置。
目前确认：

- 2025：上交所 `上证公告〔2024〕38号`，深交所
  `深证会〔2024〕413号`。
- 2026：上交所 `上证公告〔2025〕45号`，深交所
  `深证会〔2025〕481号`。

每份配置保存交易所、公告标题、公告号、发布日期和官方 URL。没有配置的年份
返回 `unknown`；工作日不会被自动标成开市。周末可以确定为休市，但不能据此
推导未知年份的工作日。

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
