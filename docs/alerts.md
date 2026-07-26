# 阶段 4 收盘后预警

## 规则边界

预警规则只保存：

- 本地自选列表 ID；
- 已保存策略 ID 与不可变版本；
- 用户自定义规则名称。

首版指标条件继续由策略版本中的严格 JSON AST 表达，支持既有 SMA、EMA、RSI 和
`volume_ratio_5d` 白名单，不增加动态代码或第二套表达式语言。规则不复制行情，
不连接券商，也不包含下单含义。

## 收盘评估与幂等

评估必须显式提供不晚于今天的 `signal_date`。执行器只读取该日期及以前的 BaoStock
canonical 数据；策略引擎要求信号日记录存在、未停牌、复权因子存在且质量为 ready。
因此只有通过这些检查的事件才会先成为 `eligible`，再由系统转换为 `triggered`。

事件幂等键为：

```text
alert_rule_id + strategy_version_id + symbol + signal_date
```

相同规则、策略版本、证券和交易日重复评估会返回同一事件，不会新增触发或重复历史；
两个规则即使绑定同一策略版本和重叠证券，也各自保留可审计事件。

## 状态机

| 状态 | 含义 |
| --- | --- |
| `pending` | 已开始评估并持久化 |
| `eligible` | 收盘数据满足触发资格 |
| `triggered` | 策略条件匹配 |
| `acknowledged` | 用户已确认查看 |
| `resolved` | 条件未匹配或已解除 |
| `suppressed` | 停牌、数据不足、质量异常或用户忽略 |
| `error` | 规则或评估过程失败 |

自动路径：

```text
pending -> eligible -> triggered
pending -> eligible -> resolved
pending -> suppressed
pending -> error
```

用户可执行：

- `triggered -> acknowledged`：确认；
- `triggered/acknowledged -> suppressed`：忽略；
- `suppressed -> eligible`：恢复评估资格；下一次显式收盘评估才可能重新触发。

每次转换都保存来源状态、目标状态、执行者、原因与时间。用户不能直接写入
`triggered`。

## API

- `POST /api/v1/alerts/rules`
- `GET /api/v1/alerts/rules`
- `POST /api/v1/alerts/rules/{rule_id}/evaluate`
- `GET /api/v1/alerts/events`
- `POST /api/v1/alerts/events/{event_id}/acknowledge`
- `POST /api/v1/alerts/events/{event_id}/suppress`
- `POST /api/v1/alerts/events/{event_id}/restore`

列表与评估响应区分 `empty`、`ready`、`partial` 和 `error`。事件包含信号交易日、
BaoStock 来源、质量状态、质量问题、策略条件解释、数据指纹和完整转换历史。

## 隐私与尚未包含

预警规则、事件及历史均在 `var/user/stock_eva_user.sqlite3`，沿用阶段 2 的本地备份
和恢复边界。静态页面不保存用户数据，日志和测试不包含真实自选或预警。

阶段 5 已在新交易日完整发布后自动调用规则评估；它仍是收盘后日线任务，不包含实时
行情、外部推送、邮件、短信、跨设备同步、券商连接、自动交易或投资建议。投入真实
使用仍需让策略所需历史窗口达到完整覆盖。
