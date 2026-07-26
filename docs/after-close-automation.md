# 阶段 5 收盘后自动闭环与私有数据备份

## 触发边界

自动闭环只接受后端市场刷新服务刚刚成功发布、且仍是当前 published pointer 的
`ready` 交易日。`partial`、`error`、尚未发布、已被更新快照替代的旧运行都不会
触发策略或预警。应用重启后会对当前完整发布快照重放闭环入口；持久化任务审计会
跳过已经完成的项目，因此不需要重新抓取行情或重复生成结果。

GET 请求和打开页面不会触发闭环。

## 执行顺序与幂等

单个交易日的顺序固定为：

1. 读取全部已保存策略的当前不可变版本；
2. 对每个版本执行全市场主板扫描；
3. 对全部已保存预警规则，按规则绑定的本地自选列表执行收盘评估。

策略任务幂等键为 `strategy version id + trade date`，并继续传入全市场扫描存储层的
唯一索引；规则任务幂等键为 `alert rule id + trade date`，规则内部事件按
`alert_rule_id + strategy_version_id + symbol + signal_date` 去重。进程重入不会
重复产生策略运行或预警事件。

审计表位于本地私有 SQLite：

- `after_close_pipeline_runs`：交易日、发布运行、总体状态和完成/失败数量；
- `after_close_tasks`：策略或规则、幂等键、尝试次数、结果引用和安全错误码。

单个策略/规则失败只把该任务记为 `error`，其他策略和规则继续运行；市场
published pointer 和刷新成功状态不回滚。下一次闭环重入只重试未完成任务。

## 私有 SQLite 一致性备份

持仓、自选、策略、预警及闭环审计都留在 Mac 本地 SQLite。备份使用 SQLite Backup
API 读取包含 WAL 已提交事务的一致快照，再对候选文件执行 `PRAGMA integrity_check`，
通过后才原子替换当天和当周文件。默认保留：

- 最近 7 个日快照；
- 最近 4 个周快照。

源数据库和备份目标都拒绝 `/Volumes`、`/Network`、`/net` 下的网络卷；市场 NAS
不保存私有数据库或私有备份。备份失败不会替换已有有效快照，输出只含安全错误码和
快照文件名。

执行：

```bash
uv run python -m backend.app.cli backup-private-data
```

默认目录为：

```text
~/Library/Application Support/Stock EVA/backups
```

也可以显式指定另一个本地目录和保留数量：

```bash
uv run python -m backend.app.cli backup-private-data \
  --backup-root "/local/path/stock-eva-backups" \
  --keep-daily 7 \
  --keep-weekly 4
```

`PrivateBackupService.run(now)` 是后续本机自动化可直接调用的窄接口；本阶段不创建
launchd、cron，也不写 NAS。

## 能力边界

- 这是收盘后日线研究链路，不是实时、分钟或盘中扫描；
- 全市场扫描结果为候选与解释，不是买卖建议；
- 不连接券商、不下单、不发送邮件/短信；
- 免费数据源没有 SLA；行情发布不完整时宁可保留上一完整日，也不运行半套结论。
