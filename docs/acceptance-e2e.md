# 真实行情 / 临时用户数据 E2E 验收

该验收只读取 NAS 归档或本机已发布行情镜像。持仓、自选、策略、扫描记录、预警规则
和事件全部写入系统临时目录中的一次性 SQLite；流程结束后自动删除，绝不打开或修改
正式用户库。

## 前置条件

- NAS 归档或本机镜像的数据集 readiness 完整通过；
- published snapshot 为 `ready`；
- 截至 published date 至少有 21 个已发布有效交易日，满足
  `volume_ratio_5d > 1 AND crosses_above(SMA5, SMA20)` 的最小窗口；
- 历史回填、每日刷新和其他写入任务已经结束。

直接读 NAS 时，先按全局 `access-nas-stock` 技能执行只读挂载检查：

```bash
/Users/finlay/.codex/skills/access-nas-stock/scripts/check-stock-nas.sh
```

不要使用 `--write-check`；该验收不需要 NAS 写权限。

## 执行

```bash
uv run python -m backend.app.acceptance \
  --nas-root /Volumes/Stock/stock-eva-market
```

正式后台环境优先验收本机发布镜像：

```bash
uv run python -m backend.app.acceptance \
  --local-dataset-root \
  "$HOME/Library/Application Support/Stock EVA/data/market-dataset"
```

两个数据集参数必须且只能提供一个。命令先获取与历史回填相同的本机刷新锁。锁忙时
退出并返回 `market_refresh_running`，不会读取或扫描行情。验收过程中只调用行情
store 的读取接口；本机控制 pointer 也在临时目录重建。

验收步骤：

1. 校验 manifest、Parquet hash/schema/行数和 ready published pointer；
2. 在临时 SQLite 创建示例持仓和自选；
3. 创建量比与 MA5/MA20 上穿组合规则；
4. 以 200 只/批执行全主板扫描，并以同一 idempotency key 重复执行；
5. 对临时自选执行两次收盘预警评估，核对事件 ID 与幂等键；
6. 校验临时持仓估值覆盖 1/1；
7. 删除全部临时用户和控制数据。

成功输出只包含数据日期、扫描/候选数量、批次状态、幂等结果、预警状态和持仓覆盖。
候选统一称为“规则候选”，并固定标注“仅用于研究，不构成投资建议”。若历史不足、
行情未 ready、存在失败批次或幂等不一致，命令 fail closed，不把部分结果称为验收
成功。
