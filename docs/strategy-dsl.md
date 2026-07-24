# 阶段 3 策略 DSL 与确定性回放

## 能力边界

阶段 3 只执行收盘后、本地、显式证券列表的规则回放。它不提供实时扫描、自动交易、
下单、仓位建议或收益承诺。单次运行最多 20 个证券，当前不会自动扩展到全市场。

规则输入必须是 JSON AST。系统不会接收 Python、SQL、JavaScript、公式字符串或
函数名，不使用 `eval`、`exec` 或动态导入。验证后的 AST 会复制为规范 JSON 并计算
SHA-256；调用方之后修改原对象不会改变已验证规则。

## 白名单

逻辑节点：

- `and`、`or`：`args` 至少两个子节点；
- `not`：单一 `arg`；
- `compare`：运算符仅 `>`、`>=`、`<`、`<=`、`==`、`!=`；
- `crosses_above`、`crosses_below`。

操作数：

- `{"type":"field","name":"close"}`；字段仅
  `open/high/low/close/preclose/volume/amount`；
- `{"type":"constant","value":1.0}`；仅有限数值；
- `SMA`、`EMA`、`RSI`，首版只接受 `field:"close"` 和 `period:2..250`；
- `{"type":"indicator","name":"volume_ratio_5d"}`。

AST 最大深度为 5，谓词最多 20。每个节点必须只包含规定键，多余键也会拒绝。

示例：

```json
{
  "op": "and",
  "args": [
    {
      "op": "crosses_above",
      "left": {
        "type": "indicator",
        "name": "SMA",
        "field": "close",
        "period": 5
      },
      "right": {
        "type": "indicator",
        "name": "SMA",
        "field": "close",
        "period": 20
      }
    },
    {
      "op": "compare",
      "left": {"type": "indicator", "name": "volume_ratio_5d"},
      "operator": ">",
      "right": {"type": "constant", "value": 1}
    }
  ]
}
```

## 时间和指标语义

所有价格字段来自 canonical 未复权价格乘 BaoStock 前复权因子。有效交易日序列排除
停牌、缺少因子和 `quality_status != ready` 的记录。请求信号日必须存在一条质量正常
的 canonical 记录；如果信号日自身异常，证券直接标记 `excluded`，不会回退到旧日期。

- `SMA(N)`：最近 N 个有效交易日的前复权收盘价算术平均。
- `EMA(N)`：前 N 个有效交易日以 SMA 初始化，之后
  `alpha=2/(N+1)` 递推。
- `RSI(N)`：最近 N 个有效交易日价格变化的简单滚动平均涨幅/跌幅计算；全涨为 100，
  全平为 50。
- `volume_ratio_5d`：当日成交量除以前 5 个有效交易日成交量均值，分母不含当日。
- `crosses_above`：`t-1 左值 <= 右值` 且 `t 左值 > 右值`。
- `crosses_below`：`t-1 左值 >= 右值` 且 `t 左值 < 右值`。

执行器查询上限固定为 `as_of_date`，数据指纹也只包含该日及以前的行；未来新增数据
不会改变历史回放。每个谓词解释包含实际值、窗口起止日期、有效日数量、信号日期和
质量状态。

## 版本与运行记录

策略和运行状态与持仓共用本地用户 SQLite，但表相互独立。版本创建使用
`expected_current_version` 乐观锁；历史版本和规则哈希不可修改。每次运行保存：

- 策略 ID 与版本；
- 信号日期、证券列表；
- 规则哈希、输入数据指纹；
- 每个证券的匹配、未匹配、排除或数据不足状态；
- 完整条件解释。

相同版本、证券、信号日期和本地数据会得到相同数据指纹与结果；运行 ID 和创建时间
仍是独立审计记录。

## API

- `POST /api/v1/strategies/validate`
- `POST /api/v1/strategies`
- `GET /api/v1/strategies`
- `GET /api/v1/strategies/{id}/versions/{version}`
- `POST /api/v1/strategies/{id}/versions`
- `POST /api/v1/strategies/{id}/runs`
- `GET /api/v1/strategies/{id}/runs`
- `GET /api/v1/strategy-runs/{run_id}`

## 真实历史数据门槛

- 仅验证 `MA5` 上穿 `MA20` 与量比：至少需要连续覆盖 21 个有效交易日。
- 对 `MA20 + RSI14 + volume_ratio_5d` 做较稳妥的小范围回放：建议至少 60 个有效
  交易日，并逐日满足因子与质量要求。
- 支持 DSL 允许的最大 250 日窗口及交叉：至少 251 个有效交易日；为覆盖停牌和质量
  缺口，受控真实回放建议准备约 260 个交易日。

这些是计算窗口门槛，不是统计显著性或策略有效性证明。扩大到全市场候选前，还需要
逐日证券覆盖率、退市/上市边界和回填失败报告达到阶段 1 的严格质量标准。
