# 个股技术分析 API

`GET /api/v1/securities/{symbol}/analysis` 为盘后个股复盘返回可追溯的前复权日线和
基础技术指标。它不提供实时行情、投资建议、券商连接或自动交易能力。

## 请求

`symbol` 必须使用 BaoStock 规范格式：`sh.600000` 或 `sz.000001`。`start` 和 `end`
都是必填的 ISO 日期：

```text
GET /api/v1/securities/sh.600000/analysis?start=2025-07-01&end=2026-07-24
```

服务只查询 `start <= trade_date <= end` 的规范行情，响应中的 `as_of` 是实际返回的
最后一个有效交易日，不会读取 `end` 之后的数据。`start > end`、非法证券代码或非法
日期返回 HTTP 422。

## 数据和质量契约

- `source` 固定为 `baostock`。
- 规范存储仍保存未复权 OHLCV 和 `adjust_factor`；此端点的价格字段统一按 `qfq`
  前复权，`price_adjustment` 固定为 `qfq`。
- 成交量和成交额不做价格复权。
- 停牌占位记录不进入返回序列，也不参与任何指标窗口。
- 请求窗口内完全没有记录时返回 `status=empty`、`as_of=null`、
  `quality_issues=["no_market_data"]` 和空 `series`。
- 只要所需有效记录缺少 `adjust_factor`，服务 fail closed 并返回 HTTP 409，不使用
  未复权价格降级或补造因子。

## 指标版本与暖机

`formula_version=ta-lib-0.7.0-r0-v1` 对应 TA-Lib Python 0.7.0 的以下固定参数。
指标只使用请求窗口内已经返回的有效交易日，暖机不足的值为 JSON `null`，不会写成
`0`：

| 输出字段 | TA-Lib 函数与参数 | 首个有效值前的暖机记录数 |
|---|---|---:|
| `ma5` | `SMA(timeperiod=5)` | 4 |
| `ma10` | `SMA(timeperiod=10)` | 9 |
| `ma20` | `SMA(timeperiod=20)` | 19 |
| `ma60` | `SMA(timeperiod=60)` | 59 |
| `ma120` | `SMA(timeperiod=120)` | 119 |
| `ma250` | `SMA(timeperiod=250)` | 249 |
| `macd`、`macd_signal`、`macd_hist` | `MACD(12,26,9)` | 33 |
| `rsi14` | `RSI(timeperiod=14)` | 14 |

每个序列点同时包含 `trade_date`、前复权 `open/high/low/close`、原始 `volume/amount`
和上述指标。后续指标应通过后端注册表逐项加入，并为参数、暖机、公式版本和独立金标
样本补充测试后才能发布。
