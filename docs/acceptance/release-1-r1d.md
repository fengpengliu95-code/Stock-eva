# Release 1 R1-D 独立板块工作区验收记录

状态：**GO（2026-08-09）**。本记录只关闭 R1-D，不代表 R1-E、Release 1 整体或
Release 2 GO。

## 交付边界

- `#sectors?as_of=...&taxonomy_id=...&sector_id=...` 是可重载的独立板块工作区；
  裸路由规范化到上海当前日和受控分类，坏日期、坏百分号/UTF-8 与非受控 taxonomy
  fail closed，不会携带无效参数请求分析接口。
- 页面按同一复盘上下文展示市场状态、后端原始板块顺序、选中板块的原始指标、
  支持/反例、质量/血缘、范围说明和研究龙头；前端不重排、不重算分数。
- 龙头进入个股后保留 `as_of / taxonomy_id / sector_id / return_view=sectors`，返回时
  恢复同一板块。离开或切换路由会中止旧请求，迟到响应不能覆盖当前视图。
- canonical 板块页不再加载 `/market/supplemental`，也不保留虚构热力图、板块历史图
  或成分表占位。现有后端没有这些可信接口，因此本切片没有伪造替代数据。
- 资金证据保持 `missing / Release 2`，成交额不被解释成资金净流入；受限沪深主板
  样本不能描述成全 A 股。

## 提交与独立复审

主线提交按顺序为：

1. `84aacfb` — 独立板块证据流；
2. `ce6886f` — 冻结 R1-D 规格与验收计划；
3. `90714a0` — 严格验证板块路由输入；
4. `605cd79` — 更新 canonical 静态契约；
5. `9ac2d33` — 限制板块排名控件，消除桌面端跨栏覆盖。

独立 reviewer 串行审查了完整实现、路由修复、测试契约和最终 CSS 修复，结论均为
`APPROVE`，无剩余 blocker。开发与审核没有并发修改同一工作树，也没有启动 R1-E/R2。

## 自动化验收

```text
cd workspace && npm test -- --run
# 14 files / 72 tests passed

cd workspace && npm run build
# Vite production build completed

uv run --extra dev pytest -q --basetemp=/tmp/stock-eva-r1d-main-final
# 769 tests collected; completed at 100%, exit 0

uv run ruff check backend tests
# passed

git diff --check
# clean for R1-D changes
```

首轮全量 Python 验收暴露两条仍要求旧 supplemental DOM/虚构板块图框的陈旧测试；先
复现 RED，再更新为 canonical 后端证据契约，最终全量通过。`ruff format --check`
仅报告 `tests/test_workspace_static.py` 三处可由 `git blame` 追溯到旧提交的格式债，
不在 R1-D 修改行内。

## 真实 API 与浏览器验收

- 生产 `RELEASE.json` 回读为
  `9ac2d334a4088e98fd3183dbcdd53b21b997b772`；API 与 workspace LaunchAgent 均 ready。
- 真实 `2026-08-09` 请求返回实际数据日 `2026-08-07`、83 个板块和受限主板范围；
  M73 为 3/31 个范围内定价成员、排名可用、置信度 75%，没有错误的
  `sector.member_price_missing`。资金证据仍明确缺失。
- 安装后的浏览器在 1440×900 下保持两列；排名按钮右边界 `588.59px`，左栏右边界
  `637.39px`，详情栏左边界 `657.39px`，因此既未越界也未覆盖详情。83 个唯一板块
  保持后端顺序，旧 supplemental/热力图/板块图节点均不存在。
- `B09有色金属矿采选业 → sh.600489 技术驾驶舱 → 返回` 保留同一日期、分类和板块；
  驾驶舱显示 K 线、MA250、MACD、RSI 与资金边界。
- 390×844 下板块工作区为 347px 单列，body 无横向溢出，一级导航可横向滚动；
  最终控制台日志为空。

## 安装与不可变性

官方 LaunchAgent 安装器发布成功，5 个 agent 已安装并加载；NAS 状态为
`destination_newer`，270 个文件、861,430 行、`copied_bytes=0`。安装前后指纹完全
一致：

| 受保护对象 | SHA-256 / tree digest |
| --- | --- |
| 本地 market dataset | `f55f042679bc2187a522fbff5cb54ea8dc79ae58b145eb4def4a98e948cfd471` |
| NAS market dataset | `d099a5a19d5361f7c2b182721edf8c99231aa2ed24f2dde065e13a59a2bd634d` |
| classification DB | `45fd786ca80dd760e79b28432d3929de1b8da76111e265ea152ef5a8388f1941` |
| market control DB | `31dbd6ff9db881bf9bf936156945efb79b898fee5016b4c9f0544ec4e671b84f` |
| supplemental audit DB | `45df0cfce85d8282a41feed7fae7b795f70df9a67e7d7b2e766dd6a2172c03dc` |
| user DB | `f6b7af57cc641b13e99fd5b58f4a3269d8e3df4b9064f0a48cbc4950c9a05555` |

## 仍未包含

- 可信板块历史序列、热力图和完整成分接口；
- Release 2 的 point-in-time 主力资金证据与趋势发布；
- R1-E 的 Release 1 最终产品级收口；
- 券商连接、自动下单或投资建议。
