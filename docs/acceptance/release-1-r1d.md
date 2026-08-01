# Release 1 R1-D 首页决策流验收记录

## 交付边界

- 首页按 `市场状态 → 资金证据 → 板块轮动 → 组合风险 → 龙头候选 → 个股技术驾驶舱` 展示后端证据。
- Release 1 只读取市场状态、板块轮动和龙头候选，不在前端重算分数、排序或市场结论。
- 实际范围固定显著标注为“仅沪深主板价格样本 / 不能代表全 A 股”。
- 资金证据保持 `missing / unavailable`；成交额仅作为量价证据，不展示或推断净流入。
- 组合风险保持 Release 2 空态；缺少本地组合风险输入时不生成仓位区间。
- 龙头候选保持 `actionable_primary=false`，展示操作性缺口、涨跌停锁定缺口、支持/反例、原始指标和排除原因，不提供买卖推荐。

## 后端依赖

- `GET /api/v1/analysis/market-regime?as_of=...`
- `GET /api/v1/analysis/sector-rotation?as_of=...&taxonomy_id=...`
- `GET /api/v1/analysis/sectors/{sector_id}/leaders?as_of=...&taxonomy_id=...`
- `GET /api/v1/market/history/dates`
- `GET /api/v1/securities/{symbol}/analysis?start=...&end=...`

首页保留后端返回的板块和龙头顺序。`as_of`、`taxonomy_id`、`sector_id` 和 `symbol` 写入可重载的 hash；从龙头进入个股后，技术分析窗口上界仍为同一 `as_of`。

## 自动验收

```text
cd workspace
npm test -- --run
# 12 files / 48 tests passed

npm run build
# Vite production build completed

git diff --check
# clean
```

覆盖场景包括 ready、degraded、missing、empty、HTTP 422、HTTP 503、历史深链、后端顺序、请求竞态、AbortSignal、语义化标题/details/status、旧持仓/自选入口回归和龙头到 R0 技术驾驶舱的历史时点往返。

## 浏览器验收入口

- 默认入口：`http://127.0.0.1:8080/workspace/#overview`
- 历史深链 fixture：`#overview?as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b`
- 真实环境若尚无已发布分类代际，板块区域必须显示证据空态，不能显示“没有热点”；该状态不阻断 R0 个股技术驾驶舱。
