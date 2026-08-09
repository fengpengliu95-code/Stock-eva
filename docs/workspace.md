# 阶段 4 复盘工作台

## 本地启动

前端源码和 npm 依赖都限制在 `workspace/`，不使用项目根目录的 `package.json`、
`node_modules` 或公网 CDN。首次检出或前端源码变化后先运行：

```bash
cd workspace
npm ci
npm test
npm run build
cd ..
```

然后分别启动 API 与现有 Python 静态文件服务：

```bash
uv run uvicorn backend.app.main:app --reload
uv run python -m backend.app.static_server \
  --host 127.0.0.1 \
  --port 8080 \
  --directory .
```

浏览器打开 `http://127.0.0.1:8080/`。根页面提供两个独立入口：

- `workspace/`：Stock EVA 收盘复盘工作台；
- `dashboard/`：原有量化学习知识库。

本地静态服务为所有响应设置 `Cache-Control: no-store`。工作台的 HTML、JavaScript
和 CSS 文件名不是内容哈希，因此发布后同一 URL 会重新读取同一 release 的资源，
不会将旧 `app.js` 与新 module 混装；不要改回无明确缓存策略的通用静态服务。

源码、构建和运行时的关系固定如下：

```text
workspace/src/**/*.ts
  + workspace/package-lock.json
  -> npm run build
  -> workspace/assets/security-cockpit.js
  -> /workspace/（开发静态服务）
  -> runtime/current/public/workspace/（LaunchAgent）
```

`workspace/assets/security-cockpit.js` 是由 Vite 生成并跟踪的审计产物，不应手工编辑。
安装器从当前 Git 提交创建隔离 staging，执行 `npm ci --ignore-scripts` 和
`npm run build` 后再发布静态目录，并删除 staging 中的 `node_modules`、TypeScript
源码和构建配置。生产运行时不需要 Node.js，也不会发布依赖缓存。

## 个股技术驾驶舱

持仓表和当前自选列表中的证券代码是原生按钮，鼠标或键盘激活后直接进入个股页；
URL hash 保存 `symbol` 和来源视图，重载或浏览器前进/后退仍可恢复，并提供返回来源
的路径。

个股页严格按以下顺序读取本地 API：

1. `GET /api/v1/market/history/dates`；
2. 取响应中最后最多 260 个有效交易日作为 `start/end`；
3. `GET /api/v1/securities/{symbol}/analysis?start=...&end=...`。

前端不会推测周末、节假日或最近交易日，也不会计算、补齐或回退任何指标。
KLineChart 10.0.0 只负责渲染后端返回的前复权 OHLC、真实成交量和
MA5/10/20/60/120/250。MACD(12,26,9) 与 RSI14 由独立数值面板展示；表格提供最近
20 个有效交易日的文本替代。所有暖机 `null` 显示为 `—`，不转换成零。

页首保留 `symbol`、实际 `as_of`、`source`、`price_adjustment`、
`formula_version`、`status` 和 `quality_issues`。`no_market_data` 与
`no_effective_trading_data` 使用不同空态；HTTP 409 作为数据质量门禁 fail closed；
一般网络错误单独说明本地 API 连接问题。loading、empty 和 error 状态都不会创建
KLineChart 实例或注入示例蜡烛。

## 数据与状态边界

工作台只调用本机 `http://127.0.0.1:8000/api/v1`，不会将持仓、自选或策略写入
静态文件、`localStorage` 或 `sessionStorage`。页面中的市场数字、估值和策略命中
都来自 API；未取得真实数据时显示明确空态或错误状态。

市场摘要保留后端 `empty`、`partial`、`stale`、`error` 和 `ready` 语义，并同时
显示数据日期、来源、覆盖数量及质量说明。缺少涨跌幅时显示“涨跌幅缺失”，不会将
缺失值解释为平盘。

前端不再要求或显示“期望交易日”，也不提供日常刷新按钮。页面启动时只读取后端
最近一次已经落库的结果，并把实际数据日、来源、覆盖数量及 `empty`、`partial`、
`stale`、`error` 或 `ready` 状态作为只读信息展示。每日行情抓取和新鲜度判断仍
属于后端 CLI/调度边界；前端不会把“页面已读取”描述成“行情已自动刷新”。

组合估值只按用户成本与最新有效收盘估算，未计交易费用。缺失行情或停牌时展示覆盖
`N/M` 和缺失原因，不补造盈亏。

策略区展示已保存策略、最近运行记录和持久化候选；显式运行使用用户填写的标的与
截止日期。没有历史数据或没有运行记录时保持空态，不生成候选或投资结论。

预警区可将一个自选列表绑定到不可变策略版本，显式评估已收盘交易日，并展示本地
历史、未确认数量、数据来源、质量状态和条件解释。确认、忽略和恢复操作只修改本地
SQLite 状态，不调用任何外部通知服务。

## 交互与可访问性

- 固定一级导航为收盘总览、市场结构、板块与成交、持仓、策略、自选预警和知识库；
- 收盘总览以市场宽度大图为核心，持仓、策略与预警只保留摘要入口；
- 板块、持仓、策略和自选预警使用独立工作区，不再挤在同一首屏；
- 桌面使用宽松的 12 列布局，主图不低于 `420px`、次级图不低于 `280px`；
- `390px` 宽度下导航可横向键盘滚动，内容降级为单列；
- 个股图表容器、元数据和指标面板在 `390px × 844px` 下保持在 viewport 内，
  宽表只在自身容器横向滚动，不造成 body 横向溢出；
- A 股红涨绿跌同时配有“上涨/下跌”文字和正负号，不只依赖颜色；
- 原生表单控件、可见焦点、跳转主内容链接和状态播报支持键盘操作；
- 页面只显示安全错误摘要，不展示服务端堆栈。

行情、板块或组合历史为空时，页面不会绘制示例 K 线、随机热力图或虚构收益曲线。
“板块与成交”现在读取 R1-C 已发布的 point-in-time 分类、板块排名和研究龙头证据，
保留后端顺序、实际数据日、支持/反例、质量、血缘和受限沪深主板范围。独立路由把
`as_of / taxonomy_id / sector_id` 写入 hash；从龙头进入个股并返回时保留同一上下文。

当前后端仍未提供可信板块历史序列、热力图或完整成分接口，因此页面不创建这些图框
或用随机数据代替。canonical 板块页也不使用 supplemental 空态接口作为排名来源；
该接口仅保留为独立后端能力。Release 2 资金证据尚未发布，页面必须显示
`missing / unavailable`，成交额不会被解释为资金净流入。

## 尚未包含

- 前端自动轮询、实时行情和盘中提醒；
- 完整全 A 股板块范围、板块历史序列、热力图和可信资金流适配层；
- 组合历史净值序列；
- 自动调度、外部推送、邮件、短信及跨设备通知；
- 大规模真实历史回填后的全市场策略扫描；
- 券商连接、下单、自动交易或投资建议。

## 浏览器验收

构建并启动本地 API/静态服务后，在真实数据环境执行：

1. 打开 `http://127.0.0.1:8080/workspace/#sectors`，确认路由规范化为带日期、分类和
   选中板块的 canonical hash，板块排名、范围与质量说明来自后端且无控制台错误；
2. 选择另一个真实板块，确认 hash、详情和龙头同步更新，前端不改变后端顺序；
3. 从一个真实龙头进入技术驾驶舱，再返回同一板块上下文；
4. 从一个真实持仓和一个真实自选分别激活证券按钮，确认一次交互进入驾驶舱；
5. 对照 Network 中的 dates/analysis 响应，核对请求窗口、页首元数据、最后一根
   OHLCV、MA、MACD、RSI 和 `null → —`；
6. 重载包含 `#security/{symbol}?from=...` 的地址，确认 symbol 恢复且返回路径正确；
7. 只用键盘 Tab、Enter、Space 和浏览器前进/后退完成两条入口与返回流程，确认焦点
   始终可见；
8. 使用 390×844 viewport 检查 body 无横向滚动，并启用 reduced motion；
9. 用可控 API 响应分别验证两个 empty、HTTP 409 和连接失败；这些状态中不得出现
   canvas 或蜡烛。
