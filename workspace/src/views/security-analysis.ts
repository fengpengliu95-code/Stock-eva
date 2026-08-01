import type { SecurityAnalysisResponse, TechnicalAnalysisPoint } from "../api";
import type { CockpitState, SourceView } from "../state";

export type ChartCleanup = () => void;
export type RenderChart = (
  container: HTMLElement,
  response: SecurityAnalysisResponse,
) => ChartCleanup;

function element<K extends keyof HTMLElementTagNameMap>(
  tag: K,
  text?: string,
  className?: string,
): HTMLElementTagNameMap[K] {
  const result = document.createElement(tag);
  if (text !== undefined) result.textContent = text;
  if (className) result.className = className;
  return result;
}

function formatValue(value: number | null, digits = 4): string {
  return value === null
    ? "—"
    : new Intl.NumberFormat("zh-CN", {
        maximumFractionDigits: digits,
      }).format(value);
}

function sourceLabel(sourceView: SourceView): string {
  if (sourceView === "portfolio") return "持仓";
  if (sourceView === "watchlists") return "自选预警";
  return "盘后决策流";
}

function renderMetadata(response: SecurityAnalysisResponse): HTMLElement {
  const list = element("dl", undefined, "security-metadata");
  const entries = [
    ["证券", response.symbol],
    ["实际数据日", response.as_of ?? "—"],
    ["来源", response.source],
    ["价格口径", response.price_adjustment],
    ["公式版本", response.formula_version],
    ["状态", response.status],
    [
      "质量问题",
      response.quality_issues.length ? response.quality_issues.join("、") : "无",
    ],
  ];
  for (const [term, value] of entries) {
    const group = element("div");
    group.append(element("dt", term), element("dd", value, "mono"));
    list.append(group);
  }
  return list;
}

function latestIndicatorPanel(
  title: string,
  values: Array<[string, number | null]>,
): HTMLElement {
  const panel = element("article", undefined, "panel indicator-panel");
  panel.append(element("h3", title));
  const list = element("dl", undefined, "indicator-values");
  for (const [label, value] of values) {
    const group = element("div");
    group.append(element("dt", label), element("dd", formatValue(value), "mono"));
    list.append(group);
  }
  panel.append(list);
  return panel;
}

function tableCell(value: string): HTMLTableCellElement {
  return element("td", value);
}

function renderAlternativeTable(
  series: TechnicalAnalysisPoint[],
): HTMLElement {
  const wrap = element("div", undefined, "table-wrap security-table");
  const table = element("table");
  table.setAttribute("aria-label", "技术指标数值替代");
  const caption = element(
    "caption",
    `最近 ${Math.min(series.length, 20)} 个有效交易日；完整图表共 ${series.length} 条`,
  );
  const head = element("thead");
  const headRow = element("tr");
  for (const label of [
    "日期",
    "开",
    "高",
    "低",
    "收",
    "成交量",
    "MA5",
    "MA10",
    "MA20",
    "MA60",
    "MA120",
    "MA250",
    "MACD",
    "Signal",
    "Hist",
    "RSI14",
  ]) {
    const th = element("th", label);
    th.scope = "col";
    headRow.append(th);
  }
  head.append(headRow);
  const body = element("tbody");
  for (const point of series.slice(-20)) {
    const row = element("tr");
    row.append(
      tableCell(point.trade_date),
      tableCell(formatValue(point.open)),
      tableCell(formatValue(point.high)),
      tableCell(formatValue(point.low)),
      tableCell(formatValue(point.close)),
      tableCell(formatValue(point.volume, 0)),
      tableCell(formatValue(point.ma5)),
      tableCell(formatValue(point.ma10)),
      tableCell(formatValue(point.ma20)),
      tableCell(formatValue(point.ma60)),
      tableCell(formatValue(point.ma120)),
      tableCell(formatValue(point.ma250)),
      tableCell(formatValue(point.macd)),
      tableCell(formatValue(point.macd_signal)),
      tableCell(formatValue(point.macd_hist)),
      tableCell(formatValue(point.rsi14)),
    );
    body.append(row);
  }
  table.append(caption, head, body);
  wrap.append(table);
  return wrap;
}

function renderEmpty(
  content: HTMLElement,
  reason: "no_market_data" | "no_effective_trading_data",
): void {
  const copy =
    reason === "no_effective_trading_data"
      ? "该窗口没有有效交易数据（记录可能全部为停牌占位）。"
      : "该窗口没有可用行情。";
  const state = element("div", undefined, "panel security-state");
  state.append(
    element("p", "EMPTY / 真实空态", "panel-kicker"),
    element(
      "h2",
      reason === "no_effective_trading_data"
        ? "没有有效交易数据"
        : "没有可用行情",
    ),
    element("p", copy, "state-message"),
  );
  content.append(state);
}

export function renderSecurityAnalysis(
  root: HTMLElement,
  state: CockpitState,
  renderChart: RenderChart,
): ChartCleanup | null {
  const back = root.querySelector<HTMLAnchorElement>("#security-back");
  const status = root.querySelector<HTMLElement>("#security-status");
  const content = root.querySelector<HTMLElement>("#security-content");
  if (!back || !status || !content) {
    throw new Error("security cockpit root is incomplete");
  }
  content.replaceChildren();
  if (state.phase === "idle") {
    status.textContent = "尚未选择证券";
    return null;
  }
  if (state.decisionContext) {
    const query = new URLSearchParams({
      as_of: state.decisionContext.asOf,
      taxonomy_id: state.decisionContext.taxonomyId,
      sector_id: state.decisionContext.sectorId,
    });
    back.href = `#overview?${query}`;
    back.dataset.decisionReturn = "true";
    delete back.dataset.viewTarget;
  } else {
    back.href = `#${state.sourceView}`;
    back.dataset.viewTarget = state.sourceView;
    delete back.dataset.decisionReturn;
  }
  back.textContent = `← 返回${sourceLabel(state.sourceView)}`;
  if (state.phase === "loading") {
    status.textContent = `正在读取 ${state.symbol} 的交易日和后端分析…`;
    const loading = element("div", undefined, "panel security-state");
    loading.append(
      element("p", "LOADING / 后端分析", "panel-kicker"),
      element("h2", state.symbol),
      element("p", "先读取有效交易日，再请求最多 260 日分析。"),
    );
    content.append(loading);
    return null;
  }
  if (state.phase === "empty") {
    status.textContent = `${state.symbol} 无可绘制数据`;
    if (state.response) content.append(renderMetadata(state.response));
    renderEmpty(content, state.reason);
    return null;
  }
  if (state.phase === "quality-error") {
    status.textContent = `${state.symbol} 未通过数据质量门禁`;
    const quality = element("div", undefined, "panel security-state");
    quality.append(
      element("p", "HTTP 409 / FAIL CLOSED", "panel-kicker"),
      element("h2", "数据质量门禁"),
      element("p", state.message, "state-message"),
      element("p", "未使用未复权数据降级，也未绘制蜡烛。", "state-message"),
    );
    content.append(quality);
    return null;
  }
  if (state.phase === "connection-error") {
    status.textContent = `${state.symbol} 连接失败`;
    const failure = element("div", undefined, "panel security-state");
    failure.append(
      element("p", "CONNECTION ERROR / 本地服务", "panel-kicker"),
      element("h2", "无法连接本地 API"),
      element("p", state.message, "state-message"),
      element("p", "请确认 Stock EVA API 仅在本机运行。", "state-message"),
    );
    content.append(failure);
    return null;
  }

  const response = state.response;
  status.textContent = `${response.symbol} 已加载 ${response.series.length} 个有效交易日`;
  content.append(renderMetadata(response));

  const chartPanel = element("article", undefined, "panel security-chart-panel");
  const heading = element("div", undefined, "panel-heading");
  const title = element("div");
  title.append(
    element("p", "QFQ DAILY / API INDICATORS", "panel-kicker"),
    element("h2", `${response.symbol} 技术驾驶舱`),
  );
  heading.append(title, element("span", `截至 ${response.as_of ?? "—"}`, "quiet-tag"));
  const summary = element(
    "p",
    `前复权日 K、真实成交量和后端 MA，共 ${response.series.length} 个有效交易日。`,
    "state-message",
  );
  const chart = element("div", undefined, "security-chart");
  chart.id = "security-chart";
  chart.dataset.testid = "security-chart";
  chart.setAttribute("role", "img");
  chart.setAttribute(
    "aria-label",
    `${response.symbol} 前复权日 K、成交量、MA5、10、20、60、120、250、MACD、RSI14 图表`,
  );
  chartPanel.append(heading, summary, chart);
  content.append(chartPanel);

  const latest = response.series[response.series.length - 1];
  const indicators = element("div", undefined, "indicator-grid");
  indicators.append(
    latestIndicatorPanel("MACD（12, 26, 9）", [
      ["MACD", latest.macd],
      ["Signal", latest.macd_signal],
      ["Histogram", latest.macd_hist],
    ]),
    latestIndicatorPanel("RSI（14）", [["RSI14", latest.rsi14]]),
  );
  content.append(indicators, renderAlternativeTable(response.series));
  return renderChart(chart, response);
}
