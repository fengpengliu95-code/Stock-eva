"use strict";

const API_BASE = "http://127.0.0.1:8000/api/v1";

const state = {
  market: null,
  supplemental: null,
  positions: [],
  watchlists: [],
  strategies: [],
  alertRules: [],
  alertEvents: [],
  selectedWatchlistId: null,
};

const byId = (id) => document.getElementById(id);

function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined && text !== null) element.textContent = String(text);
  if (className) element.className = className;
  return element;
}

function clear(element) {
  element.replaceChildren();
}

function createSecurityButton(symbol, source) {
  const analyze = node("button", symbol, "security-symbol-button mono");
  analyze.type = "button";
  analyze.dataset.securitySymbol = symbol;
  analyze.dataset.securitySource = source;
  analyze.setAttribute("aria-label", `查看 ${symbol} 个股技术分析`);
  return analyze;
}

window.stockEvaCreateSecurityButton = createSecurityButton;

function safeMessage(error) {
  if (error && Number.isInteger(error.status)) return `请求失败（HTTP ${error.status}）`;
  return "无法连接本地 API，请确认后端仅在本机运行。";
}

async function api(path, options = {}) {
  const headers = { Accept: "application/json", ...(options.headers || {}) };
  if (options.body) headers["Content-Type"] = "application/json";
  const response = await fetch(`${API_BASE}${path}`, { ...options, headers });
  if (!response.ok) {
    const error = new Error("API request failed");
    error.status = response.status;
    throw error;
  }
  if (response.status === 204) return null;
  return response.json();
}

const STATUS_TEXT = {
  empty: "无数据",
  ready: "完整",
  partial: "部分",
  stale: "过期",
  error: "错误",
  loading: "加载中",
};

const MARKET_PHASE_VIEW = {
  pre_market: {
    label: "盘前 · 展示最近完整收盘数据",
    badge: "盘前",
    badgeStatus: "empty",
  },
  market_open: {
    label: "盘中 · 本产品不提供实时行情",
    badge: "盘中",
    badgeStatus: "empty",
  },
  after_close_waiting: {
    label: "已收盘 · 等待免费日线数据就绪",
    badge: "待数据",
    badgeStatus: "partial",
  },
  refreshing: {
    label: "收盘数据更新中 · 完成前保留上一完整快照",
    badge: "更新中",
    badgeStatus: "loading",
  },
  complete: {
    label: "日终数据已通过发布门槛",
    badge: "已发布",
    badgeStatus: "ready",
  },
  delayed: {
    label: "日终数据延迟 · 当前仍展示上一完整快照",
    badge: "延迟",
    badgeStatus: "stale",
  },
  closed: {
    label: "今日休市 · 展示最近完整交易日",
    badge: "休市",
    badgeStatus: "empty",
  },
  calendar_unavailable: {
    label: "交易日历未确认 · 已停止自动判断",
    badge: "日历待确认",
    badgeStatus: "error",
  },
};

function setBadge(element, status) {
  element.className = `status-badge status-${status}`;
  element.textContent = STATUS_TEXT[status] || status;
}

async function loadMarketStatus() {
  try {
    const data = await api("/market/status");
    const view = MARKET_PHASE_VIEW[data.market_phase] || MARKET_PHASE_VIEW.calendar_unavailable;
    setBadge(byId("market-status"), view.badgeStatus);
    byId("market-status").textContent = view.badge;
    byId("data-status-primary").textContent = view.label;
    const details = [
      `日历 ${data.calendar_status}`,
      `刷新 ${data.refresh_state}`,
      `应完成 ${data.latest_expected_session || "未确认"}`,
      `已发布 ${data.published_as_of || "无"}`,
    ];
    if (data.next_retry_at) details.push(`下次校正 ${data.next_retry_at}`);
    byId("market-meta").textContent = details.join(" · ");
  } catch (error) {
    setBadge(byId("market-status"), "error");
    byId("data-status-primary").textContent = "只读数据状态暂不可用";
    byId("market-meta").textContent = safeMessage(error);
  }
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined) return "—";
  return new Intl.NumberFormat("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(Number(value));
}

function metric(label, value, className) {
  const wrapper = node("div", null, "metric");
  wrapper.append(node("span", label), node("strong", value, className));
  return wrapper;
}

function changePresentation(value) {
  if (value === null || value === undefined) {
    return { className: "market-flat", text: "涨跌幅缺失" };
  }
  const number = Number(value);
  if (number > 0) return { className: "market-up", text: `上涨 +${formatNumber(number)}%` };
  if (number < 0) return { className: "market-down", text: `下跌 ${formatNumber(number)}%` };
  return { className: "market-flat", text: `平盘 ${formatNumber(number)}%` };
}

const SVG_NS = "http://www.w3.org/2000/svg";

function svgNode(tag, attributes = {}, text) {
  const element = document.createElementNS(SVG_NS, tag);
  Object.entries(attributes).forEach(([name, value]) => element.setAttribute(name, String(value)));
  if (text !== undefined) element.textContent = String(text);
  return element;
}

function renderBreadthChart(identifier, breadth, status) {
  const chart = byId(identifier);
  clear(chart);
  const values = [
    { label: "上涨", value: breadth.advancing, className: "bar-up" },
    { label: "下跌", value: breadth.declining, className: "bar-down" },
    { label: "平盘", value: breadth.unchanged, className: "bar-flat" },
    { label: "停牌", value: breadth.suspended, className: "bar-suspended" },
  ];
  const maximum = Math.max(...values.map((item) => Number(item.value) || 0));
  if (status === "empty" || status === "error" || maximum === 0) {
    const message = status === "error"
      ? "最近一次行情处理失败，未绘制市场结构"
      : "尚无可绘制的真实市场宽度";
    chart.append(node("p", message, "empty-chart-copy"));
    return;
  }
  const svg = svgNode("svg", {
    viewBox: "0 0 800 360",
    class: "breadth-svg",
    role: "img",
    "aria-label": values.map((item) => `${item.label} ${item.value} 家`).join("，"),
  });
  svg.append(svgNode("line", { x1: 62, y1: 298, x2: 760, y2: 298, class: "chart-axis" }));
  values.forEach((item, index) => {
    const height = Math.max(2, Math.round((Number(item.value) / maximum) * 224));
    const x = 96 + index * 172;
    const y = 298 - height;
    svg.append(
      svgNode("rect", { x, y, width: 92, height, class: item.className }),
      svgNode("text", { x: x + 46, y: y - 14, class: "chart-value", "text-anchor": "middle" }, item.value),
      svgNode("text", { x: x + 46, y: 330, class: "chart-label", "text-anchor": "middle" }, item.label)
    );
  });
  chart.append(svg);
}

function renderMarketDetails(data) {
  state.market = data;
  const hasData = !["empty", "error"].includes(data.status);
  renderBreadthChart("market-primary-chart", data.breadth, data.status);
  renderBreadthChart("market-structure-chart", data.breadth, data.status);
  byId("market-structure-status").textContent = hasData
    ? `${data.breadth.eligible} 家有效交易`
    : STATUS_TEXT[data.status];
  const turnover = byId("turnover-summary");
  clear(turnover);
  turnover.append(
    node("strong", data.turnover.amount === null ? "—" : `${formatNumber(data.turnover.amount / 100000000)} 亿`),
    node("span", data.turnover.coverage ? `成交覆盖 ${data.turnover.coverage} 家` : "等待真实成交额与覆盖数")
  );
  const quality = byId("market-quality");
  clear(quality);
  const qualityRows = [
    ["来源", data.source || "未提供"],
    ["实际数据日", data.as_of || "无"],
    ["证券覆盖", `${data.completeness.loaded}/${data.completeness.requested}`],
    ["质量状态", STATUS_TEXT[data.status] || data.status],
  ];
  qualityRows.forEach(([label, value]) => {
    const row = node("div", null, "quality-item");
    row.append(node("span", label), node("strong", value, "mono"));
    quality.append(row);
  });
}

async function loadMarket() {
  setBadge(byId("market-status"), "loading");
  clear(byId("market-breadth"));
  clear(byId("market-indexes"));
  clear(byId("structure-indexes"));
  byId("market-message").textContent = "";
  try {
    const data = await api("/market/summary");
    setBadge(byId("market-status"), data.status);
    byId("market-meta").textContent = data.as_of
      ? `数据日期 ${data.as_of} · 来源 ${data.source} · 覆盖 ${data.completeness.loaded}/${data.completeness.requested}`
      : "本地尚无行情刷新记录";
    byId("data-status-primary").textContent = data.as_of
      ? `实际数据日 ${data.as_of} · ${STATUS_TEXT[data.status] || data.status}`
      : "尚无已完成的行情数据";
    renderMarketDetails(data);
    if (data.status === "empty") {
      byId("market-message").textContent = "暂无市场数据。工作台只显示后端已有结果，不从前端指定交易日。";
      return;
    }
    if (data.status === "error") {
      byId("market-message").textContent = "最近一次行情刷新失败，未生成市场结论。";
      return;
    }
    byId("market-breadth").append(
      metric("上涨", `${data.breadth.advancing} 家`, "market-up"),
      metric("下跌", `${data.breadth.declining} 家`, "market-down"),
      metric("平盘", `${data.breadth.unchanged} 家`, "market-flat"),
      metric("停牌", `${data.breadth.suspended} 家`, "market-flat"),
      metric("成交额", data.turnover.amount === null ? "—" : `${formatNumber(data.turnover.amount / 100000000)} 亿元`),
      metric("成交覆盖", `${data.turnover.coverage} 家`)
    );
    data.indexes.forEach((item) => {
      const row = node("div", null, "index-row");
      const label = node("span", item.symbol, "mono");
      const change = changePresentation(item.pct_change);
      const value = node("span", `${formatNumber(item.close)} · ${change.text}`, `mono ${change.className}`);
      row.append(label, value);
      byId("market-indexes").append(row);
      byId("structure-indexes").append(row.cloneNode(true));
    });
    byId("index-empty").hidden = data.indexes.length > 0;
    byId("structure-indexes-empty").hidden = data.indexes.length > 0;
    if (data.status !== "ready") {
      byId("market-message").textContent = `当前状态为${STATUS_TEXT[data.status]}，请结合数据日期和覆盖率阅读。`;
    }
  } catch (error) {
    setBadge(byId("market-status"), "error");
    byId("market-meta").textContent = "市场 API 不可用";
    byId("data-status-primary").textContent = "本地行情状态不可用";
    byId("market-message").textContent = safeMessage(error);
    renderBreadthChart("market-primary-chart", {}, "error");
    renderBreadthChart("market-structure-chart", {}, "error");
  }
}

function formatReportedFlow(value) {
  if (value === null || value === undefined) return "—";
  return `${formatNumber(Number(value) / 100000000)} 亿元`;
}

async function loadSupplementalMarket() {
  const body = byId("sector-flow-body");
  clear(body);
  try {
    const data = await api("/market/supplemental");
    state.supplemental = data;
    const ready = ["ready", "partial"].includes(data.status);
    setBadge(byId("supplemental-status"), ready ? data.status : "empty");
    byId("supplemental-status").textContent = ready
      ? STATUS_TEXT[data.status]
      : data.status === "not_configured" ? "未接入" : "暂无快照";
    byId("supplemental-source").textContent = ready
      ? `AKShare · 数据日 ${data.as_of || "未确认"}`
      : "AKShare 可选适配层";
    byId("market-flow-summary").textContent = data.market_flow
      ? `大盘上游报告主力净流入 ${formatReportedFlow(data.market_flow.reported_main_net_inflow)} · 数据日 ${data.market_flow.trade_date}`
      : "大盘资金流无已发布快照；不会从 OHLCV 推断主力方向。";
    data.sector_flows.forEach((item) => {
      const row = node("tr");
      row.append(
        node("td", item.scope_name),
        node("td", formatReportedFlow(item.reported_main_net_inflow), "mono"),
        node("td", item.trade_date, "mono")
      );
      body.append(row);
    });
    byId("supplemental-message").textContent = data.sector_flows.length
      ? "数值为上游报告口径，不代表交易所认证资金流，也不是投资建议。"
      : "仅接受带明确交易日的上游历史流数据；实时排名与无日期结果被拒绝。";
  } catch (error) {
    setBadge(byId("supplemental-status"), "error");
    byId("supplemental-status").textContent = "不可用";
    byId("supplemental-source").textContent = "补充数据 API 不可用";
    byId("market-flow-summary").textContent = "未展示资金流数据。";
    byId("supplemental-message").textContent = safeMessage(error);
  }
}

function resetPositionForm() {
  byId("position-form").reset();
  byId("position-id").value = "";
  byId("position-version").value = "";
  byId("position-form-title").textContent = "新增持仓";
  byId("position-symbol").disabled = false;
  byId("position-cancel").hidden = true;
}

function editPosition(position) {
  byId("position-id").value = position.id;
  byId("position-version").value = position.version;
  byId("position-symbol").value = position.symbol;
  byId("position-symbol").disabled = true;
  byId("position-quantity").value = position.quantity;
  byId("position-avg-cost").value = position.avg_cost;
  byId("position-as-of").value = position.as_of_date;
  byId("position-today-buy").value = position.today_buy_qty;
  byId("position-form-title").textContent = `编辑 ${position.symbol}`;
  byId("position-cancel").hidden = false;
  byId("position-quantity").focus();
}

async function deletePosition(position) {
  if (!window.confirm(`删除 ${position.symbol} 的手动持仓？`)) return;
  try {
    await api(`/portfolio/positions/${position.id}?expected_version=${position.version}`, { method: "DELETE" });
    byId("position-form-status").textContent = `${position.symbol} 已删除。`;
    await loadPortfolio();
  } catch (error) {
    byId("position-form-status").textContent = safeMessage(error);
  }
}

function renderPositions() {
  const body = byId("positions-body");
  clear(body);
  byId("positions-empty").textContent = state.positions.length ? "" : "尚未录入手动持仓。";
  state.positions.forEach((position) => {
    const row = node("tr");
    const symbolCell = node("td");
    const analyze = createSecurityButton(position.symbol, "portfolio");
    symbolCell.append(analyze);
    row.append(symbolCell);
    [position.quantity, position.avg_cost, position.as_of_date, `v${position.version}`]
      .forEach((value) => row.append(node("td", value)));
    const actions = node("td", null, "row-actions");
    const edit = node("button", "编辑", "button-secondary");
    edit.type = "button";
    edit.addEventListener("click", () => editPosition(position));
    const remove = node("button", "删除", "button-danger");
    remove.type = "button";
    remove.addEventListener("click", () => deletePosition(position));
    actions.append(edit, remove);
    row.append(actions);
    body.append(row);
  });
}

function renderValuation(data) {
  setBadge(byId("portfolio-status"), data.status);
  byId("portfolio-meta").textContent = data.data_date
    ? `估值日期 ${data.data_date} · 来源 ${data.source} · 覆盖 ${data.coverage.covered}/${data.coverage.total}`
    : `持仓 ${data.coverage.total} 项 · 无可用估值日期`;
  clear(byId("portfolio-metrics"));
  byId("portfolio-metrics").append(
    metric("覆盖市值", data.covered_market_value === null ? "—" : `¥ ${formatNumber(data.covered_market_value)}`),
    metric("覆盖成本", data.covered_cost_basis === null ? "—" : `¥ ${formatNumber(data.covered_cost_basis)}`),
    metric(
      "覆盖未实现盈亏",
      data.covered_unrealized_pnl === null ? "—" : `¥ ${formatNumber(data.covered_unrealized_pnl)}`,
      Number(data.covered_unrealized_pnl) > 0 ? "market-up" : Number(data.covered_unrealized_pnl) < 0 ? "market-down" : "market-flat"
    ),
    metric("行情覆盖", `${data.coverage.covered}/${data.coverage.total}`)
  );
  const issues = [];
  if (data.coverage.missing_symbols.length) issues.push(`缺失：${data.coverage.missing_symbols.join("、")}`);
  if (data.coverage.suspended_symbols.length) issues.push(`停牌：${data.coverage.suspended_symbols.join("、")}`);
  byId("portfolio-message").textContent = data.status === "empty"
    ? "尚未录入持仓。"
    : issues.length
      ? `${issues.join("；")}。缺失项未计入汇总。`
      : data.valuation_basis;
  byId("portfolio-summary").textContent = data.status === "empty"
    ? "尚未录入手动持仓"
    : data.data_date
      ? `估值日 ${data.data_date} · 覆盖 ${data.coverage.covered}/${data.coverage.total}`
      : `持仓 ${data.coverage.total} 项 · 暂无有效估值`;
}

async function loadPortfolio() {
  setBadge(byId("portfolio-status"), "loading");
  try {
    const [positions, valuation] = await Promise.all([
      api("/portfolio/positions"),
      api("/portfolio/valuation"),
    ]);
    state.positions = positions;
    renderPositions();
    renderValuation(valuation);
  } catch (error) {
    setBadge(byId("portfolio-status"), "error");
    byId("portfolio-meta").textContent = "持仓 API 不可用";
    byId("portfolio-message").textContent = safeMessage(error);
  }
}

async function submitPosition(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const id = byId("position-id").value;
  const payload = {
    symbol: byId("position-symbol").value.trim().toLowerCase(),
    quantity: byId("position-quantity").value,
    avg_cost: byId("position-avg-cost").value,
    as_of_date: byId("position-as-of").value,
    today_buy_qty: byId("position-today-buy").value,
  };
  let path = "/portfolio/positions";
  let method = "POST";
  if (id) {
    delete payload.symbol;
    payload.expected_version = Number(byId("position-version").value);
    path = `/portfolio/positions/${id}`;
    method = "PUT";
  }
  const submit = form.querySelector('button[type="submit"]');
  submit.disabled = true;
  try {
    await api(path, { method, body: JSON.stringify(payload) });
    byId("position-form-status").textContent = id ? "持仓已更新。" : "持仓已创建。";
    resetPositionForm();
    await loadPortfolio();
  } catch (error) {
    byId("position-form-status").textContent = safeMessage(error);
  } finally {
    submit.disabled = false;
  }
}

function selectedWatchlist() {
  return state.watchlists.find((item) => item.id === state.selectedWatchlistId);
}

async function loadWatchlistItems() {
  const list = byId("watchlist-items");
  clear(list);
  const current = selectedWatchlist();
  byId("watchlist-empty").textContent = current ? "正在读取列表项目…" : "尚无自选列表。";
  byId("watchlist-delete").disabled = !current;
  byId("watchlist-item-form").querySelector("button").disabled = !current;
  if (!current) return;
  try {
    const items = await api(`/watchlists/${current.id}/items`);
    byId("watchlist-empty").textContent = items.length ? "" : "当前列表还没有证券。";
    items.forEach((item) => {
      const row = node("li");
      const analyze = createSecurityButton(item.symbol, "watchlists");
      row.append(analyze);
      const remove = node("button", "移除", "button-secondary");
      remove.type = "button";
      remove.addEventListener("click", async () => {
        try {
          await api(`/watchlists/${current.id}/items/${encodeURIComponent(item.symbol)}`, { method: "DELETE" });
          await loadWatchlistItems();
        } catch (error) {
          byId("watchlist-status").textContent = safeMessage(error);
        }
      });
      row.append(remove);
      list.append(row);
    });
  } catch (error) {
    byId("watchlist-empty").textContent = safeMessage(error);
  }
}

async function loadWatchlists() {
  try {
    state.watchlists = await api("/watchlists");
    if (!state.watchlists.some((item) => item.id === state.selectedWatchlistId)) {
      state.selectedWatchlistId = state.watchlists[0]?.id || null;
    }
    const select = byId("watchlist-select");
    clear(select);
    if (!state.watchlists.length) {
      const option = node("option", "暂无列表");
      option.value = "";
      select.append(option);
      select.disabled = true;
    } else {
      select.disabled = false;
      state.watchlists.forEach((item) => {
        const option = node("option", `${item.name} · v${item.version}`);
        option.value = item.id;
        option.selected = item.id === state.selectedWatchlistId;
        select.append(option);
      });
    }
    await loadWatchlistItems();
  } catch (error) {
    byId("watchlist-status").textContent = safeMessage(error);
  }
}

async function loadStrategies() {
  setBadge(byId("strategy-status"), "loading");
  clear(byId("strategy-candidates"));
  clear(byId("strategy-results"));
  try {
    state.strategies = await api("/strategies");
    const select = byId("strategy-select");
    clear(select);
    if (!state.strategies.length) {
      const option = node("option", "暂无已保存策略");
      option.value = "";
      select.append(option);
      select.disabled = true;
      byId("strategy-run-form").querySelector('button[type="submit"]').disabled = true;
      setBadge(byId("strategy-status"), "empty");
      byId("strategy-message").textContent = "暂无策略版本。可通过策略 API 创建严格 JSON AST。";
      byId("strategy-results-empty").textContent = "没有运行结果。";
      return;
    }
    select.disabled = false;
    byId("strategy-run-form").querySelector('button[type="submit"]').disabled = false;
    state.strategies.forEach((strategy) => {
      const option = node("option", `${strategy.name} · v${strategy.current_version}`);
      option.value = strategy.id;
      option.dataset.version = strategy.current_version;
      select.append(option);
    });
    const runsByStrategy = await Promise.all(
      state.strategies.map((strategy) => api(`/strategies/${strategy.id}/runs`))
    );
    const latestRuns = runsByStrategy.map((runs) => runs.at(-1)).filter(Boolean);
    const matched = latestRuns.flatMap((run) =>
      run.results
        .filter((result) => result.status === "matched")
        .map((result) => ({ run, result }))
    );
    setBadge(byId("strategy-status"), latestRuns.length ? "ready" : "empty");
    byId("strategy-meta").textContent = latestRuns.length
      ? `${latestRuns.length} 个策略有运行记录 · ${matched.length} 个规则匹配`
      : "已保存策略尚无运行记录";
    byId("strategy-message").textContent = matched.length
      ? "以下是历史回放中的规则匹配，不是买卖建议。"
      : "当前没有已记录的规则匹配。";
    matched.forEach(({ run, result }) => {
      const row = node("div", null, "candidate-row");
      row.append(
        node("span", result.symbol, "mono"),
        node("span", `匹配 · ${run.as_of_date} · v${run.strategy_version}`, "market-up")
      );
      byId("strategy-candidates").append(row);
    });
    renderRunResults(latestRuns.at(-1));
  } catch (error) {
    setBadge(byId("strategy-status"), "error");
    byId("strategy-message").textContent = safeMessage(error);
  }
}

function renderRunResults(run) {
  const list = byId("strategy-results");
  clear(list);
  if (!run) {
    byId("strategy-results-empty").textContent = "没有运行结果。";
    return;
  }
  byId("strategy-results-empty").textContent = "";
  run.results.forEach((result) => {
    const row = node("li");
    const label = result.status === "matched"
      ? "规则匹配"
      : result.status === "not_matched"
        ? "未匹配"
        : result.status === "excluded"
          ? "已排除"
          : "数据不足";
    row.append(
      node("span", result.symbol, "mono"),
      node("span", `${label} · ${result.signal_date}`, result.status === "matched" ? "market-up" : "market-flat")
    );
    list.append(row);
  });
}

const ALERT_STATE_TEXT = {
  pending: "待处理",
  eligible: "可评估",
  triggered: "已触发",
  acknowledged: "已确认",
  resolved: "已解除",
  suppressed: "已抑制",
  error: "错误",
};

function populateAlertConfiguration() {
  const watchlistSelect = byId("alert-watchlist");
  const strategySelect = byId("alert-strategy");
  clear(watchlistSelect);
  clear(strategySelect);
  state.watchlists.forEach((watchlist) => {
    const option = node("option", watchlist.name);
    option.value = watchlist.id;
    watchlistSelect.append(option);
  });
  state.strategies.forEach((strategy) => {
    const option = node("option", `${strategy.name} · v${strategy.current_version}`);
    option.value = strategy.id;
    option.dataset.version = strategy.current_version;
    strategySelect.append(option);
  });
  watchlistSelect.disabled = !state.watchlists.length;
  strategySelect.disabled = !state.strategies.length;
  byId("alert-rule-form").querySelector('button[type="submit"]').disabled =
    !state.watchlists.length || !state.strategies.length;
}

function populateAlertRules() {
  const select = byId("alert-rule-select");
  clear(select);
  if (!state.alertRules.length) {
    const option = node("option", "暂无预警规则");
    option.value = "";
    select.append(option);
    select.disabled = true;
    byId("alert-evaluate").disabled = true;
    return;
  }
  state.alertRules.forEach((rule) => {
    const option = node("option", `${rule.name} · 策略 v${rule.strategy_version}`);
    option.value = rule.id;
    select.append(option);
  });
  select.disabled = false;
  byId("alert-evaluate").disabled = false;
}

function alertActionButton(label, event, action) {
  const button = node("button", label, "button-secondary");
  button.type = "button";
  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      await api(`/alerts/events/${event.id}/${action}`, { method: "POST" });
      byId("alert-action-status").textContent = `${event.symbol} 状态已更新。`;
      await loadAlerts();
    } catch (error) {
      byId("alert-action-status").textContent = safeMessage(error);
      button.disabled = false;
    }
  });
  return button;
}

function renderAlertEvents(payload) {
  const list = byId("alert-history");
  clear(list);
  state.alertEvents = payload.items;
  setBadge(byId("alert-status"), payload.status);
  byId("alert-meta").textContent = payload.items.length
    ? `本地历史 ${payload.items.length} 条 · 仅工作台内展示`
    : "本地尚无预警评估记录";
  byId("alert-unacknowledged").textContent = `未确认 ${payload.unacknowledged}`;
  byId("alert-unacknowledged").className = `status-badge ${
    payload.unacknowledged ? "status-partial" : "status-empty"
  }`;
  byId("alert-overview").textContent = payload.items.length
    ? `${payload.unacknowledged} 条未确认 · 共 ${payload.items.length} 条历史`
    : "暂无本地预警评估记录";
  byId("alert-empty").textContent = payload.items.length
    ? ""
    : "暂无预警。创建规则后，对明确的已收盘交易日执行评估。";

  payload.items.forEach((event) => {
    const item = node("li", null, "alert-event");
    const heading = node("div", null, "alert-event-heading");
    heading.append(
      node("strong", event.symbol, "mono"),
      node(
        "span",
        ALERT_STATE_TEXT[event.state] || event.state,
        `mono alert-state-${event.state}`
      )
    );
    const source = event.source || "来源缺失";
    const meta = node(
      "p",
      `${event.signal_date} · ${source} · 质量 ${event.quality_status}`,
      "alert-event-meta"
    );
    const issues = node(
      "p",
      event.quality_issues.length
        ? `抑制/质量原因：${event.quality_issues.join("、")}`
        : "数据质量检查通过",
      "state-message"
    );
    const details = node("details", null, "alert-explanation");
    details.append(
      node("summary", "查看触发条件解释"),
      node("code", JSON.stringify(event.explanation, null, 2), "mono")
    );
    const actions = node("div", null, "alert-event-actions");
    if (event.state === "triggered") {
      actions.append(
        alertActionButton("确认", event, "acknowledge"),
        alertActionButton("忽略", event, "suppress")
      );
    } else if (event.state === "acknowledged") {
      actions.append(alertActionButton("忽略", event, "suppress"));
    } else if (event.state === "suppressed") {
      actions.append(alertActionButton("恢复评估", event, "restore"));
    }
    item.append(heading, meta, issues, details, actions);
    list.append(item);
  });
}

async function loadAlerts() {
  setBadge(byId("alert-status"), "loading");
  try {
    const [rules, events] = await Promise.all([
      api("/alerts/rules"),
      api("/alerts/events"),
    ]);
    state.alertRules = rules;
    populateAlertConfiguration();
    populateAlertRules();
    renderAlertEvents(events);
  } catch (error) {
    setBadge(byId("alert-status"), "error");
    byId("alert-meta").textContent = "预警 API 不可用";
    byId("alert-empty").textContent = safeMessage(error);
  }
}

async function loadWorkspace() {
  byId("global-status").textContent = "正在读取本地数据视图…";
  await Promise.all([
    loadMarket(),
    loadSupplementalMarket(),
    loadPortfolio(),
    loadWatchlists(),
    loadStrategies(),
  ]);
  await loadAlerts();
  await loadMarketStatus();
  byId("global-status").textContent = "本地数据视图读取完成。";
}

const VIEW_TITLES = {
  overview: "收盘总览",
  "market-structure": "市场结构",
  sectors: "板块与成交",
  portfolio: "持仓",
  strategies: "策略",
  watchlists: "自选预警",
  security: "个股技术分析",
};

function activateView(identifier, updateHash = true) {
  const requested = identifier.startsWith("security/") ? "security" : identifier;
  const target = VIEW_TITLES[requested] ? requested : "overview";
  document.querySelectorAll("[data-view]").forEach((view) => {
    const active = view.dataset.view === target;
    view.hidden = !active;
    view.classList.toggle("is-active", active);
  });
  document.querySelectorAll(".primary-nav [data-view-target]").forEach((link) => {
    if (link.dataset.viewTarget === target) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  byId("view-title").textContent = VIEW_TITLES[target];
  document.title = `${VIEW_TITLES[target]} · Stock EVA`;
  if (updateHash && window.location.hash !== `#${target}`) {
    window.history.pushState(null, "", `#${target}`);
  }
  window.requestAnimationFrame(() => {
    window.requestAnimationFrame(() => window.scrollTo({ top: 0, behavior: "auto" }));
  });
}

window.stockEvaActivateView = activateView;

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("[data-view-target]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      activateView(link.dataset.viewTarget);
    });
  });
  window.addEventListener("popstate", () => activateView(window.location.hash.slice(1), false));
  activateView(window.location.hash.slice(1), false);
  byId("position-form").addEventListener("submit", submitPosition);
  byId("position-cancel").addEventListener("click", resetPositionForm);
  byId("watchlist-select").addEventListener("change", (event) => {
    state.selectedWatchlistId = event.target.value || null;
    loadWatchlistItems();
  });
  byId("watchlist-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    try {
      const created = await api("/watchlists", {
        method: "POST",
        body: JSON.stringify({ name: byId("watchlist-name").value.trim() }),
      });
      state.selectedWatchlistId = created.id;
      form.reset();
      byId("watchlist-status").textContent = "自选列表已创建。";
      await loadWatchlists();
      await loadAlerts();
    } catch (error) {
      byId("watchlist-status").textContent = safeMessage(error);
    }
  });
  byId("watchlist-item-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const current = selectedWatchlist();
    if (!current) return;
    try {
      const response = await api(`/watchlists/${current.id}/items`, {
        method: "POST",
        body: JSON.stringify({ symbol: byId("watchlist-symbol").value.trim().toLowerCase() }),
      });
      byId("watchlist-status").textContent = response.created ? "已加入自选。" : "该证券已在列表中。";
      form.reset();
      await loadWatchlistItems();
    } catch (error) {
      byId("watchlist-status").textContent = safeMessage(error);
    }
  });
  byId("watchlist-delete").addEventListener("click", async () => {
    const current = selectedWatchlist();
    if (!current || !window.confirm(`删除自选列表“${current.name}”？`)) return;
    try {
      await api(`/watchlists/${current.id}?expected_version=${current.version}`, { method: "DELETE" });
      state.selectedWatchlistId = null;
      byId("watchlist-status").textContent = "自选列表已删除。";
      await loadWatchlists();
      await loadAlerts();
    } catch (error) {
      byId("watchlist-status").textContent = safeMessage(error);
    }
  });
  byId("strategy-run-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const select = byId("strategy-select");
    const option = select.selectedOptions[0];
    const symbols = byId("strategy-symbols").value
      .split(",")
      .map((value) => value.trim().toLowerCase())
      .filter(Boolean);
    try {
      const run = await api(`/strategies/${select.value}/runs`, {
        method: "POST",
        body: JSON.stringify({
          version: Number(option.dataset.version),
          symbols,
          as_of_date: byId("strategy-date").value,
        }),
      });
      byId("strategy-run-status").textContent = `运行已保存：${run.status}，数据指纹 ${run.data_fingerprint.slice(0, 10)}…`;
      renderRunResults(run);
      await loadStrategies();
    } catch (error) {
      byId("strategy-run-status").textContent = safeMessage(error);
    }
  });
  byId("alert-rule-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const strategyOption = byId("alert-strategy").selectedOptions[0];
    try {
      await api("/alerts/rules", {
        method: "POST",
        body: JSON.stringify({
          name: byId("alert-rule-name").value.trim(),
          watchlist_id: byId("alert-watchlist").value,
          strategy_id: byId("alert-strategy").value,
          strategy_version: Number(strategyOption.dataset.version),
        }),
      });
      form.reset();
      byId("alert-rule-status").textContent = "预警规则已保存。";
      await loadAlerts();
    } catch (error) {
      byId("alert-rule-status").textContent = safeMessage(error);
    }
  });
  byId("alert-evaluate-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const submit = byId("alert-evaluate");
    submit.disabled = true;
    try {
      const result = await api(
        `/alerts/rules/${byId("alert-rule-select").value}/evaluate`,
        {
          method: "POST",
          body: JSON.stringify({
            signal_date: byId("alert-signal-date").value,
          }),
        }
      );
      byId("alert-evaluate-status").textContent =
        `评估完成：${STATUS_TEXT[result.status] || result.status}，记录 ${result.events.length} 条。`;
      await loadAlerts();
    } catch (error) {
      byId("alert-evaluate-status").textContent = safeMessage(error);
    } finally {
      submit.disabled = !state.alertRules.length;
    }
  });
  loadWorkspace();
});
