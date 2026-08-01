import type {
  EvidenceItem,
  MarketComponentScore,
  MarketRegimeResponse,
  MetricScore,
  SectorLeadersResponse,
  SectorRanking,
  SectorRotationResponse,
} from "../decision-api";
import type { DecisionOverview, DecisionQuery } from "../decision-state";

export type LeaderViewState =
  | { phase: "idle" }
  | { phase: "loading" }
  | { phase: "ready"; response: SectorLeadersResponse }
  | { phase: "error"; status: number | null; message: string };

export type DecisionFlowState =
  | { phase: "loading"; query: DecisionQuery }
  | {
      phase: "error";
      query: DecisionQuery;
      status: number | null;
      message: string;
    }
  | {
      phase: "ready";
      query: DecisionQuery;
      overview: DecisionOverview;
      selectedSectorId: string | null;
      leaders: LeaderViewState;
    };

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

function value(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined) return "—";
  return new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits: digits,
  }).format(value);
}

function percent(value: number | null | undefined): string {
  return value === null || value === undefined
    ? "—"
    : `${Math.round(value * 100)}%`;
}

function stage(
  name: string,
  kicker: string,
  title: string,
): { item: HTMLLIElement; body: HTMLElement } {
  const item = element("li", undefined, "decision-stage panel");
  item.dataset.decisionStage = name;
  item.append(element("p", kicker, "panel-kicker"), element("h3", title));
  const body = element("div", undefined, "decision-stage-body");
  item.append(body);
  return { item, body };
}

function status(text: string, kind = "degraded"): HTMLElement {
  const result = element("p", text, `decision-status decision-status-${kind}`);
  result.setAttribute("role", "status");
  return result;
}

function rows(entries: Array<[string, string]>): HTMLElement {
  const list = element("dl", undefined, "decision-facts");
  for (const [term, description] of entries) {
    const row = element("div");
    row.append(
      element("dt", term),
      document.createTextNode(" "),
      element("dd", description, "mono"),
    );
    list.append(row);
  }
  return list;
}

function tokens(label: string, items: string[]): HTMLElement {
  const wrapper = element("div", undefined, "decision-token-row");
  wrapper.append(element("strong", label));
  wrapper.append(
    element("span", items.length ? items.join(" · ") : "无", "mono"),
  );
  return wrapper;
}

function evidenceText(items: EvidenceItem[]): string {
  return items.length
    ? items.map((item) => `${item.code}${item.detail ? `：${item.detail}` : ""}`).join(" · ")
    : "无";
}

function metricDetails(metrics: MetricScore[]): HTMLElement {
  const wrapper = element("div", undefined, "decision-metric-list");
  for (const metric of metrics) {
    const detail = element("details", undefined, "decision-detail");
    const summary = element(
      "summary",
      `${metric.metric} · 得分 ${value(metric.score)}`,
    );
    const coverage =
      metric.observed_count !== undefined && metric.eligible_count !== undefined
        ? `${metric.observed_count} / ${metric.eligible_count}`
        : "—";
    detail.append(
      summary,
      rows([
        ["原始值", `${value(metric.raw_value, 4)} ${metric.unit}`],
        ["覆盖样本", coverage],
        ["覆盖率", percent(metric.coverage_ratio)],
        ["权重", value(metric.weight, 4)],
        ["加权分", value(metric.weighted_score, 4)],
        ["公式版本", metric.formula_version],
        ["质量", metric.quality_status],
      ]),
      tokens("缺失输入", metric.missing_inputs),
      tokens("质量问题", metric.quality_issues),
    );
    wrapper.append(detail);
  }
  return wrapper;
}

function componentDetails(components: MarketComponentScore[]): HTMLElement {
  const detail = element("details", undefined, "decision-detail");
  detail.append(element("summary", "查看市场分项、支持证据与反例"));
  if (!components.length) {
    detail.append(element("p", "没有可展示的市场分项。", "state-message"));
    return detail;
  }
  for (const component of components) {
    const section = element("section", undefined, "decision-component");
    section.append(
      element(
        "h4",
        `${component.name} · ${value(component.score)} · ${component.quality_status}`,
      ),
      rows([
        ["权重", value(component.weight, 4)],
        ["加权分", value(component.weighted_score, 4)],
        ["公式版本", component.formula_version],
      ]),
      tokens("支持", [evidenceText(component.supporting_evidence)]),
      tokens("反例", [evidenceText(component.contrary_evidence)]),
      tokens("缺失输入", component.missing_inputs),
      tokens("质量问题", component.quality_issues),
    );
    detail.append(section);
  }
  return detail;
}

function scopeWarning(): HTMLElement {
  return status(
    "仅沪深主板价格样本 / 不能代表全 A 股；以下状态是受限样本的后端分析结果。",
    "warning",
  );
}

function renderMarket(body: HTMLElement, market: MarketRegimeResponse): void {
  body.append(scopeWarning());
  if (market.status === "empty") {
    body.append(
      element("p", "市场状态证据不足，未形成可用结论。", "decision-empty"),
      tokens("缺失输入", market.missing_inputs),
      tokens("质量问题", market.quality_issues),
      componentDetails(market.component_scores),
    );
    return;
  }
  body.append(
    rows([
      ["战略样本状态", market.strategic_state],
      ["战术样本状态", market.tactical_state],
      ["后端总分", value(market.total_score)],
      ["置信度", `${percent(market.confidence.value)} / ${market.confidence.level}`],
      ["请求时点", market.as_of],
      ["实际数据日", market.data_as_of ?? "—"],
      ["公式版本", market.formula_version],
      ["样本范围", market.actual_market_scope.scope_status],
      ["覆盖证据", market.actual_market_scope.coverage_evidence_status],
    ]),
    componentDetails(market.component_scores),
    tokens("支持证据", [evidenceText(market.supporting_evidence)]),
    tokens("反例", [evidenceText(market.contrary_evidence)]),
    tokens("缺失输入", market.missing_inputs),
    tokens("质量问题", market.quality_issues),
  );
}

function renderFundFlow(body: HTMLElement): void {
  body.append(
    status("资金证据尚不可用", "missing"),
    element(
      "p",
      "Release 1 未发布 L1/L2/L3 资金证据；成交额仅属于量价证据，不等于净流入，也不用于推断主力方向。",
      "state-message",
    ),
    rows([
      ["发布阶段", "Release 2"],
      ["当前证据层级", "missing / unavailable"],
      ["可发布趋势", "否"],
    ]),
  );
}

function lineage(rotation: SectorRotationResponse): HTMLElement {
  const detail = element("details", undefined, "decision-detail");
  detail.append(element("summary", "查看分类、行情血缘与实际覆盖"));
  const classification = rotation.classification_lineage;
  const market = rotation.market_lineage;
  detail.append(
    rows([
      ["分类代际", classification?.generation_id ?? "—"],
      ["分类版本", classification?.schema_version ?? "—"],
      ["分类源日期", classification?.source_snapshot_date ?? "—"],
      ["日期语义", classification?.source_date_semantics ?? "—"],
      ["分类覆盖率", percent(classification?.coverage_ratio)],
      ["行情版本", market?.source_version ?? "—"],
      ["行情最新输入", market?.latest_input_date ?? "—"],
      ["行情内容哈希", market?.content_hash ?? "—"],
      ["实际范围", rotation.actual_scope.scope_status],
      ["定价/分类", `${rotation.actual_scope.priced_classified_symbols} / ${rotation.actual_scope.classification_eligible_symbols}`],
    ]),
  );
  return detail;
}

function renderSector(
  ranking: SectorRanking,
  selected: boolean,
  query: DecisionQuery,
): HTMLElement {
  const article = element(
    "article",
    undefined,
    `decision-ranking${selected ? " is-selected" : ""}`,
  );
  const heading = element("div", undefined, "decision-ranking-heading");
  const title = element("div");
  title.append(
    element("span", `后端顺序 ${ranking.rank}`, "quiet-tag"),
    element("h4", ranking.sector_name),
    element(
      "p",
      `${ranking.sector_id} · 得分 ${value(ranking.total_score)} · 置信度 ${percent(ranking.confidence.value)}`,
      "meta-line mono",
    ),
  );
  const button = element("button", `查看 ${ranking.sector_name} 龙头`);
  button.type = "button";
  button.dataset.decisionSector = ranking.sector_id;
  button.dataset.decisionAsOf = query.asOf;
  button.dataset.decisionTaxonomy = query.taxonomyId;
  heading.append(title, button);
  article.append(
    heading,
    rows([
      ["成员/有价", `${ranking.priced_member_count} / ${ranking.member_count}`],
      ["质量", ranking.quality_status],
      ["龙头数量", ranking.leader_count === undefined ? "—" : String(ranking.leader_count)],
      ["扩散度", percent(ranking.leader_diffusion)],
      ["持续", ranking.persistence_days === undefined ? "—" : `${ranking.persistence_days} 日`],
    ]),
    metricDetails(ranking.metric_scores),
    tokens("支持证据", [evidenceText(ranking.supporting_evidence)]),
    tokens("反例", [evidenceText(ranking.contrary_evidence)]),
    tokens("缺失输入", ranking.missing_inputs),
    tokens("质量问题", ranking.quality_issues),
  );
  return article;
}

function renderSectors(
  body: HTMLElement,
  sectors: SectorRotationResponse,
  selectedSectorId: string | null,
  query: DecisionQuery,
): void {
  body.append(scopeWarning());
  if (!sectors.rankings.length) {
    body.append(
      element(
        "p",
        "板块数据为空不代表市场没有热点；当前没有足够、已发布的分类与行情证据。",
        "decision-empty",
      ),
      tokens("缺失输入", sectors.missing_inputs),
      tokens("质量问题", sectors.quality_issues),
      lineage(sectors),
    );
    return;
  }
  body.append(
    status(`后端返回 ${sectors.rankings.length} 个板块；保持原始顺序`, sectors.quality_status),
  );
  const rankings = element("div", undefined, "decision-rankings");
  for (const ranking of sectors.rankings) {
    rankings.append(
      renderSector(ranking, ranking.sector_id === selectedSectorId, query),
    );
  }
  body.append(
    rankings,
    lineage(sectors),
    rows([
      ["响应公式", sectors.formula_version],
      ["分类体系", sectors.taxonomy_id],
      ["实际数据日", sectors.data_as_of ?? "—"],
    ]),
    tokens("缺失输入", sectors.missing_inputs),
    tokens("质量问题", sectors.quality_issues),
  );
}

function renderPortfolio(body: HTMLElement): void {
  body.append(
    status("尚无本地组合风险输入", "missing"),
    element(
      "p",
      "组合风险与目标暴露属于 Release 2。没有现金、净值和风险预算时，不生成仓位区间，也不猜测个人仓位。",
      "state-message",
    ),
  );
}

function renderLeaders(
  body: HTMLElement,
  state: LeaderViewState,
  query: DecisionQuery,
  selectedSectorId: string | null,
): void {
  body.append(
    status(
      "龙头仅为后端受限样本候选；不可作为操作首选，不构成买卖建议。",
      "warning",
    ),
  );
  if (state.phase === "idle") {
    body.append(element("p", "先选择一个有证据的板块。", "decision-empty"));
    return;
  }
  if (state.phase === "loading") {
    body.append(element("p", "正在读取后端龙头原因与风险…", "state-message"));
    return;
  }
  if (state.phase === "error") {
    body.append(
      element("p", `龙头证据读取失败：${state.message}`, "decision-empty"),
    );
    return;
  }
  const response = state.response;
  if (!response.candidates.length) {
    body.append(
      element("p", "当前没有可展示候选；这不等于板块没有龙头。", "decision-empty"),
      tokens("缺失输入", response.missing_inputs),
    );
  }
  const candidates = element("div", undefined, "decision-candidates");
  for (const candidate of response.candidates) {
    const article = element("article", undefined, "decision-candidate");
    const heading = element("div", undefined, "decision-ranking-heading");
    const title = element("div");
    title.append(
      element("span", `后端顺序 ${candidate.rank}`, "quiet-tag"),
      element("h4", `${candidate.name} · ${candidate.symbol}`),
      element(
        "p",
        `得分 ${value(candidate.total_score)} · 置信度 ${percent(candidate.confidence.value)}`,
        "meta-line mono",
      ),
    );
    const button = element(
      "button",
      `打开 ${candidate.symbol} 技术驾驶舱`,
    );
    button.type = "button";
    button.dataset.securitySymbol = candidate.symbol;
    button.dataset.securitySource = "sectors";
    button.dataset.securityAsOf = query.asOf;
    button.dataset.securityTaxonomy = query.taxonomyId;
    button.dataset.securitySector = selectedSectorId ?? response.sector_id;
    heading.append(title, button);
    article.append(
      heading,
      rows([
        ["可操作性", `${candidate.actionable_primary ? "可" : "不可"}作为操作首选`],
        ["操作状态", candidate.actionability_status],
        ["涨跌停锁定", candidate.limit_lock_status === "unavailable" ? "涨跌停锁定状态不可用" : candidate.limit_lock_status],
        ["质量", candidate.quality_status],
      ]),
      metricDetails(candidate.metric_scores),
      tokens("支持证据", [evidenceText(candidate.supporting_evidence)]),
      tokens("反例", [evidenceText(candidate.contrary_evidence)]),
      tokens("缺失输入", candidate.missing_inputs),
      tokens("风险/质量", candidate.quality_issues),
    );
    candidates.append(article);
  }
  body.append(candidates);
  if (response.exclusions.length) {
    const exclusions = element("details", undefined, "decision-detail");
    exclusions.append(element("summary", "查看被排除证券与原因"));
    for (const exclusion of response.exclusions) {
      exclusions.append(
        element(
          "p",
          `${exclusion.symbol} · ${exclusion.reasons.join(" · ")}`,
          "mono state-message",
        ),
      );
    }
    body.append(exclusions);
  }
  body.append(
    rows([
      ["响应公式", response.formula_version],
      ["板块", response.sector_name ?? response.sector_id],
      ["资金证据", `${response.fund_flow_evidence.status} / Release 2`],
      ["实际数据日", response.data_as_of ?? "—"],
    ]),
    tokens("缺失输入", response.missing_inputs),
    tokens("质量问题", response.quality_issues),
  );
}

function errorCopy(statusCode: number | null, message: string): string {
  if (statusCode === 422) return `日期不在可读取范围：${message}`;
  if (statusCode === 503) return `本地行情存储暂不可用：${message}`;
  return `本地分析接口不可用：${message}`;
}

export function renderDecisionFlow(
  root: HTMLElement,
  state: DecisionFlowState,
): void {
  root.replaceChildren();
  const header = element("header", undefined, "decision-header");
  const copy = element("div");
  copy.append(
    element("p", "RELEASE 1 / EVIDENCE FIRST", "panel-kicker"),
    element("h2", "市场 → 板块 → 龙头 → 个股"),
    element(
      "p",
      `请求时点 ${state.query.asOf} · 分类 ${state.query.taxonomyId} · 后端结论只读展示`,
      "meta-line mono",
    ),
  );
  header.append(copy);
  root.append(header);

  const flow = element("ol", undefined, "decision-flow-list");
  flow.setAttribute("aria-label", "收盘后证据决策顺序");
  const marketStage = stage("market", "01 / REGIME", "市场状态");
  const fundStage = stage("fund-flow", "02 / FUND EVIDENCE", "资金证据");
  const sectorStage = stage("sectors", "03 / ROTATION", "板块轮动");
  const portfolioStage = stage(
    "portfolio-risk",
    "04 / PORTFOLIO",
    "组合风险",
  );
  const leaderStage = stage("leaders", "05 / LEADERS", "龙头候选 → 个股");
  flow.append(
    marketStage.item,
    fundStage.item,
    sectorStage.item,
    portfolioStage.item,
    leaderStage.item,
  );
  root.append(flow);

  renderFundFlow(fundStage.body);
  renderPortfolio(portfolioStage.body);
  if (state.phase === "loading") {
    marketStage.body.append(status("正在读取市场状态与板块轮动…", "loading"));
    sectorStage.body.append(status("等待后端板块证据…", "loading"));
    renderLeaders(leaderStage.body, { phase: "idle" }, state.query, null);
    return;
  }
  if (state.phase === "error") {
    const alert = element("p", errorCopy(state.status, state.message), "decision-empty");
    alert.setAttribute("role", "alert");
    marketStage.body.append(alert, scopeWarning());
    sectorStage.body.append(
      element("p", "板块数据未读取；不解释为无热点。", "decision-empty"),
    );
    renderLeaders(leaderStage.body, { phase: "idle" }, state.query, null);
    return;
  }
  renderMarket(marketStage.body, state.overview.market);
  renderSectors(
    sectorStage.body,
    state.overview.sectors,
    state.selectedSectorId,
    state.query,
  );
  renderLeaders(
    leaderStage.body,
    state.leaders,
    state.query,
    state.selectedSectorId,
  );
}
