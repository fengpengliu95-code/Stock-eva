import type {
  MarketRegimeResponse,
  SectorLeadersResponse,
  SectorRotationResponse,
} from "../decision-api";
import type { DecisionContext } from "../state";

export type SecurityEvidenceSlice<T> =
  | { phase: "ready"; response: T }
  | { phase: "error"; status: number | null; message: string };

interface SecurityEvidenceContext {
  symbol: string;
  context: DecisionContext;
}

export type SecurityEvidenceState =
  | { phase: "idle" }
  | (SecurityEvidenceContext & { phase: "loading" })
  | (SecurityEvidenceContext & {
      phase: "ready";
      market: SecurityEvidenceSlice<MarketRegimeResponse>;
      sectors: SecurityEvidenceSlice<SectorRotationResponse>;
      leaders: SecurityEvidenceSlice<SectorLeadersResponse>;
    });

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

function facts(entries: Array<[string, string]>): HTMLElement {
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

function panel(title: string, kicker: string): HTMLElement {
  const result = element("article", undefined, "panel security-evidence-panel");
  result.append(element("p", kicker, "panel-kicker"), element("h3", title));
  return result;
}

function errorState(
  target: HTMLElement,
  label: string,
  error: Extract<SecurityEvidenceSlice<unknown>, { phase: "error" }>,
): void {
  const status = error.status === null ? "连接错误" : `HTTP ${error.status}`;
  const message = element(
    "p",
    `${label}暂不可用 · ${status} · ${error.message}`,
    "decision-status decision-status-degraded",
  );
  message.setAttribute("role", "status");
  target.append(message);
}

function renderMarket(
  target: HTMLElement,
  slice: SecurityEvidenceSlice<MarketRegimeResponse>,
): void {
  if (slice.phase === "error") {
    errorState(target, "市场证据", slice);
    return;
  }
  const market = slice.response;
  target.append(
    facts([
      ["战略样本状态", market.strategic_state],
      ["战术样本状态", market.tactical_state],
      ["质量", market.quality_status],
      ["样本范围", market.actual_market_scope.scope_status],
      ["实际数据日", market.data_as_of ?? "—"],
    ]),
    element(
      "p",
      `受限样本：${market.actual_market_scope.conclusion_disclaimer}`,
      "state-message",
    ),
  );
}

function renderSector(
  target: HTMLElement,
  context: DecisionContext,
  slice: SecurityEvidenceSlice<SectorRotationResponse>,
): void {
  if (slice.phase === "error") {
    errorState(target, "板块证据", slice);
    return;
  }
  const response = slice.response;
  const ranking = response.rankings.find(
    (item) => item.sector_id === context.sectorId,
  );
  if (!ranking) {
    target.append(
      element(
        "p",
        "该板块未出现在后端排名中；不解释为市场没有热点。",
        "decision-status decision-status-degraded",
      ),
    );
    return;
  }
  target.append(
    facts([
      ["板块", `${ranking.sector_name} · ${ranking.sector_id}`],
      ["后端顺序", String(ranking.rank)],
      ["后端总分", ranking.total_score === null ? "—" : String(ranking.total_score)],
      ["排名资格", ranking.ranking_eligible ? "eligible" : "excluded"],
      ["质量", ranking.quality_status],
      ["实际数据日", response.data_as_of ?? "—"],
    ]),
  );
}

function renderLeader(
  target: HTMLElement,
  symbol: string,
  slice: SecurityEvidenceSlice<SectorLeadersResponse>,
): void {
  if (slice.phase === "error") {
    errorState(target, "龙头证据", slice);
    return;
  }
  const response = slice.response;
  const candidate = response.candidates.find((item) => item.symbol === symbol);
  if (candidate) {
    target.append(
      facts([
        ["证券", `${candidate.name} · ${candidate.symbol}`],
        ["后端顺序", String(candidate.rank)],
        ["研究龙头资格", candidate.leader_qualified ? "qualified" : "not qualified"],
        ["资格原因", candidate.qualification_reasons.join(" · ") || "无"],
        ["不合格原因", candidate.disqualification_reasons.join(" · ") || "无"],
        ["操作状态", candidate.actionability_status],
        ["涨跌停锁定状态", candidate.limit_lock_status],
        ["质量", candidate.quality_status],
      ]),
      element("p", "后端候选仅供复盘研究，不构成操作结论。", "state-message"),
    );
    return;
  }
  const exclusion = response.exclusions.find((item) => item.symbol === symbol);
  if (exclusion) {
    target.append(
      element(
        "p",
        `该证券被后端排除：${exclusion.reasons.join(" · ")}`,
        "decision-status decision-status-degraded",
      ),
    );
    return;
  }
  target.append(
    element(
      "p",
      "该证券未出现在候选或排除列表；不解释为没有板块关联。",
      "decision-status decision-status-degraded",
    ),
  );
}

function renderFundFlow(target: HTMLElement): void {
  const message = element(
    "p",
    "资金证据 missing / unavailable",
    "decision-status decision-status-missing",
  );
  message.setAttribute("role", "status");
  target.append(
    message,
    element(
      "p",
      "Release 1 尚无可发布资金趋势；成交额不等于净流入，不能据此推断主力方向。",
      "state-message",
    ),
  );
}

export function renderSecurityEvidence(
  root: HTMLElement,
  state: SecurityEvidenceState,
): void {
  root.replaceChildren();
  if (state.phase === "idle") return;

  const header = element("header", undefined, "security-evidence-header");
  header.append(
    element("p", "DECISION CONTEXT / BACKEND EVIDENCE", "panel-kicker"),
    element("h2", "同一复盘上下文"),
    element(
      "p",
      `${state.symbol} · 请求时点 ${state.context.asOf} · 分类 ${state.context.taxonomyId} · 板块 ${state.context.sectorId}`,
      "meta-line mono",
    ),
  );
  root.append(header);
  if (state.phase === "loading") {
    const loading = element(
      "p",
      "正在读取同一时点的市场、板块与龙头证据…",
      "decision-status decision-status-loading",
    );
    loading.setAttribute("role", "status");
    root.append(loading);
    return;
  }

  const grid = element("div", undefined, "security-evidence-grid");
  const market = panel("市场状态", "01 / REGIME");
  const sector = panel("板块上下文", "02 / SECTOR");
  const leader = panel("个股龙头证据", "03 / LEADER");
  const fund = panel("资金证据边界", "04 / FUND FLOW");
  renderMarket(market, state.market);
  renderSector(sector, state.context, state.sectors);
  renderLeader(leader, state.symbol, state.leaders);
  renderFundFlow(fund);
  grid.append(market, sector, leader, fund);
  root.append(grid);
}
