import { ApiError, fetchSecurityAnalysis, NoTradingDatesError } from "./api";
import { createStockCockpitChart } from "./charts/stock-cockpit";
import {
  ApiError as DecisionApiError,
  fetchMarketRegime,
  fetchSectorLeaders,
  fetchSectorRotation,
} from "./decision-api";
import { DecisionRequestManager, type DecisionQuery } from "./decision-state";
import {
  bindSecurityEntrypoints,
  decisionHash,
  parseDecisionHash,
  parseSectorHash,
  parseSecurityHash,
  sectorHash,
  securityHash,
  type DecisionRoute,
} from "./navigation";
import {
  initialCockpitState,
  transitionCockpit,
  type CockpitState,
  type DecisionContext,
  type SourceView,
} from "./state";
import {
  renderDecisionFlow,
  renderSectorWorkspace,
  type DecisionFlowState,
} from "./views/decision-flow";
import {
  renderSecurityAnalysis,
  type ChartCleanup,
} from "./views/security-analysis";
import {
  renderSecurityEvidence,
  type SecurityEvidenceSlice,
  type SecurityEvidenceState,
} from "./views/security-evidence";

declare global {
  interface Window {
    stockEvaActivateView?: (identifier: string, updateHash?: boolean) => void;
  }
}

const DEFAULT_TAXONOMY = "baostock.industry_classification";
const ROUTE_CHANGE_EVENT = "stock-eva-route-change";

let cockpitState: CockpitState = initialCockpitState;
let decisionState: DecisionFlowState | null = null;
let securityEvidenceState: SecurityEvidenceState = { phase: "idle" };
let abortController: AbortController | null = null;
let evidenceController: AbortController | null = null;
let decisionManager: DecisionRequestManager | null = null;
let decisionView: "overview" | "sectors" = "overview";
let chartCleanup: ChartCleanup | null = null;
let restoredHash: string | null = null;
let disposeBindings: (() => void) | null = null;
let waitingForDomReady = false;

function cockpitRoot(): HTMLElement {
  const value = document.querySelector<HTMLElement>("#security-analysis");
  if (!value) throw new Error("security cockpit section is missing");
  return value;
}

function decisionRoot(view = decisionView): HTMLElement | null {
  return document.querySelector<HTMLElement>(
    view === "sectors" ? "#sector-decision-flow" : "#decision-flow",
  );
}

function securityEvidenceRoot(): HTMLElement | null {
  return cockpitRoot().querySelector<HTMLElement>("#security-evidence");
}

function renderCockpit(): void {
  chartCleanup?.();
  chartCleanup = renderSecurityAnalysis(
    cockpitRoot(),
    cockpitState,
    createStockCockpitChart,
  );
}

function renderDecision(): void {
  const root = decisionRoot();
  if (!root || !decisionState) return;
  if (decisionView === "sectors") renderSectorWorkspace(root, decisionState);
  else renderDecisionFlow(root, decisionState);
}

function renderEvidence(): void {
  const root = securityEvidenceRoot();
  if (root) renderSecurityEvidence(root, securityEvidenceState);
}

function focusDecisionSector(
  sectorId: string,
  view = decisionView,
): void {
  const control = Array.from(
    decisionRoot(view)?.querySelectorAll<HTMLElement>("[data-decision-sector]") ?? [],
  ).find((item) => item.dataset.decisionSector === sectorId);
  control?.focus();
}

function currentShanghaiDate(): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function defaultDecisionQuery(): DecisionQuery {
  return { asOf: currentShanghaiDate(), taxonomyId: DEFAULT_TAXONOMY };
}

function routeQuery(route: DecisionRoute): DecisionQuery {
  return { asOf: route.asOf, taxonomyId: route.taxonomyId };
}

function decisionError(error: unknown): { status: number | null; message: string } {
  return {
    status: error instanceof DecisionApiError ? error.status : null,
    message:
      error instanceof DecisionApiError
        ? error.detail
        : error instanceof Error
          ? error.message
          : "未知连接错误",
  };
}

function evidenceSlice<T>(
  result: PromiseSettledResult<T>,
): SecurityEvidenceSlice<T> {
  if (result.status === "fulfilled") {
    return { phase: "ready", response: result.value };
  }
  return { phase: "error", ...decisionError(result.reason) };
}

function reusableSecurityEvidence(
  symbol: string,
  context: DecisionContext,
): Extract<SecurityEvidenceState, { phase: "ready" }> | null {
  if (
    decisionState?.phase !== "ready" ||
    decisionState.query.asOf !== context.asOf ||
    decisionState.query.taxonomyId !== context.taxonomyId ||
    decisionState.selectedSectorId !== context.sectorId ||
    decisionState.leaders.phase !== "ready" ||
    decisionState.leaders.response.sector_id !== context.sectorId
  ) {
    return null;
  }
  return {
    phase: "ready",
    symbol,
    context,
    market: { phase: "ready", response: decisionState.overview.market },
    sectors: { phase: "ready", response: decisionState.overview.sectors },
    leaders: { phase: "ready", response: decisionState.leaders.response },
  };
}

async function loadSecurityEvidence(
  symbol: string,
  context: DecisionContext,
  reused: Extract<SecurityEvidenceState, { phase: "ready" }> | null,
): Promise<void> {
  evidenceController?.abort();
  evidenceController = null;
  if (reused) {
    securityEvidenceState = reused;
    renderEvidence();
    return;
  }
  const controller = new AbortController();
  evidenceController = controller;
  securityEvidenceState = { phase: "loading", symbol, context };
  renderEvidence();
  const results = await Promise.allSettled([
    fetchMarketRegime(context.asOf, fetch, controller.signal),
    fetchSectorRotation(
      context.asOf,
      context.taxonomyId,
      fetch,
      controller.signal,
    ),
    fetchSectorLeaders(
      context.asOf,
      context.taxonomyId,
      context.sectorId,
      fetch,
      controller.signal,
    ),
  ] as const);
  if (evidenceController !== controller || controller.signal.aborted) return;
  securityEvidenceState = {
    phase: "ready",
    symbol,
    context,
    market: evidenceSlice(results[0]),
    sectors: evidenceSlice(results[1]),
    leaders: evidenceSlice(results[2]),
  };
  evidenceController = null;
  renderEvidence();
}

function replaceDecisionHash(
  query: DecisionQuery,
  sectorId: string | null,
  view: "overview" | "sectors",
): void {
  const hash = view === "sectors"
    ? sectorHash({ ...query, sectorId })
    : decisionHash({ ...query, sectorId });
  restoredHash = hash;
  window.history.replaceState(null, "", hash);
}

async function loadSelectedLeaders(
  query: DecisionQuery,
  sectorId: string,
  view: "overview" | "sectors",
  preserveFocus = false,
): Promise<void> {
  if (!decisionManager || decisionState?.phase !== "ready") return;
  const overview = decisionState.overview;
  decisionState = {
    phase: "ready",
    query,
    overview,
    selectedSectorId: sectorId,
    leaders: { phase: "loading" },
  };
  renderDecision();
  if (preserveFocus) focusDecisionSector(sectorId, view);
  try {
    const response = await decisionManager.loadLeaders({
      ...query,
      sectorId,
    });
    if (
      response === null ||
      decisionState?.phase !== "ready" ||
      decisionState.query.asOf !== query.asOf ||
      decisionState.query.taxonomyId !== query.taxonomyId ||
      decisionState.selectedSectorId !== sectorId ||
      decisionView !== view
    ) {
      return;
    }
    decisionState = {
      ...decisionState,
      leaders: { phase: "ready", response },
    };
  } catch (error) {
    if (
      decisionState?.phase !== "ready" ||
      decisionState.selectedSectorId !== sectorId ||
      decisionView !== view
    ) {
      return;
    }
    decisionState = {
      ...decisionState,
      leaders: { phase: "error", ...decisionError(error) },
    };
  }
  renderDecision();
  if (preserveFocus) focusDecisionSector(sectorId, view);
}

async function loadDecisionOverview(
  query: DecisionQuery,
  preferredSectorId: string | null,
  view: "overview" | "sectors" = "overview",
): Promise<void> {
  const root = decisionRoot(view);
  if (!root || !decisionManager) return;
  decisionView = view;
  abortController?.abort();
  abortController = null;
  evidenceController?.abort();
  evidenceController = null;
  securityEvidenceState = { phase: "idle" };
  renderEvidence();
  chartCleanup?.();
  chartCleanup = null;
  window.stockEvaActivateView?.(view, false);
  decisionState = { phase: "loading", query };
  renderDecision();
  try {
    const overview = await decisionManager.loadOverview(query);
    if (overview === null) return;
    const preferredExists = overview.sectors.rankings.some(
      (item) => item.sector_id === preferredSectorId,
    );
    const selectedSectorId = preferredExists
      ? preferredSectorId
      : (overview.sectors.rankings[0]?.sector_id ?? null);
    decisionState = {
      phase: "ready",
      query,
      overview,
      selectedSectorId,
      leaders: { phase: "idle" },
    };
    replaceDecisionHash(query, selectedSectorId, view);
    renderDecision();
    if (selectedSectorId) {
      await loadSelectedLeaders(query, selectedSectorId, view);
    }
  } catch (error) {
    decisionState = {
      phase: "error",
      query,
      ...decisionError(error),
    };
    renderDecision();
  }
}

async function openSecurity(
  symbol: string,
  sourceView: SourceView,
  decisionContext?: DecisionContext,
): Promise<void> {
  abortController?.abort();
  const controller = new AbortController();
  abortController = controller;
  const reusedEvidence = decisionContext
    ? reusableSecurityEvidence(symbol, decisionContext)
    : null;
  decisionManager?.dispose();
  window.stockEvaActivateView?.("security", false);
  cockpitState = transitionCockpit(cockpitState, {
    type: "load",
    symbol,
    sourceView,
    ...(decisionContext ? { decisionContext } : {}),
  });
  renderCockpit();
  if (decisionContext) {
    void loadSecurityEvidence(symbol, decisionContext, reusedEvidence);
  } else {
    evidenceController?.abort();
    evidenceController = null;
    securityEvidenceState = { phase: "idle" };
    renderEvidence();
  }
  try {
    const response = await fetchSecurityAnalysis(
      symbol,
      fetch,
      controller.signal,
      decisionContext?.asOf,
    );
    if (abortController !== controller || controller.signal.aborted) return;
    cockpitState = transitionCockpit(cockpitState, {
      type: "success",
      response,
    });
  } catch (error) {
    if (abortController !== controller || controller.signal.aborted) return;
    if (error instanceof NoTradingDatesError) {
      cockpitState = transitionCockpit(cockpitState, {
        type: "empty",
        reason: "no_market_data",
      });
    } else {
      cockpitState = transitionCockpit(cockpitState, {
        type: "failure",
        status: error instanceof ApiError ? error.status : null,
        message:
          error instanceof ApiError
            ? error.detail
            : error instanceof Error
              ? error.message
              : "未知连接错误",
      });
    }
  }
  renderCockpit();
}

function navigate(
  symbol: string,
  sourceView: SourceView,
  decisionContext?: DecisionContext,
): void {
  const context =
    decisionContext && sourceView === "sectors" && decisionView === "sectors"
      ? { ...decisionContext, returnView: "sectors" as const }
      : decisionContext;
  const hash = securityHash(symbol, sourceView, context);
  restoredHash = hash;
  window.history.pushState(null, "", hash);
  void openSecurity(symbol, sourceView, context);
}

function restoreFromHash(): void {
  const hash = window.location.hash;
  if (hash === restoredHash) return;
  restoredHash = hash;
  const securityRoute = parseSecurityHash(hash);
  if (securityRoute) {
    const context =
      securityRoute.asOf && securityRoute.taxonomyId && securityRoute.sectorId
        ? {
            asOf: securityRoute.asOf,
            taxonomyId: securityRoute.taxonomyId,
            sectorId: securityRoute.sectorId,
            ...(securityRoute.returnView
              ? { returnView: securityRoute.returnView }
              : {}),
          }
        : undefined;
    void openSecurity(
      securityRoute.symbol,
      securityRoute.sourceView,
      context,
    );
    return;
  }
  const decisionRoute = parseDecisionHash(hash);
  if (decisionRoute) {
    void loadDecisionOverview(routeQuery(decisionRoute), decisionRoute.sectorId);
    return;
  }
  const sectorRoute = parseSectorHash(hash);
  if (sectorRoute) {
    void loadDecisionOverview(
      routeQuery(sectorRoute),
      sectorRoute.sectorId,
      "sectors",
    );
    return;
  }
  if (hash === "" || hash === "#overview") {
    void loadDecisionOverview(defaultDecisionQuery(), null);
    return;
  }
  if (hash === "#sectors" || hash.startsWith("#sectors?")) {
    void loadDecisionOverview(defaultDecisionQuery(), null, "sectors");
    return;
  }
  decisionManager?.dispose();
  decisionState = null;
  abortController?.abort();
  abortController = null;
  evidenceController?.abort();
  evidenceController = null;
  securityEvidenceState = { phase: "idle" };
  renderEvidence();
  chartCleanup?.();
  chartCleanup = null;
}

function onDecisionClick(event: Event): void {
  if (!(event.target instanceof Element)) return;
  const control = event.target.closest<HTMLElement>("[data-decision-sector]");
  const sectorId = control?.dataset.decisionSector;
  if (!sectorId || decisionState?.phase !== "ready") return;
  event.preventDefault();
  const query = decisionState.query;
  const hash = decisionView === "sectors"
    ? sectorHash({ ...query, sectorId })
    : decisionHash({ ...query, sectorId });
  restoredHash = hash;
  window.history.pushState(null, "", hash);
  void loadSelectedLeaders(query, sectorId, decisionView, true);
}

function onDecisionBack(event: Event): void {
  if (!(event.target instanceof Element)) return;
  const back = event.target.closest<HTMLAnchorElement>(
    "#security-back[data-decision-return]",
  );
  if (!back) return;
  event.preventDefault();
  event.stopImmediatePropagation();
  const overviewRoute = parseDecisionHash(back.hash);
  const sectorRoute = parseSectorHash(back.hash);
  const route = overviewRoute ?? sectorRoute;
  if (!route) return;
  restoredHash = back.hash;
  window.history.pushState(null, "", back.hash);
  void loadDecisionOverview(
    routeQuery(route),
    route.sectorId,
    sectorRoute ? "sectors" : "overview",
  );
}

export function initializeSecurityCockpit(): void {
  if (disposeBindings) return;
  cockpitState = initialCockpitState;
  decisionState = null;
  securityEvidenceState = { phase: "idle" };
  restoredHash = null;
  decisionManager = new DecisionRequestManager(fetch);
  const unbindEntrypoints = bindSecurityEntrypoints(document, navigate);
  decisionRoot("overview")?.addEventListener("click", onDecisionClick);
  decisionRoot("sectors")?.addEventListener("click", onDecisionClick);
  document.addEventListener("click", onDecisionBack, true);
  window.addEventListener("popstate", restoreFromHash);
  window.addEventListener("hashchange", restoreFromHash);
  window.addEventListener(ROUTE_CHANGE_EVENT, restoreFromHash);
  const cleanup = (): void => {
    if (disposeBindings !== cleanup) return;
    unbindEntrypoints();
    decisionRoot("overview")?.removeEventListener("click", onDecisionClick);
    decisionRoot("sectors")?.removeEventListener("click", onDecisionClick);
    document.removeEventListener("click", onDecisionBack, true);
    window.removeEventListener("popstate", restoreFromHash);
    window.removeEventListener("hashchange", restoreFromHash);
    window.removeEventListener(ROUTE_CHANGE_EVENT, restoreFromHash);
    abortController?.abort();
    abortController = null;
    evidenceController?.abort();
    evidenceController = null;
    decisionManager?.dispose();
    decisionManager = null;
    chartCleanup?.();
    chartCleanup = null;
    cockpitState = initialCockpitState;
    decisionState = null;
    decisionView = "overview";
    securityEvidenceState = { phase: "idle" };
    restoredHash = null;
    disposeBindings = null;
  };
  disposeBindings = cleanup;
  restoreFromHash();
}

function onDocumentReady(): void {
  waitingForDomReady = false;
  initializeSecurityCockpit();
}

export function disposeSecurityCockpit(): void {
  if (waitingForDomReady) {
    document.removeEventListener("DOMContentLoaded", onDocumentReady);
    waitingForDomReady = false;
  }
  disposeBindings?.();
}

if (document.readyState === "loading") {
  waitingForDomReady = true;
  document.addEventListener("DOMContentLoaded", onDocumentReady, { once: true });
} else {
  initializeSecurityCockpit();
}
