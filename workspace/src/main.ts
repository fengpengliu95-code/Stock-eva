import { ApiError, fetchSecurityAnalysis, NoTradingDatesError } from "./api";
import { createStockCockpitChart } from "./charts/stock-cockpit";
import { ApiError as DecisionApiError } from "./decision-api";
import { DecisionRequestManager, type DecisionQuery } from "./decision-state";
import {
  bindSecurityEntrypoints,
  decisionHash,
  parseDecisionHash,
  parseSecurityHash,
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
  type DecisionFlowState,
} from "./views/decision-flow";
import {
  renderSecurityAnalysis,
  type ChartCleanup,
} from "./views/security-analysis";

declare global {
  interface Window {
    stockEvaActivateView?: (identifier: string, updateHash?: boolean) => void;
  }
}

const DEFAULT_TAXONOMY = "baostock.industry_classification";

let cockpitState: CockpitState = initialCockpitState;
let decisionState: DecisionFlowState | null = null;
let abortController: AbortController | null = null;
let decisionManager: DecisionRequestManager | null = null;
let chartCleanup: ChartCleanup | null = null;
let restoredHash: string | null = null;
let disposeBindings: (() => void) | null = null;
let waitingForDomReady = false;

function cockpitRoot(): HTMLElement {
  const value = document.querySelector<HTMLElement>("#security-analysis");
  if (!value) throw new Error("security cockpit section is missing");
  return value;
}

function decisionRoot(): HTMLElement | null {
  return document.querySelector<HTMLElement>("#decision-flow");
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
  if (root && decisionState) renderDecisionFlow(root, decisionState);
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

function replaceDecisionHash(
  query: DecisionQuery,
  sectorId: string | null,
): void {
  const hash = decisionHash({ ...query, sectorId });
  restoredHash = hash;
  window.history.replaceState(null, "", hash);
}

async function loadSelectedLeaders(
  query: DecisionQuery,
  sectorId: string,
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
      decisionState.selectedSectorId !== sectorId
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
      decisionState.selectedSectorId !== sectorId
    ) {
      return;
    }
    decisionState = {
      ...decisionState,
      leaders: { phase: "error", ...decisionError(error) },
    };
  }
  renderDecision();
}

async function loadDecisionOverview(
  query: DecisionQuery,
  preferredSectorId: string | null,
): Promise<void> {
  const root = decisionRoot();
  if (!root || !decisionManager) return;
  abortController?.abort();
  abortController = null;
  chartCleanup?.();
  chartCleanup = null;
  window.stockEvaActivateView?.("overview", false);
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
    replaceDecisionHash(query, selectedSectorId);
    renderDecision();
    if (selectedSectorId) {
      await loadSelectedLeaders(query, selectedSectorId);
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
  decisionManager?.dispose();
  window.stockEvaActivateView?.("security", false);
  cockpitState = transitionCockpit(cockpitState, {
    type: "load",
    symbol,
    sourceView,
    ...(decisionContext ? { decisionContext } : {}),
  });
  renderCockpit();
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
  const hash = securityHash(symbol, sourceView, decisionContext);
  restoredHash = hash;
  window.history.pushState(null, "", hash);
  void openSecurity(symbol, sourceView, decisionContext);
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
  if (hash === "" || hash === "#overview") {
    void loadDecisionOverview(defaultDecisionQuery(), null);
    return;
  }
  abortController?.abort();
  abortController = null;
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
  const hash = decisionHash({ ...query, sectorId });
  restoredHash = hash;
  window.history.pushState(null, "", hash);
  void loadSelectedLeaders(query, sectorId);
}

function onDecisionBack(event: Event): void {
  if (!(event.target instanceof Element)) return;
  const back = event.target.closest<HTMLAnchorElement>(
    "#security-back[data-decision-return]",
  );
  if (!back) return;
  event.preventDefault();
  event.stopImmediatePropagation();
  const route = parseDecisionHash(back.hash);
  if (!route) return;
  restoredHash = back.hash;
  window.history.pushState(null, "", back.hash);
  void loadDecisionOverview(routeQuery(route), route.sectorId);
}

export function initializeSecurityCockpit(): void {
  if (disposeBindings) return;
  cockpitState = initialCockpitState;
  decisionState = null;
  restoredHash = null;
  decisionManager = new DecisionRequestManager(fetch);
  const unbindEntrypoints = bindSecurityEntrypoints(document, navigate);
  decisionRoot()?.addEventListener("click", onDecisionClick);
  document.addEventListener("click", onDecisionBack, true);
  window.addEventListener("popstate", restoreFromHash);
  window.addEventListener("hashchange", restoreFromHash);
  const cleanup = (): void => {
    if (disposeBindings !== cleanup) return;
    unbindEntrypoints();
    decisionRoot()?.removeEventListener("click", onDecisionClick);
    document.removeEventListener("click", onDecisionBack, true);
    window.removeEventListener("popstate", restoreFromHash);
    window.removeEventListener("hashchange", restoreFromHash);
    abortController?.abort();
    abortController = null;
    decisionManager?.dispose();
    decisionManager = null;
    chartCleanup?.();
    chartCleanup = null;
    cockpitState = initialCockpitState;
    decisionState = null;
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
