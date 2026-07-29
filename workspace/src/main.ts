import { ApiError, fetchSecurityAnalysis, NoTradingDatesError } from "./api";
import { createStockCockpitChart } from "./charts/stock-cockpit";
import {
  bindSecurityEntrypoints,
  parseSecurityHash,
  securityHash,
} from "./navigation";
import {
  initialCockpitState,
  transitionCockpit,
  type CockpitState,
  type SourceView,
} from "./state";
import {
  renderSecurityAnalysis,
  type ChartCleanup,
} from "./views/security-analysis";

declare global {
  interface Window {
    stockEvaActivateView?: (identifier: string, updateHash?: boolean) => void;
  }
}

let cockpitState: CockpitState = initialCockpitState;
let abortController: AbortController | null = null;
let chartCleanup: ChartCleanup | null = null;
let restoredHash: string | null = null;
let disposeBindings: (() => void) | null = null;
let waitingForDomReady = false;

function root(): HTMLElement {
  const value = document.querySelector<HTMLElement>("#security-analysis");
  if (!value) throw new Error("security cockpit section is missing");
  return value;
}

function render(): void {
  chartCleanup?.();
  chartCleanup = renderSecurityAnalysis(
    root(),
    cockpitState,
    createStockCockpitChart,
  );
}

async function openSecurity(
  symbol: string,
  sourceView: SourceView,
): Promise<void> {
  abortController?.abort();
  const controller = new AbortController();
  abortController = controller;
  window.stockEvaActivateView?.("security", false);
  cockpitState = transitionCockpit(cockpitState, {
    type: "load",
    symbol,
    sourceView,
  });
  render();
  try {
    const response = await fetchSecurityAnalysis(
      symbol,
      fetch,
      controller.signal,
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
  render();
}

function navigate(symbol: string, sourceView: SourceView): void {
  const hash = securityHash(symbol, sourceView);
  restoredHash = hash;
  window.history.pushState(null, "", hash);
  void openSecurity(symbol, sourceView);
}

function restoreFromHash(): void {
  const hash = window.location.hash;
  if (hash === restoredHash) return;
  restoredHash = hash;
  const route = parseSecurityHash(hash);
  if (route) void openSecurity(route.symbol, route.sourceView);
  else {
    abortController?.abort();
    abortController = null;
    chartCleanup?.();
    chartCleanup = null;
  }
}

export function initializeSecurityCockpit(): void {
  if (disposeBindings) return;
  cockpitState = initialCockpitState;
  restoredHash = null;
  const unbindEntrypoints = bindSecurityEntrypoints(document, navigate);
  window.addEventListener("popstate", restoreFromHash);
  window.addEventListener("hashchange", restoreFromHash);
  const cleanup = (): void => {
    if (disposeBindings !== cleanup) return;
    unbindEntrypoints();
    window.removeEventListener("popstate", restoreFromHash);
    window.removeEventListener("hashchange", restoreFromHash);
    abortController?.abort();
    abortController = null;
    chartCleanup?.();
    chartCleanup = null;
    cockpitState = initialCockpitState;
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
