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
    cockpitState = transitionCockpit(cockpitState, {
      type: "success",
      response,
    });
  } catch (error) {
    if (controller.signal.aborted) return;
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
  window.history.pushState(null, "", hash);
  void openSecurity(symbol, sourceView);
}

function restoreFromHash(): void {
  const route = parseSecurityHash(window.location.hash);
  if (route) void openSecurity(route.symbol, route.sourceView);
  else {
    abortController?.abort();
    chartCleanup?.();
    chartCleanup = null;
  }
}

function initialize(): void {
  bindSecurityEntrypoints(document, navigate);
  window.addEventListener("popstate", restoreFromHash);
  window.addEventListener("hashchange", restoreFromHash);
  restoreFromHash();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initialize, { once: true });
} else {
  initialize();
}
