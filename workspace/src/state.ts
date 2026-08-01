import type { SecurityAnalysisResponse } from "./api";

export type SourceView = "portfolio" | "watchlists" | "sectors";
export type EmptyReason = "no_market_data" | "no_effective_trading_data";

export interface DecisionContext {
  asOf: string;
  taxonomyId: string;
  sectorId: string;
}

interface CockpitContext {
  symbol: string;
  sourceView: SourceView;
  decisionContext?: DecisionContext;
}

export type CockpitState =
  | { phase: "idle" }
  | (CockpitContext & { phase: "loading" })
  | (CockpitContext & {
      phase: "ready";
      response: SecurityAnalysisResponse;
    })
  | (CockpitContext & {
      phase: "empty";
      reason: EmptyReason;
      response?: SecurityAnalysisResponse;
    })
  | (CockpitContext & { phase: "quality-error"; message: string })
  | (CockpitContext & { phase: "connection-error"; message: string });

export type CockpitAction =
  | ({ type: "load" } & CockpitContext)
  | { type: "success"; response: SecurityAnalysisResponse }
  | { type: "empty"; reason: EmptyReason }
  | { type: "failure"; status: number | null; message: string };

export const initialCockpitState: CockpitState = { phase: "idle" };

function context(state: CockpitState): CockpitContext {
  if (state.phase === "idle") {
    throw new Error("cockpit response received before load");
  }
  return {
    symbol: state.symbol,
    sourceView: state.sourceView,
    ...(state.decisionContext
      ? { decisionContext: state.decisionContext }
      : {}),
  };
}

export function transitionCockpit(
  state: CockpitState,
  action: CockpitAction,
): CockpitState {
  if (action.type === "load") {
    return {
      phase: "loading",
      symbol: action.symbol,
      sourceView: action.sourceView,
      ...(action.decisionContext
        ? { decisionContext: action.decisionContext }
        : {}),
    };
  }
  const current = context(state);
  if (action.type === "success") {
    if (action.response.status === "empty") {
      const reason = action.response.quality_issues.includes(
        "no_effective_trading_data",
      )
        ? "no_effective_trading_data"
        : "no_market_data";
      return {
        ...current,
        phase: "empty",
        reason,
        response: action.response,
      };
    }
    return { ...current, phase: "ready", response: action.response };
  }
  if (action.type === "empty") {
    return { ...current, phase: "empty", reason: action.reason };
  }
  return action.status === 409
    ? { ...current, phase: "quality-error", message: action.message }
    : { ...current, phase: "connection-error", message: action.message };
}
