import { describe, expect, it, vi } from "vitest";

import { DecisionRequestManager } from "./decision-state";

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function emptyMarket(asOf: string) {
  return {
    result_id: `regime-${asOf}`,
    formula_version: "market-regime-v1",
    as_of: asOf,
    data_as_of: null,
    status: "empty",
    quality_status: "empty",
    strategic_state: "range",
    tactical_state: "neutral",
    total_score: null,
    component_scores: [],
    weights: {},
    thresholds: {},
    confidence: { value: 0, level: "low", reasons: [] },
    missing_inputs: ["market.price_history"],
    supporting_evidence: [],
    contrary_evidence: [],
    quality_issues: ["no_market_data_at_or_before_as_of"],
    actual_market_scope: {
      expected_boards: [],
      observed_boards: [],
      missing_boards: [],
      expected_index_series: [],
      observed_index_series: [],
      missing_index_series: [],
      coverage_basis: "symbol_presence_only",
      coverage_evidence_status: "unavailable",
      observed_universe_count: 0,
      index_coverage_ratio: 0,
      scope_status: "narrow_provisional",
      can_support_full_a_share_conclusion: false,
      conclusion_disclaimer: "No full-market conclusion.",
    },
    source_lineage: [],
  };
}

function emptySectors(asOf: string) {
  return {
    result_id: `rotation-${asOf}`,
    formula_version: "sector-rotation-v1",
    status: "empty",
    quality_status: "empty",
    as_of: asOf,
    data_as_of: null,
    taxonomy_id: "baostock.industry_classification",
    classification_lineage: null,
    market_lineage: null,
    actual_scope: {
      scope_status: "narrow_main_board",
      included_markets: [],
      included_boards: ["main"],
      excluded_classification_boards: [],
      classification_eligible_symbols: 0,
      observed_market_symbols: 0,
      priced_classified_symbols: 0,
      coverage_basis: "promoted_classification_and_observed_canonical_prices",
      can_support_full_a_share_conclusion: false,
      can_support_all_industry_conclusion: false,
      conclusion_disclaimer: "No full-market conclusion.",
    },
    rankings: [],
    fund_flow_evidence: {
      status: "missing",
      evidence_tier: null,
      reason:
        "R2 fund-flow evidence is not integrated; turnover is price-volume evidence only",
    },
    missing_inputs: ["classification.promoted_generation"],
    quality_issues: ["no_promoted_classification_generation"],
  };
}

describe("decision request ownership", () => {
  it("aborts A and returns only B when old overview responses resolve last", async () => {
    const aMarket = deferred<Response>();
    const aSector = deferred<Response>();
    const fetcher = vi.fn<typeof fetch>((input, init) => {
      const url = new URL(String(input));
      const asOf = url.searchParams.get("as_of")!;
      if (asOf === "2026-01-30") {
        return url.pathname.endsWith("market-regime")
          ? aMarket.promise
          : aSector.promise;
      }
      return Promise.resolve(
        jsonResponse(
          url.pathname.endsWith("market-regime")
            ? emptyMarket(asOf)
            : emptySectors(asOf),
        ),
      );
    });
    const manager = new DecisionRequestManager(fetcher);

    const requestA = manager.loadOverview({
      asOf: "2026-01-30",
      taxonomyId: "baostock.industry_classification",
    });
    const requestB = manager.loadOverview({
      asOf: "2026-01-31",
      taxonomyId: "baostock.industry_classification",
    });
    const resultB = await requestB;
    aMarket.resolve(jsonResponse(emptyMarket("2026-01-30")));
    aSector.resolve(jsonResponse(emptySectors("2026-01-30")));
    const resultA = await requestA;

    expect(resultB?.market.as_of).toBe("2026-01-31");
    expect(resultA).toBeNull();
    expect(fetcher.mock.calls.slice(0, 2)).toSatisfy((calls: unknown[][]) =>
      calls.every((call) => {
        const init = call[1] as RequestInit | undefined;
        return (init?.signal as AbortSignal).aborted;
      }),
    );
  });

  it("aborts stale leader requests independently when sector changes", async () => {
    const old = deferred<Response>();
    const fetcher = vi.fn<typeof fetch>((input) => {
      const url = new URL(String(input));
      if (url.pathname.includes("sector-old")) return old.promise;
      return Promise.resolve(
        jsonResponse({
          ...emptySectors("2026-01-31"),
          result_id: "leaders-new",
          sector_id: "sector-new",
          sector_name: "新板块",
          candidates: [],
          exclusions: [],
        }),
      );
    });
    const manager = new DecisionRequestManager(fetcher);
    const context = {
      asOf: "2026-01-31",
      taxonomyId: "baostock.industry_classification",
    };

    const stale = manager.loadLeaders({ ...context, sectorId: "sector-old" });
    const current = manager.loadLeaders({ ...context, sectorId: "sector-new" });
    old.resolve(
      jsonResponse({
        ...emptySectors("2026-01-31"),
        result_id: "leaders-old",
        sector_id: "sector-old",
        sector_name: "旧板块",
        candidates: [],
        exclusions: [],
      }),
    );

    expect((await current)?.sector_id).toBe("sector-new");
    expect(await stale).toBeNull();
  });
});
