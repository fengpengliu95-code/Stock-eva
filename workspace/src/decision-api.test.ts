import { describe, expect, it, vi } from "vitest";

import {
  fetchMarketRegime,
  fetchSectorLeaders,
  fetchSectorRotation,
  parseMarketRegime,
  parseSectorLeaders,
  parseSectorRotation,
} from "./decision-api";

const AS_OF = "2026-01-31";
const TAXONOMY = "baostock.industry_classification";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function scope() {
  return {
    expected_boards: ["sse_main", "szse_main", "chinext", "star"],
    observed_boards: ["sse_main", "szse_main"],
    missing_boards: ["chinext", "star"],
    expected_index_series: ["sh.000001", "sz.399001"],
    observed_index_series: ["sh.000001"],
    missing_index_series: ["sz.399001"],
    coverage_basis: "symbol_presence_only",
    coverage_evidence_status: "unavailable",
    observed_universe_count: 1800,
    index_coverage_ratio: 0.5,
    scope_status: "narrow_provisional",
    can_support_full_a_share_conclusion: false,
    conclusion_disclaimer: "Main-board price sample only.",
  };
}

function evidence(code: string) {
  return {
    component: "trend",
    code,
    detail: code,
    value: 12.5,
    as_of: AS_OF,
    source: "baostock",
  };
}

function marketPayload(overrides: Record<string, unknown> = {}) {
  return {
    result_id: "regime-1",
    formula_version: "market-regime-v1",
    as_of: AS_OF,
    data_as_of: AS_OF,
    status: "degraded",
    quality_status: "degraded",
    strategic_state: "bear",
    tactical_state: "risk_off",
    total_score: -40,
    component_scores: [
      {
        name: "trend",
        score: -55,
        weight: 0.3,
        weighted_score: -16.5,
        formula_version: "trend-v1",
        quality_status: "ready",
        missing_inputs: [],
        quality_issues: [],
        supporting_evidence: [],
        contrary_evidence: [evidence("trend_down")],
        source_lineage: [],
      },
    ],
    weights: { trend: 0.3 },
    thresholds: { strategic_bear_max: -25 },
    confidence: {
      value: 0.35,
      level: "low",
      reasons: ["market_scope_coverage_not_audited"],
    },
    missing_inputs: ["leadership.sector_persistence"],
    supporting_evidence: [evidence("liquidity_support")],
    contrary_evidence: [evidence("trend_down")],
    quality_issues: ["narrow_scope_not_full_a_share"],
    actual_market_scope: scope(),
    source_lineage: [],
    ...overrides,
  };
}

function sectorScope() {
  return {
    scope_status: "narrow_main_board",
    included_markets: ["SSE", "SZSE"],
    included_boards: ["main"],
    excluded_classification_boards: ["chinext", "star"],
    classification_eligible_symbols: 2100,
    observed_market_symbols: 1800,
    priced_classified_symbols: 1700,
    coverage_basis: "promoted_classification_and_observed_canonical_prices",
    can_support_full_a_share_conclusion: false,
    can_support_all_industry_conclusion: false,
    conclusion_disclaimer: "Main-board only.",
  };
}

function metric(metricName: string, rawValue = 0.12) {
  return {
    metric: metricName,
    raw_value: rawValue,
    unit: "ratio",
    score: 24,
    weight: 0.1,
    weighted_score: 2.4,
    formula_version: `${metricName}-v1`,
    quality_status: "ready",
    missing_inputs: [],
    quality_issues: [],
    effective_count: 18,
    target_count: 20,
    coverage_ratio: 0.9,
  };
}

function sectorPayload(overrides: Record<string, unknown> = {}) {
  return {
    result_id: "rotation-1",
    formula_version: "sector-rotation-v1",
    status: "degraded",
    quality_status: "degraded",
    as_of: AS_OF,
    data_as_of: AS_OF,
    taxonomy_id: TAXONOMY,
    classification_lineage: {
      generation_id: "classification-1",
      schema_version: "classification-v3",
      source: "baostock",
      source_version: "0.9.3",
      source_snapshot_date: "2026-01-30",
      source_date_semantics: "requested_unverified",
      taxonomy_id: TAXONOMY,
      coverage_ratio: 0.97,
    },
    market_lineage: {
      source: "baostock",
      source_version: "canonical-market-v2",
      earliest_input_date: "2025-10-01",
      latest_input_date: AS_OF,
      record_count: 120000,
      content_hash: "a".repeat(64),
    },
    actual_scope: sectorScope(),
    rankings: [
      {
        rank: 2,
        ranking_id: "sector-second",
        sector_id: "sector-b",
        sector_name: "板块乙",
        member_count: 20,
        priced_member_count: 18,
        ranking_eligible: true,
        ranking_exclusion_reasons: [],
        total_score: 48,
        confidence: { value: 0.55, level: "medium", reasons: [] },
        metric_scores: [
          metric("relative_strength_20d"),
          metric("leader_count", 3),
          metric("leader_diffusion", 0.15),
          metric("leader_persistence_days", 5),
        ],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "degraded",
        missing_inputs: [],
        quality_issues: ["member_history_coverage_low"],
      },
      {
        rank: 1,
        ranking_id: "sector-first",
        sector_id: "sector-a",
        sector_name: "板块甲",
        member_count: 30,
        priced_member_count: 30,
        ranking_eligible: true,
        ranking_exclusion_reasons: [],
        total_score: 60,
        confidence: { value: 0.7, level: "medium", reasons: [] },
        metric_scores: [metric("relative_strength_20d")],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "ready",
        missing_inputs: [],
        quality_issues: [],
      },
    ],
    fund_flow_evidence: {
      status: "missing",
      evidence_tier: null,
      reason:
        "R2 fund-flow evidence is not integrated; turnover is price-volume evidence only",
    },
    missing_inputs: ["fund_flow.r2_evidence_not_integrated"],
    quality_issues: ["narrow_main_board_scope_not_full_a_share"],
    ...overrides,
  };
}

function leadersPayload(overrides: Record<string, unknown> = {}) {
  return {
    result_id: "leaders-1",
    formula_version: "leader-ranking-v1",
    status: "degraded",
    quality_status: "degraded",
    as_of: AS_OF,
    data_as_of: AS_OF,
    taxonomy_id: TAXONOMY,
    sector_id: "sector-a",
    sector_name: "板块甲",
    classification_lineage: sectorPayload().classification_lineage,
    market_lineage: sectorPayload().market_lineage,
    actual_scope: sectorScope(),
    candidates: [
      {
        rank: 2,
        candidate_id: "candidate-second",
        symbol: "sz.000002",
        name: "候选乙",
        total_score: 52,
        confidence: { value: 0.4, level: "low", reasons: [] },
        actionable_primary: false,
        actionability_status: "risk_inputs_unavailable",
        limit_lock_status: "unavailable",
        leader_qualified: true,
        qualification_version: "leader-qualification-v1",
        qualification_reasons: ["relative_strength_qualified"],
        disqualification_reasons: [],
        metric_scores: [metric("relative_strength_20d")],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "degraded",
        missing_inputs: ["leader.limit_lock_status"],
        quality_issues: ["leader.limit_lock_risk_input_unavailable"],
      },
      {
        rank: 1,
        candidate_id: "candidate-first",
        symbol: "sh.600001",
        name: "候选甲",
        total_score: 61,
        confidence: { value: 0.45, level: "low", reasons: [] },
        actionable_primary: false,
        actionability_status: "risk_inputs_unavailable",
        limit_lock_status: "unavailable",
        leader_qualified: false,
        qualification_version: "leader-qualification-v1",
        qualification_reasons: [],
        disqualification_reasons: ["trend_quality_below_threshold"],
        metric_scores: [metric("relative_strength_20d")],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "degraded",
        missing_inputs: ["leader.limit_lock_status"],
        quality_issues: ["leader.limit_lock_risk_input_unavailable"],
      },
    ],
    exclusions: [
      { symbol: "sh.600009", reasons: ["suspended", "bad_price_quality"] },
    ],
    fund_flow_evidence: sectorPayload().fund_flow_evidence,
    missing_inputs: ["fund_flow.r2_evidence_not_integrated"],
    quality_issues: ["narrow_main_board_scope_not_full_a_share"],
    ...overrides,
  };
}

describe("decision API contracts", () => {
  it("parses degraded market evidence and preserves component order", () => {
    const parsed = parseMarketRegime(marketPayload());

    expect(parsed.strategic_state).toBe("bear");
    expect(parsed.tactical_state).toBe("risk_off");
    expect(parsed.component_scores.map((item) => item.name)).toEqual(["trend"]);
    expect(parsed.contrary_evidence[0].code).toBe("trend_down");
    expect(parsed.actual_market_scope.can_support_full_a_share_conclusion).toBe(
      false,
    );
  });

  it("preserves backend sector and leader order and accepts optional leadership fields", () => {
    const rotation = parseSectorRotation(sectorPayload());
    const leaders = parseSectorLeaders(leadersPayload());

    expect(rotation.rankings.map((item) => item.rank)).toEqual([2, 1]);
    expect(rotation.rankings[0].ranking_eligible).toBe(true);
    expect(rotation.rankings[0].metric_scores[0]).toEqual(
      expect.objectContaining({ effective_count: 18, target_count: 20 }),
    );
    expect(
      rotation.rankings[0].metric_scores.map((item) => item.metric),
    ).toEqual([
      "relative_strength_20d",
      "leader_count",
      "leader_diffusion",
      "leader_persistence_days",
    ]);
    expect(leaders.candidates.map((item) => item.rank)).toEqual([2, 1]);
    expect(leaders.candidates.every((item) => !item.actionable_primary)).toBe(
      true,
    );
    expect(leaders.candidates[0]).toEqual(
      expect.objectContaining({
        leader_qualified: true,
        qualification_version: "leader-qualification-v1",
      }),
    );
  });

  it("parses truthful empty responses without inventing rankings", () => {
    const emptyMarket = parseMarketRegime(
      marketPayload({
        status: "empty",
        quality_status: "empty",
        data_as_of: null,
        total_score: null,
        component_scores: [],
      }),
    );
    const emptySectors = parseSectorRotation(
      sectorPayload({
        status: "empty",
        quality_status: "empty",
        data_as_of: null,
        classification_lineage: null,
        market_lineage: null,
        rankings: [],
      }),
    );

    expect(emptyMarket.data_as_of).toBeNull();
    expect(emptyMarket.total_score).toBeNull();
    expect(emptySectors.rankings).toEqual([]);
  });

  it("uses encoded as_of/taxonomy/sector queries and forwards one abort signal", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(jsonResponse(marketPayload()))
      .mockResolvedValueOnce(jsonResponse(sectorPayload()))
      .mockResolvedValueOnce(jsonResponse(leadersPayload()));
    const signal = new AbortController().signal;

    await fetchMarketRegime(AS_OF, fetcher, signal);
    await fetchSectorRotation(AS_OF, TAXONOMY, fetcher, signal);
    await fetchSectorLeaders(
      AS_OF,
      TAXONOMY,
      "sector/a b",
      fetcher,
      signal,
    );

    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      `http://127.0.0.1:8000/api/v1/analysis/market-regime?as_of=${AS_OF}`,
      { signal },
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      `http://127.0.0.1:8000/api/v1/analysis/sector-rotation?as_of=${AS_OF}&taxonomy_id=baostock.industry_classification`,
      { signal },
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      3,
      `http://127.0.0.1:8000/api/v1/analysis/sectors/sector%2Fa%20b/leaders?as_of=${AS_OF}&taxonomy_id=baostock.industry_classification`,
      { signal },
    );
  });

  it.each([
    [422, { detail: { code: "future_as_of" } }, "future_as_of"],
    [
      503,
      { detail: { code: "market_storage_unavailable" } },
      "market_storage_unavailable",
    ],
  ])(
    "keeps HTTP %s structured and controllable",
    async (status, payload, code) => {
      const fetcher = vi
        .fn<typeof fetch>()
        .mockResolvedValue(jsonResponse(payload, status));

      await expect(
        fetchMarketRegime(AS_OF, fetcher, new AbortController().signal),
      ).rejects.toEqual(
        expect.objectContaining({
          name: "ApiError",
          status,
          detail: code,
        }),
      );
    },
  );
});
