import type {
  MarketRegimeResponse,
  SectorLeadersResponse,
  SectorRotationResponse,
} from "../decision-api";

const AS_OF = "2026-01-31";
const TAXONOMY = "baostock.industry_classification";

const marketScope: MarketRegimeResponse["actual_market_scope"] = {
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
  conclusion_disclaimer: "Only observed main-board prices.",
};

const sectorScope: SectorRotationResponse["actual_scope"] = {
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
  conclusion_disclaimer: "Only main-board classified prices.",
};

const missingFlow = {
  status: "missing" as const,
  evidence_tier: null,
  reason:
    "R2 fund-flow evidence is not integrated; turnover is price-volume evidence only",
};

const confidence = {
  value: 0.42,
  level: "low" as const,
  reasons: ["market_scope_coverage_not_audited"],
};

const metric = {
  metric: "relative_strength_20d",
  raw_value: 0.12,
  unit: "decimal_relative_return",
  score: 24,
  weight: 0.15,
  weighted_score: 3.6,
  formula_version: "sector-relative-strength-20d-v1",
  quality_status: "degraded" as const,
  missing_inputs: [],
  quality_issues: ["metric_coverage_low"],
  effective_count: 18,
  target_count: 20,
  coverage_ratio: 0.9,
};

// R1-C commit 5b69992, derived from MetricScore.model_dump(mode="json").
// Leader metrics do not carry sector member coverage, so Pydantic emits the
// optional count/ratio keys explicitly as JSON null rather than omitting them.
const leaderMetric = {
  ...metric,
  metric: "tradability",
  raw_value: 1,
  unit: "boolean",
  score: 100,
  weight: 0.2,
  weighted_score: 20,
  formula_version: "classification-and-canonical-current-tradeability-v1",
  quality_status: "ready" as const,
  quality_issues: [],
  effective_count: null,
  target_count: null,
  coverage_ratio: null,
};

export function marketFixture(
  overrides: Partial<MarketRegimeResponse> = {},
): MarketRegimeResponse {
  return {
    result_id: "regime-fixture",
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
        formula_version: "trend-price-ma-v1",
        quality_status: "ready",
        missing_inputs: [],
        quality_issues: [],
        supporting_evidence: [],
        contrary_evidence: [
          {
            component: "trend",
            code: "trend_down",
            detail: "Observed indexes are below moving averages.",
            value: -55,
            as_of: AS_OF,
            source: "baostock",
          },
        ],
        source_lineage: [],
      },
      {
        name: "leadership",
        score: null,
        weight: 0.1,
        weighted_score: null,
        formula_version: "leadership-sector-v1",
        quality_status: "missing",
        missing_inputs: ["leadership.sector_persistence"],
        quality_issues: [],
        supporting_evidence: [],
        contrary_evidence: [],
        source_lineage: [],
      },
    ],
    weights: { trend: 0.3, leadership: 0.1 },
    thresholds: { strategic_bear_max: -25 },
    confidence,
    missing_inputs: ["leadership.sector_persistence"],
    supporting_evidence: [
      {
        component: "liquidity",
        code: "liquidity_support",
        detail: "Observed turnover is stable.",
        value: 12,
        as_of: AS_OF,
        source: "baostock",
      },
    ],
    contrary_evidence: [
      {
        component: "trend",
        code: "trend_down",
        detail: "Observed indexes are below moving averages.",
        value: -55,
        as_of: AS_OF,
        source: "baostock",
      },
    ],
    quality_issues: ["narrow_scope_not_full_a_share"],
    actual_market_scope: marketScope,
    source_lineage: [],
    ...overrides,
  };
}

export function sectorsFixture(
  overrides: Partial<SectorRotationResponse> = {},
): SectorRotationResponse {
  return {
    result_id: "rotation-fixture",
    formula_version: "sector-rotation-v1",
    status: "degraded",
    quality_status: "degraded",
    as_of: AS_OF,
    data_as_of: AS_OF,
    taxonomy_id: TAXONOMY,
    classification_lineage: {
      generation_id: "classification-fixture",
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
    actual_scope: sectorScope,
    rankings: [
      {
        rank: 2,
        ranking_id: "ranking-b",
        sector_id: "sector-b",
        sector_name: "后端第二行",
        member_count: 20,
        priced_member_count: 18,
        ranking_eligible: true,
        ranking_exclusion_reasons: [],
        total_score: 48,
        confidence,
        metric_scores: [
          metric,
          { ...metric, metric: "leader_count", raw_value: 3, unit: "qualified_research_leaders" },
          { ...metric, metric: "leader_diffusion", raw_value: 0.15, unit: "qualified_candidate_ratio" },
          { ...metric, metric: "leader_persistence_days", raw_value: 5, unit: "mean_signed_sessions" },
        ],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "degraded",
        missing_inputs: [],
        quality_issues: ["member_history_coverage_low"],
      },
      {
        rank: 1,
        ranking_id: "ranking-a",
        sector_id: "sector-a",
        sector_name: "后端第一名",
        member_count: 30,
        priced_member_count: 30,
        ranking_eligible: true,
        ranking_exclusion_reasons: [],
        total_score: 60,
        confidence: { ...confidence, value: 0.6, level: "medium" },
        metric_scores: [leaderMetric],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "ready",
        missing_inputs: [],
        quality_issues: [],
      },
    ],
    fund_flow_evidence: missingFlow,
    missing_inputs: ["fund_flow.r2_evidence_not_integrated"],
    quality_issues: ["narrow_main_board_scope_not_full_a_share"],
    ...overrides,
  };
}

export function leadersFixture(
  overrides: Partial<SectorLeadersResponse> = {},
): SectorLeadersResponse {
  const sectors = sectorsFixture();
  return {
    result_id: "leaders-fixture",
    formula_version: "leader-ranking-v1",
    status: "degraded",
    quality_status: "degraded",
    as_of: AS_OF,
    data_as_of: AS_OF,
    taxonomy_id: TAXONOMY,
    sector_id: "sector-b",
    sector_name: "后端第二行",
    classification_lineage: sectors.classification_lineage,
    market_lineage: sectors.market_lineage,
    actual_scope: sectorScope,
    candidates: [
      {
        rank: 2,
        candidate_id: "candidate-b",
        symbol: "sz.000002",
        name: "候选乙",
        total_score: 52,
        confidence,
        actionable_primary: false,
        actionability_status: "risk_inputs_unavailable",
        limit_lock_status: "unavailable",
        leader_qualified: true,
        qualification_version: "leader-qualification-v1",
        qualification_reasons: ["relative_strength_qualified"],
        disqualification_reasons: [],
        metric_scores: [leaderMetric],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "degraded",
        missing_inputs: ["leader.limit_lock_status"],
        quality_issues: ["leader.limit_lock_risk_input_unavailable"],
      },
      {
        rank: 1,
        candidate_id: "candidate-a",
        symbol: "sh.600001",
        name: "候选甲",
        total_score: 61,
        confidence,
        actionable_primary: false,
        actionability_status: "risk_inputs_unavailable",
        limit_lock_status: "unavailable",
        leader_qualified: false,
        qualification_version: "leader-qualification-v1",
        qualification_reasons: [],
        disqualification_reasons: ["trend_quality_below_threshold"],
        metric_scores: [metric],
        supporting_evidence: [],
        contrary_evidence: [],
        quality_status: "degraded",
        missing_inputs: ["leader.limit_lock_status"],
        quality_issues: ["leader.limit_lock_risk_input_unavailable"],
      },
    ],
    exclusions: [
      {
        symbol: "sh.600009",
        reasons: ["suspended", "bad_price_quality"],
      },
    ],
    fund_flow_evidence: missingFlow,
    missing_inputs: ["fund_flow.r2_evidence_not_integrated"],
    quality_issues: ["narrow_main_board_scope_not_full_a_share"],
    ...overrides,
  };
}

export const decisionQuery = {
  asOf: AS_OF,
  taxonomyId: TAXONOMY,
};
