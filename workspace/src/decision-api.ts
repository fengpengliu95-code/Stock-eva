const API_BASE = "http://127.0.0.1:8000/api/v1";
const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const SYMBOL_PATTERN = /^(sh|sz)\.[0-9]{6}$/;

export type AnalysisStatus = "empty" | "ready" | "degraded";
export type QualityStatus = "ready" | "degraded" | "missing";

export interface EvidenceItem {
  component?: string;
  code: string;
  detail: string;
  metric?: string;
  value?: number | null;
  raw_value?: number | null;
  score?: number | null;
  as_of?: string;
  source?: string;
}

export interface SourceLineage {
  source: string;
  source_version: string;
  earliest_input_date: string;
  latest_input_date: string;
  record_count: number;
  content_hash: string;
}

export interface Confidence {
  value: number;
  level: "low" | "medium" | "high";
  reasons: string[];
}

export interface MarketComponentScore {
  name: string;
  score: number | null;
  weight: number;
  weighted_score: number | null;
  formula_version: string;
  quality_status: QualityStatus;
  missing_inputs: string[];
  quality_issues: string[];
  supporting_evidence: EvidenceItem[];
  contrary_evidence: EvidenceItem[];
  source_lineage: SourceLineage[];
}

export interface ActualMarketScope {
  expected_boards: string[];
  observed_boards: string[];
  missing_boards: string[];
  expected_index_series: string[];
  observed_index_series: string[];
  missing_index_series: string[];
  coverage_basis: string;
  coverage_evidence_status: string;
  observed_universe_count: number;
  index_coverage_ratio: number;
  scope_status: string;
  can_support_full_a_share_conclusion: boolean;
  conclusion_disclaimer: string;
}

export interface MarketRegimeResponse {
  result_id: string;
  formula_version: string;
  as_of: string;
  data_as_of: string | null;
  status: AnalysisStatus;
  quality_status: AnalysisStatus;
  strategic_state: "bull" | "range" | "bear";
  tactical_state: "risk_on" | "neutral" | "risk_off";
  total_score: number | null;
  component_scores: MarketComponentScore[];
  weights: Record<string, number>;
  thresholds: Record<string, number>;
  confidence: Confidence;
  missing_inputs: string[];
  supporting_evidence: EvidenceItem[];
  contrary_evidence: EvidenceItem[];
  quality_issues: string[];
  actual_market_scope: ActualMarketScope;
  source_lineage: SourceLineage[];
}

export interface MetricScore {
  metric: string;
  raw_value: number | null;
  unit: string;
  score: number | null;
  weight: number;
  weighted_score: number | null;
  formula_version: string;
  quality_status: QualityStatus;
  missing_inputs: string[];
  quality_issues: string[];
  observed_count?: number;
  eligible_count?: number;
  coverage_ratio?: number;
}

export interface ClassificationLineage {
  generation_id: string;
  schema_version: string;
  source: string;
  source_version: string;
  source_snapshot_date: string;
  source_date_semantics?: string;
  taxonomy_id: string;
  coverage_ratio: number;
}

export interface ActualSectorScope {
  scope_status: string;
  included_markets: string[];
  included_boards: string[];
  excluded_classification_boards: string[];
  classification_eligible_symbols: number;
  observed_market_symbols: number;
  priced_classified_symbols: number;
  coverage_basis: string;
  can_support_full_a_share_conclusion: boolean;
  can_support_all_industry_conclusion: boolean;
  conclusion_disclaimer: string;
}

export interface MissingFundFlowEvidence {
  status: "missing";
  evidence_tier: null;
  reason: string;
}

export interface SectorRanking {
  rank: number;
  ranking_id: string;
  sector_id: string;
  sector_name: string;
  member_count: number;
  priced_member_count: number;
  total_score: number | null;
  confidence: Confidence;
  metric_scores: MetricScore[];
  supporting_evidence: EvidenceItem[];
  contrary_evidence: EvidenceItem[];
  quality_status: QualityStatus;
  missing_inputs: string[];
  quality_issues: string[];
  leader_count?: number;
  leader_diffusion?: number;
  persistence_days?: number;
}

export interface SectorRotationResponse {
  result_id: string;
  formula_version: string;
  status: AnalysisStatus;
  quality_status: AnalysisStatus;
  as_of: string;
  data_as_of: string | null;
  taxonomy_id: string;
  classification_lineage: ClassificationLineage | null;
  market_lineage: SourceLineage | null;
  actual_scope: ActualSectorScope;
  rankings: SectorRanking[];
  fund_flow_evidence: MissingFundFlowEvidence;
  missing_inputs: string[];
  quality_issues: string[];
}

export interface LeaderCandidate {
  rank: number;
  candidate_id: string;
  symbol: string;
  name: string;
  total_score: number | null;
  confidence: Confidence;
  actionable_primary: false;
  actionability_status: string;
  limit_lock_status: string;
  metric_scores: MetricScore[];
  supporting_evidence: EvidenceItem[];
  contrary_evidence: EvidenceItem[];
  quality_status: QualityStatus;
  missing_inputs: string[];
  quality_issues: string[];
}

export interface LeaderExclusion {
  symbol: string;
  reasons: string[];
}

export interface SectorLeadersResponse {
  result_id: string;
  formula_version: string;
  status: AnalysisStatus;
  quality_status: AnalysisStatus;
  as_of: string;
  data_as_of: string | null;
  taxonomy_id: string;
  sector_id: string;
  sector_name: string | null;
  classification_lineage: ClassificationLineage | null;
  market_lineage: SourceLineage | null;
  actual_scope: ActualSectorScope;
  candidates: LeaderCandidate[];
  exclusions: LeaderExclusion[];
  fund_flow_evidence: MissingFundFlowEvidence;
  missing_inputs: string[];
  quality_issues: string[];
}

export class ApiError extends Error {
  readonly status: number;
  readonly detail: string;

  constructor(status: number, detail: string) {
    super(detail);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function stringValue(
  value: unknown,
  label: string,
  allowed?: readonly string[],
): string {
  if (typeof value !== "string" || (allowed && !allowed.includes(value))) {
    throw new TypeError(`${label} must be a valid string`);
  }
  return value;
}

function isoDate(value: unknown, label: string): string {
  const result = stringValue(value, label);
  if (!ISO_DATE_PATTERN.test(result)) {
    throw new TypeError(`${label} must be an ISO date`);
  }
  return result;
}

function nullableIsoDate(value: unknown, label: string): string | null {
  return value === null ? null : isoDate(value, label);
}

function finiteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new TypeError(`${label} must be a finite number`);
  }
  return value;
}

function nullableNumber(value: unknown, label: string): number | null {
  return value === null ? null : finiteNumber(value, label);
}

function integer(value: unknown, label: string): number {
  const result = finiteNumber(value, label);
  if (!Number.isInteger(result) || result < 0) {
    throw new TypeError(`${label} must be a non-negative integer`);
  }
  return result;
}

function booleanValue(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") throw new TypeError(`${label} must be boolean`);
  return value;
}

function strings(value: unknown, label: string): string[] {
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    throw new TypeError(`${label} must be a string array`);
  }
  return [...value];
}

function records<T>(
  value: unknown,
  label: string,
  parse: (item: unknown, index: number) => T,
): T[] {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  return value.map(parse);
}

function numberMap(value: unknown, label: string): Record<string, number> {
  const source = record(value, label);
  return Object.fromEntries(
    Object.entries(source).map(([key, item]) => [
      key,
      finiteNumber(item, `${label}.${key}`),
    ]),
  );
}

function analysisStatus(value: unknown, label: string): AnalysisStatus {
  return stringValue(value, label, [
    "empty",
    "ready",
    "degraded",
  ]) as AnalysisStatus;
}

function qualityStatus(value: unknown, label: string): QualityStatus {
  return stringValue(value, label, [
    "ready",
    "degraded",
    "missing",
  ]) as QualityStatus;
}

function confidence(value: unknown, label: string): Confidence {
  const source = record(value, label);
  return {
    value: finiteNumber(source.value, `${label}.value`),
    level: stringValue(source.level, `${label}.level`, [
      "low",
      "medium",
      "high",
    ]) as Confidence["level"],
    reasons: strings(source.reasons, `${label}.reasons`),
  };
}

function evidence(value: unknown, label: string): EvidenceItem {
  const source = record(value, label);
  const result: EvidenceItem = {
    code: stringValue(source.code, `${label}.code`),
    detail: stringValue(source.detail, `${label}.detail`),
  };
  for (const key of ["component", "metric", "source"] as const) {
    if (source[key] !== undefined) {
      result[key] = stringValue(source[key], `${label}.${key}`);
    }
  }
  for (const key of ["value", "raw_value", "score"] as const) {
    if (source[key] !== undefined) {
      result[key] = nullableNumber(source[key], `${label}.${key}`);
    }
  }
  if (source.as_of !== undefined) {
    result.as_of = isoDate(source.as_of, `${label}.as_of`);
  }
  return result;
}

function evidenceList(value: unknown, label: string): EvidenceItem[] {
  return records(value, label, (item, index) =>
    evidence(item, `${label}[${index}]`),
  );
}

function lineage(value: unknown, label: string): SourceLineage {
  const source = record(value, label);
  return {
    source: stringValue(source.source, `${label}.source`),
    source_version: stringValue(
      source.source_version,
      `${label}.source_version`,
    ),
    earliest_input_date: isoDate(
      source.earliest_input_date,
      `${label}.earliest_input_date`,
    ),
    latest_input_date: isoDate(
      source.latest_input_date,
      `${label}.latest_input_date`,
    ),
    record_count: integer(source.record_count, `${label}.record_count`),
    content_hash: stringValue(source.content_hash, `${label}.content_hash`),
  };
}

function lineageList(value: unknown, label: string): SourceLineage[] {
  return records(value, label, (item, index) =>
    lineage(item, `${label}[${index}]`),
  );
}

function component(value: unknown, label: string): MarketComponentScore {
  const source = record(value, label);
  return {
    name: stringValue(source.name, `${label}.name`),
    score: nullableNumber(source.score, `${label}.score`),
    weight: finiteNumber(source.weight, `${label}.weight`),
    weighted_score: nullableNumber(
      source.weighted_score,
      `${label}.weighted_score`,
    ),
    formula_version: stringValue(
      source.formula_version,
      `${label}.formula_version`,
    ),
    quality_status: qualityStatus(
      source.quality_status,
      `${label}.quality_status`,
    ),
    missing_inputs: strings(source.missing_inputs, `${label}.missing_inputs`),
    quality_issues: strings(source.quality_issues, `${label}.quality_issues`),
    supporting_evidence: evidenceList(
      source.supporting_evidence,
      `${label}.supporting_evidence`,
    ),
    contrary_evidence: evidenceList(
      source.contrary_evidence,
      `${label}.contrary_evidence`,
    ),
    source_lineage: lineageList(
      source.source_lineage,
      `${label}.source_lineage`,
    ),
  };
}

function marketScope(value: unknown, label: string): ActualMarketScope {
  const source = record(value, label);
  return {
    expected_boards: strings(source.expected_boards, `${label}.expected_boards`),
    observed_boards: strings(source.observed_boards, `${label}.observed_boards`),
    missing_boards: strings(source.missing_boards, `${label}.missing_boards`),
    expected_index_series: strings(
      source.expected_index_series,
      `${label}.expected_index_series`,
    ),
    observed_index_series: strings(
      source.observed_index_series,
      `${label}.observed_index_series`,
    ),
    missing_index_series: strings(
      source.missing_index_series,
      `${label}.missing_index_series`,
    ),
    coverage_basis: stringValue(
      source.coverage_basis,
      `${label}.coverage_basis`,
    ),
    coverage_evidence_status: stringValue(
      source.coverage_evidence_status,
      `${label}.coverage_evidence_status`,
    ),
    observed_universe_count: integer(
      source.observed_universe_count,
      `${label}.observed_universe_count`,
    ),
    index_coverage_ratio: finiteNumber(
      source.index_coverage_ratio,
      `${label}.index_coverage_ratio`,
    ),
    scope_status: stringValue(source.scope_status, `${label}.scope_status`),
    can_support_full_a_share_conclusion: booleanValue(
      source.can_support_full_a_share_conclusion,
      `${label}.can_support_full_a_share_conclusion`,
    ),
    conclusion_disclaimer: stringValue(
      source.conclusion_disclaimer,
      `${label}.conclusion_disclaimer`,
    ),
  };
}

export function parseMarketRegime(value: unknown): MarketRegimeResponse {
  const source = record(value, "market regime");
  return {
    result_id: stringValue(source.result_id, "result_id"),
    formula_version: stringValue(source.formula_version, "formula_version"),
    as_of: isoDate(source.as_of, "as_of"),
    data_as_of: nullableIsoDate(source.data_as_of, "data_as_of"),
    status: analysisStatus(source.status, "status"),
    quality_status: analysisStatus(source.quality_status, "quality_status"),
    strategic_state: stringValue(source.strategic_state, "strategic_state", [
      "bull",
      "range",
      "bear",
    ]) as MarketRegimeResponse["strategic_state"],
    tactical_state: stringValue(source.tactical_state, "tactical_state", [
      "risk_on",
      "neutral",
      "risk_off",
    ]) as MarketRegimeResponse["tactical_state"],
    total_score: nullableNumber(source.total_score, "total_score"),
    component_scores: records(
      source.component_scores,
      "component_scores",
      (item, index) => component(item, `component_scores[${index}]`),
    ),
    weights: numberMap(source.weights, "weights"),
    thresholds: numberMap(source.thresholds, "thresholds"),
    confidence: confidence(source.confidence, "confidence"),
    missing_inputs: strings(source.missing_inputs, "missing_inputs"),
    supporting_evidence: evidenceList(
      source.supporting_evidence,
      "supporting_evidence",
    ),
    contrary_evidence: evidenceList(
      source.contrary_evidence,
      "contrary_evidence",
    ),
    quality_issues: strings(source.quality_issues, "quality_issues"),
    actual_market_scope: marketScope(
      source.actual_market_scope,
      "actual_market_scope",
    ),
    source_lineage: lineageList(source.source_lineage, "source_lineage"),
  };
}

function metric(value: unknown, label: string): MetricScore {
  const source = record(value, label);
  const result: MetricScore = {
    metric: stringValue(source.metric, `${label}.metric`),
    raw_value: nullableNumber(source.raw_value, `${label}.raw_value`),
    unit: stringValue(source.unit, `${label}.unit`),
    score: nullableNumber(source.score, `${label}.score`),
    weight: finiteNumber(source.weight, `${label}.weight`),
    weighted_score: nullableNumber(
      source.weighted_score,
      `${label}.weighted_score`,
    ),
    formula_version: stringValue(
      source.formula_version,
      `${label}.formula_version`,
    ),
    quality_status: qualityStatus(
      source.quality_status,
      `${label}.quality_status`,
    ),
    missing_inputs: strings(source.missing_inputs, `${label}.missing_inputs`),
    quality_issues: strings(source.quality_issues, `${label}.quality_issues`),
  };
  for (const key of ["observed_count", "eligible_count"] as const) {
    if (source[key] !== undefined) {
      result[key] = integer(source[key], `${label}.${key}`);
    }
  }
  if (source.coverage_ratio !== undefined) {
    result.coverage_ratio = finiteNumber(
      source.coverage_ratio,
      `${label}.coverage_ratio`,
    );
  }
  return result;
}

function metricList(value: unknown, label: string): MetricScore[] {
  return records(value, label, (item, index) =>
    metric(item, `${label}[${index}]`),
  );
}

function classificationLineage(
  value: unknown,
  label: string,
): ClassificationLineage {
  const source = record(value, label);
  const result: ClassificationLineage = {
    generation_id: stringValue(source.generation_id, `${label}.generation_id`),
    schema_version: stringValue(source.schema_version, `${label}.schema_version`),
    source: stringValue(source.source, `${label}.source`),
    source_version: stringValue(
      source.source_version,
      `${label}.source_version`,
    ),
    source_snapshot_date: isoDate(
      source.source_snapshot_date,
      `${label}.source_snapshot_date`,
    ),
    taxonomy_id: stringValue(source.taxonomy_id, `${label}.taxonomy_id`),
    coverage_ratio: finiteNumber(
      source.coverage_ratio,
      `${label}.coverage_ratio`,
    ),
  };
  if (source.source_date_semantics !== undefined) {
    result.source_date_semantics = stringValue(
      source.source_date_semantics,
      `${label}.source_date_semantics`,
    );
  }
  return result;
}

function sectorScope(value: unknown, label: string): ActualSectorScope {
  const source = record(value, label);
  return {
    scope_status: stringValue(source.scope_status, `${label}.scope_status`),
    included_markets: strings(
      source.included_markets,
      `${label}.included_markets`,
    ),
    included_boards: strings(
      source.included_boards,
      `${label}.included_boards`,
    ),
    excluded_classification_boards: strings(
      source.excluded_classification_boards,
      `${label}.excluded_classification_boards`,
    ),
    classification_eligible_symbols: integer(
      source.classification_eligible_symbols,
      `${label}.classification_eligible_symbols`,
    ),
    observed_market_symbols: integer(
      source.observed_market_symbols,
      `${label}.observed_market_symbols`,
    ),
    priced_classified_symbols: integer(
      source.priced_classified_symbols,
      `${label}.priced_classified_symbols`,
    ),
    coverage_basis: stringValue(
      source.coverage_basis,
      `${label}.coverage_basis`,
    ),
    can_support_full_a_share_conclusion: booleanValue(
      source.can_support_full_a_share_conclusion,
      `${label}.can_support_full_a_share_conclusion`,
    ),
    can_support_all_industry_conclusion: booleanValue(
      source.can_support_all_industry_conclusion,
      `${label}.can_support_all_industry_conclusion`,
    ),
    conclusion_disclaimer: stringValue(
      source.conclusion_disclaimer,
      `${label}.conclusion_disclaimer`,
    ),
  };
}

function fundFlow(value: unknown, label: string): MissingFundFlowEvidence {
  const source = record(value, label);
  if (source.evidence_tier !== null) {
    throw new TypeError(`${label}.evidence_tier must be null`);
  }
  return {
    status: stringValue(source.status, `${label}.status`, [
      "missing",
    ]) as "missing",
    evidence_tier: null,
    reason: stringValue(source.reason, `${label}.reason`),
  };
}

function sectorRanking(value: unknown, label: string): SectorRanking {
  const source = record(value, label);
  const result: SectorRanking = {
    rank: integer(source.rank, `${label}.rank`),
    ranking_id: stringValue(source.ranking_id, `${label}.ranking_id`),
    sector_id: stringValue(source.sector_id, `${label}.sector_id`),
    sector_name: stringValue(source.sector_name, `${label}.sector_name`),
    member_count: integer(source.member_count, `${label}.member_count`),
    priced_member_count: integer(
      source.priced_member_count,
      `${label}.priced_member_count`,
    ),
    total_score: nullableNumber(source.total_score, `${label}.total_score`),
    confidence: confidence(source.confidence, `${label}.confidence`),
    metric_scores: metricList(source.metric_scores, `${label}.metric_scores`),
    supporting_evidence: evidenceList(
      source.supporting_evidence,
      `${label}.supporting_evidence`,
    ),
    contrary_evidence: evidenceList(
      source.contrary_evidence,
      `${label}.contrary_evidence`,
    ),
    quality_status: qualityStatus(
      source.quality_status,
      `${label}.quality_status`,
    ),
    missing_inputs: strings(source.missing_inputs, `${label}.missing_inputs`),
    quality_issues: strings(source.quality_issues, `${label}.quality_issues`),
  };
  for (const key of ["leader_count", "persistence_days"] as const) {
    if (source[key] !== undefined) {
      result[key] = integer(source[key], `${label}.${key}`);
    }
  }
  if (source.leader_diffusion !== undefined) {
    result.leader_diffusion = finiteNumber(
      source.leader_diffusion,
      `${label}.leader_diffusion`,
    );
  }
  return result;
}

export function parseSectorRotation(value: unknown): SectorRotationResponse {
  const source = record(value, "sector rotation");
  return {
    result_id: stringValue(source.result_id, "result_id"),
    formula_version: stringValue(source.formula_version, "formula_version"),
    status: analysisStatus(source.status, "status"),
    quality_status: analysisStatus(source.quality_status, "quality_status"),
    as_of: isoDate(source.as_of, "as_of"),
    data_as_of: nullableIsoDate(source.data_as_of, "data_as_of"),
    taxonomy_id: stringValue(source.taxonomy_id, "taxonomy_id"),
    classification_lineage:
      source.classification_lineage === null
        ? null
        : classificationLineage(
            source.classification_lineage,
            "classification_lineage",
          ),
    market_lineage:
      source.market_lineage === null
        ? null
        : lineage(source.market_lineage, "market_lineage"),
    actual_scope: sectorScope(source.actual_scope, "actual_scope"),
    rankings: records(source.rankings, "rankings", (item, index) =>
      sectorRanking(item, `rankings[${index}]`),
    ),
    fund_flow_evidence: fundFlow(
      source.fund_flow_evidence,
      "fund_flow_evidence",
    ),
    missing_inputs: strings(source.missing_inputs, "missing_inputs"),
    quality_issues: strings(source.quality_issues, "quality_issues"),
  };
}

function leaderCandidate(value: unknown, label: string): LeaderCandidate {
  const source = record(value, label);
  if (source.actionable_primary !== false) {
    throw new TypeError(`${label}.actionable_primary must be false`);
  }
  const symbol = stringValue(source.symbol, `${label}.symbol`);
  if (!SYMBOL_PATTERN.test(symbol)) {
    throw new TypeError(`${label}.symbol must be an A-share symbol`);
  }
  return {
    rank: integer(source.rank, `${label}.rank`),
    candidate_id: stringValue(source.candidate_id, `${label}.candidate_id`),
    symbol,
    name: stringValue(source.name, `${label}.name`),
    total_score: nullableNumber(source.total_score, `${label}.total_score`),
    confidence: confidence(source.confidence, `${label}.confidence`),
    actionable_primary: false,
    actionability_status: stringValue(
      source.actionability_status,
      `${label}.actionability_status`,
    ),
    limit_lock_status: stringValue(
      source.limit_lock_status,
      `${label}.limit_lock_status`,
    ),
    metric_scores: metricList(source.metric_scores, `${label}.metric_scores`),
    supporting_evidence: evidenceList(
      source.supporting_evidence,
      `${label}.supporting_evidence`,
    ),
    contrary_evidence: evidenceList(
      source.contrary_evidence,
      `${label}.contrary_evidence`,
    ),
    quality_status: qualityStatus(
      source.quality_status,
      `${label}.quality_status`,
    ),
    missing_inputs: strings(source.missing_inputs, `${label}.missing_inputs`),
    quality_issues: strings(source.quality_issues, `${label}.quality_issues`),
  };
}

function exclusion(value: unknown, label: string): LeaderExclusion {
  const source = record(value, label);
  return {
    symbol: stringValue(source.symbol, `${label}.symbol`),
    reasons: strings(source.reasons, `${label}.reasons`),
  };
}

export function parseSectorLeaders(value: unknown): SectorLeadersResponse {
  const source = record(value, "sector leaders");
  return {
    result_id: stringValue(source.result_id, "result_id"),
    formula_version: stringValue(source.formula_version, "formula_version"),
    status: analysisStatus(source.status, "status"),
    quality_status: analysisStatus(source.quality_status, "quality_status"),
    as_of: isoDate(source.as_of, "as_of"),
    data_as_of: nullableIsoDate(source.data_as_of, "data_as_of"),
    taxonomy_id: stringValue(source.taxonomy_id, "taxonomy_id"),
    sector_id: stringValue(source.sector_id, "sector_id"),
    sector_name:
      source.sector_name === null
        ? null
        : stringValue(source.sector_name, "sector_name"),
    classification_lineage:
      source.classification_lineage === null
        ? null
        : classificationLineage(
            source.classification_lineage,
            "classification_lineage",
          ),
    market_lineage:
      source.market_lineage === null
        ? null
        : lineage(source.market_lineage, "market_lineage"),
    actual_scope: sectorScope(source.actual_scope, "actual_scope"),
    candidates: records(source.candidates, "candidates", (item, index) =>
      leaderCandidate(item, `candidates[${index}]`),
    ),
    exclusions: records(source.exclusions, "exclusions", (item, index) =>
      exclusion(item, `exclusions[${index}]`),
    ),
    fund_flow_evidence: fundFlow(
      source.fund_flow_evidence,
      "fund_flow_evidence",
    ),
    missing_inputs: strings(source.missing_inputs, "missing_inputs"),
    quality_issues: strings(source.quality_issues, "quality_issues"),
  };
}

async function errorDetail(response: Response): Promise<string> {
  try {
    const payload = record(await response.json(), "error response");
    if (typeof payload.detail === "string") return payload.detail;
    if (payload.detail !== undefined) {
      const detail = record(payload.detail, "error detail");
      if (typeof detail.code === "string") return detail.code;
    }
  } catch {
    // The HTTP status remains the authoritative fallback.
  }
  return `HTTP ${response.status}`;
}

async function fetchJson(
  path: string,
  fetcher: typeof fetch,
  signal: AbortSignal,
): Promise<unknown> {
  const response = await fetcher(`${API_BASE}${path}`, { signal });
  if (!response.ok) {
    throw new ApiError(response.status, await errorDetail(response));
  }
  return response.json();
}

function query(asOf: string, taxonomyId?: string): string {
  const parameters = new URLSearchParams({ as_of: isoDate(asOf, "as_of") });
  if (taxonomyId !== undefined) {
    parameters.set("taxonomy_id", taxonomyId);
  }
  return parameters.toString();
}

export async function fetchMarketRegime(
  asOf: string,
  fetcher: typeof fetch = fetch,
  signal: AbortSignal = new AbortController().signal,
): Promise<MarketRegimeResponse> {
  return parseMarketRegime(
    await fetchJson(`/analysis/market-regime?${query(asOf)}`, fetcher, signal),
  );
}

export async function fetchSectorRotation(
  asOf: string,
  taxonomyId: string,
  fetcher: typeof fetch = fetch,
  signal: AbortSignal = new AbortController().signal,
): Promise<SectorRotationResponse> {
  return parseSectorRotation(
    await fetchJson(
      `/analysis/sector-rotation?${query(asOf, taxonomyId)}`,
      fetcher,
      signal,
    ),
  );
}

export async function fetchSectorLeaders(
  asOf: string,
  taxonomyId: string,
  sectorId: string,
  fetcher: typeof fetch = fetch,
  signal: AbortSignal = new AbortController().signal,
): Promise<SectorLeadersResponse> {
  return parseSectorLeaders(
    await fetchJson(
      `/analysis/sectors/${encodeURIComponent(sectorId)}/leaders?${query(
        asOf,
        taxonomyId,
      )}`,
      fetcher,
      signal,
    ),
  );
}
