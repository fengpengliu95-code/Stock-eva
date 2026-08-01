const API_BASE = "http://127.0.0.1:8000/api/v1";
const SYMBOL_PATTERN = /^(sh|sz)\.[0-9]{6}$/;
const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export type NullableNumber = number | null;

export interface TechnicalAnalysisPoint {
  trade_date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount: number;
  ma5: NullableNumber;
  ma10: NullableNumber;
  ma20: NullableNumber;
  ma60: NullableNumber;
  ma120: NullableNumber;
  ma250: NullableNumber;
  macd: NullableNumber;
  macd_signal: NullableNumber;
  macd_hist: NullableNumber;
  rsi14: NullableNumber;
}

export interface SecurityAnalysisResponse {
  symbol: string;
  status: "empty" | "ready";
  as_of: string | null;
  source: "baostock";
  price_adjustment: "qfq";
  formula_version: string;
  quality_issues: string[];
  series: TechnicalAnalysisPoint[];
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

export class NoTradingDatesError extends Error {
  constructor() {
    super("no backend-provided trading dates");
    this.name = "NoTradingDatesError";
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  if (typeof value !== "string") throw new TypeError(`${key} must be a string`);
  return value;
}

function numberField(record: Record<string, unknown>, key: string): number {
  const value = record[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new TypeError(`${key} must be a finite number`);
  }
  return value;
}

function nullableNumberField(
  record: Record<string, unknown>,
  key: string,
): NullableNumber {
  if (record[key] === null) return null;
  return numberField(record, key);
}

function parsePoint(value: unknown): TechnicalAnalysisPoint {
  if (!isRecord(value)) throw new TypeError("series point must be an object");
  const tradeDate = stringField(value, "trade_date");
  if (!ISO_DATE_PATTERN.test(tradeDate)) {
    throw new TypeError("trade_date must be an ISO date");
  }
  return {
    trade_date: tradeDate,
    open: numberField(value, "open"),
    high: numberField(value, "high"),
    low: numberField(value, "low"),
    close: numberField(value, "close"),
    volume: numberField(value, "volume"),
    amount: numberField(value, "amount"),
    ma5: nullableNumberField(value, "ma5"),
    ma10: nullableNumberField(value, "ma10"),
    ma20: nullableNumberField(value, "ma20"),
    ma60: nullableNumberField(value, "ma60"),
    ma120: nullableNumberField(value, "ma120"),
    ma250: nullableNumberField(value, "ma250"),
    macd: nullableNumberField(value, "macd"),
    macd_signal: nullableNumberField(value, "macd_signal"),
    macd_hist: nullableNumberField(value, "macd_hist"),
    rsi14: nullableNumberField(value, "rsi14"),
  };
}

export function parseAnalysisResponse(value: unknown): SecurityAnalysisResponse {
  if (!isRecord(value)) throw new TypeError("analysis response must be an object");
  const symbol = stringField(value, "symbol");
  const status = stringField(value, "status");
  const source = stringField(value, "source");
  const priceAdjustment = stringField(value, "price_adjustment");
  const formulaVersion = stringField(value, "formula_version");
  if (!SYMBOL_PATTERN.test(symbol)) throw new TypeError("invalid response symbol");
  if (status !== "empty" && status !== "ready") {
    throw new TypeError("invalid analysis status");
  }
  if (source !== "baostock") throw new TypeError("invalid analysis source");
  if (priceAdjustment !== "qfq") {
    throw new TypeError("invalid price adjustment");
  }
  if (value.as_of !== null && typeof value.as_of !== "string") {
    throw new TypeError("as_of must be an ISO date or null");
  }
  if (
    typeof value.as_of === "string" &&
    !ISO_DATE_PATTERN.test(value.as_of)
  ) {
    throw new TypeError("as_of must be an ISO date or null");
  }
  if (
    !Array.isArray(value.quality_issues) ||
    !value.quality_issues.every((issue) => typeof issue === "string")
  ) {
    throw new TypeError("quality_issues must be a string array");
  }
  if (!Array.isArray(value.series)) {
    throw new TypeError("series must be an array");
  }
  return {
    symbol,
    status,
    as_of: value.as_of,
    source,
    price_adjustment: priceAdjustment,
    formula_version: formulaVersion,
    quality_issues: value.quality_issues,
    series: value.series.map(parsePoint),
  };
}

function parseTradingDates(value: unknown): string[] {
  if (
    !Array.isArray(value) ||
    !value.every(
      (item) => typeof item === "string" && ISO_DATE_PATTERN.test(item),
    )
  ) {
    throw new TypeError("trading dates must be an ISO date array");
  }
  for (let index = 1; index < value.length; index += 1) {
    if (value[index] <= value[index - 1]) {
      throw new TypeError("trading dates must be strictly ascending");
    }
  }
  return value;
}

export function selectTradingDateWindow(
  dates: string[],
  asOf?: string,
): { start: string; end: string } {
  const validDates = parseTradingDates(dates);
  if (asOf !== undefined && !ISO_DATE_PATTERN.test(asOf)) {
    throw new TypeError("as_of must be an ISO date");
  }
  const boundedDates =
    asOf === undefined
      ? validDates
      : validDates.filter((tradeDate) => tradeDate <= asOf);
  if (boundedDates.length === 0) throw new NoTradingDatesError();
  const window = boundedDates.slice(-260);
  return { start: window[0], end: window[window.length - 1] };
}

async function errorDetail(response: Response): Promise<string> {
  try {
    const payload: unknown = await response.json();
    if (isRecord(payload) && typeof payload.detail === "string") {
      return payload.detail;
    }
  } catch {
    // The status code remains the authoritative fallback.
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

export async function fetchSecurityAnalysis(
  symbol: string,
  fetcher: typeof fetch = fetch,
  signal: AbortSignal = new AbortController().signal,
  asOf?: string,
): Promise<SecurityAnalysisResponse> {
  if (!SYMBOL_PATTERN.test(symbol)) throw new TypeError("invalid security symbol");
  const dates = parseTradingDates(
    await fetchJson("/market/history/dates", fetcher, signal),
  );
  const { start, end } = selectTradingDateWindow(dates, asOf);
  const query = new URLSearchParams({ start, end });
  const payload = await fetchJson(
    `/securities/${encodeURIComponent(symbol)}/analysis?${query}`,
    fetcher,
    signal,
  );
  return parseAnalysisResponse(payload);
}
