import {
  dispose,
  init,
  registerIndicator,
  type Chart,
  type IndicatorTemplate,
  type KLineData,
} from "klinecharts";

import type {
  NullableNumber,
  SecurityAnalysisResponse,
  TechnicalAnalysisPoint,
} from "../api";

const API_MA = "STOCK_EVA_API_MA";
const API_VOLUME = "STOCK_EVA_API_VOLUME";
const MA_FIELDS = ["ma5", "ma10", "ma20", "ma60", "ma120", "ma250"] as const;

type IndicatorField =
  | (typeof MA_FIELDS)[number]
  | "macd"
  | "macd_signal"
  | "macd_hist"
  | "rsi14";
type IndicatorOutputField = IndicatorField | "volume";

export interface StockEvaKLineData extends KLineData {
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

type BackendIndicatorRow = Partial<Record<IndicatorOutputField, NullableNumber>>;

export interface ChartAdapterDependencies {
  init: (container: HTMLElement) => Pick<
    Chart,
    "setSymbol" | "setPeriod" | "setDataLoader" | "createIndicator"
  >;
  dispose: (container: HTMLElement) => void;
  registerIndicator: (template: IndicatorTemplate<BackendIndicatorRow>) => void;
}

const registeredBy = new WeakSet<ChartAdapterDependencies["registerIndicator"]>();

function shanghaiTimestamp(tradeDate: string): number {
  return Date.parse(`${tradeDate}T00:00:00+08:00`);
}

export function toKLineData(
  series: TechnicalAnalysisPoint[],
): StockEvaKLineData[] {
  return series.map((point) => ({
    timestamp: shanghaiTimestamp(point.trade_date),
    open: point.open,
    high: point.high,
    low: point.low,
    close: point.close,
    volume: point.volume,
    turnover: point.amount,
    amount: point.amount,
    ma5: point.ma5,
    ma10: point.ma10,
    ma20: point.ma20,
    ma60: point.ma60,
    ma120: point.ma120,
    ma250: point.ma250,
    macd: point.macd,
    macd_signal: point.macd_signal,
    macd_hist: point.macd_hist,
    rsi14: point.rsi14,
  }));
}

export function backendIndicatorResult(
  data: KLineData[],
  fields: readonly IndicatorField[],
): BackendIndicatorRow[] {
  return data.map((point) =>
    Object.fromEntries(fields.map((field) => [field, point[field] ?? null])),
  );
}

function registerBackendIndicators(
  register: ChartAdapterDependencies["registerIndicator"],
): void {
  if (registeredBy.has(register)) return;
  register({
    name: API_MA,
    shortName: "API MA",
    series: "price",
    figures: MA_FIELDS.map((field) => ({
      key: field,
      title: `${field.toUpperCase()}: `,
      type: "line",
    })),
    calc: (data) => backendIndicatorResult(data, MA_FIELDS),
  });
  register({
    name: API_VOLUME,
    shortName: "成交量",
    series: "volume",
    shouldFormatBigNumber: true,
    figures: [{ key: "volume", title: "VOL: ", type: "bar", baseValue: 0 }],
    calc: (data) =>
      data.map((point) => ({
        volume:
          typeof point.volume === "number" && Number.isFinite(point.volume)
            ? point.volume
            : null,
      })),
  });
  registeredBy.add(register);
}

const defaultDependencies: ChartAdapterDependencies = {
  init: (container) => {
    const chart = init(container, {
      locale: "zh-CN",
      timezone: "Asia/Shanghai",
      styles: {
        grid: {
          horizontal: { color: "#273440" },
          vertical: { color: "#273440" },
        },
        candle: {
          bar: {
            upColor: "#f05b67",
            downColor: "#24b884",
            noChangeColor: "#70808e",
            upBorderColor: "#f05b67",
            downBorderColor: "#24b884",
            noChangeBorderColor: "#70808e",
            upWickColor: "#f05b67",
            downWickColor: "#24b884",
            noChangeWickColor: "#70808e",
          },
        },
      },
    });
    if (!chart) throw new Error("KLineChart initialization failed");
    return chart;
  },
  dispose: (container) => {
    dispose(container);
  },
  registerIndicator,
};

export function createStockCockpitChart(
  container: HTMLElement,
  response: SecurityAnalysisResponse,
  dependencies: ChartAdapterDependencies = defaultDependencies,
): () => void {
  const bars = toKLineData(response.series);
  registerBackendIndicators(dependencies.registerIndicator);
  const chart = dependencies.init(container);
  chart.setSymbol({
    ticker: response.symbol,
    pricePrecision: 4,
    volumePrecision: 0,
  });
  chart.setPeriod({ span: 1, type: "day" });
  chart.setDataLoader({
    getBars: ({ callback }) => {
      callback(bars, { forward: false, backward: false });
    },
  });
  chart.createIndicator({ name: API_MA, paneId: "candle_pane" }, true);
  chart.createIndicator(API_VOLUME);
  return () => dependencies.dispose(container);
}
