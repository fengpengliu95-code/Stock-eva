import { describe, expect, it, vi } from "vitest";

import {
  backendIndicatorResult,
  createStockCockpitChart,
  toKLineData,
} from "./stock-cockpit";
import { analysisFixture } from "../__tests__/fixtures";

describe("KLineChart adapter", () => {
  it("passes backend OHLCV and indicator values through without recomputation", () => {
    const response = analysisFixture();

    const data = toKLineData(response.series);
    const result = backendIndicatorResult(data, ["ma5", "macd", "rsi14"]);

    expect(data[1]).toMatchObject({
      open: 11,
      high: 13,
      low: 10,
      close: 12,
      volume: 1200,
      ma5: 11.5,
      macd: 0.2,
      rsi14: 55,
    });
    expect(result[0]).toEqual({
      ma5: null,
      macd: null,
      rsi14: null,
    });
  });

  it("registers four API-backed indicators without calculating MACD or RSI", () => {
    const callback = vi.fn();
    const chart = {
      setSymbol: vi.fn(),
      setPeriod: vi.fn(),
      setDataLoader: vi.fn((loader) => loader.getBars({ callback })),
      createIndicator: vi.fn(),
    };
    const dispose = vi.fn();
    const registerIndicator = vi.fn();
    const container = document.createElement("div");

    const cleanup = createStockCockpitChart(container, analysisFixture(), {
      init: vi.fn(() => chart),
      dispose,
      registerIndicator,
    });

    expect(chart.setSymbol).toHaveBeenCalledBefore(chart.setPeriod);
    expect(chart.setPeriod).toHaveBeenCalledBefore(chart.setDataLoader);
    expect(callback).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({ close: 12, volume: 1200, ma5: 11.5 }),
      ]),
      { forward: false, backward: false },
    );
    expect(registerIndicator).toHaveBeenCalledTimes(4);

    const templates = registerIndicator.mock.calls.map(([template]) => template);
    const byName = new Map(templates.map((template) => [template.name, template]));
    const macd = byName.get("STOCK_EVA_API_MACD");
    const rsi = byName.get("STOCK_EVA_API_RSI");
    expect(
      macd?.figures.map((figure: { key: string }) => figure.key),
    ).toEqual([
      "macd",
      "macd_signal",
      "macd_hist",
    ]);
    expect(rsi?.figures.map((figure: { key: string }) => figure.key)).toEqual([
      "rsi14",
    ]);

    const backendData = toKLineData(analysisFixture().series).map(
      (point, index) => ({
        ...point,
        close: index === 0 ? 999_999 : -999_999,
      }),
    );
    backendData.push({
      ...backendData[1],
      timestamp: backendData[1].timestamp + 86_400_000,
      close: 123_456,
      macd: 0,
      macd_signal: 0,
      macd_hist: 0,
      rsi14: 0,
    });
    expect(macd?.calc(backendData)).toEqual([
      { macd: null, macd_signal: null, macd_hist: null },
      { macd: 0.2, macd_signal: 0.1, macd_hist: 0.1 },
      { macd: 0, macd_signal: 0, macd_hist: 0 },
    ]);
    expect(rsi?.calc(backendData)).toEqual([
      { rsi14: null },
      { rsi14: 55 },
      { rsi14: 0 },
    ]);

    expect(chart.createIndicator.mock.calls).toEqual([
      [{ name: "STOCK_EVA_API_MA", paneId: "candle_pane" }, true],
      ["STOCK_EVA_API_VOLUME"],
      ["STOCK_EVA_API_MACD"],
      ["STOCK_EVA_API_RSI"],
    ]);
    cleanup();
    expect(dispose).toHaveBeenCalledWith(container);
  });
});
