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

  it("loads backend bars and creates API-backed MA and volume panes", () => {
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
    expect(chart.createIndicator).toHaveBeenCalledTimes(2);
    cleanup();
    expect(dispose).toHaveBeenCalledWith(container);
  });
});
