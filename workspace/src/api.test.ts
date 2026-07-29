import { describe, expect, it, vi } from "vitest";

import {
  ApiError,
  fetchSecurityAnalysis,
  parseAnalysisResponse,
  selectTradingDateWindow,
} from "./api";
import { analysisFixture } from "./__tests__/fixtures";

describe("security analysis API", () => {
  it("uses the last at most 260 backend-provided trading dates", () => {
    const dates = Array.from({ length: 300 }, (_, index) => {
      const date = new Date(Date.UTC(2024, 0, index + 1));
      return date.toISOString().slice(0, 10);
    });

    expect(selectTradingDateWindow(dates)).toEqual({
      start: dates[40],
      end: dates[299],
    });
    expect(selectTradingDateWindow(dates.slice(0, 2))).toEqual({
      start: dates[0],
      end: dates[1],
    });
  });

  it("preserves indicator nulls instead of coercing them to zero", () => {
    const payload = analysisFixture();

    const parsed = parseAnalysisResponse(payload);

    expect(parsed.series[0].ma5).toBeNull();
    expect(parsed.series[0].macd).toBeNull();
    expect(parsed.series[0].rsi14).toBeNull();
  });

  it("fetches dates before analysis and uses only that date window", async () => {
    const payload = analysisFixture();
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify(["2025-01-02", "2025-01-03"]), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify(payload), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );

    const result = await fetchSecurityAnalysis("sh.600000", fetcher);

    expect(result).toEqual(payload);
    expect(fetcher).toHaveBeenNthCalledWith(
      1,
      "http://127.0.0.1:8000/api/v1/market/history/dates",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(fetcher).toHaveBeenNthCalledWith(
      2,
      "http://127.0.0.1:8000/api/v1/securities/sh.600000/analysis?start=2025-01-02&end=2025-01-03",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
  });

  it("keeps HTTP 409 distinguishable from connection failures", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response(JSON.stringify(["2025-01-02"]), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ detail: "missing adjust_factor" }), {
          status: 409,
          headers: { "content-type": "application/json" },
        }),
      );

    await expect(fetchSecurityAnalysis("sh.600000", fetcher)).rejects.toEqual(
      expect.objectContaining({
        name: "ApiError",
        status: 409,
        detail: "missing adjust_factor",
      }),
    );
  });
});
