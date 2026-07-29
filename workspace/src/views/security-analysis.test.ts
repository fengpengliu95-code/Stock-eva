import { getByRole, queryByTestId } from "@testing-library/dom";
import { describe, expect, it, vi } from "vitest";

import { renderSecurityAnalysis } from "./security-analysis";
import { analysisFixture } from "../__tests__/fixtures";
import type { CockpitState } from "../state";

function root(): HTMLElement {
  document.body.innerHTML = `
    <section id="security-analysis">
      <a id="security-back"></a>
      <div id="security-status" role="status"></div>
      <div id="security-content"></div>
    </section>
  `;
  return document.querySelector("section")!;
}

describe("security analysis view", () => {
  it("renders truthful metadata, null alternatives, and accessible indicator tables", () => {
    const renderChart = vi.fn(() => vi.fn());
    const state: CockpitState = {
      phase: "ready",
      symbol: "sh.600000",
      sourceView: "portfolio",
      response: analysisFixture(),
    };

    renderSecurityAnalysis(root(), state, renderChart);

    expect(document.body.textContent).toContain("sh.600000");
    expect(document.body.textContent).toContain("2025-01-03");
    expect(document.body.textContent).toContain("baostock");
    expect(document.body.textContent).toContain("qfq");
    expect(document.body.textContent).toContain("ta-lib-0.7.0-r0-v1");
    expect(document.body.textContent).toContain("—");
    expect(
      getByRole(document.body, "img").getAttribute("aria-label"),
    ).toContain("MACD、RSI14");
    expect(getByRole(document.body, "table", { name: "技术指标数值替代" })).toBeTruthy();
    expect(renderChart).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["empty", "no_market_data", "没有可用行情"],
    ["empty", "no_effective_trading_data", "没有有效交易数据"],
    ["quality-error", "missing adjust_factor", "数据质量门禁"],
    ["connection-error", "Failed to fetch", "无法连接本地 API"],
  ] as const)(
    "renders %s without a chart",
    (phase, message, expected) => {
      const renderChart = vi.fn();
      const state = {
        phase,
        symbol: "sh.600000",
        sourceView: "watchlists",
        ...(phase === "empty" ? { reason: message } : { message }),
      } as CockpitState;

      renderSecurityAnalysis(root(), state, renderChart);

      expect(document.body.textContent).toContain(expected);
      expect(renderChart).not.toHaveBeenCalled();
      expect(queryByTestId(document.body, "security-chart")).toBeNull();
    },
  );
});
