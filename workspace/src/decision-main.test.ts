import { fireEvent, getByRole, waitFor } from "@testing-library/dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  leadersFixture,
  marketFixture,
  sectorsFixture,
} from "./__tests__/decision-fixtures";

interface WorkspaceLifecycle {
  initializeSecurityCockpit?: () => void;
  disposeSecurityCockpit?: () => void;
}

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function installDocument(): void {
  document.body.innerHTML = `
    <section id="overview" data-view="overview">
      <section id="decision-flow" aria-label="盘后决策流"></section>
    </section>
    <section id="security-analysis" data-view="security" hidden>
      <a id="security-back"></a>
      <div id="security-status" role="status"></div>
      <div id="security-content"></div>
    </section>
  `;
  window.stockEvaActivateView = vi.fn((identifier: string) => {
    document.querySelectorAll<HTMLElement>("[data-view]").forEach((view) => {
      view.hidden = view.dataset.view !== identifier;
    });
  });
}

let dispose: (() => void) | undefined;

describe("decision-flow and technical-cockpit integration", () => {
  afterEach(() => {
    dispose?.();
    dispose = undefined;
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    delete window.stockEvaActivateView;
    document.body.replaceChildren();
    window.history.replaceState(null, "", "#overview");
  });

  it("restores a historical sector deep link and keeps as_of through the cockpit round trip", async () => {
    installDocument();
    window.history.replaceState(
      null,
      "",
      "#overview?as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b",
    );
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/analysis/market-regime")) {
        return jsonResponse(marketFixture());
      }
      if (url.pathname.endsWith("/analysis/sector-rotation")) {
        return jsonResponse(sectorsFixture());
      }
      if (url.pathname.includes("/analysis/sectors/sector-b/leaders")) {
        return jsonResponse(leadersFixture());
      }
      if (url.pathname.endsWith("/market/history/dates")) {
        return jsonResponse(["2026-01-30", "2026-01-31", "2026-02-02"]);
      }
      if (url.pathname.endsWith("/securities/sz.000002/analysis")) {
        return jsonResponse({
          symbol: "sz.000002",
          status: "empty",
          as_of: null,
          source: "baostock",
          price_adjustment: "qfq",
          formula_version: "historical-integration-v1",
          quality_issues: ["no_market_data"],
          series: [],
        });
      }
      return jsonResponse({ detail: "unexpected endpoint" }, 503);
    });
    vi.stubGlobal("fetch", fetcher);

    const module = (await import("./main")) as WorkspaceLifecycle;
    dispose = module.disposeSecurityCockpit;
    document.dispatchEvent(new Event("DOMContentLoaded", { bubbles: true }));

    const cockpit = await waitFor(() =>
      getByRole(document.body, "button", {
        name: "打开 sz.000002 技术驾驶舱",
      }),
    );
    expect(fetcher).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/analysis/sectors/sector-b/leaders?as_of=2026-01-31&taxonomy_id=baostock.industry_classification",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );

    fireEvent.click(cockpit);

    await waitFor(() =>
      expect(window.location.hash).toBe(
        "#security/sz.000002?from=sectors&as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b",
      ),
    );
    await waitFor(() =>
      expect(fetcher).toHaveBeenCalledWith(
        "http://127.0.0.1:8000/api/v1/securities/sz.000002/analysis?start=2026-01-30&end=2026-01-31",
        expect.objectContaining({ signal: expect.any(AbortSignal) }),
      ),
    );
    const back = getByRole(document.body, "link", { name: /返回盘后决策流/ });
    expect(back.getAttribute("href")).toBe(
      "#overview?as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b",
    );
  });

  it.each([
    [422, "future_as_of", "日期不在可读取范围"],
    [503, "market_storage_unavailable", "本地行情存储暂不可用"],
  ])("keeps decision HTTP %s controllable in the assembled lifecycle", async (status, code, copy) => {
    installDocument();
    window.history.replaceState(
      null,
      "",
      "#overview?as_of=2026-01-31&taxonomy_id=baostock.industry_classification",
    );
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ detail: { code } }, status)),
    );

    const module = (await import("./main")) as WorkspaceLifecycle;
    dispose = module.disposeSecurityCockpit;
    module.initializeSecurityCockpit?.();

    await waitFor(() =>
      expect(getByRole(document.body, "alert").textContent).toContain(copy),
    );
    expect(document.body.textContent).toContain("板块数据未读取");
    expect(document.body.textContent).not.toContain("市场没有热点");
  });
});
