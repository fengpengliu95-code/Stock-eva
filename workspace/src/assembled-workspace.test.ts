import {
  fireEvent,
  getByRole,
  queryByTestId,
  waitFor,
} from "@testing-library/dom";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

// @ts-expect-error Vite's ?raw loader supplies the assembled production HTML.
import workspaceHtml from "../index.html?raw";

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function installAssembledDocument(html: string): void {
  const parsed = new DOMParser().parseFromString(html, "text/html");
  document.head.innerHTML = parsed.head.innerHTML;
  document.body.innerHTML = parsed.body.innerHTML;
  window.history.replaceState(null, "", "#portfolio");
}

describe("assembled legacy workspace and cockpit entry", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("uses real legacy holding/watchlist controls for source-aware navigation", async () => {
    installAssembledDocument(workspaceHtml);
    vi.stubGlobal("scrollTo", vi.fn());
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input));
        if (url.pathname.endsWith("/portfolio/positions")) {
          return jsonResponse([
            {
              id: "position-1",
              symbol: "sh.600000",
              quantity: "100",
              avg_cost: "10.25",
              as_of_date: "2025-01-03",
              today_buy_qty: "0",
              version: 1,
            },
          ]);
        }
        if (url.pathname.endsWith("/portfolio/valuation")) {
          return jsonResponse({
            status: "ready",
            data_date: "2025-01-03",
            source: "baostock",
            covered_market_value: 1200,
            covered_cost_basis: 1025,
            covered_unrealized_pnl: 175,
            valuation_basis: "synthetic assembled-page test",
            coverage: {
              covered: 1,
              total: 1,
              missing_symbols: [],
              suspended_symbols: [],
            },
          });
        }
        if (url.pathname.endsWith("/watchlists")) {
          return jsonResponse([{ id: "watchlist-1", name: "验收列表", version: 1 }]);
        }
        if (url.pathname.endsWith("/watchlists/watchlist-1/items")) {
          return jsonResponse([{ symbol: "sz.000001" }]);
        }
        if (url.pathname.endsWith("/market/history/dates")) {
          return jsonResponse(["2025-01-02", "2025-01-03"]);
        }
        if (url.pathname.includes("/securities/")) {
          const symbol = decodeURIComponent(
            url.pathname.match(/\/securities\/([^/]+)\/analysis$/)?.[1] ?? "",
          );
          return jsonResponse({
            symbol,
            status: "empty",
            as_of: null,
            source: "baostock",
            price_adjustment: "qfq",
            formula_version: "synthetic-test-v1",
            quality_issues: ["no_market_data"],
            series: [],
          });
        }
        return jsonResponse({ detail: "unused synthetic endpoint" }, 503);
      }),
    );

    // app.js is the production legacy renderer; main.ts is the source of the
    // generated module referenced by the same assembled index.html.
    // @ts-expect-error app.js intentionally remains an untyped legacy script.
    await import("../app.js");
    await import("./main");
    document.dispatchEvent(new Event("DOMContentLoaded", { bubbles: true }));

    const holding = await waitFor(() =>
      getByRole(document.body, "button", {
        name: "查看 sh.600000 个股技术分析",
      }),
    );
    expect(holding.dataset.securitySource).toBe("portfolio");
    fireEvent.click(holding);
    await waitFor(() =>
      expect(window.location.hash).toBe(
        "#security/sh.600000?from=portfolio",
      ),
    );
    expect(queryByTestId(document.body, "security-chart")).toBeNull();

    const back = getByRole(document.body, "link", { name: /返回持仓/ });
    fireEvent.click(back);
    expect(window.location.hash).toBe("#portfolio");
    expect(document.querySelector<HTMLElement>("#portfolio")?.hidden).toBe(false);

    fireEvent.click(
      document.querySelector<HTMLElement>(
        ".primary-nav [data-view-target='watchlists']",
      )!,
    );
    const watchlist = await waitFor(() =>
      getByRole(document.body, "button", {
        name: "查看 sz.000001 个股技术分析",
      }),
    );
    expect(watchlist.dataset.securitySource).toBe("watchlists");
    watchlist.focus();
    await userEvent.setup().keyboard("{Enter}");
    await waitFor(() =>
      expect(window.location.hash).toBe(
        "#security/sz.000001?from=watchlists",
      ),
    );
  });
});
