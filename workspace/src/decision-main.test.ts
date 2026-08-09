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

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function currentShanghaiDate(): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function installDocument(): void {
  document.body.innerHTML = `
    <section id="overview" data-view="overview">
      <section id="decision-flow" aria-label="盘后决策流"></section>
    </section>
    <section id="security-analysis" data-view="security" hidden>
      <a id="security-back"></a>
      <div id="security-status" role="status"></div>
      <section id="security-evidence" aria-label="个股复盘上下文"></section>
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
    module.initializeSecurityCockpit?.();

    let cockpit = await waitFor(() =>
      getByRole(document.body, "button", {
        name: "打开 sz.000002 技术驾驶舱",
      }),
    );
    expect(fetcher).toHaveBeenCalledWith(
      "http://127.0.0.1:8000/api/v1/analysis/sectors/sector-b/leaders?as_of=2026-01-31&taxonomy_id=baostock.industry_classification",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );

    const sectorButton = getByRole(document.body, "button", {
      name: "查看 后端第二行 龙头",
    });
    sectorButton.focus();
    fireEvent.click(sectorButton);
    await waitFor(() =>
      expect(
        (document.activeElement as HTMLElement | null)?.dataset.decisionSector,
      ).toBe("sector-b"),
    );
    cockpit = await waitFor(() =>
      getByRole(document.body, "button", {
        name: "打开 sz.000002 技术驾驶舱",
      }),
    );
    expect(
      (document.activeElement as HTMLElement | null)?.dataset.decisionSector,
    ).toBe("sector-b");
    const contextCallsBeforeOpen = fetcher.mock.calls.filter(([input]) =>
      String(input).includes("/api/v1/analysis/"),
    ).length;

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
    const evidence = document.querySelector<HTMLElement>("#security-evidence")!;
    expect(evidence.textContent).toContain("同一复盘上下文");
    expect(evidence.textContent).toContain("后端第二行");
    expect(evidence.textContent).toContain("候选乙");
    expect(evidence.textContent).toContain("资金证据 missing / unavailable");
    expect(
      fetcher.mock.calls.filter(([input]) =>
        String(input).includes("/api/v1/analysis/"),
      ),
    ).toHaveLength(contextCallsBeforeOpen);
  });

  it("round-trips a standalone sector deep link through the technical cockpit", async () => {
    installDocument();
    document.body.insertAdjacentHTML(
      "afterbegin",
      '<section id="sectors" data-view="sectors"><section id="sector-decision-flow" aria-label="板块证据工作区"></section></section>',
    );
    window.history.replaceState(
      null,
      "",
      "#sectors?as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b",
    );
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/analysis/market-regime")) return jsonResponse(marketFixture());
      if (url.pathname.endsWith("/analysis/sector-rotation")) return jsonResponse(sectorsFixture());
      if (url.pathname.includes("/analysis/sectors/sector-b/leaders")) return jsonResponse(leadersFixture());
      if (url.pathname.endsWith("/market/history/dates")) return jsonResponse(["2026-01-30", "2026-01-31"]);
      if (url.pathname.endsWith("/securities/sz.000002/analysis")) {
        return jsonResponse({ symbol: "sz.000002", status: "empty", as_of: null, source: "baostock", price_adjustment: "qfq", formula_version: "sector-route-test", quality_issues: ["no_market_data"], series: [] });
      }
      return jsonResponse({ detail: "unexpected endpoint" }, 503);
    });
    vi.stubGlobal("fetch", fetcher);

    const module = (await import("./main")) as WorkspaceLifecycle;
    dispose = module.disposeSecurityCockpit;
    document.dispatchEvent(new Event("DOMContentLoaded", { bubbles: true }));
    module.initializeSecurityCockpit?.();

    const cockpit = await waitFor(() => getByRole(document.body, "button", { name: "打开 sz.000002 技术驾驶舱" }));
    expect(document.querySelector("#sector-decision-flow")?.textContent).toContain("板块证据工作区");
    fireEvent.click(cockpit);
    await waitFor(() => expect(window.location.hash).toContain("return_view=sectors"));
    const back = getByRole(document.body, "link", { name: /返回盘后决策流/ });
    expect(back.getAttribute("href")).toBe("#sectors?as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b");
    fireEvent.click(back);
    await waitFor(() => expect(window.location.hash).toBe("#sectors?as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b"));
  });

  it("aborts a pending standalone-sector request when navigation leaves the decision views", async () => {
    installDocument();
    document.body.insertAdjacentHTML(
      "afterbegin",
      '<section id="sectors" data-view="sectors"><section id="sector-decision-flow" aria-label="板块证据工作区"></section></section>',
    );
    window.history.replaceState(
      null,
      "",
      "#sectors?as_of=2026-02-02&taxonomy_id=baostock.industry_classification",
    );
    const market = deferred<Response>();
    const sectors = deferred<Response>();
    const fetcher = vi.fn<typeof fetch>((input) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/analysis/market-regime")) return market.promise;
      if (url.pathname.endsWith("/analysis/sector-rotation")) return sectors.promise;
      return Promise.resolve(jsonResponse({ detail: "unexpected endpoint" }, 503));
    });
    vi.stubGlobal("fetch", fetcher);

    const module = (await import("./main")) as WorkspaceLifecycle;
    dispose = module.disposeSecurityCockpit;
    module.initializeSecurityCockpit?.();
    await waitFor(() =>
      expect(fetcher.mock.calls.filter(([input]) => String(input).includes("/analysis/")).length).toBe(2),
    );

    window.history.pushState(null, "", "#watchlists");
    window.dispatchEvent(new Event("stock-eva-route-change"));
    const signals = fetcher.mock.calls
      .filter(([input]) => String(input).includes("/analysis/"))
      .map(([, init]) => init?.signal as AbortSignal);
    expect(signals.every((signal) => signal.aborted)).toBe(true);

    market.resolve(jsonResponse(marketFixture()));
    sectors.resolve(jsonResponse(sectorsFixture()));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(window.location.hash).toBe("#watchlists");
    expect(document.querySelector("#sector-decision-flow")?.textContent).not.toContain("后端板块排名");
  });

  it("fails closed from malformed standalone-sector input before analysis requests", async () => {
    installDocument();
    document.body.insertAdjacentHTML(
      "afterbegin",
      '<section id="sectors" data-view="sectors"><section id="sector-decision-flow" aria-label="板块证据工作区"></section></section>',
    );
    window.history.replaceState(
      null,
      "",
      "#sectors?as_of=2026-13-40&taxonomy_id=unreviewed.taxonomy",
    );
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/analysis/market-regime")) return jsonResponse(marketFixture());
      if (url.pathname.endsWith("/analysis/sector-rotation")) return jsonResponse(sectorsFixture());
      if (url.pathname.includes("/analysis/sectors/sector-b/leaders")) return jsonResponse(leadersFixture());
      return jsonResponse({ detail: "unexpected endpoint" }, 503);
    });
    vi.stubGlobal("fetch", fetcher);

    const module = (await import("./main")) as WorkspaceLifecycle;
    dispose = module.disposeSecurityCockpit;
    module.initializeSecurityCockpit?.();

    await waitFor(() => expect(document.querySelector("#sector-decision-flow")?.textContent).toContain("后端板块排名"));
    const analysisCalls = fetcher.mock.calls.filter(([input]) => String(input).includes("/analysis/"));
    expect(analysisCalls).not.toHaveLength(0);
    for (const [input] of analysisCalls) {
      const url = new URL(String(input));
      expect(url.searchParams.get("as_of")).toBe(currentShanghaiDate());
      if (!url.pathname.endsWith("/analysis/market-regime")) {
        expect(url.searchParams.get("taxonomy_id")).toBe("baostock.industry_classification");
      }
      expect(String(input)).not.toContain("2026-13-40");
      expect(String(input)).not.toContain("unreviewed.taxonomy");
    }
    expect(window.location.hash).toBe(
      `#sectors?as_of=${currentShanghaiDate()}&taxonomy_id=baostock.industry_classification&sector_id=sector-b`,
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

  it("keeps technicals and available context visible when one direct-link evidence slice is 503", async () => {
    installDocument();
    window.history.replaceState(
      null,
      "",
      "#security/sz.000002?from=sectors&as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b",
    );
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/analysis/market-regime")) {
        return jsonResponse(
          { detail: { code: "market_storage_unavailable" } },
          503,
        );
      }
      if (url.pathname.endsWith("/analysis/sector-rotation")) {
        return jsonResponse(sectorsFixture());
      }
      if (url.pathname.includes("/analysis/sectors/sector-b/leaders")) {
        return jsonResponse(leadersFixture());
      }
      if (url.pathname.endsWith("/market/history/dates")) {
        return jsonResponse(["2026-01-30", "2026-01-31"]);
      }
      if (url.pathname.endsWith("/securities/sz.000002/analysis")) {
        return jsonResponse({
          symbol: "sz.000002",
          status: "empty",
          as_of: null,
          source: "baostock",
          price_adjustment: "qfq",
          formula_version: "direct-context-v1",
          quality_issues: ["no_market_data"],
          series: [],
        });
      }
      return jsonResponse({ detail: "unexpected endpoint" }, 503);
    });
    vi.stubGlobal("fetch", fetcher);

    const module = (await import("./main")) as WorkspaceLifecycle;
    dispose = module.disposeSecurityCockpit;
    module.initializeSecurityCockpit?.();

    await waitFor(() =>
      expect(document.querySelector("#security-content")?.textContent).toContain(
        "没有可用行情",
      ),
    );
    const evidence = document.querySelector<HTMLElement>("#security-evidence")!;
    await waitFor(() => {
      expect(evidence.textContent).toContain("市场证据暂不可用");
      expect(evidence.textContent).toContain("HTTP 503");
      expect(evidence.textContent).toContain("后端第二行");
      expect(evidence.textContent).toContain("候选乙");
    });
    expect(
      fetcher.mock.calls.filter(([input]) =>
        String(input).includes("/api/v1/analysis/"),
      ),
    ).toHaveLength(3);
  });

  it("aborts stale direct-link context and keeps the newer symbol evidence", async () => {
    installDocument();
    window.history.replaceState(
      null,
      "",
      "#security/sh.600000?from=sectors&as_of=2026-01-31&taxonomy_id=baostock.industry_classification&sector_id=sector-b",
    );
    const oldMarket = deferred<Response>();
    const oldSectors = deferred<Response>();
    const oldLeaders = deferred<Response>();
    const oldContext = [oldMarket, oldSectors, oldLeaders];
    const nextSector = {
      ...sectorsFixture().rankings[0],
      sector_id: "sector-c",
      sector_name: "新板块",
    };
    const nextCandidate = {
      ...leadersFixture().candidates[0],
      symbol: "sz.000001",
      name: "新候选",
    };
    const fetcher = vi.fn((input: RequestInfo | URL, _init?: RequestInit) => {
      const url = new URL(String(input));
      const asOf = url.searchParams.get("as_of");
      if (url.pathname.endsWith("/analysis/market-regime")) {
        return asOf === "2026-01-31"
          ? oldMarket.promise
          : Promise.resolve(
              jsonResponse(marketFixture({ as_of: "2026-02-02", data_as_of: "2026-02-02" })),
            );
      }
      if (url.pathname.endsWith("/analysis/sector-rotation")) {
        return asOf === "2026-01-31"
          ? oldSectors.promise
          : Promise.resolve(
              jsonResponse(
                sectorsFixture({
                  as_of: "2026-02-02",
                  data_as_of: "2026-02-02",
                  rankings: [nextSector],
                }),
              ),
            );
      }
      if (url.pathname.includes("/analysis/sectors/")) {
        return asOf === "2026-01-31"
          ? oldLeaders.promise
          : Promise.resolve(
              jsonResponse(
                leadersFixture({
                  as_of: "2026-02-02",
                  data_as_of: "2026-02-02",
                  sector_id: "sector-c",
                  sector_name: "新板块",
                  candidates: [nextCandidate],
                }),
              ),
            );
      }
      if (url.pathname.endsWith("/market/history/dates")) {
        return Promise.resolve(jsonResponse(["2026-01-30", "2026-02-02"]));
      }
      const symbol = decodeURIComponent(
        url.pathname.match(/\/securities\/([^/]+)\/analysis$/)?.[1] ?? "",
      );
      return Promise.resolve(
        jsonResponse({
          symbol,
          status: "empty",
          as_of: null,
          source: "baostock",
          price_adjustment: "qfq",
          formula_version: "context-race-v1",
          quality_issues: ["no_market_data"],
          series: [],
        }),
      );
    });
    vi.stubGlobal("fetch", fetcher);

    const module = (await import("./main")) as WorkspaceLifecycle;
    dispose = module.disposeSecurityCockpit;
    module.initializeSecurityCockpit?.();
    await waitFor(() =>
      expect(
        fetcher.mock.calls.filter(([input]) =>
          String(input).includes("/api/v1/analysis/"),
        ),
      ).toHaveLength(3),
    );

    window.history.pushState(
      null,
      "",
      "#security/sz.000001?from=sectors&as_of=2026-02-02&taxonomy_id=baostock.industry_classification&sector_id=sector-c",
    );
    window.dispatchEvent(new PopStateEvent("popstate"));

    const evidence = document.querySelector<HTMLElement>("#security-evidence")!;
    await waitFor(() => {
      expect(evidence.textContent).toContain("sz.000001");
      expect(evidence.textContent).toContain("新板块");
      expect(evidence.textContent).toContain("新候选");
    });
    const oldSignals = fetcher.mock.calls
      .filter(([input]) => String(input).includes("as_of=2026-01-31"))
      .map(([, init]) => init?.signal as AbortSignal);
    expect(oldSignals).toHaveLength(3);
    expect(oldSignals.every((signal) => signal.aborted)).toBe(true);

    oldMarket.resolve(jsonResponse(marketFixture()));
    oldSectors.resolve(jsonResponse(sectorsFixture()));
    oldLeaders.resolve(jsonResponse(leadersFixture()));
    await Promise.all(oldContext.map((item) => item.promise));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(evidence.textContent).toContain("sz.000001");
    expect(evidence.textContent).toContain("新板块");
    expect(evidence.textContent).not.toContain("候选乙");
  });
});
