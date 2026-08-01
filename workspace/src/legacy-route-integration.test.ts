import { fireEvent, getByRole, waitFor } from "@testing-library/dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  marketFixture,
  sectorsFixture,
} from "./__tests__/decision-fixtures";

// @ts-expect-error Vite's ?raw loader supplies the assembled production HTML.
import workspaceHtml from "../index.html?raw";

let disposeSecurityCockpit: (() => void) | undefined;

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

function installAssembledDocument(): void {
  const parsed = new DOMParser().parseFromString(workspaceHtml, "text/html");
  document.head.innerHTML = parsed.head.innerHTML;
  document.body.innerHTML = parsed.body.innerHTML;
  window.history.replaceState(
    null,
    "",
    "#overview?as_of=2026-01-31&taxonomy_id=baostock.industry_classification",
  );
}

describe("legacy and decision router integration", () => {
  afterEach(() => {
    disposeSecurityCockpit?.();
    disposeSecurityCockpit = undefined;
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    document.body.replaceChildren();
    window.history.replaceState(null, "", "#overview");
  });

  it("reloads the default decision date when legacy navigation pushes #overview", async () => {
    installAssembledDocument();
    vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
    const requestFrame = vi.spyOn(window, "requestAnimationFrame");
    document.documentElement.scrollTop = 120;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input));
        const asOf = url.searchParams.get("as_of") ?? "2026-01-31";
        if (url.pathname.endsWith("/analysis/market-regime")) {
          return jsonResponse(
            marketFixture({ as_of: asOf, data_as_of: asOf }),
          );
        }
        if (url.pathname.endsWith("/analysis/sector-rotation")) {
          return jsonResponse(
            sectorsFixture({ as_of: asOf, data_as_of: asOf, rankings: [] }),
          );
        }
        return jsonResponse({ detail: "unused route fixture" }, 503);
      }),
    );

    // @ts-expect-error app.js intentionally remains an untyped legacy script.
    await import("../app.js");
    const cockpitModule = await import("./main");
    disposeSecurityCockpit = cockpitModule.disposeSecurityCockpit;
    document.dispatchEvent(new Event("DOMContentLoaded", { bubbles: true }));
    cockpitModule.initializeSecurityCockpit();

    await waitFor(() =>
      expect(document.querySelector("#decision-flow")?.textContent).toContain(
        "请求时点 2026-01-31",
      ),
    );

    fireEvent.click(getByRole(document.body, "link", { name: /Stock EVA/ }));

    const today = currentShanghaiDate();
    await waitFor(() => {
      expect(window.location.hash).toBe(
        `#overview?as_of=${today}&taxonomy_id=baostock.industry_classification`,
      );
      expect(document.querySelector("#decision-flow")?.textContent).toContain(
        `请求时点 ${today}`,
      );
    });
    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(document.documentElement.scrollTop).toBe(0);
    expect(requestFrame).not.toHaveBeenCalled();
  });

  it("aborts a pending overview before legacy navigation leaves the decision view", async () => {
    installAssembledDocument();
    window.history.replaceState(
      null,
      "",
      "#overview?as_of=2026-02-02&taxonomy_id=baostock.industry_classification",
    );
    vi.spyOn(window, "scrollTo").mockImplementation(() => undefined);
    const market = deferred<Response>();
    const sectors = deferred<Response>();
    const fetcher = vi.fn<typeof fetch>((input) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/analysis/market-regime")) return market.promise;
      if (url.pathname.endsWith("/analysis/sector-rotation")) return sectors.promise;
      return Promise.resolve(
        jsonResponse({ detail: "unused route fixture" }, 503),
      );
    });
    vi.stubGlobal("fetch", fetcher);

    // @ts-expect-error app.js intentionally remains an untyped legacy script.
    await import("../app.js");
    const cockpitModule = await import("./main");
    disposeSecurityCockpit = cockpitModule.disposeSecurityCockpit;
    document.dispatchEvent(new Event("DOMContentLoaded", { bubbles: true }));
    cockpitModule.initializeSecurityCockpit();

    await waitFor(() =>
      expect(
        fetcher.mock.calls.filter(([input]) =>
          String(input).includes("/analysis/"),
        ),
      ).toHaveLength(2),
    );

    fireEvent.click(
      document.querySelector<HTMLElement>(
        ".primary-nav [data-view-target='watchlists']",
      )!,
    );

    const decisionSignals = fetcher.mock.calls
      .filter(([input]) => String(input).includes("/analysis/"))
      .map(([, init]) => init?.signal as AbortSignal);
    expect(decisionSignals.every((signal) => signal.aborted)).toBe(true);

    market.resolve(
      jsonResponse(
        marketFixture({ as_of: "2026-02-02", data_as_of: "2026-02-02" }),
      ),
    );
    sectors.resolve(
      jsonResponse(
        sectorsFixture({
          as_of: "2026-02-02",
          data_as_of: "2026-02-02",
          rankings: [],
        }),
      ),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(window.location.hash).toBe("#watchlists");
    expect(document.querySelector<HTMLElement>("#watchlists")?.hidden).toBe(
      false,
    );

    const callsBeforeDispose = fetcher.mock.calls.length;
    disposeSecurityCockpit();
    disposeSecurityCockpit = undefined;
    window.history.pushState(null, "", "#overview");
    window.dispatchEvent(new Event("stock-eva-route-change"));
    expect(fetcher).toHaveBeenCalledTimes(callsBeforeDispose);
  });
});
