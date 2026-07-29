import { fireEvent, waitFor } from "@testing-library/dom";
import { afterEach, describe, expect, it, vi } from "vitest";

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
}

interface CockpitLifecycle {
  initializeSecurityCockpit?: () => void;
  disposeSecurityCockpit?: () => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

function emptyAnalysis(symbol: string): Response {
  return jsonResponse({
    symbol,
    status: "empty",
    as_of: null,
    source: "baostock",
    price_adjustment: "qfq",
    formula_version: "race-test-v1",
    quality_issues: ["no_market_data"],
    series: [],
  });
}

function installCockpit(): void {
  document.body.innerHTML = `
    <button type="button" data-security-symbol="sh.600000" data-security-source="portfolio">
      Open A
    </button>
    <button type="button" data-security-symbol="sz.000001" data-security-source="watchlists">
      Open B
    </button>
    <section id="security-analysis">
      <a id="security-back"></a>
      <div id="security-status" role="status"></div>
      <div id="security-content"></div>
    </section>
  `;
  window.history.replaceState(null, "", "#portfolio");
  window.stockEvaActivateView = vi.fn();
}

function renderedSymbol(): string | null {
  return document.querySelector(".security-metadata dd")?.textContent ?? null;
}

let disposeCockpitUnderTest: (() => void) | undefined;

async function settleRequest(): Promise<void> {
  await new Promise((resolve) => window.setTimeout(resolve, 0));
}

describe("security cockpit request ownership", () => {
  afterEach(() => {
    disposeCockpitUnderTest?.();
    disposeCockpitUnderTest = undefined;
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    delete window.stockEvaActivateView;
    document.body.replaceChildren();
    window.history.replaceState(null, "", "#portfolio");
  });

  it("keeps B when an aborted A resolves or rejects after B", async () => {
    installCockpit();
    const aSuccess = deferred<Response>();
    const bAfterSuccess = deferred<Response>();
    const aFailure = deferred<Response>();
    const bAfterFailure = deferred<Response>();
    const aHistory = deferred<Response>();
    const aReinitialize = deferred<Response>();
    const bHistory = deferred<Response>();
    const bDuplicate = deferred<Response>();
    const responseQueues = new Map([
      ["sh.600000", [aSuccess, aFailure, aHistory, aReinitialize]],
      [
        "sz.000001",
        [bAfterSuccess, bAfterFailure, bHistory, bDuplicate],
      ],
    ]);
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/market/history/dates")) {
        return Promise.resolve(jsonResponse(["2025-01-02", "2025-01-03"]));
      }
      const symbol = decodeURIComponent(
        url.pathname.match(/\/securities\/([^/]+)\/analysis$/)?.[1] ?? "",
      );
      const next = responseQueues.get(symbol)?.shift();
      if (!next) throw new Error(`unexpected request for ${url.pathname}`);
      return next.promise;
    });
    vi.stubGlobal("fetch", fetchMock);

    const cockpitModule = (await import("./main")) as CockpitLifecycle;
    disposeCockpitUnderTest = cockpitModule.disposeSecurityCockpit;
    document.dispatchEvent(new Event("DOMContentLoaded", { bubbles: true }));
    const [openA, openB] = Array.from(
      document.querySelectorAll<HTMLButtonElement>("[data-security-symbol]"),
    );

    fireEvent.click(openA);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    fireEvent.click(openB);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(4));
    bAfterSuccess.resolve(emptyAnalysis("sz.000001"));
    await waitFor(() => expect(renderedSymbol()).toBe("sz.000001"));
    aSuccess.resolve(emptyAnalysis("sh.600000"));
    await settleRequest();

    expect(window.location.hash).toBe(
      "#security/sz.000001?from=watchlists",
    );
    expect(renderedSymbol()).toBe("sz.000001");

    fireEvent.click(openA);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(6));
    fireEvent.click(openB);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(8));
    bAfterFailure.resolve(emptyAnalysis("sz.000001"));
    await waitFor(() => expect(renderedSymbol()).toBe("sz.000001"));
    aFailure.reject(new Error("stale A failure"));
    await settleRequest();

    expect(window.location.hash).toBe(
      "#security/sz.000001?from=watchlists",
    );
    expect(renderedSymbol()).toBe("sz.000001");
    expect(document.body.textContent).not.toContain("stale A failure");

    window.history.replaceState(
      null,
      "",
      "#security/sh.600000?from=portfolio",
    );
    window.dispatchEvent(new PopStateEvent("popstate"));
    await settleRequest();
    expect(fetchMock).toHaveBeenCalledTimes(10);
    window.history.replaceState(
      null,
      "",
      "#security/sz.000001?from=watchlists",
    );
    window.dispatchEvent(new PopStateEvent("popstate"));
    window.dispatchEvent(new HashChangeEvent("hashchange"));
    await settleRequest();
    expect(fetchMock).toHaveBeenCalledTimes(12);
    bHistory.resolve(emptyAnalysis("sz.000001"));
    await settleRequest();
    expect(renderedSymbol()).toBe("sz.000001");

    expect(typeof cockpitModule.disposeSecurityCockpit).toBe("function");
    expect(typeof cockpitModule.initializeSecurityCockpit).toBe("function");
    cockpitModule.disposeSecurityCockpit?.();
    const callsBeforeDisposeProbe = fetchMock.mock.calls.length;
    fireEvent.click(openA);
    window.dispatchEvent(new PopStateEvent("popstate"));
    window.dispatchEvent(new HashChangeEvent("hashchange"));
    await settleRequest();
    expect(fetchMock).toHaveBeenCalledTimes(callsBeforeDisposeProbe);

    window.history.replaceState(null, "", "#portfolio");
    cockpitModule.initializeSecurityCockpit?.();
    fireEvent.click(openA);
    await settleRequest();
    expect(fetchMock).toHaveBeenCalledTimes(callsBeforeDisposeProbe + 2);
    aReinitialize.resolve(emptyAnalysis("sh.600000"));
    await settleRequest();
    expect(renderedSymbol()).toBe("sh.600000");
    cockpitModule.disposeSecurityCockpit?.();
  });
});
