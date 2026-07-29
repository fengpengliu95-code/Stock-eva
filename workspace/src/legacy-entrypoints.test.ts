import { describe, expect, it } from "vitest";

declare global {
  interface Window {
    stockEvaCreateSecurityButton?: (
      symbol: string,
      source: "portfolio" | "watchlists",
    ) => HTMLButtonElement;
  }
}

describe("legacy workspace security entrypoints", () => {
  it.each([
    ["sh.600000", "portfolio"],
    ["sz.000001", "watchlists"],
  ] as const)(
    "creates an accessible %s entry from %s",
    async (symbol, source) => {
      // @ts-expect-error app.js intentionally remains an untyped legacy script.
      await import("../app.js");

      const control = window.stockEvaCreateSecurityButton?.(symbol, source);

      expect(control).toBeInstanceOf(HTMLButtonElement);
      expect(control?.type).toBe("button");
      expect(control?.textContent).toBe(symbol);
      expect(control?.dataset.securitySymbol).toBe(symbol);
      expect(control?.dataset.securitySource).toBe(source);
      expect(control?.getAttribute("aria-label")).toContain(symbol);
    },
  );
});
