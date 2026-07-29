import { describe, expect, it } from "vitest";

import { initialCockpitState, transitionCockpit } from "./state";
import { analysisFixture } from "./__tests__/fixtures";

describe("security cockpit state machine", () => {
  it("transitions through loading and ready", () => {
    const loading = transitionCockpit(initialCockpitState, {
      type: "load",
      symbol: "sh.600000",
      sourceView: "portfolio",
    });
    const ready = transitionCockpit(loading, {
      type: "success",
      response: analysisFixture(),
    });

    expect(loading).toMatchObject({
      phase: "loading",
      symbol: "sh.600000",
      sourceView: "portfolio",
    });
    expect(ready).toMatchObject({ phase: "ready" });
  });

  it.each(["no_market_data", "no_effective_trading_data"])(
    "keeps %s as an explicit empty state",
    (reason) => {
      const state = transitionCockpit(
        transitionCockpit(initialCockpitState, {
          type: "load",
          symbol: "sh.600000",
          sourceView: "watchlists",
        }),
        {
          type: "success",
          response: analysisFixture({
            status: "empty",
            as_of: null,
            quality_issues: [reason],
            series: [],
          }),
        },
      );

      expect(state).toMatchObject({ phase: "empty", reason });
    },
  );

  it("keeps quality and connection errors separate", () => {
    const loading = transitionCockpit(initialCockpitState, {
      type: "load",
      symbol: "sh.600000",
      sourceView: "portfolio",
    });

    expect(
      transitionCockpit(loading, {
        type: "failure",
        status: 409,
        message: "missing adjust_factor",
      }),
    ).toMatchObject({ phase: "quality-error" });
    expect(
      transitionCockpit(loading, {
        type: "failure",
        status: null,
        message: "Failed to fetch",
      }),
    ).toMatchObject({ phase: "connection-error" });
  });
});
