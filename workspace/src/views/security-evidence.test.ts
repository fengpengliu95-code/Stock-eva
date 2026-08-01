import { getAllByRole, getByRole } from "@testing-library/dom";
import { describe, expect, it } from "vitest";

import {
  leadersFixture,
  marketFixture,
  sectorsFixture,
} from "../__tests__/decision-fixtures";
import {
  renderSecurityEvidence,
  type SecurityEvidenceState,
} from "./security-evidence";

const context = {
  asOf: "2026-01-31",
  taxonomyId: "baostock.industry_classification",
  sectorId: "sector-b",
};

function root(): HTMLElement {
  document.body.innerHTML = `<section id="security-evidence" aria-label="个股复盘上下文"></section>`;
  return document.querySelector("section")!;
}

function readyState(
  overrides: Partial<Extract<SecurityEvidenceState, { phase: "ready" }>> = {},
): Extract<SecurityEvidenceState, { phase: "ready" }> {
  return {
    phase: "ready",
    symbol: "sz.000002",
    context,
    market: { phase: "ready", response: marketFixture() },
    sectors: { phase: "ready", response: sectorsFixture() },
    leaders: { phase: "ready", response: leadersFixture() },
    ...overrides,
  };
}

describe("security decision-context evidence", () => {
  it("keeps market, sector, leader, and missing fund-flow evidence beside the technicals", () => {
    renderSecurityEvidence(root(), readyState());

    expect(
      getByRole(document.body, "heading", { name: "同一复盘上下文" }),
    ).toBeTruthy();
    expect(document.body.textContent).toContain("2026-01-31");
    expect(document.body.textContent).toContain("bear");
    expect(document.body.textContent).toContain("risk_off");
    expect(document.body.textContent).toContain("narrow_provisional");
    expect(document.body.textContent).toContain("后端第二行");
    expect(document.body.textContent).toContain("后端顺序 2");
    expect(document.body.textContent).toContain("候选乙");
    expect(document.body.textContent).toContain("qualified");
    expect(document.body.textContent).toContain("relative_strength_qualified");
    expect(document.body.textContent).toContain("risk_inputs_unavailable");
    expect(document.body.textContent).toContain("涨跌停锁定状态 unavailable");
    expect(document.body.textContent).toContain("资金证据 missing / unavailable");
    expect(document.body.textContent).toContain("成交额不等于净流入");
    expect(document.body.textContent).not.toContain("买入");
    expect(document.body.textContent).not.toContain("卖出");
    expect(document.body.textContent).not.toContain("推荐");
  });

  it("degrades one evidence slice without hiding the other context", () => {
    renderSecurityEvidence(
      root(),
      readyState({
        market: {
          phase: "error",
          status: 503,
          message: "market_storage_unavailable",
        },
      }),
    );

    expect(getAllByRole(document.body, "article")).toHaveLength(4);
    expect(document.body.textContent).toContain("市场证据暂不可用");
    expect(document.body.textContent).toContain("HTTP 503");
    expect(document.body.textContent).toContain("后端第二行");
    expect(document.body.textContent).toContain("候选乙");
  });

  it("states truthfully when the symbol is excluded or absent", () => {
    renderSecurityEvidence(
      root(),
      readyState({
        symbol: "sh.600009",
      }),
    );
    expect(document.body.textContent).toContain("后端排除");
    expect(document.body.textContent).toContain("suspended");

    renderSecurityEvidence(root(), readyState({ symbol: "sh.600008" }));
    expect(document.body.textContent).toContain("未出现在候选或排除列表");
    expect(document.body.textContent).toContain("不解释为没有板块关联");
  });
});
