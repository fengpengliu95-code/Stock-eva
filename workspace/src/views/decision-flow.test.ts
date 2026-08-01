import { getAllByRole, getByRole, queryByRole } from "@testing-library/dom";
import { describe, expect, it } from "vitest";

import {
  decisionQuery,
  leadersFixture,
  marketFixture,
  sectorsFixture,
} from "../__tests__/decision-fixtures";
import {
  renderDecisionFlow,
  type DecisionFlowState,
} from "./decision-flow";

function root(): HTMLElement {
  document.body.innerHTML = `<section id="decision-flow" aria-label="盘后决策流"></section>`;
  return document.querySelector("section")!;
}

function readyState(
  overrides: Partial<Extract<DecisionFlowState, { phase: "ready" }>> = {},
): Extract<DecisionFlowState, { phase: "ready" }> {
  return {
    phase: "ready",
    query: decisionQuery,
    overview: {
      market: marketFixture(),
      sectors: sectorsFixture(),
    },
    selectedSectorId: "sector-b",
    leaders: { phase: "ready", response: leadersFixture() },
    ...overrides,
  };
}

describe("after-close decision flow view", () => {
  it("renders five ordered evidence stages without elevating a narrow sample", () => {
    renderDecisionFlow(root(), readyState());

    const stages = getAllByRole(document.body, "listitem");
    expect(stages.map((item) => item.dataset.decisionStage)).toEqual([
      "market",
      "fund-flow",
      "sectors",
      "portfolio-risk",
      "leaders",
    ]);
    expect(document.body.textContent).toContain("仅沪深主板价格样本");
    expect(document.body.textContent).toContain("不能代表全 A 股");
    expect(document.body.textContent).toContain("战略样本状态");
    expect(document.body.textContent).toContain("bear");
    expect(document.body.textContent).toContain("战术样本状态");
    expect(document.body.textContent).toContain("risk_off");
    expect(document.body.textContent).toContain("置信度 42%");
    expect(document.body.textContent).toContain("market-regime-v1");
    expect(document.body.textContent).toContain("trend_down");
    expect(document.body.textContent).not.toContain("全 A 股熊市");
  });

  it("keeps fund flow and portfolio risk explicitly unavailable", () => {
    renderDecisionFlow(root(), readyState());

    const fund = document.querySelector<HTMLElement>(
      "[data-decision-stage='fund-flow']",
    )!;
    const portfolio = document.querySelector<HTMLElement>(
      "[data-decision-stage='portfolio-risk']",
    )!;
    expect(fund.textContent).toContain("资金证据尚不可用");
    expect(fund.textContent).toContain("成交额仅属于量价证据");
    expect(fund.textContent).toContain("Release 2");
    expect(portfolio.textContent).toContain("尚无本地组合风险输入");
    expect(portfolio.textContent).toContain("不生成仓位区间");
    expect(document.body.textContent).not.toContain("主力净流入");
  });

  it("preserves backend ranking order and expands raw metrics, coverage, and lineage", () => {
    renderDecisionFlow(root(), readyState());

    const sectorStage = document.querySelector<HTMLElement>(
      "[data-decision-stage='sectors']",
    )!;
    const sectorButtons = getAllByRole(sectorStage, "button", {
      name: /查看.*龙头/,
    });
    expect(sectorButtons.map((button) => button.textContent)).toEqual([
      expect.stringContaining("后端第二行"),
      expect.stringContaining("后端第一名"),
    ]);
    expect(sectorStage.textContent).toContain("原始值 0.12");
    expect(sectorStage.textContent).toContain("18 / 20");
    expect(sectorStage.textContent).toContain("覆盖率 90%");
    expect(sectorStage.textContent).toContain(
      "sector-relative-strength-20d-v1",
    );
    expect(sectorStage.textContent).toContain("classification-fixture");
    expect(sectorStage.textContent).toContain("requested_unverified");
    expect(sectorStage.textContent).toContain("龙头数量 3");
    expect(sectorStage.textContent).toContain("扩散度 15%");
    expect(sectorStage.textContent).toContain("持续 5 日");
  });

  it("renders non-actionable candidates, exclusions, and one-click cockpit controls", () => {
    renderDecisionFlow(root(), readyState());

    const leaders = document.querySelector<HTMLElement>(
      "[data-decision-stage='leaders']",
    )!;
    const controls = getAllByRole(leaders, "button", {
      name: /打开.*技术驾驶舱/,
    });
    expect(controls.map((item) => item.dataset.securitySymbol)).toEqual([
      "sz.000002",
      "sh.600001",
    ]);
    expect(controls[0].dataset.securityAsOf).toBe("2026-01-31");
    expect(controls[0].dataset.securitySector).toBe("sector-b");
    expect(leaders.textContent).toContain("不可作为操作首选");
    expect(leaders.textContent).toContain("涨跌停锁定状态不可用");
    expect(leaders.textContent).toContain("leader-qualification-v1");
    expect(leaders.textContent).toContain("relative_strength_qualified");
    expect(leaders.textContent).toContain("trend_quality_below_threshold");
    expect(leaders.textContent).toContain("sh.600009");
    expect(leaders.textContent).toContain("suspended");
    expect(leaders.textContent).toContain("bad_price_quality");
    expect(leaders.textContent).not.toContain("买入");
    expect(leaders.textContent).not.toContain("卖出");
    expect(leaders.textContent).not.toContain("推荐");
  });

  it("distinguishes empty evidence from an assertion that no hotspot exists", () => {
    renderDecisionFlow(
      root(),
      readyState({
        overview: {
          market: marketFixture({
            status: "empty",
            quality_status: "empty",
            data_as_of: null,
            total_score: null,
            component_scores: [],
          }),
          sectors: sectorsFixture({
            status: "empty",
            quality_status: "empty",
            data_as_of: null,
            classification_lineage: null,
            market_lineage: null,
            rankings: [],
            missing_inputs: ["classification.promoted_generation"],
          }),
        },
        selectedSectorId: null,
        leaders: { phase: "idle" },
      }),
    );

    expect(document.body.textContent).toContain("市场状态证据不足");
    expect(document.body.textContent).toContain(
      "板块数据为空不代表市场没有热点",
    );
    expect(document.body.textContent).toContain(
      "classification.promoted_generation",
    );
    expect(queryByRole(document.body, "button", { name: /查看.*龙头/ })).toBeNull();
  });

  it.each([
    [422, "future_as_of", "日期不在可读取范围"],
    [503, "market_storage_unavailable", "本地行情存储暂不可用"],
  ])("renders a controllable HTTP %s state", (status, message, expected) => {
    const state: DecisionFlowState = {
      phase: "error",
      query: decisionQuery,
      status,
      message,
    };

    renderDecisionFlow(root(), state);

    expect(
      getByRole(document.body, "alert").textContent,
    ).toContain(expected);
    expect(document.body.textContent).toContain("资金证据尚不可用");
    expect(document.body.textContent).toContain("尚无本地组合风险输入");
  });

  it("uses native headings, details, and named status regions", () => {
    renderDecisionFlow(root(), readyState());

    expect(getByRole(document.body, "heading", { name: "市场状态" })).toBeTruthy();
    expect(getByRole(document.body, "heading", { name: "板块轮动" })).toBeTruthy();
    expect(getByRole(document.body, "heading", { name: "龙头候选 → 个股" })).toBeTruthy();
    expect(getAllByRole(document.body, "status").length).toBeGreaterThan(0);
    const details = getAllByRole(document.body, "group");
    expect(details.length).toBeGreaterThan(2);
    expect(details[0].tagName).toBe("DETAILS");
    expect(details[0].textContent).toContain("查看");
  });
});
