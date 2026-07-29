import { fireEvent } from "@testing-library/dom";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import {
  bindSecurityEntrypoints,
  parseSecurityHash,
  securityHash,
} from "./navigation";

describe("security navigation", () => {
  it("round-trips symbol and source through a reloadable hash", () => {
    const hash = securityHash("sz.000001", "watchlists");

    expect(hash).toBe("#security/sz.000001?from=watchlists");
    expect(parseSecurityHash(hash)).toEqual({
      symbol: "sz.000001",
      sourceView: "watchlists",
    });
    expect(parseSecurityHash("#security/not-a-symbol")).toBeNull();
  });

  it("opens native symbol buttons for click, Enter and Space activation", async () => {
    document.body.innerHTML = `
      <button type="button" data-security-symbol="sh.600000" data-security-source="portfolio">
        查看 sh.600000
      </button>
    `;
    const navigate = vi.fn();
    const unbind = bindSecurityEntrypoints(document, navigate);
    const button = document.querySelector("button")!;
    const user = userEvent.setup();

    fireEvent.click(button);
    expect(navigate).toHaveBeenCalledTimes(1);
    button.focus();
    await user.keyboard("{Enter}");
    expect(navigate).toHaveBeenCalledTimes(2);
    await user.keyboard(" ");
    expect(navigate).toHaveBeenCalledTimes(3);
    fireEvent.keyDown(button, { key: "Enter" });
    expect(navigate).toHaveBeenCalledTimes(4);
    fireEvent.keyDown(button, { key: " " });
    expect(navigate).toHaveBeenCalledTimes(5);
    expect(navigate).toHaveBeenLastCalledWith("sh.600000", "portfolio");
    unbind();
  });
});
