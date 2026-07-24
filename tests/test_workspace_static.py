from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).parents[1]


class WorkspaceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()
        self.labels_for: set[str] = set()
        self.links: set[str] = set()
        self.tags: list[tuple[str, dict[str, str]]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        attributes = dict(attrs)
        self.tags.append((tag, attributes))
        if identifier := attributes.get("id"):
            self.ids.add(identifier)
        if tag == "label" and (target := attributes.get("for")):
            self.labels_for.add(target)
        if tag == "a" and (href := attributes.get("href")):
            self.links.add(href)


def parse(path: Path) -> WorkspaceParser:
    parser = WorkspaceParser()
    parser.feed(path.read_text())
    return parser


def test_root_entry_separates_workspace_from_learning_dashboard() -> None:
    parser = parse(ROOT / "index.html")

    assert "./workspace/" in parser.links
    assert "./dashboard/" in parser.links


def test_workspace_has_accessible_landmarks_labels_and_live_status() -> None:
    parser = parse(ROOT / "workspace" / "index.html")
    required_ids = {
        "expected-date",
        "position-symbol",
        "position-quantity",
        "position-avg-cost",
        "position-as-of",
        "position-today-buy",
        "watchlist-name",
        "watchlist-symbol",
        "strategy-select",
        "strategy-symbols",
        "strategy-date",
    }

    assert required_ids <= parser.ids
    assert required_ids <= parser.labels_for
    assert any(tag == "main" for tag, _ in parser.tags)
    assert any(tag == "nav" for tag, _ in parser.tags)
    assert any(attrs.get("aria-live") == "polite" for _, attrs in parser.tags)
    assert any(href == "#main" for href in parser.links)


def test_workspace_uses_native_controls_and_has_no_inline_event_handlers() -> None:
    parser = parse(ROOT / "workspace" / "index.html")

    assert any(tag == "button" for tag, _ in parser.tags)
    assert all(
        not any(name.startswith("on") for name in attributes)
        for _, attributes in parser.tags
    )


def test_workspace_script_uses_real_api_without_browser_storage_or_fake_market_data() -> None:
    script = (ROOT / "workspace" / "app.js").read_text()

    for endpoint in (
        "/market/summary",
        "/portfolio/positions",
        "/portfolio/valuation",
        "/watchlists",
        "/strategies",
    ):
        assert endpoint in script
    assert "localStorage" not in script
    assert "sessionStorage" not in script
    assert "innerHTML" not in script
    assert "eval(" not in script
    assert "event.currentTarget.reset()" not in script
    assert "value === null || value === undefined" in script


def test_workspace_styles_include_focus_mobile_and_reduced_motion_rules() -> None:
    styles = (ROOT / "workspace" / "style.css").read_text()

    assert ":focus-visible" in styles
    assert "@media (max-width:" in styles
    assert "prefers-reduced-motion" in styles
    assert "--market-up:" in styles
    assert "--market-down:" in styles


def test_workspace_exposes_accessible_alert_controls_and_history() -> None:
    parser = parse(ROOT / "workspace" / "index.html")
    required_ids = {
        "alert-rule-name",
        "alert-watchlist",
        "alert-strategy",
        "alert-signal-date",
        "alert-rule-select",
        "alert-evaluate",
        "alert-history",
        "alert-status",
    }

    assert required_ids <= parser.ids
    assert {
        "alert-rule-name",
        "alert-watchlist",
        "alert-strategy",
        "alert-signal-date",
        "alert-rule-select",
    } <= parser.labels_for
    assert any(
        attributes.get("aria-live") == "polite"
        for _, attributes in parser.tags
    )


def test_workspace_alert_script_uses_local_api_and_safe_dom_rendering() -> None:
    script = (ROOT / "workspace" / "app.js").read_text()

    for endpoint in (
        "/alerts/rules",
        "/alerts/events",
    ):
        assert endpoint in script
    for action in ('"acknowledge"', '"suppress"', '"restore"'):
        assert action in script
    assert script.count("await loadWatchlists();\n      await loadAlerts();") >= 2
    assert "localStorage" not in script
    assert "innerHTML" not in script
