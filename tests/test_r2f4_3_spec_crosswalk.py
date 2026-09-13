"""Strict current-document crosswalk checks for the R2-F4.3 RC."""

import ast
import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
DOCS = (
    ROOT / "docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-design.md",
    ROOT / "docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-implementation.md",
)


def _test_names() -> set[str]:
    names: set[str] = set()
    for path in (ROOT / "tests").rglob("test_*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names.update(
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name.startswith("test_")
        )
    return names


def test_current_docs_have_only_real_test_anchors_and_shared_release_boundary() -> None:
    names = _test_names()
    anchors = []
    for path in DOCS:
        text = path.read_text(encoding="utf-8")
        assert "RC / pending final independent gate" in text
        assert "LOCAL_CHAIN_ONLY" in text
        assert "Candidate/NO-GO pending" in text
        anchors.extend(re.findall(r"(?<![A-Za-z0-9_])test_[A-Za-z0-9_]+", text))
    assert anchors
    assert set(anchors) <= names


def test_current_docs_repeat_the_same_exact_anchor_set() -> None:
    sets = [
        set(re.findall(r"(?<![A-Za-z0-9_])test_[A-Za-z0-9_]+", path.read_text(encoding="utf-8")))
        for path in DOCS
    ]
    assert sets[0] == sets[1]
