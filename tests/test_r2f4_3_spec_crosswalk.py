"""Strict current-document crosswalk checks for the R2-F4.3 RC."""

import ast
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
DOCS = (
    ROOT / "docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-design.md",
    ROOT / "docs/plans/2026-09-09-stock-eva-r2f4-3-replication-restore-implementation.md",
)
ID_PATTERN = re.compile(r"\b(?:FR|NFR|AC|EC)-[0-9]+[a-z]?\b")
ROW_PATTERN = re.compile(
    r"^\| ((?:FR|NFR|AC|EC)-[0-9]+[a-z]?) \| (.*?) \| (test_[A-Za-z0-9_]+) \|$",
    re.MULTILINE,
)
EXPECTED_COUNTS = {"FR": 42, "NFR": 16, "AC": 31, "EC": 40}
# Approved digest of the effective semantic matrix recovered from the
# pre-consolidation contract and its implemented supersessions.  This catches
# a return to one generic placeholder description while allowing anchors to be
# reused across requirements.
SEMANTIC_MATRIX_SHA256 = "5aac85dbc263abf4dae42a27856579c1b5c117d743cde6e37b5a81613eb6db59"


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
        assert "RC / Candidate/NO-GO pending next independent audit" in text
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


def test_normative_corpus_has_exact_unique_ids_and_real_crosswalk_rows() -> None:
    names = _test_names()
    parsed: list[tuple[set[str], list[tuple[str, str, str]]]] = []
    for path in DOCS:
        text = path.read_text(encoding="utf-8")
        ids = ID_PATTERN.findall(text)
        rows = ROW_PATTERN.findall(text)
        assert len(ids) == sum(EXPECTED_COUNTS.values())
        assert len(set(ids)) == len(ids)
        assert len(rows) == sum(EXPECTED_COUNTS.values())
        assert {anchor for _id, _description, anchor in rows} <= names
        assert set(ids) == {row_id for row_id, _description, _anchor in rows}
        descriptions = [description for _id, description, _anchor in rows]
        assert len(set(descriptions)) >= 120
        for prefix, count in EXPECTED_COUNTS.items():
            assert sum(item.startswith(prefix + "-") for item in ids) == count
        matrix = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        assert hashlib.sha256(matrix.encode()).hexdigest() == SEMANTIC_MATRIX_SHA256
        parsed.append((set(ids), rows))
    assert parsed[0][0] == parsed[1][0]
    assert parsed[0][1] == parsed[1][1]
