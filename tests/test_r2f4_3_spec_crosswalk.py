"""Strict normative corpus and semantic evidence-matrix checks for R2-F4.3."""

from __future__ import annotations

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
MATRIX = ROOT / "docs/acceptance/r2f4-3-requirement-evidence-matrix.md"
ID_PATTERN = re.compile(r"\b(?:FR|NFR|AC|EC)-[0-9]+[a-z]?\b")
ROW_PATTERN = re.compile(
    r"^\| ((?:FR|NFR|AC|EC)-[0-9]+[a-z]?) \| (.*?) \| (test_[A-Za-z0-9_]+) \|$",
    re.MULTILINE,
)
MATRIX_ROW_PATTERN = re.compile(
    r"^\| ((?:FR|NFR|AC|EC)-[0-9]+[a-z]?) \| (.*?) \| (.*?) \| (.*?) \| (.*?) \|$",
    re.MULTILINE,
)
EXPECTED_COUNTS = {"FR": 42, "NFR": 16, "AC": 31, "EC": 40}
# Approved effective-matrix drift guard. This digest is not semantic proof; the
# matrix's reviewed summaries, references, bodies, and rationales are the proof.
SEMANTIC_MATRIX_SHA256 = "2f7d16fbdb385682cb64a1eb813fbd5b23c772a810ebfd383e65b5d981ad0505"
PLACEHOLDER_TEXT = {
    "effective latest contract remains fail-closed",
    "todo",
    "tbd",
    "placeholder",
}


def _test_catalog() -> dict[str, set[str]]:
    catalog: dict[str, set[str]] = {}
    for path in (ROOT / "tests").rglob("test_*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith(
                "test_"
            ):
                catalog.setdefault(node.name, set()).add(path.relative_to(ROOT).as_posix())
    return catalog


def _counts(ids: list[str]) -> dict[str, int]:
    return {
        prefix: sum(item.startswith(prefix + "-") for item in ids) for prefix in EXPECTED_COUNTS
    }


def _matrix_rows() -> list[tuple[str, str, str, str, str]]:
    return MATRIX_ROW_PATTERN.findall(MATRIX.read_text(encoding="utf-8"))


def _anchors(cell: str) -> set[str]:
    return set(re.findall(r"test_[A-Za-z0-9_]+", cell))


def test_current_docs_have_only_real_test_anchors_and_shared_release_boundary() -> None:
    catalog = _test_catalog()
    anchors = []
    for path in DOCS:
        text = path.read_text(encoding="utf-8")
        assert "RC / Candidate/NO-GO pending next independent audit" in text
        assert "LOCAL_CHAIN_ONLY" in text
        assert "Candidate/NO-GO pending" in text
        assert "r2f4-3-requirement-evidence-matrix.md" in text
        anchors.extend(re.findall(r"(?<![A-Za-z0-9_])test_[A-Za-z0-9_]+", text))
    assert anchors
    assert set(anchors) <= set(catalog)


def test_current_docs_repeat_the_same_exact_anchor_set() -> None:
    sets = [
        set(re.findall(r"(?<![A-Za-z0-9_])test_[A-Za-z0-9_]+", path.read_text(encoding="utf-8")))
        for path in DOCS
    ]
    assert sets[0] == sets[1]


def test_normative_corpus_has_exact_unique_ids_and_semantic_digest() -> None:
    catalog = _test_catalog()
    parsed: list[list[tuple[str, str, str]]] = []
    for path in DOCS:
        text = path.read_text(encoding="utf-8")
        ids = ID_PATTERN.findall(text)
        rows = ROW_PATTERN.findall(text)
        assert len(ids) == sum(EXPECTED_COUNTS.values())
        assert len(set(ids)) == len(ids)
        assert len(rows) == sum(EXPECTED_COUNTS.values())
        assert {anchor for _id, _description, anchor in rows} <= set(catalog)
        assert set(ids) == {row_id for row_id, _description, _anchor in rows}
        assert _counts(ids) == EXPECTED_COUNTS
        descriptions = [description for _id, description, _anchor in rows]
        assert len(set(descriptions)) >= 120
        matrix = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
        assert hashlib.sha256(matrix.encode()).hexdigest() == SEMANTIC_MATRIX_SHA256
        parsed.append(rows)
    assert parsed[0] == parsed[1]


def test_requirement_matrix_has_direct_bodies_refs_rationales_and_bounded_reuse() -> None:
    catalog = _test_catalog()
    rows = _matrix_rows()
    assert len(rows) == sum(EXPECTED_COUNTS.values())
    ids = [row[0] for row in rows]
    assert len(set(ids)) == len(ids)
    assert _counts(ids) == EXPECTED_COUNTS

    anchor_use: dict[str, int] = {}
    for _requirement_id, summary, _refs, anchors, rationale in rows:
        assert summary.strip()
        assert rationale.strip()
        assert "placeholder" not in summary.lower()
        assert "placeholder" not in rationale.lower()
        assert not summary.strip().endswith("—")
        anchor_names = _anchors(anchors)
        assert anchor_names
        assert anchor_names <= set(catalog)
        for anchor in anchor_names:
            anchor_use[anchor] = anchor_use.get(anchor, 0) + 1
    assert len(anchor_use) >= 60
    assert max(anchor_use.values()) <= 4


def test_acceptance_refs_are_known_and_overlap_direct_fr_nfr_evidence() -> None:
    rows = _matrix_rows()
    by_id = {row[0]: row for row in rows}
    known = set(by_id)
    evidence = {requirement_id: _anchors(row[3]) for requirement_id, row in by_id.items()}
    for requirement_id, _summary, refs, _anchors_cell, _rationale in rows:
        if not requirement_id.startswith("AC-"):
            continue
        ref_ids = refs.split(", ")
        assert ref_ids and ref_ids != ["—"]
        assert set(ref_ids) <= known
        assert all(ref.startswith(("FR-", "NFR-")) for ref in ref_ids)
        assert any(evidence[ref_id] & evidence[requirement_id] for ref_id in ref_ids)


def test_requirement_matrix_has_one_ast_catalog_and_execution_command_source() -> None:
    rows = _matrix_rows()
    anchors = sorted({anchor for row in rows for anchor in _anchors(row[3])})
    assert len(anchors) >= 60
    assert all(anchor.startswith("test_") for anchor in anchors)
