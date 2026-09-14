"""SPEC-FIRST validator for the R2-F5.0 acceptance-harness documents.

This test validates only documentation/crosswalk invariants. It intentionally does not import
or execute an acceptance service, provider, database writer, restore operation, or production
runtime. Planned anchors are not asserted to exist until the implementation phase.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
DESIGN = ROOT / "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md"
PLAN = (
    ROOT / "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-implementation.md"
)
MATRIX = ROOT / "docs/acceptance/r2f5-0-requirement-evidence-matrix.md"
ID = re.compile(r"^(?:FR|NFR|AC|EC)-\d+$")
DESIGN_ID = re.compile(r"^(?:- (?:FR|NFR|EC)-\d+:|### (?:AC)-\d+:)")
MATRIX_ROW = re.compile(
    r"^\| ((?:FR|NFR|AC|EC)-\d+) \| (.*?) \| (.*?) \| (.*?) \| (.*?) \|$",
    re.MULTILINE,
)
SLO_ROW = re.compile(
    r"^\| `([a-z_]+)` \| (.*?) \| (.*?) \| `PLANNED::(test_r2f5_[A-Za-z0-9_]+)` \|$",
    re.MULTILINE,
)

EXPECTED = {"FR": 28, "NFR": 15, "AC": 23, "EC": 26}
EXPECTED_SLO = {
    "continuity",
    "next_morning_availability",
    "same_evening_availability",
    "coverage",
    "canonical_integrity",
    "source_purity",
    "recovery",
    "failover",
    "provenance",
    "replay",
    "adjustment",
    "calendar",
    "universe",
    "error_handling",
    "local_nas_isolation",
    "replication",
    "restore",
}


def _matrix_rows() -> list[tuple[str, str, str, str, str]]:
    return MATRIX_ROW.findall(MATRIX.read_text(encoding="utf-8"))


def _design_ids() -> list[str]:
    ids: list[str] = []
    for line in DESIGN.read_text(encoding="utf-8").splitlines():
        if DESIGN_ID.match(line):
            match = re.search(r"(?:FR|NFR|AC|EC)-\d+", line)
            assert match is not None
            ids.append(match.group(0))
    return ids


def _definition_ids(path: Path) -> list[str]:
    """Extract only definition lines, excluding parent-reference prose."""
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if re.match(r"^- (?:FR|NFR|EC)-\d+:", line) or re.match(r"^### AC-\d+:", line):
            match = re.search(r"(?:FR|NFR|AC|EC)-\d+", line)
            assert match is not None
            ids.append(match.group(0))
    return ids


def test_r2f5_spec_has_mandatory_sections_and_boundary() -> None:
    text = DESIGN.read_text(encoding="utf-8")
    for heading in (
        "## Context",
        "## Functional Requirements",
        "## Non-Functional Requirements",
        "## Acceptance Criteria",
        "## Edge Cases",
        "## API Contracts",
        "## Data Models",
        "## Out of Scope",
    ):
        assert heading in text
    assert "SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO" in text
    assert "5393f499dbc8b84398658816f7a555dd3e547d47" in text
    assert "MUST" in text and "MUST NOT" in text
    assert "production_window_started=false" in text
    assert "Task 20" in text and "20 confirmed consecutive" in text


def test_r2f5_crosswalk_is_exact() -> None:
    rows = _matrix_rows()
    assert len(rows) == sum(EXPECTED.values())
    ids = [row[0] for row in rows]
    assert len(ids) == len(set(ids))
    assert all(ID.fullmatch(item) for item in ids)
    assert {
        prefix: sum(item.startswith(prefix + "-") for item in ids) for prefix in EXPECTED
    } == EXPECTED
    assert set(ids) == set(_design_ids())
    design_ids = _definition_ids(DESIGN)
    plan_ids = _definition_ids(PLAN)
    assert len(design_ids) == len(set(design_ids))
    assert len(plan_ids) == len(set(plan_ids))
    assert set(design_ids) == set(ids)
    assert set(plan_ids) == set(ids)
    for requirement_id, summary, parents, anchors, stage in rows:
        assert summary.strip()
        assert "PLANNED" in anchors
        assert stage == "SPEC CANDIDATE; PLANNED"
        assert "PASS" not in stage and "GO" not in stage
        if requirement_id.startswith("AC-"):
            refs = [item.strip() for item in parents.split(",")]
            assert refs and all(item.startswith(("FR-", "NFR-")) for item in refs)
            assert all(item in ids for item in refs)


def test_r2f5_no_anchor_or_result_is_claimed() -> None:
    matrix = MATRIX.read_text(encoding="utf-8")
    assert "No anchor below is claimed" in matrix
    assert "Task 20" in matrix and "production soak" in matrix
    assert "IMPLEMENTATION NOT STARTED" in PLAN.read_text(encoding="utf-8")
    assert "NO-GO" in PLAN.read_text(encoding="utf-8")
    assert "Do not create a service" in PLAN.read_text(encoding="utf-8")


def test_r2f5_plan_preserves_required_commands_and_no_execute() -> None:
    text = PLAN.read_text(encoding="utf-8")
    for token in ("RED", "GREEN", "focused", "full", "ruff", "compileall", "git diff --check"):
        assert token in text
    assert "--execute" in text
    assert "no provider" in text.lower()
    assert "nas" in text.lower()


def test_r2f5_slo_inventory_and_planned_anchor_contract_are_explicit() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    for metric in (
        "continuity",
        "next_morning_availability",
        "same_evening_availability",
        "coverage",
        "canonical_integrity",
        "source_purity",
        "recovery",
        "failover",
        "provenance",
        "replay",
        "adjustment",
        "calendar",
        "universe",
        "error_handling",
        "local_nas_isolation",
        "replication",
        "restore",
    ):
        assert f"| `{metric}`" in design or f"  {metric}:" in design
    anchors = re.findall(r"PLANNED::(test_r2f5_[A-Za-z0-9_]+)", MATRIX.read_text(encoding="utf-8"))
    assert anchors
    assert all(name.startswith("test_r2f5_") for name in anchors)
    for row in _matrix_rows():
        row_anchors = re.findall(r"PLANNED::(test_r2f5_[A-Za-z0-9_]+)", row[3])
        assert row_anchors and len(row_anchors) == len(set(row_anchors))
    assert "LOCAL_CHAIN_ONLY" in design
    assert "REMOTE_VERIFIED" in design
    assert "create=True" in design
    assert "network_allowed: false" in design


def test_r2f5_matrix_has_one_explicit_row_per_roadmap_slo() -> None:
    rows = SLO_ROW.findall(MATRIX.read_text(encoding="utf-8"))
    assert {row[0] for row in rows} == EXPECTED_SLO
    assert len(rows) == len(EXPECTED_SLO)
    assert all(row[3].startswith("test_r2f5_") for row in rows)
    assert all(target.strip() and source.strip() for _field, target, source, _anchor in rows)
