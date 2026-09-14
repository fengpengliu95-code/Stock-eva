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

EXPECTED = {"FR": 18, "NFR": 10, "AC": 14, "EC": 18}


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
