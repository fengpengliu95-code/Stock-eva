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
REQ_ROW = re.compile(r"^\| ((?:FR|NFR|AC|EC)-\d+) \| (.*?) \| (.*?) \| (.*?) \| (.*?) \|$")
CATALOG_ROW = re.compile(r"^\| ((?:FR|NFR|AC|EC)-\d+) \| (test_r2f5_req_[a-z]+_\d{2}) \|$")
ANCHOR = re.compile(r"^test_r2f5_req_(?:fr|nfr|ac|ec)_\d{2}$")
SLO_ANCHOR = re.compile(r"^test_r2f5_slo_[a-z_]+$")
RFC2119 = re.compile(r"\b(?:MUST(?: NOT)?|SHOULD(?: NOT)?|MAY)\b")
REASON_SHAPES = (
    "_FAILED",
    "_UNAVAILABLE",
    "_MISMATCH",
    "_INVALID",
    "_DRIFT",
    "_LAG",
    "_EXCEEDED",
    "_MISSING",
)
EXTRA_REASON_CODES = {
    "INVALID_ARGUMENTS",
    "PATH_INVALID",
    "SNAPSHOT_CHANGED",
    "CONTROL_STATE_UNAVAILABLE",
    "PIT_VISIBILITY_INVALID",
    "CALENDAR_CONFLICT",
    "SESSION_COUNT_NOT_20",
    "REPLAY_SEMANTIC_MISMATCH",
    "REMOTE_PROOF_MISSING",
}


def _section(text: str, heading: str, next_heading: str) -> str:
    start = text.index(heading) + len(heading)
    end = text.index(next_heading, start)
    return text[start:end]


def _roadmap_dimensions() -> list[str]:
    section = _section(
        (ROOT / "docs/plans/2026-08-12-stock-eva-r2f-data-reliability-roadmap.md").read_text(
            encoding="utf-8"
        ),
        "## 10. Final SLO and acceptance matrix",
        "## 11. Dependency and release graph",
    )
    dimensions: list[str] = []
    for line in section.splitlines():
        match = re.match(r"^\| ([^|]+) \| ([^|]+) \|$", line)
        if not match or match.group(1).strip() == "Dimension":
            continue
        token = "_".join(
            re.findall(
                r"[a-z]+", match.group(1).strip().lower().replace("/", " ").replace("-", " ")
            )
        )
        if token:
            dimensions.append(token)
        # Section 10 names replication lag in the Local/NAS row. Promote that explicit
        # mandatory sub-dimension without maintaining a separate hand-written count/list.
        if "replication" in match.group(2).lower() and "replication" not in dimensions:
            dimensions.append("replication")
    assert dimensions and len(dimensions) == len(set(dimensions))
    return dimensions


def _matrix_rows() -> list[tuple[str, str, str, str, str]]:
    return [
        match.groups()
        for match in (
            REQ_ROW.match(line) for line in MATRIX.read_text(encoding="utf-8").splitlines()
        )
        if match
    ]


def _definition_ids(path: Path) -> list[str]:
    """Extract only normative definition lines, excluding parent-reference prose."""
    ids: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if re.match(r"^- (?:FR|NFR|EC)-\d+:", line) or re.match(r"^### AC-\d+:", line):
            match = re.search(r"(?:FR|NFR|AC|EC)-\d+", line)
            assert match is not None
            ids.append(match.group(0))
    return ids


def _requirement_blocks(text: str) -> list[tuple[str, str]]:
    lines = text.splitlines()
    starts = [
        index
        for index, line in enumerate(lines)
        if re.match(r"^- (?:FR|NFR|EC)-\d+:", line) or re.match(r"^### AC-\d+:", line)
    ]
    blocks: list[tuple[str, str]] = []
    for offset, start in enumerate(starts):
        end = starts[offset + 1] if offset + 1 < len(starts) else len(lines)
        match = re.search(r"(?:FR|NFR|AC|EC)-\d+", lines[start])
        assert match is not None
        blocks.append((match.group(0), "\n".join(lines[start:end])))
    return blocks


def _planned_anchors(value: str) -> list[str]:
    return re.findall(r"PLANNED::(test_r2f5_[A-Za-z0-9_]+)", value)


def _reason_source(design: str) -> list[str]:
    match = re.search(
        r"const ACCEPTANCE_REASON_CODES = \[(.*?)\] as const;", design, flags=re.DOTALL
    )
    assert match, "single ACCEPTANCE_REASON_CODES source missing"
    return re.findall(r'"([A-Z][A-Z0-9_]+)"', match.group(1))


def _reason_tokens(text: str) -> set[str]:
    tokens = set(re.findall(r"\b[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+\b", text))
    return {
        token
        for token in tokens
        if token in EXTRA_REASON_CODES
        or token.startswith("SESSION_")
        or any(token.endswith(shape) for shape in REASON_SHAPES)
    }


def _slo_rows(text: str) -> list[tuple[str, str, str, str, str]]:
    section = _section(
        text,
        "## R2-F5.0 metric contract and evidence sources",
        "## Secondary admission and failover evidence contract",
    )
    rows: list[tuple[str, str, str, str, str]] = []
    for line in section.splitlines():
        match = re.match(r"^\| `([a-z_]+)` \| (.*?) \| (.*?) \| (.*?) \| (.*?) \|$", line)
        if match:
            rows.append(match.groups())
    return rows


def _matrix_slo_rows(text: str) -> list[tuple[str, str, str, str, str, str]]:
    section = _section(text, "## Mandatory SLO crosswalk", "## Crosswalk interpretation")
    rows: list[tuple[str, str, str, str, str, str]] = []
    for line in section.splitlines():
        match = re.match(
            r"^\| `([a-z_]+)` \| (.*?) \| (.*?) \| `([A-Z0-9_]+)` \| (AC-\d+) \| "
            r"`PLANNED::(test_r2f5_slo_[a-z_]+)` \|$",
            line,
        )
        if match:
            rows.append(match.groups())
    return rows


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
    assert "d4ca71e7000d5dc8c93ab139409b8b6c682e4ff7" in text
    assert "MUST" in text and "MUST NOT" in text
    assert "production_window_started=false" in text
    assert "Task 20" in text and "20 confirmed consecutive" in text


def test_r2f5_crosswalk_is_exact_and_anchored() -> None:
    rows = _matrix_rows()
    assert rows
    ids = [row[0] for row in rows]
    assert len(ids) == len(set(ids)) and all(ID.fullmatch(item) for item in ids)
    assert {item.split("-")[0] for item in ids} == {"FR", "NFR", "AC", "EC"}
    assert set(ids) == set(_definition_ids(DESIGN)) == set(_definition_ids(PLAN))
    for requirement_id, summary, parents, anchors, stage in rows:
        assert summary.strip()
        row_anchors = _planned_anchors(anchors)
        assert len(row_anchors) == 1 and ANCHOR.fullmatch(row_anchors[0])
        assert stage == "SPEC CANDIDATE; PLANNED"
        if requirement_id.startswith(("AC-", "EC-")):
            refs = [item.strip() for item in parents.split(",")]
            assert refs and all(item.startswith(("FR-", "NFR-")) for item in refs)
            assert all(item in ids for item in refs)
    all_anchors = [_planned_anchors(row[3])[0] for row in rows]
    assert len(all_anchors) == len(set(all_anchors)), "requirement anchors must be globally unique"

    plan = PLAN.read_text(encoding="utf-8")
    catalog = [
        match.groups()
        for match in (
            re.match(r"^\| ((?:FR|NFR|AC|EC)-\d+) \| (test_r2f5_req_[a-z]+_\d{2}) \|$", line)
            for line in _section(
                plan, "## Planned pytest anchor catalog (X3)", "## Planned implementation tasks"
            ).splitlines()
        )
        if match
    ]
    assert len(catalog) == len({item[0] for item in catalog})
    assert {item[0] for item in catalog} == set(ids)
    assert {item[1] for item in catalog} == set(all_anchors)
    assert len(catalog) == len(set(item[1] for item in catalog))


def test_r2f5_requirement_blocks_are_rfc2119_and_unique() -> None:
    for path in (DESIGN, PLAN):
        blocks = _requirement_blocks(path.read_text(encoding="utf-8"))
        assert blocks
        block_ids = [identifier for identifier, _ in blocks]
        assert len(block_ids) == len(set(block_ids))
        for identifier, block in blocks:
            if identifier.startswith(("FR-", "NFR-")):
                assert re.search(r"\b(?:MUST(?: NOT)?|SHOULD(?: NOT)?|MAY)\b", block), identifier


def test_r2f5_no_anchor_or_result_is_claimed() -> None:
    matrix = MATRIX.read_text(encoding="utf-8")
    assert "d4ca71e7000d5dc8c93ab139409b8b6c682e4ff7" in matrix
    assert "5393f499dbc8b84398658816f7a555dd3e547d47" in matrix
    assert "No catalog entry exists or passes yet" in PLAN.read_text(encoding="utf-8")
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
    dimensions = _roadmap_dimensions()
    assert set(row[0] for row in _slo_rows(DESIGN.read_text(encoding="utf-8"))) == set(dimensions)
    assert set(row[0] for row in _matrix_slo_rows(MATRIX.read_text(encoding="utf-8"))) == set(
        dimensions
    )
    assert "LOCAL_CHAIN_ONLY" in design
    assert "REMOTE_VERIFIED" in design
    assert "create=True" in design
    assert "network_allowed: false" in design
    metric = _section(design, "interface MetricResult {", "const ACCEPTANCE_REASON_CODES = [")
    assert 'acceptance_ref: "AC-15";' in metric
    assert "planned_test_anchor: string;" in metric


def test_r2f5_reason_source_is_closed_unique_and_used() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    reasons = _reason_source(design)
    assert reasons and len(reasons) == len(set(reasons))
    assert "CONTINUITY_FAILED" in reasons and "READ_BOUNDARY_FAILED" in reasons
    assert "type AcceptanceReasonCode = ValuesOf<typeof ACCEPTANCE_REASON_CODES>" in design
    assert design.count("type AcceptanceReasonCode =") == 1
    used = _reason_tokens(
        design + PLAN.read_text(encoding="utf-8") + MATRIX.read_text(encoding="utf-8")
    )
    assert used <= set(reasons), sorted(used - set(reasons))


def test_r2f5_report_and_session_observation_models_are_bound() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    report = _section(design, "interface R2FAcceptanceReport {", "interface SnapshotIdentity {")
    dimensions = _roadmap_dimensions()
    assert all(
        re.search(rf"^\s+{re.escape(field)}: MetricResult;", report, re.MULTILINE)
        for field in dimensions
    )
    observation = _section(
        design,
        "interface SessionObservation {",
        "interface CompletedReplicationRestoreSnapshotV1 {",
    )
    for field in (
        "session",
        "same_evening_published_at",
        "next_morning_published_at",
        "required_count",
        "loaded_count",
        "canonical_provider_ids",
        "evidence",
        "pointer_reconciliation",
        "replication_observation",
        "recovery_observation",
        "error_handling_observation",
        "local_nas_isolation_observation",
        "observation_sha256",
    ):
        assert re.search(rf"^\s+{re.escape(field)}:", observation, re.MULTILINE), field
    for nested in (
        "SessionEvidenceBinding",
        "PointerReconciliation",
        "ReplicationObservation",
        "RecoveryObservation",
        "ErrorHandlingObservation",
        "LocalNasIsolationObservation",
    ):
        assert f"interface {nested} " in design
    versions = _section(
        design, "interface FrozenReliabilityVersions {", "interface R2FAcceptanceReport {"
    )
    for field in (
        "git_commit",
        "installed_release",
        "installed_release_sha256",
        "dataset_generation",
        "primary_provider_id",
        "secondary_provider_id",
        "qualification_id",
        "admission_id",
        "adapter_version",
        "endpoint_contract_version",
        "schema_version",
        "reconciliation_policy_version",
        "selection_policy_version",
        "config_digest",
        "auto_failover_enabled",
        "failover_kill_switch",
        "provider_priority",
        "continuity_start_date",
        "repair_policy_version",
        "calendar_generation",
        "universe_generation",
        "replication_policy_version",
        "replication_evidence_version",
        "replication_trust_scope",
        "destination_generation",
        "destination_head_sha256",
        "remote_verification_sha256",
        "restore_policy_version",
        "restore_evidence_version",
    ):
        assert re.search(rf"^\s+{re.escape(field)}:", versions, re.MULTILINE), field
    for field in (
        "requested_start",
        "requested_end",
        "as_of_utc",
        "as_of_timezone",
        "input_fingerprints",
        "frozen_versions",
        "input_fingerprint_sha256",
        "frozen_version_vector_sha256",
        "snapshot_sha256",
    ):
        assert re.search(
            rf"^\s+{re.escape(field)}:",
            _section(design, "interface SnapshotIdentity {", "interface DiagnosticEnvelope {"),
            re.MULTILINE,
        ), field


def test_r2f5_snapshot_roles_replay_and_readers_are_explicit() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    for role in (
        "dataset",
        "evidence",
        "calendar",
        "universe",
        "replication",
        "restore",
        "control",
    ):
        assert f'"{role}"' in design
    assert 'descriptor_state: "present" | "absent"' in design
    assert "OfflineReplayRegistry" in design and "OfflineReplayResolver" in design
    assert "network/socket fake" in design
    assert "EvidenceReader` default path MUST NOT be called directly" in design
    assert "read_status" in design and "read_window" in design
    assert "create=True" in design and "RestoreAuditStore" in design
    assert "lstat" in design and "O_NOFOLLOW" in design
    assert "network_allowed: false" in design


def test_r2f5_matrix_has_one_explicit_row_per_roadmap_slo() -> None:
    dimensions = _roadmap_dimensions()
    rows = _matrix_slo_rows(MATRIX.read_text(encoding="utf-8"))
    assert {row[0] for row in rows} == set(dimensions)
    assert len(rows) == len(dimensions)
    reasons = set(_reason_source(DESIGN.read_text(encoding="utf-8")))
    assert all(
        target.strip() and source.strip() and reason in reasons and acceptance == "AC-15"
        for _field, target, source, reason, acceptance, _anchor in rows
    )
    assert all(SLO_ANCHOR.fullmatch(anchor) for *_rest, anchor in rows)
