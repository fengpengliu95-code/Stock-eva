"""SPEC-FIRST validator for the R2-F5.0 acceptance-harness documents.

This test validates only documentation/crosswalk invariants. It intentionally does not import
or execute an acceptance service, provider, database writer, restore operation, or production
runtime. Planned anchors are not asserted to exist until the implementation phase.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sqlite3
from copy import deepcopy
from pathlib import Path

import pytest

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

EXPECTED_ROADMAP_DIMENSIONS = (
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
    "restore",
    "read_boundary",
)


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
    assert tuple(dimensions) == EXPECTED_ROADMAP_DIMENSIONS
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


def _x8_contract(design: str) -> dict[str, object]:
    match = re.search(
        r"<!-- R2F5_X8_CONTRACTS_JSON -->\s*```json\s*(\{.*?\})\s*```",
        design,
        flags=re.DOTALL,
    )
    assert match, "X8 structured contract block missing"
    value = json.loads(match.group(1))
    assert isinstance(value, dict)
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    declared = re.search(
        r"R2F5_X8_CONTRACTS_JSON` canonical block digest .*? is `([0-9a-f]{64})`",
        design,
        flags=re.DOTALL,
    )
    assert declared, "X8 canonical block digest declaration missing"
    assert declared.group(1) == hashlib.sha256(canonical).hexdigest()
    return value


_AUTHORITATIVE_CATALOG_SPECS = {
    "replication_sidecar": (
        ROOT / "backend/app/storage/replication.py",
        "SIDECAR_DDL",
        None,
        1,
    ),
    "daily_shadow": (
        ROOT / "backend/app/market/daily_shadow_schema.py",
        "DAILY_SHADOW_DDL",
        None,
        0,
    ),
    "shadow_registry": (
        ROOT / "backend/app/market/shadow_registry_schema.py",
        "REGISTRY_DDL",
        "MIGRATION_0002_DDL",
        0,
    ),
    "calendar_generation": (
        ROOT / "backend/app/market/calendar_generation.py",
        "CALENDAR_GENERATION_DDL",
        None,
        0,
    ),
    "universe": (
        ROOT / "backend/app/market/universe.py",
        "UNIVERSE_DDL",
        None,
        1,
    ),
}


def _authoritative_string(source: Path, name: str) -> str:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            continue
        value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        if (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "strip"
            and isinstance(value.func.value, ast.Constant)
            and isinstance(value.func.value.value, str)
        ):
            return value.func.value.value.strip()
    raise AssertionError(f"authoritative DDL constant missing: {source}:{name}")


def _authoritative_catalog(role: str) -> dict[str, object]:
    source, ddl_name, extra_name, expected_user_version = _AUTHORITATIVE_CATALOG_SPECS[role]
    ddl = _authoritative_string(source, ddl_name)
    if extra_name:
        ddl += "\n" + _authoritative_string(source, extra_name)
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(ddl)
        objects = connection.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        tables: dict[str, object] = {}
        for (table_name,) in connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ):
            info = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
            tables[table_name] = {
                "columns": [f"{row[1]}:{row[2] or ''}" for row in info],
                "primary_key": [row[1] for row in sorted(info, key=lambda row: row[5]) if row[5]],
            }
        return {
            "user_version": connection.execute("PRAGMA user_version").fetchone()[0],
            "objects": [f"{kind}:{name}" for kind, name in objects],
            "tables": tables,
            "expected_user_version": expected_user_version,
        }
    finally:
        connection.close()


def _model_digest_fields(design: str) -> set[str]:
    """Read digest-typed fields from the TypeScript interfaces, including nested error events."""
    block = _section(design, "type MetricValue =", "### Status and reason vocabulary")
    fields: set[str] = set()
    for match in re.finditer(
        r"interface\s+(\w+(?:<T>)?)\s*\{(.*?)(?=\ninterface\s+|\ntype\s+|\Z)",
        block,
        flags=re.DOTALL,
    ):
        model, body = match.groups()
        model = model.removesuffix("<T>")
        for field in re.findall(r"\b((?:[A-Za-z][A-Za-z0-9_]*(?:sha256|digest))|sha256)\s*:", body):
            qualified = f"{model}.{field}"
            if model == "ErrorHandlingObservation" and field == "evidence_sha256":
                qualified = "ErrorHandlingObservation.events.evidence_sha256"
            fields.add(qualified)
    return fields


def _resolve_schema_path(
    ast: dict[str, object], root: str, path: str
) -> tuple[str, str, str] | None:
    """Resolve a path to its root, full path and object/tuple-item schema kind.

    The path is deliberately resolved from the named root and its nested tuple
    schema; no digest-name suffix lookup is involved here.  Digest target
    qualification is performed separately from the returned source path.
    """
    node = ast.get(root)
    if not isinstance(node, dict):
        return None
    fields = set(node.get("object_fields", []))
    tuple_items = node.get("tuple_item_schemas", {})
    if not isinstance(tuple_items, dict):
        return None
    schema_kind = "object_field"
    segments = path.split(".")
    for index, segment in enumerate(segments):
        is_tuple = segment.endswith("[]")
        name = segment[:-2] if is_tuple else segment
        if name not in fields:
            return None
        if is_tuple:
            items = tuple_items.get(name)
            if not isinstance(items, list):
                return None
            fields = set(items)
            schema_kind = "tuple_item"
        elif index < len(path.split(".")) - 1:
            nested = ast.get(name)
            if not isinstance(nested, dict):
                return None
            fields = set(nested.get("object_fields", []))
            schema_kind = "object_field"
    return root, path, schema_kind


def _schema_path_exists(ast: dict[str, object], root: str, path: str) -> bool:
    return _resolve_schema_path(ast, root, path) is not None


def _resolve_digest_target(
    contract: dict[str, object], root: str, path: str
) -> tuple[str, str, str, str | None, tuple[str, str] | None] | None:
    """Resolve source schema path and its fully-qualified digest target.

    The target lookup uses the complete source-model path, never a digest
    suffix.  A path with no local contract is an explicitly opaque external
    leaf and returns ``target_node=None``.
    """
    ast = contract["model_schema_ast"]
    entries = contract["digest_contracts"]
    resolved = _resolve_schema_path(ast, root, path)
    if resolved is None:
        return None
    qualified_path = f"{root}.{path.replace('[]', '')}"
    matches = sorted(
        (entry for entry in entries if entry["field"] == qualified_path),
        key=lambda entry: (entry["root_object_type"], entry["field"]),
    )
    if not matches:
        return (*resolved, None, None)
    target = matches[0]
    target_node = (target["root_object_type"], target["field"])
    return (*resolved, target["root_object_type"], target_node)


def _digest_dependency_graph(
    contract: dict[str, object],
) -> dict[tuple[str, str], list[tuple[str, str]]]:
    ast = contract["model_schema_ast"]
    entries = contract["digest_contracts"]
    assert isinstance(ast, dict) and isinstance(entries, list)
    nodes = {(entry["root_object_type"], entry["field"]): entry for entry in entries}
    # ``field`` is already the source model's fully-qualified digest path
    # (for example ``SessionObservation.observation_sha256``).  Keep the
    # qualified path as the lookup key and retain the complete node tuple as
    # the value.  Never unpack a node into a short model-name alias: two roots
    # can legitimately expose the same digest suffix.
    graph: dict[tuple[str, str], list[tuple[str, str]]] = {node: [] for node in nodes}
    for node in sorted(nodes):
        entry = nodes[node]
        root = entry["root_object_type"]
        for path in sorted(entry["included_field_paths"]):
            resolved = _resolve_digest_target(contract, root, path)
            assert resolved is not None, f"missing digest path {root}.{path}"
            target = resolved[-1]
            if target is not None:
                if target == node:
                    raise AssertionError(f"digest dependency self-edge at {node}")
                graph[node].append(target)
        graph[node].sort()
    visiting: set[tuple[str, str]] = set()
    visited: set[tuple[str, str]] = set()

    def visit(node: tuple[str, str]) -> None:
        assert node not in visiting, f"digest dependency cycle at {node}"
        if node in visited:
            return
        visiting.add(node)
        for dependency in graph[node]:
            visit(dependency)
        visiting.remove(node)
        visited.add(node)

    for node in sorted(graph):
        visit(node)
    return graph


def _validate_digest_contract_paths(contract: dict[str, object]) -> None:
    ast = contract["model_schema_ast"]
    entries = contract["digest_contracts"]
    assert isinstance(ast, dict) and isinstance(entries, list)
    fields = {entry["field"] for entry in entries}
    assert len(fields) == len(entries)
    for entry in entries:
        root = entry["root_object_type"]
        assert root in ast, root
        for path in entry["included_field_paths"]:
            assert _schema_path_exists(ast, root, path), f"missing digest path {root}.{path}"
        field_name = entry["field"].rsplit(".", 1)[-1]
        assert field_name in entry["excluded_fields"]
        assert all("?" not in item for item in entry["excluded_fields"])
    graph = _digest_dependency_graph(contract)
    assert all(isinstance(node, tuple) and len(node) == 2 for node in graph)


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
    rows: list[tuple[str, str, str, str, str, str]] = []
    for line in section.splitlines():
        match = re.match(
            r"^\| `([a-z_]+)`(?: \*\(child of local/NAS\)\*)? \| (.*?) \| (.*?) \| "
            r"(.*?) \| (.*?) \| (.*?) \|$",
            line,
        )
        if match:
            rows.append(match.groups())
    return rows


def _matrix_slo_rows(text: str) -> list[tuple[str, str, str, str, str, str]]:
    section = _section(text, "## Mandatory SLO crosswalk", "## Crosswalk interpretation")
    rows: list[tuple[str, str, str, str, str, str]] = []
    for line in section.splitlines():
        match = re.match(
            r"^\| `([a-z_]+)`(?: \*\(child of local/NAS\)\*)? \| (.*?) \| (.*?) \| "
            r"`([A-Z0-9_]+)` \| (AC-\d+) \| "
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
    assert "SPEC APPROVED - AMENDMENT CANDIDATE / IMPLEMENTATION PAUSED / R2-F5.0 NO-GO" in text
    assert "5393f499dbc8b84398658816f7a555dd3e547d47" in text
    assert "a7d3be1c6b9f760c659470fffcf6299bcd8ddf73" in text
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
                plan, "## Planned pytest anchor catalog (X8)", "## Planned implementation tasks"
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
    assert "a7d3be1c6b9f760c659470fffcf6299bcd8ddf73" in matrix
    assert "5393f499dbc8b84398658816f7a555dd3e547d47" in matrix
    assert "No catalog entry exists or passes yet" in PLAN.read_text(encoding="utf-8")
    assert "Task 20" in matrix and "production soak" in matrix
    assert "IMPLEMENTATION PAUSED" in PLAN.read_text(encoding="utf-8")
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
    assert set(row[0] for row in _slo_rows(DESIGN.read_text(encoding="utf-8"))) == {
        *dimensions,
        "replication",
    }
    assert set(row[0] for row in _matrix_slo_rows(MATRIX.read_text(encoding="utf-8"))) == {
        *dimensions,
        "replication",
    }
    assert "LOCAL_CHAIN_ONLY" in design
    assert "REMOTE_VERIFIED" in design
    assert "create=True" in design
    assert "network_allowed: false" in design
    metric = _section(design, "type MetricResult =", "// This is the only reason-code source")
    assert 'status: "pass"' in metric and 'status: "fail"' in metric
    assert 'status: "unavailable"' in metric
    assert "reason_code: null" in metric and "FailureReasonCode" in metric


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
    replication = _section(
        design, "interface ReplicationObservation {", "interface RecoveryObservation {"
    )
    reason_field = _section(replication, "reason_code:", "observed_at:")
    explicit_replication_reasons = set(re.findall(r'"([A-Z][A-Z0-9_]+)"', reason_field))
    assert explicit_replication_reasons <= set(reasons)


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
        "type WindowEvidenceBundle =",
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
    assert "type WindowEvidenceBundle =" in design
    assert "window_evidence_bundle" in report
    assert not re.search(
        r"^\s+(recovery_observation|error_handling_observation|local_nas_isolation_observation):",
        observation,
        re.MULTILINE,
    )
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
        "qualification_window_id",
        "qualification_proof_status",
        "adapter_hash",
        "endpoint_contract_hash",
        "source_schema_hash",
        "normalizer_hash",
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
        "remote_proof_artifact_ref",
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
    assert {row[0] for row in rows} == {*dimensions, "replication"}
    assert len(rows) == len(dimensions) + 1
    reasons = set(_reason_source(DESIGN.read_text(encoding="utf-8")))
    assert all(
        target.strip() and source.strip() and reason in reasons and acceptance == "AC-15"
        for _field, target, source, reason, acceptance, _anchor in rows
    )
    assert all(SLO_ANCHOR.fullmatch(anchor) for *_rest, anchor in rows)
    design_rows = _slo_rows(DESIGN.read_text(encoding="utf-8"))
    assert {row[0] for row in design_rows} == {row[0] for row in rows}
    assert len({row[-1] for row in rows}) == 18
    assert all(row[2].strip() and ";" in row[2] for row in design_rows)


def test_r2f5_x8_roadmap_source_and_anchor_catalog_have_no_legacy_tokens() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    plan = PLAN.read_text(encoding="utf-8")
    matrix = MATRIX.read_text(encoding="utf-8")
    req_anchors = {_planned_anchors(row[3])[0] for row in _matrix_rows()}
    slo_anchors = {row[-1] for row in _matrix_slo_rows(matrix)}
    allowed = req_anchors | slo_anchors
    body_tokens = {
        match.group(0)
        for match in re.finditer(r"test_r2f5_[A-Za-z0-9_]+", design + plan)
        if (design + plan)[match.end() : match.end() + 3] != ".py"
    }
    assert body_tokens <= allowed, sorted(body_tokens - allowed)
    assert "## Planned pytest anchor catalog (X8)" in plan
    assert "no hand-written dimension count" in design or "fixed expected tuple" in design
    assert "replication" in matrix and "child of local/NAS" in matrix


def test_r2f5_x8_metric_union_and_window_evidence_are_closed() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    assert "type MetricResult =" in design
    assert 'status: "pass"' in design and "reason_code: null" in design
    assert 'status: "fail"' in design and "FailureReasonCode" in design
    assert 'status: "unavailable"' in design and "UnavailableReasonCode" in design
    assert "type WindowEvidenceBundle =" in design
    assert "observation_count: 1" in design
    assert "ErrorHandlingObservation.events` is an exact six-element tuple" in design
    assert "nas_failure_did_not_block_local=true" in design
    assert "publication_count=1" in design
    assert "Overall status precedence" in design


def test_r2f5_x8_model_source_mapping_and_read_only_restore_contract() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    assert "provider_record.provider_id" in design
    assert "qualification_window.window_id" in design
    for forbidden in ("capability_sha256", "qualification_session_digest", "admission_sha256"):
        assert forbidden not in design
    assert "VerifiedArchiveSnapshot" in design
    assert "RestoreReport" in design and "RestoreAuditEvent" in design
    assert "FrozenR2F4PolicyThresholds" in design
    assert "reviewed-r2f4-policy-evidence" in design
    assert "MUST NOT hash current" in design
    assert "streaming SHA-256" in design
    assert "maximum" in design and "unavailable" in design


def test_r2f5_x8_time_hash_and_cardinality_contracts_are_explicit() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    assert "YYYY-MM-DD" in design
    assert "RFC3339" in design or "UTC instant" in design
    assert "Asia/Shanghai" in design
    assert "[0-9a-f]{64}" in design
    assert "exactly 20" in design
    assert "exactly six" in design
    assert "max 64" in design and "max 32" in design
    assert "canonical JSON" in design and "hash preimage" in design


def test_r2f5_x8_validator_checks_interface_tokens_and_collection_bounds() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    fingerprint = _section(design, "interface SnapshotFingerprint {", "// All fields")
    for field in (
        "descriptor_role",
        "descriptor_id",
        "descriptor_state",
        "fingerprint_kind",
        "hash_scope",
        "sha256",
    ):
        assert re.search(rf"^\s+{field}:", fingerprint, re.MULTILINE), field
    captured = _section(design, "interface CapturedSnapshot {", "All identifiers")
    for field in (
        "snapshot_identity",
        "raw_calendar_observations",
        "confirmed_sessions",
        "input_descriptors",
        "frozen_versions",
        "session_observations",
        "window_evidence_bundle",
        "captured_at_utc",
    ):
        assert re.search(rf"^\s+{field}:", captured, re.MULTILINE), field
    error = _section(
        design, "interface ErrorHandlingObservation {", "interface LocalNasIsolationObservation {"
    )
    assert all(
        f'forced_error_class: "{name}"' in error
        for name in ("timeout", "auth", "rate", "schema", "coverage", "storage")
    )
    assert "exactly six records" in design and "unique" in design
    assert "len(selected_sessions)=len(session_observations)=20" in design
    assert "window_evidence_refs" in design and "exactly one" in design


def test_r2f5_x8_structured_contract_is_closed_and_crosswalked() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    contract = _x8_contract(design)
    canonical_contract = json.dumps(
        contract, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    assert hashlib.sha256(canonical_contract).hexdigest() in design
    dimensions = list(_roadmap_dimensions())
    metric_fields = contract["metric_fields"]
    assert contract["roadmap_dimensions"] == dimensions
    assert metric_fields == [*dimensions[:15], "replication", *dimensions[15:]]
    partitions = contract["reason_partitions"]
    assert isinstance(partitions, dict)
    failure = partitions["failure"]
    unavailable = partitions["unavailable"]
    assert isinstance(failure, list) and isinstance(unavailable, list)
    assert not set(failure) & set(unavailable)
    assert set(failure) | set(unavailable) == set(_reason_source(design))
    assert "LINEAGE_UNAVAILABLE" in unavailable
    assert "LINEAGE_INVALID" in failure
    assert contract["status_reason_matrix"] == {
        "pass": [None],
        "fail": ["failure"],
        "unavailable": ["unavailable"],
    }
    reducers = contract["metric_reducers"]
    assert isinstance(reducers, dict)
    assert set(reducers) == set(metric_fields)
    assert all(isinstance(fields, list) and fields for fields in reducers.values())
    assert contract["artifact_envelope_fields"] == [
        "artifact_id",
        "artifact_ref",
        "schema_version",
        "creator_kind",
        "creator_version",
        "created_at",
        "payload",
        "canonicalization_version",
        "payload_sha256",
        "envelope_sha256",
    ]
    assert contract["cardinality"] == {
        "sessions": 20,
        "window_bundle": 1,
        "error_classes": 6,
        "input_roles_max": 32,
        "tree_entries_max": 100000,
        "input_bytes_max": 536870912,
    }


def test_r2f5_x8_digest_contracts_cover_models_without_self_inclusion() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    contract = _x8_contract(design)
    entries = contract["digest_contracts"]
    assert isinstance(entries, list) and entries
    fields = [entry["field"] for entry in entries]
    assert len(fields) == len(set(fields))
    assert set(fields) == _model_digest_fields(design)
    digest_fields = contract["digest_fields_by_model"]
    expected_digest_fields: dict[str, list[str]] = {}
    for field in fields:
        model, name = field.rsplit(".", 1)
        expected_digest_fields.setdefault(model, []).append(name)
    assert digest_fields == expected_digest_fields
    ast = contract["model_schema_ast"]
    for model, names in digest_fields.items():
        if model.endswith(".events"):
            parent, tuple_name = model.rsplit(".", 1)
            assert tuple_name in ast[parent]["tuple_item_schemas"]
            assert set(names) <= set(ast[parent]["tuple_item_schemas"][tuple_name])
        else:
            assert model in ast
            assert names and len(names) == len(set(names))
    _validate_digest_contract_paths(contract)
    required_keys = {
        "field",
        "canonicalization_version",
        "root_object_type",
        "included_field_paths",
        "excluded_fields",
        "ordering",
        "null_encoding",
        "domain_separation_prefix",
    }
    for entry in entries:
        assert set(entry) == required_keys
        field_name = entry["field"].rsplit(".", 1)[-1]
        assert field_name not in entry["included_field_paths"]
        assert field_name in " ".join(entry["excluded_fields"])
        assert not any(
            token in entry["included_field_paths"]
            for token in (
                "complete_object",
                "complete_frozen_version_vector",
                "all_declared_session_fields",
                "metric_results",
                "validated_config_fields",
            )
        )
        assert entry["canonicalization_version"] == "project-canonical-json-v1"
        assert entry["domain_separation_prefix"].startswith("r2f5/")
        assert entry["included_field_paths"]
        assert entry["ordering"] and entry["null_encoding"]


def test_r2f5_x8_digest_path_validator_rejects_bad_nesting_and_cycles() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    contract = _x8_contract(design)
    broken_path = deepcopy(contract)
    broken_path["digest_contracts"][0]["included_field_paths"] = ["payload.no_such_field"]
    with pytest.raises(AssertionError, match="missing digest path"):
        _validate_digest_contract_paths(broken_path)

    broken_tuple = deepcopy(contract)
    error_entry = next(
        item
        for item in broken_tuple["digest_contracts"]
        if item["field"] == "ErrorHandlingObservation.observation_sha256"
    )
    error_entry["included_field_paths"] = ["events.event_id"]
    with pytest.raises(AssertionError, match="missing digest path"):
        _validate_digest_contract_paths(broken_tuple)

    cyclic = {
        "model_schema_ast": {
            "A": {"object_fields": ["a", "b"], "tuple_item_schemas": {}},
            "B": {"object_fields": ["a", "b"], "tuple_item_schemas": {}},
        },
        "digest_contracts": [
            {
                "field": "A.a",
                "root_object_type": "A",
                "included_field_paths": ["b"],
                "excluded_fields": ["a"],
                "canonicalization_version": "project-canonical-json-v1",
                "ordering": "sorted",
                "null_encoding": "JSON null",
                "domain_separation_prefix": "r2f5/a-v1\\0",
            },
            {
                "field": "A.b",
                "root_object_type": "A",
                "included_field_paths": ["a"],
                "excluded_fields": ["b"],
                "canonicalization_version": "project-canonical-json-v1",
                "ordering": "sorted",
                "null_encoding": "JSON null",
                "domain_separation_prefix": "r2f5/b-v1\\0",
            },
        ],
    }
    with pytest.raises(AssertionError, match="digest dependency cycle"):
        _validate_digest_contract_paths(cyclic)

    self_edge = deepcopy(cyclic)
    self_edge["digest_contracts"][0]["included_field_paths"] = ["a"]
    with pytest.raises(AssertionError, match="self-edge"):
        _validate_digest_contract_paths(self_edge)

    qualified = deepcopy(cyclic)
    qualified["model_schema_ast"] = {
        "A": {
            "object_fields": ["a", "observation_sha256", "manifest_sha256"],
            "tuple_item_schemas": {},
        },
        "B": {
            "object_fields": ["a", "observation_sha256", "manifest_sha256"],
            "tuple_item_schemas": {},
        },
    }
    qualified["digest_contracts"] = [
        {
            **qualified["digest_contracts"][0],
            "field": "A.observation_sha256",
            "included_field_paths": ["manifest_sha256"],
            "excluded_fields": ["observation_sha256"],
        },
        {
            **qualified["digest_contracts"][1],
            "field": "B.observation_sha256",
            "root_object_type": "B",
            "included_field_paths": ["manifest_sha256"],
            "excluded_fields": ["observation_sha256"],
        },
        {
            **qualified["digest_contracts"][0],
            "field": "A.manifest_sha256",
            "included_field_paths": ["a"],
            "excluded_fields": ["manifest_sha256"],
        },
        {
            **qualified["digest_contracts"][1],
            "field": "B.manifest_sha256",
            "root_object_type": "B",
            "included_field_paths": ["a"],
            "excluded_fields": ["manifest_sha256"],
        },
    ]
    graph = _digest_dependency_graph(qualified)
    assert ("A", "A.observation_sha256") in graph
    assert ("B", "B.observation_sha256") in graph
    assert graph[("A", "A.observation_sha256")] == [("A", "A.manifest_sha256")]
    assert graph[("B", "B.observation_sha256")] == [("B", "B.manifest_sha256")]

    real_cycle = deepcopy(contract)
    schema_digest = next(
        item
        for item in real_cycle["digest_contracts"]
        if item["field"] == "SessionObservation.schema_policy_digest"
    )
    schema_digest["included_field_paths"] = ["observation_sha256"]
    with pytest.raises(AssertionError, match="digest dependency cycle"):
        _validate_digest_contract_paths(real_cycle)


def test_r2f5_x8_date_metric_creator_and_limits_contracts_match_models() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    contract = _x8_contract(design)
    assert set(contract["date_time_formats"]) == {"session_date", "rfc3339_utc", "as_of"}
    assert "YYYY-MM-DD" in contract["date_time_formats"]["session_date"]
    assert "RFC3339" in contract["date_time_formats"]["rfc3339_utc"]
    assert "Asia/Shanghai" in contract["date_time_formats"]["as_of"]
    metric_kinds = contract["metric_value_kinds"]
    assert set(metric_kinds) == set(contract["metric_fields"])
    for field, spec in metric_kinds.items():
        assert spec["observed"] in {"count", "ratio", "duration_seconds", "bool", "hash"}, field
        assert spec["target"] == spec["observed"], field
        assert spec["unavailable_null"] is True
        assert spec["range"]
    creators = contract["creator_allowlist"]
    assert creators["production_creator_kind"] == "task20_writer"
    assert creators["test_envelope"] == {
        "schema_version": "r2f5-test-envelope-v1",
        "creator_kind": "test_fixture",
        "production_reader_accepts": False,
    }
    assert set(creators["production_envelope_payloads"]) == {
        "WindowEvidenceBundlePayload",
        "RecoveryObservation",
        "WholeSessionFailoverDrill",
        "ReplaySampleEvidence",
        "AdjustmentEquivalenceEvidence",
        "ErrorHandlingObservation",
        "LocalNasIsolationObservation",
        "RestoreDrillEvidence",
    }
    assert all(
        value == ["task20_writer"] for value in creators["production_envelope_payloads"].values()
    )
    assert creators["reader_projections"] == {
        "SecondaryQualificationProjection": [],
        "CompletedReplicationRestoreSnapshotV1": [],
    }
    assert creators["synthetic_envelope_only"] == ["test_fixture"]
    assert creators["reader_never_creates"] == ["r2f5_reader", "r2f4_writer"]
    assert 'type ProductionCreatorKind = "task20_writer";' in design
    test_envelope = _section(design, "type TestEnvelope<T> =", "// JCS-compatible project JSON")
    assert 'creator_kind: "test_fixture";' in test_envelope
    assert "MUST be rejected by a" in design and "production reader" in design
    assert contract["limits"] == {
        "max_entries": 100000,
        "max_input_bytes": 536870912,
        "max_db_rows": 1000000,
        "max_input_roots": 32,
        "max_sessions": 20,
        "max_replay_samples": 3,
        "max_elapsed_ms": 10000,
    }

    catalogs = contract["sqlite_catalogs"]
    assert set(catalogs) == {
        "replication_sidecar",
        "daily_shadow",
        "shadow_registry",
        "calendar_generation",
        "universe",
    }
    expected_roles = {
        "replication_sidecar": "replication",
        "daily_shadow": "qualification",
        "shadow_registry": "qualification",
        "calendar_generation": "calendar",
        "universe": "universe",
    }
    expected_versions = {"daily_shadow": 0, "shadow_registry": 0, "calendar_generation": 0}
    for role, catalog in catalogs.items():
        assert catalog["role"] == expected_roles[role]
        assert catalog["user_version"] == expected_versions.get(role, 1)
        authoritative = _authoritative_catalog(role)
        assert catalog["user_version"] == authoritative["expected_user_version"]
        assert authoritative["user_version"] == catalog["user_version"]
        assert catalog["sqlite_master_allowlist"] == authoritative["objects"]
        assert catalog["system_tables"] == []
        tables = catalog["tables"]
        assert catalog["allowed_tables"] == sorted(authoritative["tables"])
        assert set(tables) == set(authoritative["tables"])
        for table_name, schema in tables.items():
            assert table_name in catalog["allowed_tables"]
            assert schema["columns"] and all(":" in column for column in schema["columns"])
            assert schema["primary_key"] and schema["order_by"]
            assert schema["columns"] == authoritative["tables"][table_name]["columns"]
            assert schema["primary_key"] == authoritative["tables"][table_name]["primary_key"]
            declared_names = {column.split(":", 1)[0] for column in schema["columns"]}
            assert all(column in declared_names for column in schema["order_by"])
        # L1: source identity is part of the frozen catalog contract.  Prefix
        # checks accepted a different module, DDL constant, or migration suffix
        # while still appearing valid; compare the complete declared identity.
        catalog_sources = {
            "replication_sidecar": "backend/app/storage/replication:SIDECAR_DDL",
            "daily_shadow": (
                "backend/app/market/daily_shadow_schema:DAILY_SHADOW_DDL+MIGRATION_ID"
            ),
            "shadow_registry": (
                "backend/app/market/shadow_registry_schema:REGISTRY_DDL+MIGRATION_SQL"
            ),
            "calendar_generation": (
                "backend/app/market/calendar_generation:CALENDAR_GENERATION_DDL"
            ),
            "universe": "backend/app/market/universe:UNIVERSE_DDL",
        }
        source_identity = catalog_sources[role]
        assert catalog["schema_version_source"] == (
            f"PRAGMA user_version={catalog['user_version']}; exact sqlite_master; "
            + source_identity
        )
        assert catalog["catalog_digest_source"] == source_identity


def test_r2f5_x8_crosswalk_reads_plan_matrix_and_report_interfaces() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    plan = PLAN.read_text(encoding="utf-8")
    matrix = MATRIX.read_text(encoding="utf-8")
    contract = _x8_contract(design)
    metric_fields = set(contract["metric_fields"])
    report = _section(design, "interface R2FAcceptanceReport {", "interface SnapshotIdentity {")
    report_fields = set(re.findall(r"^\s+([a-z_]+): MetricResult;", report, re.MULTILINE))
    assert report_fields == metric_fields
    assert metric_fields <= set(_field for _field, *_rest in _slo_rows(design))
    assert metric_fields <= set(_field for _field, *_rest in _matrix_slo_rows(matrix))
    assert all(field in plan for field in metric_fields)
    reducers = contract["metric_reducers"]
    for field, paths in reducers.items():
        assert paths and all(path.count(".") >= 1 for path in paths), field
        assert all(
            path.split(".", 1)[0] in {"session", "window", "frozen_versions"} for path in paths
        ), field
    creators = contract["creator_allowlist"]["production_envelope_payloads"]
    for payload_type in creators:
        assert (
            payload_type == "WindowEvidenceBundlePayload" or f"interface {payload_type} " in design
        )
    assert "r2f5_reader" in design and "MUST NOT appear as envelope creators" in design


def test_r2f5_sqlite_zero_write_amendment_forbids_direct_input_open() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    plan = PLAN.read_text(encoding="utf-8")
    contract = _x8_contract(design)
    capture = contract["sqlite_capture"]
    assert capture == {
        "algorithm": "descriptor-copy-two-fingerprint-v1",
        "input_members": ["db", "-wal", "-shm"],
        "member_discovery": "validated_parent_dirfd_only",
        "open_flags": ["O_RDONLY", "O_NOFOLLOW", "O_CLOEXEC"],
        "source_write_policy": "zero_write",
        "stability_protocol": "read_lock_or_consistent_capture_protocol",
        "copy_strategy": "full_bytes_to_private_mkdtemp_outside_input_roots",
        "copy_verification": [
            "fstat_before",
            "full_sha256",
            "size",
            "mtime_ns",
            "inode",
            "fstat_after",
        ],
        "source_fingerprint_rounds": 2,
        "max_attempts": 1,
        "wal_policy": "temp_trio_wal_must_be_applied_before_logical_read",
        "temp_sqlite_policy": "normal_or_wal_aware_temp_connection_only",
        "direct_input_sqlite_open": False,
        "input_wal_or_shm_change": "SNAPSHOT_CHANGED",
        "locked_or_unstable": "unavailable",
        "cleanup": "always_close_delete_temp_on_success_failure_exception",
        "semantic_fingerprint": "logical_snapshot_digest_plus_source_member_descriptors",
    }
    assert "mode=ro&immutable=false" not in design
    assert "SQLite input files MUST never be opened directly by SQLite" in design
    for phrase in ("fstat", "full-stream SHA-256", "writer during copy", "captured old snapshot"):
        assert phrase in design
    for text in (design, plan):
        assert "private `mkdtemp`" in text or "private temporary" in text
        assert "DB/WAL/SHM" in text
        assert "WAL" in text and "temp-trio" in text
        assert (
            "close/delete" in text
            or "closed and deleted" in text
            or "unconditional temporary cleanup" in text
        )
        assert "max_attempts=1" in text or "one attempt" in text
    assert "writer after" in design


def test_r2f5_x8_model_ast_roots_and_tuple_items_are_explicit() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    contract = _x8_contract(design)
    ast = contract["model_schema_ast"]
    assert ast and all(
        isinstance(node.get("object_fields"), list)
        and isinstance(node.get("tuple_item_schemas"), dict)
        for node in ast.values()
    )
    for root in {entry["root_object_type"] for entry in contract["digest_contracts"]}:
        assert root in ast
    assert ast["ErrorHandlingObservation"]["tuple_item_schemas"]["events"] == [
        "event_id",
        "forced_error_class",
        "sanitized_reason",
        "normalized_result",
        "attempt_id",
        "expected_class",
        "observed_class",
        "evidence_sha256",
        "observed_at",
    ]
    assert "schema_policy_versions" in ast["SessionObservation"]["object_fields"]
    assert "duplicate_proof_sha256" in ast["RecoveryObservation"]["object_fields"]


def test_r2f5_x8_envelope_raw_facts_and_pre_capture_models_are_explicit() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    envelope = _section(
        design,
        "interface ImmutableObservationEnvelopeV1<T> {",
        "type TestEnvelope<T> =",
    )
    assert re.findall(r"^\s+([a-z0-9_]+):", envelope, re.MULTILINE) == [
        "artifact_id",
        "artifact_ref",
        "schema_version",
        "creator_kind",
        "creator_version",
        "created_at",
        "payload",
        "canonicalization_version",
        "payload_sha256",
        "envelope_sha256",
    ]
    observation = _section(design, "interface SessionObservation {", "type WindowEvidenceBundle =")
    for field in (
        "frozen_versions_sha256",
        "calendar_raw_facts",
        "read_boundary_raw_facts",
        "schema_policy_digest",
    ):
        assert re.search(rf"^\s+{field}:", observation, re.MULTILINE)
    for field in (
        "source_sequence",
        "generation",
        "confirmed",
        "unknown_state",
        "conflict_state",
        "raw_facts_sha256",
    ):
        assert re.search(
            rf"^\s+{field}:",
            _section(design, "interface CalendarRawFacts {", "interface ReadBoundaryRawFacts {"),
            re.MULTILINE,
        )
    boundary = _section(
        design, "interface ReadBoundaryRawFacts {", "interface SessionEvidenceBinding {"
    )
    for field in (
        "requested_as_of",
        "max_visible_session",
        "future_rows_seen",
        "future_rows_count",
        "query_count",
        "write_count",
        "probe_schema_digest",
    ):
        assert re.search(rf"^\s+{field}:", boundary, re.MULTILINE)
    pre_capture = _section(
        design, "interface PreCaptureFailurePayloadV1 {", "interface CalendarRawFacts {"
    )
    for field in (
        "reason_code",
        "requested_start",
        "requested_end",
        "as_of_utc",
        "descriptor_states",
        "semantic_report_sha256",
    ):
        assert re.search(rf"^\s+{field}:", pre_capture, re.MULTILINE)
    assert "canonical payload hash excludes only its hash field" in design
    assert "Task20 writers create these bytes" in design
    assert "Task19 only opens immutable refs" in design


def test_r2f5_x8_metric_values_tree_hash_and_window_envelopes_are_bound() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    metric_value = _section(design, "type MetricValue =", "type MetricResult =")
    assert all(
        f'kind: "{kind}"' in metric_value
        for kind in ("count", "ratio", "duration_seconds", "bool", "hash")
    )
    local_nas = _section(
        design, "interface LocalNasIsolationObservation {", "interface SessionObservation {"
    )
    for field in (
        "lag_seconds",
        "lag_threshold_seconds",
        "retryable",
        "retry_state",
        "backlog_before_ids",
        "backlog_after_ids",
        "backlog_before_count",
        "backlog_after_count",
    ):
        assert re.search(rf"^\s+{field}:", local_nas, re.MULTILINE)
    assert "deterministic tree hash" in design
    assert "full streaming content SHA" in design
    assert "hardlink ambiguity" in design
    assert "SQLite" in design and "full-file bytes" in design
    bundle = _section(
        design, "interface WindowEvidenceBundlePayload {", "interface WholeSessionFailoverDrill {"
    )
    assert bundle.count("ImmutableObservationEnvelopeV1<") == 7
    assert "payloads MUST NOT repeat an ambiguous artifact hash" in design
