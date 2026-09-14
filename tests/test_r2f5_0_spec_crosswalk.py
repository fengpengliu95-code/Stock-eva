"""SPEC-FIRST validator for the R2-F5.0 acceptance-harness documents.

This test validates only documentation/crosswalk invariants. It intentionally does not import
or execute an acceptance service, provider, database writer, restore operation, or production
runtime. Planned anchors are not asserted to exist until the implementation phase.
"""

from __future__ import annotations

import hashlib
import json
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


def _x5_contract(design: str) -> dict[str, object]:
    match = re.search(
        r"<!-- R2F5_X5_CONTRACTS_JSON -->\s*```json\s*(\{.*?\})\s*```",
        design,
        flags=re.DOTALL,
    )
    assert match, "X5 structured contract block missing"
    value = json.loads(match.group(1))
    assert isinstance(value, dict)
    return value


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
    assert "SPEC CANDIDATE / IMPLEMENTATION NOT STARTED / R2-F5.0 NO-GO" in text
    assert "5393f499dbc8b84398658816f7a555dd3e547d47" in text
    assert "affa7153ef088dbd6e7004eeed721588e24656cc" in text
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
                plan, "## Planned pytest anchor catalog (X5)", "## Planned implementation tasks"
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
    assert "affa7153ef088dbd6e7004eeed721588e24656cc" in matrix
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


def test_r2f5_x5_roadmap_source_and_anchor_catalog_have_no_legacy_tokens() -> None:
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
    assert "## Planned pytest anchor catalog (X5)" in plan
    assert "no hand-written dimension count" in design or "fixed expected tuple" in design
    assert "replication" in matrix and "child of local/NAS" in matrix


def test_r2f5_x5_metric_union_and_window_evidence_are_closed() -> None:
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


def test_r2f5_x5_model_source_mapping_and_read_only_restore_contract() -> None:
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


def test_r2f5_x5_time_hash_and_cardinality_contracts_are_explicit() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    assert "YYYY-MM-DD" in design
    assert "RFC3339" in design or "UTC instant" in design
    assert "Asia/Shanghai" in design
    assert "[0-9a-f]{64}" in design
    assert "exactly 20" in design
    assert "exactly six" in design
    assert "max 64" in design and "max 32" in design
    assert "canonical JSON" in design and "hash preimage" in design


def test_r2f5_x5_validator_checks_interface_tokens_and_collection_bounds() -> None:
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


def test_r2f5_x5_structured_contract_is_closed_and_crosswalked() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    contract = _x5_contract(design)
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


def test_r2f5_x5_envelope_raw_facts_and_pre_capture_models_are_explicit() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    envelope = _section(
        design,
        "interface ImmutableObservationEnvelopeV1<T> {",
        "interface SnapshotFingerprint {",
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


def test_r2f5_x5_metric_values_tree_hash_and_window_envelopes_are_bound() -> None:
    design = DESIGN.read_text(encoding="utf-8")
    metric_value = _section(design, "type MetricValue =", "type MetricResult =")
    assert all(
        f'kind: "{kind}"' in metric_value
        for kind in ("count", "ratio", "duration_ms", "bool", "hash")
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
