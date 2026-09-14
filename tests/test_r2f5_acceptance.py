"""RED acceptance contract for R2-F5.0 Task 19.

The fixtures in this module are deliberately local and synthetic. They describe the
read-only input boundary and expected observations, but do not create a provider, NAS,
LaunchAgent, production store, or Task 20 envelope. At the SPEC-APPROVED base the
acceptance reader is absent; each test therefore fails with an explicit RED diagnostic
after collecting successfully. Once the reader exists, these assertions become the
concrete contract checks for the fixture mutation.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import json
import os
import re
import socket
import sqlite3
import stat
import sys
import time
import types
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).parents[1]
DESIGN = ROOT / "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md"
MATRIX = ROOT / "docs/acceptance/r2f5-0-requirement-evidence-matrix.md"
INPUT_KEYS = {
    "start",
    "end",
    "local_dataset_root",
    "evidence_root",
    "control_store_roots",
    "now",
}
REQUIREMENT_ANCHORS = tuple(
    re.findall(r"PLANNED::(test_r2f5_req_[a-z]+_\d{2})", MATRIX.read_text(encoding="utf-8"))
)
REQUIREMENT_SUMMARIES = {
    anchor: summary
    for requirement, summary, anchor in re.findall(
        r"^\| ((?:FR|NFR|AC|EC)-\d+) \| (.*?) \|.*?\| `PLANNED::(test_r2f5_req_[a-z]+_\d{2})`",
        MATRIX.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
}
EXPECTED_METRICS = (
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
    "read_boundary",
)
METRIC_CONTRACTS = {
    "continuity": ("count", 0, 20),
    "next_morning_availability": ("ratio", 1, 1),
    "same_evening_availability": ("ratio", 0.9, 0.9),
    "coverage": ("ratio", 1, 1),
    "canonical_integrity": ("bool", True, True),
    "source_purity": ("bool", True, True),
    "recovery": ("bool", True, True),
    "failover": ("bool", True, True),
    "provenance": ("bool", True, True),
    "replay": ("bool", True, True),
    "adjustment": ("bool", True, True),
    "calendar": ("bool", True, True),
    "universe": ("bool", True, True),
    "error_handling": ("bool", True, True),
    "local_nas_isolation": ("bool", True, True),
    "replication": ("duration_seconds", 0, 300),
    "restore": ("bool", True, True),
    "read_boundary": ("bool", True, True),
}
REPORT_CORE_FIELDS = {
    "status",
    "window_start",
    "window_end",
    "selected_sessions",
    "frozen_versions",
    "quality_issues",
    "snapshot_identity",
    "session_observations",
    "observation_refs",
    "window_evidence_bundle",
    "window_evidence_refs",
    "pre_capture_failure",
    "semantic_report_sha256",
    "provider_requests",
    "writes",
    "restore_started",
    "production_window_started",
}
SLO_ANCHORS = tuple(f"test_r2f5_slo_{metric}" for metric in EXPECTED_METRICS)
TARGET_MODULE = "backend.app.market.reliability_acceptance"
REASONS = {
    "SESSION_COUNT_NOT_20",
    "SESSION_SEQUENCE_INVALID",
    "PIT_VISIBILITY_INVALID",
    "CALENDAR_UNAVAILABLE",
    "CALENDAR_CONFLICT",
    "UNIVERSE_COUNT_MISMATCH",
    "VERSION_DRIFT",
    "LINEAGE_UNAVAILABLE",
    "REPLAY_UNAVAILABLE",
    "REPLAY_SEMANTIC_MISMATCH",
    "INPUT_LIMIT_EXCEEDED",
    "SNAPSHOT_CHANGED",
    "CONTROL_STATE_UNAVAILABLE",
    "PATH_INVALID",
}


def _tree_digest(root: Path) -> str:
    entries: list[tuple[str, str]] = []
    for path in sorted(root.rglob("*")):
        if path.is_file():
            entries.append(
                (path.relative_to(root).as_posix(), hashlib.sha256(path.read_bytes()).hexdigest())
            )
    return hashlib.sha256(json.dumps(entries, separators=(",", ":")).encode()).hexdigest()


def _physical_fingerprint(root: Path) -> tuple[tuple[str, int, int, int, int, str | None], ...]:
    """Capture descriptor metadata and complete bytes without following symlinks."""

    rows: list[tuple[str, int, int, int, int, str | None]] = []
    for path in sorted(root.rglob("*")):
        info = os.lstat(path)
        digest = (
            hashlib.sha256(path.read_bytes()).hexdigest() if stat.S_ISREG(info.st_mode) else None
        )
        rows.append(
            (
                path.relative_to(root).as_posix(),
                info.st_mode,
                info.st_ino,
                info.st_size,
                info.st_mtime_ns,
                digest,
            )
        )
    return tuple(rows)


def _authoritative_ddl(source: Path, name: str) -> str:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                return node.value.value
            if (
                isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr == "strip"
                and isinstance(node.value.func.value, ast.Constant)
            ):
                return str(node.value.func.value.value).strip()
    raise AssertionError(f"missing authoritative DDL {source}:{name}")


CATALOG_DDL = {
    "replication_sidecar": (
        ROOT / "backend/app/storage/replication.py",
        "SIDECAR_DDL",
        None,
    ),
    "daily_shadow": (
        ROOT / "backend/app/market/daily_shadow_schema.py",
        "DAILY_SHADOW_DDL",
        None,
    ),
    "shadow_registry": (
        ROOT / "backend/app/market/shadow_registry_schema.py",
        "REGISTRY_DDL",
        "MIGRATION_0002_DDL",
    ),
    "calendar_generation": (
        ROOT / "backend/app/market/calendar_generation.py",
        "CALENDAR_GENERATION_DDL",
        None,
    ),
    "universe": (ROOT / "backend/app/market/universe.py", "UNIVERSE_DDL", None),
}


def _create_authoritative_catalog(path: Path, role: str) -> None:
    source, ddl_name, migration_name = CATALOG_DDL[role]
    ddl = _authoritative_ddl(source, ddl_name)
    if migration_name:
        ddl += "\n" + _authoritative_ddl(source, migration_name)
    with sqlite3.connect(path) as connection:
        connection.executescript(ddl)


def _session_dates(count: int = 20) -> list[str]:
    result: list[str] = []
    current = date(2026, 8, 3)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


@pytest.fixture
def captured_window(tmp_path: Path) -> dict[str, Any]:
    """Build local-only input without initializing a control DB."""

    dataset = tmp_path / "dataset"
    evidence = tmp_path / "evidence"
    control = tmp_path / "control"
    for root in (dataset, evidence, control):
        root.mkdir()
    (dataset / "manifest.json").write_text(
        '{"dataset":"test-only","generation":"g20","schema_version":2}\n', encoding="utf-8"
    )
    (evidence / "README").write_text("synthetic immutable bytes only\n", encoding="utf-8")
    window_payload = {
        "recovery_observation": None,
        "failover_observation": None,
        "replay_sample": None,
        "adjustment_equivalence": None,
        "error_handling_observation": None,
        "local_nas_isolation_observation": None,
        "restore_observation": None,
        "observation_count": 1,
    }
    canonical = (json.dumps(window_payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    window_envelope = {
        "artifact_id": "window-001",
        "artifact_ref": "evidence/window-001",
        "schema_version": "r2f5-observation-envelope-v1",
        "creator_kind": "task20_writer",
        "creator_version": "v1",
        "created_at": "2026-09-14T06:00:00Z",
        "payload": window_payload,
        "canonicalization_version": "project-canonical-json-v1",
        "payload_sha256": hashlib.sha256(b"r2f5/envelope-payload-v1\0" + canonical).hexdigest(),
    }
    envelope_canonical = (
        json.dumps(window_envelope, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    window_envelope["envelope_sha256"] = hashlib.sha256(
        b"r2f5/envelope-v1\0" + envelope_canonical
    ).hexdigest()
    (evidence / "window.json").write_text(
        json.dumps(window_envelope, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    catalog_roles = (
        "replication_sidecar",
        "daily_shadow",
        "shadow_registry",
        "calendar_generation",
        "universe",
    )
    control_paths = []
    for role in catalog_roles:
        path = control / f"{role}.db"
        _create_authoritative_catalog(path, role)
        control_paths.append(str(path))
    sessions = _session_dates()
    return {
        "start": sessions[0],
        "end": sessions[-1],
        "as_of_utc": "2026-09-14T06:00:00Z",
        "now": "2026-09-14T06:00:00Z",
        "local_dataset_root": str(dataset),
        "evidence_root": str(evidence),
        "control_store_roots": control_paths,
        "raw_calendar": sessions,
        "confirmed_sessions": sessions,
        "expected_session_count": 20,
        "provider_requests": 0,
        "writes": False,
        "restore_started": False,
        "production_window_started": False,
        "metric_fields": EXPECTED_METRICS,
        "frozen_versions": {
            "git_commit": "0" * 40,
            "installed_release": "test-release",
            "dataset_generation": "g20",
            "primary_provider_id": "baostock",
            "secondary_provider_id": "tickflow",
            "calendar_generation": "calendar-g20",
            "universe_generation": "universe-g20",
            "replication_policy_version": "r2f4-replication-v1",
            "restore_policy_version": "r2f4-restore-v1",
        },
        "canonical_provider_ids": ["baostock"],
        "secondary_qualification_proof": None,
        "whole_session_failover_drill": None,
        "window_evidence_bundle": {
            "schema_version": "r2f5-observation-envelope-v1",
            "creator_kind": "task20_writer",
            "payload_sha256": "0" * 64,
            "envelope_sha256": "1" * 64,
        },
        "offline_replay": {"network_allowed": False, "provider_requests": 0},
        "sqlite_catalogs": (
            "replication_sidecar",
            "daily_shadow",
            "shadow_registry",
            "calendar_generation",
            "universe",
        ),
        "limits": {
            "max_entries": 100_000,
            "max_input_bytes": 536_870_912,
            "max_db_rows": 1_000_000,
            "max_input_roots": 32,
            "max_sessions": 20,
            "max_replay_samples": 3,
            "max_elapsed_ms": 10_000,
        },
        "forced_error_classes": ("timeout", "auth", "rate", "schema", "coverage", "storage"),
        "local_nas_retry_transition": ("queued", "retrying"),
        "restore_verification_state": "verified",
        "diagnostic_fields": ("elapsed_ms", "read_operations", "replay_sample_count"),
    }


def _red_reader() -> Any:
    """Import lazily so the full RED suite collects before implementation."""

    try:
        return importlib.import_module(TARGET_MODULE)
    except ModuleNotFoundError as error:
        if error.name != TARGET_MODULE:
            raise
        pytest.fail(f"RED: {TARGET_MODULE} is not implemented yet ({error.name})")


def _evaluate(payload: dict[str, Any]) -> Any:
    # Scenario metadata stays test-local. Only the approved AcceptanceInput
    # projection is ever handed to the future reader.
    request = {key: payload[key] for key in INPUT_KEYS}
    assert set(request) == INPUT_KEYS
    module = _red_reader()
    reader_type = getattr(module, "AcceptanceReader", None)
    assert reader_type is not None, "AcceptanceReader is the required read-only contract"
    reader = reader_type()
    evaluate = getattr(reader, "evaluate", None)
    assert callable(evaluate), "AcceptanceReader.evaluate must be callable"
    return evaluate(request)


def _as_dict(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return result
    if hasattr(result, "model_dump"):
        return result.model_dump(mode="json")
    if hasattr(result, "dict"):
        return result.dict()
    raise AssertionError("acceptance result must be a mapping or Pydantic model")


@dataclass(frozen=True)
class _CaseSpec:
    """One matrix anchor's expected report and mutation contract."""

    requirement: str
    mutation: str
    status: str
    reason: str
    metric: str
    observed_kind: str | None
    observed_value: Any
    target_kind: str | None
    target_value: Any
    expected_exception: str | None
    effect: str
    builder: Any = None


_CASE_OVERRIDES: dict[str, tuple[str, str, str, str]] = {
    "FR-1": ("missing_control", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
    "FR-2": ("unsafe_path", "unavailable", "PATH_INVALID", "read_boundary"),
    "FR-3": ("snapshot_changed", "unavailable", "SNAPSHOT_CHANGED", "read_boundary"),
    "FR-4": ("missing_control", "unavailable", "CONTROL_STATE_UNAVAILABLE", "calendar"),
    "FR-5": ("nineteen_sessions", "not_ready", "SESSION_COUNT_NOT_20", "continuity"),
    "FR-6": ("missing_middle", "unavailable", "SESSION_SEQUENCE_INVALID", "continuity"),
    "FR-7": ("version_drift", "not_ready", "VERSION_DRIFT", "provenance"),
    "FR-8": (
        "after_cutoff",
        "not_ready",
        "AVAILABILITY_CUTOFF_FAILED",
        "same_evening_availability",
    ),
    "FR-9": ("coverage_failure", "not_ready", "COVERAGE_FAILED", "coverage"),
    "FR-10": ("lineage_missing", "unavailable", "LINEAGE_UNAVAILABLE", "provenance"),
    "FR-11": ("unknown_replay_identity", "unavailable", "REPLAY_UNAVAILABLE", "replay"),
    "FR-12": ("calendar_conflict", "not_ready", "CALENDAR_CONFLICT", "calendar"),
    "FR-13": ("missing_restore_drill", "unavailable", "RESTORE_UNAVAILABLE", "restore"),
    "FR-14": (
        "unavailable_precedence",
        "unavailable",
        "CONTROL_STATE_UNAVAILABLE",
        "read_boundary",
    ),
    "FR-15": ("invalid_cli_range", "unavailable", "INVALID_ARGUMENTS", "read_boundary"),
    "FR-16": ("api_read_only", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
    "FR-17": ("typed_markers", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
    "FR-18": (
        "pre_capture_unavailable",
        "unavailable",
        "CONTROL_STATE_UNAVAILABLE",
        "read_boundary",
    ),
    "FR-19": ("missing_metric", "unavailable", "CONTROL_STATE_UNAVAILABLE", "coverage"),
    "FR-20": ("baostock_only", "not_ready", "FAILOVER_UNAVAILABLE", "failover"),
    "FR-21": ("local_chain_only", "unavailable", "REMOTE_PROOF_MISSING", "replication"),
    "FR-22": ("offline_replay", "unavailable", "REPLAY_UNAVAILABLE", "replay"),
    "FR-23": ("missing_version", "unavailable", "VERSION_DRIFT", "provenance"),
    "FR-24": ("duplicate_raw_calendar", "unavailable", "SESSION_SEQUENCE_INVALID", "calendar"),
    "FR-25": ("observation_count", "unavailable", "CONTROL_STATE_UNAVAILABLE", "continuity"),
    "FR-26": ("volatile_digest", "not_ready", "READ_BOUNDARY_FAILED", "read_boundary"),
    "FR-27": ("invalid_typed_value", "unavailable", "INVALID_ARGUMENTS", "read_boundary"),
    "FR-28": ("toctou", "unavailable", "SNAPSHOT_CHANGED", "read_boundary"),
}
_CASE_OVERRIDES.update(
    {
        f"NFR-{number}": value
        for number, value in {
            1: (
                "predecessor_compatibility",
                "unavailable",
                "CONTROL_STATE_UNAVAILABLE",
                "read_boundary",
            ),
            2: ("readonly_sqlite", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
            3: ("sanitized_error", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
            4: ("performance_limit", "unavailable", "INPUT_LIMIT_EXCEEDED", "read_boundary"),
            5: ("anchor_inventory", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
            6: ("redacted_error", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
            7: ("row_limit", "unavailable", "INPUT_LIMIT_EXCEEDED", "read_boundary"),
            8: ("snapshot_identity", "unavailable", "SNAPSHOT_CHANGED", "read_boundary"),
            9: ("red_boundary", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
            10: ("production_gate", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
            11: ("roadmap_threshold", "not_ready", "COVERAGE_FAILED", "coverage"),
            12: (
                "missing_policy_threshold",
                "unavailable",
                "REPLICATION_UNAVAILABLE",
                "replication",
            ),
            13: ("descriptor_change", "unavailable", "SNAPSHOT_CHANGED", "read_boundary"),
            14: ("digest_contract", "unavailable", "INVALID_ARGUMENTS", "read_boundary"),
            15: ("additive_api", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
        }.items()
    }
)
_CASE_OVERRIDES.update(
    {
        "AC-1": ("nineteen_sessions", "not_ready", "SESSION_COUNT_NOT_20", "continuity"),
        "AC-2": (
            "cutoff_boundary",
            "not_ready",
            "AVAILABILITY_CUTOFF_FAILED",
            "same_evening_availability",
        ),
        "AC-3": ("version_drift", "not_ready", "VERSION_DRIFT", "provenance"),
        "AC-4": ("mixed_source", "not_ready", "SOURCE_PURITY_FAILED", "source_purity"),
        "AC-5": ("lineage_replay", "unavailable", "LINEAGE_UNAVAILABLE", "provenance"),
        "AC-6": ("replication_lag", "unavailable", "REPLICATION_UNAVAILABLE", "replication"),
        "AC-7": (
            "physical_fingerprint",
            "unavailable",
            "CONTROL_STATE_UNAVAILABLE",
            "read_boundary",
        ),
        "AC-8": ("invalid_path", "unavailable", "PATH_INVALID", "read_boundary"),
        "AC-9": ("api_cli_parity", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
        "AC-10": ("status_precedence", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
        "AC-11": ("perf_fixture", "unavailable", "INPUT_LIMIT_EXCEEDED", "read_boundary"),
        "AC-12": (
            "protected_compatibility",
            "unavailable",
            "CONTROL_STATE_UNAVAILABLE",
            "read_boundary",
        ),
        "AC-13": ("task20_pending", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
        "AC-14": ("determinism", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
        "AC-15": ("metric_inventory", "unavailable", "CONTROL_STATE_UNAVAILABLE", "coverage"),
        "AC-16": ("whole_session_failover", "not_ready", "FAILOVER_UNAVAILABLE", "failover"),
        "AC-17": ("local_chain_only", "unavailable", "REMOTE_PROOF_MISSING", "replication"),
        "AC-18": ("offline_identity", "unavailable", "REPLAY_UNAVAILABLE", "replay"),
        "AC-19": ("version_vector", "unavailable", "VERSION_DRIFT", "provenance"),
        "AC-20": ("raw_sequence", "unavailable", "SESSION_SEQUENCE_INVALID", "calendar"),
        "AC-21": ("twenty_observations", "unavailable", "CONTROL_STATE_UNAVAILABLE", "continuity"),
        "AC-22": ("semantic_digest", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
        "AC-23": ("bounded_types", "unavailable", "INVALID_ARGUMENTS", "read_boundary"),
    }
)
_CASE_OVERRIDES.update(
    {
        f"EC-{number}": value
        for number, value in {
            1: ("nineteen_sessions", "not_ready", "SESSION_COUNT_NOT_20", "continuity"),
            2: ("duplicate_raw_calendar", "unavailable", "SESSION_SEQUENCE_INVALID", "calendar"),
            3: ("future_session", "unavailable", "PIT_VISIBILITY_INVALID", "calendar"),
            4: ("calendar_conflict", "unavailable", "CALENDAR_CONFLICT", "calendar"),
            5: ("universe_mismatch", "not_ready", "UNIVERSE_COUNT_MISMATCH", "universe"),
            6: (
                "non_shanghai_timestamp",
                "unavailable",
                "INVALID_ARGUMENTS",
                "same_evening_availability",
            ),
            7: (
                "late_repair",
                "not_ready",
                "AVAILABILITY_CUTOFF_FAILED",
                "same_evening_availability",
            ),
            8: ("version_drift", "not_ready", "VERSION_DRIFT", "provenance"),
            9: ("lineage_corrupt", "unavailable", "LINEAGE_INVALID", "provenance"),
            10: ("replay_corrupt", "unavailable", "REPLAY_UNAVAILABLE", "replay"),
            11: (
                "replication_corrupt",
                "unavailable",
                "REPLICATION_STATE_UNAVAILABLE",
                "replication",
            ),
            12: ("restore_corrupt", "unavailable", "RESTORE_UNAVAILABLE", "restore"),
            13: ("replacement_race", "unavailable", "SNAPSHOT_CHANGED", "read_boundary"),
            14: ("unsafe_path", "unavailable", "PATH_INVALID", "read_boundary"),
            15: (
                "missing_locked_sqlite",
                "unavailable",
                "CONTROL_STATE_UNAVAILABLE",
                "read_boundary",
            ),
            16: (
                "sanitized_exception",
                "unavailable",
                "CONTROL_STATE_UNAVAILABLE",
                "read_boundary",
            ),
            17: ("input_limit", "unavailable", "INPUT_LIMIT_EXCEEDED", "read_boundary"),
            18: ("invalid_arguments", "unavailable", "INVALID_ARGUMENTS", "read_boundary"),
            19: ("missing_slo", "unavailable", "CONTROL_STATE_UNAVAILABLE", "coverage"),
            20: ("baostock_only", "not_ready", "FAILOVER_UNAVAILABLE", "failover"),
            21: ("incomplete_failover", "unavailable", "FAILOVER_UNAVAILABLE", "failover"),
            22: ("local_chain_only", "unavailable", "REMOTE_PROOF_MISSING", "replication"),
            23: ("unknown_replay_identity", "unavailable", "REPLAY_UNAVAILABLE", "replay"),
            24: ("missing_version", "unavailable", "VERSION_DRIFT", "provenance"),
            25: ("out_of_order_raw", "unavailable", "SESSION_SEQUENCE_INVALID", "calendar"),
            26: ("volatile_digest", "unavailable", "CONTROL_STATE_UNAVAILABLE", "read_boundary"),
        }.items()
    }
)


def _case_specs() -> dict[str, _CaseSpec]:
    failed_values = {"count": 1, "ratio": 0.85, "bool": False, "duration_seconds": 301}
    cases: dict[str, _CaseSpec] = {}
    for anchor in REQUIREMENT_ANCHORS:
        requirement = re.search(r"(?:^|_)(FR|NFR|AC|EC)_(\d+)$", anchor, flags=re.IGNORECASE)
        assert requirement is not None
        requirement_id = f"{requirement.group(1).upper()}-{int(requirement.group(2))}"
        mutation_name, status, reason, metric = _CASE_OVERRIDES.get(
            requirement_id,
            (
                f"contract_{requirement_id.lower()}",
                "unavailable",
                "CONTROL_STATE_UNAVAILABLE",
                "read_boundary",
            ),
        )
        unavailable = status == "unavailable" or reason.endswith("_UNAVAILABLE")
        value_kind, _passed_value, target_value = METRIC_CONTRACTS[metric]
        cases[anchor] = _CaseSpec(
            requirement=requirement_id,
            mutation=mutation_name,
            status=status,
            reason=reason,
            metric=metric,
            observed_kind=None if unavailable else value_kind,
            observed_value=None if unavailable else failed_values[value_kind],
            target_kind=None if unavailable else value_kind,
            target_value=None if unavailable else target_value,
            expected_exception=None,
            effect="no_filesystem_db_provider_or_pointer_write",
        )
    return cases


CASE_SPECS = _case_specs()


def _build_case_input(case: _CaseSpec, captured_window: dict[str, Any]) -> dict[str, Any]:
    payload = deepcopy(captured_window)
    dataset = Path(payload["local_dataset_root"])
    evidence = Path(payload["evidence_root"])
    if case.mutation in {"nineteen_sessions", "exact_twenty"}:
        payload["raw_calendar"] = payload["raw_calendar"][:-1]
        (dataset / "calendar.json").write_text(
            json.dumps({"source_sequence": payload["raw_calendar"]}) + "\n", encoding="utf-8"
        )
    elif case.mutation in {"duplicate_raw_calendar", "duplicate"}:
        payload["raw_calendar"][5] = payload["raw_calendar"][4]
        (dataset / "calendar.json").write_text(
            json.dumps({"source_sequence": payload["raw_calendar"]}) + "\n", encoding="utf-8"
        )
    elif case.mutation in {"out_of_order_raw", "replacement_race"}:
        payload["raw_calendar"][5], payload["raw_calendar"][6] = (
            payload["raw_calendar"][6],
            payload["raw_calendar"][5],
        )
        (dataset / "calendar.json").write_text(
            json.dumps({"source_sequence": payload["raw_calendar"]}) + "\n", encoding="utf-8"
        )
    elif case.mutation == "missing_middle":
        payload["raw_calendar"].pop(10)
        (dataset / "calendar.json").write_text(
            json.dumps({"source_sequence": payload["raw_calendar"]}) + "\n", encoding="utf-8"
        )
    elif case.mutation == "future_session":
        payload["raw_calendar"][-1] = "2099-01-01"
        (dataset / "calendar.json").write_text(
            json.dumps({"source_sequence": payload["raw_calendar"]}) + "\n", encoding="utf-8"
        )
    elif case.mutation == "missing_control" or case.mutation == "missing_locked_sqlite":
        missing = Path(payload["control_store_roots"][-1])
        if missing.exists():
            missing.unlink()
    elif case.mutation == "corrupt_sqlite":
        Path(payload["control_store_roots"][0]).write_bytes(b"not sqlite")
    elif case.mutation == "unsafe_path":
        payload["local_dataset_root"] = "relative-dataset"
    elif case.mutation == "coverage_failure" or case.mutation == "universe_mismatch":
        payload["required_count"] = 100
        payload["loaded_count"] = 99
        payload["unknown_count"] = 1
    elif case.mutation == "mixed_source":
        payload["canonical_provider_ids"] = ["baostock", "tickflow"]
    elif case.mutation == "version_drift":
        payload["frozen_versions"]["calendar_generation"] = "calendar-drift"
        manifest = json.loads((dataset / "manifest.json").read_text(encoding="utf-8"))
        manifest["generation"] = "calendar-drift"
        (dataset / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8"
        )
    elif case.mutation == "unknown_replay_identity":
        payload["offline_replay"] = {
            "adapter_id": "unknown",
            "normalizer_id": "unknown",
            "network_allowed": False,
            "provider_requests": 0,
        }
    elif case.mutation == "local_chain_only":
        payload["replication_trust_scope"] = "LOCAL_CHAIN_ONLY"
    elif case.mutation == "missing_restore_drill":
        payload["restore_verification_state"] = None
    elif case.mutation == "lineage_missing":
        payload["lineage"] = None
    elif case.mutation == "calendar_conflict":
        payload["calendar_conflict"] = True
    elif case.mutation == "toctou":
        payload["toctou_probe"] = True
    # Every remaining named drill has a material immutable evidence mutation;
    # the reader receives no fixture-control key for it.
    (evidence / f"{case.mutation}.json").write_text(
        json.dumps({"artifact_id": case.mutation, "creator_kind": "task20_writer"}) + "\n",
        encoding="utf-8",
    )
    return payload


def _anchor_builder(case: _CaseSpec):
    def build(captured: dict[str, Any]) -> dict[str, Any]:
        return _build_case_input(case, captured)

    build.__name__ = f"build_{case.requirement.lower()}"
    return build


for _case in CASE_SPECS.values():
    # Each matrix anchor owns a named callable builder, even when two anchors
    # share the same fixture mutation implementation.
    object.__setattr__(_case, "builder", _anchor_builder(_case))


def _assert_case_report(case: _CaseSpec, payload: dict[str, Any]) -> None:
    report = _as_dict(_evaluate(payload))
    assert REPORT_CORE_FIELDS <= report.keys()
    assert set(EXPECTED_METRICS) <= report.keys()
    expected_status = "unavailable" if case.reason.endswith("_UNAVAILABLE") else case.status
    assert report["status"] == expected_status
    metric = report[case.metric]
    expected_metric_status = (
        "unavailable"
        if expected_status == "unavailable"
        else ("pass" if expected_status == "ready" else "fail")
    )
    assert metric["status"] == expected_metric_status
    assert metric["reason_code"] == case.reason
    if expected_status == "unavailable":
        assert metric["observed"] is None and metric["target"] is None
    else:
        assert metric["observed"] == {"kind": case.observed_kind, "value": case.observed_value}
        assert metric["target"] == {"kind": case.target_kind, "value": case.target_value}
    assert report["provider_requests"] == 0
    assert report["writes"] is False
    assert report["restore_started"] is False
    assert report["production_window_started"] is False


def _make_requirement_test(anchor: str):
    case = CASE_SPECS[anchor]

    def test(captured_window: dict[str, Any]) -> None:
        assert REQUIREMENT_SUMMARIES[anchor], f"matrix summary missing for {anchor}"
        assert callable(case.builder)
        payload = case.builder(captured_window)
        roots = [Path(payload[key]) for key in ("local_dataset_root", "evidence_root")] + [
            Path(path).parent for path in payload["control_store_roots"]
        ]
        before = tuple(_physical_fingerprint(root) for root in roots)
        _assert_case_report(case, payload)
        assert tuple(_physical_fingerprint(root) for root in roots) == before

    test.__name__ = anchor
    test.__qualname__ = anchor
    test.__doc__ = f"Concrete RED contract anchor for {anchor}: {REQUIREMENT_SUMMARIES[anchor]}"
    return test


for _anchor in REQUIREMENT_ANCHORS:
    globals()[_anchor] = _make_requirement_test(_anchor)


@dataclass(frozen=True)
class _SloCase:
    metric: str
    mutation: str
    report_status: str
    metric_status: str
    reason: str | None
    observed_kind: str | None
    observed_value: Any
    target_kind: str | None
    target_value: Any


def _slo_cases(metric: str, reason: str, kind: str, passed: Any, failed: Any, target: Any):
    failed_status = "unavailable" if reason.endswith("_UNAVAILABLE") else "not_ready"
    failed_kind = None if failed_status == "unavailable" else kind
    failed_value = None if failed_status == "unavailable" else failed
    failed_target = None if failed_status == "unavailable" else target
    return (
        _SloCase(metric, "pass", "ready", "pass", None, kind, passed, kind, target),
        _SloCase(
            metric,
            "fail",
            failed_status,
            failed_status,
            reason,
            failed_kind,
            failed_value,
            failed_kind,
            failed_target,
        ),
        _SloCase(
            metric,
            "unavailable",
            "unavailable",
            "unavailable",
            "CONTROL_STATE_UNAVAILABLE",
            None,
            None,
            None,
            None,
        ),
    )


SLO_CASES = {
    metric: _slo_cases(metric, reason, kind, passed, failed, target)
    for metric, reason, kind, passed, failed, target in (
        ("continuity", "CONTINUITY_FAILED", "count", 0, 1, 0),
        ("next_morning_availability", "AVAILABILITY_CUTOFF_FAILED", "ratio", 1, 0.95, 1),
        ("same_evening_availability", "AVAILABILITY_CUTOFF_FAILED", "ratio", 0.9, 0.85, 0.9),
        ("coverage", "COVERAGE_FAILED", "ratio", 1, 0.99, 1),
        ("canonical_integrity", "CANONICAL_INTEGRITY_FAILED", "bool", True, False, True),
        ("source_purity", "SOURCE_PURITY_FAILED", "bool", True, False, True),
        ("recovery", "RECOVERY_FAILED", "bool", True, False, True),
        ("failover", "FAILOVER_UNAVAILABLE", "bool", True, False, True),
        ("provenance", "LINEAGE_UNAVAILABLE", "bool", True, False, True),
        ("replay", "REPLAY_SEMANTIC_MISMATCH", "bool", True, False, True),
        ("adjustment", "ADJUSTMENT_UNAVAILABLE", "bool", True, False, True),
        ("calendar", "CALENDAR_CONFLICT", "bool", True, False, True),
        ("universe", "UNIVERSE_COUNT_MISMATCH", "bool", True, False, True),
        ("error_handling", "ERROR_HANDLING_FAILED", "bool", True, False, True),
        ("local_nas_isolation", "LOCAL_NAS_ISOLATION_FAILED", "bool", True, False, True),
        ("replication", "REPLICATION_LAG", "duration_seconds", 0, 301, 300),
        ("restore", "RESTORE_UNAVAILABLE", "bool", True, False, True),
        ("read_boundary", "READ_BOUNDARY_FAILED", "bool", True, False, True),
    )
}


def _assert_slo_report(case: _SloCase, captured_window: dict[str, Any]) -> None:
    payload = deepcopy(captured_window)
    evidence = Path(payload["evidence_root"])
    if case.mutation == "unavailable":
        (evidence / "window.json").unlink()
    else:
        (evidence / f"{case.metric}-{case.mutation}.json").write_text(
            json.dumps(
                {
                    "metric_value_kind": case.observed_kind,
                    "observed_value": case.observed_value,
                    "target_value": case.target_value,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    roots = (
        Path(payload["local_dataset_root"]),
        Path(payload["evidence_root"]),
        Path(payload["control_store_roots"][0]).parent,
    )
    before = tuple(_physical_fingerprint(root) for root in roots)
    assert case.metric in EXPECTED_METRICS
    assert case.observed_kind is None or case.observed_kind in {
        "count",
        "ratio",
        "duration_seconds",
        "bool",
        "hash",
    }
    report = _as_dict(_evaluate(payload))
    assert REPORT_CORE_FIELDS <= report.keys()
    assert set(EXPECTED_METRICS) <= report.keys()
    assert report["status"] == case.report_status
    result = report[case.metric]
    assert result["status"] == case.metric_status
    assert result["reason_code"] == case.reason
    if case.metric_status == "unavailable":
        assert result["observed"] is None and result["target"] is None
    else:
        assert result["observed"] == {"kind": case.observed_kind, "value": case.observed_value}
        assert result["target"] == {"kind": case.target_kind, "value": case.target_value}
    assert result["acceptance_ref"] == "AC-15"
    assert result["planned_test_anchor"] == f"test_r2f5_slo_{case.metric}"
    assert report["provider_requests"] == 0
    assert report["writes"] is False
    assert report["restore_started"] is False
    assert report["production_window_started"] is False
    assert tuple(_physical_fingerprint(root) for root in roots) == before


def _make_slo_test(metric: str):
    def test(captured_window: dict[str, Any], case: _SloCase) -> None:
        _assert_slo_report(case, captured_window)

    test.__name__ = f"test_r2f5_slo_{metric}"
    test.__qualname__ = test.__name__
    test.__doc__ = f"Concrete RED SLO reducer anchor for {metric}."
    return pytest.mark.parametrize("case", SLO_CASES[metric], ids=("pass", "fail", "unavailable"))(
        test
    )


for _metric in EXPECTED_METRICS:
    globals()[f"test_r2f5_slo_{_metric}"] = _make_slo_test(_metric)


def test_r2f5_acceptance_fixture_has_exact_anchor_inventory() -> None:
    assert len(REQUIREMENT_ANCHORS) == 92
    assert len(set(REQUIREMENT_ANCHORS)) == 92
    assert len(SLO_ANCHORS) == 18
    assert set(SLO_ANCHORS) == {f"test_r2f5_slo_{metric}" for metric in EXPECTED_METRICS}
    assert all(
        callable(case.builder) and case.builder.__name__.startswith("build_")
        for case in CASE_SPECS.values()
    )


def test_r2f5_input_projection_has_no_undeclared_control_fields() -> None:
    assert INPUT_KEYS == {
        "start",
        "end",
        "local_dataset_root",
        "evidence_root",
        "control_store_roots",
        "now",
    }
    source = Path(__file__).read_text(encoding="utf-8")
    assert "request['" + "scenario']" not in source
    assert "request[" + '"scenario"]' not in source


def test_r2f5_reader_receives_only_strict_acceptance_input(captured_window, monkeypatch) -> None:
    seen = {}
    module = types.ModuleType(TARGET_MODULE)

    class Reader:
        def evaluate(self, request):
            seen.update(request)
            return {}

    module.AcceptanceReader = Reader
    monkeypatch.setitem(sys.modules, TARGET_MODULE, module)
    _evaluate(captured_window)
    assert set(seen) == INPUT_KEYS


def test_r2f5_metric_kinds_match_approved_structured_contract() -> None:
    block = json.loads(
        re.search(
            r"<!-- R2F5_X8_CONTRACTS_JSON -->\s*```json\s*(\{.*?\})\s*```",
            DESIGN.read_text(encoding="utf-8"),
            re.S,
        ).group(1)
    )
    assert set(block["metric_fields"]) == set(METRIC_CONTRACTS)
    for metric, (kind, _passed, target) in METRIC_CONTRACTS.items():
        contract = block["metric_value_kinds"][metric]
        expected_range = {
            "continuity": "[0,20]",
            "count": "[0,20]",
            "ratio": "[0,1]",
            "bool": "boolean",
            "duration_seconds": "[0,2147483647]",
        }[kind]
        assert contract == {
            "observed": kind,
            "target": kind,
            "unavailable_null": True,
            "range": expected_range,
        }
        assert METRIC_CONTRACTS[metric][2] == target


def test_r2f5_strict_protocol_simulator_accepts_each_builder_and_material_mutation(
    captured_window,
) -> None:
    for anchor, case in CASE_SPECS.items():
        assert callable(case.builder), anchor
        before = {key: captured_window[key] for key in INPUT_KEYS}
        before_fingerprint = tuple(
            _physical_fingerprint(Path(captured_window[key]))
            for key in ("local_dataset_root", "evidence_root")
        ) + (_physical_fingerprint(Path(captured_window["control_store_roots"][0]).parent),)
        payload = case.builder(captured_window)
        request = {key: payload[key] for key in INPUT_KEYS}
        assert set(request) == INPUT_KEYS
        assert all(isinstance(request[key], (str, list)) for key in INPUT_KEYS)
        changed = request != before
        after_fingerprint = tuple(
            _physical_fingerprint(Path(captured_window[key]))
            for key in ("local_dataset_root", "evidence_root")
        ) + (_physical_fingerprint(Path(captured_window["control_store_roots"][0]).parent),)
        assert changed or after_fingerprint != before_fingerprint


def _fake_reader_module(result: Any) -> types.ModuleType:
    module = types.ModuleType(TARGET_MODULE)

    class FakeReader:
        def evaluate(self, _payload: dict[str, Any]) -> Any:
            return result

    module.AcceptanceReader = FakeReader
    return module


def test_r2f5_mutation_sanity_rejects_dummy_none(captured_window, monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, TARGET_MODULE, _fake_reader_module(None))
    with pytest.raises(AssertionError, match="mapping or Pydantic"):
        _assert_case_report(CASE_SPECS[REQUIREMENT_ANCHORS[0]], captured_window)


def test_r2f5_mutation_sanity_rejects_constant_ready(captured_window, monkeypatch) -> None:
    constant = {"status": "ready", "provider_requests": 0, "writes": False}
    monkeypatch.setitem(sys.modules, TARGET_MODULE, _fake_reader_module(constant))
    with pytest.raises(AssertionError):
        _assert_case_report(CASE_SPECS[REQUIREMENT_ANCHORS[0]], captured_window)


def test_r2f5_mutation_sanity_rejects_wrong_reason_and_metric(captured_window, monkeypatch) -> None:
    wrong = {
        "status": "unavailable",
        "read_boundary": {
            "status": "unavailable",
            "reason_code": "WRONG_REASON",
            "observed": None,
            "target": None,
        },
        "provider_requests": 0,
        "writes": False,
        "restore_started": False,
        "production_window_started": False,
    }
    monkeypatch.setitem(sys.modules, TARGET_MODULE, _fake_reader_module(wrong))
    with pytest.raises(AssertionError):
        _assert_case_report(CASE_SPECS[REQUIREMENT_ANCHORS[0]], captured_window)


def test_r2f5_red_reader_does_not_mask_internal_missing_dependency(monkeypatch) -> None:
    def import_internal(_name: str):
        raise ModuleNotFoundError("internal dependency", name="backend.app.market.models")

    monkeypatch.setattr(importlib, "import_module", import_internal)
    with pytest.raises(ModuleNotFoundError, match="internal dependency"):
        _red_reader()


def test_r2f5_reference_fixture_uses_no_provider_or_production_root(captured_window) -> None:
    assert all("Application Support" not in path for path in captured_window["control_store_roots"])
    assert all("Volumes" not in path for path in captured_window["control_store_roots"])
    assert captured_window["provider_requests"] == 0
    assert captured_window["writes"] is False


def test_r2f5_reference_fixture_fingerprint_is_stable(captured_window) -> None:
    roots = [Path(captured_window[key]) for key in ("local_dataset_root", "evidence_root")]
    before = tuple(_tree_digest(root) for root in roots)
    assert before == tuple(_tree_digest(root) for root in roots)


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("nineteen", "SESSION_COUNT_NOT_20"),
        ("duplicate", "SESSION_SEQUENCE_INVALID"),
        ("out_of_order", "SESSION_SEQUENCE_INVALID"),
        ("missing_middle", "SESSION_SEQUENCE_INVALID"),
        ("future", "PIT_VISIBILITY_INVALID"),
    ],
    ids=("19", "duplicate", "out-of-order", "missing-middle", "future"),
)
def test_r2f5_session_negative_cases(captured_window, mutation, expected_reason) -> None:
    case = deepcopy(captured_window)
    if mutation == "nineteen":
        case["raw_calendar"] = case["raw_calendar"][:-1]
    elif mutation == "duplicate":
        case["raw_calendar"][5] = case["raw_calendar"][4]
    elif mutation == "out_of_order":
        case["raw_calendar"][5], case["raw_calendar"][6] = (
            case["raw_calendar"][6],
            case["raw_calendar"][5],
        )
    elif mutation == "missing_middle":
        case["raw_calendar"].pop(10)
    elif mutation == "future":
        case["raw_calendar"][-1] = "2099-01-01"
    assert expected_reason in REASONS
    assert case["raw_calendar"] != captured_window["raw_calendar"]
    (Path(case["local_dataset_root"]) / "calendar.json").write_text(
        json.dumps({"source_sequence": case["raw_calendar"]}) + "\n", encoding="utf-8"
    )
    report = _as_dict(_evaluate(case))
    assert report["status"] in {"not_ready", "unavailable"}
    metric = "calendar" if mutation == "future" else "continuity"
    expected_status = "not_ready" if mutation == "nineteen" else "unavailable"
    assert report["status"] == expected_status
    assert report[metric]["status"] == ("fail" if expected_status == "not_ready" else "unavailable")
    assert report[metric]["reason_code"] == expected_reason
    if expected_status == "unavailable":
        assert report[metric]["observed"] is None and report[metric]["target"] is None
    else:
        assert report[metric]["observed"] == {"kind": "count", "value": 1}
        assert report[metric]["target"] == {"kind": "count", "value": 20}


@pytest.mark.parametrize(
    ("published_at", "metric", "should_pass"),
    [
        ("2026-08-03T13:15:00Z", "same_evening_availability", True),
        ("2026-08-03T13:16:00Z", "same_evening_availability", False),
        ("2026-08-04T00:00:00Z", "next_morning_availability", True),
        ("2026-08-04T00:01:00Z", "next_morning_availability", False),
    ],
    ids=("evening-inclusive", "evening-after-cutoff", "morning-inclusive", "morning-after-cutoff"),
)
def test_r2f5_shanghai_cutoff_boundaries(
    captured_window, published_at, metric, should_pass
) -> None:
    case = deepcopy(captured_window)
    case["published_at"] = published_at
    (Path(case["evidence_root"]) / "cutoff.json").write_text(
        json.dumps({"published_at": published_at}) + "\n", encoding="utf-8"
    )
    assert metric in EXPECTED_METRICS
    report = _as_dict(_evaluate(case))
    expected_status = "pass" if should_pass else "fail"
    assert report[metric]["status"] == expected_status
    if should_pass:
        assert report[metric]["reason_code"] is None
        expected_value = 1 if metric == "next_morning_availability" else 0.9
        assert report[metric]["observed"] == {"kind": "ratio", "value": expected_value}
        assert report[metric]["target"] == {"kind": "ratio", "value": expected_value}
    else:
        assert report[metric]["reason_code"] == "AVAILABILITY_CUTOFF_FAILED"
        assert report[metric]["observed"]["kind"] == "ratio"
        assert report[metric]["observed"]["value"] < report[metric]["target"]["value"]


def test_r2f5_missing_control_db_never_initializes(captured_window) -> None:
    missing = Path(captured_window["control_store_roots"][-1])
    missing.unlink()
    assert not missing.exists()
    _evaluate(captured_window)
    assert not missing.exists()


def test_r2f5_catalog_fixture_probe_is_readonly(tmp_path: Path) -> None:
    database = tmp_path / "existing.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE fixture_probe (id INTEGER PRIMARY KEY)")
    before = database.read_bytes()
    assert database.exists()
    assert database.read_bytes() == before


def test_r2f5_typed_metric_reason_and_digest_contract(captured_window) -> None:
    assert set(captured_window["metric_fields"]) == set(EXPECTED_METRICS)
    assert len(captured_window["metric_fields"]) == 18
    assert "VERSION_DRIFT" in REASONS
    assert len(captured_window["window_evidence_bundle"]["payload_sha256"]) == 64
    _evaluate(captured_window)


def test_r2f5_complete_frozen_vector_is_bound_and_drift_is_not_ready(captured_window) -> None:
    versions = captured_window["frozen_versions"]
    assert versions["primary_provider_id"] == "baostock"
    assert versions["secondary_provider_id"] == "tickflow"
    assert all(versions.values())
    drifted = deepcopy(captured_window)
    drifted["frozen_versions"]["calendar_generation"] = "calendar-drift"
    assert drifted["frozen_versions"] != versions
    manifest = Path(drifted["local_dataset_root"]) / "manifest.json"
    manifest.write_text(
        '{"dataset":"test-only","generation":"calendar-drift","schema_version":2}\n',
        encoding="utf-8",
    )
    report = _as_dict(_evaluate(drifted))
    assert report["status"] == "not_ready"
    provenance = report["provenance"]
    assert provenance["status"] == "fail"
    assert provenance["reason_code"] == "VERSION_DRIFT"
    assert provenance["observed"] == {"kind": "bool", "value": False}
    assert provenance["target"] == {"kind": "bool", "value": True}


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("baostock_only", "FAILOVER_UNAVAILABLE"),
        ("no_secondary_qualification", "FAILOVER_UNAVAILABLE"),
        ("missing_whole_session_drill", "FAILOVER_UNAVAILABLE"),
        ("local_chain_only", "REMOTE_PROOF_MISSING"),
        ("missing_replication_threshold", "REPLICATION_UNAVAILABLE"),
        ("missing_restore_drill", "RESTORE_UNAVAILABLE"),
        ("unknown_replay_identity", "REPLAY_UNAVAILABLE"),
        ("replay_semantic_mismatch", "REPLAY_SEMANTIC_MISMATCH"),
        ("lineage_missing", "LINEAGE_UNAVAILABLE"),
        ("corrupt_envelope_hash", "LINEAGE_INVALID"),
        ("nas_outage_not_retryable", "LOCAL_NAS_ISOLATION_FAILED"),
        ("recovery_duplicate_publication", "RECOVERY_FAILED"),
        ("forced_error_missing_class", "ERROR_HANDLING_FAILED"),
        ("snapshot_content_changed", "SNAPSHOT_CHANGED"),
        ("tree_entry_limit", "INPUT_LIMIT_EXCEEDED"),
        ("locked_sqlite", "CONTROL_STATE_UNAVAILABLE"),
        ("unsafe_path", "PATH_INVALID"),
        ("pre_capture_unavailable", "CONTROL_STATE_UNAVAILABLE"),
    ],
)
def test_r2f5_evidence_failure_is_bounded_and_readonly(
    captured_window, mutation: str, expected_reason: str
) -> None:
    case = deepcopy(captured_window)
    case["fixture_mutation"] = mutation
    assert expected_reason in REASONS or expected_reason in {
        "FAILOVER_UNAVAILABLE",
        "REPLICATION_UNAVAILABLE",
        "RESTORE_UNAVAILABLE",
        "LINEAGE_INVALID",
        "LOCAL_NAS_ISOLATION_FAILED",
        "RECOVERY_FAILED",
        "ERROR_HANDLING_FAILED",
    }
    roots = (
        Path(case["local_dataset_root"]),
        Path(case["evidence_root"]),
        Path(case["control_store_roots"][0]).parent,
    )
    (roots[1] / f"{mutation}.json").write_text(
        json.dumps({"artifact_id": mutation, "creator_kind": "task20_writer"}) + "\n",
        encoding="utf-8",
    )
    before = tuple(_tree_digest(root) for root in roots)
    report = _as_dict(_evaluate(case))
    metric_by_mutation = {
        "baostock_only": "failover",
        "no_secondary_qualification": "failover",
        "missing_whole_session_drill": "failover",
        "local_chain_only": "replication",
        "missing_replication_threshold": "replication",
        "missing_restore_drill": "restore",
        "unknown_replay_identity": "replay",
        "replay_semantic_mismatch": "replay",
        "lineage_missing": "provenance",
        "corrupt_envelope_hash": "provenance",
        "nas_outage_not_retryable": "local_nas_isolation",
        "recovery_duplicate_publication": "recovery",
        "forced_error_missing_class": "error_handling",
    }
    metric = metric_by_mutation.get(mutation, "read_boundary")
    unavailable = expected_reason.endswith("_UNAVAILABLE") or expected_reason in {
        "CONTROL_STATE_UNAVAILABLE",
        "PATH_INVALID",
        "SNAPSHOT_CHANGED",
        "PIT_VISIBILITY_INVALID",
        "REMOTE_PROOF_MISSING",
        "INPUT_LIMIT_EXCEEDED",
    }
    assert report["status"] == ("unavailable" if unavailable else "not_ready")
    assert report[metric]["status"] == ("unavailable" if unavailable else "fail")
    assert report[metric]["reason_code"] == expected_reason
    if unavailable:
        assert report[metric]["observed"] is None and report[metric]["target"] is None
    else:
        assert report[metric]["observed"] is not None
    assert tuple(_tree_digest(root) for root in roots) == before


def test_r2f5_window_evidence_is_single_task20_envelope(captured_window) -> None:
    envelope = captured_window["window_evidence_bundle"]
    assert envelope["creator_kind"] == "task20_writer"
    assert envelope["schema_version"] == "r2f5-observation-envelope-v1"
    assert envelope["creator_kind"] not in {"r2f5_reader", "r2f4_writer"}
    actual = json.loads(
        (Path(captured_window["evidence_root"]) / "window.json").read_text(encoding="utf-8")
    )
    assert actual["creator_kind"] == "task20_writer"
    assert len(actual["payload_sha256"]) == 64
    assert len(actual["envelope_sha256"]) == 64
    _evaluate(captured_window)


def test_r2f5_offline_replay_has_zero_provider_and_network_requests(captured_window) -> None:
    replay = captured_window["offline_replay"]
    assert replay == {"network_allowed": False, "provider_requests": 0}
    _evaluate(captured_window)


def test_r2f5_fixed_sqlite_catalog_roles_and_limits_are_closed(captured_window) -> None:
    assert captured_window["sqlite_catalogs"] == (
        "replication_sidecar",
        "daily_shadow",
        "shadow_registry",
        "calendar_generation",
        "universe",
    )
    assert captured_window["limits"]["max_sessions"] == 20
    assert captured_window["limits"]["max_replay_samples"] == 3
    _evaluate(captured_window)


def test_r2f5_recovery_error_nas_and_restore_window_evidence_is_not_per_session(
    captured_window,
) -> None:
    assert captured_window["forced_error_classes"] == (
        "timeout",
        "auth",
        "rate",
        "schema",
        "coverage",
        "storage",
    )
    assert captured_window["local_nas_retry_transition"] == ("queued", "retrying")
    assert captured_window["restore_verification_state"] == "verified"
    _evaluate(captured_window)


def test_r2f5_semantic_report_excludes_volatile_diagnostics(captured_window) -> None:
    assert captured_window["diagnostic_fields"] == (
        "elapsed_ms",
        "read_operations",
        "replay_sample_count",
    )
    first = deepcopy(captured_window)
    second = deepcopy(captured_window)
    first["diagnostics"] = {"elapsed_ms": 10, "read_operations": 20, "replay_sample_count": 1}
    second["diagnostics"] = {"elapsed_ms": 999, "read_operations": 99, "replay_sample_count": 3}
    assert first["diagnostics"] != second["diagnostics"]
    first_report = _as_dict(_evaluate(first))
    second_report = _as_dict(_evaluate(second))
    assert first_report["semantic_report_sha256"] == second_report["semantic_report_sha256"]


def test_r2f5_physical_fingerprint_is_unchanged_on_error(captured_window) -> None:
    dataset = Path(captured_window["local_dataset_root"])
    evidence = Path(captured_window["evidence_root"])
    before = (_physical_fingerprint(dataset), _physical_fingerprint(evidence))
    case = deepcopy(captured_window)
    case["fixture_mutation"] = "snapshot_content_changed"
    _evaluate(case)
    assert (_physical_fingerprint(dataset), _physical_fingerprint(evidence)) == before


@pytest.mark.parametrize(
    "filesystem_shape", ("symlink", "intermediate_replacement", "special", "hardlink")
)
def test_r2f5_tree_identity_rejects_unsafe_filesystem_shapes(
    captured_window, filesystem_shape
) -> None:
    dataset = Path(captured_window["local_dataset_root"])
    if filesystem_shape == "symlink":
        (dataset / "symlink").symlink_to(dataset / "manifest.json")
    elif filesystem_shape == "intermediate_replacement":
        nested = dataset / "nested"
        nested.mkdir()
        (nested / "payload").write_bytes(b"before")
        nested.rename(dataset / "nested-replaced")
        (dataset / "nested").symlink_to(dataset / "nested-replaced", target_is_directory=True)
    elif filesystem_shape == "special":
        os.mkfifo(dataset / "named-pipe")
    else:
        os.link(dataset / "manifest.json", dataset / "manifest-hardlink.json")
    case = deepcopy(captured_window)
    case["fixture_mutation"] = f"unsafe_{filesystem_shape}"
    assert _physical_fingerprint(dataset)
    _evaluate(case)


def test_r2f5_sqlite_corrupt_and_locked_inputs_remain_write_free(captured_window) -> None:
    database = Path(captured_window["control_store_roots"][0])
    database.write_bytes(b"not sqlite")
    before_corrupt = _physical_fingerprint(database.parent)
    corrupt = deepcopy(captured_window)
    corrupt["fixture_mutation"] = "corrupt_sqlite"
    _evaluate(corrupt)
    assert database.read_bytes() == b"not sqlite"
    assert _physical_fingerprint(database.parent) == before_corrupt
    _create_authoritative_catalog(database, "replication_sidecar")
    before_locked = _physical_fingerprint(database.parent)
    connection = sqlite3.connect(database, timeout=0.01)
    connection.execute("BEGIN EXCLUSIVE")
    locked = deepcopy(captured_window)
    locked["fixture_mutation"] = "locked_sqlite"
    _evaluate(locked)
    connection.rollback()
    connection.close()
    assert _physical_fingerprint(database.parent) == before_locked


@pytest.mark.parametrize("drift", ("extra_table", "missing_table", "column", "user_version"))
def test_r2f5_sqlite_catalog_drift_is_unavailable_without_migration(captured_window, drift) -> None:
    database = Path(captured_window["control_store_roots"][3])
    with sqlite3.connect(database) as connection:
        if drift == "extra_table":
            connection.execute("CREATE TABLE unexpected (id INTEGER PRIMARY KEY)")
        elif drift == "missing_table":
            connection.execute("DROP TABLE calendar_generation_meta")
        elif drift == "column":
            connection.execute("ALTER TABLE calendar_generation_meta ADD COLUMN unexpected TEXT")
        else:
            connection.execute("PRAGMA user_version=99")
    before = _physical_fingerprint(database.parent)
    case = deepcopy(captured_window)
    case["fixture_mutation"] = f"catalog_{drift}_drift"
    report = _as_dict(_evaluate(case))
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] == "CONTROL_STATE_UNAVAILABLE"
    assert _physical_fingerprint(database.parent) == before


def test_r2f5_wal_snapshot_is_readonly_and_rejects_concurrent_change(captured_window) -> None:
    database = Path(captured_window["control_store_roots"][0])
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE IF NOT EXISTS wal_probe (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO wal_probe VALUES (1)")
        connection.commit()
    before = _physical_fingerprint(database.parent)
    case = deepcopy(captured_window)
    case["fixture_mutation"] = "wal_concurrent_snapshot"
    report = _as_dict(_evaluate(case))
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] in {
        "SNAPSHOT_CHANGED",
        "CONTROL_STATE_UNAVAILABLE",
    }
    assert _physical_fingerprint(database.parent) == before


def test_r2f5_wal_writer_thread_is_external_and_reader_reports_snapshot_change(
    captured_window,
) -> None:
    import threading

    database = Path(captured_window["control_store_roots"][0])
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE wal_thread (id INTEGER PRIMARY KEY, value TEXT)")
    go = threading.Event()
    finished = threading.Event()

    def writer() -> None:
        go.wait(timeout=1)
        with sqlite3.connect(database, timeout=1) as connection:
            connection.execute("INSERT INTO wal_thread VALUES (1, 'external')")
            connection.commit()
        finished.set()

    thread = threading.Thread(target=writer)
    thread.start()
    before = _physical_fingerprint(database.parent)
    go.set()
    try:
        report = _as_dict(_evaluate(captured_window))
    finally:
        thread.join(timeout=2)
    assert finished.is_set()
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] == "SNAPSHOT_CHANGED"
    assert report["provider_requests"] == 0 and report["writes"] is False
    assert _physical_fingerprint(database.parent) != before


def test_r2f5_tree_size_limit_is_bounded_not_sampled(captured_window) -> None:
    oversized = Path(captured_window["local_dataset_root"]) / "oversized.bin"
    oversized.open("wb").truncate(536_870_913)
    before = _physical_fingerprint(oversized.parent)
    case = deepcopy(captured_window)
    case["fixture_mutation"] = "size_limit"
    report = _as_dict(_evaluate(case))
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] == "INPUT_LIMIT_EXCEEDED"
    assert _physical_fingerprint(oversized.parent) == before


def _independent_jcs(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def test_r2f5_digest_known_vector_and_self_exclusion_are_independent(captured_window) -> None:
    payload = {"z": 1, "a": "汉字", "nullable": None}
    preimage = _independent_jcs(payload)
    assert preimage == b'{"a":"\xe6\xb1\x89\xe5\xad\x97","nullable":null,"z":1}\n'
    digest = hashlib.sha256(b"r2f5/test-v1\0" + preimage).hexdigest()
    assert digest == "78bace6ee01a85ac5794470c6acfa60103a0f9f3cff40f6cadf6805667d6902b"
    case = deepcopy(captured_window)
    case["fixture_mutation"] = "digest_known_vector"
    case["expected_payload_sha256"] = digest
    _evaluate(case)


def test_r2f5_external_payload_envelope_and_semantic_vectors_are_literal() -> None:
    payload = b'{"a":1,"b":"x"}\n'
    envelope = (
        b'{"artifact_id":"a","artifact_ref":"r",'
        b'"canonicalization_version":"project-canonical-json-v1",'
        b'"created_at":"2026-01-01T00:00:00Z","creator_kind":"task20_writer",'
        b'"creator_version":"v1","payload_sha256":"00",'
        b'"schema_version":"r2f5-observation-envelope-v1"}\n'
    )
    semantic = b'{"status":"unavailable"}\n'
    assert hashlib.sha256(b"r2f5/envelope-payload-v1\0" + payload).hexdigest() == (
        "030f98b799fb67a849527d2442a4a846ead97f2af036a4b60ebdccc3a0d17a3d"
    )
    assert hashlib.sha256(b"r2f5/envelope-v1\0" + envelope).hexdigest() == (
        "eb5fcb4e1da1692d93d4ce2787828362f117a49f01d07ef2121d4fd65f5fad52"
    )
    assert hashlib.sha256(b"r2f5/semantic-report-v1\0" + semantic).hexdigest() == (
        "a58f08564f68f6d5b2bd89e78c6ae1e5aea6c1053ad4d873544083e74a25f2ff"
    )


def test_r2f5_wrong_creator_schema_or_hash_is_unavailable(captured_window) -> None:
    path = Path(captured_window["evidence_root"]) / "window.json"
    for field, value in (
        ("creator_kind", "r2f5_reader"),
        ("schema_version", "r2f4-envelope-v1"),
        ("payload_sha256", "f" * 64),
    ):
        envelope = json.loads(path.read_text(encoding="utf-8"))
        case = deepcopy(captured_window)
        case["fixture_mutation"] = f"tamper_{field}"
        envelope[field] = value
        path.write_text(json.dumps(envelope, sort_keys=True) + "\n", encoding="utf-8")
        report = _as_dict(_evaluate(case))
        assert report["status"] == "unavailable"
        assert report["canonical_integrity"]["reason_code"] == "CANONICAL_INTEGRITY_FAILED"


def test_r2f5_provider_and_socket_sentinels_are_never_called(captured_window, monkeypatch) -> None:
    calls = {"connect": 0}

    def forbidden(*_args, **_kwargs):
        calls["connect"] += 1
        raise AssertionError("provider/network I/O is forbidden")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    case = deepcopy(captured_window)
    case["fixture_mutation"] = "offline_replay"
    _evaluate(case)
    assert calls["connect"] == 0


def test_r2f5_provider_adapter_ctor_login_query_and_default_reader_are_sentinels(
    captured_window, monkeypatch
) -> None:
    module = _red_reader()
    calls = {"count": 0}

    class ForbiddenProvider:
        def __init__(self, *_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("provider construction is forbidden")

        def login(self, *_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("provider login is forbidden")

        def query(self, *_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("provider query is forbidden")

    for name in ("BaoStockProviderAdapter", "BaoStockProvider", "EvidenceReader"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, ForbiddenProvider)
    _evaluate(captured_window)
    assert calls["count"] == 0


def test_r2f5_toctou_hook_replacement_invalidates_whole_snapshot(
    captured_window, monkeypatch
) -> None:
    module = _red_reader()
    assert hasattr(module, "os")
    dataset = Path(captured_window["local_dataset_root"])
    replacement = dataset / "manifest.replacement"
    replacement.write_text("replacement\n", encoding="utf-8")
    called = {"value": False}

    original_fstat = module.os.fstat

    def fstat(fd):
        if not called["value"]:
            called["value"] = True
            os.replace(replacement, dataset / "manifest.json")
        return original_fstat(fd)

    monkeypatch.setattr(module.os, "fstat", fstat)
    report = _as_dict(_evaluate(captured_window))
    assert called["value"] is True
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] == "SNAPSHOT_CHANGED"


def test_r2f5_maximum_reference_fixture_is_bounded_and_write_free(captured_window) -> None:
    dataset = Path(captured_window["local_dataset_root"])
    rows = dataset / "100000-rows.jsonl"
    rows.write_text("".join(f'{{"row":{index}}}\n' for index in range(100_000)), encoding="utf-8")
    before = _physical_fingerprint(dataset)
    started = time.perf_counter()
    reports = [_as_dict(_evaluate(captured_window)) for _ in range(3)]
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert elapsed_ms < 10_000
    assert all(report["provider_requests"] == 0 for report in reports)
    assert all(report["writes"] is False for report in reports)
    assert _physical_fingerprint(dataset) == before


def test_r2f5_maximum_sqlite_fixture_uses_public_reader_and_p95(captured_window) -> None:
    database = Path(captured_window["control_store_roots"][0])
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE perf_rows (id INTEGER PRIMARY KEY, value TEXT)")
        connection.executemany(
            "INSERT INTO perf_rows VALUES (?, ?)",
            ((index, str(index)) for index in range(100_000)),
        )
    before = _physical_fingerprint(database.parent)
    samples = []
    for _ in range(6):
        started = time.perf_counter()
        _evaluate(captured_window)
        samples.append((time.perf_counter() - started) * 1000)
    assert sorted(samples)[4] < 10_000
    assert _physical_fingerprint(database.parent) == before


def test_r2f5_pre_capture_failure_has_typed_payload_and_no_fabricated_observations(
    captured_window,
) -> None:
    Path(captured_window["control_store_roots"][-1]).unlink()
    case = deepcopy(captured_window)
    case["fixture_mutation"] = "pre_capture_unavailable"
    roots = (
        Path(case["local_dataset_root"]),
        Path(case["evidence_root"]),
        Path(case["control_store_roots"][0]).parent,
    )
    before = tuple(_physical_fingerprint(root) for root in roots)
    report = _as_dict(_evaluate(case))
    failure = report["pre_capture_failure"]
    assert report["status"] == "unavailable"
    assert failure["schema_version"] == "r2f5-pre-capture-failure-v1"
    assert failure["reason_code"] == "CONTROL_STATE_UNAVAILABLE"
    assert report["snapshot_identity"] is None
    assert report["session_observations"] == []
    assert report["semantic_report_sha256"] == failure["semantic_report_sha256"]
    assert report["provider_requests"] == 0 and report["writes"] is False
    assert tuple(_physical_fingerprint(root) for root in roots) == before
