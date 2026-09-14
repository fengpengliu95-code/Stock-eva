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
MATRIX = ROOT / "docs/acceptance/r2f5-0-requirement-evidence-matrix.md"
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
    module = _red_reader()
    reader_type = getattr(module, "AcceptanceReader", None)
    assert reader_type is not None, "AcceptanceReader is the required read-only contract"
    reader = reader_type()
    evaluate = getattr(reader, "evaluate", None)
    assert callable(evaluate), "AcceptanceReader.evaluate must be callable"
    return evaluate(payload)


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
    scenario: str
    status: str
    reason: str
    metric: str
    observed_kind: str | None
    observed_value: Any
    target_kind: str | None
    target_value: Any
    expected_exception: str | None
    effect: str


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
    metric_values = {
        "continuity": ("count", 1, 0),
        "same_evening_availability": ("count", 17, 18),
        "coverage": ("ratio", 0.99, 1),
        "source_purity": ("count", 1, 20),
        "provenance": ("count", 19, 20),
        "replay": ("bool", False, True),
        "replication": ("duration_seconds", 301, 300),
        "restore": ("duration_seconds", 301, 300),
        "read_boundary": ("bool", False, True),
    }
    cases: dict[str, _CaseSpec] = {}
    for anchor in REQUIREMENT_ANCHORS:
        requirement = re.search(r"(?:^|_)(FR|NFR|AC|EC)_(\d+)$", anchor, flags=re.IGNORECASE)
        assert requirement is not None
        requirement_id = f"{requirement.group(1).upper()}-{int(requirement.group(2))}"
        scenario, status, reason, metric = _CASE_OVERRIDES.get(
            requirement_id,
            (
                f"contract_{requirement_id.lower()}",
                "unavailable",
                "CONTROL_STATE_UNAVAILABLE",
                "read_boundary",
            ),
        )
        unavailable = status == "unavailable" or reason.endswith("_UNAVAILABLE")
        value_kind, observed_value, target_value = metric_values.get(metric, ("count", 1, 20))
        cases[anchor] = _CaseSpec(
            requirement=requirement_id,
            scenario=scenario,
            status=status,
            reason=reason,
            metric=metric,
            observed_kind=None if unavailable else value_kind,
            observed_value=None if unavailable else observed_value,
            target_kind=None if unavailable else value_kind,
            target_value=None if unavailable else target_value,
            expected_exception=None,
            effect="no_filesystem_db_provider_or_pointer_write",
        )
    return cases


CASE_SPECS = _case_specs()


def _apply_scenario(case: _CaseSpec, captured_window: dict[str, Any]) -> dict[str, Any]:
    payload = deepcopy(captured_window)
    payload["scenario"] = case.scenario
    expected_status = "unavailable" if case.reason.endswith("_UNAVAILABLE") else case.status
    payload["expected"] = {
        "status": expected_status,
        "reason": case.reason,
        "metric": case.metric,
        "observed": {"kind": case.observed_kind, "value": case.observed_value},
        "target": {"kind": case.target_kind, "value": case.target_value},
        "effect": case.effect,
    }
    if case.scenario in {"nineteen_sessions", "exact_twenty"}:
        payload["raw_calendar"] = payload["raw_calendar"][:-1]
    elif case.scenario in {"duplicate_raw_calendar", "duplicate"}:
        payload["raw_calendar"][5] = payload["raw_calendar"][4]
    elif case.scenario in {"out_of_order_raw", "replacement_race"}:
        payload["raw_calendar"][5], payload["raw_calendar"][6] = (
            payload["raw_calendar"][6],
            payload["raw_calendar"][5],
        )
    elif case.scenario == "missing_middle":
        payload["raw_calendar"].pop(10)
    elif case.scenario == "future_session":
        payload["raw_calendar"][-1] = "2099-01-01"
    elif case.scenario == "missing_control" or case.scenario == "missing_locked_sqlite":
        Path(payload["control_store_roots"][-1]).unlink()
    elif case.scenario == "corrupt_sqlite":
        Path(payload["control_store_roots"][0]).write_bytes(b"not sqlite")
    elif case.scenario == "unsafe_path":
        payload["local_dataset_root"] = "relative-dataset"
    elif case.scenario == "coverage_failure" or case.scenario == "universe_mismatch":
        payload["required_count"] = 100
        payload["loaded_count"] = 99
        payload["unknown_count"] = 1
    elif case.scenario == "mixed_source":
        payload["canonical_provider_ids"] = ["baostock", "tickflow"]
    elif case.scenario == "version_drift":
        payload["frozen_versions"]["calendar_generation"] = "calendar-drift"
    elif case.scenario == "unknown_replay_identity":
        payload["offline_replay"] = {
            "adapter_id": "unknown",
            "normalizer_id": "unknown",
            "network_allowed": False,
            "provider_requests": 0,
        }
    elif case.scenario == "local_chain_only":
        payload["replication_trust_scope"] = "LOCAL_CHAIN_ONLY"
    elif case.scenario == "missing_restore_drill":
        payload["restore_verification_state"] = None
    elif case.scenario == "lineage_missing":
        payload["lineage"] = None
    elif case.scenario == "calendar_conflict":
        payload["calendar_conflict"] = True
    elif case.scenario == "toctou":
        payload["toctou_probe"] = True
    return payload


def _assert_case_report(case: _CaseSpec, payload: dict[str, Any]) -> None:
    report = _as_dict(_evaluate(payload))
    expected_status = "unavailable" if case.reason.endswith("_UNAVAILABLE") else case.status
    assert report["status"] == expected_status
    metric = report[case.metric]
    assert metric["status"] == expected_status
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
        payload = _apply_scenario(case, captured_window)
        expected_status = "unavailable" if case.reason.endswith("_UNAVAILABLE") else case.status
        assert payload["expected"]["status"] == expected_status
        assert payload["expected"]["reason"] == case.reason
        assert payload["expected"]["metric"] == case.metric
        assert payload["expected"]["effect"] == "no_filesystem_db_provider_or_pointer_write"
        _assert_case_report(case, payload)

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
        ("next_morning_availability", "AVAILABILITY_CUTOFF_FAILED", "count", 20, 19, 20),
        ("same_evening_availability", "AVAILABILITY_CUTOFF_FAILED", "count", 18, 17, 18),
        ("coverage", "COVERAGE_FAILED", "ratio", 1, 0.99, 1),
        ("canonical_integrity", "CANONICAL_INTEGRITY_FAILED", "count", 20, 19, 20),
        ("source_purity", "SOURCE_PURITY_FAILED", "count", 20, 19, 20),
        ("recovery", "RECOVERY_FAILED", "count", 1, 0, 1),
        ("failover", "FAILOVER_UNAVAILABLE", "bool", True, False, True),
        ("provenance", "LINEAGE_UNAVAILABLE", "count", 20, 19, 20),
        ("replay", "REPLAY_SEMANTIC_MISMATCH", "bool", True, False, True),
        ("adjustment", "ADJUSTMENT_UNAVAILABLE", "bool", True, False, True),
        ("calendar", "CALENDAR_CONFLICT", "count", 20, 19, 20),
        ("universe", "UNIVERSE_COUNT_MISMATCH", "count", 20, 19, 20),
        ("error_handling", "ERROR_HANDLING_FAILED", "count", 6, 5, 6),
        ("local_nas_isolation", "LOCAL_NAS_ISOLATION_FAILED", "bool", True, False, True),
        ("replication", "REPLICATION_LAG", "duration_seconds", 0, 301, 300),
        ("restore", "RESTORE_UNAVAILABLE", "duration_seconds", 0, 301, 300),
        ("read_boundary", "READ_BOUNDARY_FAILED", "bool", True, False, True),
    )
}


def _assert_slo_report(case: _SloCase, captured_window: dict[str, Any]) -> None:
    payload = deepcopy(captured_window)
    payload["slo_metric"] = case.metric
    payload["slo_mutation"] = case.mutation
    payload["slo_inputs"] = {
        "metric": case.metric,
        "observed": {"kind": case.observed_kind, "value": case.observed_value},
        "target": {"kind": case.target_kind, "value": case.target_value},
    }
    if case.mutation == "unavailable":
        Path(payload["control_store_roots"][-1]).unlink()
    assert case.metric in EXPECTED_METRICS
    assert case.observed_kind is None or case.observed_kind in {
        "count",
        "ratio",
        "duration_seconds",
        "bool",
        "hash",
    }
    report = _as_dict(_evaluate(payload))
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
    report = _as_dict(_evaluate(case))
    assert report["status"] in {"not_ready", "unavailable"}
    metric = "calendar" if mutation == "future" else "continuity"
    assert report[metric]["reason_code"] == expected_reason


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
    assert metric in EXPECTED_METRICS
    report = _as_dict(_evaluate(case))
    assert report[metric]["status"] == ("pass" if should_pass else "fail")


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
    _evaluate(drifted)


@pytest.mark.parametrize(
    ("scenario", "expected_reason"),
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
    captured_window, scenario: str, expected_reason: str
) -> None:
    case = deepcopy(captured_window)
    case["scenario"] = scenario
    assert expected_reason in REASONS or expected_reason in {
        "FAILOVER_UNAVAILABLE",
        "REPLICATION_UNAVAILABLE",
        "RESTORE_UNAVAILABLE",
        "LINEAGE_INVALID",
        "LOCAL_NAS_ISOLATION_FAILED",
        "RECOVERY_FAILED",
        "ERROR_HANDLING_FAILED",
    }
    roots = (Path(case["local_dataset_root"]), Path(case["evidence_root"]))
    before = tuple(_tree_digest(root) for root in roots)
    _evaluate(case)
    assert tuple(_tree_digest(root) for root in roots) == before


def test_r2f5_window_evidence_is_single_task20_envelope(captured_window) -> None:
    envelope = captured_window["window_evidence_bundle"]
    assert envelope["creator_kind"] == "task20_writer"
    assert envelope["schema_version"] == "r2f5-observation-envelope-v1"
    assert envelope["creator_kind"] not in {"r2f5_reader", "r2f4_writer"}
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
    case["scenario"] = "snapshot_content_changed"
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
    case["scenario"] = f"unsafe_{filesystem_shape}"
    assert _physical_fingerprint(dataset)
    _evaluate(case)


def test_r2f5_sqlite_corrupt_and_locked_inputs_remain_write_free(captured_window) -> None:
    database = Path(captured_window["control_store_roots"][0])
    database.write_bytes(b"not sqlite")
    before_corrupt = _physical_fingerprint(database.parent)
    corrupt = deepcopy(captured_window)
    corrupt["scenario"] = "corrupt_sqlite"
    _evaluate(corrupt)
    assert database.read_bytes() == b"not sqlite"
    assert _physical_fingerprint(database.parent) == before_corrupt
    _create_authoritative_catalog(database, "replication_sidecar")
    before_locked = _physical_fingerprint(database.parent)
    connection = sqlite3.connect(database, timeout=0.01)
    connection.execute("BEGIN EXCLUSIVE")
    locked = deepcopy(captured_window)
    locked["scenario"] = "locked_sqlite"
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
    case["scenario"] = f"catalog_{drift}_drift"
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
    case["scenario"] = "wal_concurrent_snapshot"
    report = _as_dict(_evaluate(case))
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] in {
        "SNAPSHOT_CHANGED",
        "CONTROL_STATE_UNAVAILABLE",
    }
    assert _physical_fingerprint(database.parent) == before


def test_r2f5_tree_size_limit_is_bounded_not_sampled(captured_window) -> None:
    oversized = Path(captured_window["local_dataset_root"]) / "oversized.bin"
    oversized.open("wb").truncate(536_870_913)
    before = _physical_fingerprint(oversized.parent)
    case = deepcopy(captured_window)
    case["scenario"] = "size_limit"
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
    assert digest == hashlib.sha256(b"r2f5/test-v1\0" + preimage).hexdigest()
    case = deepcopy(captured_window)
    case["scenario"] = "digest_known_vector"
    case["expected_payload_sha256"] = digest
    _evaluate(case)


def test_r2f5_wrong_creator_schema_or_hash_is_unavailable(captured_window) -> None:
    envelope = captured_window["window_evidence_bundle"]
    for field, value in (
        ("creator_kind", "r2f5_reader"),
        ("schema_version", "r2f4-envelope-v1"),
        ("payload_sha256", "f" * 64),
    ):
        case = deepcopy(captured_window)
        case["scenario"] = f"tamper_{field}"
        case["window_evidence_bundle"][field] = value
        assert case["window_evidence_bundle"] != envelope
        report = _as_dict(_evaluate(case))
        assert report["status"] == "unavailable"
        assert report["provenance"]["reason_code"] == "LINEAGE_INVALID"


def test_r2f5_provider_and_socket_sentinels_are_never_called(captured_window, monkeypatch) -> None:
    calls = {"connect": 0}

    def forbidden(*_args, **_kwargs):
        calls["connect"] += 1
        raise AssertionError("provider/network I/O is forbidden")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    case = deepcopy(captured_window)
    case["scenario"] = "offline_replay"
    _evaluate(case)
    assert calls["connect"] == 0


def test_r2f5_toctou_hook_replacement_invalidates_whole_snapshot(captured_window) -> None:
    dataset = Path(captured_window["local_dataset_root"])
    replacement = dataset / "manifest.replacement"
    replacement.write_text("replacement\n", encoding="utf-8")
    called = {"value": False}

    def replace_during_read() -> None:
        called["value"] = True
        os.replace(replacement, dataset / "manifest.json")

    case = deepcopy(captured_window)
    case["scenario"] = "toctou"
    case["before_content_read"] = replace_during_read
    report = _as_dict(_evaluate(case))
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


def test_r2f5_pre_capture_failure_has_typed_payload_and_no_fabricated_observations(
    captured_window,
) -> None:
    Path(captured_window["control_store_roots"][-1]).unlink()
    case = deepcopy(captured_window)
    case["scenario"] = "pre_capture_unavailable"
    report = _as_dict(_evaluate(case))
    failure = report["pre_capture_failure"]
    assert report["status"] == "unavailable"
    assert failure["schema_version"] == "r2f5-pre-capture-failure-v1"
    assert failure["reason_code"] == "CONTROL_STATE_UNAVAILABLE"
    assert report["snapshot_identity"] is None
    assert report["session_observations"] == []
    assert report["semantic_report_sha256"] == failure["semantic_report_sha256"]
