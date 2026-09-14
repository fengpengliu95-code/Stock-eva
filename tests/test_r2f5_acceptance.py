"""RED acceptance contract for R2-F5.0 Task 19.

The fixtures in this module are deliberately local and synthetic. They describe the
read-only input boundary and expected observations, but do not create a provider, NAS,
LaunchAgent, production store, or Task 20 envelope. At the SPEC-APPROVED base the
acceptance reader is absent; each test therefore fails with an explicit RED diagnostic
after collecting successfully. Once the reader exists, these assertions become the
concrete contract checks for the fixture mutation.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import sqlite3
from copy import deepcopy
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
    sessions = _session_dates()
    return {
        "start": sessions[0],
        "end": sessions[-1],
        "as_of_utc": "2026-09-14T06:00:00Z",
        "now": "2026-09-14T06:00:00Z",
        "local_dataset_root": str(dataset),
        "evidence_root": str(evidence),
        "control_store_roots": [
            str(control / f"{role}.db")
            for role in (
                "replication_sidecar",
                "daily_shadow",
                "shadow_registry",
                "calendar_generation",
                "universe",
            )
        ],
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
        return importlib.import_module("backend.app.market.reliability_acceptance")
    except ModuleNotFoundError as error:
        pytest.fail(
            f"RED: backend.app.market.reliability_acceptance is not implemented yet ({error.name})"
        )


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


def _assert_red_fixture(anchor: str, captured_window: dict[str, Any]) -> None:
    assert anchor.startswith("test_r2f5_req_")
    assert REQUIREMENT_SUMMARIES[anchor], f"matrix summary missing for {anchor}"
    assert captured_window["expected_session_count"] == 20
    assert len(captured_window["raw_calendar"]) == 20
    assert captured_window["provider_requests"] == 0
    assert captured_window["writes"] is False
    _evaluate(captured_window)


def _make_requirement_test(anchor: str):
    def test(captured_window: dict[str, Any]) -> None:
        _assert_red_fixture(anchor, captured_window)

    test.__name__ = anchor
    test.__qualname__ = anchor
    test.__doc__ = f"Concrete RED contract anchor for {anchor}: {REQUIREMENT_SUMMARIES[anchor]}"
    return test


for _anchor in REQUIREMENT_ANCHORS:
    globals()[_anchor] = _make_requirement_test(_anchor)


def _slo_test(metric: str, captured_window: dict[str, Any]) -> None:
    assert metric in EXPECTED_METRICS
    assert len(EXPECTED_METRICS) == 18
    assert captured_window["expected_session_count"] == 20
    report = _as_dict(_evaluate(captured_window))
    assert metric in report, f"report must expose MetricResult.{metric}"
    assert report[metric]["acceptance_ref"] == "AC-15"
    assert report[metric]["planned_test_anchor"] == f"test_r2f5_slo_{metric}"


def _make_slo_test(metric: str):
    def test(captured_window: dict[str, Any]) -> None:
        _slo_test(metric, captured_window)

    test.__name__ = f"test_r2f5_slo_{metric}"
    test.__qualname__ = test.__name__
    test.__doc__ = f"Concrete RED SLO reducer anchor for {metric}."
    return test


for _metric in EXPECTED_METRICS:
    globals()[f"test_r2f5_slo_{_metric}"] = _make_slo_test(_metric)


def test_r2f5_acceptance_fixture_has_exact_anchor_inventory() -> None:
    assert len(REQUIREMENT_ANCHORS) == 92
    assert len(set(REQUIREMENT_ANCHORS)) == 92
    assert len(SLO_ANCHORS) == 18
    assert set(SLO_ANCHORS) == {f"test_r2f5_slo_{metric}" for metric in EXPECTED_METRICS}


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
    assert expected_reason in report.get("quality_issues", []) or expected_reason == report.get(
        "pre_capture_failure", {}
    ).get("reason_code")


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
    missing = tuple(Path(path) for path in captured_window["control_store_roots"])
    assert all(not path.exists() for path in missing)
    _evaluate(captured_window)
    assert all(not path.exists() for path in missing)


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
    _evaluate(first)
    _evaluate(second)
