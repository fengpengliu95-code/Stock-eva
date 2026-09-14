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
import shutil
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

import httpx
import pytest

import backend.app.cli as cli_module
from backend.app.config import Settings
from backend.app.main import app

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
    "REMOTE_PROOF_MISSING",
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


def _jcs(value: Any) -> bytes:
    """Test writer JCS: compact sorted UTF-8 JSON, independent of production."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _task20_envelope(payload: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    metadata = {
        "artifact_id": artifact_id,
        "artifact_ref": f"evidence/{artifact_id}",
        "schema_version": "r2f5-observation-envelope-v1",
        "creator_kind": "task20_writer",
        "creator_version": "v1",
        "created_at": "2026-09-14T06:00:00Z",
        "canonicalization_version": "project-canonical-json-v1",
        "payload_sha256": hashlib.sha256(_jcs(payload)).hexdigest(),
    }
    return {
        **metadata,
        "payload": payload,
        "envelope_sha256": hashlib.sha256(b"r2f5/envelope-v1\0" + _jcs(metadata)).hexdigest(),
    }


@dataclass
class GoldenTree:
    request: dict[str, Any]
    dataset: Path
    evidence: Path
    controls: list[Path]
    sessions: list[str]
    session_observations: list[dict[str, Any]]
    window_envelope: dict[str, Any]
    snapshot_identity: dict[str, Any]
    frozen_versions: dict[str, Any]


def _build_complete_golden_tree(tmp_path: Path) -> GoldenTree:
    """Build the complete approved tree; no public input flags encode its state."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    dataset, evidence, control = (
        tmp_path / name for name in ("golden-dataset", "golden-evidence", "golden-control")
    )
    for root in (dataset, evidence, control):
        root.mkdir()
    sessions = _session_dates()
    frozen = {
        "git_commit": "1" * 40,
        "installed_release": "stock-eva-r2f5.0",
        "dataset_generation": "g20",
        "primary_provider_id": "baostock",
        "secondary_provider_id": "tickflow",
        "calendar_generation": "calendar-g20",
        "universe_generation": "universe-g20",
        "replication_policy_version": "r2f4-replication-v1",
        "restore_policy_version": "r2f4-restore-v1",
    }
    manifest = {
        "dataset": "stock-eva-market",
        "generation": "g20",
        "schema_version": 2,
        "sessions": sessions,
    }
    (dataset / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8"
    )
    (dataset / "calendar.json").write_text(
        json.dumps(
            {
                "source_sequence": sessions,
                "generation": "calendar-g20",
                "confirmed": True,
                "unknown_state": False,
                "conflict_state": False,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    session_observations = []
    for ordinal, session in enumerate(sessions, start=1):
        digest = hashlib.sha256(_jcs({"session": session, "ordinal": ordinal})).hexdigest()

        def metric(kind: str, value: Any, target: Any) -> dict[str, Any]:
            return {
                "status": "pass",
                "observed": {"kind": kind, "value": value},
                "target": {"kind": kind, "value": target},
                "reason_code": None,
                "acceptance_ref": "AC-15",
                "planned_test_anchor": "test_r2f5_slo",
            }

        observation = {
            "session": session,
            "ordinal": ordinal,
            "frozen_versions_sha256": digest,
            "same_evening_published_at": f"{session}T13:15:00Z",
            "next_morning_published_at": f"{session}T00:00:00Z",
            "required_count": 1,
            "loaded_count": 1,
            "suspension_count": 0,
            "not_listed_count": 0,
            "delisted_count": 0,
            "unknown_count": 0,
            "canonical_provider_ids": ["baostock"],
            "evidence": {"evidence_id": f"e-{ordinal:02d}", "evidence_sha256": digest},
            "lineage": {
                "lineage_id": f"lineage-{ordinal:02d}",
                "source_id": f"source-{ordinal:02d}",
                "candidate_id": f"candidate-{ordinal:02d}",
                "gate_report_id": f"gate-{ordinal:02d}",
                "lineage_sha256": digest,
            },
            "manifest": {
                "manifest_id": f"manifest-{ordinal:02d}",
                "manifest_sha256": digest,
                "generation": "g20",
                "schema_version": 2,
            },
            "pointer_reconciliation": {
                "pointer_id": f"p-{ordinal:02d}",
                "pointer_sha256": digest,
                "manifest_sha256": digest,
                "object_sha256": digest,
                "pointer_manifest_object_match": True,
                "descriptor_sha256": digest,
            },
            "replication_observation": {
                "immutable": True,
                "state": "completed",
                "checkpoint_id": f"cp-{ordinal:02d}",
                "source_commit_sha256": digest,
                "intent_id": f"intent-{ordinal:02d}",
                "enqueue_state": "enqueued",
                "reason_code": None,
                "observed_at": "2026-09-14T06:00:00Z",
                "lag_seconds": 0,
                "trust_scope": "REMOTE_VERIFIED",
                "destination_generation": "g20",
                "destination_record_sha256": digest,
                "destination_head_sha256": digest,
                "observation_sha256": digest,
            },
            "calendar_raw_facts": {
                "source_sequence": sessions,
                "generation": "calendar-g20",
                "confirmed": True,
                "unknown_state": False,
                "conflict_state": False,
                "raw_facts_sha256": digest,
            },
            "universe_facts": {
                "required_count": 1,
                "loaded_count": 1,
                "unknown_count": 0,
                "universe_generation": "universe-g20",
                "universe_sha256": digest,
            },
            "read_boundary_raw_facts": {
                "requested_as_of": "2026-09-14T06:00:00Z",
                "max_visible_session": session,
                "future_rows_seen": False,
                "future_rows_count": 0,
                "query_count": 1,
                "write_count": 0,
                "probe_schema_digest": digest,
            },
            "schema_policy_versions": ["r2f5-schema-v1"],
            "schema_policy_digest": digest,
            "cutoff_results": {
                "same_evening": metric("ratio", 0.9, 0.9),
                "next_morning": metric("ratio", 1, 1),
            },
            "coverage": metric("ratio", 1, 1),
            "canonical_integrity": metric("bool", True, True),
            "source_purity": metric("bool", True, True),
            "provenance": metric("bool", True, True),
            "calendar": metric("bool", True, True),
            "universe": metric("bool", True, True),
            "replication": metric("duration_seconds", 0, 300),
            "read_boundary": metric("bool", True, True),
            "observation_sha256": digest,
        }
        observation["evidence"] = {
            "evidence_id": f"e-{ordinal:02d}",
            "evidence_sha256": digest,
            "candidate_id": f"candidate-{ordinal:02d}",
            "candidate_sha256": digest,
            "gate_report_id": f"gate-{ordinal:02d}",
            "gate_report_sha256": digest,
            "manifest_id": f"manifest-{ordinal:02d}",
            "manifest_sha256": digest,
            "object_id": f"object-{ordinal:02d}",
            "object_sha256": digest,
            "selection_id": f"selection-{ordinal:02d}",
            "selection_sha256": digest,
            "binding_sha256": digest,
        }
        observation["observation_sha256"] = hashlib.sha256(
            _jcs({key: value for key, value in observation.items() if key != "observation_sha256"})
        ).hexdigest()
        session_observations.append(observation)
        (dataset / "sessions").mkdir(exist_ok=True)
        (dataset / "sessions" / f"{session}.json").write_text(
            json.dumps(observation, sort_keys=True) + "\n", encoding="utf-8"
        )
    child_payloads = {
        "recovery_observation": {
            "immutable": True,
            "event_id": "recovery-001",
            "attempt_id": "attempt-001",
            "before_generation": "g19",
            "after_generation": "g20",
            "queue_identity": "queue-001",
            "restart_boundary": "2026-09-14T05:00:00Z",
            "exactly_once_publication_id": "publication-001",
            "publication_count": 1,
        },
        "failover_observation": {
            "source_schema": "task20-writer-owned",
            "primary_unavailable": True,
            "qualified_secondary_provider_id": "tickflow",
            "qualification_proof_status": "available",
            "session": sessions[0],
            "selected_provider_id": "tickflow",
            "mixed_source_rows": 0,
        },
        "replay_sample": {
            "sample_object_sha256": "2" * 64,
            "candidate_sha256": "3" * 64,
            "semantic_equal": True,
            "offline_context": {
                "adapter_id": "baostock",
                "adapter_version": "offline-v1",
                "normalizer_id": "market",
                "normalizer_version": "v1",
                "implementation_sha256": "4" * 64,
                "network_allowed": False,
                "provider_requests": 0,
            },
        },
        "adjustment_equivalence": {
            "compared_sessions": sessions[:2],
            "tolerance_policy_version": "r2f5-adjustment-v1",
            "equivalence_passed": True,
        },
        "error_handling_observation": {
            "immutable": True,
            "events": [
                {
                    "event_id": f"error-{i}",
                    "forced_error_class": klass,
                    "sanitized_reason": "ERROR_HANDLING_FAILED",
                    "normalized_result": "unavailable",
                    "attempt_id": f"attempt-{i}",
                    "expected_class": klass,
                    "observed_class": klass,
                    "evidence_sha256": "5" * 64,
                    "observed_at": "2026-09-14T06:00:00Z",
                }
                for i, klass in enumerate(
                    ("auth", "schema", "storage", "timeout", "decode", "unknown")
                )
            ],
            "observation_sha256": "6" * 64,
        },
        "local_nas_isolation_observation": {
            "immutable": True,
            "event_id": "nas-001",
            "local_publication_ready": True,
            "local_publication_id": "local-001",
            "local_pointer_sha256": "7" * 64,
            "outage_start": "2026-09-14T05:00:00Z",
            "outage_end": "2026-09-14T05:01:00Z",
            "backlog_before_ids": [],
            "backlog_after_ids": [],
            "backlog_before_count": 0,
            "backlog_after_count": 0,
            "lag_seconds": 0,
            "lag_threshold_seconds": 300,
            "retryable": True,
            "retry_state": "complete",
            "retry_transition": "queued_to_complete",
            "nas_failure_did_not_block_local": True,
            "attempt_id": "nas-attempt-001",
            "observed_at": "2026-09-14T06:00:00Z",
            "observation_sha256": "8" * 64,
        },
        "restore_observation": {
            "source_schema": "task20-writer-owned",
            "sentinel_sha256": "9" * 64,
            "destination_id": "restore-001",
            "destination_generation": "g20",
            "destination_head_sha256": "a" * 64,
            "record_sha256": "b" * 64,
            "manifest_sha256": "c" * 64,
            "checkpoint_id": "cp-restore-001",
            "restore_report_id": "restore-report-001",
            "restore_report_sha256": "d" * 64,
            "schema_version": "r2f5-restore-v1",
            "row_count": 20,
            "api_readback_sha256": "e" * 64,
            "verification_state": "verified",
        },
    }
    child_payloads["recovery_observation"].update(
        {
            "after_manifest_sha256": "f" * 64,
            "after_pointer_sha256": "f" * 64,
            "after_selection_sha256": "f" * 64,
            "duplicate_proof_sha256": "f" * 64,
            "observed_at": "2026-09-14T06:00:00Z",
            "observation_sha256": "f" * 64,
        }
    )
    child_payloads["failover_observation"].update(
        {
            "selection_sha256": "f" * 64,
            "manifest_sha256": "f" * 64,
            "pointer_sha256": "f" * 64,
            "readback_sha256": "f" * 64,
        }
    )
    for payload in child_payloads.values():
        payload["observation_sha256"] = hashlib.sha256(
            _jcs({key: value for key, value in payload.items() if key != "observation_sha256"})
        ).hexdigest()
    bundle_payload = {
        key: _task20_envelope(value, key.replace("_observation", "") + "-001")
        for key, value in child_payloads.items()
    }
    bundle_payload["observation_count"] = 1
    window_envelope = _task20_envelope(bundle_payload, "window-001")
    (evidence / "window.json").write_text(
        json.dumps(window_envelope, sort_keys=True) + "\n", encoding="utf-8"
    )
    (evidence / "session_observations.json").write_text(
        json.dumps(session_observations, sort_keys=True) + "\n", encoding="utf-8"
    )
    controls = []
    for role in (
        "replication_sidecar",
        "daily_shadow",
        "shadow_registry",
        "calendar_generation",
        "universe",
    ):
        path = control / f"{role}.db"
        _create_authoritative_catalog(path, role)
        controls.append(path)
    descriptors = [
        {
            "descriptor_role": "dataset",
            "descriptor_id": "manifest",
            "descriptor_state": "available",
            "sha256": hashlib.sha256((dataset / "manifest.json").read_bytes()).hexdigest(),
        },
        {
            "descriptor_role": "evidence",
            "descriptor_id": "window-001",
            "descriptor_state": "available",
            "sha256": hashlib.sha256((evidence / "window.json").read_bytes()).hexdigest(),
        },
    ] + [
        {
            "descriptor_role": role,
            "descriptor_id": path.name,
            "descriptor_state": "available",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for role, path in zip(
            (
                "replication_sidecar",
                "daily_shadow",
                "shadow_registry",
                "calendar_generation",
                "universe",
            ),
            controls,
            strict=True,
        )
    ]
    snapshot = {
        "requested_start": sessions[0],
        "requested_end": sessions[-1],
        "as_of_utc": "2026-09-14T06:00:00Z",
        "as_of_timezone": "Asia/Shanghai",
        "input_fingerprints": descriptors,
        "frozen_versions": frozen,
    }
    snapshot["input_fingerprint_sha256"] = hashlib.sha256(_jcs(descriptors)).hexdigest()
    snapshot["frozen_version_vector_sha256"] = hashlib.sha256(_jcs(frozen)).hexdigest()
    snapshot["snapshot_sha256"] = hashlib.sha256(
        _jcs({key: value for key, value in snapshot.items() if key != "snapshot_sha256"})
    ).hexdigest()
    (evidence / "frozen_versions.json").write_text(
        json.dumps(frozen, sort_keys=True) + "\n", encoding="utf-8"
    )
    (evidence / "snapshot_identity.json").write_text(
        json.dumps(snapshot, sort_keys=True) + "\n", encoding="utf-8"
    )
    return GoldenTree(
        {
            "start": sessions[0],
            "end": sessions[-1],
            "local_dataset_root": str(dataset),
            "evidence_root": str(evidence),
            "control_store_roots": [str(path) for path in controls],
            "now": "2026-09-14T06:00:00Z",
        },
        dataset,
        evidence,
        controls,
        sessions,
        session_observations,
        window_envelope,
        snapshot,
        frozen,
    )


def _clone_golden_tree(source: GoldenTree, destination: Path) -> GoldenTree:
    dataset, evidence, control = (destination / name for name in ("dataset", "evidence", "control"))
    shutil.copytree(source.dataset, dataset)
    shutil.copytree(source.evidence, evidence)
    shutil.copytree(source.controls[0].parent, control)
    controls = [control / path.name for path in source.controls]
    request = {
        **source.request,
        "local_dataset_root": str(dataset),
        "evidence_root": str(evidence),
        "control_store_roots": [str(path) for path in controls],
    }
    return GoldenTree(
        request,
        dataset,
        evidence,
        controls,
        source.sessions,
        source.session_observations,
        source.window_envelope,
        source.snapshot_identity,
        source.frozen_versions,
    )


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
    calendar_path = dataset / "calendar.json"
    if calendar_path.exists():
        calendar_document = json.loads(calendar_path.read_text(encoding="utf-8"))
        raw_calendar = list(calendar_document.get("source_sequence", ()))
    else:
        # The minimal RED fixture intentionally omits this artifact; the local
        # builder creates the authoritative input before applying its mutation.
        raw_calendar = _session_dates()
        calendar_path.write_text(
            json.dumps({"source_sequence": raw_calendar}, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def write_calendar(values: list[str], **updates: Any) -> None:
        calendar_path.write_text(
            json.dumps({"source_sequence": values, **updates}, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def mutate_session(**updates: Any) -> None:
        session_path = dataset / "sessions" / f"{raw_calendar[0]}.json"
        session = (
            json.loads(session_path.read_text(encoding="utf-8")) if session_path.exists() else {}
        )
        session.update(updates)
        session_path.parent.mkdir(exist_ok=True)
        session_path.write_text(json.dumps(session, sort_keys=True) + "\n", encoding="utf-8")

    if case.mutation in {"nineteen_sessions", "exact_twenty"}:
        raw_calendar = raw_calendar[:-1]
        write_calendar(raw_calendar)
    elif case.mutation in {"duplicate_raw_calendar", "duplicate"}:
        raw_calendar[5] = raw_calendar[4]
        write_calendar(raw_calendar)
    elif case.mutation in {"out_of_order_raw", "replacement_race"}:
        raw_calendar[5], raw_calendar[6] = raw_calendar[6], raw_calendar[5]
        write_calendar(raw_calendar)
    elif case.mutation == "missing_middle":
        raw_calendar.pop(10)
        write_calendar(raw_calendar)
    elif case.mutation == "future_session":
        raw_calendar[-1] = "2099-01-01"
        write_calendar(raw_calendar)
    elif case.mutation == "missing_control" or case.mutation == "missing_locked_sqlite":
        missing = Path(payload["control_store_roots"][-1])
        if missing.exists():
            missing.unlink()
    elif case.mutation == "corrupt_sqlite":
        Path(payload["control_store_roots"][0]).write_bytes(b"not sqlite")
    elif case.mutation == "unsafe_path":
        payload["local_dataset_root"] = "relative-dataset"
    elif case.mutation == "coverage_failure" or case.mutation == "universe_mismatch":
        mutate_session(required_count=100, loaded_count=99, unknown_count=1)
    elif case.mutation == "mixed_source":
        mutate_session(canonical_provider_ids=["baostock", "tickflow"])
    elif case.mutation == "version_drift":
        frozen_path = evidence / "frozen_versions.json"
        frozen = json.loads(frozen_path.read_text(encoding="utf-8")) if frozen_path.exists() else {}
        frozen["calendar_generation"] = "calendar-drift"
        frozen_path.write_text(json.dumps(frozen, sort_keys=True) + "\n", encoding="utf-8")
        manifest = json.loads((dataset / "manifest.json").read_text(encoding="utf-8"))
        manifest["generation"] = "calendar-drift"
        (dataset / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8"
        )
    elif case.mutation == "unknown_replay_identity":
        (evidence / "replay-identity.json").write_text(
            json.dumps(
                {"adapter_id": "unknown", "normalizer_id": "unknown", "network_allowed": False}
            )
            + "\n",
            encoding="utf-8",
        )
    elif case.mutation == "local_chain_only":
        mutate_session(replication_observation={"trust_scope": "LOCAL_CHAIN_ONLY"})
    elif case.mutation == "missing_restore_drill":
        (evidence / "restore_observation.json").unlink(missing_ok=True)
    elif case.mutation == "lineage_missing":
        (dataset / "sessions" / f"{raw_calendar[0]}.json").unlink(missing_ok=True)
    elif case.mutation == "calendar_conflict":
        write_calendar(raw_calendar, conflict_state=True)
    elif case.mutation == "toctou":
        (evidence / "toctou-probe.json").write_text("before\n", encoding="utf-8")
    # Every remaining named drill has a material immutable evidence mutation;
    # the reader receives no fixture-control key for it.
    target_name = f"{case.requirement.lower()}.json"
    (evidence / target_name).write_text(
        json.dumps(
            {
                "artifact_id": case.requirement.lower(),
                "creator_kind": "task20_writer",
                "metric": case.metric,
                "expected_status": case.status,
                "expected_reason": case.reason,
                "effect": case.effect,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    # Bind the named mutation into the authoritative Task 20 window bundle so
    # it is on the future reader's input graph, rather than being an orphan
    # marker that only the test itself can see.
    window_path = evidence / "window.json"
    try:
        window = json.loads(window_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        window = _task20_envelope({}, "window-001")
    window_payload = dict(window.get("payload", {}))
    window_payload[case.requirement.lower()] = json.loads(
        (evidence / target_name).read_text(encoding="utf-8")
    )
    window = _task20_envelope(window_payload, str(window.get("artifact_id", "window-001")))
    window_path.write_text(json.dumps(window, sort_keys=True) + "\n", encoding="utf-8")
    return {key: payload[key] for key in INPUT_KEYS}


def _anchor_builder(case: _CaseSpec):
    def build(captured: dict[str, Any]) -> dict[str, Any]:
        return _build_case_input(case, captured)

    build.__name__ = f"build_{case.requirement.lower()}"
    build.target_descriptor = "evidence_root"
    build.mutated_path = f"{case.requirement.lower()}.json"
    build.expected = {
        "metric": case.metric,
        "status": case.status,
        "reason": case.reason,
    }
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
    # A measured SLO breach is a discriminated MetricResult ``fail`` even when
    # its reason name contains UNAVAILABLE.  Only absent/corrupt evidence uses
    # the separate unavailable branch below.
    failed_kind = kind
    failed_value = failed
    failed_target = target
    return (
        _SloCase(metric, "pass", "ready", "pass", None, kind, passed, kind, target),
        _SloCase(
            metric,
            "fail",
            "not_ready",
            "fail",
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


def test_r2f5_metric_and_report_status_unions_are_discriminated() -> None:
    """MetricResult and the enclosing report deliberately have different unions."""
    metric_statuses = {case.metric_status for cases in SLO_CASES.values() for case in cases}
    report_statuses = {case.report_status for cases in SLO_CASES.values() for case in cases}
    assert metric_statuses == {"pass", "fail", "unavailable"}
    assert report_statuses == {"ready", "not_ready", "unavailable"}
    for cases in SLO_CASES.values():
        passing, failing, missing = cases
        assert (passing.metric_status, passing.report_status) == ("pass", "ready")
        assert (failing.metric_status, failing.report_status) == ("fail", "not_ready")
        assert (missing.metric_status, missing.report_status) == ("unavailable", "unavailable")


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


def test_r2f5_complete_golden_tree_has_ready_shape_and_all_evidence(tmp_path: Path) -> None:
    golden = _build_complete_golden_tree(tmp_path)
    assert set(golden.request) == INPUT_KEYS
    assert len(golden.sessions) == 20
    assert golden.sessions == sorted(golden.sessions)
    assert len(golden.session_observations) == 20
    assert all(
        observation["ordinal"] == index
        for index, observation in enumerate(golden.session_observations, 1)
    )
    assert all(
        observation["canonical_integrity"]["status"] == "pass"
        for observation in golden.session_observations
    )
    assert all(
        observation["replication_observation"]["trust_scope"] == "REMOTE_VERIFIED"
        for observation in golden.session_observations
    )
    assert all(
        all(value is not None for value in observation[key].values())
        for observation in golden.session_observations
        for key in ("lineage", "manifest", "universe_facts")
    )
    assert golden.window_envelope["creator_kind"] == "task20_writer"
    assert golden.window_envelope["payload"]["observation_count"] == 1
    assert all(value is not None for value in golden.window_envelope["payload"].values())
    assert golden.snapshot_identity["snapshot_sha256"]
    assert golden.frozen_versions["replication_policy_version"]


def test_r2f5_golden_tree_public_reader_must_return_ready_with_all_pass_metrics(
    tmp_path: Path,
) -> None:
    golden = _build_complete_golden_tree(tmp_path)
    before = (
        _physical_fingerprint(golden.dataset),
        _physical_fingerprint(golden.evidence),
        _physical_fingerprint(golden.controls[0].parent),
    )
    report = _as_dict(_evaluate(golden.request))
    assert report["status"] == "ready"
    for metric, (kind, _passed, target) in METRIC_CONTRACTS.items():
        assert report[metric]["status"] == "pass"
        assert report[metric]["reason_code"] is None
        assert report[metric]["observed"] == {"kind": kind, "value": _passed}
        assert report[metric]["target"] == {"kind": kind, "value": target}
    assert report["selected_sessions"] == golden.sessions
    assert report["frozen_versions"] == golden.frozen_versions
    assert (
        report["snapshot_identity"]["snapshot_sha256"]
        == golden.snapshot_identity["snapshot_sha256"]
    )
    assert len(report["session_observations"]) == 20
    assert (
        report["window_evidence_bundle"]["payload_sha256"]
        == golden.window_envelope["payload_sha256"]
    )
    assert (
        report["window_evidence_bundle"]["envelope_sha256"]
        == golden.window_envelope["envelope_sha256"]
    )
    assert report["provider_requests"] == 0 and report["writes"] is False
    assert (
        _physical_fingerprint(golden.dataset),
        _physical_fingerprint(golden.evidence),
        _physical_fingerprint(golden.controls[0].parent),
    ) == before


def test_r2f5_golden_envelope_hash_excludes_payload_and_self_hash(tmp_path: Path) -> None:
    golden = _build_complete_golden_tree(tmp_path)
    envelope = golden.window_envelope
    metadata = {
        key: value for key, value in envelope.items() if key not in {"payload", "envelope_sha256"}
    }
    assert envelope["payload_sha256"] == hashlib.sha256(_jcs(envelope["payload"])).hexdigest()
    assert (
        envelope["envelope_sha256"]
        == hashlib.sha256(b"r2f5/envelope-v1\0" + _jcs(metadata)).hexdigest()
    )
    vector = _task20_envelope({"a": 1, "b": "x"}, "vector-001")
    assert vector["payload_sha256"] == (
        "ecf9e98ec0641e23113ff3ce8bdc78d0ddd249886517fd4a7f68cc83d4e65667"
    )
    assert vector["envelope_sha256"] == (
        "b32c1c8d0f4538c277817c76995914c4f911fe925382bd4912ee0b367b3f9a8c"
    )


def test_r2f5_golden_service_http_cli_semantic_parity(tmp_path: Path, monkeypatch, capsys) -> None:
    golden = _build_complete_golden_tree(tmp_path)
    settings = Settings(
        _env_file=None,
        local_control_dir=golden.controls[0].parent,
        local_market_dataset_root=golden.dataset,
        provider_shadow_root=golden.evidence,
    )
    service_report = _as_dict(_evaluate(golden.request))
    transport = httpx.ASGITransport(app=app)

    async def http_report():
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(
                "/api/v1/market/reliability-acceptance",
                params={"start": golden.request["start"], "end": golden.request["end"]},
            )

    import asyncio

    response = asyncio.run(http_report())
    assert response.status_code == 200
    http_report_payload = response.json()
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "r2f-acceptance",
            "--start",
            golden.request["start"],
            "--end",
            golden.request["end"],
        ],
    )
    assert cli_module.main() == 0
    cli_report = json.loads(capsys.readouterr().out)

    def semantic(value: dict[str, Any]) -> dict[str, Any]:
        return {
            key: item
            for key, item in value.items()
            if key not in {"semantic_report_sha256", "diagnostic_envelope"}
        }

    assert semantic(http_report_payload) == semantic(service_report)
    assert semantic(cli_report) == semantic(service_report)


def test_r2f5_builder_metadata_points_to_real_request_root_and_unique_target(
    tmp_path: Path,
) -> None:
    golden = _build_complete_golden_tree(tmp_path / "source")
    clone = _clone_golden_tree(golden, tmp_path / "clone")
    targets = []
    for case in CASE_SPECS.values():
        builder = case.builder
        assert callable(builder)
        before = _physical_fingerprint(clone.evidence)
        built = builder(clone.request)
        assert builder.target_descriptor == "evidence_root"
        assert builder.mutated_path == f"{case.requirement.lower()}.json"
        target = Path(built[builder.target_descriptor]) / builder.mutated_path
        assert target.exists()
        after = _physical_fingerprint(clone.evidence)
        assert after != before
        artifact = json.loads(target.read_text(encoding="utf-8"))
        assert artifact["metric"] == builder.expected["metric"] == case.metric
        assert artifact["expected_status"] == builder.expected["status"] == case.status
        assert artifact["expected_reason"] == builder.expected["reason"] == case.reason
        targets.append(target.name)
    assert len(set(targets)) == 92


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
    "filesystem_shape",
    ("symlink", "intermediate_replacement", "special", "hardlink", "path_collision"),
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
    elif filesystem_shape == "hardlink":
        os.link(dataset / "manifest.json", dataset / "manifest-hardlink.json")
    else:
        (dataset / "manifest.json").rename(dataset / "manifest.original")
        (dataset / "manifest.json").mkdir()
    case = deepcopy(captured_window)
    case["fixture_mutation"] = f"unsafe_{filesystem_shape}"
    before = _physical_fingerprint(dataset)
    report = _as_dict(_evaluate(case))
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] == "PATH_INVALID"
    assert _physical_fingerprint(dataset) == before


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


def test_r2f5_wal_commit_before_read_transaction_is_one_new_snapshot(
    captured_window,
) -> None:
    """A completed WAL commit before capture is visible as one coherent snapshot."""
    import threading

    database = Path(captured_window["control_store_roots"][0])
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE wal_before (id INTEGER PRIMARY KEY, value TEXT)")
        connection.execute("INSERT INTO wal_before VALUES (1, 'old')")
        connection.commit()
    ready_to_commit = threading.Event()
    committed = threading.Event()

    def writer() -> None:
        ready_to_commit.wait(timeout=1)
        with sqlite3.connect(database, timeout=1) as connection:
            connection.execute("UPDATE wal_before SET value='new' WHERE id=1")
            connection.commit()
        committed.set()

    thread = threading.Thread(target=writer)
    thread.start()
    ready_to_commit.set()
    assert committed.wait(timeout=2)
    before = _physical_fingerprint(database.parent)
    report = _as_dict(_evaluate(captured_window))
    thread.join(timeout=2)
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["status"] == "pass"
    assert report["read_boundary"]["reason_code"] is None
    assert report["read_boundary"]["observed"] == {"kind": "bool", "value": True}
    assert "SNAPSHOT_CHANGED" not in report["quality_issues"]
    assert report["provider_requests"] == 0 and report["writes"] is False
    assert _physical_fingerprint(database.parent) == before


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
