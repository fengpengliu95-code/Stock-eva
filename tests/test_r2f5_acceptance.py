"""Behavioral RED contract for the R2-F5.0 read-only acceptance reader.

The fixture is deliberately synthetic and local, but its object shapes, digest
preimages, creators, limits, metric kinds, reasons, and SQLite catalogs are read
from the approved X8 design.  Expected outcomes live only in pytest case objects;
the future reader receives the six-field ``AcceptanceInput`` and ordinary artifact
or database bytes.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import importlib
import importlib.util
import json
import math
import os
import re
import shutil
import socket
import sqlite3
import stat
import struct
import sys
import threading
import time
import urllib.request
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

import backend.app.cli as cli_module
from backend.app.api import market as market_api
from backend.app.config import Settings, get_settings
from backend.app.main import app

ROOT = Path(__file__).parents[1]
DESIGN = ROOT / "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md"
MATRIX = ROOT / "docs/acceptance/r2f5-0-requirement-evidence-matrix.md"
TARGET_MODULE = "backend.app.market.reliability_acceptance"
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SESSION_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
IDENTIFIER_FIELDS = {
    "artifact_id",
    "artifact_ref",
    "canonical_schema",
    "checkpoint_id",
    "creator_kind",
    "creator_version",
    "dataset",
    "descriptor_id",
    "descriptor_role",
    "evidence_schema",
    "installed_release",
    "primary_provider_id",
    "qualification_proof_status",
    "replication_trust_scope",
    "secondary_provider_id",
    "selected_provider_id",
    "source_schema",
    "trust_scope",
}
TIMESTAMP_FIELDS = {
    "as_of_utc",
    "created_at",
    "next_morning_published_at",
    "observed_at",
    "outage_end",
    "outage_start",
    "requested_as_of",
    "restart_boundary",
    "same_evening_published_at",
}
FORBIDDEN_INPUT_KEYS = {
    "expected",
    "expected_status",
    "expected_reason",
    "effect",
    "metric",
    "scenario",
    "fixture_mutation",
}


def _design_text() -> str:
    return DESIGN.read_text(encoding="utf-8")


def _x8_contract() -> dict[str, Any]:
    match = re.search(
        r"<!-- R2F5_X8_CONTRACTS_JSON -->\s*```json\s*(\{.*?\})\s*```",
        _design_text(),
        re.DOTALL,
    )
    assert match, "approved R2F5_X8_CONTRACTS_JSON block is missing"
    contract = json.loads(match.group(1))
    canonical = json.dumps(
        contract,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    declared = re.search(
        r"R2F5_X8_CONTRACTS_JSON` canonical block digest .*? is `([0-9a-f]{64})`",
        _design_text(),
        re.DOTALL,
    )
    assert declared and hashlib.sha256(canonical).hexdigest() == declared.group(1)
    return contract


X8 = _x8_contract()
MODELS: dict[str, dict[str, Any]] = X8["model_schema_ast"]
DIGESTS: dict[str, dict[str, Any]] = {item["field"]: item for item in X8["digest_contracts"]}
METRICS = tuple(X8["metric_fields"])
METRIC_KINDS = X8["metric_value_kinds"]
REASON_PARTITIONS = X8["reason_partitions"]
REASONS = frozenset(REASON_PARTITIONS["failure"] + REASON_PARTITIONS["unavailable"])
CREATORS = X8["creator_allowlist"]
LIMITS = X8["limits"]
CATALOGS = X8["sqlite_catalogs"]
CARDINALITY = X8["cardinality"]
REQUIREMENT_ROWS = tuple(
    re.findall(
        r"^\| ((?:FR|NFR|AC|EC)-\d+) \| (.*?) \|.*?"
        r"`PLANNED::(test_r2f5_req_[a-z]+_\d{2})`",
        MATRIX.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
)
REQUIREMENT_ANCHORS = tuple(row[2] for row in REQUIREMENT_ROWS)
REQUIREMENT_SUMMARIES = {row[2]: row[1] for row in REQUIREMENT_ROWS}
SLO_ANCHORS = tuple(f"test_r2f5_slo_{metric}" for metric in METRICS)


def _canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode()


def _domain_prefix(contract: dict[str, Any]) -> bytes:
    prefix = contract["domain_separation_prefix"]
    assert prefix.startswith("r2f5/") and prefix.endswith("\\0")
    return prefix[:-2].encode() + b"\0"


def _projection(root: dict[str, Any], paths: list[str]) -> Any:
    """Build the exact named-field projection used by one X8 digest contract."""

    if len(paths) == 1 and "." not in paths[0] and "[]" not in paths[0]:
        return root[paths[0]]
    projected: dict[str, Any] = {}
    grouped_tuple_paths: dict[str, list[str]] = {}
    for path in paths:
        if "[]." in path:
            collection, child = path.split("[].", 1)
            grouped_tuple_paths.setdefault(collection, []).append(child)
        elif "." in path:
            first, remainder = path.split(".", 1)
            projected.setdefault(first, {})[remainder] = root[first][remainder]
        else:
            projected[path] = root[path]
    for collection, children in grouped_tuple_paths.items():
        projected[collection] = [
            {child: item[child] for child in children} for item in root[collection]
        ]
    return projected


def _digest(field_path: str, root: dict[str, Any]) -> str:
    contract = DIGESTS[field_path]
    assert contract["canonicalization_version"] == "project-canonical-json-v1"
    preimage = _projection(root, contract["included_field_paths"])
    return hashlib.sha256(_domain_prefix(contract) + _canonical_json(preimage)).hexdigest()


def _interface_fields(name: str) -> tuple[str, ...]:
    match = re.search(
        rf"interface {re.escape(name)}(?:<T>)? \{{(.*?)\n\}}", _design_text(), re.DOTALL
    )
    assert match, name
    return tuple(re.findall(r"(?:^|[;\n])\s*([a-z][a-z0-9_]*):", match.group(1)))


INPUT_KEYS = frozenset(_interface_fields("AcceptanceInput"))


@dataclass
class ValidationStats:
    objects: int = 0
    digests: int = 0
    envelopes: int = 0
    metrics: int = 0
    sqlite_catalogs: int = 0
    digest_fields: set[str] = field(default_factory=set)


class StrictFixtureValidator:
    """Test-side closed validator sourced from the approved design block."""

    def __init__(self) -> None:
        self.stats = ValidationStats()

    def shape(self, model: str, value: dict[str, Any]) -> None:
        expected = set(MODELS[model]["object_fields"])
        assert set(value) == expected, (
            f"{model}: missing={expected - set(value)} unknown={set(value) - expected}"
        )
        self.stats.objects += 1

    def interface_shape(self, model: str, value: dict[str, Any]) -> None:
        expected = set(_interface_fields(model))
        assert expected and set(value) == expected, (
            f"{model}: missing={expected - set(value)} unknown={set(value) - expected}"
        )
        self.stats.objects += 1

    def scalar_contract(self, value: Any, path: str) -> None:
        name = path.rsplit(".", 1)[-1]
        unindexed_name = re.sub(r"\[\d+\]$", "", name)
        if value is None:
            return
        is_filesystem_path = unindexed_name in {
            "local_dataset_root",
            "evidence_root",
            "control_store_roots",
        }
        if isinstance(value, str) and not is_filesystem_path and not name.endswith("_bytes"):
            assert len(value.encode("utf-8")) <= 128, path
        if name.endswith(("sha256", "digest")) or name == "sha256":
            assert isinstance(value, str) and SHA256.fullmatch(value), path
        elif name in {
            "ordinal",
            "required_count",
            "loaded_count",
            "suspension_count",
            "not_listed_count",
            "delisted_count",
            "unknown_count",
            "future_rows_count",
            "query_count",
            "write_count",
            "lag_seconds",
            "lag_threshold_seconds",
            "publication_count",
            "row_count",
        }:
            assert (
                isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 2**31 - 1
            )
        elif (
            name == "session"
            or name.endswith("_date")
            or name
            in {
                "requested_start",
                "requested_end",
                "window_start",
                "window_end",
            }
        ):
            assert isinstance(value, str) and SESSION_DATE.fullmatch(value), path
            date.fromisoformat(value)
        elif name in TIMESTAMP_FIELDS:
            assert isinstance(value, str), path
            _validate_timestamp(value)
        elif isinstance(value, str) and (
            name in IDENTIFIER_FIELDS
            or name.endswith(("_id", "_version", "_generation"))
            or unindexed_name.endswith(("provider_ids", "provider_priority"))
        ):
            assert SAFE_ID.fullmatch(value), path
        if isinstance(value, float):
            assert math.isfinite(value)

    def walk_scalars(self, value: Any, path: str = "") -> None:
        if isinstance(value, dict):
            assert not FORBIDDEN_INPUT_KEYS & set(value), path
            for key, child in value.items():
                self.walk_scalars(child, f"{path}.{key}" if path else key)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                self.walk_scalars(child, f"{path}[{index}]")
        else:
            self.scalar_contract(value, path)

    def exact_digest(self, field_path: str, root: dict[str, Any], actual: str) -> None:
        assert field_path in DIGESTS
        assert actual == _digest(field_path, root), field_path
        self.stats.digests += 1
        self.stats.digest_fields.add(field_path)

    def metric(self, name: str, value: dict[str, Any]) -> None:
        assert set(value) == {
            "status",
            "observed",
            "target",
            "reason_code",
            "acceptance_ref",
            "planned_test_anchor",
        }
        status = value["status"]
        assert status in X8["status_reason_matrix"]
        assert value["acceptance_ref"] == "AC-15"
        assert value["planned_test_anchor"] == f"test_r2f5_slo_{name}"
        if status == "unavailable":
            assert value["observed"] is value["target"] is None
            assert value["reason_code"] in REASON_PARTITIONS["unavailable"]
        else:
            assert (
                value["reason_code"] is None
                if status == "pass"
                else (value["reason_code"] in REASON_PARTITIONS["failure"])
            )
            for side in ("observed", "target"):
                metric_value = value[side]
                assert set(metric_value) == {"kind", "value"}
                assert metric_value["kind"] == METRIC_KINDS[name][side]
                kind = metric_value["kind"]
                number = metric_value["value"]
                if kind == "bool":
                    assert isinstance(number, bool)
                elif kind == "ratio":
                    assert isinstance(number, (int, float)) and not isinstance(number, bool)
                    assert math.isfinite(number) and 0 <= number <= 1
                elif kind in {"count", "duration_seconds"}:
                    assert isinstance(number, int) and not isinstance(number, bool)
                    assert 0 <= number <= 2**31 - 1
                else:
                    assert kind == "hash" and isinstance(number, str) and SHA256.fullmatch(number)
        self.stats.metrics += 1

    def envelope(self, value: dict[str, Any], payload_model: str) -> None:
        self.shape("ImmutableObservationEnvelopeV1", value)
        assert value["schema_version"] == "r2f5-observation-envelope-v1"
        assert value["canonicalization_version"] == "project-canonical-json-v1"
        assert value["creator_kind"] in CREATORS["production_envelope_payloads"][payload_model]
        assert value["creator_kind"] not in CREATORS["reader_never_creates"]
        assert SAFE_ID.fullmatch(value["artifact_id"])
        assert SAFE_ID.fullmatch(value["artifact_ref"])
        assert SAFE_ID.fullmatch(value["creator_version"])
        _validate_timestamp(value["created_at"])
        self.exact_digest(
            "ImmutableObservationEnvelopeV1.payload_sha256", value, value["payload_sha256"]
        )
        self.exact_digest(
            "ImmutableObservationEnvelopeV1.envelope_sha256", value, value["envelope_sha256"]
        )
        self.stats.envelopes += 1


def _validate_timestamp(value: str) -> None:
    assert value.endswith("Z") and "T" in value
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None and parsed.utcoffset() == timedelta(0)


_CATALOG_DDL = {
    "replication_sidecar": (ROOT / "backend/app/storage/replication.py", "SIDECAR_DDL", None),
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
_CATALOG_FILENAMES = {
    "replication_sidecar": "replication.sqlite3",
    "daily_shadow": "daily_bar_shadow.sqlite3",
    "shadow_registry": "provider_registry.sqlite3",
    "calendar_generation": "calendar_generations.sqlite3",
    "universe": "market_universe.sqlite3",
}


def _sqlite_catalog_key(path: Path) -> str | None:
    """Return the approved catalog key for a control DB path, if any."""

    for key, filename in _CATALOG_FILENAMES.items():
        if path.name == filename:
            return key
    return None


def _source_string(path: Path, name: str) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
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
    raise AssertionError(f"missing DDL {path}:{name}")


def _create_catalog(path: Path, role: str) -> None:
    """Create a legal, non-empty control fixture through reviewed bootstrap code.

    The evaluator under test never calls these writers.  They are used only while
    assembling the input fixture, before any read-only/provider sentinels are
    installed.  Keeping the bootstrap here makes an accidental empty-DLL fixture
    impossible to mistake for a valid production control store.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    if role == "daily_shadow":
        from backend.app.market.daily_shadow_schema import initialize_daily_shadow_schema

        with sqlite3.connect(path) as connection:
            initialize_daily_shadow_schema(connection, applied_at="2026-09-14T06:00:00Z")
        return
    if role == "shadow_registry":
        from backend.app.market.shadow_registry_schema import initialize_registry

        with sqlite3.connect(path) as connection:
            initialize_registry(connection)
        return
    if role == "calendar_generation":
        from backend.app.market.calendar_generation import CalendarGenerationStore

        store = CalendarGenerationStore(path)
        with sqlite3.connect(path) as connection:
            store._initialize(connection)  # authoritative store bootstrap, test fixture only
            connection.commit()
        return
    if role == "universe":
        from backend.app.market.universe import UniverseSidecarStore

        UniverseSidecarStore(path).initialize()
        return

    if role == "replication_sidecar":
        from backend.app.storage.replication import _generation_payload

        path.write_bytes(
            _generation_payload(
                "b" * 64,
                "c" * 64,
                "2026-09-14T06:00:00Z",
            )
        )
        return

    source, ddl_name, migration_name = _CATALOG_DDL[role]
    ddl = _source_string(source, ddl_name)
    if migration_name:
        ddl += "\n" + _source_string(source, migration_name)
    with sqlite3.connect(path) as connection:
        connection.executescript(ddl)
        connection.commit()


def _validate_catalog(path: Path, role: str, validator: StrictFixtureValidator) -> None:
    expected = CATALOGS[role]
    uri = f"file:{path}?mode=ro&immutable=false"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.execute("PRAGMA query_only=ON")
        actual_version = connection.execute("PRAGMA user_version").fetchone()[0]
        actual_objects = [
            f"{kind}:{name}"
            for kind, name in connection.execute(
                "SELECT type,name FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
            )
        ]
        assert actual_version == expected["user_version"]
        assert actual_objects == expected["sqlite_master_allowlist"]
        for table_name, table in expected["tables"].items():
            rows = connection.execute(f'PRAGMA table_info("{table_name}")').fetchall()
            assert [f"{row[1]}:{row[2] or ''}" for row in rows] == table["columns"]
            assert [row[1] for row in sorted(rows, key=lambda item: item[5]) if row[5]] == table[
                "primary_key"
            ]
            assert all(
                column in {item.split(":", 1)[0] for item in table["columns"]}
                for column in table["order_by"]
            )
        row_counts = {
            table_name: connection.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()[0]
            for table_name in expected["tables"]
        }
        assert sum(row_counts.values()) > 0, f"{role}: authoritative fixture must be non-empty"
        if role == "daily_shadow":
            assert row_counts["schema_migration"] >= 1
        elif role == "shadow_registry":
            assert row_counts["schema_migration"] == 2
        elif role == "calendar_generation":
            assert row_counts["calendar_generation_meta"] == 1
            assert row_counts["calendar_generation_head"] == 1
        elif role == "universe":
            assert row_counts["universe_meta"] >= 4
        elif role == "replication_sidecar":
            assert row_counts["replication_sidecar_meta"] == 1
    validator.stats.sqlite_catalogs += 1


def _session_dates() -> list[str]:
    values: list[str] = []
    current = date(2026, 8, 3)
    while len(values) < CARDINALITY["sessions"]:
        if current.weekday() < 5:
            values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _metric(name: str, observed: Any, target: Any) -> dict[str, Any]:
    kind = METRIC_KINDS[name]["observed"]
    return {
        "status": "pass",
        "observed": {"kind": kind, "value": observed},
        "target": {"kind": kind, "value": target},
        "reason_code": None,
        "acceptance_ref": "AC-15",
        "planned_test_anchor": f"test_r2f5_slo_{name}",
    }


def _envelope(payload: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    value = {
        "artifact_id": artifact_id,
        "artifact_ref": f"task20:{artifact_id}.json",
        "schema_version": "r2f5-observation-envelope-v1",
        "creator_kind": CREATORS["production_creator_kind"],
        "creator_version": "task20-fixture-v1",
        "created_at": "2026-09-14T06:00:00Z",
        "payload": payload,
        "canonicalization_version": "project-canonical-json-v1",
        "payload_sha256": "0" * 64,
        "envelope_sha256": "0" * 64,
    }
    value["payload_sha256"] = _digest("ImmutableObservationEnvelopeV1.payload_sha256", value)
    value["envelope_sha256"] = _digest("ImmutableObservationEnvelopeV1.envelope_sha256", value)
    return value


@dataclass
class SessionFixture:
    observation: dict[str, Any]
    sources: dict[str, dict[str, Any]]


@dataclass
class GoldenTree:
    request: dict[str, Any]
    dataset: Path
    evidence: Path
    control: Path
    controls: dict[str, Path]
    sessions: list[str]
    frozen_versions: dict[str, Any]
    frozen_sources: dict[str, dict[str, Any]]
    session_fixtures: list[SessionFixture]
    window_envelope: dict[str, Any]
    window_sources: dict[str, dict[str, Any]]
    qualification_projection: dict[str, Any]
    completed_replication_restore: dict[str, Any]
    completed_sources: dict[str, dict[str, Any]]
    snapshot_identity: dict[str, Any]
    fingerprint_sources: list[dict[str, Any]]
    validation_stats: ValidationStats = field(default_factory=ValidationStats)

    def roots(self) -> tuple[Path, ...]:
        return self.dataset, self.evidence, self.control


def _leaf_digest(field_path: str, model: str, **fields: Any) -> tuple[str, dict[str, Any]]:
    root = dict(fields)
    assert set(root) == set(MODELS[model]["object_fields"])
    return _digest(field_path, root), root


def _build_frozen() -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    sha = "1" * 64
    config = {
        "dataset_root_descriptor": "dataset-root-v1",
        "evidence_root_descriptor": "evidence-root-v1",
        "control_store_descriptor": "control-root-v1",
        "clock_policy": "asia-shanghai-v1",
        "cutoff_policy": "2115-0800-v1",
        "limits": LIMITS,
        "replay_policy": "offline-exact-v1",
        "replication_policy": "reviewed-r2f4-policy-v1",
        "restore_policy": "reviewed-r2f4-policy-v1",
        "redaction_policy": "allowlisted-v1",
    }
    installed = {"release_identity": "stock-eva-r2f5.0", "immutable_release_bytes": "release"}
    calendar = {"calendar_generation": "calendar-g20", "immutable_calendar_bytes": "calendar"}
    universe = {"universe_generation": "universe-g20", "immutable_universe_bytes": "universe"}
    destination = {
        "destination_generation": "destination-g20",
        "destination_head_bytes": "head",
        "immutable_destination_head_bytes": "head",
    }
    sources = {
        "FrozenReliabilityVersions.config_digest": config,
        "FrozenReliabilityVersions.installed_release_sha256": installed,
        "FrozenReliabilityVersions.calendar_sha256": calendar,
        "FrozenReliabilityVersions.universe_sha256": universe,
        "FrozenReliabilityVersions.destination_head_sha256": destination,
    }
    frozen = {
        "git_commit": "1" * 40,
        "installed_release": "stock-eva-r2f5.0",
        "installed_release_sha256": _digest(
            "FrozenReliabilityVersions.installed_release_sha256", installed
        ),
        "dataset_generation": "dataset-g20",
        "canonical_schema": "canonical-v2",
        "evidence_schema": "r2f5-observation-envelope-v1",
        "primary_provider_id": "baostock",
        "secondary_provider_id": "tickflow",
        "qualification_window_id": "qualification-window-20",
        "qualification_proof_status": "available",
        "adapter_hash": sha,
        "endpoint_contract_hash": "2" * 64,
        "source_schema_hash": "3" * 64,
        "normalizer_hash": "4" * 64,
        "reconciliation_policy_version": "reconcile-v1",
        "selection_policy_version": "selection-v1",
        "config_digest": _digest("FrozenReliabilityVersions.config_digest", config),
        "auto_failover_enabled": True,
        "failover_kill_switch": False,
        "provider_priority": ["baostock", "tickflow"],
        "continuity_start_date": "2026-08-03",
        "repair_policy_version": "repair-v1",
        "calendar_generation": "calendar-g20",
        "calendar_sha256": _digest("FrozenReliabilityVersions.calendar_sha256", calendar),
        "universe_generation": "universe-g20",
        "universe_sha256": _digest("FrozenReliabilityVersions.universe_sha256", universe),
        "replication_policy_version": "reviewed-r2f4-policy-v1",
        "replication_evidence_version": "r2f4.3-v1",
        "replication_trust_scope": "REMOTE_VERIFIED",
        "destination_generation": "destination-g20",
        "destination_head_sha256": _digest(
            "FrozenReliabilityVersions.destination_head_sha256", destination
        ),
        "remote_proof_artifact_ref": "task20:remote-proof-20.json",
        "restore_policy_version": "reviewed-r2f4-policy-v1",
        "restore_evidence_version": "restore-v1",
    }
    return frozen, sources


def _build_session(
    session: str, ordinal: int, sessions: list[str], frozen: dict[str, Any]
) -> SessionFixture:
    sources: dict[str, dict[str, Any]] = {}

    def leaf(field_path: str, model: str, **values: Any) -> str:
        digest, root = _leaf_digest(field_path, model, **values)
        sources[field_path] = root
        return digest

    evidence_id = f"evidence-{ordinal:02d}"
    candidate_id = f"candidate-{ordinal:02d}"
    gate_id = f"gate-{ordinal:02d}"
    manifest_id = f"manifest-{ordinal:02d}"
    object_id = f"object-{ordinal:02d}"
    selection_id = f"selection-{ordinal:02d}"
    evidence = {
        "evidence_id": evidence_id,
        "evidence_sha256": leaf(
            "SessionEvidenceBinding.evidence_sha256",
            "EvidenceObject",
            evidence_id=evidence_id,
            immutable_evidence_bytes=f"raw-evidence-{session}",
        ),
        "candidate_id": candidate_id,
        "candidate_sha256": leaf(
            "SessionEvidenceBinding.candidate_sha256",
            "CandidateObject",
            candidate_id=candidate_id,
            immutable_candidate_bytes=f"candidate-{session}",
        ),
        "gate_report_id": gate_id,
        "gate_report_sha256": leaf(
            "SessionEvidenceBinding.gate_report_sha256",
            "GateReport",
            gate_report_id=gate_id,
            immutable_gate_report_bytes=f"gate-{session}",
        ),
        "manifest_id": manifest_id,
        "manifest_sha256": leaf(
            "SessionEvidenceBinding.manifest_sha256",
            "Manifest",
            manifest_id=manifest_id,
            immutable_manifest_bytes=f"manifest-{session}",
        ),
        "object_id": object_id,
        "object_sha256": leaf(
            "SessionEvidenceBinding.object_sha256",
            "ObjectEvidence",
            object_id=object_id,
            immutable_object_bytes=f"canonical-{session}",
        ),
        "selection_id": selection_id,
        "selection_sha256": leaf(
            "SessionEvidenceBinding.selection_sha256",
            "SessionSelection",
            selection_id=selection_id,
            immutable_selection_bytes=f"selection-{session}",
        ),
        "binding_sha256": "0" * 64,
    }
    evidence["binding_sha256"] = _digest("SessionEvidenceBinding.binding_sha256", evidence)

    pointer_id = f"pointer-{ordinal:02d}"
    pointer_manifest = {
        "manifest_id": manifest_id,
        "immutable_manifest_bytes": f"manifest-{session}",
    }
    pointer_object = {"object_id": object_id, "immutable_object_bytes": f"canonical-{session}"}
    pointer_descriptor = {
        "descriptor_id": f"descriptor-{ordinal:02d}",
        "descriptor_metadata": {"session": session, "ordinal": ordinal},
    }
    pointer = {
        "pointer_id": pointer_id,
        "pointer_sha256": leaf(
            "PointerReconciliation.pointer_sha256",
            "PointerRecord",
            pointer_id=pointer_id,
            immutable_pointer_bytes=f"pointer-{session}",
        ),
        "manifest_sha256": _digest("PointerReconciliation.manifest_sha256", pointer_manifest),
        "object_sha256": _digest("PointerReconciliation.object_sha256", pointer_object),
        "pointer_manifest_object_match": True,
        "descriptor_sha256": _digest("PointerReconciliation.descriptor_sha256", pointer_descriptor),
    }
    sources["PointerReconciliation.manifest_sha256"] = pointer_manifest
    sources["PointerReconciliation.object_sha256"] = pointer_object
    sources["PointerReconciliation.descriptor_sha256"] = pointer_descriptor

    source_commit = {
        "source_commit_id": f"source-{ordinal:02d}",
        "source_commit_bytes": f"source-commit-{session}",
        "destination_record_id": f"destination-record-{ordinal:02d}",
        "destination_record_bytes": f"destination-record-{session}",
        "destination_generation": "destination-g20",
        "immutable_destination_record_bytes": f"destination-record-{session}",
    }
    destination_record = dict(source_commit)
    destination_head = {
        "destination_generation": "destination-g20",
        "destination_head_bytes": f"destination-head-{session}",
        "immutable_destination_head_bytes": f"destination-head-{session}",
    }
    replication = {
        "immutable": True,
        "state": "ready",
        "checkpoint_id": f"checkpoint-{ordinal:02d}",
        "source_commit_sha256": _digest(
            "ReplicationObservation.source_commit_sha256", source_commit
        ),
        "intent_id": f"intent-{ordinal:02d}",
        "enqueue_state": "replicated",
        "reason_code": "NONE",
        "observed_at": "2026-09-14T06:00:00Z",
        "lag_seconds": 0,
        "trust_scope": "REMOTE_VERIFIED",
        "destination_generation": "destination-g20",
        "destination_record_sha256": _digest(
            "ReplicationObservation.destination_record_sha256", destination_record
        ),
        "destination_head_sha256": _digest(
            "ReplicationObservation.destination_head_sha256", destination_head
        ),
        "observation_sha256": "0" * 64,
    }
    replication["observation_sha256"] = _digest(
        "ReplicationObservation.observation_sha256", replication
    )
    sources.update(
        {
            "ReplicationObservation.source_commit_sha256": source_commit,
            "ReplicationObservation.destination_record_sha256": destination_record,
            "ReplicationObservation.destination_head_sha256": destination_head,
        }
    )

    calendar = {
        "source_sequence": list(sessions),
        "generation": "calendar-g20",
        "confirmed": True,
        "unknown_state": False,
        "conflict_state": False,
        "raw_facts_sha256": "0" * 64,
    }
    calendar["raw_facts_sha256"] = _digest("CalendarRawFacts.raw_facts_sha256", calendar)
    boundary = {
        "requested_as_of": "2026-09-14T06:00:00Z",
        "max_visible_session": session,
        "future_rows_seen": False,
        "future_rows_count": 0,
        "query_count": 1,
        "write_count": 0,
        "probe_schema_digest": "0" * 64,
    }
    boundary["probe_schema_digest"] = _digest("ReadBoundaryRawFacts.probe_schema_digest", boundary)
    frozen_digest = _digest("SessionObservation.frozen_versions_sha256", frozen)
    observation = {
        "session": session,
        "ordinal": ordinal,
        "frozen_versions_sha256": frozen_digest,
        "same_evening_published_at": f"{session}T13:15:00Z",
        "next_morning_published_at": (
            datetime.fromisoformat(session).replace(tzinfo=UTC) + timedelta(days=1)
        ).strftime("%Y-%m-%dT00:00:00Z"),
        "required_count": 5_000,
        "loaded_count": 5_000,
        "suspension_count": 0,
        "not_listed_count": 0,
        "delisted_count": 0,
        "unknown_count": 0,
        "canonical_provider_ids": ["baostock"],
        "evidence": evidence,
        "pointer_reconciliation": pointer,
        "replication_observation": replication,
        "calendar_raw_facts": calendar,
        "read_boundary_raw_facts": boundary,
        "schema_policy_versions": ["canonical-v2", "selection-v1", "reconcile-v1"],
        "schema_policy_digest": "0" * 64,
        "cutoff_results": {
            "same_evening": _metric("same_evening_availability", 0.9, 0.9),
            "next_morning": _metric("next_morning_availability", 1.0, 1.0),
        },
        "coverage": _metric("coverage", 1.0, 1.0),
        "canonical_integrity": _metric("canonical_integrity", True, True),
        "source_purity": _metric("source_purity", True, True),
        "provenance": _metric("provenance", True, True),
        "calendar": _metric("calendar", True, True),
        "universe": _metric("universe", True, True),
        "replication": _metric("replication", 0, 300),
        "read_boundary": _metric("read_boundary", True, True),
        "observation_sha256": "0" * 64,
    }
    observation["schema_policy_digest"] = _digest(
        "SessionObservation.schema_policy_digest", observation
    )
    observation["observation_sha256"] = _digest(
        "SessionObservation.observation_sha256", observation
    )
    return SessionFixture(observation, sources)


def _build_window(sessions: list[str]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    sources: dict[str, dict[str, Any]] = {}

    recovery_sources = {
        "RecoveryObservation.after_manifest_sha256": {
            "manifest_id": "manifest-20",
            "immutable_manifest_bytes": "after-manifest",
        },
        "RecoveryObservation.after_pointer_sha256": {
            "pointer_id": "pointer-20",
            "immutable_pointer_bytes": "after-pointer",
        },
        "RecoveryObservation.after_selection_sha256": {
            "selection_id": "selection-20",
            "immutable_selection_bytes": "after-selection",
        },
    }
    recovery = {
        "immutable": True,
        "event_id": "recovery-20",
        "attempt_id": "recovery-attempt-20",
        "before_generation": "dataset-g19",
        "after_generation": "dataset-g20",
        "queue_identity": "queue-20",
        "restart_boundary": "2026-09-14T05:00:00Z",
        "exactly_once_publication_id": "publication-20",
        "after_manifest_sha256": _digest(
            "RecoveryObservation.after_manifest_sha256",
            recovery_sources["RecoveryObservation.after_manifest_sha256"],
        ),
        "after_pointer_sha256": _digest(
            "RecoveryObservation.after_pointer_sha256",
            recovery_sources["RecoveryObservation.after_pointer_sha256"],
        ),
        "after_selection_sha256": _digest(
            "RecoveryObservation.after_selection_sha256",
            recovery_sources["RecoveryObservation.after_selection_sha256"],
        ),
        "publication_count": 1,
        "duplicate_proof_sha256": "0" * 64,
        "observed_at": "2026-09-14T06:00:00Z",
        "observation_sha256": "0" * 64,
    }
    recovery["duplicate_proof_sha256"] = _digest(
        "RecoveryObservation.duplicate_proof_sha256", recovery
    )
    recovery["observation_sha256"] = _digest("RecoveryObservation.observation_sha256", recovery)
    sources.update(recovery_sources)

    failover_sources = {
        "WholeSessionFailoverDrill.selection_sha256": {
            "selection_id": "failover-selection",
            "immutable_selection_bytes": "tickflow-whole-session",
        },
        "WholeSessionFailoverDrill.manifest_sha256": {
            "manifest_id": "failover-manifest",
            "immutable_manifest_bytes": "tickflow-manifest",
        },
        "WholeSessionFailoverDrill.pointer_sha256": {
            "pointer_id": "failover-pointer",
            "immutable_pointer_bytes": "tickflow-pointer",
        },
        "WholeSessionFailoverDrill.readback_sha256": {
            "readback_id": "failover-readback",
            "immutable_readback_bytes": "tickflow-readback",
        },
    }
    failover = {
        "source_schema": "task20-writer-owned",
        "primary_unavailable": True,
        "qualified_secondary_provider_id": "tickflow",
        "qualification_proof_status": "available",
        "session": sessions[-1],
        "selected_provider_id": "tickflow",
        "selection_sha256": _digest(
            "WholeSessionFailoverDrill.selection_sha256",
            failover_sources["WholeSessionFailoverDrill.selection_sha256"],
        ),
        "manifest_sha256": _digest(
            "WholeSessionFailoverDrill.manifest_sha256",
            failover_sources["WholeSessionFailoverDrill.manifest_sha256"],
        ),
        "pointer_sha256": _digest(
            "WholeSessionFailoverDrill.pointer_sha256",
            failover_sources["WholeSessionFailoverDrill.pointer_sha256"],
        ),
        "readback_sha256": _digest(
            "WholeSessionFailoverDrill.readback_sha256",
            failover_sources["WholeSessionFailoverDrill.readback_sha256"],
        ),
        "mixed_source_rows": 0,
    }
    sources.update(failover_sources)

    replay_sample = {"sample_object_id": "sample-20", "immutable_sample_bytes": "raw-sample"}
    replay_candidate = {
        "candidate_id": "replay-candidate-20",
        "immutable_candidate_bytes": "normalized-sample",
    }
    offline_implementation = {
        "adapter_id": "baostock",
        "adapter_version": "offline-v1",
        "normalizer_id": "market-normalizer",
        "normalizer_version": "v1",
        "immutable_implementation_bytes": "implementation-v1",
    }
    offline_context = {
        "adapter_id": "baostock",
        "adapter_version": "offline-v1",
        "normalizer_id": "market-normalizer",
        "normalizer_version": "v1",
        "implementation_sha256": _digest(
            "OfflineReplayContext.implementation_sha256", offline_implementation
        ),
        "network_allowed": False,
        "provider_requests": 0,
    }
    replay = {
        "sample_object_sha256": _digest("ReplaySampleEvidence.sample_object_sha256", replay_sample),
        "candidate_sha256": _digest("ReplaySampleEvidence.candidate_sha256", replay_candidate),
        "semantic_equal": True,
        "offline_context": offline_context,
    }
    sources.update(
        {
            "ReplaySampleEvidence.sample_object_sha256": replay_sample,
            "ReplaySampleEvidence.candidate_sha256": replay_candidate,
            "OfflineReplayContext.implementation_sha256": offline_implementation,
        }
    )

    error_events = []
    for forced_class in ("timeout", "auth", "rate", "schema", "coverage", "storage"):
        event = {
            "event_id": f"error-{forced_class}",
            "forced_error_class": forced_class,
            "sanitized_reason": "CONTROL_STATE_UNAVAILABLE",
            "normalized_result": "unavailable",
            "attempt_id": f"attempt-{forced_class}",
            "expected_class": forced_class,
            "observed_class": forced_class,
            "evidence_sha256": "0" * 64,
            "observed_at": "2026-09-14T06:00:00Z",
        }
        source = {
            "event_id": event["event_id"],
            "forced_error_class": forced_class,
            "immutable_evidence_bytes": f"forced-{forced_class}",
        }
        event["evidence_sha256"] = _digest(
            "ErrorHandlingObservation.events.evidence_sha256", source
        )
        sources[f"ErrorHandlingObservation.events.evidence_sha256:{forced_class}"] = source
        error_events.append(event)
    errors = {"immutable": True, "events": error_events, "observation_sha256": "0" * 64}
    errors["observation_sha256"] = _digest("ErrorHandlingObservation.observation_sha256", errors)

    local_pointer = {
        "local_publication_id": "local-publication-20",
        "immutable_pointer_bytes": "local-pointer",
    }
    local_nas = {
        "immutable": True,
        "event_id": "nas-outage-20",
        "local_publication_ready": True,
        "local_publication_id": "local-publication-20",
        "local_pointer_sha256": _digest(
            "LocalNasIsolationObservation.local_pointer_sha256", local_pointer
        ),
        "outage_start": "2026-09-14T05:00:00Z",
        "outage_end": "2026-09-14T05:01:00Z",
        "backlog_before_ids": ["intent-before"],
        "backlog_after_ids": ["intent-after"],
        "backlog_before_count": 1,
        "backlog_after_count": 1,
        "lag_seconds": 60,
        "lag_threshold_seconds": 300,
        "retryable": True,
        "retry_state": "retrying",
        "retry_transition": "retrying",
        "nas_failure_did_not_block_local": True,
        "attempt_id": "nas-attempt-20",
        "observed_at": "2026-09-14T06:00:00Z",
        "observation_sha256": "0" * 64,
    }
    local_nas["observation_sha256"] = _digest(
        "LocalNasIsolationObservation.observation_sha256", local_nas
    )
    sources["LocalNasIsolationObservation.local_pointer_sha256"] = local_pointer

    restore_source_values = {
        "RestoreDrillEvidence.sentinel_sha256": (
            "RestoreSentinel",
            {"sentinel_id": "sentinel-20", "immutable_sentinel_bytes": "sentinel"},
        ),
        "RestoreDrillEvidence.destination_head_sha256": (
            "DestinationHead",
            {
                "destination_generation": "destination-g20",
                "destination_head_bytes": "head",
                "immutable_destination_head_bytes": "head",
            },
        ),
        "RestoreDrillEvidence.record_sha256": (
            "RestoreRecord",
            {"record_id": "restore-record-20", "immutable_record_bytes": "record"},
        ),
        "RestoreDrillEvidence.manifest_sha256": (
            "Manifest",
            {"manifest_id": "restore-manifest-20", "immutable_manifest_bytes": "manifest"},
        ),
        "RestoreDrillEvidence.restore_report_sha256": (
            "RestoreReport",
            {"restore_report_id": "restore-report-20", "immutable_restore_report_bytes": "report"},
        ),
        "RestoreDrillEvidence.api_readback_sha256": (
            "RestoreApiReadback",
            {"readback_id": "restore-readback-20", "immutable_readback_bytes": "readback"},
        ),
    }
    restore_hashes = {
        field_path.rsplit(".", 1)[-1]: _digest(field_path, source)
        for field_path, (_model, source) in restore_source_values.items()
    }
    sources.update(
        {field_path: source for field_path, (_model, source) in restore_source_values.items()}
    )
    restore = {
        "source_schema": "task20-writer-owned",
        "sentinel_sha256": restore_hashes["sentinel_sha256"],
        "destination_id": "restore-destination-20",
        "destination_generation": "destination-g20",
        "destination_head_sha256": restore_hashes["destination_head_sha256"],
        "record_sha256": restore_hashes["record_sha256"],
        "manifest_sha256": restore_hashes["manifest_sha256"],
        "checkpoint_id": "restore-checkpoint-20",
        "restore_report_id": "restore-report-20",
        "restore_report_sha256": restore_hashes["restore_report_sha256"],
        "schema_version": "restore-v1",
        "row_count": 100_000,
        "api_readback_sha256": restore_hashes["api_readback_sha256"],
        "verification_state": "verified",
    }
    adjustment = {
        "compared_sessions": sessions[:2],
        "tolerance_policy_version": "reviewed-adjustment-policy-v1",
        "equivalence_passed": True,
    }
    payloads = {
        "recovery_observation": (recovery, "RecoveryObservation"),
        "failover_observation": (failover, "WholeSessionFailoverDrill"),
        "replay_sample": (replay, "ReplaySampleEvidence"),
        "adjustment_equivalence": (adjustment, "AdjustmentEquivalenceEvidence"),
        "error_handling_observation": (errors, "ErrorHandlingObservation"),
        "local_nas_isolation_observation": (local_nas, "LocalNasIsolationObservation"),
        "restore_observation": (restore, "RestoreDrillEvidence"),
    }
    bundle_payload = {
        name: _envelope(payload, name.replace("_observation", "").replace("_equivalence", ""))
        for name, (payload, _model) in payloads.items()
    }
    bundle_payload["observation_count"] = CARDINALITY["window_bundle"]
    return _envelope(bundle_payload, "window-20"), sources


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_json(value))


def _tree_content_digest(root: Path) -> str:
    """Hash a tree while delegating SQLite identity to its logical catalog hash.

    Database files and their ``-wal``/``-shm`` siblings are deliberately not tree
    leaves.  The approved SQLite algorithm below owns those bytes; hashing them a
    second time would make a valid WAL commit look like a tree mutation.
    """

    sqlite_names = set(_CATALOG_FILENAMES.values())
    records: list[list[Any]] = []
    for path in sorted(
        root.rglob("*"), key=lambda item: item.relative_to(root).as_posix().encode()
    ):
        rel = path.relative_to(root).as_posix()
        if path.name in sqlite_names or any(
            path.name == f"{name}-wal" or path.name == f"{name}-shm"
            for name in sqlite_names
        ):
            continue
        info = os.lstat(path)
        if stat.S_ISREG(info.st_mode):
            entry_type = "regular"
            content = _stream_sha256(path)
        elif stat.S_ISDIR(info.st_mode):
            entry_type = "directory"
            content = None
        else:
            entry_type = "unsafe"
            content = None
        records.append(
            [
                rel,
                entry_type,
                info.st_dev,
                info.st_ino,
                stat.S_IMODE(info.st_mode),
                info.st_size,
                info.st_mtime_ns,
                content,
            ]
        )
    return hashlib.sha256(b"r2f5/tree-v1\0" + _canonical_json(records)).hexdigest()


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sqlite_typed_bytes(value: Any) -> bytes:
    """Encode one SQLite value with an explicit type/null marker."""

    if value is None:
        return b"N"
    if isinstance(value, bool):
        raw = b"1" if value else b"0"
        return b"I" + struct.pack(">Q", len(raw)) + raw
    if isinstance(value, int):
        raw = str(value).encode("ascii")
        return b"I" + struct.pack(">Q", len(raw)) + raw
    if isinstance(value, float):
        raw = repr(value).encode("ascii")
        return b"F" + struct.pack(">Q", len(raw)) + raw
    if isinstance(value, bytes):
        return b"B" + struct.pack(">Q", len(value)) + value
    raw = str(value).encode("utf-8")
    return b"T" + struct.pack(">Q", len(raw)) + raw


def _sqlite_logical_digest(path: Path, role: str) -> str:
    """Compute the approved catalog/table digest without hashing DB/WAL/SHM bytes."""

    catalog_key = _sqlite_catalog_key(path)
    assert catalog_key is not None
    catalog = CATALOGS[catalog_key]
    uri = f"file:{path}?mode=ro&immutable=false"
    chunks = [b"r2f5/sqlite-logical-v1\0"]
    with sqlite3.connect(uri, uri=True, timeout=0) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        chunks.append(_sqlite_typed_bytes(connection.execute("PRAGMA page_count").fetchone()[0]))
        chunks.append(_sqlite_typed_bytes(connection.execute("PRAGMA user_version").fetchone()[0]))
        schema_rows = connection.execute(
            "SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        ).fetchall()
        chunks.append(_sqlite_typed_bytes(tuple(schema_rows)))
        row_total = 0
        encoded_total = 0
        for table_name, table_spec in catalog["tables"].items():
            columns = [item.split(":", 1)[0] for item in table_spec["columns"]]
            order = ", ".join(f'"{column}"' for column in table_spec["order_by"])
            quoted = ", ".join(f'"{column}"' for column in columns)
            rows = connection.execute(
                f'SELECT {quoted} FROM "{table_name}" ORDER BY {order}'
            )
            for row in rows:
                row_total += 1
                if row_total > LIMITS["max_db_rows"]:
                    raise AssertionError("fixture exceeded approved SQLite row bound")
                encoded = _sqlite_typed_bytes(tuple(row))
                encoded_total += len(encoded)
                chunks.append(_sqlite_typed_bytes(table_name))
                chunks.append(encoded)
        assert encoded_total <= LIMITS["max_input_bytes"]
        connection.rollback()
    return hashlib.sha256(b"".join(chunks)).hexdigest()


def _fingerprint(role: str, path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    info = os.lstat(path)
    catalog_key = _sqlite_catalog_key(path)
    if catalog_key is not None:
        captured = f"sqlite-logical:{_sqlite_logical_digest(path, catalog_key)}"
        fingerprint_kind = "content_sha256"
        hash_scope = "none"
    else:
        captured = _tree_content_digest(path) if path.is_dir() else _stream_sha256(path)
        fingerprint_kind = "content_sha256"
        hash_scope = "full_streaming_bytes"
    subject = {
        "descriptor_role": role,
        "descriptor_id": path.name,
        "descriptor_state": "present",
        "device": info.st_dev,
        "inode": info.st_ino,
        "size_bytes": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "ctime_ns": info.st_ctime_ns,
        "fingerprint_kind": fingerprint_kind,
        "hash_scope": hash_scope,
        "captured_content_bytes": captured,
    }
    fingerprint = {
        key: subject[key]
        for key in MODELS["SnapshotFingerprint"]["object_fields"]
        if key != "sha256"
    }
    fingerprint["sha256"] = _digest("SnapshotFingerprint.sha256", subject)
    return fingerprint, subject


def _build_golden_tree(tmp_path: Path) -> GoldenTree:
    dataset = tmp_path / "dataset"
    evidence = tmp_path / "evidence"
    control = tmp_path / "control"
    for root in (dataset, evidence, control):
        root.mkdir(parents=True)
    sessions = _session_dates()
    frozen, frozen_sources = _build_frozen()
    session_fixtures = [
        _build_session(session, ordinal, sessions, frozen)
        for ordinal, session in enumerate(sessions, start=1)
    ]
    window, window_sources = _build_window(sessions)
    qualification_projection = {
        "provider_id": "tickflow",
        "admission_state": "qualified",
        "adapter_hash": "1" * 64,
        "endpoint_contract_hash": "2" * 64,
        "source_schema_hash": "3" * 64,
        "normalizer_hash": "4" * 64,
        "reconciliation_policy_hash": "5" * 64,
        "terms_evidence_hash": "6" * 64,
        "terms_review_id": "terms-review-20",
        "window_id": "qualification-window-20",
        "window_start": sessions[0],
        "window_end": sessions[-1],
        "consecutive_sessions": 20,
        "version_vector_sha256": "7" * 64,
        "calendar_generation": "calendar-g20",
        "calendar_sha256": frozen["calendar_sha256"],
        "window_state": "qualified",
        "last_session_report_id": "qualification-report-20",
        "qualification_evidence_sha256": "8" * 64,
        "qualification_candidate_sha256": "9" * 64,
        "terminal_attestation_id": "qualification-attestation-20",
        "qualification_proof_status": "available",
    }
    completed_sources = {
        "CompletedReplicationRestoreSnapshotV1.replication_observation_sha256": deepcopy(
            session_fixtures[-1].observation["replication_observation"]
        ),
        "CompletedReplicationRestoreSnapshotV1.restore_report_sha256": {
            "restore_report_id": "restore-report-20",
            "immutable_restore_report_bytes": "verified-restore-report",
        },
        "CompletedReplicationRestoreSnapshotV1.destination_record_sha256": {
            "source_commit_id": "source-20",
            "source_commit_bytes": "source",
            "destination_record_id": "destination-record-20",
            "destination_record_bytes": "destination",
            "destination_generation": "destination-g20",
            "immutable_destination_record_bytes": "destination",
        },
        "CompletedReplicationRestoreSnapshotV1.destination_head_sha256": {
            "destination_generation": "destination-g20",
            "destination_head_bytes": "destination-head",
            "immutable_destination_head_bytes": "destination-head",
        },
    }
    completed_replication_restore = {
        "trust_scope": "REMOTE_VERIFIED",
        "destination_generation": "destination-g20",
        "destination_head_sha256": _digest(
            "CompletedReplicationRestoreSnapshotV1.destination_head_sha256",
            completed_sources["CompletedReplicationRestoreSnapshotV1.destination_head_sha256"],
        ),
        "destination_record_sha256": _digest(
            "CompletedReplicationRestoreSnapshotV1.destination_record_sha256",
            completed_sources["CompletedReplicationRestoreSnapshotV1.destination_record_sha256"],
        ),
        "checkpoint_id": "checkpoint-20",
        "replication_policy_version": "reviewed-r2f4-policy-v1",
        "restore_policy_version": "reviewed-r2f4-policy-v1",
        "replication_observation_sha256": _digest(
            "CompletedReplicationRestoreSnapshotV1.replication_observation_sha256",
            completed_sources[
                "CompletedReplicationRestoreSnapshotV1.replication_observation_sha256"
            ],
        ),
        "restore_report_sha256": _digest(
            "CompletedReplicationRestoreSnapshotV1.restore_report_sha256",
            completed_sources["CompletedReplicationRestoreSnapshotV1.restore_report_sha256"],
        ),
        "policy_thresholds": {
            "source": "reviewed-r2f4-policy-evidence",
            "policy_version": "reviewed-r2f4-policy-v1",
            "replication_lag_seconds": 300,
            "restore_duration_seconds": 600,
        },
    }
    _write_json(
        dataset / "manifest.json",
        {
            "dataset": "stock-eva-market",
            "schema_version": 2,
            "generation": "dataset-g20",
            "sessions": sessions,
        },
    )
    _write_json(
        dataset / "calendar.json",
        {
            "source_sequence": sessions,
            "generation": "calendar-g20",
            "confirmed": True,
            "unknown_state": False,
            "conflict_state": False,
        },
    )
    for fixture in session_fixtures:
        _write_json(
            dataset / "sessions" / f"{fixture.observation['session']}.json", fixture.observation
        )
    _write_json(evidence / "frozen_versions.json", frozen)
    _write_json(evidence / "window.json", window)
    _write_json(evidence / "secondary_qualification.json", qualification_projection)
    _write_json(evidence / "completed_replication_restore.json", completed_replication_restore)
    # Digest preimages are retained in the in-memory fixture object for strict
    # test validation only.  They are not sidecar control files and must not be
    # presented as evidence to the future reader.
    controls: dict[str, Path] = {}
    for role, filename in _CATALOG_FILENAMES.items():
        path = control / filename
        _create_catalog(path, role)
        controls[role] = path

    input_fingerprints = []
    fingerprint_sources = []
    for role, path in (("dataset", dataset), ("evidence", evidence)):
        fingerprint, subject = _fingerprint(role, path)
        input_fingerprints.append(fingerprint)
        fingerprint_sources.append(subject)
    for role, path in controls.items():
        public_role = CATALOGS[role]["role"]
        fingerprint, subject = _fingerprint(public_role, path)
        input_fingerprints.append(fingerprint)
        fingerprint_sources.append(subject)
    snapshot = {
        "requested_start": sessions[0],
        "requested_end": sessions[-1],
        "as_of_utc": "2026-09-14T06:00:00Z",
        "as_of_timezone": "Asia/Shanghai",
        "input_fingerprints": input_fingerprints,
        "frozen_versions": frozen,
        "input_fingerprint_sha256": "0" * 64,
        "frozen_version_vector_sha256": "0" * 64,
        "snapshot_sha256": "0" * 64,
    }
    snapshot["input_fingerprint_sha256"] = _digest(
        "SnapshotIdentity.input_fingerprint_sha256", snapshot
    )
    snapshot["frozen_version_vector_sha256"] = _digest(
        "SnapshotIdentity.frozen_version_vector_sha256", snapshot
    )
    snapshot["snapshot_sha256"] = _digest("SnapshotIdentity.snapshot_sha256", snapshot)
    request = {
        "start": sessions[0],
        "end": sessions[-1],
        "local_dataset_root": str(dataset),
        "evidence_root": str(evidence),
        "control_store_roots": [str(controls[role]) for role in CATALOGS],
        "now": "2026-09-14T06:00:00Z",
    }
    golden = GoldenTree(
        request=request,
        dataset=dataset,
        evidence=evidence,
        control=control,
        controls=controls,
        sessions=sessions,
        frozen_versions=frozen,
        frozen_sources=frozen_sources,
        session_fixtures=session_fixtures,
        window_envelope=window,
        window_sources=window_sources,
        qualification_projection=qualification_projection,
        completed_replication_restore=completed_replication_restore,
        completed_sources=completed_sources,
        snapshot_identity=snapshot,
        fingerprint_sources=fingerprint_sources,
    )
    golden.validation_stats = _validate_golden(golden)
    return golden


def _validate_source(
    validator: StrictFixtureValidator, field_path: str, root: dict[str, Any], actual: str
) -> None:
    model = DIGESTS[field_path]["root_object_type"]
    validator.shape(model, root)
    validator.exact_digest(field_path, root, actual)


def _validate_frozen(
    validator: StrictFixtureValidator,
    frozen: dict[str, Any],
    sources: dict[str, dict[str, Any]],
) -> None:
    validator.shape("FrozenReliabilityVersions", frozen)
    assert frozen["qualification_proof_status"] == "available"
    assert frozen["replication_trust_scope"] == "REMOTE_VERIFIED"
    assert frozen["remote_proof_artifact_ref"] is not None
    assert 1 <= len(frozen["provider_priority"]) <= 8
    for field_path, root in sources.items():
        _validate_source(validator, field_path, root, frozen[field_path.rsplit(".", 1)[-1]])


def _validate_session_fixture(
    validator: StrictFixtureValidator,
    fixture: SessionFixture,
    frozen: dict[str, Any],
    ordinal: int,
) -> None:
    observation = fixture.observation
    validator.shape("SessionObservation", observation)
    assert observation["ordinal"] == ordinal
    assert observation["session"] == _session_dates()[ordinal - 1]
    date.fromisoformat(observation["session"])
    _validate_timestamp(observation["same_evening_published_at"])
    _validate_timestamp(observation["next_morning_published_at"])
    assert 1 <= len(observation["canonical_provider_ids"]) <= 8

    evidence = observation["evidence"]
    validator.shape("SessionEvidenceBinding", evidence)
    for field_path in (
        "SessionEvidenceBinding.evidence_sha256",
        "SessionEvidenceBinding.candidate_sha256",
        "SessionEvidenceBinding.gate_report_sha256",
        "SessionEvidenceBinding.manifest_sha256",
        "SessionEvidenceBinding.object_sha256",
        "SessionEvidenceBinding.selection_sha256",
    ):
        _validate_source(
            validator,
            field_path,
            fixture.sources[field_path],
            evidence[field_path.rsplit(".", 1)[-1]],
        )
    validator.exact_digest(
        "SessionEvidenceBinding.binding_sha256", evidence, evidence["binding_sha256"]
    )

    pointer = observation["pointer_reconciliation"]
    validator.shape("PointerReconciliation", pointer)
    for field_path in (
        "PointerReconciliation.pointer_sha256",
        "PointerReconciliation.manifest_sha256",
        "PointerReconciliation.object_sha256",
        "PointerReconciliation.descriptor_sha256",
    ):
        _validate_source(
            validator,
            field_path,
            fixture.sources[field_path],
            pointer[field_path.rsplit(".", 1)[-1]],
        )

    replication = observation["replication_observation"]
    validator.shape("ReplicationObservation", replication)
    assert replication["state"] in {"disabled", "ready", "degraded", "unavailable"}
    assert replication["enqueue_state"] in {
        "pending",
        "copying",
        "verifying",
        "retry_wait",
        "replicated",
        "dead_letter",
    }
    assert replication["reason_code"] in {
        "NONE",
        "DISABLED",
        "SOURCE_NOT_CONFIGURED",
        "SOURCE_UNAVAILABLE",
        "LOCAL_POINTER_MISMATCH",
        "REPLICATION_STATE_UNAVAILABLE",
        "DESTINATION_UNAVAILABLE",
        "DESTINATION_TRUST_FAILED",
        "COPY_FAILED",
        "VERIFY_FAILED",
        "RETRY_WAIT",
        "DEAD_LETTER",
    }
    for field_path in (
        "ReplicationObservation.source_commit_sha256",
        "ReplicationObservation.destination_record_sha256",
        "ReplicationObservation.destination_head_sha256",
    ):
        _validate_source(
            validator,
            field_path,
            fixture.sources[field_path],
            replication[field_path.rsplit(".", 1)[-1]],
        )
    validator.exact_digest(
        "ReplicationObservation.observation_sha256",
        replication,
        replication["observation_sha256"],
    )

    calendar = observation["calendar_raw_facts"]
    validator.shape("CalendarRawFacts", calendar)
    assert len(calendar["source_sequence"]) <= 64
    validator.exact_digest(
        "CalendarRawFacts.raw_facts_sha256", calendar, calendar["raw_facts_sha256"]
    )
    boundary = observation["read_boundary_raw_facts"]
    validator.shape("ReadBoundaryRawFacts", boundary)
    _validate_timestamp(boundary["requested_as_of"])
    validator.exact_digest(
        "ReadBoundaryRawFacts.probe_schema_digest",
        boundary,
        boundary["probe_schema_digest"],
    )
    validator.exact_digest(
        "SessionObservation.frozen_versions_sha256",
        frozen,
        observation["frozen_versions_sha256"],
    )
    validator.exact_digest(
        "SessionObservation.schema_policy_digest",
        observation,
        observation["schema_policy_digest"],
    )
    validator.exact_digest(
        "SessionObservation.observation_sha256",
        observation,
        observation["observation_sha256"],
    )
    validator.metric("same_evening_availability", observation["cutoff_results"]["same_evening"])
    validator.metric("next_morning_availability", observation["cutoff_results"]["next_morning"])
    for metric in (
        "coverage",
        "canonical_integrity",
        "source_purity",
        "provenance",
        "calendar",
        "universe",
        "replication",
        "read_boundary",
    ):
        validator.metric(metric, observation[metric])


def _validate_window(
    validator: StrictFixtureValidator,
    envelope: dict[str, Any],
    sources: dict[str, dict[str, Any]],
) -> None:
    validator.envelope(envelope, "WindowEvidenceBundlePayload")
    payload = envelope["payload"]
    validator.interface_shape("WindowEvidenceBundlePayload", payload)
    assert payload["observation_count"] == CARDINALITY["window_bundle"]
    child_models = {
        "recovery_observation": "RecoveryObservation",
        "failover_observation": "WholeSessionFailoverDrill",
        "replay_sample": "ReplaySampleEvidence",
        "adjustment_equivalence": "AdjustmentEquivalenceEvidence",
        "error_handling_observation": "ErrorHandlingObservation",
        "local_nas_isolation_observation": "LocalNasIsolationObservation",
        "restore_observation": "RestoreDrillEvidence",
    }
    for name, model in child_models.items():
        child = payload[name]
        validator.envelope(child, model)
        child_payload = child["payload"]
        if model in MODELS:
            validator.shape(model, child_payload)
        else:
            validator.interface_shape(model, child_payload)

    recovery = payload["recovery_observation"]["payload"]
    for field_path in (
        "RecoveryObservation.after_manifest_sha256",
        "RecoveryObservation.after_pointer_sha256",
        "RecoveryObservation.after_selection_sha256",
    ):
        _validate_source(
            validator,
            field_path,
            sources[field_path],
            recovery[field_path.rsplit(".", 1)[-1]],
        )
    validator.exact_digest(
        "RecoveryObservation.duplicate_proof_sha256",
        recovery,
        recovery["duplicate_proof_sha256"],
    )
    validator.exact_digest(
        "RecoveryObservation.observation_sha256", recovery, recovery["observation_sha256"]
    )

    failover = payload["failover_observation"]["payload"]
    for field_path in (
        "WholeSessionFailoverDrill.selection_sha256",
        "WholeSessionFailoverDrill.manifest_sha256",
        "WholeSessionFailoverDrill.pointer_sha256",
        "WholeSessionFailoverDrill.readback_sha256",
    ):
        _validate_source(
            validator,
            field_path,
            sources[field_path],
            failover[field_path.rsplit(".", 1)[-1]],
        )
    replay = payload["replay_sample"]["payload"]
    for field_path in (
        "ReplaySampleEvidence.sample_object_sha256",
        "ReplaySampleEvidence.candidate_sha256",
    ):
        _validate_source(
            validator,
            field_path,
            sources[field_path],
            replay[field_path.rsplit(".", 1)[-1]],
        )
    offline = replay["offline_context"]
    validator.shape("OfflineReplayContext", offline)
    _validate_source(
        validator,
        "OfflineReplayContext.implementation_sha256",
        sources["OfflineReplayContext.implementation_sha256"],
        offline["implementation_sha256"],
    )
    assert offline["network_allowed"] is False and offline["provider_requests"] == 0

    errors = payload["error_handling_observation"]["payload"]
    events = errors["events"]
    assert len(events) == CARDINALITY["error_classes"]
    expected_classes = ("timeout", "auth", "rate", "schema", "coverage", "storage")
    assert tuple(item["forced_error_class"] for item in events) == expected_classes
    tuple_fields = set(MODELS["ErrorHandlingObservation"]["tuple_item_schemas"]["events"])
    for event, forced_class in zip(events, expected_classes, strict=True):
        assert set(event) == tuple_fields
        _validate_source(
            validator,
            "ErrorHandlingObservation.events.evidence_sha256",
            sources[f"ErrorHandlingObservation.events.evidence_sha256:{forced_class}"],
            event["evidence_sha256"],
        )
    validator.exact_digest(
        "ErrorHandlingObservation.observation_sha256", errors, errors["observation_sha256"]
    )

    local_nas = payload["local_nas_isolation_observation"]["payload"]
    _validate_source(
        validator,
        "LocalNasIsolationObservation.local_pointer_sha256",
        sources["LocalNasIsolationObservation.local_pointer_sha256"],
        local_nas["local_pointer_sha256"],
    )
    validator.exact_digest(
        "LocalNasIsolationObservation.observation_sha256",
        local_nas,
        local_nas["observation_sha256"],
    )

    restore = payload["restore_observation"]["payload"]
    for field_path in (
        "RestoreDrillEvidence.sentinel_sha256",
        "RestoreDrillEvidence.destination_head_sha256",
        "RestoreDrillEvidence.record_sha256",
        "RestoreDrillEvidence.manifest_sha256",
        "RestoreDrillEvidence.restore_report_sha256",
        "RestoreDrillEvidence.api_readback_sha256",
    ):
        _validate_source(
            validator,
            field_path,
            sources[field_path],
            restore[field_path.rsplit(".", 1)[-1]],
        )


def _validate_golden(golden: GoldenTree) -> ValidationStats:
    validator = StrictFixtureValidator()
    assert set(golden.request) == INPUT_KEYS
    validator.walk_scalars(golden.request)
    _validate_frozen(validator, golden.frozen_versions, golden.frozen_sources)
    assert len(golden.sessions) == CARDINALITY["sessions"]
    assert golden.sessions == sorted(set(golden.sessions))
    assert len(golden.session_fixtures) == CARDINALITY["sessions"]
    for ordinal, fixture in enumerate(golden.session_fixtures, start=1):
        _validate_session_fixture(validator, fixture, golden.frozen_versions, ordinal)
    _validate_window(validator, golden.window_envelope, golden.window_sources)
    validator.interface_shape("SecondaryQualificationProjection", golden.qualification_projection)
    assert golden.qualification_projection["qualification_proof_status"] == "available"
    completed = golden.completed_replication_restore
    validator.shape("CompletedReplicationRestoreSnapshotV1", completed)
    assert completed["trust_scope"] == "REMOTE_VERIFIED"
    validator.interface_shape("FrozenR2F4PolicyThresholds", completed["policy_thresholds"])
    for field_path, root in golden.completed_sources.items():
        _validate_source(
            validator,
            field_path,
            root,
            completed[field_path.rsplit(".", 1)[-1]],
        )
    validator.shape("SnapshotIdentity", golden.snapshot_identity)
    for fingerprint, subject in zip(
        golden.snapshot_identity["input_fingerprints"],
        golden.fingerprint_sources,
        strict=True,
    ):
        validator.shape("SnapshotFingerprint", fingerprint)
        validator.shape("FingerprintSubject", subject)
        validator.exact_digest("SnapshotFingerprint.sha256", subject, fingerprint["sha256"])
    validator.exact_digest(
        "SnapshotIdentity.input_fingerprint_sha256",
        golden.snapshot_identity,
        golden.snapshot_identity["input_fingerprint_sha256"],
    )
    validator.exact_digest(
        "SnapshotIdentity.frozen_version_vector_sha256",
        golden.snapshot_identity,
        golden.snapshot_identity["frozen_version_vector_sha256"],
    )
    validator.exact_digest(
        "SnapshotIdentity.snapshot_sha256",
        golden.snapshot_identity,
        golden.snapshot_identity["snapshot_sha256"],
    )
    readonly_object = {
        "descriptor_id": "readonly-evidence-20",
        "immutable_object_bytes": "readonly-object",
    }
    readonly = {
        "descriptor_id": "readonly-evidence-20",
        "object_sha256": _digest("ReadonlyEvidenceDescriptor.object_sha256", readonly_object),
        "descriptor_sha256": "0" * 64,
        "immutable": True,
        "completed": True,
        "source_generation": "dataset-g20",
    }
    readonly["descriptor_sha256"] = _digest(
        "ReadonlyEvidenceDescriptor.descriptor_sha256", readonly
    )
    _validate_source(
        validator,
        "ReadonlyEvidenceDescriptor.object_sha256",
        readonly_object,
        readonly["object_sha256"],
    )
    validator.shape("ReadonlyEvidenceDescriptor", readonly)
    validator.exact_digest(
        "ReadonlyEvidenceDescriptor.descriptor_sha256",
        readonly,
        readonly["descriptor_sha256"],
    )
    pre_capture = {
        "schema_version": "r2f5-pre-capture-failure-v1",
        "reason_code": "CONTROL_STATE_UNAVAILABLE",
        "requested_start": golden.sessions[0],
        "requested_end": golden.sessions[-1],
        "as_of_utc": "2026-09-14T06:00:00Z",
        "descriptor_states": ["control_absent"],
        "semantic_report_sha256": "0" * 64,
    }
    pre_capture["semantic_report_sha256"] = _digest(
        "PreCaptureFailurePayloadV1.semantic_report_sha256", pre_capture
    )
    validator.shape("PreCaptureFailurePayloadV1", pre_capture)
    validator.exact_digest(
        "PreCaptureFailurePayloadV1.semantic_report_sha256",
        pre_capture,
        pre_capture["semantic_report_sha256"],
    )
    semantic_report = {
        field_name: None for field_name in MODELS["R2FAcceptanceReport"]["object_fields"]
    }
    semantic_report.update(
        {
            "status": "unavailable",
            "selected_sessions": [],
            "quality_issues": ["CONTROL_STATE_UNAVAILABLE"],
            "session_observations": [],
            "observation_refs": [],
            "window_evidence_refs": [],
            "pre_capture_failure": pre_capture,
            "semantic_report_sha256": "0" * 64,
            "provider_requests": 0,
            "writes": False,
            "restore_started": False,
            "production_window_started": False,
        }
    )
    semantic_report["semantic_report_sha256"] = _digest(
        "R2FAcceptanceReport.semantic_report_sha256", semantic_report
    )
    validator.shape("R2FAcceptanceReport", semantic_report)
    validator.exact_digest(
        "R2FAcceptanceReport.semantic_report_sha256",
        semantic_report,
        semantic_report["semantic_report_sha256"],
    )
    for role, path in golden.controls.items():
        _validate_catalog(path, role, validator)
    validator.walk_scalars(golden.frozen_versions)
    validator.walk_scalars([item.observation for item in golden.session_fixtures])
    validator.walk_scalars(golden.window_envelope)
    validator.walk_scalars(golden.snapshot_identity)
    assert validator.stats.digests >= 61
    assert validator.stats.digest_fields == set(DIGESTS), (
        set(DIGESTS) - validator.stats.digest_fields,
        validator.stats.digest_fields - set(DIGESTS),
    )
    return validator.stats


@pytest.fixture
def golden(tmp_path: Path) -> GoldenTree:
    return _build_golden_tree(tmp_path)


def _red_reader() -> Any:
    try:
        return importlib.import_module(TARGET_MODULE)
    except ModuleNotFoundError as error:
        if error.name != TARGET_MODULE:
            raise
        pytest.fail(f"RED: missing target module {TARGET_MODULE}")


def _evaluate(request: dict[str, Any]) -> dict[str, Any]:
    assert set(request) == INPUT_KEYS
    reader_type = _red_reader().AcceptanceReader
    result = reader_type().evaluate(request)
    if isinstance(result, dict):
        return result
    return result.model_dump(mode="json")


def _physical_fingerprint(root: Path) -> tuple[tuple[Any, ...], ...]:
    """Capture zero-write evidence only; logical DB identity is separate."""

    if not root.exists():
        return ()
    values = []
    for path in sorted(root.rglob("*")):
        info = os.lstat(path)
        # SQLite/WAL/SHM bytes are intentionally excluded.  A transactionally
        # stable logical digest proves identity; this tuple only proves that the
        # acceptance reader did not write, replace, or create an entry.
        is_sqlite_sidecar = path.suffix in {".sqlite", ".sqlite3"} or path.name.endswith(
            ("-wal", "-shm")
        )
        digest = None
        values.append(
            (
                path.relative_to(root).as_posix(),
                info.st_mode,
                info.st_ino,
                info.st_size,
                info.st_mtime_ns,
                digest,
                is_sqlite_sidecar,
            )
        )
    return tuple(values)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _rehash_envelope(envelope: dict[str, Any]) -> None:
    envelope["payload_sha256"] = _digest("ImmutableObservationEnvelopeV1.payload_sha256", envelope)
    envelope["envelope_sha256"] = _digest(
        "ImmutableObservationEnvelopeV1.envelope_sha256", envelope
    )


def _mutate_window(golden: GoldenTree, child_name: str, updates: dict[str, Any]) -> None:
    path = golden.evidence / "window.json"
    window = _read_json(path)
    child = window["payload"][child_name]
    child["payload"].update(updates)
    payload = child["payload"]
    self_hashes = {
        "recovery_observation": (
            "RecoveryObservation.duplicate_proof_sha256",
            "RecoveryObservation.observation_sha256",
        ),
        "error_handling_observation": ("ErrorHandlingObservation.observation_sha256",),
        "local_nas_isolation_observation": ("LocalNasIsolationObservation.observation_sha256",),
    }
    for field_path in self_hashes.get(child_name, ()):
        payload[field_path.rsplit(".", 1)[-1]] = _digest(field_path, payload)
    _rehash_envelope(child)
    _rehash_envelope(window)
    _write_json(path, window)


def _mutate_session(
    golden: GoldenTree,
    updates: dict[str, Any],
    *,
    ordinal: int = 1,
    nested: str | None = None,
    tamper_hash: bool = False,
) -> None:
    session = golden.sessions[ordinal - 1]
    path = golden.dataset / "sessions" / f"{session}.json"
    observation = _read_json(path)
    target = observation if nested is None else observation[nested]
    target.update(updates)
    if not tamper_hash:
        if nested == "calendar_raw_facts":
            target["raw_facts_sha256"] = _digest("CalendarRawFacts.raw_facts_sha256", target)
        elif nested == "read_boundary_raw_facts":
            target["probe_schema_digest"] = _digest(
                "ReadBoundaryRawFacts.probe_schema_digest", target
            )
        elif nested == "replication_observation":
            target["observation_sha256"] = _digest(
                "ReplicationObservation.observation_sha256", target
            )
        observation["schema_policy_digest"] = _digest(
            "SessionObservation.schema_policy_digest", observation
        )
        observation["observation_sha256"] = _digest(
            "SessionObservation.observation_sha256", observation
        )
    _write_json(path, observation)


def _drop_window_child(golden: GoldenTree, child_name: str) -> None:
    path = golden.evidence / "window.json"
    window = _read_json(path)
    window["payload"][child_name] = None
    _rehash_envelope(window)
    _write_json(path, window)


def _mutate_calendar(golden: GoldenTree, mutation: str) -> None:
    path = golden.dataset / "calendar.json"
    value = _read_json(path)
    sequence = list(value["source_sequence"])
    if mutation == "calendar19":
        sequence.pop()
    elif mutation == "calendar21":
        sequence.append("2026-08-31")
    elif mutation == "calendar_duplicate":
        sequence[5] = sequence[4]
    elif mutation == "calendar_out_of_order":
        sequence[5], sequence[6] = sequence[6], sequence[5]
    elif mutation == "calendar_future":
        sequence[-1] = "2099-01-01"
    elif mutation == "calendar_conflict":
        value["conflict_state"] = True
    elif mutation == "calendar_unknown":
        value["unknown_state"] = True
    else:
        raise AssertionError(mutation)
    value["source_sequence"] = sequence
    _write_json(path, value)


def _apply_mutation(golden: GoldenTree, mutation: str) -> dict[str, Any]:
    request = deepcopy(golden.request)
    if mutation == "missing_control":
        golden.controls["universe"].unlink()
    elif mutation == "corrupt_control":
        golden.controls["replication_sidecar"].write_bytes(b"not a sqlite database")
    elif mutation == "catalog_extra":
        with sqlite3.connect(golden.controls["calendar_generation"]) as connection:
            connection.execute("CREATE TABLE unexpected_catalog_object (id INTEGER PRIMARY KEY)")
    elif mutation == "relative_path":
        request["local_dataset_root"] = "relative/dataset"
    elif mutation == "root_path":
        request["local_dataset_root"] = "/"
    elif mutation == "overlap_path":
        request["evidence_root"] = request["local_dataset_root"]
    elif mutation == "invalid_range":
        request["start"] = "2026-02-30"
    elif mutation == "reversed_range":
        request["start"], request["end"] = request["end"], request["start"]
    elif mutation == "naive_clock":
        request["now"] = "2026-09-14T06:00:00"
    elif mutation.startswith("calendar"):
        _mutate_calendar(golden, mutation)
    elif mutation == "cutoff_evening":
        for ordinal in (1, 2, 3):
            _mutate_session(
                golden,
                {"same_evening_published_at": (f"{golden.sessions[ordinal - 1]}T13:16:00Z")},
                ordinal=ordinal,
            )
    elif mutation == "cutoff_morning":
        next_day = date.fromisoformat(golden.sessions[0]) + timedelta(days=1)
        _mutate_session(
            golden,
            {"next_morning_published_at": f"{next_day.isoformat()}T00:01:00Z"},
        )
    elif mutation == "coverage_fail":
        _mutate_session(golden, {"loaded_count": 4_999})
    elif mutation == "continuity_gap":
        path = golden.dataset / "manifest.json"
        value = _read_json(path)
        value["sessions"].pop(10)
        _write_json(path, value)
    elif mutation == "universe_unknown":
        _mutate_session(golden, {"unknown_count": 1})
    elif mutation == "mixed_source":
        _mutate_session(golden, {"canonical_provider_ids": ["baostock", "tickflow"]})
    elif mutation == "pointer_mismatch":
        _mutate_session(
            golden,
            {"pointer_manifest_object_match": False},
            nested="pointer_reconciliation",
        )
    elif mutation == "version_drift":
        _mutate_session(golden, {"frozen_versions_sha256": "f" * 64}, ordinal=10)
    elif mutation == "missing_version":
        path = golden.evidence / "frozen_versions.json"
        value = _read_json(path)
        del value["endpoint_contract_hash"]
        _write_json(path, value)
    elif mutation == "lineage_missing":
        # Mutate the formal SessionObservation binding, not a synthetic source
        # map.  The zero digest is a validly-shaped but unavailable lineage leaf.
        _mutate_session(
            golden,
            {"object_sha256": "0" * 64},
            nested="evidence",
        )
    elif mutation == "lineage_tamper":
        _mutate_session(
            golden,
            {"evidence_sha256": "f" * 64},
            nested="evidence",
        )
    elif mutation == "replay_unknown":
        path = golden.evidence / "window.json"
        window = _read_json(path)
        child = window["payload"]["replay_sample"]
        offline = child["payload"]["offline_context"]
        offline["adapter_id"] = "unknown-adapter"
        offline["implementation_sha256"] = "f" * 64
        _rehash_envelope(child)
        _rehash_envelope(window)
        _write_json(path, window)
    elif mutation == "replay_mismatch":
        _mutate_window(golden, "replay_sample", {"semantic_equal": False})
    elif mutation == "replay_missing":
        _drop_window_child(golden, "replay_sample")
    elif mutation == "recovery_fail":
        _mutate_window(
            golden,
            "recovery_observation",
            {"before_generation": "dataset-g20"},
        )
    elif mutation == "recovery_missing":
        _drop_window_child(golden, "recovery_observation")
    elif mutation == "failover_missing":
        _drop_window_child(golden, "failover_observation")
    elif mutation == "failover_baostock":
        _mutate_window(
            golden,
            "failover_observation",
            {
                "primary_unavailable": False,
                "selected_provider_id": "baostock",
                "mixed_source_rows": 0,
            },
        )
    elif mutation == "failover_identity_mismatch":
        _mutate_window(
            golden,
            "failover_observation",
            {"selected_provider_id": "tushare"},
        )
    elif mutation == "adjustment_missing":
        _drop_window_child(golden, "adjustment_equivalence")
    elif mutation == "adjustment_mismatch":
        _mutate_window(
            golden,
            "adjustment_equivalence",
            {"compared_sessions": ["2026-01-01", "2026-01-02"]},
        )
    elif mutation == "error_fail":
        path = golden.evidence / "window.json"
        window = _read_json(path)
        child = window["payload"]["error_handling_observation"]
        child["payload"]["events"][0]["sanitized_reason"] = "COVERAGE_FAILED"
        child["payload"]["observation_sha256"] = _digest(
            "ErrorHandlingObservation.observation_sha256", child["payload"]
        )
        _rehash_envelope(child)
        _rehash_envelope(window)
        _write_json(path, window)
    elif mutation == "error_missing":
        _drop_window_child(golden, "error_handling_observation")
    elif mutation == "nas_fail":
        _mutate_window(
            golden,
            "local_nas_isolation_observation",
            {"retryable": False},
        )
    elif mutation == "nas_missing":
        _drop_window_child(golden, "local_nas_isolation_observation")
    elif mutation == "replication_local":
        _mutate_session(
            golden,
            {"trust_scope": "LOCAL_CHAIN_ONLY"},
            nested="replication_observation",
        )
    elif mutation == "replication_lag":
        _mutate_session(golden, {"lag_seconds": 301}, nested="replication_observation")
    elif mutation == "missing_threshold":
        path = golden.evidence / "completed_replication_restore.json"
        value = _read_json(path)
        value["policy_thresholds"] = None
        _write_json(path, value)
    elif mutation == "restore_missing":
        _drop_window_child(golden, "restore_observation")
    elif mutation == "restore_fail":
        _mutate_window(golden, "restore_observation", {"row_count": 99_999})
    elif mutation == "restore_tamper":
        path = golden.evidence / "window.json"
        window = _read_json(path)
        window["payload"]["restore_observation"]["payload_sha256"] = "f" * 64
        _write_json(path, window)
    elif mutation == "read_write":
        _mutate_session(golden, {"write_count": 1}, nested="read_boundary_raw_facts")
    elif mutation == "negative_count":
        _mutate_session(golden, {"required_count": -1})
    elif mutation == "oversized_id":
        path = golden.evidence / "window.json"
        window = _read_json(path)
        window["artifact_id"] = "x" * 129
        _rehash_envelope(window)
        _write_json(path, window)
    elif mutation == "creator_invalid":
        path = golden.evidence / "window.json"
        window = _read_json(path)
        window["creator_kind"] = "test_fixture"
        _rehash_envelope(window)
        _write_json(path, window)
    elif mutation == "session_missing":
        (golden.dataset / "sessions" / f"{golden.sessions[-1]}.json").unlink()
    elif mutation == "session_hash_tamper":
        _mutate_session(golden, {"observation_sha256": "f" * 64}, tamper_hash=True)
    elif mutation == "input_roots_limit":
        copies = []
        for index in range(LIMITS["max_input_roots"] + 1):
            target = golden.control / f"extra-{index:02d}.sqlite3"
            shutil.copy2(golden.controls["replication_sidecar"], target)
            copies.append(str(target))
        request["control_store_roots"] = copies
    elif mutation == "quality_source_change":
        _mutate_session(golden, {"future_rows_seen": True}, nested="read_boundary_raw_facts")
    else:
        raise AssertionError(f"unhandled real mutation: {mutation}")
    return request


@dataclass(frozen=True)
class BehaviorCase:
    requirement: str
    anchor: str
    mutation: str
    target_path: str
    report_status: str
    metric: str
    metric_status: str
    reason: str
    expected_path: str
    mode: str = "reader"


def _case(
    requirement: str,
    mutation: str,
    target_path: str,
    report_status: str,
    metric: str,
    reason: str,
    *,
    mode: str = "reader",
) -> BehaviorCase:
    target_path = _canonical_target_path(target_path)
    assert _approved_target_path(target_path), target_path
    anchor = next(anchor for req, _summary, anchor in REQUIREMENT_ROWS if req == requirement)
    metric_status = "unavailable" if report_status == "unavailable" else "fail"
    reason_partition = "unavailable" if metric_status == "unavailable" else "failure"
    assert reason in REASON_PARTITIONS[reason_partition], (requirement, report_status, reason)
    return BehaviorCase(
        requirement,
        anchor,
        mutation,
        target_path,
        report_status,
        metric,
        metric_status,
        reason,
        f"$.{metric}.reason_code",
        mode,
    )


def _approved_target_path(path: str) -> bool:
    root, _, remainder = path.partition(".")
    if root == "AcceptanceInput":
        return remainder in INPUT_KEYS
    if root == "WindowEvidenceBundlePayload":
        return remainder.split(".", 1)[0] in _interface_fields(root)
    if root == "CapturedSnapshot":
        return remainder.split(".", 1)[0].removesuffix("[]") in _interface_fields(root)
    if root not in MODELS:
        return False
    field = remainder.split(".", 1)[0].removesuffix("[]")
    return field in MODELS[root]["object_fields"]


def _canonical_target_path(raw: str) -> str:
    """Translate display metadata to an approved model/reducer path."""

    request_fields = {
        "request.local_dataset_root": "AcceptanceInput.local_dataset_root",
        "request.evidence_root": "AcceptanceInput.evidence_root",
        "request.control_store_roots": "AcceptanceInput.control_store_roots",
        "request.start": "AcceptanceInput.start",
        "request.start/end": "AcceptanceInput.start",
        "request.now": "AcceptanceInput.now",
    }
    if raw in request_fields:
        return request_fields[raw]
    if raw.startswith("control/"):
        return "SnapshotFingerprint.descriptor_role"
    if raw.startswith("dataset/calendar"):
        field = raw.split(".", 1)[-1].split("[", 1)[0]
        return f"CalendarRawFacts.{field}"
    if raw == "dataset/sessions/20":
        return "CapturedSnapshot.session_observations[]"
    if raw.startswith("dataset/sessions/"):
        field = raw.rsplit(".", 1)[-1].split("[", 1)[0]
        if raw.find(".replication.") >= 0:
            return f"ReplicationObservation.{field}"
        if raw.endswith(".replication.trust_scope"):
            return "ReplicationObservation.trust_scope"
        if raw.endswith(".replication.lag_seconds"):
            return "ReplicationObservation.lag_seconds"
        if raw.find(".read_boundary") >= 0:
            if field == "read_boundary":
                field = "write_count"
            return f"ReadBoundaryRawFacts.{field}"
        if raw.find(".observation_sha256") >= 0:
            return "SessionObservation.observation_sha256"
        return f"SessionObservation.{field}"
    if raw.startswith("evidence/frozen_versions."):
        return f"FrozenReliabilityVersions.{raw.rsplit('.', 1)[-1]}"
    if raw.startswith("evidence/completed.policy_thresholds"):
        return "CompletedReplicationRestoreSnapshotV1.policy_thresholds"
    if raw.startswith("evidence/window."):
        suffix = raw[len("evidence/window.") :]
        field = suffix.rsplit(".", 1)[-1].split("[", 1)[0]
        if suffix.startswith("replay.offline_context."):
            return f"OfflineReplayContext.{field}"
        if field in {"payload_sha256", "creator_kind", "artifact_id"}:
            return f"ImmutableObservationEnvelopeV1.{field}"
        if "." not in suffix:
            return "WindowEvidenceBundlePayload." + field
        if suffix.startswith("failover."):
            return f"WholeSessionFailoverDrill.{field}"
        return "WindowEvidenceBundlePayload." + field
    return raw


_BEHAVIOR_ROWS: dict[str, tuple[str, str, str, str, str, str]] = {
    "FR-1": (
        "missing_control",
        "control/universe",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "FR-2": (
        "relative_path",
        "request.local_dataset_root",
        "unavailable",
        "read_boundary",
        "PATH_INVALID",
        "reader",
    ),
    "FR-3": (
        "session_hash_tamper",
        "dataset/sessions/1.observation_sha256",
        "unavailable",
        "canonical_integrity",
        "SNAPSHOT_CHANGED",
        "reader",
    ),
    "FR-4": (
        "missing_control",
        "control/universe",
        "unavailable",
        "calendar",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "FR-5": (
        "calendar19",
        "dataset/calendar.source_sequence",
        "unavailable",
        "continuity",
        "SESSION_COUNT_NOT_20",
        "reader",
    ),
    "FR-6": (
        "calendar_duplicate",
        "dataset/calendar.source_sequence[5]",
        "unavailable",
        "calendar",
        "SESSION_SEQUENCE_INVALID",
        "reader",
    ),
    "FR-7": (
        "version_drift",
        "dataset/sessions/10.frozen_versions_sha256",
        "not_ready",
        "provenance",
        "VERSION_DRIFT",
        "reader",
    ),
    "FR-8": (
        "cutoff_evening",
        "dataset/sessions/1.same_evening_published_at",
        "not_ready",
        "same_evening_availability",
        "AVAILABILITY_CUTOFF_FAILED",
        "reader",
    ),
    "FR-9": (
        "coverage_fail",
        "dataset/sessions/1.loaded_count",
        "not_ready",
        "coverage",
        "COVERAGE_FAILED",
        "reader",
    ),
    "FR-10": (
        "lineage_missing",
        "SessionObservation.evidence.object_sha256",
        "unavailable",
        "provenance",
        "LINEAGE_UNAVAILABLE",
        "reader",
    ),
    "FR-11": (
        "replay_unknown",
        "evidence/window.replay.offline_context.adapter_id",
        "unavailable",
        "replay",
        "REPLAY_UNAVAILABLE",
        "reader",
    ),
    "FR-12": (
        "calendar_conflict",
        "dataset/calendar.conflict_state",
        "not_ready",
        "calendar",
        "CALENDAR_CONFLICT",
        "reader",
    ),
    "FR-13": (
        "restore_missing",
        "evidence/window.restore_observation",
        "unavailable",
        "restore",
        "RESTORE_UNAVAILABLE",
        "reader",
    ),
    "FR-14": (
        "missing_control",
        "control/universe",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "FR-15": (
        "invalid_range",
        "request.start",
        "unavailable",
        "read_boundary",
        "INVALID_ARGUMENTS",
        "cli",
    ),
    "FR-16": (
        "missing_control",
        "control/universe",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "api",
    ),
    "FR-17": (
        "corrupt_control",
        "control/replication",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "FR-18": (
        "creator_invalid",
        "evidence/window.creator_kind",
        "unavailable",
        "canonical_integrity",
        "LINEAGE_UNAVAILABLE",
        "reader",
    ),
    "FR-19": (
        "negative_count",
        "dataset/sessions/1.required_count",
        "unavailable",
        "coverage",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "FR-20": (
        "failover_baostock",
        "evidence/window.failover.selected_provider_id",
        "unavailable",
        "failover",
        "FAILOVER_UNAVAILABLE",
        "reader",
    ),
    "FR-21": (
        "replication_local",
        "dataset/sessions/1.replication.trust_scope",
        "unavailable",
        "replication",
        "REMOTE_PROOF_MISSING",
        "reader",
    ),
    "FR-22": (
        "replay_unknown",
        "evidence/window.replay.offline_context.adapter_id",
        "unavailable",
        "replay",
        "REPLAY_UNAVAILABLE",
        "provider",
    ),
    "FR-23": (
        "missing_version",
        "evidence/frozen_versions.endpoint_contract_hash",
        "unavailable",
        "provenance",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "FR-24": (
        "calendar_duplicate",
        "dataset/calendar.source_sequence[5]",
        "unavailable",
        "calendar",
        "SESSION_SEQUENCE_INVALID",
        "reader",
    ),
    "FR-25": (
        "session_missing",
        "dataset/sessions/20",
        "unavailable",
        "continuity",
        "LINEAGE_UNAVAILABLE",
        "reader",
    ),
    "FR-26": (
        "quality_source_change",
        "dataset/sessions/1.read_boundary",
        "not_ready",
        "read_boundary",
        "READ_BOUNDARY_FAILED",
        "determinism",
    ),
    "FR-27": (
        "oversized_id",
        "evidence/window.artifact_id",
        "unavailable",
        "read_boundary",
        "INVALID_ARGUMENTS",
        "reader",
    ),
    "FR-28": (
        "session_hash_tamper",
        "dataset/sessions/1.observation_sha256",
        "unavailable",
        "read_boundary",
        "SNAPSHOT_CHANGED",
        "toctou",
    ),
    "NFR-1": (
        "catalog_extra",
        "control/calendar.sqlite_master",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "NFR-2": (
        "corrupt_control",
        "control/replication",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "NFR-3": (
        "corrupt_control",
        "control/replication",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "redaction",
    ),
    "NFR-4": (
        "input_roots_limit",
        "request.control_store_roots",
        "unavailable",
        "read_boundary",
        "INPUT_LIMIT_EXCEEDED",
        "performance",
    ),
    "NFR-5": (
        "root_path",
        "request.local_dataset_root",
        "unavailable",
        "read_boundary",
        "PATH_INVALID",
        "reader",
    ),
    "NFR-6": (
        "input_roots_limit",
        "request.control_store_roots",
        "unavailable",
        "read_boundary",
        "INPUT_LIMIT_EXCEEDED",
        "reader",
    ),
    "NFR-7": (
        "input_roots_limit",
        "request.control_store_roots",
        "unavailable",
        "read_boundary",
        "INPUT_LIMIT_EXCEEDED",
        "performance",
    ),
    "NFR-8": (
        "version_drift",
        "dataset/sessions/10.frozen_versions_sha256",
        "not_ready",
        "provenance",
        "VERSION_DRIFT",
        "reader",
    ),
    "NFR-9": (
        "catalog_extra",
        "control/calendar.sqlite_master",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "NFR-10": (
        "creator_invalid",
        "evidence/window.creator_kind",
        "unavailable",
        "canonical_integrity",
        "LINEAGE_UNAVAILABLE",
        "reader",
    ),
    "NFR-11": (
        "coverage_fail",
        "dataset/sessions/1.loaded_count",
        "not_ready",
        "coverage",
        "COVERAGE_FAILED",
        "reader",
    ),
    "NFR-12": (
        "missing_threshold",
        "evidence/completed.policy_thresholds",
        "unavailable",
        "replication",
        "REPLICATION_UNAVAILABLE",
        "reader",
    ),
    "NFR-13": (
        "session_hash_tamper",
        "dataset/sessions/1.observation_sha256",
        "unavailable",
        "read_boundary",
        "SNAPSHOT_CHANGED",
        "wal",
    ),
    "NFR-14": (
        "lineage_tamper",
        "SessionObservation.evidence.object_sha256",
        "not_ready",
        "canonical_integrity",
        "CANONICAL_INTEGRITY_FAILED",
        "reader",
    ),
    "NFR-15": (
        "missing_control",
        "control/universe",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "api",
    ),
    "AC-1": (
        "calendar21",
        "dataset/calendar.source_sequence",
        "unavailable",
        "continuity",
        "SESSION_COUNT_NOT_20",
        "reader",
    ),
    "AC-2": (
        "cutoff_morning",
        "dataset/sessions/1.next_morning_published_at",
        "not_ready",
        "next_morning_availability",
        "AVAILABILITY_CUTOFF_FAILED",
        "reader",
    ),
    "AC-3": (
        "version_drift",
        "dataset/sessions/10.frozen_versions_sha256",
        "not_ready",
        "provenance",
        "VERSION_DRIFT",
        "reader",
    ),
    "AC-4": (
        "mixed_source",
        "dataset/sessions/1.canonical_provider_ids",
        "not_ready",
        "source_purity",
        "SOURCE_PURITY_FAILED",
        "reader",
    ),
    "AC-5": (
        "lineage_tamper",
        "SessionObservation.evidence.object_sha256",
        "not_ready",
        "provenance",
        "LINEAGE_INVALID",
        "reader",
    ),
    "AC-6": (
        "replication_lag",
        "dataset/sessions/1.replication.lag_seconds",
        "not_ready",
        "replication",
        "REPLICATION_LAG",
        "reader",
    ),
    "AC-7": (
        "missing_control",
        "control/universe",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "AC-8": (
        "overlap_path",
        "request.evidence_root",
        "unavailable",
        "read_boundary",
        "PATH_INVALID",
        "reader",
    ),
    "AC-9": (
        "missing_control",
        "control/universe",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "parity",
    ),
    "AC-10": (
        "corrupt_control",
        "control/replication",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "AC-11": (
        "input_roots_limit",
        "request.control_store_roots",
        "unavailable",
        "read_boundary",
        "INPUT_LIMIT_EXCEEDED",
        "performance",
    ),
    "AC-12": (
        "catalog_extra",
        "control/calendar.sqlite_master",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "AC-13": (
        "creator_invalid",
        "evidence/window.creator_kind",
        "unavailable",
        "canonical_integrity",
        "LINEAGE_UNAVAILABLE",
        "reader",
    ),
    "AC-14": (
        "quality_source_change",
        "dataset/sessions/1.read_boundary",
        "not_ready",
        "read_boundary",
        "READ_BOUNDARY_FAILED",
        "determinism",
    ),
    "AC-15": (
        "negative_count",
        "dataset/sessions/1.required_count",
        "unavailable",
        "coverage",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "AC-16": (
        "failover_missing",
        "evidence/window.failover_observation",
        "unavailable",
        "failover",
        "FAILOVER_UNAVAILABLE",
        "reader",
    ),
    "AC-17": (
        "replication_local",
        "dataset/sessions/1.replication.trust_scope",
        "unavailable",
        "replication",
        "REMOTE_PROOF_MISSING",
        "reader",
    ),
    "AC-18": (
        "replay_unknown",
        "evidence/window.replay.offline_context.adapter_id",
        "unavailable",
        "replay",
        "REPLAY_UNAVAILABLE",
        "provider",
    ),
    "AC-19": (
        "missing_version",
        "evidence/frozen_versions.endpoint_contract_hash",
        "unavailable",
        "provenance",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "AC-20": (
        "calendar_out_of_order",
        "dataset/calendar.source_sequence",
        "unavailable",
        "calendar",
        "SESSION_SEQUENCE_INVALID",
        "reader",
    ),
    "AC-21": (
        "session_missing",
        "dataset/sessions/20",
        "unavailable",
        "continuity",
        "LINEAGE_UNAVAILABLE",
        "reader",
    ),
    "AC-22": (
        "quality_source_change",
        "dataset/sessions/1.read_boundary",
        "not_ready",
        "read_boundary",
        "READ_BOUNDARY_FAILED",
        "determinism",
    ),
    "AC-23": (
        "negative_count",
        "dataset/sessions/1.required_count",
        "unavailable",
        "read_boundary",
        "INVALID_ARGUMENTS",
        "reader",
    ),
    "EC-1": (
        "calendar19",
        "dataset/calendar.source_sequence",
        "unavailable",
        "continuity",
        "SESSION_COUNT_NOT_20",
        "reader",
    ),
    "EC-2": (
        "calendar_duplicate",
        "dataset/calendar.source_sequence[5]",
        "unavailable",
        "calendar",
        "SESSION_SEQUENCE_INVALID",
        "reader",
    ),
    "EC-3": (
        "calendar_future",
        "dataset/calendar.source_sequence[19]",
        "unavailable",
        "calendar",
        "PIT_VISIBILITY_INVALID",
        "reader",
    ),
    "EC-4": (
        "calendar_unknown",
        "dataset/calendar.unknown_state",
        "unavailable",
        "calendar",
        "CALENDAR_UNAVAILABLE",
        "reader",
    ),
    "EC-5": (
        "universe_unknown",
        "dataset/sessions/1.unknown_count",
        "not_ready",
        "universe",
        "UNIVERSE_UNKNOWN_NONZERO",
        "reader",
    ),
    "EC-6": (
        "naive_clock",
        "request.now",
        "unavailable",
        "read_boundary",
        "INVALID_ARGUMENTS",
        "reader",
    ),
    "EC-7": (
        "cutoff_evening",
        "dataset/sessions/1.same_evening_published_at",
        "not_ready",
        "same_evening_availability",
        "AVAILABILITY_CUTOFF_FAILED",
        "reader",
    ),
    "EC-8": (
        "version_drift",
        "dataset/sessions/10.frozen_versions_sha256",
        "not_ready",
        "provenance",
        "VERSION_DRIFT",
        "reader",
    ),
    "EC-9": (
        "lineage_missing",
        "SessionObservation.evidence.object_sha256",
        "unavailable",
        "provenance",
        "LINEAGE_UNAVAILABLE",
        "reader",
    ),
    "EC-10": (
        "replay_missing",
        "evidence/window.replay_sample",
        "unavailable",
        "replay",
        "REPLAY_UNAVAILABLE",
        "reader",
    ),
    "EC-11": (
        "corrupt_control",
        "control/replication",
        "unavailable",
        "replication",
        "REPLICATION_STATE_UNAVAILABLE",
        "reader",
    ),
    "EC-12": (
        "restore_tamper",
        "evidence/window.restore.payload_sha256",
        "unavailable",
        "restore",
        "RESTORE_UNAVAILABLE",
        "reader",
    ),
    "EC-13": (
        "session_hash_tamper",
        "dataset/sessions/1.observation_sha256",
        "unavailable",
        "read_boundary",
        "SNAPSHOT_CHANGED",
        "toctou",
    ),
    "EC-14": (
        "relative_path",
        "request.local_dataset_root",
        "unavailable",
        "read_boundary",
        "PATH_INVALID",
        "reader",
    ),
    "EC-15": (
        "missing_control",
        "control/universe",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "EC-16": (
        "corrupt_control",
        "control/replication",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "redaction",
    ),
    "EC-17": (
        "input_roots_limit",
        "request.control_store_roots",
        "unavailable",
        "read_boundary",
        "INPUT_LIMIT_EXCEEDED",
        "performance",
    ),
    "EC-18": (
        "reversed_range",
        "request.start/end",
        "unavailable",
        "read_boundary",
        "INVALID_ARGUMENTS",
        "reader",
    ),
    "EC-19": (
        "negative_count",
        "dataset/sessions/1.required_count",
        "unavailable",
        "coverage",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "EC-20": (
        "failover_baostock",
        "evidence/window.failover.selected_provider_id",
        "unavailable",
        "failover",
        "FAILOVER_UNAVAILABLE",
        "reader",
    ),
    "EC-21": (
        "failover_missing",
        "evidence/window.failover_observation",
        "unavailable",
        "failover",
        "FAILOVER_UNAVAILABLE",
        "reader",
    ),
    "EC-22": (
        "missing_threshold",
        "evidence/completed.policy_thresholds",
        "unavailable",
        "replication",
        "REMOTE_PROOF_MISSING",
        "reader",
    ),
    "EC-23": (
        "replay_unknown",
        "evidence/window.replay.offline_context.adapter_id",
        "unavailable",
        "replay",
        "REPLAY_UNAVAILABLE",
        "provider",
    ),
    "EC-24": (
        "missing_version",
        "evidence/frozen_versions.endpoint_contract_hash",
        "unavailable",
        "provenance",
        "CONTROL_STATE_UNAVAILABLE",
        "reader",
    ),
    "EC-25": (
        "calendar_out_of_order",
        "dataset/calendar.source_sequence",
        "unavailable",
        "calendar",
        "SESSION_SEQUENCE_INVALID",
        "reader",
    ),
    "EC-26": (
        "session_missing",
        "dataset/sessions/20",
        "unavailable",
        "read_boundary",
        "CONTROL_STATE_UNAVAILABLE",
        "determinism",
    ),
}


BEHAVIOR_CASES = {
    requirement: _case(requirement, *values[:-1], mode=values[-1])
    for requirement, values in _BEHAVIOR_ROWS.items()
}


def _assert_report(report: dict[str, Any], case: BehaviorCase, golden: GoldenTree) -> None:
    assert set(report) == set(_interface_fields("R2FAcceptanceReport"))
    assert report["status"] == case.report_status
    validator = StrictFixtureValidator()
    for metric in METRICS:
        validator.metric(metric, report[metric])
    result = report[case.metric]
    assert result["status"] == case.metric_status, case.expected_path
    assert result["reason_code"] == case.reason, case.expected_path
    assert report["provider_requests"] == 0
    assert report["writes"] is False
    assert report["restore_started"] is False
    assert report["production_window_started"] is False
    assert report["quality_issues"] == sorted(
        report["quality_issues"], key=lambda item: _reason_precedence().index(item)
    )
    assert len(report["quality_issues"]) <= 64
    assert all(reason in REASONS for reason in report["quality_issues"])
    # A case carries one normative trigger.  The public issue list is closed,
    # deterministic and de-duplicated; do not accept a merely matching member.
    assert report["quality_issues"] == [case.reason]
    pre_capture = report["pre_capture_failure"]
    if case.reason in {
        "INVALID_ARGUMENTS",
        "PATH_INVALID",
        "CONTROL_STATE_UNAVAILABLE",
        "INPUT_LIMIT_EXCEEDED",
    }:
        assert pre_capture is not None
        assert pre_capture["reason_code"] == case.reason
        assert pre_capture["semantic_report_sha256"] == report["semantic_report_sha256"]
    else:
        assert pre_capture is None
    if report["snapshot_identity"] is not None:
        validator.shape("SnapshotIdentity", report["snapshot_identity"])
        if case.report_status == "ready" and case.mutation is None:
            expected_snapshot = golden.snapshot_identity
            actual_snapshot = report["snapshot_identity"]
            # Public snapshot identity must bind the fixture's expected tree
            # and logical-SQLite digests; a separate physical tuple is only
            # zero-write evidence and cannot substitute for these values.
            assert actual_snapshot["input_fingerprints"] == expected_snapshot["input_fingerprints"]
            assert actual_snapshot["input_fingerprint_sha256"] == expected_snapshot[
                "input_fingerprint_sha256"
            ]
            assert actual_snapshot["frozen_version_vector_sha256"] == expected_snapshot[
                "frozen_version_vector_sha256"
            ]
            assert actual_snapshot["snapshot_sha256"] == expected_snapshot["snapshot_sha256"]
        validator.exact_digest(
            "SnapshotIdentity.input_fingerprint_sha256",
            report["snapshot_identity"],
            report["snapshot_identity"]["input_fingerprint_sha256"],
        )
        validator.exact_digest(
            "SnapshotIdentity.frozen_version_vector_sha256",
            report["snapshot_identity"],
            report["snapshot_identity"]["frozen_version_vector_sha256"],
        )
        validator.exact_digest(
            "SnapshotIdentity.snapshot_sha256",
            report["snapshot_identity"],
            report["snapshot_identity"]["snapshot_sha256"],
        )
    if report["semantic_report_sha256"] is not None:
        validator.exact_digest(
            "R2FAcceptanceReport.semantic_report_sha256",
            report,
            report["semantic_report_sha256"],
        )
    assert set(report["diagnostic_envelope"]) == {
        "elapsed_ms",
        "read_operations",
        "replay_sample_count",
    }
    if report["snapshot_identity"] is not None:
        assert report["window_evidence_refs"] == [golden.window_envelope["envelope_sha256"]]
        if report["window_evidence_bundle"] is not None:
            assert report["window_evidence_bundle"]["envelope_sha256"] == golden.window_envelope[
                "envelope_sha256"
            ]


def _reason_precedence() -> list[str]:
    return [
        "INVALID_ARGUMENTS",
        "PATH_INVALID",
        "SNAPSHOT_CHANGED",
        "CONTROL_STATE_UNAVAILABLE",
        "PIT_VISIBILITY_INVALID",
        "CALENDAR_UNAVAILABLE",
        "CALENDAR_CONFLICT",
        "SESSION_SEQUENCE_INVALID",
        "SESSION_COUNT_NOT_20",
        "VERSION_DRIFT",
        "LINEAGE_UNAVAILABLE",
        "CANONICAL_INTEGRITY_FAILED",
        "UNIVERSE_UNKNOWN_NONZERO",
        "UNIVERSE_COUNT_MISMATCH",
        "CONTINUITY_FAILED",
        "AVAILABILITY_CUTOFF_FAILED",
        "COVERAGE_FAILED",
        "SOURCE_PURITY_FAILED",
        "RECOVERY_FAILED",
        "FAILOVER_UNAVAILABLE",
        "REPLAY_UNAVAILABLE",
        "REPLAY_SEMANTIC_MISMATCH",
        "ADJUSTMENT_UNAVAILABLE",
        "ERROR_HANDLING_FAILED",
        "LOCAL_NAS_ISOLATION_FAILED",
        "REPLICATION_UNAVAILABLE",
        "REPLICATION_LAG",
        "REMOTE_PROOF_MISSING",
        "RESTORE_UNAVAILABLE",
        "READ_BOUNDARY_FAILED",
        "INPUT_LIMIT_EXCEEDED",
    ] + sorted(
        REASONS
        - {
            "INVALID_ARGUMENTS",
            "PATH_INVALID",
            "SNAPSHOT_CHANGED",
            "CONTROL_STATE_UNAVAILABLE",
            "PIT_VISIBILITY_INVALID",
            "CALENDAR_UNAVAILABLE",
            "CALENDAR_CONFLICT",
            "SESSION_SEQUENCE_INVALID",
            "SESSION_COUNT_NOT_20",
            "VERSION_DRIFT",
            "LINEAGE_UNAVAILABLE",
            "CANONICAL_INTEGRITY_FAILED",
            "UNIVERSE_UNKNOWN_NONZERO",
            "UNIVERSE_COUNT_MISMATCH",
            "CONTINUITY_FAILED",
            "AVAILABILITY_CUTOFF_FAILED",
            "COVERAGE_FAILED",
            "SOURCE_PURITY_FAILED",
            "RECOVERY_FAILED",
            "FAILOVER_UNAVAILABLE",
            "REPLAY_UNAVAILABLE",
            "REPLAY_SEMANTIC_MISMATCH",
            "ADJUSTMENT_UNAVAILABLE",
            "ERROR_HANDLING_FAILED",
            "LOCAL_NAS_ISOLATION_FAILED",
            "REPLICATION_UNAVAILABLE",
            "REPLICATION_LAG",
            "REMOTE_PROOF_MISSING",
            "RESTORE_UNAVAILABLE",
            "READ_BOUNDARY_FAILED",
            "INPUT_LIMIT_EXCEEDED",
        }
    )


def _settings(golden: GoldenTree, request: dict[str, Any] | None = None) -> Settings:
    request = request or golden.request
    return Settings(
        _env_file=None,
        local_market_dataset_root=Path(request["local_dataset_root"]),
        provider_evidence_root=Path(request["evidence_root"]),
        local_control_dir=golden.control,
        replication_database_name=_CATALOG_FILENAMES["replication_sidecar"],
        daily_bar_shadow_database_name=_CATALOG_FILENAMES["daily_shadow"],
        provider_registry_database_name=_CATALOG_FILENAMES["shadow_registry"],
        calendar_generation_database_name=_CATALOG_FILENAMES["calendar_generation"],
        universe_contract_database_name=_CATALOG_FILENAMES["universe"],
        auto_refresh_enabled=False,
        calendar_runtime_enabled=False,
    )


def _api_get(golden: GoldenTree, request: dict[str, Any] | None = None) -> httpx.Response:
    request = request or golden.request
    settings = _settings(golden, request)
    app.dependency_overrides[get_settings] = lambda: settings
    instant = datetime.fromisoformat(request["now"].replace("Z", "+00:00"))
    app.dependency_overrides[market_api.get_market_clock] = lambda: lambda: instant

    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(
                "/api/v1/market/reliability-acceptance",
                params={"start": request["start"], "end": request["end"]},
            )

    try:
        return asyncio.run(send())
    finally:
        app.dependency_overrides.clear()


def _run_cli(
    golden: GoldenTree,
    request: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> tuple[int, dict[str, Any]]:
    monkeypatch.setattr(cli_module, "get_settings", lambda: _settings(golden, request))
    instant = datetime.fromisoformat(request["now"].replace("Z", "+00:00"))
    monkeypatch.setattr(cli_module, "_calendar_runtime_now", lambda: instant)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "stock-eva",
            "r2f-acceptance",
            "--start",
            request["start"],
            "--end",
            request["end"],
            "--local-dataset-root",
            request["local_dataset_root"],
            "--evidence-root",
            request["evidence_root"],
        ],
    )
    exit_code = cli_module.main()
    captured = capsys.readouterr()
    assert captured.err == ""
    assert len(captured.out.strip().splitlines()) == 1
    return exit_code, json.loads(captured.out)


def _install_provider_sentinels(module: Any, monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    calls = {"count": 0}

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        calls["count"] += 1
        raise AssertionError("provider/network construction is forbidden")

    monkeypatch.setattr(socket, "create_connection", forbidden)

    class ForbiddenSocket:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            forbidden()

    monkeypatch.setattr(socket, "socket", ForbiddenSocket)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    monkeypatch.setattr(httpx.Client, "__init__", forbidden)
    monkeypatch.setattr(httpx.AsyncClient, "__init__", forbidden)
    if importlib.util.find_spec("requests") is not None:
        requests = importlib.import_module("requests")
        monkeypatch.setattr(requests.sessions.Session, "request", forbidden)
    baostock = importlib.import_module("baostock")
    for name, value in vars(baostock).items():
        if name == "login" or name.startswith("query") or name in {"logout", "get_trade_days"}:
            if callable(value):
                monkeypatch.setattr(baostock, name, forbidden)
    typed_provider = importlib.import_module("backend.app.market.providers.baostock")
    legacy_provider = importlib.import_module("backend.app.market.baostock")
    evidence_module = importlib.import_module("backend.app.market.evidence")
    monkeypatch.setattr(typed_provider.BaoStockProviderAdapter, "__init__", forbidden)
    monkeypatch.setattr(legacy_provider.BaoStockProvider, "__init__", forbidden)
    monkeypatch.setattr(evidence_module.EvidenceReader, "__init__", forbidden)
    # Patch the actual lookup aliases held by the target reader as well as the
    # imported modules.  A direct ``from ... import login`` must not evade the
    # network sentinel simply because the module object was patched.
    for name, value in tuple(vars(module).items()):
        lowered = name.lower()
        if lowered in {"socket", "create_connection", "urlopen", "login"} or "query" in lowered:
            if callable(value):
                monkeypatch.setattr(module, name, forbidden)
        elif isinstance(value, type) and (
            "provider" in lowered or "adapter" in lowered or "evidencereader" in lowered
        ):
            monkeypatch.setattr(value, "__init__", forbidden)
    assert module.EvidenceReader is evidence_module.EvidenceReader, (
        "the acceptance service must expose the EvidenceReader lookup used for offline replay"
    )
    return calls


def _run_behavior_case(
    case: BehaviorCase,
    golden: GoldenTree,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    request = _apply_mutation(golden, case.mutation)
    before = tuple(_physical_fingerprint(root) for root in golden.roots())
    if case.mode == "api":
        response = _api_get(golden, request)
        expected_http = (
            422
            if case.reason in {"INVALID_ARGUMENTS", "PATH_INVALID"}
            else (503 if case.report_status == "unavailable" else 200)
        )
        assert response.status_code == expected_http
        assert str(golden.dataset) not in response.text
        report = response.json()
    elif case.mode == "cli":
        exit_code, report = _run_cli(golden, request, monkeypatch, capsys)
        expected_exit = (
            2
            if case.reason in {"INVALID_ARGUMENTS", "PATH_INVALID"}
            else (1 if case.report_status == "unavailable" else 0)
        )
        assert exit_code == expected_exit
    elif case.mode == "parity":
        direct = _evaluate(request)
        response = _api_get(golden, request)
        exit_code, cli = _run_cli(golden, request, monkeypatch, capsys)
        assert response.status_code == 503 and exit_code == 1
        api = response.json()
        for value in (api, cli):
            value.pop("diagnostic_envelope", None)
        direct.pop("diagnostic_envelope", None)
        assert api == cli == direct
        report = response.json()
    elif case.mode == "provider":
        module = _red_reader()
        calls = _install_provider_sentinels(module, monkeypatch)
        report = _evaluate(request)
        assert calls["count"] == 0
    elif case.mode == "redaction":
        report = _evaluate(request)
        public = _canonical_json(report).decode()
        assert str(golden.control) not in public
        assert all(
            token not in public for token in ("SELECT ", "http://", "token=", "not a sqlite")
        )
    elif case.mode == "determinism":
        first = _evaluate(request)
        second = _evaluate(request)
        first_diagnostics = first.pop("diagnostic_envelope")
        second_diagnostics = second.pop("diagnostic_envelope")
        assert first == second
        assert set(first_diagnostics) == set(second_diagnostics)
        report = {**first, "diagnostic_envelope": first_diagnostics}
    elif case.mode == "performance":
        started = time.perf_counter()
        report = _evaluate(request)
        assert (time.perf_counter() - started) * 1000 < LIMITS["max_elapsed_ms"]
    elif case.mode in {"toctou", "wal"}:
        module = _red_reader()
        original_fstat = module.os.fstat
        touched = {"done": False}
        target = golden.dataset / "manifest.json"

        def replacing_fstat(descriptor: int) -> os.stat_result:
            result = original_fstat(descriptor)
            if not touched["done"]:
                touched["done"] = True
                replacement = target.with_suffix(".replacement")
                replacement.write_bytes(target.read_bytes() + b" ")
                os.replace(replacement, target)
            return result

        monkeypatch.setattr(module.os, "fstat", replacing_fstat)
        report = _evaluate(request)
        assert touched["done"]
    else:
        report = _evaluate(request)
    _assert_report(report, case, golden)
    after = tuple(_physical_fingerprint(root) for root in golden.roots())
    if case.mode not in {"toctou", "wal"}:
        assert after == before


def _make_requirement_test(case: BehaviorCase):
    def test(
        golden: GoldenTree,
        monkeypatch: pytest.MonkeyPatch,
        capsys: Any,
    ) -> None:
        assert REQUIREMENT_SUMMARIES[case.anchor]
        assert case.target_path and case.expected_path
        _run_behavior_case(case, golden, monkeypatch, capsys)

    test.__name__ = case.anchor
    test.__qualname__ = case.anchor
    test.__doc__ = f"{case.requirement}: {REQUIREMENT_SUMMARIES[case.anchor]}"
    return test


for _requirement_case in BEHAVIOR_CASES.values():
    globals()[_requirement_case.anchor] = _make_requirement_test(_requirement_case)


@dataclass(frozen=True)
class SloCase:
    state: str
    mutation: str | None
    report_status: str
    metric_status: str
    reason: str | None


def _approved_slo_contracts() -> dict[str, dict[str, Any]]:
    """Parse target/reason/anchor from the approved normative SLO table.

    The X8 JSON block supplies the closed metric/type and reason partitions; the
    adjacent six-column table supplies each exact threshold and failure reason.
    No expected SLO value is duplicated in this test module.
    """

    design = _design_text()
    start_heading = "## R2-F5.0 metric contract and evidence sources"
    end_heading = "## Secondary admission and failover evidence contract"
    start = design.index(start_heading) + len(start_heading)
    section = design[start : design.index(end_heading, start)]
    contracts: dict[str, dict[str, Any]] = {}
    for line in section.splitlines():
        match = re.match(
            r"^\| `([a-z_]+)`(?: \*\(child of local/NAS\)\*)? \| (.*?) \| "
            r"(.*?) \| `([A-Z0-9_]+)` \| (AC-\d+) \| `(test_r2f5_slo_[a-z_]+)` \|$",
            line,
        )
        if not match:
            continue
        metric, threshold, _reducer, failure_reason, acceptance_ref, anchor = match.groups()
        assert metric in METRICS
        assert acceptance_ref == "AC-15"
        assert failure_reason in REASONS
        # Parse the threshold prose, preserving the approved roadmap values.
        if "zero missing" in threshold:
            target: Any = 0
        elif "20/20" in threshold:
            target = 1.0
        elif "18/20" in threshold:
            target = 0.9
        elif "100%" in threshold:
            target = 1.0
        elif "zero mixed" in threshold or "publishes exactly once" in threshold:
            target = True
        elif "pass" in threshold or "satisfies" in threshold or "leaves local ready" in threshold:
            target = True
        elif "frozen lag policy" in threshold:
            # The exact numeric threshold is a reviewed R2-F4 policy value and
            # is read from the completed fixture, never guessed here.
            target = None
        else:
            # Remaining roadmap dimensions are boolean pass predicates; the
            # exact predicate is supplied by the reducer field list above.
            target = True
        contracts[metric] = {
            "target": target,
            "failure_reason": failure_reason,
            "acceptance_ref": acceptance_ref,
            "planned_test_anchor": anchor,
        }
    assert tuple(contracts) == METRICS
    return contracts


SLO_CONTRACTS = _approved_slo_contracts()
_SLO_FAILURE_MUTATIONS = {
    "continuity": "continuity_gap",
    "next_morning_availability": "cutoff_morning",
    "same_evening_availability": "cutoff_evening",
    "coverage": "coverage_fail",
    "canonical_integrity": "pointer_mismatch",
    "source_purity": "mixed_source",
    "recovery": "recovery_fail",
    "failover": "failover_identity_mismatch",
    "provenance": "lineage_tamper",
    "replay": "replay_mismatch",
    "adjustment": "adjustment_mismatch",
    "calendar": "calendar_conflict",
    "universe": "universe_unknown",
    "error_handling": "error_fail",
    "local_nas_isolation": "nas_fail",
    "replication": "replication_lag",
    "restore": "restore_fail",
    "read_boundary": "read_write",
}
_SLO_UNAVAILABLE_MUTATIONS = {
    "continuity": "calendar_unknown",
    "next_morning_availability": "calendar_unknown",
    "same_evening_availability": "calendar_unknown",
    "coverage": "session_missing",
    "canonical_integrity": "lineage_missing",
    "source_purity": "session_missing",
    "recovery": "recovery_missing",
    "failover": "failover_missing",
    "provenance": "lineage_missing",
    "replay": "replay_missing",
    "adjustment": "adjustment_missing",
    "calendar": "calendar_unknown",
    "universe": "session_missing",
    "error_handling": "error_missing",
    "local_nas_isolation": "nas_missing",
    "replication": "missing_threshold",
    "restore": "restore_missing",
    "read_boundary": "corrupt_control",
}
_UNAVAILABLE_REASON_BY_MUTATION = {
    "calendar_unknown": "CALENDAR_UNAVAILABLE",
    "session_missing": "LINEAGE_UNAVAILABLE",
    "lineage_missing": "LINEAGE_UNAVAILABLE",
    "recovery_missing": "CONTROL_STATE_UNAVAILABLE",
    "failover_missing": "FAILOVER_UNAVAILABLE",
    "replay_missing": "REPLAY_UNAVAILABLE",
    "adjustment_missing": "ADJUSTMENT_UNAVAILABLE",
    "error_missing": "CONTROL_STATE_UNAVAILABLE",
    "nas_missing": "CONTROL_STATE_UNAVAILABLE",
    "missing_threshold": "REPLICATION_UNAVAILABLE",
    "restore_missing": "RESTORE_UNAVAILABLE",
    "corrupt_control": "CONTROL_STATE_UNAVAILABLE",
}


def _unavailable_reason(metric: str, mutation: str) -> str:
    reason = _UNAVAILABLE_REASON_BY_MUTATION.get(mutation)
    assert reason in REASON_PARTITIONS["unavailable"], (metric, mutation, reason)
    return reason


def _slo_observed(metric: str, golden: GoldenTree) -> Any:
    """Derive the expected public value from formal fixture fields."""

    observations = [
        _read_json(golden.dataset / "sessions" / f"{session}.json")
        for session in golden.sessions
        if (golden.dataset / "sessions" / f"{session}.json").exists()
    ]
    if metric == "continuity":
        manifest = _read_json(golden.dataset / "manifest.json")
        return max(0, CARDINALITY["sessions"] - len(manifest["sessions"]))
    if metric in {"next_morning_availability", "same_evening_availability"}:
        field = (
            "next_morning_published_at"
            if metric == "next_morning_availability"
            else "same_evening_published_at"
        )
        suffix = "T00:00:00Z" if metric.startswith("next_") else "T13:15:00Z"
        return sum(
            1 for item in observations if item[field].endswith(suffix)
        ) / CARDINALITY["sessions"]
    if metric == "coverage":
        return min(
            item["loaded_count"] / item["required_count"] for item in observations
        )
    if metric == "canonical_integrity":
        return all(
            item["pointer_reconciliation"]["pointer_manifest_object_match"] for item in observations
        )
    if metric == "source_purity":
        return all(
            len(item["canonical_provider_ids"]) == 1 for item in observations
        )
    if metric == "replication":
        return max(
            item["replication_observation"]["lag_seconds"] for item in observations
        )
    if metric == "universe":
        return all(item["unknown_count"] == 0 for item in observations)
    if metric == "read_boundary":
        return all(item["read_boundary_raw_facts"]["write_count"] == 0 for item in observations)
    # Window-level pass/fail values are closed booleans in the approved model.
    return True


def _slo_cases(metric: str) -> tuple[SloCase, SloCase, SloCase]:
    contract = SLO_CONTRACTS[metric]
    failure_mutation = _SLO_FAILURE_MUTATIONS[metric]
    unavailable_mutation = _SLO_UNAVAILABLE_MUTATIONS[metric]
    failure_status = (
        ("unavailable", "unavailable")
        if contract["failure_reason"] in REASON_PARTITIONS["unavailable"]
        else ("not_ready", "fail")
    )
    return (
        SloCase("pass", None, "ready", "pass", None),
        SloCase("fail", failure_mutation, *failure_status, contract["failure_reason"]),
        SloCase(
            "unavailable",
            unavailable_mutation,
            "unavailable",
            "unavailable",
            _unavailable_reason(metric, unavailable_mutation),
        ),
    )


def _make_slo_test(metric: str):
    @pytest.mark.parametrize("case", _slo_cases(metric), ids=("pass", "fail", "unavailable"))
    def test(golden: GoldenTree, case: SloCase) -> None:
        request = (
            golden.request if case.mutation is None else _apply_mutation(golden, case.mutation)
        )
        before = tuple(_physical_fingerprint(root) for root in golden.roots())
        report = _evaluate(request)
        assert report["status"] == case.report_status
        result = report[metric]
        assert result["status"] == case.metric_status
        assert result["reason_code"] == case.reason
        if case.state == "pass":
            assert report["snapshot_identity"] == golden.snapshot_identity
            assert report["window_evidence_refs"] == [golden.window_envelope["envelope_sha256"]]
        if case.metric_status == "unavailable":
            assert result["observed"] is result["target"] is None
        else:
            kind = METRIC_KINDS[metric]["observed"]
            observed = _slo_observed(metric, golden)
            if case.state == "fail" and kind in {"bool"}:
                observed = False
            target = SLO_CONTRACTS[metric]["target"]
            if metric == "replication":
                target = _read_json(golden.evidence / "completed_replication_restore.json")[
                    "policy_thresholds"
                ]["replication_lag_seconds"]
            assert result["observed"] == {"kind": kind, "value": observed}
            assert result["target"] == {"kind": kind, "value": target}
        assert result["planned_test_anchor"] == f"test_r2f5_slo_{metric}"
        assert tuple(_physical_fingerprint(root) for root in golden.roots()) == before

    test.__name__ = f"test_r2f5_slo_{metric}"
    test.__qualname__ = test.__name__
    return test


for _metric_name in METRICS:
    globals()[f"test_r2f5_slo_{_metric_name}"] = _make_slo_test(_metric_name)


def test_r2f5_contract_catalog_and_anchor_inventory_are_exact() -> None:
    assert X8["contract_version"] == "r2f5-x8"
    assert len(DIGESTS) == 61
    assert len(REQUIREMENT_ANCHORS) == len(set(REQUIREMENT_ANCHORS)) == 92
    assert set(BEHAVIOR_CASES) == {row[0] for row in REQUIREMENT_ROWS}
    assert {case.anchor for case in BEHAVIOR_CASES.values()} == set(REQUIREMENT_ANCHORS)
    assert all(callable(globals()[anchor]) for anchor in REQUIREMENT_ANCHORS)
    assert len(METRICS) == len(SLO_ANCHORS) == 18
    assert all(callable(globals()[anchor]) for anchor in SLO_ANCHORS)
    assert set(CATALOGS) == set(_CATALOG_DDL) == set(_CATALOG_FILENAMES)


def test_r2f5_complete_golden_is_strictly_validated_from_x8(golden: GoldenTree) -> None:
    stats = golden.validation_stats
    assert stats.objects == 452
    assert stats.digests == 466
    assert stats.envelopes == 8
    assert stats.metrics == 200
    assert stats.sqlite_catalogs == 5
    assert stats.digest_fields == set(DIGESTS)
    assert len(golden.frozen_versions) == len(MODELS["FrozenReliabilityVersions"]["object_fields"])
    assert len(golden.session_fixtures) == 20
    assert len(golden.window_envelope["payload"]) == 8
    assert golden.completed_replication_restore["policy_thresholds"] is not None
    assert golden.qualification_projection["qualification_proof_status"] == "available"


def test_r2f5_golden_artifacts_never_contain_test_control_fields(golden: GoldenTree) -> None:
    assert set(golden.request) == INPUT_KEYS
    assert not any(
        path.name.endswith("_digest_sources.json") or path.name == "session_digest_sources.json"
        for path in golden.evidence.rglob("*")
    )
    for root in golden.roots():
        for path in root.rglob("*.json"):
            StrictFixtureValidator().walk_scalars(json.loads(path.read_text(encoding="utf-8")))


@pytest.mark.parametrize("failure", ("unknown", "missing", "enum", "cardinality", "hash"))
def test_r2f5_strict_fixture_validator_rejects_each_contract_class(
    golden: GoldenTree, failure: str
) -> None:
    validator = StrictFixtureValidator()
    if failure in {"unknown", "missing"}:
        value = deepcopy(golden.frozen_versions)
        if failure == "unknown":
            value["unexpected"] = True
        else:
            del value["adapter_hash"]
        with pytest.raises(AssertionError, match="FrozenReliabilityVersions"):
            validator.shape("FrozenReliabilityVersions", value)
    elif failure == "enum":
        value = _metric("coverage", 1.0, 1.0)
        value["status"] = "maybe"
        with pytest.raises(AssertionError):
            validator.metric("coverage", value)
    elif failure == "cardinality":
        broken = deepcopy(golden)
        broken.session_fixtures.pop()
        with pytest.raises(AssertionError):
            _validate_golden(broken)
    else:
        value = deepcopy(golden.window_envelope)
        value["payload_sha256"] = "f" * 64
        with pytest.raises(AssertionError, match="ImmutableObservationEnvelopeV1.payload_sha256"):
            validator.envelope(value, "WindowEvidenceBundlePayload")


def test_r2f5_external_known_digest_literals_are_fixed() -> None:
    payload = {"a": 1, "b": "x"}
    payload_root = {
        "artifact_id": "a",
        "artifact_ref": "r",
        "schema_version": "r2f5-observation-envelope-v1",
        "creator_kind": "task20_writer",
        "creator_version": "v1",
        "created_at": "2026-01-01T00:00:00Z",
        "payload": payload,
        "canonicalization_version": "project-canonical-json-v1",
        "payload_sha256": "00",
        "envelope_sha256": "00",
    }
    assert _digest("ImmutableObservationEnvelopeV1.payload_sha256", payload_root) == (
        "030f98b799fb67a849527d2442a4a846ead97f2af036a4b60ebdccc3a0d17a3d"
    )
    assert _digest("ImmutableObservationEnvelopeV1.envelope_sha256", payload_root) == (
        "eb5fcb4e1da1692d93d4ce2787828362f117a49f01d07ef2121d4fd65f5fad52"
    )


def test_r2f5_public_digest_bindings_cover_payload_envelope_tree_sqlite_snapshot_report(
    golden: GoldenTree,
) -> None:
    """Bind each digest family to the future reader's public report output."""

    report = _evaluate(golden.request)
    bundle = report["window_evidence_bundle"]
    assert bundle["payload_sha256"] == _digest(
        "ImmutableObservationEnvelopeV1.payload_sha256", bundle
    )
    assert bundle["envelope_sha256"] == _digest(
        "ImmutableObservationEnvelopeV1.envelope_sha256", bundle
    )
    snapshot = report["snapshot_identity"]
    assert snapshot["input_fingerprint_sha256"] == golden.snapshot_identity[
        "input_fingerprint_sha256"
    ]
    sqlite_fingerprints = [
        item for item in snapshot["input_fingerprints"] if item["descriptor_role"] == "calendar"
    ]
    expected_sqlite = next(
        item
        for item in golden.snapshot_identity["input_fingerprints"]
        if item["descriptor_role"] == "calendar"
    )
    assert sqlite_fingerprints and sqlite_fingerprints[0] == expected_sqlite
    tree_fingerprints = [
        item
        for item in snapshot["input_fingerprints"]
        if item["descriptor_role"] in {"dataset", "evidence"}
    ]
    assert tree_fingerprints
    assert snapshot["snapshot_sha256"] == _digest("SnapshotIdentity.snapshot_sha256", snapshot)
    assert report["semantic_report_sha256"] == _digest(
        "R2FAcceptanceReport.semantic_report_sha256", report
    )


def test_r2f5_public_digest_validator_matches_external_literals() -> None:
    module = _red_reader()
    validator = module.validate_canonical_digest
    payload = {"a": 1, "b": "x"}
    assert validator("ImmutableObservationEnvelopeV1.payload_sha256", payload) == (
        "030f98b799fb67a849527d2442a4a846ead97f2af036a4b60ebdccc3a0d17a3d"
    )


def test_r2f5_complete_golden_public_reader_is_ready(golden: GoldenTree) -> None:
    before = tuple(_physical_fingerprint(root) for root in golden.roots())
    report = _evaluate(golden.request)
    assert report["status"] == "ready"
    assert report["selected_sessions"] == golden.sessions
    assert len(report["session_observations"]) == len(report["observation_refs"]) == 20
    assert report["frozen_versions"] == golden.frozen_versions
    assert report["snapshot_identity"] == golden.snapshot_identity
    assert report["window_evidence_refs"] == [golden.window_envelope["envelope_sha256"]]
    assert report["window_evidence_bundle"]["envelope_sha256"] == golden.window_envelope[
        "envelope_sha256"
    ]
    assert report["window_evidence_bundle"] is not None
    assert len(report["window_evidence_refs"]) == 1
    assert all(report[metric]["status"] == "pass" for metric in METRICS)
    assert report["provider_requests"] == 0 and report["writes"] is False
    assert report["restore_started"] is report["production_window_started"] is False
    assert tuple(_physical_fingerprint(root) for root in golden.roots()) == before


def test_r2f5_service_api_cli_share_golden_settings_clock_and_semantics(
    golden: GoldenTree,
    monkeypatch: pytest.MonkeyPatch,
    capsys: Any,
) -> None:
    direct = _evaluate(golden.request)
    response = _api_get(golden, golden.request)
    exit_code, cli = _run_cli(golden, golden.request, monkeypatch, capsys)
    assert response.status_code == 200 and exit_code == 0
    api = response.json()
    for value in (direct, api, cli):
        value.pop("diagnostic_envelope")
    assert direct == api == cli


def test_r2f5_cli_rejects_execute_before_any_io(golden: GoldenTree) -> None:
    before = tuple(_physical_fingerprint(root) for root in golden.roots())
    with pytest.raises(cli_module._CliArgumentError):
        cli_module.build_parser().parse_args(
            [
                "r2f-acceptance",
                "--start",
                golden.sessions[0],
                "--end",
                golden.sessions[-1],
                "--execute",
            ]
        )
    assert tuple(_physical_fingerprint(root) for root in golden.roots()) == before


def test_r2f5_red_reader_masks_only_exact_missing_target(monkeypatch: pytest.MonkeyPatch) -> None:
    missing = ModuleNotFoundError("missing dependency")
    missing.name = "some.transitive.dependency"

    def fail_import(_name: str) -> Any:
        raise missing

    monkeypatch.setattr(importlib, "import_module", fail_import)
    with pytest.raises(ModuleNotFoundError) as error:
        _red_reader()
    assert error.value is missing


def test_r2f5_golden_and_replay_patch_every_provider_network_lookup(
    golden: GoldenTree, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _red_reader()
    calls = _install_provider_sentinels(module, monkeypatch)
    first = _evaluate(golden.request)
    replay_request = _apply_mutation(golden, "replay_mismatch")
    second = _evaluate(replay_request)
    assert first["status"] == "ready"
    assert second["replay"]["reason_code"] == "REPLAY_SEMANTIC_MISMATCH"
    assert calls["count"] == 0


@pytest.mark.parametrize(
    ("mutation", "http_status", "reason"),
    (
        ("relative_path", 422, "PATH_INVALID"),
        ("missing_control", 503, "CONTROL_STATE_UNAVAILABLE"),
        ("corrupt_control", 503, "CONTROL_STATE_UNAVAILABLE"),
        ("catalog_extra", 503, "CONTROL_STATE_UNAVAILABLE"),
    ),
)
def test_r2f5_api_unsafe_missing_corrupt_and_catalog_inputs_are_exact_and_write_free(
    golden: GoldenTree, mutation: str, http_status: int, reason: str
) -> None:
    request = _apply_mutation(golden, mutation)
    before = tuple(_physical_fingerprint(root) for root in golden.roots())
    response = _api_get(golden, request)
    assert response.status_code == http_status
    report = response.json()
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] == reason
    assert report["quality_issues"] == [reason]
    assert report["pre_capture_failure"] is not None
    assert report["pre_capture_failure"]["reason_code"] == reason
    assert report["provider_requests"] == 0 and report["writes"] is False
    assert str(golden.control) not in response.text
    assert tuple(_physical_fingerprint(root) for root in golden.roots()) == before


def test_r2f5_locked_sqlite_is_exact_unavailable_and_unchanged(golden: GoldenTree) -> None:
    database = golden.controls["calendar_generation"]
    connection = sqlite3.connect(database, timeout=0.01)
    connection.execute("BEGIN EXCLUSIVE")
    before = _physical_fingerprint(golden.control)
    try:
        report = _evaluate(golden.request)
    finally:
        connection.rollback()
        connection.close()
    assert report["status"] == "unavailable"
    assert "CONTROL_STATE_UNAVAILABLE" in report["quality_issues"]
    assert _physical_fingerprint(golden.control) == before


def test_r2f5_toctou_replacement_uses_system_call_barrier_and_exact_reason(
    golden: GoldenTree, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _red_reader()
    original = module.os.fstat
    target = golden.dataset / "manifest.json"
    barrier = threading.Barrier(2)
    replaced = threading.Event()

    def writer() -> None:
        barrier.wait(timeout=2)
        replacement = target.with_suffix(".replacement")
        replacement.write_bytes(target.read_bytes() + b" ")
        os.replace(replacement, target)
        replaced.set()

    thread = threading.Thread(target=writer)
    thread.start()
    fired = False

    def fstat(descriptor: int) -> os.stat_result:
        nonlocal fired
        result = original(descriptor)
        if not fired:
            fired = True
            barrier.wait(timeout=2)
            assert replaced.wait(timeout=2)
        return result

    monkeypatch.setattr(module.os, "fstat", fstat)
    try:
        report = _evaluate(golden.request)
    finally:
        thread.join(timeout=2)
    assert report["status"] == "unavailable"
    assert report["read_boundary"]["reason_code"] == "SNAPSHOT_CHANGED"


@pytest.mark.parametrize("commit_timing", ("before_transaction", "after_snapshot"))
def test_r2f5_wal_barrier_reads_one_logical_snapshot(
    golden: GoldenTree,
    monkeypatch: pytest.MonkeyPatch,
    commit_timing: str,
) -> None:
    module = _red_reader()
    database = golden.controls["calendar_generation"]
    old_hash, new_hash = "1" * 64, "2" * 64
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("INSERT INTO calendar_official_object VALUES (?, ?)", (old_hash, b"old"))
    old_fingerprint = _fingerprint("calendar", database)[0]
    real_connect = sqlite3.connect
    start_writer = threading.Event()
    writer_done = threading.Event()
    observed_values: list[bytes] = []

    def writer() -> None:
        assert start_writer.wait(timeout=2)
        with real_connect(database) as connection:
            connection.execute(
                "DELETE FROM calendar_official_object WHERE body_sha256=?", (old_hash,)
            )
            connection.execute(
                "INSERT INTO calendar_official_object VALUES (?, ?)", (new_hash, b"new")
            )
        writer_done.set()

    thread = threading.Thread(target=writer)
    thread.start()

    class BarrierConnection(sqlite3.Connection):
        transaction_started = False
        snapshot_established = False

        def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:
            normalized = " ".join(sql.upper().split())
            if normalized == "BEGIN" and commit_timing == "before_transaction":
                start_writer.set()
                assert writer_done.wait(timeout=2)
            cursor = super().execute(sql, parameters)
            if normalized == "BEGIN":
                self.transaction_started = True
            elif (
                self.transaction_started
                and not self.snapshot_established
                and (normalized.startswith("SELECT") or normalized.startswith("PRAGMA"))
            ):
                self.snapshot_established = True
                current = (
                    super()
                    .execute("SELECT body_bytes FROM calendar_official_object ORDER BY body_sha256")
                    .fetchall()
                )
                observed_values.extend(row[0] for row in current)
                if commit_timing == "after_snapshot":
                    start_writer.set()
                    assert writer_done.wait(timeout=2)
            return cursor

    def wrapped_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        raw_path = str(args[0]) if args else str(kwargs.get("database", ""))
        if database.name in raw_path:
            kwargs["factory"] = BarrierConnection
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(module.sqlite3, "connect", wrapped_connect)
    try:
        report = _evaluate(golden.request)
    finally:
        if not start_writer.is_set():
            start_writer.set()
        thread.join(timeout=2)
    expected_value = b"new" if commit_timing == "before_transaction" else b"old"
    assert expected_value in observed_values
    # The barrier wrapper only controls when the writer commits.  The expected
    # old/new logical identities are bound to the public snapshot fingerprint;
    # raw DB/WAL/SHM bytes are never used as evidence.
    new_fingerprint = _fingerprint("calendar", database)[0]
    expected_fingerprint = (
        new_fingerprint if commit_timing == "before_transaction" else old_fingerprint
    )
    if report["snapshot_identity"] is not None:
        actual = next(
            item
            for item in report["snapshot_identity"]["input_fingerprints"]
            if item["descriptor_role"] == "calendar"
        )
        assert actual == expected_fingerprint
    if commit_timing == "before_transaction":
        assert report["read_boundary"]["status"] == "pass"
    else:
        assert report["status"] == "ready" or (
            report["status"] == "unavailable"
            and report["read_boundary"]["reason_code"] == "SNAPSHOT_CHANGED"
        )


def test_r2f5_100k_allowed_sqlite_rows_are_fully_fingerprinted_with_p95_under_limit(
    golden: GoldenTree,
) -> None:
    database = golden.controls["calendar_generation"]
    with sqlite3.connect(database) as connection:
        connection.executemany(
            "INSERT INTO calendar_official_object VALUES (?, ?)",
            ((f"{index:064x}", f"row-{index}".encode()) for index in range(100_000)),
        )
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM calendar_official_object").fetchone()[0]
            == 100_000
        )
    samples: list[float] = []
    reports: list[dict[str, Any]] = []
    for _ in range(6):
        before = _physical_fingerprint(golden.control)
        started = time.perf_counter()
        reports.append(_evaluate(golden.request))
        samples.append((time.perf_counter() - started) * 1000)
        assert _physical_fingerprint(golden.control) == before
    assert sorted(samples)[math.ceil(0.95 * len(samples)) - 1] < LIMITS["max_elapsed_ms"]
    fingerprints = [
        item["sha256"]
        for item in reports[-1]["snapshot_identity"]["input_fingerprints"]
        if item["descriptor_role"] == "calendar"
    ]
    assert len(fingerprints) == 1
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE calendar_official_object SET body_bytes=? WHERE body_sha256=?",
            (b"last-row-mutated", f"{99_999:064x}"),
        )
    changed = _evaluate(golden.request)
    changed_fingerprints = [
        item["sha256"]
        for item in changed["snapshot_identity"]["input_fingerprints"]
        if item["descriptor_role"] == "calendar"
    ]
    assert changed_fingerprints != fingerprints, "the 100000th logical row was not fingerprinted"
