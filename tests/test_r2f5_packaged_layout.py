"""Installed-runtime asset layout contracts for the R2-F5 acceptance reader."""

from __future__ import annotations

import importlib.util
import json
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
SOURCE = ROOT / "backend/app/market/reliability_acceptance.py"
DESIGN_RELATIVE = Path(
    "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md"
)


def _load_installed_reader(root: Path, design: bool, mutate_contract=None):
    module_root = root / "backend/app/market"
    module_root.mkdir(parents=True)
    target = module_root / SOURCE.name
    shutil.copy2(SOURCE, target)
    if design:
        packaged_design = root / "public" / DESIGN_RELATIVE
        packaged_design.parent.mkdir(parents=True)
        source_text = (ROOT / DESIGN_RELATIVE).read_text(encoding="utf-8")
        if mutate_contract is not None:
            match = re.search(
                r"(<!-- R2F5_X8_CONTRACTS_JSON -->\s*```json\s*)(\{.*?\})(\s*```)",
                source_text,
                re.S,
            )
            assert match is not None
            contract = json.loads(match.group(2))
            mutate_contract(contract)
            source_text = (
                source_text[: match.start(2)]
                + json.dumps(contract, ensure_ascii=False, indent=2)
                + source_text[match.end(2) :]
            )
        packaged_design.write_text(source_text, encoding="utf-8")
    name = f"r2f5_packaged_{root.name}"
    spec = importlib.util.spec_from_file_location(name, target)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(name, None)
        raise
    return module, name


def test_packaged_layout_loads_the_approved_x8_contracts(tmp_path: Path) -> None:
    module, name = _load_installed_reader(tmp_path / "release", design=True)
    try:
        assert module.DESIGN == tmp_path / "release" / "public" / DESIGN_RELATIVE
        assert len(module._DIGESTS) >= 60
        assert {
            "replication_sidecar",
            "daily_shadow",
            "shadow_registry",
            "calendar_generation",
            "universe",
        } <= set(module._CATALOGS)
        result = module.AcceptanceReader().evaluate(
            {
                "start": "2026-01-01",
                "end": "2026-01-02",
                "local_dataset_root": "relative-dataset",
                "evidence_root": "/tmp/evidence",
                "control_store_roots": (),
                "now": "2026-01-03T00:00:00Z",
            }
        )
        assert result.status == "unavailable"
        assert result.quality_issues == ("PATH_INVALID",)
        assert result.pre_capture_failure is not None
        assert result.pre_capture_failure.reason_code == "PATH_INVALID"
    finally:
        sys.modules.pop(name, None)


def test_missing_contract_asset_is_typed_unavailable_not_runtime_500(tmp_path: Path) -> None:
    module, name = _load_installed_reader(tmp_path / "release-without-public-docs", design=False)
    try:
        result = module.AcceptanceReader().evaluate(
            {
                "start": "2026-01-01",
                "end": "2026-01-02",
                "local_dataset_root": "relative-dataset",
                "evidence_root": "/tmp/evidence",
                "control_store_roots": (),
                "now": "2026-01-03T00:00:00Z",
            }
        )
        assert result.status == "unavailable"
        assert result.quality_issues == ("CONTROL_STATE_UNAVAILABLE",)
        assert result.pre_capture_failure is not None
        assert result.pre_capture_failure.reason_code == "CONTROL_STATE_UNAVAILABLE"
    finally:
        sys.modules.pop(name, None)


def _assert_malformed_contract_is_typed_unavailable(tmp_path: Path, mutate_contract) -> None:
    module, name = _load_installed_reader(
        tmp_path / "release-with-invalid-contract", design=True, mutate_contract=mutate_contract
    )
    try:
        assert module._CONTRACT_READY is False
        result = module.AcceptanceReader().evaluate(
            {
                "start": "2026-01-01",
                "end": "2026-01-02",
                "local_dataset_root": "relative-dataset",
                "evidence_root": "/tmp/evidence",
                "control_store_roots": (),
                "now": "2026-01-03T00:00:00Z",
            }
        )
        assert result.status == "unavailable"
        assert result.quality_issues == ("CONTROL_STATE_UNAVAILABLE",)
        assert result.pre_capture_failure is not None
        assert result.pre_capture_failure.reason_code == "CONTROL_STATE_UNAVAILABLE"
        assert result.provider_requests == 0
        assert result.writes is False
    finally:
        sys.modules.pop(name, None)


def test_incomplete_digest_contract_is_typed_unavailable_before_path_validation(
    tmp_path: Path,
) -> None:
    def mutate(contract: dict) -> None:
        del contract["digest_contracts"][0]["included_field_paths"]

    _assert_malformed_contract_is_typed_unavailable(tmp_path, mutate)


def test_empty_catalog_contract_is_typed_unavailable_before_path_validation(tmp_path: Path) -> None:
    _assert_malformed_contract_is_typed_unavailable(
        tmp_path, lambda contract: contract.update(sqlite_catalogs={})
    )


def test_missing_reason_partition_is_typed_unavailable_before_path_validation(
    tmp_path: Path,
) -> None:
    def mutate(contract: dict) -> None:
        del contract["reason_partitions"]["failure"]

    _assert_malformed_contract_is_typed_unavailable(tmp_path, mutate)


def test_incomplete_unavailable_reasons_are_typed_unavailable_before_path_validation(
    tmp_path: Path,
) -> None:
    def mutate(contract: dict) -> None:
        contract["reason_partitions"]["unavailable"] = ["PATH_INVALID"]

    _assert_malformed_contract_is_typed_unavailable(tmp_path, mutate)


def test_duplicate_digest_field_is_typed_unavailable_before_path_validation(tmp_path: Path) -> None:
    def mutate(contract: dict) -> None:
        contract["digest_contracts"][1]["field"] = contract["digest_contracts"][0]["field"]

    _assert_malformed_contract_is_typed_unavailable(tmp_path, mutate)


def test_wrong_digest_container_type_is_import_safe_and_typed_unavailable(tmp_path: Path) -> None:
    _assert_malformed_contract_is_typed_unavailable(
        tmp_path, lambda contract: contract.update(digest_contracts=None)
    )


def test_wrong_metric_and_catalog_types_are_typed_unavailable(tmp_path: Path) -> None:
    def mutate(contract: dict) -> None:
        contract["metric_fields"] = {"coverage": "ratio"}
        contract["sqlite_catalogs"]["universe"]["tables"] = []

    _assert_malformed_contract_is_typed_unavailable(tmp_path, mutate)
