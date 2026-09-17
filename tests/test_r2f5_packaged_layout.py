"""Installed-runtime asset layout contracts for the R2-F5 acceptance reader."""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
SOURCE = ROOT / "backend/app/market/reliability_acceptance.py"
DESIGN_RELATIVE = Path(
    "docs/plans/2026-09-14-stock-eva-r2f5-0-read-only-acceptance-harness-design.md"
)


def _load_installed_reader(root: Path, design: bool):
    module_root = root / "backend/app/market"
    module_root.mkdir(parents=True)
    target = module_root / SOURCE.name
    shutil.copy2(SOURCE, target)
    if design:
        packaged_design = root / "public" / DESIGN_RELATIVE
        packaged_design.parent.mkdir(parents=True)
        shutil.copy2(ROOT / DESIGN_RELATIVE, packaged_design)
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
        assert result.quality_issues == ("PATH_INVALID",)
        assert result.pre_capture_failure is not None
    finally:
        sys.modules.pop(name, None)
