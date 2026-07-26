import json
from pathlib import Path

import httpx
import pytest

from backend.app import cli
from backend.app.config import Settings, get_settings
from backend.app.main import app
from backend.app.storage.initialize import (
    DatasetInitializationError,
    EmptyDatasetInitializer,
)
from backend.app.storage.layout import StorageLayout
from backend.app.storage.preflight import (
    MountInfo,
    StoragePreflight,
    SystemMountInspector,
)


class FakeMountInspector:
    def __init__(self, mount: MountInfo | None) -> None:
        self.mount = mount
        self.calls = 0

    def find_mount(self, path: Path) -> MountInfo | None:
        self.calls += 1
        return self.mount


def settings_for(tmp_path: Path, *, nas_root: Path | None = None) -> Settings:
    return Settings(
        _env_file=None,
        market_data_dir=tmp_path / "market",
        user_data_dir=tmp_path / "user",
        local_control_dir=tmp_path / "control",
        local_staging_dir=tmp_path / "staging",
        local_lock_dir=tmp_path / "locks",
        local_temp_dir=tmp_path / "tmp",
        nas_market_dataset_root=nas_root,
    )


def write_dataset_markers(root: Path) -> None:
    root.mkdir()
    (root / ".stock-eva-dataset.json").write_text(
        json.dumps({"dataset": "stock-eva-market", "schema_version": 1}),
        encoding="utf-8",
    )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "dataset": "stock-eva-market",
                "schema_version": 1,
                "generation": "generation-20260724",
                "files": [],
            }
        ),
        encoding="utf-8",
    )


def test_default_local_mode_preserves_existing_development_paths(tmp_path: Path) -> None:
    settings = settings_for(tmp_path)
    inspector = FakeMountInspector(None)

    result = StoragePreflight(settings, mount_inspector=inspector).inspect()

    assert result.mode == "local"
    assert result.status == "ready"
    assert result.market_data_available is True
    assert result.serving_source == "local"
    assert result.local_mutable_storage is True
    assert inspector.calls == 0


def test_mount_parser_supports_macos_smbfs_and_linux_cifs() -> None:
    macos = SystemMountInspector._parse_mount(
        "//user@nas/market on /Volumes/StockEva (smbfs, nodev, nosuid)"
    )
    linux = SystemMountInspector._parse_mount(
        "//nas/market on /mnt/stock-eva type cifs (rw,vers=3.1.1)"
    )

    assert macos == MountInfo(
        mount_point=Path("/Volumes/StockEva"),
        filesystem_type="smbfs",
        options=("nodev", "nosuid"),
    )
    assert linux == MountInfo(
        mount_point=Path("/mnt/stock-eva"),
        filesystem_type="cifs",
        options=("rw", "vers=3.1.1"),
    )


def test_missing_nas_fails_closed_without_creating_the_configured_path(
    tmp_path: Path,
) -> None:
    nas_root = tmp_path / "not-mounted" / "market-dataset"
    settings = settings_for(tmp_path, nas_root=nas_root)

    result = StoragePreflight(
        settings,
        mount_inspector=FakeMountInspector(None),
    ).inspect()

    assert result.mode == "nas"
    assert result.status == "unavailable"
    assert result.reason_code == "dataset_root_missing"
    assert result.market_data_available is False
    assert nas_root.exists() is False


def test_local_runtime_layout_never_creates_the_configured_nas_root(
    tmp_path: Path,
) -> None:
    nas_root = tmp_path / "not-mounted" / "market-dataset"
    settings = settings_for(tmp_path, nas_root=nas_root)
    layout = StorageLayout(settings)

    layout.ensure_local_runtime_dirs()

    assert layout.market_refresh_lock.parent == tmp_path / "locks"
    assert layout.duckdb_temporary == tmp_path / "tmp" / "duckdb"
    assert layout.local_paths.market_database.parent == tmp_path / "market"
    assert layout.local_paths.user_database.parent == tmp_path / "user"
    assert layout.local_paths.factor_cache_database == (
        tmp_path / "control" / "baostock_factor_cache.sqlite3"
    )
    assert all(
        path.is_dir()
        for path in (
            tmp_path / "control",
            tmp_path / "staging",
            tmp_path / "locks",
            tmp_path / "tmp",
            tmp_path / "tmp" / "duckdb",
        )
    )
    assert nas_root.exists() is False


def test_nas_preflight_validates_smb_mount_sentinel_and_manifest(
    tmp_path: Path,
) -> None:
    nas_root = tmp_path / "mounted-share"
    write_dataset_markers(nas_root)
    settings = settings_for(tmp_path, nas_root=nas_root)
    inspector = FakeMountInspector(
        MountInfo(
            mount_point=nas_root,
            filesystem_type="smbfs",
            options=("nodev", "nosuid"),
        )
    )

    result = StoragePreflight(settings, mount_inspector=inspector).inspect()

    assert result.status == "ready"
    assert result.mount_type == "smbfs"
    assert result.sentinel_status == "ready"
    assert result.manifest_status == "ready"
    assert result.dataset_generation == "generation-20260724"
    assert result.market_data_available is True
    assert result.serving_source == "nas"
    assert result.reason_code is None


@pytest.mark.parametrize("filesystem_type", ["apfs", "nfs", "ext4"])
def test_nas_preflight_rejects_non_smb_mounts(
    tmp_path: Path,
    filesystem_type: str,
) -> None:
    nas_root = tmp_path / "mounted-share"
    write_dataset_markers(nas_root)
    settings = settings_for(tmp_path, nas_root=nas_root)

    result = StoragePreflight(
        settings,
        mount_inspector=FakeMountInspector(
            MountInfo(
                mount_point=nas_root,
                filesystem_type=filesystem_type,
                options=(),
            )
        ),
    ).inspect()

    assert result.status == "unavailable"
    assert result.reason_code == "unexpected_mount_type"
    assert result.market_data_available is False


def test_nas_preflight_rejects_invalid_manifest_without_listing_dataset(
    tmp_path: Path,
) -> None:
    nas_root = tmp_path / "mounted-share"
    write_dataset_markers(nas_root)
    (nas_root / "manifest.json").write_text("{not-json", encoding="utf-8")
    settings = settings_for(tmp_path, nas_root=nas_root)

    result = StoragePreflight(
        settings,
        mount_inspector=FakeMountInspector(
            MountInfo(
                mount_point=nas_root,
                filesystem_type="smbfs",
                options=(),
            )
        ),
    ).inspect()

    assert result.status == "unavailable"
    assert result.manifest_status == "invalid"
    assert result.reason_code == "manifest_invalid"


def test_cli_fails_closed_before_creating_local_market_fallback(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = settings_for(
        tmp_path,
        nas_root=tmp_path / "missing-nas" / "market-dataset",
    )
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr(
        "sys.argv",
        [
            "stock-eva",
            "refresh",
            "--date",
            "2026-07-24",
            "--symbols",
            "sh.600000",
            "--dry-run",
        ],
    )

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["quality_issues"] == ["market_storage_unavailable"]
    assert payload["writes_market_data"] is False
    assert settings.market_data_dir.exists() is False
    assert settings.nas_market_dataset_root.exists() is False


def test_empty_nas_dataset_initializer_creates_only_a_new_child_dataset(
    tmp_path: Path,
) -> None:
    share_root = tmp_path / "Stock"
    share_root.mkdir()
    nas_root = share_root / "stock-eva-market"
    settings = settings_for(tmp_path, nas_root=nas_root)
    inspector = FakeMountInspector(MountInfo(share_root, "smbfs", ("nodev", "nosuid")))

    result = EmptyDatasetInitializer(
        settings,
        mount_inspector=inspector,
    ).initialize()

    assert result == nas_root
    assert json.loads((nas_root / ".stock-eva-dataset.json").read_text()) == {
        "dataset": "stock-eva-market",
        "schema_version": 1,
    }
    manifest = json.loads((nas_root / "manifest.json").read_text())
    assert manifest["files"] == []
    assert manifest["generation"].startswith("generation-")
    assert all(
        (nas_root / path).is_dir()
        for path in (
            "bars/source=baostock",
            "manifests/history",
            "checksums",
            "_staging",
            "quarantine",
        )
    )


def test_empty_nas_dataset_initializer_refuses_share_root_or_existing_target(
    tmp_path: Path,
) -> None:
    share_root = tmp_path / "Stock"
    share_root.mkdir()
    inspector = FakeMountInspector(MountInfo(share_root, "smbfs", ()))
    root_settings = settings_for(tmp_path, nas_root=share_root)

    with pytest.raises(DatasetInitializationError, match="dataset_root_already_exists"):
        EmptyDatasetInitializer(
            root_settings,
            mount_inspector=inspector,
        ).initialize()

    existing_target = share_root / "stock-eva-market"
    existing_target.mkdir()
    child_settings = settings_for(tmp_path, nas_root=existing_target)
    with pytest.raises(DatasetInitializationError, match="dataset_root_already_exists"):
        EmptyDatasetInitializer(
            child_settings,
            mount_inspector=inspector,
        ).initialize()


def test_storage_init_cli_requires_explicit_acknowledgement(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    settings = settings_for(tmp_path, nas_root=tmp_path / "nas" / "market")
    monkeypatch.setattr(cli, "get_settings", lambda: settings)
    monkeypatch.setattr("sys.argv", ["stock-eva", "storage-init"])

    assert cli.main() == 1
    assert json.loads(capsys.readouterr().out)["reason_code"] == (
        "explicit_initialization_required"
    )


@pytest.mark.anyio
async def test_unavailable_nas_keeps_private_api_but_blocks_market_consumers(
    tmp_path: Path,
) -> None:
    settings = settings_for(
        tmp_path,
        nas_root=tmp_path / "missing-nas" / "market-dataset",
    )
    app.dependency_overrides[get_settings] = lambda: settings
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            health = await client.get("/api/v1/health")
            positions = await client.get("/api/v1/portfolio/positions")
            strategies = await client.get("/api/v1/strategies")
            alert_events = await client.get("/api/v1/alerts/events")
            readiness = await client.get("/api/v1/storage/readiness")
            summary = await client.get("/api/v1/market/summary")
            history = await client.get(
                "/api/v1/market/history/sh.600000",
                params={"start": "2026-01-01", "end": "2026-07-24"},
            )
            valuation = await client.get("/api/v1/portfolio/valuation")
    finally:
        app.dependency_overrides.clear()

    assert health.status_code == 200
    assert positions.status_code == 200
    assert positions.json() == []
    assert strategies.status_code == 200
    assert strategies.json() == []
    assert alert_events.status_code == 200
    assert alert_events.json()["status"] == "empty"
    assert readiness.status_code == 200
    assert readiness.json()["reason_code"] == "dataset_root_missing"
    for response in (summary, history, valuation):
        assert response.status_code == 503
        assert response.json()["detail"] == {
            "code": "market_storage_unavailable",
            "storage_status": "unavailable",
            "reason_code": "dataset_root_missing",
        }
