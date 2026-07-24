import json
import os
from datetime import datetime
from pathlib import Path

from backend.app.config import Settings
from backend.app.storage.preflight import MountInspector, SystemMountInspector

_SENTINEL_NAME = ".stock-eva-dataset.json"
_MANIFEST_NAME = "manifest.json"
_REQUIRED_DIRECTORIES = (
    "bars/source=baostock",
    "manifests/history",
    "checksums",
    "_staging",
    "quarantine",
)


class DatasetInitializationError(RuntimeError):
    pass


class EmptyDatasetInitializer:
    """Create only a new, empty NAS market dataset after explicit confirmation."""

    def __init__(
        self,
        settings: Settings,
        *,
        mount_inspector: MountInspector | None = None,
        now: datetime | None = None,
    ) -> None:
        self.settings = settings
        self.mount_inspector = mount_inspector or SystemMountInspector()
        self.now = now or datetime.now().astimezone()

    def initialize(self) -> Path:
        root = self.settings.nas_market_dataset_root
        if root is None:
            raise DatasetInitializationError("nas_dataset_root_not_configured")
        if not root.is_absolute():
            raise DatasetInitializationError("dataset_root_not_absolute")
        if root.exists():
            raise DatasetInitializationError("dataset_root_already_exists")
        if not root.parent.exists() or not root.parent.is_dir():
            raise DatasetInitializationError("dataset_parent_missing")

        mount = self.mount_inspector.find_mount(root.parent)
        if mount is None:
            raise DatasetInitializationError("mount_not_detected")
        if mount.filesystem_type.lower() not in {"smbfs", "cifs"}:
            raise DatasetInitializationError("unexpected_mount_type")
        if root.parent != mount.mount_point and mount.mount_point not in root.parents:
            raise DatasetInitializationError("dataset_root_outside_mount")
        if root == mount.mount_point:
            raise DatasetInitializationError("dataset_root_cannot_be_share_root")

        # mkdir is deliberately exclusive: an unexpected concurrent creator is never
        # overwritten or treated as this application's dataset.
        try:
            root.mkdir(mode=0o755)
        except FileExistsError as error:
            raise DatasetInitializationError("dataset_root_already_exists") from error

        for relative_path in _REQUIRED_DIRECTORIES:
            (root / relative_path).mkdir(parents=True, exist_ok=False)

        generation = self.now.strftime("generation-%Y%m%d-%H%M%S")
        self._write_json_exclusive(
            root / _SENTINEL_NAME,
            {"dataset": "stock-eva-market", "schema_version": 1},
        )
        self._write_json_exclusive(
            root / _MANIFEST_NAME,
            {
                "dataset": "stock-eva-market",
                "schema_version": 1,
                "generation": generation,
                "files": [],
            },
        )
        return root

    @staticmethod
    def _write_json_exclusive(path: Path, payload: dict[str, object]) -> None:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
