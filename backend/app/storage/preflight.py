import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from pydantic import ValidationError

from backend.app.config import Settings
from backend.app.storage.models import (
    DatasetManifest,
    DatasetSentinel,
    StorageReadiness,
)

_MAX_METADATA_BYTES = 1024 * 1024
_SMB_FILESYSTEMS = {"smbfs", "cifs"}
_SENTINEL_NAME = ".stock-eva-dataset.json"
_MANIFEST_NAME = "manifest.json"


@dataclass(frozen=True)
class MountInfo:
    mount_point: Path
    filesystem_type: str
    options: tuple[str, ...]


class MountInspector(Protocol):
    def find_mount(self, path: Path) -> MountInfo | None: ...


class SystemMountInspector:
    def find_mount(self, path: Path) -> MountInfo | None:
        try:
            result = subprocess.run(
                ["mount"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
        except (OSError, subprocess.SubprocessError):
            return None
        candidates = [
            item
            for line in result.stdout.splitlines()
            if (item := self._parse_mount(line)) is not None
            and _contains(item.mount_point, path)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda item: len(item.mount_point.parts))

    @staticmethod
    def _parse_mount(line: str) -> MountInfo | None:
        linux = re.match(r"^.+ on (.+) type ([^ ]+) \((.*)\)$", line)
        if linux:
            return MountInfo(
                mount_point=Path(linux.group(1)),
                filesystem_type=linux.group(2).lower(),
                options=tuple(filter(None, linux.group(3).split(","))),
            )
        macos = re.match(r"^.+ on (.+) \(([^,)]+)(?:,(.*))?\)$", line)
        if macos:
            return MountInfo(
                mount_point=Path(macos.group(1)),
                filesystem_type=macos.group(2).lower(),
                options=tuple(
                    filter(None, (macos.group(3) or "").replace(" ", "").split(","))
                ),
            )
        return None


def _contains(parent: Path, child: Path) -> bool:
    try:
        return os.path.commonpath((parent.resolve(), child.resolve())) == str(
            parent.resolve()
        )
    except (OSError, ValueError):
        return False


class StoragePreflight:
    def __init__(
        self,
        settings: Settings,
        *,
        mount_inspector: MountInspector | None = None,
    ) -> None:
        self.settings = settings
        self.mount_inspector = mount_inspector or SystemMountInspector()

    def inspect(self) -> StorageReadiness:
        root = self.settings.nas_market_dataset_root
        if root is None:
            return StorageReadiness(
                mode="local",
                status="ready",
                market_data_available=True,
                serving_source="local",
                sentinel_status="not_required",
                manifest_status="not_required",
            )
        if not root.is_absolute():
            return self._unavailable("misconfigured", "dataset_root_not_absolute")
        if self._overlaps_local_storage(root):
            return self._unavailable("misconfigured", "dataset_root_overlaps_local")
        if not root.exists():
            return self._unavailable("unavailable", "dataset_root_missing")
        if not root.is_dir():
            return self._unavailable("misconfigured", "dataset_root_not_directory")

        mount = self.mount_inspector.find_mount(root)
        if mount is None:
            return self._unavailable("unavailable", "mount_not_detected")
        if mount.filesystem_type.lower() not in _SMB_FILESYSTEMS:
            return self._unavailable(
                "unavailable",
                "unexpected_mount_type",
                mount_type=mount.filesystem_type,
            )

        sentinel_status, _ = self._read_metadata(
            root / _SENTINEL_NAME,
            DatasetSentinel,
        )
        if sentinel_status != "ready":
            return self._unavailable(
                "unavailable",
                f"sentinel_{sentinel_status}",
                mount_type=mount.filesystem_type,
                sentinel_status=sentinel_status,
            )
        manifest_status, manifest = self._read_metadata(
            root / _MANIFEST_NAME,
            DatasetManifest,
        )
        if manifest_status != "ready":
            return self._unavailable(
                "unavailable",
                f"manifest_{manifest_status}",
                mount_type=mount.filesystem_type,
                sentinel_status="ready",
                manifest_status=manifest_status,
            )
        return StorageReadiness(
            mode="nas",
            status="ready",
            market_data_available=True,
            serving_source="nas",
            mount_type=mount.filesystem_type,
            sentinel_status="ready",
            manifest_status="ready",
            dataset_generation=manifest.generation,
        )

    def _overlaps_local_storage(self, root: Path) -> bool:
        local_paths = (
            self.settings.market_data_dir,
            self.settings.user_data_dir,
            self.settings.local_control_dir,
            self.settings.local_staging_dir,
            self.settings.local_lock_dir,
            self.settings.local_temp_dir,
        )
        return any(_contains(root, path) or _contains(path, root) for path in local_paths)

    @staticmethod
    def _read_metadata(path: Path, model_type):
        if not path.exists():
            return "missing", None
        if not path.is_file():
            return "invalid", None
        try:
            if path.stat().st_size > _MAX_METADATA_BYTES:
                return "invalid", None
            payload = json.loads(path.read_text(encoding="utf-8"))
            return "ready", model_type.model_validate(payload)
        except PermissionError:
            return "unreadable", None
        except (OSError, UnicodeError):
            return "unreadable", None
        except (json.JSONDecodeError, ValidationError):
            return "invalid", None

    @staticmethod
    def _unavailable(
        status: str,
        reason_code: str,
        *,
        mount_type: str | None = None,
        sentinel_status: str = "missing",
        manifest_status: str = "missing",
    ) -> StorageReadiness:
        return StorageReadiness(
            mode="nas",
            status=status,
            market_data_available=False,
            serving_source="none",
            reason_code=reason_code,
            mount_type=mount_type,
            sentinel_status=sentinel_status,
            manifest_status=manifest_status,
        )
