"""Verified local mirror for an immutable market dataset."""

import argparse
import json
import os
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from backend.app.market.store import MarketStore
from backend.app.storage.dataset import (
    MANIFEST_NAME,
    SENTINEL_NAME,
    DatasetError,
    NasMarketStore,
    _safe_relative,
    _sha256,
)


@dataclass(frozen=True)
class MirrorResult:
    status: str
    generation: str
    file_count: int
    row_count: int
    copied_bytes: int
    source: str
    destination: str


def _same_file(path: Path, item: dict[str, object]) -> bool:
    return path.stat().st_size > 0 and _sha256(path) == item["sha256"]


class MarketDatasetMirror:
    def __init__(self, source: Path, destination: Path) -> None:
        self.source = source.resolve()
        self.destination = destination.resolve()

    @staticmethod
    def _validate(root: Path) -> dict[str, object]:
        with TemporaryDirectory(prefix="stock-eva-mirror-validate-") as raw:
            workspace = Path(raw)
            control = MarketStore(
                workspace / "control.duckdb",
                temp_directory=workspace / "tmp",
            )
            store = NasMarketStore(control, root, workspace / "staging")
            store.validate_readiness()
            return store._manifest()

    def _validate_paths(self) -> None:
        if self.source == self.destination:
            raise DatasetError("mirror source and destination must differ")
        if self.destination.is_relative_to(self.source):
            raise DatasetError("mirror destination must not be inside source")
        if self.source.is_relative_to(self.destination):
            raise DatasetError("mirror source must not be inside destination")

    @staticmethod
    def _entries(manifest: dict[str, object]) -> dict[tuple[str, str], dict[str, object]]:
        return {(str(item["source"]), str(item["trade_date"])): item for item in manifest["files"]}

    @classmethod
    def _relationship(
        cls,
        source: dict[str, object],
        destination: dict[str, object],
    ) -> str:
        source_entries = cls._entries(source)
        destination_entries = cls._entries(destination)
        for key in source_entries.keys() & destination_entries.keys():
            source_item = source_entries[key]
            destination_item = destination_entries[key]
            comparable = ("path", "sha256", "row_count")
            if any(source_item[field] != destination_item[field] for field in comparable):
                return "conflict"
        if source_entries.keys() == destination_entries.keys():
            return "same"
        if source_entries.keys() < destination_entries.keys():
            return "destination_newer"
        if destination_entries.keys() < source_entries.keys():
            return "source_newer"
        return "conflict"

    def sync(self) -> MirrorResult:
        self._validate_paths()
        source_manifest = self._validate(self.source)
        source_text = (self.source / MANIFEST_NAME).read_text(encoding="utf-8")
        destination_manifest = None
        if self.destination.exists():
            try:
                destination_manifest = self._validate(self.destination)
            except DatasetError:
                if (self.destination / MANIFEST_NAME).exists():
                    raise
            if destination_manifest is not None:
                relationship = self._relationship(source_manifest, destination_manifest)
                if relationship in {"same", "destination_newer"}:
                    return self._result(
                        "reused" if relationship == "same" else "destination_newer",
                        destination_manifest,
                        0,
                    )
                if relationship == "conflict":
                    raise DatasetError("mirror source conflicts with the local dataset")

        self.destination.parent.mkdir(parents=True, exist_ok=True)
        stage = self.destination.with_name(f".{self.destination.name}.{uuid4().hex}.partial")
        copied_bytes = 0
        try:
            stage.mkdir(mode=0o700)
            sentinel = self.source / SENTINEL_NAME
            shutil.copy2(sentinel, stage / SENTINEL_NAME)
            copied_bytes += sentinel.stat().st_size
            for item in source_manifest["files"]:
                relative = _safe_relative(str(item["path"]))
                source_path = self.source / relative
                destination_path = stage / relative
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, destination_path)
                copied_bytes += source_path.stat().st_size
            (stage / MANIFEST_NAME).write_text(source_text, encoding="utf-8")
            copied_bytes += (self.source / MANIFEST_NAME).stat().st_size
            self._validate(stage)

            self.destination.mkdir(mode=0o700, exist_ok=True)
            for item in source_manifest["files"]:
                relative = _safe_relative(str(item["path"]))
                staged_path = stage / relative
                final_path = self.destination / relative
                final_path.parent.mkdir(parents=True, exist_ok=True)
                if final_path.is_file() and _same_file(final_path, item):
                    continue
                os.replace(staged_path, final_path)
            os.replace(stage / SENTINEL_NAME, self.destination / SENTINEL_NAME)
            # The manifest is the publication pointer and must always move last.
            os.replace(stage / MANIFEST_NAME, self.destination / MANIFEST_NAME)
            self._validate(self.destination)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
        return self._result("copied", source_manifest, copied_bytes)

    def _result(
        self,
        status: str,
        manifest: dict[str, object],
        copied_bytes: int,
    ) -> MirrorResult:
        files = manifest["files"]
        return MirrorResult(
            status=status,
            generation=str(manifest["generation"]),
            file_count=len(files),
            row_count=sum(int(item["row_count"]) for item in files),
            copied_bytes=copied_bytes,
            source=str(self.source),
            destination=str(self.destination),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify and copy a market dataset locally")
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if not args.execute:
        print(
            json.dumps(
                {
                    "status": "dry-run",
                    "source": str(args.source),
                    "destination": str(args.destination),
                    "writes_destination": False,
                },
                ensure_ascii=False,
            )
        )
        return 0
    try:
        result = MarketDatasetMirror(args.source, args.destination).sync()
    except (DatasetError, OSError):
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason_code": "market_dataset_mirror_failed",
                    "writes_destination": False,
                },
                ensure_ascii=False,
            )
        )
        return 1
    print(json.dumps(asdict(result), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
