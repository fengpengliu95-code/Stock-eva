from pathlib import Path
from typing import Protocol

from pydantic import BaseModel


class StagedDataset(BaseModel):
    generation: str
    local_path: Path
    manifest_path: Path
    sha256: str


class PartialDataset(BaseModel):
    generation: str
    relative_path: Path
    sha256: str


class PublishedDataset(BaseModel):
    generation: str
    manifest_relative_path: Path
    sha256: str


class DatasetPublication(Protocol):
    """Future SMB publisher contract; implementations must preserve these phases."""

    def stage_and_validate(self, source: Path, staging_root: Path) -> StagedDataset: ...

    def upload_partial(
        self,
        staged: StagedDataset,
        dataset_root: Path,
    ) -> PartialDataset: ...

    def readback_and_verify(
        self,
        partial: PartialDataset,
        dataset_root: Path,
    ) -> None: ...

    def publish_by_atomic_rename(
        self,
        partial: PartialDataset,
        dataset_root: Path,
    ) -> PublishedDataset: ...

    def publish_manifest(
        self,
        published: PublishedDataset,
        dataset_root: Path,
    ) -> None: ...
